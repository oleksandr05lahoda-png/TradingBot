package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * SignalSender (upgraded for futures / MTF analysis)
 *
 * Replaces previous SignalSender with:
 * - multi-timeframe fetching (1m,5m,15m,1h)
 * - EMA trend filter (1h)
 * - ATR volatility filter
 * - volume filter
 * - impulse (huge candle) filter
 * - cooldown per symbol
 * - adaptive confidence combining indicators + confirmations
 *
 * Notes:
 * - No external libs required except org.json
 * - Tunable via ENV variables
 */
public class SignalSender {

    private final TelegramBotSender bot;
    private final HttpClient http;
    private final Set<String> STABLE = Set.of("USDT");

    // CONFIG (from ENV or defaults)
    private final int TOP_N;
    private final double MIN_CONF;            // base raw threshold (0..1)
    private final int INTERVAL_MIN;           // scheduler interval minutes
    private final int KLINES_LIMIT;           // number of candles fetched for each TF
    private final long REQUEST_DELAY_MS;      // delay between HTTP calls to avoid rate limits

    // MTF timeframes used
    private final String TF_ENTRY = "1m";     // entry TF (could be 1m/3m/5m)
    private final String TF_SHORT = "5m";
    private final String TF_MED   = "15m";
    private final String TF_LONG  = "1h";     // trend TF

    // additional thresholds
    private final double IMPULSE_PCT = Double.parseDouble(System.getenv().getOrDefault("IMPULSE_PCT", "0.015")); // 1.5%
    private final double VOL_MULTIPLIER = Double.parseDouble(System.getenv().getOrDefault("VOL_MULT", "0.8"));
    private final double ATR_MIN_PCT = Double.parseDouble(System.getenv().getOrDefault("ATR_MIN_PCT", "0.0005")); // 0.05%
    private final long COOLDOWN_MS = Long.parseLong(System.getenv().getOrDefault("COOLDOWN_MS", String.valueOf(30*1000))); // default 30s cooldown per symbol

    private Set<String> BINANCE_PAIRS = new HashSet<>();
    private long lastBinancePairsRefresh = 0L; // millis
    private final long BINANCE_REFRESH_INTERVAL_MS = 60 * 60 * 1000L; // 60 min

    private final Map<String, Long> lastOpenTimeMap = new ConcurrentHashMap<>();  // to avoid dupes (per pair)
    private final Map<String, Long> lastSignalTime = new ConcurrentHashMap<>();   // cooldown per pair

    public SignalSender(TelegramBotSender bot) {
        this.bot = bot;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        this.TOP_N = Integer.parseInt(System.getenv().getOrDefault("TOP_N", "100"));
        this.MIN_CONF = Double.parseDouble(System.getenv().getOrDefault("MIN_CONFIDENCE", "0.5"));
        this.INTERVAL_MIN = Integer.parseInt(System.getenv().getOrDefault("INTERVAL_MINUTES", "15"));
        this.KLINES_LIMIT = Integer.parseInt(System.getenv().getOrDefault("KLINES", "80"));
        this.REQUEST_DELAY_MS = Long.parseLong(System.getenv().getOrDefault("REQUEST_DELAY_MS", "150"));
    }

    // ---------- Binace helpers ----------
    public Set<String> getBinanceSymbols() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.binance.com/api/v3/exchangeInfo"))
                    .timeout(Duration.ofSeconds(10))
                    .GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            JSONObject json = new JSONObject(resp.body());
            JSONArray arr = json.getJSONArray("symbols");

            Set<String> result = new HashSet<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject s = arr.getJSONObject(i);
                String symbol = s.getString("symbol");
                String status = s.optString("status", "TRADING");
                if ("TRADING".equalsIgnoreCase(status) && symbol.endsWith("USDT")) {
                    result.add(symbol);
                }
            }
            System.out.println("[Binance] Loaded " + result.size() + " spot USDT pairs");
            return result;
        } catch (Exception e) {
            System.out.println("[Binance] Could NOT load pairs: " + e.getMessage());
            return Set.of("BTCUSDT", "ETHUSDT", "BNBUSDT");
        }
    }

    private void ensureBinancePairsFresh() {
        long now = System.currentTimeMillis();
        if (BINANCE_PAIRS.isEmpty() || (now - lastBinancePairsRefresh) > BINANCE_REFRESH_INTERVAL_MS) {
            BINANCE_PAIRS = getBinanceSymbols();
            lastBinancePairsRefresh = now;
        }
    }

    public List<String> getTopSymbols(int limit) {
        try {
            String url = String.format(
                    "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=%d&page=1",
                    limit
            );
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            JSONArray arr = new JSONArray(resp.body());

            List<String> list = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject c = arr.getJSONObject(i);
                String sym = c.getString("symbol").toUpperCase();
                if (STABLE.contains(sym)) continue;
                list.add(sym + "USDT");
            }
            return list;
        } catch (Exception e) {
            System.out.println("[CoinGecko] Error: " + e.getMessage());
            return List.of("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT");
        }
    }

    // ---------- Candle / Klines parser ----------
    public static class Candle {
        public final long openTime;
        public final double open;
        public final double high;
        public final double low;
        public final double close;
        public final double volume;
        public final double quoteAssetVolume;
        public final long closeTime;

        public Candle(long openTime, double open, double high, double low, double close,
                      double volume, double quoteAssetVolume, long closeTime) {
            this.openTime = openTime;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.quoteAssetVolume = quoteAssetVolume;
            this.closeTime = closeTime;
        }
    }

    /**
     * Fetch klines for given symbol & interval.
     * Returns list ordered by time ascending (oldest -> newest)
     */
    public List<Candle> fetchKlines(String symbol, String interval, int limit) {
        try {
            Thread.sleep(REQUEST_DELAY_MS);
            String url = String.format("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d",
                    symbol, interval, limit);
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            String body = resp.body();
            if (body == null || body.isEmpty() || !body.startsWith("[")) {
                System.out.println("[Binance] Invalid klines response for " + symbol + " " + interval + ": " + body);
                return Collections.emptyList();
            }
            JSONArray arr = new JSONArray(body);
            List<Candle> list = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONArray k = arr.getJSONArray(i);
                long openTime = k.getLong(0);
                double open = Double.parseDouble(k.getString(1));
                double high = Double.parseDouble(k.getString(2));
                double low = Double.parseDouble(k.getString(3));
                double close = Double.parseDouble(k.getString(4));
                double vol = Double.parseDouble(k.getString(5));
                double qvol = Double.parseDouble(k.getString(7));
                long closeTime = k.getLong(6);
                list.add(new Candle(openTime, open, high, low, close, vol, qvol, closeTime));
            }
            return list;
        } catch (Exception e) {
            System.out.println("[Binance] Error fetching klines " + symbol + " " + interval + " : " + e.getMessage());
            return Collections.emptyList();
        }
    }

    // ---------- Indicators (basic implementations) ----------
    // SMA
    public static double sma(List<Double> prices, int period) {
        if (prices == null || prices.size() < period) return prices.get(prices.size()-1);
        double sum = 0;
        for (int i = prices.size()-period; i < prices.size(); i++) sum += prices.get(i);
        return sum / period;
    }

    // EMA (simple iterative)
    public static double ema(List<Double> prices, int period) {
        double k = 2.0 / (period + 1);
        double ema = prices.get(0);
        for (double p : prices) ema = p * k + ema * (1 - k);
        return ema;
    }

    // RSI (Wilder's with simple method)
    public static double rsi(List<Double> prices, int period) {
        if (prices == null || prices.size() <= period) return 50.0;
        double gain = 0, loss = 0;
        for (int i = prices.size() - period; i < prices.size(); i++) {
            double diff = prices.get(i) - prices.get(i - 1);
            if (diff > 0) gain += diff; else loss -= diff;
        }
        if (gain + loss == 0) return 50.0;
        double rs = gain / (loss + 1e-12);
        return 100.0 - (100.0 / (1.0 + rs));
    }

    // ATR (average true range) - returns latest ATR value (not normalized)
    public static double atr(List<Candle> candles, int period) {
        if (candles == null || candles.size() <= period) return 0.0;
        List<Double> trs = new ArrayList<>();
        for (int i = 1; i < candles.size(); i++) {
            Candle prev = candles.get(i - 1);
            Candle cur = candles.get(i);
            double highLow = cur.high - cur.low;
            double highClose = Math.abs(cur.high - prev.close);
            double lowClose = Math.abs(cur.low - prev.close);
            double tr = Math.max(highLow, Math.max(highClose, lowClose));
            trs.add(tr);
        }
        if (trs.size() < period) return 0.0;
        double sum = 0;
        for (int i = trs.size() - period; i < trs.size(); i++) sum += trs.get(i);
        return sum / period;
    }

    // momentum: last price change over n candles (relative)
    public static double momentumPct(List<Double> prices, int n) {
        if (prices == null || prices.size() <= n) return 0.0;
        double last = prices.get(prices.size() - 1);
        double prev = prices.get(prices.size() - 1 - n);
        return (last - prev) / (prev + 1e-12);
    }

    // ---------- Utility helpers ----------
    private double avgQuoteVolume(List<Candle> candles, int lookback) {
        if (candles == null || candles.isEmpty()) return 0.0;
        int lb = Math.min(lookback, candles.size());
        double s = 0;
        for (int i = candles.size() - lb; i < candles.size(); i++) s += candles.get(i).quoteAssetVolume;
        return s / lb;
    }

    // cooldown check
    private boolean isCooldown(String pair) {
        long now = System.currentTimeMillis();
        long last = lastSignalTime.getOrDefault(pair, 0L);
        return (now - last) < COOLDOWN_MS;
    }
    private void markSignalSent(String pair) {
        lastSignalTime.put(pair, System.currentTimeMillis());
    }

    // ---------- Filters / Signal confirmation ----------
    // Trend filter using EMA on 1h: if 1h shortEMA > longEMA => uptrend
    public boolean isUptrend(List<Candle> candles1h) {
        if (candles1h == null || candles1h.size() < 50) return false;
        List<Double> closes = new ArrayList<>();
        for (Candle c : candles1h) closes.add(c.close);
        double e20 = ema(closes, 20);
        double e50 = ema(closes, 50);
        return e20 > e50;
    }
    public boolean isDowntrend(List<Candle> candles1h) {
        if (candles1h == null || candles1h.size() < 50) return false;
        List<Double> closes = new ArrayList<>();
        for (Candle c : candles1h) closes.add(c.close);
        double e20 = ema(closes, 20);
        double e50 = ema(closes, 50);
        return e20 < e50;
    }

    // volatility stable: ATR relative to price (pct)
    public boolean isVolatilityOk(List<Candle> candles, double minAtrPct) {
        if (candles == null || candles.size() < 20) return false;
        double atrVal = atr(candles, 14);
        double lastPrice = candles.get(candles.size()-1).close;
        double atrPct = atrVal / (lastPrice + 1e-12);
        return atrPct >= minAtrPct; // require at least some ATR (not zero)
    }

    // volume filter: require recent quote volume not too low vs recent average
    public boolean isVolumeOk(List<Candle> candles) {
        if (candles == null || candles.size() < 10) return false;
        double avg = avgQuoteVolume(candles, 10);
        double lastQ = candles.get(candles.size()-1).quoteAssetVolume;
        // require last >= avg * VOL_MULTIPLIER (VOL_MULT default 0.8)
        return lastQ >= avg * VOL_MULTIPLIER;
    }

    // impulse filter: ignore huge one-minute candles (can be liquidation)
    public boolean isImpulseCandle(Candle c) {
        if (c == null) return false;
        double change = Math.abs(c.close - c.open) / (c.open + 1e-12);
        return change >= IMPULSE_PCT;
    }

    // multi-timeframe confirmation: returns +1 (long confirmed), -1 (short confirmed), 0 none
    public int multiTFConfirmation(List<Candle> c1m, List<Candle> c5m, List<Candle> c15m, List<Candle> c1h) {
        // basic rule:
        // - 1h determines bias (up/down)
        // - 15m/5m must agree with 1m
        boolean up1h = isUptrend(c1h);
        boolean down1h = isDowntrend(c1h);
        // compute short-term ema direction on 15m and 5m and 1m
        int dir1m = emaDirection(c1m, 9, 21);
        int dir5m = emaDirection(c5m, 9, 21);
        int dir15m = emaDirection(c15m, 9, 21);

        // If 1h says up, require 15m & 5m non-negative
        if (up1h) {
            if ((dir15m >= 0) && (dir5m >= 0)) return dir1m >= 0 ? +1 : 0;
            else return 0;
        }
        if (down1h) {
            if ((dir15m <= 0) && (dir5m <= 0)) return dir1m <= 0 ? -1 : 0;
            else return 0;
        }
        // if no strong 1h bias: require agreement between 1m/5m/15m
        if (dir1m > 0 && dir5m > 0 && dir15m > 0) return +1;
        if (dir1m < 0 && dir5m < 0 && dir15m < 0) return -1;
        return 0;
    }

    // emaDirection returns 1 if shortEMA > longEMA, -1 if shortEMA < longEMA, 0 if unclear
    private int emaDirection(List<Candle> candles, int shortP, int longP) {
        if (candles == null || candles.size() < longP + 2) return 0;
        List<Double> closes = new ArrayList<>();
        for (Candle c : candles) closes.add(c.close);
        double s = ema(closes, shortP);
        double l = ema(closes, longP);
        if (s > l * 1.0005) return 1;
        if (s < l * 0.9995) return -1;
        return 0;
    }

    // compose adaptive confidence: baseScore from indicators + confirmations
    private double composeConfidence(double rawScore, int mtfConfirm, boolean volOk, boolean atrOk, boolean impulseOk) {
        double base = Math.min(1.0, Math.abs(rawScore));
        // boost if confirmations align with sign
        double boost = 0.0;
        if (mtfConfirm != 0) boost += 0.15; // MTF confirmation is strong
        if (volOk) boost += 0.1;
        if (atrOk) boost += 0.05;
        if (!impulseOk) boost += 0.05; // small bonus if not impulse (i.e. safe)
        double conf = Math.min(1.0, base + boost);
        return conf;
    }

    // ---------- Strategy: evaluate with filters ----------
    public Optional<Signal> evaluate(String pair,
                                     List<Candle> c1m, List<Candle> c5m, List<Candle> c15m, List<Candle> c1h) {
        // basic checks
        if (c1m == null || c1m.size() < 22) return Optional.empty();

        // cooldown
        if (isCooldown(pair)) {
            return Optional.empty();
        }

        // avoid sending duplicate signal for same candle (use open time from 1m last)
        long lastOpen = lastOpenTimeMap.getOrDefault(pair, 0L);
        long newOpen = c1m.get(c1m.size() - 1).openTime;
        if (newOpen <= lastOpen) return Optional.empty(); // already processed
        lastOpenTimeMap.put(pair, newOpen);

        // compute indicator primitives on entry timeframe
        List<Double> closes1m = new ArrayList<>();
        for (Candle c : c1m) closes1m.add(c.close);

        // indicators (similar to your earlier strategy, but on prices from 1m)
        double emaScore = strategyEMANormFromCloses(closes1m);  // -1..1
        double rsiScore = strategyRSINormFromCloses(closes1m);  // -1..1
        double macdScore = strategyMACDNormFromCloses(closes1m);
        double momScore = strategyMomentumNormFromCloses(closes1m);

        double rawScore = emaScore * 0.35 + rsiScore * 0.30 + macdScore * 0.25 + momScore * 0.10;

        // compute filter flags
        boolean volOk = isVolumeOk(c1m); // volume on 1m
        boolean atrOk = isVolatilityOk(c5m, ATR_MIN_PCT); // ATR check on 5m
        boolean impulse = isImpulseCandle(c1m.get(c1m.size() - 1));

        // MTF confirmation
        int mtf = multiTFConfirmation(c1m, c5m, c15m, c1h);

        // compose adaptive confidence
        double confidence = composeConfidence(rawScore, mtf, volOk, atrOk, !impulse);

        String direction;
        if (rawScore >= 0 && confidence >= MIN_CONF && (mtf >= 0)) direction = "LONG";
        else if (rawScore <= 0 && confidence >= MIN_CONF && (mtf <= 0)) direction = "SHORT";
        else return Optional.empty();

        // extra safety: if impulse candle present, skip
        if (impulse) {
            System.out.println(String.format("[Filter] %s skipped due impulse candle (%.3f)", pair,
                    Math.abs(c1m.get(c1m.size()-1).close - c1m.get(c1m.size()-1).open) / (c1m.get(c1m.size()-1).open + 1e-12)));
            return Optional.empty();
        }

        // ensure trend alignment with 1h: if 1h up, skip SHORT; if 1h down, skip LONG
        boolean up1h = isUptrend(c1h);
        boolean down1h = isDowntrend(c1h);
        if (direction.equals("LONG") && down1h) {
            return Optional.empty();
        }
        if (direction.equals("SHORT") && up1h) {
            return Optional.empty();
        }

        // finalize signal
        double lastPrice = c1m.get(c1m.size()-1).close;
        double rsiVal = rsi(closes1m, 14);
        double rawAbs = Math.abs(rawScore);

        Signal s = new Signal(pair.replace("USDT", ""), direction, confidence, lastPrice, rsiVal, rawScore, mtf, volOk, atrOk);
        // mark cooldown
        markSignalSent(pair);
        return Optional.of(s);
    }

    // --- Strategy helper functions (based on closes arrays) ---
    private double strategyEMANormFromCloses(List<Double> closes) {
        if (closes == null || closes.size() < 200) {
            // fallback to shorter EMAs if not enough data
            double e50 = ema(closes, Math.min(50, closes.size()-1));
            double e200 = ema(closes, Math.min(200, closes.size()-1));
            return e50 > e200 ? 1.0 : -1.0;
        } else {
            double e50 = ema(closes, 50);
            double e200 = ema(closes, 200);
            return e50 > e200 ? 1.0 : -1.0;
        }
    }

    private double strategyRSINormFromCloses(List<Double> closes) {
        double r = rsi(closes, 14);
        if (r < 30) return 1.0;
        else if (r > 70) return -1.0;
        else return 0.0;
    }

    private double strategyMACDNormFromCloses(List<Double> closes) {
        // approximate macd histogram via ema difference, normalized by price
        double macd = ema(closes, 12) - ema(closes, 26);
        double last = closes.get(closes.size() - 1);
        double rel = macd / (last + 1e-12);
        return Math.max(-1.0, Math.min(1.0, rel / 0.01));
    }

    private double strategyMomentumNormFromCloses(List<Double> closes) {
        double raw = momentumPct(closes, 3);
        return Math.max(-1.0, Math.min(1.0, raw / 0.01));
    }

    // ---------- Signal data class ----------
    public static class Signal {
        public final String symbol;
        public final String direction;
        public final double confidence;
        public final double price;
        public final double rsi;
        public final double rawScore;
        public final int mtfConfirm;
        public final boolean volOk;
        public final boolean atrOk;
        public final Instant created = Instant.now();

        public Signal(String symbol, String direction, double confidence, double price, double rsi, double rawScore,
                      int mtfConfirm, boolean volOk, boolean atrOk) {
            this.symbol = symbol;
            this.direction = direction;
            this.confidence = confidence;
            this.price = price;
            this.rsi = rsi;
            this.rawScore = rawScore;
            this.mtfConfirm = mtfConfirm;
            this.volOk = volOk;
            this.atrOk = atrOk;
        }

        public String toTelegramMessage() {
            return String.format("*%s* → *%s*\nConfidence: *%.2f*\nPrice: %.8f\nRSI(14): %.2f\n_raw: %.3f mtf:%d vol:%b atr:%b_\n_time: %s_",
                    symbol, direction, confidence, price, rsi, rawScore, mtfConfirm, volOk, atrOk, created.toString());
        }
    }

    // ---------- Start Scheduler (main loop) ----------
    public void start() {
        System.out.println("[SignalSender] Starting TOP_N=" + TOP_N + " MIN_CONF=" + MIN_CONF + " INTERVAL_MIN=" + INTERVAL_MIN);
        ensureBinancePairsFresh();
        try {
            bot.sendSignal("✅ SignalSender (futures mode) запущен и работает!");
        } catch (Exception e) {
            System.out.println("[Telegram Test Message Error] " + e.getMessage());
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                ensureBinancePairsFresh();
                List<String> filtered = getTopSymbols(TOP_N).stream().filter(BINANCE_PAIRS::contains).toList();
                for (String pair : filtered) {
                    // For each pair fetch MTF klines (1m,5m,15m,1h)
                    List<Candle> c1m = fetchKlines(pair, TF_ENTRY, Math.max(KLINES_LIMIT, 80));
                    if (c1m.isEmpty()) continue;

                    // use smaller lookback for faster decisions on short TFs
                    List<Candle> c5m = fetchKlines(pair, TF_SHORT, Math.max(60, KLINES_LIMIT/3));
                    List<Candle> c15m = fetchKlines(pair, TF_MED, Math.max(60, KLINES_LIMIT/6));
                    List<Candle> c1h = fetchKlines(pair, TF_LONG, Math.max(80, KLINES_LIMIT/12));

                    // evaluate signal using MTF and filters
                    Optional<Signal> opt = evaluate(pair, c1m, c5m, c15m, c1h);
                    opt.ifPresent(s -> {
                        try { bot.sendSignal(s.toTelegramMessage()); }
                        catch (Exception e) { System.out.println("[Telegram] send error: " + e.getMessage()); }
                    });
                }
            } catch (Exception e) {
                System.out.println("[Job error] " + e.getMessage());
            }
        }, 0, INTERVAL_MIN, TimeUnit.MINUTES);
    }
}
