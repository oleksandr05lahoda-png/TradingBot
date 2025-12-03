package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SignalSender {

    private final TelegramBotSender bot;
    private final HttpClient http;

    // ---------------- CONFIG (env or defaults) ----------------
    private final int TOP_N;
    private final double MIN_CONF;            // 0..1
    private final int INTERVAL_MIN;           // scheduler interval minutes
    private final int KLINES_LIMIT;           // number of candles fetched per TF
    private final long REQUEST_DELAY_MS;      // delay between HTTP calls

    private final double IMPULSE_PCT;         // one-minute impulse threshold (relative)
    private final double VOL_MULTIPLIER;      // lastQ >= avgQ * VOL_MULTIPLIER
    private final double ATR_MIN_PCT;         // min ATR pct for volatility check
    private final long COOLDOWN_MS;           // cooldown per symbol
    private final long BINANCE_REFRESH_INTERVAL_MS; // refresh pairs

    private final LocalTime VWAP_SESSION_START;

    // Micro / tick params
    private final int TICK_HISTORY;
    private final double OBI_THRESHOLD;
    private final double VOLUME_SPIKE_MULT;

    // ---------------- internal state ----------------
    private final Set<String> STABLE; // tokens to skip
    private Set<String> BINANCE_PAIRS = new HashSet<>();
    private long lastBinancePairsRefresh = 0L;

    private final Map<String, Long> lastOpenTimeMap = new ConcurrentHashMap<>();   // openTime per symbol processed
    private final Map<String, Long> lastSignalTime = new ConcurrentHashMap<>();     // last sent timestamp
    private final Map<String, Double> lastSentConfidence = new ConcurrentHashMap<>(); // last confidence
    private final Map<String, Double> lastPriceMap = new ConcurrentHashMap<>();

    private final DateTimeFormatter dtf = DateTimeFormatter.ISO_INSTANT;

    // tick / ws state
    private java.net.http.WebSocket wsTick; // optional single ws used per pair connect model
    private final Map<String, Deque<Double>> tickPriceDeque = new ConcurrentHashMap<>();
    private final Map<String, Double> lastTickPrice = new ConcurrentHashMap<>();
    private final Map<String, Long> lastTickTime = new ConcurrentHashMap<>();
    private final Map<String, MicroCandleBuilder> microBuilders = new ConcurrentHashMap<>();
    private final Map<String, OrderbookSnapshot> orderbookMap = new ConcurrentHashMap<>();

    // daily request tracking / rate limiting
    private final AtomicLong dailyRequests = new AtomicLong(0);
    private long dailyResetTs = System.currentTimeMillis();

    // scheduler
    private ScheduledExecutorService scheduler;

    // Constructor
    public SignalSender(TelegramBotSender bot) {
        this.bot = bot;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        // defaults (use env to override)
        this.TOP_N = envInt("TOP_N", 100);
        this.MIN_CONF = envDouble("MIN_CONFIDENCE", 0.7); // user earlier wanted >=0.5 default
        this.INTERVAL_MIN = envInt("INTERVAL_MINUTES", 1); // quick cycles for futures (1 min)
        this.KLINES_LIMIT = envInt("KLINES", 240);
        this.REQUEST_DELAY_MS = envLong("REQUEST_DELAY_MS", 120);

        this.IMPULSE_PCT = envDouble("IMPULSE_PCT", 0.02);
        this.VOL_MULTIPLIER = envDouble("VOL_MULT", 0.9);
        this.ATR_MIN_PCT = envDouble("ATR_MIN_PCT", 0.0007);
        this.COOLDOWN_MS = envLong("COOLDOWN_MS", 30000); // 3 minutes default
        long brMin = envLong("BINANCE_REFRESH_MINUTES", 60);
        this.BINANCE_REFRESH_INTERVAL_MS = brMin * 60 * 1000L;

        this.VWAP_SESSION_START = LocalTime.parse(System.getenv().getOrDefault("SESSION_START", "00:00"));

        this.TICK_HISTORY = envInt("TICK_HISTORY", 300);
        this.OBI_THRESHOLD = envDouble("OBI_THRESHOLD", 0.28);
        this.VOLUME_SPIKE_MULT = envDouble("VOL_SPIKE_MULT", 1.4);

        this.STABLE = Set.of("USDT", "USDC", "BUSD");

        System.out.println("[SignalSender] INIT: TOP_N=" + TOP_N + " MIN_CONF=" + MIN_CONF + " INTERVAL_MIN=" + INTERVAL_MIN);
    }

    // ========================= Helpers for env parsing =========================
    private int envInt(String k, int def) {
        try { return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(def))); }
        catch (Exception e) { return def; }
    }
    private long envLong(String k, long def) {
        try { return Long.parseLong(System.getenv().getOrDefault(k, String.valueOf(def))); }
        catch (Exception e) { return def; }
    }
    private double envDouble(String k, double def) {
        try { return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(def))); }
        catch (Exception e) { return def; }
    }

    // ========================= Binance Helpers =========================
    public Set<String> getBinanceSymbolsFutures() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://fapi.binance.com/fapi/v1/exchangeInfo"))
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
                String contractType = s.optString("contractType", "");
                // only USDT perpetual / TRADING
                if ("TRADING".equalsIgnoreCase(status) && symbol.endsWith("USDT")) {
                    result.add(symbol);
                }
            }
            System.out.println("[BinanceFutures] Loaded " + result.size() + " pairs");
            return result;
        } catch (Exception e) {
            System.out.println("[BinanceFutures] Could NOT load pairs: " + e.getMessage());
            return Set.of("BTCUSDT", "ETHUSDT", "BNBUSDT");
        }
    }

    private void ensureBinancePairsFresh() {
        long now = System.currentTimeMillis();
        if (BINANCE_PAIRS.isEmpty() || (now - lastBinancePairsRefresh) > BINANCE_REFRESH_INTERVAL_MS) {
            // Получаем топ 100 монет CoinGecko
            List<String> topSymbols = getTopSymbols(TOP_N); // TOP_N = 100
            // Оставляем только те, которые есть на Binance USDT фьючерсах
            Set<String> allBinance = getBinanceSymbolsFutures();
            BINANCE_PAIRS = topSymbols.stream()
                    .filter(allBinance::contains)
                    .collect(Collectors.toSet());
            lastBinancePairsRefresh = now;
            System.out.println("[BinanceFutures] Using TOP " + BINANCE_PAIRS.size() + " pairs");
        }
    }


    // ========================= Fetch Klines (Futures) =========================
    public CompletableFuture<List<Candle>> fetchKlinesAsync(String symbol, String interval, int limit) {
        try {
            String url = String.format("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d", symbol, interval, limit);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            return http.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                    .thenApply(resp -> {
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
                    });
        } catch (Exception e) {
            System.out.println("[Binance] Error preparing klines request for " + symbol + " : " + e.getMessage());
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    // ========================= Candle data class =========================
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

        public double body() { return Math.abs(close - open); }
        public double bodyPct() { return body() / (open + 1e-12); }
        public boolean isBull() { return close > open; }
        public boolean isBear() { return close < open; }
    }

    // ========================= Micro candle builder (ticks -> tiny candles) =========================
    public static class MicroCandleBuilder {
        private final int intervalMs;
        private long currentBucketStart = -1;
        private double open = Double.NaN, high = Double.NEGATIVE_INFINITY, low = Double.POSITIVE_INFINITY, close = Double.NaN;
        private double volume = 0.0;
        private long closeTime = -1;

        public MicroCandleBuilder(int intervalMs) { this.intervalMs = intervalMs; }

        public Optional<Candle> addTick(long tsMillis, double price, double qty) {
            long bucket = (tsMillis / intervalMs) * intervalMs;
            if (currentBucketStart == -1) {
                currentBucketStart = bucket;
                open = price; high = price; low = price; close = price; volume = qty; closeTime = bucket + intervalMs - 1;
                return Optional.empty();
            }
            if (bucket == currentBucketStart) {
                high = Math.max(high, price);
                low = Math.min(low, price);
                close = price;
                volume += qty;
                return Optional.empty();
            } else {
                Candle c = new Candle(currentBucketStart, open, high, low, close, volume, volume, closeTime);
                currentBucketStart = bucket;
                open = price; high = price; low = price; close = price; volume = qty; closeTime = bucket + intervalMs - 1;
                return Optional.of(c);
            }
        }
    }

    // ========================= Orderbook snapshot =========================
    public static class OrderbookSnapshot {
        public final double bidVolume;
        public final double askVolume;
        public final long timestamp;

        public OrderbookSnapshot(double bidVolume, double askVolume, long timestamp) {
            this.bidVolume = bidVolume;
            this.askVolume = askVolume;
            this.timestamp = timestamp;
        }

        public double obi() {
            double s = bidVolume + askVolume + 1e-12;
            return (bidVolume - askVolume) / s;
        }
    }

    // ========================= Microtrend container =========================
    public static class MicroTrendResult {
        public final double speed;
        public final double accel;
        public final double avgTick;
        public MicroTrendResult(double speed, double accel, double avgTick) {
            this.speed = speed; this.accel = accel; this.avgTick = avgTick;
        }
    }

    // ========================= Indicator implementations =========================

    // SMA
    public static double sma(List<Double> prices, int period) {
        if (prices == null || prices.size() < period) return prices.get(prices.size() - 1);
        double sum = 0;
        for (int i = prices.size() - period; i < prices.size(); i++) sum += prices.get(i);
        return sum / period;
    }

    // EMA
    public static double ema(List<Double> prices, int period) {
        if (prices == null || prices.isEmpty()) return 0.0;
        double k = 2.0 / (period + 1);
        double ema = prices.get(0);
        for (double p : prices) ema = p * k + ema * (1 - k);
        return ema;
    }

    // RSI (Wilder)
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

    // ATR
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

    // Momentum % over n candles
    public static double momentumPct(List<Double> prices, int n) {
        if (prices == null || prices.size() <= n) return 0.0;
        double last = prices.get(prices.size() - 1);
        double prev = prices.get(prices.size() - 1 - n);
        return (last - prev) / (prev + 1e-12);
    }

    // OBV
    public static double obv(List<Candle> candles) {
        if (candles == null || candles.isEmpty()) return 0.0;
        double val = 0.0;
        for (int i = 1; i < candles.size(); i++) {
            Candle p = candles.get(i - 1);
            Candle c = candles.get(i);
            if (c.close > p.close) val += c.volume;
            else if (c.close < p.close) val -= c.volume;
        }
        return val;
    }

    // VWAP (simple across list)
    public static double vwap(List<Candle> candles) {
        if (candles == null || candles.isEmpty()) return 0.0;
        double pv = 0.0, vol = 0.0;
        for (Candle c : candles) {
            double tp = (c.high + c.low + c.close) / 3.0;
            pv += tp * c.volume;
            vol += c.volume;
        }
        if (vol == 0) return candles.get(candles.size() - 1).close;
        return pv / vol;
    }

    // ========================= Price Action helpers =========================
    public static List<Integer> detectSwingHighs(List<Candle> candles, int leftRight) {
        List<Integer> res = new ArrayList<>();
        for (int i = leftRight; i < candles.size() - leftRight; i++) {
            double v = candles.get(i).high;
            boolean isHigh = true;
            for (int l = i - leftRight; l <= i + leftRight; l++) {
                if (candles.get(l).high > v) { isHigh = false; break; }
            }
            if (isHigh) res.add(i);
        }
        return res;
    }

    public static List<Integer> detectSwingLows(List<Candle> candles, int leftRight) {
        List<Integer> res = new ArrayList<>();
        for (int i = leftRight; i < candles.size() - leftRight; i++) {
            double v = candles.get(i).low;
            boolean isLow = true;
            for (int l = i - leftRight; l <= i + leftRight; l++) {
                if (candles.get(l).low < v) { isLow = false; break; }
            }
            if (isLow) res.add(i);
        }
        return res;
    }

    public static int marketStructure(List<Candle> candles) {
        if (candles == null || candles.size() < 20) return 0;
        List<Integer> highs = detectSwingHighs(candles, 3);
        List<Integer> lows = detectSwingLows(candles, 3);
        if (highs.size() < 2 || lows.size() < 2) return 0;
        int lastHighIdx = highs.get(highs.size() - 1);
        int prevHighIdx = highs.get(highs.size() - 2);
        int lastLowIdx = lows.get(lows.size() - 1);
        int prevLowIdx = lows.get(lows.size() - 2);
        double lastHigh = candles.get(lastHighIdx).high;
        double prevHigh = candles.get(prevHighIdx).high;
        double lastLow = candles.get(lastLowIdx).low;
        double prevLow = candles.get(prevLowIdx).low;
        boolean hh = lastHigh > prevHigh;
        boolean hl = lastLow > prevLow;
        boolean ll = lastLow < prevLow;
        boolean lh = lastHigh < prevHigh;
        if (hh && hl) return 1;
        if (ll && lh) return -1;
        return 0;
    }

    public static boolean detectBOS(List<Candle> candles) {
        if (candles == null || candles.size() < 10) return false;
        List<Integer> highs = detectSwingHighs(candles, 3);
        List<Integer> lows = detectSwingLows(candles, 3);
        if (highs.size() < 2 && lows.size() < 2) return false;
        Candle last = candles.get(candles.size() - 1);
        if (highs.size() >= 2) {
            double lastSwingHigh = candles.get(highs.get(highs.size() - 1)).high;
            if (last.close > lastSwingHigh * 1.0006) return true;
        }
        if (lows.size() >= 2) {
            double lastSwingLow = candles.get(lows.get(lows.size() - 1)).low;
            if (last.close < lastSwingLow * 0.9994) return true;
        }
        return false;
    }

    public static boolean detectLiquiditySweep(List<Candle> candles) {
        if (candles == null || candles.size() < 6) return false;
        int n = candles.size();
        Candle last = candles.get(n - 1);
        Candle prev = candles.get(n - 2);
        double upperWick = last.high - Math.max(last.open, last.close);
        double lowerWick = Math.min(last.open, last.close) - last.low;
        double body = Math.abs(last.close - last.open);
        if (upperWick > body * 1.8 && last.close < prev.close) return true;
        if (lowerWick > body * 1.8 && last.close > prev.close) return true;
        return false;
    }

    // EMA direction helper (with hysteresis)
    private int emaDirection(List<Candle> candles, int shortP, int longP, double hysteresis) {
        if (candles == null || candles.size() < longP + 2) return 0;
        List<Double> closes = candles.stream().map(c -> c.close).collect(Collectors.toList());
        double s = ema(closes, shortP);
        double l = ema(closes, longP);
        if (s > l * (1 + hysteresis)) return 1;
        if (s < l * (1 - hysteresis)) return -1;
        return 0;
    }

    // Overload with default hysteresis
    private int emaDirection(List<Candle> candles, int shortP, int longP) {
        return emaDirection(candles, shortP, longP, 0.001);
    }

    // ========================= Strategy primitives -> normalize to -1..1 =========================

    // EMA norm: measure e20-e50 and e50-e100
    private double strategyEMANorm(List<Double> closes) {
        if (closes == null || closes.size() < 100) return 0.0;
        double e20 = ema(closes, 20);
        double e50 = ema(closes, 50);
        double e100 = ema(closes, 100);
        double a = (e20 - e50) / (e50 + 1e-12);
        double b = (e50 - e100) / (e100 + 1e-12);
        double combined = (a + b) / 2.0;
        return Math.max(-1.0, Math.min(1.0, combined / 0.02));
    }

    private double strategyRSINorm(List<Double> closes) {
        double r = rsi(closes, 14);
        if (r < 20) return 0.9;
        if (r < 30) return 0.5;
        if (r > 80) return -0.9;
        if (r > 70) return -0.5;
        return 0.0;
    }

    private double strategyMACDNorm(List<Double> closes) {
        if (closes == null || closes.size() < 26) return 0.0;
        double macd = ema(closes, 12) - ema(closes, 26);
        double last = closes.get(closes.size() - 1);
        double rel = macd / (last + 1e-12);
        return Math.max(-1.0, Math.min(1.0, rel / 0.008));
    }

    private double strategyMomentumNorm(List<Double> closes) {
        double raw = momentumPct(closes, 3);
        return Math.max(-1.0, Math.min(1.0, raw / 0.01));
    }

    // ----------------- volume helpers -----------------
    private double avgQuoteVolume(List<Candle> candles, int lookback) {
        if (candles == null || candles.isEmpty()) return 0.0;
        int lb = Math.min(lookback, candles.size());
        double s = 0;
        for (int i = candles.size() - lb; i < candles.size(); i++) s += candles.get(i).quoteAssetVolume;
        return s / lb;
    }

    public boolean isVolumeOk(List<Candle> candles) {
        if (candles == null || candles.size() < 10) return false;
        double avg = avgQuoteVolume(candles, 20);
        double lastQ = candles.get(candles.size() - 1).quoteAssetVolume;
        return lastQ >= avg * VOL_MULTIPLIER;
    }

    public boolean isVolatilityOk(List<Candle> candles, double minAtrPct) {
        if (candles == null || candles.size() < 20) return false;
        double atrVal = atr(candles, 14);
        double lastPrice = candles.get(candles.size() - 1).close;
        double atrPct = atrVal / (lastPrice + 1e-12);
        return atrPct >= minAtrPct;
    }

    // Adaptive impulse test
    public boolean isImpulseCandle(Candle c, List<Candle> recent) {
        if (c == null || recent == null || recent.size() < 5) return false;
        double avgBody = recent.stream()
                .skip(Math.max(0, recent.size() - 20))
                .mapToDouble(x -> Math.abs(x.close - x.open))
                .average().orElse(IMPULSE_PCT);
        double adaptiveImpulse = Math.max(IMPULSE_PCT, avgBody * 1.5);
        double change = Math.abs(c.close - c.open) / (c.open + 1e-12);
        return change >= adaptiveImpulse;
    }

    // ----------------- Multi-TF confirm -----------------
    private int multiTFConfirm(int dir1h, int dir15m, int dir5m, int dir1m) {
        int score = 0;
        score += dir1h * 3;
        score += dir15m * 2;
        score += dir5m * 1;
        score += dir1m * 0; // 1m is trigger only
        if (score > 2) return 1;
        if (score < -2) return -1;
        return 0;
    }

    // ----------------- Compose confidence -----------------
    private double composeConfidence(double rawScore, int mtfConfirm, boolean volOk, boolean atrOk,
                                     boolean notImpulse, boolean vwapAligned, boolean structureAligned,
                                     boolean bos, boolean liquiditySweep) {
        double base = Math.min(1.0, Math.abs(rawScore));
        double boost = 0.0;
        if (structureAligned) boost += 0.12;
        if (mtfConfirm != 0 && Integer.signum(mtfConfirm) == Integer.signum((int)Math.signum(rawScore))) boost += 0.10;
        if (volOk) boost += 0.05;
        if (atrOk) boost += 0.03;
        if (vwapAligned) boost += 0.04;
        if (bos) boost += 0.03;
        if (!notImpulse) boost -= 0.02;
        if (liquiditySweep) {
            if (!structureAligned || mtfConfirm == 0) boost -= 0.10;
            else boost -= 0.03;
        }
        double conf = Math.min(1.0, Math.max(0.0, base + boost));
        return conf;
    }

    // ========================= Evaluate strategy per symbol =========================
    public Optional<Signal> evaluate(String pair,
                                     List<Candle> c1m, List<Candle> c5m, List<Candle> c15m, List<Candle> c1h) {
        try {
            // базовые проверки — сначала убедимся, что данные есть
            if (c1m == null || c1m.size() < 30) return Optional.empty();
            if (c5m == null || c5m.size() < 20) return Optional.empty(); // для atr / structure
            if (isCooldown(pair)) return Optional.empty();

            // direction per TF
            int dir1h = emaDirection(c1h, 20, 50, 0.002);
            int dir15m = emaDirection(c15m, 9, 21, 0.002);
            int dir5m = emaDirection(c5m, 9, 21, 0.002);
            int dir1m = emaDirection(c1m, 9, 21, 0.001);

            int mtfConfirm = multiTFConfirm(dir1h, dir15m, dir5m, dir1m);

            long newOpen = c1m.get(c1m.size() - 1).openTime;
            long lastOpen = lastOpenTimeMap.getOrDefault(pair, 0L);
            if (newOpen <= lastOpen) return Optional.empty();
            lastOpenTimeMap.put(pair, newOpen);

            // закладываем списки закрытий
            List<Double> closes1m = c1m.stream().map(c -> c.close).collect(Collectors.toList());
            List<Double> closes5m = c5m.stream().map(c -> c.close).collect(Collectors.toList());
            List<Double> closes15m = c15m.stream().map(c -> c.close).collect(Collectors.toList());
            List<Double> closes1h = c1h.stream().map(c -> c.close).collect(Collectors.toList());

            // индикаторы -> rawScore
            double emaScore = strategyEMANorm(closes1m);
            double rsiScore = strategyRSINorm(closes1m);
            double macdScore = strategyMACDNorm(closes1m);
            double momScore = strategyMomentumNorm(closes1m);
            double rawScore = emaScore * 0.38 + macdScore * 0.28 + rsiScore * 0.19 + momScore * 0.15;

            // ранний триггер — теперь после rawScore (чтобы можно было смотреть знак rawScore)
            boolean earlyTrigger = earlyTrendTrigger(c1m);
            if (earlyTrigger) {
                if (rawScore > 0) mtfConfirm = 1;
                else if (rawScore < 0) mtfConfirm = -1;
            }

            // теперь вычисляем vol/atr/impulse правильно, опираясь на c1m и c5m
            boolean volOk = isVolumeOk(c1m);                    // использует quoteAssetVolume 1m
            boolean atrOk = isVolatilityOk(c5m, ATR_MIN_PCT);   // ATR на 5m
            boolean impulse = isImpulseCandle(c1m.get(c1m.size() - 1), c1m);

            // adaptive impulse (ты уже делаешь это дальше — здесь можно оставить, или убрать двойной расчёт)
            double avgBody = c1m.stream()
                    .skip(Math.max(0, c1m.size() - 20))
                    .mapToDouble(c -> Math.abs(c.close - c.open))
                    .average().orElse(IMPULSE_PCT);
            double adaptiveImpulse = Math.max(IMPULSE_PCT, avgBody * 1.5);
            impulse = Math.abs(c1m.get(c1m.size() - 1).close - c1m.get(c1m.size() - 1).open) / (c1m.get(c1m.size() - 1).open + 1e-12) >= adaptiveImpulse;

            boolean liquSweep = detectLiquiditySweep(c1m);
            int structure1h = marketStructure(c1h);
            int structure15m = marketStructure(c15m);
            int structure5m = marketStructure(c5m);
            int structure1m = marketStructure(c1m);
            boolean structureAligned = (Integer.signum((int)Math.signum(rawScore)) == Integer.signum(structure15m)) ||
                    (Integer.signum((int)Math.signum(rawScore)) == Integer.signum(structure5m));

            double vwap15 = vwap(c15m);
            double lastPrice = c1m.get(c1m.size() - 1).close;
            boolean vwapAligned = (rawScore > 0 && lastPrice > vwap15) || (rawScore < 0 && lastPrice < vwap15);

            boolean bos = detectBOS(c5m);

            double atrVal = atr(c5m, 14);
            List<Integer> highs = detectSwingHighs(c5m, 3);
            List<Integer> lows = detectSwingLows(c5m, 3);
            double lastSwingHigh = highs.isEmpty() ? Double.NaN : c5m.get(highs.get(highs.size() - 1)).high;
            double lastSwingLow = lows.isEmpty() ? Double.NaN : c5m.get(lows.get(lows.size() - 1)).low;
            boolean atrBreakLong = !Double.isNaN(lastSwingHigh) && lastPrice > lastSwingHigh + atrVal;
            boolean atrBreakShort = !Double.isNaN(lastSwingLow) && lastPrice < lastSwingLow - atrVal;

            double confidence = composeConfidence(rawScore, mtfConfirm, volOk, atrOk, !impulse, vwapAligned, structureAligned, bos, liquSweep);

            boolean strongTrigger = false;
            if ((impulse && volOk) || atrBreakLong || atrBreakShort) {
                strongTrigger = true;
                confidence = Math.min(1.0, confidence + 0.15);
            }

            String direction = null;
            double prevClose = c1m.get(c1m.size() - 2).close;
            boolean priceUp = lastPrice > prevClose;
            boolean priceDown = lastPrice < prevClose;

            if ((rawScore > 0 || atrBreakLong || (impulse && priceUp)) &&
                    confidence >= MIN_CONF &&
                    ( (mtfConfirm >= 0 && structureAligned && vwapAligned) || strongTrigger )) {
                direction = "LONG";
            } else if ((rawScore < 0 || atrBreakShort || (impulse && priceDown)) &&
                    confidence >= MIN_CONF &&
                    ( (mtfConfirm <= 0 && structureAligned && vwapAligned) || strongTrigger )) {
                direction = "SHORT";
            } else {
                return Optional.empty();
            }

            if (liquSweep && confidence < 0.92) confidence *= 0.85;

            if (!strongTrigger) {
                if (direction.equals("LONG") && dir1h < 0 && confidence < 0.85) return Optional.empty();
                if (direction.equals("SHORT") && dir1h > 0 && confidence < 0.85) return Optional.empty();
            }

            double rsiVal = rsi(closes1m, 14);
            Signal s = new Signal(pair.replace("USDT", ""), direction, confidence, lastPrice, rsiVal, rawScore, mtfConfirm, volOk, atrOk, strongTrigger, atrBreakLong, atrBreakShort, impulse, earlyTrigger);
            markSignalSent(pair);
            return Optional.of(s);
        } catch (Exception ex) {
            System.out.println("[evaluate] error for " + pair + " : " + ex.getMessage());
            return Optional.empty();
        }
    }


    // ========================= Signal class =========================
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
        public final boolean strongTrigger;
        public final boolean atrBreakLong;
        public final boolean atrBreakShort;
        public final boolean impulse;
        public final boolean earlyTrigger;
        public final Instant created = Instant.now();

        public Signal(String symbol, String direction, double confidence, double price, double rsi, double rawScore,
                      int mtfConfirm, boolean volOk, boolean atrOk, boolean strongTrigger, boolean atrBreakLong,
                      boolean atrBreakShort, boolean impulse, boolean earlyTrigger) {
            this.symbol = symbol;
            this.direction = direction;
            this.confidence = confidence;
            this.price = price;
            this.rsi = rsi;
            this.rawScore = rawScore;
            this.mtfConfirm = mtfConfirm;
            this.volOk = volOk;
            this.atrOk = atrOk;
            this.strongTrigger = strongTrigger;
            this.atrBreakLong = atrBreakLong;
            this.atrBreakShort = atrBreakShort;
            this.impulse = impulse;
            this.earlyTrigger = earlyTrigger;
        }

        public String toTelegramMessage() {
            String flags = (strongTrigger ? "⚡strong " : "") +
                    (earlyTrigger ? "⚡early " : "") +
                    (atrBreakLong ? "ATR↑ " : "") +
                    (atrBreakShort ? "ATR↓ " : "") +
                    (impulse ? "IMPULSE " : "");
            return String.format("*%s* → *%s*\nConfidence: *%.2f*\nPrice: %.8f\nRSI(14): %.2f\n_flags_: %s\n_raw: %.3f mtf:%d vol:%b atr:%b_\n_time: %s_",
                    symbol, direction, confidence, price, rsi, flags.trim(), rawScore, mtfConfirm, volOk, atrOk, created.toString());
        }
    }

    // ========================= Cooldown / Debounce =========================
    public boolean isCooldown(String pair) {
        long now = System.currentTimeMillis();
        long last = lastSignalTime.getOrDefault(pair, 0L);
        return (now - last) < (COOLDOWN_MS / 3); // в 3 раза чаще
    }

    public void markSignalSent(String pair) {
        lastSignalTime.put(pair, System.currentTimeMillis());
    }

    // ========================= Microtick + WS (helpers) =========================
    public void connectTickWebSocket(String pair) {
        // NOTE: this opens per-pair websockets in simplified manner.
        // In production consider multiplexed streams to avoid many connections.
        try {
            final String symbol = pair.toLowerCase();
            String aggUrl = String.format("wss://fstream.binance.com/ws/%s@aggTrade", symbol);

            java.net.http.WebSocket ws = java.net.http.HttpClient.newHttpClient()
                    .newWebSocketBuilder()
                    .buildAsync(URI.create(aggUrl), new java.net.http.WebSocket.Listener() {
                        @Override
                        public CompletionStage<?> onText(java.net.http.WebSocket webSocket, CharSequence data, boolean last) {
                            try {
                                JSONObject json = new JSONObject(data.toString());
                                double price = json.has("p") ? Double.parseDouble(json.getString("p")) : json.optDouble("p", 0.0);
                                double qty = json.has("q") ? Double.parseDouble(json.getString("q")) : json.optDouble("q", 0.0);
                                long ts = json.has("T") ? json.getLong("T") : System.currentTimeMillis();

                                tickPriceDeque.computeIfAbsent(pair, k -> new ArrayDeque<>()).addLast(price);
                                Deque<Double> dq = tickPriceDeque.get(pair);
                                while (dq.size() > TICK_HISTORY) dq.removeFirst();
                                lastTickPrice.put(pair, price);
                                lastTickTime.put(pair, ts);

                                microBuilders.computeIfAbsent(pair, k -> new MicroCandleBuilder(1000));
                                MicroCandleBuilder b1 = microBuilders.get(pair);
                                Optional<Candle> micro = b1.addTick(ts, price, qty);
                                micro.ifPresent(c -> evaluateMicroCandle(pair, c));

                                if (dq.size() >= 6) {
                                    MicroTrendResult tr = computeMicroTrend(pair, dq);
                                    OrderbookSnapshot obs = orderbookMap.getOrDefault(pair, new OrderbookSnapshot(0,0, ts));
                                    evaluateTick(pair, price, qty, ts, tr, obs);
                                }
                            } catch (Exception ex) {
                                System.out.println("[WS tick parse] " + ex.getMessage());
                            }
                            return java.net.http.WebSocket.Listener.super.onText(webSocket, data, last);
                        }
                    }).join();
            System.out.println("[WS-TICK] connected aggTrade for " + pair);
        } catch (Exception e) {
            System.out.println("[WS connect] error for " + pair + " : " + e.getMessage());
        }
    }

    private void evaluateMicroCandle(String pair, Candle c) {
        // lightweight micro analysis: detect big micro-body (> threshold) and push early signal attempts
        double bodyPct = c.bodyPct();
        if (bodyPct > IMPULSE_PCT * 1.2) {
            // push a small message or mark internal state (no Telegram here)
            System.out.println(String.format("[Micro] %s micro impulse bodyPct=%.4f", pair, bodyPct));
        }
    }

    private MicroTrendResult computeMicroTrend(String pair, Deque<Double> dq) {
        // compute simple derivative and accel over last N ticks (6)
        List<Double> arr = new ArrayList<>(dq);
        if (arr.size() < 3) return new MicroTrendResult(0,0, arr.isEmpty()?0:arr.get(arr.size()-1));
        int n = Math.min(arr.size(), 10);
        double sumDiff = 0;
        for (int i = arr.size() - n + 1; i < arr.size(); i++) sumDiff += arr.get(i) - arr.get(i-1);
        double speed = sumDiff / (n - 1 + 1e-12);
        // accel approximated by diff of last two diffs
        double lastDiff = arr.get(arr.size()-1) - arr.get(arr.size()-2);
        double prevDiff = arr.get(arr.size()-2) - arr.get(arr.size()-3);
        double accel = lastDiff - prevDiff;
        double avg = arr.stream().mapToDouble(Double::doubleValue).average().orElse(arr.get(arr.size()-1));
        return new MicroTrendResult(speed, accel, avg);
    }

    private void evaluateTick(String pair, double price, double qty, long ts, MicroTrendResult tr, OrderbookSnapshot obs) {
        double obi = obs.obi();
        double microSpeed = tr.speed;
        double microAccel = tr.accel;
        double conf = Math.min(1.0, Math.abs(microSpeed) * 0.4 + Math.abs(obi) * 0.35 + Math.abs(microAccel) * 0.15);

        boolean strongTickTrigger = Math.abs(obi) > OBI_THRESHOLD && Math.abs(microSpeed) > IMPULSE_PCT;
        // добавляем microtrend ускорение и предсказание импульса
        double predictedMove = microSpeed * 2 + microAccel * 1.5;
        if (predictedMove > IMPULSE_PCT && !isCooldown(pair)) {
            strongTickTrigger = true;
        }

        if (strongTickTrigger && conf > MIN_CONF && !isCooldown(pair)) {
            String type = (obi > 0) ? "TICK_LONG_OBI" : "TICK_SHORT_OBI";
            lastSentConfidence.put(pair, conf);
            lastSignalTime.put(pair, System.currentTimeMillis());

            // Улучшенные фьючерсные сигналы: включаем microtrend ускорение
            String msg = String.format("%s %s conf=%.2f price=%.8f microSpeed=%.5f microAccel=%.5f obi=%.5f",
                    pair, type, conf, price, microSpeed, microAccel, obi);
            bot.sendSignal(msg);
            System.out.println("[TickSignal] " + msg);
        }
    }

    public void updateOrderbook(String pair, double bidVol, double askVol) {
        orderbookMap.put(pair, new OrderbookSnapshot(bidVol, askVol, System.currentTimeMillis()));
    }



    // stop
    public void stop() {
        if (scheduler != null) scheduler.shutdownNow();
        System.out.println("[SignalSender] stopped");
    }

    // ========================= Helper: top symbols via CoinGecko =========================
    public List<String> getTopSymbols(int limit) {
        try {
            String url = String.format("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=%d&page=1", limit);
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build();
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

    // ========================= Misc helpers =========================
    private boolean earlyTrendTrigger(List<Candle> candles) {
        if (candles == null || candles.size() < 3) return false;
        int bull = 0, bear = 0;
        for (int i = candles.size() - 3; i < candles.size(); i++) {
            if (candles.get(i).isBull()) bull++;
            if (candles.get(i).isBear()) bear++;
        }
        return bull >= 2 || bear >= 2;
    }

    // ========================= Signal sending wrapper (if you prefer) =========================
    private void sendRaw(String msg) {
        try { bot.sendSignal(msg); } catch (Exception e) { System.out.println("[sendRaw] " + e.getMessage()); }
    }
    public List<Candle> fetchKlines(String symbol, String interval, int limit) {
        try {
            return fetchKlinesAsync(symbol, interval, limit).get(); // блокируем, ждем завершения
        } catch (Exception e) {
            System.out.println("[fetchKlines] error for " + symbol + " " + interval + ": " + e.getMessage());
            return Collections.emptyList();
        }
    }
    public void start() {
        System.out.println("[SignalSender] Scheduler started");

        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                ensureBinancePairsFresh(); // обновляем список пар
                for (String pair : BINANCE_PAIRS) {
                    try {
                        CompletableFuture<List<Candle>> f1m = fetchKlinesAsync(pair, "1m", 50);
                        CompletableFuture<List<Candle>> f5m = fetchKlinesAsync(pair, "5m", 50);
                        CompletableFuture<List<Candle>> f15m = fetchKlinesAsync(pair, "15m", 50);
                        CompletableFuture<List<Candle>> f1h = fetchKlinesAsync(pair, "1h", 50);

                        CompletableFuture.allOf(f1m, f5m, f15m, f1h).thenAccept(v -> {
                            try {
                                List<Candle> c1m = f1m.get();
                                List<Candle> c5m = f5m.get();
                                List<Candle> c15m = f15m.get();
                                List<Candle> c1h = f1h.get();

                                Optional<Signal> sigOpt = evaluate(pair, c1m, c5m, c15m, c1h);
                                sigOpt.ifPresent(sig -> bot.sendSignal(sig.toTelegramMessage()));
                            } catch (Exception e) {
                                System.out.println("[async evaluate] " + e.getMessage());
                            }
                        });
                    } catch (Exception ex) {
                        System.out.println("[start] Error evaluating " + pair + ": " + ex.getMessage());
                    }
                }
            } catch (Exception e) {
                System.out.println("[start] Scheduler error: " + e.getMessage());
            }
        }, 0, INTERVAL_MIN, TimeUnit.MINUTES);
    }
}
