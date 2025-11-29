package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class SignalSender {

    private final TelegramBotSender bot;
    private final HttpClient http;
    private final Set<String> STABLE = Set.of("USDT", "USDC", "BUSD"); // skip stable-like tokens

    // CONFIG (from ENV or defaults)
    private final int TOP_N;
    private final double MIN_CONF;            // base threshold (0..1)
    private final int INTERVAL_MIN;           // scheduler interval minutes
    private final int KLINES_LIMIT;           // number of candles fetched for each TF
    private final long REQUEST_DELAY_MS;      // delay between HTTP calls to avoid rate limits

    // MTF timeframes
    private final String TF_ENTRY = "1m";     // entry TF
    private final String TF_SHORT = "5m";
    private final String TF_MED = "15m";
    private final String TF_LONG = "1h";

    // thresholds (env)
    private final double IMPULSE_PCT;      // if one-minute candle body > this -> impulse (skip)
    private final double VOL_MULTIPLIER;   // volume multiplier vs avg to consider vol OK
    private final double ATR_MIN_PCT;      // min ATR relative to price
    private final long COOLDOWN_MS;        // cooldown per symbol
    private final long BINANCE_REFRESH_INTERVAL_MS;

    // VWAP session start (HH:mm) - intraday VWAP; default midnight UTC
    private final LocalTime VWAP_SESSION_START;

    private Set<String> BINANCE_PAIRS = new HashSet<>();
    private long lastBinancePairsRefresh = 0L;

    private final Map<String, Long> lastOpenTimeMap = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSignalTime = new ConcurrentHashMap<>();
    private final Map<String, Double> lastSentConfidence = new ConcurrentHashMap<>();

    // small utilities
    private final DateTimeFormatter dtf = DateTimeFormatter.ISO_INSTANT;

    public SignalSender(TelegramBotSender bot) {
        this.bot = bot;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        this.TOP_N = Integer.parseInt(System.getenv().getOrDefault("TOP_N", "100"));
        this.MIN_CONF = Double.parseDouble(System.getenv().getOrDefault("MIN_CONFIDENCE", "0.3")); // PRO default
        this.INTERVAL_MIN = Integer.parseInt(System.getenv().getOrDefault("INTERVAL_MINUTES", "10"));
        this.KLINES_LIMIT = Integer.parseInt(System.getenv().getOrDefault("KLINES", "120"));
        this.REQUEST_DELAY_MS = Long.parseLong(System.getenv().getOrDefault("REQUEST_DELAY_MS", "150"));

        this.IMPULSE_PCT = Double.parseDouble(System.getenv().getOrDefault("IMPULSE_PCT", "0.02")); // 2%  (more strict)
        this.VOL_MULTIPLIER = Double.parseDouble(System.getenv().getOrDefault("VOL_MULT", "0.9"));
        this.ATR_MIN_PCT = Double.parseDouble(System.getenv().getOrDefault("ATR_MIN_PCT", "0.0007")); // 0.07%
        this.COOLDOWN_MS = Long.parseLong(System.getenv().getOrDefault("COOLDOWN_MS", String.valueOf(60 * 1000))); // default 60s cooldown per pair
        long binanceRefreshMin = Long.parseLong(System.getenv().getOrDefault("BINANCE_REFRESH_MINUTES", "60"));
        this.BINANCE_REFRESH_INTERVAL_MS = binanceRefreshMin * 60 * 1000L;

        String sessionStart = System.getenv().getOrDefault("SESSION_START", "00:00"); // default UTC midnight
        this.VWAP_SESSION_START = LocalTime.parse(sessionStart);

        System.out.println("[SignalSender] INIT: TOP_N=" + TOP_N + " MIN_CONF=" + MIN_CONF + " INTERVAL_MIN=" + INTERVAL_MIN);
    }

    // -------------------- Binance / CoinGecko helpers --------------------
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

    // -------------------- Candle structure --------------------
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

        public double body() {
            return Math.abs(close - open);
        }

        public double bodyPct() {
            return body() / (open + 1e-12);
        }

        public boolean isBull() { return close > open; }
        public boolean isBear() { return close < open; }
    }

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

    // -------------------- Indicators --------------------
    public static double sma(List<Double> prices, int period) {
        if (prices == null || prices.size() < period) return prices.get(prices.size()-1);
        double sum = 0;
        for (int i = prices.size()-period; i < prices.size(); i++) sum += prices.get(i);
        return sum / period;
    }

    public static double ema(List<Double> prices, int period) {
        if (prices == null || prices.isEmpty()) return 0.0;
        double k = 2.0 / (period + 1);
        double ema = prices.get(0);
        for (double p : prices) ema = p * k + ema * (1 - k);
        return ema;
    }

    // RSI - Wilder's basic implementation with period lookback
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

    // momentum: last price change over n candles (relative)
    public static double momentumPct(List<Double> prices, int n) {
        if (prices == null || prices.size() <= n) return 0.0;
        double last = prices.get(prices.size() - 1);
        double prev = prices.get(prices.size() - 1 - n);
        return (last - prev) / (prev + 1e-12);
    }

    // OBV (On Balance Volume)
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

    // VWAP (intraday). Computes VWAP for list of candles but aligned to session start:
    // For simplicity we calculate VWAP across provided list assuming they cover a session.
    public static double vwap(List<Candle> candles) {
        if (candles == null || candles.isEmpty()) return 0.0;
        double pv = 0.0;
        double vol = 0.0;
        for (Candle c : candles) {
            double tp = (c.high + c.low + c.close) / 3.0;
            pv += tp * c.volume;
            vol += c.volume;
        }
        if (vol == 0) return candles.get(candles.size() - 1).close;
        return pv / vol;
    }

    // -------------------- Price Action helpers --------------------
    // detect simple swing highs/lows: returns list of pairs (index, price) for recent swings
    // We'll implement small window swing detection (n left, n right)
    public static List<Integer> detectSwingHighs(List<Candle> candles, int leftRight) {
        List<Integer> res = new ArrayList<>();
        for (int i = leftRight; i < candles.size() - leftRight; i++) {
            double v = candles.get(i).high;
            boolean isHigh = true;
            for (int l = i - leftRight; l <= i + leftRight; l++) {
                if (candles.get(l).high > v) {
                    isHigh = false;
                    break;
                }
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
                if (candles.get(l).low < v) {
                    isLow = false;
                    break;
                }
            }
            if (isLow) res.add(i);
        }
        return res;
    }

    // Determine market structure on given timeframe using recent swings.
    // Returns: 1 = bullish structure (HH/HL), -1 = bearish (LL/LH), 0 = unclear
    public static int marketStructure(List<Candle> candles) {
        if (candles == null || candles.size() < 20) return 0;
        // Very simple approach: compare last two swing highs/lows
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

    // Break Of Structure detection: did price break previous swing high/low significantly?
    public static boolean detectBOS(List<Candle> candles) {
        if (candles == null || candles.size() < 10) return false;
        List<Integer> highs = detectSwingHighs(candles, 3);
        List<Integer> lows = detectSwingLows(candles, 3);
        if (highs.size() < 2 && lows.size() < 2) return false;

        Candle last = candles.get(candles.size()-1);
        // if price breaks above last swing high by a small margin -> BOS up
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

    // Simple liquidity sweep detection: check for wick beyond recent swing high/low followed by reversal
    public static boolean detectLiquiditySweep(List<Candle> candles) {
        if (candles == null || candles.size() < 6) return false;
        int n = candles.size();
        Candle last = candles.get(n-1);
        Candle prev = candles.get(n-2);
        // look for long upper wick then close back lower (bull trap) or opposite
        double upperWick = last.high - Math.max(last.open, last.close);
        double lowerWick = Math.min(last.open, last.close) - last.low;
        double body = Math.abs(last.close - last.open);
        if (upperWick > body * 1.8 && last.close < prev.close) return true; // possible short-sweep (liquid long)
        if (lowerWick > body * 1.8 && last.close > prev.close) return true; // possible long-sweep (liquid short)
        return false;
    }

    // EMA direction helper with hysteresis threshold
    private int emaDirection(List<Candle> candles, int shortP, int longP, double hysteresis) {
        if (candles == null || candles.size() < longP + 2) return 0;
        List<Double> closes = candles.stream().map(c -> c.close).collect(Collectors.toList());
        double s = ema(closes, shortP);
        double l = ema(closes, longP);
        if (s > l * (1 + hysteresis)) return 1;
        if (s < l * (1 - hysteresis)) return -1;
        return 0;
    }

    // -------------------- Confidence composition (improved) --------------------
    // rawScore in -1..1 space built from robust primitives (ema trend, rsi extremes, macd-like, momentum)
    private double composeConfidence(double rawScore, int mtfConfirm, boolean volOk, boolean atrOk,
                                     boolean notImpulse, boolean vwapAligned, boolean structureAligned,
                                     boolean bos, boolean liquiditySweep) {
        // base absolute strength
        double base = Math.min(1.0, Math.abs(rawScore));

        // require structure alignment to give meaningful boost
        double boost = 0.0;
        if (structureAligned) boost += 0.12;             // structure alignment matters most
        if (mtfConfirm != 0 && Integer.signum(mtfConfirm) == Integer.signum((int)Math.signum(rawScore))) boost += 0.10;
        if (volOk) boost += 0.05;
        if (atrOk) boost += 0.03;
        if (vwapAligned) boost += 0.04;
        if (bos) boost += 0.03;
        if (!notImpulse) {
            // if impulse present, *penalize* confidence
            boost -= 0.08;
        }
        if (liquiditySweep) {
            // liquidity sweeps decrease reliability unless structure and mtf both confirm
            if (!structureAligned || mtfConfirm == 0) boost -= 0.12;
            else boost -= 0.03;
        }
        double conf = Math.min(1.0, Math.max(0.0, base + boost));
        return conf;
    }

    // -------------------- Strategy: evaluate --------------------
    public Optional<Signal> evaluate(String pair,
                                     List<Candle> c1m, List<Candle> c5m, List<Candle> c15m, List<Candle> c1h) {
        // Require enough candles
        if (c1m == null || c1m.size() < 30) return Optional.empty();

        // cooldown
        if (isCooldown(pair)) return Optional.empty();

        // avoid duplicate processing for same candle
        long newOpen = c1m.get(c1m.size() - 1).openTime;
        long lastOpen = lastOpenTimeMap.getOrDefault(pair, 0L);
        if (newOpen <= lastOpen) return Optional.empty();
        lastOpenTimeMap.put(pair, newOpen);

        // compute primitive arrays
        List<Double> closes1m = c1m.stream().map(c -> c.close).collect(Collectors.toList());
        List<Double> closes5m = c5m.stream().map(c -> c.close).collect(Collectors.toList());
        List<Double> closes15m = c15m.stream().map(c -> c.close).collect(Collectors.toList());
        List<Double> closes1h = c1h.stream().map(c -> c.close).collect(Collectors.toList());

        // primitives
        double emaScore = strategyEMANorm(closes1m);        // -1..1
        double rsiScore = strategyRSINorm(closes1m);        // -1..1
        double macdScore = strategyMACDNorm(closes1m);      // -1..1
        double momScore = strategyMomentumNorm(closes1m);   // -1..1

        // combine to raw score with tuned weights (EMA and MACD stronger)
        double rawScore = emaScore * 0.38 + macdScore * 0.28 + rsiScore * 0.19 + momScore * 0.15;
        // sign is direction

        // filters
        boolean volOk = isVolumeOk(c1m);                    // on entry TF
        boolean atrOk = isVolatilityOk(c5m, ATR_MIN_PCT);
        boolean impulse = isImpulseCandle(c1m.get(c1m.size() - 1));
        boolean liquSweep = detectLiquiditySweep(c1m);

        // multi-TF
        int dir1h = emaDirection(c1h, 20, 50, 0.002); // trend on 1h
        int dir15m = emaDirection(c15m, 9, 21, 0.002);
        int dir5m = emaDirection(c5m, 9, 21, 0.002);
        int dir1m = emaDirection(c1m, 9, 21, 0.001);

        int mtfConfirm = multiTFConfirm(dir1h, dir15m, dir5m, dir1m);

        // price action
        int structure1h = marketStructure(c1h);
        int structure15m = marketStructure(c15m);
        int structure5m = marketStructure(c5m);
        int structure1m = marketStructure(c1m);
        boolean structureAligned = (Integer.signum((int)Math.signum(rawScore)) == Integer.signum(structure15m)) ||
                (Integer.signum((int)Math.signum(rawScore)) == Integer.signum(structure5m));

        // VWAP alignment: compute vwap on 1h-ish candles? We'll use 15m VWAP window (or full session if available)
        double vwap15 = vwap(c15m);
        double lastPrice = c1m.get(c1m.size() - 1).close;
        boolean vwapAligned = (rawScore > 0 && lastPrice > vwap15) || (rawScore < 0 && lastPrice < vwap15);

        boolean bos = detectBOS(c5m); // treat BOS on 5m as important

        // compose confidence
        double confidence = composeConfidence(rawScore, mtfConfirm, volOk, atrOk, !impulse, vwapAligned, structureAligned, bos, liquSweep);

        // decide direction and strict checks
        String direction;
        if (rawScore > 0 && confidence >= MIN_CONF && mtfConfirm >= 0 && structureAligned && vwapAligned) direction = "LONG";
        else if (rawScore < 0 && confidence >= MIN_CONF && mtfConfirm <= 0 && structureAligned && vwapAligned) direction = "SHORT";
        else return Optional.empty();

        // extra safety: if impulse present or liquidity sweep and confidence not extremely high -> skip
        if (impulse && confidence < 0.95) {
            System.out.println(String.format("[Filter] %s rejected due to impulse (bodyPct=%.4f) conf=%.2f", pair,
                    c1m.get(c1m.size()-1).bodyPct(), confidence));
            return Optional.empty();
        }
        if (liquSweep && confidence < 0.92) {
            System.out.println(String.format("[Filter] %s rejected due liquidity sweep conf=%.2f", pair, confidence));
            return Optional.empty();
        }

        // ensure trend alignment with 1h: skip counter-trend
        if (direction.equals("LONG") && dir1h < 0) return Optional.empty();
        if (direction.equals("SHORT") && dir1h > 0) return Optional.empty();

        double rsiVal = rsi(closes1m, 14);
        Signal s = new Signal(pair.replace("USDT", ""), direction, confidence, lastPrice, rsiVal, rawScore, mtfConfirm, volOk, atrOk);
        markSignalSent(pair);
        return Optional.of(s);
    }

    // Improved strategy helpers for norms
    private double strategyEMANorm(List<Double> closes) {
        if (closes == null || closes.size() < 50) return 0.0;
        double e20 = ema(closes, 20);
        double e50 = ema(closes, 50);
        double e100 = ema(closes, 100);
        // measure e20 - e50 and e50 - e100
        double a = (e20 - e50) / (e50 + 1e-12);
        double b = (e50 - e100) / (e100 + 1e-12);
        double combined = (a + b) / 2.0;
        // normalize to -1..1 using a reasonable scale (0.02 ~ strong)
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
        return Math.max(-1.0, Math.min(1.0, rel / 0.008)); // scaled more aggressively
    }

    private double strategyMomentumNorm(List<Double> closes) {
        double raw = momentumPct(closes, 3);
        return Math.max(-1.0, Math.min(1.0, raw / 0.01));
    }

    // volume helpers
    private double avgQuoteVolume(List<Candle> candles, int lookback) {
        if (candles == null || candles.isEmpty()) return 0.0;
        int lb = Math.min(lookback, candles.size());
        double s = 0;
        for (int i = candles.size() - lb; i < candles.size(); i++) s += candles.get(i).quoteAssetVolume;
        return s / lb;
    }

    public boolean isCooldown(String pair) {
        long now = System.currentTimeMillis();
        long last = lastSignalTime.getOrDefault(pair, 0L);
        return (now - last) < COOLDOWN_MS;
    }
    public void markSignalSent(String pair) {
        lastSignalTime.put(pair, System.currentTimeMillis());
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
        double lastPrice = candles.get(candles.size()-1).close;
        double atrPct = atrVal / (lastPrice + 1e-12);
        return atrPct >= minAtrPct;
    }

    public boolean isImpulseCandle(Candle c) {
        if (c == null) return false;
        double change = Math.abs(c.close - c.open) / (c.open + 1e-12);
        return change >= IMPULSE_PCT;
    }

    // multiTF confirm: simple voting with bias to higher TF (1h>15m>5m>1m)
    private int multiTFConfirm(int dir1h, int dir15m, int dir5m, int dir1m) {
        int score = 0;
        score += dir1h * 3;
        score += dir15m * 2;
        score += dir5m * 1;
        score += dir1m * 0; // 1m is for trigger only
        if (score > 2) return 1;
        if (score < -2) return -1;
        return 0;
    }

    // emaDirection wrapper for external use
    private int emaDirection(List<Candle> candles, int shortP, int longP) {
        return emaDirection(candles, shortP, longP, 0.001);
    }

    // -------------------- Signal data class --------------------
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

    // -------------------- Start Scheduler (main loop) --------------------
    public void start() {
        System.out.println("[SignalSender] Starting PRO mode TOP_N=" + TOP_N + " MIN_CONF=" + MIN_CONF + " INTERVAL_MIN=" + INTERVAL_MIN);
        ensureBinancePairsFresh();
        try {
            bot.sendSignal("✅ SignalSender (PRO mode) запущен и работает! MIN_CONF=" + MIN_CONF);
        } catch (Exception e) {
            System.out.println("[Telegram Test Message Error] " + e.getMessage());
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                ensureBinancePairsFresh();
                List<String> filtered = getTopSymbols(TOP_N).stream().filter(BINANCE_PAIRS::contains).toList();
                // iterate
                for (String pair : filtered) {
                    // fetch MTF klines
                    List<Candle> c1m = fetchKlines(pair, TF_ENTRY, Math.max(KLINES_LIMIT, 120));
                    if (c1m.isEmpty()) continue;

                    List<Candle> c5m = fetchKlines(pair, TF_SHORT, Math.max(80, KLINES_LIMIT / 3));
                    if (c5m.isEmpty()) continue;

                    List<Candle> c15m = fetchKlines(pair, TF_MED, Math.max(80, KLINES_LIMIT / 6));
                    if (c15m.isEmpty()) continue;

                    List<Candle> c1h = fetchKlines(pair, TF_LONG, Math.max(80, KLINES_LIMIT / 12));
                    if (c1h.isEmpty()) continue;

                    Optional<Signal> opt = evaluate(pair, c1m, c5m, c15m, c1h);
                    opt.ifPresent(s -> {
                        try {
                            // avoid spamming same symbol with slightly different confidence too often
                            double prevConf = lastSentConfidence.getOrDefault(pair, 0.0);
                            if (Math.abs(prevConf - s.confidence) < 0.02 && (System.currentTimeMillis() - lastSignalTime.getOrDefault(pair, 0L)) < 3 * 60 * 1000) {
                                // skip too-similar small updates within 3 minutes
                                System.out.println("[Debounce] skipping similar signal for " + pair + " conf=" + s.confidence);
                                return;
                            }
                            bot.sendSignal(s.toTelegramMessage());
                            lastSentConfidence.put(pair, s.confidence);
                        } catch (Exception e) {
                            System.out.println("[Telegram] send error: " + e.getMessage());
                        }
                    });
                }
            } catch (Exception e) {
                System.out.println("[Job error] " + e.getMessage());
            }
        }, 0, INTERVAL_MIN, TimeUnit.MINUTES);
    }
}
