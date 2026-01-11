package com.bot;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SignalSender {
    // ===== Anti-spam / cooldown =====
    private final Map<String, Long> lastSignalTime = new ConcurrentHashMap<>();
    private final Map<String, Integer> ideaDirection = new ConcurrentHashMap<>();
    private final Map<String, Double> ideaInvalidation = new ConcurrentHashMap<>();
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
    private final Map<String, Double> lastSentConfidence = new ConcurrentHashMap<>(); // last confidence
    private final Map<String, Double> lastPriceMap = new ConcurrentHashMap<>();

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
        this.MIN_CONF = 0.35;
        this.INTERVAL_MIN = envInt("INTERVAL_MINUTES", 5);
        this.KLINES_LIMIT = envInt("KLINES", 240);
        this.REQUEST_DELAY_MS = envLong("REQUEST_DELAY_MS", 120);

        this.IMPULSE_PCT = envDouble("IMPULSE_PCT", 0.02);
        this.VOL_MULTIPLIER = envDouble("VOL_MULT", 0.9);
        this.ATR_MIN_PCT = envDouble("ATR_MIN_PCT", 0.0007);
        this.COOLDOWN_MS = envLong("COOLDOWN_MS", 300000);
        long brMin = envLong("BINANCE_REFRESH_MINUTES", 60);
        this.BINANCE_REFRESH_INTERVAL_MS = brMin * 60 * 1000L;

        this.VWAP_SESSION_START = LocalTime.parse(System.getenv().getOrDefault("SESSION_START", "00:00"));

        this.TICK_HISTORY = envInt("TICK_HISTORY", 100);
        this.OBI_THRESHOLD = envDouble("OBI_THRESHOLD", 0.28);
        this.VOLUME_SPIKE_MULT = envDouble("VOL_SPIKE_MULT", 1.4);

        this.STABLE = Set.of("USDT", "USDC", "BUSD");

        System.out.println("[SignalSender] INIT: TOP_N=" + TOP_N + " MIN_CONF=" + MIN_CONF + " INTERVAL_MIN=" + INTERVAL_MIN);
    }

    // ========================= Helpers for env parsing =========================
    private int envInt(String k, int def) {
        try {
            return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(def)));
        } catch (Exception e) {
            return def;
        }
    }
    private String getLocalTimeString() {
        LocalDateTime now = LocalDateTime.now(); // локальное системное время
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss");
        return now.format(fmt);
    }

    private long envLong(String k, long def) {
        try {
            return Long.parseLong(System.getenv().getOrDefault(k, String.valueOf(def)));
        } catch (Exception e) {
            return def;
        }
    }

    private double envDouble(String k, double def) {
        try {
            return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(def)));
        } catch (Exception e) {
            return def;
        }
    }

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

        public double body() {
            return Math.abs(close - open);
        }

        public double bodyPct() {
            return body() / (open + 1e-12);
        }

        public boolean isBull() {
            return close > open;
        }

        public boolean isBear() {
            return close < open;
        }
    }

    // ========================= Micro candle builder (ticks -> tiny candles) =========================
    public static class MicroCandleBuilder {
        private final int intervalMs;
        private long currentBucketStart = -1;
        private double open = Double.NaN, high = Double.NEGATIVE_INFINITY, low = Double.POSITIVE_INFINITY, close = Double.NaN;
        private double volume = 0.0;
        private long closeTime = -1;

        public MicroCandleBuilder(int intervalMs) {
            this.intervalMs = intervalMs;
        }

        public Optional<Candle> addTick(long tsMillis, double price, double qty) {
            long bucket = (tsMillis / intervalMs) * intervalMs;
            if (currentBucketStart == -1) {
                currentBucketStart = bucket;
                open = price;
                high = price;
                low = price;
                close = price;
                volume = qty;
                closeTime = bucket + intervalMs - 1;
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
                open = price;
                high = price;
                low = price;
                close = price;
                volume = qty;
                closeTime = bucket + intervalMs - 1;
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
            this.speed = speed;
            this.accel = accel;
            this.avgTick = avgTick;
        }
    }

    public static double sma(List<Double> prices, int period) {
        if (prices == null || prices.size() < period) return prices.get(prices.size() - 1);
        double sum = 0;
        for (int i = prices.size() - period; i < prices.size(); i++) sum += prices.get(i);
        return sum / period;
    }
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
            if (diff > 0) gain += diff;
            else loss -= diff;
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

    // Trend prediction based on linear regression of last N candles
    public TrendPrediction predictTrend(List<Candle> candles) {
        if (candles == null || candles.size() < 30) {
            return new TrendPrediction("NONE", 0.0);
        }

        int N = Math.min(60, candles.size());
        List<Double> closes = new ArrayList<>();
        for (int i = candles.size() - N; i < candles.size(); i++) {
            closes.add(candles.get(i).close);
        }

        // Linear regression y = a*x + b
        int size = closes.size();
        double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;

        for (int i = 0; i < size; i++) {
            sumX += i;
            sumY += closes.get(i);
            sumXY += i * closes.get(i);
            sumXX += i * i;
        }

        double slope = (size * sumXY - sumX * sumY) / (size * sumXX - sumX * sumX + 1e-12);

        double avgClose = closes.stream().mapToDouble(Double::doubleValue).average().orElse(closes.get(size - 1));
        double slopePct = slope / (avgClose + 1e-12);

        double conf = Math.min(1.0, Math.abs(slopePct) * 12);

        if (conf < 0.1)
            return new TrendPrediction("NONE", conf);

        String dir = slopePct > 0 ? "LONG" : "SHORT";

        return new TrendPrediction(dir, conf);
    }

    // ========================= Price Action helpers =========================
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

    private double strategyEMANorm(List<Double> closes) {
        if (closes == null || closes.size() < 100) return 0.0;
        double e20 = ema(closes, 20);
        double e50 = ema(closes, 50);
        double e100 = ema(closes, 100);
        double a = (e20 - e50) / (e50 + 1e-12);
        double b = (e50 - e100) / (e100 + 1e-12);
        double combined = (a + b) / 2.0;
        return Math.max(-1.0, Math.min(1.0, combined)); // делим уже внутри a и b, нормализируем до [-1,1]
    }

    private double strategyRSINorm(List<Double> closes) {
        double r = rsi(closes, 14);
        // 50 -> нейтрально, >50 -> LONG, <50 -> SHORT
        double score = (r - 50) / 50.0;  // теперь результат в [-1,1]
        return Math.max(-1.0, Math.min(1.0, score));
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

    private int multiTFConfirm(int dir1h, int dir15m, int dir5m) {
        int score = 0;
        score += dir1h * 3;
        score += dir15m * 2;
        score += dir5m * 1;
        if (score > 2) return 1;
        if (score < -2) return -1;
        return 0;
    }

    private double composeConfidence(double rawScore, int mtfConfirm, boolean volOk, boolean atrOk,
                                     boolean notImpulse, boolean vwapAligned, boolean structureAligned,
                                     boolean bos, boolean liquiditySweep) {
        double base = Math.min(1.0, Math.abs(rawScore));
        double boost = 0.0;
        if (structureAligned) boost += 0.15;
        if (mtfConfirm != 0 && Integer.signum(mtfConfirm) == Integer.signum((int) Math.signum(rawScore))) boost += 0.12;
        if (volOk) boost += 0.07;
        if (atrOk) boost += 0.05;
        if (vwapAligned) boost += 0.06;
        if (bos) boost += 0.04;
        if (liquiditySweep) {
            if (!structureAligned || mtfConfirm == 0) boost -= 0.10;
            else boost -= 0.03;
        }
        double conf = Math.min(1.0, Math.max(0.0, base + boost));
        return conf;
    }

    public Optional<Signal> evaluate(String pair,
                                     List<Candle> c5m,
                                     List<Candle> c15m,
                                     List<Candle> c1h) {

        System.out.println("[EVAL START] " + pair);
        final String p = pair;

        try {
            if (c5m.size() < 10) return Optional.empty();
            if (c15m.size() < 10) return Optional.empty();
            if (c1h.size() < 10) return Optional.empty();

            int dir1h = emaDirection(c1h, 20, 50, 0.001);
            int dir15m = emaDirection(c15m, 9, 21, 0.001);
            int dir5m = emaDirection(c5m, 9, 21, 0.001);
            int mtfConfirm = multiTFConfirm(dir1h, dir15m, dir5m);

            lastOpenTimeMap.put(p, c5m.get(c5m.size() - 1).openTime);

            // ===== 3. RAW SCORE + MICRO TREND =====
            List<Double> closes5m = c5m.stream().map(c -> c.close).toList();
            double rawScore = strategyEMANorm(closes5m) * 0.38 +
                    strategyMACDNorm(closes5m) * 0.28 +
                    strategyRSINorm(closes5m) * 0.19 +
                    strategyMomentumNorm(closes5m) * 0.15;

            MicroTrendResult mt = computeMicroTrend(p, tickPriceDeque.getOrDefault(p, new ArrayDeque<>()));
            if (Math.signum(rawScore) == Math.signum(mt.speed)) {
                rawScore += 0.05 * Math.signum(rawScore); // бонус microtrend
            }

            boolean earlyTrigger = earlyTrendTrigger(c5m);
            if (earlyTrigger && Math.abs(rawScore) > 0.25) {
                mtfConfirm = Integer.signum((int) Math.signum(rawScore));
            }

            // ===== 4. СВЕЧА =====
            Candle lastCandle = c5m.get(c5m.size() - 1);
            double lastPrice = lastCandle.close;
            double atrVal = atr(c5m, 14);
            double atrPct = atrVal / (lastPrice + 1e-12);

            double candleBody = Math.abs(lastCandle.close - lastCandle.open);
            double candleRange = lastCandle.high - lastCandle.low;
            if (candleRange <= 0) return Optional.empty();
            boolean weakCandle = candleBody / candleRange < 0.15;

            // ===== 5. IMPULSE =====
            double avgBody = c5m.subList(Math.max(0, c5m.size() - 20), c5m.size())
                    .stream().mapToDouble(c -> Math.abs(c.close - c.open)).average().orElse(0);
            double adaptiveImpulse = Math.max(IMPULSE_PCT, avgBody * 1.5);
            boolean impulse = candleBody / (lastCandle.open + 1e-12) >= adaptiveImpulse;

            boolean structureAligned = marketStructure(c1h) != 0 ||
                    marketStructure(c15m) != 0 ||
                    marketStructure(c5m) != 0;

            double vwap15 = vwap(c15m);
            boolean vwapAligned = (rawScore > 0 && lastPrice > vwap15) ||
                    (rawScore < 0 && lastPrice < vwap15);

            // ===== 7. CONFIDENCE =====
            double confidence = composeConfidence(
                    rawScore,
                    mtfConfirm,
                    !weakCandle,
                    atrPct >= ATR_MIN_PCT,
                    !impulse,
                    vwapAligned,
                    structureAligned,
                    detectBOS(c5m),
                    detectLiquiditySweep(c5m)
            );

            // Бонусы
            if (mtfConfirm != 0 && Integer.signum(mtfConfirm) == Integer.signum((int) Math.signum(rawScore)))
                confidence += 0.08;
            if (rawScore * mt.speed > 0) confidence += 0.12;
            if (structureAligned) confidence += 0.10;
            if (vwapAligned) confidence += 0.05;
            if (impulse) confidence += 0.12;
            if (earlyTrigger && Math.abs(rawScore) > 0.2) confidence += 0.05;
            if (isVolumeStrong(p, lastPrice)) confidence += 0.10;
            if (earlyTrigger) confidence += 0.05;

            confidence = Math.max(0.0, Math.min(1.0, confidence));

            // ===== 8. ATR BREAK =====
            boolean atrBreakLong = lastPrice > lastSwingHigh(c5m) + atrVal * 0.4;
            boolean atrBreakShort = lastPrice < lastSwingLow(c5m) - atrVal * 0.4;
            if ((atrBreakLong && rawScore < 0) || (atrBreakShort && rawScore > 0)) confidence -= 0.05;

            boolean strongTrigger = impulse || atrBreakLong || atrBreakShort ||
                    (isVolumeStrong(p, lastPrice) && Math.abs(mt.speed) > adaptiveImpulse * 0.5);

            // ===== 9. ОПРЕДЕЛЕНИЕ НАПРАВЛЕНИЯ =====
            boolean canGoLong = rawScore > 0 && confidence >= MIN_CONF;
            boolean canGoShort = rawScore < 0 && confidence >= MIN_CONF;

            String direction;
            if (rawScore > 0) direction = "LONG";
            else if (rawScore < 0) direction = "SHORT";
            else direction = "NONE";

            if ("NONE".equals(direction)) return Optional.empty();
            Signal s = new Signal(
                    p.replace("USDT", ""),
                    direction,
                    confidence,
                    lastPrice,
                    rsi(closes5m, 14),
                    rawScore,
                    mtfConfirm,
                    !weakCandle,
                    true,
                    strongTrigger,
                    atrBreakLong,
                    atrBreakShort,
                    impulse,
                    earlyTrigger,
                    rsi(closes5m, 7),
                    rsi(closes5m, 4)
            );

            // ===== 11. СОХРАНЕНИЕ НАПРАВЛЕНИЯ И ИНВАЛИДАЦИЯ =====
            int dirVal = direction.equals("LONG") ? 1 : -1;
            ideaDirection.put(p, dirVal);
            double invalidation = dirVal == 1
                    ? lastSwingLow(c5m) - atrVal * 0.3
                    : lastSwingHigh(c5m) + atrVal * 0.3;
            ideaInvalidation.put(p, invalidation);

            return Optional.of(s);

        } catch (Exception e) {
            System.out.println("[evaluate] " + p + " error: " + e.getMessage());
            return Optional.empty();
        }
    }
    private double lastSwingHigh(List<Candle> candles) {
        int lookback = Math.min(20, candles.size());
        double high = Double.NEGATIVE_INFINITY;
        for (int i = candles.size() - lookback; i < candles.size(); i++)
            high = Math.max(high, candles.get(i).high);
        return high;
    }
    private double lastSwingLow(List<Candle> candles) {
        int lookback = Math.min(20, candles.size());
        double low = Double.POSITIVE_INFINITY;
        for (int i = candles.size() - lookback; i < candles.size(); i++)
            low = Math.min(low, candles.get(i).low);
        return low;
    }
    private TradeSignal buildTrade(
            String symbol,
            String side,
            double price,
            double atr,
            double confidence,
            String reason
    ) {
        TradeSignal s = new TradeSignal();
        s.symbol = symbol;
        s.side = side;
        s.entry = price;

        double risk = atr * 1.2; // базовый риск

        if (side.equals("LONG")) {
            s.stop = price - risk;
            s.take = price + risk * 2.5;
        } else {
            s.stop = price + risk;
            s.take = price - risk * 2.5;
        }

        s.confidence = confidence;
        s.reason = reason;
        return s;
    }
    // ===== Real confidence =====
    private double calcConfidence(
            double emaFast,
            double emaSlow,
            double rsi,
            double atrPct
    ) {
        double score = 0.0;

        double trend = Math.abs(emaFast - emaSlow) / emaSlow;
        score += Math.min(trend * 80, 30);

        if (rsi > 45 && rsi < 65) score += 20;
        if (atrPct > 0.002 && atrPct < 0.01) score += 20;

        return Math.min(score / 70.0, 1.0);
    }

    public static class Signal {
        public final String symbol;
        public final String direction;
        public final double confidence;
        public final double price;
        public final double rsi;
        public final double rsi7;
        public final double rsi4;
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

        public Signal(String symbol, String direction, double confidence, double price, double rsi,
                      double rawScore, int mtfConfirm, boolean volOk, boolean atrOk, boolean strongTrigger,
                      boolean atrBreakLong, boolean atrBreakShort, boolean impulse, boolean earlyTrigger,
                      double rsi7, double rsi4) {
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
            this.rsi7 = rsi7;
            this.rsi4 = rsi4;
        }

        public String toTelegramMessage() {
            String flags = (strongTrigger ? "⚡strong " : "") +
                    (earlyTrigger ? "⚡early " : "") +
                    (atrBreakLong ? "ATR↑ " : "") +
                    (atrBreakShort ? "ATR↓ " : "") +
                    (impulse ? "IMPULSE " : "");

            // вместо created.toString() используем локальное время
            String localTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

            return String.format("*%s* → *%s*\nConfidence: *%.2f*\nPrice: %.8f\nRSI(14): %.2f\n_flags_: %s\n_raw: %.3f mtf:%d vol:%b atr:%b_\n_time: %s_",
                    symbol, direction, confidence, price, rsi, flags.trim(), rawScore, mtfConfirm, volOk, atrOk, localTime);
        }
    }

    public static class TrendPrediction {
        public final String direction;
        public final double confidence;

        public TrendPrediction(String direction, double confidence) {
            this.direction = direction;
            this.confidence = confidence;
        }
    }

    private final Map<String, Map<String, Long>> lastSignalTimeDir = new ConcurrentHashMap<>();
    private boolean isCooldown(String pair, String direction) {
        long now = System.currentTimeMillis();
        lastSignalTimeDir.putIfAbsent(pair, new ConcurrentHashMap<>());
        long last = lastSignalTimeDir.get(pair).getOrDefault(direction, 0L);
        return (now - last) < COOLDOWN_MS;
    }
    public void markSignalSent(String pair, String direction, double confidence) {
        lastSignalTimeDir
                .computeIfAbsent(pair, k -> new ConcurrentHashMap<>())
                .put(direction, System.currentTimeMillis());
        lastSentConfidence.put(pair, confidence);
    }
    public void connectTickWebSocket(String pair) {
        try {
            final String symbol = pair.toLowerCase();
            String aggUrl = String.format("wss://fstream.binance.com/ws/%s@aggTrade", symbol);
            System.out.println("[WS] Connecting to " + aggUrl);

            java.net.http.WebSocket ws = java.net.http.HttpClient.newHttpClient()
                    .newWebSocketBuilder()
                    .buildAsync(URI.create(aggUrl), new java.net.http.WebSocket.Listener() {
                        public CompletionStage<?> onText(java.net.http.WebSocket webSocket, CharSequence data, boolean last) {
                            try {
                                JSONObject json = new JSONObject(data.toString());
                                double price = json.has("p") ? Double.parseDouble(json.getString("p")) : 0.0;
                                double qty = json.has("q") ? Double.parseDouble(json.getString("q")) : 0.0;
                                long ts = json.has("T") ? json.getLong("T") : System.currentTimeMillis();

                                // сохраняем тики
                                tickPriceDeque.computeIfAbsent(pair, k -> new ArrayDeque<>()).addLast(price);
                                Deque<Double> dq = tickPriceDeque.get(pair);
                                while (dq.size() > TICK_HISTORY) dq.removeFirst();

                                lastTickPrice.put(pair, price);
                                lastTickTime.put(pair, ts);

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

    private Optional<Candle> predictNextCandleFromPrices(List<Double> recentPrices) {
        if (recentPrices == null || recentPrices.size() < 3) return Optional.empty();

        int n = recentPrices.size();
        double lastPrice = recentPrices.get(n - 1);
        double prevPrice = recentPrices.get(n - 2);
        double prevPrevPrice = recentPrices.get(n - 3);

        double speed = lastPrice - prevPrice;
        double accel = (lastPrice - prevPrice) - (prevPrice - prevPrevPrice);
// --- SMA сглаживание скорости и ускорения ---
        int smoothN = Math.min(5, recentPrices.size() - 1);
        double sumSpeed = 0;
        double sumAccel = 0;
        for (int i = recentPrices.size() - smoothN; i < recentPrices.size() - 1; i++) {
            double sp = recentPrices.get(i + 1) - recentPrices.get(i);
            sumSpeed += sp;
            if (i > recentPrices.size() - smoothN) {
                double prevSp = recentPrices.get(i) - recentPrices.get(i - 1);
                sumAccel += sp - prevSp;
            }
        }
        speed = sumSpeed / smoothN;
        accel = sumAccel / Math.max(1, smoothN - 1);
        // Ограничиваем экстремальные скачки
        double maxChangePct = 0.05;
        speed = Math.max(-lastPrice * maxChangePct, Math.min(speed, lastPrice * maxChangePct));
        accel = Math.max(-lastPrice * maxChangePct, Math.min(accel, lastPrice * maxChangePct));

        double predictedClose = lastPrice + speed + accel;

        double predictedHigh = Math.max(lastPrice, predictedClose) * 1.001;
        double predictedLow = Math.min(lastPrice, predictedClose) * 0.999;

        long predictedTime = System.currentTimeMillis() + 60_000;

        return Optional.of(new Candle(predictedTime, lastPrice, predictedHigh, predictedLow, predictedClose, 0.0, 0.0, predictedTime));
    }
    private MicroTrendResult computeMicroTrend(String pair, Deque<Double> dq) {
        if (dq == null || dq.size() < 3) return new MicroTrendResult(0, 0, 0);
        List<Double> arr = new ArrayList<>(dq);
        int n = Math.min(arr.size(), 10);

        double alpha = 0.5;
        double speed = 0;
        for (int i = arr.size() - n + 1; i < arr.size(); i++) {
            double diff = arr.get(i) - arr.get(i - 1);
            speed = alpha * diff + (1 - alpha) * speed;
        }

        double accel = 0;
        if (arr.size() >= 3) {
            double lastDiff = arr.get(arr.size() - 1) - arr.get(arr.size() - 2);
            double prevDiff = arr.get(arr.size() - 2) - arr.get(arr.size() - 3);
            accel = alpha * (lastDiff - prevDiff) + (1 - alpha) * accel;
        }

        double avg = arr.stream().mapToDouble(Double::doubleValue).average().orElse(arr.get(arr.size() - 1));
        return new MicroTrendResult(speed, accel, avg);
    }
    private final Map<String, List<Signal>> signalHistory = new ConcurrentHashMap<>();
    private boolean isVolumeStrong(String pair, double lastPrice) {
        OrderbookSnapshot obs = orderbookMap.get(pair);
        if (obs == null) return false;
        double obi = obs.obi();
        Deque<Double> history = tickPriceDeque.getOrDefault(pair, new ArrayDeque<>());
        double avgObi = history.isEmpty() ? 0.02 : history.stream().mapToDouble(p -> p).average().orElse(0.02) * 0.5;
        return Math.abs(obi) > avgObi;
    }
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

    private void sendRaw(String msg) {
        try {
            bot.sendSignal(msg);
        } catch (Exception e) {
            System.out.println("[sendRaw] " + e.getMessage());
        }
    }
    private void sendSignalIfAllowed(String pair,
                                     String direction,
                                     double confidence,
                                     double price,
                                     double rawScore,
                                     int mtfConfirm,
                                     double rsi14,
                                     List<Double> closes5m) {

        Signal s = new Signal(
                pair.replace("USDT", ""),
                direction,
                confidence,
                price,
                rsi14,
                rawScore,
                mtfConfirm,
                true,  // volOk (можно изменить логику)
                true,  // atrOk
                false, // strongTrigger
                false, // atrBreakLong
                false, // atrBreakShort
                false, // impulse
                false, // earlyTrigger
                rsi(closes5m, 7),
                rsi(closes5m, 4)
        );

        long now = System.currentTimeMillis();
        lastSignalTimeDir.putIfAbsent(pair, new ConcurrentHashMap<>());
        Map<String, Long> dirMap = lastSignalTimeDir.get(pair);

        List<String> reasons = new ArrayList<>();
        Long lastTime = dirMap.get(s.direction);
        if (lastTime != null && (now - lastTime) < COOLDOWN_MS) reasons.add("COOLDOWN");
        if (s.confidence < MIN_CONF) reasons.add("CONF < MIN_CONF");

        if (!reasons.isEmpty()) {
            System.out.println("[SKIP] " + pair + " → " + s.direction + " reasons: " + String.join(", ", reasons));
            return;
        }

        signalHistory.computeIfAbsent(pair, k -> new ArrayList<>()).add(s);
        dirMap.put(s.direction, now);
        lastSentConfidence.put(pair, s.confidence);

        System.out.println("[SEND] " + pair + " → " + s.direction + " confidence=" + s.confidence);
        sendRaw(s.toTelegramMessage());
    }
    public List<Candle> fetchKlines(String symbol, String interval, int limit) {
        try {
            List<Candle> candles = fetchKlinesAsync(symbol, interval, limit).get();
            if (candles.isEmpty()) {
                System.out.println("[KLİNES] Пустой ответ для " + symbol + " интервал " + interval);
            } else {
                System.out.println("[KLİNES] Получено " + candles.size() + " свечей для " + symbol + " интервал " + interval);
            }
            return candles;
        } catch (Exception e) {
            System.out.println("[fetchKlines] error for " + symbol + " " + interval + ": " + e.getMessage());
            return Collections.emptyList();
        }
    }
    private List<Candle> fetchKlinesSync(String symbol, String interval, int limit) {
        try {
            return fetchKlinesAsync(symbol, interval, limit).join();
        } catch (Exception e) {
            System.out.println("[fetchKlinesSync] error for " + symbol + ": " + e.getMessage());
            return Collections.emptyList();
        }
    }
    public void start() {
        System.out.println("[SignalSender] Scheduler started");
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                for (String pair : BINANCE_PAIRS) {
                    try {
                        List<Candle> c5m = fetchKlinesSync(pair, "5m", KLINES_LIMIT);
                        List<Candle> c15m = fetchKlinesSync(pair, "15m", KLINES_LIMIT);
                        List<Candle> c1h = fetchKlinesSync(pair, "1h", KLINES_LIMIT);

                        evaluate(pair, c5m, c15m, c1h)
                                .ifPresent(sig -> {
                                    List<Double> closes5m = c5m.stream().map(c -> c.close).toList();
                                    double rsi14 = rsi(closes5m, 14);
                                    int mtfConfirm = sig.mtfConfirm;
                                    double rawScore = sig.rawScore;

                                    sendSignalIfAllowed(
                                            pair,
                                            sig.direction,
                                            sig.confidence,
                                            sig.price,
                                            rawScore,
                                            mtfConfirm,
                                            rsi14,
                                            closes5m
                                    );
                                });
                    } catch (Exception e) {
                        System.out.println("[Scheduler] Error for " + pair + " : " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                System.out.println("[Scheduler] General error: " + e.getMessage());
            }
        }, 0, INTERVAL_MIN, TimeUnit.MINUTES);
    }
    static class TradeSignal {
        String symbol;
        String side;      // LONG / SHORT
        double entry;     // вход
        double stop;      // стоп
        double take;      // тейк
        double confidence;
        String reason;
    }
}
