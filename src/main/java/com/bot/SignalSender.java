package com.bot;
import org.json.JSONArray;
import org.json.JSONObject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.net.URI;
import java.net.http.*;
import java.util.Optional;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SignalSender {
    private final com.bot.TelegramBotSender bot;
    private final HttpClient http;
    MarketContext ctx = null; // инициализируем позже в evaluate
    private final Object wsLock = new Object(); // для безопасного добавления тиков из WebSocket

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
    private final Map<String, Double> lastSentConfidence = new ConcurrentHashMap<>(); // last confidence
    private final Map<String, Deque<Double>> tickPriceDeque = new ConcurrentHashMap<>();
    private final Map<String, Double> lastTickPrice = new ConcurrentHashMap<>();
    private final Map<String, java.net.http.WebSocket> wsMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService wsWatcher = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, Long> lastTickTime = new ConcurrentHashMap<>();
    private final Map<String, MicroCandleBuilder> microBuilders = new ConcurrentHashMap<>();
    private final Map<String, OrderbookSnapshot> orderbookMap = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> lastSignalCandleTs = new ConcurrentHashMap<>();
    private final AtomicLong dailyRequests = new AtomicLong(0);
    private Set<String> cachedPairs = new HashSet<>();
    private final DecisionEngineMerged decisionEngine;
    private final Elite5MinAnalyzer elite5MinAnalyzer;
    private final TradingCore.AdaptiveBrain adaptiveBrain;
    private final SignalOptimizer optimizer;
    private final TradingCore.RiskEngine riskEngine = new TradingCore.RiskEngine(0.01);
    private int signalsThisCycle = 0;  // <-- ДОБАВИТЬ
    private final Map<String, Signal> activeSignals = new ConcurrentHashMap<>();
    private final Map<String, List<Signal>> signalHistory = new ConcurrentHashMap<>();
    private long dailyResetTs = System.currentTimeMillis();
    private ScheduledExecutorService scheduler;
    public SignalSender(com.bot.TelegramBotSender bot) {

        this.bot = bot;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        // defaults (use env to override)
        this.TOP_N = envInt("TOP_N", 70);
        this.MIN_CONF = 0.50;
        this.INTERVAL_MIN = envInt("INTERVAL_MINUTES", 15);
        this.KLINES_LIMIT = envInt("KLINES", 240);
        this.REQUEST_DELAY_MS = envLong("REQUEST_DELAY_MS", 120);

        this.IMPULSE_PCT = envDouble("IMPULSE_PCT", 0.02);
        this.VOL_MULTIPLIER = envDouble("VOL_MULT", 0.9);
        this.ATR_MIN_PCT = envDouble("ATR_MIN_PCT", 0.0007);
        this.COOLDOWN_MS = envLong("COOLDOWN_MS", 15 * 60_000L);
        long brMin = envLong("BINANCE_REFRESH_MINUTES", 60);
        this.BINANCE_REFRESH_INTERVAL_MS = brMin * 60 * 1000L;

        this.VWAP_SESSION_START = LocalTime.parse(System.getenv().getOrDefault("SESSION_START", "00:00"));

        this.TICK_HISTORY = envInt("TICK_HISTORY", 100);
        this.OBI_THRESHOLD = envDouble("OBI_THRESHOLD", 0.28);
        this.VOLUME_SPIKE_MULT = envDouble("VOL_SPIKE_MULT", 1.4);

        this.STABLE = Set.of("USDT", "USDC", "BUSD");
        this.decisionEngine = new DecisionEngineMerged();
        this.adaptiveBrain = new TradingCore.AdaptiveBrain();
        this.elite5MinAnalyzer = new Elite5MinAnalyzer();
        TradingCore.AdaptiveBrain brain = new TradingCore.AdaptiveBrain();
        this.optimizer = new SignalOptimizer(this.tickPriceDeque, brain);
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
    enum MarketPhase {
        TREND_CONTINUATION,
        TREND_EXHAUSTION,
        NO_TRADE
    }

    public class MarketContext {
        public final double vwapDev;
        public final boolean higherLows;
        public final boolean lowerHighs;
        public final double atr;
        public final double atrCompression;
        public final double rsi;
        public final String higherTFTrend;

        public MarketContext(
                double vwapDev,
                boolean higherLows,
                boolean lowerHighs,
                double atr,
                double atrCompression,
                double rsi,
                String higherTFTrend
        ) {
            this.vwapDev = vwapDev;
            this.higherLows = higherLows;
            this.lowerHighs = lowerHighs;
            this.atr = atr;
            this.atrCompression = atrCompression;
            this.rsi = rsi;
            this.higherTFTrend = higherTFTrend;
        }
    }

    public CompletableFuture<List<TradingCore.Candle>> fetchKlinesAsync(String symbol, String interval, int limit) {
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
                        List<TradingCore.Candle> list = new ArrayList<>();
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
                            list.add(new TradingCore.Candle(openTime, open, high, low, close, vol, qvol, closeTime));
                        }
                        return list;
                    });
        } catch (Exception e) {
            System.out.println("[Binance] Error preparing klines request for " + symbol + " : " + e.getMessage());
            return CompletableFuture.completedFuture(Collections.emptyList());
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

        public Optional<TradingCore.Candle> addTick(long tsMillis, double price, double qty) {
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
                TradingCore.Candle c = new TradingCore.Candle(currentBucketStart, open, high, low, close, volume, volume, closeTime);
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

    public static double rsi(List<Double> prices, int period) {
        if (prices == null || prices.size() <= period) return 50.0;
        double gain = 0, loss = 0;
        for (int i = prices.size() - period; i < prices.size(); i++) {
            double diff = prices.get(i) - prices.get(i - 1);
            if (diff > 0) gain += diff;
            else loss += -diff;
        }
        if (gain + loss == 0) return 50.0;
        double rs = gain / (loss + 1e-12);
        return 100.0 - (100.0 / (1.0 + rs));
    }


    // ATR
    public static double atr(List<TradingCore.Candle> candles, int period) {
        if (candles == null || candles.size() <= period) return 0.0;
        List<Double> trs = new ArrayList<>();
        for (int i = 1; i < candles.size(); i++) {
            TradingCore.Candle prev = candles.get(i - 1);
            TradingCore.Candle cur = candles.get(i);
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

    public static double vwap(List<TradingCore.Candle> candles) {
        if (candles == null || candles.isEmpty()) return 0.0;
        double pv = 0.0, vol = 0.0;
        for (TradingCore.Candle c : candles) {
            double tp = (c.high + c.low + c.close) / 3.0;
            pv += tp * c.volume;
            vol += c.volume;
        }
        if (vol == 0) return candles.get(candles.size() - 1).close;
        return pv / vol;
    }

    // ========================= Price Action helpers =========================
    public static List<Integer> detectSwingHighs(List<TradingCore.Candle> candles, int leftRight) {
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

    public static List<Integer> detectSwingLows(List<TradingCore.Candle> candles, int leftRight) {
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

    public static int marketStructure(List<TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 20) return 0;
        List<Integer> highs = detectSwingHighs(candles, 5);
        List<Integer> lows = detectSwingLows(candles, 5);
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
    private MarketPhase detectMarketPhase(
            List<TradingCore.Candle> c15m,
            MicroTrendResult micro
    ) {
        int structure = marketStructure(c15m);
        if (structure == 0) {
            return MarketPhase.NO_TRADE; // флэт = не торгуем
        }

        List<Double> closes = c15m.stream().map(c -> c.close).toList();

        double atr = atr(c15m, 14);
        if (atr <= 0) return MarketPhase.NO_TRADE;

        double ema20 = ema(closes, 20);
        double ema50 = ema(closes, 50);

        double trendStrength = Math.abs(ema20 - ema50) / atr;

        // ---- 1. СЕРЕДИНА ТРЕНДА ----
        if (trendStrength > 0.6 && trendStrength < 1.2) {
            if (Math.abs(micro.accel) < 0.0003) {
                return MarketPhase.TREND_CONTINUATION;
            }
        }

        // ---- 2. КОНЕЦ ТРЕНДА (EXHAUSTION) ----
        if (trendStrength >= 1.2) {
            boolean momentumFading =
                    Math.abs(micro.speed) < 0.0006 &&
                            micro.accel < 0;

            boolean structureExtreme =
                    detectLiquiditySweep(c15m) ||
                            detectBOS(c15m);

            if (momentumFading && structureExtreme) {
                return MarketPhase.TREND_EXHAUSTION;
            }
        }

        return MarketPhase.NO_TRADE;
    }
    public static boolean detectBOS(List<TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 10) return false;
        List<Integer> highs = detectSwingHighs(candles, 3);
        List<Integer> lows = detectSwingLows(candles, 3);
        if (highs.size() < 2 && lows.size() < 2) return false;
        TradingCore.Candle last = candles.get(candles.size() - 1);
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

    public static boolean detectLiquiditySweep(List<TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 6) return false;
        int n = candles.size();
        TradingCore.Candle last = candles.get(n - 1);
        TradingCore.Candle prev = candles.get(n - 2);
        double upperWick = last.high - Math.max(last.open, last.close);
        double lowerWick = Math.min(last.open, last.close) - last.low;
        double body = Math.abs(last.close - last.open);
        if (upperWick > body * 1.8 && last.close < prev.close) return true;
        if (lowerWick > body * 1.8 && last.close > prev.close) return true;
        return false;
    }

    private int emaDirection(List<TradingCore.Candle> candles, int fast, int slow) {
        if (candles == null || candles.size() < Math.max(slow + 5, 60)) return 0;

        List<Double> closes = candles.subList(candles.size() - 60, candles.size())
                .stream()
                .map(c -> c.close)
                .collect(Collectors.toList());

        double emaFast = ema(closes, fast);
        double emaSlow = ema(closes, slow);
        return Double.compare(emaFast, emaSlow);
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

    private double holdProbability5Bars(
            List<TradingCore.Candle> c15m,
            String direction
    ) {
        if (c15m.size() < 30) return 0.5;

        List<Double> closes = c15m.stream().map(c -> c.close).toList();

        double emaScore = strategyEMANorm(closes);
        double momScore = strategyMomentumNorm(closes);
        double rsi = rsi(closes, 14);

        double dir = direction.equals("LONG") ? 1 : -1;

        double score =
                dir * emaScore * 0.45 +
                        dir * momScore * 0.35 +
                        (dir == 1 ? (50 - rsi) : (rsi - 50)) / 50 * 0.20;

        double prob = 0.5 + score * 0.4;
        return Math.max(0.1, Math.min(0.9, prob));
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

    private int multiTFConfirm(int dir1h, int dir15m) {
        int score = 0;
        score += dir1h * 3;
        score += dir15m * 2;
        if (score > 1) return 1;   // раньше было >2
        if (score < -1) return -1; // раньше было <-2
        return 0;
    }

    private double composeConfidence(
            double rawScore,
            int mtfConfirm,
            boolean volOk,
            boolean atrOk,
            boolean impulse,
            boolean vwapAligned,
            boolean structureAligned,
            boolean bos,
            boolean liquiditySweep
    ) {
        double conf = 0.5;

        conf += Math.abs(rawScore) * 0.55; // увеличили вклад
        if (mtfConfirm != 0) conf += 0.10;
        if (volOk) conf += 0.08;
        if (atrOk) conf += 0.08;
        if (impulse && Math.abs(rawScore) > 0.25) conf += 0.04;
        if (vwapAligned) conf += 0.05;
        if (structureAligned) conf += 0.06;
        if (bos) conf += 0.05;

        if (liquiditySweep && rawScore * mtfConfirm < 0) {
            conf += 0.10; // sweep ПРОТИВ тренда = хорошо для разворота
        } else if (liquiditySweep) {
            conf -= 0.10;
        }

        // расширяем диапазон
        conf = Math.max(0.20, Math.min(0.90, conf));
        return conf;
    }

    private double lastSwingLow(List<TradingCore.Candle> candles) {
        int lookback = Math.min(20, candles.size());
        double low = Double.POSITIVE_INFINITY;
        for (int i = candles.size() - lookback; i < candles.size(); i++)
            low = Math.min(low, candles.get(i).low);
        return low;
    }

    private double lastSwingHigh(List<TradingCore.Candle> candles) {
        int lookback = Math.min(20, candles.size());
        double high = Double.NEGATIVE_INFINITY;
        for (int i = candles.size() - lookback; i < candles.size(); i++)
            high = Math.max(high, candles.get(i).high);
        return high;
    }

    private void sendSignalIfAllowed(String pair,
                                     Signal s,
                                     List<TradingCore.Candle> closes15m) {

        if (s.confidence < MIN_CONF) {
            System.out.println("[DEBUG] Skipped " + pair + " due to low confidence: " + s.confidence);
            return;
        }
        if (closes15m.size() < 4) return;

        List<TradingCore.Candle> recent = new ArrayList<>(closes15m);

        boolean bos = detectBOS(recent);
        boolean liqSweep = detectLiquiditySweep(recent);

        MicroTrendResult micro = computeMicroTrend(s.symbol, tickPriceDeque.get(s.symbol));

        double lastHigh = lastSwingHigh(recent);
        double lastLow = lastSwingLow(recent);

        boolean nearSwingHigh = s.direction.equals("LONG") && recent.get(recent.size() - 1).close >= lastHigh * 0.995;
        boolean nearSwingLow  = s.direction.equals("SHORT") && recent.get(recent.size() - 1).close <= lastLow * 1.005;

        boolean endOfTrend = nearSwingHigh || nearSwingLow;

        // ==== Для теста временно отключаем резкое снижение confidence ====
        //if (endOfTrend) {
        //    double microFactor = 0.85 + Math.min(0.15, Math.abs(micro.speed) * 100);
        //    s.confidence *= microFactor;
        //    if (!bos && !liqSweep) {
        //        s.confidence *= Math.max(0.75, 1.0 - Math.abs(micro.speed) * 100);
        //    }
        //}
        //if ((bos || liqSweep) && endOfTrend) {
        //    s.confidence *= 0.85;
        //}

        if (isCooldown(pair, s)) {
            System.out.println("[DEBUG] Skipped " + pair + " due to cooldown");
            return;
        }
        markSignalSent(pair, s.direction);

        signalHistory.computeIfAbsent(pair, k -> new ArrayList<>()).add(s);

        System.out.println("[DEBUG] Sending signal: " + s.toTelegramMessage());
        bot.sendMessageAsync(s.toTelegramMessage());

        signalsThisCycle++;
    }
    public static class Signal {
        public final String symbol;
        public final String direction;
        public double confidence;
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
        public Double stop;
        public Double take;
        public final Instant created = Instant.now();

        public Signal(String symbol, String direction, double confidence, double price, double rsi,
                      double rawScore, int mtfConfirm, boolean volOk, boolean atrOk, boolean strongTrigger,
                      boolean atrBreakLong, boolean atrBreakShort, boolean impulse,
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
            this.rsi7 = rsi7;
            this.rsi4 = rsi4;
        }

        public String toTelegramMessage() {
            String flags = (strongTrigger ? "⚡strong " : "") +
                    (atrBreakLong ? "ATR↑ " : "") +
                    (atrBreakShort ? "ATR↓ " : "") +
                    (impulse ? "IMPULSE " : "");
            String localTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

            return String.format("*%s* → *%s*\n" + "Confidence: *%.2f*\n" + "Price: %.8f\n" + "SL: %.8f\n" + "TP: %.8f\n" + "RSI(14): %.2f\n" + "_flags_: %s\n" + "_raw: %.3f mtf:%d vol:%b atr:%b_\n" + "_time: %s_",
                    symbol, direction, confidence, price, stop != null ? stop : 0.0, take != null ? take : 0.0, rsi, flags.trim(), rawScore, mtfConfirm, volOk, atrOk,
                    localTime
            );
        }
    }

    private final Map<String, Map<String, Long>> lastSignalTimeDir = new ConcurrentHashMap<>();

    private boolean isCooldown(String pair, Signal s) {
        long now = System.currentTimeMillis();
        Map<String, Long> dirMap = lastSignalTimeDir.computeIfAbsent(pair, k -> new ConcurrentHashMap<>());
        Long lastTimeSameDir = dirMap.get(s.direction);

        if (lastTimeSameDir != null && now - lastTimeSameDir < 15 * 60_000) {
            return true; // 15 минут cooldown вместо 45
        }

        return false; // все остальное разрешено
    }
    private void markSignalSent(String pair, String direction) {
        lastSignalTimeDir.computeIfAbsent(pair, k -> new ConcurrentHashMap<>())
                .put(direction, System.currentTimeMillis());
    }

    public void connectTickWebSocket(String pair) {
        wsWatcher.execute(() -> connectWsInternal(pair));
    }

    private void connectWsInternal(String pair) {
        try {
            final String symbol = pair.toLowerCase();
            String url = "wss://fstream.binance.com/ws/" + symbol + "@aggTrade";

            System.out.println("[WS] Connecting " + pair);
            HttpClient client = HttpClient.newHttpClient();
            client.newWebSocketBuilder()
                    .buildAsync(URI.create(url), new java.net.http.WebSocket.Listener() {
                        @Override
                        public CompletionStage<?> onText(java.net.http.WebSocket webSocket, CharSequence data, boolean last) {
                            try {
                                JSONObject json = new JSONObject(data.toString());
                                double price = Double.parseDouble(json.getString("p"));
                                long ts = json.getLong("T");

                                synchronized (wsLock) {
                                    Deque<Double> dq = tickPriceDeque.computeIfAbsent(pair, k -> new ArrayDeque<>());
                                    dq.addLast(price);
                                    while (dq.size() > TICK_HISTORY) dq.removeFirst();
                                }

                                lastTickPrice.put(pair, price);
                                lastTickTime.put(pair, ts);
                            } catch (Exception ignored) {}
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override
                        public void onError(java.net.http.WebSocket webSocket, Throwable error) {
                            System.out.println("[WS ERROR] " + pair + " " + error.getMessage());
                            wsWatcher.schedule(() -> connectWsInternal(pair), 5, TimeUnit.SECONDS);
                        }

                        @Override
                        public CompletionStage<?> onClose(java.net.http.WebSocket webSocket, int statusCode, String reason) {
                            System.out.println("[WS CLOSED] " + pair + " code=" + statusCode);
                            wsWatcher.schedule(() -> connectWsInternal(pair), 5, TimeUnit.SECONDS);
                            return CompletableFuture.completedFuture(null);
                        }
                    })
                    .thenAccept(ws -> wsMap.put(pair, ws))
                    .exceptionally(ex -> {
                        System.out.println("[WS] Failed connect " + pair + " retry in 5s: " + ex.getMessage());
                        wsWatcher.schedule(() -> connectWsInternal(pair), 5, TimeUnit.SECONDS);
                        return null;
                    });
            java.net.http.WebSocket ws = HttpClient.newHttpClient()
                    .newWebSocketBuilder()
                    .buildAsync(URI.create(url), new java.net.http.WebSocket.Listener() {

                        @Override
                        public CompletionStage<?> onText(java.net.http.WebSocket webSocket, CharSequence data, boolean last) {
                            try {
                                JSONObject json = new JSONObject(data.toString());
                                double price = Double.parseDouble(json.getString("p"));
                                long ts = json.getLong("T");

                                synchronized (wsLock) {
                                    Deque<Double> dq = tickPriceDeque.computeIfAbsent(pair, k -> new ArrayDeque<>());
                                    dq.addLast(price);
                                    while (dq.size() > TICK_HISTORY) dq.removeFirst();
                                }

                                lastTickPrice.put(pair, price);
                                lastTickTime.put(pair, ts);

                            } catch (Exception ignored) {}
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override
                        public void onError(java.net.http.WebSocket webSocket, Throwable error) {
                            System.out.println("[WS ERROR] " + pair + " " + error.getMessage());
                            reconnect(pair);
                        }

                        @Override
                        public CompletionStage<?> onClose(java.net.http.WebSocket webSocket, int statusCode, String reason) {
                            System.out.println("[WS CLOSED] " + pair + " code=" + statusCode);
                            reconnect(pair);
                            return CompletableFuture.completedFuture(null);
                        }
                    }).join();

            wsMap.put(pair, ws);
            System.out.println("[WS] Connected " + pair);

        } catch (Exception e) {
            System.out.println("[WS] Failed connect " + pair + " retry in 5s");
            reconnect(pair);
        }
    }

    private void reconnect(String pair) {
        wsWatcher.schedule(() -> connectWsInternal(pair), 5, TimeUnit.SECONDS);
    }
    private MicroTrendResult computeMicroTrend(String pair, Deque<Double> dq) {
        if (dq == null || dq.size() < 5) return new MicroTrendResult(0, 0, 0);
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
    private boolean isVolumeStrong(String pair, double lastPrice) {
        OrderbookSnapshot obs = orderbookMap.get(pair);
        if (obs == null) return false;

        double obi = Math.abs(obs.obi());
        return obi > OBI_THRESHOLD;
    }

    public void stop() {
        System.out.println("[SignalSender] stopping...");
        if (scheduler != null) scheduler.shutdown();
        wsWatcher.shutdown();
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


    public List<TradingCore.Candle> fetchKlines(String symbol, String interval, int limit) {
        try {
            List<TradingCore.Candle> candles = fetchKlinesAsync(symbol, interval, limit).get();
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

    public void start() {
        System.out.println("[SignalSender] Scheduler started");

        if (BINANCE_PAIRS == null || BINANCE_PAIRS.isEmpty()) {
            BINANCE_PAIRS = getTopSymbolsSet(TOP_N)
                    .stream()
                    .filter(p -> p.endsWith("USDT"))
                    .collect(Collectors.toSet());

            System.out.println("[INIT] Loaded pairs: " + BINANCE_PAIRS.size());
            for (String pair : BINANCE_PAIRS) {
                connectTickWebSocket(pair);
            }
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "signal-scheduler");
            t.setDaemon(false);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                runSchedulerCycle();
            } catch (Throwable t) {
                System.out.println("[Scheduler-FATAL] cycle crashed: " + t.getMessage());
                t.printStackTrace();
            }
        }, 0, INTERVAL_MIN, TimeUnit.MINUTES);
    }

    public Set<String> getTopSymbolsSet(int limit) {
        List<String> list = getTopSymbols(limit); // используем существующий метод List<String>
        return new HashSet<>(list); // конвертируем в Set
    }

    // ========================= AGGREGATE candles =========================
    private List<TradingCore.Candle> aggregate(List<TradingCore.Candle> base, int factor) {
        List<TradingCore.Candle> res = new ArrayList<>();
        if (base == null || base.size() < factor) return res;

        for (int i = 0; i + factor <= base.size(); i += factor) {
            long openTime = base.get(i).openTime;
            double open = base.get(i).open;
            double high = Double.NEGATIVE_INFINITY;
            double low = Double.POSITIVE_INFINITY;
            double close = base.get(i + factor - 1).close;
            double volume = 0;
            double qvol = 0;
            long closeTime = base.get(i + factor - 1).closeTime;

            for (int j = i; j < i + factor; j++) {
                TradingCore.Candle c = base.get(j);
                high = Math.max(high, c.high);
                low = Math.min(low, c.low);
                volume += c.volume;
                qvol += c.quoteAssetVolume;
            }

            res.add(new TradingCore.Candle(
                    openTime,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    qvol,
                    closeTime
            ));
        }
        return res;
    }
    private void runSchedulerCycle() {
        signalsThisCycle = 0;
        long now = System.currentTimeMillis();

        // ===== REFRESH SYMBOLS =====
        if (cachedPairs.isEmpty() || now - lastBinancePairsRefresh > BINANCE_REFRESH_INTERVAL_MS) {
            cachedPairs = getTopSymbolsSet(TOP_N);
            lastBinancePairsRefresh = now;
            System.out.println("[Pairs] Refreshed top symbols: " + cachedPairs.size());
        }

        Set<String> symbols = cachedPairs.stream()
                .filter(BINANCE_PAIRS::contains)
                .collect(Collectors.toSet());

        for (String pair : symbols) {
            try {
                CompletableFuture<List<TradingCore.Candle>> f15 =
                        fetchKlinesAsync(pair, "15m", KLINES_LIMIT / 3);
                CompletableFuture<List<TradingCore.Candle>> f1h =
                        fetchKlinesAsync(pair, "1h", KLINES_LIMIT / 12);
                CompletableFuture.allOf(f15, f1h).join();

                List<TradingCore.Candle> c15m = new ArrayList<>(f15.join());
                List<TradingCore.Candle> c1h  = new ArrayList<>(f1h.join());
                if (c15m.size() < 30 || c1h.size() < 30) continue;

                TradingCore.Candle last = c15m.get(c15m.size() - 1);
                MicroTrendResult micro = computeMicroTrend(pair, tickPriceDeque.getOrDefault(pair, new ArrayDeque<>()));
                MarketPhase phase = detectMarketPhase(c15m, micro);
                if (phase == MarketPhase.NO_TRADE) continue;

                double atr15 = atr(c15m, 14);
                if (atr15 <= 0) continue;
                double atrPercent = atr15 / last.close;
                if (atrPercent < 0.002) continue; // ослаблено для теста

                boolean volOk = true; // для теста игнорируем

                List<Double> closes15 = c15m.stream().map(c -> c.close).toList();
                double rsi14 = SignalSender.rsi(closes15, 14);

                int dir15 = marketStructure(c15m);
                int dir1h = marketStructure(c1h);
                //if (dir15 == 0) continue; // временно отключаем фильтр
                int mtfConfirm = multiTFConfirm(dir1h, dir15);

                boolean impulseUp   = last.close > last.open && (last.high - last.low) > atr15 * 0.5; // порог снижен
                boolean impulseDown = last.close < last.open && (last.high - last.low) > atr15 * 0.5;

                TradingCore.Side side = null;
                if (dir15 >= 0 && rsi14 > 30 && rsi14 < 80) side = TradingCore.Side.LONG;
                else if (dir15 <= 0 && rsi14 > 20 && rsi14 < 70) side = TradingCore.Side.SHORT;
                if (side == null) continue;

                double rawScore = strategyEMANorm(closes15) * 0.35 +
                        strategyRSINorm(closes15) * 0.25 +
                        strategyMomentumNorm(closes15) * 0.20 +
                        strategyMACDNorm(closes15) * 0.20;

                double conf = composeConfidence(
                        rawScore,
                        mtfConfirm,
                        volOk,
                        true,
                        (side == TradingCore.Side.LONG && impulseUp) || (side == TradingCore.Side.SHORT && impulseDown),
                        true,
                        true,
                        detectBOS(c15m),
                        detectLiquiditySweep(c15m)
                );
                if (side == TradingCore.Side.SHORT) conf -= 0.03;

                double pct = Math.max(0.003, Math.min(0.01, atrPercent));
                double stop = side == TradingCore.Side.LONG ? last.close * (1 - pct) : last.close * (1 + pct);
                double take = side == TradingCore.Side.LONG ? last.close * (1 + pct * 1.4) : last.close * (1 - pct * 1.4);

                Signal s = new Signal(
                        pair,
                        side.toString(),
                        conf,
                        last.close,
                        rsi14,
                        rawScore,
                        mtfConfirm,
                        volOk,
                        true,
                        true,
                        side == TradingCore.Side.LONG,
                        side == TradingCore.Side.SHORT,
                        true,
                        SignalSender.rsi(closes15, 7),
                        SignalSender.rsi(closes15, 4)
                );
                s.stop = stop;
                s.take = take;

                if (conf < MIN_CONF) continue;

                sendSignalIfAllowed(pair, s, c15m);

            } catch (Exception e) {
                System.out.println("[Scheduler] Error for " + pair + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("[Cycle] Signals sent: " + signalsThisCycle);
    }
}