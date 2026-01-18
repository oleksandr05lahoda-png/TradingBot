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
    private final TelegramBotSender bot;
    private final HttpClient http;
    MarketContext ctx = null; // инициализируем позже в evaluate

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
    private final Map<String, Long> lastTickTime = new ConcurrentHashMap<>();
    private final Map<String, MicroCandleBuilder> microBuilders = new ConcurrentHashMap<>();
    private final Map<String, OrderbookSnapshot> orderbookMap = new ConcurrentHashMap<>();
    private final AtomicLong dailyRequests = new AtomicLong(0);
    private final DecisionEngineV2 decisionEngine = new DecisionEngineV2();
    private long dailyResetTs = System.currentTimeMillis();
    private final AdaptiveBrain brain = new AdaptiveBrain();
    private final RiskEngine riskEngine = new RiskEngine(10.0); // maxLeverage = 10, можешь изменить
    private ScheduledExecutorService scheduler;
    CandlePredictor predictor = new CandlePredictor();

    public SignalSender(TelegramBotSender bot) {
        this.bot = bot;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        // defaults (use env to override)
        this.TOP_N = envInt("TOP_N", 70);
        this.MIN_CONF = 0.55;
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
    public class CandlePredictor {

        public List<String> predictNextNCandlesDirection(List<Candle> candles, int n) {
            List<String> res = new ArrayList<>();
            if (candles.size() < 5) return res;

            Candle last = candles.get(candles.size() - 1);
            Candle prev = candles.get(candles.size() - 2);

            double atr = atr(candles, 14);
            double body = Math.abs(last.close - last.open);

            String dir;
            if (last.close < prev.low && body > atr) dir = "SHORT";
            else if (last.close > prev.high && body > atr) dir = "LONG";
            else dir = last.close > prev.close ? "LONG" : "SHORT";

            for (int i = 0; i < n; i++) res.add(dir);
            return res;
        }

        public double computeForecastConfidence(List<Candle> pastCandles, int horizon, String dir) {
            if (pastCandles == null || pastCandles.size() < horizon) return 0.5;
            int success = 0;
            for (int i = horizon; i < pastCandles.size(); i++) {
                Candle start = pastCandles.get(i - horizon);
                Candle end = pastCandles.get(i);
                if ("LONG".equals(dir) && end.close > start.close) success++;
                if ("SHORT".equals(dir) && end.close < start.close) success++;
            }
            return (double) success / (pastCandles.size() - horizon);
        }
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
            boolean liquiditySweep,
            String pair // добавляем pair для истории
    ) {
        double conf = brain.adaptConfidence("FAST-MOMENTUM", 0.72);

        conf = brain.adaptConfidence("FAST-MOMENTUM", conf);
        conf += brain.impulsePenalty(pair);
        conf += brain.sessionBoost();

        conf = Math.max(0.55, Math.min(0.85, conf));


        // ===== Базовая оценка =====
        conf += Math.min(1.0, Math.abs(rawScore));

        if (mtfConfirm != 0) conf += 0.1;
        if (volOk) conf += 0.1;
        if (atrOk) conf += 0.1;
        if (impulse) conf += 0.05;
        if (vwapAligned) conf += 0.05;
        if (liquiditySweep && Math.abs(rawScore) > 0.2) {
            conf -= 0.25;
        }
        return Math.max(0.0, Math.min(1.0, conf));
    }
    private double lastSwingLow(List<Candle> candles) {
        int lookback = Math.min(20, candles.size());
        double low = Double.POSITIVE_INFINITY;
        for (int i = candles.size() - lookback; i < candles.size(); i++)
            low = Math.min(low, candles.get(i).low);
        return low;
    }
    private double lastSwingHigh(List<Candle> candles) {
        int lookback = Math.min(20, candles.size());
        double high = Double.NEGATIVE_INFINITY;
        for (int i = candles.size() - lookback; i < candles.size(); i++)
            high = Math.max(high, candles.get(i).high);
        return high;
    }
    private RiskEngine.TradeSignal addStopTake(RiskEngine.TradeSignal ts, String direction, double price, double atr) {

        double risk = atr * 1.2; // базовый риск

        if ("LONG".equals(direction)) {
            ts.stop = price - risk;
            ts.take = price + risk * 2.5;
        } else if ("SHORT".equals(direction)) {
            ts.stop = price + risk;
            ts.take = price - risk * 2.5;
        }
        return ts;
    }
    private void sendSignalIfAllowed(String pair,
                                     Signal s,
                                     List<Candle> closes5m) {

        // 1) Минимальная уверенность
        if (s.confidence < MIN_CONF) return;

        // 2) cooldown
        if (isCooldown(pair, s.direction)) return;

        // 3) risk + stop/take
        RiskEngine.TradeSignal ts = riskEngine.applyRisk(
                s.symbol,
                s.direction,
                s.price,
                atr(closes5m, 14),
                s.confidence,
                "AutoRiskEngine"
        );
        ts = addStopTake(ts, s.direction, s.price, atr(closes5m, 14));
        s.stop = ts.stop;
        s.take = ts.take;

        // 4) save history
        signalHistory.computeIfAbsent(pair, k -> new ArrayList<>()).add(s);

        // 5) send
        sendRaw(s.toTelegramMessage());

        // 6) mark cooldown
        markSignalSent(pair, s.direction, s.confidence);
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
        public Double stop;
        public Double take;
        public final boolean earlyTrigger;
        public final Instant created = Instant.now();
        public final List<String> next5Candles;

        public Signal(String symbol, String direction, double confidence, double price, double rsi,
                      double rawScore, int mtfConfirm, boolean volOk, boolean atrOk, boolean strongTrigger,
                      boolean atrBreakLong, boolean atrBreakShort, boolean impulse, boolean earlyTrigger,
                      double rsi7, double rsi4, List<String> next5Candles) {
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
            this.next5Candles = next5Candles;
        }

        public String toTelegramMessage() {
            String flags = (strongTrigger ? "⚡strong " : "") +
                    (earlyTrigger ? "⚡early " : "") +
                    (atrBreakLong ? "ATR↑ " : "") +
                    (atrBreakShort ? "ATR↓ " : "") +
                    (impulse ? "IMPULSE " : "");

            String next5Str = next5Candles != null ? String.join(" → ", next5Candles) : "N/A";
            String localTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

            return String.format("*%s* → *%s*\nNext 5: %s\nConfidence: *%.2f*\nPrice: %.8f\nRSI(14): %.2f\n_flags_: %s\n_raw: %.3f mtf:%d vol:%b atr:%b_\n_time: %s_",
                    symbol, direction, next5Str, confidence, price, rsi, flags.trim(), rawScore, mtfConfirm, volOk, atrOk, localTime);
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

        double obi = Math.abs(obs.obi());
        return obi > OBI_THRESHOLD;
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
        return (bull >= 2 && bear <= 1) || (bear >= 2 && bull <= 1);
    }
    private void sendRaw(String msg) {
        try {
            bot.sendSignal(msg);
        } catch (Exception e) {
            System.out.println("[sendRaw] " + e.getMessage());
        }
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
    public void start() {
        System.out.println("[SignalSender] Scheduler started");

        if (BINANCE_PAIRS == null || BINANCE_PAIRS.isEmpty()) {
            BINANCE_PAIRS = getTopSymbolsSet(TOP_N)
                    .stream()
                    .filter(p -> p.endsWith("USDT"))
                    .collect(Collectors.toSet());

            System.out.println("[INIT] Loaded pairs: " + BINANCE_PAIRS.size());
        }

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                runSchedulerCycle();
            } catch (Exception e) {
                System.out.println("[Scheduler] error: " + e.getMessage());
                e.printStackTrace();
            }
        }, 0, INTERVAL_MIN, TimeUnit.MINUTES);
    }
    public Set<String> getTopSymbolsSet(int limit) {
        List<String> list = getTopSymbols(limit); // используем существующий метод List<String>
        return new HashSet<>(list); // конвертируем в Set
    }
    private void sendFastSignal(String pair, String dir, double price, double atr, List<Candle> c5m) {

        double rsi14 = rsi(c5m.stream().map(c -> c.close).toList(), 14);
        List<String> next5 = predictor.predictNextNCandlesDirection(c5m, 5);

        Signal s = new Signal(
                pair.replace("USDT",""),
                dir,
                0.72,              // фикс confidence
                price,
                rsi14,
                0.0,
                0,
                true,
                true,
                true,              // strongTrigger
                dir.equals("LONG"),
                dir.equals("SHORT"),
                true,              // impulse
                false,
                rsi(c5m.stream().map(c->c.close).toList(),7),
                rsi(c5m.stream().map(c->c.close).toList(),4),
                next5
        );

        RiskEngine.TradeSignal ts =
                riskEngine.applyRisk(s.symbol, s.direction, price, atr, s.confidence, "FAST-MOMENTUM");

        s.stop = ts.stop;
        s.take = ts.take;

        sendRaw(s.toTelegramMessage());
    }
    private void runSchedulerCycle() {
        Set<String> symbols = getTopSymbolsSet(TOP_N);
        for (String pair : symbols) {
            try {
                CompletableFuture<List<Candle>> f5 =
                        fetchKlinesAsync(pair, "5m", KLINES_LIMIT);
                CompletableFuture<List<Candle>> f15 =
                        fetchKlinesAsync(pair, "15m", KLINES_LIMIT / 3);
                CompletableFuture<List<Candle>> f1h =
                        fetchKlinesAsync(pair, "1h", KLINES_LIMIT / 12);

                CompletableFuture.allOf(f5, f15, f1h).join();

                List<Candle> c5m = f5.join();
                List<Candle> c15m = f15.join();
                List<Candle> c1h = f1h.join();

                if (c5m.size() < 20 || c15m.isEmpty()) continue;

                // ================= FAST MOMENTUM STRATEGY =================
                Candle last = c5m.get(c5m.size() - 1);
                Candle prev = c5m.get(c5m.size() - 2);
                Candle prev2 = c5m.get(c5m.size() - 3);

                double atr5 = SignalSender.atr(c5m, 14);
                double body = Math.abs(last.close - last.open);

                boolean crashDown =
                        prev.close < c5m.get(c5m.size() - 3).low &&
                                Math.abs(prev.close - prev.open) > 0.9 * atr5;

                boolean crashUp =
                        prev.close > c5m.get(c5m.size() - 3).high &&
                                Math.abs(prev.close - prev.open) > 0.9 * atr5;

                if (crashDown || crashUp) {
                    String side = crashDown ? "SHORT" : "LONG";
                    double conf = 0.72; // добавлено

                    List<Double> closes5 = c5m.stream().map(c -> c.close).toList();
                    double rsi14 = SignalSender.rsi(closes5, 14);
                    double rsi7 = SignalSender.rsi(closes5, 7);
                    double rsi4 = SignalSender.rsi(closes5, 4);

                    List<String> next5 = predictor.predictNextNCandlesDirection(c5m, 5);

                    RiskEngine.TradeSignal ts = riskEngine.applyRisk(
                            pair.replace("USDT",""),
                            side,
                            last.close,
                            atr5,
                            conf,
                            "FAST-MOMENTUM"
                    );

                    Signal s = new Signal(
                            pair.replace("USDT",""),
                            side,
                            conf,
                            last.close,
                            rsi14,
                            0.0,
                            0,
                            true,
                            true,
                            true,
                            side.equals("LONG"),
                            side.equals("SHORT"),
                            true,
                            false,
                            rsi7,
                            rsi4,
                            next5
                    );

                    s.stop = ts.stop;
                    s.take = ts.take;

                    sendRaw(s.toTelegramMessage());
                    markSignalSent(pair, side, conf);
                    continue;
                }
                Optional<DecisionEngineV2.TradeIdea> idea =
                        decisionEngine.evaluate(pair, c5m, c15m, c1h);

                idea.ifPresent(i -> {

                    double conf = i.confidence;
                    conf = brain.adaptConfidence(i.reason, conf);
                    conf += brain.sessionBoost();
                    conf += brain.impulsePenalty(pair);
                    conf = Math.max(0.50, Math.min(0.88, conf));

                    if (isCooldown(pair, i.side)) return;

                    double entryPrice = i.entry;

                    // risk + stop/take
                    RiskEngine.TradeSignal ts = riskEngine.applyRisk(
                            pair.replace("USDT",""),
                            i.side,
                            entryPrice,
                            i.atr,
                            conf,
                            "FAST-MOMENTUM"
                    );
                    ts = addStopTake(ts, i.side, entryPrice, i.atr);

                    // indicators
                    List<Double> closes5 = c5m.stream().map(c -> c.close).toList();
                    double rsi14 = SignalSender.rsi(closes5, 14);
                    double rsi7  = SignalSender.rsi(closes5, 7);
                    double rsi4  = SignalSender.rsi(closes5, 4);

                    // next 5 forecast
                    List<String> next5 = predictor.predictNextNCandlesDirection(c5m, 5);

                    // build signal
                    Signal s = new Signal(
                            i.symbol.replace("USDT", ""),
                            i.side,
                            conf,
                            i.entry,
                            rsi14,
                            0.0,
                            0,
                            true,
                            true,
                            true,
                            false,
                            false,
                            true,
                            false,
                            rsi7,
                            rsi4,
                            next5
                    );

                    s.stop = ts.stop;
                    s.take = ts.take;

                    sendSignalIfAllowed(pair, s, c5m);
                });
            } catch (Exception e) {
                System.out.println("[Scheduler] error for " + pair + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}