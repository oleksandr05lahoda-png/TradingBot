package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SignalSender {
    private final com.bot.TelegramBotSender bot;
    private final HttpClient http;
    MarketContext ctx = null;
    private final Object wsLock = new Object();

    private final int TOP_N;
    private final double MIN_CONF;
    private final int INTERVAL_MIN;
    private final int KLINES_LIMIT;
    private final long REQUEST_DELAY_MS;

    private final double IMPULSE_PCT;
    private final double VOL_MULTIPLIER;
    private final double ATR_MIN_PCT;
    private final long COOLDOWN_MS;
    private final long BINANCE_REFRESH_INTERVAL_MS;

    private final LocalTime VWAP_SESSION_START;

    private final int TICK_HISTORY;
    private final double OBI_THRESHOLD;
    private final double VOLUME_SPIKE_MULT;

    private static final long FUNDING_REFRESH_INTERVAL_MS = 5 * 60_000;
    private long lastFundingRefresh = 0L;

    private final Set<String> STABLE;
    private Set<String> BINANCE_PAIRS = new HashSet<>();
    private long lastBinancePairsRefresh = 0L;
    private final Map<String, Double> lastSentConfidence = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>> tickPriceDeque = new ConcurrentHashMap<>();
    private final List<com.bot.TradingCore.Candle> btcCandles = new CopyOnWriteArrayList<>();
    private final Map<String, Double> lastTickPrice = new ConcurrentHashMap<>();
    private final Map<String, java.net.http.WebSocket> wsMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService wsWatcher = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, Long> lastTickTime = new ConcurrentHashMap<>();
    private final Map<String, MicroCandleBuilder> microBuilders = new ConcurrentHashMap<>();
    private final Map<String, OrderbookSnapshot> orderbookMap = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> lastSignalCandleTs = new ConcurrentHashMap<>();
    private final AtomicLong dailyRequests = new AtomicLong(0);
    private Set<String> cachedPairs = new HashSet<>();
    private final com.bot.DecisionEngineMerged decisionEngine;
    private final com.bot.GlobalImpulseController globalImpulse;
    private final Map<String, List<com.bot.TradingCore.Candle>> histM15 = new ConcurrentHashMap<>();
    private final Map<String, List<com.bot.TradingCore.Candle>> histH1  = new ConcurrentHashMap<>();
    private final Map<String, List<com.bot.TradingCore.Candle>> histH2  = new ConcurrentHashMap<>();

    private final com.bot.TradingCore.AdaptiveBrain adaptiveBrain;
    private final com.bot.SignalOptimizer optimizer;
    private final com.bot.TradingCore.RiskEngine riskEngine = new com.bot.TradingCore.RiskEngine(0.01, 0.05, 1.0);
    private final com.bot.InstitutionalSignalCore core =
            new com.bot.InstitutionalSignalCore(20, 2, 0.35, 0.52, 0.002, 10000);
    private int signalsThisCycle = 0;

    private final com.bot.PumpHunter pumpHunter;

    // ========================= PUBLIC API =========================

    @Override
    public String toString() {
        return "SignalSender{" +
                "TOP_N=" + TOP_N +
                ", MIN_CONF=" + MIN_CONF +
                ", cachedPairs=" + cachedPairs.size() +
                '}';
    }

    public List<com.bot.DecisionEngineMerged.TradeIdea> generateSignals() {

        List<com.bot.DecisionEngineMerged.TradeIdea> result = new ArrayList<>();

        if (cachedPairs == null || cachedPairs.isEmpty() ||
                System.currentTimeMillis() - lastBinancePairsRefresh > BINANCE_REFRESH_INTERVAL_MS) {

            cachedPairs = getTopSymbolsSet(TOP_N);
            lastBinancePairsRefresh = System.currentTimeMillis();
        }

        // === НОВОЕ: Обновляем Funding Rate ===
        if (System.currentTimeMillis() - lastFundingRefresh > FUNDING_REFRESH_INTERVAL_MS) {
            refreshAllFundingRates();
            lastFundingRefresh = System.currentTimeMillis();
        }

        com.bot.DecisionEngineMerged engine = decisionEngine;

        for (String pair : cachedPairs) {

            List<com.bot.TradingCore.Candle> m1  = fetchKlines(pair,"1m",KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> m5  = fetchKlines(pair,"5m",KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> m15 = fetchKlines(pair,"15m",KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> h1  = fetchKlines(pair,"1h",KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> h2  = fetchKlines(pair,"2h", 100);

            if (m1.size() < 60 || m5.size() < 60 || m15.size() < 60 || h1.size() < 60)
                continue;

            optimizer.updateFromCandles(pair, m15);

            com.bot.DecisionEngineMerged.TradeIdea idea =
                    engine.analyze(
                            pair, m1, m5, m15, h1, h2,
                            com.bot.DecisionEngineMerged.CoinCategory.TOP
                    );

            if (idea == null || idea.probability < MIN_CONF)
                continue;

            // === НОВОЕ: Проверка PumpHunter ===
            PumpHunter.PumpEvent pump = pumpHunter.detectPump(pair, m1, m5, m15);
            if (pump != null && pump.strength > 0.5) {
                boolean pumpAligned = (idea.side == com.bot.TradingCore.Side.LONG && pump.isBullish()) ||
                        (idea.side == com.bot.TradingCore.Side.SHORT && pump.isBearish());
                if (pumpAligned) {
                    List<String> newFlags = new ArrayList<>(idea.flags);
                    newFlags.add("PUMP_" + pump.type.name());
                    newFlags.add("pump_str=" + String.format("%.0f", pump.strength * 100));

                    idea = new com.bot.DecisionEngineMerged.TradeIdea(
                            idea.symbol,
                            idea.side,
                            idea.price,
                            idea.stop,
                            idea.take,
                            Math.min(85, idea.probability + pump.strength * 8),
                            newFlags,
                            idea.fundingRate,
                            idea.oiChange,
                            idea.htfBias
                    );
                }
            }

            // === SignalOptimizer корректировка ===
            idea = optimizer.withAdjustedConfidence(idea);

            if (idea.probability < MIN_CONF)
                continue;

            if (!core.allowSignal(idea))
                continue;

            core.registerSignal(idea);

            result.add(idea);
        }

        result.sort(Comparator.comparingDouble(
                (com.bot.DecisionEngineMerged.TradeIdea i) -> i.probability
        ).reversed());

        int topN = Math.min(15, result.size());
        return result.subList(0, topN);
    }

    public com.bot.SignalOptimizer getOptimizer() {
        return optimizer;
    }

    public com.bot.InstitutionalSignalCore getSignalCore() {
        return core;
    }

    public com.bot.PumpHunter getPumpHunter() {
        return pumpHunter;
    }

    public double getAtr(String symbol) {
        List<com.bot.TradingCore.Candle> candles = histM15.getOrDefault(symbol, Collections.emptyList());
        return atr(candles, 14);
    }

    private final Map<String, List<Signal>> signalHistory = new ConcurrentHashMap<>();
    private long dailyResetTs = System.currentTimeMillis();
    private ScheduledExecutorService scheduler;

    public SignalSender(com.bot.TelegramBotSender bot) {

        this.bot = bot;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        this.TOP_N = envInt("TOP_N", 100);
        this.MIN_CONF = 0.50;
        this.INTERVAL_MIN = envInt("INTERVAL_MINUTES", 15);
        this.KLINES_LIMIT = envInt("KLINES", 220);
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

        this.decisionEngine = new com.bot.DecisionEngineMerged();
        this.adaptiveBrain = new com.bot.TradingCore.AdaptiveBrain();
        this.optimizer = new com.bot.SignalOptimizer(this.tickPriceDeque);
        this.globalImpulse = new com.bot.GlobalImpulseController();
        this.pumpHunter = new com.bot.PumpHunter();

        this.decisionEngine.setPumpHunter(this.pumpHunter);
        this.optimizer.setPumpHunter(this.pumpHunter);

        System.out.println("[SignalSender] INIT: TOP_N=" + TOP_N + " MIN_CONF=" + MIN_CONF +
                " INTERVAL_MIN=" + INTERVAL_MIN + " PumpHunter=ENABLED");

    }

    private void refreshAllFundingRates() {
        try {
            System.out.println("[FundingRate] Refreshing funding rates...");

            String url = "https://fapi.binance.com/fapi/v1/premiumIndex";
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .GET().build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            JSONArray arr = new JSONArray(resp.body());

            Map<String, Double> fundingRates = new HashMap<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject obj = arr.getJSONObject(i);
                String symbol = obj.getString("symbol");
                double fundingRate = obj.optDouble("lastFundingRate", 0);
                fundingRates.put(symbol, fundingRate);
            }

            for (String pair : cachedPairs) {
                try {
                    fetchAndUpdateOpenInterest(pair, fundingRates.getOrDefault(pair, 0.0));
                    Thread.sleep(50);
                } catch (Exception e) {
                    // continue
                }
            }

            System.out.println("[FundingRate] Updated " + fundingRates.size() + " pairs");

        } catch (Exception e) {
            System.out.println("[FundingRate] Error: " + e.getMessage());
        }
    }

    private void fetchAndUpdateOpenInterest(String symbol, double fundingRate) {
        try {
            String oiUrl = "https://fapi.binance.com/fapi/v1/openInterest?symbol=" + symbol;
            HttpRequest oiReq = HttpRequest.newBuilder()
                    .uri(URI.create(oiUrl))
                    .timeout(Duration.ofSeconds(5))
                    .GET().build();

            HttpResponse<String> oiResp = http.send(oiReq, HttpResponse.BodyHandlers.ofString());
            JSONObject oiJson = new JSONObject(oiResp.body());
            double currentOI = oiJson.optDouble("openInterest", 0);

            String histUrl = "https://fapi.binance.com/futures/data/openInterestHist?symbol=" + symbol +
                    "&period=1h&limit=5";
            HttpRequest histReq = HttpRequest.newBuilder()
                    .uri(URI.create(histUrl))
                    .timeout(Duration.ofSeconds(5))
                    .GET().build();

            HttpResponse<String> histResp = http.send(histReq, HttpResponse.BodyHandlers.ofString());
            JSONArray histArr = new JSONArray(histResp.body());

            double oiChange1h = 0;
            double oiChange4h = 0;

            if (histArr.length() >= 2) {
                double oi1hAgo = histArr.getJSONObject(histArr.length() - 2).optDouble("sumOpenInterest", currentOI);
                oiChange1h = ((currentOI - oi1hAgo) / oi1hAgo) * 100;
            }

            if (histArr.length() >= 5) {
                double oi4hAgo = histArr.getJSONObject(0).optDouble("sumOpenInterest", currentOI);
                oiChange4h = ((currentOI - oi4hAgo) / oi4hAgo) * 100;
            }

            decisionEngine.updateFundingOI(symbol, fundingRate, currentOI, oiChange1h, oiChange4h);

        } catch (Exception e) {
            decisionEngine.updateFundingOI(symbol, fundingRate, 0, 0, 0);
        }
    }

    private int envInt(String k, int def) {
        try {
            return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(def)));
        } catch (Exception e) {
            return def;
        }
    }

    private String getLocalTimeString() {
        LocalDateTime now = LocalDateTime.now();
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

    private boolean wasSameDirectionRecently(String pair, String direction, int lastNCycles) {
        List<Signal> history = signalHistory.getOrDefault(pair, Collections.emptyList());
        if (history.isEmpty()) return false;

        int count = 0;
        for (int i = history.size() - 1; i >= 0 && count < lastNCycles; i--, count++) {
            if (history.get(i).direction.equals(direction)) {
                return true;
            }
        }
        return false;
    }

    private boolean isRecentOppositeSignal(String pair, String direction, int lastNCycles) {
        List<Signal> history = signalHistory.getOrDefault(pair, Collections.emptyList());
        if (history.isEmpty()) return false;

        int count = 0;
        for (int i = history.size() - 1; i >= 0 && count < lastNCycles; i--, count++) {
            if (!history.get(i).direction.equals(direction)) {
                return true;
            }
        }
        return false;
    }

    public CompletableFuture<List<com.bot.TradingCore.Candle>> fetchKlinesAsync(String symbol, String interval, int limit) {
        try {
            String url = String.format(
                    "https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
                    symbol, interval, limit
            );
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            return http.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                    .thenApply((HttpResponse<String> resp) -> {
                        String body = resp.body();
                        if (!body.trim().startsWith("[")) {
                            System.out.println("[Binance] Unexpected response for " + symbol + ": " + body);
                            return Collections.<com.bot.TradingCore.Candle>emptyList();
                        }
                        JSONArray arr = new JSONArray(body);
                        List<com.bot.TradingCore.Candle> list = new ArrayList<>();
                        for (int i = 0; i < arr.length(); i++) {
                            JSONArray k = arr.getJSONArray(i);
                            long openTime = k.getLong(0);
                            double open  = Double.parseDouble(k.getString(1));
                            double high  = Double.parseDouble(k.getString(2));
                            double low   = Double.parseDouble(k.getString(3));
                            double close = Double.parseDouble(k.getString(4));
                            double vol   = Double.parseDouble(k.getString(5));
                            double qvol  = k.length() > 7 ? Double.parseDouble(k.getString(7)) : 0.0;
                            long closeTime = k.getLong(6);

                            list.add(new com.bot.TradingCore.Candle(openTime, open, high, low, close, vol, qvol, closeTime));
                        }
                        return list;
                    })
                    .exceptionally(e -> {
                        System.out.println("[Binance] Error fetching klines for " + symbol + ": " + e.getMessage());
                        return Collections.<com.bot.TradingCore.Candle>emptyList();
                    });

        } catch (Exception e) {
            System.out.println("[Binance] Error preparing klines request for " + symbol + ": " + e.getMessage());
            return CompletableFuture.completedFuture(Collections.<com.bot.TradingCore.Candle>emptyList());
        }
    }

    public static class MicroCandleBuilder {
        private final int intervalMs;
        private long currentBucketStart = -1;
        private double open = Double.NaN, high = Double.NEGATIVE_INFINITY, low = Double.POSITIVE_INFINITY, close = Double.NaN;
        private double volume = 0.0;
        private long closeTime = -1;

        public MicroCandleBuilder(int intervalMs) {
            this.intervalMs = intervalMs;
        }

        public Optional<com.bot.TradingCore.Candle> addTick(long tsMillis, double price, double qty) {
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
                com.bot.TradingCore.Candle c = new com.bot.TradingCore.Candle(currentBucketStart, open, high, low, close, volume, volume, closeTime);
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

    public static double atr(List<com.bot.TradingCore.Candle> candles, int period) {
        if (candles == null || candles.size() <= period) return 0.0;
        List<Double> trs = new ArrayList<>();
        for (int i = 1; i < candles.size(); i++) {
            com.bot.TradingCore.Candle prev = candles.get(i - 1);
            com.bot.TradingCore.Candle cur = candles.get(i);
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

    public static double momentumPct(List<Double> prices, int n) {
        if (prices == null || prices.size() <= n) return 0.0;
        double last = prices.get(prices.size() - 1);
        double prev = prices.get(prices.size() - 1 - n);
        return (last - prev) / (prev + 1e-12);
    }

    public static double vwap(List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.isEmpty()) return 0.0;
        double pv = 0.0, vol = 0.0;
        for (com.bot.TradingCore.Candle c : candles) {
            double tp = (c.high + c.low + c.close) / 3.0;
            pv += tp * c.volume;
            vol += c.volume;
        }
        if (vol == 0) return candles.get(candles.size() - 1).close;
        return pv / vol;
    }

    public static List<Integer> detectSwingHighs(List<com.bot.TradingCore.Candle> candles, int leftRight) {
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

    public static List<Integer> detectSwingLows(List<com.bot.TradingCore.Candle> candles, int leftRight) {
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

    public static int marketStructure(List<com.bot.TradingCore.Candle> candles) {
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
            List<com.bot.TradingCore.Candle> c15m,
            MicroTrendResult micro
    ) {
        int structure = marketStructure(c15m);
        if (structure == 0) {
            return MarketPhase.NO_TRADE;
        }

        List<Double> closes = c15m.stream().map(c -> c.close).toList();

        double atr = atr(c15m, 14);
        if (atr <= 0) return MarketPhase.NO_TRADE;

        double ema20 = ema(closes, 20);
        double ema50 = ema(closes, 50);

        double trendStrength = Math.abs(ema20 - ema50) / atr;

        if (trendStrength > 0.6 && trendStrength < 1.2) {
            if (Math.abs(micro.accel) < 0.0003) {
                return MarketPhase.TREND_CONTINUATION;
            }
        }

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

    public static boolean detectBOS(List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 10) return false;
        List<Integer> highs = detectSwingHighs(candles, 3);
        List<Integer> lows = detectSwingLows(candles, 3);
        if (highs.size() < 2 && lows.size() < 2) return false;
        com.bot.TradingCore.Candle last = candles.get(candles.size() - 1);
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

    public static boolean detectLiquiditySweep(List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 6) return false;
        int n = candles.size();
        com.bot.TradingCore.Candle last = candles.get(n - 1);
        com.bot.TradingCore.Candle prev = candles.get(n - 2);
        double upperWick = last.high - Math.max(last.open, last.close);
        double lowerWick = Math.min(last.open, last.close) - last.low;
        double body = Math.abs(last.close - last.open);
        if (upperWick > body * 1.8 && last.close < prev.close) return true;
        if (lowerWick > body * 1.8 && last.close > prev.close) return true;
        return false;
    }

    private int emaDirection(List<com.bot.TradingCore.Candle> candles, int fast, int slow) {
        if (candles == null || candles.size() < Math.max(slow + 5, 60)) return 0;

        List<Double> closes = candles.subList(candles.size() - 60, candles.size())
                .stream()
                .map(c -> c.close)
                .collect(Collectors.toList());

        double emaFast = ema(closes, fast);
        double emaSlow = ema(closes, slow);
        return Double.compare(emaFast, emaSlow);
    }

    private int getBtcTrend() {
        try {
            List<com.bot.TradingCore.Candle> btcH1 = fetchKlines("BTCUSDT", "1h", 120);
            if (btcH1.size() < 60) return 0;

            return emaDirection(btcH1, 20, 50);
        } catch (Exception e) {
            return 0;
        }
    }

    private int getBtc2HTrend() {
        try {
            List<com.bot.TradingCore.Candle> btcH2 = fetchKlines("BTCUSDT", "2h", 80);
            if (btcH2.size() < 40) return 0;

            return emaDirection(btcH2, 12, 26);
        } catch (Exception e) {
            return 0;
        }
    }

    private double strategyEMANorm(List<Double> closes) {
        if (closes == null || closes.size() < 100) return 0.0;
        double e20 = ema(closes, 20);
        double e50 = ema(closes, 50);
        double e100 = ema(closes, 100);
        double a = (e20 - e50) / (e50 + 1e-12);
        double b = (e50 - e100) / (e100 + 1e-12);
        double combined = (a + b) / 2.0;
        return Math.max(-1.0, Math.min(1.0, combined));
    }

    private double strategyRSINorm(List<Double> closes) {
        double r = rsi(closes, 14);
        double score = (r - 50) / 50.0;
        return Math.max(-1.0, Math.min(1.0, score));
    }

    private double holdProbability5Bars(
            List<com.bot.TradingCore.Candle> c15m,
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
        if (score > 1) return 1;
        if (score < -1) return -1;
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

        conf += rawScore * 0.55;

        conf += mtfConfirm * 0.05;

        conf += volOk ? 0.05 : -0.02;
        conf += atrOk ? 0.05 : -0.02;
        conf += vwapAligned ? 0.03 : 0;
        conf += structureAligned ? 0.04 : -0.01;
        if (impulse && Math.abs(rawScore) > 0.25) conf += 0.04;
        else if (!impulse) conf -= 0.02;

        conf += bos ? 0.05 : 0;

        if (liquiditySweep && rawScore * mtfConfirm < 0) conf += 0.10;
        else if (liquiditySweep) conf -= 0.05;

        conf = Math.max(0.20, Math.min(0.90, conf));

        return conf;
    }

    private double lastSwingLow(List<com.bot.TradingCore.Candle> candles) {
        int lookback = Math.min(20, candles.size());
        double low = Double.POSITIVE_INFINITY;
        for (int i = candles.size() - lookback; i < candles.size(); i++)
            low = Math.min(low, candles.get(i).low);
        return low;
    }

    private double lastSwingHigh(List<com.bot.TradingCore.Candle> candles) {
        int lookback = Math.min(20, candles.size());
        double high = Double.NEGATIVE_INFINITY;
        for (int i = candles.size() - lookback; i < candles.size(); i++)
            high = Math.max(high, candles.get(i).high);
        return high;
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
            String flags = (strongTrigger ? "strong " : "") +
                    (atrBreakLong ? "ATR_UP " : "") +
                    (atrBreakShort ? "ATR_DOWN " : "") +
                    (impulse ? "IMPULSE " : "");

            String timeStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

            return String.format("*%s* -> *%s*\n" +
                            "Probability: *%.0f%%*\n" +
                            "Price: %.8f\n" +
                            "SL: %.8f\n" +
                            "TP: %.8f\n" +
                            "RSI(14): %.2f | RSI7: %.2f | RSI4: %.2f\n" +
                            "_flags_: %s\n" +
                            "_time: %s_",
                    symbol,
                    direction,
                    confidence * 100.0,
                    price,
                    stop != null ? stop : 0.0,
                    take != null ? take : 0.0,
                    rsi,
                    rsi7,
                    rsi4,
                    flags.trim(),
                    timeStr
            );
        }
    }

    private final Map<String, Map<String, Long>> lastSignalTimeDir = new ConcurrentHashMap<>();
    private final Map<String, String> lastDirection = new ConcurrentHashMap<>();

    private boolean isCooldown(String pair, Signal s, long candleCloseTime) {
        Map<String, Long> dirMap = lastSignalTimeDir.computeIfAbsent(pair, k -> new ConcurrentHashMap<>());
        Long lastCloseTime = dirMap.get(s.direction);
        return lastCloseTime != null && candleCloseTime - lastCloseTime < 15 * 60_000;
    }

    private void markSignalSent(String pair, String direction, long candleCloseTime) {
        lastSignalTimeDir.computeIfAbsent(pair, k -> new ConcurrentHashMap<>())
                .put(direction, candleCloseTime);
    }

    public void connectTickWebSocket(String pair) {
        wsWatcher.scheduleAtFixedRate(() -> {
            if (!wsMap.containsKey(pair) || wsMap.get(pair).isInputClosed()) {
                connectWsInternal(pair);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private Signal analyzePair(String pair, List<com.bot.TradingCore.Candle> c15m, List<com.bot.TradingCore.Candle> c1h) {
        if(c15m.size()<60 || c1h.size()<60) return null;

        com.bot.TradingCore.Candle last = c15m.get(c15m.size() - 1);
        List<Double> closes15 = c15m.stream().map(c -> c.close).toList();
        List<Double> closes1h = c1h.stream().map(c -> c.close).toList();

        MicroTrendResult micro = computeMicroTrend(pair, tickPriceDeque.get(pair));
        double emaScore  = strategyEMANorm(closes15);
        double rsiScore  = strategyRSINorm(closes15);
        double momScore  = strategyMomentumNorm(closes15);
        double macdScore = strategyMACDNorm(closes15);
        double microBias = Math.tanh(micro.speed * 500);

        double rawScore = emaScore*0.25 + rsiScore*0.15 + momScore*0.20 + macdScore*0.15 + microBias*0.25;
        rawScore = Math.max(-1.0, Math.min(1.0, rawScore));

        int mtfConfirm = multiTFConfirm(emaDirection(c1h,20,50), emaDirection(c15m,20,50));
        boolean volOk = computeVolatilityOk(c15m, atr(c15m,14));
        boolean atrOk = atr(c15m,14)/last.close > ATR_MIN_PCT;
        boolean impulse = Math.abs(micro.speed) > 0.0005;
        boolean vwapAligned = checkVWAPAlignment(c15m,last.close);
        boolean structureAligned = checkStructureAlignment(c15m, rawScore>=0 ? 1 : -1);
        boolean bos = detectBOS(c15m);
        boolean sweep = detectLiquiditySweep(c15m);

        double confidence = composeConfidence(rawScore, mtfConfirm, volOk, atrOk, impulse, vwapAligned, structureAligned, bos, sweep);

        double atrValue = atr(c15m,14);
        double entry = last.close;
        double risk = atrValue*1.2;
        double stop = rawScore>=0 ? entry-risk : entry+risk;
        double take = rawScore>=0 ? entry+risk*2.4 : entry-risk*2.4;

        Signal s = new Signal(
                pair,
                rawScore>=0 ? "LONG" : "SHORT",
                confidence,
                entry,
                rsi(closes15,14),
                rawScore,
                mtfConfirm,
                volOk,
                atrOk,
                Math.abs(rawScore)>0.6,
                rawScore>=0 && entry>stop,
                rawScore<0 && entry<stop,
                impulse,
                rsi(closes15,7),
                rsi(closes15,4)
        );

        s.stop = stop;
        s.take = take;
        return s;
    }

    private void connectWsInternal(String pair) {
        try {
            final String symbol = pair.toLowerCase();
            final String url = "wss://fstream.binance.com/ws/" + symbol + "@aggTrade";

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

                                // ✅ ИСПРАВЛЕНО: вызываем ВНЕШНИЙ метод класса
                                com.bot.DecisionEngineMerged.TradeIdea earlySignal =
                                        generateEarlyTickSignal(pair, price, tickPriceDeque.get(pair));

                                if (earlySignal != null && earlySignal.probability > 65) {
                                    System.out.println("[EARLY_TICK] " + earlySignal.symbol + " " + earlySignal.side +
                                            " prob=" + earlySignal.probability);
                                    // ✅ ПРИМЕНЯЕМ ФИЛЬТРЫ
                                    if (filterEarlySignal(earlySignal)) {
                                        bot.sendMessageAsync(earlySignal.toString());
                                    }
                                }

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

                    })
                    .thenAccept(ws -> {
                        wsMap.put(pair, ws);
                        System.out.println("[WS] Connected " + pair);
                    })
                    .exceptionally(ex -> {
                        System.out.println("[WS] Failed connect " + pair + " retry in 5s: " + ex.getMessage());
                        wsWatcher.schedule(() -> connectWsInternal(pair), 5, TimeUnit.SECONDS);
                        return null;
                    });

        } catch (Exception e) {
            System.out.println("[WS] Failed connect " + pair + " retry in 5s: " + e.getMessage());
            reconnect(pair);
        }
    }

    // ✅ ВЫНЕСЕНО ВО ВНЕШНИЙ МЕТОД КЛАССА
    private com.bot.DecisionEngineMerged.TradeIdea generateEarlyTickSignal(
            String symbol, double currentPrice, Deque<Double> dq) {

        // УЛУЧШЕНИЕ: Ждем больше тиков для сглаживания и требуем бОльшую скорость.
        if (dq == null || dq.size() < 20) return null; // Было 8, стало 20 (фильтруем сквизы)

        List<Double> buffer = new ArrayList<>(dq);
        int n = buffer.size();

        double move = buffer.get(n-1) - buffer.get(n-20);
        double avgPrice = buffer.stream().mapToDouble(Double::doubleValue).average().orElse(currentPrice);
        double velocity = Math.abs(move) / Math.max(avgPrice, 1e-9);

        double move1 = buffer.get(n/2-1) - buffer.get(0);
        double move2 = buffer.get(n-1) - buffer.get(n/2);
        boolean accelerating = Math.abs(move2) > Math.abs(move1) * 1.5; // Было 1.15, стало 1.5 (строже)

        // Требуем скорости минимум 0.15% за 20 тиков
        boolean fastVelocity = velocity > 0.0015; // Было 0.0007, убрали реакцию на микро-шум
        boolean upMove = move > 0;

        if (fastVelocity && accelerating) {
            List<String> flags = new ArrayList<>();
            flags.add("EARLY_TICK");
            flags.add(upMove ? "UP_PUMP" : "DOWN_DUMP");
            flags.add("velocity=" + String.format("%.2e", velocity));

            // Захват текущего ATR, если он есть, иначе дефолт
            double atr = getAtr(symbol);
            if (atr <= 0) atr = currentPrice * 0.005;

            double stopDist = atr * 1.5;
            double takeDist = atr * 3.0;

            double stop = upMove ? currentPrice - stopDist : currentPrice + stopDist;
            double take = upMove ? currentPrice + takeDist : currentPrice - takeDist;

            // Смягчили безумную прибавку к вероятности (+15000 * velocity было ошибкой)
            double confidence = 55 + (velocity * 5000);

            return new com.bot.DecisionEngineMerged.TradeIdea(
                    symbol,
                    upMove ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT,
                    currentPrice,
                    stop,
                    take,
                    Math.min(75, confidence),
                    flags
            );
        }

        return null;
    }

    // ✅ НОВЫЙ МЕТОД: применяем все фильтры к раннему сигналу
    private boolean filterEarlySignal(com.bot.DecisionEngineMerged.TradeIdea signal) {

        // Фильтр 1: GlobalImpulse (BTC контекст)
        GlobalImpulseController.GlobalContext ctx = getLatestBTCContext();
        if (ctx != null) {
            double coeff = globalImpulse.filterSignal(signal);
            if (coeff <= 0.05) {
                System.out.println("[EARLY] GlobalImpulse BLOCKED: " + signal.symbol);
                return false;
            }
        }

        // Фильтр 2: Institutional risk check
        if (!core.allowSignal(signal)) {
            System.out.println("[EARLY] ISC REJECTED: " + signal.symbol);
            return false;
        }

        // Фильтр 3: SignalOptimizer exhaustion check
        double adjustedConf = optimizer.adjustConfidence(signal);
        if (adjustedConf < 55) {
            System.out.println("[EARLY] Exhaustion filter blocked: " + signal.symbol);
            return false;
        }

        return true;
    }

    // ✅ КЕШИРУЕМ последний BTC контекст
    private GlobalImpulseController.GlobalContext cachedBTCContext = null;
    private long lastBTCContextUpdate = 0;

    private GlobalImpulseController.GlobalContext getLatestBTCContext() {
        // Если кеш свежий (< 1 мин), используем его
        if (System.currentTimeMillis() - lastBTCContextUpdate < 60_000) {
            return cachedBTCContext;
        }

        // Иначе обновляем
        try {
            List<com.bot.TradingCore.Candle> btcCandles = fetchKlines("BTCUSDT", "15m", 200);
            if (btcCandles != null && btcCandles.size() > 20) {
                GlobalImpulseController gic = new GlobalImpulseController();
                gic.update(btcCandles);
                cachedBTCContext = gic.getContext();
                lastBTCContextUpdate = System.currentTimeMillis();
                return cachedBTCContext;
            }
        } catch (Exception e) {
            System.out.println("[BTC] Error updating context: " + e.getMessage());
        }

        return cachedBTCContext;  // вернём старый кеш если ошибка
    }

    private void reconnect(String pair) {
        wsWatcher.schedule(() -> connectWsInternal(pair), 5, TimeUnit.SECONDS);
    }

    private MicroTrendResult computeMicroTrend(String pair, Deque<Double> dq) {
        if(dq==null || dq.size()<5) return new MicroTrendResult(0,0,0);
        List<Double> arr = new ArrayList<>(dq);
        int n = Math.min(arr.size(), 10);
        double alpha=0.5;
        double speed=0;
        for(int i=arr.size()-n+1;i<arr.size();i++){
            double diff=arr.get(i)-arr.get(i-1);
            speed=alpha*diff + (1-alpha)*speed;
        }
        double accel=0;
        if(arr.size()>=3){
            double lastDiff = arr.get(arr.size()-1)-arr.get(arr.size()-2);
            double prevDiff = arr.get(arr.size()-2)-arr.get(arr.size()-3);
            accel = alpha*(lastDiff-prevDiff)+(1-alpha)*accel;
        }
        double avg = arr.stream().mapToDouble(Double::doubleValue).average().orElse(arr.get(arr.size()-1));
        return new MicroTrendResult(speed,accel,avg);
    }

    private boolean isVolumeStrong(String pair, double lastPrice) {
        OrderbookSnapshot obs = orderbookMap.get(pair);
        if (obs == null) return false;

        double obi = Math.abs(obs.obi());
        return obi > OBI_THRESHOLD;
    }

    public List<com.bot.TradingCore.Candle> fetchKlines(String symbol, String interval, int limit) {
        try {
            List<com.bot.TradingCore.Candle> candles =
                    fetchKlinesAsync(symbol, interval, limit)
                            .get(12, TimeUnit.SECONDS);

            if (candles.isEmpty()) {
                System.out.println("[KLINES] empty " + symbol + " " + interval);
            }

            return candles;

        } catch (TimeoutException e) {
            System.out.println("[TIMEOUT] " + symbol + " " + interval);
            return Collections.emptyList();

        } catch (Exception e) {
            System.out.println("[fetchKlines] error for " + symbol + " " + interval + ": " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public Set<String> getTopSymbolsSet(int limit) {
        try {
            Set<String> binancePairs = getBinanceSymbolsFutures();

            String url = "https://api.coingecko.com/api/v3/coins/markets" +
                    "?vs_currency=usd&order=market_cap_desc&per_page=250&page=1";
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            JSONArray arr = new JSONArray(resp.body());

            Set<String> topPairs = new LinkedHashSet<>();
            for (int i = 0; i < arr.length(); i++) {
                String sym = arr.getJSONObject(i).getString("symbol").toUpperCase();
                if (Set.of("USDT", "USDC", "BUSD").contains(sym)) continue;
                String pair = sym + "USDT";
                if (binancePairs.contains(pair)) {
                    topPairs.add(pair);
                }
                if (topPairs.size() >= limit) break;
            }

            if (topPairs.size() < limit) {
                for (String p : binancePairs) {
                    if (topPairs.size() >= limit) break;
                    topPairs.add(p);
                }
            }

            System.out.println("[PAIRS] Loaded TOP " + topPairs.size() + " USDT pairs (real pairs only)");
            return topPairs;

        } catch (Exception e) {
            System.out.println("[PAIRS] ERROR fetching top symbols: " + e.getMessage());
            return Set.of("BTCUSDT", "ETHUSDT");
        }
    }

    private List<com.bot.TradingCore.Candle> aggregate(List<com.bot.TradingCore.Candle> base, int factor) {
        List<com.bot.TradingCore.Candle> res = new ArrayList<>();
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
                com.bot.TradingCore.Candle c = base.get(j);
                high = Math.max(high, c.high);
                low = Math.min(low, c.low);
                volume += c.volume;
                qvol += c.qvol;
            }

            res.add(new com.bot.TradingCore.Candle(openTime, open, high, low, close, volume, qvol, closeTime));
        }
        return res;
    }

    private boolean computeVolatilityOk(List<com.bot.TradingCore.Candle> candles, double atr) {
        return atr / candles.get(candles.size() - 1).close > 0.0015;
    }

    private boolean checkVWAPAlignment(List<com.bot.TradingCore.Candle> candles, double price) {
        if (candles == null || candles.isEmpty()) return true;
        double v = vwap(candles);
        return price > v * 0.995 && price < v * 1.005;
    }

    private boolean checkStructureAlignment(List<com.bot.TradingCore.Candle> candles, int dir) {
        int structure = marketStructure(candles);
        return structure == dir;
    }

    private void runScheduleCycle() {
        try {
            signalsThisCycle = 0;

            if (System.currentTimeMillis() - dailyResetTs > 24 * 60 * 60_000L) {
                dailyRequests.set(0);
                dailyResetTs = System.currentTimeMillis();
            }

            if (cachedPairs == null || cachedPairs.isEmpty() ||
                    System.currentTimeMillis() - lastBinancePairsRefresh > BINANCE_REFRESH_INTERVAL_MS) {

                cachedPairs = getTopSymbolsSet(TOP_N);
                lastBinancePairsRefresh = System.currentTimeMillis();
            }

            if (System.currentTimeMillis() - lastFundingRefresh > FUNDING_REFRESH_INTERVAL_MS) {
                refreshAllFundingRates();
                lastFundingRefresh = System.currentTimeMillis();
            }

            com.bot.DecisionEngineMerged engine = decisionEngine;

            int btcTrend = getBtcTrend();
            int btc2hTrend = getBtc2HTrend();

            for (String pair : cachedPairs) {

                List<com.bot.TradingCore.Candle> c1  = fetchKlines(pair, "1m", KLINES_LIMIT);
                List<com.bot.TradingCore.Candle> c5  = fetchKlines(pair, "5m", KLINES_LIMIT);
                List<com.bot.TradingCore.Candle> c15 = fetchKlines(pair, "15m", KLINES_LIMIT);
                List<com.bot.TradingCore.Candle> h1  = fetchKlines(pair, "1h", KLINES_LIMIT);
                List<com.bot.TradingCore.Candle> h2  = fetchKlines(pair, "2h", 100);

                if (c1.size() < 60 || c5.size() < 60 || c15.size() < 60 || h1.size() < 60) continue;

                c1.remove(c1.size() - 1);
                c5.remove(c5.size() - 1);
                c15.remove(c15.size() - 1);
                h1.remove(h1.size() - 1);
                h2.remove(h2.size() - 1);

                optimizer.updateFromCandles(pair, c15);

                com.bot.DecisionEngineMerged.TradeIdea idea =
                        engine.analyze(pair, c1, c5, c15, h1, h2, com.bot.DecisionEngineMerged.CoinCategory.TOP);

                if (idea == null || idea.probability < MIN_CONF) continue;

                PumpHunter.PumpEvent pump = pumpHunter.detectPump(pair, c1, c5, c15);
                if (pump != null && pump.strength > 0.5) {
                    boolean pumpAligned = (idea.side == com.bot.TradingCore.Side.LONG && pump.isBullish()) ||
                            (idea.side == com.bot.TradingCore.Side.SHORT && pump.isBearish());
                    if (pumpAligned) {
                        List<String> newFlags = new ArrayList<>(idea.flags);
                        newFlags.add("PUMP_" + pump.type.name());

                        idea = new com.bot.DecisionEngineMerged.TradeIdea(
                                idea.symbol,
                                idea.side,
                                idea.price,
                                idea.stop,
                                idea.take,
                                Math.min(85, idea.probability + pump.strength * 8),
                                newFlags,
                                idea.fundingRate,
                                idea.oiChange,
                                idea.htfBias
                        );
                    }
                }

                double probAdjustment = 0;

                if (btc2hTrend < 0 && idea.side.name().equals("LONG")) {
                    probAdjustment -= 5;
                }
                if (btc2hTrend > 0 && idea.side.name().equals("SHORT")) {
                    probAdjustment -= 5;
                }

                if (btcTrend < 0 && idea.side.name().equals("LONG")) {
                    probAdjustment -= 3;
                }
                if (btcTrend > 0 && idea.side.name().equals("SHORT")) {
                    probAdjustment -= 3;
                }

                if (btc2hTrend > 0 && idea.side.name().equals("LONG")) {
                    probAdjustment += 3;
                }
                if (btc2hTrend < 0 && idea.side.name().equals("SHORT")) {
                    probAdjustment += 3;
                }

                if (probAdjustment != 0) {
                    idea = new com.bot.DecisionEngineMerged.TradeIdea(
                            idea.symbol,
                            idea.side,
                            idea.price,
                            idea.stop,
                            idea.take,
                            idea.probability + probAdjustment,
                            idea.flags,
                            idea.fundingRate,
                            idea.oiChange,
                            idea.htfBias
                    );
                }

                if (idea.probability < MIN_CONF * 100) continue;

                long candleCloseTime = c15.get(c15.size() - 1).closeTime;

                Signal tempSignal = new Signal(
                        idea.symbol,
                        idea.side.name(),
                        idea.probability,
                        idea.price,
                        engine.rsi(c15, 14),
                        Math.max(0.0, idea.probability),
                        1,
                        engine.volumeSpike(c15, com.bot.DecisionEngineMerged.CoinCategory.TOP),
                        engine.atr(c15, 14) > idea.price * 0.001,
                        true,
                        false,
                        false,
                        engine.impulse(c1),
                        engine.rsi(c15, 7),
                        engine.rsi(c15, 4)
                );

                if (isCooldown(pair, tempSignal, candleCloseTime)) continue;
                if (dailyRequests.incrementAndGet() > 5000) continue;

                markSignalSent(pair, idea.side.name(), candleCloseTime);
                signalHistory.computeIfAbsent(pair, k -> new ArrayList<>()).add(tempSignal);

                if (!core.allowSignal(idea)) continue;
                core.registerSignal(idea);

                String fundingInfo = "";
                if (Math.abs(idea.fundingRate) > 0.0001) {
                    fundingInfo = String.format("\nFR: %.4f%%", idea.fundingRate * 100);
                }
                String oiInfo = "";
                if (Math.abs(idea.oiChange) > 0.5) {
                    oiInfo = String.format(" | OI: %+.1f%%", idea.oiChange);
                }
                String biasInfo = "";
                if (!idea.htfBias.equals("NONE")) {
                    biasInfo = String.format("\n2H Bias: %s", idea.htfBias);
                }

                String pumpInfo = "";
                if (pump != null && pump.strength > 0.4) {
                    pumpInfo = String.format("\nPump: %s (%.0f%%)", pump.type.name(), pump.strength * 100);
                }

                String message = String.format(
                        "*%s* -> *%s*\n" +
                                "Price: %.6f\n" +
                                "Stop: %.6f | Take: %.6f\n" +
                                "Probability: *%.0f%%*%s%s%s%s\n" +
                                "Flags: %s\n" +
                                "_time: %s_",
                        idea.symbol,
                        idea.side,
                        idea.price,
                        idea.stop,
                        idea.take,
                        idea.probability,
                        fundingInfo,
                        oiInfo,
                        biasInfo,
                        pumpInfo,
                        idea.flags,
                        LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))
                );

                bot.sendMessageAsync(message);
                signalsThisCycle++;

                if (signalsThisCycle >= TOP_N) break;
            }

            System.out.println("[Scheduler] Cycle completed: signals sent " + signalsThisCycle);

        } catch (Exception e) {
            System.out.println("[Scheduler] Exception in runScheduleCycle: " + e.getMessage());
            e.printStackTrace();
        }
    }
}