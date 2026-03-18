package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  SignalSender — GODBOT PRO EDITION v7.0                                 ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  [FIX-BLIND]  14-минутная слепота устранена — LiveCandleAssembler      ║
 * ║  [FIX-WS]     WebSocket автозапускается для топ-30 пар по объёму       ║
 * ║  [FIX-UDS]    User Data Stream — реальное закрытие ордеров с биржи     ║
 * ║  [FIX-COMP]   Размер позиции масштабируется с балансом $100→$1M        ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public final class SignalSender {

    private final com.bot.TelegramBotSender bot;
    private final HttpClient              http;
    private final com.bot.GlobalImpulseController gic;
    private final com.bot.InstitutionalSignalCore isc;
    private final Object wsLock = new Object();

    private final int    TOP_N;
    private final double MIN_CONF;
    private final int    KLINES_LIMIT;
    private final long   BINANCE_REFRESH_MS;
    private final int    TICK_HISTORY;
    private final double OBI_THRESHOLD;
    private final double DELTA_BLOCK_CONF;

    // API ключи — нужны для UDS и размера позиции
    private final String API_KEY;
    private final String API_SECRET;

    private static final long FUNDING_REFRESH_MS  = 5 * 60_000L;
    private static final long DELTA_WINDOW_MS     = 60_000L;

    private static final double MIN_PROFIT_TOP  = 0.0025;
    private static final double MIN_PROFIT_ALT  = 0.0035;
    private static final double MIN_PROFIT_MEME = 0.0050;

    private static final double MIN_VOL_TOP_USD  = 50_000_000;
    private static final double MIN_VOL_ALT_USD  = 5_000_000;
    private static final double MIN_VOL_MEME_USD = 1_000_000;

    private static final double STOP_CLUSTER_SHIFT = 0.0025;
    private static final int    MAX_WS_CONNECTIONS  = 30;
    private static final long   WS_INITIAL_DELAY_MS = 3_000L;
    private static final long   WS_MAX_DELAY_MS     = 120_000L;

    // Volume Delta
    private final Map<String, Double> deltaBuffer      = new ConcurrentHashMap<>();
    private final Map<String, Long>   deltaWindowStart = new ConcurrentHashMap<>();
    private final Map<String, Double> deltaHistory     = new ConcurrentHashMap<>();

    // Tick / WebSocket
    private final Map<String, Deque<Double>>      tickPriceDeque  = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>      tickVolumeDeque = new ConcurrentHashMap<>();
    private final Map<String, Long>               lastTickTime    = new ConcurrentHashMap<>();
    private final Map<String, Double>             lastTickPrice   = new ConcurrentHashMap<>();
    private final Map<String, WebSocket>          wsMap           = new ConcurrentHashMap<>();
    private final Map<String, Long>               wsReconnectDelay= new ConcurrentHashMap<>();
    private final Map<String, MicroCandleBuilder> microBuilders   = new ConcurrentHashMap<>();

    private final ScheduledExecutorService wsWatcher = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ws-watcher"); t.setDaemon(true); return t;
    });

    // [FIX-UDS] User Data Stream
    private volatile String    udsListenKey   = null;
    private volatile WebSocket udsWebSocket   = null;
    private final ScheduledExecutorService udsExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "uds-listener"); t.setDaemon(true); return t;
    });

    // [FIX-BLIND] Буфер 1m свечей для LiveCandleAssembler
    private final Map<String, List<com.bot.TradingCore.Candle>> liveM1Buffer = new ConcurrentHashMap<>();
    private static final int LIVE_M1_BUFFER_SIZE = 20;

    private final Map<String, Long> lastFetchTime = new ConcurrentHashMap<>();

    // Orderbook
    private final Map<String, OrderbookSnapshot> orderbookMap = new ConcurrentHashMap<>();

    // Candle Cache
    private final Map<String, CachedCandles> candleCache = new ConcurrentHashMap<>();

    private static final Map<String, Long> CACHE_TTL = Map.of(
            "1m",  55_000L,
            "5m",  4  * 60_000L,
            "15m", 14 * 60_000L,   // исторические свечи ок, последняя пересобирается live
            "1h",  59 * 60_000L,
            "2h",  119 * 60_000L
    );

    private static final class CachedCandles {
        final List<com.bot.TradingCore.Candle> candles;
        final long fetchedAt;
        CachedCandles(List<com.bot.TradingCore.Candle> c) {
            this.candles = Collections.unmodifiableList(c);
            this.fetchedAt = System.currentTimeMillis();
        }
        boolean isStale(long ttl) { return System.currentTimeMillis() - fetchedAt > ttl; }
    }

    // Pairs / volumes
    private volatile Set<String>  cachedPairs      = new LinkedHashSet<>();
    private volatile long         lastPairsRefresh = 0L;
    private volatile long         lastFundingRefresh = 0L;
    private final Map<String, Double> volume24hUSD = new ConcurrentHashMap<>();
    private volatile long         lastVolRefresh   = 0L;
    private static final long     VOL_REFRESH_MS   = 30 * 60_000L;

    // [FIX-COMP] Баланс для компаундинга
    private volatile double accountBalance    = 100.0;
    private volatile long   lastBalanceRefresh = 0;

    // Rate limiter
    private static final int  RATE_LIMIT_MAX_WEIGHT = 1800;
    private static final long RATE_LIMIT_WINDOW_MS  = 60_000L;
    private final AtomicInteger usedWeight = new AtomicInteger(0);
    private volatile long weightWindowStart = System.currentTimeMillis();
    private volatile boolean rateLimitPaused = false;

    private void trackWeight(int weight) {
        long now = System.currentTimeMillis();
        if (now - weightWindowStart > RATE_LIMIT_WINDOW_MS) {
            usedWeight.set(0); weightWindowStart = now; rateLimitPaused = false;
        }
        if (usedWeight.addAndGet(weight) > RATE_LIMIT_MAX_WEIGHT && !rateLimitPaused) {
            rateLimitPaused = true;
        }
    }

    private void awaitRateLimit() {
        if (!rateLimitPaused) return;
        long waitMs = RATE_LIMIT_WINDOW_MS - (System.currentTimeMillis() - weightWindowStart);
        if (waitMs > 0) {
            try { Thread.sleep(Math.min(waitMs + 200, RATE_LIMIT_WINDOW_MS)); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        usedWeight.set(0); weightWindowStart = System.currentTimeMillis(); rateLimitPaused = false;
    }

    // RS history
    private final Map<String, Deque<Double>> relStrengthHistory = new ConcurrentHashMap<>();
    private static final int RS_HISTORY = 12;

    // Core
    private final com.bot.DecisionEngineMerged decisionEngine;
    private final com.bot.TradingCore.AdaptiveBrain adaptiveBrain;
    private final com.bot.SignalOptimizer optimizer;
    private final com.bot.PumpHunter pumpHunter;
    private final CorrelationGuard correlationGuard;
    private final ExecutorService fetchPool;

    // Stats
    private final AtomicLong totalFetches   = new AtomicLong(0);
    private final AtomicLong cacheHits      = new AtomicLong(0);
    private final AtomicLong earlySignals   = new AtomicLong(0);
    private final AtomicLong blockedLiq     = new AtomicLong(0);
    private final AtomicLong blockedCorr    = new AtomicLong(0);
    private final AtomicLong blockedProfit  = new AtomicLong(0);
    private final AtomicLong wsMessageCount = new AtomicLong(0);
    private final AtomicLong udsEventsCount = new AtomicLong(0);

    private static final Set<String> STABLE = Set.of("USDT","USDC","BUSD","TUSD","USDP","DAI");

    private static String detectSector(String pair) {
        String s = pair.endsWith("USDT") ? pair.substring(0, pair.length() - 4) : pair;
        return switch (s) {
            case "DOGE","SHIB","PEPE","FLOKI","WIF","BONK","MEME",
                 "NEIRO","POPCAT","COW","MOG","BRETT","TURBO" -> "MEME";
            case "BTC","ETH","BNB","OKB"                      -> "TOP";
            case "SOL","AVAX","NEAR","APT","SUI","ADA","DOT",
                 "ATOM","FTM","ONE","HBAR","VET","THETA"      -> "L1";
            case "MATIC","ARB","OP","IMX","LRC","ZK","METIS"  -> "L2";
            case "UNI","AAVE","CRV","GMX","SNX","COMP","MKR",
                 "SUSHI","YFI","1INCH","DYDX","RUNE","JUP"    -> "DEFI";
            case "LINK","BAND","API3","GRT","FIL","AR","STORJ" -> "INFRA";
            case "XRP","XLM","LTC","BCH","DASH","XMR"         -> "PAYMENT";
            case "AXS","SAND","MANA","ENJ","GALA","GMT"       -> "GAMING";
            case "FET","AGIX","OCEAN","RNDR","WLD","TAO"      -> "AI";
            default -> null;
        };
    }

    // ══════════════════════════════════════════════════════════════
    //  CONSTRUCTOR
    // ══════════════════════════════════════════════════════════════

    public SignalSender(com.bot.TelegramBotSender bot,
                        com.bot.GlobalImpulseController sharedGIC,
                        com.bot.InstitutionalSignalCore sharedISC) {
        this.bot = bot;
        this.gic = sharedGIC;
        this.isc = sharedISC;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(12))
                .version(HttpClient.Version.HTTP_2)
                .build();

        this.API_KEY    = System.getenv().getOrDefault("BINANCE_API_KEY", "");
        this.API_SECRET = System.getenv().getOrDefault("BINANCE_API_SECRET", "");

        this.TOP_N            = envInt("TOP_N", 100);
        this.MIN_CONF         = envDouble("MIN_CONF", 50.0);
        this.KLINES_LIMIT     = envInt("KLINES", 220);
        this.BINANCE_REFRESH_MS = envLong("BINANCE_REFRESH_MINUTES", 60) * 60_000L;
        this.TICK_HISTORY     = envInt("TICK_HISTORY", 120);
        this.OBI_THRESHOLD    = envDouble("OBI_THRESHOLD", 0.26);
        this.DELTA_BLOCK_CONF = envDouble("DELTA_BLOCK_CONF", 73.0);

        this.decisionEngine   = new com.bot.DecisionEngineMerged();
        this.adaptiveBrain    = new com.bot.TradingCore.AdaptiveBrain();
        this.optimizer        = new com.bot.SignalOptimizer(this.tickPriceDeque);
        this.pumpHunter       = new com.bot.PumpHunter();
        this.correlationGuard = new CorrelationGuard();

        this.decisionEngine.setPumpHunter(this.pumpHunter);
        this.decisionEngine.setGIC(this.gic);
        this.optimizer.setPumpHunter(this.pumpHunter);

        int poolSize = Math.max(6, Math.min(TOP_N / 4, 25));
        this.fetchPool = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "fetch-" + r.hashCode());
            t.setDaemon(true); return t;
        });

        // [FIX-UDS] User Data Stream
        if (!API_KEY.isBlank()) {
            udsExecutor.schedule(this::initUserDataStream, 5, TimeUnit.SECONDS);
            udsExecutor.scheduleAtFixedRate(this::renewListenKey, 28, 28, TimeUnit.MINUTES);
            wsWatcher.scheduleAtFixedRate(this::refreshAccountBalance, 10, 120, TimeUnit.SECONDS);
        }

        // WS health check
        wsWatcher.scheduleAtFixedRate(this::checkWsHealth, 30, 30, TimeUnit.SECONDS);

        System.out.printf("[SignalSender v7.0] TOP_N=%d POOL=%d LIVE_CANDLE=ON WS_AUTO=ON UDS=%s BALANCE_TRACK=%s%n",
                TOP_N, poolSize,
                !API_KEY.isBlank() ? "ON" : "OFF",
                !API_KEY.isBlank() ? "ON" : "MANUAL");
    }

    // ══════════════════════════════════════════════════════════════
    //  GENERATE SIGNALS
    // ══════════════════════════════════════════════════════════════

    public List<com.bot.DecisionEngineMerged.TradeIdea> generateSignals() {

        if (cachedPairs.isEmpty() ||
                System.currentTimeMillis() - lastPairsRefresh > BINANCE_REFRESH_MS) {
            Set<String> fresh = getTopSymbolsSet(TOP_N);
            if (!fresh.isEmpty()) {
                startWebSocketsForTopPairs(fresh); // [FIX-WS]
                cachedPairs = fresh;
                lastPairsRefresh = System.currentTimeMillis();
            }
        }

        if (System.currentTimeMillis() - lastFundingRefresh > FUNDING_REFRESH_MS) {
            refreshAllFundingRates();
            lastFundingRefresh = System.currentTimeMillis();
        }

        if (System.currentTimeMillis() - lastVolRefresh > VOL_REFRESH_MS) {
            refreshVolume24h();
            lastVolRefresh = System.currentTimeMillis();
        }

        correlationGuard.resetCycle();

        try {
            List<com.bot.TradingCore.Candle> btc5m = getCached("BTCUSDT", "5m", 30);
            if (btc5m != null && btc5m.size() >= 10) gic.updateFast(btc5m);
        } catch (Exception ignored) {}

        List<CompletableFuture<com.bot.DecisionEngineMerged.TradeIdea>> futures = new ArrayList<>();
        for (String pair : cachedPairs) {
            futures.add(CompletableFuture.supplyAsync(() -> processPair(pair), fetchPool));
        }

        List<com.bot.DecisionEngineMerged.TradeIdea> result = new ArrayList<>();
        for (CompletableFuture<com.bot.DecisionEngineMerged.TradeIdea> f : futures) {
            try {
                com.bot.DecisionEngineMerged.TradeIdea idea = f.get(18, TimeUnit.SECONDS);
                if (idea != null) result.add(idea);
            } catch (Exception ignored) {}
        }

        result.sort(Comparator.comparingDouble(
                (com.bot.DecisionEngineMerged.TradeIdea i) -> i.probability).reversed());

        logCycleStats();
        return result;
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-WS] АВТОЗАПУСК WEBSOCKET
    // ══════════════════════════════════════════════════════════════

    private void startWebSocketsForTopPairs(Set<String> pairs) {
        List<String> sorted = new ArrayList<>(pairs);
        sorted.sort((a, b) -> Double.compare(
                volume24hUSD.getOrDefault(b, 0.0),
                volume24hUSD.getOrDefault(a, 0.0)));

        int connected = 0;
        for (String pair : sorted) {
            if (connected >= MAX_WS_CONNECTIONS) break;
            if (!wsMap.containsKey(pair)) {
                connectWsInternal(pair);
                connected++;
                try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            } else {
                connected++;
            }
        }
        System.out.printf("[WS] Active: %d/%d (pairs: %d)%n", wsMap.size(), MAX_WS_CONNECTIONS, pairs.size());
    }

    // ══════════════════════════════════════════════════════════════
    //  PROCESS PAIR
    // ══════════════════════════════════════════════════════════════

    private com.bot.DecisionEngineMerged.TradeIdea processPair(String pair) {
        try {
            List<com.bot.TradingCore.Candle> m1  = getCached(pair, "1m",  KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> m5  = getCached(pair, "5m",  KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> m15 = getCached15mWithLive(pair); // [FIX-BLIND]
            List<com.bot.TradingCore.Candle> h1  = getCached(pair, "1h",  KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> h2  = getCached(pair, "2h",  120);

            if (m1 != null && !m1.isEmpty()) updateLiveM1Buffer(pair, m1);

            if (m15 == null || m15.size() < 160 || h1 == null || h1.size() < 160) return null;

            com.bot.DecisionEngineMerged.CoinCategory cat = categorizePair(pair);
            String sector = detectSector(pair);

            if (!checkLiquidity(pair, cat)) { blockedLiq.incrementAndGet(); return null; }

            optimizer.updateFromCandles(pair, m15);

            double normDelta = getNormalizedDelta(pair);
            decisionEngine.setVolumeDelta(pair, normDelta);

            double relStrength = computeRelativeStrength(pair, m15);
            decisionEngine.updateRelativeStrength(pair, getSymbolReturn15m(m15), getBtcReturn15m());

            com.bot.DecisionEngineMerged.TradeIdea idea =
                    decisionEngine.analyze(pair, m1, m5, m15, h1, h2, cat);

            if (idea == null) return null;

            // [v7.0 FIX] Используем ISC effective min confidence (с учётом streak guard)
            // вместо хардкодного MIN_CONF=50. Это предотвращает бесполезную работу
            // GIC/PumpHunter/OBI/Optimizer на сигналах которые ISC потом отклонит.
            double earlyMinConf = Math.max(MIN_CONF, isc.getEffectiveMinConfidence());
            if (idea.probability < earlyMinConf) return null;

            boolean isLong   = idea.side == com.bot.TradingCore.Side.LONG;
            double gicWeight = gic.getFilterWeight(pair, isLong, relStrength, sector);

            if (gicWeight <= 0.0) return null;
            if (gicWeight < 1.0) {
                double penaltyProb = idea.probability * gicWeight;
                if (penaltyProb < MIN_CONF) return null;
                idea = rebuildIdea(idea, penaltyProb, idea.flags);
            } else if (gicWeight > 1.0) {
                // [v7.0 FIX] Cap at 88 (not 90) to respect DecisionEngine calibration
                idea = rebuildIdea(idea, Math.min(88, idea.probability * gicWeight), idea.flags);
            }

            if (!correlationGuard.allow(pair, idea.side, cat, sector)) {
                blockedCorr.incrementAndGet(); return null;
            }

            // [v7.0] PumpHunter: diminishing boost, cap 88
            com.bot.PumpHunter.PumpEvent pump = pumpHunter.detectPump(pair, m1, m5, m15);
            if (pump != null && pump.strength > 0.40) {
                boolean aligned = (idea.side == com.bot.TradingCore.Side.LONG && pump.isBullish()) ||
                        (idea.side == com.bot.TradingCore.Side.SHORT && pump.isBearish());
                if (aligned) {
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("PH_" + pump.type.name());
                    // [v7.0] Diminishing: бонус уменьшается чем выше текущая probability
                    double headroom = 88.0 - idea.probability;
                    double bonus = Math.min(pump.strength * 6, headroom * 0.5);
                    idea = rebuildIdea(idea, Math.min(88, idea.probability + bonus), nf);
                }
            }

            idea = optimizer.withAdjustedConfidence(idea);
            if (idea.probability < MIN_CONF) return null;

            // [v7.0] OBI: diminishing boost, cap 88
            OrderbookSnapshot obs = orderbookMap.get(pair);
            if (obs != null && obs.isFresh()) {
                double obi = obs.obi();
                boolean obiContra = (isLong && obi < -OBI_THRESHOLD * 1.5) ||
                        (!isLong && obi > OBI_THRESHOLD * 1.5);
                boolean obiAligned = (isLong && obi > OBI_THRESHOLD) || (!isLong && obi < -OBI_THRESHOLD);
                if (obiContra && idea.probability < 77) return null;
                if (obiAligned) {
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("OBI" + String.format("%+.0f", obi * 100));
                    double headroom = 88.0 - idea.probability;
                    double bonus = Math.min(Math.abs(obi) * 4, headroom * 0.4);
                    idea = rebuildIdea(idea, Math.min(88, idea.probability + bonus), nf);
                }
            }

            // [v7.0] Delta: diminishing boost, cap 88
            boolean deltaOk = (isLong && normDelta > 0.14) || (!isLong && normDelta < -0.14) || Math.abs(normDelta) < 0.07;
            if (!deltaOk && idea.probability < DELTA_BLOCK_CONF) return null;
            if (Math.abs(normDelta) > 0.28) {
                List<String> nf = new ArrayList<>(idea.flags);
                nf.add("Δ" + (normDelta > 0 ? "BUY" : "SELL") + pct(Math.abs(normDelta)));
                double headroom = 88.0 - idea.probability;
                double bonus = Math.min(Math.abs(normDelta) * 3, headroom * 0.35);
                idea = rebuildIdea(idea, Math.min(88, idea.probability + bonus), nf);
            }

            if (sector != null && isLong) {
                double weakness = gic.getSectorWeakness(sector);
                if (weakness > 0.5) {
                    double penaltyProb = idea.probability * (1 - weakness * 0.3);
                    if (penaltyProb < MIN_CONF) return null;
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("WEAK_SECTOR");
                    idea = rebuildIdea(idea, penaltyProb, nf);
                }
            }

            idea = adjustStopForClusters(idea, m15);

            if (!checkMinProfit(idea, cat)) { blockedProfit.incrementAndGet(); return null; }

            // [FIX-COMP] Добавляем рекомендованный размер позиции
            double posSize = getPositionSizeUsdt(idea, cat);
            List<String> nf = new ArrayList<>(idea.flags);
            nf.add(String.format("SIZE=%.1f$", posSize));
            idea = rebuildIdea(idea, idea.probability, nf);

            if (!isc.allowSignal(idea)) return null;
            isc.registerSignal(idea);
            correlationGuard.register(pair, idea.side, cat, sector);

            return idea;
        } catch (Exception e) {
            System.out.println("[processPair] " + pair + ": " + e.getMessage());
            return null;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-BLIND] LIVE CANDLE ASSEMBLER
    // ══════════════════════════════════════════════════════════════

    /**
     * Возвращает список 15m свечей, где ПОСЛЕДНЯЯ СВЕЧА — живая,
     * собранная из 1m данных прямо сейчас.
     *
     * Это устраняет 14-минутную слепоту: RSI, EMA, ATR пересчитываются
     * каждую минуту вместо раза в 14 минут.
     */
    private List<com.bot.TradingCore.Candle> getCached15mWithLive(String pair) {
        List<com.bot.TradingCore.Candle> historical = getCached(pair, "15m", KLINES_LIMIT);
        if (historical == null || historical.isEmpty()) return historical;

        List<com.bot.TradingCore.Candle> m1buf = liveM1Buffer.get(pair);
        if (m1buf == null || m1buf.isEmpty()) return historical;

        com.bot.TradingCore.Candle liveCurrent = assembleLive15mCandle(m1buf);
        if (liveCurrent == null) return historical;

        com.bot.TradingCore.Candle lastHistorical = historical.get(historical.size() - 1);
        long livePeriod = liveCurrent.openTime / (15 * 60_000L);
        long lastPeriod = lastHistorical.openTime / (15 * 60_000L);

        List<com.bot.TradingCore.Candle> result = new ArrayList<>(historical);
        if (livePeriod == lastPeriod) {
            result.set(result.size() - 1, liveCurrent); // заменяем текущую
        } else if (livePeriod > lastPeriod) {
            result.add(liveCurrent); // новая свеча началась
        }
        return Collections.unmodifiableList(result);
    }

    private com.bot.TradingCore.Candle assembleLive15mCandle(List<com.bot.TradingCore.Candle> m1) {
        if (m1 == null || m1.isEmpty()) return null;
        long now = System.currentTimeMillis();
        long current15mStart = (now / (15 * 60_000L)) * (15 * 60_000L);

        double open = Double.NaN, high = Double.NEGATIVE_INFINITY,
                low  = Double.POSITIVE_INFINITY, close = Double.NaN;
        double volume = 0, qvol = 0;
        int count = 0;

        for (com.bot.TradingCore.Candle c : m1) {
            if (c.openTime >= current15mStart) {
                if (Double.isNaN(open)) open = c.open;
                high   = Math.max(high, c.high);
                low    = Math.min(low, c.low);
                close  = c.close;
                volume += c.volume;
                qvol   += c.qvol;
                count++;
            }
        }

        if (count == 0 || Double.isNaN(open)) return null;
        return new com.bot.TradingCore.Candle(
                current15mStart, open, high, low, close, volume, qvol,
                current15mStart + 15 * 60_000L - 1);
    }

    private void updateLiveM1Buffer(String pair, List<com.bot.TradingCore.Candle> m1) {
        if (m1 == null || m1.isEmpty()) return;
        int start = Math.max(0, m1.size() - LIVE_M1_BUFFER_SIZE);
        liveM1Buffer.put(pair, new ArrayList<>(m1.subList(start, m1.size())));
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-COMP] РАЗМЕР ПОЗИЦИИ — Kelly-inspired компаундинг
    // ══════════════════════════════════════════════════════════════

    /**
     * Рассчитывает размер позиции в USDT на основе текущего баланса.
     *
     * Формула: рискСумма / стопПроцент = размерПозиции
     * рискСумма = баланс * рискПроцент (1-2% в зависимости от категории)
     *
     * По мере роста баланса $100→$1000→$100000 позиции растут пропорционально.
     * Это и есть механизм превращения $100 в $1,000,000.
     */
    public double getPositionSizeUsdt(com.bot.DecisionEngineMerged.TradeIdea idea,
                                      com.bot.DecisionEngineMerged.CoinCategory cat) {
        double balance = Math.max(accountBalance, 100.0);

        double riskPct = switch (cat) {
            case TOP  -> 0.015; // 1.5% риска для топ-монет
            case ALT  -> 0.020; // 2.0% для альтов
            case MEME -> 0.010; // 1.0% для мем-коинов
        };

        // Высокая уверенность → чуть больше позиция
        if (idea.probability >= 80)      riskPct *= 1.30;
        else if (idea.probability >= 70) riskPct *= 1.15;
        else if (idea.probability < 58)  riskPct *= 0.75;

        double riskUsdt  = balance * riskPct;
        double stopPct   = Math.max(0.005, Math.abs(idea.price - idea.stop) / idea.price);
        double posSize   = riskUsdt / stopPct;

        // Лимиты: не более 20% баланса, не менее $6.5
        posSize = Math.min(posSize, balance * 0.20);
        posSize = Math.max(posSize, 6.5);

        return Math.round(posSize * 100.0) / 100.0;
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-COMP] ОБНОВЛЕНИЕ БАЛАНСА
    // ══════════════════════════════════════════════════════════════

    private void refreshAccountBalance() {
        if (API_KEY.isBlank()) return;
        try {
            long ts = System.currentTimeMillis();
            String qs = "timestamp=" + ts;
            String sig = hmacSHA256(API_SECRET, qs);

            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v2/balance?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", API_KEY)
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() == 200) {
                JSONArray arr = new JSONArray(resp.body());
                for (int i = 0; i < arr.length(); i++) {
                    JSONObject o = arr.getJSONObject(i);
                    if ("USDT".equals(o.optString("asset"))) {
                        double nb = o.optDouble("availableBalance", 0);
                        if (nb > 0) {
                            double old = accountBalance;
                            accountBalance = nb;
                            lastBalanceRefresh = System.currentTimeMillis();
                            checkBalanceMilestone(old, nb);
                        }
                        break;
                    }
                }
            }
        } catch (Exception ignored) {}
    }

    private void checkBalanceMilestone(double old, double newBal) {
        double[] milestones = {200, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 500000, 1_000_000};
        for (double m : milestones) {
            if (old < m && newBal >= m) {
                bot.sendMessageAsync(String.format("🎯 *MILESTONE* Баланс достиг $%.0f! 🚀🚀🚀", m));
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-UDS] USER DATA STREAM
    // ══════════════════════════════════════════════════════════════

    private void initUserDataStream() {
        if (API_KEY.isBlank()) return;
        try {
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/listenKey"))
                            .timeout(Duration.ofSeconds(10))
                            .header("X-MBX-APIKEY", API_KEY)
                            .POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() != 200) { scheduleUdsRetry(10); return; }
            udsListenKey = new JSONObject(resp.body()).getString("listenKey");
            connectUserDataStream(udsListenKey);
        } catch (Exception e) {
            System.out.println("[UDS] Init error: " + e.getMessage());
            scheduleUdsRetry(30);
        }
    }

    private void connectUserDataStream(String listenKey) {
        HttpClient udsClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
        udsClient.newWebSocketBuilder()
                .buildAsync(URI.create("wss://fstream.binance.com/ws/" + listenKey), new WebSocket.Listener() {

                    @Override
                    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                        try { processUserDataEvent(new JSONObject(data.toString())); udsEventsCount.incrementAndGet(); }
                        catch (Exception ignored) {}
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void onError(WebSocket ws, Throwable error) {
                        udsWebSocket = null; scheduleUdsRetry(10);
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                        udsWebSocket = null; scheduleUdsRetry(5);
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenAccept(ws -> { udsWebSocket = ws; System.out.println("[UDS] ✅ Connected"); })
                .exceptionally(ex -> { scheduleUdsRetry(15); return null; });
    }

    private void processUserDataEvent(JSONObject event) {
        String type = event.optString("e", "");
        switch (type) {
            case "ORDER_TRADE_UPDATE" -> {
                JSONObject o = event.optJSONObject("o");
                if (o == null) return;
                String symbol  = o.optString("s");
                String status  = o.optString("X");
                String oType   = o.optString("o");
                String sideStr = o.optString("S");
                double avgPrice     = o.optDouble("ap", 0);
                double qty          = o.optDouble("q", 0);
                double realizedPnl  = o.optDouble("rp", 0);
                boolean isBuy       = "BUY".equals(sideStr);
                boolean isClose     = "true".equals(o.optString("R", "false")) ||
                        "STOP_MARKET".equals(oType) ||
                        "TAKE_PROFIT_MARKET".equals(oType) ||
                        "TRAILING_STOP_MARKET".equals(oType);

                if ("FILLED".equals(status) && isClose) {
                    com.bot.TradingCore.Side closedSide = isBuy
                            ? com.bot.TradingCore.Side.SHORT
                            : com.bot.TradingCore.Side.LONG;
                    double pnlPct = avgPrice > 0 ? realizedPnl / (avgPrice * qty) * 100 : 0;
                    isc.closeTrade(symbol, closedSide, pnlPct);
                    String emoji = realizedPnl >= 0 ? "✅" : "❌";
                    bot.sendMessageAsync(String.format("%s *CLOSED* %s %s\nPnL: %+.4f$ (%+.2f%%)",
                            emoji, symbol, closedSide, realizedPnl, pnlPct));
                    System.out.printf("[UDS] CLOSED %s %s PnL=%+.4f%n", symbol, closedSide, realizedPnl);
                }
            }

            case "ACCOUNT_UPDATE" -> {
                JSONObject a = event.optJSONObject("a");
                if (a == null) return;
                JSONArray balances = a.optJSONArray("B");
                if (balances == null) return;
                for (int i = 0; i < balances.length(); i++) {
                    JSONObject b = balances.getJSONObject(i);
                    if ("USDT".equals(b.optString("a"))) {
                        double wb = b.optDouble("wb", 0);
                        if (wb > 0) { checkBalanceMilestone(accountBalance, wb); accountBalance = wb; }
                        break;
                    }
                }
            }

            case "MARGIN_CALL" -> {
                bot.sendMessageAsync("🚨 *MARGIN CALL* — немедленно проверьте аккаунт!");
                System.out.println("[UDS] ⚠️ MARGIN CALL!");
            }

            case "listenKeyExpired" -> {
                udsWebSocket = null; scheduleUdsRetry(1);
            }
        }
    }

    private void renewListenKey() {
        if (API_KEY.isBlank() || udsListenKey == null) return;
        try {
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/listenKey"))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", API_KEY)
                            .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) { System.out.println("[UDS] Key renewed"); }
            else { initUserDataStream(); }
        } catch (Exception e) { initUserDataStream(); }
    }

    private void scheduleUdsRetry(int delaySec) {
        udsExecutor.schedule(this::initUserDataStream, delaySec, TimeUnit.SECONDS);
    }

    // ══════════════════════════════════════════════════════════════
    //  WS HEALTH CHECK
    // ══════════════════════════════════════════════════════════════

    private void checkWsHealth() {
        long now = System.currentTimeMillis();
        long staleThreshold = 5 * 60_000L;
        for (Map.Entry<String, WebSocket> e : wsMap.entrySet()) {
            Long last = lastTickTime.get(e.getKey());
            if (last != null && now - last > staleThreshold) {
                System.out.printf("[WS-HEALTH] %s stale — reconnecting%n", e.getKey());
                reconnectWs(e.getKey());
            }
        }
        if (!API_KEY.isBlank() && udsWebSocket == null) {
            scheduleUdsRetry(2);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  LIQUIDITY GUARD
    // ══════════════════════════════════════════════════════════════

    private boolean checkLiquidity(String pair, com.bot.DecisionEngineMerged.CoinCategory cat) {
        Double vol = volume24hUSD.get(pair);
        if (vol == null) return true;
        double minVol = switch (cat) {
            case TOP  -> MIN_VOL_TOP_USD;
            case ALT  -> MIN_VOL_ALT_USD;
            case MEME -> MIN_VOL_MEME_USD;
        };
        if (vol < minVol) return false;
        OrderbookSnapshot obs = orderbookMap.get(pair);
        if (obs != null && obs.isFresh()) {
            double maxObi = switch (cat) { case TOP -> 0.85; case ALT -> 0.75; case MEME -> 0.65; };
            if (Math.abs(obs.obi()) > maxObi) return false;
        }
        return true;
    }

    // ══════════════════════════════════════════════════════════════
    //  RELATIVE STRENGTH
    // ══════════════════════════════════════════════════════════════

    private double computeRelativeStrength(String pair, List<com.bot.TradingCore.Candle> m15) {
        if (m15 == null || m15.size() < 5) return 0.5;
        int n = m15.size();
        double symRet = (m15.get(n-1).close - m15.get(n-4).close) / (m15.get(n-4).close + 1e-9);
        double btcRet = getBtcReturn15m();
        double rs;
        if (Math.abs(btcRet) < 0.001) rs = symRet > 0 ? 0.65 : 0.35;
        else if (btcRet < 0 && symRet > 0) rs = Math.min(0.98, 0.78 + symRet * 5);
        else rs = clamp(0.5 + (symRet - btcRet) / (Math.abs(btcRet) + 0.001) * 0.15, 0.0, 1.0);
        Deque<Double> h = relStrengthHistory.computeIfAbsent(pair, k -> new ArrayDeque<>());
        h.addLast(rs); if (h.size() > RS_HISTORY) h.removeFirst();
        return h.stream().mapToDouble(Double::doubleValue).average().orElse(0.5);
    }

    private double getSymbolReturn15m(List<com.bot.TradingCore.Candle> m15) {
        if (m15 == null || m15.size() < 5) return 0;
        int n = m15.size();
        return (m15.get(n-1).close - m15.get(n-4).close) / (m15.get(n-4).close + 1e-9);
    }

    private volatile double cachedBtcReturn = 0.0;
    private volatile long   lastBtcReturnTime = 0;

    private double getBtcReturn15m() {
        if (System.currentTimeMillis() - lastBtcReturnTime < 30_000) return cachedBtcReturn;
        CachedCandles c = candleCache.get("BTCUSDT_15m");
        if (c == null || c.candles.size() < 5) return 0;
        int n = c.candles.size();
        cachedBtcReturn = (c.candles.get(n-1).close - c.candles.get(n-4).close) / (c.candles.get(n-4).close + 1e-9);
        lastBtcReturnTime = System.currentTimeMillis();
        return cachedBtcReturn;
    }

    // ══════════════════════════════════════════════════════════════
    //  STOP CLUSTER AVOIDANCE
    // ══════════════════════════════════════════════════════════════

    private com.bot.DecisionEngineMerged.TradeIdea adjustStopForClusters(
            com.bot.DecisionEngineMerged.TradeIdea idea,
            List<com.bot.TradingCore.Candle> m15) {
        if (m15 == null || m15.size() < 20) return idea;
        double price = idea.price, oldStop = idea.stop, newStop = oldStop;
        int n = m15.size(), lookback = Math.min(20, n - 1);

        if (idea.side == com.bot.TradingCore.Side.LONG) {
            double swingLow = Double.MAX_VALUE;
            for (int i = n - lookback; i < n - 1; i++) {
                double low = m15.get(i).low;
                if (low >= oldStop * 0.98 && low <= oldStop * 1.005) swingLow = Math.min(swingLow, low);
            }
            if (swingLow != Double.MAX_VALUE) {
                newStop = Math.max(swingLow * (1 - STOP_CLUSTER_SHIFT), price - atr(m15, 14) * 2.2);
            }
        } else {
            double swingHigh = Double.MIN_VALUE;
            for (int i = n - lookback; i < n - 1; i++) {
                double high = m15.get(i).high;
                if (high >= oldStop * 0.995 && high <= oldStop * 1.02) swingHigh = Math.max(swingHigh, high);
            }
            if (swingHigh != Double.MIN_VALUE) {
                newStop = Math.min(swingHigh * (1 + STOP_CLUSTER_SHIFT), price + atr(m15, 14) * 2.2);
            }
        }

        if (newStop == oldStop) return idea;
        List<String> nf = new ArrayList<>(idea.flags);
        nf.add("SL_ADJ");
        return new com.bot.DecisionEngineMerged.TradeIdea(
                idea.symbol, idea.side, idea.price, newStop, idea.take, idea.rr,
                idea.probability, nf, idea.fundingRate, idea.fundingDelta,
                idea.oiChange, idea.htfBias, idea.category);
    }

    // ══════════════════════════════════════════════════════════════
    //  MINIMUM PROFIT GUARD
    // ══════════════════════════════════════════════════════════════

    private boolean checkMinProfit(com.bot.DecisionEngineMerged.TradeIdea idea,
                                   com.bot.DecisionEngineMerged.CoinCategory cat) {
        double gross = Math.abs(idea.tp1 - idea.price) / idea.price;
        double slip  = switch (cat) { case TOP -> 0.0005; case ALT -> 0.0015; case MEME -> 0.0040; };
        double min   = switch (cat) { case TOP -> MIN_PROFIT_TOP; case ALT -> MIN_PROFIT_ALT; case MEME -> MIN_PROFIT_MEME; };
        return (gross - 0.0008 - slip) >= min;
    }

    // ══════════════════════════════════════════════════════════════
    //  CORRELATION GUARD
    // ══════════════════════════════════════════════════════════════

    private static final class CorrelationGuard {
        private int longCount = 0, shortCount = 0;
        private final Map<String, Integer> sectorCount = new HashMap<>();
        private final Set<String> registered = new HashSet<>();
        // [v7.0] TOP coins no longer bypass limits. Increased MAX_DIR slightly to compensate.
        private static final int MAX_DIR = 8, MAX_SECTOR = 3, MAX_TOP_SAME_DIR = 4;

        synchronized void resetCycle() { longCount = 0; shortCount = 0; sectorCount.clear(); registered.clear(); }

        synchronized boolean allow(String pair, com.bot.TradingCore.Side side,
                                   com.bot.DecisionEngineMerged.CoinCategory cat, String sector) {
            // [v7.0 FIX] TOP coins are no longer exempt — 6 BTC/ETH/SOL longs in a crash = catastrophe
            if (side == com.bot.TradingCore.Side.LONG  && longCount  >= MAX_DIR) return false;
            if (side == com.bot.TradingCore.Side.SHORT && shortCount >= MAX_DIR) return false;
            if (sector != null && sectorCount.getOrDefault(sector, 0) >= MAX_SECTOR) return false;
            // [v7.0] Extra limit: max 4 TOP coins in same direction
            if (cat == com.bot.DecisionEngineMerged.CoinCategory.TOP) {
                long topSameDir = registered.stream()
                        .filter(p -> categorizePair(p) == com.bot.DecisionEngineMerged.CoinCategory.TOP)
                        .count();
                if (topSameDir >= MAX_TOP_SAME_DIR) return false;
            }
            return true;
        }

        synchronized void register(String pair, com.bot.TradingCore.Side side,
                                   com.bot.DecisionEngineMerged.CoinCategory cat, String sector) {
            if (registered.contains(pair)) return;
            registered.add(pair);
            if (side == com.bot.TradingCore.Side.LONG) longCount++; else shortCount++;
            if (sector != null) sectorCount.merge(sector, 1, Integer::sum);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  CANDLE CACHE
    // ══════════════════════════════════════════════════════════════

    private List<com.bot.TradingCore.Candle> getCached(String symbol, String interval, int limit) {
        String key = symbol + "_" + interval;
        long   ttl = CACHE_TTL.getOrDefault(interval, 60_000L);
        CachedCandles cached = candleCache.get(key);
        totalFetches.incrementAndGet();
        if (cached != null && !cached.isStale(ttl) && !cached.candles.isEmpty()) {
            cacheHits.incrementAndGet(); return cached.candles;
        }
        List<com.bot.TradingCore.Candle> fresh = fetchKlinesDirect(symbol, interval, limit);
        if (!fresh.isEmpty()) { candleCache.put(key, new CachedCandles(fresh)); lastFetchTime.put(key, System.currentTimeMillis()); }
        else if (cached != null) return cached.candles;
        return fresh;
    }

    public List<com.bot.TradingCore.Candle> fetchKlines(String symbol, String interval, int limit) {
        return getCached(symbol, interval, limit);
    }

    private List<com.bot.TradingCore.Candle> fetchKlinesDirect(String symbol, String interval, int limit) {
        awaitRateLimit();
        try {
            String url = String.format("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
                    symbol, interval, limit);
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());

            resp.headers().firstValue("X-MBX-USED-WEIGHT-1M").ifPresent(w -> {
                try { int sw = Integer.parseInt(w); usedWeight.set(Math.max(usedWeight.get(), sw)); if (sw > 2000) rateLimitPaused = true; }
                catch (NumberFormatException ignored) {}
            });

            if (resp.statusCode() == 429 || resp.statusCode() == 418) {
                rateLimitPaused = true;
                try { Thread.sleep(30_000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                return Collections.emptyList();
            }

            trackWeight(5);
            String body = resp.body();
            if (!body.trim().startsWith("[")) return Collections.emptyList();
            JSONArray arr = new JSONArray(body);
            List<com.bot.TradingCore.Candle> list = new ArrayList<>(arr.length());
            for (int i = 0; i < arr.length(); i++) {
                JSONArray k = arr.getJSONArray(i);
                list.add(new com.bot.TradingCore.Candle(
                        k.getLong(0), Double.parseDouble(k.getString(1)),
                        Double.parseDouble(k.getString(2)), Double.parseDouble(k.getString(3)),
                        Double.parseDouble(k.getString(4)), Double.parseDouble(k.getString(5)),
                        k.length() > 7 ? Double.parseDouble(k.getString(7)) : 0.0, k.getLong(6)));
            }
            return list;
        } catch (Exception e) { return Collections.emptyList(); }
    }

    public CompletableFuture<List<com.bot.TradingCore.Candle>> fetchKlinesAsync(String symbol, String interval, int limit) {
        return CompletableFuture.supplyAsync(() -> fetchKlinesDirect(symbol, interval, limit), fetchPool);
    }

    // ══════════════════════════════════════════════════════════════
    //  VOLUME DELTA
    // ══════════════════════════════════════════════════════════════

    public double getRawDelta(String symbol) { return deltaBuffer.getOrDefault(symbol, 0.0); }

    public double getNormalizedDelta(String symbol) {
        double d = deltaBuffer.getOrDefault(symbol, 0.0);
        if (d == 0.0) return 0.0;
        double absMax = deltaBuffer.values().stream().mapToDouble(Math::abs).max().orElse(1.0);
        return Math.max(-1.0, Math.min(1.0, d / (absMax + 1e-9)));
    }

    // ══════════════════════════════════════════════════════════════
    //  WEBSOCKET (aggTrade)
    // ══════════════════════════════════════════════════════════════

    public void connectWs(String pair) { connectWsInternal(pair); }

    private void connectWsInternal(String pair) {
        try {
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build()
                    .newWebSocketBuilder()
                    .buildAsync(URI.create("wss://fstream.binance.com/ws/" + pair.toLowerCase() + "@aggTrade"),
                            new WebSocket.Listener() {
                                @Override
                                public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                                    try {
                                        wsMessageCount.incrementAndGet();
                                        JSONObject j = new JSONObject(data.toString());
                                        double price = Double.parseDouble(j.getString("p"));
                                        double qty   = Double.parseDouble(j.getString("q"));
                                        long   ts    = j.getLong("T");
                                        double side  = !j.getBoolean("m") ? qty : -qty;

                                        deltaWindowStart.putIfAbsent(pair, ts);
                                        long age = ts - deltaWindowStart.get(pair);
                                        if (age > DELTA_WINDOW_MS) {
                                            deltaHistory.put(pair, deltaBuffer.getOrDefault(pair, 0.0));
                                            deltaBuffer.put(pair, side); deltaWindowStart.put(pair, ts);
                                        } else { deltaBuffer.merge(pair, side, Double::sum); }

                                        synchronized (wsLock) {
                                            Deque<Double> dq = tickPriceDeque.computeIfAbsent(pair, k -> new ArrayDeque<>());
                                            dq.addLast(price); while (dq.size() > TICK_HISTORY) dq.removeFirst();
                                            Deque<Double> vq = tickVolumeDeque.computeIfAbsent(pair, k -> new ArrayDeque<>());
                                            vq.addLast(qty);   while (vq.size() > TICK_HISTORY) vq.removeFirst();
                                        }
                                        lastTickPrice.put(pair, price); lastTickTime.put(pair, ts);
                                        microBuilders.computeIfAbsent(pair, k -> new MicroCandleBuilder(30_000)).addTick(ts, price, qty);

                                        com.bot.DecisionEngineMerged.TradeIdea et = generateEarlyTickSignal(pair, price, ts);
                                        if (et != null && filterEarlySignal(et)) {
                                            bot.sendMessageAsync("🎯 *EARLY TICK*\n" + et.toTelegramString());
                                            earlySignals.incrementAndGet(); isc.registerSignal(et);
                                        }
                                    } catch (Exception ignored) {}
                                    return CompletableFuture.completedFuture(null);
                                }
                                @Override public void onError(WebSocket ws, Throwable error) { reconnectWs(pair); }
                                @Override public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                                    reconnectWs(pair); return CompletableFuture.completedFuture(null);
                                }
                            })
                    .thenAccept(ws -> { wsMap.put(pair, ws); wsReconnectDelay.put(pair, WS_INITIAL_DELAY_MS); })
                    .exceptionally(ex -> { long delay = wsReconnectDelay.getOrDefault(pair, WS_INITIAL_DELAY_MS);
                        wsWatcher.schedule(() -> connectWsInternal(pair), delay, TimeUnit.MILLISECONDS);
                        wsReconnectDelay.put(pair, Math.min(delay * 2, WS_MAX_DELAY_MS)); return null; });
        } catch (Exception e) { reconnectWs(pair); }
    }

    private void reconnectWs(String pair) {
        wsMap.remove(pair);
        long delay = wsReconnectDelay.getOrDefault(pair, WS_INITIAL_DELAY_MS);
        wsWatcher.schedule(() -> connectWsInternal(pair), delay, TimeUnit.MILLISECONDS);
        wsReconnectDelay.put(pair, Math.min(delay * 2, WS_MAX_DELAY_MS));
    }

    private com.bot.DecisionEngineMerged.TradeIdea generateEarlyTickSignal(String symbol, double price, long ts) {
        Deque<Double> dq = tickPriceDeque.get(symbol); Deque<Double> vq = tickVolumeDeque.get(symbol);
        if (dq == null || dq.size() < 30 || vq == null || vq.size() < 30) return null;
        List<Double> buf = new ArrayList<>(dq), volBuf = new ArrayList<>(vq);
        int n = buf.size();
        double move = buf.get(n-1) - buf.get(n-22), avg = buf.stream().mapToDouble(Double::doubleValue).average().orElse(price);
        double vel = Math.abs(move) / (avg + 1e-9);
        if (vel < 0.0022) return null;
        double m1 = buf.get(n/2-1) - buf.get(0), m2 = buf.get(n-1) - buf.get(n/2);
        if (!(Math.abs(m2) > Math.abs(m1) * 1.60)) return null;
        int vw = Math.min(30, volBuf.size());
        double avgVol = volBuf.subList(0, vw-5).stream().mapToDouble(Double::doubleValue).average().orElse(0.001);
        double recVol = volBuf.subList(vw-5, vw).stream().mapToDouble(Double::doubleValue).average().orElse(0);
        if (recVol < avgVol * 1.6) return null;
        boolean up = move > 0; int streak = 0;
        for (int i = n-1; i >= Math.max(0, n-5); i--) {
            if (i == 0) break;
            if ((buf.get(i) >= buf.get(i-1)) == up) streak++; else break;
        }
        if (streak < 3) return null;
        if (n >= 3) { double last3 = buf.get(n-1) - buf.get(n-3); if (up && last3 < 0) return null; if (!up && last3 > 0) return null; }
        double atrV = getAtr(symbol); if (atrV <= 0) atrV = price * 0.005;
        double conf = Math.min(68, 50 + vel * 4500);
        return new com.bot.DecisionEngineMerged.TradeIdea(
                symbol, up ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT,
                price, up ? price - atrV*1.5 : price + atrV*1.5,
                up ? price + atrV*3.2 : price - atrV*3.2, conf,
                List.of("EARLY_TICK", up?"UP":"DN", "v="+String.format("%.2e",vel), "stk="+streak));
    }

    private boolean filterEarlySignal(com.bot.DecisionEngineMerged.TradeIdea sig) {
        boolean isLong = sig.side == com.bot.TradingCore.Side.LONG;
        double rs = relStrengthHistory.getOrDefault(sig.symbol, new ArrayDeque<>())
                .stream().mapToDouble(Double::doubleValue).average().orElse(0.5);
        if (gic.getFilterWeight(sig.symbol, isLong, rs, detectSector(sig.symbol)) < 0.60) return false;
        if (!isc.allowSignal(sig)) return false;
        return optimizer.adjustConfidence(sig) >= 56;
    }

    // ══════════════════════════════════════════════════════════════
    //  FUNDING + OI
    // ══════════════════════════════════════════════════════════════

    private void refreshAllFundingRates() {
        try {
            JSONArray arr = new JSONArray(http.send(
                    HttpRequest.newBuilder().uri(URI.create("https://fapi.binance.com/fapi/v1/premiumIndex"))
                            .timeout(Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.ofString()).body());
            Map<String, Double> rates = new HashMap<>(arr.length());
            for (int i = 0; i < arr.length(); i++) { JSONObject o = arr.getJSONObject(i); rates.put(o.getString("symbol"), o.optDouble("lastFundingRate", 0)); }
            List<String> pairs = new ArrayList<>(cachedPairs);
            for (int i = 0; i < pairs.size(); i++) {
                try { fetchAndUpdateOI(pairs.get(i), rates.getOrDefault(pairs.get(i), 0.0)); if (i%10==9) Thread.sleep(40); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                catch (Exception ignored) {}
            }
        } catch (Exception e) { System.out.println("[FR] Error: " + e.getMessage()); }
    }

    private void fetchAndUpdateOI(String symbol, double fr) {
        try {
            JSONObject oiJ = new JSONObject(http.send(
                    HttpRequest.newBuilder().uri(URI.create("https://fapi.binance.com/fapi/v1/openInterest?symbol="+symbol))
                            .timeout(Duration.ofSeconds(6)).GET().build(), HttpResponse.BodyHandlers.ofString()).body());
            double oi = oiJ.optDouble("openInterest", 0);
            JSONArray hist = new JSONArray(http.send(
                    HttpRequest.newBuilder().uri(URI.create("https://fapi.binance.com/futures/data/openInterestHist?symbol="+symbol+"&period=1h&limit=5"))
                            .timeout(Duration.ofSeconds(6)).GET().build(), HttpResponse.BodyHandlers.ofString()).body());
            double oi1h = 0, oi4h = 0;
            if (hist.length() >= 2) { double p = hist.getJSONObject(hist.length()-2).optDouble("sumOpenInterest", oi); oi1h = ((oi-p)/(p+1e-9))*100; }
            if (hist.length() >= 5) { double p = hist.getJSONObject(0).optDouble("sumOpenInterest", oi); oi4h = ((oi-p)/(p+1e-9))*100; }
            decisionEngine.updateFundingOI(symbol, fr, oi, oi1h, oi4h);
        } catch (Exception e) { decisionEngine.updateFundingOI(symbol, fr, 0, 0, 0); }
    }

    // ══════════════════════════════════════════════════════════════
    //  REFRESH VOLUME + PAIRS
    // ══════════════════════════════════════════════════════════════

    private void refreshVolume24h() {
        try {
            JSONArray arr = new JSONArray(http.send(
                    HttpRequest.newBuilder().uri(URI.create("https://fapi.binance.com/fapi/v1/ticker/24hr"))
                            .timeout(Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.ofString()).body());
            for (int i = 0; i < arr.length(); i++) { JSONObject o = arr.getJSONObject(i); double v = o.optDouble("quoteVolume", 0); if (v > 0) volume24hUSD.put(o.getString("symbol"), v); }
        } catch (Exception e) { System.out.println("[VOL24H] Error: " + e.getMessage()); }
    }

    public Set<String> getTopSymbolsSet(int limit) {
        try {
            Set<String> binancePairs = getBinanceSymbolsFutures();
            JSONArray cg = new JSONArray(http.send(
                    HttpRequest.newBuilder().uri(URI.create("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1"))
                            .timeout(Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.ofString()).body());
            Set<String> top = new LinkedHashSet<>();
            for (int i = 0; i < cg.length(); i++) {
                String sym = cg.getJSONObject(i).getString("symbol").toUpperCase();
                if (STABLE.contains(sym)) continue;
                String pair = sym + "USDT";
                if (binancePairs.contains(pair)) top.add(pair);
                if (top.size() >= limit) break;
            }
            if (top.size() < limit) { for (String p : binancePairs) { if (top.size() >= limit) break; top.add(p); } }
            System.out.println("[PAIRS] Loaded " + top.size());
            return top;
        } catch (Exception e) {
            return new LinkedHashSet<>(Arrays.asList("BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","LINKUSDT"));
        }
    }

    public Set<String> getBinanceSymbolsFutures() {
        try {
            JSONArray arr = new JSONObject(http.send(
                    HttpRequest.newBuilder().uri(URI.create("https://fapi.binance.com/fapi/v1/exchangeInfo"))
                            .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString()).body()).getJSONArray("symbols");
            Set<String> res = new HashSet<>();
            for (int i = 0; i < arr.length(); i++) { JSONObject s = arr.getJSONObject(i); if ("TRADING".equalsIgnoreCase(s.optString("status","TRADING")) && s.getString("symbol").endsWith("USDT")) res.add(s.getString("symbol")); }
            return res;
        } catch (Exception e) { return new HashSet<>(Arrays.asList("BTCUSDT","ETHUSDT","BNBUSDT")); }
    }

    private static com.bot.DecisionEngineMerged.CoinCategory categorizePair(String pair) {
        String sym = pair.endsWith("USDT") ? pair.substring(0, pair.length()-4) : pair;
        return switch (sym) {
            case "DOGE","SHIB","PEPE","FLOKI","WIF","BONK","MEME","NEIRO","POPCAT","COW","MOG","BRETT","TURBO" -> com.bot.DecisionEngineMerged.CoinCategory.MEME;
            case "BTC","ETH","BNB","SOL","XRP","ADA","AVAX","DOT","LINK","MATIC","LTC","ATOM","UNI","AAVE" -> com.bot.DecisionEngineMerged.CoinCategory.TOP;
            default -> com.bot.DecisionEngineMerged.CoinCategory.ALT;
        };
    }

    private com.bot.DecisionEngineMerged.TradeIdea rebuildIdea(com.bot.DecisionEngineMerged.TradeIdea src, double p, List<String> f) {
        return new com.bot.DecisionEngineMerged.TradeIdea(src.symbol, src.side, src.price, src.stop, src.take, src.rr, p, f, src.fundingRate, src.fundingDelta, src.oiChange, src.htfBias, src.category);
    }

    private void logCycleStats() {
        long total = totalFetches.get(), hits = cacheHits.get();
        if (total > 0 && total % 500 == 0) {
            System.out.printf("[Stats] cache=%.1f%% early=%d liq=%d corr=%d ws=%d msgs=%d bal=$%.2f%n",
                    100.0*hits/total, earlySignals.get(), blockedLiq.get(), blockedCorr.get(), wsMap.size(), wsMessageCount.get(), accountBalance);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  ACCESSORS
    // ══════════════════════════════════════════════════════════════

    public double getAtr(String symbol) {
        CachedCandles cc = candleCache.get(symbol + "_15m");
        if (cc == null || cc.candles.isEmpty()) return 0;
        return atr(cc.candles, 14);
    }

    public double getAccountBalance() { return accountBalance; }
    public int  getActiveWsCount()   { return wsMap.size(); }
    public boolean isUdsConnected()  { return udsWebSocket != null; }

    public com.bot.DecisionEngineMerged getDecisionEngine() { return decisionEngine; }
    public com.bot.SignalOptimizer getOptimizer()           { return optimizer; }
    public com.bot.InstitutionalSignalCore getSignalCore()  { return isc; }
    public com.bot.PumpHunter getPumpHunter()               { return pumpHunter; }
    public com.bot.GlobalImpulseController getGIC()         { return gic; }
    public Map<String, Deque<Double>> getTickDeque()        { return tickPriceDeque; }

    // ══════════════════════════════════════════════════════════════
    //  STATIC MATH UTILS
    // ══════════════════════════════════════════════════════════════

    public static double atr(List<com.bot.TradingCore.Candle> c, int period) {
        if (c == null || c.size() <= period) return 0;
        double sum = 0;
        for (int i = c.size() - period; i < c.size(); i++) {
            com.bot.TradingCore.Candle pr = c.get(i-1), cu = c.get(i);
            sum += Math.max(cu.high-cu.low, Math.max(Math.abs(cu.high-pr.close), Math.abs(cu.low-pr.close)));
        }
        return sum / period;
    }

    public static double rsi(List<Double> prices, int period) {
        if (prices == null || prices.size() <= period) return 50.0;
        double gain = 0, loss = 0;
        for (int i = prices.size()-period; i < prices.size(); i++) { double d = prices.get(i)-prices.get(i-1); if (d>0) gain+=d; else loss+=-d; }
        return gain+loss == 0 ? 50.0 : 100.0 - (100.0 / (1.0 + gain / (loss + 1e-12)));
    }

    public static double ema(List<Double> prices, int period) {
        if (prices == null || prices.isEmpty()) return 0;
        double k = 2.0/(period+1), e = prices.get(0);
        for (double p : prices) e = p*k + e*(1-k);
        return e;
    }

    public static double sma(List<Double> prices, int period) {
        if (prices == null || prices.size() < period) return 0;
        double sum = 0; for (int i = prices.size()-period; i < prices.size(); i++) sum += prices.get(i);
        return sum / period;
    }

    public static double vwap(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.isEmpty()) return 0;
        double pv = 0, vol = 0;
        for (com.bot.TradingCore.Candle x : c) { double tp = (x.high+x.low+x.close)/3.0; pv+=tp*x.volume; vol+=x.volume; }
        return vol == 0 ? c.get(c.size()-1).close : pv/vol;
    }

    public static boolean detectBOS(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 10) return false;
        List<Integer> highs = com.bot.DecisionEngineMerged.swingHighs(c, 3);
        List<Integer> lows  = com.bot.DecisionEngineMerged.swingLows(c, 3);
        com.bot.TradingCore.Candle last = c.get(c.size()-1);
        if (!highs.isEmpty() && last.close > c.get(highs.get(highs.size()-1)).high * 1.0005) return true;
        if (!lows.isEmpty()  && last.close < c.get(lows.get(lows.size()-1)).low   * 0.9995) return true;
        return false;
    }

    public static List<Integer> detectSwingHighs(List<com.bot.TradingCore.Candle> c, int lr) { return com.bot.DecisionEngineMerged.swingHighs(c, lr); }
    public static List<Integer> detectSwingLows(List<com.bot.TradingCore.Candle> c, int lr)  { return com.bot.DecisionEngineMerged.swingLows(c, lr); }
    public static int marketStructure(List<com.bot.TradingCore.Candle> c) { return com.bot.DecisionEngineMerged.marketStructure(c); }
    public static boolean detectLiquiditySweep(List<com.bot.TradingCore.Candle> c) { return com.bot.DecisionEngineMerged.detectLiquiditySweep(c); }

    private static String hmacSHA256(String secret, String data) throws Exception {
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] hash = mac.doFinal(data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    // ══════════════════════════════════════════════════════════════
    //  INNER CLASSES
    // ══════════════════════════════════════════════════════════════

    public static final class OrderbookSnapshot {
        public final double bidVolume, askVolume; public final long timestamp;
        public OrderbookSnapshot(double b, double a, long t) { bidVolume=b; askVolume=a; timestamp=t; }
        public double obi() { return (bidVolume-askVolume)/(bidVolume+askVolume+1e-12); }
        public boolean isFresh() { return System.currentTimeMillis()-timestamp < 30_000; }
    }

    public static final class MicroCandleBuilder {
        private final int intervalMs;
        private long bucketStart=-1;
        private double open=Double.NaN, high=Double.NEGATIVE_INFINITY, low=Double.POSITIVE_INFINITY, close=Double.NaN;
        private double volume=0; private long closeTime=-1;
        public MicroCandleBuilder(int intervalMs) { this.intervalMs=intervalMs; }
        public Optional<com.bot.TradingCore.Candle> addTick(long ts, double price, double qty) {
            long bucket = (ts/intervalMs)*intervalMs;
            if (bucketStart==-1) { bucketStart=bucket; open=high=low=close=price; volume=qty; closeTime=bucket+intervalMs-1; return Optional.empty(); }
            if (bucket==bucketStart) { high=Math.max(high,price); low=Math.min(low,price); close=price; volume+=qty; return Optional.empty(); }
            com.bot.TradingCore.Candle c = new com.bot.TradingCore.Candle(bucketStart,open,high,low,close,volume,volume,closeTime);
            bucketStart=bucket; open=high=low=close=price; volume=qty; closeTime=bucket+intervalMs-1;
            return Optional.of(c);
        }
    }

    public static final class Signal {
        public final String symbol, direction; public final double confidence, price; public final long timestamp;
        public Signal(String sym, String dir, double conf, double price) { symbol=sym; direction=dir; confidence=conf; this.price=price; timestamp=System.currentTimeMillis(); }
    }

    private int    envInt(String k, int d)      { try { return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(d))); } catch (Exception e) { return d; } }
    private long   envLong(String k, long d)    { try { return Long.parseLong(System.getenv().getOrDefault(k, String.valueOf(d)));   } catch (Exception e) { return d; } }
    private double envDouble(String k, double d){ try { return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(d))); } catch (Exception e) { return d; } }
    private static double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
    private static String pct(double v) { return String.format("%.0f", v * 100); }
}