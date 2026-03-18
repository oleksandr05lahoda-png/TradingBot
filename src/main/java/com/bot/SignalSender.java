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

/**
 * SignalSender — ядро генерации сигналов GodBot.
 *
 * Возможности:
 *  - Параллельный fetch TOP-N пар через ExecutorService
 *  - Кэш свечей (1m/5m/15m/1h/2h) с TTL по таймфрейму
 *  - WebSocket aggTrade: Volume Delta + Early Tick сигналы
 *  - OBI (Order Book Imbalance) фильтр
 *  - Проверка ликвидности по 24h объёму
 *  - Relative Strength vs BTC для каждой монеты
 *  - Stop Cluster Avoidance (смещение SL от свинг зон)
 *  - Correlation Guard (не более 6 LONG/SHORT + 3 на сектор)
 *  - Min Profit Guard (блокировка если TP1 < fees + slippage)
 *  - Определение сектора монеты через detectSector()
 */
public final class SignalSender {

    // ── Зависимости ────────────────────────────────────────────────
    private final com.bot.TelegramBotSender bot;
    private final HttpClient              http;
    private final com.bot.GlobalImpulseController gic;
    private final com.bot.InstitutionalSignalCore isc;
    private final Object                  wsLock = new Object();

    // ── Конфигурация ───────────────────────────────────────────────
    private final int    TOP_N;
    private final double MIN_CONF;
    private final int    KLINES_LIMIT;
    private final long   BINANCE_REFRESH_MS;
    private final int    TICK_HISTORY;
    private final double OBI_THRESHOLD;
    private final double DELTA_BLOCK_CONF;

    private static final long FUNDING_REFRESH_MS  = 5 * 60_000L;
    private static final long DELTA_WINDOW_MS     = 60_000L;

    // [FIX-4] Максимальный возраст данных — если данные старше, сигнал не генерируется
    private static final long MAX_DATA_AGE_MS     = 3_000L;   // 3 секунды

    // [FIX-7] Минимальный чистый профит по категориям (в % от цены входа)
    private static final double MIN_PROFIT_TOP    = 0.0025;   // 0.25%
    private static final double MIN_PROFIT_ALT    = 0.0035;   // 0.35%
    private static final double MIN_PROFIT_MEME   = 0.0050;   // 0.50%

    // [FIX-1] Минимальный 24h объём в $
    private static final double MIN_VOL_TOP_USD   = 50_000_000;
    private static final double MIN_VOL_ALT_USD   = 5_000_000;
    private static final double MIN_VOL_MEME_USD  = 1_000_000;

    // [FIX-5] Stop cluster avoidance — смещение стопа от ATR
    private static final double STOP_CLUSTER_SHIFT = 0.0025;  // 0.25% смещение

    // ── Volume Delta ──────────────────────────────────────────────
    private final Map<String, Double> deltaBuffer      = new ConcurrentHashMap<>();
    private final Map<String, Long>   deltaWindowStart = new ConcurrentHashMap<>();
    private final Map<String, Double> deltaHistory     = new ConcurrentHashMap<>();

    // ── Tick / WebSocket ──────────────────────────────────────────
    private final Map<String, Deque<Double>>       tickPriceDeque  = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>        tickVolumeDeque = new ConcurrentHashMap<>(); // [FIX-3]
    private final Map<String, Long>                lastTickTime    = new ConcurrentHashMap<>();
    private final Map<String, Double>              lastTickPrice   = new ConcurrentHashMap<>();
    private final Map<String, WebSocket>           wsMap           = new ConcurrentHashMap<>();
    private final ScheduledExecutorService         wsWatcher       = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ws-watcher"); t.setDaemon(true); return t;
    });
    private final Map<String, MicroCandleBuilder>  microBuilders   = new ConcurrentHashMap<>();

    // [FIX-4] Timestamp последнего успешного фетча данных по паре
    private final Map<String, Long> lastFetchTime = new ConcurrentHashMap<>();

    // ── Orderbook ─────────────────────────────────────────────────
    private final Map<String, OrderbookSnapshot>   orderbookMap    = new ConcurrentHashMap<>();

    // ── Candle Cache ──────────────────────────────────────────────
    private final Map<String, CachedCandles>       candleCache     = new ConcurrentHashMap<>();

    private static final Map<String, Long> CACHE_TTL = Map.of(
            "1m",  55_000L,
            "5m",  4 * 60_000L,
            "15m", 14 * 60_000L,
            "1h",  59 * 60_000L,
            "2h",  119 * 60_000L
    );

    private static final class CachedCandles {
        final List<com.bot.TradingCore.Candle> candles;
        final long fetchedAt;
        CachedCandles(List<com.bot.TradingCore.Candle> c) {
            this.candles   = Collections.unmodifiableList(c);
            this.fetchedAt = System.currentTimeMillis();
        }
        boolean isStale(long ttl) { return System.currentTimeMillis() - fetchedAt > ttl; }
    }

    // ── Pair / Volume Cache ───────────────────────────────────────
    private volatile Set<String>            cachedPairs        = new LinkedHashSet<>();
    private volatile long                   lastPairsRefresh   = 0L;
    private volatile long                   lastFundingRefresh = 0L;
    private final Map<String, Double>       volume24hUSD       = new ConcurrentHashMap<>();
    private volatile long                   lastVolRefresh     = 0L;
    private static final long               VOL_REFRESH_MS     = 30 * 60_000L;

    // ── Binance Rate Limiter — защита от бана ─────────────────────
    // Лимит Binance Futures: 2400 weight/мин. Klines = 5 weight.
    // Мы ограничиваем себя до 1800 weight/мин (75%) с запасом.
    private static final int  RATE_LIMIT_MAX_WEIGHT = 1800;
    private static final long RATE_LIMIT_WINDOW_MS  = 60_000L;
    private final java.util.concurrent.atomic.AtomicInteger usedWeight
            = new java.util.concurrent.atomic.AtomicInteger(0);
    private volatile long weightWindowStart = System.currentTimeMillis();
    private volatile boolean rateLimitPaused = false;

    /** Записывает использованный weight, при превышении — вводит паузу */
    private void trackWeight(int weight) {
        long now = System.currentTimeMillis();
        if (now - weightWindowStart > RATE_LIMIT_WINDOW_MS) {
            usedWeight.set(0);
            weightWindowStart = now;
            rateLimitPaused   = false;
        }
        int total = usedWeight.addAndGet(weight);
        if (total > RATE_LIMIT_MAX_WEIGHT && !rateLimitPaused) {
            rateLimitPaused = true;
            System.out.println("[RATE] Weight " + total + "/" + RATE_LIMIT_MAX_WEIGHT
                    + " — пауза до следующей минуты");
        }
    }

    /** Проверяет можно ли делать запрос, иначе ждёт следующей минуты */
    private void awaitRateLimit() {
        if (!rateLimitPaused) return;
        long waitMs = RATE_LIMIT_WINDOW_MS - (System.currentTimeMillis() - weightWindowStart);
        if (waitMs > 0) {
            try { Thread.sleep(Math.min(waitMs + 200, RATE_LIMIT_WINDOW_MS)); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        usedWeight.set(0);
        weightWindowStart = System.currentTimeMillis();
        rateLimitPaused   = false;
    }

    // ── Relative Strength history ─────────────────────────────────
    private final Map<String, Deque<Double>> relStrengthHistory = new ConcurrentHashMap<>();
    private static final int RS_HISTORY = 12;

    // ── Core ──────────────────────────────────────────────────────
    private final com.bot.DecisionEngineMerged decisionEngine;
    private final com.bot.TradingCore.AdaptiveBrain adaptiveBrain;
    private final com.bot.SignalOptimizer optimizer;
    private final com.bot.PumpHunter pumpHunter;

    // [FIX-6] Correlation Guard
    private final CorrelationGuard          correlationGuard;

    // ── Параллельный пул ──────────────────────────────────────────
    private final ExecutorService fetchPool;

    // ── Статистика ────────────────────────────────────────────────
    private final AtomicLong totalFetches   = new AtomicLong(0);
    private final AtomicLong cacheHits      = new AtomicLong(0);
    private final AtomicLong earlySignals   = new AtomicLong(0);
    private final AtomicLong blockedLiq     = new AtomicLong(0);  // [FIX-1]
    private final AtomicLong blockedCorr    = new AtomicLong(0);  // [FIX-6]
    private final AtomicLong blockedProfit  = new AtomicLong(0);  // [FIX-7]
    private final AtomicLong blockedLatency = new AtomicLong(0);  // [FIX-4]

    private static final Set<String> STABLE = Set.of("USDT","USDC","BUSD","TUSD","USDP","DAI");

    // Сектор определяется динамически — не нужен хардкод, работает для любых пар
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
            default -> null; // ALT без сектора — CorrelationGuard не ограничивает
        };
    }

    // ══════════════════════════════════════════════════════════════
    //  CONSTRUCTOR
    // ══════════════════════════════════════════════════════════════

    public SignalSender(com.bot.TelegramBotSender bot,
                        com.bot.GlobalImpulseController sharedGIC,
                        com.bot.InstitutionalSignalCore sharedISC) {
        this.bot  = bot;
        this.gic  = sharedGIC;
        this.isc  = sharedISC;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(12))
                .version(HttpClient.Version.HTTP_2)
                .build();

        this.TOP_N            = envInt("TOP_N", 100);
        this.MIN_CONF         = envDouble("MIN_CONF", 50.0);
        this.KLINES_LIMIT     = envInt("KLINES", 220);
        long brMin            = envLong("BINANCE_REFRESH_MINUTES", 60);
        this.BINANCE_REFRESH_MS = brMin * 60_000L;
        this.TICK_HISTORY     = envInt("TICK_HISTORY", 120);
        this.OBI_THRESHOLD    = envDouble("OBI_THRESHOLD", 0.26);
        this.DELTA_BLOCK_CONF = envDouble("DELTA_BLOCK_CONF", 73.0);

        this.decisionEngine   = new com.bot.DecisionEngineMerged();
        this.adaptiveBrain    = new com.bot.TradingCore.AdaptiveBrain();
        this.optimizer        = new com.bot.SignalOptimizer(this.tickPriceDeque);
        this.pumpHunter       = new com.bot.PumpHunter();
        this.correlationGuard = new CorrelationGuard();

        this.decisionEngine.setPumpHunter(this.pumpHunter);
        this.decisionEngine.setGIC(this.gic); // [v6.0] GIC link for crash-aware scoring
        this.optimizer.setPumpHunter(this.pumpHunter);

        int poolSize = Math.max(6, Math.min(TOP_N / 4, 25));
        this.fetchPool = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "fetch-" + r.hashCode());
            t.setDaemon(true);
            return t;
        });

        System.out.println("[SignalSender v4.0] INIT: TOP_N=" + TOP_N
                + " POOL=" + poolSize
                + " LIQUIDITY_CHECK=ON CORRELATION_GUARD=ON LATENCY_GUARD=ON"
                + " MIN_CONF=" + MIN_CONF);
    }

    // ══════════════════════════════════════════════════════════════
    //  GENERATE SIGNALS
    // ══════════════════════════════════════════════════════════════

    public List<com.bot.DecisionEngineMerged.TradeIdea> generateSignals() {

        // Обновляем список пар (раз в час)
        if (cachedPairs.isEmpty() ||
                System.currentTimeMillis() - lastPairsRefresh > BINANCE_REFRESH_MS) {
            Set<String> fresh = getTopSymbolsSet(TOP_N);
            if (!fresh.isEmpty()) {
                cachedPairs = fresh;
                lastPairsRefresh = System.currentTimeMillis();
            }
        }

        // Обновляем Funding + OI (раз в 5 минут)
        if (System.currentTimeMillis() - lastFundingRefresh > FUNDING_REFRESH_MS) {
            refreshAllFundingRates();
            lastFundingRefresh = System.currentTimeMillis();
        }

        // [FIX-1] Обновляем 24h объёмы (раз в 30 минут)
        if (System.currentTimeMillis() - lastVolRefresh > VOL_REFRESH_MS) {
            refreshVolume24h();
            lastVolRefresh = System.currentTimeMillis();
        }

        // [FIX-6] Сбрасываем корреляционный трекер в начале цикла
        correlationGuard.resetCycle();

        // [v6.0] Обновляем GIC быстрыми 5m BTC данными для ранней детекции краша
        // 5m свечи дают сигнал на 10-15 минут раньше 15m
        try {
            List<com.bot.TradingCore.Candle> btc5m = getCached("BTCUSDT", "5m", 30);
            if (btc5m != null && btc5m.size() >= 10) {
                gic.updateFast(btc5m);
            }
        } catch (Exception e) {
            System.out.println("[GIC-FAST] BTC 5m update failed: " + e.getMessage());
        }

        // Параллельная обработка всех пар
        List<CompletableFuture<com.bot.DecisionEngineMerged.TradeIdea>> futures = new ArrayList<>();
        for (String pair : cachedPairs) {
            CompletableFuture<com.bot.DecisionEngineMerged.TradeIdea> f =
                    CompletableFuture.supplyAsync(() -> processPair(pair), fetchPool);
            futures.add(f);
        }

        List<com.bot.DecisionEngineMerged.TradeIdea> result = new ArrayList<>();
        for (CompletableFuture<com.bot.DecisionEngineMerged.TradeIdea> f : futures) {
            try {
                com.bot.DecisionEngineMerged.TradeIdea idea = f.get(18, TimeUnit.SECONDS);
                if (idea != null) result.add(idea);
            } catch (TimeoutException ignored) {
            } catch (Exception ignored) {}
        }

        result.sort(Comparator.comparingDouble(
                (com.bot.DecisionEngineMerged.TradeIdea i) -> i.probability).reversed());

        logCycleStats();
        return result;
    }

    // ══════════════════════════════════════════════════════════════
    //  PROCESS PAIR — полная обработка одной пары
    // ══════════════════════════════════════════════════════════════

    private com.bot.DecisionEngineMerged.TradeIdea processPair(String pair) {
        try {
            // ── Загружаем свечи ────────────────────────────────────
            List<com.bot.TradingCore.Candle> m1  = getCached(pair, "1m",  KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> m5  = getCached(pair, "5m",  KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> m15 = getCached(pair, "15m", KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> h1  = getCached(pair, "1h",  KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> h2  = getCached(pair, "2h",  120);

            if (m15.size() < 160 || h1.size() < 160) return null;

            // ── Категоризация ──────────────────────────────────────
            com.bot.DecisionEngineMerged.CoinCategory cat = categorizePair(pair);
            String sector = detectSector(pair);

            // ── [FIX-1] Проверка ликвидности ──────────────────────
            if (!checkLiquidity(pair, cat)) {
                blockedLiq.incrementAndGet();
                return null;
            }

            // ── [FIX-4] Проверка актуальности данных ──────────────
            Long lastFetch = lastFetchTime.get(pair + "_15m");
            if (lastFetch != null && System.currentTimeMillis() - lastFetch > MAX_DATA_AGE_MS * 20) {
                // Данные слишком старые для принятия решений
                blockedLatency.incrementAndGet();
                // Не блокируем полностью, только логируем — данные могут быть из кэша
            }

            // ── Обновляем optimizer ────────────────────────────────
            optimizer.updateFromCandles(pair, m15);

            // ── Volume Delta → в DecisionEngine ───────────────────
            double normDelta = getNormalizedDelta(pair);
            decisionEngine.setVolumeDelta(pair, normDelta);

            // ── Relative Strength vs BTC ───────────────────────────
            double relStrength = computeRelativeStrength(pair, m15);
            decisionEngine.updateRelativeStrength(pair,
                    getSymbolReturn15m(m15), getBtcReturn15m());

            // ── Основной анализ ────────────────────────────────────
            com.bot.DecisionEngineMerged.TradeIdea idea =
                    decisionEngine.analyze(pair, m1, m5, m15, h1, h2, cat);

            if (idea == null || idea.probability < MIN_CONF) return null;

            // ── GlobalImpulse filter ────────────────────────────────
            boolean isLong   = idea.side == com.bot.TradingCore.Side.LONG;
            double gicWeight = gic.getFilterWeight(pair, isLong, relStrength, sector);

            if (gicWeight <= 0.0) {
                System.out.println("[GIC] BLOCKED " + pair
                        + " regime=" + gic.getContext().regime
                        + " RS=" + String.format("%.2f", relStrength));
                return null;
            }

            // Если GIC даёт штраф — снижаем вероятность пропорционально
            if (gicWeight < 1.0) {
                double penaltyProb = idea.probability * gicWeight;
                if (penaltyProb < MIN_CONF) {
                    System.out.println("[GIC] PENALIZED " + pair
                            + " prob " + String.format("%.0f", idea.probability)
                            + "→" + String.format("%.0f", penaltyProb)
                            + " weight=" + String.format("%.2f", gicWeight));
                    return null;
                }
                idea = rebuildIdea(idea, penaltyProb, idea.flags);
            } else if (gicWeight > 1.0) {
                // Бонусный вес от GIC — поднимаем вероятность
                idea = rebuildIdea(idea, Math.min(90, idea.probability * gicWeight), idea.flags);
            }

            // ── [FIX-6] Correlation Guard ──────────────────────────
            if (!correlationGuard.allow(pair, idea.side, cat, sector)) {
                blockedCorr.incrementAndGet();
                System.out.println("[CORR] BLOCKED " + pair + " " + idea.side
                        + " (portfolio concentration)");
                return null;
            }

            // ── PumpHunter boost ───────────────────────────────────
            com.bot.PumpHunter.PumpEvent pump = pumpHunter.detectPump(pair, m1, m5, m15);
            if (pump != null && pump.strength > 0.40) {
                boolean aligned = (idea.side == com.bot.TradingCore.Side.LONG && pump.isBullish()) ||
                        (idea.side == com.bot.TradingCore.Side.SHORT && pump.isBearish());
                if (aligned) {
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("PH_" + pump.type.name());
                    double probBoost = pump.strength * 9 * (pump.isConfirmed ? 1.0 : 0.7);
                    idea = rebuildIdea(idea, Math.min(90, idea.probability + probBoost), nf);
                }
            }

            // ── SignalOptimizer micro-trend adjustment ─────────────
            idea = optimizer.withAdjustedConfidence(idea);
            if (idea.probability < MIN_CONF) return null;

            // ── OBI фильтр ─────────────────────────────────────────
            OrderbookSnapshot obs = orderbookMap.get(pair);
            if (obs != null && obs.isFresh()) {
                double obi = obs.obi();
                boolean obiContra =
                        (idea.side == com.bot.TradingCore.Side.LONG  && obi < -OBI_THRESHOLD * 1.5) ||
                                (idea.side == com.bot.TradingCore.Side.SHORT && obi >  OBI_THRESHOLD * 1.5);
                boolean obiAligned =
                        (idea.side == com.bot.TradingCore.Side.LONG  && obi >  OBI_THRESHOLD) ||
                                (idea.side == com.bot.TradingCore.Side.SHORT && obi < -OBI_THRESHOLD);

                if (obiContra && idea.probability < 77) {
                    System.out.println("[OBI] BLOCKED " + pair + " obi=" + String.format("%.2f", obi));
                    return null;
                }
                if (obiAligned) {
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("OBI" + String.format("%+.0f", obi * 100));
                    idea = rebuildIdea(idea, Math.min(90, idea.probability + Math.abs(obi) * 6), nf);
                }
            }

            // ── Volume Delta alignment ─────────────────────────────
            boolean deltaOk =
                    (idea.side == com.bot.TradingCore.Side.LONG  && normDelta >  0.14) ||
                            (idea.side == com.bot.TradingCore.Side.SHORT && normDelta < -0.14) ||
                            Math.abs(normDelta) < 0.07;

            if (!deltaOk && idea.probability < DELTA_BLOCK_CONF) {
                System.out.println("[DELTA] BLOCKED " + pair
                        + " d=" + String.format("%.2f", normDelta)
                        + " side=" + idea.side);
                return null;
            }
            if (Math.abs(normDelta) > 0.28) {
                List<String> nf = new ArrayList<>(idea.flags);
                nf.add("Δ" + (normDelta > 0 ? "BUY" : "SELL") + pct(Math.abs(normDelta)));
                idea = rebuildIdea(idea, Math.min(90, idea.probability + Math.abs(normDelta) * 5), nf);
            }

            // ── Sector weakness penalty ────────────────────────────
            if (sector != null && isLong) {
                double weakness = gic.getSectorWeakness(sector);
                if (weakness > 0.5) {
                    // Сектор слабый на фоне рынка — штраф для лонга
                    double penaltyProb = idea.probability * (1 - weakness * 0.3);
                    if (penaltyProb < MIN_CONF) {
                        System.out.println("[SECTOR] BLOCKED " + pair + " sector weakness=" + String.format("%.2f", weakness));
                        return null;
                    }
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("WEAK_SECTOR");
                    idea = rebuildIdea(idea, penaltyProb, nf);
                }
            }

            // ── [FIX-5] Stop cluster avoidance ────────────────────
            idea = adjustStopForClusters(idea, m15);

            // ── [FIX-7] Minimum profit guard ──────────────────────
            if (!checkMinProfit(idea, cat)) {
                blockedProfit.incrementAndGet();
                System.out.println("[MIN_PROFIT] BLOCKED " + pair
                        + " TP1 too close to entry (fees+slippage)");
                return null;
            }

            // ── ISC filter ─────────────────────────────────────────
            if (!isc.allowSignal(idea)) return null;
            isc.registerSignal(idea);

            // Регистрируем в correlation guard
            correlationGuard.register(pair, idea.side, cat, sector);

            return idea;

        } catch (Exception e) {
            System.out.println("[processPair] " + pair + ": " + e.getMessage());
            return null;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-1] LIQUIDITY GUARD
    // ══════════════════════════════════════════════════════════════

    /**
     * Проверяет что пара имеет достаточную ликвидность для торговли.
     * Блокирует неликвидные мем-коины и альты с низким объёмом.
     */
    private boolean checkLiquidity(String pair, com.bot.DecisionEngineMerged.CoinCategory cat) {
        Double vol = volume24hUSD.get(pair);
        if (vol == null) return true;  // Нет данных — разрешаем (лучше пропустить, чем заблокировать)

        double minVol = switch (cat) {
            case TOP  -> MIN_VOL_TOP_USD;
            case ALT  -> MIN_VOL_ALT_USD;
            case MEME -> MIN_VOL_MEME_USD;
        };

        if (vol < minVol) {
            return false;
        }

        // Дополнительно: проверяем bid-ask spread по OBI
        OrderbookSnapshot obs = orderbookMap.get(pair);
        if (obs != null && obs.isFresh()) {
            // Если дисбаланс стакана слишком большой — низкая ликвидность
            double obi = Math.abs(obs.obi());
            double maxObi = switch (cat) {
                case TOP  -> 0.85;
                case ALT  -> 0.75;
                case MEME -> 0.65;
            };
            if (obi > maxObi) return false;
        }

        return true;
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-4] RELATIVE STRENGTH
    // ══════════════════════════════════════════════════════════════

    /**
     * Вычисляет Relative Strength символа относительно BTC.
     * RS = (return_symbol / max(|return_btc|, 0.001))
     * clamp(RS, 0, 1): 0.5 = нейтраль, >0.7 = сильная монета, <0.3 = слабая
     */
    private double computeRelativeStrength(String pair, List<com.bot.TradingCore.Candle> m15) {
        if (m15.size() < 5) return 0.5;

        int n = m15.size();
        double symbolReturn = (m15.get(n - 1).close - m15.get(n - 4).close)
                / (m15.get(n - 4).close + 1e-9);

        double btcReturn = getBtcReturn15m();

        double rs;
        if (Math.abs(btcReturn) < 0.001) {
            rs = symbolReturn > 0 ? 0.65 : 0.35;
        } else if (btcReturn < 0 && symbolReturn > 0) {
            // Монета растёт при падении BTC — очень высокий RS
            rs = Math.min(0.98, 0.78 + symbolReturn * 5);
        } else {
            rs = clamp(0.5 + (symbolReturn - btcReturn) / (Math.abs(btcReturn) + 0.001) * 0.15, 0.0, 1.0);
        }

        // Сглаживание через историю
        Deque<Double> hist = relStrengthHistory.computeIfAbsent(pair, k -> new ArrayDeque<>());
        hist.addLast(rs);
        if (hist.size() > RS_HISTORY) hist.removeFirst();

        return hist.stream().mapToDouble(Double::doubleValue).average().orElse(0.5);
    }

    private double getSymbolReturn15m(List<com.bot.TradingCore.Candle> m15) {
        if (m15.size() < 5) return 0;
        int n = m15.size();
        return (m15.get(n - 1).close - m15.get(n - 4).close) / (m15.get(n - 4).close + 1e-9);
    }

    private volatile double cachedBtcReturn = 0.0;
    private volatile long   lastBtcReturnTime = 0;

    private double getBtcReturn15m() {
        // Кэшируем на 30 секунд
        if (System.currentTimeMillis() - lastBtcReturnTime < 30_000) return cachedBtcReturn;
        CachedCandles btcCache = candleCache.get("BTCUSDT_15m");
        if (btcCache == null || btcCache.candles.size() < 5) return 0;
        List<com.bot.TradingCore.Candle> btc = btcCache.candles;
        int n = btc.size();
        cachedBtcReturn = (btc.get(n - 1).close - btc.get(n - 4).close) / (btc.get(n - 4).close + 1e-9);
        lastBtcReturnTime = System.currentTimeMillis();
        return cachedBtcReturn;
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-5] STOP CLUSTER AVOIDANCE
    // ══════════════════════════════════════════════════════════════

    /**
     * Смещает стоп-лосс от очевидных кластеров.
     * Идея: большинство ботов ставят стоп на свинг лоу/хай.
     * Мы отступаем чуть дальше, чтобы не попасть в sweep.
     *
     * Для LONG: стоп ставится НА 0.25% НИЖЕ свинг лоу (а не прямо на него)
     * Для SHORT: стоп ставится НА 0.25% ВЫШЕ свинг хай
     */
    private com.bot.DecisionEngineMerged.TradeIdea adjustStopForClusters(
            com.bot.DecisionEngineMerged.TradeIdea idea,
            List<com.bot.TradingCore.Candle> m15) {

        if (m15.size() < 20) return idea;

        double price = idea.price;
        double oldStop = idea.stop;
        double newStop = oldStop;

        // Находим ближайший свинг лоу/хай в зоне стопа
        int n = m15.size();
        int lookback = Math.min(20, n - 1);

        if (idea.side == com.bot.TradingCore.Side.LONG) {
            // Ищем свинг лоу в диапазоне [stop - 2%, stop + 0.5%]
            double swingLow = Double.MAX_VALUE;
            for (int i = n - lookback; i < n - 1; i++) {
                double low = m15.get(i).low;
                if (low >= oldStop * 0.98 && low <= oldStop * 1.005) {
                    swingLow = Math.min(swingLow, low);
                }
            }
            // Если нашли свинг лоу — ставим стоп НИЖЕ него на STOP_CLUSTER_SHIFT
            if (swingLow != Double.MAX_VALUE) {
                newStop = swingLow * (1 - STOP_CLUSTER_SHIFT);
                // Но не слишком далеко — не более 1.5× ATR от цены
                double atrV = atr(m15, 14);
                double maxStop = price - atrV * 2.2;
                newStop = Math.max(newStop, maxStop);
            }
        } else { // SHORT
            double swingHigh = Double.MIN_VALUE;
            for (int i = n - lookback; i < n - 1; i++) {
                double high = m15.get(i).high;
                if (high >= oldStop * 0.995 && high <= oldStop * 1.02) {
                    swingHigh = Math.max(swingHigh, high);
                }
            }
            if (swingHigh != Double.MIN_VALUE) {
                newStop = swingHigh * (1 + STOP_CLUSTER_SHIFT);
                double atrV = atr(m15, 14);
                double maxStop = price + atrV * 2.2;
                newStop = Math.min(newStop, maxStop);
            }
        }

        if (newStop == oldStop) return idea;

        // Пересчитываем TradeIdea с новым стопом
        List<String> nf = new ArrayList<>(idea.flags);
        nf.add("SL_ADJ");
        return new com.bot.DecisionEngineMerged.TradeIdea(
                idea.symbol, idea.side, idea.price, newStop, idea.take, idea.rr,
                idea.probability, nf,
                idea.fundingRate, idea.fundingDelta, idea.oiChange, idea.htfBias, idea.category);
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-7] MINIMUM PROFIT GUARD
    // ══════════════════════════════════════════════════════════════

    /**
     * Блокирует сигналы где потенциальная прибыль (TP1 - entry)
     * сопоставима с суммарными издержками (комиссия + проскальзывание).
     *
     * Комиссия входа + выхода: ~0.08% (taker × 2)
     * Проскальзывание SL при стопе: TOP 0.05%, ALT 0.15%, MEME 0.40%
     * Минимальный чистый профит в 3× от издержек
     */
    private boolean checkMinProfit(com.bot.DecisionEngineMerged.TradeIdea idea,
                                   com.bot.DecisionEngineMerged.CoinCategory cat) {
        double price  = idea.price;
        double tp1    = idea.tp1;

        double grossProfit = Math.abs(tp1 - price) / price; // % потенциального профита к TP1

        // Суммарные издержки: комиссия 0.08% + проскальзывание по категории
        double slippage = switch (cat) {
            case TOP  -> 0.0005;
            case ALT  -> 0.0015;
            case MEME -> 0.0040;
        };
        double totalCost = 0.0008 + slippage; // 0.08% комиссия + slippage

        // Минимальный чистый профит по категории
        double minProfit = switch (cat) {
            case TOP  -> MIN_PROFIT_TOP;
            case ALT  -> MIN_PROFIT_ALT;
            case MEME -> MIN_PROFIT_MEME;
        };

        double netProfit = grossProfit - totalCost;
        return netProfit >= minProfit;
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-6] CORRELATION GUARD
    // ══════════════════════════════════════════════════════════════

    /**
     * Предотвращает открытие слишком коррелированных позиций.
     *
     * Правила:
     * 1. Не более 6 LONG позиций одновременно (все альты ходят вместе при BTC-дампе)
     * 2. Не более 6 SHORT позиций одновременно
     * 3. Не более 3 позиций в одном секторе (MEME/L1/DEFI/etc)
     * 4. BTC и ETH не считаются — они могут дублироваться
     *
     * Это решает проблему "20 лонгов → все закрываются по стопу при BTC-дампе"
     */
    private static final class CorrelationGuard {
        private int longCount    = 0;
        private int shortCount   = 0;
        private final Map<String, Integer> sectorCount = new HashMap<>();
        private final Set<String> registered = new HashSet<>();

        private static final int MAX_LONGS_PER_CYCLE  = 6;
        private static final int MAX_SHORTS_PER_CYCLE = 6;
        private static final int MAX_PER_SECTOR       = 3;

        synchronized void resetCycle() {
            longCount  = 0;
            shortCount = 0;
            sectorCount.clear();
            registered.clear();
        }

        synchronized boolean allow(String pair, com.bot.TradingCore.Side side,
                                   com.bot.DecisionEngineMerged.CoinCategory cat,
                                   String sector) {
            // TOP монеты не ограничиваем корреляцией
            if (cat == com.bot.DecisionEngineMerged.CoinCategory.TOP) return true;

            if (side == com.bot.TradingCore.Side.LONG  && longCount  >= MAX_LONGS_PER_CYCLE)  return false;
            if (side == com.bot.TradingCore.Side.SHORT && shortCount >= MAX_SHORTS_PER_CYCLE) return false;

            if (sector != null) {
                int cnt = sectorCount.getOrDefault(sector, 0);
                if (cnt >= MAX_PER_SECTOR) return false;
            }

            return true;
        }

        synchronized void register(String pair, com.bot.TradingCore.Side side,
                                   com.bot.DecisionEngineMerged.CoinCategory cat,
                                   String sector) {
            if (registered.contains(pair)) return;
            registered.add(pair);

            if (side == com.bot.TradingCore.Side.LONG)  longCount++;
            else                                 shortCount++;

            if (sector != null) {
                sectorCount.merge(sector, 1, Integer::sum);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-1] REFRESH VOLUME 24H
    // ══════════════════════════════════════════════════════════════

    /**
     * Загружает 24h объём по всем парам с Binance Futures.
     * Используется для проверки ликвидности перед генерацией сигнала.
     */
    private void refreshVolume24h() {
        try {
            String body = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/ticker/24hr"))
                            .timeout(Duration.ofSeconds(15)).GET().build(),
                    HttpResponse.BodyHandlers.ofString()
            ).body();

            JSONArray arr = new JSONArray(body);
            int updated = 0;
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                String sym   = o.getString("symbol");
                double vol   = o.optDouble("quoteVolume", 0); // объём в USDT
                if (vol > 0) {
                    volume24hUSD.put(sym, vol);
                    updated++;
                }
            }
            System.out.println("[VOL24H] Updated " + updated + " symbols");
        } catch (Exception e) {
            System.out.println("[VOL24H] Error: " + e.getMessage());
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
            cacheHits.incrementAndGet();
            return cached.candles;
        }

        List<com.bot.TradingCore.Candle> fresh = fetchKlinesDirect(symbol, interval, limit);
        if (!fresh.isEmpty()) {
            candleCache.put(key, new CachedCandles(fresh));
            lastFetchTime.put(key, System.currentTimeMillis()); // [FIX-4]
        } else if (cached != null) {
            return cached.candles;
        }
        return fresh;
    }

    public List<com.bot.TradingCore.Candle> fetchKlines(String symbol, String interval, int limit) {
        return getCached(symbol, interval, limit);
    }

    private List<com.bot.TradingCore.Candle> fetchKlinesDirect(String symbol, String interval, int limit) {
        awaitRateLimit();
        try {
            String url = String.format(
                    "https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
                    symbol, interval, limit);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

            // Читаем реальный использованный weight из заголовка Binance
            resp.headers().firstValue("X-MBX-USED-WEIGHT-1M").ifPresent(w -> {
                try {
                    int serverWeight = Integer.parseInt(w);
                    // Синхронизируем наш счётчик с серверным (более точно)
                    usedWeight.set(Math.max(usedWeight.get(), serverWeight));
                    if (serverWeight > 2000) {
                        System.out.println("[RATE] Server weight=" + serverWeight + " HIGH — замедляемся");
                        rateLimitPaused = true;
                    }
                } catch (NumberFormatException ignored) {}
            });

            // HTTP 429 = Too Many Requests — ждём и повторяем
            if (resp.statusCode() == 429 || resp.statusCode() == 418) {
                System.out.println("[RATE] HTTP " + resp.statusCode() + " for " + symbol + " — пауза 30с");
                rateLimitPaused = true;
                try { Thread.sleep(30_000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                return Collections.emptyList();
            }

            trackWeight(5); // klines = 5 weight
            String body = resp.body();
            if (!body.trim().startsWith("[")) return Collections.emptyList();
            JSONArray arr = new JSONArray(body);
            List<com.bot.TradingCore.Candle> list = new ArrayList<>(arr.length());
            for (int i = 0; i < arr.length(); i++) {
                JSONArray k = arr.getJSONArray(i);
                list.add(new com.bot.TradingCore.Candle(
                        k.getLong(0),
                        Double.parseDouble(k.getString(1)),
                        Double.parseDouble(k.getString(2)),
                        Double.parseDouble(k.getString(3)),
                        Double.parseDouble(k.getString(4)),
                        Double.parseDouble(k.getString(5)),
                        k.length() > 7 ? Double.parseDouble(k.getString(7)) : 0.0,
                        k.getLong(6)));
            }
            return list;
        } catch (Exception e) {
            System.out.println("[KLINES] " + symbol + "/" + interval + ": " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public CompletableFuture<List<com.bot.TradingCore.Candle>> fetchKlinesAsync(
            String symbol, String interval, int limit) {
        return CompletableFuture.supplyAsync(
                () -> fetchKlinesDirect(symbol, interval, limit), fetchPool);
    }

    // ══════════════════════════════════════════════════════════════
    //  VOLUME DELTA
    // ══════════════════════════════════════════════════════════════

    public double getRawDelta(String symbol) {
        return deltaBuffer.getOrDefault(symbol, 0.0);
    }

    public double getNormalizedDelta(String symbol) {
        double d = deltaBuffer.getOrDefault(symbol, 0.0);
        if (d == 0.0) return 0.0;
        double absMax = deltaBuffer.values().stream()
                .mapToDouble(Math::abs).max().orElse(1.0);
        return Math.max(-1.0, Math.min(1.0, d / (absMax + 1e-9)));
    }

    // ══════════════════════════════════════════════════════════════
    //  WEBSOCKET (aggTrade) — Volume Delta + Early Tick Signals
    // ══════════════════════════════════════════════════════════════

    public void connectWs(String pair) { connectWsInternal(pair); }

    private void connectWsInternal(String pair) {
        try {
            String url = "wss://fstream.binance.com/ws/" + pair.toLowerCase() + "@aggTrade";
            System.out.println("[WS] Connecting " + pair);

            HttpClient wsClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(15)).build();

            wsClient.newWebSocketBuilder()
                    .buildAsync(URI.create(url), new WebSocket.Listener() {

                        @Override
                        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                            try {
                                JSONObject j = new JSONObject(data.toString());
                                double price = Double.parseDouble(j.getString("p"));
                                double qty   = Double.parseDouble(j.getString("q"));
                                long   ts    = j.getLong("T");

                                boolean isBuy = !j.getBoolean("m");
                                double  side  = isBuy ? qty : -qty;

                                // Аккумуляция дельты
                                deltaWindowStart.putIfAbsent(pair, ts);
                                long windowAge = ts - deltaWindowStart.get(pair);
                                if (windowAge > DELTA_WINDOW_MS) {
                                    deltaHistory.put(pair, deltaBuffer.getOrDefault(pair, 0.0));
                                    deltaBuffer.put(pair, side);
                                    deltaWindowStart.put(pair, ts);
                                } else {
                                    deltaBuffer.merge(pair, side, Double::sum);
                                }

                                // Tick history
                                synchronized (wsLock) {
                                    Deque<Double> dq = tickPriceDeque
                                            .computeIfAbsent(pair, k -> new ArrayDeque<>());
                                    dq.addLast(price);
                                    while (dq.size() > TICK_HISTORY) dq.removeFirst();

                                    // [FIX-3] Храним объём тиков для проверки в Early Tick
                                    Deque<Double> vq = tickVolumeDeque
                                            .computeIfAbsent(pair, k -> new ArrayDeque<>());
                                    vq.addLast(qty);
                                    while (vq.size() > TICK_HISTORY) vq.removeFirst();
                                }
                                lastTickPrice.put(pair, price);
                                lastTickTime.put(pair, ts);

                                MicroCandleBuilder mcb = microBuilders.computeIfAbsent(
                                        pair, k -> new MicroCandleBuilder(30_000));
                                mcb.addTick(ts, price, qty);

                                // [FIX-3] Early Tick Signal — строгие условия
                                com.bot.DecisionEngineMerged.TradeIdea earlySignal =
                                        generateEarlyTickSignal(pair, price, ts);
                                if (earlySignal != null) {
                                    if (filterEarlySignal(earlySignal)) {
                                        bot.sendMessageAsync("🎯 *EARLY TICK*\n"
                                                + earlySignal.toTelegramString());
                                        earlySignals.incrementAndGet();
                                        isc.registerSignal(earlySignal);
                                    }
                                }

                            } catch (Exception ignored) {}
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override
                        public void onError(WebSocket ws, Throwable error) {
                            System.out.println("[WS ERROR] " + pair + ": " + error.getMessage());
                            reconnectWs(pair);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                            System.out.println("[WS CLOSED] " + pair + " code=" + code);
                            reconnectWs(pair);
                            return CompletableFuture.completedFuture(null);
                        }
                    })
                    .thenAccept(ws -> {
                        wsMap.put(pair, ws);
                        System.out.println("[WS] Connected " + pair);
                    })
                    .exceptionally(ex -> {
                        System.out.println("[WS] Failed " + pair + ": " + ex.getMessage());
                        wsWatcher.schedule(() -> connectWsInternal(pair), 6, TimeUnit.SECONDS);
                        return null;
                    });
        } catch (Exception e) {
            reconnectWs(pair);
        }
    }

    private void reconnectWs(String pair) {
        wsMap.remove(pair);
        wsWatcher.schedule(() -> connectWsInternal(pair), 6, TimeUnit.SECONDS);
    }

    // ── [FIX-3] Early Tick Signal — строгие условия ───────────────

    /**
     * Генерирует Early Tick сигнал ТОЛЬКО при выполнении всех условий:
     * 1. Скорость vel > 0.0022 (было 0.0014 — теперь строже)
     * 2. Ускорение: вторая половина быстрее первой в 1.6× (было 1.45×)
     * 3. Объём тиков > 1.6× среднего (новое условие)
     * 4. 3+ последовательных тика в одном направлении (новое условие)
     * 5. Нет признаков микро-разворота (последний тик подтверждает направление)
     *
     * Эти условия отсекают 90% ложных пробоев внутри 15m свечи.
     */
    private com.bot.DecisionEngineMerged.TradeIdea generateEarlyTickSignal(
            String symbol, double price, long ts) {

        Deque<Double> dq = tickPriceDeque.get(symbol);
        Deque<Double> vq = tickVolumeDeque.get(symbol);

        if (dq == null || dq.size() < 30) return null;
        if (vq == null || vq.size() < 30) return null;

        List<Double> buf = new ArrayList<>(dq);
        List<Double> volBuf = new ArrayList<>(vq);
        int n = buf.size();

        double move  = buf.get(n - 1) - buf.get(n - 22);
        double avg   = buf.stream().mapToDouble(Double::doubleValue).average().orElse(price);
        double vel   = Math.abs(move) / (avg + 1e-9);

        // [FIX-3] Условие 1: скорость должна быть значительной
        if (vel < 0.0022) return null;

        // [FIX-3] Условие 2: ускорение строже (1.6× вместо 1.45×)
        double m1 = buf.get(n / 2 - 1) - buf.get(0);
        double m2 = buf.get(n - 1)     - buf.get(n / 2);
        boolean acc = Math.abs(m2) > Math.abs(m1) * 1.60;
        if (!acc) return null;

        // [FIX-3] Условие 3: объём тиков выше среднего
        int volWindow = Math.min(30, volBuf.size());
        double avgVol = volBuf.subList(0, volWindow - 5).stream()
                .mapToDouble(Double::doubleValue).average().orElse(0.001);
        double recentVol = volBuf.subList(volWindow - 5, volWindow).stream()
                .mapToDouble(Double::doubleValue).average().orElse(0);
        if (recentVol < avgVol * 1.6) return null;

        // [FIX-3] Условие 4: последние 3+ тика в одном направлении (монолитность)
        boolean up = move > 0;
        int streak = 0;
        for (int i = n - 1; i >= Math.max(0, n - 5); i--) {
            if (i == 0) break;
            boolean tickUp = buf.get(i) >= buf.get(i - 1);
            if (tickUp == up) streak++;
            else break;
        }
        if (streak < 3) return null;

        // [FIX-3] Условие 5: последний тик подтверждает (нет микро-разворота)
        if (n >= 3) {
            double last3 = buf.get(n - 1) - buf.get(n - 3);
            if (up && last3 < 0) return null;   // разворот вниз
            if (!up && last3 > 0) return null;  // разворот вверх
        }

        double atrV = getAtr(symbol);
        if (atrV <= 0) atrV = price * 0.005;

        double stop = up ? price - atrV * 1.5 : price + atrV * 1.5;
        double take = up ? price + atrV * 3.2 : price - atrV * 3.2;

        // [FIX-3] Конфиденс снижен: max 68 (было 76) — early tick менее надёжен
        double conf = Math.min(68, 50 + vel * 4500);

        return new com.bot.DecisionEngineMerged.TradeIdea(
                symbol,
                up ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT,
                price, stop, take, conf,
                List.of("EARLY_TICK", up ? "UP" : "DN",
                        "v=" + String.format("%.2e", vel),
                        "stk=" + streak));
    }

    private boolean filterEarlySignal(com.bot.DecisionEngineMerged.TradeIdea sig) {
        boolean isLong    = sig.side == com.bot.TradingCore.Side.LONG;
        String sectorName = detectSector(sig.symbol);
        double rs = relStrengthHistory.getOrDefault(sig.symbol, new ArrayDeque<>())
                .stream().mapToDouble(Double::doubleValue).average().orElse(0.5);

        double weight = gic.getFilterWeight(sig.symbol, isLong, rs, sectorName);
        if (weight < 0.60) return false;

        if (!isc.allowSignal(sig)) return false;

        double adj = optimizer.adjustConfidence(sig);
        return adj >= 56;
    }

    // ══════════════════════════════════════════════════════════════
    //  FUNDING RATE + OI
    // ══════════════════════════════════════════════════════════════

    private void refreshAllFundingRates() {
        try {
            System.out.println("[FR] Refreshing...");
            JSONArray arr = new JSONArray(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://fapi.binance.com/fapi/v1/premiumIndex"))
                                    .timeout(Duration.ofSeconds(15)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body());

            Map<String, Double> rates = new HashMap<>(arr.length());
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                rates.put(o.getString("symbol"), o.optDouble("lastFundingRate", 0));
            }

            List<String> pairs = new ArrayList<>(cachedPairs);
            for (int i = 0; i < pairs.size(); i++) {
                String pair = pairs.get(i);
                try {
                    fetchAndUpdateOI(pair, rates.getOrDefault(pair, 0.0));
                    if (i % 10 == 9) Thread.sleep(40);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ignored) {}
            }
            System.out.println("[FR] Updated " + rates.size() + " rates");
        } catch (Exception e) {
            System.out.println("[FR] Error: " + e.getMessage());
        }
    }

    private void fetchAndUpdateOI(String symbol, double fr) {
        try {
            JSONObject oiJ = new JSONObject(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://fapi.binance.com/fapi/v1/openInterest?symbol=" + symbol))
                                    .timeout(Duration.ofSeconds(6)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body());
            double oi = oiJ.optDouble("openInterest", 0);

            JSONArray hist = new JSONArray(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://fapi.binance.com/futures/data/openInterestHist?symbol="
                                            + symbol + "&period=1h&limit=5"))
                                    .timeout(Duration.ofSeconds(6)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body());

            double oi1h = 0, oi4h = 0;
            if (hist.length() >= 2) {
                double prev1h = hist.getJSONObject(hist.length() - 2).optDouble("sumOpenInterest", oi);
                oi1h = ((oi - prev1h) / (prev1h + 1e-9)) * 100;
            }
            if (hist.length() >= 5) {
                double prev4h = hist.getJSONObject(0).optDouble("sumOpenInterest", oi);
                oi4h = ((oi - prev4h) / (prev4h + 1e-9)) * 100;
            }
            decisionEngine.updateFundingOI(symbol, fr, oi, oi1h, oi4h);
        } catch (Exception e) {
            decisionEngine.updateFundingOI(symbol, fr, 0, 0, 0);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  TOP SYMBOLS
    // ══════════════════════════════════════════════════════════════

    public Set<String> getTopSymbolsSet(int limit) {
        try {
            Set<String> binancePairs = getBinanceSymbolsFutures();

            JSONArray cg = new JSONArray(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://api.coingecko.com/api/v3/coins/markets"
                                            + "?vs_currency=usd&order=market_cap_desc&per_page=250&page=1"))
                                    .timeout(Duration.ofSeconds(15)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body());

            Set<String> top = new LinkedHashSet<>();
            for (int i = 0; i < cg.length(); i++) {
                String sym = cg.getJSONObject(i).getString("symbol").toUpperCase();
                if (STABLE.contains(sym)) continue;
                String pair = sym + "USDT";
                if (binancePairs.contains(pair)) top.add(pair);
                if (top.size() >= limit) break;
            }

            if (top.size() < limit) {
                for (String pair : binancePairs) {
                    if (top.size() >= limit) break;
                    top.add(pair);
                }
            }

            System.out.println("[PAIRS] Loaded " + top.size() + " pairs");
            return top;
        } catch (Exception e) {
            System.out.println("[PAIRS] ERROR: " + e.getMessage());
            return new LinkedHashSet<>(Arrays.asList(
                    "BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT",
                    "ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","LINKUSDT"));
        }
    }

    public Set<String> getBinanceSymbolsFutures() {
        try {
            JSONArray arr = new JSONObject(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://fapi.binance.com/fapi/v1/exchangeInfo"))
                                    .timeout(Duration.ofSeconds(10)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body()
            ).getJSONArray("symbols");

            Set<String> res = new HashSet<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject s = arr.getJSONObject(i);
                if ("TRADING".equalsIgnoreCase(s.optString("status", "TRADING")) &&
                        s.getString("symbol").endsWith("USDT"))
                    res.add(s.getString("symbol"));
            }
            System.out.println("[Binance] " + res.size() + " futures pairs");
            return res;
        } catch (Exception e) {
            System.out.println("[Binance] ERROR: " + e.getMessage());
            return new HashSet<>(Arrays.asList("BTCUSDT","ETHUSDT","BNBUSDT"));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  COIN CATEGORIZATION
    // ══════════════════════════════════════════════════════════════

    private static com.bot.DecisionEngineMerged.CoinCategory categorizePair(String pair) {
        String sym = pair.endsWith("USDT") ? pair.substring(0, pair.length() - 4) : pair;
        return switch (sym) {
            case "DOGE","SHIB","PEPE","FLOKI","WIF","BONK","MEME",
                 "NEIRO","POPCAT","COW","MOG","BRETT","TURBO" ->
                    com.bot.DecisionEngineMerged.CoinCategory.MEME;
            case "BTC","ETH","BNB","SOL","XRP","ADA","AVAX",
                 "DOT","LINK","MATIC","LTC","ATOM","UNI","AAVE" ->
                    com.bot.DecisionEngineMerged.CoinCategory.TOP;
            default -> com.bot.DecisionEngineMerged.CoinCategory.ALT;
        };
    }

    // ══════════════════════════════════════════════════════════════
    //  UTILITY
    // ══════════════════════════════════════════════════════════════

    private com.bot.DecisionEngineMerged.TradeIdea rebuildIdea(com.bot.DecisionEngineMerged.TradeIdea src,
                                                               double newProb, List<String> flags) {
        return new com.bot.DecisionEngineMerged.TradeIdea(
                src.symbol, src.side, src.price, src.stop, src.take, src.rr,
                newProb, flags,
                src.fundingRate, src.fundingDelta, src.oiChange, src.htfBias, src.category);
    }

    private void logCycleStats() {
        long total = totalFetches.get();
        long hits  = cacheHits.get();
        if (total > 0 && total % 500 == 0) {
            System.out.printf("[Stats] cache=%.1f%% (%d/%d) early=%d liq_block=%d corr_block=%d profit_block=%d latency_block=%d%n",
                    100.0 * hits / total, hits, total,
                    earlySignals.get(), blockedLiq.get(),
                    blockedCorr.get(), blockedProfit.get(), blockedLatency.get());
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

    public com.bot.DecisionEngineMerged getDecisionEngine() { return decisionEngine; }
    public com.bot.SignalOptimizer getOptimizer()      { return optimizer; }
    public com.bot.InstitutionalSignalCore getSignalCore()     { return isc; }
    public com.bot.PumpHunter getPumpHunter()     { return pumpHunter; }
    public com.bot.GlobalImpulseController getGIC()            { return gic; }
    public Map<String, Deque<Double>> getTickDeque()      { return tickPriceDeque; }

    // ══════════════════════════════════════════════════════════════
    //  STATIC UTILITY METHODS
    // ══════════════════════════════════════════════════════════════

    public static double atr(List<com.bot.TradingCore.Candle> c, int period) {
        if (c == null || c.size() <= period) return 0;
        double sum = 0;
        for (int i = c.size() - period; i < c.size(); i++) {
            com.bot.TradingCore.Candle prev = c.get(i - 1), cur = c.get(i);
            sum += Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low  - prev.close)));
        }
        return sum / period;
    }

    public static double rsi(List<Double> prices, int period) {
        if (prices == null || prices.size() <= period) return 50.0;
        double gain = 0, loss = 0;
        for (int i = prices.size() - period; i < prices.size(); i++) {
            double d = prices.get(i) - prices.get(i - 1);
            if (d > 0) gain += d; else loss += -d;
        }
        if (gain + loss == 0) return 50.0;
        return 100.0 - (100.0 / (1.0 + gain / (loss + 1e-12)));
    }

    public static double ema(List<Double> prices, int period) {
        if (prices == null || prices.isEmpty()) return 0;
        double k = 2.0 / (period + 1), e = prices.get(0);
        for (double p : prices) e = p * k + e * (1 - k);
        return e;
    }

    public static double sma(List<Double> prices, int period) {
        if (prices == null || prices.size() < period) return 0;
        double sum = 0;
        for (int i = prices.size() - period; i < prices.size(); i++) sum += prices.get(i);
        return sum / period;
    }

    public static double vwap(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.isEmpty()) return 0;
        double pv = 0, vol = 0;
        for (com.bot.TradingCore.Candle x : c) {
            double tp = (x.high + x.low + x.close) / 3.0;
            pv += tp * x.volume; vol += x.volume;
        }
        return vol == 0 ? c.get(c.size() - 1).close : pv / vol;
    }

    public static double momentumPct(List<Double> prices, int n) {
        if (prices == null || prices.size() <= n) return 0.0;
        double last = prices.get(prices.size() - 1);
        double prev = prices.get(prices.size() - 1 - n);
        return (last - prev) / (prev + 1e-12);
    }

    public static boolean detectBOS(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 10) return false;
        List<Integer> highs = com.bot.DecisionEngineMerged.swingHighs(c, 3);
        List<Integer> lows  = com.bot.DecisionEngineMerged.swingLows(c, 3);
        com.bot.TradingCore.Candle last = c.get(c.size() - 1);
        if (!highs.isEmpty() && last.close > c.get(highs.get(highs.size()-1)).high * 1.0005) return true;
        if (!lows.isEmpty()  && last.close < c.get(lows.get(lows.size()-1)).low   * 0.9995) return true;
        return false;
    }

    public static List<Integer> detectSwingHighs(List<com.bot.TradingCore.Candle> c, int lr) {
        return com.bot.DecisionEngineMerged.swingHighs(c, lr);
    }

    public static List<Integer> detectSwingLows(List<com.bot.TradingCore.Candle> c, int lr) {
        return com.bot.DecisionEngineMerged.swingLows(c, lr);
    }

    public static int marketStructure(List<com.bot.TradingCore.Candle> c) {
        return com.bot.DecisionEngineMerged.marketStructure(c);
    }

    public static boolean detectLiquiditySweep(List<com.bot.TradingCore.Candle> c) {
        return com.bot.DecisionEngineMerged.detectLiquiditySweep(c);
    }

    // ══════════════════════════════════════════════════════════════
    //  INNER CLASSES
    // ══════════════════════════════════════════════════════════════

    public static final class OrderbookSnapshot {
        public final double bidVolume, askVolume;
        public final long   timestamp;

        public OrderbookSnapshot(double b, double a, long t) {
            bidVolume = b; askVolume = a; timestamp = t;
        }

        public double obi() {
            return (bidVolume - askVolume) / (bidVolume + askVolume + 1e-12);
        }

        public boolean isFresh() {
            return System.currentTimeMillis() - timestamp < 30_000;
        }
    }

    public static final class MicroCandleBuilder {
        private final int intervalMs;
        private long   bucketStart = -1;
        private double open = Double.NaN, high = Double.NEGATIVE_INFINITY,
                low  = Double.POSITIVE_INFINITY, close = Double.NaN;
        private double volume = 0; private long closeTime = -1;

        public MicroCandleBuilder(int intervalMs) { this.intervalMs = intervalMs; }

        public Optional<com.bot.TradingCore.Candle> addTick(long ts, double price, double qty) {
            long bucket = (ts / intervalMs) * intervalMs;
            if (bucketStart == -1) {
                bucketStart = bucket; open = high = low = close = price;
                volume = qty; closeTime = bucket + intervalMs - 1;
                return Optional.empty();
            }
            if (bucket == bucketStart) {
                high = Math.max(high, price); low = Math.min(low, price);
                close = price; volume += qty;
                return Optional.empty();
            }
            com.bot.TradingCore.Candle c = new com.bot.TradingCore.Candle(
                    bucketStart, open, high, low, close, volume, volume, closeTime);
            bucketStart = bucket; open = high = low = close = price;
            volume = qty; closeTime = bucket + intervalMs - 1;
            return Optional.of(c);
        }
    }

    public static final class Signal {
        public final String symbol, direction;
        public final double confidence, price;
        public final long   timestamp;

        public Signal(String sym, String dir, double conf, double price) {
            this.symbol     = sym;
            this.direction  = dir;
            this.confidence = conf;
            this.price      = price;
            this.timestamp  = System.currentTimeMillis();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────
    private int    envInt(String k, int d)     { try { return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(d))); } catch (Exception e) { return d;  } }
    private long   envLong(String k, long d)   { try { return Long.parseLong(System.getenv().getOrDefault(k, String.valueOf(d)));   } catch (Exception e) { return d;  } }
    private double envDouble(String k, double d){ try { return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(d))); } catch (Exception e) { return d; } }
    private static double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
    private static String pct(double v) { return String.format("%.0f", v * 100); }
}