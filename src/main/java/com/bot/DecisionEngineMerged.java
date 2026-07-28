package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

// DecisionEngineMerged — REFACTORED v38-FINAL
// CHANGES vs original:
//   List<String> allFlags moved BEFORE CI filter block
//                    → fixes "Cannot resolve symbol 'allFlags'" compile error
//   MIN_AGREEING_CLUSTERS = 3 (was 2)
//   MIN_CLUSTER_SCORE = 0.25 (was none)
//   Choppiness Index filter (CI > 61.8/68.0 block)
//   Thread-safe symbolMinConf via compute()
//   vdHistory/cvdHistory via computeIfAbsent()
public final class DecisionEngineMerged {
    // [v72] Unified logger
    private static final Logger LOG = Logger.getLogger(DecisionEngineMerged.class.getName());


    public static volatile java.time.ZoneId USER_ZONE = java.time.ZoneId.of("Europe/Warsaw");

    // ── Enums ──────────────────────────────────────────────────────
    public enum CoinCategory { TOP, ALT, MEME }

    public enum AssetType {
        CRYPTO("₿", "Криптовалюта"),
        PRECIOUS_METAL_GOLD("🥇", "Золото"),
        PRECIOUS_METAL_SILVER("🥈", "Серебро"),
        PRECIOUS_METAL_PLATINUM("💎", "Платина"),
        PRECIOUS_METAL_OTHER("⚙️", "Драг. металл"),
        COMMODITY_OIL("🛢", "Нефть"),
        COMMODITY_GAS("⛽", "Природный газ"),
        COMMODITY_OTHER("🌾", "Сырьё"),
        FOREX("💱", "Форекс"),
        INDEX("📈", "Индекс"),
        UNKNOWN("📊", "Актив");

        public final String emoji;
        public final String label;
        AssetType(String emoji, String label) {
            this.emoji = emoji;
            this.label = label;
        }
    }

    private static final java.util.Map<String, AssetType> ASSET_KEYWORD_MAP;
    static {
        java.util.Map<String, AssetType> m = new java.util.LinkedHashMap<>();
        m.put("XAU",     AssetType.PRECIOUS_METAL_GOLD);
        m.put("GOLD",    AssetType.PRECIOUS_METAL_GOLD);
        m.put("PAXG",    AssetType.PRECIOUS_METAL_GOLD);    // PAX Gold token
        m.put("XAUT",    AssetType.PRECIOUS_METAL_GOLD);    // Tether Gold
        m.put("KAG",     AssetType.PRECIOUS_METAL_SILVER);
        m.put("XAG",     AssetType.PRECIOUS_METAL_SILVER);
        m.put("SILVER",  AssetType.PRECIOUS_METAL_SILVER);
        m.put("XPT",     AssetType.PRECIOUS_METAL_PLATINUM);
        m.put("PLAT",    AssetType.PRECIOUS_METAL_PLATINUM);
        m.put("BRONZE",  AssetType.PRECIOUS_METAL_OTHER);
        m.put("COPPER",  AssetType.PRECIOUS_METAL_OTHER);
        m.put("XCU",     AssetType.PRECIOUS_METAL_OTHER);
        m.put("PALLADIUM", AssetType.PRECIOUS_METAL_OTHER);
        m.put("XPD",     AssetType.PRECIOUS_METAL_OTHER);
        // ── Oil ──
        m.put("OIL",     AssetType.COMMODITY_OIL);
        m.put("BRENT",   AssetType.COMMODITY_OIL);
        m.put("WTI",     AssetType.COMMODITY_OIL);
        m.put("CRUDE",   AssetType.COMMODITY_OIL);
        m.put("PETRO",   AssetType.COMMODITY_OIL);
        // Binance 2026 commodity tickers (CLUSDT=WTI, BZUSDT=Brent)
        m.put("CL",      AssetType.COMMODITY_OIL);
        m.put("BZ",      AssetType.COMMODITY_OIL);
        // ── Gas ──
        m.put("GAS",     AssetType.COMMODITY_GAS);
        m.put("NATGAS",  AssetType.COMMODITY_GAS);
        m.put("NGAS",    AssetType.COMMODITY_GAS);
        m.put("LNG",     AssetType.COMMODITY_GAS);
        // ── Other commodities ──
        m.put("WHEAT",   AssetType.COMMODITY_OTHER);
        m.put("CORN",    AssetType.COMMODITY_OTHER);
        m.put("SOYBEAN", AssetType.COMMODITY_OTHER);
        m.put("COFFEE",  AssetType.COMMODITY_OTHER);
        m.put("SUGAR",   AssetType.COMMODITY_OTHER);
        m.put("COTTON",  AssetType.COMMODITY_OTHER);
        // ── Forex proxies ──
        m.put("EUR",     AssetType.FOREX);
        m.put("GBP",     AssetType.FOREX);
        m.put("JPY",     AssetType.FOREX);
        // ── Index proxies ──
        m.put("SPX",     AssetType.INDEX);
        m.put("NDX",     AssetType.INDEX);
        m.put("DJI",     AssetType.INDEX);
        ASSET_KEYWORD_MAP = Collections.unmodifiableMap(m);
    }

    public static AssetType detectAssetType(String symbol) {
        if (symbol == null || symbol.isEmpty()) return AssetType.UNKNOWN;
        String base = symbol.endsWith("USDT") ? symbol.substring(0, symbol.length() - 4)
                : symbol.endsWith("BUSD") ? symbol.substring(0, symbol.length() - 4)
                  : symbol.endsWith("USDC") ? symbol.substring(0, symbol.length() - 4)
                    : symbol;
        String upper = base.toUpperCase();

        // Exact match first (most reliable)
        AssetType exact = ASSET_KEYWORD_MAP.get(upper);
        if (exact != null) return exact;

        // Contains match (catches e.g. "OILUSDT", "GOLDUSDT", "XAUUSDT")
        for (java.util.Map.Entry<String, AssetType> e : ASSET_KEYWORD_MAP.entrySet()) {
            if (upper.contains(e.getKey())) return e.getValue();
        }

        // Default: everything on Binance Futures that ends in USDT = crypto
        return AssetType.CRYPTO;
    }

    // ── Константы ─────────────────────────────────────────────────
    // MIN_BARS lowered 150 → 100 to allow recently-listed pairs (ETH, SOL, BNB,
    // XRP, DOGE, AVAX, LINK, UNI) with only 80-100 bars history into analysis.
    // 100 1h bars = ~4 days, enough for VWAP (~50 bars) + ATR (14) + trend (20-30).
    private static final int    MIN_BARS        = 100;

    private static final long   COOLDOWN_TOP    = 6  * 60_000L;  // was 10m → 6m
    private static final long   COOLDOWN_ALT    = 5  * 60_000L;  // was 8m  → 5m
    private static final long   COOLDOWN_MEME   = 8  * 60_000L;  // was 12m → 8m

    // [v70] Floor 60→55 / ceil 82→76. При neutral BTC + плоских альтах probability
    // физически не дотягивает до 60 (3 кластера clusterBase=50, -5 RANGE, -3 ALT
    // уже даёт ~42). Новые значения позволяют пропустить ранние тренды / развороты /
    // pump/dump setups в диапазоне 55–75, при этом downstream Dispatcher + ISC
    // остаются authoritative quality-gate (их пороги тоже понижены).
    // [v71] MIN_CONF_FLOOR 55→52: после rebalance формулы (cluster bases 56→60,
    // RANGE penalty -2→-0.5, ALT -1→0) валидный 3-cluster setup в плоском рынке
    // выдаёт 53-58. С floor=55 он валился. С floor=52 — проходит, но Dispatcher
    // cold-start gate (53/57) и калибратор остаются authoritative quality control.
    // [FIX-9PCT 2026-05-02] BASE_CONF 48 → 58 / FLOOR 48 → 58 / CEIL 76 → 80.
    // Корневая причина 9% WR (6/68 wins): пороги были снижены до уровня шума.
    // На WR=9% downstream Dispatcher / Calibrator / ISC уже не успевали
    // отфильтровать слабые сигналы — слишком много мусора пробивало DE-этап.
    // Возврат к v70-уровню: только сигналы с реальной структурной идеей
    // доходят до калибратора. Бот будет молчать чаще — это правильно при
    // NEUTRAL BTC. Меньше плохих сигналов = больше edge.
    // [FIX-9PCT-MIDPOINT 2026-05-02] BASE 58→53 / FLOOR 58→52 / CEIL 80→78.
    // Полный возврат к 58 оказался over-correction: на 49000 свечей backtest
    // дал 0 сделок, на 4-часовом live окне 4 сигнала. 3-кластерные setup'ы
    // в RANGE+ALT дают prob 55-57 (см. формулу строка 3807) — недотягивают
    // до 58. Опускаем до 53 — середина между 48 (где был 9% WR) и 58 (где
    // паралич). Калибратор + Dispatcher + ISC остаются authoritative quality
    // gate: сигналы с prob 53-58 будут проходить DE, но если калибратор
    // обучится что они часто проигрывают — он их занизит и они отвалятся.
    // [FIX-FLAT-MARKET 2026-05-02] FLOOR 52→50. С env MIN_CONF=53 авторитетным
    // становится SignalSender.earlyMinConf=53. DE-floor=50 даёт DE возможность
    // выпускать setup'ы с prob 50-52 — они дойдут до SignalSender, где env-MIN_CONF
    // решает финально. Без этого DE рубит сетапы ещё до того как они увидят env-floor.
    // [FLAT-MARKET LOOSEN 2026-05-05] BASE_CONF 53→50. В NEUTRAL str=0.06-0.22 BTC
    // RANGE adapt даёт base+1=54, что отсекало 99% сетапов. Снижение до 50 = равно
    // floor; адаптации (RANGE +1, vol +2, UTC -1.5) теперь работают вокруг 50.
    private static final double BASE_CONF       = 50.0;
    private static final int    CALIBRATION_WIN = 120;
    // [HOLE-5 FIX 2026-05-08] MIN_CONF_FLOOR 48 → 52. С env MIN_CONF=53 (рекомендация
    // в коде SignalSender:608) DE выпускал идеи в диапазоне 48-52 которые СРАЗУ
    // отсекались downstream — wasted compute ~30%. Поднимаем floor до 52, чтобы
    // ранний reject экономил CPU. Authoritative env-MIN_CONF остаётся в SignalSender.
    private static final double MIN_CONF_FLOOR  = 52.0;
    private static final double MIN_CONF_CEIL   = 78.0;

    // Дивергенции — штраф вместо хард-лока
    private static final double DIV_PENALTY_SCORE  = 0.55;
    private static final double DIV_VOL_DELTA_GATE = 1.80;
    private static final double DIV_TREND_RSI_GATE = 72.0;

    // Crash score порог
    private static final double CRASH_SCORE_BOOST_THRESHOLD = 0.35;
    private static final double CRASH_SHORT_BOOST_BASE = 0.75;

    // Cluster confluence bonus
    private static final double CLUSTER_CONFLUENCE_BONUS = 0.15;

    // [HOLE-4 FIX 2026-05-08] MIN_AGREEING_CLUSTERS теперь АДАПТИВНЫЙ.
    // OLD: фиксированно 2 — пропускало шумовые сетапы в RANGE-рынке.
    // NEW: 2 для TREND/STRONG_TREND (импульс достаточно говорит сам за себя)
    //      3 для RANGE (чтобы 2 случайно совпавших cluster'а не дали сигнал)
    // Метод clustersRequired(MarketState) ниже возвращает нужное значение в runtime.
    // Default fallback всё ещё 2 для совместимости с unit-тестами/legacy callers.
    private static final int    MIN_AGREEING_CLUSTERS    = 2;
    private static final int    MIN_AGREEING_CLUSTERS_RANGE = 3;
    // [FLAT-FIX 2026-05-07] 0.28 → 0.22. Кластер квалифицируется как "agreeing"
    // если его направленный score ≥ 0.22 (было 0.28). На флэте кластеры дают
    // score 0.20-0.30 — старый порог отсекал большинство. 0.22 оставляет защиту
    // от чисто шумовых ассоциаций (random < 0.15).
    private static final double MIN_CLUSTER_SCORE        = 0.22;



    // Single authoritative probability ceiling. All intermediate caps and the
    // final calibrator clamp must reference this constant. Previously hardcoded 85 in 5+ places.
    private static final double PROB_CEIL = 85.0;

    // ── State ─────────────────────────────────────────────────────
    private final Map<String, Double>           symbolMinConf    = new ConcurrentHashMap<>();

    private final java.util.concurrent.atomic.AtomicReference<Double> globalMinConf
            = new java.util.concurrent.atomic.AtomicReference<>(BASE_CONF);
    private final Map<String, Long>             cooldownMap      = new ConcurrentHashMap<>();
    // [v69] Отдельный per-symbol skip для post-pump пар. Key = symbol, value = until-timestamp.
    // Не переиспользуем cooldownMap (у того другая semantics: time-of-last-signal, key=sym_side).
    private final Map<String, Long>             postPumpSkipUntil = new ConcurrentHashMap<>();
    // [FIX-SYM 2026-05-02] Зеркало postPumpSkipUntil для post-dump bounce кейсов
    // (SHORT в отскок ножа после капитуляции). Аналогичный 30-min cooldown.
    private final Map<String, Long>             postDumpSkipUntil = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>>    recentDirs       = new ConcurrentHashMap<>();
    private final Map<String, Double>           lastSigPrice     = new ConcurrentHashMap<>();
    private final Map<String, FundingOIData>    fundingCache     = new ConcurrentHashMap<>();
    private final Map<String, Deque<CalibRecord>> calibHist      = new ConcurrentHashMap<>();
    private final Map<String, Double>           volumeDeltaMap   = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>    vdHistory        = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>    relStrengthHistory = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger>    signalCountBySymbol = new ConcurrentHashMap<>();

    // [project_state id=14, id=23] ProbabilityCalibrator removed. Its write path was dead —
    // recordOutcome*() was never called from anywhere — so the isotonic-regression machinery,
    // the HMAC audit chain and the persisted CSV were all operating on an empty sample.
    // Outcome accounting moves to the paper-harness, which pre-registers predictions and
    // signs them (project_state id=17).

    // [v67] Reject trace — per-reason counter, deltas printed each cycle via getAndResetRejectTrace().
    // Lets SignalSender's [DIAG] line show WHICH internal gate is killing 24 of 25 pairs.
    // Zero cost when not logging — single AtomicLong increment per reject.
    private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicLong>
            REJECT_TRACE = new java.util.concurrent.ConcurrentHashMap<>();
    static TradeIdea reject(String reason) {
        REJECT_TRACE.computeIfAbsent(reason, k -> new java.util.concurrent.atomic.AtomicLong()).incrementAndGet();
        return null;
    }


    // [v42.0 FIX #12] Last GC timestamp for postExitCooldown leak fix
    private volatile long lastCooldownGcMs = 0L;
    private static final int POST_EXIT_MAX_SIZE = 5000;

    private final Map<String, Double>           cvdMap           = new ConcurrentHashMap<>();
    private final Map<String, Double>           vdaMap           = new ConcurrentHashMap<>();

    private final Map<String, Deque<Double>>    cvdHistory       = new ConcurrentHashMap<>();

    // CVD must persist for 2 bars before it counts as confirmation. At 1 bar
    // the filter accepted spikes from short-covering or single large market
    // orders that had no follow-through. Latency is fixed via EARLY_TICK,
    // CVD must remain a real confirmation filter.
    private static final int CVD_PERSIST_BARS = 2;

    private final Map<String, Integer> consecutiveLossMap = new ConcurrentHashMap<>();
    private static final double CONF_PENALTY_PER_LOSS = 3.0;
    private static final double CONF_PENALTY_MAX      = 15.0;
    private static final int    CONF_PENALTY_THRESHOLD = 3; // start penalizing after 3rd loss

    private final Map<String, Long> postExitCooldown = new ConcurrentHashMap<>();

    private static final long POST_EXIT_COOLDOWN_MS = 20 * 60_000L;

    private volatile com.bot.TradingCore.ForecastEngine forecastEngine = null;

    /** Optional ISC reference — used only for chain-pause check at the top of
     *  generate(). Null until BotMain wires it; null-safe at every call site. */

    public DecisionEngineMerged() {}

    private final java.util.concurrent.atomic.AtomicReference<Double> bayesPrior
            = new java.util.concurrent.atomic.AtomicReference<>(0.50);

    // [v17.0 FIX §5] AtomicInteger for bayesSampleTrades — same race condition fix.
    private final java.util.concurrent.atomic.AtomicInteger bayesSampleTrades
            = new java.util.concurrent.atomic.AtomicInteger(0);

    // Two-speed Bayesian prior — fixes regime-change lag.
    //
    // Old behavior: pure linear blend over 120 trades. After regime change
    // (e.g. trend → choppy), engine kept using stale prior for 100+ trades.
    // On 15m timeframe that is several days of degraded signals.
    //
    // New behavior: two exponential moving averages at different speeds.
    //   - FAST (alpha ~0.08, half-life ~8 trades) tracks recent regime
    //   - SLOW (alpha ~0.015, half-life ~46 trades) provides stability
    // Prior = blend(slow, fast). When |fast-slow| exceeds REGIME_SHIFT
    // threshold, weight shifts toward fast EWMA (adaptive to new regime).
    // When they agree, slow dominates (noise suppression).
    private final java.util.concurrent.atomic.AtomicReference<Double> bayesPriorFast
            = new java.util.concurrent.atomic.AtomicReference<>(0.50);
    private final java.util.concurrent.atomic.AtomicReference<Double> bayesPriorSlow
            = new java.util.concurrent.atomic.AtomicReference<>(0.50);
    private final java.util.concurrent.atomic.AtomicInteger bayesUpdateCount
            = new java.util.concurrent.atomic.AtomicInteger(0);

    // Tunable — smaller alpha = slower response.
    private static final double BAYES_FAST_ALPHA = 0.08;   // half-life ~8 trades
    private static final double BAYES_SLOW_ALPHA = 0.015;  // half-life ~46 trades
    private static final double BAYES_REGIME_SHIFT = 0.08; // |fast-slow| above this = regime change
    private static final int    BAYES_MIN_TRADES = 15;     // below this, hold neutral 0.50
    private static final int    BAYES_FULL_TRUST = 80;     // above this, fully trust live priors





    // ── Setters ───────────────────────────────────────────────────
    public void setForecastEngine(com.bot.TradingCore.ForecastEngine fe) { this.forecastEngine = fe; }

    // [v86.56 MR-SHADOW] Backtest-only override of STRATEGY_MODE for measure-only
    // comparison passes (e.g. does the dormant mean-rev earn in chop where TREND
    // bleeds?). Live path never sets this — stays null → env/default as before.
    private volatile String strategyModeOverride = null;

    // Category-aware directional score thresholds for EARLY_TICK gate.
    //
    // Old: single 0.25 threshold for all categories.
    // EARLY_TICK forecast gate — asymmetric thresholds.
    // EARLY_TICK fires on tick velocity (leading); ForecastEngine works on closed
    // 15m bars (lagging). Demanding strong same-direction confirmation defeats the
    // purpose: by the time the closed bar agrees, the move has already happened.
    // Solution: only block when forecast is *meaningfully* against the trade.
    // Neutral or weakly-aligned forecast = allow (this is the pre-move regime).
    private static final double EARLY_TICK_FC_OPPOSE_TOP  = 0.18;
    private static final double EARLY_TICK_FC_OPPOSE_ALT  = 0.22;
    private static final double EARLY_TICK_FC_OPPOSE_MEME = 0.28;
    private static final double EARLY_TICK_FC_OPPOSE_DEF  = 0.22;

    private static double earlyTickOpposeThresholdFor(CoinCategory cat) {
        if (cat == null) return EARLY_TICK_FC_OPPOSE_DEF;
        return switch (cat) {
            case TOP  -> EARLY_TICK_FC_OPPOSE_TOP;
            case ALT  -> EARLY_TICK_FC_OPPOSE_ALT;
            case MEME -> EARLY_TICK_FC_OPPOSE_MEME;
        };
    }

    public static boolean forecastPassesEarlyTickGate(
            com.bot.TradingCore.ForecastEngine.ForecastResult forecast,
            boolean isLong) {
        return forecastPassesEarlyTickGate(forecast, isLong, null);
    }

    /**
     * Category-aware EARLY_TICK forecast gate.
     * Pass-through unless ForecastEngine is *actively* against the trade direction
     * by more than the category-specific opposing threshold.
     */
    public static boolean forecastPassesEarlyTickGate(
            com.bot.TradingCore.ForecastEngine.ForecastResult forecast,
            boolean isLong,
            CoinCategory category) {

        if (forecast == null) return true;

        double score = forecast.directionScore;
        double opposeThr = earlyTickOpposeThresholdFor(category);

        if ( isLong && score < -opposeThr) return false;
        if (!isLong && score >  opposeThr) return false;

        return true;
    }
    // Now accepts price to properly track lastSigPrice.
    public void confirmSignal(String symbol, com.bot.TradingCore.Side side, double price, long now) {
        registerSignal(symbol, side, now);
        lastSigPrice.put(symbol, price);
    }

    // Backward-compatible overload
    public void confirmSignal(String symbol, com.bot.TradingCore.Side side, long now) {
        registerSignal(symbol, side, now);
    }


    public void recordLoss(String symbol, com.bot.TradingCore.Side side) {
        int losses = consecutiveLossMap.merge(symbol, 1, Integer::sum);
        if (losses >= CONF_PENALTY_THRESHOLD) {
            final double penalty = Math.min(CONF_PENALTY_MAX,
                    (losses - CONF_PENALTY_THRESHOLD + 1) * CONF_PENALTY_PER_LOSS);
            // compute() вместо get()+put() — атомарное read-modify-write.
            // Старый код: get()+put() = lost update при конкурентном доступе из fetchPool (34 потока).
            symbolMinConf.compute(symbol, (k, cur) -> {
                double base = (cur != null ? cur : globalMinConf.get());
                return Math.min(MIN_CONF_CEIL, base + penalty);
            });
        }
        postExitCooldown.put(symbol + "_" + side.name(), System.currentTimeMillis());
    }

    /**
     * Record win — decays confidence penalty, resets consecutive losses.
     * Call from BotMain TradeResolver after TP hit.
     */
    public void recordWin(String symbol, com.bot.TradingCore.Side side) {
        consecutiveLossMap.put(symbol, 0);
        // compute() — атомарный декремент штрафа confidence
        symbolMinConf.compute(symbol, (k, cur) -> {
            double base = (cur != null ? cur : globalMinConf.get());
            return Math.max(globalMinConf.get(), base - CONF_PENALTY_PER_LOSS);
        });
    }

    /**
     * Called from BotMain after any position close (TP/SL/Chandelier).
     * Blocks re-entry in same direction for POST_EXIT_COOLDOWN_MS to prevent spin-trading.
     */
    public void markPostExitCooldown(String symbol, com.bot.TradingCore.Side side) {
        postExitCooldown.put(symbol + "_" + side.name(), System.currentTimeMillis());
    }




    /** [ДЫРА №1] CVD — устанавливается из SignalSender после вычисления накопленной дельты */
    public void setCVD(String sym, double cvdNormalized) {
        cvdMap.put(sym, cvdNormalized);
        // Build CVD history for persistence check (short-covering vs real demand)
        Deque<Double> hist = cvdHistory.computeIfAbsent(sym,
                k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        hist.addLast(cvdNormalized);
        if (hist.size() > 10) hist.removeFirst();
    }



    /** [v29] VDA score [-1..+1]: +1=buy acceleration, -1=sell acceleration */
    public void setVDA(String sym, double score) {
        vdaMap.put(sym, score);
    }




    //  CLUSTER SCORE HOLDER
    //  Каждый кластер хранит свой лучший LONG и SHORT score



    //  VOLATILITY BUCKET — per-symbol volatility classification
    //
    //  PROBLEM (RIVER case): bот ставил стоп 0.70% на монете с ATR 2-3%.
    //  Причина: ATR во время консолидации был искусственно сжат.
    //  Бот использовал текущий (сжатый) ATR как базу для стопа.
    //  При первом же «вздохе» цены стоп сносило.
    //
    //  РЕШЕНИЕ: классифицировать монету по ДОЛГОСРОЧНОМУ ATR и применять
    //  соответствующие минимальные кратные для стопа и тейков.

    public enum VolatilityBucket {
        LOW    ("LOW",    2.2, 0.04, 1.00),  // <0.5% ATR/price:  BTC/ETH
        MEDIUM ("MEDIUM", 2.8, 0.06, 1.10),  // 0.5-1.5%: major ALTs
        HIGH   ("HIGH",   3.5, 0.09, 1.25),  // 1.5-3.5%: volatile ALTs
        EXTREME("EXTREME",4.5, 0.14, 1.45);  // >3.5%: micro-caps/memes

        public final String label;
        public final double minAtrMult;   // minimum ATR multiplier for stop floor
        public final double maxStopPct;   // max stop distance as % of price
        public final double tpShrink;     // TP shrink factor (HIGH vol → tighter TPs)

        VolatilityBucket(String l, double m, double s, double t) {
            label = l; minAtrMult = m; maxStopPct = s; tpShrink = t;
        }
    }


    /**
     * Robust ATR: max(currentATR, longTermATR × 0.80).
     *
     * PROBLEM: during consolidation, current ATR can drop to 30-40% of normal.
     * This makes the ATR-based stop floor dangerously tight.
     * Using the long-term ATR as a floor ensures the stop always respects
     * the coin's actual trading noise, even during quiet periods.
     */
    public static double robustAtr(List<com.bot.TradingCore.Candle> c15, int fastN) {
        // [v43 PATCH FIX #2] Weighted long-term ATR to prevent consolidation stop squeeze.
        //
        // ROOT CAUSE of narrow stops: during consolidation the 15m ATR can collapse
        // to 30-40% of its normal value. The old code used only 50 bars (12.5 hours)
        // for the "long-term" reference — still well inside the consolidation window.
        // A coin that normally moves 2% daily was getting stops sized for 0.8% ATR.
        //
        // FIX: use 300 bars (75 hours ≈ 3 days) as the long-term reference window.
        // Blend: 70% long-term + 30% current. This guarantees the stop always reflects
        // the coin's real trading noise even when the market goes quiet.
        //
        // Example: AEVO normal ATR=2%, consolidation ATR=0.9%
        //   OLD: robustAtr = max(0.9%, 0.8% × 0.80) = max(0.9%, 0.72%) = 0.9%  ← too tight
        //   NEW: longTerm=2%, fast=0.9%, weighted=2%×0.70+0.9%×0.30=1.67%
        //        floor=max(2%×0.80, 1.67%)=max(1.60%, 1.67%)=1.67%  ← respects real noise
        if (c15.size() < fastN + 2) {
            return com.bot.TradingCore.atr(c15, Math.min(fastN, c15.size() - 1));
        }

        double fastAtr = com.bot.TradingCore.atr(c15, fastN);

        // Need at least fastN+50 bars for a meaningful long-term estimate
        if (c15.size() < fastN + 50) return fastAtr;

        // Long-term window: up to 300 bars (75h on 15m TF ≈ 3 trading days)
        int ltWindow = Math.min(300, c15.size() - 1);
        double longTermAtr = com.bot.TradingCore.atr(
                c15.subList(c15.size() - ltWindow, c15.size()),
                Math.min(14, ltWindow - 1));

        // 70/30 weighted blend — current gets weight so fresh breakouts aren't ignored
        double weighted = longTermAtr * 0.70 + fastAtr * 0.30;

        // Hard floor: never go below 80% of long-term (protects against extreme consolidation)
        return Math.max(longTermAtr * 0.80, weighted);
    }

    /**
     * Noise Score: average wick/body ratio over last N candles.
     *
     * High score (>2.5) = long wicks relative to body = market is choppy.
     * RIVER on the screenshot = classic "noisy" coin: wicks dominate body.
     * Noisy coins need wider stops and higher confidence thresholds.
     *
     * Score interpretation:
     *   <1.5 = clean (trending candles)
     *   1.5–2.5 = moderate noise
     *   >2.5 = high noise (like RIVER in consolidation)
     *   >4.0 = extreme noise (avoid entirely)
     */
    public static double computeNoiseScore(List<com.bot.TradingCore.Candle> c, int n) {
        if (c == null || c.size() < n) return 1.0;
        double ratioSum = 0;
        int count = 0;
        for (int i = c.size() - n; i < c.size(); i++) {
            com.bot.TradingCore.Candle bar = c.get(i);
            double body = Math.abs(bar.close - bar.open);
            double wickRange = (bar.high - bar.low);
            if (body > 0 && wickRange > 0) {
                ratioSum += wickRange / body;
                count++;
            }
        }
        return count > 0 ? ratioSum / count : 1.5;
    }

    public static final class FundingOIData {
        public final double fundingRate, openInterest, oiChange1h, oiChange4h;
        public final double prevFundingRate, fundingDelta;
        // [MODULE 1 v33] FR MOMENTUM — acceleration of funding rate change.
        // Single delta (fr - prevFr) tells you the direction of the last tick.
        // Acceleration (delta - prevDelta) tells you if it's SPEEDING UP or reversing.
        //
        // Examples:
        //   FR: +0.01% → +0.05% → +0.09%  = delta=+0.04%, accel=+0.04% (longs overheating → short setup)
        //   FR: +0.09% → +0.05% → +0.03%  = delta=-0.04%, accel=+0.02% (peak FR, shorts forming)
        //   FR: -0.08% → -0.05% → -0.02%  = delta=+0.03%, accel=+0.03% (short squeeze building → long setup)
        //
        // frAcceleration > 0 = FR moving faster in the SAME direction = momentum building
        // frAcceleration < 0 = FR reversing = peak/trough forming = contrarian signal
        public final double frAcceleration; // 2nd derivative of FR (per update cycle)
        public final boolean frPeakWarning; // FR > extreme AND decelerating → reversal imminent
        public final boolean frTroughWarning; // FR < extreme negative AND decelerating
        public final long   timestamp;

        public FundingOIData(double fr, double oi, double oi1h, double oi4h) {
            this(fr, oi, oi1h, oi4h, fr, 0.0, 0.0);
        }
        public FundingOIData(double fr, double oi, double oi1h, double oi4h, double prevFr, double delta) {
            this(fr, oi, oi1h, oi4h, prevFr, delta, 0.0);
        }
        public FundingOIData(double fr, double oi, double oi1h, double oi4h,
                             double prevFr, double delta, double accel) {
            this.fundingRate = fr; this.openInterest = oi;
            this.oiChange1h = oi1h; this.oiChange4h = oi4h;
            this.prevFundingRate = prevFr; this.fundingDelta = delta;
            this.frAcceleration  = accel;
            // Peak warning: FR is extremely positive AND acceleration is turning negative
            // (rate of increase is slowing down = longs are exhausted, squeeze incoming)
            this.frPeakWarning   = fr > 0.0008 && accel < -0.0001;
            // Trough warning: FR is extremely negative AND acceleration turning positive
            // (rate of decrease slowing = shorts exhausted, bounce incoming)
            this.frTroughWarning = fr < -0.0005 && accel > 0.0001;
            this.timestamp = System.currentTimeMillis();
        }
        public boolean isValid() { return System.currentTimeMillis() - timestamp < 5 * 60_000L; }
    }

    private static final class CalibRecord {
        final double predicted;
        final boolean correct;
        CalibRecord(double p, boolean c) { predicted = p; correct = c; }
    }

    //  TRADE IDEA

    public static final class TradeIdea {
        public final String           symbol;
        public final com.bot.TradingCore.Side side;
        public final double           price, stop, take, tp1, tp2, tp3;
        public final double           probability;
        public final List<String>     flags;
        public final double           fundingRate, fundingDelta, oiChange;
        public final String           htfBias;
        public final double           rr;
        public final CoinCategory     category;
        // ForecastEngine integration
        public final com.bot.TradingCore.ForecastEngine.ForecastResult forecast;
        public final String trendPhase;
        // [ДЫРА №6] Адаптивные множители TP по режиму рынка
        public final double tp1Mult, tp2Mult, tp3Mult;
        // [v43 PATCH FIX #5] Expose robustAtrPct at signal time.
        // BotMain.trackSignal() stores this in ForecastRecord → correct vol-bucket
        // in calibrator (avoids consolidation ATR collapse misclassifying HIGH→LOW bucket).
        // [v50 AUDIT FIX] Allow override of robustAtrPct by analyze() with the real value.
        // Previously this was derived from abs(stop-price)/price, which is incorrect for
        // structural stops (where stop distance != ATR). Corruption of vol bucket
        // classification was silently breaking calibrator learning.
        private volatile double robustAtrPctOverride = -1.0;
        public double getRobustAtrPct() {
            return robustAtrPctOverride > 0 ? robustAtrPctOverride : robustAtrPct;
        }
        public void setRobustAtrPct(double v) {
            // v = ATR/price ratio (NOT percent). Realistic range: (0, 0.5].
            // 0.5 = 50% ATR/price = extreme volatility edge. Anything >0.5 is a bug source.
            if (v > 0 && v <= 0.5) this.robustAtrPctOverride = v;
        }

        // [v86.60 PHASE-0] Возраст 4h-тренда на момент сигнала: закрытых 4h-баров с
        // последнего пересечения EMA20/50. -1 = не размечен (не-TREND стратегии).
        // Чистая разметка для отчёта «PnL по возрасту тренда» — поведение не меняет.
        private volatile int trendAge4h = -1;
        public int getTrendAge4h() { return trendAge4h; }
        public void setTrendAge4h(int v) { this.trendAge4h = v; }

        public final double robustAtrPct;
        // Signal age tracking — enables decay-based filtering in
        // earlyTickBuffer and anywhere else ideas sit in a queue. Stale signals
        // (older than ~90s on 15m tf) lose edge because the move they predicted
        // may have already played out. Consumers use ageMs() to apply penalty.
        public final long createdAtMs;

        /** Age of the signal in milliseconds since creation. */
        public long ageMs() { return System.currentTimeMillis() - createdAtMs; }


        // [HOLE-1 FIX 2026-05-08] Unified size multiplier passthrough.
        // SignalSender computes ALL modifiers (category, flag-based, session, ISC,
        // small-balance) when building Telegram display, then stores the resulting
        // ratio here. Executor reads it and applies to base qty so on-exchange size
        // matches what Telegram showed. Default 1.0 = no modifier (safe fallback
        // when idea didn't go through SignalSender path, e.g. LiveTradeProbe).
        // Clamped to [0.20, 1.20] in setter to prevent malformed values.
        private volatile double executorSizeMultiplier = 1.0;
        public double getExecutorSizeMultiplier() { return executorSizeMultiplier; }
        public void setExecutorSizeMultiplier(double m) {
            if (Double.isNaN(m) || Double.isInfinite(m) || m <= 0) return;
            this.executorSizeMultiplier = Math.max(0.20, Math.min(1.20, m));
        }

        // [B1 2026-05-08] Direction-correct count of clusters agreeing with the chosen side.
        // Default -1 = "not set" → consumer (Dispatcher) falls back to flag-substring counting.
        // Set by analyze() right after TradeIdea construction with `supportingClusters`.
        // Why: BotMain.countClusterFlags() counts SUBSTRINGS (HTF_, BREAKOUT, DIV, etc.) which
        // double-counts: one signal can carry both HTF_BULL and HTF_OPPOSE flags, BREAKOUT
        // appears regardless of side, BULL_DIV/BEAR_DIV both match "DIV". Result:
        // dispatcher saw "5 clusters" when reality was 2. This was likely the single biggest
        // contributor to low-confidence signals leaking through the quality gate.
        private volatile int agreeingClusters = -1;
        public int getAgreeingClusters() { return agreeingClusters; }
        public void setAgreeingClusters(int n) {
            if (n < 0 || n > 20) return;
            this.agreeingClusters = n;
        }

        /** Главный конструктор — с адаптивными TP множителями */
        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take, double rr,
                         double probability, List<String> flags,
                         double fundingRate, double fundingDelta,
                         double oiChange, String htfBias, CoinCategory cat,
                         com.bot.TradingCore.ForecastEngine.ForecastResult forecast,
                         double tp1Mult, double tp2Mult, double tp3Mult) {
            this.symbol = symbol; this.side = side;
            this.price = price; this.stop = stop; this.take = take;
            this.rr = rr; this.probability = probability;
            this.flags = flags != null ? Collections.unmodifiableList(new ArrayList<>(flags)) : List.of();
            this.fundingRate = fundingRate; this.fundingDelta = fundingDelta;
            this.oiChange = oiChange; this.htfBias = htfBias; this.category = cat;
            this.forecast = forecast;
            this.trendPhase = forecast != null ? forecast.trendPhase.name() : "UNKNOWN";
            this.tp1Mult = tp1Mult; this.tp2Mult = tp2Mult; this.tp3Mult = tp3Mult;
            // Compute robustAtrPct from stop distance (best available proxy without passing ATR directly)
            this.robustAtrPct = price > 0 ? Math.abs(price - stop) / price : 0.01;
            // Stamp creation time — all overloads chain through this ctor.
            this.createdAtMs = System.currentTimeMillis();

            double risk = Math.abs(price - stop);
            boolean long_ = side == com.bot.TradingCore.Side.LONG;
            this.tp1 = long_ ? price + risk * tp1Mult : price - risk * tp1Mult;
            this.tp2 = long_ ? price + risk * tp2Mult : price - risk * tp2Mult;
            this.tp3 = long_ ? price + risk * tp3Mult : price - risk * tp3Mult;
        }

        /** Обратная совместимость — без адаптивных TP (стандартные 1.0/2.0/3.2) */
        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take, double rr,
                         double probability, List<String> flags,
                         double fundingRate, double fundingDelta,
                         double oiChange, String htfBias, CoinCategory cat,
                         com.bot.TradingCore.ForecastEngine.ForecastResult forecast) {
            this(symbol, side, price, stop, take, rr, probability, flags,
                    fundingRate, fundingDelta, oiChange, htfBias, cat, forecast,
                    1.0, 2.0, 3.2);
        }

        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take,
                         double probability, List<String> flags) {
            // Цепочка: 7-arg → 14-arg (forecast=null) → 16-arg (tp mults=1.0/2.0/3.2)
            this(symbol, side, price, stop, take, 2.0, probability, flags,
                    0, 0, 0, "NONE", CoinCategory.ALT, null);
        }

        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take,
                         double probability, List<String> flags,
                         double fundingRate, double oiChange, String htfBias) {
            // Цепочка: 10-arg → 14-arg (forecast=null) → 16-arg (tp mults=1.0/2.0/3.2)
            this(symbol, side, price, stop, take, 2.0, probability, flags,
                    fundingRate, 0, oiChange, htfBias, CoinCategory.ALT, null);
        }
        // ── Префиксы внутренних флагов движка — не показываем трейдеру ──────────
        // Всё что начинается с этих префиксов — внутренняя механика анализатора.
        // Трейдеру важны только: размер позиции, OBI, дельта объёма, конфлюэнция.
        private static final java.util.Set<String> INTERNAL_FLAG_PREFIXES = java.util.Set.of(
                "GIC_", "FC_", "PH_", "STRUCT_", "ATR_", "ADX_", "LATE_", "CRASH_CONF_",
                "CONFL_", "HIGH_ATR", "BULL_DIV", "BEAR_DIV", "BULL_DIV_PENALTY",
                "BEAR_DIV_PENALTY", "BULL_DIV_VOL_OVERRIDE", "BEAR_DIV_VOL_OVERRIDE",
                "HIDDEN_BULL_DIV_S_PENALTY", "HIDDEN_BEAR_DIV_L_PENALTY",
                "DIV_", "LONG_CRASH_PENALTY", "CLUST_", "LEXH_", "SEXH_", "REV_",
                "ANTI_LAG_", "RSI_SHIFT_", "EARLY_VETO_", "EARLY_BULL", "EARLY_BEAR",
                "BTC_CRASH", "BTC_ACCEL", "IMP_UP", "IMP_DN", "PULL_UP", "PULL_DN",
                "COMP_BREAK_", "HH_HL", "LL_LH", "FVG_", "OB_", "LIQ_SWEEP_",
                "VD_BUY", "VD_SELL", "VOL_SPIKE", "1H_BULL", "1H_BEAR", "2H_BULL",
                "2H_BEAR", "1H2H_BULL", "1H2H_BEAR", "HTF_CONFLICT", "VWAP_BULL",
                "VWAP_BEAR", "FR_NEG", "FR_POS", "FR_FALL", "FR_RISE", "OI_UP", "OI_DN",
                "PUMP_HUNT_",
                // PATCH #20: Added missing internal prefixes that could leak to Telegram
                "BOS5_",      // e.g. BOS5_LVL=219.3400 — internal swing level number
                "CHOCH_",     // internal BoS direction flags
                "LOW_CLUSTERS_", // internal cluster count debug info
                "DYN_THRESH_",   // internal threshold debug
                "LATE_ENTRY_SIZE_CUT", // internal size flag (shown via SIZE= already)
                "ATR_STOP", "STRUCT_STOP", "STRUCT_WIDE", // internal stop-type debug
                "VOL_NEUTRAL",   // internal volume state
                "CVD_BUY", "CVD_SELL", // base CVD — shown via ⚠️CVD_DIV in traderFlags
                "GIC_VETO_",     // internal GIC veto log
                "VDA_DIV_",      // VDA divergence — internal
                "EXHAUST_PENALTY_", // exhaustion penalty debug
                "EXHAUST_VETO_",  // exhaustion hard veto debug
                // [MODULE 3 v33] VSA internal flags — structural signals, not for Telegram
                "VSA_STOP_VOL_", "VSA_EFFORT_FAIL_", "VSA_NO_SUPPLY", "VSA_NO_DEMAND",
                "VSA_ABSORB_", "VSA_WEAK_BRK_",
                // [MODULE 1 v33] FR Momentum internal flags
                "FR_PEAK_WARN", "FR_TROUGH_WARN", "FR_ACCEL_DIV_"
        );

        /** Флаги видимые трейдеру — размер, OBI, дельта, конфлюэнция, фаза */
        private List<String> traderFlags() {
            // Priority-ranked flag rendering.
            // PROBLEM: old code showed CVD_DIV⚠ + CVD_SELL + CVD_DIV_BEAR — 3 flags for 1 fact.
            // FIX: deduplicate by semantic group, cap at 5 visible flags, priority order:
            //   1. Risk warnings (VOLATILE, BTC_BLOCK, LOW_SESSION)
            //   2. Execution quality (SIZE, LIQ_MAGNET, BTC_SYNC)
            //   3. Volume context (CVD, VOL_OPPOSE)
            //   4. Confluence (CONFL)
            //   5. TP mode (TP×TREND/RANGE/СКАЛЬП)
            //   6. Session (NY)

            List<String> result  = new java.util.ArrayList<>();
            boolean cvdShown     = false; // deduplicate: CVD_DIV⚠ + CVD_SELL + CVD_DIV_BEAR → 1 flag
            boolean volShown     = false; // deduplicate: VOL_OPPOSE + VOL_NEUTRAL → 1 flag
            boolean tpShown      = false; // only one TP mode flag

            // ── PASS 1: always-show flags (risk-critical) ─────────────────────
            for (String f : flags) {
                if (f.equals("HIGH_ATR"))          { result.add("⚡ VOLATILE");    continue; }
                if (f.equals("GIC_BLOCK"))         { result.add("🔴 BTC_BLOCK");   continue; }
                if (f.equals("SESS_LOW"))          { result.add("🌙 НОЧЬ");        continue; }
                if (f.startsWith("LIQ_MAGNET"))    { result.add("🧲 " + f);        continue; }
                // [v76] THIN_LIQ — daily volume <$5M means real fill on this pair
                // can slip 0.05–0.20% from displayed entry/SL. Trader needs to see
                // this BEFORE clicking — execution slippage on thin pairs eats
                // expected R:R asymmetrically (entry slip + exit slip on TP/SL).
                if (f.equals("THIN_LIQ"))          { result.add("💧 ТОНК.ЛИКВИД"); continue; }
            }

            // ── PASS 2: execution & context flags ─────────────────────────────
            for (String f : flags) {
                if (f.startsWith("SIZE="))         { result.add(f);                continue; }
                if (f.startsWith("GIC_BOOST"))     { result.add("📡 BTC_SYNC");    continue; }
                if (f.startsWith("GIC_WEAK"))      { result.add("⚠️ BTC_WEAK");    continue; }
                if (f.equals("SESS_NY"))           { result.add("🗽 NY");           continue; }
            }

            // ── PASS 3: volume — deduplicated (show only strongest CVD signal) ─
            for (String f : flags) {
                // CVD group: CVD_DIV_BEAR, CVD_DIV_BULL, CVD_DIV⚠, CVD_SELL, CVD_BUY → 1 flag
                if (!cvdShown && (f.startsWith("CVD_DIV_BEAR") || f.startsWith("CVD_DIV_BULL"))) {
                    String dir = f.contains("BEAR") ? "↓" : "↑";
                    result.add("⚠️ CVD_DIV" + dir);
                    cvdShown = true;
                    continue;
                }
                if (!cvdShown && f.equals("CVD_DIV⚠")) {
                    result.add("⚠️ CVD_DIV");
                    cvdShown = true;
                    continue;
                }
                // VOL_OPPOSE — show once
                if (!volShown && f.equals("VOL_OPPOSE")) {
                    result.add("🔻 VOL_OPP");
                    volShown = true;
                    continue;
                }
                // VDA: VDA+0.75 → "⚡VDA↑", VDA-0.80 → "⚡VDA↓"
                if (f.startsWith("VDA+") || f.startsWith("VDA-")) {
                    result.add("⚡VDA" + (f.startsWith("VDA+") ? "↑" : "↓"));
                    continue;
                }
            }

            // ── PASS 4: confluence + TP mode ──────────────────────────────────
            for (String f : flags) {
                if (f.startsWith("CONFL_L") || f.startsWith("CONFL_S")) {
                    result.add("🔥 " + f);
                    continue;
                }
                if (!tpShown) {
                    if (f.startsWith("TP_TREND_EARLY")) { result.add("🚀 TP×TREND+"); tpShown = true; continue; }
                    if (f.startsWith("TP_TREND"))       { result.add("📈 TP×TREND");  tpShown = true; continue; }
                    if (f.equals("TP_RANGE"))            { result.add("↔️ TP×RANGE");  tpShown = true; continue; }
                    if (f.equals("TP_EXHAUST"))          { result.add("⛽ TP×СКАЛЬП"); tpShown = true; continue; }
                }
            }

            // ── PASS 5: μ micro-momentum adjustment ───────────────────────────
            for (String f : flags) {
                if (f.startsWith("μ")) { result.add(f); break; }
            }

            // ── PASS 6: SL adjustment flag (important for trader) ─────────────
            for (String f : flags) {
                if (f.startsWith("SL_ADJ")) { result.add("📐 " + f); break; }
            }

            // Cap at 6 flags max — cognitive load limit.
            // A trader needs to act in <3 seconds. More than 6 flags = ignored.
            if (result.size() > 6) {
                return result.subList(0, 6);
            }

            return result;
        }


        public String toTelegramString() {
            boolean isLong  = side == com.bot.TradingCore.Side.LONG;
            AssetType assetType = detectAssetType(symbol);

            // Адаптивный формат цены (научная нотация исключена)
            String fmt = price < 0.0001 ? "%.8f"
                    : price < 0.001  ? "%.6f"
                      : price < 0.01   ? "%.5f"
                        : price < 1      ? "%.4f"
                          : price < 100    ? "%.4f"
                            : price < 10000  ? "%.2f"
                              : "%.2f";

            // [v75] Честный расчет дистанций в процентах. Точность снижена 2 → 1
            // знак после запятой: трейдер быстрее читает "+2.5%" чем "+2.53%",
            // и десятые в SL/TP визуальный шум, не информация.
            // [v86.47] PnL-ЗНАК, не ценовая дельта: знак отражает ПРИБЫЛЬ/УБЫТОК.
            // Для SHORT TP теперь "+" (профит при движении вниз), SL "−" (убыток
            // при движении вверх) — как и для LONG. Раньше показывались сырые
            // (price-entry)/entry, из-за чего у шорта TP выглядел минусом, а SL
            // плюсом («плюсы наоборот»). dir переворачивает знак для шорта.
            double dir = isLong ? 1.0 : -1.0;
            double slPct  = dir * (stop - price) / price * 100;
            double tp1Pct = tp1 > 0 ? dir * (tp1 - price) / price * 100 : 0;
            double tp2Pct = tp2 > 0 ? dir * (tp2 - price) / price * 100 : 0;
            double tp3Pct = tp3 > 0 ? dir * (tp3 - price) / price * 100 : 0;

            // Локализация времени
            java.time.ZonedDateTime now = java.time.ZonedDateTime.now(DecisionEngineMerged.USER_ZONE);
            String timeStr = now.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            String zoneId = DecisionEngineMerged.USER_ZONE.getId();
            String city = zoneId.contains("/") ? zoneId.substring(zoneId.lastIndexOf('/') + 1).replace('_', ' ') : zoneId;

            // Сборка строгого вертикального сообщения
            // [v75.1] Symbol name через mdEscape — некоторые контракты Binance
            // (особенно кросс-фьючерсы) могут содержать `_` в тикере, например
            // 1000PEPE_USDT в early-listing. Без escape это ломает Markdown.
            StringBuilder sb = new StringBuilder();
            sb.append(assetType.emoji).append(" *").append(_mdEscape(symbol)).append("*")
                    .append(" · ").append(assetType.label).append("\n");
            sb.append(isLong ? "🟢 *LONG*\n" : "🔴 *SHORT*\n");
            sb.append("━━━━━━━━━━━━━━━━━━━━━━━\n");
            sb.append("▫️ Вход:    `").append(String.format(fmt, price)).append("`\n");
            sb.append("━━━━━━━━━━━━━━━━━━━━━━━\n");
            // [v75] TP precision 2 → 1: читается быстрее, десятые роли не играют
            // [v86.46] + R-множитель: TP1 стоит на 1.0R (= дистанция SL, частичный выход 50%),
            // поэтому |TP1%| == |SL%| — это дизайн, а не «TP на стоп-лоссе».
            double riskAbsPct = Math.abs(slPct);
            if (tp1 > 0) sb.append(String.format("🎯 TP1:    `" + fmt + "`  (%+.1f%% · %.1fR)%n", tp1, tp1Pct,
                    riskAbsPct > 1e-9 ? Math.abs(tp1Pct) / riskAbsPct : 0));
            if (tp2 > 0) sb.append(String.format("🎯 TP2:    `" + fmt + "`  (%+.1f%% · %.1fR)%n", tp2, tp2Pct,
                    riskAbsPct > 1e-9 ? Math.abs(tp2Pct) / riskAbsPct : 0));
            // [v80] TP3 убран из вывода — пользователь практически не доходит до него.
            // Внутренняя логика TP3 (для расчётов trailing-stop) остаётся.
            sb.append("━━━━━━━━━━━━━━━━━━━━━━━\n");
            sb.append(String.format("🛑 SL:      `" + fmt + "`  (%+.1f%%)%n", stop, slPct));

            // [v75 FIX] Explicit Risk:Reward display.
            // Раньше трейдер должен был в уме делить tp%/|sl%|. Теперь видит сразу.
            // Используем R:R до TP2 как наиболее представительный (TP1 — частичный
            // выход, TP3 — exit-runner). Защищаемся от деления на 0 и аномалий.
            double rrToTp2 = (Math.abs(slPct) > 1e-9 && tp2Pct != 0)
                    ? Math.abs(tp2Pct) / Math.abs(slPct) : 0;
            if (rrToTp2 > 0.1) {
                sb.append(String.format("⚖️ R:R (TP2): *1:%.1f*%n", rrToTp2));
            }
            sb.append("━━━━━━━━━━━━━━━━━━━━━━━\n");

            // [v61] Honest display. No cosmetic shrinkage — show the real model score.
            // Dispatcher filters sub-30-sample signals upstream, so any signal reaching
            // Telegram has earned its score through strict gates (prob≥78, clusters≥4).
            // NOTE: probability is already clamped to PROB_CEIL=85 at line ~3353 (calibrate),
            // so no extra clamp needed here. Keeping the floor at 0 as paranoia guard.
            double _prob = Math.max(0.0, probability);

            // [v78.3] SIGNAL GRADE — единый показатель который трейдер видит сразу.
            // Объединяет 3 независимых фактора качества:
            //   1) probability (после калибровки)
            //   2) число согласующихся кластеров (структура/моментум/объём/HTF/derivatives/early)
            //   3) состояние калибратора (сколько outcomes уже учтено)
            // Grade A = доверять и торговать обычным размером
            // Grade B = ОК сигнал, торговать ×0.7
            // Grade C = в paper / ×0.3 на live
            // Grade D = только paper
            // [v86.48] CLUSTER/GRADE DISPLAY FIX. countAgreeingClusters() re-derives
            // the count from flag substrings and only recognises OLD VCB-style flags
            // (CLUSTER_x/RSI/VOL_…); a TREND signal carries HTF_UP/ADX/HTF_BIAS_ALIGN,
            // of which only "HTF" matches → it returned 1 for EVERY trend signal, so a
            // genuine 3-cluster 75% signal printed "Кластеров: 1 · Grade D" (looked like
            // trash). The strategy already stores the authoritative count via
            // setAgreeingClusters (TREND=3, VCB=5/4, funding=1) — the SAME value the
            // BotMain quality gate enforces (getAgreeingClusters). Use it; fall back to
            // the flag re-count only when unset (-1). Display-only, no behaviour change.
            int _clusterCount = getAgreeingClusters() >= 0
                    ? getAgreeingClusters() : countAgreeingClusters();
            String grade = computeSignalGrade(_prob, _clusterCount);
            sb.append(String.format("🏷️ Grade: *%s*  ·  Кластеров: %d%n",
                    grade, _clusterCount));

            // [project_state id=23] The "Cal: N" counter and the three calibration branches
            // are gone with the calibrator. The card always printed "Cal: 0" and
            // "Калибровка обучается", because nothing ever recorded an outcome — a progress
            // bar for training that was not happening.
            sb.append(String.format("📊 Скор: *%.0f%%*  _%s_%n",
                    _prob, signalQualityLabel(_prob)));

            // [v75 FIX] CRITICAL BUG: traderFlags() never displayed.
            // Was computed (deduplicates CVD_DIV, prioritises by importance, caps at 6),
            // then dropped on the floor. As a result trader saw price/SL/TP but had
            // NO information on WHY: position size (SIZE=N$), confluence level
            // (CONFL_L4), CVD divergence, BTC sync, liquidity magnet, etc.
            // Now: render the prioritized list as a 📋 Контекст section.
            // Empty list → skip section entirely (don't show empty header).
            //
            // [v75.1] Markdown safety: flag names contain underscores (LIQ_MAGNET,
            // CONFL_L4, CVD_DIV, etc.). In Telegram parse_mode=Markdown an odd
            // count of unescaped underscores → HTTP 400 → fallback to plain text
            // (with visible asterisks/underscores). Pre-escape `_` and `*` in
            // each rendered flag so the string is always parser-safe.
            List<String> _tFlags = traderFlags();
            if (_tFlags != null && !_tFlags.isEmpty()) {
                sb.append("📋 ");
                for (int i = 0; i < _tFlags.size(); i++) {
                    if (i > 0) sb.append("  ");
                    sb.append(_mdEscape(_tFlags.get(i)));
                }
                sb.append("\n");
            }

            // Warn trader when SL is very tight relative to ATR.
            double _slPctAbs = Math.abs(slPct);
            if (_slPctAbs > 0 && _slPctAbs < 0.60) {
                sb.append("\n⚠️ _Стоп очень тесный — риск выноса шумом_");
            }

            // [v76] Explicit slippage warning for thin-liquidity pairs.
            // The flag 💧 ТОНК.ЛИКВИД in Pass 1 above is a passing label; this
            // is an explicit message telling the trader what to expect on fill.
            // Without it, traders treat displayed entry/SL as exact when on a
            // $2-5M-volume pair the realistic slip is ±0.05-0.20% per leg.
            if (flags != null && flags.contains("THIN_LIQ")) {
                sb.append("\n⚠️ _Малый объём — реальный fill ±0.05–0.20%_");
            }

            // [v82] Time-stop expectation. SYNC с ISC.TIME_STOP_BARS (default 12 = 180 min).
            // История бага: v75 заявлял 90 мин, v81 укоротил до 60 мин но строка осталась
            // 90 — пользователь видел одно, бот делал другое. Сейчас default ISC = 12 баров
            // = 180 мин. Если меняешь ISC_TIME_STOP_BARS env, обнови соответствующее число
            // здесь. (Не делаем cross-class import чтобы не плодить зависимости.)
            // [v86.20 CLEANUP] Was hardcoded "180 мин" (stale 15m value) — misleading on 1h,
        // where the real position time-stop (PositionTracker PT_TIME_STOP_MS) is 480 мин.
        // Show the value that actually applies for the current PRIMARY_TF.
        // [v86.91] 4h → 1440 мин (24ч), синхрон с PT_TIME_STOP_MS / ISC / BT.
        // [v86.94] 30m → 300 мин (10 баров × 30мин), синхрон с PT_TIME_STOP_MS / ISC / BT.
        String _tsPrimaryTf = System.getenv().getOrDefault("PRIMARY_TF", "1h").trim();
        int _tsMin = "1h".equals(_tsPrimaryTf) ? 480 : "4h".equals(_tsPrimaryTf) ? 1440 : "30m".equals(_tsPrimaryTf) ? 300 : 180;
        sb.append(String.format("%n⏳ Time-stop: %d мин", _tsMin));

            sb.append("\n⏱ ").append(timeStr).append(" · ").append(city);

            return sb.toString();
        }

        @Override public String toString() { return toTelegramString(); }

        /**
         * [v75.1] Escape Telegram Markdown v1 special chars in dynamic content.
         *
         * Telegram parse_mode=Markdown treats *, _, ` as formatting markers.
         * If our content contains an odd number of these (e.g. "LIQ_MAGNET=66200"
         * has one `_`), the parser thinks formatting is unclosed → HTTP 400 →
         * the whole alert falls back to ugly plain text with visible *_`.
         *
         * This method preserves the structure of intentional formatting (added
         * by toTelegramString) by only being applied to dynamic strings — flag
         * names, symbol names — never to the static template.
         *
         * NOTE: We do NOT escape `*` here because trader flags may legitimately
         * contain emoji-prefixed structure that doesn't include `*`. If a flag
         * ever does (none currently), add it here.
         */
        private static String _mdEscape(String s) {
            if (s == null || s.isEmpty()) return s;
            // Escape only the characters that actually appear in dynamic content
            // and that Markdown v1 treats as pair-markers.
            StringBuilder sb = new StringBuilder(s.length() + 4);
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '_' || c == '*' || c == '`' || c == '[' || c == ']') {
                    sb.append('\\');
                }
                sb.append(c);
            }
            return sb.toString();
        }

        /**
         * Converts raw probability score [0..100] to a human-readable quality descriptor.
         *
         * CONTEXT FOR TRADER:
         *   The percentage shown is a composite technical conviction score — it reflects
         *   how many independent signal clusters agree, how strong the trend/reversal
         *   structure is, and how well aligned higher-timeframe context is.
         *   It is NOT a calibrated win-rate (calibration activates after ~50 resolved trades).
         *   Use it as a RELATIVE quality filter: 80%+ = high conviction, 65-79% = moderate.
         *
         * Labels are intentionally short (fit on mobile) and avoid misleading words like
         * "probability" or "win rate" until the calibrator has sufficient data.
         */
        private static String signalQualityLabel(double prob) {
            // [FIX] Labels reflect cluster/model score only — NOT calibrated win-rate.
            // Calibration activates after 50+ resolved trades.
            if (prob >= 83) return "сильный кластер";
            if (prob >= 77) return "хороший кластер";
            if (prob >= 70) return "умеренный кластер";
            if (prob >= 65) return "базовый кластер";
            return "слабый кластер";
        }

        /**
         * [v78.3] Считает число согласующихся кластеров для grade-расчёта.
         * Кластеры в архитектуре: STRUCTURE, MOMENTUM, VOLUME, HTF, DERIVATIVES, EARLY.
         * Флаги в idea.flags содержат имена кластеров когда они «голосуют» за сигнал.
         */
        private int countAgreeingClusters() {
            if (flags == null || flags.isEmpty()) return 0;
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (String f : flags) {
                if (f == null) continue;
                String u = f.toUpperCase();
                if (u.contains("CLUSTER_S") || u.contains("CLUST_S") || u.startsWith("STR_") || u.contains("BOS") || u.contains("FVG"))
                    seen.add("STR");
                if (u.contains("CLUSTER_M") || u.contains("CLUST_M") || u.contains("RSI") || u.contains("MACD") || u.contains("DIVERG"))
                    seen.add("MOM");
                if (u.contains("CLUSTER_V") || u.contains("CLUST_V") || u.contains("VOL_") || u.contains("VSA") || u.contains("OFV"))
                    seen.add("VOL");
                if (u.contains("CLUSTER_H") || u.contains("CLUST_H") || u.contains("HTF") || u.contains("1H_") || u.contains("2H_"))
                    seen.add("HTF");
                if (u.contains("CLUSTER_D") || u.contains("CLUST_D") || u.contains("OBI") || u.contains("FR_") || u.contains("OI_"))
                    seen.add("DRV");
                if (u.contains("CLUSTER_E") || u.contains("CLUST_E") || u.contains("EARLY") || u.contains("PUMP_HUNT"))
                    seen.add("EARLY");
            }
            return seen.size();
        }

        /**
         * [v78.3] Единый Grade A/B/C/D — то что трейдер видит сразу.
         *
         *  Grade A = 4+ кластера, prob >= 70, калибратор обучен (n >= 50).
         *            → Доверять, торговать обычным размером.
         *  Grade B = 3 кластера, prob >= 60.
         *            → Хороший сигнал, размер ×0.7.
         *  Grade C = 2 кластера, prob >= 55, или калибратор еще учится.
         *            → На live с размером ×0.3, либо в paper.
         *  Grade D = меньше — только paper / observation.
         */
        /**
         * [project_state id=23] The calibrator sample count used to be the third factor here.
         * It was ALWAYS 0 — nothing ever recorded an outcome — so calOK was permanently false,
         * grade A was unreachable and the B branch always collapsed to prob >= 70. This two-arg
         * form is behaviourally identical to the old one; only the dead factor is gone.
         */
        private static String computeSignalGrade(double prob, int clusters) {
            if (clusters >= 3 && prob >= 70.0) return "B";
            if (clusters >= 2 && prob >= 55.0) return "C";
            return "D";
        }
    }

    //  PUBLIC API

    // [MODULE 1 v33] FR MOMENTUM HISTORY — stores last N funding rate snapshots per symbol.
    // Needed to compute 2nd derivative (acceleration) of funding rate.
    // Deque bounded at FR_HISTORY_SIZE to prevent memory growth.
    // Each entry: [fundingRate, timestamp] stored as double[2].
    private final Map<String, Deque<double[]>> frHistory = new ConcurrentHashMap<>();
    private static final int FR_HISTORY_SIZE = 12; // ~1 hour at 5-min refresh = 12 snapshots

    public void updateFundingOI(String sym, double fr, double oi, double oi1h, double oi4h) {
        FundingOIData prev = fundingCache.get(sym);
        double prevFr    = prev != null ? prev.fundingRate   : fr;
        double prevDelta = prev != null ? prev.fundingDelta  : 0.0;
        double delta     = fr - prevFr;
        double accel     = delta - prevDelta; // 2nd derivative: is FR changing faster or slower?

        // [MODULE 1] Persist FR history for rolling acceleration analysis
        Deque<double[]> hist = frHistory.computeIfAbsent(sym, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        hist.addLast(new double[]{fr, System.currentTimeMillis()});
        while (hist.size() > FR_HISTORY_SIZE) hist.removeFirst();

        // [BUG-FIX v33.1] frHistory TTL eviction — remove entries older than 2 hours.
        // Without this, pairs that stop trading (delisted, low vol rotated out) permanently
        // accumulate entries in frHistory. At FR_HISTORY_SIZE=12 per pair × N dead pairs
        // over weeks of uptime → significant memory leak on Railway container.
        long twoHoursAgo = System.currentTimeMillis() - 2 * 60 * 60_000L;
        hist.removeIf(e -> e[1] < twoHoursAgo);

        fundingCache.put(sym, new FundingOIData(fr, oi, oi1h, oi4h, prevFr, delta, accel));
    }

    public FundingOIData getFundingOI(String sym) {
        FundingOIData d = fundingCache.get(sym);
        return (d != null && d.isValid()) ? d : null;
    }





    //  CORE GENERATE — v110 VWAP MEAN REVERSION (Plan B revision 2)
    //
    //  Старая v100 (Funding Rate MR): хорошая теория, на 15-дневном бэктесте
    //  только 7 сделок — funding extremum-ы редкие. Пользователю нужно больше
    //  сигналов для адекватной частоты сетапов.
    //
    //  Новая гипотеза: VWAP Mean Reversion с volume-confirmation.
    //
    //  Экономическое обоснование: VWAP (rolling 24h, 96 баров m15) — это
    //  справедливая цена дня, взвешенная по объёму. Когда цена отклоняется
    //  от VWAP на 1.8+ stdev И импульс сопровождается ОБЫЧНЫМ объёмом
    //  (а не institutional-flush), это retail-driven перенос. Statistically
    //  значимый возврат к VWAP в течение 4-12 баров. Edge документирован
    //  с 2018, до сих пор работает на менее ликвидных перпах (alts).
    //
    //  Активация LONG (цена ниже VWAP, отскок ожидается):
    //    - (price - vwap) / vwap < -1.8 × rolling_stdev_pct(60)
    //    - volume[-1] < 1.3 × volume_sma20  (НЕ institutional flush)
    //    - ATR percentile (m15) ∈ [0.30, 0.85]
    //    - BTC regime != STRONG_DOWN / IMPULSE_DOWN / CRASH / PANIC / CHOPPY
    //    - cooldown 60 минут на пару прошёл
    //
    //  Активация SHORT (цена выше VWAP, откат ожидается): зеркально.
    //
    //  Exit:
    //    SL  = entry ± 1.2 × ATR(14, m15)  — узкий, mean-reversion
    //    TP1 = 1.0R  — close 50%, move SL to BE (executor)
    //    TP2 = 1.5R  — close remaining (R:R 1:1.5 как договорились)
    //    Time stop: 3 часа = 12 m15 баров (управляется ISC)
    //
    //  Probability scoring:
    //    Base 0.55. +0.04 за каждое: deviation > 2.5 sigma, RSI confirm,
    //    BTC neutral. Cap 0.70.
    //
    //  ПАРАМЕТРЫ ЗАФИКСИРОВАНЫ. Не подкручивать под результаты бэктеста.

    // ──────────────────────────────────────────────────────────────────────
    // PHASE 2: REGIME-AWARE STRATEGY ROUTER  [v92 2026-05-10]
    // ──────────────────────────────────────────────────────────────────────
    //
    // Phase 1: generate() called only VWAP-MR. Other detectors (PumpHunter,
    // BoS) ran but only as TAGS, not as trade generators. Result: ~1.5
    // trades/pair/day ceiling, blind to trends and pumps.
    //
    // Phase 2: generate() is now a ROUTER. Detects market regime (1h ADX-based),
    // routes to appropriate strategy. Each strategy can independently open
    // trades. Cross-strategy cooldown via csLastSignalTime prevents whipsaw
    // (one symbol → max one open trade across all strategies).
    //
    // Strategies:
    //   1. PumpHunter setup     — pre-pump/exhaustion (any regime, top priority)
    //   2. Breakout             — when 1h ADX > PHASE2_BREAKOUT_MIN_ADX + BoS
    //   3. VWAP Mean Reversion  — when ranging (1h ADX < PHASE2_RANGE_MAX_ADX)
    //
    // Why this is safe (no whipsaw, no double-fire):
    //   - Only ONE strategy fires per pair per cycle (router selects)
    //   - csLastSignalTime is GLOBAL across all strategies (cross-lock)
    //   - correlationGuard in SignalSender still active (cluster-cap)
    //   - CHOPPY/UNCLEAR regime → fallback to MR or reject
    //
    // Env tunables (all optional, sensible defaults):
    //   PHASE2_PUMPHUNTER_ENABLE   — enable PumpHunter as generator (default false; set "true" to experiment)
    //   PHASE2_BREAKOUT_ENABLE     — enable Breakout strategy (default false; set "true" to experiment)
    //   PHASE2_BREAKOUT_MIN_ADX    — 1h ADX threshold for TREND (default 25.0)
    //   PHASE2_RANGE_MAX_ADX       — 1h ADX threshold for RANGE (default 22.0)
    //   PHASE2_PUMP_MIN_STRENGTH   — min PumpHunter strength to fire (default 0.50)
    // ──────────────────────────────────────────────────────────────────────



    /**
     * VOLATILITY COMPRESSION BREAKOUT (VCB) v7 [2026-05-25] — PROFESSIONAL
     *
     * Стратегия которую используют institutional desks на 15m crypto.
     * Концепция: ловим момент когда smart money закончил накопление
     * (volatility compression в нижних 15% percentile) и начал двигать
     * (breakout с volume confirmation + HTF align + momentum).
     *
     * ПОЧЕМУ ЭТО РАБОТАЕТ ЛУЧШЕ RSI DIVERGENCE / EMA PULLBACK:
     *
     *   1. SQUEEZE = ОБЪЕКТИВНОЕ накопление позиции. Когда волатильность
     *      сжата в bottom 10-15% percentile за 96 баров, это значит крупный
     *      игрок собирает позицию тихо. Это measurable fact, не предположение.
     *
     *   2. BREAKOUT WITH VOLUME = ПОДТВЕРЖДЕНИЕ направления. Squeeze release
     *      без volume = false breakout (часто разворачивается). Squeeze release
     *      с volume >1.5× = ENGAGED institutional flow = trend начался.
     *
     *   3. HTF + MOMENTUM ALIGNMENT = mathematical edge. Торговать с HTF
     *      трендом + RSI на нашей стороне = WR на 8-12pp выше contra-trend.
     *
     *   4. NOT EXTENDED = анти-FOMO. Не входим в parabolic moves где
     *      probability mean reversion >>  trend continuation.
     *
     * SETUP LONG (SHORT зеркально):
     *
     *   1. SQUEEZE CONTEXT — bandwidthPctile <= 0.20 за последние 8 баров
     *      (т.е. в окне 8 баров был хотя бы 1 squeeze)
     *
     *   2. BREAKOUT — close текущего бара > upper BB (для long)
     *
     *   3. VOLUME CONFIRMATION — volume bar > 1.5× SMA20
     *
     *   4. CANDLE STRENGTH — body > 50% range + close > open (для long)
     *
     *   5. HTF ALIGNED — price 1h > EMA50_1h × 0.99 (не сильно против)
     *
     *   6. LTF MOMENTUM — RSI(14) > 50 (для long; < 50 для short)
     *
     *   7. NOT EXTENDED — за последние 7 баров не должно быть 6+ same-direction
     *
     *   8. ATR sanity 0.4% - 3.5%
     *
     *   9. HARD BLOCKS — MEME skip, BTC PANIC/CRASH, BTC.onlyShort блочит LONG
     *
     * RISK:
     *   - SL = middle BB (BB EMA20) OR entry − ATR×1.4 (что ближе к entry)
     *   - SL range: 0.5% — 2.5%
     *   - TP1 = 1.0R partial 50%, TP2 = 2.2R
     *   - R:R 1:2.2 breakeven WR = 31%. С трендом ожидаем 50-60% WR
     *
     * EXPECTED:
     *   - 40-80 trade/30 days × 30 pairs (1-3 trade/day)
     *   - WR 50-60%
     *   - NetPnL +10..+20% после fees/slippage
     */

    /**
     * [v9.9 2026-05-29] MEAN REVERSION v1 — RANGE-bound markets strategy.
     *
     * DISABLED by default. Enable via env MEAN_REV_ENABLED=1.
     *
     * Концепция: VCB работает в trending markets, но crypto часто в RANGE
     * (ADX<22, BB wide). В range market VCB генерирует мало signals → bot idle.
     * Mean Reversion ловит BB extreme touches в этих режимах = complementary
     * strategy without competing с VCB.
     *
     * SETUP LONG (mirror для SHORT):
     *   1. Market RANGE: ADX_15m < 22 AND ADX_1h < 25
     *   2. NOT in squeeze: bandwidthPctile > 0.30 (squeeze = VCB territory)
     *   3. BB lower touch: percentB < 0.05 (price near/below lower BB)
     *   4. RSI extreme: RSI(14) < 35 (oversold)
     *   5. Normal volume: volRatio 0.5-1.5 (not breakout volume)
     *   6. ATR sanity: 0.5% - 5%
     *   7. BTC regime: not PANIC/CRASH, not onlyShort blocks
     *
     * RISK:
     *   - SL = lower_BB - ATR×0.5 (beyond extreme + buffer)
     *   - TP = mid_BB (revert to mean)
     *   - R:R typically 1.3-1.8 (lower than VCB)
     *   - Position size 50% от VCB (untested)
     *
     * EXPECTED (math):
     *   - 30-50 signals/мес в range periods
     *   - WR target 50-55%
     *   - R:R 1:1.5 avg → expectancy +0.25R/trade
     *   - Monthly contribution: +2-3%/мес NetPnL
     *
     * SAFETY:
     *   - Returns null если все filters не passed (no signal)
     *   - Lower prob cap 0.78 (vs VCB 0.85) пока не validated
     *   - Cross-strategy cooldown shared (csLastSignalTime)
     */

    // [v86.84] FLOW_FADE — mean-reversion range-edge fade, GATED to fire ONLY when the
    // ADVERSE aggressor taker-flow is EXHAUSTING at the edge, ONLY in chop (htfSep<TA_HTF_SEP_MIN
    // = exactly where TREND is silent). Falsification (MR×flow-exhaustion, v86.83) showed the
    // exhaustion subset = WR 71% / +0.543% vs the rest WR 40% / −0.304%. LEAK-FREE: flow is
    // computed ONLY from CLOSED pre-entry bars (the forming bar is stripped, mirror of v86.15).
    // Measure-only shadow (FLOW_FADE-SHADOW); never feeds calibrator/ISC. Clone of MR + 4 gates
    // (chop / wick-rejection / flow-exhaustion / stricter R:R). Returns null on any reject (MR
    // semantics); the generate() dispatch converts null → reject("flow_fade_no_setup").


    // [v8.4 CLEANUP] Удалены dead helpers от старой Sweep+Reclaim стратегии:
    //   tpDetectTrendDirection, tpDetectPullback, tpCheckEntryBar,
    //   tpIsExtendedMove, tpComputeStop — не вызываются нигде после VCB v7.
    // Оставлены только active helpers: tpComputeVolSma, tpIsMajorCoin.




    // [Phase 2.3 rollback 2026-05-10] Defaults flipped true→false. Backtests
    // showed Phase 2.1 and 2.2 underperforming Phase 1 (+4.92% vs +7.57%) on
    // identical 13-day window. PumpHunter exhaustion + Breakout opened "extra"
    // trades on bars where MR rejected, but those trades had worse expectancy
    // than skipping the bar. With both defaults=false, the router falls
    // through to MR-only — behaviorally identical to Phase 1. To re-enable
    // experimentally without code changes, set env vars to "true" in Railway.
    // [TREND-PULLBACK 2026-05-19] MR kill-switch. MR had no env flag (unlike PH/FM),
    // so it always preempted Breakout in the router. Set PHASE_MR_ENABLE=false to
    // run pure trend-pullback (Breakout-only) experiment. Default true = legacy behavior.
    private static final boolean PHASE_MR_ENABLE          = csEnvBool("PHASE_MR_ENABLE",          true);
    private static final boolean PHASE5_FUNDING_MOMENTUM_ENABLE = csEnvBool("PHASE5_FUNDING_MOMENTUM_ENABLE", true);
    private static final double  PHASE5_FUNDING_THRESHOLD       = csEnvDouble("PHASE5_FUNDING_THRESHOLD",      0.0004);
    private static final double  PHASE5_SL_ATR_MULT             = csEnvDouble("PHASE5_SL_ATR_MULT",            2.0);
    private static final double  PHASE5_TP_R                    = csEnvDouble("PHASE5_TP_R",                   2.0);

    // ─────────────────────────────────────────────────────────────────────
    // STRATEGY: FUNDING RATE MOMENTUM (Phase 5.0 — 2026-05-10)
    // ─────────────────────────────────────────────────────────────────────
    // Hypothesis: extreme funding rates indicate one-sided positioning that
    // the market historically resolves via squeezes/flushes. Academic research
    // (Cube Exchange, OUINEX, ScienceDirect 2025) supports a stable edge on
    // ±0.04%+ funding events with ~58-65% win rate on 6-24h horizons.
    //
    // This strategy is FUNDAMENTALLY DIFFERENT from MR:
    //   - MR uses price (deviation from VWAP)
    //   - FM uses POSITIONING (funding rate of perpetual contract)
    //   - Both can fire on the same bar — they rarely will (extreme funding
    //     usually coincides with extreme prices that MR already filters as
    //     mr_funding_overheated_*, leaving the slot clean for FM).
    //
    // Backtest support:
    //   - SimpleBacktester now loads historical funding rates per symbol
    //   - DE.setSimulatedFunding() injects per-bar funding into fundingCache
    //   - Live: fundingCache populated by SignalSender.refreshAllFundingRates()
    // ─────────────────────────────────────────────────────────────────────
    // [v86.0] TREND-ALIGNED INTRADAY (designed for PRIMARY_TF=1h)
    // ─────────────────────────────────────────────────────────────────────
    // Thesis: pure intraday prediction is noise (proven). The ONE directional
    // premium that survives is TREND — but it lives on higher timeframes. So:
    //   • DIRECTION comes from the HTF (c1h = 4h when primary is 1h): EMA20>EMA50,
    //     rising, price above EMA50 → only trade WITH the bigger trend.
    //   • TIMING comes from the primary (c15 = 1h): a pullback to EMA20 that
    //     RESUMES in the trend direction (don't chase, buy the dip in an uptrend).
    //   • EXIT is same-day: ATR-based SL/TP (R:R≈1.8) + the engine's time-stop.
    // This captures a slice of the trend premium while closing within a day, and
    // avoids the HFT zone (no millisecond games). Honest: edge here is thin and
    // MUST be confirmed by walk-forward — this is the best version, not a promise.
    // All thresholds are env-tunable (TA_*) for tuning без передеплоя.


    // ─────────────────────────────────────────────────────────────────────
    // [v86.60] TREND_EARLY — ранний вход: первый 1h-откат после СВЕЖЕГО
    // пересечения 4h EMA20/50 (возраст ≤ 12 закрытых 4h-баров = 48ч).
    // ─────────────────────────────────────────────────────────────────────
    // Тезис: текущий TREND по построению поздний (требует зрелый тренд через
    // htfSep≥0.4%). TREND_EARLY торгует ту же механику отката/возобновления, но
    // в МОЛОДОМ тренде — самая длинная дорога до TP2 (спроектировано 8-агентным
    // анализом 12.06.2026; единственная непробованная ось: все прошлые
    // эксперименты меняли 1h-триггер, эта меняет ЗРЕЛОСТЬ HTF-тренда).
    // СТРУКТУРНАЯ НЕПЕРЕСЕКАЕМОСТЬ с TREND: требует htfSep < 0.4% — точное
    // дополнение гейта TREND (sep≥0.4%) → на одном баре сработать могут только
    // ВМЕСТО друг друга, никогда вместе (иммунитет к разбавлению эджа v86.24).
    // Компенсаторы вместо chop-гейта (мы сознательно инвертируем его — честный
    // риск, ~половина свежих пересечений = пила): (1) возраст ≤ 12; (2) 4h ADX
    // ≥ 15 И растёт vs 3 бара назад; (3) опц. structConfirm: свинг-структура 1h
    // согласна (BOS-bias). Весь 1h-кор (откат/тело/RSI/ADX+DI/чоп/объём/BTC) —
    // ДОСЛОВНО как у TREND. SL/TP/скоринг — те же. Floor TE_MIN_CONF=50 на время
    // тени (порог заморозим по СОБСТВЕННЫМ тирам режима при промоушене).
    // ПУТЬ: только measure-only тень в startup-BT (TE-SHADOW). KILL-линии
    // предзарегистрированы: median slPct < ~1.0% (кост >0.25R) = смерть;
    // объём-вместо-эджа = смерть; красный в чопе = смерть.

    // [v86.77] MOMENTUM — «ЛОВЛЯ ВЗРЫВА»: ловим СТАРТ большого движения (AVAX/TAO памп),
    // который trend пропускает (входит поздно, у вершины). Прямой ответ на жалобу юзера
    // «теряю большие свечи». Тезис: взрывная свеча (range ≥ 2.5×ATR + объём ≥ 2.5×SMA) с
    // сильным ОДНОСТОРОННИМ агрессор-флоу (taker-buy, v86.68) = реальный катализатор, не
    // фейк-спайк. Входим С движением; ТУГОЙ стоп (1.2×ATR, режем быстро) + БОЛЬШАЯ цель
    // (3.5R) → профиль НИЗКИЙ WR / крупные winner'ы (моментум, не тренд). Честный риск:
    // голый пробой мёртв (VCB −96%); flow+объём — новый фильтр реальности vs фейк. Только
    // measure-only тень. (Перепрофилирован из v86.69 FLOW_BREAK value-area — был красный.)

    // ──────────────────────────────────────────────────────────────────────
    // [v86.89 ABSORB_BREAK] STRUCTURAL INVERSE of MOMENTUM/generateFlowBreak.
    // MOMENTUM needs an EXPLOSIVE big-range bar; ABSORB_BREAK needs a passive
    // wall ABSORBING one-sided aggressor flow first — on CLOSED bars this shows
    // as HIGH taker volume + TINY price range (a "stall", not an explosion).
    // When price then BREAKS the stall level WITH continued flow, it runs.
    // Fully backtestable on per-bar taker-buy data (BT candles since v86.68).
    // LEAK-FREE: window n-2..n-4 and release bar n-1 are CLOSED (forming bar
    // stripped at top, mirror generateFlowBreak's strip); never reads c15.get(n)
    // or any future bar. BT fills entry on the next bar's open.
    // ──────────────────────────────────────────────────────────────────────




    // ──────────────────────────────────────────────────────────────────────
    // VWAP & deviation helpers
    // ──────────────────────────────────────────────────────────────────────




    // ──────────────────────────────────────────────────────────────────────
    // CleanStrategy v111 parameters — env-overridable. DO NOT TUNE on
    // observed backtest results. If hypothesis fails on backtest, switch
    // hypothesis entirely — don't fiddle with thresholds.
    //
    // [v90 1H-PRIMARY 2026-05-09] Defaults retuned for 1h primary TF:
    //   - VWAP window: 96 bars × 1h = 96h (4 days) — was 24h on 15m
    //   - Deviation window: 48 bars × 1h = 2 days — was 60 × 15m = 15h
    //   - Time stop: 8 bars × 1h = 8h — was 12 × 15m = 3h
    //   - Cooldown: 240 min = 4h — was 60 min on 15m
    //   - SL multiplier: 1.5× ATR — wider on 1h to absorb intra-bar noise
    //   - Sigma threshold: 1.6 — slightly looser, fewer 1h bars exist than 15m
    //
    // To revert to 15m defaults set env CS_PROFILE=15m (overrides below).
    // ──────────────────────────────────────────────────────────────────────
    // [v86.91] 4h-support helpers (duplicated per-file by design — no new classes).
    // [v86.94] 30m-support: 30m arm added everywhere 4h has one (tfMin/barsPerDay,
    // trendAge-on-primary, time-stop). HTF for 30m = 4h (reuses the wired 4h HTF).
    private static int    tfMin(String tf)      { return "15m".equals(tf)?15 : "4h".equals(tf)?240 : "30m".equals(tf)?30 : 60; }
    private static int    barsPerDay(String tf) { return "15m".equals(tf)?96 : "4h".equals(tf)?6  : "30m".equals(tf)?48 : 24; }
    private static long   tfBarMs(String tf)     { return tfMin(tf)*60_000L; }
    private static final String CS_PRIMARY_TF = System.getenv().getOrDefault("PRIMARY_TF", "1h").trim();
    private static final boolean CS_IS_15M = "15m".equals(CS_PRIMARY_TF);
    private static final boolean CS_IS_4H  = "4h".equals(CS_PRIMARY_TF);
    private static final boolean CS_IS_30M = "30m".equals(CS_PRIMARY_TF);

    // 4h inherits the 1h sigma thresholds (1.6 / 2.2) — same else-branch value.
    private static final double CS_SIGMA_THRESHOLD   = csEnvDouble("CS_SIGMA_THRESHOLD",
            CS_IS_15M ? 1.8 : 1.6);
    private static final double CS_STRONG_SIGMA      = csEnvDouble("CS_STRONG_SIGMA",
            CS_IS_15M ? 2.5 : 2.2);
    private static final double CS_MAX_VOL_RATIO     = csEnvDouble("CS_MAX_VOL_RATIO",     1.3);
    // [v86.91] 4h: VWAP window 24 bars (4 days), deviation window 12 bars (2 days).
    private static final int    CS_VWAP_WINDOW      = (int) csEnvLong("CS_VWAP_WINDOW",
            CS_IS_15M ? 96 : CS_IS_4H ? 24 : 96);   // 24h on 15m / 4 days on 1h / 4 days on 4h
    private static final int    CS_DEVIATION_WINDOW = (int) csEnvLong("CS_DEVIATION_WINDOW",
            CS_IS_15M ? 60 : CS_IS_4H ? 12 : 48);   // 15h on 15m / 2 days on 1h / 2 days on 4h
    private static final double CS_MIN_ATR_PCTILE    = csEnvDouble("CS_MIN_ATR_PCTILE",    0.30);
    private static final double CS_MAX_ATR_PCTILE    = csEnvDouble("CS_MAX_ATR_PCTILE",    0.85);
    private static final double CS_SL_ATR_MULT       = csEnvDouble("CS_SL_ATR_MULT",
            CS_IS_15M ? 1.2 : 1.5);
    // [v86.57 EXIT-GEOMETRY B] 1.0 → 1.5: данные EXIT-SHADOW (3 прогона подряд,
    // v86.53/54) — вариант B (частичный 50% на 1.5R вместо 1.0R) бил контроль во
    // ВСЕХ прогонах (+0.174/+0.190/+0.138 vs +0.112/+0.127/+0.092 %/сд) И во всех
    // красных WF-периодах (П2/П4 не хуже контроля). Лечит payoff skew: гарант-нога
    // платит 1.5R вместо 1.0R (модальный выигрыш +0.75R vs −1R прежних +0.5R).
    // Вход НЕ меняется — те же сделки, другой выход (риск класса v86.50 исключён).
    // ОТКАТ БЕЗ ДЕПЛОЯ: env CS_TP1_R=1.0 на Railway. Вариант D (без частичного,
    // +0.20%/сд) — кандидат №2, требует кода в трекере; после валидации B живьём.
    private static final double CS_TP1_R             = csEnvDouble("CS_TP1_R",             1.5);
    private static final double CS_TP2_R             = csEnvDouble("CS_TP2_R",
            CS_IS_15M ? 1.5 : 1.8);
    // [v86.91] 4h: cooldown 960 min (= 4 bars × 240min, mirrors 1h's 4-bar cooldown);
    // time-stop 6 bars (= 24h), the hard invariant.
    // [v86.94] 30m: time-stop 10 bars (= 300 min), the hard invariant (mirror of the 4h arm).
    private static final long   CS_COOLDOWN_MS       = csEnvLong("CS_COOLDOWN_MIN",
            CS_IS_15M ? 60 : CS_IS_4H ? 960 : 240) * 60_000L;
    private static final long   CS_TIME_STOP_BARS_M15 = csEnvLong("CS_TIME_STOP_BARS",
            CS_IS_15M ? 12 : CS_IS_4H ? 6 : CS_IS_30M ? 10 : 8);
    private static final boolean CS_SKIP_MEME        = csEnvBool("CS_SKIP_MEME",           true);

    /** Per-symbol last signal timestamp for cooldown. */
    private final java.util.Map<String, Long> csLastSignalTime =
            new java.util.concurrent.ConcurrentHashMap<>();

    private static double csEnvDouble(String name, double def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return def;
        try { return Double.parseDouble(v.trim()); }
        catch (NumberFormatException e) { return def; }
    }
    private static long csEnvLong(String name, long def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return def;
        try { return Long.parseLong(v.trim()); }
        catch (NumberFormatException e) { return def; }
    }
    private static boolean csEnvBool(String name, boolean def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return def;
        v = v.trim().toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("yes");
    }



    // [v42.1 REMOVED] historicalAccuracy() — was dead code (called only from removed
    // 70/30 blend in computeClusterConfidence). Calibration now lives entirely in
    // ProbabilityCalibrator (PAV isotonic regression with vol-buckets).

    //  COOLDOWN




    private void registerSignal(String sym, com.bot.TradingCore.Side side, long now) {
        cooldownMap.put(sym + "_" + side, now);
        Deque<String> h = recentDirs.computeIfAbsent(sym, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        h.addLast(side.name());
        if (h.size() > 3) h.removeFirst();
        signalCountBySymbol.computeIfAbsent(sym, k -> new AtomicInteger(0)).incrementAndGet();
    }

    /**
     * [v24.0 FIX BUG-3] CHECK ONLY — does NOT update lastSigPrice.
     * Old code updated price here, so rejected signals blocked future valid ones.
     * A signal rejected by FC/ISC would still update lastSigPrice → next valid signal
     * 2 minutes later was blocked as "price not moved enough". Now lastSigPrice
     * is updated ONLY in confirmSignal() after ISC approves.
     */
    // PATCH #13: Dynamic price-moved threshold.
    // OLD: static 0.35% regardless of volatility — in 2% ATR market, 0.35% is one candle's noise.
    // NEW: max(0.35%, ATR * 0.15) — scales with current volatility.
    // atr14Pct is passed in from generate() where atr14 is already computed.
    private boolean priceMovedEnough(String sym, double price, double atr14Pct) {
        Double last = lastSigPrice.get(sym);
        if (last == null) return true;
        // [FLAT-FIX 2026-05-07] 0.0035 → 0.0020. На флэте альты двигаются 0.1-0.3%
        // между сигналами, старый порог 0.35% блокировал валидные re-entries.
        // 0.20% оставляет защиту от same-bar duplicates.
        double dynThreshold = Math.max(0.0020, atr14Pct * 0.15);
        return Math.abs(price - last) / last >= dynThreshold;
    }

    // Backward-compatible overload (uses static threshold when ATR not available)
    private boolean priceMovedEnough(String sym, double price) {
        return priceMovedEnough(sym, price, 0.0);
    }


    //  MARKET STRUCTURE

    public static int marketStructure(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 20) return 0;
        List<Integer> highs = swingHighs(c, 5);
        List<Integer> lows  = swingLows(c, 5);
        if (highs.size() < 2 || lows.size() < 2) return 0;

        double lastHigh = c.get(highs.get(highs.size() - 1)).high;
        double prevHigh = c.get(highs.get(highs.size() - 2)).high;
        double lastLow  = c.get(lows.get(lows.size() - 1)).low;
        double prevLow  = c.get(lows.get(lows.size() - 2)).low;

        if (lastHigh > prevHigh && lastLow > prevLow)  return  1;
        if (lastHigh < prevHigh && lastLow < prevLow)  return -1;
        return 0;
    }

    public static List<Integer> swingHighs(List<com.bot.TradingCore.Candle> c, int lr) {
        List<Integer> res = new ArrayList<>();
        for (int i = lr; i < c.size() - lr; i++) {
            double v = c.get(i).high; boolean ok = true;
            for (int l = i - lr; l <= i + lr && ok; l++)
                if (c.get(l).high > v) ok = false;
            if (ok) res.add(i);
        }
        return res;
    }

    public static List<Integer> swingLows(List<com.bot.TradingCore.Candle> c, int lr) {
        List<Integer> res = new ArrayList<>();
        for (int i = lr; i < c.size() - lr; i++) {
            double v = c.get(i).low; boolean ok = true;
            for (int l = i - lr; l <= i + lr && ok; l++)
                if (c.get(l).low < v) ok = false;
            if (ok) res.add(i);
        }
        return res;
    }



    //  MARKET STATE + HTF BIAS








    //  MATH PRIMITIVES

    /**
     * Wilder's Smoothed ATR — matches TradingView/Binance exactly.
     * Old code used simple SMA of TR — gives 15-20% different values.
     * All ATR-dependent thresholds (stops, impulse, overextension) were miscalibrated.
     */
    public double atr(List<com.bot.TradingCore.Candle> c, int period) {
        if (c.size() < period + 1) return 0;

        // Step 1: SMA seed for first 'period' TRs
        double atrVal = 0;
        int seedStart = c.size() - period * 2;
        if (seedStart < 1) seedStart = 1;
        int seedEnd = Math.min(seedStart + period, c.size());

        for (int i = seedStart; i < seedEnd; i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            atrVal += Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));
        }
        atrVal /= (seedEnd - seedStart);

        // Step 2: Wilder's smoothing for remaining bars
        for (int i = seedEnd; i < c.size(); i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));
            atrVal = (atrVal * (period - 1) + tr) / period;
        }
        return atrVal;
    }

    /**
     * Wilder's ADX — proper smoothed calculation.
     * Old code used simple sum, not Wilder's smoothing.
     * This caused ADX to read 15 where real ADX was 28 → wrong RANGE detection.
     * The bot was entering RANGE trades that were actually trending, and vice versa.
     */
    private double adx(List<com.bot.TradingCore.Candle> c, int period) {
        if (c.size() < period * 2 + 1) return 15; // not enough data

        int startIdx = c.size() - period * 2;
        if (startIdx < 1) startIdx = 1;

        // Step 1: seed +DI, -DI, TR with SMA
        double sumPlusDM = 0, sumMinusDM = 0, sumTR = 0;
        int seedEnd = startIdx + period;
        for (int i = startIdx; i < seedEnd && i < c.size(); i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            double hd = cur.high - prev.high;
            double ld = prev.low - cur.low;
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));
            sumTR += tr;
            if (hd > ld && hd > 0) sumPlusDM += hd;
            if (ld > hd && ld > 0) sumMinusDM += ld;
        }

        double smoothPlusDM = sumPlusDM;
        double smoothMinusDM = sumMinusDM;
        double smoothTR = sumTR;

        // Step 2: Wilder's smoothing for DI lines
        double sumDX = 0;
        int dxCount = 0;
        for (int i = seedEnd; i < c.size(); i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            double hd = cur.high - prev.high;
            double ld = prev.low - cur.low;
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));

            smoothTR = smoothTR - (smoothTR / period) + tr;
            double curPlusDM = (hd > ld && hd > 0) ? hd : 0;
            double curMinusDM = (ld > hd && ld > 0) ? ld : 0;
            smoothPlusDM = smoothPlusDM - (smoothPlusDM / period) + curPlusDM;
            smoothMinusDM = smoothMinusDM - (smoothMinusDM / period) + curMinusDM;

            double plusDI = smoothTR > 0 ? 100 * smoothPlusDM / smoothTR : 0;
            double minusDI = smoothTR > 0 ? 100 * smoothMinusDM / smoothTR : 0;
            double diSum = plusDI + minusDI;
            double dx = diSum > 0 ? 100 * Math.abs(plusDI - minusDI) / diSum : 0;
            sumDX += dx;
            dxCount++;
        }

        return dxCount > 0 ? sumDX / dxCount : 15;
    }

    private double ema(List<com.bot.TradingCore.Candle> c, int p) {
        if (c.size() < p) return last(c).close;
        double k = 2.0 / (p + 1), e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    /**
     * [v24.0 FIX BUG-1] Delegates to TradingCore.rsi() — SINGLE source of truth.
     * Old code had its own seed window (c.size() - period*2) which diverged 3-8 points
     * from TradingCore.rsi() seed (starting at index 1). Clusters and ForecastEngine
     * saw DIFFERENT RSI for identical data → flipped signals, false divergences.
     */
    public double rsi(List<com.bot.TradingCore.Candle> c, int period) {
        return com.bot.TradingCore.rsi(c, period);
    }







    private double vwap(List<com.bot.TradingCore.Candle> c) {
        double pv = 0, vol = 0;
        for (com.bot.TradingCore.Candle x : c) {
            double tp = (x.high + x.low + x.close) / 3.0;
            pv += tp * x.volume; vol += x.volume;
        }
        return vol == 0 ? last(c).close : pv / vol;
    }

    // ── Utility ─────────────────────────────────────────────────
    private com.bot.TradingCore.Candle last(List<com.bot.TradingCore.Candle> c) { return c.get(c.size() - 1); }
    private boolean valid(List<?> c)  { return c != null && c.size() >= MIN_BARS; }
    private double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }


}