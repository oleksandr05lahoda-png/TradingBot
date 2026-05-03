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
    public enum MarketState  { STRONG_TREND, WEAK_TREND, RANGE }
    public enum HTFBias      { BULL, BEAR, NONE }

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
    private static final int    MIN_BARS        = 150;

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
    private static final double BASE_CONF       = 53.0;
    private static final int    CALIBRATION_WIN = 120;
    private static final double MIN_CONF_FLOOR  = 50.0;
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
    // [FIX-FLAT-MARKET 2026-05-02] MIN_AGREEING_CLUSTERS 3→2.
    // В NEUTRAL BTC (str < 0.4) 3-кластерные setup'ы редкость: cluster bases
    // 56-60 минус -5 RANGE -3 ALT даёт ~48-52 prob. С 2 кластерами база ~40-45,
    // но валидные patterns (pump/dump, structural breaks) надёжно триггерят
    // Volume + Momentum, а Structure добавляется опционально. 3 было хорошо
    // в трендовом рынке, 2 даёт сигналы и в флэте. Качество защищено
    // downstream: env MIN_CONF=53, ISC track-record gate, calibrator PAV.
    private static final int    MIN_AGREEING_CLUSTERS    = 2;
    private static final double MIN_CLUSTER_SCORE        = 0.28;

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

    private static final ProbabilityCalibrator CALIBRATOR = new ProbabilityCalibrator();
    public static ProbabilityCalibrator getCalibrator() { return CALIBRATOR; }

    // [v67] Reject trace — per-reason counter, deltas printed each cycle via getAndResetRejectTrace().
    // Lets SignalSender's [DIAG] line show WHICH internal gate is killing 24 of 25 pairs.
    // Zero cost when not logging — single AtomicLong increment per reject.
    private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicLong>
            REJECT_TRACE = new java.util.concurrent.ConcurrentHashMap<>();
    static TradeIdea reject(String reason) {
        REJECT_TRACE.computeIfAbsent(reason, k -> new java.util.concurrent.atomic.AtomicLong()).incrementAndGet();
        return null;
    }
    /** Returns "k1=v1 k2=v2 ..." of deltas since last call, resets the map. */
    public static String getAndResetRejectTrace() {
        if (REJECT_TRACE.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        java.util.List<java.util.Map.Entry<String, java.util.concurrent.atomic.AtomicLong>> entries =
                new java.util.ArrayList<>(REJECT_TRACE.entrySet());
        entries.sort((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()));
        int count = 0;
        for (var e : entries) {
            long v = e.getValue().getAndSet(0);
            if (v == 0) continue;
            if (sb.length() > 0) sb.append(' ');
            sb.append(e.getKey()).append('=').append(v);
            if (++count >= 12) break;
        }
        return sb.toString();
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

    private volatile com.bot.GlobalImpulseController gicRef = null;
    private com.bot.PumpHunter pumpHunter;
    private volatile com.bot.TradingCore.ForecastEngine forecastEngine = null;

    /** Optional ISC reference — used only for chain-pause check at the top of
     *  generate(). Null until BotMain wires it; null-safe at every call site. */
    private volatile com.bot.InstitutionalSignalCore iscRef = null;
    public void setIsc(com.bot.InstitutionalSignalCore isc) { this.iscRef = isc; }

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

    /**
     * Update Bayesian prior with recent trade outcome.
     *
     * @param winRate         rolling win-rate over last N trades (0.0–1.0)
     * @param confirmedTrades total confirmed trades count
     */
    public void updateBayesPrior(double winRate, int confirmedTrades) {
        int trades = Math.max(0, confirmedTrades);
        bayesSampleTrades.set(trades);

        if (trades < BAYES_MIN_TRADES) {
            // Too few trades to trust: use neutral prior
            bayesPrior.set(0.50);
            bayesPriorFast.set(0.50);
            bayesPriorSlow.set(0.50);
            return;
        }

        // Clamp live prior to realistic range — protects against outlier streaks
        double livePrior = Math.max(0.40, Math.min(0.75, winRate));

        // Update both EWMAs
        double newFast = bayesPriorFast.get() * (1.0 - BAYES_FAST_ALPHA)
                + livePrior * BAYES_FAST_ALPHA;
        double newSlow = bayesPriorSlow.get() * (1.0 - BAYES_SLOW_ALPHA)
                + livePrior * BAYES_SLOW_ALPHA;
        bayesPriorFast.set(newFast);
        bayesPriorSlow.set(newSlow);
        bayesUpdateCount.incrementAndGet();

        // Warmup blend: 15 → 80 trades = linearly shift from neutral to EWMA blend
        double warmupMix;
        if (trades >= BAYES_FULL_TRUST) {
            warmupMix = 1.0;
        } else {
            warmupMix = (trades - BAYES_MIN_TRADES) / (double)(BAYES_FULL_TRUST - BAYES_MIN_TRADES);
            warmupMix = Math.max(0.0, Math.min(1.0, warmupMix));
        }

        // Regime detection: when fast deviates from slow, weight fast higher.
        // This is the core of two-speed design — adapts to regime change in ~8 trades
        // instead of waiting 100+ trades for pure linear blend.
        double divergence = Math.abs(newFast - newSlow);
        double fastWeight;
        if (divergence > BAYES_REGIME_SHIFT) {
            // Regime change detected — bias toward fast EWMA (cap at 0.7)
            fastWeight = Math.min(0.70, 0.40 + (divergence - BAYES_REGIME_SHIFT) * 3.0);
        } else {
            // Stable regime — slow dominates (fast weight 0.25)
            fastWeight = 0.25;
        }

        double blended = newFast * fastWeight + newSlow * (1.0 - fastWeight);
        double finalPrior = 0.50 * (1.0 - warmupMix) + blended * warmupMix;

        // Clamp final output to safe range
        bayesPrior.set(Math.max(0.42, Math.min(0.72, finalPrior)));
    }

    public double getBayesPrior() { return bayesPrior.get(); }

    /** Diagnostic — returns current regime shift magnitude (|fast-slow|). */
    public double getBayesRegimeShift() {
        return Math.abs(bayesPriorFast.get() - bayesPriorSlow.get());
    }

    /** Diagnostic — true if fast and slow EWMAs diverge beyond threshold. */
    public boolean isBayesRegimeShift() {
        return getBayesRegimeShift() > BAYES_REGIME_SHIFT;
    }

    // ── Setters ───────────────────────────────────────────────────
    public void setPumpHunter(com.bot.PumpHunter ph) { this.pumpHunter = ph; }
    public void setGIC(com.bot.GlobalImpulseController gic) { this.gicRef = gic; }
    public void setForecastEngine(com.bot.TradingCore.ForecastEngine fe) { this.forecastEngine = fe; }

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

    private boolean isPostExitBlocked(String symbol, com.bot.TradingCore.Side side) {
        cooldownGc();  // [v42.0 FIX #12] lazy TTL cleanup — was leaking forever
        String k = symbol + "_" + side.name();
        Long ts = postExitCooldown.get(k);
        if (ts == null) return false;
        long age = System.currentTimeMillis() - ts;
        if (age >= POST_EXIT_COOLDOWN_MS) {
            postExitCooldown.remove(k);
            return false;
        }
        return true;
    }

    /**
     * [v42.0 FIX #12] Lazy garbage collector for postExitCooldown.
     * Runs at most once per minute. Removes expired entries and bounds map size.
     */
    private void cooldownGc() {
        long now = System.currentTimeMillis();
        if (now - lastCooldownGcMs < 60_000L) return;
        lastCooldownGcMs = now;
        postExitCooldown.entrySet().removeIf(e -> now - e.getValue() > POST_EXIT_COOLDOWN_MS);
        if (postExitCooldown.size() > POST_EXIT_MAX_SIZE) {
            // Hard bound — drop oldest entries by timestamp
            java.util.List<Map.Entry<String, Long>> sorted =
                    new java.util.ArrayList<>(postExitCooldown.entrySet());
            sorted.sort(Map.Entry.comparingByValue());
            int toDrop = postExitCooldown.size() - POST_EXIT_MAX_SIZE;
            for (int i = 0; i < toDrop && i < sorted.size(); i++) {
                postExitCooldown.remove(sorted.get(i).getKey());
            }
        }
    }

    public void setVolumeDelta(String sym, double delta) {
        volumeDeltaMap.put(sym, delta);
        Deque<Double> hist = vdHistory.computeIfAbsent(sym, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        hist.addLast(Math.abs(delta));
        if (hist.size() > 50) hist.removeFirst();
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

    /**
     * CVD Persistence check.
     * Returns true only if the LAST minBars readings ALL agree in direction.
     * Prevents short-covering spikes from triggering LONG signals.
     * FIXED: was counting any matching bar in the window — now requires the last N to be consecutive.
     */
    private boolean isCVDPersistent(String sym, boolean bullish, int minBars) {
        Deque<Double> hist = cvdHistory.get(sym);
        if (hist == null || hist.size() < minBars) return false;
        List<Double> list = new ArrayList<>(hist);
        int n = list.size();
        // Check only the LAST minBars entries — all must agree
        for (int i = n - minBars; i < n; i++) {
            double v = list.get(i);
            if (bullish  && v <= 0.05) return false;  // any non-positive breaks the chain
            if (!bullish && v >= -0.05) return false; // any non-negative breaks the chain
        }
        return true;
    }

    private int getBullCvdPersistenceBars(String symbol) {
        if (gicRef != null) {
            com.bot.GlobalImpulseController.CascadeLevel level = gicRef.getCurrentCascadeLevel();
            if (level == com.bot.GlobalImpulseController.CascadeLevel.CRASH
                    || level == com.bot.GlobalImpulseController.CascadeLevel.PANIC) {
                return CVD_PERSIST_BARS;
            }
        }
        return getRelativeStrength(symbol) >= 0.62 ? 2 : CVD_PERSIST_BARS;
    }

    /** [v29] VDA score [-1..+1]: +1=buy acceleration, -1=sell acceleration */
    public void setVDA(String sym, double score) {
        vdaMap.put(sym, score);
    }

    private double getVolumeDeltaRatio(String sym) {
        Double current = volumeDeltaMap.get(sym);
        if (current == null || current == 0) return 0.0;
        Deque<Double> hist = vdHistory.get(sym);
        if (hist == null || hist.size() < 5) return 1.0;
        double avg = hist.stream().mapToDouble(Double::doubleValue).average().orElse(1.0);
        return avg < 1e-9 ? 1.0 : Math.abs(current) / avg;
    }

    public void updateRelativeStrength(String symbol, double symbolReturn15m, double btcReturn15m) {
        double rs;
        if (Math.abs(btcReturn15m) < 0.0001) {
            rs = symbolReturn15m > 0 ? 0.7 : 0.3;
        } else if (btcReturn15m < 0 && symbolReturn15m > 0) {
            rs = 0.85 + Math.min(Math.abs(symbolReturn15m) * 10, 0.14);
        } else {
            rs = clamp(0.5 + (symbolReturn15m - btcReturn15m) / (Math.abs(btcReturn15m) * 2), 0.0, 1.0);
        }
        Deque<Double> h = relStrengthHistory.computeIfAbsent(symbol, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        h.addLast(rs);
        if (h.size() > 20) h.removeFirst();
    }

    private double getRelativeStrength(String symbol) {
        Deque<Double> h = relStrengthHistory.get(symbol);
        if (h == null || h.isEmpty()) return 0.5;
        return h.stream().mapToDouble(Double::doubleValue).average().orElse(0.5);
    }

    //  CLUSTER SCORE HOLDER
    //  Каждый кластер хранит свой лучший LONG и SHORT score

    private static final class ClusterScores {
        double longScore  = 0;
        double shortScore = 0;
        final List<String> flags = new ArrayList<>();

        // Per-cluster cap reduced: prevents single market event
        // from inflating one cluster's score via correlated sub-signals.
        // 0.75 (was 0.85) — more conservative, fewer false-strong signals
        private static final double CLUSTER_CAP = 0.75;

        void addLong(double score, String flag) {
            longScore = Math.min(CLUSTER_CAP, Math.max(longScore, score));
            if (flag != null) flags.add(flag);
        }

        void addShort(double score, String flag) {
            shortScore = Math.min(CLUSTER_CAP, Math.max(shortScore, score));
            if (flag != null) flags.add(flag);
        }

        void boostLong(double score, String flag) {
            longScore = Math.min(CLUSTER_CAP, longScore + score);
            if (flag != null) flags.add(flag);
        }

        void boostShort(double score, String flag) {
            shortScore = Math.min(CLUSTER_CAP, shortScore + score);
            if (flag != null) flags.add(flag);
        }

        void penalizeLong(double mult) { longScore *= mult; }
        void penalizeShort(double mult) { shortScore *= mult; }

        boolean favorsLong()  { return longScore > shortScore && longScore > 0.10; }
        boolean favorsShort() { return shortScore > longScore && shortScore > 0.10; }
        boolean hasSignal()   { return longScore > 0.10 || shortScore > 0.10; }

        double netLong()  { return longScore - shortScore * 0.3; }
        double netShort() { return shortScore - longScore * 0.3; }
    }

    //  MARKET CONTEXT

    private static final class MarketContext {
        final double volMultiplier;
        final double scoreScale;
        final double thresholdScale;
        final double atrPct;

        MarketContext(double volMult, double atrPct) {
            this.volMultiplier  = volMult;
            this.atrPct         = atrPct;
            this.scoreScale     = clamp(1.0 / Math.sqrt(volMult), 0.65, 1.45);
            this.thresholdScale = clamp(volMult, 0.5, 2.0);
        }

        double s(double baseScore) { return baseScore * scoreScale; }

        static double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
    }

    private MarketContext buildMarketContext(List<com.bot.TradingCore.Candle> c15, double price) {
        double atrCurrent = atr(c15, 14);
        int baseWindow = Math.min(c15.size() - 14, 672);
        double atrBase = baseWindow >= 50
                ? atr(c15.subList(0, baseWindow), Math.min(14, baseWindow - 1))
                : atrCurrent;
        double volMult = atrBase > 0 ? atrCurrent / atrBase : 1.0;
        return new MarketContext(clamp(volMult, 0.3, 3.0), atrCurrent / (price + 1e-9));
    }

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

    /** Classify coin by robust ATR percentage (uses long-term ATR to avoid consolidation trap) */
    public static VolatilityBucket classifyVolatility(double atrPct) {
        if (atrPct < 0.005) return VolatilityBucket.LOW;
        if (atrPct < 0.015) return VolatilityBucket.MEDIUM;
        if (atrPct < 0.035) return VolatilityBucket.HIGH;
        return VolatilityBucket.EXTREME;
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

        public final double robustAtrPct;
        // Signal age tracking — enables decay-based filtering in
        // earlyTickBuffer and anywhere else ideas sit in a queue. Stale signals
        // (older than ~90s on 15m tf) lose edge because the move they predicted
        // may have already played out. Consumers use ageMs() to apply penalty.
        public final long createdAtMs;

        /** Age of the signal in milliseconds since creation. */
        public long ageMs() { return System.currentTimeMillis() - createdAtMs; }

        /**
         * Confidence decay factor (1.0 = fresh, 0.0 = fully stale).
         * Linear decay over staleThresholdMs; clamped to [0, 1].
         * Typical use: adjustedProb = probability * ageDecay(90_000L).
         */
        public double ageDecay(long staleThresholdMs) {
            if (staleThresholdMs <= 0) return 1.0;
            long age = ageMs();
            if (age <= 0) return 1.0;
            if (age >= staleThresholdMs) return 0.0;
            return 1.0 - ((double) age / staleThresholdMs);
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

        // TradingView-совместимый тикер для поиска монеты
        private static String toTradingViewTicker(String symbol) {
            // BTCUSDT → BINANCE:BTCUSDT.P (Perpetual Futures)
            return "BINANCE:" + symbol + ".P";
        }

        // Аналог в традиционных активах — помогает понять масштаб монеты
        private static String getComparableAsset(String symbol, CoinCategory cat) {
            if (cat == CoinCategory.TOP) {
                if (symbol.startsWith("BTC"))  return "🥇 ≈ Золото (XAU)";
                if (symbol.startsWith("ETH"))  return "🥈 ≈ Серебро (XAG)";
                if (symbol.startsWith("BNB"))  return "🛢 ≈ Нефть (CL)";
                if (symbol.startsWith("SOL"))  return "⛽ ≈ Газ (NG)";
                if (symbol.startsWith("XRP"))  return "🏦 ≈ Forex (EUR/USD)";
                return "💎 ≈ Платина (XPT)";
            }
            if (cat == CoinCategory.MEME) return "🎰 ≈ Пенни-акция";
            // ALT
            if (symbol.contains("AI") || symbol.contains("FET") || symbol.contains("RENDER"))
                return "🤖 ≈ Tech-акция (NVDA)";
            if (symbol.contains("LINK") || symbol.contains("DOT") || symbol.contains("ATOM"))
                return "🔗 ≈ Инфраструктура (AWS)";
            if (symbol.contains("UNI") || symbol.contains("AAVE") || symbol.contains("MKR"))
                return "🏦 ≈ Финтех-акция";
            return "📊 ≈ Mid-cap акция";
        }

        // Сектор монеты для контекста
        private static String detectCoinSector(String symbol) {
            String s = symbol.toUpperCase();
            if (s.contains("AI") || s.contains("FET") || s.contains("RENDER") || s.contains("RNDR")
                    || s.contains("AGIX") || s.contains("OCEAN")) return "AI";
            if (s.contains("UNI") || s.contains("AAVE") || s.contains("SUSHI") || s.contains("CRV")
                    || s.contains("COMP") || s.contains("MKR") || s.contains("SNX")) return "DeFi";
            if (s.contains("MATIC") || s.contains("ARB") || s.contains("OP") || s.contains("STRK")
                    || s.contains("MANTA") || s.contains("ZK")) return "L2";
            if (s.contains("SOL") || s.contains("AVAX") || s.contains("NEAR") || s.contains("APT")
                    || s.contains("SUI") || s.contains("SEI") || s.contains("TIA")) return "L1";
            if (s.contains("DOGE") || s.contains("SHIB") || s.contains("PEPE") || s.contains("FLOKI")
                    || s.contains("WIF") || s.contains("BONK") || s.contains("MEME")) return "Meme";
            if (s.contains("LINK") || s.contains("DOT") || s.contains("ATOM")
                    || s.contains("INJ") || s.contains("PYTH")) return "Infra";
            if (s.contains("ONDO") || s.contains("PENDLE") || s.contains("TRU")) return "RWA";
            return "Crypto";
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
            double slPct  = (stop - price) / price * 100;
            double tp1Pct = tp1 > 0 ? (tp1 - price) / price * 100 : 0;
            double tp2Pct = tp2 > 0 ? (tp2 - price) / price * 100 : 0;
            double tp3Pct = tp3 > 0 ? (tp3 - price) / price * 100 : 0;

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
            if (tp1 > 0) sb.append(String.format("🎯 TP1:    `" + fmt + "`  (%+.1f%%)%n", tp1, tp1Pct));
            if (tp2 > 0) sb.append(String.format("🎯 TP2:    `" + fmt + "`  (%+.1f%%)%n", tp2, tp2Pct));
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
            int _calSamples = DecisionEngineMerged.getCalibrator().totalOutcomeCount();
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
            int _clusterCount = countAgreeingClusters();
            String grade = computeSignalGrade(_prob, _clusterCount, _calSamples);
            sb.append(String.format("🏷️ Grade: *%s*  ·  Кластеров: %d  ·  Cal: %d%n",
                    grade, _clusterCount, _calSamples));

            if (_calSamples < 30) {
                sb.append(String.format("📊 Скор: *%.0f%%*  _%s_%n",
                        _prob, signalQualityLabel(_prob)));
                sb.append("_Калибровка обучается — торгуй меньшим размером_\n");
            } else if (_calSamples < 100) {
                sb.append(String.format("📊 Уверенность: *%.0f%%*  _%s_%n",
                        _prob, signalQualityLabel(_prob)));
                sb.append(String.format("_Калибровка: %d/100_%n", _calSamples));
            } else {
                sb.append(String.format("📊 Уверенность: *%.0f%%*  _%s_%n",
                        _prob, signalQualityLabel(_prob)));
            }

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

            // [v75 FIX] Time-stop expectation. ISC auto-closes positions after
            // TIME_STOP_BARS=6 bars (90 min @ 15m). Without this line trader
            // can sit in a setup for hours wondering why the bot is silent —
            // not knowing the bot has internally given up on the idea.
            sb.append(String.format("%n⏳ Time-stop: 90 мин"));

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
        private static String computeSignalGrade(double prob, int clusters, int calSamples) {
            boolean calOK = calSamples >= 50;
            if (clusters >= 4 && prob >= 70.0 && calOK)            return "A";
            if (clusters >= 3 && prob >= 60.0 && (calOK || prob >= 70.0)) return "B";
            if (clusters >= 2 && prob >= 55.0)                     return "C";
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

    public void recordSignalResult(String sym, double prob, boolean correct) {
        Deque<CalibRecord> h = calibHist.computeIfAbsent(sym, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        h.addLast(new CalibRecord(prob, correct));
        while (h.size() > CALIBRATION_WIN) h.removeFirst();
        updateSymbolThreshold(sym);
    }

    private void updateSymbolThreshold(String sym) {
        Deque<CalibRecord> hist = calibHist.get(sym);
        if (hist == null || hist.size() < 20) return;
        long correct = hist.stream().filter(r -> r.correct).count();
        double accuracy = (double) correct / hist.size();
        double base = globalMinConf.get();
        if (accuracy < 0.45)      base += 5.0;
        else if (accuracy < 0.50) base += 2.5;
        else if (accuracy > 0.65) base -= 3.0;
        else if (accuracy > 0.60) base -= 1.5;
        // compute() — атомарное обновление, безопасно при конкурентном доступе
        final double newVal = clamp(base, MIN_CONF_FLOOR, MIN_CONF_CEIL);
        symbolMinConf.compute(sym, (k, cur) -> newVal);
    }

    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             List<com.bot.TradingCore.Candle> c2h,
                             CoinCategory cat) {
        return generate(symbol, c1, c5, c15, c1h, c2h, cat, System.currentTimeMillis());
    }

    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             CoinCategory cat) {
        return generate(symbol, c1, c5, c15, c1h, null, cat, System.currentTimeMillis());
    }

    //  5m BREAK OF STRUCTURE / CHANGE OF CHARACTER
    //  Primary entry trigger: fires 1-3 bars BEFORE 15m structure confirms.
    //  BoS  = breakout WITH the HTF trend  → strong signal (score 0.62)
    //  ChoCh = breakout AGAINST HTF trend  → counter-trend caution (score 0.42)

    private static final class BosResult {
        final boolean detected;
        final boolean isBullish;  // true = BoS up (buy break), false = BoS down (sell break)
        final boolean isChoch;    // true = Change of Character (counter-trend)
        final double  swingLevel; // the level that was broken
        BosResult(boolean d, boolean b, boolean c, double l) {
            detected = d; isBullish = b; isChoch = c; swingLevel = l;
        }
        static BosResult none() { return new BosResult(false, false, false, 0); }
    }

    /**
     * Detect a Break of Structure on 5m candles.
     * Was using global MAX pivot high / MIN pivot low over the entire window.
     * A real BoS breaks the MOST RECENT swing, not an arbitrary historical extreme.
     * Fixed: scan from right-to-left and stop at the FIRST confirmed pivot.
     */
    private static BosResult detectBoS(List<com.bot.TradingCore.Candle> c5, int htfStructure) {
        if (c5 == null || c5.size() < 20) return BosResult.none();
        int n = c5.size();

        double lastSwingHigh = 0;
        double lastSwingLow  = Double.MAX_VALUE;
        boolean foundHigh = false, foundLow = false;

        // Scan right-to-left: stop at FIRST confirmed pivot (most recent)
        for (int i = n - 3; i >= 2; i--) {
            double hi = c5.get(i).high;
            double lo = c5.get(i).low;

            if (!foundHigh) {
                boolean pivotHigh = hi > c5.get(i - 1).high && hi > c5.get(i - 2).high
                        && hi > c5.get(i + 1).high && hi > c5.get(i + 2).high;
                if (pivotHigh) { lastSwingHigh = hi; foundHigh = true; }
            }
            if (!foundLow) {
                boolean pivotLow = lo < c5.get(i - 1).low && lo < c5.get(i - 2).low
                        && lo < c5.get(i + 1).low && lo < c5.get(i + 2).low;
                if (pivotLow) { lastSwingLow = lo; foundLow = true; }
            }
            if (foundHigh && foundLow) break;
        }

        double lastClose = c5.get(n - 1).close;

        boolean bosBull = foundHigh
                && lastClose > lastSwingHigh * 1.00025;
        boolean bosBear = foundLow
                && lastClose < lastSwingLow  * 0.99975;

        if (!bosBull && !bosBear) return BosResult.none();

        boolean choch = (bosBull && htfStructure == -1) || (bosBear && htfStructure == 1);

        return new BosResult(
                true,
                bosBull,
                choch,
                bosBull ? lastSwingHigh : lastSwingLow
        );
    }

    //  CORE GENERATE — v7.0 CLUSTER ARCHITECTURE

    private TradeIdea generate(String symbol,
                               List<com.bot.TradingCore.Candle> c1,
                               List<com.bot.TradingCore.Candle> c5,
                               List<com.bot.TradingCore.Candle> c15,
                               List<com.bot.TradingCore.Candle> c1h,
                               List<com.bot.TradingCore.Candle> c2h,
                               CoinCategory cat,
                               long now) {

        if (!valid(c15) || !valid(c1h)) return reject("invalid_candles");

        // [v69] Post-pump skip — ранний выход без анализа для пар, недавно заблокированных
        // post-pump вето. Экономит CPU и чистит логи от повторяющегося [POST-PUMP VETO].
        Long ppUntil = postPumpSkipUntil.get(symbol);
        if (ppUntil != null) {
            if (now < ppUntil) return reject("post_pump_cooldown");
            postPumpSkipUntil.remove(symbol); // expired
        }
        // [FIX-SYM 2026-05-02] Зеркальный ранний выход для post-dump bounce.
        Long pdUntil = postDumpSkipUntil.get(symbol);
        if (pdUntil != null) {
            if (now < pdUntil) return reject("post_dump_cooldown");
            postDumpSkipUntil.remove(symbol); // expired
        }

        double price     = last(c15).close;
        double atr14     = atr(c15, 14);
        double move5     = c15.size() >= 5 ? (last(c15).close - c15.get(c15.size() - 5).close) / price : 0.0;
        double lastRange = last(c15).high - last(c15).low;

        // [v71] Threshold 4.5→6.5. Широкая impulse-свеча 4.5×ATR это и есть
        // начало тренда — ровно то что мы хотим поймать. Блокировали именно
        // breakout-сетапы. 6.5×ATR оставляет защиту от истинных flash spikes.
        if (lastRange > atr14 * 6.5 || atr14 <= 0) return reject("range_or_atr");
        atr14 = Math.max(atr14, price * 0.0012);

        // ── GIC crash state ──────────────────────────────────────
        com.bot.GlobalImpulseController.GlobalContext gicCtx =
                gicRef != null ? gicRef.getContext() : null;
        boolean aggressiveShort = gicRef != null && gicRef.isAggressiveShortMode();
        // [FIX-SYM 2026-05-02] Зеркало aggressiveShort — используется для override
        // gicOnlyShort веток. GIC уже имеет isAggressiveLongMode() (см. GIC v69).
        boolean aggressiveLongMode = gicRef != null && gicRef.isAggressiveLongMode();
        double btcCrashScore    = gicCtx != null ? gicCtx.btcCrashScore : 0.0;
        double btcAccel         = gicCtx != null ? gicCtx.btcMomentumAccel : 0.0;
        double gicShortBoost    = gicCtx != null ? gicCtx.shortBoost : 1.0;

        // Read explicit longSuppressionMult field — no more sentinel decode.
        // Old code: if (confAdj < -50) longSuppression = (confAdj+150)/100 — opaque and fragile.
        double gicLongSuppression = gicCtx != null ? gicCtx.longSuppressionMult : 1.0;

        // [FIX-ROUND2 2026-05-02] Симметричный shortSuppression — самая
        // важная асимметрия в коде. GIC уже подавляет LONG при любой
        // слабости BTC (longSuppressionMult 0.30-0.90 при DANGER/WATCH/
        // IMPULSE_DOWN/STRONG_DOWN). Но НЕТ зеркального для SHORT при
        // силе BTC (IMPULSE_UP/STRONG_UP). Это и был встроенный SHORT-bias.
        // Вычисляем зеркальную формулу здесь же в DE — не трогая GIC class.
        double gicShortSuppression = 1.0;
        if (gicCtx != null && !aggressiveShort) {
            // Mirror longSuppressionMult logic but for BTC up moves.
            // Trigger: BTC_STRONG_UP с rawStrength≥0.60 → жёсткое подавление SHORT.
            //          BTC_IMPULSE_UP с rawStrength≥0.50 → умеренное.
            //          aggressiveLong (cascadeLevel-эквивалент для UP) → промежуточное.
            boolean btcDecelerating = btcAccel < 0.002 && gicCtx.impulseStrength < 0.65;
            if (gicCtx.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP
                    && gicCtx.impulseStrength >= 0.60) {
                if (btcDecelerating) gicShortSuppression = 0.55;
                else                 gicShortSuppression = 0.30;
            } else if (gicCtx.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_IMPULSE_UP
                    && gicCtx.impulseStrength >= 0.50) {
                if (btcDecelerating) gicShortSuppression = 0.78;
                else                 gicShortSuppression = 0.55;
            }
        }

        // GIC DIRECTIONAL GATE — consume onlyLong / onlyShort that were
        // previously computed in GIC but never enforced in generate().
        // onlyLong  = BTC_STRONG_UP + strength > 0.82  → veto all SHORT signals
        // onlyShort = BTC_STRONG_DOWN / CRASH / PANIC + strength > 0.78 → veto all LONG signals
        // Applied BEFORE cluster computation so we don't waste CPU on blocked directions.
        // Exception: aggressiveShort overrides onlyLong (crash trumps BTC up-trend for altcoins).
        // [FIX-SYM 2026-05-02] Зеркальный exception: aggressiveLongMode перекрывает
        // onlyShort (BTC strong-up trumps onlyShort). Раньше onlyShort был жёстким
        // вето без override — это был встроенный SHORT-bias на уровне GIC gate.
        if (gicCtx != null) {
            if (gicCtx.onlyLong && !aggressiveShort) {
                // BTC in extreme up-impulse — only LONG signals allowed on alts
                // SHORT against BTC STRONG_UP = very high failure rate → hard veto
                // candidateSide not known yet, so we store as pre-filter flag;
                // actual veto happens post-candidate-selection below via earlyReturn sentinel
            }
            if (gicCtx.onlyShort && !aggressiveLongMode) {
                // BTC crashing — LONG on alts = catching a falling knife
                // Hard veto: no LONG signals during BTC CRASH/PANIC mode
                // Exception: aggressiveLongMode (BTC own strong-up) overrides
            }
        }
        // Sentinel booleans for post-candidate veto (direction known after cluster aggregation)
        final boolean gicOnlyLong  = gicCtx != null && gicCtx.onlyLong  && !aggressiveShort;
        final boolean gicOnlyShort = gicCtx != null && gicCtx.onlyShort && !aggressiveLongMode;

        MarketContext mctx = buildMarketContext(c15, price);
        MarketState state  = detectState(c15);
        HTFBias     bias1h = detectBias1H(c1h);
        HTFBias     bias2h = (c2h != null && c2h.size() >= 50) ? detectBias2H(c2h) : HTFBias.NONE;

        adaptGlobalMinConf(state, atr14, price);

        // LOCAL EXHAUSTION — detect per-pair dumps/pumps independent of BTC GIC.
        // Critical for altcoins that dump alone while BTC stays flat.
        // Applied AFTER candidateSide is known (see veto below).
        LocalExhaustion localExh = detectLocalExhaustion(c15, atr14);

        // POST-PUMP / POST-DUMP DETECTION
        // LONG case (postPumpDump): монета выросла >35% за 20 свечей, потом упала >8%
        //                           от хая → ловля ножа на LONG. Кейс SOONUSDT.
        // SHORT case (postDumpBounce) [FIX-SYM 2026-05-02]: монета упала >35% за 20
        //                           свечей, потом отскочила >8% от лоу → ловля ножа
        //                           наоборот на SHORT (классический капитуляционный bounce).
        // Обе проверки используют ОДИН range threshold (postPumpGain ≥ 35% — это
        // нормализованная амплитуда движения за окно, симметрично работает в обе стороны)
        // и применяются ПЕРЕД кластерами — экономит CPU на заведомо битых сетапах.
        boolean postPumpDump   = false;
        boolean postDumpBounce = false;
        double postPumpGain = 0, postPumpDropFromHi = 0, postDumpBounceFromLo = 0;
        if (c15.size() >= 20) {
            double recentHigh = 0, recentLow = Double.MAX_VALUE;
            int fromPP = c15.size() - 20;
            for (int i = fromPP; i < c15.size(); i++) {
                recentHigh = Math.max(recentHigh, c15.get(i).high);
                recentLow  = Math.min(recentLow,  c15.get(i).low);
            }
            if (recentLow > 0 && recentHigh > 0) {
                postPumpGain         = (recentHigh - recentLow) / recentLow;
                postPumpDropFromHi   = (recentHigh - price)      / recentHigh;
                postDumpBounceFromLo = (price - recentLow)       / recentLow;
                if (postPumpGain >= 0.35) {
                    if (postPumpDropFromHi   >= 0.08) postPumpDump   = true;
                    if (postDumpBounceFromLo >= 0.08) postDumpBounce = true;
                }
            }
        }

        // ADX low in RANGE: penalty, not veto
        boolean adxRangePenalty = false;
        if (!aggressiveShort && state == MarketState.RANGE && adx(c15, 14) < 15) {
            adxRangePenalty = true;
        }

        // allFlags перенесён сюда из низа метода — PostPump блок пишет в него.
        // Все предыдущие add() в этот список сохранены ниже в том же порядке.
        List<String> allFlags = new ArrayList<>();
        if (adxRangePenalty) allFlags.add("ADX_LOW_RANGE");
        if (postPumpDump) {
            allFlags.add(String.format("POST_PUMP_DUMP_gain%.0f_drop%.0f",
                    postPumpGain * 100, postPumpDropFromHi * 100));
        }
        // [FIX-SYM 2026-05-02] Зеркальный flag для post-dump bounce.
        if (postDumpBounce) {
            allFlags.add(String.format("POST_DUMP_BOUNCE_drop%.0f_bounce%.0f",
                    postPumpGain * 100, postDumpBounceFromLo * 100));
        }

        // [v75] REGIME FILTER переделан с hard-reject на soft-penalty.
        // Старая логика: ADX<9 + atrPctile<0.08 = reject — но именно ТАК выглядит
        // pre-pump компрессия, лучший момент для входа. Теперь просто штраф,
        // а CVD/PreMove детекторы могут пробить его если есть accumulation.
        // [v75] choppy_range: вместо reject — штраф, и пропускаем если есть
        // confirming flag (REVERSAL_SETUP, FVG_FILL, OB_TAP, COMPRESSION_BREAKOUT).
        boolean flatMarketPenalty = false;
        boolean choppyRangePenalty = false;
        if (!aggressiveShort && state == MarketState.RANGE) {
            double adxVal = adx(c15, 14);
            double atrPctile = com.bot.TradingCore.atrPercentile(c15, 14, 96);
            if (adxVal < 9 && atrPctile < 0.08) {
                // Compression detected — soft penalty, let PreMove/CVD pull through.
                flatMarketPenalty = true;
                allFlags.add("FLAT_MARKET_COMPRESSION");
            }
            // [v78.3 ROLLBACK] Возвращаем hard reject в RANGE.
            // Soft-penalty подход добавлял ~25% noise в калибратор: mid-RSI
            // в плоском рынке физически не имеет edge (mean-reversion работает
            // только на extreme), а оставление сигнала на финальный MIN_CONF gate
            // приводило к низкокачественным записям в калибратор. Возврат на reject
            // даёт калибратору только сигналы с реальной структурной идеей.
            if (adxVal < 15 && atrPctile < 0.20) {
                double rsi14 = rsi(c15, 14);
                // В RANGE с низким ADX — только extreme RSI пропускаем (mean-reversion)
                if (rsi14 > 22 && rsi14 < 78) return reject("range_rsi_mid");
            }
        }

        // [v70/v75] Choppiness смягчён: CI threshold 61.8 → 68 для RANGE,
        // 68 → 72 для WEAK_TREND. RSI window 28..72 → 24..76.
        // [v75] choppy_range теперь не возвращает reject — это soft penalty.
        // Реальные reversal сетапы в чопе бывают (sweep + reclaim) — пусть проходят.
        if (!aggressiveShort && state != MarketState.STRONG_TREND && c15.size() >= 15) {
            double ci = com.bot.TradingCore.choppinessIndex(c15, 14);
            if (ci > 68.0) {
                if (state == MarketState.RANGE) {
                    double rsiVal = rsi(c15, 14);
                    if (rsiVal > 24 && rsiVal < 76) {
                        allFlags.add("CI_HIGH_" + String.format("%.0f", ci));
                        choppyRangePenalty = true;
                    }
                } else if (ci > 76.0) {
                    // [v71] CI threshold 72→76. В WEAK_TREND CI>72 случается на каждой
                    // второй паре — слишком агрессивный фильтр. 76 ловит реальный chop.
                    allFlags.add("CI_BLOCK_" + String.format("%.0f", ci));
                    return reject("choppy_weak");
                }
            }
        }

        // SYMBOL NAME FILTER — блокировка мусорных пар
        // Пары с не-ASCII символами (иероглифы, спецзнаки) = неликвид
        for (int ci = 0; ci < symbol.length(); ci++) {
            char ch = symbol.charAt(ci);
            if (ch > 127) return reject("symbol_non_ascii"); // не-ASCII = мусорная пара
        }

        int n15 = c15.size();

        // ── LATE-MOVE DETECTION (multi-window) ───────────────────────
        // Old single-window move4bars caught only ~1h displacement and
        // missed slow grinds (5-8 small candles in one direction).
        // New scheme runs three orthogonal checks:
        //   1) ATR depth across 4 / 8 / 12-bar windows
        //   2) Consecutive same-direction bar streak
        //   3) Velocity blow-up (3-bar avg vs 20-bar median)
        // Severity tiers: NONE / SOFT (penalty) / HARD (reject).
        LateMoveSignal lateMove = detectLateMove(c15, atr14);
        boolean lateEntryLong  = lateMove.severity != 0 &&  lateMove.dirUp;
        boolean lateEntryShort = lateMove.severity != 0 && !lateMove.dirUp;
        double lateMoveAtrMul  = lateMove.maxAtrMul;
        double move4bars = (n15 >= 5)
                ? last(c15).close - c15.get(n15 - 5).close
                : 0.0;
        if (lateMove.severity != 0) {
            allFlags.add("LM_" + (lateMove.severity == 2 ? "HARD" : "SOFT")
                    + "_" + (lateMove.dirUp ? "UP" : "DN"));
        }

        // [v50 §7] MOMENTUM EXHAUSTION GATE — detect spent impulses BEFORE cluster scoring.
        // If the last 3 bars show: bodies shrinking + wicks growing + volume declining
        // → the impulse is DYING. Even if clusters agree, the fuel is gone.
        boolean momentumExhausted = false;
        int exhaustionDirection = 0; // +1 = uptrend exhausting, -1 = downtrend exhausting
        if (n15 >= 6) {
            double b1 = Math.abs(c15.get(n15-1).close - c15.get(n15-1).open);
            double b2 = Math.abs(c15.get(n15-2).close - c15.get(n15-2).open);
            double b3 = Math.abs(c15.get(n15-3).close - c15.get(n15-3).open);
            double w1 = (c15.get(n15-1).high - c15.get(n15-1).low) - b1;
            double w2 = (c15.get(n15-2).high - c15.get(n15-2).low) - b2;
            double w3 = (c15.get(n15-3).high - c15.get(n15-3).low) - b3;
            double v1 = c15.get(n15-1).volume;
            double v2 = c15.get(n15-2).volume;
            double v3 = c15.get(n15-3).volume;
            // Bodies shrinking: each bar < 75% of previous
            boolean bodyShrink = b1 < b2 * 0.75 && b2 < b3 * 0.75;
            // Wicks growing: rejection increasing
            boolean wickGrow = w1 > w2 * 1.1 && w2 > w3 * 1.1;
            // Volume declining
            boolean volDecline = v1 < v2 * 0.85;
            if (bodyShrink && (wickGrow || volDecline)) {
                momentumExhausted = true;
                exhaustionDirection = move4bars > 0 ? 1 : -1;
                // allFlags.add moved below after allFlags is declared
            }
        }

        // [v50 §11] VELOCITY DECAY — 5-bar body average declining 40%+
        boolean velocityDecay = false;
        if (n15 >= 10) {
            double recentBodyAvg = 0, priorBodyAvg = 0;
            for (int i = n15-3; i < n15; i++)
                recentBodyAvg += Math.abs(c15.get(i).close - c15.get(i).open);
            for (int i = n15-8; i < n15-3; i++)
                priorBodyAvg += Math.abs(c15.get(i).close - c15.get(i).open);
            recentBodyAvg /= 3.0;
            priorBodyAvg /= 5.0;
            if (priorBodyAvg > atr14 * 0.3 && recentBodyAvg < priorBodyAvg * 0.60) {
                velocityDecay = true;
                // allFlags.add moved below after allFlags is declared
            }
        }

        //  ИНИЦИАЛИЗАЦИЯ 5 КЛАСТЕРОВ
        ClusterScores cStructure   = new ClusterScores(); // BOS, HH/HL, FVG, OB, LiqSweep
        ClusterScores cMomentum    = new ClusterScores(); // Impulse, AntiLag, Pump, Compression
        ClusterScores cVolume      = new ClusterScores(); // VolumeDelta, VolumeSpike
        ClusterScores cHTF         = new ClusterScores(); // 1H, 2H bias, VWAP
        ClusterScores cDerivatives = new ClusterScores(); // Funding, OI, Divergences
        ClusterScores cEarly       = new ClusterScores(); // [v7.1] Early Reversal Detection
        // allFlags уже объявлен выше (перед CI filter). Добавляем отложенные флаги.
        if (momentumExhausted) allFlags.add("MOM_EXHAUSTED_" + (exhaustionDirection > 0 ? "UP" : "DN"));
        if (velocityDecay) allFlags.add("VEL_DECAY");

        // 5m BREAK OF STRUCTURE — PRIMARY ENTRY TRIGGER
        // Fires 1-3 bars before 15m structure confirms.
        // BoS aligned with HTF → high-conviction structural entry.
        // ChoCh against HTF → early reversal caution (lower score).
        int htfStructure15 = marketStructure(c15); // reuse: +1 bull, -1 bear, 0 neutral
        BosResult bos5 = detectBoS(c5, htfStructure15);
        if (bos5.detected) {
            // BoS confirmed with HTF trend = strong confluence entry
            // ChoCh against HTF = possible reversal but weaker conviction
            // BoS5m score raised: BoS 0.62→0.72, ChoCh 0.42→0.52.
            // 5m BoS is the EARLIEST reliable structural signal before 15m candle closes.
            // It was underweighted vs FVG (0.50) and OB (0.52). Fixed.
            double bosBaseScore = bos5.isChoch ? 0.52 : 0.72;
            double bosScore = mctx.s(bosBaseScore);
            if (bos5.isBullish) {
                cStructure.addLong(bosScore, bos5.isChoch ? "CHOCH_UP_5M" : "BOS_UP_5M");
            } else {
                cStructure.addShort(bosScore, bos5.isChoch ? "CHOCH_DN_5M" : "BOS_DN_5M");
            }
            allFlags.add(String.format("BOS5_LVL=%.4f", bos5.swingLevel));
        }

        // [CLUSTER 0] BTC CRASH — прямой override, ДО кластеров
        double crashBoost = 0;
        if (btcCrashScore >= CRASH_SCORE_BOOST_THRESHOLD) {
            crashBoost = (btcCrashScore - CRASH_SCORE_BOOST_THRESHOLD)
                    / (1.0 - CRASH_SCORE_BOOST_THRESHOLD) * CRASH_SHORT_BOOST_BASE;
            allFlags.add("BTC_CRASH" + String.format("%.0f", btcCrashScore * 100));
        }

        if (btcAccel > 0.004 && gicCtx != null
                && gicCtx.regime != com.bot.GlobalImpulseController.GlobalRegime.NEUTRAL
                && gicCtx.regime != com.bot.GlobalImpulseController.GlobalRegime.BTC_IMPULSE_UP
                && gicCtx.regime != com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP) {
            double accelBoost = Math.min(0.50, btcAccel * 40);
            crashBoost += accelBoost;
            allFlags.add("BTC_ACCEL" + String.format("%.0f", btcAccel * 10000));
        }

        // [FIX-ROUND2 2026-05-02] BTC PUMP boost — симметричный аналог crashBoost.
        // Ранее: при btcCrashScore>=0.35 → totalShort += boost (даже до 0.75).
        //         НО для LONG не было ничего эквивалентного на росте BTC.
        //         Это асимметрия №1 — встроенный SHORT-bias на уровне scoring.
        // Теперь: если BTC в IMPULSE_UP/STRONG_UP с rawStrength>=0.45 →
        //         totalLong += proportional boost. Логика зеркальна crashBoost:
        //         сильнее BTC растёт → больше LONG-confluence на альтах.
        double pumpBoost = 0;
        if (gicCtx != null && (gicCtx.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_IMPULSE_UP
                || gicCtx.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP)
                && gicCtx.impulseStrength >= 0.45) {
            // Зеркальная формула: (impulseStrength - 0.45) / (1 - 0.45) × CRASH_SHORT_BOOST_BASE
            pumpBoost = (gicCtx.impulseStrength - 0.45) / 0.55 * CRASH_SHORT_BOOST_BASE;
            allFlags.add("BTC_PUMP" + String.format("%.0f", gicCtx.impulseStrength * 100));
        }
        if (btcAccel > 0.004 && gicCtx != null
                && (gicCtx.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_IMPULSE_UP
                || gicCtx.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP)) {
            double accelBoostUp = Math.min(0.50, btcAccel * 40);
            pumpBoost += accelBoostUp;
            allFlags.add("BTC_ACCEL_UP" + String.format("%.0f", btcAccel * 10000));
        }

        // КЛАСТЕР 1: STRUCTURE

        // Market Structure (HH/HL vs LL/LH)
        int structure = marketStructure(c15);
        if (structure ==  1) cStructure.addLong(mctx.s(0.55), "HH_HL");
        if (structure == -1) cStructure.addShort(mctx.s(0.55), "LL_LH");

        // BOS functionality deferred to ForecastEngine analysis

        // FVG
        FVGResult fvg = detectFVG(c15);
        if (fvg.detected) {
            if (fvg.isBullish) cStructure.addLong(mctx.s(0.50), "FVG_BULL");
            else               cStructure.addShort(mctx.s(0.50), "FVG_BEAR");
        }

        // Order Block
        OrderBlockResult ob = detectOrderBlock(c15);
        if (ob.detected) {
            if (ob.isBullish && price <= ob.zone * 1.008)
                cStructure.addLong(mctx.s(0.52), "OB_BULL");
            if (!ob.isBullish && price >= ob.zone * 0.992)
                cStructure.addShort(mctx.s(0.52), "OB_BEAR");
        }

        // Liquidity Sweep
        boolean liqSweep = detectLiquiditySweep(c15);
        if (liqSweep) {
            com.bot.TradingCore.Candle lc = last(c15);
            double uw = lc.high - Math.max(lc.open, lc.close);
            double lw = Math.min(lc.open, lc.close) - lc.low;
            if (lw > uw) cStructure.addLong(mctx.s(0.58), "LIQ_SWEEP_L");
            else         cStructure.addShort(mctx.s(0.58), "LIQ_SWEEP_S");
        }

        // Bullish/Bearish structure
        if (bullishStructure(c15)) cStructure.boostLong(mctx.s(0.18), null);
        if (bearishStructure(c15)) cStructure.boostShort(mctx.s(0.18), null);

        // [MODULE 3 v33] VSA — VOLUME SPREAD ANALYSIS
        // Applied as the FINAL layer of cStructure. VSA is a structural filter:
        // it confirms or contradicts what price structure is showing.
        //
        // Integration logic:
        //   Bullish VSA + bullish structure  → reinforce (boost cStructure LONG)
        //   Bearish VSA + bearish structure  → reinforce (boost cStructure SHORT)
        //   Bullish VSA + bearish structure  → conflict (partial penalty on SHORT)
        //   Bearish VSA + bullish structure  → conflict (partial penalty on LONG)
        //   NO_DEMAND / NO_SUPPLY            → exhaustion veto (hard penalty regardless of structure)
        //   WEAK_BREAKOUT detected           → scale back both sides (false breakout warning)
        //
        // VSA scan: last 3 candles (avoid stale patterns from 8+ bars ago)
        if (c15.size() >= 25) {
            com.bot.TradingCore.VsaResult vsa = com.bot.TradingCore.vsaAnalyse(c15, 3);
            if (vsa.hasSignal()) {
                double vsaW = mctx.s(vsa.strength * 0.65);

                // [BUG-FIX v33.1] DEMAND_ABSORPTION and SUPPLY_ABSORPTION are NOT
                // isBullish or isBearish in TradingCore.VsaResult (they are paradoxical
                // — a bullish-looking bar with a bearish implication and vice versa).
                // The original code placed DEMAND_ABSORPTION inside the isBullish switch,
                // making it UNREACHABLE because vsa.isBullish=false for that signal.
                // Fix: handle paradoxical patterns explicitly FIRST, before directional checks.

                if (vsa.signal == com.bot.TradingCore.VsaResult.VsaSignal.DEMAND_ABSORPTION) {
                    // Narrow UP bar + ultra-high vol = smart money selling INTO retail bids.
                    // Despite being an up-bar, this is a BEARISH distribution signal.
                    cStructure.boostShort(mctx.s(vsa.strength * 0.45), "VSA_ABSORB_D");

                } else if (vsa.signal == com.bot.TradingCore.VsaResult.VsaSignal.SUPPLY_ABSORPTION) {
                    // Narrow DOWN bar + ultra-high vol = smart money buying INTO retail sells.
                    // Despite being a down-bar, this is a BULLISH accumulation signal.
                    cStructure.boostLong(mctx.s(vsa.strength * 0.45), "VSA_ABSORB_S");

                } else if (vsa.signal == com.bot.TradingCore.VsaResult.VsaSignal.WEAK_BREAKOUT) {
                    // Wide-range breakout on low volume = probable retail fakeout.
                    double breakPenalty = mctx.s(vsa.strength * 0.50);
                    if (move5 > 0) cStructure.boostShort(breakPenalty, "VSA_WEAK_BRK_U");
                    else           cStructure.boostLong(breakPenalty,  "VSA_WEAK_BRK_D");

                } else if (vsa.isBullish) {
                    switch (vsa.signal) {
                        case STOPPING_VOLUME_BULL ->
                                cStructure.addLong(mctx.s(vsa.strength * 0.75), "VSA_STOP_VOL_B");
                        case EFFORT_TO_FALL_FAILED ->
                                cStructure.addLong(vsaW, "VSA_EFFORT_FAIL_D");
                        case NO_SUPPLY ->
                                cStructure.boostLong(mctx.s(vsa.strength * 0.40), "VSA_NO_SUPPLY");
                        default -> {}
                    }

                } else if (vsa.isBearish) {
                    switch (vsa.signal) {
                        case STOPPING_VOLUME_BEAR ->
                                cStructure.addShort(mctx.s(vsa.strength * 0.75), "VSA_STOP_VOL_S");
                        case EFFORT_TO_RISE_FAILED ->
                                cStructure.addShort(vsaW, "VSA_EFFORT_FAIL_U");
                        case NO_DEMAND ->
                                cStructure.boostShort(mctx.s(vsa.strength * 0.40), "VSA_NO_DEMAND");
                        default -> {}
                    }
                }
            }
        }

        // КЛАСТЕР 2: MOMENTUM

        // Anti-Lag (1m+5m+15m)
        // [v15.0 FIX KITEUSDT] Block ANTI_LAG in volatility squeeze (ATR < 60% of 50-bar avg)
        double atrAvg50 = 0;
        if (c15.size() >= 65) {
            for (int i = c15.size() - 50; i < c15.size(); i++) {
                int pi = Math.max(1, i - 1);
                atrAvg50 += Math.max(c15.get(i).high - c15.get(i).low,
                        Math.max(Math.abs(c15.get(i).high - c15.get(pi).close),
                                Math.abs(c15.get(i).low - c15.get(pi).close)));
            }
            atrAvg50 /= 50;
        }
        boolean atrSqueeze = atrAvg50 > 0 && atr14 < atrAvg50 * 0.60;
        CompressionResult comp = detectCompression(c15, c1);
        double vdaVal = vdaMap.getOrDefault(symbol, 0.0);
        AntiLagResult antiLag = detectAntiLag(c1, c5, c15);
        if (antiLag != null && antiLag.strength > 0.38) {
            boolean squeezeLeadAligned = atrSqueeze && (
                    (comp.breakout && comp.direction == antiLag.direction)
                            || (Math.abs(vdaVal) >= 0.22
                            && Math.signum(vdaVal) == Math.signum((double) antiLag.direction))
            );
            if (!atrSqueeze || squeezeLeadAligned) {
                double bonus = mctx.s(antiLag.strength * (atrSqueeze ? 0.82 : 1.30));
                if (antiLag.direction > 0) cMomentum.addLong(bonus, "ANTI_LAG_UP");
                else                       cMomentum.addShort(bonus, "ANTI_LAG_DN");
                if (atrSqueeze) allFlags.add("ANTI_LAG_SQUEEZE_SOFT");
            } else {
                allFlags.add("ANTI_LAG_SQUEEZE_BLOCKED");
            }
        }

        // Impulse (>0.55 ATR за 5 баров)
        boolean impulseFlag = impulse(c15);
        if (impulseFlag) {
            if (move5 > 0) cMomentum.addLong(mctx.s(0.50), "IMP_UP");
            else           cMomentum.addShort(mctx.s(0.50), "IMP_DN");
        }

        // Pullback к EMA21
        boolean pullUp   = pullback(c15, true);
        boolean pullDown = pullback(c15, false);
        if (pullUp)   cMomentum.addLong(mctx.s(0.55), "PULL_UP");
        if (pullDown) cMomentum.addShort(mctx.s(0.55), "PULL_DN");

        // Age-aware PumpHunter integration. Key changes vs v50:
        //   1. Use decayedStrength() instead of raw strength — events older than 3min
        //      get progressively weaker voice, 0 at 15min. Fixes late-echo signals.
        //   2. Handle new reversal types: PUMP_EXHAUSTION_SHORT → crosses SHORT into
        //      cMomentum at higher weight (0.70 vs 0.55) because exhaustion is
        //      structurally stronger evidence than generic continuation.
        //   3. PRE_PUMP_LONG / PRE_DUMP_SHORT routed to cEarly (anticipatory) cluster.
        if (pumpHunter != null) {
            com.bot.PumpHunter.PumpEvent pump = pumpHunter.getRecentPump(symbol);
            if (pump != null) {
                double effStr = pump.decayedStrength();
                if (effStr > 0.45) {
                    if (pump.type == com.bot.PumpHunter.PumpType.PUMP_EXHAUSTION_SHORT) {
                        cMomentum.addShort(mctx.s(effStr * 0.70), "PUMP_EXH_S");
                        allFlags.add("PUMP_EXH_TOP");
                    } else if (pump.type == com.bot.PumpHunter.PumpType.DUMP_EXHAUSTION_LONG) {
                        cMomentum.addLong(mctx.s(effStr * 0.70), "DUMP_EXH_L");
                        allFlags.add("DUMP_EXH_BOT");
                    } else if (pump.type == com.bot.PumpHunter.PumpType.PRE_PUMP_LONG) {
                        cEarly.addLong(mctx.s(effStr * 0.60), "PRE_PUMP_L");
                        allFlags.add("PRE_PUMP");
                    } else if (pump.type == com.bot.PumpHunter.PumpType.PRE_DUMP_SHORT) {
                        cEarly.addShort(mctx.s(effStr * 0.60), "PRE_DUMP_S");
                        allFlags.add("PRE_DUMP");
                    } else if (pump.isBullish()) {
                        cMomentum.addLong(mctx.s(effStr * 0.55), "PUMP_HUNT_B");
                    } else if (pump.isBearish()) {
                        cMomentum.addShort(mctx.s(effStr * 0.55), "PUMP_HUNT_S");
                    }
                }
            }
        }

        // Compression Breakout
        if (comp.breakout) {
            if (comp.direction > 0) cMomentum.addLong(mctx.s(0.58), "COMP_BREAK_UP");
            else                    cMomentum.addShort(mctx.s(0.58), "COMP_BREAK_DN");
        }

        // [v50 §4] PRE-BREAKOUT ENTRY — THE KEY PREDICTIVE SIGNAL.
        // When compression is detected (volatility squeeze) but breakout has NOT happened yet,
        // check order flow direction. If VDA shows strong one-sided flow (institutions
        // accumulating/distributing quietly), enter BEFORE the breakout candle.
        // This is the single most impactful change: entering 1-3 bars early.
        // [v50 §4] PRE-BREAKOUT ENTRY — THE KEY PREDICTIVE SIGNAL.
        // atrSqueeze = volatility compressed. !comp.breakout = breakout hasn't happened yet.
        // If VDA shows strong one-sided flow → institutions accumulating → enter BEFORE breakout.
        if (atrSqueeze && !comp.breakout) {
            // Strong one-sided flow during compression = imminent breakout
            if (Math.abs(vdaVal) >= 0.25) {
                double preBreakScore = mctx.s(0.72); // high weight — this is our edge
                if (vdaVal > 0) {
                    cMomentum.addLong(preBreakScore, "PRE_BREAK_UP");
                    cEarly.addLong(mctx.s(0.40), "PRE_BRK_EARLY_UP");
                    allFlags.add("PRE_BREAK_UP");
                } else {
                    cMomentum.addShort(preBreakScore, "PRE_BREAK_DN");
                    cEarly.addShort(mctx.s(0.40), "PRE_BRK_EARLY_DN");
                    allFlags.add("PRE_BREAK_DN");
                }
            }
            // Moderate flow + CVD agreement = softer pre-breakout signal
            else if (Math.abs(vdaVal) >= 0.15 && Math.abs(cvdMap.getOrDefault(symbol, 0.0)) > 0.15) {
                boolean sameDir = Math.signum(vdaVal) == Math.signum(cvdMap.getOrDefault(symbol, 0.0));
                if (sameDir) {
                    double softPreBreak = mctx.s(0.45);
                    if (vdaVal > 0) {
                        cMomentum.addLong(softPreBreak, "PRE_BREAK_SOFT_UP");
                        allFlags.add("PRE_BREAK_SOFT_UP");
                    } else {
                        cMomentum.addShort(softPreBreak, "PRE_BREAK_SOFT_DN");
                        allFlags.add("PRE_BREAK_SOFT_DN");
                    }
                }
            }
        }

        // VDA — Volume Delta Acceleration. Leading indicator: fires on first ticks
        // of a real impulse before the candle shows it. Weight 0.60 = highest in cMomentum.
        // VDA weight raised 0.60→0.75 — this is the most leading indicator we have.
        if (Math.abs(vdaVal) >= 0.20) {
            double vdaScore = mctx.s(Math.min(0.75, Math.abs(vdaVal) * 0.85));
            if (vdaVal > 0) cMomentum.addLong(vdaScore,  "VDA_BUY_ACCEL");
            else            cMomentum.addShort(vdaScore, "VDA_SELL_ACCEL");
            allFlags.add(String.format("VDA%+.2f", vdaVal));
        }
        if (move5 > 0.002 && vdaVal < -0.25) { cMomentum.penalizeLong(0.55);  allFlags.add("VDA_DIV_BEAR"); }
        if (move5 < -0.002 && vdaVal > 0.25) { cMomentum.penalizeShort(0.55); allFlags.add("VDA_DIV_BULL"); }

        // КЛАСТЕР 3: VOLUME

        // PATCH #4: CVD triple-count fix.
        // SHORT-COVERING BOUNCE FIX: LONG requires CVD persistent 3+ bars.
        // A single CVD spike on the bottom is NOT real demand — it's shorts covering stops.
        // Only flag CVD_BUY (LONG signal) when positive CVD holds across multiple bars.
        // SHORT side has no persistence requirement: one bar of aggressive selling is enough.

        Double vd = volumeDeltaMap.get(symbol);
        double vdRatio = getVolumeDeltaRatio(symbol);
        double cvdVal = cvdMap.getOrDefault(symbol, 0.0);

        if (vd != null || Math.abs(cvdVal) > 0.10) {
            double vdScore = (vd != null && vdRatio > 1.5)
                    ? mctx.s(Math.min(0.55, vdRatio * 0.14)) : 0.0;
            double cvdScore = Math.abs(cvdVal) > 0.10
                    ? mctx.s(Math.min(0.65, Math.abs(cvdVal) * 0.70)) : 0.0;
            double bestScore = Math.max(vdScore, cvdScore);

            // LONG CVD remains stricter than SHORT, but RS leaders can confirm faster.
            int bullPersistBars = getBullCvdPersistenceBars(symbol);
            boolean cvdBullPersistent = isCVDPersistent(symbol, true, bullPersistBars);
            boolean volBull = (vd != null && vd > 0 && vdRatio > 1.5) || (cvdVal > 0.10 && cvdBullPersistent);
            boolean volBear = (vd != null && vd < 0 && vdRatio > 1.5) || cvdVal < -0.10; // bear: instant

            // DISTRIBUTION DETECTOR
            // Classic distribution: volume expanding on last 3 bars, but price gain
            // decelerating > 50% vs prior 3 bars. Institutions offloading to retail —
            // CVD still positive (retail hitting ask) but price can no longer advance.
            // When detected → volBull is suppressed so LONG loses its CVD confirmation.
            boolean distributionPattern = false;
            if (volBull && n15 >= 8) {
                double prevAvgGain = 0, currAvgGain = 0;
                double prevVolSum = 0, currVolSum = 0;
                for (int i = n15 - 7; i < n15 - 4; i++) {
                    prevAvgGain += Math.max(0, c15.get(i).close - c15.get(i - 1).close);
                    prevVolSum += c15.get(i).volume;
                }
                for (int i = n15 - 3; i < n15; i++) {
                    currAvgGain += Math.max(0, c15.get(i).close - c15.get(i - 1).close);
                    currVolSum += c15.get(i).volume;
                }
                prevAvgGain /= 3.0; currAvgGain /= 3.0;
                double prevAvgVol = prevVolSum / 3.0;
                double currAvgVol = currVolSum / 3.0;

                boolean volumeExpanding = currAvgVol > prevAvgVol * 1.2;
                // [v50 §9] Distribution detection tightened: 50%→35% deceleration.
                // At 50% the distribution was nearly complete before detection.
                // At 35% we catch it 1-2 bars earlier while institutions are still offloading.
                boolean momentumDecelerating = prevAvgGain > 0 && currAvgGain < prevAvgGain * 0.35;
                distributionPattern = volumeExpanding && momentumDecelerating;

                if (distributionPattern) {
                    volBull = false; // block CVD-based LONG confirmation
                    allFlags.add("DISTRIBUTION_PATTERN");
                    // Also add a soft short hint — effort-fail at top is bearish
                    cVolume.addShort(mctx.s(0.35), "DIST_EFFORT_FAIL");
                }
            }

            if (volBull && bestScore > 0) cVolume.addLong(bestScore,  cvdScore >= vdScore ? "CVD_BUY" : "VD_BUY");
            if (volBear && bestScore > 0) cVolume.addShort(bestScore, cvdScore >= vdScore ? "CVD_SELL" : "VD_SELL");

            // Divergence penalties — structurally distinct from base CVD
            if (move5 > 0.002 && cvdVal < -0.10) cVolume.penalizeLong(0.60);
            if (move5 < -0.002 && cvdVal > 0.10) cVolume.penalizeShort(0.60);
        }

        // CVD-divergence (high-confidence threshold: price strongly disagrees with CVD)
        boolean cvdDivBear = move5 > 0.003 && cvdVal < -0.15;
        boolean cvdDivBull = move5 < -0.003 && cvdVal > 0.15;
        // Remove duplicate penalize calls — addShort/addLong already handle it
        if (cvdDivBear) { cVolume.addShort(mctx.s(0.55), "CVD_DIV_BEAR"); allFlags.add("CVD_DIV⚠"); }
        if (cvdDivBull) { cVolume.addLong(mctx.s(0.55),  "CVD_DIV_BULL"); allFlags.add("CVD_DIV⚠"); }

        // Volume Spike (independent signal — price-volume acceleration, not delta direction)
        if (volumeSpike(c15, cat)) {
            if (move5 > 0) cVolume.boostLong(mctx.s(0.22), "VOL_SPIKE");
            else           cVolume.boostShort(mctx.s(0.22), "VOL_SPIKE");
        }

        // КЛАСТЕР 4: HTF (Higher Timeframe)

        // 1H Bias
        if (bias1h == HTFBias.BULL) {
            cHTF.addLong(mctx.s(0.60), "1H_BULL");
        } else if (bias1h == HTFBias.BEAR) {
            cHTF.addShort(mctx.s(0.60), "1H_BEAR");
        }

        // 2H Bias
        if (bias2h == HTFBias.BULL) {
            cHTF.addLong(mctx.s(0.55), "2H_BULL");
        } else if (bias2h == HTFBias.BEAR) {
            cHTF.addShort(mctx.s(0.55), "2H_BEAR");
        }

        // 1H + 2H согласие = bonus
        if (bias1h == bias2h && bias1h != HTFBias.NONE) {
            if (bias1h == HTFBias.BULL) cHTF.boostLong(mctx.s(0.30), "1H2H_BULL");
            else                         cHTF.boostShort(mctx.s(0.35), "1H2H_BEAR");
        }

        // HTF конфликт = мягкое ослабление (аддитивное, не множительное)
        if ((bias1h == HTFBias.BULL && bias2h == HTFBias.BEAR) ||
                (bias1h == HTFBias.BEAR && bias2h == HTFBias.BULL)) {
            if (!aggressiveShort) {
                // Аддитивный штраф вместо *= 0.50
                cHTF.boostLong(-0.15, null);
                cHTF.boostShort(-0.15, null);
            } else {
                cHTF.boostLong(-0.20, null);
            }
            allFlags.add("HTF_CONFLICT");
        }

        // VWAP alignment
        int vwapLen = Math.min(50, c15.size());
        double vwapVal = vwap(c15.subList(c15.size() - vwapLen, c15.size()));
        if (price > vwapVal * 1.0008) cHTF.boostLong(mctx.s(0.18), "VWAP_BULL");
        if (price < vwapVal * 0.9992) cHTF.boostShort(mctx.s(0.18), "VWAP_BEAR");

        // VWAP counter-trend penalty.
        // Price aggressively below VWAP + LONG candidate = trading against institutional flow.
        // Price aggressively above VWAP + SHORT candidate (not crash mode) = same issue.
        // This is the #1 killer of false LONG setups in bear markets.
        if (price < vwapVal * 0.9985 && !aggressiveShort) {
            cHTF.penalizeLong(0.60);
            allFlags.add("VWAP_BELOW_PENALTY");
        }
        if (price > vwapVal * 1.0015 && !aggressiveShort) {
            cHTF.penalizeShort(0.65);
            allFlags.add("VWAP_ABOVE_PENALTY");
        }

        // КЛАСТЕР 5: DERIVATIVES (Funding, OI, Divergences)

        FundingOIData frData = fundingCache.get(symbol);
        boolean hasFR = false;
        double fundingRate = 0, fundingDelta = 0, oiChange = 0;
        // Funding Rate Confidence Filter
        // FR > +0.05% → ритейл переплачивает за LONG → штраф -10 для LONG, бонус +5 для SHORT
        // FR < -0.05% → ритейл переплачивает за SHORT → штраф -10 для SHORT, бонус +5 для LONG
        double frConfPenaltyLong  = 0;
        double frConfPenaltyShort = 0;
        if (frData != null && frData.isValid()) {
            fundingRate  = frData.fundingRate;
            fundingDelta = frData.fundingDelta;
            oiChange     = frData.oiChange1h;

            // FR CONFIDENCE ADJUSTMENT — один из немногих бесплатных edge в крипте
            if (fundingRate > 0.0005) { // +0.05%: crowded long
                frConfPenaltyLong  = -Math.min(12, fundingRate / 0.0005 * 5); // до -12 для LONG
                frConfPenaltyShort = Math.min(8, fundingRate / 0.0005 * 3);   // до +8 для SHORT
            } else if (fundingRate < -0.0005) { // -0.05%: crowded short
                frConfPenaltyShort = Math.max(-12, fundingRate / 0.0005 * 5); // до -12 для SHORT
                frConfPenaltyLong  = Math.min(8, Math.abs(fundingRate) / 0.0005 * 3); // до +8 для LONG
            }

            // ── Baseline FR directional signals (existing) ──────────────────
            if (fundingRate < -0.0005) { cDerivatives.addLong(mctx.s(0.45), "FR_NEG"); hasFR = true; }
            if (fundingRate >  0.0010) { cDerivatives.addShort(mctx.s(0.40), "FR_POS"); hasFR = true; }
            if (fundingDelta < -0.0003) cDerivatives.boostLong(mctx.s(0.18), "FR_FALL");
            if (fundingDelta >  0.0003) cDerivatives.boostShort(mctx.s(0.18), "FR_RISE");
            if (oiChange > 3.0 && move5 > 0) cDerivatives.boostLong(mctx.s(0.25), "OI_UP");
            if (oiChange < -3.0 && move5 < 0) cDerivatives.boostShort(mctx.s(0.25), "OI_DN");

            // [MODULE 1 v33] FR MOMENTUM — 2nd derivative signals.
            if (frData.frPeakWarning) {
                cDerivatives.addShort(mctx.s(0.55), "FR_PEAK_WARN");
                hasFR = true;
            }
            if (frData.frTroughWarning) {
                cDerivatives.addLong(mctx.s(0.55), "FR_TROUGH_WARN");
                hasFR = true;
            }

            // ACCELERATION DIVERGENCE
            boolean priceUp   = move5 > 0.003;
            boolean priceDown = move5 < -0.003;
            if (priceUp   && frData.frAcceleration < -0.0002) {
                cDerivatives.boostShort(mctx.s(0.30), "FR_ACCEL_DIV_BEAR");
            }
            if (priceDown && frData.frAcceleration > 0.0002) {
                cDerivatives.boostLong(mctx.s(0.30), "FR_ACCEL_DIV_BULL");
            }
        }

        // RSI Divergences
        double rsi14 = rsi(c15, 14);
        double rsi7  = rsi(c15, 7);
        boolean bullDiv = bullDiv(c15);
        boolean bearDiv = bearDiv(c15);
        // Hidden divergence = continuation signals (high win-rate with trend context).
        // Classic div catches REVERSALS, hidden div catches CONTINUATIONS.
        // Together they cover both ends: top/bottom detection + healthy trend entries.
        boolean hiddenBull = hiddenBullDiv(c15); // price HL + RSI LL → uptrend continues
        boolean hiddenBear = hiddenBearDiv(c15); // price LH + RSI HH → downtrend continues

        if (bullDiv)    cDerivatives.addLong(mctx.s(0.60),  "BULL_DIV");
        if (bearDiv)    cDerivatives.addShort(mctx.s(0.60), "BEAR_DIV");
        // Hidden divergence gets a slightly lower score than classic (it needs HTF context
        // to be reliable — we add it as a supporting signal, not a primary one).
        if (hiddenBull) cDerivatives.addLong(mctx.s(0.48),  "HIDDEN_BULL_DIV");
        if (hiddenBear) cDerivatives.addShort(mctx.s(0.48), "HIDDEN_BEAR_DIV");

        // Дивергенции против позиции — штраф
        if (bearDiv) {
            double vdR = getVolumeDeltaRatio(symbol);
            if (vdR < DIV_VOL_DELTA_GATE) {
                cDerivatives.penalizeLong(DIV_PENALTY_SCORE);
                allFlags.add("BEAR_DIV_PENALTY");
            } else {
                allFlags.add("BEAR_DIV_VOL_OVERRIDE");
            }
        }
        if (bullDiv && !aggressiveShort) {
            double vdR = getVolumeDeltaRatio(symbol);
            if (vdR < DIV_VOL_DELTA_GATE) {
                cDerivatives.penalizeShort(DIV_PENALTY_SCORE);
                allFlags.add("BULL_DIV_PENALTY");
            } else {
                allFlags.add("BULL_DIV_VOL_OVERRIDE");
            }
        }
        // Hidden divergence against direction = softer penalty (0.30 vs 0.55).
        // Hidden div is a continuation signal — it supports existing trends.
        // If we're going SHORT but hidden bull div fires, the trend may still be up.
        if (hiddenBull && !aggressiveShort) {
            cDerivatives.penalizeShort(0.30);
            allFlags.add("HIDDEN_BULL_DIV_S_PENALTY");
        }
        if (hiddenBear) {
            cDerivatives.penalizeLong(0.30);
            allFlags.add("HIDDEN_BEAR_DIV_L_PENALTY");
        }

        // КЛАСТЕР 6: EARLY — РАННИЙ РАЗВОРОТ
        // Ловит момент когда текущий тренд ОСЛАБЕВАЕТ
        // но ещё не сломалась структура на 15m.
        // Использует 1m/5m micro-structure + momentum deceleration
        // + volume divergence + wick rejection + RSI shift.
        // Даёт сигнал на 1-2 свечи РАНЬШЕ чем Structure/Momentum.

        EarlyReversalResult earlyRev = detectEarlyReversal(c1, c5, c15, rsi14, rsi7, price, atr14);
        // Threshold lowered 0.35→0.30: catches reversals 1 candle earlier.
        // Volume confirmation via HTF OVERRIDE gate compensates for lower threshold.
        if (earlyRev.detected && earlyRev.strength > 0.30) {
            double earlyScore = mctx.s(earlyRev.strength * 0.70);
            if (earlyRev.direction > 0) {
                cEarly.addLong(earlyScore, "EARLY_BULL");
            } else {
                cEarly.addShort(earlyScore, "EARLY_BEAR");
            }
            allFlags.addAll(earlyRev.flags);
        }

        // STRONG REVERSAL DETECTOR — fires after N-bar streaks
        // when confluence of RSI/MACD/volume agrees the trend is exhausted.
        // High weight (0.80) because it requires 3+ confluence already.
        // This is the "many candles in one direction = possible reversal" logic.
        double[] rsiArrForRev = com.bot.TradingCore.rsiSeries(c15, 14);
        StrongReversalResult strongRev = detectStrongReversal(c15, rsiArrForRev, rsi14);
        if (strongRev.detected) {
            double srScore = mctx.s(strongRev.strength * 0.80);
            if (strongRev.direction > 0) {
                cEarly.addLong(srScore, "STRONG_REV_UP");
            } else {
                cEarly.addShort(srScore, "STRONG_REV_DN");
            }
            allFlags.addAll(strongRev.flags);
        }

        //  АГРЕГАЦИЯ КЛАСТЕРОВ

        ClusterScores[] clusters = { cStructure, cMomentum, cVolume, cHTF, cDerivatives, cEarly };
        String[] clusterNames = { "STR", "MOM", "VOL", "HTF", "DRV", "EARLY" };

        double totalLong  = 0;
        double totalShort = 0;
        int longClusters  = 0;
        int shortClusters = 0;

        for (int i = 0; i < clusters.length; i++) {
            ClusterScores cl = clusters[i];
            totalLong  += cl.longScore;
            totalShort += cl.shortScore;
            // Apply MIN_CLUSTER_SCORE: a cluster counts as "agreeing" only if its
            // contribution exceeds the threshold. Prevents micro-contributions (0.10-0.15)
            // from padding the cluster count and inflating false signals.
            if (cl.favorsLong()  && cl.longScore  >= MIN_CLUSTER_SCORE) longClusters++;
            if (cl.favorsShort() && cl.shortScore >= MIN_CLUSTER_SCORE) shortClusters++;
            allFlags.addAll(cl.flags);
        }

        // Crash boost добавляется поверх кластеров
        if (crashBoost > 0) {
            totalShort += crashBoost;
            if (btcCrashScore >= 0.65) {
                totalLong -= crashBoost * 0.60;
                allFlags.add("LONG_CRASH_PENALTY");
            }
        }

        // [FIX-ROUND2 2026-05-02] Pump boost — зеркальное применение к totalLong.
        // При сильном BTC pump (rawStrength >= 0.65) тоже срезаем totalShort
        // на 60% от pumpBoost — симметрично crash logic.
        if (pumpBoost > 0) {
            totalLong += pumpBoost;
            if (gicCtx != null && gicCtx.impulseStrength >= 0.65) {
                totalShort -= pumpBoost * 0.60;
                allFlags.add("SHORT_PUMP_PENALTY");
            }
        }

        // GIC SHORT boost при краше
        if (aggressiveShort && gicShortBoost > 1.0 && totalShort > 0) {
            totalShort *= gicShortBoost;
            allFlags.add("GIC_BOOST" + String.format("%.0f", gicShortBoost * 100));
        }

        // GIC GRADIENT LONG SUPPRESSION
        // Applies when BTC is in IMPULSE_DOWN or STRONG_DOWN without hitting hard veto threshold.
        // Proportionally crushes LONG score so signals don't pass unless they have
        // overwhelming confluence (5+ clusters vs normal 2 minimum).
        if (gicLongSuppression < 1.0 && totalLong > 0) {
            totalLong *= gicLongSuppression;
            allFlags.add("GIC_LONG_SUPPRESS" + String.format("%.0f", gicLongSuppression * 100));
        }

        // [FIX-ROUND2 2026-05-02] Зеркальное GIC SHORT SUPPRESSION.
        // Применяется при BTC_IMPULSE_UP / BTC_STRONG_UP — режет totalShort
        // пропорционально силе бычьего импульса, чтобы SHORT-сигналы
        // проходили только при overwhelming confluence (как у LONG в крахе).
        if (gicShortSuppression < 1.0 && totalShort > 0) {
            totalShort *= gicShortSuppression;
            allFlags.add("GIC_SHORT_SUPPRESS" + String.format("%.0f", gicShortSuppression * 100));
        }

        // [Hole 2 FIX] Symmetric GIC LONG boost / SHORT SUPPRESSION during extreme bull runs
        boolean aggressiveLong = gicCtx != null && gicCtx.onlyLong && !aggressiveShort;
        if (aggressiveLong && totalLong > 0) {
            totalLong *= 1.25; // Synthesize a crash boost for pumps
            allFlags.add("GIC_BULL_BOOST125");
            if (totalShort > 0) {
                totalShort *= 0.50; // Hard crush short score during vertical pumps
                allFlags.add("GIC_SHORT_SUPPRESS50");
            }
        }

        // DUAL HTF DIRECTIONAL GATE — with EARLY REVERSAL OVERRIDE
        // When BOTH 1H and 2H agree on direction, the counter-direction
        // candidate needs much higher conviction to override them.
        //
        // OVERRIDE: If EarlyReversal detected + strong volume confirmation
        // (VSA absorption/stopping + CVD persistence), we soften the penalty.
        // This lets the bot catch bottoms/tops BEFORE the HTF EMA flips —
        // which is where the real edge lives (leading, not lagging).
        //
        // Without override: bot waits for 1H+2H to flip → price already moved 3-5%
        // With override: bot enters on micro-structure reversal + volume intent
        com.bot.TradingCore.Side prelimSide = totalLong > totalShort
                ? com.bot.TradingCore.Side.LONG
                : com.bot.TradingCore.Side.SHORT;

        // Detect strong leading (volume-based) reversal signals
        // [v50 §6] EARLY REVERSAL STRENGTH: 0.50→0.38 for earlier trigger.
        // At 0.50 the reversal was already confirming on the chart.
        // At 0.38 we catch it 1-2 bars earlier when micro-structure just shifts.
        boolean strongEarlyReversal = earlyRev.detected && earlyRev.strength > 0.38;
        boolean strongVolumeLong  = cVolume.favorsLong()  && cVolume.longScore > 0.40;
        boolean strongVolumeShort = cVolume.favorsShort() && cVolume.shortScore > 0.40;
        // VSA institutional footprint flags
        boolean vsaAbsorptionBull = allFlags.stream().anyMatch(f ->
                f.startsWith("VSA_STOP_VOL_BULL") || f.startsWith("VSA_ABSORB_BULL")
                        || f.equals("VSA_NO_SUPPLY"));
        boolean vsaAbsorptionBear = allFlags.stream().anyMatch(f ->
                f.startsWith("VSA_STOP_VOL_BEAR") || f.startsWith("VSA_ABSORB_BEAR")
                        || f.equals("VSA_NO_DEMAND"));

        // [v70] DUAL-HTF — штраф смягчён 0.45 → 0.65.
        // HTF EMA на 1H/2H лагает 2-4 часа на разворотах. При нейтральном BTC
        // альты часто разворачиваются первыми, HTF остаётся в прежнем направлении.
        // Старый 0.45 убивал эти ранние reversal signals.
        boolean aggressiveLongHtf = gicRef != null && gicRef.isAggressiveLongMode();
        if (bias1h == HTFBias.BEAR && bias2h == HTFBias.BEAR
                && !aggressiveLongHtf && prelimSide == com.bot.TradingCore.Side.LONG) {
            totalLong *= 0.65;
            allFlags.add("DUAL_HTF_BEAR_PENALTY");
        }
        if (bias1h == HTFBias.BULL && bias2h == HTFBias.BULL
                && !aggressiveShort && prelimSide == com.bot.TradingCore.Side.SHORT) {
            totalShort *= 0.65;
            allFlags.add("DUAL_HTF_BULL_PENALTY");
        }

        // [v70] SINGLE-HTF смягчён 0.75 → 0.85
        if (prelimSide == com.bot.TradingCore.Side.LONG
                && (bias1h == HTFBias.BEAR || bias2h == HTFBias.BEAR)
                && bias1h != HTFBias.BULL && bias2h != HTFBias.BULL) {
            totalLong *= 0.85;
            allFlags.add("SINGLE_HTF_BEAR_PENALTY");
        }
        if (prelimSide == com.bot.TradingCore.Side.SHORT
                && (bias1h == HTFBias.BULL || bias2h == HTFBias.BULL)
                && bias1h != HTFBias.BEAR && bias2h != HTFBias.BEAR) {
            totalShort *= 0.85;
            allFlags.add("SINGLE_HTF_BULL_PENALTY");
        }

        // CONFLUENCE BONUS
        double scoreLong  = totalLong;
        double scoreShort = totalShort;

        // [v42.0 FIX #16] Cap cluster confluence bonus.
        // OLD: linear in cluster count → at 8 clusters = +1.20 score, drowning all penalties.
        // NEW: capped at CLUSTER_TOTAL_CAP = 0.45.
        final double CLUSTER_TOTAL_CAP = 0.45;
        if (longClusters >= 3)  scoreLong  += Math.min(CLUSTER_TOTAL_CAP, CLUSTER_CONFLUENCE_BONUS * longClusters);
        if (shortClusters >= 3) scoreShort += Math.min(CLUSTER_TOTAL_CAP, CLUSTER_CONFLUENCE_BONUS * shortClusters);

        // Confluence flag
        if (longClusters >= 3) allFlags.add("CONFL_L" + longClusters);
        if (shortClusters >= 3) allFlags.add("CONFL_S" + shortClusters);

        double scoreDiff = Math.abs(scoreLong - scoreShort);

        // [v70] EXHAUSTION → REVERSAL (not hard veto)
        //
        // Старое: при движении >4.5×ATR за 15 баров hard reject обеих сторон.
        // Проблема: это КЛАССИЧЕСКИЙ разворотный setup. Мы блокировали именно
        // те сигналы которые трейдеры называют "overextended reversal".
        //
        // Новое:
        //   • Если candidate = сторона движения (trend continuation): hard reject
        //     (экстремум истощён, вход против последующего разворота)
        //   • Если candidate = противоположная сторона: это РАЗВОРОТ, boost score
        //     и добавляем reversal-кластер. Signal passes если confluence есть.
        //   • Нет candidate из клатеров, но extension огромный → фрорсим reversal
        //     сторону как кандидат (чистый разворот от истощения).
        {
            int lb = Math.min(15, c15.size() - 1);
            double rHigh = Double.NEGATIVE_INFINITY, rLow = Double.MAX_VALUE;
            for (int i = c15.size() - 1 - lb; i < c15.size() - 1; i++) {
                rHigh = Math.max(rHigh, c15.get(i).high);
                rLow  = Math.min(rLow,  c15.get(i).low);
            }
            double extUp   = price - rLow;
            double extDown = rHigh - price;
            double extDir  = Math.max(extUp, extDown);
            int extSign    = extUp > extDown ? +1 : -1; // +1 = price moved up, -1 = moved down

            if (extDir > atr14 * 7.5) {
                // [FIX-9PCT 2026-05-02] Threshold 5.5 → 7.5: в нормальной альт-
                // волатильности 5.5×ATR за 15 баров = типичный памп-ретест
                // (НЕ parabolic). Раньше бот шортил каждый рост альты
                // >5.5×ATR — это и был главный источник 80/20 SHORT-перекоса.
                // 7.5×ATR за 4 часа — реально parabolic move, заслуживающий
                // reversal-внимания.
                //
                // [FIX-9PCT 2026-05-02] Reversal boost 0.55 → 0.25: даём
                // только маленький bias, не делаем reversal-сторону
                // доминирующей. Кластеры должны согласиться независимо.
                boolean candLong = scoreLong > scoreShort;
                if (extSign > 0) {
                    // Price pumped → reversal = SHORT
                    if (candLong) {
                        // Long candidate on pump extension = chase, veto
                        allFlags.add("EXHAUST_LONG_VETO_" + String.format("%.1fx", extDir / atr14));
                        return reject("exhaust_hard_veto");
                    } else {
                        scoreShort += 0.25;
                        allFlags.add("EXHAUST_REV_SHORT_" + String.format("%.1fx", extDir / atr14));
                    }
                } else {
                    // Price dumped → reversal = LONG
                    if (!candLong && scoreShort > 0) {
                        allFlags.add("EXHAUST_SHORT_VETO_" + String.format("%.1fx", extDir / atr14));
                        return reject("exhaust_hard_veto");
                    } else {
                        scoreLong += 0.25;
                        allFlags.add("EXHAUST_REV_LONG_" + String.format("%.1fx", extDir / atr14));
                    }
                }
                scoreDiff = Math.abs(scoreLong - scoreShort);
            } else if (extDir > atr14 * 3.0) {
                // Moderate extension: penalty for trend-continuation side only
                boolean candLong = scoreLong > scoreShort;
                if ((extSign > 0 && candLong) || (extSign < 0 && !candLong)) {
                    if (candLong) scoreLong *= 0.55; else scoreShort *= 0.55;
                    allFlags.add("EXHAUST_PENALTY_" + String.format("%.1fx", extDir / atr14));
                    scoreDiff = Math.abs(scoreLong - scoreShort);
                }
            }
        }

        // REVERSE EXHAUSTION CHECK
        ReverseWarning rw = detectReversePattern(c15, c1h, state);
        if (rw != null && rw.confidence > 0.48) {
            allFlags.add("⚠REV_" + rw.type);
            if ("LONG_EXHAUSTION".equals(rw.type)) {
                boolean confirmed = confirmReversalStructure(c1, c5, com.bot.TradingCore.Side.SHORT);
                if (!confirmed) {
                    scoreLong *= 0.35;
                    // Flag for penalty, not veto
                    if (scoreLong < 0.20) allFlags.add("LEXH_SCORE_CRUSHED");
                } else {
                    allFlags.add("LEXH_CONFIRMED_1M");
                }
            } else if ("SHORT_EXHAUSTION".equals(rw.type)) {
                if (aggressiveShort) {
                    allFlags.add("REV_IGNORED_CRASH");
                } else {
                    boolean confirmed = confirmReversalStructure(c1, c5, com.bot.TradingCore.Side.LONG);
                    if (!confirmed) {
                        scoreShort *= 0.35;
                        if (scoreShort < 0.20) allFlags.add("SEXH_SCORE_CRUSHED");
                    } else {
                        allFlags.add("REV_CONFIRMED_1M");
                    }
                }
            }
        }

        // EXHAUSTION FILTERS
        // Removed overlapping and arbitrary EXHAUSTION, OVEREXTENDED, and 2H VETO
        // multipliers (e.g. scoreLong *= 0.45, etc.) that effectively forced the bot to
        // "fade" out of perfectly valid setups. ForecastEngine now governs macro directional confidence.

        // МИНИМУМ КЛАСТЕРОВ — АДАПТИВНЫЙ
        // Обычно: 2 кластера
        // Если EARLY сильный (> 0.65): достаточно 1 кластер
        // Это позволяет ловить развороты ДО подтверждения структуры
        com.bot.TradingCore.Side candidateSide = scoreLong > scoreShort
                ? com.bot.TradingCore.Side.LONG
                : com.bot.TradingCore.Side.SHORT;

        // HARD VETO: Если очень мощный разворотный сигнал идет ПРОТИВ тренда,
        // который набрал баллы на отстающих индикаторах (как 1H EMA), убиваем трендовый сигнал.
        if (earlyRev.detected && earlyRev.strength > 0.30) {
            com.bot.TradingCore.Side earlySide = earlyRev.direction > 0 ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT;

            boolean rsiSupportsEarly = (earlySide == com.bot.TradingCore.Side.LONG && rsi14 < 45) ||
                    (earlySide == com.bot.TradingCore.Side.SHORT && rsi14 > 55);
            boolean antiLagSupportsEarly = (earlySide == com.bot.TradingCore.Side.LONG && allFlags.contains("ANTI_LAG_UP")) ||
                    (earlySide == com.bot.TradingCore.Side.SHORT && allFlags.contains("ANTI_LAG_DN"));
            boolean rsiShiftSupportsEarly = (earlySide == com.bot.TradingCore.Side.LONG && allFlags.contains("RSI_SHIFT_UP")) ||
                    (earlySide == com.bot.TradingCore.Side.SHORT && allFlags.contains("RSI_SHIFT_DN"));
            boolean strongOverride = rsiSupportsEarly || antiLagSupportsEarly || rsiShiftSupportsEarly || earlyRev.strength > 0.55;

            if (earlySide != candidateSide) {
                // Прямой конфликт: сильный ранний разворот ПРОТИВ кандидата
                if (strongOverride) {
                    // [v24.0 FIX WEAK-4] Cap at 0.30 (was 0.15 = 85% crush, destroyed valid trends).
                    // One wick rejection + deceleration was killing 5-cluster trend setups.
                    if (candidateSide == com.bot.TradingCore.Side.LONG) scoreLong *= 0.30;
                    else scoreShort *= 0.30;
                    allFlags.add("EARLY_VETO_OPPOSITE");
                    // Пересчитаем кандидата после штрафа
                    candidateSide = scoreLong > scoreShort ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT;
                }
            } else {
                // Прямое совпадение: ранний разворот ВЕДЕТ кандидата.
                // Отстающие индикаторы на противоположной стороне просто "съедают" scoreDiff, мешая поймать разворот мгновенно.
                // Штрафуем противоположную сторону, чтобы пробить мин. scoreDiff барьер!
                if (strongOverride && earlyRev.strength > 0.45) {
                    if (candidateSide == com.bot.TradingCore.Side.LONG) {
                        scoreShort *= 0.30;
                        allFlags.add("EARLY_CLEAR_S");
                    } else {
                        scoreLong *= 0.30;
                        allFlags.add("EARLY_CLEAR_L");
                    }
                }
            }
        }

        int supportingClusters = candidateSide == com.bot.TradingCore.Side.LONG
                ? longClusters : shortClusters;

        // EARLY-SOLO: сильный ранний сигнал может пройти с 1 кластером
        boolean earlyStrong = earlyRev.detected && earlyRev.strength > 0.65;
        boolean earlyLongLead = cEarly.favorsLong() && (
                cVolume.favorsLong()
                        || cDerivatives.favorsLong()
                        || (cMomentum.favorsLong() && (
                        allFlags.contains("ANTI_LAG_UP")
                                || allFlags.contains("COMP_BREAK_UP")
                                || allFlags.contains("PUMP_HUNT_B")
                                || allFlags.stream().anyMatch(f -> f.startsWith("VDA+"))))
                        || (cStructure.favorsLong() && allFlags.contains("BOS_UP_5M"))
        );
        boolean earlyShortLead = cEarly.favorsShort() && (
                cVolume.favorsShort()
                        || cDerivatives.favorsShort()
                        || (cMomentum.favorsShort() && (
                        allFlags.contains("ANTI_LAG_DN")
                                || allFlags.contains("COMP_BREAK_DN")
                                || allFlags.contains("PUMP_HUNT_S")
                                || allFlags.stream().anyMatch(f -> f.startsWith("VDA-"))))
                        || (cStructure.favorsShort() && allFlags.contains("BOS_DN_5M"))
        );
        boolean earlySoloAllowed = earlyStrong
                && ((candidateSide == com.bot.TradingCore.Side.LONG && earlyLongLead)
                || (candidateSide == com.bot.TradingCore.Side.SHORT && earlyShortLead));

        // [FIX SIGNAL ACCURACY] EARLY_SOLO was 1 cluster. Changed to 2.
        // Root cause of "4/4 wrong" signals:
        //   earlyLongLead can be true when Momentum score=0.15 (below MIN_CLUSTER_SCORE=0.30).
        //   With requiredClusters=1 and longClusters=1 (only Early), the signal passes
        //   even when Structure/HTF/Volume/Derivatives ALL disagree.
        // Fix: EARLY_SOLO still bypasses the normal MIN_AGREEING_CLUSTERS=3 requirement,
        //   but we demand at least 2 clusters scoring >= MIN_CLUSTER_SCORE.
        //   This means Early must be strong AND one other cluster truly confirms (Volume/Momentum/Derivatives/Structure).
        //   Effect: eliminates solo-Early misfires while keeping legitimate early pump detection.
        int requiredClusters = earlySoloAllowed ? 2 : MIN_AGREEING_CLUSTERS;

        // [FIX-9PCT 2026-05-02] RANGE = всегда 3 кластера, никаких обходов.
        // Старая логика (decisiveSplit ≥ 0.32 / reversalContext) пропускала
        // 2-кластерные шумовые сетапы. Это главная дыра 9% WR: большинство
        // сигналов в логе имели "Кластеров: 2" или "Кластеров: 3 со слабым
        // кластером" — они ВСЕ проходили именно через эту лазейку.
        //
        // Новое: если в RANGE нет 3 действительно согласных кластеров —
        // edge не доказан, молчим. EARLY_SOLO (2 кластера + сильный Early)
        // тоже больше не работает в RANGE.
        if (state == MarketState.RANGE && !aggressiveShort) {
            requiredClusters = 3;
        }

        // Insufficient clusters → penalty flag (was return null)
        boolean clusterPenalty = false;
        if (supportingClusters < requiredClusters) {
            if (!(aggressiveShort && candidateSide == com.bot.TradingCore.Side.SHORT && crashBoost > 0.30)) {
                clusterPenalty = true;
                allFlags.add("LOW_CLUSTERS_" + supportingClusters + "/" + requiredClusters);
            }
        }

        if (earlySoloAllowed) allFlags.add("EARLY_SOLO");

        // MINIMUM SCORE DIFFERENCE
        scoreDiff = Math.abs(scoreLong - scoreShort);
        double minDiff;
        if (aggressiveShort) {
            minDiff = 0.08;
        } else {
            minDiff = state == MarketState.STRONG_TREND ? 0.16
                    : state == MarketState.RANGE ? 0.28
                      : 0.20;
        }
        // scoreDiff/dynThresh → penalty flags (was return null)
        boolean scoreDiffPenalty = scoreDiff < minDiff;
        boolean dynThreshPenalty = false;
        double dynThresh;
        if (aggressiveShort) {
            dynThresh = 0.40;
        } else {
            dynThresh = state == MarketState.STRONG_TREND ? 0.68 : 0.58;
        }
        if (scoreLong < dynThresh && scoreShort < dynThresh) {
            if (!bullDiv && !bearDiv && !hasFR && !aggressiveShort) {
                dynThreshPenalty = true;
                allFlags.add("DYN_THRESH_LOW");
            }
        }

        com.bot.TradingCore.Side side = candidateSide;

        // CHAIN-PAUSE GATE — block re-entry if pair has 2+ consecutive
        // TIME_STOPs (4h side pause) or 3+ on either side (8h symbol pause).
        // Catches the ENJUSDT-style chain on dead structure.
        if (iscRef != null && iscRef.isPausedByChain(symbol, side)) {
            return reject("ts_chain_pause");
        }

        // [v70] LOCAL EXHAUSTION — разрешаем reversal-сторону при strong confluence.
        //
        // Прежде: hard reject counter-trend entries после локального pump/dump.
        // Проблема: мы блокировали именно те сигналы которые хотим — развороты после
        // локального истощения. Clusters уже проголосовали за reversal сторону,
        // значит confirmation уже есть (volume/structure/momentum).
        //
        // [FIX-9PCT 2026-05-02] Threshold 0.35 → 0.50: реверсивная сторона должна
        // иметь СЕРЬЁЗНОЕ confluence чтобы её рассматривать. На 0.35 проходило
        // слишком много шумовых reversal-сетапов (второй источник SHORT-перекоса
        // после EXHAUST_REV).
        //
        // [FIX-9PCT 2026-05-02] Boost 0.30 → 0.10: это контекстный бонус,
        // не доминирующий фактор. Кластеры должны согласиться сами.
        if (localExh != null && !aggressiveShort) {
            boolean reversalSide = (localExh.direction == -1 && side == com.bot.TradingCore.Side.LONG)
                    || (localExh.direction == +1 && side == com.bot.TradingCore.Side.SHORT);
            if (reversalSide) {
                double reversalScoreDiff = Math.abs(scoreLong - scoreShort);
                if (reversalScoreDiff < 0.50) {
                    // Weak confluence — reversal call без подтверждения = нож в падение
                    allFlags.add("LOCAL_EXHAUST_UNCONFIRMED_"
                            + String.format("%.1fATR", localExh.moveAtr));
                    return reject("local_exhaust_unconfirmed");
                }
                // Strong confluence — reversal trade разрешён с small bonus
                allFlags.add("LOCAL_REVERSAL_" + (localExh.direction > 0 ? "TOP_" : "BOT_")
                        + String.format("%.1fATR", localExh.moveAtr));
                if (side == com.bot.TradingCore.Side.LONG) scoreLong += 0.10;
                else scoreShort += 0.10;
            }
        }

        // [v50 §2] LATE ENTRY: REAL penalty — no more excuses.
        boolean lateEntryPenalty = false;
        if (side == com.bot.TradingCore.Side.LONG && lateEntryLong && !aggressiveShort) {
            lateEntryPenalty = true;
            allFlags.add("LATE_ENTRY_L");
        }
        if (side == com.bot.TradingCore.Side.SHORT && lateEntryShort && !aggressiveShort) {
            lateEntryPenalty = true;
            allFlags.add("LATE_ENTRY_S");
        }

        // [v77 LATENCY — KEY FIX]
        // Old: hard reject on lateMove.severity == 2 same-direction.
        // Result: SHORT signals during dumps were rejected for the duration
        // of the impulse — bot only fired SHORT after the dump exhausted,
        // i.e. at the local bottom (SOON-style). Same for LONG on pumps.
        //
        // New: lateMove.severity == 2 + same-direction → tag as LATE_HARD
        // and let the signal pass. Position size will be cut downstream
        // (lateEntryPenalty path) and SL is already widened by ATR. The
        // calibrator will price the late-entry win-rate over time.
        // LM_HARD_ATR was raised 2.5→4.5 so severity==2 now requires a
        // genuinely parabolic move — those are rare; when they happen the
        // soft-cut + R:R 1:2 still gives a positive expectancy.
        if (lateMove.severity == 2) {
            if ((lateMove.dirUp && side == com.bot.TradingCore.Side.LONG)
                    || (!lateMove.dirUp && side == com.bot.TradingCore.Side.SHORT)) {
                allFlags.add(String.format("LATE_HARD_PENALTY_atr=%.2f_streak=%d",
                        lateMove.maxAtrMul, lateMove.streakBars));
                lateEntryPenalty = true; // size cut + score penalty downstream
                if (side == com.bot.TradingCore.Side.LONG) scoreLong  *= 0.85;
                else                                       scoreShort *= 0.85;
            }
        }

        // [v77 LATENCY] momentum_exhausted same-direction: reject → soft penalty.
        // The detector fires on shrinking bodies + growing wicks + declining
        // volume in the LAST 3 bars. That can mean exhaustion (bot's old
        // assumption: don't add to a dying move) but on 15m TF it ALSO means
        // "consolidation before continuation" — base of the next leg. Hard-
        // rejecting same-direction blocked classic flag/pennant breakouts.
        // Soft penalty: -10pt prob and 0.80× score, still allow opposite-side
        // boost to fire when the reversal is real.
        if (momentumExhausted) {
            if ((exhaustionDirection > 0 && side == com.bot.TradingCore.Side.LONG)
                    || (exhaustionDirection < 0 && side == com.bot.TradingCore.Side.SHORT)) {
                allFlags.add("EXHAUSTION_SOFT_PENALTY");
                if (side == com.bot.TradingCore.Side.LONG) scoreLong  *= 0.80;
                else                                       scoreShort *= 0.80;
            }
            if ((exhaustionDirection > 0 && side == com.bot.TradingCore.Side.SHORT)
                    || (exhaustionDirection < 0 && side == com.bot.TradingCore.Side.LONG)) {
                if (side == com.bot.TradingCore.Side.LONG) {
                    cEarly.addLong(mctx.s(1.0), "EXHAUST_REV_L");
                } else {
                    cEarly.addShort(mctx.s(1.0), "EXHAUST_REV_S");
                }
                allFlags.add("REVERSAL_SETUP");
                allFlags.add("EXHAUSTION_REVERSAL_BOOST");
            }
        }

        // [v77 LATENCY] vel_decay + lateEntryPenalty: reject → penalty.
        // Combined trigger of both conditions still counts as a real
        // headwind, but with widened LM_HARD_ATR (4.5) lateEntryPenalty
        // also fires on weaker conditions; rejecting outright would lose
        // the very entries we want to catch. Aggressive size cut + score
        // penalty handles the residual risk; calibrator filters the rest.
        if (velocityDecay && lateEntryPenalty) {
            allFlags.add("VEL_DECAY_LATE_PENALTY");
            if (side == com.bot.TradingCore.Side.LONG) scoreLong  *= 0.75;
            else                                       scoreShort *= 0.75;
        }

        // [v77] After applying LATE_HARD / EXHAUSTION / VEL_DECAY soft penalties,
        // refresh scoreDiff so downstream gates (volume, htf, cluster checks)
        // see the actual post-penalty values.
        scoreDiff = Math.abs(scoreLong - scoreShort);

        // [v50 §1] leadBreakoutOverride REMOVED.
        // Old logic: "if BOS/AntiLag/VDA flag present → cancel late penalty."
        // This was circular — those flags fire BECAUSE the move already happened.
        // Late entry penalty now stands regardless of other signals.

        // VOLUME CONFIRMATION GATE
        boolean volumeSupports = (side == com.bot.TradingCore.Side.LONG && cVolume.favorsLong())
                || (side == com.bot.TradingCore.Side.SHORT && cVolume.favorsShort());
        boolean volumeOpposes = (side == com.bot.TradingCore.Side.LONG && cVolume.favorsShort())
                || (side == com.bot.TradingCore.Side.SHORT && cVolume.favorsLong());

        // [v14.0 FIX] Volume = SOFT GATE
        if (volumeOpposes && !aggressiveShort) {
            if (candidateSide == com.bot.TradingCore.Side.LONG) scoreLong *= 0.55;
            else scoreShort *= 0.55;
            allFlags.add("VOL_OPPOSE");
            scoreDiff = Math.abs(scoreLong - scoreShort);
        }
        // Нет volume confirmation — мягкий штраф 15%
        if (!volumeSupports && !volumeOpposes && !aggressiveShort) {
            if (candidateSide == com.bot.TradingCore.Side.LONG) scoreLong *= 0.85;
            else scoreShort *= 0.85;
            allFlags.add("VOL_NEUTRAL");
        }

        // Cooldown
        long shortCooldownOverride = aggressiveShort ? 60_000L : -1;
        if (!cooldownAllowedEx(symbol, side, cat, now, shortCooldownOverride)) return reject("cooldown_block");
        if (!flipAllowed(symbol, side)) return reject("flip_block");

        // Post-exit directional cooldown: prevents re-entry in same direction
        // for 10 minutes after any position close. Eliminates spin-trading on same pair.
        if (isPostExitBlocked(symbol, side)) return reject("post_exit_block");

        // Panic remains a hard veto for LONG.
        // Outside panic, SignalSender reapplies nuanced GIC weights after all downstream
        // probability adjustments. Returning null here was double-counting the same filter
        // and zeroing out counter-trend reversals before RS/sector overrides could act.
        if (gicCtx != null
                && (gicCtx.panicMode
                || gicCtx.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_PANIC)
                && side == com.bot.TradingCore.Side.LONG) {
            allFlags.add("GIC_PANIC_VETO_LONG");
            return reject("gic_panic_long");
        }
        // [v78.3 ROLLBACK] COUNTER_TREND pockets удалены.
        //
        // ПОЧЕМУ: попытка ловить «сильный oversold reversal» в bear-режиме
        // на условиях (3+ clusters + earlyRev > 0.50 + RSI < 28) выглядит
        // строгой на бумаге, но в реальном крахе RSI<28 + earlyRev сигнал
        // вспыхивают 5-10 раз за день — каждый «отскок» выглядит как разворот,
        // продолжается падение. Это classic falling-knife trap.
        //
        // GIC.onlyShort/onlyLong были спроектированы как HARD VETO именно
        // против этого паттерна. Восстанавливаем оригинал.
        //
        // Если действительно нужны редкие counter-trend setups —
        // используется ELITE_REVERSAL_POCKET ниже (см. строку ~2780+).

        // BTC in extreme up-impulse: short on an alt = very high failure rate
        if (gicOnlyLong && side == com.bot.TradingCore.Side.SHORT) {
            allFlags.add("GIC_STRONG_BTCUP");
            return reject("gic_btcup_short_veto");
        }
        // BTC crashing: long on an alt = catching a falling knife
        if (gicOnlyShort && side == com.bot.TradingCore.Side.LONG) {
            // [v78.3] ELITE_REVERSAL_POCKET — единственная узкая дверь для LONG
            // в bear-режиме. Условия НАМЕРЕННО очень строгие — фильтр должен
            // пропускать ~1 раз в крупный крах, не каждые 30 минут.
            //
            // Все 5 условий обязательны (AND, не OR):
            //   1. 4+ LONG-кластера (полная конфлюэнция структуры/моментума/объёма/HTF)
            //   2. earlyRev.strength > 0.65 (сильный early-reversal сигнал)
            //   3. localExh.direction == -1 (подтверждённое локальное истощение шорта)
            //   4. rsi14 < 22 (extreme oversold, не просто "перепродан")
            //   5. relStrength > 0.85 (sector leader — монета держится при крахе BTC)
            //
            // Если хоть одно условие нарушено → стандартный hard veto.
            // Score дисконтируется ×0.75 (больше чем 0.85 в старой версии)
            // плюс сигнал помечается ELITE_REV для трейдера.
            double rsi14ForRev = rsi(c15, 14);
            double relStr = getRelativeStrength(symbol);
            boolean elite = longClusters >= 4
                    && earlyRev.detected
                    && earlyRev.strength > 0.65
                    && localExh != null && localExh.direction == -1
                    && rsi14ForRev < 22.0
                    && relStr > 0.85;

            if (!elite) {
                allFlags.add("GIC_STRONG_BTCDOWN");
                return reject("gic_btcdown_long_veto");
            }
            allFlags.add("ELITE_REV_LONG");
            scoreLong *= 0.75;
        }

        // POST-PUMP LONG VETO + POST-DUMP SHORT VETO
        // Финальные veto после определения candidateSide/side.
        // LONG: gain≥35%, drop≥8% от хая → блокируем LONG (кейс SOONUSDT).
        // SHORT [FIX-SYM 2026-05-02]: gain≥35% range, bounce≥8% от лоу → блокируем
        //                              SHORT (классический капитуляционный отскок).
        //
        // [v69 FIX] post-*-skip в отдельных map'ах (cooldownMap использует
        // key = sym+"_"+side и другую semantics — нельзя переиспользовать).
        // Проверки postPumpSkipUntil/postDumpSkipUntil вынесены в начало analyze()
        // для раннего выхода без полного анализа. 30 минут вполне достаточно чтобы
        // 20-барное окно сдвинулось и условие перестало срабатывать естественно.
        if (postPumpDump && side == com.bot.TradingCore.Side.LONG) {
            LOG.info("[POST-PUMP VETO] " + symbol + " LONG blocked: gain="
                    + String.format("%.0f%%", postPumpGain * 100)
                    + " dropFromHi=" + String.format("%.0f%%", postPumpDropFromHi * 100));
            postPumpSkipUntil.put(symbol, System.currentTimeMillis() + 30 * 60_000L);
            return reject("post_pump_long");
        }
        if (postDumpBounce && side == com.bot.TradingCore.Side.SHORT) {
            LOG.info("[POST-DUMP VETO] " + symbol + " SHORT blocked: rangeGain="
                    + String.format("%.0f%%", postPumpGain * 100)
                    + " bounceFromLo=" + String.format("%.0f%%", postDumpBounceFromLo * 100));
            postDumpSkipUntil.put(symbol, System.currentTimeMillis() + 30 * 60_000L);
            return reject("post_dump_short");
        }

        // КАЛИБРОВАННАЯ УВЕРЕННОСТЬ — на кластерах
        double probability = computeClusterConfidence(
                symbol, scoreLong, scoreShort, scoreDiff,
                longClusters, shortClusters,
                state, cat, atr14, price,
                bullDiv, bearDiv, pullUp, pullDown,
                impulseFlag, false, hasFR,
                fvg.detected, ob.detected, false, liqSweep,
                bias2h, vwapVal
        );

        // [FIX-ROUND2 2026-05-02] Симметричный confidence boost.
        // Ранее: aggressiveShort+SHORT получал +до 10pt (btcCrashScore × 10),
        //        aggressiveLong+LONG получал cap +4pt — асимметрия 2.5×.
        // Теперь: оба бустера зеркальные. SHORT получает btcCrashScore × 10,
        //         LONG получает impulseStrength × 10 (тот же масштаб).
        if (aggressiveShort && side == com.bot.TradingCore.Side.SHORT) {
            double crashConfBoost = btcCrashScore * 10.0;
            probability = Math.min(PROB_CEIL, probability + crashConfBoost);
            allFlags.add("CRASH_CONF_BOOST");
        }
        if (aggressiveLong && side == com.bot.TradingCore.Side.LONG) {
            // Зеркально crashConfBoost: масштаб × 10, без жёсткого cap (только PROB_CEIL).
            double bullConfBoost = (gicCtx != null ? gicCtx.impulseStrength : 0.5) * 10.0;
            probability = Math.min(PROB_CEIL, probability + bullConfBoost);
            allFlags.add("BULL_CONF_BOOST");
        }

        // WEIGHTED ENSEMBLE — replaces additive penalty hell.
        // Each factor VOTES with a weight [-1..+1].
        // Total adjustment is capped at [-14, +8] — no single factor
        // can kill a signal with 5/6 agreeing clusters.
        //
        // RATIONALE: Old code applied flat -8, -10, -12, -15 subtractions
        // independently. A valid 4-cluster TREND setup with a slightly late
        // entry could lose -41 points and die at 50. That was a bug, not logic.

        // Factor 1: Structure/cluster agreement  [weight 35%]
        double activeScore = (side == com.bot.TradingCore.Side.LONG) ? scoreLong : scoreShort;
        double structVote  = Math.min(1.0, activeScore / 2.5);               // 0..+1
        double ensAdj      = structVote * 0.35 * 14.0;

        // Factor 2: Volume alignment             [weight 25%]
        double volVote = volumeOpposes ? -0.8 : (volumeSupports ? 0.6 : 0.0);
        ensAdj += volVote * 0.25 * 14.0;

        // Factor 3: ADX context                  [weight 15%]
        double adxVal25 = adx(c15, 14);
        double adxVote  = adxRangePenalty ? -0.5 : (adxVal25 > 25 ? 0.7 : 0.2);
        ensAdj += adxVote * 0.15 * 14.0;

        // Factor 4: Entry timing                 [weight 30%] ← was 15%
        // [v50 §2] LATE = strong penalty. Weight tripled, vote doubled.
        // A late entry into a completed move should DROP probability by 7-8 points,
        // not 0.7. This is the difference between "maybe trade" and "definitely skip".
        double lateVote = lateEntryPenalty ? -0.85 : 0.0;
        ensAdj += lateVote * 0.30 * 14.0;

        // Factor 5: Cluster count adequacy       [weight 20%] ← raised from 10%
        // At 10% a 1/3 cluster penalty was only -0.91 pts — barely felt.
        // At 20% it becomes -1.82 pts — enough to drop a 67% signal below 65% floor.
        int activeClCount = (side == com.bot.TradingCore.Side.LONG) ? longClusters : shortClusters;
        double clVote = clusterPenalty ? -0.65 : (activeClCount >= 4 ? 0.55 : 0.0);
        ensAdj += clVote * 0.20 * 14.0;

        // Cap: ensemble cannot exceed these bounds.
        // scoreDiffPenalty and dynThreshPenalty are represented
        // by their contributing factors above — no extra flat deduction.
        ensAdj = Math.max(-14.0, Math.min(+8.0, ensAdj));
        probability = Math.max(0.0, Math.min(PROB_CEIL, probability + ensAdj));

        // [v75] FLAT_MARKET / CHOPPY_RANGE soft penalties (replaced hard rejects upstream).
        // Эти штрафы НЕ кумулятивные с ensAdj — они отдельный штраф за regime quality.
        // FLAT_MARKET_COMPRESSION:  -4 (мягко — может быть pre-pump, не убиваем)
        // CI_HIGH (chop без extreme RSI): -6 (выше чем flat, но всё ещё не reject)
        if (flatMarketPenalty) {
            probability = Math.max(0, probability - 4);
        }
        if (choppyRangePenalty) {
            probability = Math.max(0, probability - 6);
        }

        // Exhaustion score crush: score already penalized upstream via cluster multiply.
        // Apply only a capped -7 here (was -12 flat).
        if (allFlags.contains("LEXH_SCORE_CRUSHED") || allFlags.contains("SEXH_SCORE_CRUSHED"))
            probability = Math.max(0, probability - 7);

        // LATE_ENTRY → carry flag for position-size reduction in SignalSender.
        // Probability already mildly dipped above. No further deduction needed.
        if (lateEntryPenalty) allFlags.add("LATE_ENTRY_SIZE_CUT");

        // [FIX SIGNAL ACCURACY] HTF ALIGNMENT GATE — extra penalty when 1h/2h HTF opposes signal.
        // The most common cause of wrong signals: 15m says SHORT, 1h says BULL.
        // With 6 equal-weight clusters, HTF disagreement was only -4 to -6 pts.
        // After ensemble: signal at 71% passes minimum. Then it's wrong.
        // New: if HTF actively opposes (not just neutral) AND we're in non-crash neutral market:
        //   LONG with BEAR HTF → -5 additional penalty
        //   SHORT with BULL HTF → -5 additional penalty
        // This is NOT applied when: aggressiveShort (crash), EXHAUSTION_REVERSAL (HTF lags reversals),
        //   or when htfBias == NONE (HTF is neutral — ambiguous is OK).
        if (!aggressiveShort && !allFlags.contains("EXHAUSTION_REVERSAL_BOOST")) {
            boolean htfOpposes = (side == com.bot.TradingCore.Side.LONG && bias2h == HTFBias.BEAR)
                    || (side == com.bot.TradingCore.Side.SHORT && bias2h == HTFBias.BULL);
            if (htfOpposes) {
                probability = Math.max(0, probability - 5.0);
                allFlags.add("HTF_OPPOSE-5");
            }
        }

        // FUNDING RATE CONFIDENCE ADJUSTMENT — applied after all cluster logic
        boolean _isLong = (side == com.bot.TradingCore.Side.LONG);
        double frAdj = _isLong ? frConfPenaltyLong : frConfPenaltyShort;
        if (Math.abs(frAdj) > 0.5) {
            probability = Math.max(0, Math.min(PROB_CEIL, probability + frAdj));
            if (frAdj < -3) allFlags.add("FR_CROWD_PENALTY");
            if (frAdj > 3)  allFlags.add("FR_EDGE_BOOST");
        }

        // [v75] ADVANCED CONFLUENCE (SMC + AVWAP + SuperTrend + CVD + PreMove)
        // ─────────────────────────────────────────────────────────────────────
        // Bolts on as a 7th cluster equivalent. Reads everything in TradingCore's
        // new advanced-indicators block and produces a single bull/bear score.
        // Mapping to probability:
        //   - aligned with our side AND diff>=0.20  → +6 (strong agreement)
        //   - aligned with our side AND diff>=0.10  → +3 (mild agreement)
        //   - opposed to our side AND diff>=0.20    → -6 (strong opposition)
        //   - opposed to our side AND diff>=0.10    → -3 (mild opposition)
        // Capped contribution: ±6 — cannot single-handedly flip a signal.
        try {
            com.bot.TradingCore.ConfluenceReport adv = com.bot.TradingCore.advancedConfluence(c15, price);
            double advAdj = 0;
            double diff = adv.diff();   // bull - bear
            if (_isLong) {
                if (diff >= 0.20)       advAdj = +6;
                else if (diff >= 0.10)  advAdj = +3;
                else if (diff <= -0.20) advAdj = -6;
                else if (diff <= -0.10) advAdj = -3;
            } else {
                if (diff <= -0.20)      advAdj = +6;
                else if (diff <= -0.10) advAdj = +3;
                else if (diff >= 0.20)  advAdj = -6;
                else if (diff >= 0.10)  advAdj = -3;
            }
            if (advAdj != 0) {
                probability = Math.max(0, Math.min(PROB_CEIL, probability + advAdj));
                allFlags.add(String.format("ADV_CONFL_%s%.0f",
                        advAdj > 0 ? "+" : "", advAdj));
                // Tag the strongest factor in flags for downstream visibility
                List<String> winningSide = (_isLong && diff > 0) || (!_isLong && diff < 0)
                        ? (_isLong ? adv.bullFactors : adv.bearFactors)
                        : (_isLong ? adv.bearFactors : adv.bullFactors);
                if (winningSide != null && !winningSide.isEmpty()) {
                    // Add up to 3 factors to flags (small footprint in logs)
                    for (int k = 0; k < Math.min(3, winningSide.size()); k++) {
                        allFlags.add(winningSide.get(k));
                    }
                }
            }

            // [v75] PRE-MOVE PATTERN — separate predictive boost.
            // If detectPreMove finds a high-confidence pattern aligned with our side,
            // add up to +5. If aligned against our side with high confidence, -4.
            com.bot.TradingCore.PreMoveSignal pm = com.bot.TradingCore.detectPreMove(c15);
            if (pm.detected() && pm.confidence > 0.40) {
                double pmAdj = 0;
                if ((pm.bullish && _isLong) || (!pm.bullish && !_isLong)) {
                    pmAdj = +pm.confidence * 5.0;
                } else {
                    pmAdj = -pm.confidence * 4.0;
                }
                probability = Math.max(0, Math.min(PROB_CEIL, probability + pmAdj));
                allFlags.add(String.format("PREMOVE_%s_%.2f", pm.type.name(), pm.confidence));
            }
        } catch (Throwable t) {
            // never let advanced confluence block the pipeline — log & continue
            allFlags.add("ADV_CONFL_ERR");
        }

        double minConf = symbolMinConf.getOrDefault(symbol, globalMinConf.get());
        // [FIX-ROUND2 2026-05-02] Симметричное ослабление minConf для агрессивных режимов.
        // Ранее: aggressiveShort+SHORT получал -8pt (floor 45),
        //        aggressiveLong+LONG получал -4pt (floor 0) — асимметрия 2× в пользу SHORT.
        // Теперь: оба режима получают -6pt (компромисс), floor 50 для обоих.
        if (aggressiveShort && side == com.bot.TradingCore.Side.SHORT) {
            minConf = Math.max(50.0, minConf - 6.0);
        }
        if (aggressiveLong && side == com.bot.TradingCore.Side.LONG) {
            minConf = Math.max(50.0, minConf - 6.0);
        }
        if (probability < minConf) {
            // [v78.1] Bucketed reject — гистограмма в [DIAG-ANALYZE].
            // NEAR (gap < 3pt): порог настроен в точку, рынок плотный
            // MID  (gap 3-7):   рассмотри -2pt в floor если паттерн постоянный
            // FAR  (gap > 7):   setup'ы реально слабые, не понижай floor
            String bucket;
            double gap = minConf - probability;
            if (gap < 3.0)      bucket = "prob_lt_minConf_early_NEAR";
            else if (gap < 7.0) bucket = "prob_lt_minConf_early_MID";
            else                bucket = "prob_lt_minConf_early_FAR";
            return reject(bucket);
        }
        //
        // RIVER FIX: бот ставил стоп 0.70% на монете с ATR 2-3%.
        // Причина: ATR был сжат в консолидации → стоп = noise level.
        // Решение: robustAtr() = max(currentATR, longTermATR×0.80).
        // Noise score: монеты с большими хвостами требуют шире стоп.
        double robustAtr14   = robustAtr(c15, 14);
        double robustAtrPct  = robustAtr14 / price;
        double noiseScore    = computeNoiseScore(c15, 14); // avg wick/body ratio

        // Classify coin's volatility for this symbol
        VolatilityBucket volBucket = classifyVolatility(robustAtrPct);

        // EXTREME VOLATILITY BLOCK: atr/price > 5% = shitcoin/micro-cap noise.
        // No indicator works reliably at this level. Signal would be 90% false positive.
        if (robustAtrPct > 0.05) {
            allFlags.add("EXTREME_VOL_BLOCK");
            return reject("extreme_vol");
        }

        // HIGH_ATR threshold corrected: 0.2% (was) → 1.5% (meaningful).
        // At 0.2% every single ALT would trigger HIGH_ATR — meaningless label.
        if (robustAtrPct > 0.015) allFlags.add("HIGH_ATR");

        // Noise → soft penalty only. Final minConf gate decides.
        // Прежний combo-reject (noise>2.7 AND prob<50) был double-penalty:
        // -15 за noise + reject за low-prob после того же штрафа. Если probability
        // после -15 всё ещё прошла minConf, сетап валидный — пусть идёт дальше.
        if (noiseScore > 2.8) {
            probability = Math.max(0, probability - 15);
            allFlags.add("HIGH_NOISE");
        } else if (noiseScore > 2.2) {
            probability = Math.max(0, probability - 8);
            allFlags.add("MOD_NOISE");
        }
        if (probability < minConf) {
            String bucket;
            double gap = minConf - probability;
            if (gap < 3.0)      bucket = "prob_lt_minConf_postNoise_NEAR";
            else if (gap < 7.0) bucket = "prob_lt_minConf_postNoise_MID";
            else                bucket = "prob_lt_minConf_postNoise_FAR";
            return reject(bucket);
        }

        // СТОП И ТЕЙК — [v37.0] ROBUST STRUCTURAL STOP PLACEMENT
        //
        // Иерархия приоритетов:
        // 1. Structural stop (swing high/low) — уважаем рыночную структуру
        // 2. ATR floor = robustAtr × minAtrMult (зависит от VolatilityBucket)
        // 3. Noise adjustment: шумные монеты получают +20-40% к стопу
        // 4. Cap: category-specific max stop % (не более maxStopPct)
        // REGIME-ADAPTIVE RISK PARAMETERS.
        // TREND: tight stop (1.2× ATR floor), wide TP (rrRatio boosted).
        //        Trend moves are directional — tight stop survives, wide TP captures the move.
        // RANGE: wide stop (1.8× ATR floor) behind channel boundary, tight TP to opposite wall.
        //        Range = mean-reversion. Wide stop avoids wick sweeps, tight TP = take profit at wall.
        // WEAK_TREND: balanced defaults.
        double riskMult;
        double rrRatio;
        double stopFloorMult; // multiplier for ATR stop minimum
        if (state == MarketState.STRONG_TREND) {
            riskMult = cat == CoinCategory.MEME ? 1.10 : cat == CoinCategory.ALT ? 0.85 : 0.70;
            rrRatio  = scoreDiff > 1.2 ? 3.8 : scoreDiff > 0.9 ? 3.4 : scoreDiff > 0.6 ? 3.0 : 2.6;
            stopFloorMult = 1.2; // tight stop in trend — noise is directional
            allFlags.add("REGIME_TREND");
        } else if (state == MarketState.RANGE) {
            riskMult = cat == CoinCategory.MEME ? 1.65 : cat == CoinCategory.ALT ? 1.35 : 1.10;
            rrRatio  = scoreDiff > 1.2 ? 2.2 : scoreDiff > 0.9 ? 1.9 : 1.6;
            stopFloorMult = 1.8; // wide stop in range — avoid wick sweeps
            allFlags.add("REGIME_RANGE");
        } else {
            // WEAK_TREND — balanced
            riskMult = cat == CoinCategory.MEME ? 1.40 : cat == CoinCategory.ALT ? 1.10 : 0.88;
            rrRatio  = scoreDiff > 1.2 ? 3.4 : scoreDiff > 0.9 ? 3.0 : scoreDiff > 0.6 ? 2.7 : 2.3;
            stopFloorMult = 1.5; // default
        }

        // ATR floor: uses robustAtr + VolatilityBucket multiplier.
        // БЫЛО: atr14 * 1.85 * riskMult → на RIVER в консолидации = 0.70%.
        // СТАЛО: robustAtr14 * bucket.minAtrMult * riskMult → всегда учитывает долгосрочный шум.
        double atrStop = Math.max(
                robustAtr14 * volBucket.minAtrMult * riskMult,
                price * 0.0025   // абсолютный минимум 0.25% (было 0.18%)
        );

        // [v43 PATCH FIX #4] Noise adjustment now starts at noiseScore > 2.0 (was 2.5).
        // Noisy coins need wider stops to survive their natural wick behaviour.
        // noiseScore=2.0: +0%, noiseScore=2.5: +15%, noiseScore=3.5: +45%, noiseScore=5.0: +75%
        if (noiseScore > 2.0) {
            double noiseAdj = Math.min(0.75, (noiseScore - 2.0) * 0.30);
            atrStop *= (1.0 + noiseAdj);
        }

        // Structural stop
        double structuralStop = findStructuralStop(c15, side, price, robustAtr14);

        // [v43 PATCH FIX #4b] Smart stop selection.
        //
        // OLD: stopDist = max(structDist, atrStop) — always takes raw atrStop as minimum.
        // PROBLEM: atrStop could be less than 1 wick of noise on volatile coins.
        // If the structural stop is 4% away but atrStop is 7%, old code took 7% — correct.
        // But if structural stop is 5% and atrStop is 3.5%, old code took 5% — also fine.
        // The bug was in the *no-structure* case: stopDist = atrStop × 1.0 → too tight.
        //
        // FIX: when no structural stop found → use 1.5× ATR (was 1.0×).
        //      when structural stop exists → floor it at 1.3× ATR for noise buffer.
        double stopDist;
        if (structuralStop <= 0) {
            // No swing level — ATR stop with regime-adaptive buffer
            stopDist = atrStop * stopFloorMult;
            allFlags.add("ATR_STOP");
        } else {
            double structDist = side == com.bot.TradingCore.Side.LONG
                    ? price - structuralStop
                    : structuralStop - price;
            // Regime-aware floor: trend needs tight (1.1×), range needs wide (1.5×)
            double atrFloor = atrStop * Math.max(1.1, stopFloorMult * 0.85);
            stopDist = Math.max(structDist, atrFloor);
            allFlags.add("STRUCT_STOP");
        }

        // Category-specific stop cap (replaces flat 3% for all)
        // ALT монеты могут требовать 5-8% стоп в HIGH_VOL режиме — это нормально.
        stopDist = Math.min(stopDist, price * volBucket.maxStopPct);

        // Safety flag: if cap trimmed the stop below ATR floor, flag it
        if (stopDist < atrStop) {
            allFlags.add("STOP_CAP_TIGHT");
        }

        // Wide structure → penalty, not veto
        if (stopDist > atrStop * 2.5) {
            allFlags.add("STRUCT_WIDE");
            probability = Math.max(0, probability - 7);
            if (probability < minConf) return reject("struct_wide_lt_minConf");
        }

        double stopPrice = side == com.bot.TradingCore.Side.LONG  ? price - stopDist : price + stopDist;
        double takePrice = side == com.bot.TradingCore.Side.LONG  ? price + stopDist * rrRatio
                : price - stopDist * rrRatio;

        if (!priceMovedEnough(symbol, price, robustAtrPct)) return reject("price_not_moved");

        // ForecastEngine Integration — RELAXED GATING
        // Philosophy: ForecastEngine is an ADVISOR, not a DICTATOR.
        // Only block when forecast STRONGLY disagrees. Otherwise,
        // let the cluster-based signal through with a penalty.
        com.bot.TradingCore.ForecastEngine.ForecastResult forecastResult = null;
        // [v24.0 FIX WEAK-3] Save probability before FC for penalty cap
        final double probBeforeFC = probability;
        if (forecastEngine != null) {
            try {
                vd = volumeDeltaMap.getOrDefault(symbol, 0.0);
                forecastResult = forecastEngine.forecast(c5, c15, c1h, vd);
                if (forecastResult != null) {
                    allFlags.add("FC_" + forecastResult.bias.name());
                    allFlags.add("PH_" + forecastResult.trendPhase.name());

                    boolean sigLong = side == com.bot.TradingCore.Side.LONG;
                    double fcDirAbs = Math.abs(forecastResult.directionScore);
                    boolean fcBull = forecastResult.directionScore > 0.2;
                    boolean fcBear = forecastResult.directionScore < -0.2;

                    // ALL VETOES → PENALTIES
                    // ForecastEngine is an ADVISOR, not a DICTATOR.
                    // Squeeze = OPPORTUNITY (penalty reduced, breakout boosted).

                    // SQUEEZE: reduced penalty, NOT veto (was return null)
                    Double squeezeFlag = forecastResult.factorScores.get("SQUEEZE");
                    if (squeezeFlag != null && squeezeFlag > 0.5) {
                        probability = Math.max(0, probability - 5);
                        allFlags.add("FC_SQUEEZE_PENALTY");
                    }

                    // EXHAUSTION: penalty -8 (was return null)
                    Double exhaustionFlag = forecastResult.factorScores.get("EXHAUSTION");
                    if (exhaustionFlag != null && exhaustionFlag > 0.70
                            && forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EXHAUSTION) {
                        Double moveDirFlag = forecastResult.factorScores.get("MOVE_DIR");
                        int moveDir = moveDirFlag != null ? (int) Math.signum(moveDirFlag) : 0;
                        if ((sigLong && moveDir > 0) || (!sigLong && moveDir < 0)) {
                            probability = Math.max(0, probability - 8);
                            allFlags.add("FC_EXHAUST_PENALTY");
                        }
                    }

                    // EXHAUSTION phase: penalty -6 unless overridden (was return null)
                    if (forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EXHAUSTION) {
                        boolean exhaustionOverride = forecastResult.confidence >= 0.76
                                && fcDirAbs >= 0.34
                                && ((sigLong && forecastResult.directionScore > 0)
                                || (!sigLong && forecastResult.directionScore < 0));
                        if (!exhaustionOverride) {
                            probability = Math.max(0, probability - 6);
                            allFlags.add("FC_EXHAUST_PHASE");
                        } else {
                            allFlags.add("FC_EXHAUST_OVERRIDE");
                        }
                    }

                    // STRONG disagreement: heavy penalty -15 (was return null)
                    if (sigLong && forecastResult.bias == com.bot.TradingCore.ForecastEngine.ForecastBias.STRONG_BEAR && fcDirAbs >= 0.35) {
                        probability = Math.max(0, probability - 15);
                        allFlags.add("FC_STRONG_BEAR_PENALTY");
                    }
                    if (!sigLong && forecastResult.bias == com.bot.TradingCore.ForecastEngine.ForecastBias.STRONG_BULL && fcDirAbs >= 0.35) {
                        probability = Math.max(0, probability - 15);
                        allFlags.add("FC_STRONG_BULL_PENALTY");
                    }

                    // Mild disagreement → penalty -4
                    if ((sigLong && fcBear) || (!sigLong && fcBull)) {
                        probability = Math.max(0, probability - 4);
                        allFlags.add("FC_DISAGREE");
                    }

                    // Projected move against direction → penalty -3
                    if (sigLong && forecastResult.projectedMovePct <= 0) {
                        probability = Math.max(0, probability - 3);
                        allFlags.add("FC_PROJ_DN");
                    }
                    if (!sigLong && forecastResult.projectedMovePct >= 0) {
                        probability = Math.max(0, probability - 3);
                        allFlags.add("FC_PROJ_UP");
                    }

                    // Early Counter-Trend: penalty -10 (was return null)
                    if (forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EARLY) {
                        boolean earlyOpposed = (sigLong && forecastResult.directionScore < -0.15)
                                || (!sigLong && forecastResult.directionScore > 0.15);
                        if (earlyOpposed && forecastResult.confidence > 0.40) {
                            probability = Math.max(0, probability - 10);
                            allFlags.add("FC_EARLY_REV_PENALTY");
                        }
                    }

                    // BOOST: EARLY phase aligned — raised +3→+7.
                    // EARLY_BULL/BEAR = fresh EMA cross + ATR expanding + MACD positive.
                    // This is the BEST entry phase. Under-rewarding it (only +3) caused
                    // the bot to rank exhaustion-phase signals equally with early signals.
                    if (forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EARLY
                            && Math.abs(forecastResult.directionScore) > 0.20) {
                        boolean earlyAligned = (sigLong && forecastResult.directionScore > 0)
                                || (!sigLong && forecastResult.directionScore < 0);
                        if (earlyAligned) {
                            probability = Math.min(PROB_CEIL, probability + 7);
                            allFlags.add("FC_EARLY_BOOST");
                        }
                    }

                    // ── RANGE quality gate — penalty, NOT veto ──
                    // Was hard veto → now soft penalty (-5)
                    double stopRetAbs = stopDist / (price + 1e-9);
                    double fcMoveAbs = Math.abs(forecastResult.projectedMovePct);
                    double fcConf = forecastResult.confidence;
                    boolean isRange = state == MarketState.RANGE;

                    if (isRange) {
                        boolean moveOk = fcMoveAbs >= stopRetAbs * 0.90;
                        boolean confOk = fcConf >= 0.50;
                        if (!moveOk || !confOk) {
                            probability = Math.max(0, probability - 5);
                            allFlags.add("FC_RANGE_PENALTY");
                        }
                        if (forecastResult.bias == com.bot.TradingCore.ForecastEngine.ForecastBias.NEUTRAL) {
                            probability = Math.max(0, probability - 3);
                            allFlags.add("FC_NEUTRAL_RANGE");
                        }
                    } else {
                        // PATCH #6: FC NEUTRAL in TREND was 0 penalty — fixed to -4.
                        // If ForecastEngine sees no direction in a trending market, that IS a signal:
                        // the trend may be exhausting or the data is ambiguous. Require more conviction.
                        if (forecastResult.bias == com.bot.TradingCore.ForecastEngine.ForecastBias.NEUTRAL) {
                            probability = Math.max(0, probability - 4);
                            allFlags.add("FC_NEUTRAL_TREND");
                        }
                        // Non-range: mild adjustments
                        if (forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EXHAUSTION) {
                            probability = Math.max(0, probability - 2);
                            allFlags.add("FC_EXHAUST_SOFT");
                        }
                        if (fcMoveAbs < stopRetAbs * 0.80) {
                            probability = Math.max(0, probability - 2);
                            allFlags.add("FC_MOVE_WEAK");
                        } else {
                            allFlags.add("FC_MOVE_OK");
                        }
                    }
                    // REMOVED: FC_LOWCONF_NEUTRAL veto — NEUTRAL means "I don't know",
                    // not "block everything". Let the cluster signal decide.

                    // [v24.0 FIX WEAK-3] FC PENALTY CAP — max -25 total from ForecastEngine.
                    // Old code stacked 7+ penalties totalling -51 points, always killing signals
                    // even when 5/6 clusters agreed. Cap prevents FC from being a dictator.
                    double fcTotalPenalty = probability - probBeforeFC;
                    if (fcTotalPenalty < -25.0) {
                        probability = Math.max(0, probBeforeFC - 25.0);
                        allFlags.add("FC_PENALTY_CAPPED_" + String.format("%.0f", fcTotalPenalty));
                    }
                }
            } catch (OutOfMemoryError oom) {
                throw oom; // не глотаем
            } catch (RuntimeException e) {
                // ForecastEngine hard-failed. Это НЕ нормально —
                // раньше мы просто игнорировали, теперь применяем conservative penalty
                // чтобы сигнал не прошёл "втихую" без FC-проверки.
                System.out.printf("[FC-ERROR] %s %s: %s%n",
                        symbol, e.getClass().getSimpleName(), e.getMessage());
                probability = Math.max(0, probability - 8);
                allFlags.add("FC_ERROR_PENALTY");
                forecastResult = null;
            } catch (Exception e) {
                System.out.printf("[FC] %s forecast error: %s%n", symbol, e.getMessage());
                probability = Math.max(0, probability - 8);
                allFlags.add("FC_ERROR_PENALTY");
                forecastResult = null;
            }
        }

        // VDA VOLUME DECELERATION EXHAUSTION VETO
        // Problem: bot shorts into the BOTTOM of a move because bearish
        // trend is confirmed, but the SPEED of selling is already dying.
        // Fix: if last 3×1m candles show BOTH price deceleration AND
        // volume shrinkage in the signal direction → soft penalty -10.
        // This is NOT a hard veto — it just pushes borderline signals
        // below the confidence floor. Strong signals survive.
        if (c1 != null && c1.size() >= 5) {
            int n1v = c1.size();
            com.bot.TradingCore.Candle cv1 = c1.get(n1v - 1);
            com.bot.TradingCore.Candle cv2 = c1.get(n1v - 2);
            com.bot.TradingCore.Candle cv3 = c1.get(n1v - 3);

            // Volume shrinking across last 3 bars
            boolean vShrinking = cv1.volume < cv2.volume * 0.88
                    && cv2.volume < cv3.volume * 0.88;

            if (vShrinking) {
                // Check if price momentum is slowing in signal direction
                double move1 = Math.abs(cv1.close - cv1.open);
                double move2 = Math.abs(cv2.close - cv2.open);
                double move3 = Math.abs(cv3.close - cv3.open);
                boolean momentumSlowing = move1 < move2 * 0.80 && move2 < move3 * 0.80;

                // Only apply for continuation signals (not early-reversal entries)
                boolean continuationSignal = (side == com.bot.TradingCore.Side.SHORT && cv3.close < cv3.open)
                        || (side == com.bot.TradingCore.Side.LONG && cv3.close > cv3.open);

                if (momentumSlowing && continuationSignal) {
                    probability = Math.max(0, probability - 10);
                    allFlags.add("VDA_DIV_" + side.name());
                }
            }
        }

        // [v42.0 FIX #1,#3,#4] CALIBRATED FINAL GATE
        // OLD: Math.max(50, Math.min(85, probability)) — artificial floor of 50,
        //      made calibration mathematically impossible.
        // NEW: clamp to [0..100] and pass through ProbabilityCalibrator (isotonic regression).
        //      If <50 historical samples → uses raw score as-is (no floor).
        //      If ≥50 samples → maps raw score to empirical win-rate via PAV.
        // CALIBRATOR BUCKET ALIGNMENT — BUG FIX.
        // БЫЛО: calibrate(symbol, rawProb01) → overload → calibrate(symbol, raw, 1.0) → ВСЕГДА бакет MID.
        // Запись (recordOutcome в BotMain): atrPct = robustAtrPct * 100.0 (в процентах, напр. 2.0 для 2% ATR).
        // Чтение раньше: всегда 1.0 → VolBucket.MID. Запись попадала в HIGH. Несовпадение = kalibrator
        // никогда не находил свою историю → fallback shrinkage ×0.5 → 72% без реальной коррекции.
        // СТАЛО: передаём тот же atrPct (в процентах) что используется при recordOutcome(). Теперь
        // запись и чтение используют идентичный VolBucket → per-symbol калибровка работает.
        double calibAtrPct  = robustAtrPct * 100.0;
        double rawProb01 = Math.max(0.0, Math.min(1.0, probability / 100.0));
        // [v79 SEGMENT] Pass BTC regime so calibrator can segment learning
        // by trending vs choppy market state. gicRef may be null in unit tests.
        String btcRegimeName = "NEUTRAL";
        try {
            if (gicRef != null) {
                com.bot.GlobalImpulseController.GlobalContext gc = gicRef.getContext();
                if (gc != null && gc.regime != null) btcRegimeName = gc.regime.name();
            }
        } catch (Throwable ignored) {}
        double calibrated01 = CALIBRATOR.calibrate(symbol, rawProb01, calibAtrPct, btcRegimeName);
        probability = calibrated01 * 100.0;
        // Single authoritative cap via PROB_CEIL. All intermediate caps above reference
        // the same constant, so this is purely a safety net for calibrator edge cases.
        probability = Math.max(0.0, Math.min(PROB_CEIL, probability));
        if (probability < minConf) {
            // [v78.1] Bucketed reject — see early reject above for interpretation
            String bucket;
            double gap = minConf - probability;
            if (gap < 3.0)      bucket = "calibrated_lt_minConf_NEAR";
            else if (gap < 7.0) bucket = "calibrated_lt_minConf_MID";
            else                bucket = "calibrated_lt_minConf_FAR";
            return reject(bucket);
        }
        // Одни и те же множители TP для всех режимов — главная причина
        // "недобора" в тренде и "перелёта" в боковике.
        //
        // RANGE:  цена ходит в канале → короткие TP, быстро фиксируем
        // TREND:  цена идёт далеко → длинные TP, не закрываем рано
        // EXHAUST: движение умирает → очень короткие TP, скальп
        double tp1Mult, tp2Mult, tp3Mult;
        boolean isTrendState  = state == MarketState.STRONG_TREND;
        boolean isRangeState  = state == MarketState.RANGE;
        boolean isExhaustPhase = forecastResult != null
                && forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EXHAUSTION;
        boolean isEarlyPhase  = forecastResult != null
                && forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EARLY;

        if (isExhaustPhase) {
            // [v80 FIX] EXHAUSTION: TP1 was 0.60×SL = NEGATIVE EV. Fixed to 1.20×SL.
            // Reasoning: даже при WR=60% и TP1=0.60×SL имеем (0.6×0.6)−(0.4×1) = −0.04
            // = −4% per trade. Минимум TP1 = 1.20×SL для positive EV при WR≥45%.
            tp1Mult = 1.20; tp2Mult = 2.00; tp3Mult = 3.00;
            allFlags.add("TP_EXHAUST");
        } else if (isRangeState) {
            // [v80 FIX] RANGE: TP1 was 0.65×SL = NEGATIVE EV. Same bug. Fixed.
            // Mean-reversion в боковике может работать, но только с правильным RR.
            // Если канал слишком узкий для RR≥1.2 — лучше не торговать вообще.
            tp1Mult = 1.20; tp2Mult = 2.00; tp3Mult = 2.80;
            allFlags.add("TP_RANGE");
        } else if (isTrendState && isEarlyPhase) {
            // EARLY TREND: widest TPs — the move has just started, let it run.
            tp1Mult = 1.40; tp2Mult = 2.80; tp3Mult = 4.50;
            allFlags.add("TP_TREND_EARLY");
        } else if (isTrendState) {
            // MID TREND: still wide, but not as aggressive as early.
            tp1Mult = 1.20; tp2Mult = 2.40; tp3Mult = 3.80;
            allFlags.add("TP_TREND");
        } else {
            // [v80 FIX] WEAK_TREND: TP1 поднят 1.00 → 1.20. Все режимы теперь имеют
            // TP1 ≥ 1.20×SL — гарантированная positive EV при WR ≥ 45%.
            tp1Mult = 1.20; tp2Mult = 2.20; tp3Mult = 3.50;
        }

        // VOLATILITY-BUCKET TP ADJUSTMENT
        //
        // HIGH/EXTREME vol монеты разворачиваются быстрее → тейки ближе.
        // Нет смысла держать позицию на +4% если монета ходит по ±3% в день.
        //
        // LOW vol в тренде (BTC/ETH) → можно держать дольше, движение чище.
        //
        // Bucket  | Factor  | Логика
        // LOW     | +10%    | Чистый тренд, мало шума, можно держать
        // MEDIUM  | ×1.00   | Стандарт
        // HIGH    | ×0.80   | Волатильная ALT — берём быстрее
        // EXTREME | ×0.60   | Шиткоин — только скальп, иначе уйдёт обратно
        double vtpFactor = switch (volBucket) {
            case LOW     -> isTrendState ? 1.10 : 1.00;
            case MEDIUM  -> 1.00;
            case HIGH    -> isExhaustPhase ? 0.65 : 0.80;
            case EXTREME -> 0.60;
        };
        if (vtpFactor != 1.00) {
            tp1Mult *= vtpFactor; tp2Mult *= vtpFactor; tp3Mult *= vtpFactor;
            allFlags.add("TP_VOL_" + volBucket.label);
        }

        // Пересчитываем rrRatio на основе выбранного tp3
        double adaptiveRR = tp3Mult;

        // SL WIDTH + R:R GUARD
        // Кейс SOONUSDT: SL 1.40% для монеты с ATR 5-8% → 0.25×ATR — выбивает шумом.
        // Минимум SL ширины = 0.8×ATR. R:R до TP1 мin 1:1.2, до TP2 мин 1:1.80.
        //
        // В этой архитектуре:
        //   tp1Mult/tp2Mult/tp3Mult — МНОЖИТЕЛИ risk-дистанции (R).
        //   TradeIdea ctor: tp1 = price ± risk × tp1Mult, где risk = |price - stop|
        // Значит R:R(tp1) = tp1Mult, R:R(tp2) = tp2Mult. Контролируем эти числа.
        double slDistanceD = Math.abs(price - stopPrice);
        // [FIX] SL width guard must use robustAtr14, not fast atr14.
        // Fast atr14 collapses 40-60% during consolidation → guard threshold drops to ~0.4%
        // → structural stop 0.82% passes the guard → noise stop guaranteed.
        // robustAtr14 = max(longTermATR × 0.80, 70%×longTerm + 30%×current) — always respects
        // the coin's real trading noise even during quiet periods.
        double minSlDistD  = robustAtr14 * 0.8;

        if (slDistanceD < minSlDistD) {
            stopPrice = (side == com.bot.TradingCore.Side.LONG)
                    ? price - minSlDistD
                    : price + minSlDistD;
            slDistanceD = minSlDistD;
            allFlags.add("SL_WIDEN_ATR");
        }

        // [FIX-9PCT 2026-05-02] R:R FLOOR не применяется в RANGE.
        //
        // Старая логика: в RANGE tp1=0.65, tp2=1.10 (короткие тейки —
        // правильно для боковика), но потом floor забивал tp2=2.00.
        // Получалось: цена в боковике, бот ставит TP за пределы канала,
        // цена не доходит → TIME_STOP. Это объясняет 77/89 = 87% TIME_STOP
        // в текущей выборке.
        //
        // Новое:
        //   RANGE: оставляем родные короткие TP. Если downstream Dispatcher
        //          отклонит R:R<2 — пусть отклоняет, в боковике может быть
        //          правильно НЕ торговать.
        //   TREND/WEAK: оригинальный floor 2.0 имеет смысл (даём цене
        //               пространство).
        // [v80 FIX] УНИФИЦИРОВАННЫЙ R:R FLOOR — TP1 ≥ 1.20R, TP2 ≥ 2.00R, TP3 ≥ TP2×1.30.
        // Старая логика разрешала RANGE иметь TP1=0.5R / TP2=0.9R = NEGATIVE EV.
        // В боковике с TP1<SL даже 70% WR даёт убыток. Если канал не позволяет
        // RR≥1.2 — это сигнал что НЕ НАДО торговать в боковике, а не повод
        // принимать плохие отношения.
        if (tp1Mult < 1.20) tp1Mult = 1.20;
        if (tp2Mult < 2.00) tp2Mult = 2.00;
        if (tp3Mult < tp2Mult * 1.30) tp3Mult = tp2Mult * 1.30;

        // Safety net: tp2Mult всегда ≥ 2.00.
        if (tp2Mult < 2.00) {
            LOG.info("[R:R FLOOR] " + symbol + " rejected: tp2Mult="
                    + String.format("%.2f", tp2Mult) + " < 2.00 (user pref 1:2)");
            return reject("rr_tp2_lt_2");
        }
        // [v80] Дополнительная проверка: TP1 < SL = математический баг, отклоняем.
        if (tp1Mult < 1.0) {
            LOG.info("[R:R FLOOR] " + symbol + " rejected: tp1Mult="
                    + String.format("%.2f", tp1Mult) + " < 1.0 (negative EV bug)");
            return reject("rr_tp1_lt_sl");
        }
        adaptiveRR = tp3Mult; // пересчитываем после возможной правки tp3Mult

        // ═══════════════════════════════════════════════════════════════
        // [v80] PRO-EDGE FILTERS — институциональные проверки edge
        // Решают проблему 25.5% WR / 88% time-stop из backtest:
        //   1. Late-entry: цена уже отъехала от EMA20 → движение отыграно
        //   2. MTF alignment: 15m против 1H тренда без RSI-confirmation
        //   3. Volume confirmation: сигнал на затухающем объёме
        //   4. BTC FLAT GATE: SHORT в нейтральном флэте = -EV
        // ═══════════════════════════════════════════════════════════════
        boolean isLong = side == com.bot.TradingCore.Side.LONG;

        // [1] LATE-ENTRY FILTER — distance from EMA20 в единицах ATR
        try {
            double ema20 = com.bot.TradingCore.ema(c15, 20);
            if (atr14 > 0 && ema20 > 0) {
                double distFromEma = Math.abs(price - ema20) / atr14;
                // Если цена уже >1.8×ATR от EMA20 в направлении сигнала — late entry.
                // Для LONG: блок если price >> ema20. Для SHORT: блок если price << ema20.
                boolean v80LateEntry = (isLong && price > ema20 && distFromEma > 1.8)
                        || (!isLong && price < ema20 && distFromEma > 1.8);
                if (v80LateEntry) {
                    return reject("late_entry_ema20");
                }
                allFlags.add(String.format("DEMA=%.1f", distFromEma));
            }
        } catch (Throwable ignored) {}

        // [2] MULTI-TIMEFRAME ALIGNMENT — 1H trend должен не противоречить.
        // Counter-trend разрешён только при extreme RSI (oversold для LONG, overbought для SHORT).
        try {
            if (c1h != null && c1h.size() >= 50) {
                double ema1h_20 = com.bot.TradingCore.ema(c1h, 20);
                double ema1h_50 = com.bot.TradingCore.ema(c1h, 50);
                double price1h = c1h.get(c1h.size() - 1).close;
                boolean bull1h = ema1h_20 > ema1h_50 && price1h > ema1h_20;
                boolean bear1h = ema1h_20 < ema1h_50 && price1h < ema1h_20;

                if (isLong && bear1h) {
                    double rsi15 = com.bot.TradingCore.rsi(c15, 14);
                    if (rsi15 > 38) return reject("mtf_long_vs_1h_bear");
                    allFlags.add("MTF_OS_BOUNCE");
                }
                if (!isLong && bull1h) {
                    double rsi15 = com.bot.TradingCore.rsi(c15, 14);
                    if (rsi15 < 62) return reject("mtf_short_vs_1h_bull");
                    allFlags.add("MTF_OB_SHORT");
                }
                if ((isLong && bull1h) || (!isLong && bear1h)) {
                    allFlags.add("MTF_ALIGN");
                }
            }
        } catch (Throwable ignored) {}

        // [3] VOLUME CONFIRMATION — последние 3 бара ≥ 1.05× от 20-бар avg.
        // Сигналы на затухающем объёме статистически проигрывают.
        try {
            if (c15.size() >= 25) {
                int vN15 = c15.size();
                double avgVol20 = 0;
                for (int i = vN15 - 23; i < vN15 - 3; i++) avgVol20 += c15.get(i).volume;
                avgVol20 /= 20.0;
                double recent3Vol = (c15.get(vN15-1).volume + c15.get(vN15-2).volume + c15.get(vN15-3).volume) / 3.0;
                if (avgVol20 > 0) {
                    double volRatio = recent3Vol / avgVol20;
                    if (volRatio < 1.05) {
                        return reject("low_volume");
                    }
                    if (volRatio >= 1.5) allFlags.add("VOL_SURGE");
                }
            }
        } catch (Throwable ignored) {}

        // [4] BTC REGIME GATE — для SHORT в NEUTRAL+слабом BTC = блок.
        // Backtest показал: 80% сигналов SHORT при BTC NEUTRAL str=0.12-0.17 → -81% PnL.
        try {
            if (gicCtx != null && !isLong) {
                String regime = String.valueOf(gicCtx.regime);
                double btcStr = gicCtx.impulseStrength;
                boolean btcFlat = regime.contains("NEUTRAL") || regime.contains("FLAT") || regime.contains("RANGE");
                if (btcFlat && btcStr < 0.30) {
                    return reject("btc_flat_short_block");
                }
                // SHORT vs strong BTC bull — только при ≥4 кластерах
                boolean btcBull = regime.contains("UP") || regime.contains("BULL");
                if (btcBull && btcStr > 0.5) {
                    int clusterTotal = 0;
                    for (String f : allFlags) {
                        if (f != null && f.contains("CONFL_S")) {
                            String tail = f.replaceAll("[^0-9]", "");
                            try { clusterTotal = Math.max(clusterTotal, Integer.parseInt(tail)); }
                            catch (NumberFormatException e2) {}
                        }
                    }
                    if (clusterTotal < 4) return reject("btc_bull_short_underconfirmed");
                }
            }
        } catch (Throwable ignored) {}

        // [5] BB SQUEEZE CHECK — не входим в volatility contraction (squeeze).
        // BB width рассчитывается через TradingCore.bollinger().
        try {
            if (c15.size() >= 25) {
                com.bot.TradingCore.BollingerResult bb = com.bot.TradingCore.bollinger(c15, 20, 2.0);
                if (bb != null && price > 0) {
                    double bbWidth = (bb.upper - bb.lower) / price;
                    if (bbWidth < 0.012) {
                        return reject("bb_squeeze");
                    }
                    allFlags.add(String.format("BBW=%.3f", bbWidth));
                }
            }
        } catch (Throwable ignored) {}

        // ═══════════════════════════════════════════════════════════════
        // END PRO-EDGE FILTERS
        // ═══════════════════════════════════════════════════════════════

        TradeIdea idea = new TradeIdea(symbol, side, price, stopPrice, takePrice, adaptiveRR,
                probability, allFlags,
                fundingRate, fundingDelta, oiChange, bias2h.name(), cat,
                forecastResult,
                tp1Mult, tp2Mult, tp3Mult);
        // [v50 AUDIT FIX] Thread the real robustAtrPct through to the TradeIdea so that
        // ProbabilityCalibrator gets the correct VolBucket instead of a stop-distance proxy.
        idea.setRobustAtrPct(robustAtrPct);
        return idea;
    }

    //  CLUSTER-BASED CONFIDENCE

    private double computeClusterConfidence(
            String symbol,
            double scoreLong, double scoreShort, double scoreDiff,
            int longClusters, int shortClusters,
            MarketState state, CoinCategory cat,
            double atr, double price,
            boolean bullDiv, boolean bearDiv,
            boolean pullUp, boolean pullDown,
            boolean impulse, boolean pump,
            boolean hasFR, boolean hasFVG, boolean hasOB, boolean hasBOS, boolean liqSweep,
            HTFBias bias2h, double vwap) {

        boolean isLong = scoreLong > scoreShort;
        int clusters = isLong ? longClusters : shortClusters;

        // ── Score normalization ─────────────────────────────────────────
        // Divides by 5.0 (restored from 4.5). The smaller divisor was inflating
        // norm for moderate scoreDiff values and compressing the upper range.
        // scoreDiff range in practice: 0.3 (weak) to 3.5 (strong); /5.0 gives [0.06..0.70].
        double norm = Math.min(1.0, scoreDiff / 5.0);

        // ── Cluster bonus ───────────────────────────────────────────────
        // Reduced from {0.18/0.14/0.09/0.04} to {0.12/0.09/0.05/0.02}.
        // Old values on top of base=80 for 6 clusters made every 6-cluster signal
        // hit PROB_CEIL regardless of scoreDiff (even a weak 6-cluster confluence).
        // New values still reward cluster count but preserve score-diff discrimination.
        double clusterBonus = switch (clusters) {
            case 6 -> 0.12;
            case 5 -> 0.09;
            case 4 -> 0.05;
            case 3 -> 0.02;
            default -> 0.0;
        };
        norm += clusterBonus;

        // ── HTF alignment bonus ─────────────────────────────────────────
        if ((bias2h == HTFBias.BULL && isLong) || (bias2h == HTFBias.BEAR && !isLong)) {
            norm += 0.06;
        }

        // ── VWAP alignment bonus ────────────────────────────────────────
        if ((isLong && price > vwap * 1.0005) || (!isLong && price < vwap * 0.9995)) {
            norm += 0.025;
        }

        // ── Divergence quality bonuses ──────────────────────────────────
        if ((isLong && bullDiv) || (!isLong && bearDiv)) norm += 0.07;

        if (hasFVG && hasOB) norm += 0.04;
        else if (hasFVG || hasOB) norm += 0.02;
        if (liqSweep) norm += 0.03;
        if (hasFR) norm += 0.03;

        norm = Math.min(1.0, norm);

        // [v71] CRITICAL REBALANCE — главная причина silence бота.
        //
        // Анализ старой математики: для типичной альты в NEUTRAL рынке (3 кластера,
        // scoreDiff=0.30) norm = 0.30/5.0 + 0.02 + 0.06(HTF) + 0.025(VWAP) ≈ 0.165.
        //   prob = 56 + (0.165 - 0.50) × 24 = 56 - 8.04 = 48
        //   - RANGE -2 - ALT -1 = 45 < MIN_CONF_FLOOR=55 → REJECT.
        // 3-cluster setup был МАТЕМАТИЧЕСКИ невозможен в RANGE/NEUTRAL.
        //
        // Фикс: (1) поднять clusterBase для 2-3 кластеров, (2) сместить center
        // формулы с 0.50 на 0.30 (реалистичный median norm в плоском рынке),
        // (3) уменьшить gain 24→22 чтобы не раздувать 4-6 cluster setups.
        //
        // Новая математика для тех же условий: 60 + (0.165 - 0.30) × 22 = 60 - 2.97 = 57.
        // - RANGE -0.5 (A2) - ALT 0 (A3) = 56.5 → проходит floor=52, даёт калибратору шанс.
        double clusterBase = switch (clusters) {
            case 6 -> 78.0;
            case 5 -> 70.0;
            case 4 -> 63.0;
            case 3 -> 60.0;   // 56→60: 3-cluster setup в RANGE+ALT теперь дотягивает
            case 2 -> 53.0;   // 48→53: 2-cluster + сильный score теперь имеет шанс
            default -> 40.0;  // 38→40
        };

        double prob = clusterBase + (norm - 0.30) * 22.0;

        // [v71] State penalty: RANGE -2→-0.5. RANGE = normal, не должен жёстко
        // штрафоваться. MOVRUSDT (единственный сигнал за 8 часов) был в RANGE
        // и взял TP1 — доказательство что RANGE сетапы валидны.
        if      (state == MarketState.STRONG_TREND) prob += 3.0;
        else if (state == MarketState.RANGE)        prob -= 0.5;

        // [v71] Category penalties: ALT -1→0. 80% монет это ALT — постоянный
        // штраф был arbitrary tax. MEME оставлен -8 (реальная noise penalty).
        if (cat == CoinCategory.MEME)     prob -= 8.0;
        else if (cat == CoinCategory.ALT) prob -= 0.0;

        // ── Live Bayesian prior blend ───────────────────────────────────
        double priorProb = 50.0 + (bayesPrior.get() - 0.50) * 26.0;
        int totalTrades = (int) Math.min(200L, Math.max(0L, bayesSampleTrades.get()));
        double priorWeight = totalTrades >= 80 ? 0.12 : totalTrades >= 30 ? 0.08 : 0.04;
        prob = prob * (1.0 - priorWeight) + priorProb * priorWeight;

        return Math.round(clamp(prob, 0, 100));
    }

    // [v42.1 REMOVED] historicalAccuracy() — was dead code (called only from removed
    // 70/30 blend in computeClusterConfidence). Calibration now lives entirely in
    // ProbabilityCalibrator (PAV isotonic regression with vol-buckets).

    //  COOLDOWN

    /**
     * [v24.0 FIX BUG-2] CHECK ONLY — does NOT set cooldown anymore.
     * Old code set cooldown here (line 1295), so rejected signals burned the cooldown window.
     * Valid signals coming 30s later were blocked because the rejected signal consumed the cooldown.
     * Now cooldown is set ONLY through confirmSignal() after ISC approves.
     */
    private boolean cooldownAllowedEx(String sym, com.bot.TradingCore.Side side,
                                      CoinCategory cat, long now, long shortOverrideMs) {
        String key  = sym + "_" + side;
        long   base;
        if (side == com.bot.TradingCore.Side.SHORT && shortOverrideMs > 0) {
            base = shortOverrideMs;
        } else {
            base = cat == CoinCategory.TOP  ? COOLDOWN_TOP :
                    cat == CoinCategory.ALT  ? COOLDOWN_ALT : COOLDOWN_MEME;
        }
        Long last = cooldownMap.get(key);
        // CHECK ONLY — removed: cooldownMap.put(key, now)
        return last == null || now - last >= base;
    }

    private boolean cooldownAllowed(String sym, com.bot.TradingCore.Side side, CoinCategory cat, long now) {
        return cooldownAllowedEx(sym, side, cat, now, -1);
    }

    private boolean flipAllowed(String sym, com.bot.TradingCore.Side newSide) {
        Deque<String> h = recentDirs.computeIfAbsent(sym, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        if (h.size() < 2) return true;
        Iterator<String> it = h.descendingIterator();
        String last = it.next(), prev = it.next();
        return !(!last.equals(newSide.name()) && prev.equals(newSide.name()));
    }

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
        double dynThreshold = Math.max(0.0035, atr14Pct * 0.15);
        return Math.abs(price - last) / last >= dynThreshold;
    }

    // Backward-compatible overload (uses static threshold when ATR not available)
    private boolean priceMovedEnough(String sym, double price) {
        return priceMovedEnough(sym, price, 0.0);
    }

    //  REVERSAL STRUCTURE CONFIRMATION

    private boolean confirmReversalStructure(List<com.bot.TradingCore.Candle> c1,
                                             List<com.bot.TradingCore.Candle> c5,
                                             com.bot.TradingCore.Side side) {
        boolean longSide = side == com.bot.TradingCore.Side.LONG;
        if (c1 != null && c1.size() >= 10) {
            int n = c1.size();
            double localHigh = Double.NEGATIVE_INFINITY;
            double localLow  = Double.POSITIVE_INFINITY;
            for (int i = n - 8; i < n - 1; i++) {
                localHigh = Math.max(localHigh, c1.get(i).high);
                localLow  = Math.min(localLow,  c1.get(i).low);
            }
            com.bot.TradingCore.Candle last1m = last(c1);
            if (longSide  && last1m.close > localHigh * 1.0003) return true;
            if (!longSide && last1m.close < localLow  * 0.9997) return true;
        }
        if (c1 != null && c1.size() >= 5) {
            int n = c1.size();
            int bullCount = 0, bearCount = 0;
            for (int i = n - 4; i < n; i++) {
                com.bot.TradingCore.Candle c = c1.get(i);
                if (c.close > c.open) bullCount++; else bearCount++;
            }
            if (longSide  && bullCount >= 3) return true;
            if (!longSide && bearCount >= 3) return true;
        }
        if (c5 != null && c5.size() >= 3) {
            com.bot.TradingCore.Candle last5m = last(c5);
            double body5  = Math.abs(last5m.close - last5m.open);
            double range5 = last5m.high - last5m.low + 1e-10;
            if (longSide  && last5m.close > last5m.open && body5 / range5 > 0.60) return true;
            if (!longSide && last5m.close < last5m.open && body5 / range5 > 0.60) return true;
        }
        return false;
    }

    //  ANTI-LAG DETECTION

    private static class AntiLagResult {
        final int direction; final double strength;
        AntiLagResult(int d, double s) { direction = d; strength = s; }
    }

    private AntiLagResult detectAntiLag(List<com.bot.TradingCore.Candle> c1,
                                        List<com.bot.TradingCore.Candle> c5,
                                        List<com.bot.TradingCore.Candle> c15) {
        if (c1 == null || c1.size() < 5 || c5 == null || c5.size() < 3) return null;
        int n1 = c1.size();
        double atr1 = atr(c1, Math.min(14, n1 - 1));

        double range1 = last(c1).high - last(c1).low;
        double body1  = Math.abs(last(c1).close - last(c1).open);
        if (range1 > atr1 * 1.85 && body1 / range1 > 0.70) {
            int d = last(c1).close > last(c1).open ? 1 : -1;
            double s = Math.min(0.78, range1 / atr1 * 0.28);
            if (s > 0.38) return new AntiLagResult(d, s);
        }

        int n5 = c5.size();
        double atr5 = atr(c5, Math.min(14, n5 - 1));
        com.bot.TradingCore.Candle lc5 = last(c5);
        double avgV5 = c5.subList(Math.max(0, n5 - 8), n5 - 1)
                .stream().mapToDouble(c -> c.volume).average().orElse(lc5.volume);
        if (lc5.volume > avgV5 * 1.7) {
            double mv5 = Math.abs(lc5.close - lc5.open);
            if (mv5 > atr5 * 0.60) {
                int d = lc5.close > lc5.open ? 1 : -1;
                double s = Math.min(0.75, mv5 / atr5 * 0.38);
                if (s > 0.36) return new AntiLagResult(d, s);
            }
        }

        int grn = 0, red = 0; double serMove = 0;
        for (int i = Math.max(0, n1 - 4); i < n1; i++) {
            com.bot.TradingCore.Candle c = c1.get(i);
            if (c.close > c.open) grn++; else red++;
            serMove += c.close - c.open;
        }
        if ((grn >= 3 || red >= 3) && Math.abs(serMove) > atr1 * 1.45) {
            int d = serMove > 0 ? 1 : -1;
            double s = Math.min(0.72, Math.abs(serMove) / atr1 * 0.37);
            if (s > 0.34) return new AntiLagResult(d, s);
        }
        return null;
    }

    //  REVERSE EXHAUSTION DETECTION

    private static class ReverseWarning {
        final String type; final double confidence;
        ReverseWarning(String t, double c) { type = t; confidence = c; }
    }

    private ReverseWarning detectReversePattern(List<com.bot.TradingCore.Candle> c15,
                                                List<com.bot.TradingCore.Candle> c1h,
                                                MarketState state) {
        if (c15.size() < 8 || c1h.size() < 5) return null;
        double score = 0;
        boolean longExh = false, shortExh = false;

        double rsi1h = rsi(c1h, 14);
        if (rsi1h > 75.0) { score += 0.27; longExh  = true; }
        if (rsi1h < 25.0) { score += 0.27; shortExh = true; }

        double mom  = momentumPct(c15, 5, 0);
        double mom1 = momentumPct(c15, 5, 5);
        double mom2 = momentumPct(c15, 5, 10);
        if (mom < mom1 * 0.63 && mom1 < mom2 * 0.68 && mom1 > 0) { score += 0.48; longExh  = true; }
        if (mom > mom1 * 1.37 && mom1 > mom2 * 1.32 && mom1 < 0) { score += 0.48; shortExh = true; }

        double avgVol = c15.subList(Math.max(0, c15.size() - 15), c15.size() - 3)
                .stream().mapToDouble(c -> c.volume).average().orElse(1);
        if (last(c15).volume < avgVol * 0.52) score += 0.32;

        com.bot.TradingCore.Candle lc = last(c15);
        double uw = lc.high - Math.max(lc.open, lc.close);
        double lw = Math.min(lc.open, lc.close) - lc.low;
        double bd = Math.abs(lc.close - lc.open) + 1e-10;
        if (uw > bd * 2.4 && lc.close < lc.open) { score += 0.38; longExh  = true; }
        if (lw > bd * 2.4 && lc.close > lc.open) { score += 0.38; shortExh = true; }

        if (c15.size() >= 2) {
            double adxC = adx(c15, 14);
            double adxP = adx(c15.subList(0, c15.size() - 1), 14);
            if (adxP > adxC && adxC > 20) score += 0.28;
        }

        if (c15.size() >= 3) {
            com.bot.TradingCore.Candle ca = c15.get(c15.size() - 3);
            com.bot.TradingCore.Candle cb = c15.get(c15.size() - 2);
            com.bot.TradingCore.Candle cc = c15.get(c15.size() - 1);
            double ba = Math.abs(ca.close - ca.open);
            double bb = Math.abs(cb.close - cb.open);
            double bc = Math.abs(cc.close - cc.open);
            if (ca.close > ca.open && cb.close > cb.open && cc.close > cc.open
                    && ba > bb * 1.08 && bb > bc * 1.08) { score += 0.32; longExh = true; }
            if (ca.close < ca.open && cb.close < cb.open && cc.close < cc.open
                    && ba > bb * 1.08 && bb > bc * 1.08) { score += 0.32; shortExh = true; }
        }

        if (c15.size() >= 2) {
            com.bot.TradingCore.Candle prevC = c15.get(c15.size() - 2);
            if (prevC.close > prevC.open) {
                double prevBd = prevC.close - prevC.open;
                if (prevBd > bd * 2.0 && uw > bd * 1.7) { score += 0.38; longExh = true; }
            }
        }

        if (score < 0.48) return null;

        String type;
        if (longExh && !shortExh)      type = "LONG_EXHAUSTION";
        else if (shortExh && !longExh) type = "SHORT_EXHAUSTION";
        else                           type = "REVERSAL";

        return new ReverseWarning(type, score);
    }

    private double momentumPct(List<com.bot.TradingCore.Candle> c, int bars, int offset) {
        if (c.size() < bars + offset + 1) return 0;
        int n = c.size();
        double base = c.get(n - offset - bars - 1).close;
        return (c.get(n - offset - 1).close - base) / (base + 1e-9);
    }

    //  EARLY REVERSAL DETECTION
    //  5 независимых сигналов раннего разворота:
    //  1. Momentum Deceleration — свечи уменьшаются, тренд слабеет
    //  2. Volume Divergence — цена = новый экстремум, объём падает
    //  3. Wick Rejection — длинная тень на 1m/5m = отвергли уровень
    //  4. RSI Momentum Shift — RSI разворачивается раньше цены
    //  5. Micro Structure Break — на 1m сломалась структура

    private static final class EarlyReversalResult {
        final boolean detected;
        final int     direction; // +1 = reversal to LONG, -1 = reversal to SHORT
        final double  strength;  // 0..1
        final List<String> flags;

        EarlyReversalResult(boolean d, int dir, double s, List<String> f) {
            detected = d; direction = dir; strength = s; flags = f;
        }
    }

    private EarlyReversalResult detectEarlyReversal(
            List<com.bot.TradingCore.Candle> c1,
            List<com.bot.TradingCore.Candle> c5,
            List<com.bot.TradingCore.Candle> c15,
            double rsi14, double rsi7,
            double price, double atr14) {

        if (c15.size() < 12) return new EarlyReversalResult(false, 0, 0, List.of());

        double score = 0;
        int bullSignals = 0, bearSignals = 0;
        List<String> flags = new ArrayList<>();

        // ── Определяем текущий тренд на 15m ──────────────────
        double move4 = (last(c15).close - c15.get(c15.size() - 5).close) / price;
        // [v50 §6] Lowered trend detection threshold 0.003→0.002 for earlier reversal catch
        boolean inUptrend   = move4 > 0.002;
        boolean inDowntrend = move4 < -0.002;

        // Если нет выраженного тренда — ранний разворот не применяется
        if (!inUptrend && !inDowntrend) return new EarlyReversalResult(false, 0, 0, List.of());

        // 1. MOMENTUM DECELERATION
        // Тренд идёт вверх/вниз, но свечи УМЕНЬШАЮТСЯ.
        // Тело 3-й < тело 2-й < тело 1-й = тренд теряет силу.
        if (c15.size() >= 5) {
            double b1 = Math.abs(c15.get(c15.size()-1).close - c15.get(c15.size()-1).open);
            double b2 = Math.abs(c15.get(c15.size()-2).close - c15.get(c15.size()-2).open);
            double b3 = Math.abs(c15.get(c15.size()-3).close - c15.get(c15.size()-3).open);

            // Все 3 свечи в одном направлении, но уменьшаются
            boolean allUp = c15.get(c15.size()-1).close > c15.get(c15.size()-1).open
                    && c15.get(c15.size()-2).close > c15.get(c15.size()-2).open
                    && c15.get(c15.size()-3).close > c15.get(c15.size()-3).open;
            boolean allDown = c15.get(c15.size()-1).close < c15.get(c15.size()-1).open
                    && c15.get(c15.size()-2).close < c15.get(c15.size()-2).open
                    && c15.get(c15.size()-3).close < c15.get(c15.size()-3).open;

            // [v50 §6] Relaxed deceleration thresholds: 0.72→0.80, 0.82→0.88
            // Catches momentum loss 1 bar earlier when trend is starting to slow
            if (allUp && b1 < b2 * 0.80 && b2 < b3 * 0.88) {
                // Лонг-тренд слабеет → разворот вниз
                score += 0.30;
                bearSignals++;
                flags.add("DECEL_UP");
            }
            if (allDown && b1 < b2 * 0.80 && b2 < b3 * 0.88) {
                // Шорт-тренд слабеет → разворот вверх
                score += 0.30;
                bullSignals++;
                flags.add("DECEL_DN");
            }
        }

        // 2. VOLUME DIVERGENCE
        // Цена делает новый хай/лой, но объём падает.
        // Smart money уже вышли — розница догоняет.
        if (c15.size() >= 6) {
            com.bot.TradingCore.Candle cur  = c15.get(c15.size() - 1);
            com.bot.TradingCore.Candle prev = c15.get(c15.size() - 2);
            double avgVol3 = (c15.get(c15.size()-4).volume + c15.get(c15.size()-3).volume
                    + c15.get(c15.size()-2).volume) / 3.0;

            if (inUptrend && cur.high > prev.high && cur.volume < avgVol3 * 0.70) {
                score += 0.28;
                bearSignals++;
                flags.add("VDIV_UP");
            }
            if (inDowntrend && cur.low < prev.low && cur.volume < avgVol3 * 0.70) {
                score += 0.28;
                bullSignals++;
                flags.add("VDIV_DN");
            }
        }

        // 3. WICK REJECTION на 1m/5m
        // Длинная тень в направлении тренда = цену отвергли.
        // Если на 5m последняя свеча имеет тень > 2× тело
        // в направлении тренда — это rejection.
        if (c5 != null && c5.size() >= 3) {
            com.bot.TradingCore.Candle lc5 = c5.get(c5.size() - 1);
            double body5 = Math.abs(lc5.close - lc5.open) + 1e-10;
            double upperWick5 = lc5.high - Math.max(lc5.open, lc5.close);
            double lowerWick5 = Math.min(lc5.open, lc5.close) - lc5.low;

            if (inUptrend && upperWick5 > body5 * 2.0 && lc5.close < lc5.open) {
                // Длинная верхняя тень при аптренде + медвежья свеча = rejection сверху
                score += 0.32;
                bearSignals++;
                flags.add("WICK_REJ_UP");
            }
            if (inDowntrend && lowerWick5 > body5 * 2.0 && lc5.close > lc5.open) {
                // Длинная нижняя тень при даунтренде + бычья свеча = rejection снизу
                score += 0.32;
                bullSignals++;
                flags.add("WICK_REJ_DN");
            }
        }

        // Дополнительно проверяем на 1m
        if (c1 != null && c1.size() >= 5) {
            com.bot.TradingCore.Candle lc1 = c1.get(c1.size() - 1);
            double body1 = Math.abs(lc1.close - lc1.open) + 1e-10;
            double uWick1 = lc1.high - Math.max(lc1.open, lc1.close);
            double lWick1 = Math.min(lc1.open, lc1.close) - lc1.low;

            if (inUptrend && uWick1 > body1 * 2.5) {
                score += 0.18;
                bearSignals++;
                flags.add("WICK1M_UP");
            }
            if (inDowntrend && lWick1 > body1 * 2.5) {
                score += 0.18;
                bullSignals++;
                flags.add("WICK1M_DN");
            }
        }

        // 4. RSI MOMENTUM SHIFT
        // RSI7 начинает падать с зоны перекупленности/перепроданности
        // РАНЬШЕ чем цена развернулась.
        if (inUptrend && rsi7 < 62 && rsi14 > 65) {
            // RSI7 уже упал ниже 62, но RSI14 ещё выше 65 = дивергенция скоростей
            score += 0.25;
            bearSignals++;
            flags.add("RSI_SHIFT_DN");
        }
        if (inDowntrend && rsi7 > 38 && rsi14 < 35) {
            // RSI7 уже поднялся выше 38, но RSI14 ещё ниже 35
            score += 0.25;
            bullSignals++;
            flags.add("RSI_SHIFT_UP");
        }

        // 5. MICRO STRUCTURE BREAK на 1m
        // На 15m ещё HH/HL (бычий), но на 1m уже появился LH
        // (Lower High) — микро-структура сломалась РАНЬШЕ.
        if (c1 != null && c1.size() >= 15) {
            int n1 = c1.size();
            // Ищем структуру на последних 12 минутных свечах
            double micro1H = Double.NEGATIVE_INFINITY;
            double micro2H = Double.NEGATIVE_INFINITY;
            double micro1L = Double.POSITIVE_INFINITY;
            double micro2L = Double.POSITIVE_INFINITY;

            // Первая половина (6 свечей назад)
            for (int i = n1 - 12; i < n1 - 6; i++) {
                micro1H = Math.max(micro1H, c1.get(i).high);
                micro1L = Math.min(micro1L, c1.get(i).low);
            }
            // Вторая половина (последние 6 свечей)
            for (int i = n1 - 6; i < n1; i++) {
                micro2H = Math.max(micro2H, c1.get(i).high);
                micro2L = Math.min(micro2L, c1.get(i).low);
            }

            if (inUptrend && micro2H < micro1H * 0.9995) {
                // LH на 1m при аптренде на 15m = ранний медвежий сигнал
                score += 0.26;
                bearSignals++;
                flags.add("MICRO_LH");
            }
            if (inDowntrend && micro2L > micro1L * 1.0005) {
                // HL на 1m при даунтренде на 15m = ранний бычий сигнал
                score += 0.26;
                bullSignals++;
                flags.add("MICRO_HL");
            }
        }

        // АГРЕГАЦИЯ
        // Нужно минимум 2 сигнала в одном направлении
        if (bearSignals >= 2 && inUptrend && score >= 0.45) {
            // Ранний разворот вниз (SHORT): тренд был вверх, но слабеет
            return new EarlyReversalResult(true, -1, Math.min(score, 0.85), flags);
        }
        if (bullSignals >= 2 && inDowntrend && score >= 0.45) {
            // Ранний разворот вверх (LONG): тренд был вниз, но слабеет
            return new EarlyReversalResult(true, 1, Math.min(score, 0.85), flags);
        }

        return new EarlyReversalResult(false, 0, 0, flags);
    }

    //  STRONG REVERSAL DETECTOR
    //  High-conviction reversal after extended one-sided moves.
    //  Fires ONLY when at least 3 of 5 confluence signals agree:
    //    1. N consecutive bars in one direction (extended move)
    //    2. RSI in extreme zone (>72 or <28)
    //    3. RSI divergence (regular bearish/bullish)
    //    4. MACD histogram sign flip
    //    5. Volume drying up (3-bar avg < 70% of 10-bar avg)
    //  Strength >= 0.55 required to pass threshold.
    //  This is the "many candles in one direction — watch for reversal" logic
    //  the user asked for.

    private static final class StrongReversalResult {
        final boolean detected;
        final int     direction; // +1 reversal UP (LONG after downtrend), -1 reversal DOWN (SHORT after uptrend)
        final double  strength;  // 0..1
        final List<String> flags;
        StrongReversalResult(boolean d, int dir, double s, List<String> f) {
            detected = d; direction = dir; strength = s; flags = f;
        }
    }

    private StrongReversalResult detectStrongReversal(
            List<com.bot.TradingCore.Candle> c15,
            double[] rsiSeries,
            double rsi14) {

        List<String> flags = new ArrayList<>();
        if (c15 == null || c15.size() < 20 || rsiSeries == null || rsiSeries.length < 20) {
            return new StrongReversalResult(false, 0, 0, flags);
        }

        int n = c15.size();
        int confluence = 0;
        int direction = 0; // +1 up, -1 down

        // ── 1. Consecutive one-sided candles (extended move) ──────
        int greenStreak = 0, redStreak = 0;
        for (int i = n - 1; i >= Math.max(0, n - 8); i--) {
            com.bot.TradingCore.Candle c = c15.get(i);
            if (c.close > c.open) {
                if (redStreak > 0) break;
                greenStreak++;
            } else if (c.close < c.open) {
                if (greenStreak > 0) break;
                redStreak++;
            } else break;
        }
        boolean extendedUp   = greenStreak >= 5;  // 5+ consecutive green = extended uptrend
        boolean extendedDown = redStreak   >= 5;  // 5+ consecutive red   = extended downtrend
        if (extendedUp) {
            confluence++;
            direction = -1; // reversal down
            flags.add("EXTD_UP_" + greenStreak);
        } else if (extendedDown) {
            confluence++;
            direction = +1; // reversal up
            flags.add("EXTD_DN_" + redStreak);
        } else {
            // Without extended move, reversal signal has no context — return early
            return new StrongReversalResult(false, 0, 0, flags);
        }

        // ── 2. RSI in extreme zone ──────────────────────────
        if (direction < 0 && rsi14 >= 72) { confluence++; flags.add("RSI_OVERBOUGHT"); }
        if (direction > 0 && rsi14 <= 28) { confluence++; flags.add("RSI_OVERSOLD"); }

        // ── 3. RSI regular divergence ───────────────────────
        // Bearish: price higher high, RSI lower high → sell
        // Bullish: price lower low,  RSI higher low  → buy
        try {
            List<com.bot.TradingCore.Divergence> divs = com.bot.TradingCore.detectDivergences(c15, 14, 30, 2);
            if (divs != null && !divs.isEmpty()) {
                com.bot.TradingCore.Divergence latest = divs.get(divs.size() - 1);
                if (direction < 0 && latest.type == com.bot.TradingCore.Divergence.Type.REGULAR_BEARISH) {
                    confluence++; flags.add("RSI_DIV_BEAR");
                }
                if (direction > 0 && latest.type == com.bot.TradingCore.Divergence.Type.REGULAR_BULLISH) {
                    confluence++; flags.add("RSI_DIV_BULL");
                }
            }
        } catch (Throwable ignored) {}

        // ── 4. MACD histogram sign flip ─────────────────────
        // MACDResult returns only current value, so compute last 3 by slicing history.
        try {
            if (n >= 3) {
                com.bot.TradingCore.MACDResult macd1 = com.bot.TradingCore.macd(c15);
                com.bot.TradingCore.MACDResult macd2 = com.bot.TradingCore.macd(c15.subList(0, n - 1));
                com.bot.TradingCore.MACDResult macd3 = com.bot.TradingCore.macd(c15.subList(0, n - 2));
                double h1 = macd1.histogram;
                double h2 = macd2.histogram;
                double h3 = macd3.histogram;
                if (direction < 0 && h3 > 0 && h2 > 0 && h1 < h2 * 0.5) {
                    confluence++; flags.add("MACD_FLIP_DN");
                }
                if (direction > 0 && h3 < 0 && h2 < 0 && h1 > h2 * 0.5) {
                    confluence++; flags.add("MACD_FLIP_UP");
                }
            }
        } catch (Throwable ignored) {}

        // ── 5. Volume drying up ─────────────────────────────
        if (n >= 13) {
            double recent3 = (c15.get(n-1).volume + c15.get(n-2).volume + c15.get(n-3).volume) / 3.0;
            double prior10 = 0;
            for (int i = n-13; i < n-3; i++) prior10 += c15.get(i).volume;
            prior10 /= 10.0;
            if (prior10 > 0 && recent3 < prior10 * 0.70) {
                confluence++;
                flags.add("VOL_DRY");
            }
        }

        // ── Decision ────────────────────────────────────────
        // Minimum 3 confluences (out of 5) to call it a strong reversal.
        // Extended move is already counted as 1, so this effectively requires 2 more.
        if (confluence < 3) {
            return new StrongReversalResult(false, 0, 0, flags);
        }

        // Strength: 3 confluences = 0.55, 4 = 0.72, 5 = 0.88
        double strength = 0.40 + confluence * 0.10;
        strength = Math.min(0.92, strength);
        if (strength < 0.55) return new StrongReversalResult(false, 0, 0, flags);

        flags.add("STRONG_REV_" + (direction > 0 ? "UP" : "DN"));
        return new StrongReversalResult(true, direction, strength, flags);
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

    //  SMC: FVG + ORDER BLOCK

    private static final class FVGResult {
        final boolean detected, isBullish;
        final double gapLow, gapHigh;
        FVGResult(boolean d, boolean b, double lo, double hi) {
            detected = d; isBullish = b; gapLow = lo; gapHigh = hi;
        }
    }

    private FVGResult detectFVG(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 10) return new FVGResult(false, false, 0, 0);
        for (int i = c.size() - 3; i >= c.size() - 9 && i >= 2; i--) {
            com.bot.TradingCore.Candle c1 = c.get(i - 1), c2 = c.get(i), c3 = c.get(i + 1);
            double bs   = Math.abs(c2.close - c2.open);
            double atrL = atr(c.subList(Math.max(0, i - 14), i + 1), Math.min(14, i));
            if (atrL <= 0) continue;
            if (c2.close > c2.open && bs > atrL * 1.45) {
                double lo = c1.high, hi = c3.low;
                if (hi > lo) return new FVGResult(true, true, lo, hi);
            }
            if (c2.close < c2.open && bs > atrL * 1.45) {
                double hi = c1.low, lo = c3.high;
                if (hi > lo) return new FVGResult(true, false, lo, hi);
            }
        }
        return new FVGResult(false, false, 0, 0);
    }

    private static final class OrderBlockResult {
        final boolean detected, isBullish;
        final double zone;
        OrderBlockResult(boolean d, boolean b, double z) { detected = d; isBullish = b; zone = z; }
    }

    private OrderBlockResult detectOrderBlock(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 15) return new OrderBlockResult(false, false, 0);
        double atrL = atr(c, 14);
        for (int i = c.size() - 5; i >= c.size() - 13 && i >= 3; i--) {
            com.bot.TradingCore.Candle pot = c.get(i);
            double move = 0;
            for (int j = i + 1; j < Math.min(i + 5, c.size()); j++)
                move += c.get(j).close - c.get(j).open;
            if (pot.close < pot.open && move > atrL * 2.0)
                return new OrderBlockResult(true, true, pot.low);
            if (pot.close > pot.open && move < -atrL * 2.0)
                return new OrderBlockResult(true, false, pot.high);
        }
        return new OrderBlockResult(false, false, 0);
    }

    //  LIQUIDITY SWEEP

    public static boolean detectLiquiditySweep(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 6) return false;
        com.bot.TradingCore.Candle la = c.get(c.size() - 1);
        com.bot.TradingCore.Candle pr = c.get(c.size() - 2);
        double uw = la.high - Math.max(la.open, la.close);
        double lw = Math.min(la.open, la.close) - la.low;
        double bd = Math.abs(la.close - la.open) + 1e-10;
        return (uw > bd * 1.75 && la.close < pr.close) ||
                (lw > bd * 1.75 && la.close > pr.close);
    }


    //  COMPRESSION BREAKOUT

    private static final class CompressionResult {
        final boolean breakout; final int direction;
        CompressionResult(boolean b, int d) { breakout = b; direction = d; }
    }

    private CompressionResult detectCompression(List<com.bot.TradingCore.Candle> c15,
                                                List<com.bot.TradingCore.Candle> c1) {
        if (c15.size() < 30 || c1 == null || c1.size() < 10)
            return new CompressionResult(false, 0);
        double atrRecent = atr(c15.subList(c15.size() - 8, c15.size()), 7);
        double atrPast   = atr(c15.subList(c15.size() - 26, c15.size() - 10), 14);
        if (atrRecent >= atrPast * 0.52) return new CompressionResult(false, 0);
        double atr1 = atr(c1, Math.min(14, c1.size() - 1));
        int lk = Math.min(4, c1.size() - 1);
        double bk = last(c1).close - c1.get(c1.size() - 1 - lk).close;
        if (Math.abs(bk) > atr1 * 1.75)
            return new CompressionResult(true, bk > 0 ? 1 : -1);
        return new CompressionResult(false, 0);
    }

    //  EXHAUSTION CHECKS

    private boolean isLongExhausted(List<com.bot.TradingCore.Candle> c15,
                                    List<com.bot.TradingCore.Candle> c1h,
                                    double rsi14, double rsi7, double price) {
        if (rsi14 > 76 && rsi7 > 79) return true;
        double ema21 = ema(c15, 21);
        if ((price - ema21) / ema21 > 0.026 && rsi14 > 68) return true;
        if (c1h.size() >= 8 && rsi(c1h, 14) > 78) return true;
        if (c15.size() >= 6) {
            double b1 = Math.abs(c15.get(c15.size()-1).close - c15.get(c15.size()-1).open);
            double b2 = Math.abs(c15.get(c15.size()-2).close - c15.get(c15.size()-2).open);
            double b3 = Math.abs(c15.get(c15.size()-3).close - c15.get(c15.size()-3).open);
            if (b1 < b2 * 0.58 && b2 < b3 * 0.80) return true;
            double v1 = c15.get(c15.size()-1).volume;
            double v2 = c15.get(c15.size()-2).volume;
            double v3 = c15.get(c15.size()-3).volume;
            if (price > ema21 && v1 < v2 * 0.78 && v2 < v3 * 0.88) return true;
        }
        com.bot.TradingCore.Candle lc = last(c15);
        double uw = lc.high - Math.max(lc.open, lc.close);
        double bd = Math.abs(lc.close - lc.open) + 1e-10;
        if (uw > bd * 1.6 && lc.close < lc.open) return true;
        return false;
    }

    private boolean isShortExhausted(List<com.bot.TradingCore.Candle> c15,
                                     List<com.bot.TradingCore.Candle> c1h,
                                     double rsi14, double rsi7, double price) {
        if (rsi14 < 24 && rsi7 < 21) return true;
        double ema21 = ema(c15, 21);
        if ((ema21 - price) / ema21 > 0.026 && rsi14 < 32) return true;
        if (c1h.size() >= 8 && rsi(c1h, 14) < 22) return true;
        if (c15.size() >= 6) {
            double b1 = Math.abs(c15.get(c15.size()-1).close - c15.get(c15.size()-1).open);
            double b2 = Math.abs(c15.get(c15.size()-2).close - c15.get(c15.size()-2).open);
            double b3 = Math.abs(c15.get(c15.size()-3).close - c15.get(c15.size()-3).open);
            if (b1 < b2 * 0.58 && b2 < b3 * 0.80) return true;
            double v1 = c15.get(c15.size()-1).volume;
            double v2 = c15.get(c15.size()-2).volume;
            double v3 = c15.get(c15.size()-3).volume;
            if (price < ema21 && v1 < v2 * 0.78 && v2 < v3 * 0.88) return true;
        }
        com.bot.TradingCore.Candle lc = last(c15);
        double lw = Math.min(lc.open, lc.close) - lc.low;
        double bd = Math.abs(lc.close - lc.open) + 1e-10;
        if (lw > bd * 1.6 && lc.close > lc.open) return true;
        return false;
    }

    //  MARKET STATE + HTF BIAS

    private MarketState detectState(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 55) return MarketState.WEAK_TREND;
        double ema20 = ema(c, 20);
        double ema50 = ema(c, 50);
        int    n     = c.size();
        double slope = (ema20 - ema(c.subList(0, Math.max(1, n - 10)), 20)) / (c.get(0).close + 1e-9);
        double vol   = atr(c, 14) / (c.get(n - 1).close + 1e-9);
        if (Math.abs(slope) < 0.0005 || vol < 0.0015) return MarketState.RANGE;
        if ((ema20 > ema50 && slope > 0) || (ema20 < ema50 && slope < 0))
            return MarketState.STRONG_TREND;
        return MarketState.WEAK_TREND;
    }

    /**
     * detectBias1H REWRITE.
     *
     * PROBLEM: EMA50 vs EMA200 crossover on 1H takes 8-10 DAYS to flip.
     * During any altcoin correction (2-5 days), EMA50 stays below EMA200 everywhere.
     * Result: HTFBias.BEAR on 100% of pairs → zero LONG signals possible.
     *
     * FIX: Multi-factor adaptive bias combining:
     *   1. Fast EMA alignment (9 vs 21 — reacts in 1-2 days)
     *   2. Price position vs EMA50 (key institutional reference)
     *   3. RSI momentum zone (confirms direction vs exhaustion)
     *   4. Recent swing structure (higher highs/lows on 1H)
     *
     * Threshold: minimum 3 of 4 factors must agree to assign BULL/BEAR.
     * With only 1-2 factors → NONE (neutral = no bias penalty).
     * This prevents false BEAR lock that kills all LONG opportunities.
     */
    /**
     * Weighted HTFBias instead of vote-counting.
     * Old version: 4 binary votes, threshold 3/4. RSI=47 → bear; RSI=48 → NONE.
     * Single noisy factor could flip the bias.
     * New version: each factor contributes proportionally; sum compared to 3.0.
     * Plus margin requirement (winner must exceed loser by 0.5) prevents razor-edge flips.
     */
    private HTFBias detectBias1H(List<com.bot.TradingCore.Candle> c) {
        if (!valid(c)) return HTFBias.NONE;

        double bullWeight = 0;
        double bearWeight = 0;

        // Factor 1: Fast EMA alignment (EMA9 vs EMA21), up to 1.5
        double e9  = ema(c, 9);
        double e21 = ema(c, 21);
        double e50 = ema(c, 50);
        double emaRatio = (e9 - e21) / (e21 + 1e-9);
        if (emaRatio > 0.0005)       bullWeight += Math.min(1.5, emaRatio / 0.003 * 1.5);
        else if (emaRatio < -0.0005) bearWeight += Math.min(1.5, -emaRatio / 0.003 * 1.5);

        // Factor 2: Price vs EMA50 (institutional baseline), up to 1.5
        double price1h = last(c).close;
        double price50Ratio = (price1h - e50) / (e50 + 1e-9);
        if (price50Ratio > 0.001)       bullWeight += Math.min(1.5, price50Ratio / 0.01 * 1.5);
        else if (price50Ratio < -0.001) bearWeight += Math.min(1.5, -price50Ratio / 0.01 * 1.5);

        // Factor 3: RSI zone, up to 1.2 — smooth contribution
        double rsi1h = rsi(c, 14);
        if (rsi1h > 52)      bullWeight += Math.min(1.2, (rsi1h - 50) / 20.0 * 1.2);
        else if (rsi1h < 48) bearWeight += Math.min(1.2, (50 - rsi1h) / 20.0 * 1.2);

        // Factor 4: Recent swing structure, up to 1.3
        if (c.size() >= 20) {
            if (checkHH_HL(c))      bullWeight += 1.3;
            else if (checkLL_LH(c)) bearWeight += 1.3;
        }

        // Factor 5: Price slope over last 10 bars (early divergence capture), up to 0.8
        int n = c.size();
        if (n >= 15) {
            double priceChangePct = (c.get(n - 1).close - c.get(n - 10).close)
                    / (c.get(n - 10).close + 1e-9);
            if (priceChangePct > 0.008)       bullWeight += Math.min(0.8, priceChangePct / 0.03 * 0.8);
            else if (priceChangePct < -0.008) bearWeight += Math.min(0.8, -priceChangePct / 0.03 * 0.8);
        }

        // Threshold 3.0 out of max ~6.3, plus margin requirement
        if (bullWeight >= 3.0 && bullWeight > bearWeight + 0.5) return HTFBias.BULL;
        if (bearWeight >= 3.0 && bearWeight > bullWeight + 0.5) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    /**
     * detectBias2H uses EMA12/26/50 — already faster than 1H version,
     * but still biased to slow cross. Add RSI and swing structure confirmation.
     * Same 3/4 factors threshold to avoid BEAR lock during corrections.
     */
    /**
     * detectBias2H — same weighted approach as 1H.
     * Weights slightly higher since 2H is slower/more significant than 1H.
     */
    private HTFBias detectBias2H(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 30) return HTFBias.NONE;

        double ema12 = ema(c, 12);
        double ema26 = ema(c, 26);
        double ema50 = c.size() >= 50 ? ema(c, 50) : ema26;
        double price = last(c).close;

        double bullWeight = 0;
        double bearWeight = 0;

        // Factor 1: EMA alignment (up to 1.6)
        boolean bullEMA = ema12 > ema26 && ema26 > ema50 * 0.998;
        boolean bearEMA = ema12 < ema26 && ema26 < ema50 * 1.002;
        if (bullEMA) {
            double strength = (ema12 - ema26) / (ema26 + 1e-9);
            bullWeight += Math.min(1.6, strength / 0.005 * 1.6);
        } else if (bearEMA) {
            double strength = (ema26 - ema12) / (ema26 + 1e-9);
            bearWeight += Math.min(1.6, strength / 0.005 * 1.6);
        }

        // Factor 2: Price vs EMAs (up to 1.5)
        if (price > ema12 && price > ema26) {
            double r = (price - ema26) / (ema26 + 1e-9);
            bullWeight += Math.min(1.5, r / 0.015 * 1.5);
        } else if (price < ema12 && price < ema26) {
            double r = (ema26 - price) / (ema26 + 1e-9);
            bearWeight += Math.min(1.5, r / 0.015 * 1.5);
        }

        // Factor 3: RSI (up to 1.1)
        double rsi2h = rsi(c, 14);
        if (rsi2h > 52)      bullWeight += Math.min(1.1, (rsi2h - 50) / 20.0 * 1.1);
        else if (rsi2h < 48) bearWeight += Math.min(1.1, (50 - rsi2h) / 20.0 * 1.1);

        // Factor 4: Swing structure (up to 1.4)
        if (checkHH_HL(c))      bullWeight += 1.4;
        else if (checkLL_LH(c)) bearWeight += 1.4;

        // Factor 5: Price slope over last 15 bars (up to 0.9)
        int n = c.size();
        if (n >= 20) {
            double priceChangePct = (c.get(n - 1).close - c.get(n - 15).close)
                    / (c.get(n - 15).close + 1e-9);
            if (priceChangePct > 0.015)       bullWeight += Math.min(0.9, priceChangePct / 0.05 * 0.9);
            else if (priceChangePct < -0.015) bearWeight += Math.min(0.9, -priceChangePct / 0.05 * 0.9);
        }

        if (bullWeight >= 3.0 && bullWeight > bearWeight + 0.5) return HTFBias.BULL;
        if (bearWeight >= 3.0 && bearWeight > bullWeight + 0.5) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    private boolean checkHH_HL(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 15) return false;
        int n = c.size();
        double h1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.high).max().orElse(0);
        double h2 = c.subList(n-8, n).stream().mapToDouble(x->x.high).max().orElse(0);
        double l1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.low).min().orElse(0);
        double l2 = c.subList(n-8, n).stream().mapToDouble(x->x.low).min().orElse(0);
        return h2 > h1 && l2 > l1;
    }

    private boolean checkLL_LH(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 15) return false;
        int n = c.size();
        double h1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.high).max().orElse(0);
        double h2 = c.subList(n-8, n).stream().mapToDouble(x->x.high).max().orElse(0);
        double l1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.low).min().orElse(0);
        double l2 = c.subList(n-8, n).stream().mapToDouble(x->x.low).min().orElse(0);
        return h2 < h1 && l2 < l1;
    }

    private synchronized void adaptGlobalMinConf(MarketState state, double atr, double price) {
        double vol  = atr / (price + 1e-9);
        double base = BASE_CONF;
        if (state == MarketState.STRONG_TREND) base -= 3.0;
        else if (state == MarketState.RANGE)   base += 1.0;
        if (vol > 0.025)      base += 2.0;
        else if (vol > 0.018) base += 1.0;
        int utcHour = java.time.ZonedDateTime.now(java.time.ZoneId.of("UTC")).getHour();
        if (utcHour >= 8 && utcHour <= 12)       base -= 1.0;
        else if (utcHour >= 13 && utcHour <= 21)  base -= 1.5;
        globalMinConf.set(clamp(base, MIN_CONF_FLOOR, MIN_CONF_CEIL));
    }

    //  STRUCTURAL STOP PLACEMENT
    //  Finds nearest swing low (for LONG) or swing high (for SHORT)
    //  behind current price. SL goes below/above that level + buffer.

    private double findStructuralStop(List<com.bot.TradingCore.Candle> c15,
                                      com.bot.TradingCore.Side side,
                                      double price, double atr14) {
        if (c15.size() < 20) return -1;

        double buffer = atr14 * 0.25;
        int n = c15.size();

        // VOLUME-PROFILE STOP: find the nearest high-volume zone behind price.
        // Market makers defend volume nodes — placing SL behind one means
        // price must break through institutional defence to hit your stop.
        //
        // Algorithm: bucket last 30 bars into price zones, find zone with most volume.
        // SL goes behind the nearest high-volume zone below (LONG) or above (SHORT) price.
        double volProfileStop = findVolumeProfileStop(c15, side, price, atr14);

        if (side == com.bot.TradingCore.Side.LONG) {
            // 1. Try volume profile stop first (most reliable)
            if (volProfileStop > 0 && volProfileStop < price) {
                return volProfileStop - buffer;
            }
            // 2. Swing low fallback
            List<Integer> lows = swingLows(c15, 3);
            double bestStop = -1;
            for (int idx = lows.size() - 1; idx >= 0 && idx >= lows.size() - 6; idx--) {
                double swLow = c15.get(lows.get(idx)).low;
                if (swLow < price && swLow > price * 0.93) {
                    bestStop = swLow - buffer;
                    break;
                }
            }
            // 3. Recent 10-bar low fallback (was 8)
            if (bestStop <= 0) {
                double recentLow = Double.MAX_VALUE;
                for (int i = Math.max(0, n - 10); i < n - 1; i++) {
                    recentLow = Math.min(recentLow, c15.get(i).low);
                }
                if (recentLow < price && recentLow > price * 0.93) {
                    bestStop = recentLow - buffer;
                }
            }
            return bestStop > 0 ? bestStop : -1;
        } else {
            // SHORT side
            if (volProfileStop > 0 && volProfileStop > price) {
                return volProfileStop + buffer;
            }
            List<Integer> highs = swingHighs(c15, 3);
            double bestStop = -1;
            for (int idx = highs.size() - 1; idx >= 0 && idx >= highs.size() - 6; idx--) {
                double swHigh = c15.get(highs.get(idx)).high;
                if (swHigh > price && swHigh < price * 1.07) {
                    bestStop = swHigh + buffer;
                    break;
                }
            }
            if (bestStop <= 0) {
                double recentHigh = Double.NEGATIVE_INFINITY;
                for (int i = Math.max(0, n - 10); i < n - 1; i++) {
                    recentHigh = Math.max(recentHigh, c15.get(i).high);
                }
                if (recentHigh > price && recentHigh < price * 1.07) {
                    bestStop = recentHigh + buffer;
                }
            }
            return bestStop > 0 ? bestStop : -1;
        }
    }

    /**
     * VOLUME PROFILE STOP — find nearest high-volume price zone.
     * Divides recent price range into 20 buckets, counts volume per bucket.
     * Returns the center of the nearest high-volume bucket behind price.
     * High-volume = top 30% by volume. "Behind" = below for LONG, above for SHORT.
     */
    private double findVolumeProfileStop(List<com.bot.TradingCore.Candle> c15,
                                         com.bot.TradingCore.Side side,
                                         double price, double atr14) {
        int n = c15.size();
        int lookback = Math.min(30, n - 1);
        if (lookback < 15) return -1;

        // Find price range over lookback period
        double hi = Double.NEGATIVE_INFINITY, lo = Double.POSITIVE_INFINITY;
        for (int i = n - lookback; i < n; i++) {
            hi = Math.max(hi, c15.get(i).high);
            lo = Math.min(lo, c15.get(i).low);
        }
        double range = hi - lo;
        if (range < atr14 * 0.5) return -1; // too compressed

        int BUCKETS = 20;
        double bucketSize = range / BUCKETS;
        double[] bucketVol = new double[BUCKETS];

        // Distribute volume into buckets
        for (int i = n - lookback; i < n; i++) {
            com.bot.TradingCore.Candle c = c15.get(i);
            double mid = (c.high + c.low) / 2.0;
            int bucket = (int) Math.min(BUCKETS - 1, Math.max(0, (mid - lo) / bucketSize));
            bucketVol[bucket] += c.volume;
        }

        // Find volume threshold (top 30%)
        double[] sorted = bucketVol.clone();
        java.util.Arrays.sort(sorted);
        double threshold = sorted[(int) (BUCKETS * 0.70)];

        // Find nearest high-volume zone behind price
        if (side == com.bot.TradingCore.Side.LONG) {
            // Look for high-volume zone BELOW price
            for (int b = Math.min(BUCKETS - 1, (int) ((price - lo) / bucketSize)) - 1; b >= 0; b--) {
                if (bucketVol[b] >= threshold) {
                    double zoneCenter = lo + (b + 0.5) * bucketSize;
                    if (zoneCenter < price && zoneCenter > price - atr14 * 4) {
                        return zoneCenter;
                    }
                }
            }
        } else {
            // Look for high-volume zone ABOVE price
            for (int b = Math.max(0, (int) ((price - lo) / bucketSize)) + 1; b < BUCKETS; b++) {
                if (bucketVol[b] >= threshold) {
                    double zoneCenter = lo + (b + 0.5) * bucketSize;
                    if (zoneCenter > price && zoneCenter < price + atr14 * 4) {
                        return zoneCenter;
                    }
                }
            }
        }
        return -1;
    }

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

    /**
     * Classic Bullish RSI Divergence — price makes lower low, RSI makes higher low.
     *
     * Improvement over old fixed n-8 approach:
     *   OLD: always compared bar n-8 vs bar n-1. If the actual trough was at n-5 or n-12,
     *        the comparison was meaningless — could fire when there's no real divergence,
     *        or miss a genuine one because it was at a different bar.
     *   NEW: scans backward to find the actual LOWEST price bar within the lookback window,
     *        then compares RSI at that exact bar vs current RSI.
     *        Uses TradingCore.rsiSeries() for accurate RSI at arbitrary bars (no warm-up drift).
     *        Lookback: 6-20 bars — broad enough to catch formations, narrow enough to be timely.
     */
    private boolean bullDiv(List<com.bot.TradingCore.Candle> c) {
        int n = c.size();
        if (n < 28) return false;

        double[] rsiArr = com.bot.TradingCore.rsiSeries(c, 14);
        double rsiNow = rsiArr[n - 1];

        // Find the bar with the lowest price low in the lookback window [n-20, n-4]
        int lookback = Math.min(20, n - 6);
        int troughIdx = -1;
        double troughLow = Double.MAX_VALUE;
        for (int i = n - 2; i >= n - lookback; i--) {
            if (c.get(i).low < troughLow) { troughLow = c.get(i).low; troughIdx = i; }
        }
        if (troughIdx < 0 || troughIdx > n - 5) return false;

        double rsiAtTrough = rsiArr[troughIdx];
        boolean priceNewLow  = c.get(n - 1).low < troughLow * 0.9985; // current bar made new low
        boolean rsiHigherLow = rsiNow > rsiAtTrough + 3.5;            // RSI didn't confirm new low
        boolean rsiNotOverbought = rsiNow < 55;                         // must still be below neutral

        return priceNewLow && rsiHigherLow && rsiNotOverbought;
    }

    /**
     * Classic Bearish RSI Divergence — price makes higher high, RSI makes lower high.
     * Same approach as bullDiv but inverted for top detection.
     */
    private boolean bearDiv(List<com.bot.TradingCore.Candle> c) {
        int n = c.size();
        if (n < 28) return false;

        double[] rsiArr = com.bot.TradingCore.rsiSeries(c, 14);
        double rsiNow = rsiArr[n - 1];

        // Find the bar with the highest price high in the lookback window [n-20, n-4]
        int lookback = Math.min(20, n - 6);
        int peakIdx = -1;
        double peakHigh = Double.NEGATIVE_INFINITY;
        for (int i = n - 2; i >= n - lookback; i--) {
            if (c.get(i).high > peakHigh) { peakHigh = c.get(i).high; peakIdx = i; }
        }
        if (peakIdx < 0 || peakIdx > n - 5) return false;

        double rsiAtPeak = rsiArr[peakIdx];
        boolean priceNewHigh  = c.get(n - 1).high > peakHigh * 1.0015; // current bar made new high
        boolean rsiLowerHigh  = rsiNow < rsiAtPeak - 3.5;               // RSI didn't confirm new high
        boolean rsiNotOversold = rsiNow > 45;                             // must still be above neutral

        return priceNewHigh && rsiLowerHigh && rsiNotOversold;
    }

    /**
     * Hidden Bullish Divergence — price makes higher low, RSI makes lower low.
     * This is a CONTINUATION signal in an uptrend (NOT a reversal signal).
     * Meaning: price pulled back less than RSI suggests → underlying buying pressure is strong.
     * Very high win rate when detected in the context of a confirmed uptrend.
     * Used to BOOST confidence on LONG signals during healthy pullbacks.
     */
    private boolean hiddenBullDiv(List<com.bot.TradingCore.Candle> c) {
        int n = c.size();
        if (n < 28) return false;

        double[] rsiArr = com.bot.TradingCore.rsiSeries(c, 14);
        double rsiNow = rsiArr[n - 1];

        int lookback = Math.min(18, n - 6);
        int troughIdx = -1;
        double troughLow = Double.MAX_VALUE;
        for (int i = n - 2; i >= n - lookback; i--) {
            if (c.get(i).low < troughLow) { troughLow = c.get(i).low; troughIdx = i; }
        }
        if (troughIdx < 0 || troughIdx > n - 4) return false;

        double rsiAtTrough = rsiArr[troughIdx];
        // Hidden bull: price HIGHER low (price didn't go as low as before)
        //              RSI LOWER low (RSI went lower than before = oversold-looking)
        boolean priceHigherLow = c.get(n - 1).low > troughLow * 1.002;
        boolean rsiLowerLow    = rsiNow < rsiAtTrough - 3.0;
        boolean rsiInRange     = rsiNow > 28 && rsiNow < 52; // must be in pullback zone

        return priceHigherLow && rsiLowerLow && rsiInRange;
    }

    /**
     * Hidden Bearish Divergence — price makes lower high, RSI makes higher high.
     * Continuation signal in a downtrend. Price bouncing less than RSI suggests.
     * Used to BOOST confidence on SHORT signals during dead-cat bounces.
     */
    private boolean hiddenBearDiv(List<com.bot.TradingCore.Candle> c) {
        int n = c.size();
        if (n < 28) return false;

        double[] rsiArr = com.bot.TradingCore.rsiSeries(c, 14);
        double rsiNow = rsiArr[n - 1];

        int lookback = Math.min(18, n - 6);
        int peakIdx = -1;
        double peakHigh = Double.NEGATIVE_INFINITY;
        for (int i = n - 2; i >= n - lookback; i--) {
            if (c.get(i).high > peakHigh) { peakHigh = c.get(i).high; peakIdx = i; }
        }
        if (peakIdx < 0 || peakIdx > n - 4) return false;

        double rsiAtPeak = rsiArr[peakIdx];
        // Hidden bear: price LOWER high (bounce didn't reach previous high)
        //              RSI HIGHER high (RSI overbought-looking on the bounce)
        boolean priceLowerHigh = c.get(n - 1).high < peakHigh * 0.998;
        boolean rsiHigherHigh  = rsiNow > rsiAtPeak + 3.0;
        boolean rsiInRange     = rsiNow > 48 && rsiNow < 72; // bounce zone, not extreme

        return priceLowerHigh && rsiHigherHigh && rsiInRange;
    }

    public boolean impulse(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 15) return false;
        return Math.abs(last(c).close - c.get(c.size() - 5).close) > atr(c, 14) * 0.55;
    }

    public boolean volumeSpike(List<com.bot.TradingCore.Candle> c, CoinCategory cat) {
        if (c.size() < 10) return false;
        double avg = c.subList(c.size() - 10, c.size() - 1)
                .stream().mapToDouble(cd -> cd.volume).average().orElse(1);
        double thr = cat == CoinCategory.MEME ? 1.25 : cat == CoinCategory.ALT ? 1.20 : 1.15;
        return last(c).volume / avg > thr;
    }

    /**
     * pullback — tightened RSI ranges.
     * OLD LONG: RSI 30-65 — allowed entry at RSI 64 = nearly overbought = terrible timing.
     * NEW LONG: RSI 32-55 — only enter on genuine pullback, not mid-rally.
     * OLD SHORT: RSI 35-70 — allowed entry at RSI 36 = nearly oversold.
     * NEW SHORT: RSI 45-68 — only enter near distribution zone.
     */
    private boolean pullback(List<com.bot.TradingCore.Candle> c, boolean bull) {
        double e21 = ema(c, 21), p = last(c).close, r = rsi(c, 14);
        return bull
                ? p <= e21 * 1.003 && p >= e21 * 0.988 && r > 32 && r < 55  // tighter: was r<65
                : p >= e21 * 0.997 && p <= e21 * 1.012 && r < 68 && r > 45;  // tighter: was r>35
    }

    private boolean bullishStructure(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 12) return false;
        return c.get(c.size()-4).high > c.get(c.size()-8).high &&
                c.get(c.size()-4).low  > c.get(c.size()-8).low;
    }

    private boolean bearishStructure(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 12) return false;
        return c.get(c.size()-4).high < c.get(c.size()-8).high &&
                c.get(c.size()-4).low  < c.get(c.size()-8).low;
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

    // [v42.0 FIX #1, #3, #4, #7]  ProbabilityCalibrator — static nested class
    //
    // Replaces 24 hardcoded `Math.max(50, ...)` floors with empirical isotonic
    // calibration. Maps raw model score → real win-rate via Pool of Adjacent
    // Violators (PAV) regression, the standard non-parametric calibrator
    // (Zadrozny-Elkan, Niculescu-Mizil 2005).
    //
    // BUCKETED BY VOLATILITY (FIX #7): one calibration per (symbol, vol-bucket).
    // [v79.0 INTEGRITY OVERHAUL]  ProbabilityCalibrator — FULL REWRITE.
    //
    // BEFORE (v78):
    //   - PAV isotonic regression on (symbol, vol-bucket) keys
    //   - persistent state in calibrator.csv
    //   - silently lost AMBIGUOUS / TIME_STOP outcomes upstream
    //   - no integrity proof — file could be tampered with
    //
    // AFTER (v79):
    //   - Same PAV core (mathematically sound)
    //   - PLUS: regime bucket segmentation (TREND_UP/TREND_DOWN/CHOPPY/NEUTRAL)
    //   - PLUS: outcome tags (TP1/SL/AMBIGUOUS/TIME_STOP/MOVED_UP/MOVED_DOWN)
    //   - PLUS: weighted outcomes (AMBIGUOUS = 0.5 weight, others = 1.0)
    //   - PLUS: HMAC-SHA256 signature on every saved row
    //   - PLUS: append-only audit log with chained hashes (tamper-evident)
    //   - PLUS: writeDispatchAudit() captures every sent signal pre-outcome
    //   - PLUS: verifyAuditIntegrity() — public method for third-party check
    public static final class ProbabilityCalibrator {

        private static final int  MIN_SAMPLES = 50;
        private static final int  WINDOW      = 500;
        private static final int  BUCKETS     = 5;
        private static final long MAX_AGE_MS  = 30L * 24 * 60 * 60 * 1000L;

        // [v79 I3] HMAC key + audit log. Set CALIBRATOR_HMAC_KEY in env on prod.
        private static final String HMAC_KEY = resolveHmacKey();
        private static final String AUDIT_LOG_PATH = System.getenv()
                .getOrDefault("CALIBRATOR_AUDIT_LOG", "./data/calibrator_audit.log");

        private static String resolveHmacKey() {
            String k = System.getenv("CALIBRATOR_HMAC_KEY");
            if (k == null || k.isBlank()) {
                LOG.warning("[Calibrator] CALIBRATOR_HMAC_KEY not set — using default. "
                        + "FOR PRODUCTION: set unique key in Railway env vars.");
                return "default-CHANGE-ME-CALIBRATOR-KEY-2026";
            }
            return k;
        }

        public enum VolBucket { LOW, MID, HIGH;
            public static VolBucket of(double atrPct) {
                if (atrPct < 0.5) return LOW;
                if (atrPct < 1.5) return MID;
                return HIGH;
            }
        }

        // [v79 SEGMENT] Regime bucket — отдельный от vol bucket. Полная segmentation:
        // (symbol, vol_bucket, regime_bucket). Калибратор учится отдельно для трендового
        // и хаотичного рынка — раньше валил всё в одну кучу.
        //
        // ВАЖНО: вместо enum — строковые константы. Это сделано чтобы не плодить
        // вложенные классы (enum в Java = class). Семантика та же, type-safety
        // обеспечивается через regimeBucketOf() (всегда возвращает одно из 4 значений)
        // и valid set check в recordOutcomeExtended.
        private static final String REGIME_TREND_UP   = "TREND_UP";
        private static final String REGIME_TREND_DOWN = "TREND_DOWN";
        private static final String REGIME_CHOPPY     = "CHOPPY";
        private static final String REGIME_NEUTRAL    = "NEUTRAL";
        private static final java.util.Set<String> VALID_REGIMES = java.util.Set.of(
                REGIME_TREND_UP, REGIME_TREND_DOWN, REGIME_CHOPPY, REGIME_NEUTRAL);

        private static String regimeBucketOf(String btcRegime) {
            if (btcRegime == null) return REGIME_NEUTRAL;
            String r = btcRegime.toUpperCase();
            if (r.contains("STRONG_UP") || r.contains("IMPULSE_UP")) return REGIME_TREND_UP;
            if (r.contains("STRONG_DOWN") || r.contains("IMPULSE_DOWN")
                    || r.contains("CRASH") || r.contains("PANIC")) return REGIME_TREND_DOWN;
            if (r.contains("CHOPPY")) return REGIME_CHOPPY;
            return REGIME_NEUTRAL;
        }

        // [v79] Outcome tags — нужны для аудита и для weighted recording.
        // Backwards-compat: при загрузке старого calibrator.csv (без tag) дефолт = "LEGACY".
        // Опять же — без enum: просто строковые константы и Set валидации.
        private static final String TAG_TP1        = "TP1";
        private static final String TAG_SL         = "SL";
        private static final String TAG_AMBIGUOUS  = "AMBIGUOUS";
        private static final String TAG_TIME_STOP  = "TIME_STOP";
        private static final String TAG_MOVED_UP   = "MOVED_UP";
        private static final String TAG_MOVED_DOWN = "MOVED_DOWN";
        private static final String TAG_FLAT       = "FLAT";
        private static final String TAG_LEGACY     = "LEGACY";
        private static final java.util.Set<String> VALID_TAGS = java.util.Set.of(
                TAG_TP1, TAG_SL, TAG_AMBIGUOUS, TAG_TIME_STOP,
                TAG_MOVED_UP, TAG_MOVED_DOWN, TAG_FLAT, TAG_LEGACY);

        private static String sanitizeTag(String t) {
            if (t == null) return TAG_LEGACY;
            String u = t.toUpperCase();
            return VALID_TAGS.contains(u) ? u : TAG_LEGACY;
        }
        private static String sanitizeRegime(String r) {
            if (r == null) return REGIME_NEUTRAL;
            String u = r.toUpperCase();
            return VALID_REGIMES.contains(u) ? u : REGIME_NEUTRAL;
        }

        // [v79] Outcome — это УЖЕ существующий nested class в оригинальном коде.
        // Мы НЕ создаём новый, а только ДОБАВЛЯЕМ поля weight/tag/regime
        // (тип String для tag/regime — без новых enum-классов).
        private static final class Outcome {
            final double rawScore;
            final boolean hit;
            final double weight;       // 1.0 = full, 0.5 = AMBIGUOUS
            final String tag;          // one of TAG_* constants
            final String regime;       // one of REGIME_* constants
            long ts;

            Outcome(double s, boolean h) {
                this(s, h, 1.0, TAG_LEGACY, REGIME_NEUTRAL);
            }
            Outcome(double s, boolean h, double w, String t, String r) {
                this.rawScore = Math.max(0.0, Math.min(1.0, s));
                this.hit = h;
                this.weight = Math.max(0.0, Math.min(1.0, w));
                this.tag = sanitizeTag(t);
                this.regime = sanitizeRegime(r);
                this.ts = System.currentTimeMillis();
            }
        }

        // History map. Key = symbol#vol_bucket#regime_bucket (was just symbol#vol).
        // Backwards-compat fallback: на read используем 3-уровневую иерархию:
        //   1. exact match (sym + vol + regime)
        //   2. sym + vol (any regime)
        //   3. global vol bucket (any sym, any regime)
        //   4. global all (last resort)
        private final java.util.concurrent.ConcurrentHashMap<String,
                java.util.concurrent.ConcurrentLinkedDeque<Outcome>> history
                = new java.util.concurrent.ConcurrentHashMap<>();

        // [v79 I3] Audit log state. We chain hashes: each record contains the
        // previous record's HMAC, making tamper-detection trivial. Memory-only
        // counter so we know expected next sequence number on read-back.
        private final java.util.concurrent.atomic.AtomicLong auditSeq = new java.util.concurrent.atomic.AtomicLong(0);
        private volatile String lastAuditHmac = "";  // chain previous hash

        private static String key(String symbol, VolBucket b) {
            return symbol + "#" + b.name();
        }
        private static String key3(String symbol, VolBucket b, String regime) {
            return symbol + "#" + b.name() + "#" + sanitizeRegime(regime);
        }

        // ─────────────────────────────────────────────────────────────────
        //  RECORD API — backwards-compat overloads + new extended one.
        // ─────────────────────────────────────────────────────────────────

        public void recordOutcome(String symbol, double rawScore, boolean hit, double atrPct) {
            recordOutcomeExtended(symbol, rawScore, hit, atrPct,
                    1.0, hit ? TAG_TP1 : TAG_SL, REGIME_NEUTRAL, 0.0, 0.0);
        }

        public void recordOutcome(String symbol, double rawScore, boolean hit) {
            recordOutcomeExtended(symbol, rawScore, hit, 1.0,
                    1.0, hit ? TAG_TP1 : TAG_SL, REGIME_NEUTRAL, 0.0, 0.0);
        }

        // [v79 I3] FULL extended record. BotMain.checkForecastAccuracy calls this.
        public void recordOutcomeExtended(String symbol, double rawScore, boolean hit,
                                          double atrPct, double weight,
                                          String outcomeTag, String btcRegime,
                                          double entryPrice, double currentPrice) {
            if (symbol == null) return;
            VolBucket vb = VolBucket.of(atrPct);
            String rb  = regimeBucketOf(btcRegime);
            String tag = sanitizeTag(outcomeTag);

            String k = key3(symbol, vb, rb);
            java.util.concurrent.ConcurrentLinkedDeque<Outcome> dq =
                    history.computeIfAbsent(k, x -> new java.util.concurrent.ConcurrentLinkedDeque<>());
            Outcome o = new Outcome(rawScore, hit, weight, tag, rb);
            dq.addLast(o);
            while (dq.size() > WINDOW) dq.pollFirst();

            // [v79 I3] Audit log entry — HMAC chained.
            try { writeOutcomeAudit(symbol, rawScore, hit, atrPct, weight, tag, rb,
                    entryPrice, currentPrice); } catch (Throwable ignored) {}
        }

        // ─────────────────────────────────────────────────────────────────
        //  CALIBRATE — same PAV math, broader fallback chain.
        // ─────────────────────────────────────────────────────────────────

        public double calibrate(String symbol, double rawScore) {
            return calibrate(symbol, rawScore, 1.0);
        }

        /** Backwards-compat 3-arg. Uses NEUTRAL regime (fine for warm-start). */
        public double calibrate(String symbol, double rawScore, double atrPct) {
            return calibrate(symbol, rawScore, atrPct, "NEUTRAL");
        }

        /** [v79] FULL calibrate with regime context. DecisionEngineMerged should call this. */
        public double calibrate(String symbol, double rawScore, double atrPct, String btcRegime) {
            double r = clamp01(rawScore);
            if (symbol == null) return r;

            VolBucket vb = VolBucket.of(atrPct);
            String rb = regimeBucketOf(btcRegime);

            java.util.List<Outcome> snap = collect3(symbol, vb, rb);
            boolean usingFallback = false;

            // Tiered fallback chain (broader at each step).
            if (snap.size() < MIN_SAMPLES) {
                snap = collect2(symbol, vb);             // any regime
                usingFallback = true;
            }
            if (snap.size() < MIN_SAMPLES) {
                snap = collectAllForSymbol(symbol);      // any vol, any regime
                usingFallback = true;
            }
            if (snap.size() < MIN_SAMPLES) {
                snap = collectGlobalBucket(vb);          // any sym, this vol
                usingFallback = true;
            }
            if (snap.size() < MIN_SAMPLES) {
                snap = new java.util.ArrayList<>();
                history.values().forEach(snap::addAll);  // ultimate fallback
                usingFallback = true;
            }
            if (snap.size() < MIN_SAMPLES) return r;

            long now = System.currentTimeMillis();
            snap.removeIf(o -> now - o.ts > MAX_AGE_MS);
            if (snap.size() < MIN_SAMPLES) return r;

            // [v79 I1] WEIGHTED PAV — AMBIGUOUS outcomes have weight=0.5, so
            // bucket means use weight-aware averaging instead of simple count.
            snap.sort(java.util.Comparator.comparingDouble(o -> o.rawScore));

            int n = snap.size();
            int bucketSize = Math.max(1, n / BUCKETS);
            double[] x = new double[BUCKETS];
            double[] y = new double[BUCKETS];
            double[] w = new double[BUCKETS];

            for (int bi = 0; bi < BUCKETS; bi++) {
                int from = bi * bucketSize;
                int to = (bi == BUCKETS - 1) ? n : Math.min(n, from + bucketSize);
                if (from >= to) continue;
                double sx = 0, sy = 0, sw = 0;
                for (int i = from; i < to; i++) {
                    Outcome o = snap.get(i);
                    sx += o.rawScore * o.weight;
                    sy += (o.hit ? 1.0 : 0.0) * o.weight;
                    sw += o.weight;
                }
                if (sw < 1e-9) sw = 1e-9;
                x[bi] = sx / sw;
                y[bi] = sy / sw;
                w[bi] = sw;
            }

            pav(y, w);

            double zetaBase = (double) n / (n + 100.0);
            double zeta = usingFallback ? zetaBase * 0.5 : zetaBase;

            double calibrated;
            if (r <= x[0]) {
                calibrated = y[0];
            } else if (r >= x[BUCKETS - 1]) {
                calibrated = y[BUCKETS - 1];
            } else {
                calibrated = r;
                for (int i = 0; i < BUCKETS - 1; i++) {
                    if (r >= x[i] && r <= x[i + 1]) {
                        double dx = x[i + 1] - x[i];
                        if (dx < 1e-9) { calibrated = y[i]; break; }
                        double t = (r - x[i]) / dx;
                        calibrated = y[i] + t * (y[i + 1] - y[i]);
                        break;
                    }
                }
            }

            return clamp01((1.0 - zeta) * r + zeta * calibrated);
        }

        // ─────────────────────────────────────────────────────────────────
        //  COLLECT helpers — multi-level fallback for sparse data.
        // ─────────────────────────────────────────────────────────────────

        private java.util.List<Outcome> collect3(String symbol, VolBucket vb, String rb) {
            java.util.concurrent.ConcurrentLinkedDeque<Outcome> dq =
                    history.get(key3(symbol, vb, rb));
            if (dq == null) return new java.util.ArrayList<>();
            return new java.util.ArrayList<>(dq);
        }

        private java.util.List<Outcome> collect2(String symbol, VolBucket vb) {
            java.util.List<Outcome> out = new java.util.ArrayList<>();
            String prefix = symbol + "#" + vb.name() + "#";
            for (var entry : history.entrySet()) {
                if (entry.getKey().startsWith(prefix)) out.addAll(entry.getValue());
            }
            // Also old format key (sym#vol) for backwards-compat with v78 data.
            java.util.concurrent.ConcurrentLinkedDeque<Outcome> oldFmt = history.get(key(symbol, vb));
            if (oldFmt != null) out.addAll(oldFmt);
            return out;
        }

        private java.util.List<Outcome> collectAllForSymbol(String symbol) {
            java.util.List<Outcome> out = new java.util.ArrayList<>();
            String prefix = symbol + "#";
            for (var entry : history.entrySet()) {
                if (entry.getKey().startsWith(prefix)) out.addAll(entry.getValue());
            }
            return out;
        }

        private java.util.List<Outcome> collectGlobalBucket(VolBucket b) {
            java.util.List<Outcome> out = new java.util.ArrayList<>();
            String volTag = "#" + b.name();
            for (var entry : history.entrySet()) {
                if (entry.getKey().contains(volTag)) out.addAll(entry.getValue());
            }
            return out;
        }

        // ─────────────────────────────────────────────────────────────────
        //  PAV in-place isotonic regression (unchanged from v78 — it's correct).
        // ─────────────────────────────────────────────────────────────────

        private static void pav(double[] y, double[] w) {
            int n = y.length;
            if (n <= 1) return;
            double[] yy = y.clone();
            double[] ww = w.clone();
            int[] sz = new int[n];
            java.util.Arrays.fill(sz, 1);
            int len = n;

            boolean changed = true;
            while (changed) {
                changed = false;
                for (int i = 0; i < len - 1; i++) {
                    if (yy[i] > yy[i + 1] + 1e-12) {
                        double tw = ww[i] + ww[i + 1];
                        yy[i] = (yy[i] * ww[i] + yy[i + 1] * ww[i + 1]) / Math.max(1e-9, tw);
                        ww[i] = tw;
                        sz[i] += sz[i + 1];
                        for (int j = i + 1; j < len - 1; j++) {
                            yy[j] = yy[j + 1]; ww[j] = ww[j + 1]; sz[j] = sz[j + 1];
                        }
                        len--;
                        changed = true;
                        break;
                    }
                }
            }
            int idx = 0;
            for (int i = 0; i < len && idx < n; i++) {
                for (int k = 0; k < sz[i] && idx < n; k++) {
                    y[idx++] = yy[i];
                }
            }
        }

        private static double clamp01(double v) {
            if (Double.isNaN(v)) return 0.0;
            return Math.max(0.0, Math.min(1.0, v));
        }

        public int sampleCount(String symbol) {
            int n = 0;
            for (var entry : history.entrySet()) {
                if (entry.getKey().startsWith(symbol + "#")) {
                    n += entry.getValue().size();
                }
            }
            return n;
        }

        public double rawWinRate(String symbol) {
            java.util.List<Outcome> all = collectAllForSymbol(symbol);
            if (all.isEmpty()) return Double.NaN;
            double hits = 0, total = 0;
            for (Outcome o : all) {
                total += o.weight;
                if (o.hit) hits += o.weight;
            }
            return total > 0 ? hits / total : Double.NaN;
        }

        public void reset(String symbol) {
            history.entrySet().removeIf(e -> e.getKey().startsWith(symbol + "#"));
        }

        public void resetAll() {
            history.clear();
            lastAuditHmac = "";
            auditSeq.set(0);
        }

        // ─────────────────────────────────────────────────────────────────
        //  PERSISTENT STATE — save/load with HMAC integrity.
        // ─────────────────────────────────────────────────────────────────

        public synchronized void saveToFile(String path) {
            try {
                java.io.File f = new java.io.File(path);
                java.io.File parent = f.getParentFile();
                if (parent != null && !parent.exists()) parent.mkdirs();

                try (java.io.PrintWriter pw = new java.io.PrintWriter(
                        new java.io.BufferedWriter(new java.io.FileWriter(f)))) {
                    pw.println("# ProbabilityCalibrator state v2 (HMAC-signed)");
                    pw.println("# format: key;rawScore;hit;weight;tag;regime;ts;hmac");
                    long now = System.currentTimeMillis();
                    for (java.util.Map.Entry<String, java.util.concurrent.ConcurrentLinkedDeque<Outcome>> e
                            : history.entrySet()) {
                        for (Outcome o : e.getValue()) {
                            if (now - o.ts > MAX_AGE_MS) continue;
                            String payload = String.format("%s;%.6f;%d;%.3f;%s;%s;%d",
                                    e.getKey(), o.rawScore, o.hit ? 1 : 0,
                                    o.weight, o.tag, o.regime, o.ts);
                            String hmac = hmacSha256(payload, HMAC_KEY);
                            pw.println(payload + ";" + hmac);
                        }
                    }
                }
            } catch (Exception ex) {
                LOG.warning("[Calibrator] save failed: " + ex.getMessage());
            }
        }

        public synchronized void loadFromFile(String path) {
            // [v79 FIX I3] Restore audit chain state before opening calibrator file,
            // so the next audit-log entry continues the chain correctly across restarts.
            // Without this, post-restart entries write prevHmac="" while the file
            // already has a non-empty tail hmac → verifyAuditIntegrity() falsely
            // reports every post-restart row as TAMPERED.
            initAuditState();
            try {
                java.io.File f = new java.io.File(path);
                if (!f.exists()) return;

                long now = System.currentTimeMillis();
                int loaded = 0, skipped = 0, tampered = 0, legacy = 0;
                try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.startsWith("#") || line.isBlank()) continue;
                        String[] p = line.split(";");

                        try {
                            // Detect format: v1 (4 fields) vs v2 (8 fields).
                            if (p.length == 4) {
                                // Legacy v1 format — load with default tag/weight/regime.
                                String k = p[0];
                                double score = Double.parseDouble(p[1]);
                                boolean hit = "1".equals(p[2]);
                                long ts = Long.parseLong(p[3]);
                                if (now - ts > MAX_AGE_MS) { skipped++; continue; }
                                Outcome o = new Outcome(score, hit, 1.0,
                                        TAG_LEGACY, REGIME_NEUTRAL);
                                o.ts = ts;
                                history.computeIfAbsent(k, x ->
                                        new java.util.concurrent.ConcurrentLinkedDeque<>()).addLast(o);
                                loaded++; legacy++;
                            } else if (p.length == 8) {
                                // v2 format with HMAC.
                                String k = p[0];
                                double score = Double.parseDouble(p[1]);
                                boolean hit = "1".equals(p[2]);
                                double weight = Double.parseDouble(p[3]);
                                String tag = sanitizeTag(p[4]);
                                String regime = sanitizeRegime(p[5]);
                                long ts = Long.parseLong(p[6]);
                                String storedHmac = p[7];

                                String payload = String.format("%s;%.6f;%d;%.3f;%s;%s;%d",
                                        k, score, hit ? 1 : 0, weight, tag, regime, ts);
                                String calc = hmacSha256(payload, HMAC_KEY);
                                if (!calc.equals(storedHmac)) {
                                    tampered++;
                                    LOG.warning("[Calibrator] HMAC mismatch on row, skipping: " + k);
                                    continue;
                                }
                                if (now - ts > MAX_AGE_MS) { skipped++; continue; }

                                Outcome o = new Outcome(score, hit, weight, tag, regime);
                                o.ts = ts;
                                history.computeIfAbsent(k, x ->
                                        new java.util.concurrent.ConcurrentLinkedDeque<>()).addLast(o);
                                loaded++;
                            } else {
                                skipped++;
                            }
                        } catch (Exception parseEx) { skipped++; }
                    }
                }
                LOG.info("[Calibrator] loaded " + loaded + " (legacy=" + legacy
                        + " tampered=" + tampered + " skipped=" + skipped + ") from " + path);
                if (tampered > 0) {
                    LOG.severe("[Calibrator] ⚠ HMAC FAILURES = " + tampered
                            + ". File may have been edited externally.");
                }
            } catch (Exception ex) {
                LOG.warning("[Calibrator] load failed: " + ex.getMessage());
            }
        }

        public int totalOutcomeCount() {
            int n = 0;
            for (java.util.concurrent.ConcurrentLinkedDeque<Outcome> dq : history.values()) {
                n += dq.size();
            }
            return n;
        }

        // ─────────────────────────────────────────────────────────────────
        // [v79 I3] AUDIT LOG — append-only, hash-chained, HMAC-signed.
        //
        // Format per line:
        //   seq;type;ts;symbol;side|outcome_tag;raw|hit;weight;regime;data1;data2;prevHmac;hmac
        //
        // Каждая строка содержит HMAC предыдущей → tamper of any row breaks
        // chain at all subsequent rows. verifyAuditIntegrity() полностью
        // прогоняет цепь и возвращает status.
        // ─────────────────────────────────────────────────────────────────

        private synchronized void writeOutcomeAudit(String symbol, double rawScore, boolean hit,
                                                    double atrPct, double weight, String tag,
                                                    String rb, double entry, double current) {
            try {
                long seq = auditSeq.incrementAndGet();
                long ts = System.currentTimeMillis();
                String payload = String.format(
                        "%d;OUTCOME;%d;%s;%s;%.6f;%d;%.3f;%s;%.8f;%.8f;%s",
                        seq, ts, escape(symbol), sanitizeTag(tag),
                        rawScore, hit ? 1 : 0, weight, sanitizeRegime(rb),
                        entry, current,
                        lastAuditHmac == null ? "" : lastAuditHmac);
                String hmac = hmacSha256(payload, HMAC_KEY);
                appendAuditLine(payload + ";" + hmac);
                lastAuditHmac = hmac;
            } catch (Throwable t) {
                LOG.warning("[AuditLog] outcome write failed: " + t.getMessage());
            }
        }

        public synchronized void writeDispatchAudit(String symbol, String side, double price,
                                                    double tp1, double sl, double prob,
                                                    String btcRegime, String source,
                                                    boolean paperMode) {
            try {
                long seq = auditSeq.incrementAndGet();
                long ts = System.currentTimeMillis();
                String payload = String.format(
                        "%d;DISPATCH;%d;%s;%s;%.8f;%.8f;%.8f;%.2f;%s;%s;%s;%s",
                        seq, ts, escape(symbol), side,
                        price, tp1, sl, prob,
                        escape(btcRegime), escape(source),
                        paperMode ? "PAPER" : "LIVE",
                        lastAuditHmac == null ? "" : lastAuditHmac);
                String hmac = hmacSha256(payload, HMAC_KEY);
                appendAuditLine(payload + ";" + hmac);
                lastAuditHmac = hmac;
            } catch (Throwable t) {
                LOG.warning("[AuditLog] dispatch write failed: " + t.getMessage());
            }
        }

        private void appendAuditLine(String line) throws java.io.IOException {
            java.io.File f = new java.io.File(AUDIT_LOG_PATH);
            java.io.File parent = f.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            try (java.io.FileWriter fw = new java.io.FileWriter(f, true);
                 java.io.BufferedWriter bw = new java.io.BufferedWriter(fw);
                 java.io.PrintWriter pw = new java.io.PrintWriter(bw)) {
                pw.println(line);
            }
        }

        private static String escape(String s) {
            if (s == null) return "";
            return s.replace(";", ",").replace("\n", " ").replace("\r", " ");
        }

        /**
         * [v79 FIX I3] Restore (auditSeq, lastAuditHmac) from existing audit log.
         *
         * Without this, every restart resets seq to 0 and prevHmac to "", which:
         *   1. duplicates seq numbers across sessions (1..N, then 1..M after restart)
         *   2. breaks the hash chain — the first post-restart record links to ""
         *      instead of the real last hmac, and verifyAuditIntegrity() then
         *      flags every record after the boundary as TAMPERED.
         *
         * Strategy: scan from end of file backwards, find the last non-comment line,
         * extract the trailing hmac (after final ';') and the leading seq (before first ';').
         * If any IO/parse error → leave defaults (seq=0, prev=""), which is the
         * pre-fix behavior — still safer than crashing startup.
         *
         * No new classes — pure static-method-on-existing-class.
         */
        private synchronized void initAuditState() {
            try {
                java.io.File f = new java.io.File(AUDIT_LOG_PATH);
                if (!f.exists() || f.length() == 0) return;
                String lastLine = null;
                try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isBlank() || line.startsWith("#")) continue;
                        lastLine = line;
                    }
                }
                if (lastLine == null) return;
                int lastSemi = lastLine.lastIndexOf(';');
                int firstSemi = lastLine.indexOf(';');
                if (lastSemi < 0 || firstSemi < 0 || lastSemi <= firstSemi) return;
                String hmac = lastLine.substring(lastSemi + 1).trim();
                String seqStr = lastLine.substring(0, firstSemi).trim();
                long seq = Long.parseLong(seqStr);
                if (seq < 0) return;
                this.auditSeq.set(seq);
                this.lastAuditHmac = hmac;
                LOG.info("[Calibrator] audit chain restored: seq=" + seq
                        + " prev=" + (hmac.length() > 8 ? hmac.substring(0, 8) + "…" : hmac));
            } catch (Throwable t) {
                LOG.warning("[Calibrator] initAuditState skipped: " + t.getMessage());
            }
        }

        /**
         * [v79 I6] PUBLIC INTEGRITY CHECK.
         * Reads the entire audit log and verifies every HMAC + chain link.
         * Returns short status string for Telegram/UI display.
         *
         * Anyone with the HMAC key (the bot operator + auditors) can run this
         * to confirm the bot has not "lost" or modified any outcomes.
         */
        public synchronized String verifyAuditIntegrity() {
            java.io.File f = new java.io.File(AUDIT_LOG_PATH);
            if (!f.exists()) return "no_audit_log";
            int total = 0, ok = 0, broken = 0;
            String prevHmac = "";
            try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(f))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isBlank() || line.startsWith("#")) continue;
                    total++;
                    int lastSemi = line.lastIndexOf(';');
                    if (lastSemi < 0) { broken++; continue; }
                    String payload = line.substring(0, lastSemi);
                    String hmac = line.substring(lastSemi + 1);
                    String calc = hmacSha256(payload, HMAC_KEY);
                    if (!calc.equals(hmac)) { broken++; continue; }
                    // Chain check: payload должен заканчиваться предыдущим hmac.
                    if (!payload.endsWith(";" + prevHmac)) {
                        // First record has empty prev — special case.
                        if (!(prevHmac.isEmpty() && payload.endsWith(";"))) {
                            broken++; continue;
                        }
                    }
                    ok++;
                    prevHmac = hmac;
                }
            } catch (Exception e) {
                return "error: " + e.getMessage();
            }
            if (broken == 0) return "✓OK n=" + total;
            return String.format("⚠TAMPERED %d/%d", broken, total);
        }

        /** [v79 I6] Public stats for daily integrity report. */
        public String getPublicStats() {
            int total = totalOutcomeCount();
            double weightedHits = 0, weightedTotal = 0;
            int byTag_TP1 = 0, byTag_SL = 0, byTag_AMB = 0, byTag_TS = 0;
            for (java.util.concurrent.ConcurrentLinkedDeque<Outcome> dq : history.values()) {
                for (Outcome o : dq) {
                    weightedTotal += o.weight;
                    if (o.hit) weightedHits += o.weight;
                    // [v79 FIX] Switch on String tag (was broken: bare TP1/SL etc. didn't resolve
                    // because we removed the OutcomeTag enum and converted to String constants).
                    switch (o.tag) {
                        case TAG_TP1:       byTag_TP1++; break;
                        case TAG_SL:        byTag_SL++;  break;
                        case TAG_AMBIGUOUS: byTag_AMB++; break;
                        case TAG_TIME_STOP: byTag_TS++;  break;
                        default: break;
                    }
                }
            }
            double wr = weightedTotal > 0 ? weightedHits / weightedTotal * 100 : 0;
            return String.format("n=%d wr=%.1f%% TP1=%d SL=%d Amb=%d TS=%d",
                    total, wr, byTag_TP1, byTag_SL, byTag_AMB, byTag_TS);
        }

        // ─────────────────────────────────────────────────────────────────
        //  HMAC-SHA256 helper.
        // ─────────────────────────────────────────────────────────────────

        private static String hmacSha256(String payload, String key) {
            try {
                javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
                javax.crypto.spec.SecretKeySpec sk = new javax.crypto.spec.SecretKeySpec(
                        key.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256");
                mac.init(sk);
                byte[] raw = mac.doFinal(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                StringBuilder sb = new StringBuilder(raw.length * 2);
                for (byte b : raw) sb.append(String.format("%02x", b));
                return sb.toString();
            } catch (Exception e) {
                return "HMAC_ERROR";
            }
        }
    }

    // LOCAL EXHAUSTION — per-pair dump/pump detection.
    //
    // Returns:
    //   direction = +1  →  strong UP move   (blocks SHORT entries)
    //   direction = -1  →  strong DOWN move (blocks LONG  entries)
    //   direction =  0  →  no outlier move  (returns null)
    //
    // Criteria (both required):
    //   1. Accumulated close-to-close move over last 4 bars ≥ 2.5 × ATR(14)
    //   2. Mean volume of those 4 bars ≥ 1.5 × median volume of preceding 20 bars
    //
    // This catches isolated alt dumps (e.g. DOT dumps, BTC flat) that GIC misses
    // because GIC only watches BTC-driven global panics.
    private static final class LocalExhaustion {
        final int direction;   // -1, 0, +1
        final double moveAtr;  // magnitude in ATR units
        LocalExhaustion(int d, double m) { direction = d; moveAtr = m; }
    }

    private LocalExhaustion detectLocalExhaustion(List<com.bot.TradingCore.Candle> c15,
                                                  double atr14) {
        if (c15 == null || c15.size() < 25) return null;
        if (atr14 <= 0) return null;

        int n = c15.size();
        final int lookback = 4;

        // Accumulated close-to-close move
        double move = c15.get(n - 1).close - c15.get(n - 1 - lookback).close;
        double moveAtr = Math.abs(move) / atr14;
        if (moveAtr < 2.5) return null;

        // Recent volume average
        double recentVol = 0;
        for (int i = n - lookback; i < n; i++) {
            recentVol += c15.get(i).volume;
        }
        double recentAvg = recentVol / lookback;

        // Median volume over preceding 20 bars
        double[] hist = new double[20];
        for (int i = 0; i < 20; i++) {
            hist[i] = c15.get(n - lookback - 20 + i).volume;
        }
        java.util.Arrays.sort(hist);
        double medianVol = hist[10];

        if (medianVol <= 0) return null;
        if (recentAvg < medianVol * 1.5) return null;  // volume not confirming = not exhaustion

        int direction = move > 0 ? +1 : -1;
        return new LocalExhaustion(direction, moveAtr);
    }

    //  LATE-MOVE DETECTION (multi-window)

    /** Result of {@link #detectLateMove}. severity: 0=NONE, 1=SOFT, 2=HARD. */
    private static final class LateMoveSignal {
        final int severity;
        final boolean dirUp;
        final double maxAtrMul;
        final int streakBars;
        final double velocityRatio;
        LateMoveSignal(int severity, boolean dirUp, double maxAtrMul,
                       int streakBars, double velocityRatio) {
            this.severity = severity;
            this.dirUp = dirUp;
            this.maxAtrMul = maxAtrMul;
            this.streakBars = streakBars;
            this.velocityRatio = velocityRatio;
        }
    }

    private static final LateMoveSignal LATE_NONE = new LateMoveSignal(0, false, 0, 0, 1.0);

    // [v71] Late-move thresholds raised — главная причина late_hard_block=2-7/cycle.
    //
    // Анализ: на 15m таймфрейме streak 6 баров одного цвета = 1.5 часа one-color =
    // НОРМАЛЬНЫЙ начинающийся тренд, а не "late entry". Старые границы блокировали
    // именно те сетапы которые мы хотим — продолжение свежего тренда.
    //
    //   atr 1.8→2.5  : 2.5×ATR за 4-12 баров = реальный extension, а не setup
    //   streak 6→8   : 8 баров = 2 часа one-color, реально поздно
    //   vel 2.5→3.0  : умеренное ускорение это часть тренда, не late entry
    //   atr 2.5→4.5  : HARD veto только на реально вытянутых движениях.
    //                  4.5×ATR за 4-12 баров = parabolic blow-off, а не trend.
    //                  При 2.5 каждый нормальный 5%-движ на ALT триггерил veto
    //                  именно когда нужно было войти.
    //   streak 8→10  : 10 баров = 2.5 часа one-color (был 8 = 2 часа).
    //   vel 3.0→4.0  : существенное ускорение за пределы нормы.
    private static final double LM_SOFT_ATR  = 1.8;   // [v77] 1.5→1.8
    private static final double LM_HARD_ATR  = 4.5;   // [v77 LATENCY] 2.5→4.5
    private static final int    LM_STREAK_S  = 6;     // [v77] 5→6
    private static final int    LM_STREAK_H  = 10;    // [v77] 8→10
    private static final double LM_VEL_BLOWUP = 4.0;  // [v77] 3.0→4.0
    private static final int    LM_W_FAST = 4;
    private static final int    LM_W_MID  = 8;
    private static final int    LM_W_SLOW = 12;

    /**
     * Multi-window late-entry risk classifier. Returns NONE / SOFT / HARD.
     * <p>OR-combined factors:
     * <ul>
     *   <li>ATR depth on max(4,8,12)-bar window</li>
     *   <li>Consecutive same-direction bar streak</li>
     *   <li>3-bar velocity vs 20-bar median velocity</li>
     * </ul>
     * HARD when ANY of: atr ≥ 1.8, streak ≥ 6, atr ≥ 1.2 + streak ≥ 4,
     * atr ≥ 1.2 + velocity ≥ 2.5×.
     */
    private static LateMoveSignal detectLateMove(java.util.List<com.bot.TradingCore.Candle> c, double atr14) {
        if (c == null || c.size() < LM_W_SLOW + 2) return LATE_NONE;
        final int n = c.size();

        // (1) Multi-window ATR displacement
        double maxAtrMul = 0;
        Boolean atrDirUp = null;
        if (atr14 > 0) {
            double last = c.get(n - 1).close;
            int[] windows = { LM_W_FAST, LM_W_MID, LM_W_SLOW };
            for (int w : windows) {
                int idx = n - 1 - w;
                if (idx < 0) continue;
                double from = c.get(idx).close;
                double mul = Math.abs(last - from) / atr14;
                if (mul > maxAtrMul) {
                    maxAtrMul = mul;
                    atrDirUp = last > from;
                }
            }
        }

        // (2) Consecutive same-color streak
        boolean firstBull = c.get(n - 1).close > c.get(n - 1).open;
        int streak = 0;
        for (int i = n - 1; i >= Math.max(0, n - LM_STREAK_H - 2); i--) {
            boolean bull = c.get(i).close > c.get(i).open;
            if (bull == firstBull) streak++;
            else break;
        }
        Boolean streakDirUp = streak >= 2 ? firstBull : null;

        // (3) Velocity blow-up
        double velocityRatio = 1.0;
        if (n >= 22) {
            double recent = 0;
            for (int i = n - 3; i < n; i++) {
                recent += Math.abs(c.get(i).close - c.get(i - 1).close);
            }
            recent /= 3.0;
            double[] hist = new double[20];
            for (int i = 0; i < 20; i++) {
                int idx = n - 23 + i;
                hist[i] = Math.abs(c.get(idx).close - c.get(idx - 1).close);
            }
            java.util.Arrays.sort(hist);
            double median = (hist[9] + hist[10]) / 2.0;
            if (median > 1e-12) velocityRatio = recent / median;
        }

        Boolean primaryDirUp = atrDirUp != null ? atrDirUp : streakDirUp;
        if (primaryDirUp == null) return LATE_NONE;

        boolean atrSoft = maxAtrMul >= LM_SOFT_ATR;
        boolean atrHard = maxAtrMul >= LM_HARD_ATR;
        boolean strkS   = streak    >= LM_STREAK_S;
        boolean strkH   = streak    >= LM_STREAK_H;
        boolean velBlow = velocityRatio >= LM_VEL_BLOWUP;

        if (atrHard || strkH || (atrSoft && velBlow) || (atrSoft && strkS)) {
            return new LateMoveSignal(2, primaryDirUp, maxAtrMul, streak, velocityRatio);
        }
        if (atrSoft || strkS || velBlow) {
            return new LateMoveSignal(1, primaryDirUp, maxAtrMul, streak, velocityRatio);
        }
        return LATE_NONE;
    }
}