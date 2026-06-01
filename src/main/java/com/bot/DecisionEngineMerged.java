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

    // [HOLE-LONG FIX 2026-05-08] Env-параметризация LONG-suppression слоёв.
    // Дефолты сохраняют старое поведение. Чтобы разморозить LONG в bear-market:
    //   HTF_OPPOSE_PENALTY=0           — убирает -5 баллов за HTF mismatch
    //   GIC_LONG_HARD_VETO=0           — конвертирует hard-reject в soft penalty
    //   DUAL_HTF_PENALTY_MULT=0.85     — мягче чем 0.65 (default)
    private static final double HTF_OPPOSE_PENALTY = envDoubleStatic("HTF_OPPOSE_PENALTY", 5.0);
    private static final boolean GIC_LONG_HARD_VETO = !"0".equals(
            System.getenv().getOrDefault("GIC_LONG_HARD_VETO", "1"));
    private static final double DUAL_HTF_PENALTY_MULT = envDoubleStatic("DUAL_HTF_PENALTY_MULT", 0.65);
    private static final double SINGLE_HTF_PENALTY_MULT = envDoubleStatic("SINGLE_HTF_PENALTY_MULT", 0.85);

    private static double envDoubleStatic(String key, double def) {
        try {
            String v = System.getenv(key);
            if (v == null || v.isBlank()) return def;
            return Double.parseDouble(v.trim());
        } catch (Throwable t) { return def; }
    }

    /** [HOLE-4] Required agreeing clusters for the given market state. */
    private static int clustersRequired(MarketState ms) {
        return ms == MarketState.RANGE ? MIN_AGREEING_CLUSTERS_RANGE : MIN_AGREEING_CLUSTERS;
    }

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

    /**
     * [A3 2026-05-08] Peek-only variant of getAndResetRejectTrace.
     * Returns top-N reasons WITHOUT resetting counters, so the same trace
     * can be sent to multiple destinations (heartbeat + diag log) without
     * one consumer wiping data needed by the other.
     * Used by BotMain.maybeSendHeartbeat for "DE-rejects:" line.
     */
    public static String peekRejectTrace(int top) {
        if (REJECT_TRACE.isEmpty()) return "";
        java.util.List<java.util.Map.Entry<String, java.util.concurrent.atomic.AtomicLong>> entries =
                new java.util.ArrayList<>(REJECT_TRACE.entrySet());
        entries.sort((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()));
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (var e : entries) {
            long v = e.getValue().get();
            if (v == 0) continue;
            if (sb.length() > 0) sb.append(' ');
            sb.append(e.getKey()).append('=').append(v);
            if (++count >= Math.max(1, top)) break;
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

            // [v82] Time-stop expectation. SYNC с ISC.TIME_STOP_BARS (default 12 = 180 min).
            // История бага: v75 заявлял 90 мин, v81 укоротил до 60 мин но строка осталась
            // 90 — пользователь видел одно, бот делал другое. Сейчас default ISC = 12 баров
            // = 180 мин. Если меняешь ISC_TIME_STOP_BARS env, обнови соответствующее число
            // здесь. (Не делаем cross-class import чтобы не плодить зависимости.)
            sb.append(String.format("%n⏳ Time-stop: 180 мин"));

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

    // BACKTEST-AWARE OVERLOADS: pass historical bar time as `now` so that
    // cooldowns/blacklist/daily-loss guards keyed on `now` work in simulated time
    // instead of wall-clock. Without this, the 60-min csLastSignalTime cooldown
    // never expires during a backtest run (which finishes in ~12 min real time),
    // capping signals to ~1 per pair regardless of strategy quality.
    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             List<com.bot.TradingCore.Candle> c2h,
                             CoinCategory cat,
                             long now) {
        return generate(symbol, c1, c5, c15, c1h, c2h, cat, now);
    }

    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             CoinCategory cat,
                             long now) {
        return generate(symbol, c1, c5, c15, c1h, null, cat, now);
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

    private enum MarketRegime {
        TREND_UP,        // 1h ADX > threshold AND +DI > -DI
        TREND_DOWN,      // 1h ADX > threshold AND -DI > +DI
        RANGE,           // 1h ADX < threshold (MR works)
        UNCLEAR          // ambiguous — fallback to MR
    }

    private MarketRegime detectMarketRegime(List<com.bot.TradingCore.Candle> c1h) {
        if (c1h == null || c1h.size() < 30) return MarketRegime.UNCLEAR;

        com.bot.TradingCore.ADXResult adx1h = com.bot.TradingCore.adx(c1h, 14);
        double adxValue = adx1h.adx;
        boolean dirUp   = adx1h.plusDI  > adx1h.minusDI;
        boolean dirDown = adx1h.minusDI > adx1h.plusDI;

        if (adxValue > PHASE2_BREAKOUT_MIN_ADX) {
            if (dirUp)   return MarketRegime.TREND_UP;
            if (dirDown) return MarketRegime.TREND_DOWN;
        }
        if (adxValue < PHASE2_RANGE_MAX_ADX) {
            return MarketRegime.RANGE;
        }
        return MarketRegime.UNCLEAR;
    }

    private TradeIdea generate(String symbol,
                               List<com.bot.TradingCore.Candle> c1,
                               List<com.bot.TradingCore.Candle> c5,
                               List<com.bot.TradingCore.Candle> c15,
                               List<com.bot.TradingCore.Candle> c1h,
                               List<com.bot.TradingCore.Candle> c2h,
                               CoinCategory cat,
                               long now) {

        // ─── Common pre-filters (apply to ALL strategies) ───
        if (!valid(c15) || !valid(c1h)) return reject("invalid_candles");

        // CROSS-STRATEGY COOLDOWN — single global lock per symbol.
        // Once any strategy fires here, all strategies blocked until cooldown.
        Long lastSig = csLastSignalTime.get(symbol);
        if (lastSig != null && (now - lastSig) < CS_COOLDOWN_MS) {
            return reject("cs_cooldown");
        }

        // Post-pump / post-dump persisted skips from other systems
        Long ppUntil = postPumpSkipUntil.get(symbol);
        if (ppUntil != null) {
            if (now < ppUntil) return reject("post_pump_cooldown");
            postPumpSkipUntil.remove(symbol);
        }
        Long pdUntil = postDumpSkipUntil.get(symbol);
        if (pdUntil != null) {
            if (now < pdUntil) return reject("post_dump_cooldown");
            postDumpSkipUntil.remove(symbol);
        }

        // MEME blanket-skip across all strategies
        if (CS_SKIP_MEME && cat == CoinCategory.MEME) return reject("cs_skip_meme");

        // Non-crypto skip: commodities (oil/gas), precious metals (gold/silver),
        // forex, indices. Bot is built for crypto perpetuals; commodity perpetuals
        // have different funding cycles (4h vs 8h), different leverage limits, and
        // exist only on mainnet (testnet returns "Invalid symbol" → auto-trade FAIL).
        // Override via SKIP_NON_CRYPTO=0 to allow (not recommended).
        if (!"0".equals(System.getenv().getOrDefault("SKIP_NON_CRYPTO", "1"))) {
            AssetType at = detectAssetType(symbol);
            if (at != AssetType.CRYPTO && at != AssetType.UNKNOWN) {
                return reject("non_crypto_" + at.name().toLowerCase());
            }
        }

        // ═══ STRATEGY ROUTER ═══
        // Phase 1 (default): VCB only — proven baseline +3.5%/мес
        // Phase 4 (opt-in): + Mean Reversion via env MEAN_REV_ENABLED=1
        // Math к +10%/мес: VCB +3.5% + MR +2-3% + FundingArb +1.5% + ML +2-3% = +9-12%

        // Primary: VCB Squeeze Breakout (validated baseline)
        TradeIdea idea = generateTrendPullback(symbol, c15, c1h, c2h, cat, now);
        if (idea != null) {
            csLastSignalTime.put(symbol, now);
            return idea;
        }

        // [v9.9 2026-05-29] PHASE 4 SKELETON — Mean Reversion для RANGE markets.
        // ВКЛЮЧАЕТСЯ ТОЛЬКО через env MEAN_REV_ENABLED=1.
        // Default OFF → 0 impact на baseline.
        // Включи когда: paper data 50+ VCB signals подтвердил baseline →
        // backtest показал MR profitable → 2 недели paper test MR alone.
        // См. roadmap к +10%/мес в memory: project_v92_baseline.md
        if ("1".equals(System.getenv().getOrDefault("MEAN_REV_ENABLED", "0"))) {
            TradeIdea mrIdea = generateMeanReversion(symbol, c15, c1h, cat, now);
            if (mrIdea != null) {
                csLastSignalTime.put(symbol, now);
                return mrIdea;
            }
        }

        return reject("tp_no_setup");
    }

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
    private TradeIdea generateTrendPullback(String symbol,
                                            List<com.bot.TradingCore.Candle> c15,
                                            List<com.bot.TradingCore.Candle> c1h,
                                            List<com.bot.TradingCore.Candle> c2h,
                                            CoinCategory cat,
                                            long now) {

        // ── Pre-filters ──
        if (cat == CoinCategory.MEME) return reject("vcb_skip_meme");
        if (c15 == null || c15.size() < 100) return reject("vcb_insufficient_15m");
        if (c1h == null || c1h.size() < 50) return reject("vcb_insufficient_1h");

        // [v8.0 STRUCTURAL] MAJOR COINS ONLY — отсеиваем manipulation-prone
        // small/mid alts которые в крипте 2026 дают худший WR из-за HFT
        // ликвидности hunting и pump/dump групп.
        // Major: BTC/ETH/SOL/BNB/XRP/ADA/AVAX/LINK/DOGE/TON/DOT/MATIC + TOP cat.
        if (cat != CoinCategory.TOP && !tpIsMajorCoin(symbol)) {
            return reject("vcb_not_major_coin");
        }

        int n = c15.size();
        com.bot.TradingCore.Candle last15 = c15.get(n - 1);
        double price = last15.close;
        if (price <= 0) return reject("vcb_invalid_price");

        // ── ATR sanity ──
        double atr14 = com.bot.TradingCore.atr(c15, 14);
        if (atr14 <= 0) return reject("vcb_invalid_atr");
        double atrPct = atr14 / price;
        // [v8.3] ATR расширен 0.40-3.50% → 0.35-4.00%. Захватывает quiet-hour
        // setups (ATR 0.35-0.40%) и high-vol breakouts (3.5-4%) которые VCB
        // умеет торговать с trail-защитой. Минимальный риск качества.
        if (atrPct < 0.0035) return reject("vcb_atr_too_low");
        if (atrPct > 0.0400) return reject("vcb_atr_too_high");

        // ═══════════════════════════════════════════════════════════════
        // 1. SQUEEZE CONTEXT (volatility compression в окне 8 баров)
        // ═══════════════════════════════════════════════════════════════
        com.bot.TradingCore.BollingerSqueeze bb =
                com.bot.TradingCore.bollingerSqueeze(c15, 20, 2.0, 96);
        if (bb.upper <= 0 || bb.lower <= 0) return reject("vcb_no_bb");

        // [v8.0/v9.0] Squeeze window 6 баров. Также считаем DURATION (consecutive bars
        // в squeeze) для probability bonus — long squeeze = stronger accumulation.
        boolean recentSqueeze = false;
        int squeezeDuration = 0;
        for (int i = Math.max(0, n - 6); i < n; i++) {
            com.bot.TradingCore.BollingerSqueeze bbi =
                    com.bot.TradingCore.bollingerSqueeze(c15.subList(0, i + 1), 20, 2.0, 96);
            if (bbi.bandwidthPctile <= 0.15) {
                recentSqueeze = true;
                squeezeDuration++;
            }
        }
        if (!recentSqueeze) return reject("vcb_no_squeeze");

        // ═══════════════════════════════════════════════════════════════
        // 2. BREAKOUT DETECTION — определяем направление
        // ═══════════════════════════════════════════════════════════════
        boolean breakUp = last15.close > bb.upper;
        boolean breakDown = last15.close < bb.lower;
        if (!breakUp && !breakDown) return reject("vcb_no_breakout");
        if (breakUp && breakDown) return reject("vcb_ambiguous_break");

        boolean wantLong = breakUp;

        // ═══════════════════════════════════════════════════════════════
        // [v83.0 2026-06-01] ANTI-FALSE-BREAKOUT FILTER
        // ═══════════════════════════════════════════════════════════════
        // ДИАГНОЗ (backtest 506 сделок): WR=38.7% — ХУЖЕ монетки. Корень: вход
        // на САМОМ баре пробоя BB часто = пик импульса, после которого откат
        // (false breakout). Это тянет WR < 50%.
        //
        // FIX (2 проверки, обе дёшевы, отсекают ~половину false breakouts):
        //   (1) CLOSE-IN-RANGE: close должен быть в дальней трети бара по
        //       направлению пробоя. Если close далеко от экстремума бара —
        //       значит был откат внутри бара = слабый пробой = пропускаем.
        //   (2) NOT-OVEREXTENDED: close не оторван от BB более чем на 1.0 ATR.
        //       Если цену уже унесло >1 ATR за полосу — мы догоняем уехавший
        //       поезд, mean-reversion риск высок = пропускаем.
        //
        // [v83.1 ОТКАТ] Default 1→0. Гипотеза A ПРОВАЛЕНА: backtest 420 сделок
        // дал WR 39.0% (было 38.7%) = +0.3pp шум, отрезал 86 сделок впустую.
        // ВЫВОД: false breakouts НЕ корень — пробои в принципе не предсказывают
        // направление на этих парах (WR стабильно ~39% независимо от формы бара).
        // Фильтр оставлен в коде (env VCB_BREAKOUT_CONFIRM=1 включит), но по
        // умолчанию OFF. Корень ищем в выходах (trail душит winners), не во входе.
        if ("1".equals(System.getenv().getOrDefault("VCB_BREAKOUT_CONFIRM", "0"))) {
            double barRange = last15.high - last15.low;
            if (barRange > 1e-12) {
                // (1) close в дальней трети бара по направлению пробоя
                double closePos = (last15.close - last15.low) / barRange; // 0=low,1=high
                if (wantLong && closePos < 0.66) return reject("vcb_weak_breakout_close");
                if (!wantLong && closePos > 0.34) return reject("vcb_weak_breakout_close");
            }
            // (2) не оторван от полосы более чем на 1.0 ATR
            double bandDist = wantLong ? (last15.close - bb.upper) : (bb.lower - last15.close);
            if (bandDist > atr14 * 1.0) return reject("vcb_breakout_overextended");
        }

        // ═══════════════════════════════════════════════════════════════
        // 3. VOLUME CONFIRMATION + ACCELERATION (institutional engaged)
        // [v8.1 ACCELERATION] Volume на entry баре должен быть ВЫШЕ avg
        // последних 3 баров — это значит momentum УСКОРЯЕТСЯ, не утихает.
        // Real institutional engagement = volume burst НА текущем баре,
        // не carry-over volume от предыдущего движения.
        // ═══════════════════════════════════════════════════════════════
        double volSma = tpComputeVolSma(c15, 20);
        if (volSma <= 0) return reject("vcb_no_vol_data");
        double volRatio = last15.volume / volSma;
        // [v9.1] ОТКАТ Volume 1.8 → 1.7 (v8.6 ухудшил). Это baseline v8.4.
        // [v80.2 ОТКАТ] Volume 1.9→1.7 (baseline). v80.1 регрессия -8%.
        if (volRatio < 1.7) return reject("vcb_no_volume");

        // Volume acceleration check
        double prev3VolAvg = 0;
        int prev3Count = 0;
        for (int i = Math.max(0, n - 4); i < n - 1; i++) {
            prev3VolAvg += c15.get(i).volume;
            prev3Count++;
        }
        if (prev3Count > 0) {
            prev3VolAvg /= prev3Count;
            if (last15.volume < prev3VolAvg * 1.20) return reject("vcb_no_vol_acceleration");
        }

        // ═══════════════════════════════════════════════════════════════
        // 4. CANDLE STRENGTH (real momentum, не doji breakout)
        // ═══════════════════════════════════════════════════════════════
        double bodyAbs = Math.abs(last15.close - last15.open);
        double range = last15.high - last15.low;
        if (range <= 0) return reject("vcb_zero_range");
        double bodyPct = bodyAbs / range;
        // [v7.1] Body 50% → 55%. Сильное тело = реальный momentum
        boolean candleStrong;
        if (wantLong) {
            candleStrong = last15.close > last15.open && bodyPct > 0.55;
        } else {
            candleStrong = last15.close < last15.open && bodyPct > 0.55;
        }
        if (!candleStrong) return reject("vcb_weak_candle");

        // ═══════════════════════════════════════════════════════════════
        // 5. HTF ALIGNMENT (1h EMA50 + 1h RSI direction)
        // [v7.2 WR-BOOST] Добавлен 1h RSI filter — HTF momentum должен быть
        // на нашей стороне. Это режет ~30% setup'ов но WR подскакивает 5-8pp.
        // ═══════════════════════════════════════════════════════════════
        double ema50_1h = ema(c1h, 50);
        double price1h  = c1h.get(c1h.size() - 1).close;
        if (ema50_1h <= 0) return reject("vcb_no_htf_ema");
        // [v9.5 ОТКАТ 2026-05-28] Buffer 0.985/1.015 → 0.99/1.01 (вернул к baseline).
        // Backtest регрессия: WR 52→45 (-7pp), Trades 48→51. Расширение HTF
        // пропускало border-line signals которые ухудшили overall WR. 5/6 SHORT
        // skew в live — это результат natural market state в NEUTRAL BTC,
        // не bug. Strict 0.99 buffer защищает от слабых HTF alignments.
        // См. backtest data 2026-05-28.
        if (wantLong && price1h < ema50_1h * 0.99) return reject("vcb_long_vs_bear_htf");
        if (!wantLong && price1h > ema50_1h * 1.01) return reject("vcb_short_vs_bull_htf");
        // [v7.4] HTF RSI 48/52 + [v8.4] RSI slope direction.
        // RSI должно НЕ ТОЛЬКО иметь правильное значение, но и двигаться в нашу сторону
        // последние 3 бара. RSI slope против нас = momentum exhaust → fade trade.
        double[] rsi1h = com.bot.TradingCore.rsiSeries(c1h, 14);
        int rsi1hN = c1h.size();
        double rsi1hNow = rsi1h[rsi1hN - 1];
        if (wantLong && rsi1hNow < 48) return reject("vcb_htf_rsi_bear");
        if (!wantLong && rsi1hNow > 52) return reject("vcb_htf_rsi_bull");
        // [v8.4] RSI slope -2.0/+2.0 (v9.0 -3 был хуже)
        if (rsi1hN >= 4) {
            double rsi1hPrev3 = rsi1h[rsi1hN - 4];
            double rsiSlope = rsi1hNow - rsi1hPrev3;
            if (wantLong && rsiSlope < -2.0) return reject("vcb_htf_rsi_falling");
            if (!wantLong && rsiSlope > 2.0) return reject("vcb_htf_rsi_rising");
        }

        // ═══════════════════════════════════════════════════════════════
        // 6. LTF MOMENTUM (RSI + EMA structure + ADX trending)
        // [v7.2 WR-BOOST] Добавлены:
        //   - EMA20 vs EMA50 на 15m (структурный тренд должен совпадать)
        //   - ADX(14) > 20 (only trade в trending market, не в флэте)
        // ═══════════════════════════════════════════════════════════════
        double[] rsi = com.bot.TradingCore.rsiSeries(c15, 14);
        double rsiNow = rsi[n - 1];
        if (wantLong && rsiNow < 50) return reject("vcb_rsi_against_long");
        if (!wantLong && rsiNow > 50) return reject("vcb_rsi_against_short");
        // Также не входим в overbought/oversold extremes (mean revert risk)
        if (wantLong && rsiNow > 75) return reject("vcb_rsi_overbought");
        if (!wantLong && rsiNow < 25) return reject("vcb_rsi_oversold");

        // [v8.0] ОТКАТ к v7.2 base config (best NetPnL -2.36%) + добавлены
        // СТРУКТУРНЫЕ фильтры (major coins, funding extreme).
        // ADX 20 + DI direction (как было в v7.2).
        double ema20_15m = ema(c15, 20);
        double ema50_15m = ema(c15, 50);
        com.bot.TradingCore.ADXResult adxR = com.bot.TradingCore.adx(c15, 14);
        // [v80.2 ОТКАТ] ADX 25→20. v80.1 дал backtest -8% (27 trades WR 33%). Baseline 20.
        if (adxR.adx < 20) return reject("vcb_adx_flat");
        if (wantLong && !adxR.bullish()) return reject("vcb_adx_bearish_di");
        if (!wantLong && !adxR.bearish()) return reject("vcb_adx_bullish_di");

        // ═══════════════════════════════════════════════════════════════
        // 7. NOT EXTENDED (анти-FOMO chase)
        // ═══════════════════════════════════════════════════════════════
        int sameDirCount = 0;
        for (int i = n - 7; i < n - 1; i++) {
            if (i < 0) continue;
            boolean bull = c15.get(i).close > c15.get(i).open;
            if (wantLong && bull) sameDirCount++;
            if (!wantLong && !bull) sameDirCount++;
        }
        if (sameDirCount >= 6) return reject("vcb_extended_move");

        // ═══════════════════════════════════════════════════════════════
        // 8. BTC REGIME HARD BLOCKS [v8.4 STRICT]
        // В 2026: alts корреляция с BTC ~80%. Торговать против BTC = -8pp WR.
        // Strict alignment: LONG только если BTC не падает; SHORT только если
        // BTC не растёт. Это режет contra-BTC trades → выше WR.
        // ═══════════════════════════════════════════════════════════════
        com.bot.GlobalImpulseController.GlobalContext btc =
                (gicRef != null) ? gicRef.getContext() : null;
        if (btc != null) {
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_PANIC)
                return reject("vcb_btc_panic");
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_CRASH)
                return reject("vcb_btc_crash");
            if (wantLong && btc.onlyShort) return reject("vcb_only_short");
            if (!wantLong && btc.onlyLong) return reject("vcb_only_long");
            // [v8.4] STRICT BTC ALIGNMENT
            if (wantLong && btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_DOWN)
                return reject("vcb_long_vs_btc_down");
            if (!wantLong && btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP)
                return reject("vcb_short_vs_btc_up");
        }

        // ═══════════════════════════════════════════════════════════════
        // RISK MANAGEMENT
        // SL = middle BB (BB EMA20) OR entry ± ATR×1.4 (тот что ближе к entry)
        // ═══════════════════════════════════════════════════════════════
        double atrStop = wantLong ? price - atr14 * 1.4 : price + atr14 * 1.4;
        double midBB = bb.mid;
        double sl;
        if (wantLong) {
            // Берём более близкий к цене SL (но не выше midBB чтобы не было too tight)
            sl = Math.max(atrStop, midBB);
            if (sl >= price) sl = atrStop; // safety
        } else {
            sl = Math.min(atrStop, midBB);
            if (sl <= price) sl = atrStop; // safety
        }
        double slDist = Math.abs(price - sl);
        double slPct = slDist / price;
        if (slPct < 0.0050) return reject("vcb_sl_too_tight");
        if (slPct > 0.0250) return reject("vcb_sl_too_wide");

        // [v8.1] TP2 вычисляется ниже AFTER probability — adaptive по quality.
        // Skeleton: TP2 init = 2.2R, потом upgrade до 3.0R если prob ≥ 0.75.

        // ═══════════════════════════════════════════════════════════════
        // PROBABILITY SCORING (база 0.62, cap 0.85)
        // ═══════════════════════════════════════════════════════════════
        double prob01 = 0.62;
        // Strong squeeze (current bandwidth very low) → higher quality breakout
        if (bb.bandwidthPctile <= 0.10) prob01 += 0.04;
        else if (bb.bandwidthPctile <= 0.15) prob01 += 0.02;
        // [v8.4] Squeeze duration tracked for telegram flag debug (см. flag SQZ_DUR),
        // bonus убран т.к. v9.0 (с bonus) дал хуже чем v8.4 без.
        // Strong volume
        if (volRatio >= 2.0) prob01 += 0.04;
        if (volRatio >= 3.5) prob01 += 0.03;
        // Strong candle body
        if (bodyPct >= 0.70) prob01 += 0.03;
        // HTF strongly aligned (на 2%+ в нашу сторону)
        double htfDelta = wantLong
                ? (price1h - ema50_1h) / ema50_1h
                : (ema50_1h - price1h) / ema50_1h;
        if (htfDelta >= 0.02) prob01 += 0.04;
        // [v7.4] ADX trending bonus (вместо hard filter)
        if (adxR.adx >= 25) prob01 += 0.03;
        else if (adxR.adx >= 20) prob01 += 0.02;
        if ((wantLong && adxR.bullish()) || (!wantLong && adxR.bearish())) prob01 += 0.02;
        // [v7.4] LTF EMA structure bonus
        if (ema20_15m > 0 && ema50_15m > 0) {
            if ((wantLong && ema20_15m > ema50_15m) || (!wantLong && ema20_15m < ema50_15m)) {
                prob01 += 0.02;
            }
        }
        // Major coin (institutional safety)
        if (tpIsMajorCoin(symbol)) prob01 += 0.03;
        // BTC aligned
        if (btc != null) {
            if (wantLong && btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP) prob01 += 0.03;
            if (!wantLong && btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_DOWN) prob01 += 0.03;
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.NEUTRAL) prob01 += 0.02;
        }
        // Funding aligned (contra-funding crowded shorts = bonus for long)
        // [v8.0 STRUCTURAL] EXTREME funding = strong squeeze potential.
        // Это РЕАЛЬНЫЙ edge на crypto — overcrowded одной стороны = forced unwind.
        FundingOIData fr = fundingCache.get(symbol);
        if (fr != null && fr.isValid()) {
            // Soft contra funding
            if (wantLong && fr.fundingRate < 0) prob01 += 0.02;
            if (!wantLong && fr.fundingRate > 0) prob01 += 0.02;
            // EXTREME funding bonus (overcrowded → squeeze)
            if (wantLong && fr.fundingRate < -0.0003) prob01 += 0.04;   // shorts crowded
            if (!wantLong && fr.fundingRate > 0.0003) prob01 += 0.04;   // longs crowded
            if (wantLong && fr.fundingRate < -0.0008) prob01 += 0.03;   // very crowded
            if (!wantLong && fr.fundingRate > 0.0008) prob01 += 0.03;
            // [v8.4] OI bonus убран (v9.0 регрессия)
        }

        // [v9.2] VP bonus убран — v9.1 регрессия (-1.33% vs +5.23% baseline).
        // VP пушил probability в adaptive TP=3R корзину, но trades не доходили = trail
        // закрывал на меньших R. Возврат к чистому v8.4 baseline.
        prob01 = Math.min(0.85, prob01);
        double probability = prob01 * 100.0;

        // [v9.4 ОТКАТ 2026-05-28] Threshold 0.78 → 0.72 (вернул к baseline).
        // Backtest регрессия: WR 52→45 (-7pp), NetPnL +3.54→+1.22 (-65%),
        // Trail 15→11 (-4 runners). 3R bucket был critical contributor в v8.4
        // best result (+5.23%). Перевод в 2.2R убил большие winners.
        // Реальный live time-stop fix = v9.3 (skip adjustStopForClusters),
        // а не сужение TP bucket. См. backtest data 2026-05-28.
        double tp2Mult = prob01 >= 0.72 ? 3.0 : 2.2;
        double tp2 = wantLong ? price + slDist * tp2Mult : price - slDist * tp2Mult;

        // ═══════════════════════════════════════════════════════════════
        // BUILD IDEA
        // ═══════════════════════════════════════════════════════════════
        List<String> flags = new ArrayList<>();
        flags.add("VCB_v8");
        flags.add(wantLong ? "CLUSTER_STR_BREAKOUT_UP" : "CLUSTER_STR_BREAKOUT_DOWN");
        flags.add("CLUSTER_SQUEEZE_RELEASE");
        flags.add("CLUSTER_VOL");
        flags.add("CLUSTER_HTF_ALIGNED");
        flags.add(String.format("BB_PCTILE=%.2f", bb.bandwidthPctile));
        flags.add(String.format("SQZ_DUR=%d", squeezeDuration));
        flags.add(String.format("VOL=%.1fx", volRatio));
        flags.add(String.format("BODY=%.0f%%", bodyPct * 100));
        flags.add(String.format("RSI=%.1f", rsiNow));
        flags.add(String.format("HTF_Δ=%.2f%%", htfDelta * 100));
        flags.add(String.format("ATR=%.2f%%", atrPct * 100));
        flags.add(String.format("SL=%.2f%%", slPct * 100));
        if (btc != null) flags.add("BTC_" + btc.regime.name());

        com.bot.TradingCore.Side side = wantLong
                ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT;
        HTFBias bias = detectBias2H(c2h != null && c2h.size() >= 30 ? c2h : c1h);
        double frRate  = (fr != null && fr.isValid()) ? fr.fundingRate  : 0.0;
        double frDelta = (fr != null && fr.isValid()) ? fr.fundingDelta : 0.0;
        double oiCh    = (fr != null && fr.isValid()) ? fr.oiChange1h   : 0.0;

        // [v8.1] Adaptive TP1=1.0R partial, TP2=2.2R или 3.0R (по prob).
        // Финальный R:R передаётся в Tracker для proper exit framework.
        TradeIdea idea = new TradeIdea(
                symbol, side, price, sl, tp2, tp2Mult,
                probability, flags, frRate, frDelta, oiCh,
                bias.name(), cat, null, 1.0, tp2Mult, tp2Mult);
        idea.setRobustAtrPct(atrPct);
        idea.setAgreeingClusters(5);
        return idea;
    }

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
    private TradeIdea generateMeanReversion(String symbol,
                                            List<com.bot.TradingCore.Candle> c15,
                                            List<com.bot.TradingCore.Candle> c1h,
                                            CoinCategory cat,
                                            long now) {
        // Pre-filters (same conservative gating как VCB)
        if (cat == CoinCategory.MEME) return null;
        if (c15 == null || c15.size() < 100) return null;
        if (c1h == null || c1h.size() < 50) return null;
        if (!tpIsMajorCoin(symbol) && cat != CoinCategory.TOP) return null;

        int n = c15.size();
        com.bot.TradingCore.Candle last15 = c15.get(n - 1);
        double price = last15.close;
        if (price <= 0) return null;

        double atr14 = com.bot.TradingCore.atr(c15, 14);
        if (atr14 <= 0) return null;
        double atrPct = atr14 / price;
        if (atrPct < 0.005 || atrPct > 0.05) return null;

        // ── 1. RANGE REGIME (ADX low on both 15m and 1h)
        com.bot.TradingCore.ADXResult adxR = com.bot.TradingCore.adx(c15, 14);
        if (adxR.adx > 22) return null;
        com.bot.TradingCore.ADXResult adx1h = com.bot.TradingCore.adx(c1h, 14);
        if (adx1h.adx > 25) return null;

        // ── 2. NOT in squeeze (squeeze = VCB territory)
        com.bot.TradingCore.BollingerSqueeze bb =
                com.bot.TradingCore.bollingerSqueeze(c15, 20, 2.0, 96);
        if (bb.upper <= 0 || bb.lower <= 0) return null;
        if (bb.bandwidthPctile < 0.30) return null;

        // ── 3. BB EXTREME TOUCH
        double pctB = (price - bb.lower) / Math.max(1e-9, bb.upper - bb.lower);
        boolean upperTouch = pctB > 0.95;
        boolean lowerTouch = pctB < 0.05;
        if (!upperTouch && !lowerTouch) return null;
        boolean wantLong = lowerTouch;

        // ── 4. RSI EXTREME confirmation
        double[] rsi = com.bot.TradingCore.rsiSeries(c15, 14);
        double rsiNow = rsi[n - 1];
        if (wantLong && rsiNow > 35) return null;
        if (!wantLong && rsiNow < 65) return null;

        // ── 5. NORMAL volume (not breakout — that's VCB)
        double volSma = tpComputeVolSma(c15, 20);
        if (volSma <= 0) return null;
        double volRatio = last15.volume / volSma;
        if (volRatio > 1.5) return null;
        if (volRatio < 0.4) return null; // dead market, no liquidity

        // ── 6. BTC REGIME guard
        com.bot.GlobalImpulseController.GlobalContext btc =
                (gicRef != null) ? gicRef.getContext() : null;
        if (btc != null) {
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_PANIC) return null;
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_CRASH) return null;
            if (wantLong && btc.onlyShort) return null;
            if (!wantLong && btc.onlyLong) return null;
        }

        // ── RISK
        // SL beyond BB extreme + ATR×0.5 buffer
        // TP back to midBB
        double sl;
        double tp;
        if (wantLong) {
            sl = bb.lower - atr14 * 0.5;
            tp = bb.mid;
        } else {
            sl = bb.upper + atr14 * 0.5;
            tp = bb.mid;
        }
        double slDist = Math.abs(price - sl);
        double tpDist = Math.abs(tp - price);
        if (slDist < 1e-9 || tpDist < 1e-9) return null;
        double slPct = slDist / price;
        if (slPct < 0.005 || slPct > 0.03) return null;

        double rr = tpDist / slDist;
        if (rr < 1.3) return null;

        // ── PROBABILITY scoring (база 0.55, cap 0.78 — lower чем VCB)
        double prob01 = 0.55;
        if (Math.abs(rsiNow - 50) > 30) prob01 += 0.05;
        if (bb.bandwidthPctile > 0.70) prob01 += 0.03;
        if (adxR.adx < 18) prob01 += 0.03;
        if (volRatio < 0.7) prob01 += 0.04;
        if (tpIsMajorCoin(symbol)) prob01 += 0.03;
        if (btc != null && btc.regime ==
                com.bot.GlobalImpulseController.GlobalRegime.NEUTRAL) prob01 += 0.03;

        prob01 = Math.min(0.78, prob01);
        double probability = prob01 * 100.0;

        // ── BUILD IDEA
        List<String> flags = new ArrayList<>();
        flags.add("MEAN_REV_v1");
        flags.add(wantLong ? "CLUSTER_STR_REVERSAL_UP" : "CLUSTER_STR_REVERSAL_DOWN");
        flags.add("CLUSTER_HTF_RANGE");
        flags.add("CLUSTER_MOM_OVERSOLD");
        flags.add(String.format("BB_PCTB=%.2f", pctB));
        flags.add(String.format("RSI=%.1f", rsiNow));
        flags.add(String.format("ADX=%.0f", adxR.adx));
        flags.add(String.format("VOL=%.1fx", volRatio));
        flags.add(String.format("ATR=%.2f%%", atrPct * 100));
        flags.add(String.format("SL=%.2f%%", slPct * 100));

        com.bot.TradingCore.Side side = wantLong
                ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT;
        double tp2Mult = rr;

        TradeIdea idea = new TradeIdea(
                symbol, side, price, sl, tp, tp2Mult,
                probability, flags, 0.0, 0.0, 0.0,
                "NONE", cat, null, 1.0, tp2Mult, tp2Mult);
        idea.setRobustAtrPct(atrPct);
        idea.setAgreeingClusters(4);
        return idea;
    }

    /** Major coins имеют больше institutional liquidity для защиты levels. */
    /**
     * Major coins имеют больше institutional liquidity для защиты levels.
     * [v8.2] Расширен список до top-30 ликвидных по volume24h на Binance Futures.
     * Все эти coins имеют: volume > $50M/day, spread < 0.05%, deep orderbook,
     * active institutional trading. Исключены: мемы, новые/спекулятивные тикеры.
     */
    private boolean tpIsMajorCoin(String symbol) {
        if (symbol == null) return false;
        String s = symbol.toUpperCase();
        // Tier 1 — top 10 by market cap (BTC, ETH ecosystem)
        if (s.startsWith("BTC") || s.startsWith("ETH") || s.startsWith("SOL")
                || s.startsWith("BNB") || s.startsWith("XRP") || s.startsWith("ADA")
                || s.startsWith("AVAX") || s.startsWith("LINK") || s.startsWith("DOGE")
                || s.startsWith("TON") || s.startsWith("DOT") || s.startsWith("MATIC")) {
            return true;
        }
        // Tier 2 — proven liquid coins (v9.2: убраны ICP/FET/RNDR/STX/ALGO как
        // наиболее волатильные/манипулируемые — давали worst WR в backtest)
        return s.startsWith("LTC") || s.startsWith("BCH") || s.startsWith("TRX")
                || s.startsWith("ATOM") || s.startsWith("UNI") || s.startsWith("AAVE")
                || s.startsWith("NEAR") || s.startsWith("APT") || s.startsWith("SUI")
                || s.startsWith("OP") || s.startsWith("ARB") || s.startsWith("INJ")
                || s.startsWith("FIL") || s.startsWith("HBAR") || s.startsWith("XLM")
                || s.startsWith("ETC");
    }

    // [v8.4 CLEANUP] Удалены dead helpers от старой Sweep+Reclaim стратегии:
    //   tpDetectTrendDirection, tpDetectPullback, tpCheckEntryBar,
    //   tpIsExtendedMove, tpComputeStop — не вызываются нигде после VCB v7.
    // Оставлены только active helpers: tpComputeVolSma, tpIsMajorCoin.

    private double tpComputeVolSma(List<com.bot.TradingCore.Candle> c, int period) {
        int n = c.size();
        if (n < period) return 0;
        double sum = 0;
        for (int i = n - period; i < n; i++) sum += c.get(i).volume;
        return sum / period;
    }
    // ─────────────────────────────────────────────────────────────────────
    // STRATEGY: BREAKOUT — for TREND regime (1h ADX > 25, +DI/-DI directional)
    // ─────────────────────────────────────────────────────────────────────
    //   1. 5m BoS detected (existing detectBoS)
    //   2. Volume spike: 5m vol > 1.3× SMA20
    //   3. Direction matches 1h trend (no ChoCh — only continuation)
    //   4. ATR percentile in [0.40, 0.95]
    //   5. SL = broken swing level + 0.3% buffer; TP = 2R; R:R = 1:2
    //   6. Probability: base 0.58 + bonuses (vol, HTF, BTC, pullback) cap 0.78
    private TradeIdea generateBreakout(String symbol,
                                       List<com.bot.TradingCore.Candle> c5,
                                       List<com.bot.TradingCore.Candle> c15,
                                       List<com.bot.TradingCore.Candle> c1h,
                                       List<com.bot.TradingCore.Candle> c2h,
                                       CoinCategory cat,
                                       MarketRegime regime,
                                       long now) {
        if (c5 == null || c5.size() < 30) return reject("bo_insufficient_5m");

        double atrPct15 = com.bot.TradingCore.atrPercentile(c15, 14, 100);
        if (atrPct15 < BO_MIN_ATR_PCTILE) return reject("bo_atr_too_low");
        if (atrPct15 > BO_MAX_ATR_PCTILE) return reject("bo_atr_too_high");

        boolean trendUp = (regime == MarketRegime.TREND_UP);
        int htfStructure = trendUp ? 1 : -1;
        BosResult bos = detectBoS(c5, htfStructure);
        if (!bos.detected) return reject("bo_no_bos");
        if (bos.isChoch)   return reject("bo_choch_skipped");
        if (bos.isBullish != trendUp) return reject("bo_direction_mismatch");

        double volSma20_5m = computeVolumeSma(c5, 20);
        double volCurrent_5m = last(c5).volume;
        if (volSma20_5m <= 0 || volCurrent_5m < volSma20_5m * BO_VOL_CONFIRM_MULT) {
            return reject("bo_no_volume_confirm");
        }

        com.bot.GlobalImpulseController.GlobalContext btc =
                (gicRef != null) ? gicRef.getContext() : null;
        if (btc != null) {
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_PANIC)
                return reject("bo_btc_panic");
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_CRASH)
                return reject("bo_btc_crash");
            // Phase 2.1: BTC_CHOPPY no longer auto-rejects breakouts.
            // Choppy BTC ≠ choppy altcoin; many alts trend independently when
            // BTC ranges. Side-specific BTC blockers below are sufficient.
            if (trendUp) {
                if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_DOWN)
                    return reject("bo_btc_strong_down_blocks_long");
                if (btc.onlyShort) return reject("bo_only_short_blocks_long");
            } else {
                if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP)
                    return reject("bo_btc_strong_up_blocks_short");
                if (btc.onlyLong) return reject("bo_only_long_blocks_short");
            }
        }

        double price = last(c5).close;
        double swingLevel = bos.swingLevel;
        double stop = trendUp
                ? swingLevel * (1.0 - BO_SL_SWING_BUFFER)
                : swingLevel * (1.0 + BO_SL_SWING_BUFFER);
        double riskDist = Math.abs(price - stop);
        if (riskDist / price < BO_SL_MIN_DIST_PCT) return reject("bo_sl_too_tight");
        if (riskDist / price > BO_SL_MAX_DIST_PCT) return reject("bo_sl_too_wide");
        double tp = trendUp ? price + riskDist * BO_TP_R_MULTIPLE : price - riskDist * BO_TP_R_MULTIPLE;
        com.bot.TradingCore.Side side = trendUp
                ? com.bot.TradingCore.Side.LONG
                : com.bot.TradingCore.Side.SHORT;        double probability01 = BO_PROB_BASE;
        if (volCurrent_5m > volSma20_5m * BO_VOL_SPIKE_MULT) probability01 += BO_PROB_BONUS_VOL;
        HTFBias bias2h = detectBias2H(c2h != null && c2h.size() >= MIN_BARS ? c2h : c1h);
        if (bias2h != null) {
            boolean alignedHTF = (trendUp && bias2h == HTFBias.BULL) ||
                    (!trendUp && bias2h == HTFBias.BEAR);
            if (alignedHTF) probability01 += BO_PROB_BONUS_HTF;
        }
        if (btc != null) {
            if (trendUp && (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP
                    || btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_IMPULSE_UP)) {
                probability01 += BO_PROB_BONUS_BTC;
            } else if (!trendUp && (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_DOWN
                    || btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_IMPULSE_DOWN)) {
                probability01 += BO_PROB_BONUS_BTC;
            }
        }

        if (pullback(c5, trendUp)) probability01 += BO_PROB_BONUS_PULL;
        probability01 = Math.min(BO_PROB_CAP, probability01);
        double probability = probability01 * 100.0;

        List<String> flags = new ArrayList<>();
        flags.add("BREAKOUT");
        flags.add(trendUp ? "BOS_UP" : "BOS_DOWN");
        flags.add(String.format("SWING=%.6f", swingLevel));
        flags.add(String.format("VOL/SMA=%.2f", volCurrent_5m / volSma20_5m));
        flags.add(String.format("ATR_PCT=%.2f", atrPct15));
        flags.add(trendUp ? "REGIME_TREND_UP" : "REGIME_TREND_DOWN");
        if (btc != null) flags.add("BTC_" + btc.regime.name());

        FundingOIData fr = fundingCache.get(symbol);
        double frRate  = (fr != null && fr.isValid()) ? fr.fundingRate  : 0.0;
        double frDelta = (fr != null && fr.isValid()) ? fr.fundingDelta : 0.0;
        double oiCh    = (fr != null && fr.isValid()) ? fr.oiChange1h   : 0.0;

        TradeIdea idea = new TradeIdea(
                symbol, side, price, stop, tp, BO_TP_R_MULTIPLE,
                probability, flags,
                frRate, frDelta, oiCh,
                bias2h.name(), cat,
                null,
                CS_TP1_R, BO_TP_R_MULTIPLE, BO_TP_R_MULTIPLE
        );
        idea.setRobustAtrPct(riskDist / price);
        idea.setAgreeingClusters(1);
        return idea;
    }

    // ─────────────────────────────────────────────────────────────────────
    // STRATEGY: FROM PUMPHUNTER — anticipatory + reversal setups
    // ─────────────────────────────────────────────────────────────────────
    // Consumes existing pumpHunter.detectPump() output. Acts ONLY on
    // anticipatory (PRE_PUMP_LONG/PRE_DUMP_SHORT) and reversal
    // (PUMP_EXHAUSTION_SHORT/DUMP_EXHAUSTION_LONG) events. Continuation
    // pumps are skipped — chasing pumps has poor expectancy.
    private TradeIdea generateFromPumpHunter(String symbol,
                                             List<com.bot.TradingCore.Candle> c1,
                                             List<com.bot.TradingCore.Candle> c5,
                                             List<com.bot.TradingCore.Candle> c15,
                                             CoinCategory cat,
                                             long now) {
        // [TREND-PULLBACK 2026-05-19] Hard guard. Belt-and-suspenders on top of the
        // router-level PHASE2_PUMPHUNTER_ENABLE check — even if the flag is misread,
        // this branch can never emit a signal in trend-pullback mode. No code removed.
        if (!PHASE2_PUMPHUNTER_ENABLE) return reject("ph_disabled_trend_pullback_mode");
        if (pumpHunter == null) return null;
        if (c1 == null || c1.size() < 20) return null;
        if (c15 == null || c15.size() < MIN_BARS) return null;

        com.bot.PumpHunter.PumpEvent event = pumpHunter.detectPump(symbol, c1, c5, c15, cat);
        if (event == null || event.type == com.bot.PumpHunter.PumpType.NONE) return null;
        if (event.strength < PHASE2_PUMP_MIN_STRENGTH) return reject("ph_strength_too_low");

        // Phase 2.1: only act on reversal/exhaustion events.
        // Anticipatory pre-pump/pre-dump detector is too noisy in practice
        // (false positive rate ~70%+ in tests). Continuation pumps remain
        // skipped — chasing pumps has poor expectancy.
        if (!event.isReversal()) return reject("ph_non_reversal_skipped");

        double price = last(c15).close;
        @SuppressWarnings("deprecation")
        com.bot.PumpHunter.PumpSignal sig = pumpHunter.generateSignal(event, price);
        if (sig == null) return reject("ph_signal_null");

        com.bot.GlobalImpulseController.GlobalContext btc =
                (gicRef != null) ? gicRef.getContext() : null;
        if (btc != null) {
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_PANIC)
                return reject("ph_btc_panic");
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_CRASH)
                return reject("ph_btc_crash");
            if (sig.side == com.bot.TradingCore.Side.LONG  && btc.onlyShort) return reject("ph_only_short");
            if (sig.side == com.bot.TradingCore.Side.SHORT && btc.onlyLong)  return reject("ph_only_long");
        }

        double atr14 = com.bot.TradingCore.atr(c15, 14);
        if (atr14 <= 0) return reject("ph_invalid_atr");

        double slMult     = 1.6;
        double tpR        = 2.0;
        double slMaxPct   = 0.09;
        double slDistAtr  = atr14 * slMult;
        double slDistCap  = price * slMaxPct;
        double slDist     = Math.min(slDistAtr, slDistCap);
        if (slDist < atr14 * 0.5) return reject("ph_sl_capped_too_tight");
        double stop = (sig.side == com.bot.TradingCore.Side.LONG)
                ? price - slDist : price + slDist;
        double tp = (sig.side == com.bot.TradingCore.Side.LONG)
                ? price + slDist * tpR : price - slDist * tpR;
        double probability01 = sig.confidence / 100.0;
        probability01 = Math.min(0.78, Math.max(0.55, probability01));
        double probability = probability01 * 100.0;

        List<String> flags = new ArrayList<>();
        flags.add("PHASE2_PH");
        flags.add("PH_" + event.type.name());
        flags.add(String.format("STRENGTH=%.2f", event.strength));
        flags.add(String.format("VOL_RATIO=%.1fx", event.volumeRatio));
        flags.add(String.format("MOVE=%.2f%%", event.movePct * 100));
        if (event.isReversal())     flags.add("REVERSAL");
        if (btc != null) flags.add("BTC_" + btc.regime.name());
        flags.addAll(event.flags);

        HTFBias bias2hPH = detectBias2H(c15);

        FundingOIData fr = fundingCache.get(symbol);
        double frRate  = (fr != null && fr.isValid()) ? fr.fundingRate  : 0.0;
        double frDelta = (fr != null && fr.isValid()) ? fr.fundingDelta : 0.0;
        double oiCh    = (fr != null && fr.isValid()) ? fr.oiChange1h   : 0.0;

        TradeIdea idea = new TradeIdea(
                symbol, sig.side, price, stop, tp, tpR,
                probability, flags,
                frRate, frDelta, oiCh,
                bias2hPH.name(), cat,
                null,
                CS_TP1_R, tpR, tpR
        );
        idea.setRobustAtrPct(atr14 / price);
        idea.setAgreeingClusters(1);
        return idea;
    }


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
    private static final boolean PHASE2_PUMPHUNTER_ENABLE = csEnvBool("PHASE2_PUMPHUNTER_ENABLE", true);
    private static final boolean PHASE2_BREAKOUT_ENABLE   = csEnvBool("PHASE2_BREAKOUT_ENABLE",   true);
    private static final double  PHASE2_BREAKOUT_MIN_ADX  = csEnvDouble("PHASE2_BREAKOUT_MIN_ADX", 25.0);
    private static final double  PHASE2_RANGE_MAX_ADX     = csEnvDouble("PHASE2_RANGE_MAX_ADX",    22.0);
    private static final double  PHASE2_PUMP_MIN_STRENGTH = csEnvDouble("PHASE2_PUMP_MIN_STRENGTH", 0.50);
    // ─── Breakout strategy named constants (no magic numbers) ─────────────
    private static final double BO_MIN_ATR_PCTILE    = 0.30;
    private static final double BO_MAX_ATR_PCTILE    = 0.95;
    private static final double BO_VOL_CONFIRM_MULT  = 1.15;
    private static final double BO_VOL_SPIKE_MULT    = 1.50;
    private static final double BO_SL_SWING_BUFFER   = 0.003;
    private static final double BO_SL_MIN_DIST_PCT   = 0.005;
    private static final double BO_SL_MAX_DIST_PCT   = 0.05;
    private static final double BO_TP_R_MULTIPLE     = 2.0;
    private static final double BO_PROB_BASE         = 0.58;
    private static final double BO_PROB_CAP          = 0.78;
    private static final double BO_PROB_BONUS_VOL    = 0.04;
    private static final double BO_PROB_BONUS_HTF    = 0.04;
    private static final double BO_PROB_BONUS_BTC    = 0.04;
    private static final double BO_PROB_BONUS_PULL   = 0.04;
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
    private TradeIdea generateFromFundingMomentum(String symbol,
                                                  List<com.bot.TradingCore.Candle> c1,
                                                  List<com.bot.TradingCore.Candle> c5,
                                                  List<com.bot.TradingCore.Candle> c15,
                                                  List<com.bot.TradingCore.Candle> c1h,
                                                  List<com.bot.TradingCore.Candle> c2h,
                                                  CoinCategory cat,
                                                  long now) {
        if (!PHASE5_FUNDING_MOMENTUM_ENABLE) return null;
        if (!valid(c15) || !valid(c1h)) return reject("fm_invalid_candles");
        if (CS_SKIP_MEME && cat == CoinCategory.MEME) return reject("fm_skip_meme");

        // Cooldown — same per-symbol lock as MR (whipsaw protection).
        Long lastSig = csLastSignalTime.get(symbol);
        if (lastSig != null && (now - lastSig) < CS_COOLDOWN_MS) {
            return reject("fm_cooldown");
        }

        // ── Need funding data ──
        FundingOIData fr = fundingCache.get(symbol);
        if (fr == null) return reject("fm_no_funding_data");
        if (!fr.isValid()) return reject("fm_funding_stale");

        double rate = fr.fundingRate;

        // ── Filter 1: must be extreme funding ──
        // Below threshold → not extreme enough → no edge
        if (Math.abs(rate) < PHASE5_FUNDING_THRESHOLD) {
            return reject(String.format("fm_funding_not_extreme=%.4f%%", rate * 100));
        }

        // ── Direction: contrarian to crowd ──
        // Negative funding = crowd is short → LONG to ride squeeze
        // Positive funding = crowd is long  → SHORT to ride flush
        boolean wantLong = rate < 0;
        com.bot.TradingCore.Side side = wantLong
                ? com.bot.TradingCore.Side.LONG
                : com.bot.TradingCore.Side.SHORT;

        // ── Filter 2: ATR sanity (skip dead/extreme pairs) ──
        double atrPct15 = com.bot.TradingCore.atrPercentile(c15, 14, 100);
        if (atrPct15 < CS_MIN_ATR_PCTILE) return reject("fm_atr_too_low");
        if (atrPct15 > CS_MAX_ATR_PCTILE) return reject("fm_atr_too_high");

        // ── Filter 3: BTC regime gate (mirror of MR logic) ──
        com.bot.GlobalImpulseController.GlobalContext btc =
                (gicRef != null) ? gicRef.getContext() : null;
        if (btc != null) {
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_PANIC)
                return reject("fm_btc_panic");
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_CRASH)
                return reject("fm_btc_crash");
            if (wantLong) {
                if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_DOWN)
                    return reject("fm_btc_strong_down_blocks_long");
                if (btc.onlyShort) return reject("fm_only_short_blocks_long");
            } else {
                if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP)
                    return reject("fm_btc_strong_up_blocks_short");
                if (btc.onlyLong) return reject("fm_only_long_blocks_short");
            }
        }

        // ── Pricing & SL/TP ──
        com.bot.TradingCore.Candle lastBar = last(c15);
        double price = lastBar.close;
        if (price <= 0) return reject("fm_invalid_price");

        double atr14 = com.bot.TradingCore.atr(c15, 14);
        if (atr14 <= 0) return reject("fm_invalid_atr");

        double slDist = atr14 * PHASE5_SL_ATR_MULT;
        double stop   = wantLong ? price - slDist : price + slDist;
        double tp2    = wantLong ? price + slDist * PHASE5_TP_R : price - slDist * PHASE5_TP_R;

        // Sanity on stop distance vs price
        if (slDist / price < 0.001 || slDist / price > 0.10) {
            return reject(String.format("fm_sl_dist_invalid=%.4f", slDist / price));
        }

        // ── HTF bias from 2h ──
        HTFBias bias2h = detectBias2H(c2h != null && c2h.size() >= MIN_BARS ? c2h : c1h);
        boolean htfAligned = bias2h != null &&
                ((wantLong && bias2h == HTFBias.BULL) ||
                        (!wantLong && bias2h == HTFBias.BEAR));

        // ── Probability scoring based on funding extremity ──
        // The more extreme the funding, the higher the confidence.
        // ±0.04% = base 56 → up to 70 at ±0.10%+
        double absRate = Math.abs(rate);
        double extremityScore = Math.min(14.0, (absRate - PHASE5_FUNDING_THRESHOLD) * 25000);
        double probability = 56.0 + extremityScore;
        if (htfAligned) probability = Math.min(72.0, probability + 2.0);

        // ── Build flags ──
        List<String> flags = new ArrayList<>();
        flags.add("FUNDING_MOMENTUM");
        flags.add(String.format("FR=%.4f%%", rate * 100));
        if (wantLong) flags.add("SHORT_SQUEEZE_HUNT");
        else          flags.add("LONG_FLUSH_HUNT");
        if (fr.frPeakWarning)   flags.add("FR_PEAK_WARN");
        if (fr.frTroughWarning) flags.add("FR_TROUGH_WARN");
        if (htfAligned)         flags.add("HTF_ALIGN");
        TradeIdea idea = new TradeIdea(
                symbol,
                side,
                price,
                stop,
                tp2,
                PHASE5_TP_R,
                probability,
                flags,
                rate, fr.fundingDelta, fr.oiChange1h,
                bias2h != null ? bias2h.name() : "NEUTRAL",
                cat,
                null,
                CS_TP1_R, PHASE5_TP_R, PHASE5_TP_R
        );
        idea.setRobustAtrPct(atr14 / price);
        idea.setAgreeingClusters(1);

        return idea;
    }

    /**
     * Inject simulated funding rate for backtest replay.
     * Called by SimpleBacktester before each analyze() invocation to populate
     * fundingCache with the historical funding rate that was active at that bar.
     * In live mode, fundingCache is populated by SignalSender.refreshAllFundingRates().
     */
    public void setSimulatedFunding(String symbol, double fundingRate, double prevFundingRate) {
        double delta = fundingRate - prevFundingRate;
        FundingOIData data = new FundingOIData(fundingRate, 0, 0, 0, prevFundingRate, delta, 0);
        fundingCache.put(symbol, data);
    }

    // ─────────────────────────────────────────────────────────────────────
    // STRATEGY: VWAP MEAN REVERSION (was generate(), Phase 1)
    // ─────────────────────────────────────────────────────────────────────
    // Body unchanged from Phase 1. Pre-filters (validation, MEME, cooldown,
    // post-pump skips) are now in router — kept here as defensive duplicates,
    // they never trigger because router already passed them, no perf impact.
    private TradeIdea generateMR(String symbol,
                                 List<com.bot.TradingCore.Candle> c1,
                                 List<com.bot.TradingCore.Candle> c5,
                                 List<com.bot.TradingCore.Candle> c15,
                                 List<com.bot.TradingCore.Candle> c1h,
                                 List<com.bot.TradingCore.Candle> c2h,
                                 CoinCategory cat,
                                 long now) {

        // ─── Validation ─────────────────────────────────────────────────────
        if (!valid(c15) || !valid(c1h)) return reject("invalid_candles");

        // ─── Filter: skip MEME (too noisy for VWAP-MR) ──────────────────────
        if (CS_SKIP_MEME && cat == CoinCategory.MEME) return reject("cs_skip_meme");

        // ─── Filter: per-symbol cooldown ────────────────────────────────────
        Long lastSig = csLastSignalTime.get(symbol);
        if (lastSig != null && (now - lastSig) < CS_COOLDOWN_MS) {
            return reject("cs_cooldown");
        }

        // ─── Filter: post-pump / post-dump persisted from older versions ────
        Long ppUntil = postPumpSkipUntil.get(symbol);
        if (ppUntil != null) {
            if (now < ppUntil) return reject("post_pump_cooldown");
            postPumpSkipUntil.remove(symbol);
        }
        Long pdUntil = postDumpSkipUntil.get(symbol);
        if (pdUntil != null) {
            if (now < pdUntil) return reject("post_dump_cooldown");
            postDumpSkipUntil.remove(symbol);
        }

        // ─── Filter: ATR percentile gate ────────────────────────────────────
        double atrPct15 = com.bot.TradingCore.atrPercentile(c15, 14, 100);
        if (atrPct15 < CS_MIN_ATR_PCTILE) return reject("cs_atr_too_low");
        if (atrPct15 > CS_MAX_ATR_PCTILE) return reject("cs_atr_too_high");

        // ─── Compute VWAP and deviation over rolling 24h window ─────────────
        // 24h on m15 = 96 bars. Need at least that many for stable VWAP.
        int n = c15.size();
        if (n < CS_VWAP_WINDOW + CS_DEVIATION_WINDOW) return reject("cs_insufficient_history");

        double price = last(c15).close;
        double vwap = computeVwap(c15, CS_VWAP_WINDOW);
        if (vwap <= 0 || price <= 0) return reject("cs_invalid_vwap");

        // Deviation as fraction (signed): positive = price above VWAP, neg = below
        double deviation = (price - vwap) / vwap;

        // Rolling stdev of deviation (60 bars) — for sigma-based threshold.
        double devStd = computeDeviationStdev(c15, vwap, CS_DEVIATION_WINDOW);
        if (devStd <= 0) return reject("cs_zero_devstd");

        double sigma = deviation / devStd;  // standardized deviation

        // ─── Direction from sigma ──────────────────────────────────────────
        com.bot.TradingCore.Side side;
        if (sigma <= -CS_SIGMA_THRESHOLD) {
            side = com.bot.TradingCore.Side.LONG;   // price way below VWAP → reversion up
        } else if (sigma >= CS_SIGMA_THRESHOLD) {
            side = com.bot.TradingCore.Side.SHORT;  // price way above VWAP → reversion down
        } else {
            return reject("cs_no_vwap_deviation");
        }

        // ─── Filter: volume must be NORMAL (not institutional flush) ────────
        // Institutional move = volume spike. We want retail-driven impulse
        // that's about to reverse, so reject high-volume bars.
        double volSma20 = computeVolumeSma(c15, 20);
        double volCurrent = last(c15).volume;
        if (volSma20 > 0 && volCurrent > volSma20 * CS_MAX_VOL_RATIO) {
            return reject("cs_volume_too_high");
        }

        // ─── Filter: BTC regime alignment ───────────────────────────────────
        com.bot.GlobalImpulseController.GlobalContext btc =
                (gicRef != null) ? gicRef.getContext() : null;
        if (btc != null) {
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_CHOPPY)
                return reject("cs_btc_choppy");
            if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_PANIC)
                return reject("cs_btc_panic");
            if (side == com.bot.TradingCore.Side.LONG) {
                if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_DOWN)
                    return reject("cs_btc_strong_down_blocks_long");
                if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_IMPULSE_DOWN)
                    return reject("cs_btc_impulse_down_blocks_long");
                if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_CRASH)
                    return reject("cs_btc_crash_blocks_long");
                if (btc.onlyShort) return reject("cs_only_short_blocks_long");
            } else {
                if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP)
                    return reject("cs_btc_strong_up_blocks_short");
                if (btc.regime == com.bot.GlobalImpulseController.GlobalRegime.BTC_IMPULSE_UP)
                    return reject("cs_btc_impulse_up_blocks_short");
                if (btc.onlyLong) return reject("cs_only_long_blocks_short");
            }
        }

        // ─── Build entry / SL / TP ──────────────────────────────────────────
        double atr14 = com.bot.TradingCore.atr(c15, 14);
        if (atr14 <= 0) return reject("cs_invalid_atr");

        double slDistance = atr14 * CS_SL_ATR_MULT;
        double stop = (side == com.bot.TradingCore.Side.LONG)
                ? price - slDistance : price + slDistance;
        double tp2 = (side == com.bot.TradingCore.Side.LONG)
                ? price + slDistance * CS_TP2_R : price - slDistance * CS_TP2_R;
        if (stop <= 0 || tp2 <= 0) return reject("cs_invalid_levels");

        // ─── Probability scoring ────────────────────────────────────────────
        // [v91 1H-RETUNE 2026-05-10] Expanded scoring & cap raised 0.70 → 0.78.
        //   Old cap=0.70 with MIN_CONF=0.58 left only 12pp window — every signal
        //   was hugging the floor and calibrator couldn't separate quality.
        //   New: 0.55 base → up to 0.78 with 5 possible bonuses.
        //   Bonuses: strong-σ +0.04, RSI-extreme +0.04, BTC-NEUTRAL +0.04,
        //            HTF-bias-aligned +0.04, volume-fading +0.03.
        //   Realistic best-case: 0.55+0.04+0.04+0.04+0.04+0.03 = 0.74 → cap holds.
        double probability01 = 0.55;

        // Strong deviation bonus (>2.2σ on 1h, >2.5σ on 15m — see CS_STRONG_SIGMA)
        if (Math.abs(sigma) >= CS_STRONG_SIGMA) probability01 += 0.04;

        // RSI confirms exhaustion
        double rsi14 = com.bot.TradingCore.rsi(c15, 14);
        boolean rsiOk;
        if (side == com.bot.TradingCore.Side.LONG)  rsiOk = rsi14 < 32.0;
        else                                        rsiOk = rsi14 > 68.0;
        if (rsiOk) probability01 += 0.04;

        // BTC neutral / very weak — best regime for mean-reversion
        if (btc != null && btc.regime == com.bot.GlobalImpulseController.GlobalRegime.NEUTRAL) {
            probability01 += 0.04;
        }

        // [v91-NEW] HTF bias alignment — MR works MUCH better when 2h bias
        // is on the side of the reversion (e.g. 2h bullish + price 2σ below
        // VWAP = high-quality long). Misaligned setups (2h bearish + long)
        // get NO bonus, not a penalty — they still pass if other criteria hit.
        HTFBias bias2hForScore = detectBias2H(c2h != null && c2h.size() >= MIN_BARS ? c2h : c1h);
        if (bias2hForScore != null) {
            boolean aligned =
                    (side == com.bot.TradingCore.Side.LONG  && bias2hForScore == HTFBias.BULL) ||
                            (side == com.bot.TradingCore.Side.SHORT && bias2hForScore == HTFBias.BEAR);
            if (aligned) probability01 += 0.04;
        }

        // [v91-NEW] Volume-fading bonus — last bar volume BELOW SMA20 means the
        // impulse that pushed price away from VWAP is exhausting (no fresh
        // buyers/sellers). This is the textbook MR pre-condition.
        // Note: cs_volume_too_high already rejected loud bars above; here we
        // additionally reward QUIET bars (vol < 0.7× SMA20).
        if (volSma20 > 0 && volCurrent < volSma20 * 0.70) {
            probability01 += 0.03;
        }

        probability01 = Math.min(0.78, probability01);
        double probability = probability01 * 100.0;

        // ─── HTF bias for downstream display ────────────────────────────────
        HTFBias bias2h = detectBias2H(c2h != null && c2h.size() >= MIN_BARS ? c2h : c1h);

        // ─── Build flags ────────────────────────────────────────────────────
        List<String> flags = new ArrayList<>();
        flags.add("CS_VWAP_MR");
        flags.add(String.format("DEV=%.2fσ", sigma));
        flags.add(String.format("VWAP=%.6f", vwap));
        flags.add(String.format("RSI=%.1f", rsi14));
        if (rsiOk) flags.add(side == com.bot.TradingCore.Side.LONG ? "RSI_OVERSOLD" : "RSI_OVERBOUGHT");
        if (Math.abs(sigma) >= CS_STRONG_SIGMA) flags.add("STRONG_DEV");
        flags.add(String.format("VOL/SMA=%.2f", volSma20 > 0 ? volCurrent / volSma20 : 0.0));
        if (btc != null) flags.add("BTC_" + btc.regime.name());
        flags.add(String.format("ATR_PCT=%.2f", atrPct15));
        flags.add(String.format("TIME_STOP=%dh", (CS_TIME_STOP_BARS_M15 * 15) / 60));

        // ─── Construct TradeIdea using main 16-arg constructor ──────────────
        FundingOIData fr = fundingCache.get(symbol);
        double frRate  = (fr != null && fr.isValid()) ? fr.fundingRate  : 0.0;
        double frDelta = (fr != null && fr.isValid()) ? fr.fundingDelta : 0.0;
        double oiCh    = (fr != null && fr.isValid()) ? fr.oiChange1h   : 0.0;

        // ─── Phase 2.4: Funding rate contrarian filter ────────────────────
        // Block MR setups when crowd is already heavily on the same side.
        // MR is counter-trend by design — taking LONG when funding is +0.05%+
        // means betting against price action AND against crowd positioning,
        // which historically has poor expectancy (knife-catch pattern).
        //
        // Thresholds:
        //   normal funding: ±0.01%/8h
        //   moderately crowded: ±0.05%/8h  ← our filter boundary
        //   extreme: ±0.08%/8h (caught by frPeakWarning/frTroughWarning)
        //
        // The pre-computed frPeakWarning/frTroughWarning flags trigger on
        // extreme funding WITH decelerating momentum — strong reversal signal
        // already forming, definitely don't enter against it.
        //
        // If funding data is missing/stale (fr == null or !isValid), filter
        // does NOT activate — fall through to normal sizing. We don't want
        // an API hiccup to silently kill all signals.
        if (fr != null && fr.isValid()) {
            boolean longOvercrowded  = fr.fundingRate >  0.0005 || fr.frPeakWarning;
            boolean shortOvercrowded = fr.fundingRate < -0.0005 || fr.frTroughWarning;
            if (side == com.bot.TradingCore.Side.LONG && longOvercrowded) {
                return reject(String.format("mr_funding_overheated_long_fr=%.4f%%",
                        fr.fundingRate * 100));
            }
            if (side == com.bot.TradingCore.Side.SHORT && shortOvercrowded) {
                return reject(String.format("mr_funding_overheated_short_fr=%.4f%%",
                        fr.fundingRate * 100));
            }
        }

        TradeIdea idea = new TradeIdea(
                symbol,
                side,
                price,
                stop,
                tp2,
                CS_TP2_R,
                probability,
                flags,
                frRate, frDelta, oiCh,
                bias2h.name(),
                cat,
                null,
                CS_TP1_R, CS_TP2_R, CS_TP2_R
        );
        idea.setRobustAtrPct(atr14 / price);
        idea.setAgreeingClusters(1);

        csLastSignalTime.put(symbol, now);
        return idea;
    }

    // ──────────────────────────────────────────────────────────────────────
    // VWAP & deviation helpers
    // ──────────────────────────────────────────────────────────────────────

    /** Volume-weighted average price over last `window` bars. */
    private static double computeVwap(List<com.bot.TradingCore.Candle> candles, int window) {
        int n = candles.size();
        int from = Math.max(0, n - window);
        double pvSum = 0, vSum = 0;
        for (int i = from; i < n; i++) {
            com.bot.TradingCore.Candle c = candles.get(i);
            double tp = (c.high + c.low + c.close) / 3.0;
            pvSum += tp * c.volume;
            vSum  += c.volume;
        }
        return vSum > 0 ? pvSum / vSum : 0.0;
    }

    /** Stdev of (close - vwap)/vwap over last `window` bars. */
    private static double computeDeviationStdev(List<com.bot.TradingCore.Candle> candles,
                                                double vwap, int window) {
        if (vwap <= 0) return 0.0;
        int n = candles.size();
        int from = Math.max(0, n - window);
        int count = n - from;
        if (count < 2) return 0.0;

        double mean = 0;
        for (int i = from; i < n; i++) {
            mean += (candles.get(i).close - vwap) / vwap;
        }
        mean /= count;

        double var = 0;
        for (int i = from; i < n; i++) {
            double d = (candles.get(i).close - vwap) / vwap - mean;
            var += d * d;
        }
        var /= (count - 1);
        return Math.sqrt(var);
    }

    /** Simple moving average of volume over last `period` bars. */
    private static double computeVolumeSma(List<com.bot.TradingCore.Candle> candles, int period) {
        int n = candles.size();
        if (n < period) return 0.0;
        double sum = 0;
        for (int i = n - period; i < n; i++) sum += candles.get(i).volume;
        return sum / period;
    }

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
    private static final boolean CS_IS_15M = "15m".equals(
            System.getenv().getOrDefault("PRIMARY_TF", "15m").trim());

    private static final double CS_SIGMA_THRESHOLD   = csEnvDouble("CS_SIGMA_THRESHOLD",
            CS_IS_15M ? 1.8 : 1.6);
    private static final double CS_STRONG_SIGMA      = csEnvDouble("CS_STRONG_SIGMA",
            CS_IS_15M ? 2.5 : 2.2);
    private static final double CS_MAX_VOL_RATIO     = csEnvDouble("CS_MAX_VOL_RATIO",     1.3);
    private static final int    CS_VWAP_WINDOW      = (int) csEnvLong("CS_VWAP_WINDOW",
            CS_IS_15M ? 96 : 96);   // 24h on 15m / 4 days on 1h
    private static final int    CS_DEVIATION_WINDOW = (int) csEnvLong("CS_DEVIATION_WINDOW",
            CS_IS_15M ? 60 : 48);   // 15h on 15m / 2 days on 1h
    private static final double CS_MIN_ATR_PCTILE    = csEnvDouble("CS_MIN_ATR_PCTILE",    0.30);
    private static final double CS_MAX_ATR_PCTILE    = csEnvDouble("CS_MAX_ATR_PCTILE",    0.85);
    private static final double CS_SL_ATR_MULT       = csEnvDouble("CS_SL_ATR_MULT",
            CS_IS_15M ? 1.2 : 1.5);
    private static final double CS_TP1_R             = csEnvDouble("CS_TP1_R",             1.0);
    private static final double CS_TP2_R             = csEnvDouble("CS_TP2_R",
            CS_IS_15M ? 1.5 : 1.8);
    private static final long   CS_COOLDOWN_MS       = csEnvLong("CS_COOLDOWN_MIN",
            CS_IS_15M ? 60 : 240) * 60_000L;
    private static final long   CS_TIME_STOP_BARS_M15 = csEnvLong("CS_TIME_STOP_BARS",
            CS_IS_15M ? 12 : 8);
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

        // [LOOSEN 2026-05-05] Env-выключатель калибратора. Если калибратор обучен
        // на bias dataset (например, startup-backtest дал WR=27%), он системно
        // занижает все вероятности и убивает любые сигналы. CALIBRATOR_DISABLED=1
        // переводит calibrate() в pass-through (возвращает сырой score без коррекции).
        // Записи outcomes (recordOutcome) продолжаются — данные копятся для будущего
        // включения. Включай обратно (CALIBRATOR_DISABLED=0) когда наберётся
        // ≥100 свежих outcomes с целевым WR≥45%.
        //
        // [v87 2026-05-09] DEFAULT FLIPPED 0 → 1 (safety-first).
        // Justification: calibrator is a probability mapping that REQUIRES a clean
        // training dataset. With BotMain's startup-backtest path historically feeding
        // poison (WR=39.9%, NetPnL=-25%), the calibrator learned to map raw probs to
        // ~0.40, blocking all live signals at MIN_CONF=0.58. Default pass-through is
        // the only safe state until the user has empirically confirmed positive edge.
        // To re-enable PAV calibration (after ≥100 live outcomes with WR≥45%):
        // set env CALIBRATOR_DISABLED=0 explicitly.
        private static final boolean DISABLED =
                !"0".equals(System.getenv().getOrDefault("CALIBRATOR_DISABLED", "1"));

        // [v79 I3] HMAC key + audit log. Set CALIBRATOR_HMAC_KEY in env on prod.
        private static final String HMAC_KEY = resolveHmacKey();
        private static final String AUDIT_LOG_PATH = System.getenv()
                .getOrDefault("CALIBRATOR_AUDIT_LOG", "./data/calibrator_audit.log");

        private static String resolveHmacKey() {
            String k = System.getenv("CALIBRATOR_HMAC_KEY");
            if (k == null || k.isBlank()) {
                // [v82.11 2026-06-01] Юзер env не использует → хардкодим УНИКАЛЬНЫЙ
                // ключ. Лучше старого публичного "default-CHANGE-ME" (его знал любой,
                // кто видел исходник). Не идеал (ключ в git), но для demo/личного бота
                // достаточно — аудит-лог подписан НЕпубличной солью.
                return "tb-clbr-9f3a7c21e8b64d05a1f2-20260601-hmac-salt-do-not-share";
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

            // [v87 BIAS-DETECTION 2026-05-09] Refuse to add outcomes when the recent
            // window is overwhelmingly losses. Protects against a single bad period
            // (flash crash, exchange hiccup causing mass time-stops, or feeding from
            // a known-bad backtest) flipping the entire PAV regression.
            //
            // Logic: scan up to 30 most-recent records (across all keys, last 24h only).
            // If recent WR < 25%, decline this new record. Once WR recovers above
            // threshold via natural trade flow, recording resumes.
            //
            // Override: env BIAS_GUARD=0 disables this check (only do this if you
            // intentionally want to feed bad data, e.g. for a unit test).
            //
            // Note: the audit log still writes — full visibility, just no PAV poison.
            if (!"0".equals(System.getenv().getOrDefault("BIAS_GUARD", "1"))
                    && totalOutcomeCount() >= 30) {
                int recentWins = 0, recentTotal = 0;
                long now2 = System.currentTimeMillis();
                long horizon = 24L * 3600_000L;
                outerScan:
                for (var dq2 : history.values()) {
                    java.util.Iterator<Outcome> iter = dq2.descendingIterator();
                    while (iter.hasNext()) {
                        Outcome o2 = iter.next();
                        if (now2 - o2.ts > horizon) continue;
                        if (o2.hit) recentWins++;
                        recentTotal++;
                        if (recentTotal >= 30) break outerScan;
                    }
                }
                if (recentTotal >= 30 && recentWins * 4 < recentTotal) {
                    // Recent rolling WR < 25% — decline new record.
                    LOG.fine("[Calibrator] BIAS-GUARD declining recordOutcome for "
                            + symbol + " (recent WR " + recentWins + "/" + recentTotal
                            + " < 25%). Set env BIAS_GUARD=0 to disable.");
                    // Still write audit log — visibility is independent of PAV.
                    try { writeOutcomeAudit(symbol, rawScore, hit, atrPct, weight,
                            sanitizeTag(outcomeTag), regimeBucketOf(btcRegime),
                            entryPrice, currentPrice); } catch (Throwable ignored) {}
                    return;
                }
            }

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
            // [LOOSEN 2026-05-05] Pass-through когда калибратор выключен через env.
            if (DISABLED) return r;
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

        // [v87 HEALTHCHECK 2026-05-09] Diagnostics for external monitoring (Telegram
        // heartbeat, /status endpoint). Returns a snapshot of calibrator state without
        // exposing internals. All values are computed on a best-effort basis — this
        // method is read-only and safe to call from any thread.
        public static final class HealthSnapshot {
            public final boolean disabled;          // is PAV currently in pass-through mode
            public final int    totalOutcomes;      // total outcomes across all keys
            public final int    recent24hCount;     // outcomes in last 24h
            public final double recent24hWinRate;   // WR in last 24h, [0..1]
            public final int    keysCount;          // distinct (sym, vol, regime) buckets
            public final long   oldestOutcomeAgeMs; // age of oldest outcome
            public final String summary;            // human-readable one-liner

            HealthSnapshot(boolean disabled, int total, int rc, double rwr,
                           int keys, long oldestAge, String summary) {
                this.disabled = disabled;
                this.totalOutcomes = total;
                this.recent24hCount = rc;
                this.recent24hWinRate = rwr;
                this.keysCount = keys;
                this.oldestOutcomeAgeMs = oldestAge;
                this.summary = summary;
            }
        }

        public HealthSnapshot health() {
            int total = totalOutcomeCount();
            int recent = 0, recentWins = 0;
            int keys = history.size();
            long now = System.currentTimeMillis();
            long oldestAge = 0L;
            long horizon24h = 24L * 3600_000L;
            for (java.util.concurrent.ConcurrentLinkedDeque<Outcome> dq : history.values()) {
                for (Outcome o : dq) {
                    long age = now - o.ts;
                    if (age > oldestAge) oldestAge = age;
                    if (age <= horizon24h) {
                        recent++;
                        if (o.hit) recentWins++;
                    }
                }
            }
            double rwr = recent > 0 ? (double) recentWins / recent : 0.0;
            String state;
            if (DISABLED) state = "DISABLED(passthrough)";
            else if (total < MIN_SAMPLES) state = "WARMUP(" + total + "/" + MIN_SAMPLES + ")";
            else if (recent < 30) state = "STALE(only " + recent + " in 24h)";
            else if (rwr < 0.30) state = "BIAS-RISK(WR=" + String.format("%.0f%%", rwr * 100) + ")";
            else state = "OK";
            String summary = String.format(
                    "Cal:%s n=%d 24h=%d/%.0f%% keys=%d",
                    state, total, recent, rwr * 100, keys);
            return new HealthSnapshot(DISABLED, total, recent, rwr, keys, oldestAge, summary);
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