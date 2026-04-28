package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * BotMain v78 — LOW-LATENCY + SILENT (production)
 *
 *  Mode: PURE SIGNAL SCANNER. No auto-trade, no order execution.
 *        Bot watches top-30 coins by volume and emits manual-trade signals
 *        with Entry / TP1 / TP2 / TP3 / SL.
 *
 *  CORE DESIGN: Every signal → Telegram path goes through Dispatcher.dispatch().
 *               No direct telegram.sendMessageAsync(idea.toTelegramString())
 *               anywhere. Enforced in SignalSender.flushEarlyTickBuffer and
 *               SignalSender hot-pair rescan too.
 *
 *  v78 — DEAD-CODE CLEANUP (post v77):
 *    - Removed wasDispatchedToUser() method (no callers since v77 SL/TP HIT removal).
 *    - Removed lastForecastReportMs / FORECAST_REPORT_INTERVAL_MS / lastSignalQualityReport
 *      / SIGNAL_QUALITY_REPORT_MS / buildSignalQualityReport() — all referenced only
 *      by removed Telegram-spam code paths.
 *    - Daily Report (09:00 UTC) REMOVED. User wants strictly entries-only chat.
 *      Stats remain visible in [STATS] log line every 30min for operators.
 *    - sentToUser kept as accounting field (used by Dispatcher tracking).
 *
 *  v77 — anti-lag + clean chat (preserved):
 *    - Live-splice always active (RSI veto removed)
 *    - 15m cache TTL 2min → 30s, 1h TTL 55min → 5min, 2h 110min → 15min
 *    - LATE_HARD_BLOCK → soft penalty (size cut + 0.85× score)
 *    - momentum_exhausted same-direction → soft penalty (was hard reject)
 *    - vel_decay_late → soft penalty (was hard reject)
 *    - LM_HARD_ATR 2.5 → 4.5 (genuine parabolic only)
 *    - EARLY_TICK velocity floors halved (TOP 0.0008, ALT 0.0012, MEME 0.0020)
 *    - HOT_PAIR thresholds halved (TOP 0.15%, ALT 0.30%, MEME 0.50%)
 *    - HOT_PAIR cooldown 10min → 3min, MAX_EARLY_TICK_PER_HOUR 2 → 4
 *    - EVENT_COIN day-move floors raised (TOP 5→8%, ALT 8→12%, MEME 12→18%)
 *    - Stale-data guards tightened (15m 20min→10min, 1h 2h→30min)
 *    - Cold-start COLD_START_MIN_OUTCOMES 50 → 20
 *    - Phase 1: 52/55 → 48/51, Phase 2/3: 50/53 with 1-cluster
 *    - SYMBOL_DEDUP_MS 15min → 6min, post-dispatch cooldown 8min → 3min
 *    - MIN_SL_PCT 0.40% → 0.30% (tighter stop, earlier entry, same R:R)
 *    - REMOVED Telegram spam: TP1/SL HIT, TIME STOP, drought, watchdog,
 *      market alerts, forecast accuracy, walk-forward alerts, daily report
 */
public final class BotMain {

    private static final Logger LOG = Logger.getLogger(BotMain.class.getName());

    private static final String TG_TOKEN = requireEnv("TELEGRAM_TOKEN");
    private static final String CHAT_ID  = requireEnv("TELEGRAM_CHAT_ID");

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank())
            throw new IllegalStateException("Required env var missing: " + name);
        return v;
    }

    private static volatile ZoneId ZONE = ZoneId.of("Europe/Warsaw");
    private static final int INTERVAL = envInt("SIGNAL_INTERVAL_MIN", 1);

    // [v78.1] Paper/observation mode. When OBSERVATION_MODE=1, signals reach
    // Telegram tagged 🧪 [PAPER]. Calibrator still records outcomes, so the bot
    // learns without budget risk. Required for first 50+ outcomes before any
    // real-money consideration. Default OFF (live mode), but strongly recommended
    // ON for the first 7-14 days after deploy.
    public static final boolean OBSERVATION_MODE =
            "1".equals(System.getenv().getOrDefault("OBSERVATION_MODE", "0"));
    // [v66] Read BOTH env names so one Railway variable can configure the whole bot.
    // Previously BotMain read KLINES_LIMIT (default 160) while SignalSender read KLINES
    // (default 160, now 420). Setting only one name left the other at 160 → BTC context
    // fetch could lag. Now: KLINES wins if set, else KLINES_LIMIT, else default 420.
    // 420 > 400 gate in processPair; BTC GIC needs >30 bars — both covered.
    private static final int KLINES = envIntAny(420, "KLINES", "KLINES_LIMIT");
    private static final int MAX_SIGNALS_PER_CYCLE = envInt("MAX_SIGNALS_PER_CYCLE", 3);

    private static final Map<String, String> SECTOR_LEADERS = new LinkedHashMap<>() {{
        put("DOGEUSDT", "MEME"); put("SOLUSDT", "L1"); put("UNIUSDT", "DEFI");
        put("LINKUSDT", "INFRA"); put("ETHUSDT", "TOP"); put("XRPUSDT", "PAYMENT");
        put("AVAXUSDT", "L1"); put("BNBUSDT", "CEX");
    }};

    private static final AtomicLong totalCycles  = new AtomicLong(0);
    private static final AtomicLong totalSignals = new AtomicLong(0);
    private static final AtomicLong errorCount   = new AtomicLong(0);
    private static final AtomicLong forecastSeq  = new AtomicLong(0);
    private static long startTimeMs = 0;

    private static final int  CB_THRESHOLD = 10;
    private static final long CB_WINDOW_MS = 10 * 60_000L;
    private static final long CB_PAUSE_MS  = 60_000L;
    private static volatile long lastErrorWindowStart = 0;
    private static final AtomicInteger errorsInWindow = new AtomicInteger(0);
    private static volatile long cbPauseUntil = 0;

    private static volatile long lastSignalMs       = 0;
    private static volatile long lastCycleSuccessMs = 0;
    private static volatile long lastStatsSuccessMs = 0;
    private static final long SIGNAL_DROUGHT_MS = 3 * 60 * 60_000L;
    private static final AtomicBoolean droughtAnnounced = new AtomicBoolean(false);
    private static volatile long lastInfraAlertMs = 0;
    private static final long INFRA_ALERT_COOLDOWN_MS = 60 * 60_000L;
    private static final AtomicLong watchdogAlerts = new AtomicLong(0);

    // [v78] lastSummaryDay removed (daily summary feature removed).

    private static final int MAX_FORECAST_RECORDS = 500;
    static final ConcurrentHashMap<String, ForecastRecord> forecastRecords = new ConcurrentHashMap<>();

    static final class ForecastRecord {
        final String symbol;
        final com.bot.TradingCore.Side side;
        final double entryPrice;
        final String forecastBias;
        final double forecastScore;
        final double signalProbability;
        final double robustAtrPctAtSignal;
        final double tp1Level;
        final double slLevel;
        final long   createdAt;
        volatile boolean resolved = false;
        volatile String  actualOutcome = null;
        final AtomicBoolean counted = new AtomicBoolean(false);

        ForecastRecord(String sym, com.bot.TradingCore.Side side, double price,
                       String bias, double score, double signalProb, double robustAtrPct,
                       double tp1, double sl) {
            this.symbol = sym; this.side = side; this.entryPrice = price;
            this.forecastBias = bias; this.forecastScore = score;
            this.signalProbability = signalProb;
            this.robustAtrPctAtSignal = robustAtrPct;
            this.tp1Level = tp1; this.slLevel = sl;
            this.createdAt = System.currentTimeMillis();
        }
        long ageMs() { return System.currentTimeMillis() - createdAt; }
    }

    private static final AtomicInteger forecastTotal   = new AtomicInteger(0);
    private static final AtomicInteger forecastCorrect = new AtomicInteger(0);

    private static final class SignalOutcome {
        final String symbol; final double confidence; final String category;
        final boolean hit; final long ts;
        SignalOutcome(String s, double c, String cat, boolean h) {
            symbol = s; confidence = c; category = cat; hit = h;
            ts = System.currentTimeMillis();
        }
    }

    private static final ConcurrentLinkedDeque<SignalOutcome> signalOutcomes = new ConcurrentLinkedDeque<>();
    private static final int SIGNAL_OUTCOME_WINDOW = 200;
    // [v78] lastSignalQualityReport / SIGNAL_QUALITY_REPORT_MS / buildSignalQualityReport()
    // REMOVED — they only fed the v76 hourly Telegram broadcast which v77 silenced.
    // signalOutcomes deque kept: still pushed by Dispatcher and read by [STATS] log.

    public static void recordSignalOutcome(String sym, double conf, String cat, boolean hit) {
        signalOutcomes.addLast(new SignalOutcome(sym, conf, cat, hit));
        while (signalOutcomes.size() > SIGNAL_OUTCOME_WINDOW) signalOutcomes.pollFirst();
    }

    static final ConcurrentHashMap<String, TrackedSignal> trackedSignals = new ConcurrentHashMap<>();

    static final class TrackedSignal {
        final String symbol;
        final com.bot.TradingCore.Side side;
        final double entry, sl, tp1, tp2, tp3;
        final long createdAt;
        final String forecastBias;
        final double forecastScore;
        final boolean sentToUser;

        volatile boolean tp1Hit = false;
        volatile boolean tp2Hit = false;
        volatile boolean timeStopNotified = false;
        volatile double  trailingStop = 0;
        volatile boolean chandelierActive = false;

        private double extremeLow  = Double.MAX_VALUE;
        private double extremeHigh = Double.NEGATIVE_INFINITY;
        private final Object extremeLock = new Object();

        TrackedSignal(String sym, com.bot.TradingCore.Side side,
                      double entry, double sl, double tp1, double tp2, double tp3,
                      String forecastBias, double forecastScore, boolean sentToUser) {
            this.symbol = sym; this.side = side; this.entry = entry;
            this.sl = sl; this.tp1 = tp1; this.tp2 = tp2; this.tp3 = tp3;
            this.forecastBias = forecastBias; this.forecastScore = forecastScore;
            this.sentToUser = sentToUser;
            this.createdAt = System.currentTimeMillis();
        }
        long ageMs() { return System.currentTimeMillis() - createdAt; }
        void updateExtremes(double low, double high) {
            synchronized (extremeLock) {
                extremeLow  = Math.min(extremeLow,  low);
                extremeHigh = Math.max(extremeHigh, high);
            }
        }
        double getExtremeLow()  { synchronized (extremeLock) { return extremeLow; } }
        double getExtremeHigh() { synchronized (extremeLock) { return extremeHigh; } }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  DISPATCHER — the only gateway for trade-signal → Telegram.
    //  Every call to telegram.sendMessageAsync(idea.toTelegramString())
    //  has been replaced by Dispatcher.getInstance().dispatch(idea, src).
    //  If compile fails in SignalSender, it's because someone added a new
    //  direct dispatch path — route it through here instead.
    // ═══════════════════════════════════════════════════════════════════
    public static final class Dispatcher {

        // [v64 CRITICAL FIX] Cold-start gate redesign.
        //
        // ROOT CAUSE of 12h+ silence: EARLY_TICK TradeIdea is built via the 7-arg
        // constructor which sets forecast=null. Dispatcher read fcConf=0.00 always.
        // Flags = [EARLY_TICK, UP, vel=..., vda=...] → countClusterFlags=1 (only
        // "EARLY_TICK" matches). Phase 1 required clusters>=3 OR fcConf>=0.50 —
        // both physically impossible for EARLY_TICK → 100% block → calibrator
        // never received a single outcome → bot stuck in Phase 1 forever.
        //
        // New gate: OR-based, probability-first.
        //   Phase 1 (n<10):  prob>=73 AND (prob>=76 OR clusters>=2 OR fcConf>=0.45)
        //   Phase 2 (10-19): prob>=70 AND (prob>=74 OR clusters>=2 OR fcConf>=0.40)
        //   Phase 3 (20-49): prob>=68
        //   Phase 4 (50+):   no extra gate — calibrator is the authority
        //
        // Rationale: a standalone signal with probability>=76 (MAX_CONF=85 ceiling)
        // represents a very high-conviction setup and is allowed through even
        // without cluster confluence. This lets EARLY_TICK breathe while still
        // rejecting the 60-72% mediocre noise.
        //
        // [v76] Threshold bumped 30→50 to align with ProbabilityCalibrator.MIN_SAMPLES.
        // Old behavior: Phase 4 activated at n=30 ("calibrator is the authority"),
        // but the calibrator itself returned RAW scores (no calibration) until n=50,
        // so Phase 4 was effectively disabling extra gates while trusting an
        // un-calibrated raw score. Now Phase 4 only engages when the calibrator
        // actually has enough data to do PAV regression with stable buckets.
        // [v77 LATENCY] 50 → 20. На 50 outcomes калибратор копит ~3 дня при текущем
        // потоке сигналов — всё это время Phase 1/2/3 режут solo-сигналы. На 20
        // PAV regression уже стабильна для крипто (5 buckets × 4 точки/bucket).
        private static final int    COLD_START_MIN_OUTCOMES = 20;

        private static final double MIN_RR          = 2.00;
        // [v77 LATENCY] MIN_SL_PCT 0.40% → 0.30%. Узкий стоп = ранний вход с
        // меньшим риском при том же R:R 1:2. Slippage 0.05–0.10% на TOP/ALT —
        // приемлемо при экономии 10bp на стопе.
        private static final double MIN_SL_PCT      = 0.0030;
        // [v77 LATENCY] 15 → 6 мин. На 15m TF новый закрытый бар = новая инфа
        // каждые 15 мин. Дедуп 15 мин блокирует второй вход после ретеста уровня
        // ровно тогда, когда он самый ценный.
        private static final long   SYMBOL_DEDUP_MS = 6 * 60_000L;
        // [v75] 12 → 20 + rolling burst protection. The hard 12/h cap caused the
        // "2 signals in 8 hours" symptom: when BTC moves and a sector wave triggers,
        // the bot would fire 12 signals in 15 minutes and stay silent for the rest
        // of the hour — exactly when the user needed updates most. Now 20/h overall,
        // but with a 5-in-5-minute burst limit that lets sector waves through while
        // still protecting against runaway loops.
        private static final int    MAX_PER_HOUR     = 20;
        private static final int    MAX_PER_5MIN     = 5;   // burst protection
        private static final long   HOUR_MS          = 60 * 60_000L;
        private static final long   FIVE_MIN_MS      = 5 * 60_000L;

        private final com.bot.TelegramBotSender tg;
        private final com.bot.InstitutionalSignalCore isc;

        private final ConcurrentHashMap<String, Long> lastDispatchMs = new ConcurrentHashMap<>();
        private final ConcurrentLinkedDeque<Long> dispatchTimestamps = new ConcurrentLinkedDeque<>();
        private final AtomicLong totalDispatched = new AtomicLong(0);
        private final AtomicLong blockedByGate   = new AtomicLong(0);

        // [v62] Breakdown counters — visible in stats so we can tune gates from data.
        private final AtomicLong blockedBipolar   = new AtomicLong(0);
        private final AtomicLong blockedRR        = new AtomicLong(0);
        private final AtomicLong blockedSL        = new AtomicLong(0);
        private final AtomicLong blockedColdStart = new AtomicLong(0);
        private final AtomicLong blockedDedup     = new AtomicLong(0);
        private final AtomicLong blockedHourly    = new AtomicLong(0);

        private static volatile Dispatcher INSTANCE;

        private Dispatcher(com.bot.TelegramBotSender tg, com.bot.InstitutionalSignalCore isc) {
            this.tg = tg; this.isc = isc;
        }

        public static Dispatcher init(com.bot.TelegramBotSender tg, com.bot.InstitutionalSignalCore isc) {
            if (INSTANCE == null) INSTANCE = new Dispatcher(tg, isc);
            return INSTANCE;
        }

        public static Dispatcher getInstance() { return INSTANCE; }

        public static final class Result {
            public final boolean dispatched;
            public final String  reason;
            Result(boolean d, String r) { dispatched = d; reason = r; }
            static Result ok() { return new Result(true, "OK"); }
            static Result blocked(String why) { return new Result(false, why); }
        }

        // [PATCH 2026-04-28] HINT MODE — анти-тишина.
        // Если 2+ часа без сигнала, временно опускаем probFloor на 5 пунктов.
        // Hint-сигналы помечаются 💡 в Telegram, чтобы трейдер знал: качество ниже.
        // Решает симптом юзера: «10 часов ни одного сигнала».
        private static final long HINT_TRIGGER_MS = 2L * 60 * 60_000L;
        private volatile boolean hintModeActive = false;
        private final AtomicLong hintModeActivations = new AtomicLong(0);

        private void updateHintMode() {
            long silentMs = System.currentTimeMillis() - lastSignalMs;
            boolean shouldActivate = silentMs > HINT_TRIGGER_MS;
            if (shouldActivate != hintModeActive) {
                hintModeActive = shouldActivate;
                if (shouldActivate) hintModeActivations.incrementAndGet();
                LOG.info("[HINT_MODE] " + (shouldActivate ? "ACTIVATED" : "DEACTIVATED")
                        + " (silent " + (silentMs / 60_000L) + "min)");
            }
        }

        public boolean isHintModeActive() { return hintModeActive; }
        public long getHintModeActivations() { return hintModeActivations.get(); }

        /**
         * The single source of truth for "should this signal reach the user?"
         * Returns dispatched=true iff a Telegram message was queued.
         */
        public Result dispatch(com.bot.DecisionEngineMerged.TradeIdea idea, String source) {
            if (idea == null) return Result.blocked("null idea");

            // [PATCH] Обновляем hint mode при каждом dispatch.
            updateHintMode();

            if (!isc.isSymbolAvailable(idea.symbol)) {
                blockedByGate.incrementAndGet();
                blockedBipolar.incrementAndGet();
                return Result.blocked("bipolar/cooldown");
            }

            double riskDist = Math.abs(idea.stop - idea.price);
            double tp2Dist  = Math.abs(idea.tp2 - idea.price);
            double actualRR = riskDist > 1e-9 ? tp2Dist / riskDist : 0;
            if (actualRR < MIN_RR) {
                blockedByGate.incrementAndGet();
                blockedRR.incrementAndGet();
                return Result.blocked(String.format("R:R=%.2f<%.1f", actualRR, MIN_RR));
            }

            double slPct = riskDist / idea.price;
            if (slPct < MIN_SL_PCT) {
                blockedByGate.incrementAndGet();
                blockedSL.incrementAndGet();
                return Result.blocked(String.format("SL=%.3f%%<%.2f%%", slPct * 100, MIN_SL_PCT * 100));
            }

            // [v62] Progressive cold-start gate. Blocks borderline signals early,
            // relaxes as we collect outcomes.
            int calSamples = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            int clusters   = countClusterFlags(idea.flags);
            double fcConf  = idea.forecast != null ? idea.forecast.confidence : 0.0;

            // [v64] OR-based gate. probShortcut is the "high conviction solo pass":
            // a signal with prob >= probShortcut passes regardless of clusters/fc.
            // This lets EARLY_TICK (which has null forecast by design) through when
            // its standalone probability is strong enough.
            double probFloor;
            double probShortcut;
            int    minClusters;
            double minFcConf;

            if (calSamples >= COLD_START_MIN_OUTCOMES) {
                // Phase 4 — calibrator trusted, only RR/SL/dedup enforced.
                probFloor = 0; probShortcut = 0; minClusters = 0; minFcConf = 0;
            } else if (calSamples >= 10) {
                // [v77 LATENCY] Phase 2/3 объединены. С COLD_START=20 промежуточная
                // зона короткая. Пускаем при prob≥50 ИЛИ 1 кластер ИЛИ fcConf≥0.22.
                probFloor = 50.0; probShortcut = 52.0; minClusters = 1; minFcConf = 0.22;
            } else {
                // [PATCH 2026-04-28] Phase 1 упрощён: только probFloor, без shortcut.
                // Раньше: prob≥48 И (prob≥51 ИЛИ clusters≥1 ИЛИ fcConf≥0.20). На
                // холодном рынке EARLY_TICK даёт prob~48-50 без clusters и с
                // fcConf=0.0 (forecast=null) → ~99% сигналов теряется → калибратор
                // не наполняется → Phase 4 не наступает никогда (chicken-and-egg).
                // Теперь: только prob≥48 floor, остальное — пропускать. Калибратор
                // получит outcomes за 6-12ч и сам разрулит. Риск ложных сигналов
                // в paper/observation режиме приемлем; в LIVE — пользователь
                // должен включить OBSERVATION_MODE=1 в Railway env.
                probFloor = 48.0; probShortcut = 0; minClusters = 0; minFcConf = 0;
            }

            // [PATCH 2026-04-28] HINT MODE: при длительной тишине снижаем порог.
            // probFloor минус 5pt, минимум 43.0 — ниже этого качество слишком низкое.
            // probShortcut тоже снижается симметрично если задан.
            if (hintModeActive) {
                probFloor    = Math.max(43.0, probFloor - 5.0);
                if (probShortcut > 0) probShortcut = Math.max(45.0, probShortcut - 5.0);
                if (minClusters > 0)  minClusters  = Math.max(0, minClusters - 1);
            }

            if (probFloor > 0 && idea.probability < probFloor) {
                blockedByGate.incrementAndGet();
                blockedColdStart.incrementAndGet();
                return Result.blocked(String.format(
                        "cold-start: prob=%.0f<%.0f (n=%d)", idea.probability, probFloor, calSamples));
            }
            if (probShortcut > 0 && idea.probability < probShortcut) {
                // Didn't clear the solo-pass — require secondary confluence.
                boolean clustersOk = clusters >= minClusters;
                boolean fcOk       = fcConf   >= minFcConf;
                if (!clustersOk && !fcOk) {
                    blockedByGate.incrementAndGet();
                    blockedColdStart.incrementAndGet();
                    return Result.blocked(String.format(
                            "cold-start: prob=%.0f<shortcut=%.0f AND clusters=%d<%d AND fcConf=%.2f<%.2f (n=%d)",
                            idea.probability, probShortcut,
                            clusters, minClusters, fcConf, minFcConf, calSamples));
                }
            }

            String dedupKey = idea.symbol + "_" + idea.side.name();
            Long lastMs = lastDispatchMs.get(dedupKey);
            long now = System.currentTimeMillis();
            if (lastMs != null && now - lastMs < SYMBOL_DEDUP_MS) {
                blockedByGate.incrementAndGet();
                blockedDedup.incrementAndGet();
                return Result.blocked(String.format("dedup %ds", (now - lastMs) / 1000));
            }

            pruneDispatchTimestamps(now);
            // [v75] Two-tier rate limit:
            //   1) Hourly cap (20/h) — protects against runaway loops
            //   2) Burst cap   (5/5min) — protects Telegram from spam during sector waves
            //                              while still allowing 5 high-conviction signals
            //                              in 5 minutes when a real opportunity hits.
            if (dispatchTimestamps.size() >= MAX_PER_HOUR) {
                blockedByGate.incrementAndGet();
                blockedHourly.incrementAndGet();
                return Result.blocked("hourly cap " + MAX_PER_HOUR);
            }
            int recentBurst = 0;
            for (Long t : dispatchTimestamps) {
                if (t != null && now - t <= FIVE_MIN_MS) recentBurst++;
            }
            if (recentBurst >= MAX_PER_5MIN) {
                blockedByGate.incrementAndGet();
                blockedHourly.incrementAndGet();
                return Result.blocked("burst cap " + MAX_PER_5MIN + "/5min");
            }

            try {
                // [v78.1] Paper/observation mode — prefix Telegram message with
                // visible tag so user does NOT trade these signals with real money.
                // Calibrator still records outcomes (learning without risk).
                String tgMessage = idea.toTelegramString();
                if (OBSERVATION_MODE) {
                    tgMessage = "🧪 *PAPER MODE* — НЕ ТОРГУЙ ЖИВЫМИ ДЕНЬГАМИ\n"
                            + "_Сигнал для валидации, не для входа в рынок_\n"
                            + "━━━━━━━━━━━━━━━━━━━━━\n"
                            + tgMessage;
                }
                // [PATCH 2026-04-28] HINT MODE: явная пометка пониженной уверенности.
                // Сигнал прошёл по сниженному probFloor (после 2+ часов тишины).
                // Трейдер видит 💡 и понимает: качество ниже обычного, размер ×0.5.
                if (hintModeActive) {
                    tgMessage = "💡 *HINT* (длительная тишина — низкая уверенность)\n"
                            + "_Прошёл по сниженному порогу. Размер позиции ×0.5._\n"
                            + "━━━━━━━━━━━━━━━━━━━━━\n"
                            + tgMessage;
                }
                tg.sendMessageAsync(tgMessage);
                totalDispatched.incrementAndGet();
                lastDispatchMs.put(dedupKey, now);
                dispatchTimestamps.addLast(now);
                totalSignals.incrementAndGet();
                lastSignalMs = now;
                droughtAnnounced.set(false);
                trackSignal(idea, true);
                // [v77 LATENCY] 8 → 3 мин. Дедуп same-side 6 мин уже выше; этот
                // cooldown блокирует opposite-side. 3 мин достаточно чтобы исключить
                // мгновенный flip на одном тике, но даёт реагировать на reversals.
                isc.setSignalCooldown(idea.symbol, 3 * 60_000L);
                LOG.info(String.format("[DISPATCH/%s%s] %s %s prob=%.0f conf=%.2f rr=%.2f sl=%.2f%% n=%d",
                        source, OBSERVATION_MODE ? "/PAPER" : "",
                        idea.symbol, idea.side, idea.probability,
                        fcConf, actualRR, slPct * 100, calSamples));
                return Result.ok();
            } catch (Throwable t) {
                LOG.warning("[DISPATCH/" + source + "] " + idea.symbol + " failed: " + t.getMessage());
                return Result.blocked("exception");
            }
        }

        private void pruneDispatchTimestamps(long now) {
            while (!dispatchTimestamps.isEmpty()
                    && now - dispatchTimestamps.peekFirst() > HOUR_MS) {
                dispatchTimestamps.pollFirst();
            }
        }

        public long getTotalDispatched() { return totalDispatched.get(); }
        public long getBlockedByGate()   { return blockedByGate.get(); }
        public int  getHourlyCount() {
            pruneDispatchTimestamps(System.currentTimeMillis());
            return dispatchTimestamps.size();
        }

        /** [v62] Block-reason breakdown for stats logging. */
        public String getBlockBreakdown() {
            return String.format("bi:%d rr:%d sl:%d cold:%d dd:%d hr:%d",
                    blockedBipolar.get(), blockedRR.get(), blockedSL.get(),
                    blockedColdStart.get(), blockedDedup.get(), blockedHourly.get());
        }
    }

    private static int countClusterFlags(List<String> flags) {
        if (flags == null) return 0;
        int c = 0;
        for (String f : flags) {
            if (f == null) continue;
            String u = f.toUpperCase();
            // [v70] Added REVERSAL/EXHAUST_REV/LOCAL_REVERSAL — эти флаги теперь
            // считаются кластерами чтобы reversal setups проходили cold-start gate.
            if (u.startsWith("CLUSTER") || u.contains("TREND")
                    || u.contains("BREAKOUT") || u.contains("VSA") || u.contains("PUMP")
                    || u.contains("EXH") || u.contains("OFV_STRONG") || u.contains("OBI")
                    || u.contains("HTF_") || u.contains("BOS") || u.contains("FVG")
                    || u.contains("LIQ_MAGNET") || u.contains("DIV")
                    || u.contains("REVERSAL") || u.contains("EXHAUST_REV")
                    || u.contains("LOCAL_REVERSAL")) c++;
        }
        return c;
    }

    private static Runnable safe(String name, Runnable task) {
        return () -> {
            try { task.run(); }
            catch (Throwable t) {
                errorCount.incrementAndGet();
                boolean isTransient = t instanceof java.io.IOException ||
                        t instanceof java.net.http.HttpTimeoutException ||
                        t.getClass().getSimpleName().toLowerCase().contains("json") ||
                        (t.getMessage() != null && (t.getMessage().contains("502") || t.getMessage().contains("timeout")));
                if (!isTransient) errorsInWindow.incrementAndGet();
                LOG.log(Level.SEVERE, "[SAFE] Task '" + name + "' FAILED: " + t.getMessage(), t);
            }
        };
    }

    // ═══════════════════════════════════════════════════════════════════
    //  MAIN
    // ═══════════════════════════════════════════════════════════════════
    public static void main(String[] args) {
        try {
            System.setOut(new java.io.PrintStream(System.out, true, "UTF-8"));
            System.setErr(new java.io.PrintStream(System.err, true, "UTF-8"));
        } catch (java.io.UnsupportedEncodingException ignored) {}

        configureLogger();
        resolveTimezoneAsync();

        startTimeMs          = System.currentTimeMillis();
        lastErrorWindowStart = startTimeMs;
        lastSignalMs         = startTimeMs;
        lastCycleSuccessMs   = startTimeMs;
        lastStatsSuccessMs   = startTimeMs;

        final com.bot.TelegramBotSender telegram = new com.bot.TelegramBotSender(TG_TOKEN, CHAT_ID);
        final com.bot.GlobalImpulseController gic = new com.bot.GlobalImpulseController();
        final com.bot.InstitutionalSignalCore isc = new com.bot.InstitutionalSignalCore();
        final com.bot.SignalSender sender         = new com.bot.SignalSender(telegram, gic, isc);

        Dispatcher.init(telegram, isc);

        final String calibratorFile = System.getenv()
                .getOrDefault("CALIBRATOR_FILE", "./data/calibrator.csv");
        try {
            com.bot.DecisionEngineMerged.getCalibrator().loadFromFile(calibratorFile);
        } catch (Throwable t) {
            LOG.warning("[Calibrator] load failed: " + t.getMessage());
        }

        com.bot.DecisionEngineMerged.USER_ZONE = ZONE;

        // [v77 NO-SPAM] TIME_STOP сообщения убраны. Пользователь сам управляет
        // выходом. ISC всё равно дёргает callback для внутренних метрик — пусть
        // помечает в trackedSignals, но без уведомления в Telegram.
        isc.setTimeStopCallback((sym, msg) -> {
            for (TrackedSignal ts : trackedSignals.values()) {
                if (!ts.symbol.equals(sym)) continue;
                if (!ts.sentToUser) continue;
                if (ts.timeStopNotified) continue;
                ts.timeStopNotified = true;
                break;
            }
        });

        gic.setPanicCallback(msg -> {
            // [v77 NO-SPAM] Market Alert → log only. GIC panic is rare but it's
            // also informational (BTC crash detected etc.) — not actionable for the
            // user, and the actual signal flow already adapts via aggressiveShort/
            // longSuppression. Pure noise in the chat.
            LOG.warning("[GIC panic] " + msg);
        });

        ScheduledExecutorService mainSched = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "TradingBot-Main");
            t.setDaemon(false);
            t.setUncaughtExceptionHandler((th, ex) ->
                    LOG.log(Level.SEVERE, "UNCAUGHT in " + th.getName(), ex));
            return t;
        });
        ScheduledExecutorService auxSched = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "TradingBot-Aux");
            t.setDaemon(true);
            return t;
        });
        ExecutorService heavySched = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "TradingBot-Heavy");
            t.setDaemon(true);
            return t;
        });

        // [v62] Staggered startup. Previous v61 schedule (0s delay) caused bursts
        // of BTC + 8 sectors + 30-pair scan within 2s of boot → Binance 418 IP ban.
        // New schedule lets WS subscribe, cache warm up, then start cycles.
        //   - main cycle: first run at +90s (was 0s)
        //   - stats: first at +15min (was +30min, now slightly earlier for observability)
        //   - watchdog grace: first check at +5min to avoid alerts during warmup
        mainSched.scheduleAtFixedRate(
                safe("MainCycle", () -> runCycle(telegram, gic, isc, sender)),
                90, INTERVAL * 60L, TimeUnit.SECONDS);
        auxSched.scheduleAtFixedRate(
                safe("LogStats", () -> logStats(gic, isc, sender)),
                15, 30, TimeUnit.MINUTES);
        // [v78] DailySummary scheduler REMOVED — chat is entries-only.
        // Daily stats remain in [STATS] log line every 30min for operators.
        auxSched.scheduleAtFixedRate(
                safe("Watchdog", () -> runWatchdog(telegram, sender)),
                5 * 60, 120, TimeUnit.SECONDS);
        // [PATCH 2026-04-28] Heartbeat — внутренний кулдаун 90 мин.
        // Проверка каждые 15 мин, реально шлёт только если 90+ мин тишина.
        auxSched.scheduleAtFixedRate(
                safe("Heartbeat", () -> maybeSendHeartbeat(telegram, sender, gic, isc)),
                15, 15, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("CalibratorSave", () -> {
                    com.bot.DecisionEngineMerged.getCalibrator().saveToFile(calibratorFile);
                    int cnt = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
                    LOG.info("[Calibrator] auto-saved, total outcomes: " + cnt);
                }),
                30, 30, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("TimeSync", sender::syncServerTime),
                5, 120, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("ForecastChecker", () -> checkForecastAccuracy(sender, telegram)),
                20, 20, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("WalkForward", () -> {
                    int hourUtc = ZonedDateTime.now(ZoneOffset.UTC).getHour();
                    int dayOfYear = ZonedDateTime.now(ZoneOffset.UTC).getDayOfYear();
                    if (hourUtc == 5 && dayOfYear % 3 == 0) {
                        runWalkForwardValidation(sender, telegram);
                    }
                }),
                60, 60, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("BacktestSubmit", () -> {
                    int hourUtc = ZonedDateTime.now(ZoneOffset.UTC).getHour();
                    if (hourUtc == 3) {
                        heavySched.submit(safe("Backtest",
                                () -> runPeriodicBacktest(sender, isc)));
                    }
                }),
                60, 60, TimeUnit.MINUTES);

        final String calibratorFileFinal = calibratorFile;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("═══ Shutdown. Cycles: " + totalCycles.get()
                    + " | Signals: " + totalSignals.get() + " ═══");
            mainSched.shutdown(); auxSched.shutdown(); heavySched.shutdown();
            try { mainSched.awaitTermination(4, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            mainSched.shutdownNow();
            try {
                com.bot.DecisionEngineMerged.getCalibrator().saveToFile(calibratorFileFinal);
            } catch (Throwable ignored) {}
            telegram.flushAndShutdown(8000);
        }, "ShutdownHook"));

        telegram.sendMessageAsync(buildStartMessage());
        LOG.info("═══ TradingBot v78.2 SCANNER started " + nowLocalStr()
                + " (first cycle in 90s, OBSERVATION_MODE="
                + (OBSERVATION_MODE ? "ON/PAPER" : "OFF/LIVE") + ") ═══");

        // [v78.2 SAFETY] Send explicit warning to Telegram if running LIVE
        // without enough calibration data. The user must explicitly opt in
        // to live trading on a cold-start system; passive default = noisy chat.
        try {
            int _calN = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            if (!OBSERVATION_MODE && _calN < 50) {
                telegram.sendMessageAsync(String.format(
                        "🚨 *ВНИМАНИЕ: LIVE режим без калибровки*\n"
                                + "━━━━━━━━━━━━━━━━━━━━━\n"
                                + "Калибратор: %d/50 исходов\n"
                                + "Сигналы выходят с НЕкалиброванной вероятностью.\n"
                                + "━━━━━━━━━━━━━━━━━━━━━\n"
                                + "_Рекомендации:_\n"
                                + "• Установи `OBSERVATION_MODE=1` в Railway env\n"
                                + "• Либо торгуй размером ×0.25 от обычного\n"
                                + "• Не больше 2 позиций в одну сторону одновременно",
                        _calN));
            }
        } catch (Throwable ignored) {}

        // [v74] STARTUP SELF-VALIDATION — answers "does this strategy actually
        // have edge?" without waiting weeks. 5min after start (volume24hUSD is
        // populated by then) and every 6h after, walks 4 days of 15m history
        // through the live engine, simulates outcomes, sends a Telegram verdict.
        // Lives inside SimpleBacktester (no new top-level class). Toggle via
        // VALIDATOR_ENABLED env var.
        try {
            com.bot.SimpleBacktester.SelfValidator.start(sender, telegram, 5 * 60_000L);
        } catch (Throwable t) {
            LOG.warning("[SELF-VALIDATOR] init failed: " + t.getMessage());
        }
    }

    private static void runCycle(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        long cycleStart = System.currentTimeMillis();
        long now = System.currentTimeMillis();

        // [v62] Skip entire cycle if Binance has us under IP ban.
        // Previously v61 attempted fetchKlines 30+ times per cycle which produced
        // [HARD FAIL] spam AND kept the rate-limit window hot, extending the ban.
        if (sender.isRlBanned()) {
            LOG.info("[CYCLE] Skipped — Binance IP ban for " + sender.rlBanSecondsLeft() + "s");
            return;
        }

        if (now < cbPauseUntil) return;
        if (now - lastErrorWindowStart > CB_WINDOW_MS) {
            lastErrorWindowStart = now;
            errorsInWindow.set(0);
        }
        if (errorsInWindow.get() >= CB_THRESHOLD) {
            cbPauseUntil = now + CB_PAUSE_MS;
            errorsInWindow.set(0);
            LOG.warning("[CB] Too many errors, pause till " + formatLocalTime(cbPauseUntil));
            return;
        }

        long cycle = totalCycles.incrementAndGet();
        LOG.info("══ CYCLE #" + cycle + " ══ " + nowLocalStr());

        updateBtcContext(sender, gic);
        updateSectors(sender, gic);
        try { sender.getPumpHunter().periodicCleanup(); } catch (Throwable ignored) {}

        double bal = sender.getAccountBalance();
        if (bal > 0) isc.updateBalance(bal);
        int totalTrades = isc.getTotalTradeCount();
        if (totalTrades >= 50) {
            sender.getDecisionEngine().updateBayesPrior(isc.getOverallWinRate(), totalTrades);
        }

        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        // [PATCH 2026-04-28] Снапшот regime для heartbeat/drought (там нет gic-доступа).
        lastBtcRegimeForAlert = String.valueOf(ctx.regime);
        LOG.info("BTC: " + ctx.regime
                + " str=" + String.format("%.2f", ctx.impulseStrength)
                + " vol=" + String.format("%.2f", ctx.volatilityExpansion)
                + " | " + isc.getStats());

        List<com.bot.DecisionEngineMerged.TradeIdea> signals = sender.generateSignals();
        lastCycleSuccessMs = System.currentTimeMillis();
        if (signals == null || signals.isEmpty()) {
            LOG.info("No signals. " + isc.getStats());
            return;
        }

        signals.sort(Comparator
                .comparingDouble((com.bot.DecisionEngineMerged.TradeIdea i) -> i.probability).reversed()
                .thenComparingDouble(i -> i.forecast != null ? i.forecast.confidence : 0.0).reversed()
                .thenComparingDouble(i -> i.forecast != null ? Math.abs(i.forecast.directionScore) : 0.0).reversed());
        int limit = Math.min(signals.size(), MAX_SIGNALS_PER_CYCLE);
        List<com.bot.DecisionEngineMerged.TradeIdea> dispatchSignals =
                reorderSignalsForDispatch(new ArrayList<>(signals.subList(0, limit)));

        int sent = 0, blocked = 0;
        for (com.bot.DecisionEngineMerged.TradeIdea s : dispatchSignals) {
            Dispatcher.Result res = Dispatcher.getInstance().dispatch(s, "CYCLE");
            if (res.dispatched) {
                // [v78.2 CRITICAL FIX] Register ISC AFTER dispatch success.
                // Matches HOT_RESCAN/EARLY_TICK pattern (SignalSender.java:3322-3326).
                // Previously registered in processPair BEFORE dispatch, causing
                // Dispatcher.isSymbolAvailable() to immediately block as
                // "bipolar/cooldown" — 100% of valid signals died here.
                isc.registerSignal(s);
                sent++;
            } else {
                blocked++;
                // [v78.2] Safety net: unregister still called in case any code path
                // managed to register (e.g., race during refactor). No-op if not registered.
                isc.unregisterSignal(s);
                LOG.info("[CYCLE-BLOCK] " + s.symbol + " " + s.side + ": " + res.reason);
            }
        }
        LOG.info("══ CYCLE #" + cycle + " END ══ sent=" + sent + " blocked=" + blocked
                + " time=" + (System.currentTimeMillis() - cycleStart) + "ms");
    }

    private static List<com.bot.DecisionEngineMerged.TradeIdea> reorderSignalsForDispatch(
            List<com.bot.DecisionEngineMerged.TradeIdea> selected) {
        if (selected == null || selected.size() < 3) return selected;
        List<com.bot.DecisionEngineMerged.TradeIdea> longs = new ArrayList<>();
        List<com.bot.DecisionEngineMerged.TradeIdea> shorts = new ArrayList<>();
        for (com.bot.DecisionEngineMerged.TradeIdea idea : selected) {
            if (idea.side == com.bot.TradingCore.Side.LONG) longs.add(idea);
            else shorts.add(idea);
        }
        if (longs.isEmpty() || shorts.isEmpty()) return selected;
        List<com.bot.DecisionEngineMerged.TradeIdea> ordered = new ArrayList<>(selected.size());
        int li = 0, si = 0;
        boolean takeLong = selected.get(0).side == com.bot.TradingCore.Side.LONG;
        while (li < longs.size() || si < shorts.size()) {
            if (takeLong && li < longs.size()) ordered.add(longs.get(li++));
            else if (!takeLong && si < shorts.size()) ordered.add(shorts.get(si++));
            else if (li < longs.size()) ordered.add(longs.get(li++));
            else if (si < shorts.size()) ordered.add(shorts.get(si++));
            takeLong = !takeLong;
        }
        return ordered;
    }

    public static void trackSignal(com.bot.DecisionEngineMerged.TradeIdea idea) {
        trackSignal(idea, true);
    }

    static void trackSignal(com.bot.DecisionEngineMerged.TradeIdea idea, boolean sentToUser) {
        if (idea == null) return;
        String key = idea.symbol + "_" + idea.side;
        trackedSignals.remove(key);
        String forecastBias = "NEUTRAL";
        double forecastScore = 0.0;
        if (idea.forecast != null) {
            forecastBias = idea.forecast.bias.name();
            forecastScore = idea.forecast.directionScore;
        }
        double tp3 = idea.tp3 > 0 ? idea.tp3 : idea.price + (idea.tp2 - idea.price) * 1.5;
        trackedSignals.put(key, new TrackedSignal(
                idea.symbol, idea.side, idea.price, idea.stop,
                idea.tp1, idea.tp2, tp3,
                forecastBias, forecastScore, sentToUser));
        if (forecastRecords.size() >= MAX_FORECAST_RECORDS) {
            forecastRecords.entrySet().removeIf(e ->
                    e.getValue().resolved && e.getValue().ageMs() > 60 * 60_000L);
            if (forecastRecords.size() >= MAX_FORECAST_RECORDS) {
                forecastRecords.entrySet().stream()
                        .min(Comparator.comparingLong(e -> e.getValue().createdAt))
                        .ifPresent(e -> forecastRecords.remove(e.getKey()));
            }
        }
        forecastRecords.put(key + "_" + forecastSeq.incrementAndGet(),
                new ForecastRecord(idea.symbol, idea.side, idea.price,
                        forecastBias, forecastScore, idea.probability, idea.getRobustAtrPct(),
                        idea.tp1, idea.stop));
    }

    // [v78] lastForecastReportMs / FORECAST_REPORT_INTERVAL_MS REMOVED — only fed v76 hourly Telegram.

    private static void checkForecastAccuracy(com.bot.SignalSender sender,
                                              com.bot.TelegramBotSender telegram) {
        long minAgeMs = 45 * 60_000L;
        for (Iterator<Map.Entry<String, ForecastRecord>> it =
             forecastRecords.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, ForecastRecord> e = it.next();
            ForecastRecord fr = e.getValue();
            if (fr.ageMs() < minAgeMs) continue;
            if (fr.resolved) {
                if (fr.ageMs() > minAgeMs + 30 * 60_000L) it.remove();
                continue;
            }
            if (fr.ageMs() > 4 * 60 * 60_000L) { it.remove(); continue; }

            try {
                int barsNeeded = (int) Math.ceil(fr.ageMs() / (15.0 * 60_000L)) + 2;
                barsNeeded = Math.max(5, Math.min(30, barsNeeded));
                List<com.bot.TradingCore.Candle> c = sender.fetchKlines(fr.symbol, "15m", barsNeeded);
                if (c == null || c.isEmpty()) continue;
                double currentPrice = c.get(c.size() - 1).close;

                double atrAbs = fr.robustAtrPctAtSignal > 0
                        ? fr.robustAtrPctAtSignal * fr.entryPrice
                        : (c.size() >= 15 ? com.bot.TradingCore.atr(c, 14) : 0);

                boolean bullishBias = fr.forecastBias.contains("BULL");
                boolean bearishBias = fr.forecastBias.contains("BEAR");
                boolean hasOpinion = bullishBias || bearishBias;

                boolean hitTP1 = false, hitSL = false, ambiguous = false;
                double tp1Use = fr.tp1Level;
                double slUse  = fr.slLevel;
                boolean haveRealLevels = tp1Use > 0 && slUse > 0;
                if (!haveRealLevels && hasOpinion && atrAbs > 0) {
                    boolean longSide = bullishBias;
                    tp1Use = longSide ? fr.entryPrice + atrAbs * 1.0 : fr.entryPrice - atrAbs * 1.0;
                    slUse  = longSide ? fr.entryPrice - atrAbs * 0.8 : fr.entryPrice + atrAbs * 0.8;
                    haveRealLevels = true;
                }

                if (haveRealLevels && hasOpinion) {
                    boolean longSide = fr.side == com.bot.TradingCore.Side.LONG;
                    for (com.bot.TradingCore.Candle bar : c) {
                        if (bar.openTime + 15 * 60_000L < fr.createdAt) continue;
                        boolean tpHere = longSide ? bar.high >= tp1Use : bar.low  <= tp1Use;
                        boolean slHere = longSide ? bar.low  <= slUse  : bar.high >= slUse;
                        if (tpHere && slHere) {
                            double openPx = bar.open;
                            double tpDist = Math.abs(tp1Use - openPx);
                            double slDist = Math.abs(slUse  - openPx);
                            if (longSide) {
                                boolean openInFavor = openPx > fr.entryPrice && tpDist < slDist;
                                boolean openAgainst = openPx < fr.entryPrice && slDist < tpDist;
                                if (openInFavor) { hitTP1 = true; break; }
                                if (openAgainst) { hitSL  = true; break; }
                                ambiguous = true; break;
                            } else {
                                boolean openInFavor = openPx < fr.entryPrice && tpDist < slDist;
                                boolean openAgainst = openPx > fr.entryPrice && slDist < tpDist;
                                if (openInFavor) { hitTP1 = true; break; }
                                if (openAgainst) { hitSL  = true; break; }
                                ambiguous = true; break;
                            }
                        }
                        if (tpHere) { hitTP1 = true; break; }
                        if (slHere) { hitSL  = true; break; }
                    }
                }

                boolean correct; String outcome;
                if (ambiguous) {
                    fr.resolved = true; fr.actualOutcome = "AMBIGUOUS";
                    continue;
                }
                if (hitTP1) { correct = true;  outcome = "TP1"; }
                else if (hitSL) { correct = false; outcome = "SL"; }
                else {
                    double atrForFc = c.size() >= 15 ? com.bot.TradingCore.atr(c, 14) : 0;
                    double fcThreshold = atrForFc > 0
                            ? Math.max(0.3, (atrForFc / currentPrice) * 100.0 * 0.25) : 0.3;
                    double changePct = (currentPrice - fr.entryPrice) / fr.entryPrice * 100.0;
                    boolean bullishMove = changePct > fcThreshold;
                    boolean bearishMove = changePct < -fcThreshold;
                    correct = (bullishBias && bullishMove) || (bearishBias && bearishMove);
                    outcome = bullishMove ? "MOVED_UP" : bearishMove ? "MOVED_DOWN" : "FLAT";
                }

                fr.resolved = true;
                fr.actualOutcome = outcome;

                // [v77] TP1 HIT / SL HIT Telegram message REMOVED.
                // User manages exits manually and follows price themselves —
                // these notifications were pure noise. Internal accounting
                // (forecastTotal, forecastCorrect, calibrator outcomes) is
                // preserved below so calibration and walk-forward continue
                // to learn from real outcomes.

                boolean decisive = hitTP1 || hitSL || !"FLAT".equals(outcome);
                if (hasOpinion && decisive) {
                    if (fr.counted.compareAndSet(false, true)) {
                        forecastTotal.incrementAndGet();
                        if (correct) forecastCorrect.incrementAndGet();
                    }
                    try {
                        double sigProb01 = Math.max(0.01, Math.min(0.99, fr.signalProbability / 100.0));
                        double atrForFc = c.size() >= 15 ? com.bot.TradingCore.atr(c, 14) : 0;
                        double atrPctForBucket = fr.robustAtrPctAtSignal > 0
                                ? fr.robustAtrPctAtSignal * 100.0
                                : (atrForFc > 0 ? (atrForFc / currentPrice) * 100.0 : 1.0);
                        com.bot.DecisionEngineMerged.getCalibrator()
                                .recordOutcome(fr.symbol, sigProb01, correct, atrPctForBucket);
                        String cat = com.bot.DecisionEngineMerged.detectAssetType(fr.symbol).label;
                        recordSignalOutcome(fr.symbol, fr.signalProbability, cat, correct);
                    } catch (Throwable ignored) {}
                }

                int total = forecastTotal.get();
                int correct2 = forecastCorrect.get();
                double acc = total > 0 ? (double) correct2 / total * 100 : 0;
                LOG.info(String.format("[FC] %s bias=%s outcome=%s %s | Acc: %.0f%% (%d/%d)",
                        fr.symbol, fr.forecastBias, outcome, correct ? "✅" : "❌",
                        acc, correct2, total));

                // [v77] Periodic Forecast Accuracy + Signal Quality Telegram REMOVED.
                // Stats live in the [STATS] log line (every 30min) for operators.
            } catch (Exception ex) {
                LOG.fine("[FC] Fetch fail: " + fr.symbol + " " + ex.getMessage());
            }
        }
    }

    // [v78] wasDispatchedToUser() removed — no callers since v77 SL/TP HIT silenced.

    private static void runWatchdog(com.bot.TelegramBotSender telegram,
                                    com.bot.SignalSender sender) {
        long now = System.currentTimeMillis();

        // [v62] Don't alert during Binance IP ban — it's expected & self-healing.
        // Don't alert during first 10 min of uptime — WS might still be connecting.
        if (sender.isRlBanned()) return;
        if (now - startTimeMs < 10 * 60_000L) return;

        List<String> infraIssues = new ArrayList<>();
        if (now - lastCycleSuccessMs > 3 * 60_000L)
            infraIssues.add("💀 MainCycle silent " + (now - lastCycleSuccessMs) / 1000 + "s");
        if (now - lastStatsSuccessMs > 40 * 60_000L)
            infraIssues.add("💀 Stats silent " + (now - lastStatsSuccessMs) / 60_000 + "min");
        // [v62] Raised threshold 3 → 10. WebSocket count < 10 means degraded,
        // but 2-3 is expected during the first minute or after a pair reshuffle.
        if (sender.getActiveWsCount() < 10)
            infraIssues.add("⚠️ WebSockets low: " + sender.getActiveWsCount());

        boolean signalDrought = now - lastSignalMs > SIGNAL_DROUGHT_MS;

        if (sender.getActiveWsCount() < 10 || now - lastCycleSuccessMs > 3 * 60_000L) {
            sender.forceResubscribeTopPairs();
        }

        if (!infraIssues.isEmpty() && now - lastInfraAlertMs > INFRA_ALERT_COOLDOWN_MS) {
            lastInfraAlertMs = now;
            watchdogAlerts.incrementAndGet();
            // [v77 NO-SPAM] Watchdog Telegram alerts → log only.
            // Infra issues (WS low, cycle silent) are operator concerns, not trader concerns.
            LOG.warning("[Watchdog #" + watchdogAlerts.get() + "] "
                    + String.join(" | ", infraIssues));
        }

        // [PATCH 2026-04-28] DROUGHT АЛЕРТ ВЕРНУЛИ.
        // Симптом юзера: «за всю ночь не пришло ни одного сигнала».
        // Причина проблемы: автор v77 убрал drought-сообщение как «спам», но
        // тогда юзер не понимает — бот мёртв, в режиме PAPER, или просто рынок
        // плоский. Возвращаем ОДНОРАЗОВОЕ сообщение через 3 часа тишины с кратким
        // breakdown что блокирует. После прихода первого сигнала droughtAnnounced
        // сбрасывается в Dispatcher.dispatch() (см. там).
        if (signalDrought && droughtAnnounced.compareAndSet(false, true)) {
            try {
                Dispatcher disp = Dispatcher.getInstance();
                String breakdown = (disp != null) ? disp.getBlockBreakdown() : "n/a";
                long hoursSilent = (now - lastSignalMs) / (60 * 60_000L);
                String regime = ctxRegimeForAlert();
                int wsCount = sender.getActiveWsCount();
                int calN = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
                String paperFlag = OBSERVATION_MODE ? "🧪 PAPER" : "🔴 LIVE";
                telegram.sendMessageAsync(String.format(
                        "📭 *Тихо на рынке* %dh\n"
                                + "BTC: %s | WS: %d | Cal n=%d | %s\n"
                                + "Блокировки: %s\n"
                                + "_(`bi`=cooldown, `rr`=R:R<2.0, `sl`=SL<0.30%%, `cold`=cold-start, `dd`=dedup, `hr`=hourly cap)_",
                        hoursSilent, regime, wsCount, calN, paperFlag, breakdown));
            } catch (Throwable ignored) {}
        }
    }

    // [PATCH 2026-04-28] Helper для drought сообщения — берёт BTC regime у GIC.
    private static String ctxRegimeForAlert() {
        try {
            // Доступа к gic тут нет напрямую — берём из последней updateBtcContext через статический snapshot
            return lastBtcRegimeForAlert != null ? lastBtcRegimeForAlert : "?";
        } catch (Throwable t) { return "?"; }
    }
    static volatile String lastBtcRegimeForAlert = null;

    // [PATCH 2026-04-28] HEARTBEAT — каждые 90 мин если 0 сигналов в этот период.
    // Решает «бот живой?» вопрос без необходимости лезть в Railway logs.
    // Cooldown 90 мин чтобы не спамить.
    private static volatile long lastHeartbeatMs = 0;
    private static final long HEARTBEAT_INTERVAL_MS = 90 * 60_000L;
    private static final long HEARTBEAT_QUIET_MS    = 90 * 60_000L; // только если 90+ мин без сигнала

    private static void maybeSendHeartbeat(com.bot.TelegramBotSender telegram,
                                           com.bot.SignalSender sender,
                                           com.bot.GlobalImpulseController gic,
                                           com.bot.InstitutionalSignalCore isc) {
        long now = System.currentTimeMillis();
        if (now - startTimeMs < 30 * 60_000L) return;             // дать 30 мин на разогрев
        if (now - lastHeartbeatMs < HEARTBEAT_INTERVAL_MS) return;
        if (now - lastSignalMs   < HEARTBEAT_QUIET_MS) return;     // если есть сигналы — не нужно
        try {
            Dispatcher disp = Dispatcher.getInstance();
            String breakdown = (disp != null) ? disp.getBlockBreakdown() : "n/a";
            int wsCount = sender.getActiveWsCount();
            int calN    = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            com.bot.GlobalImpulseController.GlobalContext gc = gic.getContext();
            lastBtcRegimeForAlert = String.valueOf(gc.regime);
            long minSilent = (now - lastSignalMs) / 60_000L;
            String paperFlag = OBSERVATION_MODE ? "🧪 PAPER" : "🔴 LIVE";
            telegram.sendMessageAsync(String.format(
                    "💓 *Heartbeat* (%dмин без сигнала)\n"
                            + "BTC: %s str=%.2f | WS: %d | Cal n=%d | %s\n"
                            + "Cycles: %d | Errors: %d\n"
                            + "Блок: %s",
                    minSilent, gc.regime, gc.impulseStrength, wsCount, calN, paperFlag,
                    totalCycles.get(), errorCount.get(), breakdown));
            lastHeartbeatMs = now;
        } catch (Throwable ignored) {}
    }

    // [v78] maybeSendDailySummary() REMOVED — see scheduler removal above.

    private static void runPeriodicBacktest(com.bot.SignalSender sender,
                                            com.bot.InstitutionalSignalCore isc) {
        com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();
        LinkedHashSet<String> universe = new LinkedHashSet<>(List.of(
                "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "LINKUSDT", "XRPUSDT"));
        universe.addAll(sender.getScanUniverseSnapshot(6));
        double totalEV = 0; int count = 0;
        for (String sym : universe) {
            try {
                Thread.sleep(3_000L);
                List<com.bot.TradingCore.Candle> m15 = sender.fetchKlines(sym, "15m", 300);
                List<com.bot.TradingCore.Candle> h1  = sender.fetchKlines(sym, "1h",  100);
                List<com.bot.TradingCore.Candle> m1  = sender.getM1FromWs(sym);
                List<com.bot.TradingCore.Candle> m5  = sender.fetchKlines(sym, "5m",  200);
                if (m15 == null || m15.size() < 200) continue;
                com.bot.DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(sym);
                com.bot.SimpleBacktester.BacktestResult r = bt.run(sym, m1, m5, m15, h1, cat);
                if (r.total >= 5) {
                    totalEV += r.ev; count++;
                    isc.setSymbolBacktestResult(sym, r.ev);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt(); return;
            } catch (Exception ignored) {}
        }
        if (count > 0) {
            double avgEV = totalEV / count;
            isc.setBacktestResult(avgEV, System.currentTimeMillis());
        }
    }

    private static void logStats(com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        lastStatsSuccessMs = System.currentTimeMillis();
        long uptimeMin = (System.currentTimeMillis() - startTimeMs) / 60_000;
        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        Dispatcher d = Dispatcher.getInstance();
        // [v76] Surface CorrelationGuard slot usage in stats so the operator can
        // see at a glance: "is the bot quiet because nothing's setting up, or
        // because correlation cap is full?". Format: "Corr:2L/1S 3/6".
        String corrSlots = sender.getCorrelationSlotsSnapshot();
        String saturatedMark = sender.isCorrelationSaturated() ? "🔒" : "";
        String msg = String.format(
                "[STATS] Up:%dm Cyc:%d Sig:%d Trk:%d FR:%d | BTC:%s str=%.2f | "
                        + "WS:%d Corr:%s%s Disp:%d/%d [%s] | FC:%.0f%%(%d/%d) Err:%d WD:%d | %s",
                uptimeMin, totalCycles.get(), totalSignals.get(),
                trackedSignals.size(), forecastRecords.size(),
                ctx.regime, ctx.impulseStrength,
                sender.getActiveWsCount(),
                corrSlots, saturatedMark,
                d != null ? d.getTotalDispatched() : 0,
                d != null ? d.getBlockedByGate() : 0,
                d != null ? d.getBlockBreakdown() : "?",
                forecastTotal.get() > 0 ? (double) forecastCorrect.get() / forecastTotal.get() * 100 : 0.0,
                forecastCorrect.get(), forecastTotal.get(),
                errorCount.get(), watchdogAlerts.get(),
                isc.getStats());
        LOG.info(msg);
        try {
            Set<String> autoBlocked = isc.getAutoBlacklist();
            for (String sym : autoBlocked) sender.addToGarbageBlocklist(sym);
        } catch (Exception ignored) {}
    }

    private static void updateBtcContext(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        try {
            List<com.bot.TradingCore.Candle> btc = sender.fetchKlines("BTCUSDT", "15m", KLINES);
            if (btc != null && btc.size() > 30) gic.update(btc);
        } catch (Exception e) { LOG.warning("[BTC ctx] " + e.getMessage()); }
    }

    private static void updateSectors(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        for (Map.Entry<String, String> e : SECTOR_LEADERS.entrySet()) {
            try {
                List<com.bot.TradingCore.Candle> sc = sender.fetchKlines(e.getKey(), "15m", 80);
                if (sc != null && sc.size() > 25) gic.updateSector(e.getValue(), sc);
            } catch (Exception ignored) {}
        }
    }

    private static String calibrationStatus() {
        int s = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
        if (s < 30)  return String.format("⚠️ *Cold-start: %d/30 исходов*\n"
                + (OBSERVATION_MODE ? "" : "🚨 *РИСК:* В LIVE без калибровки — рекомендуется `OBSERVATION_MODE=1` или размер ×0.25"), s);
        if (s < 100) return "🔸 Калибровка: " + s + "/100";
        return "✅ Калибровка: " + s + " исходов";
    }

    private static String buildStartMessage() {
        return "⚡ *TradingBot SCANNER* `v78.2`\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + "`15M` Futures · TOP-" + envInt("TOP_N", 30) + " · Scanner-only\n"
                + "R:R min `1:2` · SL min `0.30%`\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + (OBSERVATION_MODE
                ? "🧪 *PAPER MODE АКТИВЕН* — сигналы только для валидации\n━━━━━━━━━━━━━━━━━━━━━\n"
                : "🔴 *LIVE MODE* — реальные сигналы в чат\n━━━━━━━━━━━━━━━━━━━━━\n")
                + "🎯 *Конфигурация:*\n"
                + "• DE floor 48 · ISC floor 47 · Dispatcher Phase1 floor 48\n"
                + "• ProbabilityCalibrator: PAV regression after n≥50\n"
                + "• Live-splice always (no RSI veto)\n"
                + "• Cache TTL: 15m=30s · 1h=5min · 2h=15min\n"
                + "• Dispatcher: dedup 6min, hourly 20, burst 5/5min\n"
                + "• Post-dispatch ISC cooldown: 3min · TIME-stop 90min\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + calibrationStatus() + "\n"
                + "_Прогнозы Entry / TP1-3 / SL._";
    }

    private static String nowLocalStr() {
        return ZonedDateTime.now(ZONE)
                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"));
    }

    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis).atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }

    private static void resolveTimezoneAsync() {
        String envTz = System.getenv("TIMEZONE");
        if (envTz != null && !envTz.isBlank()) {
            try {
                ZONE = ZoneId.of(envTz.trim());
                com.bot.DecisionEngineMerged.USER_ZONE = ZONE;
                return;
            } catch (Exception ignored) {}
        }
        Thread tzThread = new Thread(() -> {
            try {
                java.net.URI uri = java.net.URI.create("http://ip-api.com/json?fields=timezone,city");
                java.net.HttpURLConnection conn = (java.net.HttpURLConnection) uri.toURL().openConnection();
                conn.setConnectTimeout(3000);
                conn.setReadTimeout(3000);
                conn.setRequestMethod("GET");
                if (conn.getResponseCode() == 200) {
                    try (java.io.InputStream is = conn.getInputStream();
                         java.util.Scanner sc = new java.util.Scanner(is, "UTF-8")) {
                        String body = sc.useDelimiter("\\A").hasNext() ? sc.next() : "";
                        int idx = body.indexOf("\"timezone\"");
                        if (idx >= 0) {
                            int q1 = body.indexOf('"', idx + 10) + 1;
                            int q2 = body.indexOf('"', q1);
                            if (q1 > 0 && q2 > q1) {
                                ZoneId z = ZoneId.of(body.substring(q1, q2));
                                ZONE = z;
                                com.bot.DecisionEngineMerged.USER_ZONE = z;
                            }
                        }
                    }
                }
                conn.disconnect();
            } catch (Exception ignored) {}
        }, "tz-resolver");
        tzThread.setDaemon(true);
        tzThread.start();
    }

    private static int envInt(String k, int d) {
        try { return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(d))); }
        catch (Exception e) { return d; }
    }

    /** [v66] Read first non-empty env var from a list, fall back to default.
     *  Used to unify KLINES and KLINES_LIMIT naming across BotMain and SignalSender. */
    private static int envIntAny(int defaultValue, String... keys) {
        for (String k : keys) {
            String v = System.getenv(k);
            if (v != null && !v.isBlank()) {
                try { return Integer.parseInt(v.trim()); }
                catch (NumberFormatException ignored) {}
            }
        }
        return defaultValue;
    }

    private static void runWalkForwardValidation(com.bot.SignalSender sender,
                                                 com.bot.TelegramBotSender telegram) {
        try {
            com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();
            List<String> symbols = new ArrayList<>(SECTOR_LEADERS.keySet());
            int alerts = 0, totalWindows = 0;
            double totalDelta = 0;
            for (String sym : symbols) {
                try {
                    List<com.bot.TradingCore.Candle> m15 = sender.fetchKlines(sym, "15m", 2880);
                    List<com.bot.TradingCore.Candle> h1  = sender.fetchKlines(sym, "1h",  720);
                    if (m15 == null || m15.size() < 1500) continue;
                    com.bot.DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(sym);
                    if (cat == null) cat = com.bot.DecisionEngineMerged.CoinCategory.ALT;
                    List<com.bot.SimpleBacktester.BacktestResult> oos =
                            bt.walkForward(sym, m15, h1, cat, 1344, 288);
                    for (com.bot.SimpleBacktester.BacktestResult r : oos) totalWindows++;
                    if (oos.size() >= 2) {
                        double firstHalf = 0, secondHalf = 0;
                        int half = oos.size() / 2;
                        for (int i = 0; i < half; i++) firstHalf += oos.get(i).winRate * 100.0;
                        for (int i = half; i < oos.size(); i++) secondHalf += oos.get(i).winRate * 100.0;
                        firstHalf  /= Math.max(1, half);
                        secondHalf /= Math.max(1, oos.size() - half);
                        double delta = firstHalf - secondHalf;
                        totalDelta += Math.abs(delta);
                        // [v76] Threshold 15% → 8%. A 15-point winRate drift between
                        // window halves is already catastrophic (50→35% means strategy
                        // has lost half its edge). At 8% the alert fires while there's
                        // still time to investigate — possible regime change, stale
                        // hardcoded thresholds, or BTC volatility regime shift. False
                        // positives at this threshold are tolerable: the alert just
                        // sends a Telegram message, doesn't block trading.
                        if (Math.abs(delta) > 8.0) alerts++;
                    }
                } catch (Throwable ignored) {}
            }
            // [v77] Walk-Forward Alert moved from Telegram to log only.
            // It's a developer-grade observability signal (regime drift),
            // not actionable for the manual trader. Stays in log so it
            // shows up if someone is checking Railway output.
            if (alerts > 0) {
                LOG.info(String.format(
                        "[WalkForward] %d/%d symbols unstable, Avg Δ: %.1f%%",
                        alerts, symbols.size(),
                        totalWindows > 0 ? totalDelta / Math.max(1, totalWindows) : 0));
            }
        } catch (Throwable ignored) {}
    }

    private static void configureLogger() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        for (Handler h : root.getHandlers()) {
            h.setFormatter(new SimpleFormatter() {
                private final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss");
                @Override
                public String format(LogRecord r) {
                    return String.format("[%s][%-7s] %s%n",
                            ZonedDateTime.now(ZONE).format(fmt),
                            r.getLevel(), r.getMessage());
                }
            });
        }
    }
}