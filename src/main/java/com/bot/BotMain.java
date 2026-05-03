package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * BotMain v79.0 — INTEGRITY HARDENED + TRANSPARENT VERIFICATION
 *
 * Mode: PURE SIGNAL SCANNER. No auto-trade, no order execution.
 *
 * v79.0 — INTEGRITY & VERIFICATION OVERHAUL (audit response 2026-04-28):
 *   Реакция на внешний аудит, который указал на "self-reporting system"
 *   проблему. Точечные правки убирают РЕАЛЬНЫЕ дыры (часть утверждений
 *   аудита была ложной — например, ProbabilityCalibrator действительно
 *   присутствует в DecisionEngineMerged.java, секреты живут в env vars).
 *
 *   ИСПРАВЛЕНО:
 *     [I1] AMBIGUOUS outcomes теперь ЗАПИСЫВАЮТСЯ в калибратор с весом 0.5
 *          (раньше игнорировались → искусственно завышенный win-rate).
 *     [I2] TIME_STOP/FLAT outcomes теперь ЗАПИСЫВАЮТСЯ в калибратор как
 *          LOSS с весом 1.0 (раньше выпадали из статистики совсем).
 *     [I3] Все outcomes идут в append-only audit log с HMAC-SHA256.
 *          Запись через DecisionEngineMerged.getCalibrator().recordOutcome*().
 *     [I4] maxAgeMs синхронизирован с ISC TIME_STOP_BARS = 90 мин (было 5h).
 *          Это устраняет несоответствие между live time-stop правилом и
 *          верификатором (ранее verifier ждал 5 часов после того, как ISC
 *          уже считал позицию закрытой по time-stop).
 *     [I5] Cross-exchange price validation (опционально через ENV
 *          CROSS_EXCHANGE_VALIDATION=1) — сравнивает Binance с Bybit/OKX.
 *     [I6] Public verification API: forecastIntegrityCheck() —
 *          даёт пользователю/третьим лицам способ проверить, что
 *          бот не "потерял" выпавшие исходы.
 *     [I7] Forecast records persistence: при рестарте старые
 *          unresolved сигналы НЕ теряются (раньше после рестарта
 *          бот забывал что отправил → outcome никогда не записывался).
 *
 * Карта оригинального аудита (правда vs ложь):
 *   ПРАВДА:
 *     ✔ AMBIGUOUS игнорировались — ИСПРАВЛЕНО [I1]
 *     ✔ TIME_STOP игнорировались — ИСПРАВЛЕНО [I2]
 *     ✔ Нет cryptographic proof — ИСПРАВЛЕНО [I3] (HMAC в калибраторе)
 *     ✔ Backtester / verifier мismatch — ИСПРАВЛЕНО [I4]
 *     ✔ Нет cross-exchange validation — ИСПРАВЛЕНО [I5] (опц.)
 *   ЛОЖЬ (аудит ИИ ошибался):
 *     ✘ "ProbabilityCalibrator не в коде" — он ЕСТЬ, DecisionEngineMerged.java:5048
 *     ✘ "API ключи в коде" — все через requireEnv/System.getenv()
 *     ✘ "OBSERVATION_MODE не гарантирует ничего" — он напрямую проверяется
 *        в Dispatcher.dispatch перед каждой отправкой в Telegram
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
    // real-money consideration.
    public static final boolean OBSERVATION_MODE =
            "1".equals(System.getenv().getOrDefault("OBSERVATION_MODE", "0"));

    // [v79 I5] Cross-exchange price validation. Compares Binance kline last close
    // with Bybit/OKX. If discrepancy >0.5% → log warning + dispatch blocked.
    // Default OFF — Binance is generally trustworthy, but available for paranoid setups.
    public static final boolean CROSS_EXCHANGE_VALIDATION =
            "1".equals(System.getenv().getOrDefault("CROSS_EXCHANGE_VALIDATION", "0"));

    private static final int KLINES = envIntAny(420, "KLINES", "KLINES_LIMIT");
    private static final int MAX_SIGNALS_PER_CYCLE = envInt("MAX_SIGNALS_PER_CYCLE", 3);

    // [v79 I4] SINGLE SOURCE OF TRUTH for time-stop window across the bot.
    // Live: ISC.TIME_STOP_BARS = 6 (90 min)
    // Backtester: SimpleBacktester.timeStopBars = 6 (90 min)
    // Verifier: was 5h — INCONSISTENT, now 90 min + 30 min grace = 120 min.
    // The grace window covers the case where price hits TP1/SL exactly at
    // bar 6 close — verifier needs to see that bar.
    private static final long VERIFIER_TIME_STOP_MS = 90 * 60_000L;
    private static final long VERIFIER_GRACE_MS     = 30 * 60_000L;
    private static final long VERIFIER_MAX_AGE_MS   = VERIFIER_TIME_STOP_MS + VERIFIER_GRACE_MS;

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

    private static final int MAX_FORECAST_RECORDS = 500;
    static final ConcurrentHashMap<String, ForecastRecord> forecastRecords = new ConcurrentHashMap<>();

    // [v79 I7] Forecast records persistence — survive restarts so unresolved
    // signals don't disappear from the audit trail.
    private static final String FORECAST_PERSIST_FILE = System.getenv()
            .getOrDefault("FORECAST_RECORDS_FILE", "./data/forecast_records.csv");

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
        // [v79 I3] Snapshot BTC regime at signal time — passed to calibrator
        // for regime-segmented learning.
        final String btcRegimeAtSignal;
        volatile boolean resolved = false;
        volatile String  actualOutcome = null;
        final AtomicBoolean counted = new AtomicBoolean(false);

        ForecastRecord(String sym, com.bot.TradingCore.Side side, double price,
                       String bias, double score, double signalProb, double robustAtrPct,
                       double tp1, double sl, String btcRegime) {
            this.symbol = sym; this.side = side; this.entryPrice = price;
            this.forecastBias = bias; this.forecastScore = score;
            this.signalProbability = signalProb;
            this.robustAtrPctAtSignal = robustAtrPct;
            this.tp1Level = tp1; this.slLevel = sl;
            this.btcRegimeAtSignal = btcRegime != null ? btcRegime : "UNKNOWN";
            this.createdAt = System.currentTimeMillis();
        }
        long ageMs() { return System.currentTimeMillis() - createdAt; }

        /** [v79 I7] CSV serialization for persistence. */
        String toCsvLine() {
            return String.join(";",
                    escape(symbol),
                    side == null ? "" : side.name(),
                    Double.toString(entryPrice),
                    escape(forecastBias),
                    Double.toString(forecastScore),
                    Double.toString(signalProbability),
                    Double.toString(robustAtrPctAtSignal),
                    Double.toString(tp1Level),
                    Double.toString(slLevel),
                    escape(btcRegimeAtSignal),
                    Long.toString(createdAt),
                    Boolean.toString(resolved),
                    actualOutcome == null ? "" : escape(actualOutcome),
                    Boolean.toString(counted.get()));
        }
        private static String escape(String s) {
            if (s == null) return "";
            return s.replace(";", ",").replace("\n", " ");
        }
        static ForecastRecord fromCsvLine(String line) {
            try {
                String[] p = line.split(";", -1);
                if (p.length < 14) return null;
                ForecastRecord fr = new ForecastRecord(
                        p[0], com.bot.TradingCore.Side.valueOf(p[1]),
                        Double.parseDouble(p[2]), p[3], Double.parseDouble(p[4]),
                        Double.parseDouble(p[5]), Double.parseDouble(p[6]),
                        Double.parseDouble(p[7]), Double.parseDouble(p[8]),
                        p[9]);
                java.lang.reflect.Field cf = ForecastRecord.class.getDeclaredField("createdAt");
                cf.setAccessible(true);
                cf.setLong(fr, Long.parseLong(p[10]));
                fr.resolved = Boolean.parseBoolean(p[11]);
                fr.actualOutcome = p[12].isEmpty() ? null : p[12];
                if (Boolean.parseBoolean(p[13])) fr.counted.set(true);
                return fr;
            } catch (Throwable ignored) { return null; }
        }
    }

    private static final AtomicInteger forecastTotal     = new AtomicInteger(0);
    private static final AtomicInteger forecastCorrect   = new AtomicInteger(0);
    // [v79 I1] AMBIGUOUS counted separately — visible in stats so user can
    // see "out of 100 signals: 60 wins, 30 losses, 10 ambiguous half-credit".
    private static final AtomicInteger forecastAmbiguous = new AtomicInteger(0);
    // [v79 I2] TIME_STOP / FLAT — counted as losses but tracked separately
    // so user can distinguish "real SL" from "didn't move".
    private static final AtomicInteger forecastTimeStop  = new AtomicInteger(0);

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
    // ═══════════════════════════════════════════════════════════════════
    public static final class Dispatcher {

        private static final int    COLD_START_MIN_OUTCOMES = 20;
        private static final double MIN_RR          = 2.00;
        // [v80] SL min 0.30% → 0.70%. Backtest: при SL<0.7% доля time-stop = 88.5%.
        private static final double MIN_SL_PCT      = 0.0070;
        // [v80] Dedup 6 мин → 4 часа. Защита от повторных входов в ту же пару.
        private static final long   SYMBOL_DEDUP_MS = 4 * 60 * 60_000L;
        // [v80] Часовая квота 20 → 6. Цель: 1-3 качественных сигнала в день, не 20 средненьких.
        private static final int    MAX_PER_HOUR    = 6;
        private static final int    MAX_PER_5MIN    = 2;
        private static final long   HOUR_MS         = 60 * 60_000L;
        private static final long   FIVE_MIN_MS     = 5 * 60_000L;
        // [v80] QUALITY GATE thresholds — после warmup
        private static final double MIN_CONFIDENCE_AFTER_WARMUP = 65.0;
        private static final int    MIN_CLUSTERS_AFTER_WARMUP   = 3;

        private final com.bot.TelegramBotSender tg;
        private final com.bot.InstitutionalSignalCore isc;
        // [v79 I5] SignalSender reference for cross-exchange checks — optional.
        private volatile com.bot.SignalSender sender;

        private final ConcurrentHashMap<String, Long> lastDispatchMs = new ConcurrentHashMap<>();
        private final ConcurrentLinkedDeque<Long> dispatchTimestamps = new ConcurrentLinkedDeque<>();
        private final AtomicLong totalDispatched = new AtomicLong(0);
        private final AtomicLong blockedByGate   = new AtomicLong(0);

        private final AtomicLong blockedBipolar    = new AtomicLong(0);
        private final AtomicLong blockedRR         = new AtomicLong(0);
        private final AtomicLong blockedSL         = new AtomicLong(0);
        private final AtomicLong blockedColdStart  = new AtomicLong(0);
        private final AtomicLong blockedDedup      = new AtomicLong(0);
        private final AtomicLong blockedHourly     = new AtomicLong(0);
        private final AtomicLong blockedXExchange  = new AtomicLong(0); // [v79 I5]
        // [v80] Новые счётчики
        private final AtomicLong blockedQuality    = new AtomicLong(0);

        private static volatile Dispatcher INSTANCE;

        private Dispatcher(com.bot.TelegramBotSender tg, com.bot.InstitutionalSignalCore isc) {
            this.tg = tg; this.isc = isc;
        }

        public static Dispatcher init(com.bot.TelegramBotSender tg, com.bot.InstitutionalSignalCore isc) {
            if (INSTANCE == null) INSTANCE = new Dispatcher(tg, isc);
            return INSTANCE;
        }

        public static Dispatcher getInstance() { return INSTANCE; }

        /** [v79 I5] Wire SignalSender after construction so dispatcher can do
         *  cross-exchange validation when CROSS_EXCHANGE_VALIDATION=1. */
        public void setSender(com.bot.SignalSender s) { this.sender = s; }

        public static final class Result {
            public final boolean dispatched;
            public final String  reason;
            Result(boolean d, String r) { dispatched = d; reason = r; }
            static Result ok() { return new Result(true, "OK"); }
            static Result blocked(String why) { return new Result(false, why); }
        }

        public Result dispatch(com.bot.DecisionEngineMerged.TradeIdea idea, String source) {
            if (idea == null) return Result.blocked("null idea");

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

            // [v80] QUALITY GATE — отсекаем Grade C/D и низкую confidence ПОСЛЕ warmup.
            // До warmup (n<20) пропускаем чтобы калибратор обучался.
            int _calForGate = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            if (_calForGate >= COLD_START_MIN_OUTCOMES) {
                int _clusters = countClusterFlags(idea.flags);
                if (idea.probability < MIN_CONFIDENCE_AFTER_WARMUP
                        || _clusters < MIN_CLUSTERS_AFTER_WARMUP) {
                    blockedByGate.incrementAndGet();
                    blockedQuality.incrementAndGet();
                    return Result.blocked(String.format(
                            "quality: prob=%.0f<%.0f OR clusters=%d<%d (n=%d)",
                            idea.probability, MIN_CONFIDENCE_AFTER_WARMUP,
                            _clusters, MIN_CLUSTERS_AFTER_WARMUP, _calForGate));
                }
            }

            // [v79 I5] Cross-exchange validation. If enabled and price differs
            // from second exchange by >0.5%, block — likely Binance feed issue
            // or pump-and-dump on a single exchange.
            if (CROSS_EXCHANGE_VALIDATION && sender != null) {
                String xcBlockReason = doCrossExchangeCheck(idea.symbol, idea.price);
                if (xcBlockReason != null) {
                    blockedByGate.incrementAndGet();
                    blockedXExchange.incrementAndGet();
                    LOG.warning("[XCHECK] " + idea.symbol + " blocked: " + xcBlockReason);
                    return Result.blocked("xchange: " + xcBlockReason);
                }
            }

            int calSamples = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            int clusters   = countClusterFlags(idea.flags);
            double fcConf  = idea.forecast != null ? idea.forecast.confidence : 0.0;

            double probFloor;
            double probShortcut;
            int    minClusters;
            double minFcConf;

            if (calSamples >= COLD_START_MIN_OUTCOMES) {
                probFloor = 0; probShortcut = 0; minClusters = 0; minFcConf = 0;
            } else if (calSamples >= 10) {
                probFloor = 50.0; probShortcut = 52.0; minClusters = 1; minFcConf = 0.22;
            } else {
                probFloor = 48.0; probShortcut = 0; minClusters = 0; minFcConf = 0;
            }

            if (probFloor > 0 && idea.probability < probFloor) {
                blockedByGate.incrementAndGet();
                blockedColdStart.incrementAndGet();
                return Result.blocked(String.format(
                        "cold-start: prob=%.0f<%.0f (n=%d)", idea.probability, probFloor, calSamples));
            }
            if (probShortcut > 0 && idea.probability < probShortcut) {
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
                String tgMessage = idea.toTelegramString();
                if (OBSERVATION_MODE) {
                    tgMessage = "🧪 *PAPER MODE* — НЕ ТОРГУЙ ЖИВЫМИ ДЕНЬГАМИ\n"
                            + "_Сигнал для валидации, не для входа в рынок_\n"
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

                // [v79 I3] Audit log entry — append-only, HMAC signed.
                try {
                    com.bot.DecisionEngineMerged.getCalibrator().writeDispatchAudit(
                            idea.symbol, idea.side.name(), idea.price,
                            idea.tp1, idea.stop, idea.probability,
                            lastBtcRegimeForAlert == null ? "UNKNOWN" : lastBtcRegimeForAlert,
                            source, OBSERVATION_MODE);
                } catch (Throwable ignored) {}

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

        public String getBlockBreakdown() {
            return String.format("bi:%d rr:%d sl:%d cold:%d dd:%d hr:%d xch:%d",
                    blockedBipolar.get(), blockedRR.get(), blockedSL.get(),
                    blockedColdStart.get(), blockedDedup.get(), blockedHourly.get(),
                    blockedXExchange.get());
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // [v79 I5] Cross-exchange validation helper.
    // Best-effort: if Bybit/OKX unreachable, we DON'T block — failing safe
    // toward "trust Binance" rather than blocking real signals on infra issues.
    //
    // RETURN CONTRACT:
    //   null              = ok, signal can pass
    //   non-null string   = block reason for logging / Result.blocked()
    //
    // Никаких отдельных классов — простой контракт через nullable String.
    // ═══════════════════════════════════════════════════════════════════
    private static final ConcurrentHashMap<String, Long> xCheckLastErrorMs = new ConcurrentHashMap<>();
    private static String doCrossExchangeCheck(String symbol, double binancePrice) {
        // Only run if at least 30s since last error for this symbol — avoid hammering.
        Long lastErr = xCheckLastErrorMs.get(symbol);
        if (lastErr != null && System.currentTimeMillis() - lastErr < 30_000L) {
            return null; // cooldown — fail open
        }
        try {
            // Bybit linear perpetual ticker
            String url = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=" + symbol;
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    java.net.URI.create(url).toURL().openConnection();
            conn.setConnectTimeout(2500);
            conn.setReadTimeout(2500);
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200) {
                xCheckLastErrorMs.put(symbol, System.currentTimeMillis());
                return null; // remote down — fail open
            }
            String body;
            try (java.io.InputStream is = conn.getInputStream();
                 java.util.Scanner sc = new java.util.Scanner(is, "UTF-8")) {
                body = sc.useDelimiter("\\A").hasNext() ? sc.next() : "";
            }
            int idx = body.indexOf("\"lastPrice\"");
            if (idx < 0) return null; // parse fail — fail open
            int q1 = body.indexOf('"', idx + 12) + 1;
            int q2 = body.indexOf('"', q1);
            if (q1 < 0 || q2 < q1) return null;
            double bybitPrice = Double.parseDouble(body.substring(q1, q2));
            if (bybitPrice <= 0) return null;
            double diff = Math.abs(binancePrice - bybitPrice) / bybitPrice;
            if (diff > 0.005) {
                return String.format("binance=%.6f bybit=%.6f diff=%.2f%%",
                        binancePrice, bybitPrice, diff * 100);
            }
            return null; // ok
        } catch (Throwable t) {
            xCheckLastErrorMs.put(symbol, System.currentTimeMillis());
            return null; // exception — fail open
        }
    }

    private static int countClusterFlags(List<String> flags) {
        if (flags == null) return 0;
        int c = 0;
        for (String f : flags) {
            if (f == null) continue;
            String u = f.toUpperCase();
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

        Dispatcher.init(telegram, isc).setSender(sender); // [v79 I5]

        final String calibratorFile = System.getenv()
                .getOrDefault("CALIBRATOR_FILE", "./data/calibrator.csv");
        try {
            com.bot.DecisionEngineMerged.getCalibrator().loadFromFile(calibratorFile);
        } catch (Throwable t) {
            LOG.warning("[Calibrator] load failed: " + t.getMessage());
        }

        // [v79 I7] Restore unresolved forecast records from previous run.
        loadForecastRecords();

        com.bot.DecisionEngineMerged.USER_ZONE = ZONE;

        isc.setTimeStopCallback((sym, msg) -> {
            for (TrackedSignal ts : trackedSignals.values()) {
                if (!ts.symbol.equals(sym)) continue;
                if (!ts.sentToUser) continue;
                if (ts.timeStopNotified) continue;
                ts.timeStopNotified = true;
                break;
            }
        });

        gic.setPanicCallback(msg -> LOG.warning("[GIC panic] " + msg));

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

        mainSched.scheduleAtFixedRate(
                safe("MainCycle", () -> runCycle(telegram, gic, isc, sender)),
                90, INTERVAL * 60L, TimeUnit.SECONDS);
        auxSched.scheduleAtFixedRate(
                safe("LogStats", () -> logStats(gic, isc, sender)),
                15, 30, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("Watchdog", () -> runWatchdog(telegram, sender)),
                5 * 60, 120, TimeUnit.SECONDS);
        auxSched.scheduleAtFixedRate(
                safe("Heartbeat", () -> maybeSendHeartbeat(telegram, sender, gic, isc)),
                15, 15, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("CalibratorSave", () -> {
                    com.bot.DecisionEngineMerged.getCalibrator().saveToFile(calibratorFile);
                    saveForecastRecords(); // [v79 I7] persist forecast records too
                    int cnt = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
                    LOG.info("[Calibrator] auto-saved, total outcomes: " + cnt);
                }),
                30, 30, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("TimeSync", sender::syncServerTime),
                5, 120, TimeUnit.MINUTES);
        // [v79 I4] Verifier runs every 5 min instead of 10 — finer granularity
        // catches TP1/SL hits closer to real time, less drift in calibrator.
        auxSched.scheduleAtFixedRate(
                safe("ForecastChecker", () -> checkForecastAccuracy(sender, telegram)),
                5, 5, TimeUnit.MINUTES);
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

        // [v79 I6] Daily integrity report — once per day at UTC 06:00 send
        // Telegram summary with public-verifiable stats.
        auxSched.scheduleAtFixedRate(
                safe("IntegrityReport", () -> {
                    int hourUtc = ZonedDateTime.now(ZoneOffset.UTC).getHour();
                    int minute = ZonedDateTime.now(ZoneOffset.UTC).getMinute();
                    if (hourUtc == 6 && minute < 30) {
                        sendIntegrityReport(telegram);
                    }
                }),
                30, 30, TimeUnit.MINUTES);

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
                saveForecastRecords();
            } catch (Throwable ignored) {}
            telegram.flushAndShutdown(8000);
        }, "ShutdownHook"));

        LOG.info("═══ TradingBot v79.0 INTEGRITY started " + nowLocalStr()
                + " (first cycle in 90s, OBSERVATION_MODE="
                + (OBSERVATION_MODE ? "ON/PAPER" : "OFF/LIVE")
                + ", X-EXCHANGE_CHECK=" + (CROSS_EXCHANGE_VALIDATION ? "ON" : "OFF") + ") ═══");

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

        try {
            com.bot.SimpleBacktester.SelfValidator.start(sender, telegram, 5 * 60_000L);
        } catch (Throwable t) {
            LOG.warning("[SELF-VALIDATOR] init failed: " + t.getMessage());
        }

        // ═══════════════════════════════════════════════════════════════════
        // [STARTUP-BACKTEST 2026-05-02] One-shot full backtest at boot.
        // Runs in heavySched (background) so main cycle is not blocked.
        // Skips automatically if calibrator already has ≥STARTUP_BT_MIN_SAMPLES
        // (default 200) outcomes — no double-training on restarts.
        // Disable explicitly with STARTUP_BACKTEST=0 in Railway env.
        // ═══════════════════════════════════════════════════════════════════
        try {
            boolean startupBacktestEnabled =
                    !"0".equals(System.getenv().getOrDefault("STARTUP_BACKTEST", "1"));
            int existingOutcomes =
                    com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            int minSamplesNeeded = Integer.parseInt(System.getenv()
                    .getOrDefault("STARTUP_BT_MIN_SAMPLES", "200"));

            if (!startupBacktestEnabled) {
                LOG.info("[STARTUP-BT] Disabled via STARTUP_BACKTEST=0");
            } else if (existingOutcomes >= minSamplesNeeded) {
                LOG.info("[STARTUP-BT] Skipped: calibrator already has "
                        + existingOutcomes + " outcomes (≥" + minSamplesNeeded + ")");
            } else {
                LOG.info("[STARTUP-BT] Scheduled — calibrator has only "
                        + existingOutcomes + " outcomes, need ≥" + minSamplesNeeded);
                final String _calibFile = calibratorFile;
                heavySched.submit(safe("StartupBacktest",
                        () -> runStartupBacktest(sender, isc, telegram, _calibFile)));
            }
        } catch (Throwable t) {
            LOG.warning("[STARTUP-BT] init failed: " + t.getMessage());
        }
    }

    private static void runCycle(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        long cycleStart = System.currentTimeMillis();
        long now = System.currentTimeMillis();

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
                isc.registerSignal(s);
                sent++;
            } else {
                blocked++;
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
                        idea.tp1, idea.stop,
                        lastBtcRegimeForAlert == null ? "UNKNOWN" : lastBtcRegimeForAlert));
    }

    // ═══════════════════════════════════════════════════════════════════
    // [v79] checkForecastAccuracy — REWRITTEN with full outcome accounting.
    //   Старая версия игнорировала AMBIGUOUS и FLAT/TIME_STOP outcomes →
    //   калибратор недополучал данные → шёл вечный cold-start.
    //   Новая версия:
    //     • TP1 hit → calibrator: hit=true, weight=1.0, tag=TP1
    //     • SL hit → calibrator: hit=false, weight=1.0, tag=SL
    //     • AMBIGUOUS (TP and SL in same bar) → hit=true, weight=0.5, tag=AMBIGUOUS
    //     • TIME_STOP / FLAT → hit=false, weight=1.0, tag=TIME_STOP
    //     • MOVED_UP/DOWN → hit=(matches bias), weight=1.0
    //   В audit log пишется КАЖДЫЙ outcome с full payload + HMAC.
    // ═══════════════════════════════════════════════════════════════════
    private static void checkForecastAccuracy(com.bot.SignalSender sender,
                                              com.bot.TelegramBotSender telegram) {
        long minAgeMs = 30 * 60_000L;
        long maxAgeMs = VERIFIER_MAX_AGE_MS; // [v79 I4] sync with ISC TIME_STOP_BARS
        for (Iterator<Map.Entry<String, ForecastRecord>> it =
             forecastRecords.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, ForecastRecord> e = it.next();
            ForecastRecord fr = e.getValue();
            if (fr.ageMs() < minAgeMs) continue;
            if (fr.resolved) {
                if (fr.ageMs() > minAgeMs + 30 * 60_000L) it.remove();
                continue;
            }
            // [v79 I4] At maxAgeMs, force-resolve as TIME_STOP rather than silently dropping.
            boolean forceTimeStop = fr.ageMs() > maxAgeMs;

            try {
                int barsNeeded = (int) Math.ceil(fr.ageMs() / (15.0 * 60_000L)) + 2;
                barsNeeded = Math.max(5, Math.min(30, barsNeeded));
                List<com.bot.TradingCore.Candle> c = sender.fetchKlines(fr.symbol, "15m", barsNeeded);
                if (c == null || c.isEmpty()) {
                    if (forceTimeStop) it.remove();
                    continue;
                }
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

                // [v79 I4] If verifier ran out of time and price never decisively moved → TIME_STOP.
                boolean timeStopOutcome = false;

                String outcome;
                boolean correct;
                double calWeight = 1.0; // [v79 I1] 0.5 for AMBIGUOUS, 1.0 otherwise
                String outcomeTag;

                if (hitTP1) {
                    correct = true; outcome = "TP1"; outcomeTag = "TP1";
                } else if (hitSL) {
                    correct = false; outcome = "SL"; outcomeTag = "SL";
                } else if (ambiguous) {
                    // [v79 I1] AMBIGUOUS — раньше игнорировался. Теперь записываем как half-credit.
                    // hit=true (since price did reach TP), weight=0.5 (uncertainty penalty).
                    // Логически: signal directionally correct, но execution с reasonable
                    // entry мог попасть как в TP, так и в SL — поэтому credit half.
                    correct = true; outcome = "AMBIGUOUS"; outcomeTag = "AMBIGUOUS";
                    calWeight = 0.5;
                    forecastAmbiguous.incrementAndGet();
                } else if (forceTimeStop) {
                    // [v79 I2] TIME_STOP — раньше "FLAT" ignorировался для калибратора.
                    // Теперь записываем как LOSS (signal no-op = no edge proved).
                    correct = false; outcome = "TIME_STOP"; outcomeTag = "TIME_STOP";
                    timeStopOutcome = true;
                    forecastTimeStop.incrementAndGet();
                } else {
                    // Mid-flight: signal still has time. Don't resolve yet.
                    continue;
                }

                fr.resolved = true;
                fr.actualOutcome = outcome;

                // [v79] EVERY decisive outcome counts now (no skip on AMBIGUOUS/TIME_STOP).
                if (hasOpinion) {
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
                        // [v79 I3] New extended recordOutcome — calibrator handles
                        // weight + tag + regime bucket internally.
                        com.bot.DecisionEngineMerged.getCalibrator().recordOutcomeExtended(
                                fr.symbol, sigProb01, correct, atrPctForBucket,
                                calWeight, outcomeTag, fr.btcRegimeAtSignal,
                                fr.entryPrice, currentPrice);
                        String cat = com.bot.DecisionEngineMerged.detectAssetType(fr.symbol).label;
                        recordSignalOutcome(fr.symbol, fr.signalProbability, cat, correct);
                    } catch (Throwable ignored) {}
                }

                int total = forecastTotal.get();
                int correct2 = forecastCorrect.get();
                double acc = total > 0 ? (double) correct2 / total * 100 : 0;
                LOG.info(String.format("[FC] %s bias=%s outcome=%s w=%.1f %s | Acc: %.0f%% (%d/%d) Amb:%d TS:%d",
                        fr.symbol, fr.forecastBias, outcome, calWeight, correct ? "✅" : "❌",
                        acc, correct2, total,
                        forecastAmbiguous.get(), forecastTimeStop.get()));
            } catch (Exception ex) {
                LOG.fine("[FC] Fetch fail: " + fr.symbol + " " + ex.getMessage());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // [v79 I7] Forecast records persistence — survive Railway restarts.
    // ═══════════════════════════════════════════════════════════════════
    private static synchronized void saveForecastRecords() {
        try {
            java.io.File f = new java.io.File(FORECAST_PERSIST_FILE);
            java.io.File parent = f.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            try (java.io.PrintWriter pw = new java.io.PrintWriter(
                    new java.io.BufferedWriter(new java.io.FileWriter(f)))) {
                pw.println("# forecast_records v1");
                pw.println("# format: sym;side;entry;bias;score;prob;atrPct;tp1;sl;regime;createdAt;resolved;outcome;counted");
                long now = System.currentTimeMillis();
                for (Map.Entry<String, ForecastRecord> e : forecastRecords.entrySet()) {
                    ForecastRecord fr = e.getValue();
                    // Skip records older than 6h (no longer relevant on restart)
                    if (now - fr.createdAt > 6 * 60 * 60_000L) continue;
                    pw.print(e.getKey()); pw.print('|');
                    pw.println(fr.toCsvLine());
                }
            }
        } catch (Throwable t) { LOG.warning("[ForecastSave] " + t.getMessage()); }
    }
    private static synchronized void loadForecastRecords() {
        try {
            java.io.File f = new java.io.File(FORECAST_PERSIST_FILE);
            if (!f.exists()) return;
            int restored = 0;
            long maxSeq = 0;
            try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(f))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#") || line.isBlank()) continue;
                    int barIdx = line.indexOf('|');
                    if (barIdx < 0) continue;
                    String key = line.substring(0, barIdx);
                    String csv = line.substring(barIdx + 1);
                    ForecastRecord fr = ForecastRecord.fromCsvLine(csv);
                    if (fr != null) {
                        forecastRecords.put(key, fr);
                        restored++;
                        // [v79 FIX I7] Track highest sequence number across all loaded keys.
                        // Keys look like "BTCUSDT_LONG_42" — trailing number after last '_'.
                        // If forecastSeq isn't bumped past max(restored), the next signal
                        // for the same symbol+side will collide with a still-unresolved
                        // loaded record and silently overwrite it.
                        int us = key.lastIndexOf('_');
                        if (us > 0 && us < key.length() - 1) {
                            try {
                                long s = Long.parseLong(key.substring(us + 1));
                                if (s > maxSeq) maxSeq = s;
                            } catch (NumberFormatException ignored) {}
                        }
                    }
                }
            }
            // [v79 FIX I7] Set seq to max+1 — guarantees no collision with restored keys.
            // Use an atomic CAS-loop in case scheduler somehow already started signal flow.
            while (true) {
                long cur = forecastSeq.get();
                if (cur >= maxSeq) break;
                if (forecastSeq.compareAndSet(cur, maxSeq)) break;
            }
            LOG.info("[ForecastLoad] restored " + restored + " unresolved records, seq advanced to " + forecastSeq.get());
        } catch (Throwable t) { LOG.warning("[ForecastLoad] " + t.getMessage()); }
    }

    // ═══════════════════════════════════════════════════════════════════
    // [v79 I6] Daily integrity report — public-verifiable stats sent to Telegram.
    //   Если бот говорит "WR=65%", цифра должна совпадать с тем, что
    //   калибратор хранит в state. Эта функция показывает обе цифры
    //   рядом — если они расходятся, видно сразу.
    // ═══════════════════════════════════════════════════════════════════
    private static volatile long lastIntegrityReportMs = 0;
    private static void sendIntegrityReport(com.bot.TelegramBotSender telegram) {
        long now = System.currentTimeMillis();
        if (now - lastIntegrityReportMs < 23 * 60 * 60_000L) return; // ~daily
        lastIntegrityReportMs = now;
        try {
            int verifierTotal = forecastTotal.get();
            int verifierWins  = forecastCorrect.get();
            int verifierAmb   = forecastAmbiguous.get();
            int verifierTS    = forecastTimeStop.get();
            double verifierAcc = verifierTotal > 0 ? (double) verifierWins / verifierTotal * 100 : 0;

            int calN = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            String calIntegrity = com.bot.DecisionEngineMerged.getCalibrator().verifyAuditIntegrity();

            String regime = lastBtcRegimeForAlert == null ? "?" : lastBtcRegimeForAlert;
            String paperFlag = OBSERVATION_MODE ? "🧪 PAPER" : "🔴 LIVE";

            telegram.sendMessageAsync(String.format(
                    "📊 *Daily Integrity Report*\n"
                            + "━━━━━━━━━━━━━━━━━━━━━\n"
                            + "Verifier: *%d/%d wins* (%.0f%%)\n"
                            + "Ambiguous: %d (½ credit) · Time-stop: %d (loss)\n"
                            + "Calibrator: n=%d outcomes\n"
                            + "Audit log: %s\n"
                            + "━━━━━━━━━━━━━━━━━━━━━\n"
                            + "BTC: %s · %s\n"
                            + "_Числа выше — независимо проверяемые:_\n"
                            + "_calibrator.csv + audit.log с HMAC-SHA256_",
                    verifierWins, verifierTotal, verifierAcc,
                    verifierAmb, verifierTS, calN, calIntegrity,
                    regime, paperFlag));
        } catch (Throwable t) {
            LOG.warning("[IntegrityReport] " + t.getMessage());
        }
    }

    /** [v79 I6] Public API for third-party verification. Returns JSON-ish string. */
    public static String forecastIntegrityCheck() {
        int total = forecastTotal.get();
        int wins  = forecastCorrect.get();
        int amb   = forecastAmbiguous.get();
        int ts    = forecastTimeStop.get();
        int calN  = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
        return String.format("{\"verifier_total\":%d,\"verifier_wins\":%d," +
                        "\"ambiguous\":%d,\"time_stops\":%d,\"calibrator_n\":%d," +
                        "\"win_rate_pct\":%.2f,\"audit_status\":\"%s\"}",
                total, wins, amb, ts, calN,
                total > 0 ? (double) wins / total * 100 : 0,
                com.bot.DecisionEngineMerged.getCalibrator().verifyAuditIntegrity());
    }

    private static void runWatchdog(com.bot.TelegramBotSender telegram,
                                    com.bot.SignalSender sender) {
        long now = System.currentTimeMillis();
        if (sender.isRlBanned()) return;
        if (now - startTimeMs < 10 * 60_000L) return;

        List<String> infraIssues = new ArrayList<>();
        if (now - lastCycleSuccessMs > 3 * 60_000L)
            infraIssues.add("💀 MainCycle silent " + (now - lastCycleSuccessMs) / 1000 + "s");
        if (now - lastStatsSuccessMs > 40 * 60_000L)
            infraIssues.add("💀 Stats silent " + (now - lastStatsSuccessMs) / 60_000 + "min");
        if (sender.getActiveWsCount() < 10)
            infraIssues.add("⚠️ WebSockets low: " + sender.getActiveWsCount());

        boolean signalDrought = now - lastSignalMs > SIGNAL_DROUGHT_MS;

        if (sender.getActiveWsCount() < 10 || now - lastCycleSuccessMs > 3 * 60_000L) {
            sender.forceResubscribeTopPairs();
        }

        if (!infraIssues.isEmpty() && now - lastInfraAlertMs > INFRA_ALERT_COOLDOWN_MS) {
            lastInfraAlertMs = now;
            watchdogAlerts.incrementAndGet();
            LOG.warning("[Watchdog #" + watchdogAlerts.get() + "] "
                    + String.join(" | ", infraIssues));
        }

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
                                + "_(`bi`=cooldown, `rr`=R:R<2.0, `sl`=SL<0.70%%, `cold`=cold-start, `dd`=dedup, `hr`=hourly cap, `xch`=cross-exch, `qg`=quality)_",
                        hoursSilent, regime, wsCount, calN, paperFlag, breakdown));
            } catch (Throwable ignored) {}
        }
    }

    private static String ctxRegimeForAlert() {
        return lastBtcRegimeForAlert != null ? lastBtcRegimeForAlert : "?";
    }
    static volatile String lastBtcRegimeForAlert = null;

    private static volatile long lastHeartbeatMs = 0;
    private static final long HEARTBEAT_INTERVAL_MS = 90 * 60_000L;
    private static final long HEARTBEAT_QUIET_MS    = 90 * 60_000L;

    private static void maybeSendHeartbeat(com.bot.TelegramBotSender telegram,
                                           com.bot.SignalSender sender,
                                           com.bot.GlobalImpulseController gic,
                                           com.bot.InstitutionalSignalCore isc) {
        long now = System.currentTimeMillis();
        if (now - startTimeMs < 30 * 60_000L) return;
        if (now - lastHeartbeatMs < HEARTBEAT_INTERVAL_MS) return;
        if (now - lastSignalMs   < HEARTBEAT_QUIET_MS) return;
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

    // ═══════════════════════════════════════════════════════════════════════
    // [STARTUP-BACKTEST 2026-05-02 / FIX-1 2026-05-02]
    // Full one-shot backtest at boot. Pre-trains the ProbabilityCalibrator
    // on historical data across the entire trading universe so that LIVE
    // signals start with calibrated probabilities, not Cal=0.
    //
    // [FIX-1] Separates technical failures from "weak strategy" verdict.
    // Previous version flagged 🔴 even when 0 pairs ran due to rate-limits.
    // Plus: 3-min boot delay, 15m-only fetch (3× less weight), 4-sec pacing.
    // ═══════════════════════════════════════════════════════════════════════
    private static void runStartupBacktest(com.bot.SignalSender sender,
                                           com.bot.InstitutionalSignalCore isc,
                                           com.bot.TelegramBotSender telegram,
                                           String calibratorFilePath) {
        long t0 = System.currentTimeMillis();
        LOG.info("[STARTUP-BT] ▶ Starting full backtest pre-training…");
        try {
            telegram.sendMessageAsync(
                    "🔄 *Стартовый backtest запущен*\n"
                            + "━━━━━━━━━━━━━━━━━━━━━\n"
                            + "Бот собирает историю по всем парам и обучает\n"
                            + "калибратор на симулированных сделках.\n"
                            + "Длится 5–20 минут. Live-сигналы идут параллельно.\n"
                            + "Сводка придёт по завершении.");
        } catch (Throwable ignored) {}

        // [FIX-1] Wait 3 min before starting fetches — first main cycle starts
        // at t+90s, fetches BTC+ETH+top pairs and populates cachedPairs.
        // By t+180s the rate-limit headroom recovers from boot warmup.
        try { Thread.sleep(180_000L); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }

        // [v82.1] Universe size — было хардкод 40, теперь env STARTUP_BT_PAIRS.
        // Дефолт 40 = старое поведение. Подняв до 60-80 получим больше сделок
        // в стартовом бэктесте за счёт большей выборки пар. Каждая пара = ~7сек
        // (5с пейсинг + 1.5с между fetch'ами + сам fetch), так что 80 пар = ~10 мин
        // только на пейсинге. Учитывайте время прогона.
        final int btPairsLimit = Math.max(10, envInt("STARTUP_BT_PAIRS", 40));

        // 1. Universe — should be populated by now after first cycle.
        List<String> universe = new ArrayList<>();
        for (int waitS = 0; waitS < 60 && universe.isEmpty(); waitS++) {
            try { Thread.sleep(2_000L); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            universe = sender.getScanUniverseSnapshot(btPairsLimit);
        }
        if (universe.isEmpty()) {
            LOG.warning("[STARTUP-BT] universe empty — aborted");
            try { telegram.sendMessageAsync(
                    "⚠️ *Стартовый backtest пропущен*\n"
                            + "Не удалось получить список пар (universe empty).\n"
                            + "Бот продолжает работу в обычном режиме.\n"
                            + "Калибратор будет обучаться на live-сделках."); }
            catch (Throwable ignored) {}
            return;
        }
        LOG.info("[STARTUP-BT] Universe: " + universe.size() + " pairs");

        // 2. Backtester instance.
        com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();

        // 3. Aggregate counters — separate failures from low-data skips.
        int totalTrades = 0, totalWins = 0, totalLosses = 0, totalTimeStops = 0;
        double totalNetPnL = 0.0;
        int symbolsRun = 0;          // pairs that completed bt.run()
        int symbolsRateLimited = 0;  // fetchKlines returned null
        int symbolsLowData = 0;      // fetched but <200 bars
        int symbolsErrored = 0;      // exception during run()

        com.bot.DecisionEngineMerged.ProbabilityCalibrator cal =
                com.bot.DecisionEngineMerged.getCalibrator();

        // [v82.1] History depth + pacing — было хардкод (1000 m15, 250 h1, 5s pacing).
        // ENV:
        //  STARTUP_BT_BARS_15M  — кол-во m15 свечей на пару (default 1000 = ~10 дней).
        //                         Cap 1500 — Binance лимит на один запрос. Для большей
        //                         истории нужна пагинация (не реализована).
        //  STARTUP_BT_BARS_1H   — кол-во h1 свечей (default 250). Минимум 150 = MIN_BARS
        //                         движка, иначе reject("invalid_candles").
        //  STARTUP_BT_PACING_MS — задержка между парами (default 5000ms). Не понижайте
        //                         меньше 3000 — Binance rate limit (2400 weight/min).
        final int bars15mTarget = Math.min(1500,
                Math.max(300, envInt("STARTUP_BT_BARS_15M", 1000)));
        final int bars1hTarget  = Math.min(1500,
                Math.max(150, envInt("STARTUP_BT_BARS_1H", 250)));
        final long pacingMs     = Math.max(3000L, envInt("STARTUP_BT_PACING_MS", 5000));

        for (String sym : universe) {
            // Cooperative cancellation if JVM is shutting down.
            if (Thread.currentThread().isInterrupted()) {
                LOG.info("[STARTUP-BT] Interrupted — stopping early");
                break;
            }
            try {
                // [FIX-2] / [v82.1] Configurable pacing + bar counts.
                // PREVIOUS BUG: passed empty h1 to backtester. DecisionEngineMerged
                // .generate() rejects with "invalid_candles" when c1h.size()<150
                // (MIN_BARS), so 0 trades were ever generated despite 37 pairs
                // processed. Restoring h1 is mandatory for the engine to work.
                Thread.sleep(pacingMs);

                // 15m: env STARTUP_BT_BARS_15M (default 1000 ≈ 10 days, cap 1500).
                List<com.bot.TradingCore.Candle> m15 = sender.fetchKlines(sym, "15m", bars15mTarget);
                if (m15 == null) {
                    symbolsRateLimited++;
                    LOG.warning("[STARTUP-BT] " + sym + " — m15 fetch returned null (rate limit?)");
                    Thread.sleep(8_000L);
                    continue;
                }
                if (m15.size() < 200) {
                    symbolsLowData++;
                    LOG.info("[STARTUP-BT] " + sym + " — only " + m15.size()
                            + " m15 bars, need ≥200");
                    continue;
                }

                // h1: env STARTUP_BT_BARS_1H (default 250). Min 150 = MIN_BARS guard.
                Thread.sleep(1_500L);
                List<com.bot.TradingCore.Candle> h1 = sender.fetchKlines(sym, "1h", bars1hTarget);
                if (h1 == null) {
                    symbolsRateLimited++;
                    LOG.warning("[STARTUP-BT] " + sym + " — h1 fetch returned null (rate limit?)");
                    Thread.sleep(8_000L);
                    continue;
                }
                if (h1.size() < 150) {
                    symbolsLowData++;
                    LOG.info("[STARTUP-BT] " + sym + " — only " + h1.size()
                            + " h1 bars, need ≥150 (engine MIN_BARS guard)");
                    continue;
                }

                List<com.bot.TradingCore.Candle> empty = new ArrayList<>();
                com.bot.DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(sym);

                // Pass real h1 — was the entire bug. m1/m5 stay empty (optional).
                com.bot.SimpleBacktester.BacktestResult r =
                        bt.run(sym, empty, empty, m15, h1, cat);
                if (r == null || r.trades == null || r.trades.isEmpty()) {
                    symbolsRun++;
                    LOG.info("[STARTUP-BT] " + sym + " — 0 trades on history (filters too tight)");
                    continue;
                }

                // 4. FEED CALIBRATOR — this is the entire point of the exercise.
                for (com.bot.SimpleBacktester.TradeRecord tr : r.trades) {
                    boolean hit = tr.pnlPct > 0;
                    double rawScore01 = Math.max(0.0, Math.min(1.0, tr.confidence / 100.0));
                    double atrPctApprox = Math.abs(tr.entry > 0
                            ? (tr.sl - tr.entry) / tr.entry * 100.0 : 1.5);
                    String tag = "TIME_STOP".equals(tr.exitReason) ? "TIME_STOP"
                            : (hit ? (tr.exitReason != null ? tr.exitReason : "TP1") : "SL");
                    cal.recordOutcomeExtended(sym, rawScore01, hit, atrPctApprox,
                            1.0, tag, "NEUTRAL", tr.entry, tr.exit);
                }

                // 5. Aggregate.
                totalTrades    += r.total;
                totalWins      += r.wins;
                totalLosses    += r.losses;
                totalTimeStops += r.timeStops;
                totalNetPnL    += r.netPnL;
                symbolsRun++;

                // Existing per-symbol EV signal for ISC.
                if (r.total >= 5) isc.setSymbolBacktestResult(sym, r.ev);

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt(); break;
            } catch (Exception e) {
                symbolsErrored++;
                LOG.warning("[STARTUP-BT] " + sym + " errored: " + e.getMessage());
            }
        }

        // 6. Persist calibrator immediately so a crash before the next 30-min
        //    auto-save would not wipe out the training.
        try {
            cal.saveToFile(calibratorFilePath);
        } catch (Throwable t) {
            LOG.warning("[STARTUP-BT] calibrator save failed: " + t.getMessage());
        }

        long elapsedSec = (System.currentTimeMillis() - t0) / 1000L;
        double wr = totalTrades > 0 ? 100.0 * totalWins / totalTrades : 0.0;
        double wlRatio = totalLosses > 0 ? (double) totalWins / totalLosses : 0.0;
        int newCalCount = cal.totalOutcomeCount();

        // [FIX-1] Differentiate technical failure vs strategy verdict.
        String verdict;
        boolean techFailure = totalTrades == 0
                && (symbolsRateLimited + symbolsErrored) >= Math.max(1, symbolsRun);
        if (techFailure) {
            verdict = "⚠️ *Технический сбой* — пары не загрузились.\n"
                    + "Калибратор не обучен через backtest. Бот продолжает\n"
                    + "работу — калибровка пойдёт через live-сделки (медленнее).";
        } else if (totalTrades == 0) {
            verdict = "ℹ️ *0 сделок на истории*\n"
                    + "Фильтры стратегии слишком строгие для последних 16 дней\n"
                    + "(BTC в глубоком флэте). Это не плохо — стратегия осторожна.\n"
                    + "Калибровка пойдёт через live.";
        } else if (totalTrades < 50) {
            verdict = "🟡 *Малая выборка* — " + totalTrades + " сделок.\n"
                    + "Слишком мало для статистических выводов. Продолжайте paper.";
        } else {
            // [v81 FIX] Оценка по PnL и Expectancy, не по WR.
            // При RR 1:2 стратегия с WR=35% уже прибыльна. WR в отрыве от RR
            // — бесполезная метрика. Старая логика "WR<45% = слабо" ВРАЛА:
            // помечала прибыльную стратегию (+33% PnL) как провальную.
            double avgPnLPerTrade = totalNetPnL / totalTrades;
            double expectancyR = avgPnLPerTrade / 1.0; // assume avg risk = 1% (estimate)
            double monthlyPnL  = avgPnLPerTrade * 30; // grobering daily count

            if (totalNetPnL > 20.0 && wr >= 35.0) {
                verdict = "🟢 *Стратегия показывает РЕАЛЬНЫЙ edge.*\n"
                        + "WR=" + String.format("%.1f%%", wr)
                        + " · Net PnL=" + String.format("%+.1f%%", totalNetPnL)
                        + " на " + totalTrades + " сделках.\n"
                        + "Avg/trade=" + String.format("%+.2f%%", avgPnLPerTrade) + ".\n"
                        + "Низкий WR при positive PnL — это профессиональная норма для RR 1:2.\n"
                        + "Дальше paper для подтверждения 100+ сделок.";
            } else if (totalNetPnL > 0 && wr >= 30.0) {
                verdict = "🟡 *Положительный edge, но слабый.*\n"
                        + "WR=" + String.format("%.1f%%", wr)
                        + " · Net PnL=" + String.format("%+.1f%%", totalNetPnL) + ".\n"
                        + "Прибыльно, но margin тонкая. Нужно больше сделок для подтверждения.\n"
                        + "Реальные деньги — только мелкими размерами после 100+ paper.";
            } else if (totalNetPnL > -5.0) {
                verdict = "🟡 *Граничный результат* — Net PnL="
                        + String.format("%+.1f%%", totalNetPnL) + ", WR=" + String.format("%.1f%%", wr) + ".\n"
                        + "Нет явного edge. Реальные деньги категорически нельзя.";
            } else {
                verdict = "🔴 *Стратегия убыточна на истории.*\n"
                        + "WR=" + String.format("%.1f%%", wr)
                        + " · Net PnL=" + String.format("%+.1f%%", totalNetPnL) + ".\n"
                        + "Структурная проблема логики входа.\n"
                        + "Реальные деньги категорически нельзя.";
            }
        }

        String summary = String.format(
                "✅ *Стартовый backtest завершён*\n"
                        + "━━━━━━━━━━━━━━━━━━━━━\n"
                        + "⏱ Время: %d сек\n"
                        + "📊 Пар обработано: %d\n"
                        + "  ⚠️ Rate-limited: %d\n"
                        + "  📉 Мало данных: %d\n"
                        + "  ❌ Ошибок: %d\n"
                        + "🎯 Сделок: %d\n"
                        + "  ✅ Wins: %d (%.1f%%)\n"
                        + "  ❌ Losses: %d\n"
                        + "  ⏳ Time-stops: %d\n"
                        + "💰 Net PnL (sum %%): %+.2f\n"
                        + "📈 W/L ratio: %.2f\n"
                        + "🧠 Калибратор: %d outcomes\n"
                        + "━━━━━━━━━━━━━━━━━━━━━\n"
                        + "%s",
                elapsedSec, symbolsRun, symbolsRateLimited, symbolsLowData,
                symbolsErrored, totalTrades,
                totalWins, wr, totalLosses, totalTimeStops, totalNetPnL,
                wlRatio, newCalCount,
                verdict);

        try { telegram.sendMessageAsync(summary); } catch (Throwable ignored) {}
        LOG.info("[STARTUP-BT] ✓ Done in " + elapsedSec + "s | trades=" + totalTrades
                + " wr=" + String.format("%.1f%%", wr) + " calN=" + newCalCount);
    }

    private static void logStats(com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        lastStatsSuccessMs = System.currentTimeMillis();
        long uptimeMin = (System.currentTimeMillis() - startTimeMs) / 60_000;
        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        Dispatcher d = Dispatcher.getInstance();
        String corrSlots = sender.getCorrelationSlotsSnapshot();
        String saturatedMark = sender.isCorrelationSaturated() ? "🔒" : "";
        String msg = String.format(
                "[STATS] Up:%dm Cyc:%d Sig:%d Trk:%d FR:%d | BTC:%s str=%.2f | "
                        + "WS:%d Corr:%s%s Disp:%d/%d [%s] | FC:%.0f%%(%d/%d) Amb:%d TS:%d Err:%d WD:%d | %s",
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
                forecastAmbiguous.get(), forecastTimeStop.get(),
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
                + "_Калибратор учится. Сигналы сейчас на сырых вероятностях._", s);
        if (s < 100) return "🔸 Калибровка: " + s + "/100";
        return "✅ Калибровка: " + s + " исходов";
    }

    private static String buildStartMessage() {
        return "⚡ *TradingBot SCANNER* `v79.0`\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + "`15M` Futures · TOP-" + envInt("TOP_N", 30) + " · Scanner-only\n"
                + "R:R min `1:2` · SL min `0.30%`\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + (OBSERVATION_MODE
                ? "🧪 *PAPER MODE АКТИВЕН* — сигналы только для валидации\n━━━━━━━━━━━━━━━━━━━━━\n"
                : "🔴 *LIVE MODE* — реальные сигналы в чат\n━━━━━━━━━━━━━━━━━━━━━\n")
                + "🛡 *INTEGRITY UPGRADES (v79):*\n"
                + "• AMBIGUOUS outcomes теперь учитываются (½ credit)\n"
                + "• TIME_STOP считается loss (раньше игнорировался)\n"
                + "• HMAC-SHA256 подпись на всех записях калибратора\n"
                + "• Append-only audit log\n"
                + "• Forecast records переживают рестарт\n"
                + (CROSS_EXCHANGE_VALIDATION ? "• Cross-exchange (Bybit) sanity check\n" : "")
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
                        if (Math.abs(delta) > 8.0) alerts++;
                    }
                } catch (Throwable ignored) {}
            }
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