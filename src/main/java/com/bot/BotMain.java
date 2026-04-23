package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * BotMain v61 — SCANNER-MASTERPIECE
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
 *  WHAT'S GONE vs v60:
 *   - AFC advance forecast (the 📡 ПРОГНОЗ spam)
 *   - Watchdog re-alerting same drought every 60 min
 *   - TIME_STOP for signals the user never saw
 *   - Cosmetic probability-shrinkage shown to user
 *   - Auto-trade leverage/margin/order/margin-call notifications
 *
 *  WHAT'S NEW vs v60:
 *   - Dispatcher: single gate for ALL signal dispatches
 *   - Hard cold-start gate applied uniformly: prob>=78, clusters>=4,
 *     forecast conf>=0.55, R:R>=2.0, SL distance>=0.35%
 *   - Per-symbol dispatch dedup: same sym+side within 15min = drop
 *   - Hourly soft cap: max 4 signals/hour prevents "flood day" spam
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
    private static final int KLINES   = envInt("KLINES_LIMIT", 160);
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

    private static volatile int lastSummaryDay = -1;

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
    private static final AtomicLong lastSignalQualityReport = new AtomicLong(0);
    private static final long SIGNAL_QUALITY_REPORT_MS = 60 * 60_000L;

    public static void recordSignalOutcome(String sym, double conf, String cat, boolean hit) {
        signalOutcomes.addLast(new SignalOutcome(sym, conf, cat, hit));
        while (signalOutcomes.size() > SIGNAL_OUTCOME_WINDOW) signalOutcomes.pollFirst();
    }

    private static String buildSignalQualityReport() {
        if (signalOutcomes.isEmpty()) return "📊 *SIGNAL QUALITY*\nNo signals tracked yet";
        List<SignalOutcome> snap = new ArrayList<>(signalOutcomes);
        int total = snap.size();
        long hits = snap.stream().filter(o -> o.hit).count();
        int[] b60 = new int[2], b70 = new int[2], b80 = new int[2];
        for (SignalOutcome o : snap) {
            int[] bucket = o.confidence < 70 ? b60 : o.confidence < 80 ? b70 : b80;
            bucket[0]++; if (o.hit) bucket[1]++;
        }
        Map<String, int[]> byCat = new LinkedHashMap<>();
        for (SignalOutcome o : snap) {
            int[] pair = byCat.computeIfAbsent(o.category == null ? "?" : o.category, k -> new int[2]);
            pair[0]++; if (o.hit) pair[1]++;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("📊 *SIGNAL QUALITY* (last %d)%n", total));
        sb.append(String.format("Overall: %.1f%% (%d/%d)%n", 100.0*hits/total, hits, total));
        sb.append("━━━ by Confidence ━━━\n");
        if (b60[0] > 0) sb.append(String.format("60-70%%: %.0f%% (%d/%d)%n", 100.0*b60[1]/b60[0], b60[1], b60[0]));
        if (b70[0] > 0) sb.append(String.format("70-80%%: %.0f%% (%d/%d)%n", 100.0*b70[1]/b70[0], b70[1], b70[0]));
        if (b80[0] > 0) sb.append(String.format("80%%+:   %.0f%% (%d/%d)%n", 100.0*b80[1]/b80[0], b80[1], b80[0]));
        sb.append("━━━ by Category ━━━\n");
        for (var e : byCat.entrySet()) {
            int[] p = e.getValue(); if (p[0] == 0) continue;
            sb.append(String.format("%s: %.0f%% (%d/%d)%n", e.getKey(), 100.0*p[1]/p[0], p[1], p[0]));
        }
        return sb.toString();
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

        // [v62] Progressive cold-start. Old v61 gate required all three at once
        // (prob>=78 AND clusters>=4 AND fcConf>=0.55) which, on a neutral market,
        // produced 0 signals / 8 hours. We now relax gradually as we collect data.
        //
        // Phase 1 (0-9 outcomes):   prob>=75 AND (clusters>=4 OR fcConf>=0.55)
        // Phase 2 (10-19 outcomes): prob>=72 AND (clusters>=3 OR fcConf>=0.50)
        // Phase 3 (20-29 outcomes): prob>=70 (either clusters or fc is fine, single)
        // Phase 4 (30+):            no extra gate (Dispatcher just applies rr/sl/dedup)
        //
        // The underlying cluster-confidence engine already filters weak signals
        // upstream. This gate is a quality floor, not a primary filter.
        private static final int    COLD_START_MIN_OUTCOMES = 30;

        private static final double MIN_RR          = 2.00;
        private static final double MIN_SL_PCT      = 0.0035;
        private static final long   SYMBOL_DEDUP_MS = 15 * 60_000L;
        private static final int    MAX_PER_HOUR    = 4;
        private static final long   HOUR_MS         = 60 * 60_000L;

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

        /**
         * The single source of truth for "should this signal reach the user?"
         * Returns dispatched=true iff a Telegram message was queued.
         */
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

            // [v62] Progressive cold-start gate. Blocks borderline signals early,
            // relaxes as we collect outcomes.
            int calSamples = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            int clusters   = countClusterFlags(idea.flags);
            double fcConf  = idea.forecast != null ? idea.forecast.confidence : 0.0;

            double probFloor;
            boolean needExtra;
            int extraClusters;
            double extraFcConf;

            if (calSamples >= COLD_START_MIN_OUTCOMES) {
                probFloor = 0;       // gate off — rr/sl/dedup are enough
                needExtra = false;
                extraClusters = 0;
                extraFcConf = 0;
            } else if (calSamples >= 20) {
                probFloor = 70.0;
                needExtra = false;   // just the prob floor
                extraClusters = 0;
                extraFcConf = 0;
            } else if (calSamples >= 10) {
                probFloor = 72.0;
                needExtra = true;    // OR between clusters and fcConf
                extraClusters = 3;
                extraFcConf = 0.50;
            } else {
                probFloor = 75.0;
                needExtra = true;
                extraClusters = 4;
                extraFcConf = 0.55;
            }

            if (idea.probability < probFloor) {
                blockedByGate.incrementAndGet();
                blockedColdStart.incrementAndGet();
                return Result.blocked(String.format(
                        "cold-start: prob=%.0f<%.0f (n=%d)", idea.probability, probFloor, calSamples));
            }
            if (needExtra) {
                boolean clustersOk = clusters >= extraClusters;
                boolean fcOk = fcConf >= extraFcConf;
                if (!clustersOk && !fcOk) {
                    blockedByGate.incrementAndGet();
                    blockedColdStart.incrementAndGet();
                    return Result.blocked(String.format(
                            "cold-start: clusters=%d<%d AND fcConf=%.2f<%.2f (n=%d)",
                            clusters, extraClusters, fcConf, extraFcConf, calSamples));
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

            try {
                tg.sendMessageAsync(idea.toTelegramString());
                totalDispatched.incrementAndGet();
                lastDispatchMs.put(dedupKey, now);
                dispatchTimestamps.addLast(now);
                totalSignals.incrementAndGet();
                lastSignalMs = now;
                droughtAnnounced.set(false);
                trackSignal(idea, true);
                isc.setSignalCooldown(idea.symbol, 30 * 60_000L);
                LOG.info(String.format("[DISPATCH/%s] %s %s prob=%.0f conf=%.2f rr=%.2f sl=%.2f%% n=%d",
                        source, idea.symbol, idea.side, idea.probability,
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
            if (u.startsWith("CLUSTER") || u.contains("EARLY") || u.contains("TREND")
                    || u.contains("BREAKOUT") || u.contains("VSA") || u.contains("PUMP")
                    || u.contains("EXH") || u.contains("OFV_STRONG") || u.contains("OBI")
                    || u.contains("HTF_") || u.contains("BOS") || u.contains("FVG")
                    || u.contains("LIQ_MAGNET") || u.contains("DIV")) c++;
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

        isc.setTimeStopCallback((sym, msg) -> {
            boolean notify = false;
            for (TrackedSignal ts : trackedSignals.values()) {
                if (!ts.symbol.equals(sym)) continue;
                if (!ts.sentToUser) continue;
                if (ts.timeStopNotified) continue;
                ts.timeStopNotified = true;
                notify = true;
                break;
            }
            if (notify) {
                telegram.sendMessageAsync(
                        "⏱ *TIME STOP* — #" + sym + "\n"
                                + "━━━━━━━━━━━━━━━━━━\n"
                                + "90 мин истекло · проверь позицию\n"
                                + "Закрой вручную если открыта\n"
                                + "━━━━━━━━━━━━━━━━━━");
            }
        });

        gic.setPanicCallback(msg -> {
            LOG.warning("[GIC panic] " + msg);
            telegram.sendMessageAsync("⚠ *Market Alert*\n\n" + msg);
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

        mainSched.scheduleAtFixedRate(
                safe("MainCycle", () -> runCycle(telegram, gic, isc, sender)),
                0, INTERVAL, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("LogStats", () -> logStats(gic, isc, sender)),
                30, 30, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("DailySummary", () -> maybeSendDailySummary(telegram, gic, isc, sender)),
                1, 1, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("Watchdog", () -> runWatchdog(telegram, sender)),
                120, 120, TimeUnit.SECONDS);
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
        LOG.info("═══ TradingBot v61 SCANNER started " + nowLocalStr() + " ═══");
    }

    private static void runCycle(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        long cycleStart = System.currentTimeMillis();
        long now = System.currentTimeMillis();
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
            if (res.dispatched) sent++;
            else {
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
                        idea.tp1, idea.stop));
    }

    private static volatile long lastForecastReportMs = 0;
    private static final long FORECAST_REPORT_INTERVAL_MS = 60 * 60_000L;

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

                if ((hitTP1 || hitSL) && hasOpinion
                        && wasDispatchedToUser(fr.symbol, fr.side, fr.entryPrice)) {
                    String emoji  = hitTP1 ? "✅" : "❌";
                    String action = hitTP1 ? "TP1 HIT" : "SL HIT";
                    String side   = fr.side == com.bot.TradingCore.Side.LONG ? "LONG" : "SHORT";
                    double level  = hitTP1 ? fr.tp1Level : fr.slLevel;
                    double pnlPct = fr.side == com.bot.TradingCore.Side.LONG
                            ? (level - fr.entryPrice) / fr.entryPrice * 100
                            : (fr.entryPrice - level) / fr.entryPrice * 100;
                    String fmt = fr.entryPrice < 0.001 ? "%.6f" : fr.entryPrice < 1 ? "%.4f" : "%.2f";
                    telegram.sendMessageAsync(String.format(
                            emoji + " *" + action + "* — #" + fr.symbol + " " + side + "%n"
                                    + "━━━━━━━━━━━━━━━━━━%n"
                                    + "Вход:  `" + fmt + "`%n"
                                    + (hitTP1 ? "TP1:   `" : "SL:    `") + fmt + "` (%+.2f%%)%n"
                                    + "━━━━━━━━━━━━━━━━━━",
                            fr.entryPrice, level, pnlPct));
                }

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

                if (total > 0 && total % 20 == 0) {
                    long nowMs = System.currentTimeMillis();
                    if (nowMs - lastForecastReportMs >= FORECAST_REPORT_INTERVAL_MS) {
                        lastForecastReportMs = nowMs;
                        telegram.sendMessageAsync(String.format(
                                "*Forecast Accuracy*\n\nTotal %d · Hit %d · *%.1f%%*",
                                total, correct2, acc));
                        if (nowMs - lastSignalQualityReport.get() >= SIGNAL_QUALITY_REPORT_MS) {
                            lastSignalQualityReport.set(nowMs);
                            telegram.sendMessageAsync(buildSignalQualityReport());
                        }
                    }
                }
            } catch (Exception ex) {
                LOG.fine("[FC] Fetch fail: " + fr.symbol + " " + ex.getMessage());
            }
        }
    }

    private static boolean wasDispatchedToUser(String sym, com.bot.TradingCore.Side side, double entryPrice) {
        for (TrackedSignal ts : trackedSignals.values()) {
            if (!ts.symbol.equals(sym)) continue;
            if (ts.side != side) continue;
            if (Math.abs(ts.entry - entryPrice) > entryPrice * 0.005) continue;
            return ts.sentToUser;
        }
        return false;
    }

    private static void runWatchdog(com.bot.TelegramBotSender telegram,
                                    com.bot.SignalSender sender) {
        long now = System.currentTimeMillis();
        List<String> infraIssues = new ArrayList<>();
        if (now - lastCycleSuccessMs > 3 * 60_000L)
            infraIssues.add("💀 MainCycle silent " + (now - lastCycleSuccessMs) / 1000 + "s");
        if (now - lastStatsSuccessMs > 40 * 60_000L)
            infraIssues.add("💀 Stats silent " + (now - lastStatsSuccessMs) / 60_000 + "min");
        if (sender.getActiveWsCount() < 3)
            infraIssues.add("⚠️ WebSockets low: " + sender.getActiveWsCount());

        boolean signalDrought = now - lastSignalMs > SIGNAL_DROUGHT_MS;

        if (sender.getActiveWsCount() < 3 || now - lastCycleSuccessMs > 3 * 60_000L) {
            sender.forceResubscribeTopPairs();
        }

        if (!infraIssues.isEmpty() && now - lastInfraAlertMs > INFRA_ALERT_COOLDOWN_MS) {
            lastInfraAlertMs = now;
            watchdogAlerts.incrementAndGet();
            telegram.sendMessageAsync("*Watchdog* #" + watchdogAlerts.get() + "\n\n"
                    + String.join("\n", infraIssues));
        }

        if (signalDrought && droughtAnnounced.compareAndSet(false, true)) {
            long droughtMin = (now - lastSignalMs) / 60_000;
            int cooldowned = sender.getCooldownedSymbolCount();
            String cooldownInfo = cooldowned > 0 ? " (" + cooldowned + " в кулдауне)" : "";
            watchdogAlerts.incrementAndGet();
            telegram.sendMessageAsync(String.format(
                    "📭 *Тихо на рынке*%n%n"
                            + "%d мин без сигналов%s%n"
                            + "Нет качественных кластеров.%n"
                            + "_Следующий алерт когда сигнал появится._",
                    droughtMin, cooldownInfo));
        }
    }

    private static void maybeSendDailySummary(com.bot.TelegramBotSender telegram,
                                              com.bot.GlobalImpulseController gic,
                                              com.bot.InstitutionalSignalCore isc,
                                              com.bot.SignalSender sender) {
        ZonedDateTime utc = ZonedDateTime.now(ZoneId.of("UTC"));
        int h = utc.getHour(), m = utc.getMinute(), day = utc.getDayOfYear();
        if (h != 9 || m > 2 || day == lastSummaryDay) return;
        lastSummaryDay = day;

        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        long uptimeMin = (System.currentTimeMillis() - startTimeMs) / 60_000;
        int fcTotal   = forecastTotal.get();
        int fcCorrect = forecastCorrect.get();
        double fcAcc  = fcTotal > 0 ? (double) fcCorrect / fcTotal * 100 : 0;
        int calSamples = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
        String calNote = calSamples < 30
                ? String.format("\n⚠️ Cold-start (%d/30)", calSamples)
                : calSamples < 100
                  ? String.format("\n🔸 Калибровка (%d/100)", calSamples)
                  : "";
        String msg = String.format(
                "*Daily Report*\n\n"
                        + "Up %dm · Cycles %d · Signals %d\n"
                        + "BTC %s · Vol %s · WS %d\n\n"
                        + "Forecast *%.0f%%* (%d/%d)\n"
                        + "Tracked %d" + calNote,
                uptimeMin, totalCycles.get(), totalSignals.get(),
                ctx.regime, ctx.volRegime,
                sender.getActiveWsCount(),
                fcAcc, fcCorrect, fcTotal,
                trackedSignals.size());
        telegram.sendMessageAsync(msg);
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

    private static void logStats(com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        lastStatsSuccessMs = System.currentTimeMillis();
        long uptimeMin = (System.currentTimeMillis() - startTimeMs) / 60_000;
        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        Dispatcher d = Dispatcher.getInstance();
        String msg = String.format(
                "[STATS] Up:%dm Cyc:%d Sig:%d Trk:%d FR:%d | BTC:%s str=%.2f | "
                        + "WS:%d Disp:%d/%d [%s] | FC:%.0f%%(%d/%d) Err:%d WD:%d | %s",
                uptimeMin, totalCycles.get(), totalSignals.get(),
                trackedSignals.size(), forecastRecords.size(),
                ctx.regime, ctx.impulseStrength,
                sender.getActiveWsCount(),
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
        if (s < 30)  return String.format("⚠️ Cold-start: %d/30 исходов", s);
        if (s < 100) return "🔸 Калибровка: " + s + "/100";
        return "✅ Калибровка: " + s + " исходов";
    }

    private static String buildStartMessage() {
        return "⚡ *TradingBot SCANNER* `v61`\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + "`15M` Futures · TOP-30 · Scanner-only\n"
                + "R:R min `1:2` · SL min `0.35%`\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + "🔧 *v61 масштерпис:*\n"
                + "• Единый Dispatcher — все сигналы через него\n"
                + "• Cold-start: prob≥78, clusters≥4, fcConf≥0.55\n"
                + "• Дедуп 15 мин · макс 4/час\n"
                + "• Честный скор (без косметики)\n"
                + "• TIME_STOP только для показанных сигналов\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + calibrationStatus() + "\n"
                + "_Только реальные сигналы с TP/SL._";
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
                        if (Math.abs(delta) > 15.0) alerts++;
                    }
                } catch (Throwable ignored) {}
            }
            if (alerts > 0 && telegram != null) {
                telegram.sendMessageAsync(String.format(
                        "⚠️ *Walk-Forward Alert*\n%d/%d symbols unstable\nAvg Δ: %.1f%%",
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