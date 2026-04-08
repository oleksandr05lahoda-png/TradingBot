package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║       BotMain v36.0 — ARCH FIX: WS CANDLES + ORDER EXEC + ALERTS RESTORED       ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  [v34.0] §1 AssetType auto-detection (crypto/oil/gas/metals/forex)     ║
 * ║  [v34.0] §2 Dynamic CoinCategory (volume-based TOP, keyword MEME)      ║
 * ║  [v34.0] §3 TOP coin EARLY_TICK fix (BTC/ETH velocity thresholds)      ║
 * ║  [v34.0] §4 PumpHunter category-aware thresholds (60% for TOP)         ║
 * ║  [v34.0] §5 Telegram format: asset type + category in first line       ║
 * ║  [v34.0] §6 Advance Forecast shows asset type                          ║
 * ║  [v34.0] §7 Extended sector detection (AI, RWA, DePin + commodities)   ║
 * ║  [v17.0] §1 Bipolar Guard: isSymbolAvailable() in runCycle dispatch     ║
 * ║  [v17.0] §1 Final thread-safe bipolar check before telegram.send()      ║
 * ║  [v17.0] §4 DrawdownManager: SL/TP explicit reason in closeTrade()      ║
 * ║  [v17.0] §4 Trail-SL maps to "SL" reason; Trail-TP maps to "TP"        ║
 * ║  [v15.0] FIX Дыра 1: ConcurrentLinkedDeque everywhere (thread safety)  ║
 * ║  [v15.0] FIX Дыра 3: LR window 30→10, acceleration detection          ║
 * ║  [v15.0] FIX Дыра 4: Asymmetric streak reset (win halves boost)        ║
 * ║  [v15.0] FIX KITEUSDT: VolatilitySqueezeGuard blocks squeeze signals   ║
 * ║  [v14.0] FIX #1: tp2Hit флаг — убрал бесконечные TP2 уведомления      ║
 * ║  [v14.0] FIX #2: Circuit breaker — убрал Thread.sleep из scheduler     ║
 * ║  [v14.0] FIX #3: Trailing stop SHORT — исправлена инверсия логики      ║
 * ║  [v14.0] FIX #4: Thread safety — volatile на errorsInWindow            ║
 * ║  [v14.0] FIX #5: Memory leak — forecastRecords с MAX_SIZE + TTL        ║
 * ║  [v14.0] FIX #6: TrackedSignal — synchronized extreme updates          ║
 * ║  [v14.0] FIX #7: ForecastEngine integration                            ║
 * ║  [v14.0] FIX #8: Тихие часы расширены до 01:00-05:00 UTC              ║
 * ║  [v14.0] FIX #9: Forecast accuracy — исправлена логика correct/wrong   ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public final class BotMain {

    private static final Logger LOG = Logger.getLogger(BotMain.class.getName());

    // ── Конфигурация из env ───────────────────────────────────────────────
    private static final String TG_TOKEN  = requireEnv("TELEGRAM_TOKEN");
    private static final String CHAT_ID   = requireEnv("TELEGRAM_CHAT_ID");

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException(
                    "Required env var missing: " + name
                            + ". Set it in your environment, do NOT hardcode in source.");
        }
        return v;
    }
    // Auto-detected at startup: env TIMEZONE → IP geolocation → Warsaw fallback.
    // Each instance (yours in Warsaw, father's in Zaporizhzhia) detects its own timezone.
    private static final ZoneId ZONE      = detectTimezone();
    private static final int    INTERVAL  = envInt("SIGNAL_INTERVAL_MIN", 1);
    private static final int    KLINES    = envInt("KLINES_LIMIT", 220);
    // Hard cap to avoid Telegram queue backlog (which can make the bot
    // "silent" for hours/days under heavy signal load).
    // [FIX #1] MAX_SIGNALS_PER_CYCLE was 50 — comment said "10→5" but value was never changed.
    // 50 signals/cycle floods the Telegram queue, causes the bot to appear "silent" for hours
    // while the queue drains. 5 forces only the highest-confidence ideas to be sent.
    private static final int    MAX_SIGNALS_PER_CYCLE = envInt("MAX_SIGNALS_PER_CYCLE", 5); // was 50 — CRITICAL BUG

    // ── Секторальные лидеры для GIC ───────────────────────────────────────
    private static final Map<String, String> SECTOR_LEADERS = new LinkedHashMap<>() {{
        put("DOGEUSDT", "MEME");
        put("SOLUSDT",  "L1");
        put("UNIUSDT",  "DEFI");
        put("LINKUSDT", "INFRA");
        put("ETHUSDT",  "TOP");
        put("XRPUSDT",  "PAYMENT");
        put("AVAXUSDT", "L1");
        put("BNBUSDT",  "CEX");
    }};

    // ── Счётчики ──────────────────────────────────────────────────────────
    private static final AtomicLong totalCycles   = new AtomicLong(0);
    private static final AtomicLong totalSignals  = new AtomicLong(0);
    private static final AtomicLong errorCount    = new AtomicLong(0);
    // [v36-FIX Дыра9] Монотонный счётчик для ключей forecastRecords.
    // System.currentTimeMillis() вызывает коллизию если два сигнала приходят за 1ms.
    private static final AtomicLong forecastSeq   = new AtomicLong(0);
    private static long startTimeMs = 0;

    // ── Circuit breaker ─────────────────────────────────────────────────
    // [v14.0 FIX #2] Убран Thread.sleep. Вместо паузы — пропускаем циклы.
    // [v14.0 FIX #4] volatile на errorsInWindow
    // [v30] CB_THRESHOLD raised 5→10, window raised 5→10m, pause reduced 2→1m.
    // Old thresholds: 5 errors in 5 minutes → 2 min pause. Too aggressive.
    // A brief Binance API slowdown (common) would trigger 5 errors and pause the bot,
    // causing it to miss entire signal cycles. Now requires 10 errors in 10 min.
    private static final int  CB_THRESHOLD = 10;
    private static final long CB_WINDOW_MS = 10 * 60_000L;
    private static final long CB_PAUSE_MS  = 60_000L; // 1 min (was 2)
    private static volatile long lastErrorWindowStart = 0;
    // [v24.0 FIX BUG-4] AtomicInteger (was volatile int — race on ++ and >= check)
    private static final AtomicInteger errorsInWindow = new AtomicInteger(0);
    private static volatile long cbPauseUntil         = 0; // [FIX #2] время окончания паузы

    // ── Watchdog ──────────────────────────────────────────────────────────
    private static volatile long lastSignalMs       = 0;
    private static volatile long lastCycleSuccessMs = 0;
    private static volatile long lastStatsSuccessMs = 0;
    private static volatile long lastWatchdogAlertMs = 0;
    private static final long SIGNAL_DROUGHT_MS     = 60 * 60_000L;  // [v38.0] 30→60min: tight filters = fewer signals, not a bug
    private static final long WATCHDOG_COOLDOWN_MS  = 30 * 60_000L;  // [v38.0] 10→30min: reduce spam
    private static final AtomicLong watchdogAlerts  = new AtomicLong(0);

    // ── Daily summary ─────────────────────────────────────────────────────
    private static volatile int lastSummaryDay = -1;

    // ── [MODULE 4 v33] ADVANCE FORECAST STATE ────────────────────────────
    // Дедупликация: отправляем прогноз только если он ИЗМЕНИЛСЯ с прошлого цикла.
    // Без этого каждые 5 минут одинаковый "РОСТ БИТКОИНА" будет спамить чат.
    private static final java.util.concurrent.ConcurrentHashMap<String, String>
            lastForecastSent = new java.util.concurrent.ConcurrentHashMap<>();
    // Cooldown: один прогноз по одной паре не чаще раза в 20 минут
    private static final java.util.concurrent.ConcurrentHashMap<String, Long>
            forecastCooldown = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long ADVANCE_FORECAST_COOLDOWN_MS = 20 * 60_000L;
    // Порог значимости прогноза: слабые сигналы не отправляем
    private static final double AFC_MIN_DIRECTION_SCORE = 0.28; // |score| >= 0.28 (умеренный)
    private static final double AFC_STRONG_SCORE        = 0.55; // |score| >= 0.55 (сильный)
    // Максимум прогнозов за один запуск (не спамим)
    private static final int AFC_MAX_PER_RUN = 4;

    // ── Forecast accuracy tracker ─────────────────────────────────────────
    // [v14.0 FIX #5] MAX_FORECAST_RECORDS предотвращает утечку памяти
    private static final int MAX_FORECAST_RECORDS = 500;
    static final ConcurrentHashMap<String, ForecastRecord> forecastRecords = new ConcurrentHashMap<>();

    static final class ForecastRecord {
        final String symbol;
        final com.bot.TradingCore.Side side;
        final double entryPrice;
        final String forecastBias;
        final double forecastScore;
        // [PATCH v43 FIX #1] signalProbability = the FINAL calibrated probability shown in the
        // Telegram signal (e.g. "72%"). Previously we were feeding forecastScore (a raw
        // directional score from ForecastEngine) into the calibrator — which is WRONG because:
        //   a) forecastScore ∈ [-1..+1], not [0..1]
        //   b) it measures forecast direction confidence, NOT signal entry quality
        // The calibrator needs the same number the user sees — the signal probability.
        // This closes the feedback loop: signal said 72% → trade resolved WIN/LOSS →
        // calibrator learns "72% raw maps to X% real win-rate" → future signals corrected.
        final double signalProbability;
        // [v43] Long-term robust ATR% at signal time — needed for vol-bucketed calibration.
        // Using current ATR (from 15m candle) was wrong: in consolidation ATR shrinks 40-60%
        // making the bucket lookup wrong (e.g. MEDIUM→LOW). Use 30-day percentile instead.
        final double robustAtrPctAtSignal;
        final long   createdAt;
        volatile boolean resolved = false;
        volatile String  actualOutcome = null;
        // [FIX #19] Track whether this record was already counted in forecastTotal/forecastCorrect.
        // markForecastRecord() and checkForecastAccuracy() both increment counters.
        // When TradeResolver is re-enabled, both paths could fire for the same record → double count.
        volatile boolean counted = false;

        ForecastRecord(String sym, com.bot.TradingCore.Side side, double price,
                       String bias, double score, double signalProb, double robustAtrPct) {
            this.symbol = sym; this.side = side; this.entryPrice = price;
            this.forecastBias = bias; this.forecastScore = score;
            this.signalProbability  = signalProb;
            this.robustAtrPctAtSignal = robustAtrPct;
            this.createdAt = System.currentTimeMillis();
        }
        // [v43] Back-compat constructor (signalProb unknown → use forecastScore as fallback)
        ForecastRecord(String sym, com.bot.TradingCore.Side side, double price,
                       String bias, double score) {
            this(sym, side, price, bias, score, score, 1.0);
        }
        long ageMs() { return System.currentTimeMillis() - createdAt; }
    }

    // ── Forecast accuracy stats ───────────────────────────────────────────
    private static final AtomicInteger forecastTotal   = new AtomicInteger(0);
    private static final AtomicInteger forecastCorrect = new AtomicInteger(0);

    // ══════════════════════════════════════════════════════════════
    //  TrackedSignal — v14: tp2Hit + synchronized extremes
    // ══════════════════════════════════════════════════════════════

    static final ConcurrentHashMap<String, TrackedSignal> trackedSignals = new ConcurrentHashMap<>();

    static final class TrackedSignal {
        final String          symbol;
        final com.bot.TradingCore.Side side;
        final double          entry, sl, tp1, tp2, tp3;
        final long            createdAt;
        final String          forecastBias;
        final double          forecastScore;

        // [v14.0 FIX #1] Добавлен tp2Hit флаг
        volatile boolean tp1Hit          = false;
        volatile boolean tp2Hit          = false; // ← NEW: предотвращает дубли TP2 сообщений
        volatile double  trailingStop    = 0;

        // [v25.0] Chandelier Exit — активируется при +0.8% от entry, до TP1.
        // Позволяет закрыть позицию с прибылью если цена разворачивается
        // не дойдя до TP1. ATR-based trailing на 1m свечах.
        volatile boolean chandelierActive = false;

        // [v24.0 FIX] NEGATIVE_INFINITY for max search (Double.MIN_VALUE = tiny positive number)
        private double  extremeLow    = Double.MAX_VALUE;
        private double  extremeHigh   = Double.NEGATIVE_INFINITY;
        private final Object extremeLock = new Object();

        TrackedSignal(String sym, com.bot.TradingCore.Side side,
                      double entry, double sl, double tp1, double tp2, double tp3,
                      String forecastBias, double forecastScore) {
            this.symbol = sym; this.side = side; this.entry = entry;
            this.sl = sl; this.tp1 = tp1; this.tp2 = tp2; this.tp3 = tp3;
            this.forecastBias = forecastBias; this.forecastScore = forecastScore;
            this.createdAt = System.currentTimeMillis();
        }
        long ageMs() { return System.currentTimeMillis() - createdAt; }

        // [v14.0 FIX #6] Thread-safe extreme updates
        void updateExtremes(double newLow, double newHigh) {
            synchronized (extremeLock) {
                extremeLow  = Math.min(extremeLow, newLow);
                extremeHigh = Math.max(extremeHigh, newHigh);
            }
        }
        double getExtremeLow()  { synchronized (extremeLock) { return extremeLow; } }
        double getExtremeHigh() { synchronized (extremeLock) { return extremeHigh; } }
    }

    // ══════════════════════════════════════════════════════════════
    //  SafeRunnable
    // ══════════════════════════════════════════════════════════════

    private static Runnable safe(String name, Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Throwable t) {
                errorCount.incrementAndGet();

                // [v32] Fix Circuit Breaker Over-sensitivity:
                // Only trigger CB for internal logic crashes or critical order errors.
                // Ignore transient API/network/JSON parsing glitches.
                boolean isTransient = t instanceof java.io.IOException ||
                        t instanceof java.net.http.HttpTimeoutException ||
                        t.getClass().getSimpleName().toLowerCase().contains("json") ||
                        (t.getMessage() != null && (t.getMessage().contains("502") || t.getMessage().contains("timeout")));

                if (!isTransient) {
                    errorsInWindow.incrementAndGet();
                }

                LOG.log(Level.SEVERE, "[SAFE] Task '" + name + "' FAILED: " + t.getMessage(), t);
            }
        };
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN
    // ══════════════════════════════════════════════════════════════

    public static void main(String[] args) {
        // [SCANNER MODE v1.0] Force UTF-8 on stdout/stderr so Cyrillic log lines are readable on Railway.
        // Without this, JVM uses the container's default charset (often ASCII/Latin-1) → ??? ????????
        try {
            System.setOut(new java.io.PrintStream(System.out, true, "UTF-8"));
            System.setErr(new java.io.PrintStream(System.err, true, "UTF-8"));
        } catch (java.io.UnsupportedEncodingException ignored) {}

        configureLogger();

        if (TG_TOKEN == null || TG_TOKEN.isBlank()) {
            LOG.severe("TELEGRAM_TOKEN не задан — выход.");
            System.exit(1);
        }

        startTimeMs              = System.currentTimeMillis();
        lastErrorWindowStart     = startTimeMs;
        lastSignalMs             = startTimeMs;
        lastCycleSuccessMs       = startTimeMs;
        lastStatsSuccessMs       = startTimeMs;

        final com.bot.TelegramBotSender telegram = new com.bot.TelegramBotSender(TG_TOKEN, CHAT_ID);
        final com.bot.GlobalImpulseController gic = new com.bot.GlobalImpulseController();
        final com.bot.InstitutionalSignalCore isc = new com.bot.InstitutionalSignalCore();
        final com.bot.SignalSender sender         = new com.bot.SignalSender(telegram, gic, isc);

        // Pass auto-detected timezone to signal formatter
        com.bot.DecisionEngineMerged.USER_ZONE = ZONE;

        // [BUG-FIX v33.1] panicCallback was routed to LOG only — trader never saw BTC CRASH alerts.
        // Now: GIC CRASH/PANIC events go to Telegram immediately (bypass normal queue, use offerFirst).
        isc.setTimeStopCallback((sym, msg) -> LOG.info("[ISC time-stop] " + sym + ": " + msg));
        gic.setPanicCallback(msg -> {
            LOG.warning("[GIC panic] " + msg);
            telegram.sendMessageAsync("⚠ *Market Alert*\n\n" + msg);
        });

        // ── Schedulers ───────────────────────────────────────────
        ScheduledExecutorService mainSched = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "TradingBot-Main");
            t.setDaemon(false);
            t.setUncaughtExceptionHandler((th, ex) ->
                    LOG.log(Level.SEVERE, "UNCAUGHT in " + th.getName(), ex));
            return t;
        });

        ScheduledExecutorService auxSched = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "TradingBot-Aux");
            t.setDaemon(true);
            return t;
        });

        ExecutorService heavySched = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "TradingBot-Heavy");
            t.setDaemon(true);
            return t;
        });

        // ── Main cycle (каждые INTERVAL минут) ───────────────────
        mainSched.scheduleAtFixedRate(
                safe("MainCycle", () -> runCycle(telegram, gic, isc, sender)),
                0, INTERVAL, TimeUnit.MINUTES);

        // ── Stats в лог каждые 15 минут ──────────────────────────
        auxSched.scheduleAtFixedRate(
                safe("LogStats", () -> logStats(telegram, gic, isc, sender)),
                15, 15, TimeUnit.MINUTES);

        // ── Daily summary в Telegram в 09:00 UTC ─────────────────
        auxSched.scheduleAtFixedRate(
                safe("DailySummary", () -> maybeSendDailySummary(telegram, gic, isc, sender)),
                1, 1, TimeUnit.MINUTES);

        // ── Watchdog каждые 60s ──────────────────────────────────
        auxSched.scheduleAtFixedRate(
                safe("Watchdog", () -> runWatchdog(telegram, gic, isc, sender)),
                60, 60, TimeUnit.SECONDS);

        // ── [SCANNER MODE v2.0] TradeResolver DISABLED ───────────────
        // Бот работает в режиме чистого сканера сигналов.
        // TradeResolver отслеживал виртуальные позиции и слал фантомные
        // TP/SL уведомления по сделкам, которые трейдер не открывал.
        // При ручной торговле это создаёт путаницу. Отключено до подключения автоисполнения.
        //
        // auxSched.scheduleAtFixedRate(
        //         safe("TradeResolver", () -> runTradeResolver(sender, isc, telegram)),
        //         90, 45, TimeUnit.SECONDS);

        // ── [SCANNER MODE v2.0] PositionStatus DISABLED ──────────────
        // Нет виртуальных позиций → нет смысла в отчёте о позициях.
        //
        // auxSched.scheduleAtFixedRate(
        //         safe("PositionStatus", () -> sendPositionStatus(telegram)),
        //         15, 30, TimeUnit.MINUTES);

        // ── [SCANNER MODE v2.0] ForecastChecker DISABLED ─────────────
        // Точность прогноза считалась по виртуальным TP/SL.
        // Без TradeResolver данные некорректны. Отключено.
        //
        // [v42.1 FIX] RE-ENABLED. Scanner-mode compatible: checkForecastAccuracy
        // does NOT use virtual TP/SL — it compares real market price vs entry after
        // 2h against a dynamic ATR threshold (see BotMain.checkForecastAccuracy).
        // This is the feedback loop that populates ProbabilityCalibrator with real
        // win/loss outcomes. Without it the calibrator stays empty and all #1 work
        // is dead weight. MUST stay enabled.
        auxSched.scheduleAtFixedRate(
                safe("ForecastChecker", () -> checkForecastAccuracy(sender, telegram)),
                16, 15, TimeUnit.MINUTES);

        // ── [v42.1 FIX #10] WalkForward — daily out-of-sample validation ─
        // Runs once every 24h. For each active symbol, slides a train/test
        // window over 30 days of history. If test win-rate drops >15% vs
        // train, emits an overfit alert to Telegram. This is the only
        // protection against SignalOptimizer overfitting to historical noise.
        auxSched.scheduleAtFixedRate(
                safe("WalkForward", () -> runWalkForwardValidation(sender, telegram)),
                6 * 60, 24 * 60, TimeUnit.MINUTES);

        // ── [MODULE 4 v33] ADVANCE FORECAST ALERTS — каждые 5 минут ──
        // Анализирует рыночную структуру и отправляет заблаговременные
        // прогнозы о готовящихся пампах/дампах/разворотах — ДО сигнала входа.
        // Это даёт трейдеру время подготовиться: поставить лимитник, убрать стоп,
        // уменьшить позицию — до того как движение начнётся.
        auxSched.scheduleAtFixedRate(
                safe("AdvanceForecast", () -> runAdvanceForecast(sender, gic, isc, telegram)),
                3, 5, TimeUnit.MINUTES);

        // ── Backtest каждые 2 часа в изолированном потоке ─────────
        auxSched.scheduleAtFixedRate(
                safe("BacktestSubmit", () ->
                        heavySched.submit(safe("Backtest",
                                () -> runPeriodicBacktest(sender, isc, telegram)))),
                30, 120, TimeUnit.MINUTES);

        // ── Shutdown hook ────────────────────────────────────────
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("═══ Завершение работы. Цикл: " + totalCycles.get()
                    + " | Сигналов: " + totalSignals.get() + " ═══");
            mainSched.shutdown();
            auxSched.shutdown();
            heavySched.shutdown();
            telegram.shutdown();
            try { mainSched.awaitTermination(8, TimeUnit.SECONDS); }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                mainSched.shutdownNow();
            }
        }, "ShutdownHook"));

        // ── Startup ping ─────────────────────────────────────────
        telegram.sendMessageAsync(buildStartMessage());
        LOG.info("═══ TradingBot v15.0 ARCHITECTURE FIX стартовал " + nowWarsawStr() + " ═══");
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN CYCLE
    // ══════════════════════════════════════════════════════════════

    private static void runCycle(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        long cycleStart = System.currentTimeMillis();

        // [v30] QUIET HOURS REMOVED — 24/7 operation.
        // Crypto markets do not close. Removing the Asia-session blackout that was
        // causing the bot to "sleep" between 01:00-05:00 UTC.
        // Session weight still reduces POSITION SIZE during low-liquidity hours
        // via getPositionSizeUsdt() — this is the correct approach.

        // [v14.0 FIX #2] Circuit breaker — skip cycle instead of sleeping
        long now = System.currentTimeMillis();
        if (now < cbPauseUntil) {
            LOG.fine("[CB] Пауза активна, осталось " + (cbPauseUntil - now) / 1000 + "s");
            return;
        }
        if (now - lastErrorWindowStart > CB_WINDOW_MS) {
            lastErrorWindowStart = now;
            errorsInWindow.set(0);
        }
        if (errorsInWindow.get() >= CB_THRESHOLD) {
            cbPauseUntil = now + CB_PAUSE_MS;
            errorsInWindow.set(0);
            LOG.warning("[CB] Слишком много ошибок, пауза до " + formatLocalTime(cbPauseUntil));
            return;
        }

        long cycle = totalCycles.incrementAndGet();
        LOG.info("══ ЦИКЛ #" + cycle + " ══ " + nowWarsawStr());

        // BTC контекст + секторальные лидеры
        updateBtcContext(sender, gic);
        updateSectors(sender, gic);

        double bal = sender.getAccountBalance();
        if (bal > 0) isc.updateBalance(bal);

        // [FIX #2] updateBayesPrior threshold aligned: was 20, function requires 50 to blend.
        // Calling with 20–49 confirmed trades just sets 0.55 hardcoded prior — wasted call.
        // Now we call only when the function can actually use the data (>= 50 trades).
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
            LOG.info("Нет сигналов. " + isc.getStats());
            return;
        }

        // Keep only top signals per cycle to prevent Telegram queue backlog.
        // Priority: probability desc, then forecast confidence, then abs(directionScore).
        signals.sort(Comparator
                .comparingDouble((com.bot.DecisionEngineMerged.TradeIdea i) -> i.probability).reversed()
                .thenComparingDouble(i -> i.forecast != null ? i.forecast.confidence : 0.0).reversed()
                .thenComparingDouble(i -> i.forecast != null ? Math.abs(i.forecast.directionScore) : 0.0).reversed());
        int limit = Math.min(signals.size(), MAX_SIGNALS_PER_CYCLE);
        List<com.bot.DecisionEngineMerged.TradeIdea> dispatchSignals =
                reorderSignalsForDispatch(new ArrayList<>(signals.subList(0, limit)));

        int sent = 0;
        for (com.bot.DecisionEngineMerged.TradeIdea s : dispatchSignals) {

            // [v42.0 FIX] SLOT LEAK GUARD — every signal that reaches this loop
            // was already registered via isc.registerSignal() inside processPair().
            // If it gets rejected here (bipolar, R:R, exception), the slot MUST be freed.
            // Without this, rejected signals permanently occupy ISC slots → "Max global signals".
            boolean dispatched = false;
            try {
                // [v17.0 §1] BIPOLAR GUARD — final single-threaded check.
                if (!isc.isSymbolAvailable(s.symbol)) {
                    LOG.info("[BIPOLAR SKIP] " + s.symbol + " already active or in cooldown — dropped");
                    continue; // finally → unregister
                }

                // [PATCH B4 v33] DISPATCH R:R GATE — final check before user sees the signal.
                double _rrRiskDist = Math.abs(s.stop  - s.price);
                double _rrTp2Dist  = Math.abs(s.tp2   - s.price);
                double _rrActual   = _rrRiskDist > 1e-9 ? _rrTp2Dist / _rrRiskDist : 0;
                if (_rrActual < 1.80) {
                    LOG.info("[RR-DISPATCH-BLOCK] " + s.symbol + " " + s.side
                            + " rr=" + String.format("%.2f", _rrActual)
                            + " entry=" + s.price + " sl=" + s.stop + " tp2=" + s.tp2);
                    continue; // finally → unregister
                }

                // [v35.0] CLEAN DISPATCH — toTelegramString() is self-contained now.
                telegram.sendMessageAsync(s.toTelegramString());

                // [SCANNER MODE v2.0] AUTO EXECUTION DISABLED — ручная торговля.
                // executeOrderAsync() отключён до подключения биржевого API.
                // double autoSizeUsdt = sender.getPositionSizeUsdt(s, sender.getCoinCategory(s.symbol));
                // sender.executeOrderAsync(s, autoSizeUsdt);

                LOG.info("► " + s.symbol + " " + s.side
                        + " conf=" + String.format("%.0f%%", s.probability)
                        + (s.forecast != null ? " fc=" + s.forecast.bias.name() : ""));
                totalSignals.incrementAndGet();
                sent++;

                // [v42.1 FIX] Track signal for ProbabilityCalibrator feedback loop.
                // SCANNER MODE COMPATIBLE: trackSignal() only writes to forecastRecords
                // (no virtual position tracking, no fake TP/SL events). The ForecastChecker
                // resolves these by comparing real market price after 2h vs entry — this
                // is WHAT FEEDS THE ISOTONIC CALIBRATOR. Without this call the entire
                // ProbabilityCalibrator path stays empty forever and #1 fix is dead.
                trackSignal(s);

                // [SCANNER MODE v2.0] Вместо виртуального трекинга (trackSignal + markSymbolActive)
                // ставим простой кулдаун на монету.
                // TradeResolver отключён → markSymbolActive без markSymbolClosed = вечная блокировка.
                // Решение: сразу кулдаун 20 мин. Монета заново доступна через 20 мин.
                // Это даёт трейдеру время войти в сделку, а боту — не спамить одной монетой.
                isc.setSignalCooldown(s.symbol, 20 * 60_000L); // 20 минут

                lastSignalMs = System.currentTimeMillis();
                dispatched = true;
            } catch (Exception ex) {
                LOG.warning("[DISPATCH] " + s.symbol + " failed: " + ex.getMessage());
            } finally {
                // [v42.0] If signal was NOT dispatched → free the ISC slot that
                // processPair().registerSignal() occupied. This is the critical fix
                // for the "coins blocked forever" bug.
                if (!dispatched) {
                    isc.unregisterSignal(s);
                    LOG.info("[SLOT-FREE] " + s.symbol + " " + s.side + " — ISC slot released");
                }
            }
        }

        LOG.info("══ ЦИКЛ #" + cycle + " END ══ sent=" + sent
                + " time=" + (System.currentTimeMillis() - cycleStart) + "ms");
    }

    // ══════════════════════════════════════════════════════════════
    //  TRACK SIGNAL
    // ══════════════════════════════════════════════════════════════

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
            if (takeLong && li < longs.size()) {
                ordered.add(longs.get(li++));
            } else if (!takeLong && si < shorts.size()) {
                ordered.add(shorts.get(si++));
            } else if (li < longs.size()) {
                ordered.add(longs.get(li++));
            } else if (si < shorts.size()) {
                ordered.add(shorts.get(si++));
            }
            takeLong = !takeLong;
        }
        return ordered;
    }

    static void trackSignal(com.bot.DecisionEngineMerged.TradeIdea idea) {
        if (idea == null) return;
        String key = idea.symbol + "_" + idea.side;

        trackedSignals.remove(key);

        String  forecastBias  = "NEUTRAL";
        double  forecastScore = 0.0;
        if (idea.forecast != null) {
            forecastBias  = idea.forecast.bias.name();
            forecastScore = idea.forecast.directionScore;
        }

        double tp3 = idea.tp3 > 0 ? idea.tp3
                : idea.price + (idea.tp2 - idea.price) * 1.5;

        trackedSignals.put(key, new TrackedSignal(
                idea.symbol, idea.side, idea.price, idea.stop,
                idea.tp1, idea.tp2, tp3,
                forecastBias, forecastScore));

        // [v14.0 FIX #5] Ограничиваем размер forecastRecords
        if (forecastRecords.size() >= MAX_FORECAST_RECORDS) {
            // Удаляем самые старые resolved записи
            forecastRecords.entrySet().removeIf(e ->
                    e.getValue().resolved && e.getValue().ageMs() > 60 * 60_000L);
            // Если всё ещё переполнено — удаляем самые старые любые
            if (forecastRecords.size() >= MAX_FORECAST_RECORDS) {
                forecastRecords.entrySet().stream()
                        .min(Comparator.comparingLong(e -> e.getValue().createdAt))
                        .ifPresent(e -> forecastRecords.remove(e.getKey()));
            }
        }

        // [v43 PATCH FIX #1] Store SIGNAL probability (what user sees in Telegram),
        // NOT forecastScore. The calibrator must learn from the exact number shown
        // in the signal so it can map "bot says 72%" → "real win-rate is X%".
        // robustAtrPct is stored for vol-bucketed calibration (consolidation trap fix).
        double signalProb  = idea.probability;  // final calibrated % from DecisionEngine
        double robustAtrPct = idea.robustAtrPct; // now exposed directly from TradeIdea (v43)

        // [v36-FIX Дыра9] Используем forecastSeq вместо currentTimeMillis() — нет коллизий
        forecastRecords.put(key + "_" + forecastSeq.incrementAndGet(),
                new ForecastRecord(idea.symbol, idea.side, idea.price,
                        forecastBias, forecastScore, signalProb, robustAtrPct));
    }

    // ══════════════════════════════════════════════════════════════
    //  TRADE RESOLVER — v14: tp2Hit + fixed trailing + safe extremes
    // ══════════════════════════════════════════════════════════════

    private static void runTradeResolver(com.bot.SignalSender sender,
                                         com.bot.InstitutionalSignalCore isc,
                                         com.bot.TelegramBotSender telegram) {
        if (trackedSignals.isEmpty()) return;

        for (Iterator<Map.Entry<String, TrackedSignal>> it =
             trackedSignals.entrySet().iterator(); it.hasNext(); ) {

            Map.Entry<String, TrackedSignal> entry = it.next();
            TrackedSignal ts = entry.getValue();

            // [v28.0] PATCH #15: Force-close on deeply negative symbol score.
            // Problem: ISC blocks NEW signals for a bad symbol, but existing tracked
            //          positions on that symbol kept running until 90min EXPIRED.
            //          3 stops in a row → score dropped → 4th trade still lived 90min.
            // Fix: if symbolScore < -0.40 (approx 3 consecutive losses), force exit
            //      at current market price (neutral close, not counted as SL win).
            double symScore = isc.getSymbolScore(ts.symbol);
            if (symScore < -0.40 && !ts.tp1Hit) {
                // Only force-close pre-TP1 positions (if TP1 hit, trailing handles it)
                it.remove();
                isc.closeTrade(ts.symbol, ts.side, 0.0, "SCORE_EXIT");
                telegram.sendMessageAsync(String.format(
                        "⚠ *%s* · score exit\nScore: %.2f",
                        ts.symbol, symScore));
                LOG.info("[TR] SCORE EXIT: " + ts.symbol + " score=" + String.format("%.2f", symScore));
                continue;
            }

            // Expire: 90 минут
            if (ts.ageMs() > 90 * 60_000L) {
                it.remove();
                LOG.info("[TR] EXPIRED (neutral): " + ts.symbol + " " + ts.side);
                continue;
            }

            // ── Получаем свежие 1m candles ──────────────────────
            double priceClose;
            double atr14Trail = 0; // [v25.0] 1m ATR for Chandelier Exit
            try {
                // [v36-FIX Дыра7] Используем WS-буфер вместо REST fetchKlines.
                // REST вызов здесь из auxSched-потока → rlAcquire() с Thread.sleep() → стоп мира.
                // getM1FromWs() возвращает данные из памяти (aggTrade → MicroCandleBuilder).
                List<com.bot.TradingCore.Candle> candles = sender.getM1FromWs(ts.symbol);
                if (candles == null || candles.isEmpty()) continue;

                double newLow = Double.MAX_VALUE, newHigh = Double.NEGATIVE_INFINITY;
                for (com.bot.TradingCore.Candle c : candles) {
                    // [Hole 1 FIX] Only take extremes from candles that opened AT or AFTER the trade entry time
                    if (c.openTime >= ts.createdAt) {
                        newLow  = Math.min(newLow,  c.low);
                        newHigh = Math.max(newHigh, c.high);
                    }
                }
                priceClose = candles.get(candles.size() - 1).close;

                // [v14.0 FIX #6] Thread-safe extreme update
                ts.updateExtremes(newLow, newHigh);

                // [v25.0] Compute 1m ATR(14) for Chandelier Exit
                int atrN = Math.min(14, candles.size() - 1);
                if (atrN >= 3) {
                    double trSum = 0;
                    for (int ci = candles.size() - atrN; ci < candles.size(); ci++) {
                        com.bot.TradingCore.Candle cc = candles.get(ci);
                        double prevClose = candles.get(ci - 1).close;
                        trSum += Math.max(cc.high - cc.low,
                                Math.max(Math.abs(cc.high - prevClose),
                                        Math.abs(cc.low  - prevClose)));
                    }
                    atr14Trail = trSum / atrN;
                }
            } catch (Exception e) {
                LOG.fine("[TR] Fetch fail for " + ts.symbol + ": " + e.getMessage());
                continue;
            }

            if (priceClose <= 0) continue;

            boolean isLong = ts.side == com.bot.TradingCore.Side.LONG;
            double extremeLow  = ts.getExtremeLow();
            double extremeHigh = ts.getExtremeHigh();

            // ════════════════════════════════════════════════════
            // [v25.0] CHANDELIER EXIT — activates at +0.8% profit,
            // BEFORE TP1. Prevents giving back open profit when the
            // trade reverses without hitting TP1.
            //
            // Formula (LONG):  chandelier = extremeHigh - ATR(1m,14) * 2.2
            // Formula (SHORT): chandelier = extremeLow  + ATR(1m,14) * 2.2
            //
            // Activation threshold: +0.8% favourable move from entry.
            // Once active: trailing floor is NEVER moved against the trade.
            // ════════════════════════════════════════════════════
            if (!ts.tp1Hit && atr14Trail > 0) {
                double activationPct = isLong
                        ? (extremeHigh - ts.entry) / ts.entry
                        : (ts.entry - extremeLow)  / ts.entry;

                if (activationPct >= 0.008 && !ts.chandelierActive) {
                    ts.chandelierActive = true;
                    // Initialise trailing at breakeven (entry) — never below that
                    ts.trailingStop = ts.entry;
                    LOG.info("[TR] CHANDELIER ACTIVATED: " + ts.symbol
                            + " " + ts.side + " move=" + String.format("%.2f%%", activationPct * 100));
                }

                if (ts.chandelierActive) {
                    double chandelierMult = 2.2; // ATR multiplier — tighter than TP1 trail (0.50)
                    if (isLong) {
                        double level = extremeHigh - atr14Trail * chandelierMult;
                        // Never trail BELOW entry (breakeven floor)
                        level = Math.max(level, ts.entry);
                        // Only move trailing UP (ratchet)
                        if (ts.trailingStop == 0 || level > ts.trailingStop)
                            ts.trailingStop = level;
                    } else {
                        double level = extremeLow + atr14Trail * chandelierMult;
                        // Never trail ABOVE entry (breakeven floor for SHORT)
                        level = Math.min(level, ts.entry);
                        // Only move trailing DOWN (ratchet)
                        if (ts.trailingStop == 0 || level < ts.trailingStop)
                            ts.trailingStop = level;
                    }

                    // Check if chandelier stop was hit
                    boolean chandelierHit = isLong
                            ? priceClose <= ts.trailingStop
                            : priceClose >= ts.trailingStop;

                    if (chandelierHit) {
                        double pnl = isLong
                                ? (ts.trailingStop - ts.entry) / ts.entry * 100
                                : (ts.entry - ts.trailingStop) / ts.entry * 100;
                        it.remove();
                        isc.registerConfirmedResult(pnl > 0, ts.side);
                        isc.closeTrade(ts.symbol, ts.side, pnl);
                        if (pnl > 0) sender.getDecisionEngine().recordWin(ts.symbol, ts.side);
                        else         sender.getDecisionEngine().recordLoss(ts.symbol, ts.side);
                        sender.getDecisionEngine().markPostExitCooldown(ts.symbol, ts.side);
                        markForecastRecord(ts.symbol + "_" + ts.side,
                                pnl > 0 ? "CHANDELIER_PROFIT" : "CHANDELIER_FLAT");
                        // [v38.0] Suppress noise: PnL ≈ 0% exits are breakeven, not worth notifying
                        if (Math.abs(pnl) >= 0.20) {
                            String chE = pnl > 0 ? "✅" : "❌";
                            telegram.sendMessageAsync(String.format(
                                    "%s *%s* · trail *%+.2f%%*\nTrail: %.4f",
                                    chE, ts.symbol, pnl, ts.trailingStop));
                        }
                        LOG.info("[TR] CHANDELIER EXIT: " + ts.symbol
                                + " pnl=" + String.format("%.2f%%", pnl));
                        continue;
                    }
                }
            }

            // ── SL hit ──────────────────────────────────────────
            boolean slHit = isLong
                    ? extremeLow  <= ts.sl
                    : extremeHigh >= ts.sl;

            if (slHit) {
                // [v34.0] REALISTIC SLIPPAGE ESTIMATION
                // Problem (Gemini critique): on low-liquidity alts, wick touch ≠ fill at ts.sl.
                // Real slippage: TOP=0.05%, ALT=0.15%, MEME=0.40%
                // This makes ISC stats more realistic → better risk management decisions.
                com.bot.DecisionEngineMerged.CoinCategory slCat = sender.getCoinCategory(ts.symbol);
                double slippagePct = switch (slCat) {
                    case TOP  -> 0.05;
                    case ALT  -> 0.15;
                    case MEME -> 0.40;
                };
                double pnl = isLong
                        ? (ts.sl - ts.entry) / ts.entry * 100 - slippagePct
                        : (ts.entry - ts.sl) / ts.entry * 100 - slippagePct;
                it.remove();
                isc.registerConfirmedResult(false, ts.side);
                // [v17.0 §4] Explicit "SL" reason → ISC.closeTrade triggers recordConsecutiveSL()
                isc.closeTrade(ts.symbol, ts.side, pnl, "SL");
                // [FIX v32+] Notify DecisionEngine for dynamic confidence penalty + post-exit cooldown
                sender.getDecisionEngine().recordLoss(ts.symbol, ts.side);
                sender.getDecisionEngine().markPostExitCooldown(ts.symbol, ts.side);
                markForecastRecord(ts.symbol + "_" + ts.side, "HIT_SL");
                // [v36-FIX Дыра6] Восстановлен SL-алерт для ручной торговли.
                // PATCH B3 заглушил его с аргументом "wick touch ≠ fill" — корректно для автоторговли.
                // Для ручного трейдера молчание хуже ложной точности: он не знает что позиция умерла.
                // Дисклеймер добавлен в текст — трейдер понимает что это расчётный уровень, не факт.
                telegram.sendMessageAsync(String.format(
                        "❌ *%s* · SL *%+.2f%%*\nСтоп: %.4f",
                        ts.symbol, pnl, ts.sl));
                LOG.info("[TR] SL HIT: " + ts.symbol + " pnl=" + String.format("%.2f%%", pnl));
                continue;
            }

            // ── TP1 hit ─────────────────────────────────────────
            boolean tp1Reached = isLong
                    ? extremeHigh >= ts.tp1
                    : extremeLow  <= ts.tp1;

            if (tp1Reached && !ts.tp1Hit) {
                ts.tp1Hit = true;
                // [v25.0] If Chandelier already raised the trail above entry, keep it.
                // Otherwise set to breakeven (entry) as before.
                if (ts.trailingStop < ts.entry || ts.trailingStop == 0) {
                    ts.trailingStop = ts.entry;
                }
                // [PATCH B3 v33] TP1 SILENCED — candle extreme touch ≠ fill.
                // Trail moved to breakeven (critical risk management — KEPT).
                // Telegram alert removed: creates false confidence in a partial exit
                // that may never have executed at that exact price.
                double tp1PnlPct = isLong
                        ? (ts.tp1 - ts.entry) / ts.entry * 100
                        : (ts.entry - ts.tp1) / ts.entry * 100;
                // telegram.sendMessageAsync(String.format(
                //         "🟢 *TP1 ✓* %s %s | +%.2f%%\n"
                //                 + "💰 50%% позиции — фиксируй прибыль\n"
                //                 + "🛡 Стоп → безубыток `%.6f`",
                //         ts.symbol, ts.side, tp1PnlPct, ts.entry));
                LOG.info("[TR] TP1 HIT: " + ts.symbol + " pnl=" + String.format("%.2f%%", tp1PnlPct));
            }

            // ── После TP1: trailing + TP2 + TP3 ────────────────
            if (ts.tp1Hit) {

                // [v28.0] PATCH #11: ATR Chandelier trailing after TP1.
                // OLD: newTrail = entry + (extremeHigh - entry) * 0.50
                //   → gave back 50% of open profit on any pullback (too wide in trend).
                // NEW: chandelier = extremeHigh - ATR(1m,14) * 1.5 for LONG
                //                   extremeLow  + ATR(1m,14) * 1.5 for SHORT
                // Tighter than 50%-of-profit formula. Respects current 1m volatility.
                // Never moves against the trade (ratchet only).
                if (atr14Trail > 0) {
                    if (isLong) {
                        double chandelierLevel = extremeHigh - atr14Trail * 1.5;
                        chandelierLevel = Math.max(chandelierLevel, ts.entry); // BE floor
                        ts.trailingStop = Math.max(ts.trailingStop, chandelierLevel);
                    } else {
                        double chandelierLevel = extremeLow + atr14Trail * 1.5;
                        chandelierLevel = Math.min(chandelierLevel, ts.entry); // BE floor
                        if (ts.trailingStop == 0 || ts.trailingStop == ts.entry) {
                            ts.trailingStop = chandelierLevel;
                        } else {
                            ts.trailingStop = Math.min(ts.trailingStop, chandelierLevel);
                        }
                    }
                } else {
                    // Fallback to 50% formula if ATR unavailable (cold start)
                    if (isLong) {
                        double newTrail = ts.entry + (extremeHigh - ts.entry) * 0.50;
                        ts.trailingStop = Math.max(ts.trailingStop, newTrail);
                    } else {
                        double profit = ts.entry - extremeLow;
                        double newTrail = ts.entry - profit * 0.50;
                        if (ts.trailingStop == 0 || ts.trailingStop == ts.entry) {
                            ts.trailingStop = newTrail;
                        } else {
                            ts.trailingStop = Math.min(ts.trailingStop, newTrail);
                        }
                    }
                }

                // [v14.0 FIX #1] TP2 hit — ТОЛЬКО ОДИН РАЗ
                boolean tp2Reached = isLong
                        ? extremeHigh >= ts.tp2
                        : extremeLow  <= ts.tp2;

                if (tp2Reached && !ts.tp2Hit) {
                    ts.tp2Hit = true;
                    double tp2PnlPct = isLong
                            ? (ts.tp2 - ts.entry) / ts.entry * 100
                            : (ts.entry - ts.tp2) / ts.entry * 100;
                    // [v36-FIX Дыра6] TP2 алерт восстановлен — трейдер должен двигать стоп вручную.
                    telegram.sendMessageAsync(String.format(
                            "✅ *%s* · TP2 *+%.2f%%*\nЗакрой 30%%%% · стоп → %.4f",
                            ts.symbol, tp2PnlPct, ts.tp1));
                    LOG.info("[TR] TP2 HIT: " + ts.symbol + " pnl=" + String.format("%.2f%%", tp2PnlPct));
                    // Перемещаем trailing до tp1 уровня
                    if (isLong) ts.trailingStop = Math.max(ts.trailingStop, ts.tp1);
                    else        ts.trailingStop = Math.min(ts.trailingStop, ts.tp1);
                }

                // TP3 hit — полное закрытие
                boolean tp3Reached = isLong
                        ? extremeHigh >= ts.tp3
                        : extremeLow  <= ts.tp3;

                if (tp3Reached) {
                    double pnl = isLong
                            ? (ts.tp3 - ts.entry) / ts.entry * 100
                            : (ts.entry - ts.tp3) / ts.entry * 100;
                    it.remove();
                    isc.registerConfirmedResult(true, ts.side);
                    isc.closeTrade(ts.symbol, ts.side, pnl, "TP");
                    sender.getDecisionEngine().markPostExitCooldown(ts.symbol, ts.side);
                    markForecastRecord(ts.symbol + "_" + ts.side, "HIT_TP3");
                    // [v36-FIX Дыра6] TP3 алерт восстановлен — финальный выход, важно для трейдера.
                    telegram.sendMessageAsync(String.format(
                            "✅ *%s* · закрыто *+%.2f%%*",
                            ts.symbol, pnl));
                    LOG.info("[TR] TP3 HIT: " + ts.symbol + " pnl=" + String.format("%.2f%%", pnl));
                    continue;
                }

                // Trailing stop hit
                boolean trailHit = isLong
                        ? priceClose <= ts.trailingStop
                        : priceClose >= ts.trailingStop;

                if (trailHit) {
                    double pnl = isLong
                            ? (ts.trailingStop - ts.entry) / ts.entry * 100
                            : (ts.entry - ts.trailingStop) / ts.entry * 100;
                    it.remove();
                    isc.registerConfirmedResult(pnl > 0, ts.side);
                    isc.closeTrade(ts.symbol, ts.side, pnl, pnl > 0 ? "TP" : "SL");
                    sender.getDecisionEngine().markPostExitCooldown(ts.symbol, ts.side);
                    markForecastRecord(ts.symbol + "_" + ts.side,
                            pnl > 0 ? "EXPIRED_PROFIT" : "EXPIRED_FLAT");
                    // [v38.0] Suppress breakeven noise (PnL ≈ 0)
                    if (Math.abs(pnl) >= 0.20) {
                        String trE = pnl > 0 ? "✅" : "❌";
                        telegram.sendMessageAsync(String.format(
                                "%s *%s* · trail *%+.2f%%*\nTrail: %.4f",
                                trE, ts.symbol, pnl, ts.trailingStop));
                    }
                    LOG.info("[TR] TRAIL HIT: " + ts.symbol + " pnl=" + String.format("%.2f%%", pnl));
                }
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  [v36-FIX Дыра6] POSITION STATUS REPORTER
    //  Отправляет сводку открытых позиций каждые 30 минут.
    //  Трейдер видит актуальный статус без лишнего спама.
    // ══════════════════════════════════════════════════════════════

    private static void sendPositionStatus(com.bot.TelegramBotSender telegram) {
        if (trackedSignals.isEmpty()) return;

        StringBuilder sb = new StringBuilder();
        sb.append("*Открытые позиции*\n\n");

        int longCount = 0, shortCount = 0;
        for (TrackedSignal ts : trackedSignals.values()) {
            long ageMin = ts.ageMs() / 60_000;
            String dir = ts.side == com.bot.TradingCore.Side.LONG ? "L" : "S";
            String status = ts.tp2Hit ? " TP2" : ts.tp1Hit ? " TP1" : "";
            if (ts.side == com.bot.TradingCore.Side.LONG) longCount++; else shortCount++;

            String sl;
            if (ts.trailingStop > 0 && ts.tp1Hit) {
                sl = String.format("trail %.4f", ts.trailingStop);
            } else {
                sl = String.format("sl %.4f", ts.sl);
            }
            sb.append(String.format("%s · %s%s · %dm · %s\n",
                    ts.symbol, dir, status, ageMin, sl));
        }
        sb.append(String.format("\nL %d · S %d · Total %d",
                longCount, shortCount, trackedSignals.size()));
        telegram.sendMessageAsync(sb.toString());
    }

    // ══════════════════════════════════════════════════════════════
    //  FORECAST ACCURACY CHECKER
    // ══════════════════════════════════════════════════════════════

    // [FIX v32+] Deduplication: Forecast Accuracy Report was firing 3x per minute.
    // Root cause: checkForecastAccuracy() ran every 15m and called sendMessageAsync
    // for EVERY resolved record when total % 20 == 0, which could match multiple
    // records in the same run. Added lastForecastReportMs gate: max 1 report/hour.
    private static volatile long lastForecastReportMs = 0;
    private static final long FORECAST_REPORT_INTERVAL_MS = 60 * 60_000L; // once per hour max

    private static void checkForecastAccuracy(com.bot.SignalSender sender,
                                              com.bot.TelegramBotSender telegram) {
        long now = System.currentTimeMillis();
        long minAgeMs = 120 * 60_000L;

        for (Iterator<Map.Entry<String, ForecastRecord>> it =
             forecastRecords.entrySet().iterator(); it.hasNext(); ) {

            Map.Entry<String, ForecastRecord> e = it.next();
            ForecastRecord fr = e.getValue();

            if (fr.ageMs() < minAgeMs) continue;

            if (fr.resolved) {
                if (fr.ageMs() > minAgeMs + 30 * 60_000L) it.remove();
                continue;
            }

            // [v14.0 FIX #5] Удаляем слишком старые неразрешённые записи (>4 часа)
            if (fr.ageMs() > 4 * 60 * 60_000L) {
                it.remove();
                continue;
            }

            try {
                List<com.bot.TradingCore.Candle> c = sender.fetchKlines(fr.symbol, "15m", 20);
                if (c == null || c.isEmpty()) continue;
                double currentPrice = c.get(c.size() - 1).close;

                // [v28.0] PATCH #17: Dynamic forecast accuracy threshold.
                // OLD: static 0.3% — for ALT with ATR=1.5%, 0.3% is pure noise (20% of ATR).
                //      Forecast was marked "correct" even on random micro-moves.
                // NEW: threshold = max(0.3%, ATR(15m,14) * 0.25)
                //      At ATR=1.5%: threshold = 0.375%. At ATR=2%: threshold = 0.5%.
                double atrForFc = c.size() >= 15 ? com.bot.TradingCore.atr(c, 14) : 0;
                double fcThreshold = atrForFc > 0
                        ? Math.max(0.3, (atrForFc / currentPrice) * 100.0 * 0.25)
                        : 0.3;

                double changePct = (currentPrice - fr.entryPrice) / fr.entryPrice * 100.0;
                boolean bullishMove = changePct > fcThreshold;
                boolean bearishMove = changePct < -fcThreshold;

                boolean bullishBias = fr.forecastBias.contains("BULL");
                boolean bearishBias = fr.forecastBias.contains("BEAR");

                // [v14.0 FIX #9] Упрощённая и корректная логика:
                // Correct = bias совпал с реальным направлением
                boolean correct = (bullishBias && bullishMove) || (bearishBias && bearishMove);
                boolean hasOpinion = bullishBias || bearishBias;

                fr.resolved = true;
                fr.actualOutcome = bullishMove ? "MOVED_UP"
                        : bearishMove ? "MOVED_DOWN" : "FLAT";

                if (hasOpinion && (bullishMove || bearishMove)) {
                    // [FIX #19] Only count if not already counted via markForecastRecord
                    if (!fr.counted) {
                        fr.counted = true;
                        forecastTotal.incrementAndGet();
                        if (correct) forecastCorrect.incrementAndGet();
                    }

                    // [v42.0 FIX #1] Feed the ProbabilityCalibrator with this resolved outcome.
                    // Without this, the isotonic calibrator in DecisionEngineMerged stays empty
                    // and falls through to raw score (no calibration).
                    // [v43 PATCH FIX #1] Feed calibrator with SIGNAL probability (not forecastScore).
                    // forecastScore ∈ [-1..+1] = directional forecast; calibrator needs [0..1] win prob.
                    // signalProbability = the exact % shown in Telegram (e.g. 72%) → correct feedback loop.
                    // robustAtrPctAtSignal = ATR% captured when signal fired → correct vol-bucket lookup.
                    try {
                        double sigProb01 = Math.max(0.01, Math.min(0.99, fr.signalProbability / 100.0));
                        double atrPctForBucket = fr.robustAtrPctAtSignal > 0
                                ? fr.robustAtrPctAtSignal * 100.0
                                : (atrForFc > 0 ? (atrForFc / currentPrice) * 100.0 : 1.0);
                        com.bot.DecisionEngineMerged.getCalibrator()
                                .recordOutcome(fr.symbol, sigProb01, correct, atrPctForBucket);
                    } catch (Throwable cbe) {
                        LOG.fine("[CAL] feed failed: " + cbe.getMessage());
                    }
                }
                // FLAT результаты не считаем — бот не может предсказать "ничего не произойдёт"

                int total   = forecastTotal.get();
                int correct2 = forecastCorrect.get();
                double acc  = total > 0 ? (double) correct2 / total * 100 : 0;

                LOG.info(String.format("[FC] %s bias=%s actual=%s %s | Accuracy: %.0f%% (%d/%d)",
                        fr.symbol, fr.forecastBias, fr.actualOutcome,
                        correct ? "✅" : "❌",
                        acc, correct2, total));

                if (total > 0 && total % 20 == 0) {
                    // [FIX v32+] Rate-gate: max 1 Forecast report per hour
                    long nowMs = System.currentTimeMillis();
                    if (nowMs - lastForecastReportMs >= FORECAST_REPORT_INTERVAL_MS) {
                        lastForecastReportMs = nowMs;
                        telegram.sendMessageAsync(String.format(
                                "*Forecast*\n\nTotal %d · Hit %d · *%.1f%%*",
                                total, correct2, acc));
                    }
                }
            } catch (Exception ex) {
                LOG.fine("[FC] Fetch fail: " + fr.symbol + " " + ex.getMessage());
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  WATCHDOG
    // ══════════════════════════════════════════════════════════════

    private static void runWatchdog(com.bot.TelegramBotSender telegram,
                                    com.bot.GlobalImpulseController gic,
                                    com.bot.InstitutionalSignalCore isc,
                                    com.bot.SignalSender sender) {
        // [FIX #15] Periodic cleanup — releases stale activeSymbols entries even during droughts.
        // Without this, if no signals fire for 20+ minutes, ISC slots stay locked forever
        // because cleanupExpired() only runs inside allowSignal().
        isc.periodicCleanup();
        long now = System.currentTimeMillis();
        List<String> issues = new ArrayList<>();

        if (now - lastCycleSuccessMs > 3 * 60_000L)
            issues.add("💀 MainCycle silent " + (now - lastCycleSuccessMs) / 1000 + "s");

        if (now - lastStatsSuccessMs > 20 * 60_000L)
            issues.add("💀 Stats silent " + (now - lastStatsSuccessMs) / 60_000 + "min");

        if (now - lastSignalMs > SIGNAL_DROUGHT_MS) {
            long droughtMin = (now - lastSignalMs) / 60_000;
            double effConf  = isc.getEffectiveMinConfidence();
            issues.add("📭 No signals " + droughtMin + " min"
                    + (effConf > 62 ? " | ISC effConf=" + String.format("%.0f", effConf) + "%" : "")
                    + " WS=" + sender.getActiveWsCount()
                    + " UDS=" + (sender.isUdsConnected() ? "✅" : "❌"));
        }

        if (sender.getActiveWsCount() < 3)
            issues.add("⚠️ WebSockets low: " + sender.getActiveWsCount());

        // [SCANNER MODE v1.0] Aggressive WS recovery:
        // forceResubscribeTopPairs only refreshes the subscription list.
        // If main cycle is silent >3min or WS count is critically low, do a full force-reconnect.
        if (sender.getActiveWsCount() < 3 || now - lastCycleSuccessMs > 3 * 60_000L) {
            System.out.println("[WD] Triggering force-reconnect: WS=" + sender.getActiveWsCount()
                    + " cycleAge=" + (now - lastCycleSuccessMs) / 1000 + "s");
            sender.forceResubscribeTopPairs();
        }

        if (!issues.isEmpty() && now - lastWatchdogAlertMs > WATCHDOG_COOLDOWN_MS) {
            lastWatchdogAlertMs = now;
            watchdogAlerts.incrementAndGet();
            LOG.warning("[WD #" + watchdogAlerts.get() + "] " + String.join(" | ", issues));
            // [SCANNER MODE] Also send watchdog alerts to Telegram so issues are visible.
            telegram.sendMessageAsync("*Watchdog* #" + watchdogAlerts.get() + "\n\n"
                    + String.join("\n", issues));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  DAILY SUMMARY
    // ══════════════════════════════════════════════════════════════

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
        int fcTotal    = forecastTotal.get();
        int fcCorrect  = forecastCorrect.get();
        double fcAcc   = fcTotal > 0 ? (double) fcCorrect / fcTotal * 100 : 0;

        String msg = String.format(
                "*Daily Report*\n\n"
                        + "Up %dm · Cycles %d · Signals %d\n"
                        + "PnL *%+.2f%%* · DD %.1f%%\n"
                        + "BTC %s · Vol %s\n"
                        + "WS %d · Bal $%.2f\n\n"
                        + "Forecast *%.0f%%* (%d/%d)\n"
                        + "Open %d",
                uptimeMin, totalCycles.get(), totalSignals.get(),
                isc.getDailyPnL(), isc.getDrawdownFromPeak(),
                ctx.regime, ctx.volRegime,
                sender.getActiveWsCount(),
                sender.getAccountBalance(),
                fcAcc, fcCorrect, fcTotal,
                trackedSignals.size());

        telegram.sendMessageAsync(msg);
        LOG.info("[DAILY] Sent summary");
    }

    // ══════════════════════════════════════════════════════════════
    //  BACKTEST
    // ══════════════════════════════════════════════════════════════

    private static void runPeriodicBacktest(com.bot.SignalSender sender,
                                            com.bot.InstitutionalSignalCore isc,
                                            com.bot.TelegramBotSender telegram) {
        // [v36-FIX Дыра8] Backtest REST Burst:
        // БЫЛО: 12 пар × (500+200+500+300) bars = ~9000 weight/burst — одновременно с live-торговлей.
        // СТАЛО:
        //   • Universe сокращён до 6 seed + 6 dynamic = 12 max (было 6+12=18)
        //   • Лимиты свечей снижены: m15=300, h1=100, m5=200 (было 500/200/300)
        //   • m1 берётся из WS-буфера (0 REST weight), не из fetchKlines
        //   • Задержка 3s между символами = нагрузка растянута на 36s вместо burst
        // Итог: ~3600 weight за 36s = 100 weight/s vs старые 9000 weight/s.
        LOG.info("[BT] Начало периодического бэктеста...");
        com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();
        java.util.LinkedHashSet<String> universe = new java.util.LinkedHashSet<>(java.util.List.of(
                "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "LINKUSDT", "XRPUSDT"));
        universe.addAll(sender.getScanUniverseSnapshot(6)); // было 12 → 6 dynamic
        double totalEV = 0;
        int    count   = 0;
        StringBuilder btLog = new StringBuilder("[BT] Results: ");

        for (String sym : universe) {
            try {
                // [v36-FIX] 3s throttle между символами — не даём RL burst во время торговли
                Thread.sleep(3_000L);

                List<com.bot.TradingCore.Candle> m15 = sender.fetchKlines(sym, "15m", 300); // было 500
                List<com.bot.TradingCore.Candle> h1  = sender.fetchKlines(sym, "1h",  100); // было 200
                // [v36-FIX] m1 из WS-буфера — 0 REST weight
                List<com.bot.TradingCore.Candle> m1  = sender.getM1FromWs(sym);
                List<com.bot.TradingCore.Candle> m5  = sender.fetchKlines(sym, "5m",  200); // было 300

                if (m15 == null || m15.size() < 200) continue;

                com.bot.DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(sym);
                com.bot.SimpleBacktester.BacktestResult r = bt.run(
                        sym, m1, m5, m15, h1, cat);

                if (r.total >= 5) {
                    totalEV += r.ev;
                    count++;
                    btLog.append(sym).append("=").append(String.format("%.3f", r.ev)).append(" ");
                    isc.setSymbolBacktestResult(sym, r.ev);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.info("[BT] Прерван");
                return;
            } catch (Exception e) {
                LOG.warning("[BT] " + sym + ": " + e.getMessage());
            }
        }

        if (count > 0) {
            double avgEV = totalEV / count;
            isc.setBacktestResult(avgEV, System.currentTimeMillis());
            LOG.info(btLog + "| avgEV=" + String.format("%.4f", avgEV)
                    + " effConf=" + String.format("%.0f%%", isc.getEffectiveMinConfidence()));
        } else {
            LOG.warning("[BT] Нет достаточных данных для бэктеста");
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  STATS
    // ══════════════════════════════════════════════════════════════

    private static void logStats(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        lastStatsSuccessMs = System.currentTimeMillis();
        long uptimeMin = (System.currentTimeMillis() - startTimeMs) / 60_000;
        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();

        String msg = String.format(
                "[STATS] Up:%dm Cyc:%d Sig:%d Trk:%d FR:%d | "
                        + "BTC:%s str=%.2f | "
                        + "WS:%d UDS:%s Bal:$%.2f | "
                        + "Day:%+.2f%% DD:%.1f%% | "
                        + "FC:%.0f%%(%d/%d) Err:%d WD:%d | "
                        + "DQ:+%.0f st=%.0f%% ws=%.0f%% | %s | %s",
                uptimeMin, totalCycles.get(), totalSignals.get(),
                trackedSignals.size(), forecastRecords.size(),
                ctx.regime, ctx.impulseStrength,
                sender.getActiveWsCount(), sender.isUdsConnected() ? "OK" : "DOWN",
                sender.getAccountBalance(),
                isc.getDailyPnL(), isc.getDrawdownFromPeak(),
                forecastTotal.get() > 0 ? (double) forecastCorrect.get() / forecastTotal.get() * 100 : 0.0,
                forecastCorrect.get(), forecastTotal.get(),
                errorCount.get(), watchdogAlerts.get(),
                sender.getCycleQualityPenalty(), sender.getLastCycleStaleRatio() * 100.0,
                sender.getLastCycleWsCoverage() * 100.0,
                isc.getStats(), sender.getRejectionStats());

        LOG.info(msg);

        // [v43 PATCH FIX #8] Sync ISC auto-blacklist → SignalSender.GARBAGE_COIN_BLOCKLIST.
        // When ISC detects WR < 25% for a symbol, it soft-blocks it via +20 minConf boost.
        // We also push the symbol to SignalSender's garbage list so processPair() skips it
        // entirely — avoiding wasted REST/WS fetch overhead on known-bad coins.
        try {
            java.util.Set<String> autoBlocked = isc.getAutoBlacklist();
            for (String sym : autoBlocked) {
                sender.addToGarbageBlocklist(sym);
            }
        } catch (Exception ignored) {}

        // [v43 PATCH FIX #8b] Per-symbol win-rate metrics log (every 30 min = every 6 stat cycles at 5m).
        // Logs symbols with WR < 40% so operator can review and manually add to blocklist.
        // Also logs calibrator sample count to track when calibration becomes meaningful (≥50 samples).
        long nowMs = System.currentTimeMillis();
        if (uptimeMin % 30 < 5) {   // fire in the first 5min of each 30min window
            try {
                StringBuilder wr = new StringBuilder("[WR_METRICS]\n");
                int total = isc.getTotalTradeCount();
                double overall = isc.getOverallWinRate();
                wr.append(String.format("  Overall: %.0f%% (%d trades)\n", overall * 100, total));

                // Log auto-blacklisted symbols
                java.util.Set<String> bl = isc.getAutoBlacklist();
                if (!bl.isEmpty()) {
                    wr.append("  AUTO_BLOCKED: ").append(bl).append("\n");
                }

                // Log symbols with WR data and flag poor performers
                isc.getTradeHistorySymbols().stream()
                        .filter(s -> isc.getTradeCount(s) >= 3)
                        .sorted(java.util.Comparator.comparingDouble(isc::getWinRate))
                        .limit(10)
                        .forEach(s -> {
                            double sWr = isc.getWinRate(s);
                            int cnt = isc.getTradeCount(s);
                            String flag = sWr < 0.30 ? " ❌POOR" : sWr < 0.45 ? " ⚠️WEAK" : "";
                            wr.append(String.format("  %s: %.0f%% (%d)%s\n", s, sWr * 100, cnt, flag));
                        });

                LOG.info(wr.toString());
            } catch (Exception ignored) {}
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  HELPERS
    // ══════════════════════════════════════════════════════════════

    private static void updateBtcContext(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        try {
            List<com.bot.TradingCore.Candle> btc = sender.fetchKlines("BTCUSDT", "15m", KLINES);
            if (btc != null && btc.size() > 30) gic.update(btc);
        } catch (Exception e) {
            LOG.warning("[BTC ctx] " + e.getMessage());
        }
    }

    private static void updateSectors(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        for (Map.Entry<String, String> e : SECTOR_LEADERS.entrySet()) {
            try {
                List<com.bot.TradingCore.Candle> sc = sender.fetchKlines(e.getKey(), "15m", 80);
                if (sc != null && sc.size() > 25) gic.updateSector(e.getValue(), sc);
            } catch (Exception ignored) {}
        }
    }

    private static void markForecastRecord(String key, String outcome) {
        forecastRecords.entrySet().stream()
                .filter(e -> e.getKey().startsWith(key) && !e.getValue().resolved)
                .max(Comparator.comparingLong(e -> e.getValue().createdAt))
                .ifPresent(e -> {
                    e.getValue().resolved = true;
                    e.getValue().actualOutcome = outcome;
                    // [FIX #19] Only count if not already counted (prevents double-count when
                    // both TradeResolver and checkForecastAccuracy are active simultaneously).
                    if (!e.getValue().counted) {
                        e.getValue().counted = true;
                        boolean tp = outcome.contains("TP") || outcome.equals("EXPIRED_PROFIT");
                        if (!"EXPIRED_FLAT".equals(outcome)) {
                            forecastTotal.incrementAndGet();
                            if (tp) forecastCorrect.incrementAndGet();
                        }
                    }
                });
    }

    // ══════════════════════════════════════════════════════════════
    //  [MODULE 4 v33] ADVANCE FORECAST ALERTS
    //  Запускается каждые 5 минут. Сканирует топ-пары через ForecastEngine
    //  и отправляет заблаговременные прогнозы в Telegram ДО формирования сигнала.
    //
    //  Логика отправки прогноза:
    //  1. |directionScore| >= AFC_MIN_DIRECTION_SCORE (умеренный сигнал)
    //  2. Прогноз изменился с предыдущего цикла (дедупликация по ключу)
    //  3. Cooldown 20 минут на пару (не спамим)
    //  4. GIC не в PANIC режиме (в панике прогнозы неактуальны)
    //  5. Не более AFC_MAX_PER_RUN прогнозов за один запуск
    //
    //  Типы прогнозов:
    //    🚀 ПАМП — сильный бычий импульс ожидается (score > AFC_STRONG_SCORE)
    //    📉 ДАМП — сильный медвежий импульс ожидается
    //    🔄 РАЗВОРОТ ВВЕРХ — смена тренда с нисходящего на восходящий
    //    🔄 РАЗВОРОТ ВНИЗ — смена тренда с восходящего на нисходящий
    //    📈 ТРЕНД ФОРМИРУЕТСЯ — ранняя фаза (EARLY) направленного движения
    //    ⛽ ИСТОЩЕНИЕ — тренд заканчивается, ожидать разворота или консолидации
    // ══════════════════════════════════════════════════════════════

    private static void runAdvanceForecast(com.bot.SignalSender sender,
                                           com.bot.GlobalImpulseController gic,
                                           com.bot.InstitutionalSignalCore isc,
                                           com.bot.TelegramBotSender telegram) {
        // Не работаем в PANIC режиме — рынок непредсказуем
        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        if (ctx.panicMode) {
            LOG.fine("[AFC] SKIP — GIC in PANIC mode");
            return;
        }

        // Берём топ-пары по объёму из SignalSender
        List<String> topPairs = sender.getTopPairsForForecast(20);
        if (topPairs == null || topPairs.isEmpty()) return;

        int sent = 0;
        long now = System.currentTimeMillis();

        for (String pair : topPairs) {
            if (sent >= AFC_MAX_PER_RUN) break;

            // Cooldown проверка
            Long lastSent = forecastCooldown.get(pair);
            if (lastSent != null && now - lastSent < ADVANCE_FORECAST_COOLDOWN_MS) continue;

            // Symbol must be available (not in active trade / SL cooldown)
            if (!isc.isSymbolAvailable(pair)) continue;

            try {
                // [FIX #4] Rate-limit klines calls on auxSched thread.
                // fetchKlines → rlAcquire → Thread.sleep inside, which blocks auxSched.
                // Adding a small inter-pair sleep avoids RL burst and keeps auxSched responsive.
                try { Thread.sleep(300L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }

                // Получаем 15m и 1h свечи для ForecastEngine
                List<com.bot.TradingCore.Candle> c15 = sender.fetchKlines(pair, "15m", 100);
                List<com.bot.TradingCore.Candle> c1h  = sender.fetchKlines(pair, "1h",  48);
                if (c15 == null || c15.size() < 30) continue;

                // Запускаем ForecastEngine
                // [FIX #21] Pass empty list for c5 (not available in advance forecast context)
                // instead of null — null propagation through internal helpers is fragile.
                com.bot.TradingCore.ForecastEngine fe = new com.bot.TradingCore.ForecastEngine();
                com.bot.TradingCore.ForecastEngine.ForecastResult fc =
                        fe.forecast(List.of(), c15, c1h != null ? c1h : List.of(), 0.0);
                if (fc == null) continue;

                double score = fc.directionScore;
                if (Math.abs(score) < AFC_MIN_DIRECTION_SCORE) continue;

                // VSA дополнительное подтверждение
                com.bot.TradingCore.VsaResult vsa = com.bot.TradingCore.vsaAnalyse(c15, 3);

                // Строим ключ прогноза для дедупликации
                String forecastKey = buildForecastKey(fc, vsa, score);
                String prevKey = lastForecastSent.get(pair);
                if (forecastKey.equals(prevKey)) continue; // прогноз не изменился

                // Формируем сообщение
                String msg = buildAdvanceForecastMessage(pair, fc, vsa, score, ctx);
                if (msg == null) continue;

                telegram.sendMessageAsync(msg);
                lastForecastSent.put(pair, forecastKey);
                forecastCooldown.put(pair, now);
                sent++;
                LOG.info("[AFC] Sent advance forecast: " + pair
                        + " score=" + String.format("%.2f", score)
                        + " phase=" + fc.trendPhase
                        + " bias=" + fc.bias);

            } catch (Exception e) {
                LOG.fine("[AFC] Error for " + pair + ": " + e.getMessage());
            }
        }
    }

    /** Строит дедупликационный ключ из ключевых параметров прогноза */
    private static String buildForecastKey(
            com.bot.TradingCore.ForecastEngine.ForecastResult fc,
            com.bot.TradingCore.VsaResult vsa,
            double score) {
        // Округляем score до шага 0.10 — небольшие флуктуации не меняют суть прогноза
        int scoreStep = (int)(score * 10);
        String vsaTag  = vsa.hasSignal() ? vsa.signal.name() : "NONE";
        return fc.bias.name() + "_" + fc.trendPhase.name() + "_" + scoreStep + "_" + vsaTag;
    }

    /** Строит полное Telegram-сообщение advance forecast */
    private static String buildAdvanceForecastMessage(
            String pair,
            com.bot.TradingCore.ForecastEngine.ForecastResult fc,
            com.bot.TradingCore.VsaResult vsa,
            double score,
            com.bot.GlobalImpulseController.GlobalContext ctx) {

        boolean bullish = score > 0;
        boolean strong  = Math.abs(score) >= AFC_STRONG_SCORE;

        com.bot.DecisionEngineMerged.AssetType assetType =
                com.bot.DecisionEngineMerged.detectAssetType(pair);

        // ── Event type ──
        boolean isExhaustion = fc.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EXHAUSTION;
        boolean isEarly      = fc.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EARLY;
        boolean vsaReversal  = vsa.hasSignal() && (
                vsa.signal == com.bot.TradingCore.VsaResult.VsaSignal.STOPPING_VOLUME_BULL ||
                        vsa.signal == com.bot.TradingCore.VsaResult.VsaSignal.STOPPING_VOLUME_BEAR ||
                        vsa.signal == com.bot.TradingCore.VsaResult.VsaSignal.EFFORT_TO_FALL_FAILED ||
                        vsa.signal == com.bot.TradingCore.VsaResult.VsaSignal.EFFORT_TO_RISE_FAILED);

        String eventType;
        if (isExhaustion && vsaReversal) {
            eventType = bullish ? "Разворот вниз" : "Разворот вверх";
        } else if (isExhaustion) {
            eventType = "Истощение тренда";
        } else if (isEarly && strong) {
            eventType = bullish ? "Памп" : "Дамп";
        } else if (isEarly) {
            eventType = bullish ? "Тренд вверх" : "Тренд вниз";
        } else if (strong) {
            eventType = bullish ? "Импульс вверх" : "Импульс вниз";
        } else {
            eventType = bullish ? "Движение вверх" : "Движение вниз";
        }

        // ── VSA status line ──
        String vsaStatus = "";
        if (vsa.hasSignal()) {
            vsaStatus = switch (vsa.signal) {
                case STOPPING_VOLUME_BULL  -> "Покупки поглотили продажи";
                case STOPPING_VOLUME_BEAR  -> "Продажи поглотили покупки";
                case EFFORT_TO_FALL_FAILED -> "Продажи провалились";
                case EFFORT_TO_RISE_FAILED -> "Покупки провалились";
                case NO_SUPPLY             -> "Нет продаж";
                case NO_DEMAND             -> "Нет покупок";
                case DEMAND_ABSORPTION     -> "Поглощение спроса";
                case SUPPLY_ABSORPTION     -> "Поглощение предложения";
                case WEAK_BREAKOUT         -> "Ловушка пробоя";
                default                    -> "";
            };
        }

        double confPct = Math.abs(score) * 100;
        String icon = bullish ? "🟢" : "🔴";

        StringBuilder body = new StringBuilder();
        body.append(icon).append(" *").append(pair).append("* · ").append(eventType).append('\n');
        body.append("_").append(assetType.label).append(" · прогноз_\n\n");
        if (fc.magnetLevel > 0) {
            body.append(String.format("Магнит: %.4f%n", fc.magnetLevel));
        }
        if (fc.projectedMovePct != 0) {
            body.append(String.format("Движение: %+.2f%%%n", fc.projectedMovePct * 100));
        }
        if (!vsaStatus.isEmpty()) {
            body.append(String.format("VSA: %s%n", vsaStatus));
        }
        body.append(String.format("%n*%.0f%%* уверенность · ожидание подтверждения", confPct));

        return body.toString();
    }


    private static String buildStartMessage() {
        return "⚡ *TradingBot PRO* `v41`\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + "`15M` Futures  ·  Crypto / Commodities\n"
                + "VSA  ·  OFV  ·  EarlyRev  ·  Forecast\n"
                + "R:R min `1:2`  ·  Risk-first 🔒\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + "_Система активна. Торгуй с умом._";
    }
    private static String nowWarsawStr() {
        return ZonedDateTime.now(ZONE)
                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"));
    }

    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis).atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }

    /**
     * Auto-detects timezone for Telegram signal timestamps.
     * Priority: 1) env TIMEZONE  2) IP geolocation (ip-api.com)  3) Europe/Warsaw
     * Each bot instance detects the timezone of the machine it runs on automatically.
     * To force a specific zone, set env variable: TIMEZONE=Europe/Zaporozhye
     */
    private static ZoneId detectTimezone() {
        // 1. Manual override via environment variable
        String envTz = System.getenv("TIMEZONE");
        if (envTz != null && !envTz.isBlank()) {
            try {
                ZoneId z = ZoneId.of(envTz.trim());
                System.out.println("[TIMEZONE] Using env override: " + z.getId());
                return z;
            } catch (Exception ignored) {
                System.out.println("[TIMEZONE] Invalid env TIMEZONE value: " + envTz);
            }
        }
        // 2. Auto-detect via IP geolocation (no API key required)
        try {
            java.net.URL url = new java.net.URL("http://ip-api.com/json?fields=timezone,city");
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(4000);
            conn.setReadTimeout(4000);
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() == 200) {
                try (java.io.InputStream is = conn.getInputStream();
                     java.util.Scanner sc = new java.util.Scanner(is, "UTF-8")) {
                    String body = sc.useDelimiter("\\A").hasNext() ? sc.next() : "";
                    // Parse "timezone":"Europe/Warsaw"
                    int idx = body.indexOf("\"timezone\"");
                    if (idx >= 0) {
                        int q1 = body.indexOf('"', idx + 10) + 1;
                        int q2 = body.indexOf('"', q1);
                        if (q1 > 0 && q2 > q1) {
                            String tz = body.substring(q1, q2);
                            // Parse "city":"Warsaw"
                            String city = "";
                            int ci = body.indexOf("\"city\"");
                            if (ci >= 0) {
                                int c1 = body.indexOf('"', ci + 6) + 1;
                                int c2 = body.indexOf('"', c1);
                                if (c1 > 0 && c2 > c1) city = body.substring(c1, c2);
                            }
                            ZoneId z = ZoneId.of(tz);
                            System.out.println("[TIMEZONE] Auto-detected: " + tz
                                    + (city.isEmpty() ? "" : " (" + city + ")"));
                            return z;
                        }
                    }
                }
            }
            conn.disconnect();
        } catch (Exception e) {
            System.out.println("[TIMEZONE] IP geolocation failed: " + e.getMessage());
        }
        // 3. Default fallback
        System.out.println("[TIMEZONE] Using default: Europe/Warsaw");
        return ZoneId.of("Europe/Warsaw");
    }

    private static int envInt(String k, int d) {
        try {
            return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(d)));
        } catch (Exception e) {
            return d;
        }
    }

    /**
     * [v42.1 FIX #10] Walk-forward out-of-sample validation.
     * Runs daily. For each active symbol, slides a 14-day train / 3-day test
     * window over the last 30 days of history. If test win-rate diverges
     * significantly from train win-rate, emits an overfit alert — the only
     * protection against SignalOptimizer tuning to historical noise.
     */
    private static void runWalkForwardValidation(com.bot.SignalSender sender,
                                                 com.bot.TelegramBotSender telegram) {
        try {
            com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();
            List<String> symbols = new ArrayList<>(SECTOR_LEADERS.keySet());
            int alerts = 0;
            int totalWindows = 0;
            double totalDelta = 0;

            for (String sym : symbols) {
                try {
                    // Fetch 30 days of 15m candles (2880 bars) and 1h (720 bars)
                    List<com.bot.TradingCore.Candle> m15 = sender.fetchKlines(sym, "15m", 2880);
                    List<com.bot.TradingCore.Candle> h1  = sender.fetchKlines(sym, "1h",  720);
                    if (m15 == null || m15.size() < 1500) continue;

                    com.bot.DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(sym);
                    if (cat == null) cat = com.bot.DecisionEngineMerged.CoinCategory.ALT;

                    // 14d train = 1344 bars @15m, 3d test = 288 bars @15m
                    List<com.bot.SimpleBacktester.BacktestResult> oos =
                            bt.walkForward(sym, m15, h1, cat, 1344, 288);

                    for (com.bot.SimpleBacktester.BacktestResult r : oos) {
                        totalWindows++;
                        double wr = r.winRate * 100.0;   // winRate is [0..1], display as %
                        LOG.info(String.format("[WF] %s window winRate=%.1f%% trades=%d",
                                sym, wr, r.total));
                    }
                    if (oos.size() >= 2) {
                        // Compare first half vs second half as a stability proxy
                        double firstHalf = 0, secondHalf = 0;
                        int half = oos.size() / 2;
                        for (int i = 0; i < half; i++)              firstHalf  += oos.get(i).winRate * 100.0;
                        for (int i = half; i < oos.size(); i++)     secondHalf += oos.get(i).winRate * 100.0;
                        firstHalf  /= Math.max(1, half);
                        secondHalf /= Math.max(1, oos.size() - half);
                        double delta = firstHalf - secondHalf;
                        totalDelta += Math.abs(delta);

                        if (Math.abs(delta) > 15.0) {
                            alerts++;
                            LOG.warning(String.format(
                                    "[WF-ALERT] %s unstable: early=%.1f%% late=%.1f%% Δ=%.1f%%",
                                    sym, firstHalf, secondHalf, delta));
                        }
                    }
                } catch (Throwable perSym) {
                    LOG.warning("[WF] " + sym + " failed: " + perSym.getMessage());
                }
            }

            if (alerts > 0 && telegram != null) {
                telegram.sendMessageAsync(String.format(
                        "⚠️ *Walk-Forward Alert*\n%d/%d symbols unstable\nAvg Δ win-rate: %.1f%%\n" +
                                "Model may be overfit — consider re-tuning.",
                        alerts, symbols.size(),
                        totalWindows > 0 ? totalDelta / Math.max(1, totalWindows) : 0));
            }
            LOG.info(String.format("[WF] done: %d windows across %d symbols, %d alerts",
                    totalWindows, symbols.size(), alerts));
        } catch (Throwable t) {
            LOG.warning("[WF] validation failed: " + t.getMessage());
        }
    }

    private static void configureLogger() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        for (Handler h : root.getHandlers()) {
            h.setFormatter(new SimpleFormatter() {
                private final DateTimeFormatter fmt =
                        DateTimeFormatter.ofPattern("HH:mm:ss");
                @Override
                public String format(LogRecord r) {
                    return String.format("[%s][%-7s] %s%n",
                            ZonedDateTime.now(ZONE).format(fmt), // Используем динамическую ZONE
                            r.getLevel(), r.getMessage());
                }
            });
        }
    }
}