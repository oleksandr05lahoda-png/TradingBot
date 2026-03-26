package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║       BotMain v15.0 — CRITICAL ARCHITECTURE FIX EDITION                  ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
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
    private static final String TG_TOKEN  = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID   = System.getenv().getOrDefault("CHAT_ID", "953233853");
    private static final ZoneId ZONE      = ZoneId.of("Europe/Warsaw");
    private static final int    INTERVAL  = envInt("SIGNAL_INTERVAL_MIN", 1);
    private static final int    KLINES    = envInt("KLINES_LIMIT", 220);
    // Hard cap to avoid Telegram queue backlog (which can make the bot
    // "silent" for hours/days under heavy signal load).
    private static final int    MAX_SIGNALS_PER_CYCLE = envInt("MAX_SIGNALS_PER_CYCLE", 10);

    // [v14.0 FIX #8] Тихие часы расширены: 01:00–05:00 UTC (было 03:00–05:00)
    // Ликвидность падает уже с 01:00, спреды расширяются
    private static final int QUIET_START_H = 1;
    private static final int QUIET_END_H   = 5;
    private static final boolean QUIET_HOURS_ENABLED = envInt("QUIET_HOURS_ENABLED", 0) == 1;

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
    private static final AtomicLong skippedQuiet  = new AtomicLong(0);
    private static final AtomicLong errorCount    = new AtomicLong(0);
    private static long startTimeMs = 0;

    // ── Circuit breaker ─────────────────────────────────────────────────
    // [v14.0 FIX #2] Убран Thread.sleep. Вместо паузы — пропускаем циклы.
    // [v14.0 FIX #4] volatile на errorsInWindow
    private static final int  CB_THRESHOLD = 5;
    private static final long CB_WINDOW_MS = 5 * 60_000L;
    private static final long CB_PAUSE_MS  = 2 * 60_000L;
    private static volatile long lastErrorWindowStart = 0;
    private static volatile int  errorsInWindow       = 0;
    private static volatile long cbPauseUntil         = 0; // [FIX #2] время окончания паузы

    // ── Watchdog ──────────────────────────────────────────────────────────
    private static volatile long lastSignalMs       = 0;
    private static volatile long lastCycleSuccessMs = 0;
    private static volatile long lastStatsSuccessMs = 0;
    private static volatile long lastWatchdogAlertMs = 0;
    private static final long SIGNAL_DROUGHT_MS     = 30 * 60_000L;
    private static final long WATCHDOG_COOLDOWN_MS  = 10 * 60_000L;
    private static final AtomicLong watchdogAlerts  = new AtomicLong(0);

    // ── Daily summary ─────────────────────────────────────────────────────
    private static volatile int lastSummaryDay = -1;

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
        final long   createdAt;
        volatile boolean resolved = false;
        volatile String  actualOutcome = null;

        ForecastRecord(String sym, com.bot.TradingCore.Side side, double price,
                       String bias, double score) {
            this.symbol = sym; this.side = side; this.entryPrice = price;
            this.forecastBias = bias; this.forecastScore = score;
            this.createdAt = System.currentTimeMillis();
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
        volatile boolean tp1Hit        = false;
        volatile boolean tp2Hit        = false; // ← NEW: предотвращает дубли TP2 сообщений
        volatile double  trailingStop  = 0;

        // [v14.0 FIX #6] synchronized доступ к extremes через getter/setter
        private double  extremeLow    = Double.MAX_VALUE;
        private double  extremeHigh   = Double.MIN_VALUE;
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
                errorsInWindow++;
                LOG.log(Level.SEVERE, "[SAFE] Task '" + name + "' FAILED: " + t.getMessage(), t);
            }
        };
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN
    // ══════════════════════════════════════════════════════════════

    public static void main(String[] args) {
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

        // Callbacks — только в лог, не в Telegram
        isc.setTimeStopCallback((sym, msg) -> LOG.info("[ISC time-stop] " + sym + ": " + msg));
        gic.setPanicCallback(msg -> LOG.warning("[GIC panic] " + msg));

        // ── Schedulers ───────────────────────────────────────────
        ScheduledExecutorService mainSched = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "GodBot-Main");
            t.setDaemon(false);
            t.setUncaughtExceptionHandler((th, ex) ->
                    LOG.log(Level.SEVERE, "UNCAUGHT in " + th.getName(), ex));
            return t;
        });

        ScheduledExecutorService auxSched = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "GodBot-Aux");
            t.setDaemon(true);
            return t;
        });

        ExecutorService heavySched = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "GodBot-Heavy");
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

        // ── TradeResolver каждые 45s ──────────────────────────────
        auxSched.scheduleAtFixedRate(
                safe("TradeResolver", () -> runTradeResolver(sender, isc, telegram)),
                90, 45, TimeUnit.SECONDS);

        // ── Forecast accuracy checker (каждые 15m) ────────────────
        auxSched.scheduleAtFixedRate(
                safe("ForecastChecker", () -> checkForecastAccuracy(sender, telegram)),
                16, 15, TimeUnit.MINUTES);

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
        LOG.info("═══ GodBot v15.0 ARCHITECTURE FIX стартовал " + nowWarsawStr() + " ═══");
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN CYCLE
    // ══════════════════════════════════════════════════════════════

    private static void runCycle(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        long cycleStart = System.currentTimeMillis();

        if (isQuietHours()) {
            skippedQuiet.incrementAndGet();
            return;
        }

        // [v14.0 FIX #2] Circuit breaker БЕЗ Thread.sleep
        // Вместо блокировки потока — просто пропускаем цикл до окончания паузы
        long now = System.currentTimeMillis();
        if (now < cbPauseUntil) {
            LOG.fine("[CB] Пауза активна, осталось " + (cbPauseUntil - now) / 1000 + "s");
            return;
        }
        if (now - lastErrorWindowStart > CB_WINDOW_MS) {
            lastErrorWindowStart = now;
            errorsInWindow = 0;
        }
        if (errorsInWindow >= CB_THRESHOLD) {
            cbPauseUntil = now + CB_PAUSE_MS;
            errorsInWindow = 0;
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

        int sent = 0;
        for (int idx = 0; idx < limit; idx++) {
            com.bot.DecisionEngineMerged.TradeIdea s = signals.get(idx);
            telegram.sendMessageAsync(s.toTelegramString());

            // [v14.0 FIX #7] Безопасный доступ к forecast
            String forecastInfo = "N/A";
            String phaseInfo = "N/A";
            if (s.forecast != null) {
                forecastInfo = s.forecast.bias.name();
                phaseInfo = s.forecast.trendPhase.name();
            }

            LOG.info("► " + s.symbol + " " + s.side
                    + " conf=" + String.format("%.0f%%", s.probability)
                    + " forecast=" + forecastInfo
                    + " phase=" + phaseInfo);
            totalSignals.incrementAndGet();
            sent++;
            trackSignal(s);
            lastSignalMs = System.currentTimeMillis();
        }

        LOG.info("══ ЦИКЛ #" + cycle + " END ══ sent=" + sent
                + " time=" + (System.currentTimeMillis() - cycleStart) + "ms");
    }

    // ══════════════════════════════════════════════════════════════
    //  TRACK SIGNAL
    // ══════════════════════════════════════════════════════════════

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

        forecastRecords.put(key + "_" + System.currentTimeMillis(),
                new ForecastRecord(idea.symbol, idea.side, idea.price,
                        forecastBias, forecastScore));
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

            // Expire: 90 минут
            if (ts.ageMs() > 90 * 60_000L) {
                it.remove();
                LOG.info("[TR] EXPIRED (neutral): " + ts.symbol + " " + ts.side);
                continue;
            }

            // ── Получаем свежие 1m candles ──────────────────────
            double priceClose;
            try {
                List<com.bot.TradingCore.Candle> candles = sender.fetchKlines(ts.symbol, "1m", 4);
                if (candles == null || candles.isEmpty()) continue;

                double newLow = Double.MAX_VALUE, newHigh = Double.MIN_VALUE;
                for (com.bot.TradingCore.Candle c : candles) {
                    newLow  = Math.min(newLow,  c.low);
                    newHigh = Math.max(newHigh, c.high);
                }
                priceClose = candles.get(candles.size() - 1).close;

                // [v14.0 FIX #6] Thread-safe extreme update
                ts.updateExtremes(newLow, newHigh);
            } catch (Exception e) {
                LOG.fine("[TR] Fetch fail for " + ts.symbol + ": " + e.getMessage());
                continue;
            }

            if (priceClose <= 0) continue;

            boolean isLong = ts.side == com.bot.TradingCore.Side.LONG;
            double extremeLow  = ts.getExtremeLow();
            double extremeHigh = ts.getExtremeHigh();

            // ── SL hit ──────────────────────────────────────────
            boolean slHit = isLong
                    ? extremeLow  <= ts.sl
                    : extremeHigh >= ts.sl;

            if (slHit) {
                double pnl = isLong
                        ? (ts.sl - ts.entry) / ts.entry * 100
                        : (ts.entry - ts.sl) / ts.entry * 100;
                it.remove();
                isc.registerConfirmedResult(false, ts.side);
                isc.closeTrade(ts.symbol, ts.side, pnl);
                markForecastRecord(ts.symbol + "_" + ts.side, "HIT_SL");
                telegram.sendMessageAsync(String.format(
                        "❌ *SL HIT* %s %s | PnL: %+.2f%%\n"
                                + "Forecast was: %s (score %.2f)",
                        ts.symbol, ts.side, pnl,
                        ts.forecastBias, ts.forecastScore));
                LOG.info("[TR] SL HIT: " + ts.symbol + " pnl=" + String.format("%.2f%%", pnl));
                continue;
            }

            // ── TP1 hit ─────────────────────────────────────────
            boolean tp1Reached = isLong
                    ? extremeHigh >= ts.tp1
                    : extremeLow  <= ts.tp1;

            if (tp1Reached && !ts.tp1Hit) {
                ts.tp1Hit = true;
                ts.trailingStop = ts.entry; // trailing начинается с breakeven
                // Suppress intermediate TP1 notification to reduce Telegram spam.
            }

            // ── После TP1: trailing + TP2 + TP3 ────────────────
            if (ts.tp1Hit) {

                // [v14.0 FIX #3] Исправленный trailing stop
                if (isLong) {
                    // LONG: trailing поднимается за ценой
                    double newTrail = ts.entry + (extremeHigh - ts.entry) * 0.50;
                    ts.trailingStop = Math.max(ts.trailingStop, newTrail);
                } else {
                    // SHORT: trailing опускается за ценой (НИЖЕ entry, но ВЫШЕ extremeLow)
                    // trailingStop для SHORT должен быть НИЖЕ entry
                    // Когда цена падает (extremeLow уменьшается), trailing двигается вниз
                    double profit = ts.entry - extremeLow; // profit для SHORT = entry - low
                    double newTrail = ts.entry - profit * 0.50; // Лочим 50% прибыли
                    // Для SHORT trailing должен только УМЕНЬШАТЬСЯ (двигаться вниз = лучше)
                    if (ts.trailingStop == 0 || ts.trailingStop == ts.entry) {
                        ts.trailingStop = newTrail;
                    } else {
                        ts.trailingStop = Math.min(ts.trailingStop, newTrail);
                    }
                }

                // [v14.0 FIX #1] TP2 hit — ТОЛЬКО ОДИН РАЗ
                boolean tp2Reached = isLong
                        ? extremeHigh >= ts.tp2
                        : extremeLow  <= ts.tp2;

                if (tp2Reached && !ts.tp2Hit) {
                    ts.tp2Hit = true; // ← Устанавливаем флаг ПЕРЕД отправкой
                    // Suppress intermediate TP2 notification to reduce Telegram spam.
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
                    isc.closeTrade(ts.symbol, ts.side, pnl);
                    markForecastRecord(ts.symbol + "_" + ts.side, "HIT_TP3");
                    telegram.sendMessageAsync(String.format(
                            "✅✅ *TP3 HIT* %s %s | PnL: %+.2f%% 🚀",
                            ts.symbol, ts.side, pnl));
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
                    isc.closeTrade(ts.symbol, ts.side, pnl);
                    markForecastRecord(ts.symbol + "_" + ts.side,
                            pnl > 0 ? "EXPIRED_PROFIT" : "EXPIRED_FLAT");
                    // Suppress intermediate trailing stop notification to reduce spam.
                }
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  FORECAST ACCURACY CHECKER
    // ══════════════════════════════════════════════════════════════

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
                List<com.bot.TradingCore.Candle> c = sender.fetchKlines(fr.symbol, "15m", 2);
                if (c == null || c.isEmpty()) continue;
                double currentPrice = c.get(c.size() - 1).close;

                double changePct = (currentPrice - fr.entryPrice) / fr.entryPrice * 100.0;
                boolean bullishMove = changePct > 0.3;
                boolean bearishMove = changePct < -0.3;

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
                    forecastTotal.incrementAndGet();
                    if (correct) forecastCorrect.incrementAndGet();
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
                    telegram.sendMessageAsync(String.format(
                            "📈 *Forecast Accuracy Report*\n"
                                    + "Total: %d | Correct: %d | Accuracy: %.1f%%\n"
                                    + "%s",
                            total, correct2, acc,
                            acc >= 58 ? "✅ Edge confirmed — модель работает"
                                    : acc >= 50 ? "⚠️ Edge слабый — наблюдаем"
                                    : "🔴 Edge отрицательный — нужна перекалибровка"));
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
        if (isQuietHours()) return;
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

        if (sender.getActiveWsCount() < 3 || now - lastCycleSuccessMs > 5 * 60_000L) {
            sender.forceResubscribeTopPairs();
        }

        if (!issues.isEmpty() && now - lastWatchdogAlertMs > WATCHDOG_COOLDOWN_MS) {
            lastWatchdogAlertMs = now;
            watchdogAlerts.incrementAndGet();
            LOG.warning("[WD #" + watchdogAlerts.get() + "] " + String.join(" | ", issues));
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
                "📊 *GodBot v15.0 — Daily Report*\n"
                        + "🕙 %s (Warsaw)\n"
                        + "───────────────────────\n"
                        + "Up: %dm | Cycles: %d | Signals: %d\n"
                        + "Day PnL: %+.2f%% | MaxDD: %.1f%%\n"
                        + "BTC: %s | Vol: %s\n"
                        + "WS: %d | UDS: %s | Bal: $%.2f\n"
                        + "───────────────────────\n"
                        + "🔮 Forecast Accuracy: %.0f%% (%d/%d)\n"
                        + "📊 Tracked: %d | ForecastRecords: %d\n"
                        + "%s",
                nowWarsawStr(),
                uptimeMin, totalCycles.get(), totalSignals.get(),
                isc.getDailyPnL(), isc.getDrawdownFromPeak(),
                ctx.regime, ctx.volRegime,
                sender.getActiveWsCount(), sender.isUdsConnected() ? "✅" : "❌",
                sender.getAccountBalance(),
                fcAcc, fcCorrect, fcTotal,
                trackedSignals.size(), forecastRecords.size(),
                isc.getStats());

        telegram.sendMessageAsync(msg);
        LOG.info("[DAILY] Sent summary");
    }

    // ══════════════════════════════════════════════════════════════
    //  BACKTEST
    // ══════════════════════════════════════════════════════════════

    private static void runPeriodicBacktest(com.bot.SignalSender sender,
                                            com.bot.InstitutionalSignalCore isc,
                                            com.bot.TelegramBotSender telegram) {
        LOG.info("[BT] Начало периодического бэктеста...");
        com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();
        String[] syms = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "LINKUSDT", "XRPUSDT"};
        double totalEV = 0;
        int    count   = 0;
        StringBuilder btLog = new StringBuilder("[BT] Results: ");

        for (String sym : syms) {
            try {
                List<com.bot.TradingCore.Candle> m15 = sender.fetchKlines(sym, "15m", 500);
                List<com.bot.TradingCore.Candle> h1  = sender.fetchKlines(sym, "1h",  200);
                List<com.bot.TradingCore.Candle> m1  = sender.fetchKlines(sym, "1m",  500);
                List<com.bot.TradingCore.Candle> m5  = sender.fetchKlines(sym, "5m",  300);

                if (m15 == null || m15.size() < 250) continue;

                com.bot.SimpleBacktester.BacktestResult r = bt.run(
                        sym, m1, m5, m15, h1, com.bot.DecisionEngineMerged.CoinCategory.TOP);

                if (r.total >= 5) {
                    totalEV += r.ev;
                    count++;
                    btLog.append(sym).append("=").append(String.format("%.3f", r.ev)).append(" ");
                }
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
                        + "FC:%.0f%%(%d/%d) Err:%d WD:%d | %s",
                uptimeMin, totalCycles.get(), totalSignals.get(),
                trackedSignals.size(), forecastRecords.size(),
                ctx.regime, ctx.impulseStrength,
                sender.getActiveWsCount(), sender.isUdsConnected() ? "OK" : "DOWN",
                sender.getAccountBalance(),
                isc.getDailyPnL(), isc.getDrawdownFromPeak(),
                forecastTotal.get() > 0 ? (double) forecastCorrect.get() / forecastTotal.get() * 100 : 0.0,
                forecastCorrect.get(), forecastTotal.get(),
                errorCount.get(), watchdogAlerts.get(),
                isc.getStats());

        LOG.info(msg);
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

    private static boolean isQuietHours() {
        if (!QUIET_HOURS_ENABLED) return false;
        ZonedDateTime utc = ZonedDateTime.now(ZoneId.of("UTC"));
        int h = utc.getHour();
        return h >= QUIET_START_H && h < QUIET_END_H;
    }

    private static void markForecastRecord(String key, String outcome) {
        forecastRecords.entrySet().stream()
                .filter(e -> e.getKey().startsWith(key) && !e.getValue().resolved)
                .max(Comparator.comparingLong(e -> e.getValue().createdAt))
                .ifPresent(e -> {
                    e.getValue().resolved = true;
                    e.getValue().actualOutcome = outcome;
                    // Simplified: TP hit = correct, SL hit = wrong
                    boolean tp = outcome.contains("TP") || outcome.equals("EXPIRED_PROFIT");
                    if (!"EXPIRED_FLAT".equals(outcome)) {
                        forecastTotal.incrementAndGet();
                        if (tp) forecastCorrect.incrementAndGet();
                    }
                });
    }

    private static String buildStartMessage() {
        return String.format(
                "🚀 *GodBot v15.0 ARCHITECTURE FIX*\n"
                        + "15M Futures | 9-Factor Forecast | TOP-100\n"
                        + "───────────────────────────────\n"
                        + "✅ [Дыра 1] ConcurrentLinkedDeque everywhere\n"
                        + "✅ [Дыра 3] LR window 30→10 + acceleration\n"
                        + "✅ [Дыра 4] Streak: win halves boost aggressively\n"
                        + "✅ [KITE] VolatilitySqueezeGuard active\n"
                        + "✅ ForecastEngine 9+1 факторов активен\n"
                        + "✅ TrendPhase: EARLY/MID/LATE/EXHAUST\n"
                        + "✅ Structural stops за swing high/low\n"
                        + "✅ ISC с exponential streak decay\n"
                        + "✅ Trailing stop FIXED (SHORT logic)\n"
                        + "⏰ Тихие часы: UTC 01:00–05:00\n"
                        + "📅 Daily report в 09:00 UTC\n"
                        + "───────────────────────────────\n"
                        + "🕐 %s (Warsaw)",
                nowWarsawStr());
    }

    private static String nowWarsawStr() {
        return ZonedDateTime.now(ZONE)
                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"));
    }

    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis).atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }

    private static int envInt(String k, int d) {
        try {
            return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(d)));
        } catch (Exception e) {
            return d;
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
                            ZonedDateTime.now(ZoneId.of("Europe/Warsaw")).format(fmt),
                            r.getLevel(), r.getMessage());
                }
            });
        }
    }
}