package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.*;

public final class BotMain {

    private static final Logger LOG = Logger.getLogger(BotMain.class.getName());

    private static final String TG_TOKEN  = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID   = System.getenv().getOrDefault("CHAT_ID", "953233853");
    private static final ZoneId ZONE      = ZoneId.of("Europe/Warsaw");
    private static final int    INTERVAL  = envInt("SIGNAL_INTERVAL_MIN", 1);
    private static final int    KLINES    = envInt("KLINES_LIMIT", 220);

    // ── ИСПРАВЛЕНИЕ: сужаем тихие часы
    //    Было: UTC 03:00–07:00 (4 часа)
    //    Стало: UTC 02:00–05:30 (3.5 часа)
    //    05:30 UTC = открытие Токио — уже есть объём на Asian pairs
    //    02:00–05:30 UTC — настоящая мёртвая зона (конец США, до Токио)
    private static final int  QUIET_START_H = 2;    // 02:00 UTC
    private static final int  QUIET_END_H   = 5;    // 05:xx UTC — check minutes too
    private static final int  QUIET_END_M   = 30;   // до 05:30 тихо, после — торгуем

    private static final Map<String, String> SECTOR_LEADERS = new LinkedHashMap<>() {{
        put("DOGEUSDT",  "MEME");
        put("SOLUSDT",   "L1");
        put("UNIUSDT",   "DEFI");
        put("LINKUSDT",  "INFRA");
        put("ETHUSDT",   "TOP");
        put("XRPUSDT",   "PAYMENT");
    }};

    private static final AtomicLong totalCycles    = new AtomicLong(0);
    private static final AtomicLong totalSignals   = new AtomicLong(0);
    private static final AtomicLong skippedQuiet   = new AtomicLong(0);
    private static final AtomicLong errorCount     = new AtomicLong(0);
    private static long startTimeMs = 0;

    // ── Circuit breaker: > 5 ошибок за 5 минут → пауза 2 мин
    private static final int  CB_THRESHOLD = 5;
    private static final long CB_WINDOW_MS = 5 * 60_000L;
    private static final long CB_PAUSE_MS  = 2 * 60_000L;
    private static long lastErrorWindowStart = 0;
    private static int  errorsInWindow = 0;

    public static void main(String[] args) {
        configureLogger();

        if (TG_TOKEN == null || TG_TOKEN.isBlank()) {
            LOG.severe("TELEGRAM_TOKEN не задан — выход.");
            System.exit(1);
        }

        startTimeMs = System.currentTimeMillis();
        lastErrorWindowStart = startTimeMs;

        final com.bot.TelegramBotSender telegram = new com.bot.TelegramBotSender(TG_TOKEN, CHAT_ID);
        final com.bot.GlobalImpulseController gic      = new com.bot.GlobalImpulseController();
        final com.bot.InstitutionalSignalCore isc      = new com.bot.InstitutionalSignalCore();
        final com.bot.SignalSender sender   = new com.bot.SignalSender(telegram, gic, isc);

        // Time Stop: уведомляем в Telegram когда сигнал закрывается по времени
        isc.setTimeStopCallback((sym, msg) -> telegram.sendMessageAsync(msg));

        telegram.sendMessageAsync(buildStartMessage());
        LOG.info("═══ GodBot v3.1 стартовал " + nowWarsawStr() + " ═══");

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
                1, r -> {
                    Thread t = new Thread(r, "GodBotMain");
                    t.setDaemon(false);
                    t.setUncaughtExceptionHandler((th, ex) ->
                            LOG.log(Level.SEVERE, "UNCAUGHT in " + th.getName(), ex));
                    return t;
                });

        Runnable cycle = () -> runCycle(telegram, gic, isc, sender);
        scheduler.scheduleAtFixedRate(cycle, 0, INTERVAL, TimeUnit.MINUTES);

        // Статистика каждые 15 минут (было 30 — так чаще видим что происходит)
        ScheduledExecutorService statsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "StatsThread"); t.setDaemon(true); return t;
        });
        statsScheduler.scheduleAtFixedRate(
                () -> logStats(telegram, gic, isc),
                15, 15, TimeUnit.MINUTES
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Завершение работы...");
            scheduler.shutdown();
            statsScheduler.shutdown();
            telegram.sendMessageAsync("🛑 GodBot остановлен. Циклов: " + totalCycles.get()
                    + " | Сигналов: " + totalSignals.get());
            telegram.shutdown();
            try { scheduler.awaitTermination(8, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); scheduler.shutdownNow(); }
        }, "ShutdownHook"));
    }

    private static void runCycle(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        long cycleStart = System.currentTimeMillis();
        try {
            // ── ИСПРАВЛЕНИЕ: улучшенная проверка тихих часов
            ZonedDateTime utcNow = ZonedDateTime.now(ZoneId.of("UTC"));
            int utcH = utcNow.getHour();
            int utcM = utcNow.getMinute();

            boolean quiet = false;
            if (utcH == QUIET_START_H && utcM >= 0) quiet = true;       // 02:00–03:00
            if (utcH == 3 || utcH == 4)              quiet = true;       // 03:xx–04:xx
            if (utcH == QUIET_END_H && utcM < QUIET_END_M) quiet = true; // 05:00–05:29

            if (quiet) {
                skippedQuiet.incrementAndGet();
                if (skippedQuiet.get() % 15 == 1)
                    LOG.info("Тихие часы UTC " + utcH + ":" + String.format("%02d", utcM));
                return;
            }

            // ── Circuit breaker
            long now = System.currentTimeMillis();
            if (now - lastErrorWindowStart > CB_WINDOW_MS) {
                lastErrorWindowStart = now;
                errorsInWindow = 0;
            }
            if (errorsInWindow >= CB_THRESHOLD) {
                LOG.warning("Circuit breaker: " + errorsInWindow + " ошибок за 5 мин — пауза");
                Thread.sleep(CB_PAUSE_MS);
                errorsInWindow = 0;
                return;
            }

            long cycle = totalCycles.incrementAndGet();
            LOG.info("══ ЦИКЛ #" + cycle + " START ══ " + nowWarsawStr());

            updateBtcContext(sender, gic);
            updateSectors(sender, gic);

            com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
            LOG.info("BTC: " + ctx.regime
                    + " | str=" + String.format("%.2f", ctx.impulseStrength)
                    + " | vol=" + String.format("%.2f", ctx.volatilityExpansion)
                    + " | trend=" + String.format("%.2f", ctx.btcTrend)
                    + " | " + isc.getStats());

            List<com.bot.DecisionEngineMerged.TradeIdea> signals = sender.generateSignals();

            if (signals == null || signals.isEmpty()) {
                LOG.info("Нет сигналов. " + isc.getStats());
                return;
            }

            int sent = 0;
            for (com.bot.DecisionEngineMerged.TradeIdea s : signals) {
                telegram.sendMessageAsync(s.toTelegramString());
                LOG.info("► СИГНАЛ: " + s.symbol + " " + s.side
                        + " prob=" + String.format("%.0f%%", s.probability)
                        + " flags=" + s.flags);
                totalSignals.incrementAndGet();
                sent++;
            }

            long cycleMs = System.currentTimeMillis() - cycleStart;
            LOG.info("══ ЦИКЛ #" + cycle + " END ══ sent=" + sent
                    + " time=" + cycleMs + "ms");

        } catch (Throwable t) {
            errorsInWindow++;
            LOG.log(Level.SEVERE, "CRITICAL ERROR в цикле", t);
            telegram.sendMessageAsync("⚠️ GodBot ERROR: " + t.getClass().getSimpleName()
                    + ": " + t.getMessage());
        }
    }

    private static void updateBtcContext(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        try {
            List<com.bot.TradingCore.Candle> btc = sender.fetchKlines("BTCUSDT", "15m", KLINES);
            if (btc != null && btc.size() > 30) gic.update(btc);
        } catch (Exception e) {
            LOG.warning("BTC context update failed: " + e.getMessage());
        }
    }

    private static void updateSectors(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        for (Map.Entry<String, String> e : SECTOR_LEADERS.entrySet()) {
            try {
                List<com.bot.TradingCore.Candle> sc = sender.fetchKlines(e.getKey(), "15m", 80);
                if (sc != null && sc.size() > 25) gic.updateSector(e.getValue(), sc);
            } catch (Exception ex) {
                // Продолжаем без этого сектора
            }
        }
    }

    private static String buildStartMessage() {
        return "🚀 *GodBot v4.0 запущен*\n"
                + "Таймфрейм: 15M | Пары: TOP-100\n"
                + "Тихие часы: UTC 02:00–05:30\n"
                + "Кулдаун: TOP=4m / ALT=3m / MEME=2m\n"
                + "Time Stop: 90 мин (6 свечей)\n"
                + "Фильтры: 26 факторов + GIC + ISC + OBI + LiqGuard + CorrGuard\n"
                + "Веса: динамические (ATR-нормализация)\n"
                + "_" + nowWarsawStr() + " Warsaw_";
    }

    private static void logStats(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc) {
        long uptimeMin = (System.currentTimeMillis() - startTimeMs) / 60_000;
        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        String msg = String.format(
                "📊 *GodBot Stats*\n"
                        + "Uptime: %d min | Циклов: %d | Сигналов: %d\n"
                        + "BTC: %s (str=%.2f, vol=%.2f)\n"
                        + "Ошибок: %d | %s",
                uptimeMin, totalCycles.get(), totalSignals.get(),
                ctx.regime, ctx.impulseStrength, ctx.volatilityExpansion,
                errorCount.get(),
                isc.getStats()
        );
        telegram.sendMessageAsync(msg);
        LOG.info("[STATS] " + msg.replace("\n", " | "));
    }

    private static String nowWarsawStr() {
        return ZonedDateTime.now(ZONE)
                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"));
    }

    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis)
                .atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }

    private static int envInt(String key, int def) {
        try { return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(def))); }
        catch (Exception e) { return def; }
    }

    private static void configureLogger() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        for (Handler h : root.getHandlers()) {
            h.setFormatter(new SimpleFormatter() {
                @Override public String format(LogRecord r) {
                    return String.format("[%s][%s] %s%n",
                            ZonedDateTime.now(ZoneId.of("Europe/Warsaw"))
                                    .format(DateTimeFormatter.ofPattern("HH:mm:ss")),
                            r.getLevel(), r.getMessage());
                }
            });
        }
    }
}