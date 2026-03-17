package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.*;

/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║              GODBOT — ENTRY POINT                           ║
 * ║  15-минутный таймфрейм · Binance Futures · USDT             ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 * Архитектура shared-объектов (один экземпляр на всё приложение):
 *  · GlobalImpulseController  — BTC режим + 6 секторных лидеров
 *  · InstitutionalSignalCore  — портфолио риск-менеджмент
 *  · SignalSender             — получает оба объекта, не создаёт дубли
 *
 * Цикл:
 *  1. Тихие часы UTC 03:00–06:59 → пропускаем (нет ликвидности)
 *  2. Обновляем BTC 15m (220 свечей) → GlobalImpulse.update()
 *  3. Обновляем 6 сектор-лидеров → GlobalImpulse.updateSector()
 *  4. SignalSender.generateSignals() → все пары параллельно, кэш свечей
 *  5. Все прошедшие фильтры сигналы → Telegram
 *
 * НЕТ лимита на количество сигналов в цикле.
 * Качество = фильтры, а не обрезка по числу.
 */
public final class BotMain {

    private static final Logger LOG = Logger.getLogger(BotMain.class.getName());

    // ── Конфигурация из окружения ──────────────────────────────────
    private static final String TG_TOKEN  = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID   = System.getenv().getOrDefault("CHAT_ID", "953233853");
    private static final ZoneId ZONE      = ZoneId.of("Europe/Warsaw");
    private static final int    INTERVAL  = envInt("SIGNAL_INTERVAL_MIN", 1);
    private static final int    KLINES    = envInt("KLINES_LIMIT", 220);

    // ── Тихие часы UTC: азиатская ночь, нет ликвидности ───────────
    private static final int QUIET_START = 3;   // 03:00 UTC
    private static final int QUIET_END   = 7;   // 07:00 UTC

    /**
     * Сектор-лидеры.
     * Symbol → Sector name.
     * GlobalImpulse обновляет sector bias каждый цикл.
     * Это влияет на фильтрацию всех монет того же сектора.
     */
    private static final Map<String, String> SECTOR_LEADERS = new LinkedHashMap<>() {{
        put("DOGEUSDT",  "MEME");
        put("SOLUSDT",   "L1");
        put("UNIUSDT",   "DEFI");
        put("LINKUSDT",  "INFRA");
        put("ETHUSDT",   "TOP");
        put("XRPUSDT",   "PAYMENT");
    }};

    // ── Счётчики статистики ────────────────────────────────────────
    private static final AtomicLong totalCycles    = new AtomicLong(0);
    private static final AtomicLong totalSignals   = new AtomicLong(0);
    private static final AtomicLong skippedQuiet   = new AtomicLong(0);
    private static long startTimeMs = 0;

    // ══════════════════════════════════════════════════════════════
    public static void main(String[] args) {
        configureLogger();

        if (TG_TOKEN == null || TG_TOKEN.isBlank()) {
            LOG.severe("TELEGRAM_TOKEN not set — exiting.");
            System.exit(1);
        }

        startTimeMs = System.currentTimeMillis();

        // ── Shared компоненты ──────────────────────────────────────
        final TelegramBotSender        telegram     = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        final GlobalImpulseController  gic          = new GlobalImpulseController();
        final InstitutionalSignalCore  isc          = new InstitutionalSignalCore();
        final SignalSender             sender       = new SignalSender(telegram, gic, isc);

        // ── Стартовое сообщение ────────────────────────────────────
        telegram.sendMessageAsync(buildStartMessage());
        LOG.info("═══ GodBot started at " + nowWarsawStr() + " ═══");
        LOG.info("Interval=" + INTERVAL + "m | Pairs=TOP100 | Candle cache=ON | Parallel=ON");

        // ── Планировщик ────────────────────────────────────────────
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
                1, r -> {
                    Thread t = new Thread(r, "GodBotMainThread");
                    t.setDaemon(false);
                    t.setUncaughtExceptionHandler((th, ex) ->
                            LOG.log(Level.SEVERE, "UNCAUGHT in " + th.getName(), ex));
                    return t;
                }
        );

        Runnable cycle = () -> runCycle(telegram, gic, isc, sender);
        scheduler.scheduleAtFixedRate(cycle, 0, INTERVAL, TimeUnit.MINUTES);

        // ── Статистика каждые 30 минут ─────────────────────────────
        ScheduledExecutorService statsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "StatsThread");
            t.setDaemon(true);
            return t;
        });
        statsScheduler.scheduleAtFixedRate(
                () -> logStats(telegram, gic, isc),
                30, 30, TimeUnit.MINUTES
        );

        // ── Shutdown hook ──────────────────────────────────────────
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown initiated...");
            scheduler.shutdown();
            statsScheduler.shutdown();
            telegram.sendMessageAsync("🛑 GodBot остановлен. Циклов: " + totalCycles.get()
                    + " | Сигналов: " + totalSignals.get());
            telegram.shutdown();
            try {
                scheduler.awaitTermination(8, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }, "ShutdownHook"));
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN CYCLE
    // ══════════════════════════════════════════════════════════════

    private static void runCycle(TelegramBotSender telegram,
                                 GlobalImpulseController gic,
                                 InstitutionalSignalCore isc,
                                 SignalSender sender) {
        long cycleStart = System.currentTimeMillis();
        try {
            // ── Тихие часы ────────────────────────────────────────
            int utcHour = ZonedDateTime.now(ZoneId.of("UTC")).getHour();
            if (utcHour >= QUIET_START && utcHour < QUIET_END) {
                skippedQuiet.incrementAndGet();
                if (skippedQuiet.get() % 10 == 1)
                    LOG.info("Quiet hours UTC " + utcHour + ":xx — skipping");
                return;
            }

            long cycle = totalCycles.incrementAndGet();
            LOG.info("══ CYCLE #" + cycle + " START ══ " + nowWarsawStr());

            // ── Обновляем BTC контекст ─────────────────────────────
            updateBtcContext(sender, gic);

            // ── Обновляем секторные лидеры ─────────────────────────
            updateSectors(sender, gic);

            // Логируем BTC-состояние
            GlobalImpulseController.GlobalContext ctx = gic.getContext();
            LOG.info("BTC: " + ctx.regime
                    + " | str=" + String.format("%.2f", ctx.impulseStrength)
                    + " | vol=" + String.format("%.2f", ctx.volatilityExpansion)
                    + " | trend=" + String.format("%.2f", ctx.btcTrend)
                    + " | " + isc.getStats());

            // ── Генерация сигналов ─────────────────────────────────
            List<DecisionEngineMerged.TradeIdea> signals = sender.generateSignals();

            if (signals == null || signals.isEmpty()) {
                LOG.info("No signals this cycle. " + isc.getStats());
                return;
            }

            // ── Отправляем все прошедшие фильтры ──────────────────
            int sent = 0;
            for (DecisionEngineMerged.TradeIdea s : signals) {
                telegram.sendMessageAsync(s.toTelegramString());
                LOG.info("► SIGNAL: " + s.symbol + " " + s.side
                        + " prob=" + String.format("%.0f%%", s.probability)
                        + " flags=" + s.flags);
                totalSignals.incrementAndGet();
                sent++;
            }

            long cycleMs = System.currentTimeMillis() - cycleStart;
            LOG.info("══ CYCLE #" + cycle + " END ══ sent=" + sent
                    + " time=" + cycleMs + "ms " + isc.getStats());

        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "CRITICAL ERROR in cycle", t);
            telegram.sendMessageAsync("⚠️ *GodBot ERROR*: " + t.getClass().getSimpleName()
                    + ": " + t.getMessage());
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  BTC + SECTOR UPDATES
    // ══════════════════════════════════════════════════════════════

    private static void updateBtcContext(SignalSender sender, GlobalImpulseController gic) {
        try {
            List<TradingCore.Candle> btc = sender.fetchKlines("BTCUSDT", "15m", KLINES);
            if (btc != null && btc.size() > 30) {
                gic.update(btc);
            }
        } catch (Exception e) {
            LOG.warning("BTC context update failed: " + e.getMessage());
        }
    }

    private static void updateSectors(SignalSender sender, GlobalImpulseController gic) {
        for (Map.Entry<String, String> e : SECTOR_LEADERS.entrySet()) {
            try {
                List<TradingCore.Candle> sc = sender.fetchKlines(e.getKey(), "15m", 80);
                if (sc != null && sc.size() > 25) {
                    gic.updateSector(e.getValue(), sc);
                }
            } catch (Exception ex) {
                // Продолжаем без этого сектора
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  HELPERS
    // ══════════════════════════════════════════════════════════════

    private static String buildStartMessage() {
        return "🚀 *GodBot v3.0 запущен*\n"
                + "Таймфрейм: 15M | Пары: TOP-100\n"
                + "Фичи: Anti-Lag · FR-Delta · VolDelta-WS · FVG · OB · BOS · Liq.Sweep · Multi-TP\n"
                + "Секторный контекст: MEME/L1/DEFI/INFRA/TOP/PAYMENT\n"
                + "Cooldown: MEME 5m / ALT 10m / TOP 12m\n"
                + "Лимит сигналов: ∞ (все что прошли фильтры)\n"
                + "_" + nowWarsawStr() + " Warsaw_";
    }

    private static void logStats(TelegramBotSender telegram,
                                 GlobalImpulseController gic,
                                 InstitutionalSignalCore isc) {
        long uptimeMin = (System.currentTimeMillis() - startTimeMs) / 60_000;
        GlobalImpulseController.GlobalContext ctx = gic.getContext();
        String msg = String.format(
                "📊 *Статистика GodBot*\n"
                        + "Uptime: %d min | Циклов: %d | Сигналов: %d\n"
                        + "BTC: %s (%.2f strength)\n"
                        + "%s",
                uptimeMin, totalCycles.get(), totalSignals.get(),
                ctx.regime, ctx.impulseStrength,
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
        try {
            return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(def)));
        } catch (Exception e) { return def; }
    }

    private static void configureLogger() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        for (Handler h : root.getHandlers()) {
            h.setFormatter(new SimpleFormatter() {
                @Override
                public String format(LogRecord r) {
                    return String.format("[%s][%s] %s%n",
                            ZonedDateTime.now(ZoneId.of("Europe/Warsaw"))
                                    .format(DateTimeFormatter.ofPattern("HH:mm:ss")),
                            r.getLevel(), r.getMessage());
                }
            });
        }
    }
}
