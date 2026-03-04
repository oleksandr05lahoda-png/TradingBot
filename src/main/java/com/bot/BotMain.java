package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public final class BotMain {

    // ================= CONFIG =================

    private static final String TG_TOKEN = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID = "953233853";
    private static final ZoneId ZONE = ZoneId.systemDefault();
    private static final int SIGNAL_INTERVAL_MIN = 15;

    // ================= MAIN =================

    public static void main(String[] args) {

        if (TG_TOKEN == null || TG_TOKEN.isBlank()) {
            System.err.println("TELEGRAM_TOKEN not set!");
            return;
        }

        TelegramBotSender telegram = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        SignalSender signalSender = new SignalSender(telegram);
        GlobalImpulseController globalImpulse = new GlobalImpulseController();

        telegram.sendMessageAsync("🚀 Trading Bot запущен");
        System.out.println("Bot started at " + LocalDateTime.now());

        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor(new BotThreadFactory());

        Runnable signalTask = () -> {
            try {
                System.out.println("=== SIGNAL SCAN START === " + LocalDateTime.now());

                // Получаем BTC 15m свечи
                List<TradingCore.Candle> btcC15 = getBtcC15();

                // Обновляем глобальный контекст
                globalImpulse.update(btcC15);
                GlobalImpulseController.GlobalContext context =
                        globalImpulse.getContext();

                // Генерируем сигналы
                List<DecisionEngineMerged.TradeIdea> signals =
                        signalSender.generateSignals(context);

                if (signals == null || signals.isEmpty()) {
                    System.out.println("No valid signals this cycle.");
                    return;
                }

                for (DecisionEngineMerged.TradeIdea s : signals) {
                    telegram.sendMessageAsync(formatSignal(s));
                }

            } catch (Throwable t) {
                System.err.println("CRITICAL ERROR in signal cycle:");
                t.printStackTrace();
                telegram.sendMessageAsync("⚠ Ошибка цикла: " + t.getMessage());
            }
        };

        // Запуск сразу + каждые 15 минут
        scheduler.scheduleAtFixedRate(
                signalTask,
                0,
                SIGNAL_INTERVAL_MIN,
                TimeUnit.MINUTES
        );

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down bot...");
            scheduler.shutdown();
            telegram.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException ignored) {
                scheduler.shutdownNow();
            }
        }));
    }

    // ================= MOCK BTC DATA =================

    /**
     * Мок получения BTC 15m свечей.
     * Здесь должен быть реальный API (Binance / Bybit и т.д.)
     */
    private static List<TradingCore.Candle> getBtcC15() {
        return Collections.emptyList();
    }

    // ================= FORMAT SIGNAL =================

    /**
     * probability хранится в шкале 0.0 – 1.0
     * Для Telegram выводим в процентах.
     */
    private static String formatSignal(DecisionEngineMerged.TradeIdea s) {

        String flags = (s.flags != null && !s.flags.isEmpty())
                ? String.join(", ", s.flags)
                : "—";

        return String.format(
                "*%s* → *%s*\n" +
                        "Price: %.6f\n" +
                        "Probability: %.0f%%\n" +
                        "Stop-Take: %.6f - %.6f\n" +
                        "Flags: %s\n" +
                        "_time: %s_",
                s.symbol,
                s.side,
                s.price,
                s.probability * 100.0,   // ← важный фикс
                s.stop,
                s.take,
                flags,
                LocalTime.now().format(
                        DateTimeFormatter.ofPattern("HH:mm:ss"))
        );
    }

    // ================= THREAD FACTORY =================

    static final class BotThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "SignalSchedulerThread");
            t.setDaemon(false);

            t.setUncaughtExceptionHandler((thread, ex) -> {
                System.err.println("UNCAUGHT ERROR in " + thread.getName());
                ex.printStackTrace();
            });

            return t;
        }
    }

    // ================= TIME UTIL =================

    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis)
                .atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }
}