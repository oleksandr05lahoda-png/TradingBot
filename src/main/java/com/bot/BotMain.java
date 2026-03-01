package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.*;

public class BotMain {

    // ===== CONFIG =====
    private static final String TG_TOKEN = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID = "953233853";
    private static final ZoneId ZONE = ZoneId.systemDefault();
    private static final int SIGNAL_INTERVAL_MIN = 15;

    public static void main(String[] args) {

        if (TG_TOKEN == null || TG_TOKEN.isBlank()) {
            System.err.println("TELEGRAM_TOKEN not set!");
            return;
        }

        TelegramBotSender telegram = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        SignalSender signalSender = new SignalSender(telegram);

        telegram.sendMessageAsync("🚀 Trading Bot запущен");
        System.out.println("Bot started at " + LocalDateTime.now());

        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(2, new BotThreadFactory());

        Runnable signalTask = () -> {
            try {
                System.out.println("=== SIGNAL SCAN START === " + LocalDateTime.now());

                List<DecisionEngineMerged.TradeIdea> signals = signalSender.generateSignals();

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
        }));
    }

    /**
     * Формат сигнала для Telegram
     * Убираем reason/RSI, оставляем только flags
     */
    private static String formatSignal(DecisionEngineMerged.TradeIdea s) {

        String flags = s.flags != null && !s.flags.isEmpty()
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
                s.probability,   // ← ВОТ ТУТ
                s.stop,
                s.take,
                flags,
                LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))
        );
    }
    /**
     * Кастомный ThreadFactory с защитой от смерти потока
     */
    static class BotThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("SignalSchedulerThread");
            t.setDaemon(false);

            t.setUncaughtExceptionHandler((thread, ex) -> {
                System.err.println("UNCAUGHT ERROR in " + thread.getName());
                ex.printStackTrace();
            });

            return t;
        }
    }

    /**
     * UTC → локальное
     */
    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis)
                .atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }
}