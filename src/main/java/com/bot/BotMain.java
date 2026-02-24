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
    private static final int SIGNAL_INTERVAL_MIN = 15; // каждые 15 минут

    public static void main(String[] args) {

        TelegramBotSender telegram = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        SignalSender signalSender = new SignalSender(telegram);

        telegram.sendMessageAsync("Бот запущен");
        System.out.println("Bot started");

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        long initialDelay = computeInitialDelay();

        scheduler.scheduleAtFixedRate(() -> {
            try {

                System.out.println("Signal cycle: " + LocalDateTime.now());

                // Получаем уже готовые, отфильтрованные сигналы
                List<DecisionEngineMerged.TradeIdea> signals =
                        signalSender.generateSignals();

                for (DecisionEngineMerged.TradeIdea s : signals) {
                    telegram.sendMessageAsync(formatSignal(s));
                }

            } catch (Exception ex) {
                telegram.sendMessageAsync("Ошибка генерации сигналов: " + ex.getMessage());
                ex.printStackTrace();
            }

        }, initialDelay, SIGNAL_INTERVAL_MIN, TimeUnit.MINUTES);

        // KEEP JVM ALIVE
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {}
    }

    /**
     * Синхронизация с закрытием M15 свечи
     */
    private static long computeInitialDelay() {
        LocalDateTime now = LocalDateTime.now(ZONE);
        int minute = now.getMinute();
        int delay = SIGNAL_INTERVAL_MIN - (minute % SIGNAL_INTERVAL_MIN);
        if (delay == SIGNAL_INTERVAL_MIN) delay = 0;
        return delay;
    }

    /**
     * Формат Telegram сообщения
     */
    private static String formatSignal(DecisionEngineMerged.TradeIdea s) {
        return String.format(
                "%s %s\nEntry: %.4f\nStop: %.4f\nTake: %.4f\nConfidence: %.2f\nGrade: %s",
                s.symbol,
                s.side,
                s.entry,
                s.stop,
                s.take,
                s.confidence,
                s.grade
        );
    }

    /**
     * UTC -> локальное время
     */
    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis)
                .atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }
}