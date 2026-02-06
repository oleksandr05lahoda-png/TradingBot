package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class BotMain {

    // ===== CONFIG =====
    private static final String TG_TOKEN = "8395445212:AAF7X7oFBx72HgKGoRTcFpdFbuHcZOPfTig";
    private static final String CHAT_ID = "953233853";
    private static final ZoneId ZONE = ZoneId.of("Europe/Warsaw");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void main(String[] args) {

        TelegramBotSender telegram = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        SignalSender signalSender = new SignalSender(telegram);

        try {
            LocalDateTime now = LocalDateTime.now(ZONE);

            // Асинхронная отправка стартового сообщения
            telegram.sendMessageAsync(
                    "🚀 Бот запущен\n"
            );

            System.out.println("[" + now.format(TIME_FORMATTER) + "] Bot started");

            // ===== START CORE =====
            signalSender.start();

        } catch (Exception e) {
            telegram.sendMessageAsync(
                    "❌ Ошибка старта SignalSender: " + e.getMessage()
            );
            e.printStackTrace();
            return;
        }

        // ===== SHUTDOWN HOOK =====
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                signalSender.stop(); // корректная остановка SignalSender
                telegram.sendMessageAsync("🛑 Бот остановлен");
                System.out.println("Bot stopped");
            } catch (Exception ignored) {}
        }));

        // ===== KEEP JVM ALIVE =====
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {}
    }
}
