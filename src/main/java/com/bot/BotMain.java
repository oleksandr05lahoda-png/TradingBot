package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class BotMain {

    public static void main(String[] args) {

        // ===== CONFIG =====
        String tgToken = "PASTE_TOKEN_HERE";
        String chatId = "PASTE_CHAT_ID";

        TelegramBotSender telegram = new TelegramBotSender(tgToken, chatId);
        SignalSender signalSender = new SignalSender(telegram);

        ZoneId zone = ZoneId.of("Europe/Warsaw");
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");

        try {

            LocalDateTime now = LocalDateTime.now(zone);

            telegram.sendMessageSync(
                    "🚀 Бот запущен\n" +
                            "⏰ Время: " + now.format(dtf) + "\n" +
                            "📡 Режим: FUTURES 15m"
            );

            System.out.println("[" + now.format(dtf) + "] Bot started");

            // ===== START CORE =====
            signalSender.start();

        } catch (Exception e) {

            telegram.sendMessageSync(
                    "❌ Критическая ошибка старта:\n" + e.getMessage()
            );

            e.printStackTrace();
            return;
        }

        // ===== SHUTDOWN HOOK =====
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                signalSender.stop();
                telegram.sendMessageSync("🛑 Бот остановлен");
                System.out.println("Bot stopped");
            } catch (Exception ignored) {}
        }));

        // ===== KEEP JVM ALIVE =====
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {}
    }
}
