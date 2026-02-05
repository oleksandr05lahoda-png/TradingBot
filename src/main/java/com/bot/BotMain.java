package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class BotMain {

    public static void main(String[] args) {

        String tgToken = "8395445212:AAF7X7oFBx72HgKGoRTcFpdFbuHcZOPfTig";
        String chatId = "953233853";

        TelegramBotSender telegram = new TelegramBotSender(tgToken, chatId);
        SignalSender signalSender = new SignalSender(telegram);

        ZoneId zone = ZoneId.of("Europe/Warsaw");
        LocalDateTime now = LocalDateTime.now(zone);
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");

        telegram.sendMessageSync(
                "🚀 Бот запущен\n" +
                        "⏰ Время: " + now.format(dtf) + "\n" +
                        "📡 Режим: FUTURES 15m"
        );

        System.out.println("[" + now.format(dtf) + "] Bot started");

        try {
            signalSender.start();
        } catch (Exception e) {
            telegram.sendMessageSync("❌ Ошибка старта SignalSender: " + e.getMessage());
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            telegram.sendMessageSync("🛑 Бот остановлен");
            System.out.println("Bot stopped");
        }));
    }
}
