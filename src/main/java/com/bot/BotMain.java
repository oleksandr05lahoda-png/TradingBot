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

        // ===== INIT BOT =====
        TelegramBotSender telegram = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        SignalSender signalSender = new SignalSender(telegram);

        // ===== START BOT =====
        try {
            LocalDateTime now = LocalDateTime.now(ZONE);

            telegram.sendMessageAsync("🚀 Бот запущен");
            System.out.println("[" + now.format(TIME_FORMATTER) + "] Bot started");

            // ===== START SIGNALS =====
            signalSender.start(); // запускаем все анализаторы
        } catch (Exception e) {
            telegram.sendMessageAsync("❌ Ошибка старта SignalSender: " + e.getMessage());
            e.printStackTrace();
        }

        // ===== KEEP JVM ALIVE 24/7 =====
        while (true) {
            try {
                Thread.sleep(60_000); // спим по 1 минуте
            } catch (InterruptedException ignored) {}
        }
    }
}
