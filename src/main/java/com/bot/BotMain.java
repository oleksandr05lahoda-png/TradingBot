package com.bot;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BotMain {

    public static void main(String[] args) {
        String tgToken = "ВАШ_TELEGRAM_BOT_TOKEN";
        String chatId = "ВАШ_CHAT_ID";

        TelegramBotSender telegram = new TelegramBotSender(tgToken, chatId);
        SignalSender signalSender = new SignalSender(telegram);

        // Отправляем стартовое сообщение
        telegram.sendMessage("🚀 Бот запущен! Время: " +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));

        signalSender.start();

        System.out.println("[" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] Бот запущен и работает!");
    }
}
