package com.bot;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BotMain {

    public static void main(String[] args) {
        String tgToken = "TELEGRAM_TOKEN";  // вставь свой токен
        String chatId = "TELEGRAM_CHAT_ID"; // вставь свой chat_id

        TelegramBotSender telegram = new TelegramBotSender(tgToken, chatId);
        SignalSender signalSender = new SignalSender(telegram);

        // Отправляем стартовое сообщение синхронно
        boolean sent = telegram.sendMessageSync("🚀 Бот запущен! Время: " +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));

        if (sent) {
            System.out.println("Стартовое сообщение отправлено в Telegram!");
        } else {
            System.out.println("Не удалось отправить стартовое сообщение.");
        }

        // Запускаем SignalSender
        signalSender.start();

        System.out.println("[" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] Бот запущен и работает!");
    }
}
