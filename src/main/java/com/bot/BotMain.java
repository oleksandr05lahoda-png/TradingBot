package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class BotMain {

    public static void main(String[] args) {
        // Настройка Telegram
        String tgToken = "8395445212:AAF7X7oFBx72HgKGoRTcFpdFbuHcZOPfTig";  // вставь свой токен
        String chatId = "953233853"; // вставь свой chat_id
        TelegramBotSender telegram = new TelegramBotSender(tgToken, chatId);

        // Создаём SignalSender
        SignalSender signalSender = new SignalSender(telegram);

        // Получаем текущее локальное время в нужном часовом поясе
        ZoneId zone = ZoneId.of("Europe/Warsaw"); // здесь можно поставить любой твой часовой пояс
        LocalDateTime now = LocalDateTime.now(zone);
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");

        // Отправляем стартовое сообщение синхронно
        boolean sent = telegram.sendMessageSync("🚀 Бот запущен! Время: " + now.format(dtf));

        if (sent) {
            System.out.println("Стартовое сообщение отправлено в Telegram!");
        } else {
            System.out.println("Не удалось отправить стартовое сообщение.");
        }

        // Запускаем SignalSender
        signalSender.start();

        System.out.println("[" + now.format(dtf) + "] Бот запущен и работает!");
    }
}
