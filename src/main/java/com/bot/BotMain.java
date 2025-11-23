package com.bot;

public class BotMain {
    public static void main(String[] args) {
        System.out.println("Trading Telegram Bot запущен...");

        // Создаем TelegramBotSender
        TelegramBotSender bot = new TelegramBotSender();

        // Передаем бот в SignalSender
        SignalSender sender = new SignalSender(bot);

        // Запуск анализа
        sender.start();
    }
}
