package com.bot;

public class BotMain {
    public static void main(String[] args) {
        System.out.println("Starting Trading Bot...");
        TelegramBotSender tele = new TelegramBotSender();
        SignalSender sender = new SignalSender(tele);

        // Запуск scheduler
        sender.start();

        // Main thread ждёт бесконечно
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Main interrupted");
        }
    }
}
