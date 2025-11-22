package com.bot;

public class BotMain {
    public static void main(String[] args) {
        System.setProperty("java.awt.headless", "true");
        System.out.println("Trading Telegram Bot запущен...");
        SignalSender sender = new SignalSender();
        sender.start();
    }
}