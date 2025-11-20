package com.bot;

import java.io.*;
import java.util.*;

public class SignalSender {

    private String telegramToken;
    private String chatId;
    private List<String> coins;
    private String timeframe;
    private double signalThreshold;

    public SignalSender() {
        // Читаем переменные окружения
        telegramToken = System.getenv("8395445212:AAF7X7oFBx72HgKGoRTcFpdFbuHcZOPfTig");
        chatId = System.getenv("953233853");
        String coinsEnv = System.getenv("COINS"); // Например "BTCUSDT,ETHUSDT,BNBUSDT"
        coins = coinsEnv != null ? Arrays.asList(coinsEnv.split(",")) : List.of("BTCUSDT", "ETHUSDT", "BNBUSDT");
        timeframe = System.getenv().getOrDefault("TIMEFRAME", "1m");
        signalThreshold = Double.parseDouble(System.getenv().getOrDefault("SIGNAL_THRESHOLD", "0.7"));

        if (telegramToken == null || chatId == null) {
            System.out.println("Переменные TELEGRAM_TOKEN и CHAT_ID не заданы!");
        }
    }

    public void start() {
        try {
            // Запускаем Python-анализатор
            ProcessBuilder pb = new ProcessBuilder("python3", "src/python-core/analysis.py");
            pb.redirectErrorStream(true);
            System.out.println("Запускаем Python-анализатор...");
            Process process = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Сигнал: " + line);
                // TODO: подключить Telegram API для отправки сигналов
            }

            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
