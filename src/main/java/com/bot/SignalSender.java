package com.bot;

import java.io.*;
import java.util.Properties;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SignalSender {

    private String telegramToken;
    private String chatId;

    public SignalSender() {
        // Загружаем конфиг
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.json")) {
            if (input == null) {
                System.out.println("Конфиг не найден!");
                return;
            }
            String json = new String(input.readAllBytes());
            telegramToken = json.split("\"telegram_token\":")[1].split("\"")[1];
            chatId = json.split("\"chat_id\":")[1].split("\"")[1];
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        try {
            ProcessBuilder pb = new ProcessBuilder("python3", "python-core/analysis.py");
            Process process = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                // Пример: BTCUSDT,LONG,0.85
                System.out.println("Сигнал: " + line);
                // TODO: здесь подключить отправку в Telegram через Bot API
            }

            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
