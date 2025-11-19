package com.bot;

import java.io.*;

public class SignalSender {

    private String telegramToken;
    private String chatId;

    public SignalSender() {
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
            // Запускаем Python через виртуальное окружение
            ProcessBuilder pb = new ProcessBuilder("./venv/bin/python", "python-core/analysis.py");
            pb.redirectErrorStream(true);
            System.out.println("Запускаем Python-анализатор...");
            Process process = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Сигнал: " + line);
                // TODO: подключить Telegram API для отправки
            }

            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
