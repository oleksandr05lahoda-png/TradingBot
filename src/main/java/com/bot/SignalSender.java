package com.bot;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import org.json.JSONObject;

public class SignalSender {

    private String telegramToken;
    private String chatId;

    public SignalSender() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.json")) {
            if (input == null) {
                System.out.println("Конфиг не найден в resources!");
                return;
            }

            // Читаем весь файл в строку
            String jsonText = new String(input.readAllBytes(), StandardCharsets.UTF_8);

            // Создаем JSON объект
            JSONObject json = new JSONObject(jsonText);

            telegramToken = json.getString("telegram_token");
            chatId = json.getString("chat_id");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        System.out.println("SignalSender запущен. Token: " + telegramToken + ", ChatID: " + chatId);
        // Здесь позже будет логика запуска Python анализа и отправки сигналов
    }
}
