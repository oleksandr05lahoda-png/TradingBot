package com.bot;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TelegramBotSender {

    private final String token;
    private final String chatId;
    private final HttpClient client;

    public TelegramBotSender(String token, String chatId) {
        this.token = token;
        this.chatId = chatId;
        this.client = HttpClient.newHttpClient();
    }

    // Асинхронная отправка сообщения
    public void sendMessage(String message) {
        try {
            String url = "https://api.telegram.org/bot" + token + "/sendMessage?chat_id=" +
                    chatId + "&text=" + URLEncoder.encode(message, StandardCharsets.UTF_8);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(resp -> System.out.println("[TG " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] Ответ: " + resp.body()))
                    .exceptionally(e -> {
                        System.out.println("[TG] Ошибка отправки: " + e.getMessage());
                        return null;
                    });
        } catch (Exception e) {
            System.out.println("[TG] Ошибка создания запроса: " + e.getMessage());
        }
    }

    // Синхронная отправка сообщения
    public boolean sendMessageSync(String message) {
        try {
            String url = "https://api.telegram.org/bot" + token + "/sendMessage?chat_id=" +
                    chatId + "&text=" + URLEncoder.encode(message, StandardCharsets.UTF_8);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("[TG " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] Ответ: " + response.body());
            return true;
        } catch (Exception e) {
            System.out.println("[TG] Ошибка отправки: " + e.getMessage());
            return false;
        }
    }

    // Удобный метод для сигналов
    public void sendSignal(String symbol, String direction, double confidence, double price, int rsi, String flags) {
        String message = symbol + " → " + direction + "\n" +
                "Confidence: " + String.format("%.2f", confidence) + "\n" +
                "Price: " + price + "\n" +
                "RSI: " + rsi + "\n" +
                "Flags: " + flags + "\n" +
                "Time: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        sendMessage(message);
    }

    // Перегрузка для простого сообщения без параметров
    public void sendSignal(String message) {
        sendMessage(message);
    }
}
