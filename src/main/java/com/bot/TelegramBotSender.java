package com.bot;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

public class TelegramBotSender {
    private final String token;
    private final String chatId;
    private final HttpClient client;

    public TelegramBotSender(String token, String chatId) {
        this.token = token;
        this.chatId = chatId;
        this.client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
    }
    public void sendSignal(String message) {
        // Подставим дефолтные значения для остальных параметров
        sendSignal("", "", 0.0, 0.0, 0, message);
    }

    public void sendSignal(String symbol, String direction, double confidence, double price, int rsi, String flags) {
        String message = symbol + " → " + direction + "\n" +
                "Confidence: " + String.format("%.2f", confidence) + "\n" +
                "Price: " + String.format("%.2f", price) + "\n" +
                "RSI: " + rsi + "\n" +
                "Flags: " + flags + "\n" +
                "Time: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        sendMessage(message);
    }


    public void sendMessage(String message) {
        try {
            String url = "https://api.telegram.org/bot" + token + "/sendMessage?chat_id=" + chatId + "&text=" + message;
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
}
