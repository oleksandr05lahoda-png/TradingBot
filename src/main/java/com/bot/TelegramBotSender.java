package com.bot;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class TelegramBotSender {

    private final String token;
    private final String chatId;
    private final HttpClient client;
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public TelegramBotSender(String token, String chatId) {
        this.token = token;
        this.chatId = chatId;
        this.client = HttpClient.newHttpClient();
    }

    // ----------------- ASYNC MESSAGE -----------------
    public void sendMessage(String message) {
        try {
            String url = "https://api.telegram.org/bot" + token + "/sendMessage?chat_id=" +
                    chatId + "&text=" + URLEncoder.encode(message, StandardCharsets.UTF_8);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(resp -> System.out.println("[TG " + LocalDateTime.now().format(dtf) + "] Ответ: " + resp.body()))
                    .exceptionally(e -> {
                        System.err.println("[TG] Ошибка отправки: " + e.getMessage());
                        return null;
                    });

        } catch (Exception e) {
            System.err.println("[TG] Ошибка создания запроса: " + e.getMessage());
        }
    }

    // ----------------- SYNC MESSAGE -----------------
    public boolean sendMessageSync(String message) {
        try {
            String url = "https://api.telegram.org/bot" + token + "/sendMessage?chat_id=" +
                    chatId + "&text=" + URLEncoder.encode(message, StandardCharsets.UTF_8);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("[TG " + LocalDateTime.now().format(dtf) + "] Ответ: " + response.body());
            return true;
        } catch (Exception e) {
            System.err.println("[TG] Ошибка отправки: " + e.getMessage());
            return false;
        }
    }

    // ----------------- SIGNAL MESSAGE -----------------
    public void sendSignal(String symbol, String direction, double confidence, double price, int rsi, String flags) {
        String message = String.format(
                "%s → %s\nConfidence: %.2f\nPrice: %.2f\nRSI: %d\nFlags: %s\nTime: %s",
                symbol, direction, confidence, price, rsi, flags, LocalDateTime.now().format(dtf)
        );
        sendMessage(message);
    }

    // ----------------- SIMPLE MESSAGE -----------------
    public void sendSignal(String message) {
        sendMessage(message);
    }
}
