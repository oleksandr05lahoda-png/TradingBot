package com.bot;

import java.net.URI;
import java.net.http.*;
import java.net.URLEncoder;
import java.time.Duration;

public class TelegramBotSender {

    private final String TOKEN;
    private final String CHAT_ID;
    private final HttpClient http;

    public TelegramBotSender() {
        this.TOKEN = System.getenv("TELEGRAM_TOKEN");
        this.CHAT_ID = System.getenv("TELEGRAM_CHAT_ID");

        if (TOKEN == null || TOKEN.isEmpty()) {
            System.out.println("[Telegram] ERROR: TELEGRAM_TOKEN is null or empty!");
        } else {
            System.out.println("[Telegram] Token loaded OK");
        }

        if (CHAT_ID == null || CHAT_ID.isEmpty()) {
            System.out.println("[Telegram] ERROR: TELEGRAM_CHAT_ID is null or empty!");
        } else {
            System.out.println("[Telegram] Chat ID loaded OK");
        }

        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public void sendSignal(String msg) {
        if (TOKEN == null || CHAT_ID == null) {
            System.out.println("[Telegram] Cannot send message, token or chat_id is null");
            return;
        }

        try {
            String encodedMsg = URLEncoder.encode(msg, "UTF-8");
            String url = String.format(
                    "https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s&parse_mode=Markdown",
                    TOKEN, CHAT_ID, encodedMsg
            );

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .timeout(Duration.ofSeconds(10))
                    .build();

            System.out.println("[Telegram] Sending message: " + msg);
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            System.out.println("[Telegram] Response code: " + resp.statusCode());
            System.out.println("[Telegram] Response body: " + resp.body());

            if (resp.statusCode() != 200) {
                System.out.println("[Telegram] Warning: message might not have been delivered!");
            }

        } catch (Exception e) {
            System.out.println("[Telegram] Exception sending message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Тестовое сообщение сразу при запуске
    public void testMessage() {
        sendSignal("✅ TelegramBotSender тестовое сообщение. Bot работает!");
    }
}
