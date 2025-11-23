package com.bot;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;

public class TelegramBotSender {
    private final String TOKEN = System.getenv("TELEGRAM_TOKEN");
    private final String CHAT_ID = System.getenv("TELEGRAM_CHAT_ID");
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    public void sendSignal(String msg) {
        try {
            String url = String.format(
                    "https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s&parse_mode=Markdown",
                    TOKEN, CHAT_ID, msg.replace(" ", "%20")
            );
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).GET().timeout(Duration.ofSeconds(10)).build();
            http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.out.println("[Telegram] Error: " + e.getMessage());
        }
    }
}
