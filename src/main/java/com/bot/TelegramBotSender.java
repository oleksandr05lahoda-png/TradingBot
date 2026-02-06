package com.bot;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TelegramBotSender {

    private final String token;
    private final String chatId;

    private final HttpClient client;
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(1000);
    private final ScheduledExecutorService sender;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private static final DateTimeFormatter DTF =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final int RATE_LIMIT_MS = 1200; // безопасно для TG
    private static final int MAX_RETRY = 3;

    public TelegramBotSender(String token, String chatId) {
        this.token = token;
        this.chatId = chatId;

        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        this.sender = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tg-sender");
            t.setDaemon(true);
            return t;
        });

        startWorker();
        startHeartbeat();
    }

    // ======================= PUBLIC API =======================
    public void sendMessageSync(String message) {
        try {

            HttpRequest req = buildRequest(message);

            HttpResponse<String> resp =
                    client.send(req, HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() == 200) {
                log("SYNC OK");
            } else {
                log("SYNC HTTP " + resp.statusCode());
            }

        } catch (Exception e) {
            log("SYNC ERROR: " + e.getMessage());
        }
    }

    public void send(String message) {
        if (!running.get()) return;
        if (!queue.offer(message)) {
            System.err.println("[TG] Очередь переполнена, сообщение отброшено");
        }
    }

    public void shutdown() {
        running.set(false);
        sender.shutdown();
    }

    // ======================= INTERNAL =======================

    private void startWorker() {
        sender.scheduleWithFixedDelay(() -> {
            try {
                String msg = queue.poll(2, TimeUnit.SECONDS);
                if (msg != null) {
                    sendWithRetry(msg);
                    Thread.sleep(RATE_LIMIT_MS);
                }
            } catch (Throwable t) {
                System.err.println("[TG] Критическая ошибка sender: " + t.getMessage());
                t.printStackTrace();
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    private void sendWithRetry(String message) {
        for (int i = 1; i <= MAX_RETRY; i++) {
            try {
                HttpRequest req = buildRequest(message);
                HttpResponse<String> resp =
                        client.send(req, HttpResponse.BodyHandlers.ofString());

                if (resp.statusCode() == 200) {
                    log("OK");
                    return;
                } else {
                    log("HTTP " + resp.statusCode());
                }

            } catch (Exception e) {
                log("Retry " + i + " failed: " + e.getMessage());
                sleep(800L * i);
            }
        }
    }

    private HttpRequest buildRequest(String message) throws Exception {
        String url = "https://api.telegram.org/bot" + token + "/sendMessage"
                + "?chat_id=" + chatId
                + "&text=" + URLEncoder.encode(message, StandardCharsets.UTF_8);

        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
    }

    // ======================= HEARTBEAT =======================

    private void startHeartbeat() {
        sender.scheduleAtFixedRate(() -> {
            send("🤖 Bot alive: " + LocalDateTime.now().format(DTF));
        }, 10, 30, TimeUnit.MINUTES);
    }

    // ======================= UTILS =======================

    private void log(String msg) {
        System.out.println("[TG " + LocalDateTime.now().format(DTF) + "] " + msg);
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {}
    }
}
