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

/**
 * Класс для безопасной отправки сообщений в Telegram.
 * Асинхронная очередь + Heartbeat + Retry.
 */
public final class TelegramBotSender {

    private final String token;
    private final String chatId;
    private final HttpClient client;
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(1000);
    private final ScheduledExecutorService sender;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
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

    /** Асинхронная отправка сообщения через очередь */
    public void sendMessageAsync(String message) {
        if (!running.get()) return;
        if (!queue.offer(message)) {
            log("[WARN] Очередь переполнена, сообщение отброшено");
        }
    }

    /** Синхронная отправка сообщения (блокирующая) */
    public void sendMessageSync(String message) {
        sendWithRetry(message);
    }

    /** Корректная остановка */
    public void shutdown() {
        running.set(false);
        sender.shutdown();
        try {
            if (!sender.awaitTermination(5, TimeUnit.SECONDS)) {
                sender.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            sender.shutdownNow();
        }
    }

    // ======================= INTERNAL =======================

    /** Поток обработки очереди сообщений */
    private void startWorker() {
        sender.scheduleWithFixedDelay(() -> {
            try {
                String msg = queue.poll(2, TimeUnit.SECONDS);
                if (msg != null) {
                    sendWithRetry(msg);
                    Thread.sleep(RATE_LIMIT_MS);
                }
            } catch (Throwable t) {
                log("[ERROR] Sender thread: " + t.getMessage());
                t.printStackTrace();
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    /** Отправка сообщения с повтором в случае ошибок */
    private void sendWithRetry(String message) {
        for (int i = 1; i <= MAX_RETRY; i++) {
            try {
                HttpRequest req = buildRequest(message);
                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());

                if (resp.statusCode() == 200) {
                    log("[OK] Message sent");
                    return;
                } else {
                    log("[WARN] HTTP " + resp.statusCode());
                }

            } catch (Exception e) {
                log("[Retry " + i + "] failed: " + e.getMessage());
                sleep(800L * i); // экспоненциальная задержка
            }
        }
        log("[ERROR] Failed to send message after " + MAX_RETRY + " attempts");
    }

    /** Создание запроса к Telegram API */
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

    /** Периодическое сообщение, что бот жив */
    private void startHeartbeat() {
        sender.scheduleAtFixedRate(() -> {
            sendMessageAsync("🤖 Bot alive: " + LocalDateTime.now().format(DTF));
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
