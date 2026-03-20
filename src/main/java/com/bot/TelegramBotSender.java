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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TelegramBotSender {

    private final String token;
    private final String chatId;
    private final HttpClient client;
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(1000);
    private final ScheduledExecutorService sender;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("HH:mm:ss");    private static final int RATE_LIMIT_MS = 1200; // безопасно для TG
    private static final int MAX_RETRY = 3;
    // Hard limit for Telegram `sendMessage` is 4096 UTF-16 code units.
    // Use a smaller value to avoid edge cases (URL encoding expansion, newlines, emojis).
    private static final int TELEGRAM_MAX_CHARS = 3900;

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
    }

    // ======================= PUBLIC API =======================

    /** Асинхронная отправка сообщения через очередь */
    public void sendMessageAsync(String message) {
        if (!running.get()) return;
        if (!queue.offer(message)) {
            // If queue is full, drop one *oldest pending* message and enqueue the new one.
            // This prevents "silent missing signals" when the bot is busy.
            String dropped = queue.poll();
            log("[WARN] Очередь переполнена. dropOldestLen="
                    + (dropped != null ? dropped.length() : -1)
                    + " msgLen=" + (message != null ? message.length() : -1));
            if (!queue.offer(message)) {
                log("[WARN] Очередь переполнена снова. Сообщение отброшено msgLen="
                        + (message != null ? message.length() : -1));
            }
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
                    List<String> parts = splitForTelegram(msg);
                    for (int i = 0; i < parts.size(); i++) {
                        // Keep original text; just chunk it to fit Telegram limits.
                        sendWithRetry(parts.get(i));
                        Thread.sleep(RATE_LIMIT_MS);
                    }
                }
            } catch (Throwable t) {
                log("[ERROR] Sender thread: " + t.getMessage());
                t.printStackTrace();
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    /** Отправка сообщения с повтором в случае ошибок */
    private void sendWithRetry(String message) {
        int msgLen = message != null ? message.length() : -1;
        for (int i = 1; i <= MAX_RETRY; i++) {
            try {
                HttpRequest req = buildRequest(message);
                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());

                if (resp.statusCode() == 200) {
                    log("[OK] Message sent");
                    return;
                } else {
                    String body = resp.body();
                    log("[WARN] HTTP " + resp.statusCode()
                            + " msgLen=" + msgLen
                            + " body=" + shortBody(body, 220));
                }

            } catch (Exception e) {
                log("[Retry " + i + "] failed: " + e.getMessage()
                        + " msgLen=" + msgLen);
                sleep(800L * i); // экспоненциальная задержка
            }
        }
        log("[ERROR] Failed to send message after " + MAX_RETRY + " attempts");
    }

    /** Создание запроса к Telegram API */
    private HttpRequest buildRequest(String message) throws Exception {
        // POST is more robust than GET for long messages (avoids URL length limits).
        String url = "https://api.telegram.org/bot" + token + "/sendMessage";
        String body = "chat_id=" + URLEncoder.encode(chatId, StandardCharsets.UTF_8)
                + "&text=" + URLEncoder.encode(message, StandardCharsets.UTF_8);

        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private void log(String msg) {
        System.out.println("[TG " + LocalDateTime.now().format(DTF) + "] " + msg);
    }

    private static String shortBody(String body, int max) {
        if (body == null) return "null";
        body = body.trim();
        if (body.length() <= max) return body;
        return body.substring(0, max) + "...";
    }

    /**
     * Split long Telegram messages into chunks to avoid HTTP 400 ("message is too long").
     * Also tries to split on newline/space boundaries for readability.
     */
    private static List<String> splitForTelegram(String message) {
        if (message == null) return List.of();
        if (message.length() <= TELEGRAM_MAX_CHARS) return List.of(message);

        List<String> parts = new ArrayList<>();
        int start = 0;
        while (start < message.length()) {
            int end = Math.min(message.length(), start + TELEGRAM_MAX_CHARS);

            if (end < message.length()) {
                int nl = message.lastIndexOf('\n', end);
                if (nl > start + 200) {
                    end = nl + 1; // include newline
                } else {
                    int sp = message.lastIndexOf(' ', end);
                    if (sp > start + 200) end = sp + 1;
                }
            }

            // Avoid splitting surrogate pairs (emojis) in half.
            if (end > start + 1 && end < message.length()) {
                char prev = message.charAt(end - 1);
                char next = message.charAt(end);
                if (Character.isHighSurrogate(prev) && Character.isLowSurrogate(next)) {
                    end--;
                }
            }

            if (end <= start) end = Math.min(message.length(), start + TELEGRAM_MAX_CHARS);
            parts.add(message.substring(start, end));
            start = end;
        }
        return parts;
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {}
    }
}