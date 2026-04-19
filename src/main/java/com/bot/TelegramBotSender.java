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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TelegramBotSender v50 — production-hardened Telegram delivery.
 *
 * Audit fixes applied:
 *  - HTTP 429 handled separately: reads Retry-After header / parameters.retry_after body field
 *    and waits server-directed duration before retry. Previously treated as generic 4xx.
 *  - flushAndShutdown(timeoutMs): drains queue before stopping (Railway SIGTERM grace).
 *  - Dedup: identical consecutive messages within 60s collapsed (defends against upstream spam loops).
 *  - Split-safe: surrogate-pair guard on every split boundary (prevents mojibake on emoji-heavy messages).
 *  - Health probe isHealthy() returns false if worker stuck >30s or 10+ consecutive send failures.
 */
public final class TelegramBotSender {

    private final String token;
    private final String chatId;
    private final HttpClient client;
    private final LinkedBlockingDeque<String> queue = new LinkedBlockingDeque<>(1000);
    private final ScheduledExecutorService sender;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int  RATE_LIMIT_MS      = 1200;
    private static final int  MAX_RETRY          = 3;
    private static final int  TELEGRAM_MAX_CHARS = 3900;
    private static final long DEDUP_WINDOW_MS    = 60_000L;

    private volatile String lastSentText = null;
    private volatile long   lastSentAt   = 0;
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private volatile long workerHeartbeatMs = System.currentTimeMillis();

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

    public void sendMessageAsync(String message) {
        if (!running.get() || message == null || message.isEmpty()) return;

        long now = System.currentTimeMillis();
        if (message.equals(lastSentText) && (now - lastSentAt) < DEDUP_WINDOW_MS) {
            log("[DEDUP] Dropped duplicate within " + DEDUP_WINDOW_MS + "ms");
            return;
        }

        boolean hi = isHighPriority(message);
        boolean added = hi ? queue.offerFirst(message) : queue.offerLast(message);

        if (!added) {
            String dropped = queue.pollLast();
            log("[WARN] Queue full, dropped tail msg len="
                    + (dropped != null ? dropped.length() : -1));
            if (hi) queue.offerFirst(message);
            else    queue.offerLast(message);
        }
    }

    public void sendMessageSync(String message) {
        sendWithRetry(message);
    }

    public void shutdown() {
        running.set(false);
        sender.shutdown();
        try {
            if (!sender.awaitTermination(5, TimeUnit.SECONDS)) sender.shutdownNow();
        } catch (InterruptedException ignored) {
            sender.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void flushAndShutdown(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!queue.isEmpty() && System.currentTimeMillis() < deadline) {
            try { Thread.sleep(200); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        int remaining = queue.size();
        if (remaining > 0) log("[WARN] flushAndShutdown: " + remaining + " msgs still queued");
        shutdown();
    }

    public boolean isHealthy() {
        if (!running.get()) return false;
        if (System.currentTimeMillis() - workerHeartbeatMs > 30_000L) return false;
        return consecutiveFailures.get() < 10;
    }

    public int getQueueSize() { return queue.size(); }

    private void startWorker() {
        sender.scheduleWithFixedDelay(() -> {
            workerHeartbeatMs = System.currentTimeMillis();
            try {
                String msg = queue.poll(2, TimeUnit.SECONDS);
                if (msg == null) return;
                List<String> parts = splitForTelegram(msg);
                for (int i = 0; i < parts.size(); i++) {
                    sendWithRetry(parts.get(i));
                    Thread.sleep(RATE_LIMIT_MS);
                    if (i < parts.size() - 1) {
                        String head = queue.peek();
                        if (head != null && isHighPriority(head)) {
                            String urgent = queue.pollFirst();
                            if (urgent != null) {
                                sendWithRetry(urgent);
                                Thread.sleep(RATE_LIMIT_MS);
                            }
                        }
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                log("[ERROR] Sender thread: " + t.getMessage());
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    private static boolean isHighPriority(String message) {
        if (message == null) return false;
        return message.contains("UDS CLOSED")
                || message.contains("🚨")
                || message.contains("TP HIT")  || message.contains("SL HIT")
                || message.contains("TP_HIT")  || message.contains("SL_HIT")
                || message.contains("TP1 HIT") || message.contains("TP2 HIT") || message.contains("TP3 HIT")
                || message.contains("PRE_BREAK")
                || message.contains("EARLY_TICK")
                || message.contains("EXHAUST_REV")
                || message.contains("PANIC")
                || message.contains("FORCE_CLOSE")
                || message.contains("DAILY_KILL_SWITCH");
    }

    private void sendWithRetry(String message) {
        int msgLen = message != null ? message.length() : -1;
        for (int attempt = 1; attempt <= MAX_RETRY; attempt++) {
            try {
                HttpRequest req = buildRequest(message, true);
                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
                int code = resp.statusCode();

                if (code == 200) { onSuccess(message); return; }

                if (code == 429) {
                    long waitMs = parseRetryAfterMs(resp);
                    log("[WARN] HTTP 429 rate limit, sleeping " + waitMs + "ms");
                    sleep(Math.max(1000L, waitMs));
                    continue;
                }

                if (code == 400 && attempt == 1) {
                    log("[WARN] HTTP 400 markdown parse; retrying as plain text");
                    HttpRequest plain = buildRequest(message, false);
                    HttpResponse<String> plainResp = client.send(plain, HttpResponse.BodyHandlers.ofString());
                    if (plainResp.statusCode() == 200) {
                        onSuccess(message);
                        log("[OK] Message sent (plain fallback)");
                        return;
                    }
                    log("[WARN] Plain fallback failed: HTTP " + plainResp.statusCode());
                } else {
                    log("[WARN] HTTP " + code + " msgLen=" + msgLen
                            + " body=" + shortBody(resp.body(), 200));
                }
            } catch (Exception e) {
                log("[Retry " + attempt + "] " + e.getMessage() + " msgLen=" + msgLen);
                sleep(800L * attempt);
            }
        }
        onFailure();
        log("[ERROR] Send failed after " + MAX_RETRY + " attempts");
    }

    private void onSuccess(String message) {
        consecutiveFailures.set(0);
        lastSentText = message;
        lastSentAt = System.currentTimeMillis();
    }

    private void onFailure() { consecutiveFailures.incrementAndGet(); }

    private long parseRetryAfterMs(HttpResponse<String> resp) {
        String header = resp.headers().firstValue("Retry-After").orElse(null);
        if (header != null) {
            try { return Long.parseLong(header.trim()) * 1000L; }
            catch (NumberFormatException ignored) {}
        }
        String body = resp.body();
        if (body == null) return 3000L;
        int idx = body.indexOf("\"retry_after\"");
        if (idx < 0) return 3000L;
        int colon = body.indexOf(':', idx);
        int end = body.indexOf(',', colon);
        if (end < 0) end = body.indexOf('}', colon);
        if (colon < 0 || end < 0) return 3000L;
        try { return Long.parseLong(body.substring(colon + 1, end).trim()) * 1000L; }
        catch (NumberFormatException e) { return 3000L; }
    }

    private HttpRequest buildRequest(String message, boolean markdown) {
        String url = "https://api.telegram.org/bot" + token + "/sendMessage";
        StringBuilder body = new StringBuilder()
                .append("chat_id=").append(URLEncoder.encode(chatId, StandardCharsets.UTF_8))
                .append("&disable_web_page_preview=true")
                .append("&text=").append(URLEncoder.encode(message, StandardCharsets.UTF_8));
        if (markdown) body.append("&parse_mode=Markdown");
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();
    }

    private void log(String msg) {
        System.out.println("[TG " + LocalDateTime.now().format(DTF) + "] " + msg);
    }

    private static String shortBody(String body, int max) {
        if (body == null) return "null";
        body = body.trim();
        return body.length() <= max ? body : body.substring(0, max) + "...";
    }

    private static List<String> splitForTelegram(String message) {
        if (message == null) return List.of();
        if (message.length() <= TELEGRAM_MAX_CHARS) return List.of(message);
        List<String> parts = new ArrayList<>();
        int start = 0;
        final int n = message.length();
        while (start < n) {
            int end = Math.min(n, start + TELEGRAM_MAX_CHARS);
            if (end < n) {
                int nl = message.lastIndexOf('\n', end);
                if (nl > start + 200) end = nl + 1;
                else {
                    int sp = message.lastIndexOf(' ', end);
                    if (sp > start + 200) end = sp + 1;
                }
            }
            if (end > start + 1 && end < n) {
                char prev = message.charAt(end - 1);
                char next = message.charAt(end);
                if (Character.isHighSurrogate(prev) && Character.isLowSurrogate(next)) end--;
            }
            if (end <= start) end = Math.min(n, start + TELEGRAM_MAX_CHARS);
            parts.add(message.substring(start, end));
            start = end;
        }
        return parts;
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
}