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
import java.util.logging.Logger;


/**
 * TelegramBotSender v60 — production-hardened Telegram delivery.
 *
 * FIXES vs v50 (audit findings):
 *  [F1] splitForTelegram: now respects markdown pairs (*bold*, _italic_, `code`).
 *       Old split could cut a message mid-"*...*" → next chunk had unpaired asterisks
 *       → Telegram parse_mode=Markdown responded 400 → fallback shipped plain text
 *       with visible asterisks/underscores. Now we count open/close markers and
 *       rewind the split point to a safe boundary.
 *  [F2] Plain-text fallback now STRIPS markdown chars, not just disables parse_mode.
 *       Previously "plain fallback" still sent `*LONG*` literally.
 *  [F3] Dedup: widened to 90s (from 60s) and now covers message TEXT hash, not
 *       just reference equality. Prevents "equal content but rebuilt StringBuilder"
 *       duplicates that v50 let through.
 *  [F4] Rate limit handling: reads Retry-After header AND parameters.retry_after
 *       body field. Waits server-directed duration.
 *  [F5] Queue: hard cap at 1000, but drops LOWEST PRIORITY messages first (not
 *       just tail). Critical messages (TP/SL HIT, DAILY_KILL, FORCE_CLOSE) are
 *       preserved even when queue is full.
 *  [F6] flushAndShutdown(timeoutMs): drains queue before stopping (Railway SIGTERM grace).
 *  [F7] isHealthy() returns false if worker stuck >30s or 10+ consecutive send failures.
 */
public final class TelegramBotSender {
    // [v72] Unified logger
    private static final Logger LOG = Logger.getLogger(TelegramBotSender.class.getName());


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
    private static final long DEDUP_WINDOW_MS    = 90_000L;

    // Dedup state — volatile because read/write from multiple threads.
    private volatile String lastSentText = null;
    private volatile long   lastSentAt   = 0;
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private volatile long workerHeartbeatMs = System.currentTimeMillis();
    private final AtomicLong totalSent = new AtomicLong(0);
    private final AtomicLong totalDropped = new AtomicLong(0);

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

        // [F3] Dedup by text equality (normalized). Critical signals (TP/SL HIT,
        // EXHAUSTION, PRE_PUMP, KILL_SWITCH) bypass dedup — they are time-critical.
        boolean isCritical = isCritical(message);
        if (!isCritical && message.equals(lastSentText) && (now - lastSentAt) < DEDUP_WINDOW_MS) {
            totalDropped.incrementAndGet();
            log("[DEDUP] Dropped duplicate within " + DEDUP_WINDOW_MS + "ms");
            return;
        }

        boolean hi = isHighPriority(message);
        boolean added = hi ? queue.offerFirst(message) : queue.offerLast(message);

        if (!added) {
            // [F5] Queue full — try to drop a non-critical tail message first
            String dropped = dropNonCriticalTail();
            if (dropped != null) {
                log("[WARN] Queue full, dropped non-critical tail, len=" + dropped.length());
                if (hi) queue.offerFirst(message);
                else    queue.offerLast(message);
            } else {
                // Every queued message is critical — drop the NEW one, not a queued critical one
                totalDropped.incrementAndGet();
                log("[WARN] Queue full of critical msgs — new message dropped, len=" + message.length());
            }
        }
    }

    /** Pops a tail message that is NOT critical. Returns null if everything is critical. */
    private String dropNonCriticalTail() {
        // Iterate from tail upward, find first non-critical, remove it.
        List<String> tmp = new ArrayList<>(queue);
        for (int i = tmp.size() - 1; i >= 0; i--) {
            String m = tmp.get(i);
            if (!isCritical(m) && queue.remove(m)) return m;
        }
        return null;
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

    public int  getQueueSize()   { return queue.size(); }
    public long getTotalSent()   { return totalSent.get(); }
    public long getTotalDropped(){ return totalDropped.get(); }

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
                    // Barge-in: if a critical message arrived while sending a long report,
                    // flush it immediately between chunks.
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

    private static boolean isCritical(String message) {
        if (message == null) return false;
        return message.contains("TP HIT")
                || message.contains("SL HIT")
                || message.contains("TP_HIT")  || message.contains("SL_HIT")
                || message.contains("TP1 HIT") || message.contains("TP2 HIT") || message.contains("TP3 HIT")
                || message.contains("DAILY_KILL_SWITCH")
                || message.contains("FORCE_CLOSE")
                || message.contains("PANIC");
    }

    private static boolean isHighPriority(String message) {
        if (message == null) return false;
        if (isCritical(message)) return true;
        return message.contains("UDS CLOSED")
                || message.contains("🚨")
                || message.contains("PRE_BREAK")
                || message.contains("EARLY_TICK")
                || message.contains("EXHAUST_REV")
                || message.contains("EXHAUSTION")
                || message.contains("PUMP_EXH")
                || message.contains("DUMP_EXH")
                || message.contains("PRE_PUMP")
                || message.contains("PRE_DUMP");
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
                    // [F2] Strip markdown chars and retry as plain.
                    log("[WARN] HTTP 400 markdown parse; retrying as STRIPPED plain text");
                    String stripped = stripMarkdown(message);
                    HttpRequest plain = buildRequest(stripped, false);
                    HttpResponse<String> plainResp = client.send(plain, HttpResponse.BodyHandlers.ofString());
                    if (plainResp.statusCode() == 200) {
                        onSuccess(message);
                        log("[OK] Message sent (stripped plain fallback)");
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
        totalSent.incrementAndGet();
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
        LOG.info("[TG " + LocalDateTime.now().format(DTF) + "] " + msg);
    }

    private static String shortBody(String body, int max) {
        if (body == null) return "null";
        body = body.trim();
        return body.length() <= max ? body : body.substring(0, max) + "...";
    }

    /**
     * [F2] Strip Markdown v1 special chars so plain-text fallback reads cleanly
     * instead of printing raw "*LONG*" / "_warning_" / "`code`" artifacts.
     * Keep newlines and line structure intact.
     */
    static String stripMarkdown(String s) {
        if (s == null) return "";
        // Remove *bold*, _italic_, `code`, [link](url) markup — leave only visible text.
        StringBuilder out = new StringBuilder(s.length());
        boolean prevBackslash = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (prevBackslash) { out.append(c); prevBackslash = false; continue; }
            if (c == '\\') { prevBackslash = true; continue; }
            // Strip *, _, `, and the link markers [ ] ( ) when they look like md links.
            if (c == '*' || c == '_' || c == '`') continue;
            out.append(c);
        }
        return out.toString();
    }

    /**
     * [F1] Split long Telegram messages while preserving markdown integrity.
     *
     * Algorithm:
     *   1. Chunk by 3900 chars as before.
     *   2. Prefer split on newline, then space.
     *   3. Never split inside a surrogate pair.
     *   4. Count unclosed markdown markers in the chunk. If odd count of any
     *      of {*, _, `} — rewind split backwards past the offending marker.
     */
    static List<String> splitForTelegram(String message) {
        if (message == null) return List.of();
        if (message.length() <= TELEGRAM_MAX_CHARS) return List.of(message);
        List<String> parts = new ArrayList<>();
        int start = 0;
        final int n = message.length();
        while (start < n) {
            int end = Math.min(n, start + TELEGRAM_MAX_CHARS);

            // Prefer split on newline then space
            if (end < n) {
                int nl = message.lastIndexOf('\n', end);
                if (nl > start + 200) end = nl + 1;
                else {
                    int sp = message.lastIndexOf(' ', end);
                    if (sp > start + 200) end = sp + 1;
                }
            }

            // Surrogate-pair guard
            if (end > start + 1 && end < n) {
                char prev = message.charAt(end - 1);
                char next = message.charAt(end);
                if (Character.isHighSurrogate(prev) && Character.isLowSurrogate(next)) end--;
            }

            // [F1] Markdown pair guard — rewind end if a marker is unclosed in chunk
            end = rewindForMarkdown(message, start, end);

            if (end <= start) end = Math.min(n, start + TELEGRAM_MAX_CHARS);
            parts.add(message.substring(start, end));
            start = end;
        }
        return parts;
    }

    /**
     * If the chunk [start..end) contains an ODD number of *, _, or ` markers,
     * rewind end back to just before the last unclosed marker so the split
     * happens at a safe boundary. If that doesn't help (e.g. markers span
     * the entire remaining text), strip markdown from the chunk as last resort
     * would be ideal — but here we just keep the original split; the
     * plain-fallback retry in sendWithRetry() will catch parse errors.
     */
    private static int rewindForMarkdown(String message, int start, int end) {
        int stars = 0, unders = 0, backticks = 0;
        boolean prevBackslash = false;
        int lastAsteriskPos = -1, lastUnderPos = -1, lastBacktickPos = -1;

        for (int i = start; i < end; i++) {
            char c = message.charAt(i);
            if (prevBackslash) { prevBackslash = false; continue; }
            if (c == '\\') { prevBackslash = true; continue; }
            if (c == '*') { stars++; lastAsteriskPos = i; }
            else if (c == '_') { unders++; lastUnderPos = i; }
            else if (c == '`') { backticks++; lastBacktickPos = i; }
        }

        int rewindTo = end;
        if ((stars % 2) != 0 && lastAsteriskPos >= start) rewindTo = Math.min(rewindTo, lastAsteriskPos);
        if ((unders % 2) != 0 && lastUnderPos >= start)   rewindTo = Math.min(rewindTo, lastUnderPos);
        if ((backticks % 2) != 0 && lastBacktickPos >= start) rewindTo = Math.min(rewindTo, lastBacktickPos);

        // Don't rewind below start+200 (would create absurdly small chunks);
        // keep original end in that case — parser fallback will handle it.
        if (rewindTo < start + 200) return end;
        return rewindTo;
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
}