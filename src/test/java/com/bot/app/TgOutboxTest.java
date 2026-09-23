package com.bot.app;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The trade lines' own sender. The loop only offers; retries and failures happen here, bounded,
 * and nothing a dead Telegram does can grow the heap or reach the caller.
 */
class TgOutboxTest {

    private final List<Long> sleeps = new ArrayList<>();

    @Test
    @DisplayName("a refused line is tried three times with growing pauses, then succeeds")
    void retriesThenDelivers() {
        AtomicInteger calls = new AtomicInteger();
        TgOutbox box = new TgOutbox(html -> TgOutbox.Attempt.of(calls.incrementAndGet() >= 3), sleeps::add);
        box.offer("✅ Тейк");

        assertEquals(1, box.drainNow());
        assertEquals(3, calls.get());
        assertEquals(List.of(2_000L, 4_000L), sleeps);
        assertEquals(0, box.pending());
    }

    @Test
    @DisplayName("a delivery that throws is given up after three tries, and the count is released")
    void throwingDeliveryIsContained() {
        AtomicInteger calls = new AtomicInteger();
        TgOutbox box = new TgOutbox(html -> {
            calls.incrementAndGet();
            throw new IllegalStateException("network");
        }, sleeps::add);
        box.offer("🛑 Стоп");
        box.offer("✅ Тейк");

        assertEquals(2, box.drainNow());
        assertEquals(6, calls.get());
        assertEquals(0, box.pending(), "a shutdown flush must not wait on lines already given up");
    }

    @Test
    @DisplayName("flood control: a 429 with retry_after=7 is waited out for 7 s and the line delivered")
    void floodControlWaitsTelegramsOwnPause() {
        AtomicInteger calls = new AtomicInteger();
        TgOutbox box = new TgOutbox(html -> calls.incrementAndGet() == 1
                ? new TgOutbox.Attempt(false, 429, "{\"ok\":false,\"parameters\":{\"retry_after\":7}}")
                : TgOutbox.Attempt.DELIVERED, sleeps::add);
        box.offer("🧯 ADA");

        box.drainNow();
        assertEquals(2, calls.get());
        assertEquals(List.of(7_000L), sleeps);
        assertEquals(0, box.pending());
    }

    @Test
    @DisplayName("flood control earns one extra try beyond the usual three")
    void floodControlAddsOneAttempt() {
        AtomicInteger calls = new AtomicInteger();
        TgOutbox box = new TgOutbox(html -> {
            calls.incrementAndGet();
            return new TgOutbox.Attempt(false, 429, "{\"parameters\":{\"retry_after\":12}}");
        }, sleeps::add);
        box.offer("🧯 ADA");

        box.drainNow();
        assertEquals(TgOutbox.ATTEMPTS + 1, calls.get());
        assertEquals(List.of(12_000L, 12_000L, 12_000L), sleeps);
    }

    @Test
    @DisplayName("401/403/404 is the token or the chat: one call, no pauses")
    void refusedForGoodIsNotRetried() {
        AtomicInteger calls = new AtomicInteger();
        TgOutbox box = new TgOutbox(html -> {
            calls.incrementAndGet();
            return new TgOutbox.Attempt(false, 403, "{\"description\":\"Forbidden: bot was blocked by the user\"}");
        }, sleeps::add);
        box.offer("✅ Тейк");

        box.drainNow();
        assertEquals(1, calls.get());
        assertEquals(List.of(), sleeps);
        assertEquals(0, box.pending());
    }

    @Test
    @DisplayName("a full queue drops the newest line instead of blocking the loop")
    void boundedQueue() {
        List<String> delivered = new ArrayList<>();
        TgOutbox box = new TgOutbox(html -> TgOutbox.Attempt.of(delivered.add(html)), sleeps::add);
        for (int i = 0; i < TgOutbox.CAPACITY + 10; i++) box.offer("line " + i);
        box.offer("");
        box.offer(null);

        assertEquals(TgOutbox.CAPACITY, box.pending());
        assertEquals(TgOutbox.CAPACITY, box.drainNow());
        assertEquals("line 0", delivered.get(0));
        assertEquals(0, box.pending());
    }
}
