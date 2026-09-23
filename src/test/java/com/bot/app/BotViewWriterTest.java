package com.bot.app;

import com.bot.core.Side;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * bot_view.json, the file the lab bot's 📊 Панель reads (BOT VIEW CONTRACT v1, 24.09): exactly the
 * contract's fields, null for what the loop did not know, replaced atomically, at most every 30 s,
 * and a disk that fails or stalls never reaches the trading loop.
 */
class BotViewWriterTest {

    private static final Instant AT = Instant.parse("2026-09-24T08:15:02Z");

    /** The contract's own example: a tripped day, one long, the wallet and the exchange's net P&L. */
    static OperatorSnapshot fixture() {
        OperatorSnapshot.Position tia = new OperatorSnapshot.Position("TIAUSDT", Side.LONG, 11.0, 0.5075, 0.5042,
                -0.0363, 0.4418, 0.6224, Instant.parse("2026-09-23T14:05:00Z"), true);
        return new OperatorSnapshot(AT, Instant.parse("2026-09-23T19:44:23Z"), "20260923-194423Z", false, true,
                0L, false, false, 0, 0, Instant.parse("2026-09-24T08:15:01Z"),
                new OperatorSnapshot.Account(141.62, 120.10, 0.18, Instant.parse("2026-09-24T08:15:01Z")),
                List.of(tia), true,
                new OperatorSnapshot.Pnl(-2.23, 1.10, 3.05, Instant.parse("2026-09-24T08:14:30Z"), null),
                List.of(),
                new OperatorSnapshot.KillSwitch(true, 0.03, 145.86, 0.0337, Instant.parse("2026-09-25T00:00:00Z")));
    }

    /** The same moment before the first exchange read: nothing known, nothing invented. */
    private static OperatorSnapshot blank() {
        return new OperatorSnapshot(AT, AT.minusSeconds(20), null, false, false, 0L, false, false, 0, 0, null,
                null, List.of(), false, null, List.of());
    }

    private static final class MovableClock extends Clock {
        final AtomicReference<Instant> now = new AtomicReference<>(AT);
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now.get(); }
        void plus(Duration d) { now.set(now.get().plus(d)); }
    }

    private final List<LogRecord> logged = Collections.synchronizedList(new ArrayList<>());
    private final Handler capture = new Handler() {
        @Override public void publish(LogRecord r) { logged.add(r); }
        @Override public void flush() {}
        @Override public void close() {}
    };
    private final Logger log = Logger.getLogger(BotViewWriter.class.getName());

    @BeforeEach void captureLog() { log.addHandler(capture); }
    @AfterEach void releaseLog() { log.removeHandler(capture); }

    private long warnings() {
        synchronized (logged) {
            return logged.stream().filter(r -> r.getLevel() == Level.WARNING).count();
        }
    }

    // ─── The contract ────────────────────────────────────────────────────────────────────────

    /** Pasted into the hand-off: the file a tripped morning with one long produces, byte for byte. */
    static final String FIXTURE_JSON = "{\"v\":1,\"ts\":\"2026-09-24T08:15:02Z\",\"build\":\"20260923-194423Z\","
            + "\"started_at\":\"2026-09-23T19:44:23Z\",\"state\":\"kill_switch\","
            + "\"state_line\":\"⏸ Дневной лимит убытка −3.4% — входов нет\",\"halt_reason\":null,"
            + "\"account\":{\"wallet\":141.62,\"margin_balance\":141.8,\"available\":120.1,\"unrealized\":0.18,"
            + "\"at\":\"2026-09-24T08:15:01Z\"},"
            + "\"pnl\":{\"today\":-2.23,\"d7\":1.1,\"d30\":3.05,\"net\":true,\"at\":\"2026-09-24T08:14:30Z\",\"stale\":false},"
            + "\"kill_switch\":{\"tripped\":true,\"limit_frac\":0.03,\"day_start_balance\":145.86,\"day_loss_frac\":0.0337,"
            + "\"resumes_at\":\"2026-09-25T00:00:00Z\"},"
            + "\"positions\":[{\"symbol\":\"TIAUSDT\",\"side\":\"LONG\",\"qty\":11,\"entry\":0.5075,\"mark\":0.5042,"
            + "\"pnl_usd\":-0.04,\"pnl_pct\":-0.65,\"stop\":0.4418,\"take\":0.6224,\"to_stop_pct\":-12.38,"
            + "\"to_take_pct\":23.44,\"opened_at\":\"2026-09-23T14:05:00Z\",\"risk_usd\":0.72}],"
            + "\"pending_closes\":0,\"last_reconcile_ok\":\"2026-09-24T08:15:01Z\"}\n";

    @Test
    @DisplayName("the fixture renders to exactly the contract's fields, in its order, with its values")
    void rendersTheContract() {
        String body = BotViewWriter.render(fixture(), Optional.empty(), AT);
        assertEquals(FIXTURE_JSON, body);
        JSONObject root = new JSONObject(body);
        assertEquals(Set.of("v", "ts", "build", "started_at", "state", "state_line", "halt_reason", "account", "pnl",
                "kill_switch", "positions", "pending_closes", "last_reconcile_ok"), root.keySet(),
                "no field outside the contract - nothing a token or key could ride in on");
        assertEquals(Set.of("wallet", "margin_balance", "available", "unrealized", "at"),
                root.getJSONObject("account").keySet());
        assertEquals(Set.of("today", "d7", "d30", "net", "at", "stale"), root.getJSONObject("pnl").keySet());
        assertEquals(Set.of("tripped", "limit_frac", "day_start_balance", "day_loss_frac", "resumes_at"),
                root.getJSONObject("kill_switch").keySet());
        assertEquals(Set.of("symbol", "side", "qty", "entry", "mark", "pnl_usd", "pnl_pct", "stop", "take",
                "to_stop_pct", "to_take_pct", "opened_at", "risk_usd"),
                root.getJSONArray("positions").getJSONObject(0).keySet());
    }

    @Test
    @DisplayName("state and state_line are /status's own headline, as plain text")
    void stateIsTheStatusHeadline() {
        OperatorSnapshot s = fixture();
        JSONObject halted = new JSONObject(BotViewWriter.render(s, Optional.of("operator via <b>Telegram</b>"), AT));
        assertEquals("halt", halted.getString("state"));
        assertEquals(TgFormat.plain(OperatorViews.stateLine(s, Optional.of("operator via <b>Telegram</b>"), AT)),
                halted.getString("state_line"));
        assertEquals("⏸ Халт: operator via <b>Telegram</b>", halted.getString("state_line"),
                "plain text: the screen's markup gone, the reason's own text kept as typed");
        assertEquals("operator via <b>Telegram</b>", halted.getString("halt_reason"), "the reason as given");

        JSONObject silent = new JSONObject(BotViewWriter.render(s, Optional.empty(), AT.plusSeconds(600)));
        assertEquals("loop_silent", silent.getString("state"), "rendered late, the file says the loop is quiet");
        assertEquals("starting", OperatorViews.stateCode(null, Optional.empty(), AT));
    }

    @Test
    @DisplayName("unknown numbers are null, never 0: no read yet, no P&L yet, no stop, no mark")
    void unknownIsNull() {
        JSONObject root = new JSONObject(BotViewWriter.render(blank(), Optional.empty(), AT));
        assertTrue(root.isNull("account"));
        assertTrue(root.isNull("pnl"));
        assertTrue(root.isNull("kill_switch"));
        assertTrue(root.isNull("last_reconcile_ok"));
        assertTrue(root.isNull("halt_reason"));
        assertEquals("unknown", root.getString("build"));
        assertEquals(0, root.getJSONArray("positions").length());
        assertEquals("trading", root.getString("state"));

        // The book alone (no exchange read): no mark, no P&L; no stop on record: no risk either.
        OperatorSnapshot.Position bare = new OperatorSnapshot.Position("ADAUSDT", Side.SHORT, 5, 0.6, Double.NaN,
                Double.NaN, Double.NaN, Double.NaN, null, false);
        OperatorSnapshot withBare = new OperatorSnapshot(AT, AT, "b", false, false, 0L, false, false, 0, 1, AT,
                null, List.of(bare), false, new OperatorSnapshot.Pnl(Double.NaN, Double.NaN, Double.NaN, null, AT),
                List.of(), new OperatorSnapshot.KillSwitch(false, 0.03, Double.NaN, Double.NaN, null));
        JSONObject r2 = new JSONObject(BotViewWriter.render(withBare, Optional.empty(), AT));
        JSONObject p = r2.getJSONArray("positions").getJSONObject(0);
        assertEquals("SHORT", p.getString("side"));
        for (String k : List.of("mark", "pnl_usd", "pnl_pct", "stop", "take", "to_stop_pct", "to_take_pct",
                "opened_at", "risk_usd")) {
            assertTrue(p.isNull(k), k + " unknown must be null: " + p);
        }
        assertTrue(r2.isNull("pnl"), "a probe that never succeeded is no P&L, not $0");
        JSONObject ks = r2.getJSONObject("kill_switch");
        assertFalse(ks.getBoolean("tripped"));
        assertTrue(ks.isNull("day_start_balance"));
        assertTrue(ks.isNull("day_loss_frac"));
        assertTrue(ks.isNull("resumes_at"));
        assertEquals(1, r2.getInt("pending_closes"));
    }

    @Test
    @DisplayName("a P&L read that failed after a good one is the good one, marked stale")
    void staleProbe() {
        OperatorSnapshot s = fixture();
        OperatorSnapshot stale = new OperatorSnapshot(s.at(), s.startedAt(), s.buildStamp(), false, false, 0L,
                false, false, 0, 0, s.lastReconcileOkAt(), s.account(), s.positions(), true,
                new OperatorSnapshot.Pnl(-2.23, 1.10, 3.05, Instant.parse("2026-09-24T07:00:00Z"), AT), List.of(), null);
        JSONObject pnl = new JSONObject(BotViewWriter.render(stale, Optional.empty(), AT)).getJSONObject("pnl");
        assertTrue(pnl.getBoolean("stale"));
        assertEquals("2026-09-24T07:00:00Z", pnl.getString("at"));
        assertEquals(-2.23, pnl.getDouble("today"));
    }

    @Test
    @DisplayName("a halt reason quoting a whole error page is one line, clipped, and valid JSON")
    void longReasonClipped() {
        String reason = "drift \"quoted\"\n<html>" + "x".repeat(1000);
        JSONObject root = new JSONObject(BotViewWriter.render(fixture(), Optional.of(reason), AT));
        String r = root.getString("halt_reason");
        assertEquals(BotViewWriter.MAX_REASON_CHARS, r.length());
        assertFalse(r.contains("\n"));
        assertTrue(r.startsWith("drift \"quoted\" <html>"), r);
    }

    @Test
    @DisplayName("the clip never splits an emoji: a halt with 🔴 at the cut is still written as UTF-8")
    void clipKeepsSurrogatePairsWhole(@TempDir Path dir) throws Exception {
        // 298 x's, then U+1F534 across the old cut at 299: half a pair could not be encoded, and
        // every write failed for as long as the halt stood.
        String reason = "x".repeat(298) + "🔴" + "y".repeat(50);
        String clipped = BotViewWriter.clip(reason);
        assertTrue(clipped.length() <= BotViewWriter.MAX_REASON_CHARS);
        assertTrue(clipped.endsWith("…"));
        for (int i = 0; i < clipped.length(); i++) {
            char c = clipped.charAt(i);
            if (Character.isHighSurrogate(c)) {
                assertTrue(i + 1 < clipped.length() && Character.isLowSurrogate(clipped.charAt(i + 1)), "split at " + i);
            }
        }
        // The emoji just inside the cut is kept whole.
        assertEquals("x".repeat(297) + "🔴…", BotViewWriter.clip("x".repeat(297) + "🔴" + "y".repeat(50)));

        String body = BotViewWriter.render(fixture(), Optional.of(reason), AT);
        Path f = dir.resolve(BotViewWriter.FILE_NAME);
        assertDoesNotThrow(() -> BotViewWriter.atomicReplace(f, body));
        assertEquals(body, Files.readString(f, StandardCharsets.UTF_8));
        JSONObject root = new JSONObject(body);
        assertEquals(clipped, root.getString("halt_reason"));
    }

    @Test
    @DisplayName("a lone surrogate from an exception message is coded, so the UTF-8 write cannot fail")
    void loneSurrogateIsCoded(@TempDir Path dir) {
        String reason = "boom \uD83D end";   // half an emoji, as a truncated exception message can carry
        String body = BotViewWriter.render(fixture(), Optional.of(reason), AT);
        assertTrue(body.contains("boom \\ud83d end"), body);
        assertTrue(body.contains("🟢") || body.contains("⏸"), "whole emoji stay as they are: " + body);
        assertDoesNotThrow(() -> BotViewWriter.atomicReplace(dir.resolve(BotViewWriter.FILE_NAME), body));
    }

    @Test
    @DisplayName("state_line is one bounded line even when the halt reason is a multi-line error page")
    void stateLineIsOneBoundedLine() {
        String page = "boot read failed: <html>\n<head><title>502</title></head>\n" + "z".repeat(2_000) + "\n</html>";
        JSONObject root = new JSONObject(BotViewWriter.render(fixture(), Optional.of(page), AT));
        String line = root.getString("state_line");
        assertEquals("halt", root.getString("state"));
        assertTrue(line.startsWith("⏸ Халт: boot read failed: <html> <head>"), line);
        assertFalse(line.contains("\n") || line.contains("\r"), line);
        assertTrue(line.length() <= BotViewWriter.MAX_REASON_CHARS, "length " + line.length());
    }

    @Test
    @DisplayName("strings are escaped for JSON only: quotes and control characters coded, «—» and «−» kept readable")
    void escaping() {
        String reason = "a\\b \"c\" \t\u0001 — −3% \u2028";
        String body = BotViewWriter.render(fixture(), Optional.of(reason), AT);
        assertEquals(reason, new JSONObject(body).getString("halt_reason"), "round-trips");
        assertTrue(body.contains("\"halt_reason\":\"a\\\\b \\\"c\\\" \\t\\u0001 — −3% \\u2028\""), body);
        assertTrue(body.contains("входов нет") || body.contains("Халт"), "Cyrillic written as is: " + body);
    }

    // ─── The file ────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("atomic replace: new content in place, no temp file left, UTF-8")
    void atomicReplace(@TempDir Path dir) throws Exception {
        Path f = dir.resolve(BotViewWriter.FILE_NAME);
        Files.writeString(f, "{\"old\":true}");
        BotViewWriter.atomicReplace(f, "{\"state_line\":\"⏸ пауза\"}\n");
        assertEquals("{\"state_line\":\"⏸ пауза\"}\n", Files.readString(f, StandardCharsets.UTF_8));
        try (var files = Files.list(dir)) {
            assertEquals(List.of(f.getFileName().toString()), files.map(p -> p.getFileName().toString()).toList());
        }
    }

    @Test
    @DisplayName("a failed replace leaves no temp file behind and says so to its caller")
    void failedReplaceCleansUp(@TempDir Path dir) throws Exception {
        Path target = dir.resolve(BotViewWriter.FILE_NAME);
        Files.createDirectory(target);
        Files.writeString(target.resolve("keep"), "x");   // a non-empty directory cannot be replaced
        assertThrows(IOException.class, () -> BotViewWriter.atomicReplace(target, "{}"));
        assertFalse(Files.exists(dir.resolve(BotViewWriter.FILE_NAME + ".tmp")));
    }

    @Test
    @DisplayName("a reader polling while the file is rewritten never sees half a file")
    void readerNeverSeesHalfAFile(@TempDir Path dir) throws Exception {
        Path f = dir.resolve(BotViewWriter.FILE_NAME);
        String small = BotViewWriter.render(fixture(), Optional.empty(), AT);
        String big = BotViewWriter.render(fixture(), Optional.of("y".repeat(BotViewWriter.MAX_REASON_CHARS)), AT);
        BotViewWriter.atomicReplace(f, small);
        AtomicBoolean done = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        AtomicReference<String> torn = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            while (!done.get()) {
                String text;
                try {
                    text = Files.readString(f, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    continue;   // Windows may refuse a read mid-rename; a refused read is not a torn one
                }
                reads.incrementAndGet();
                if (!text.equals(small) && !text.equals(big)) torn.compareAndSet(null, text);
            }
        });
        reader.start();
        for (int i = 0; i < 300; i++) {
            try {
                BotViewWriter.atomicReplace(f, i % 2 == 0 ? big : small);
            } catch (IOException e) {
                // Windows: the rename can meet the reader's open handle; production runs on Linux.
            }
        }
        done.set(true);
        reader.join(5_000);
        assertNull(torn.get(), "a reader saw a partial file");
        assertTrue(reads.get() > 0);
    }

    // ─── The loop's side ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("at most one write per pass, whatever the loop offers")
    void throttled(@TempDir Path dir) throws Exception {
        MovableClock clock = new MovableClock();
        List<String> written = new ArrayList<>();
        BotViewWriter w = new BotViewWriter(dir.resolve("v.json"), clock, Runnable::run, (p, body) -> written.add(body));

        assertTrue(w.offer(fixture(), Optional.empty()));
        clock.plus(Duration.ofSeconds(1));
        assertFalse(w.offer(fixture(), Optional.empty()), "a second offer in the same pass: skipped");
        clock.plus(Duration.ofSeconds(23));
        assertFalse(w.offer(fixture(), Optional.empty()), "24 s after the last write: skipped");
        clock.plus(Duration.ofSeconds(6));
        assertTrue(w.offer(fixture(), Optional.empty()), "30 s later: written");
        assertFalse(w.offer(null, Optional.empty()), "no snapshot, no file");
        assertEquals(2, written.size());
    }

    @Test
    @DisplayName("a pass that ends 29.3 s after a slow one is still written: every pass, not every other")
    void fasterPassAfterASlowOneIsWritten(@TempDir Path dir) {
        MovableClock clock = new MovableClock();
        List<String> written = new ArrayList<>();
        BotViewWriter w = new BotViewWriter(dir.resolve("v.json"), clock, Runnable::run, (p, body) -> written.add(body));

        // Passes start 30 s apart; offers come at their ends. Pass k took 1.2 s (the P&L probe was
        // due), pass k+1 took 0.4 s: its offer lands 29.2 s after pass k's. The old strict 30 s
        // throttle dropped it, and the file moved every 60 s.
        for (int k = 0; k < 10; k++) {
            clock.plus(Duration.ofMillis(k % 2 == 0 ? 30_800 : 29_200));
            assertTrue(w.offer(fixture(), Optional.empty()), "pass " + k + " written");
        }
        assertEquals(10, written.size());
    }

    @Test
    @DisplayName("during an exchange hold the watchdog writes the parked view as exchange_hold; a short hold writes nothing")
    void holdWatchdogWritesTheHold(@TempDir Path dir) {
        MovableClock clock = new MovableClock();
        List<String> written = new ArrayList<>();
        BotViewWriter w = new BotViewWriter(dir.resolve("v.json"), clock, Runnable::run, (p, body) -> written.add(body));
        OperatorSnapshot last = fixture();
        assertTrue(w.offer(last, Optional.empty()), "the loop's last pass before it parked");

        clock.plus(Duration.ofSeconds(60));
        assertFalse(w.offerDuringHold(30_000L, () -> last.withExchangeHold(30_000L), Optional::empty),
                "a 30 s hold: the loop writes again itself within a pass");
        assertFalse(w.offerDuringHold(0L, () -> last, Optional::empty));
        assertEquals(1, written.size());

        long held = Duration.ofHours(16).toMillis();
        assertTrue(w.offerDuringHold(held, () -> last.withExchangeHold(held), Optional::empty));
        assertEquals(2, written.size());
        JSONObject root = new JSONObject(written.get(1));
        assertEquals("exchange_hold", root.getString("state"));
        assertTrue(root.getString("state_line").startsWith("🔴 Биржа не отвечает — пауза ещё"), root.getString("state_line"));
        assertEquals("2026-09-24T08:15:02Z", root.getString("ts"), "the data stays the last pass's, dated as such");

        clock.plus(Duration.ofSeconds(10));
        assertFalse(w.offerDuringHold(held, () -> last.withExchangeHold(held), Optional::empty),
                "the watchdog obeys the same throttle as the loop");
        assertFalse(w.offerDuringHold(held, () -> null, Optional::empty), "no snapshot yet: nothing");
        clock.plus(Duration.ofSeconds(60));
        assertFalse(assertDoesNotThrow(() -> w.offerDuringHold(held, () -> {
            throw new IllegalStateException("hold reader broke");
        }, Optional::empty)), "a throwing reader costs one write, never the watchdog thread");
        assertEquals(2, written.size());
    }

    @Test
    @DisplayName("a failing disk never throws into the loop and logs once, then at most every 10 minutes")
    void failingDiskOnlyLogs(@TempDir Path dir) {
        MovableClock clock = new MovableClock();
        AtomicBoolean broken = new AtomicBoolean(true);
        BotViewWriter w = new BotViewWriter(dir.resolve("v.json"), clock, Runnable::run, (p, body) -> {
            if (broken.get()) throw new IOException("No space left on device");
        });

        for (int i = 0; i < 20; i++) {   // ten minutes of passes, 30 s apart
            assertDoesNotThrow(() -> w.offer(fixture(), Optional.empty()));
            clock.plus(Duration.ofSeconds(30));
        }
        assertEquals(1, warnings(), "twenty failures in ten minutes are one line");
        assertDoesNotThrow(() -> w.offer(fixture(), Optional.empty()));
        assertEquals(2, warnings(), "ten minutes on: said again");

        broken.set(false);
        clock.plus(Duration.ofSeconds(30));
        w.offer(fixture(), Optional.empty());
        assertTrue(logged.stream().anyMatch(r -> r.getLevel() == Level.INFO
                && r.getMessage().contains("written again after 21 failure(s)")), "the recovery is said once");
        clock.plus(Duration.ofSeconds(30));
        broken.set(true);
        w.offer(fixture(), Optional.empty());
        assertEquals(3, warnings(), "a new run of failures speaks up at once");
    }

    @Test
    @DisplayName("a snapshot that cannot be rendered is logged, not thrown, and the next one still goes")
    void renderFailureOnlyLogs(@TempDir Path dir) {
        MovableClock clock = new MovableClock();
        List<String> written = new ArrayList<>();
        BotViewWriter w = new BotViewWriter(dir.resolve("v.json"), clock, Runnable::run, (p, body) -> written.add(body));
        OperatorSnapshot s = fixture();
        OperatorSnapshot broken = new OperatorSnapshot(s.at(), s.startedAt(), s.buildStamp(), false, false, 0L,
                false, false, 0, 0, s.lastReconcileOkAt(), s.account(),
                List.of(new OperatorSnapshot.Position("XUSDT", null, 1, 1, 1, 0, 0.9, 1.1, null, true)),
                true, null, List.of());

        assertFalse(assertDoesNotThrow(() -> w.offer(broken, Optional.empty())));
        assertEquals(1, warnings());
        assertTrue(w.offer(fixture(), Optional.empty()), "a failed render does not start the 30 s wait");
        assertEquals(1, written.size());
    }

    @Test
    @DisplayName("a stalled disk costs a stale file, never a waiting loop; the latest view wins when it frees")
    void stalledDiskNeverBlocks(@TempDir Path dir) throws Exception {
        MovableClock clock = new MovableClock();
        Path f = dir.resolve(BotViewWriter.FILE_NAME);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstEntered = new CountDownLatch(1);
        List<String> written = Collections.synchronizedList(new ArrayList<>());
        BotViewWriter w = BotViewWriter.start(f, clock, (p, body) -> {
            firstEntered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            written.add(body);
        });

        assertTrue(w.offer(fixture(), Optional.empty()));
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        long t0 = System.nanoTime();
        String last = null;
        for (int i = 1; i <= 50; i++) {
            clock.plus(Duration.ofSeconds(30));
            assertTrue(w.offer(fixture(), Optional.of("pass " + i)));
            last = "pass " + i;
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(elapsedMs < 2_000, "fifty offers against a stuck disk took " + elapsedMs + " ms");

        release.countDown();
        long deadline = System.currentTimeMillis() + 5_000;
        while (written.size() < 2 && System.currentTimeMillis() < deadline) Thread.onSpinWait();
        Thread.sleep(100);
        assertEquals(2, written.size(), "the stuck write, then only the newest - the queue never grows");
        assertEquals(last, new JSONObject(written.get(1)).getString("halt_reason"));
    }
}
