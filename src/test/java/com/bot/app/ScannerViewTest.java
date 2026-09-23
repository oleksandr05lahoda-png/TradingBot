package com.bot.app;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The scanner's view file as the bot reads it (SCANNER VIEW CONTRACT v1). Optional by contract:
 * whatever lies on disk - nothing, a torn write, a foreign file, a stale pass - becomes a status
 * the screens can name, never an exception on the Telegram thread.
 */
class ScannerViewTest {

    static final Instant PASS = Instant.parse("2026-09-23T12:42:47Z");

    @TempDir
    Path dir;

    /** A realistic pass: full book, two waiting, one opened, one closed, one "why". */
    static JSONObject doc() {
        return new JSONObject()
                .put("v", 1).put("ts", "2026-09-23T12:42:47Z").put("next_pass_eta", "2026-09-23T13:42:47Z")
                .put("held", 15).put("room", 0).put("max_positions", 15)
                .put("entry_ok", 20).put("hold_ok", 91).put("halted", false)
                .put("btc", new JSONObject().put("price", 86437.0).put("sma50", 74125.0).put("short_gate_open", false))
                .put("short_armed", true)
                .put("queue", new JSONArray()
                        .put(new JSONObject().put("symbol", "XUSDT").put("side", "LONG").put("trig", "trend")
                                .put("reason", "book_full").put("ret30", 0.123).put("from_high20", 0.021)
                                .put("vol_ratio", 2.1).put("stop_frac", 0.131).put("note", "нет свободного места"))
                        .put(new JSONObject().put("symbol", "WIDEUSDT").put("side", "LONG").put("trig", "trend")
                                .put("reason", "too_wide").put("ret30", 0.4).put("from_high20", JSONObject.NULL)
                                .put("vol_ratio", JSONObject.NULL).put("stop_frac", 0.221).put("note", "стоп")))
                .put("opened", new JSONArray().put(new JSONObject().put("symbol", "TIAUSDT").put("side", "LONG").put("trig", "trend")))
                .put("closed", new JSONArray().put(new JSONObject().put("symbol", "ADAUSDT").put("reason", "max-hold")))
                .put("why", new JSONObject().put("TIAUSDT", new JSONObject()
                        .put("opened_ts", "2026-09-23T12:05:00Z").put("side", "LONG").put("trig", "trend")
                        .put("price", 0.5058).put("ret30", 0.18).put("from_high20", 0.01).put("from_high90", 0.05)
                        .put("vol_ratio", 2.4).put("atr", 0.033).put("stop_frac", 0.129)));
    }

    Path write(String body) throws Exception {
        Path f = dir.resolve(ScannerView.FILE_NAME);
        Files.writeString(f, body, StandardCharsets.UTF_8);
        return f;
    }

    @Test
    @DisplayName("a fresh v1 file parses whole, nulls read as unknown")
    void freshFileParses() throws Exception {
        ScannerView.Read r = ScannerView.read(write(doc().toString()), PASS.plusSeconds(600));

        assertEquals(ScannerView.Status.OK, r.status());
        ScannerView v = r.view();
        assertEquals(PASS, v.ts());
        assertEquals(15, v.held());
        assertEquals(0, v.room());
        assertEquals(86437.0, v.btc().price());
        assertEquals(Boolean.FALSE, v.btc().shortGateOpen());
        assertTrue(v.shortArmed());
        assertEquals(2, v.queue().size());
        assertEquals("book_full", v.queue().get(0).reason());
        assertTrue(Double.isNaN(v.queue().get(1).volRatio()), "null is unknown, not zero");
        assertTrue(Double.isNaN(v.queue().get(1).fromHigh20()));
        assertEquals("TIAUSDT", v.opened().get(0).symbol());
        assertEquals("max-hold", v.closed().get(0).reason());
        assertEquals(0.129, v.why().get("TIAUSDT").stopFrac());
        assertEquals(Instant.parse("2026-09-23T12:05:00Z"), v.why().get("TIAUSDT").openedAt());
    }

    @Test
    @DisplayName("older than 2h15m is stale - and still shown, under a warning")
    void staleKeepsTheData() throws Exception {
        Path f = write(doc().toString());

        assertEquals(ScannerView.Status.OK, ScannerView.read(f, PASS.plusSeconds(135 * 60)).status());
        ScannerView.Read r = ScannerView.read(f, PASS.plusSeconds(135 * 60 + 1));
        assertEquals(ScannerView.Status.STALE, r.status());
        assertTrue(r.usable());
        assertTrue(r.problem().startsWith("сканер молчит 2 ч 15 мин"), r.problem());
    }

    @Test
    @DisplayName("no file, no path: said plainly, nothing thrown")
    void missing() {
        ScannerView.Read r = ScannerView.read(dir.resolve("nope.json"), PASS);
        assertEquals(ScannerView.Status.MISSING, r.status());
        assertFalse(r.usable());
        assertEquals(ScannerView.Status.MISSING, ScannerView.read(null, PASS).status());
        assertEquals(ScannerView.Status.MISSING, ScannerView.read(dir, PASS).status(), "a directory is not the file");
    }

    @Test
    @DisplayName("a torn write, a foreign file and a future version are garbage, not a crash")
    void garbage() throws Exception {
        assertEquals(ScannerView.Status.GARBAGE, ScannerView.read(write("{\"v\":1,\"ts\":\"2026-09-"), PASS).status());
        assertEquals(ScannerView.Status.GARBAGE, ScannerView.read(write("[1,2,3]"), PASS).status());
        assertEquals(ScannerView.Status.GARBAGE, ScannerView.read(write(""), PASS).status());
        assertEquals(ScannerView.Status.GARBAGE,
                ScannerView.read(write(doc().put("v", 2).toString()), PASS).status());
        assertEquals(ScannerView.Status.GARBAGE,
                ScannerView.read(write(doc().put("ts", "yesterday").toString()), PASS).status());
    }

    @Test
    @DisplayName("a file over 1 MB is refused before it is read")
    void huge() throws Exception {
        String pad = " ".repeat((int) ScannerView.MAX_BYTES);
        ScannerView.Read r = ScannerView.read(write(doc().toString() + pad), PASS);
        assertEquals(ScannerView.Status.TOO_BIG, r.status());
        assertNull(r.view());
    }

    @Test
    @DisplayName("bad rows are skipped, strings where numbers belong are unknown, the queue is capped at 30")
    void badRowsAndCaps() throws Exception {
        JSONArray q = new JSONArray().put("not a row").put(new JSONObject().put("reason", "corr"))
                .put(new JSONObject().put("symbol", "ZUSDT").put("ret30", "12%").put("stop_frac", "wide"));
        for (int i = 0; i < 50; i++) q.put(new JSONObject().put("symbol", "S" + i + "USDT").put("reason", "book_full"));
        ScannerView v = ScannerView.read(write(doc().put("queue", q).toString()), PASS).view();

        assertEquals(ScannerView.MAX_QUEUE, v.queue().size());
        assertEquals("ZUSDT", v.queue().get(0).symbol());
        assertTrue(Double.isNaN(v.queue().get(0).ret30()));
        assertEquals("", v.queue().get(0).reason());
    }

    @Test
    @DisplayName("a missing btc block and missing counts are unknown, not zero")
    void missingBlocks() throws Exception {
        JSONObject d = doc();
        d.remove("btc");
        d.put("held", JSONObject.NULL);
        d.remove("room");
        ScannerView v = ScannerView.read(write(d.toString()), PASS).view();
        assertNull(v.btc());
        assertNull(v.held());
        assertNull(v.room());
    }
}
