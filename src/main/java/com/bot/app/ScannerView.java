package com.bot.app;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * What the scanner saw on its last pass, read from {@code <DATA_DIR>/scanner_view.json} (SCANNER
 * VIEW CONTRACT v1, written by autoscan.py after every pass). The owner could always see the book
 * but never the queue behind it: "why is nothing opening?" took an offline replay on 03.09 and a
 * 22-agent audit on 16.09, while the answer sat in the scanner's memory the whole time.
 *
 * <p>Optional by contract: a missing, oversized, unparseable or stale file is a state the screens
 * name, never an exception. Read on the Telegram thread only - a local file, never the exchange -
 * and nothing here steers a trade.
 */
record ScannerView(
        Instant ts,
        Instant nextPassEta,
        Integer held,
        Integer room,
        Integer maxPositions,
        Integer entryOk,
        Integer holdOk,
        boolean halted,
        Btc btc,
        boolean shortArmed,
        List<Candidate> queue,
        List<Opened> opened,
        List<Closed> closed,
        Map<String, Why> why) {

    /** The file name inside the data directory; the scanner's {@code VIEW_FILE}. */
    static final String FILE_NAME = "scanner_view.json";
    /** The scanner runs hourly; two missed passes plus slack before the view is called stale. */
    static final Duration STALE_AFTER = Duration.ofMinutes(135);
    /** A real view is a few KB. Anything past this is not the scanner's file and is not parsed. */
    static final long MAX_BYTES = 1_048_576L;
    /** The contract caps the queue at 30; a longer list is trimmed rather than trusted. */
    static final int MAX_QUEUE = 30;
    private static final int MAX_WHY = 100;
    private static final int MAX_NOTE = 60;

    ScannerView {
        queue = queue == null ? List.of() : List.copyOf(queue);
        opened = opened == null ? List.of() : List.copyOf(opened);
        closed = closed == null ? List.of() : List.copyOf(closed);
        why = why == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(why));
    }

    /** BTC against its 50-day average, and whether the bear arm's gate is open. */
    record Btc(double price, double sma50, Boolean shortGateOpen) {}

    /** One symbol that passed an entry gate and was not opened. Unknown numbers are NaN. */
    record Candidate(String symbol, String side, String trig, String reason, double ret30,
                     double fromHigh20, double volRatio, double stopFrac, String note) {}

    record Opened(String symbol, String side, String trig) {}

    record Closed(String symbol, String reason) {}

    /** Why the scanner opened a position it still holds. Unknown numbers are NaN. */
    record Why(Instant openedAt, String side, String trig, double price, double ret30, double fromHigh20,
               double fromHigh90, double volRatio, double atr, double stopFrac) {}

    enum Status { OK, STALE, MISSING, TOO_BIG, GARBAGE }

    /**
     * One read of the file: the status, the view when it could be parsed (a stale view is still
     * shown, under a warning), and a short Russian line saying what went wrong.
     */
    record Read(Status status, ScannerView view, String problem) {
        boolean usable() {
            return view != null;
        }
    }

    /** Reads and judges the file. Never throws. */
    static Read read(Path file, Instant now) {
        if (file == null) return new Read(Status.MISSING, null, "путь к файлу сканера не задан");
        try {
            if (!Files.isRegularFile(file)) return new Read(Status.MISSING, null, "файла сканера нет");
            long size = Files.size(file);
            if (size > MAX_BYTES) {
                return new Read(Status.TOO_BIG, null, "файл сканера " + (size / 1024) + " КБ — больше 1 МБ, не читаю");
            }
            String body = Files.readString(file, StandardCharsets.UTF_8);
            return judge(parse(body), now);
        } catch (IOException | java.io.UncheckedIOException | SecurityException e) {
            return new Read(Status.MISSING, null, "файл сканера не читается");
        } catch (RuntimeException e) {
            // org.json throws JSONException (a RuntimeException) on a torn or foreign file.
            return new Read(Status.GARBAGE, null, "файл сканера испорчен — не разбирается");
        }
    }

    private static Read judge(ScannerView v, Instant now) {
        if (v.ts() == null) return new Read(Status.GARBAGE, null, "в файле сканера нет времени прохода");
        if (now != null && Duration.between(v.ts(), now).compareTo(STALE_AFTER) > 0) {
            return new Read(Status.STALE, v, "сканер молчит " + TgFormat.age(Duration.between(v.ts(), now)));
        }
        return new Read(Status.OK, v, "");
    }

    /** Parses one document. Throws on anything that is not a v1 view; a bad row is skipped. */
    static ScannerView parse(String body) {
        JSONObject o = new JSONObject(body);
        int version = o.optInt("v", -1);
        if (version != 1) throw new IllegalArgumentException("unsupported scanner view version " + version);
        JSONObject btc = o.optJSONObject("btc");
        List<Candidate> queue = new ArrayList<>();
        JSONArray q = o.optJSONArray("queue");
        if (q != null) {
            for (int i = 0; i < q.length() && queue.size() < MAX_QUEUE; i++) {
                JSONObject c = q.optJSONObject(i);
                String symbol = c == null ? "" : text(c, "symbol");
                if (symbol.isEmpty()) continue;
                queue.add(new Candidate(symbol, text(c, "side").toUpperCase(Locale.ROOT), text(c, "trig"),
                        text(c, "reason"), num(c, "ret30"), num(c, "from_high20"), num(c, "vol_ratio"),
                        num(c, "stop_frac"), cap(text(c, "note"))));
            }
        }
        List<Opened> opened = new ArrayList<>();
        JSONArray op = o.optJSONArray("opened");
        if (op != null) {
            for (int i = 0; i < op.length() && opened.size() < MAX_QUEUE; i++) {
                JSONObject r = op.optJSONObject(i);
                if (r == null || text(r, "symbol").isEmpty()) continue;
                opened.add(new Opened(text(r, "symbol"), text(r, "side").toUpperCase(Locale.ROOT), text(r, "trig")));
            }
        }
        List<Closed> closed = new ArrayList<>();
        JSONArray cl = o.optJSONArray("closed");
        if (cl != null) {
            for (int i = 0; i < cl.length() && closed.size() < MAX_QUEUE; i++) {
                JSONObject r = cl.optJSONObject(i);
                if (r == null || text(r, "symbol").isEmpty()) continue;
                closed.add(new Closed(text(r, "symbol"), text(r, "reason")));
            }
        }
        Map<String, Why> why = new LinkedHashMap<>();
        JSONObject w = o.optJSONObject("why");
        if (w != null) {
            for (String symbol : w.keySet()) {
                if (why.size() >= MAX_WHY) break;
                JSONObject r = w.optJSONObject(symbol);
                if (r == null || symbol.isBlank()) continue;
                why.put(symbol, new Why(instant(r, "opened_ts"), text(r, "side").toUpperCase(Locale.ROOT),
                        text(r, "trig"), num(r, "price"), num(r, "ret30"), num(r, "from_high20"),
                        num(r, "from_high90"), num(r, "vol_ratio"), num(r, "atr"), num(r, "stop_frac")));
            }
        }
        return new ScannerView(instant(o, "ts"), instant(o, "next_pass_eta"), integer(o, "held"),
                integer(o, "room"), integer(o, "max_positions"), integer(o, "entry_ok"), integer(o, "hold_ok"),
                o.optBoolean("halted", false),
                btc == null ? null : new Btc(num(btc, "price"), num(btc, "sma50"),
                        btc.isNull("short_gate_open") || !btc.has("short_gate_open")
                                ? null : btc.optBoolean("short_gate_open")),
                o.optBoolean("short_armed", false), queue, opened, closed, why);
    }

    private static String text(JSONObject o, String key) {
        if (!o.has(key) || o.isNull(key)) return "";
        Object v = o.opt(key);
        return v instanceof String s ? s.trim() : "";
    }

    private static String cap(String s) {
        return s.length() <= MAX_NOTE ? s : s.substring(0, MAX_NOTE - 1) + "…";
    }

    /** A finite number or NaN: null, a string, NaN in the file all read as "unknown". */
    private static double num(JSONObject o, String key) {
        if (!o.has(key) || o.isNull(key)) return Double.NaN;
        Object v = o.opt(key);
        if (!(v instanceof Number n)) return Double.NaN;
        double d = n.doubleValue();
        return Double.isFinite(d) ? d : Double.NaN;
    }

    private static Integer integer(JSONObject o, String key) {
        double d = num(o, key);
        return Double.isNaN(d) ? null : (int) Math.round(d);
    }

    private static Instant instant(JSONObject o, String key) {
        String s = text(o, key);
        if (s.isEmpty()) return null;
        try {
            return Instant.parse(s);
        } catch (RuntimeException e) {
            return null;
        }
    }
}
