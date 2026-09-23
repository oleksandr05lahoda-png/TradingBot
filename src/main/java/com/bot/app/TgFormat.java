package com.bot.app;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The operator's Telegram typography in one place: HTML escaping, money, signed percents, Warsaw
 * times, Russian ages and plurals, and the 4096-character split. Every human-facing line the bot
 * sends goes through these so the chat reads as one voice - "+$1.23", "−$0.40", "+1.2%", "14:05" -
 * whichever thread or class wrote it. Pure functions; nothing here touches the network.
 */
final class TgFormat {

    private TgFormat() {}

    /** Telegram refuses a longer text with HTTP 400; a 30-symbol summary reached it once (AlertSink). */
    static final int MAX_MESSAGE_CHARS = 4096;
    /** The owner reads times in his own zone; the exchange's UTC day is not his day. */
    static final ZoneId WARSAW = ZoneId.of("Europe/Warsaw");
    /** A real minus sign: a hyphen next to a dollar reads as a dash on a phone. */
    static final String MINUS = "−";

    private static final DateTimeFormatter HH_MM = DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT);

    /**
     * Escapes the three characters parse_mode=HTML cares about. Every dynamic string - a symbol, a
     * halt reason quoting an exchange error like "{@code <html>}" - goes through this, or one stray
     * '&lt;' turns the message into a 400 and the operator into someone who heard nothing.
     */
    static String esc(String raw) {
        if (raw == null) return "";
        StringBuilder sb = new StringBuilder(raw.length() + 8);
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /** The plain-text twin of an HTML message, for the one resend after Telegram refuses the markup. */
    static String plain(String html) {
        if (html == null) return "";
        return html.replaceAll("<[^>]{1,40}>", "")
                .replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&amp;", "&");
    }

    /** {@code +$1.23}, {@code −$0.40}, {@code $0.00}; "—" when unknown. */
    static String money(double usd) {
        if (!Double.isFinite(usd)) return "—";
        long cents = Math.round(usd * 100.0);
        if (cents == 0) return "$0.00";
        String body = String.format(Locale.ROOT, "$%.2f", Math.abs(cents) / 100.0);
        return (cents > 0 ? "+" : MINUS) + body;
    }

    /** A balance, which has no sign to show: {@code $143.10}. */
    static String balance(double usd) {
        if (!Double.isFinite(usd)) return "—";
        return (usd < 0 ? MINUS : "") + String.format(Locale.ROOT, "$%.2f", Math.abs(usd));
    }

    /** {@code +1.2%}, {@code −0.4%}, {@code 0.0%}; "—" when unknown. Takes percent points, not a fraction. */
    static String pct(double points) {
        if (!Double.isFinite(points)) return "—";
        long tenths = Math.round(points * 10.0);
        if (tenths == 0) return "0.0%";
        return (tenths > 0 ? "+" : MINUS) + String.format(Locale.ROOT, "%.1f%%", Math.abs(tenths) / 10.0);
    }

    /** {@code 14:05} on the owner's clock. */
    static String time(Instant at) {
        return at == null ? "—" : HH_MM.format(at.atZone(WARSAW));
    }

    private static final DateTimeFormatter DD_MM_HH_MM = DateTimeFormatter.ofPattern("dd.MM HH:mm", Locale.ROOT);

    /** {@code 23.09 14:05} on the owner's clock - for anything that may be from another day. */
    static String dateTime(Instant at) {
        return at == null ? "—" : DD_MM_HH_MM.format(at.atZone(WARSAW));
    }

    /**
     * A price as the exchange would print it: six significant digits, no trailing zeros, never
     * scientific - {@code 0.5075}, {@code 86437}, {@code 0.00001234}. "—" when unknown.
     */
    static String price(double value) {
        if (!Double.isFinite(value) || value <= 0) return "—";
        return new java.math.BigDecimal(value).round(new java.math.MathContext(6))
                .stripTrailingZeros().toPlainString();
    }

    /** {@code 12 с}, {@code 5 мин}, {@code 3 ч 10 мин}, {@code 2 д 4 ч} - the age of anything. */
    static String age(Duration d) {
        if (d == null) return "—";
        long s = Math.max(0, d.getSeconds());
        if (s < 60) return s + " с";
        long m = s / 60;
        if (m < 60) return m + " мин";
        long h = m / 60;
        if (h < 24) return h + " ч" + (m % 60 == 0 ? "" : " " + (m % 60) + " мин");
        long days = h / 24;
        return days + " д" + (h % 24 == 0 ? "" : " " + (h % 24) + " ч");
    }

    /** {@code 2 д 4 ч} with the minutes dropped past an hour - a holding time, read at a glance. */
    static String held(Duration d) {
        if (d == null) return "—";
        long m = Math.max(0, d.toMinutes());
        if (m < 60) return m + " мин";
        long h = m / 60;
        if (h < 24) return h + " ч";
        return h / 24 + " д" + (h % 24 == 0 ? "" : " " + (h % 24) + " ч");
    }

    /** {@code 12 с назад}; the building block of every "data as of" footer. */
    static String ago(Instant at, Instant now) {
        if (at == null) return "ещё не было";
        return age(Duration.between(at, now)) + " назад";
    }

    /** Russian plural: 1 позиция, 2 позиции, 5 позиций, 21 позиция. */
    static String plural(long n, String one, String few, String many) {
        long mod100 = Math.abs(n) % 100;
        long mod10 = mod100 % 10;
        if (mod100 >= 11 && mod100 <= 14) return many;
        if (mod10 == 1) return one;
        if (mod10 >= 2 && mod10 <= 4) return few;
        return many;
    }

    /** {@code 3 позиции} - the count and its word together, the way the chat says it. */
    static String positions(long n) {
        return n + " " + plural(n, "позиция", "позиции", "позиций");
    }

    /** ADAUSDT reads as ADA in a table a phone must fit; anything else is kept as it is. */
    static String shortSymbol(String symbol) {
        if (symbol == null) return "";
        String s = symbol.endsWith("USDT") && symbol.length() > 4 ? symbol.substring(0, symbol.length() - 4) : symbol;
        return s.length() > 8 ? s.substring(0, 8) : s;
    }

    /** Left-justified to {@code width}, never cut. */
    static String padRight(String s, int width) {
        StringBuilder sb = new StringBuilder(s == null ? "" : s);
        while (sb.length() < width) sb.append(' ');
        return sb.toString();
    }

    /** Right-justified to {@code width}, never cut. */
    static String padLeft(String s, int width) {
        StringBuilder sb = new StringBuilder();
        String v = s == null ? "" : s;
        for (int i = v.length(); i < width; i++) sb.append(' ');
        return sb.append(v).toString();
    }

    /**
     * Splits an HTML message into parts Telegram accepts, on line boundaries. A {@code <pre>} block
     * cut in two is closed at the end of one part and reopened at the start of the next, so every
     * part is valid markup on its own - a table of 300 positions arrives as three tables, not as a
     * 400 and silence. A single line longer than a part is cut hard, never inside a tag or an entity.
     */
    static List<String> split(String html, int max) {
        if (html == null || html.isEmpty()) return List.of("");
        if (html.length() <= max) return List.of(html);
        final String open = "<pre>", close = "</pre>";
        // Room a part must keep for the closing tag it may need, plus the reopened tag it may start with.
        int budget = max - close.length();
        List<String> parts = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inPre = false;           // state at the end of `cur`
        boolean fresh = true;            // `cur` holds no line yet (at most a reopened <pre>)
        for (String rawLine : html.split("\n", -1)) {
            boolean firstPiece = true;   // later pieces of a hard-cut line continue it: no newline
            for (String line : hardCut(rawLine, budget - open.length() - 1)) {
                boolean after = preStateAfter(line, inPre);
                int glue = !fresh && firstPiece ? 1 : 0;
                if (!fresh && cur.length() + glue + line.length() > budget) {
                    if (inPre) cur.append(close);
                    parts.add(cur.toString());
                    cur.setLength(0);
                    if (inPre) cur.append(open);
                    fresh = true;
                }
                if (!fresh && firstPiece) cur.append('\n');
                cur.append(line);
                fresh = false;
                firstPiece = false;
                inPre = after;
            }
        }
        if (cur.length() > 0) {
            if (inPre) cur.append(close);
            parts.add(cur.toString());
        }
        return parts;
    }

    private static boolean preStateAfter(String line, boolean before) {
        int lastOpen = line.lastIndexOf("<pre>");
        int lastClose = line.lastIndexOf("</pre>");
        if (lastOpen < 0 && lastClose < 0) return before;
        return lastOpen > lastClose;
    }

    /** Cuts an over-long line into pieces of at most {@code max}, backing off from tags and entities. */
    private static List<String> hardCut(String line, int max) {
        if (line.length() <= max || max <= 0) return List.of(line);
        List<String> out = new ArrayList<>();
        int from = 0;
        while (line.length() - from > max) {
            int cut = from + max;
            int lt = line.lastIndexOf('<', cut - 1);
            int gt = line.lastIndexOf('>', cut - 1);
            if (lt >= from && lt > gt) cut = lt;               // inside a tag
            int amp = line.lastIndexOf('&', cut - 1);
            int semi = line.lastIndexOf(';', cut - 1);
            if (amp >= from && amp > semi && cut - amp < 10) cut = amp;   // inside an entity
            if (cut <= from) cut = from + max;                 // nothing better: a plain cut
            out.add(line.substring(from, cut));
            from = cut;
        }
        out.add(line.substring(from));
        return out;
    }
}
