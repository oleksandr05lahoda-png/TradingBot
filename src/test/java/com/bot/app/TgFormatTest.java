package com.bot.app;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The chat's typography: one way to write money, percents, times and ages, and markup that survives. */
class TgFormatTest {

    @Test
    @DisplayName("money is signed with a real minus, cents rounded, zero unsigned, unknown a dash")
    void money() {
        assertEquals("+$1.23", TgFormat.money(1.234));
        assertEquals("−$0.40", TgFormat.money(-0.4));
        assertEquals("$0.00", TgFormat.money(-0.004));
        assertEquals("—", TgFormat.money(Double.NaN));
        assertEquals("$143.10", TgFormat.balance(143.1));
    }

    @Test
    @DisplayName("percents carry a sign and one decimal")
    void percents() {
        assertEquals("+1.2%", TgFormat.pct(1.24));
        assertEquals("−0.4%", TgFormat.pct(-0.36));
        assertEquals("0.0%", TgFormat.pct(0.04));
        assertEquals("—", TgFormat.pct(Double.POSITIVE_INFINITY));
    }

    @Test
    @DisplayName("prices print like the exchange: six significant digits, no zeros, never scientific")
    void prices() {
        assertEquals("0.5075", TgFormat.price(0.50750));
        assertEquals("86437", TgFormat.price(86437.0));
        assertEquals("0.00001234", TgFormat.price(0.00001234));
        assertEquals("312.457", TgFormat.price(312.4571));
        assertEquals("—", TgFormat.price(Double.NaN));
        assertEquals("—", TgFormat.price(0));
    }

    @Test
    @DisplayName("a date-time and a holding time read at a glance")
    void dateTimeAndHeld() {
        assertEquals("23.09 14:05", TgFormat.dateTime(Instant.parse("2026-09-23T12:05:00Z")));
        assertEquals("—", TgFormat.dateTime(null));
        assertEquals("40 мин", TgFormat.held(Duration.ofMinutes(40)));
        assertEquals("5 ч", TgFormat.held(Duration.ofMinutes(5 * 60 + 59)));
        assertEquals("2 д 1 ч", TgFormat.held(Duration.ofHours(49)));
        assertEquals("2 д", TgFormat.held(Duration.ofHours(48)));
    }

    @Test
    @DisplayName("times are the owner's: Warsaw, summer and winter")
    void warsawTimes() {
        assertEquals("14:05", TgFormat.time(Instant.parse("2026-09-23T12:05:00Z")));
        assertEquals("13:05", TgFormat.time(Instant.parse("2026-12-01T12:05:00Z")));
    }

    @Test
    @DisplayName("ages read like speech, in Russian")
    void ages() {
        assertEquals("12 с", TgFormat.age(Duration.ofSeconds(12)));
        assertEquals("5 мин", TgFormat.age(Duration.ofMinutes(5)));
        assertEquals("3 ч 10 мин", TgFormat.age(Duration.ofMinutes(190)));
        assertEquals("3 ч", TgFormat.age(Duration.ofHours(3)));
        assertEquals("2 д 4 ч", TgFormat.age(Duration.ofHours(52)));
        assertEquals("ещё не было", TgFormat.ago(null, Instant.EPOCH));
    }

    @Test
    @DisplayName("Russian plurals: 1 позиция, 2 позиции, 5 и 11 позиций, 21 позиция")
    void plurals() {
        assertEquals("1 позиция", TgFormat.positions(1));
        assertEquals("2 позиции", TgFormat.positions(2));
        assertEquals("5 позиций", TgFormat.positions(5));
        assertEquals("11 позиций", TgFormat.positions(11));
        assertEquals("21 позиция", TgFormat.positions(21));
        assertEquals("0 позиций", TgFormat.positions(0));
    }

    @Test
    @DisplayName("escaping covers & < > and the plain-text twin undoes it exactly")
    void escapeAndPlain() {
        String raw = "A<B>&C \"q\"";
        assertEquals("A&lt;B&gt;&amp;C \"q\"", TgFormat.esc(raw));
        assertEquals(raw, TgFormat.plain("<b>" + TgFormat.esc(raw) + "</b>"));
        assertEquals("", TgFormat.esc(null));
    }

    @Test
    @DisplayName("a long table splits into parts under 4096, each closing and reopening its <pre>")
    void splitKeepsTablesValid() {
        StringBuilder sb = new StringBuilder("📒 <b>head</b>\n<pre>");
        for (int i = 0; i < 400; i++) sb.append(i == 0 ? "" : "\n").append(String.format("row %04d ........................", i));
        sb.append("</pre>\nfooter &amp; end");
        List<String> parts = TgFormat.split(sb.toString(), TgFormat.MAX_MESSAGE_CHARS);

        assertTrue(parts.size() >= 4, "parts: " + parts.size());
        StringBuilder rejoined = new StringBuilder();
        for (String p : parts) {
            assertTrue(p.length() <= TgFormat.MAX_MESSAGE_CHARS, "part of " + p.length());
            assertEquals(p.split("<pre>", -1).length, p.split("</pre>", -1).length, "balanced: " + p);
            rejoined.append(TgFormat.plain(p)).append('\n');
        }
        for (int i = 0; i < 400; i++) {
            assertTrue(rejoined.indexOf(String.format("row %04d", i)) >= 0, "row " + i + " survived");
        }
        assertTrue(parts.get(parts.size() - 1).endsWith("footer &amp; end"));
        assertEquals(List.of("short"), TgFormat.split("short", 4096));
    }

    @Test
    @DisplayName("a single line longer than a message is cut, never inside an entity")
    void hardCutAvoidsEntities() {
        String line = "x".repeat(4080) + "&amp;" + "y".repeat(100);
        List<String> parts = TgFormat.split(line, TgFormat.MAX_MESSAGE_CHARS);
        assertEquals(2, parts.size());
        for (String p : parts) {
            assertTrue(p.length() <= TgFormat.MAX_MESSAGE_CHARS);
            int amp = p.lastIndexOf('&');
            assertTrue(amp < 0 || p.indexOf(';', amp) > amp, "entity intact in: ..." + p.substring(Math.max(0, p.length() - 20)));
        }
        assertEquals(line, String.join("", parts));
    }
}
