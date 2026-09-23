package com.bot.app;

import com.bot.core.Side;
import com.bot.exec.ExecutionCoordinator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One Telegram line per trade. What matters: the owner can read an entry and an exit at a glance,
 * the P&amp;L is honest about being an estimate, quiet hours hold lines (never lose them) and hand
 * the night over as one summary on the owner's wall clock - DST included - and nothing here can
 * throw into the loop that calls it.
 */
class TradeNotifierTest {

    private static final Instant T0 = Instant.parse("2026-09-23T10:00:00Z");   // 12:00 Warsaw

    private final List<String> sent = new ArrayList<>();

    private TradeNotifier notifier(String quiet) {
        return new TradeNotifier(sent::add, TradeNotifier.QuietHours.parse(quiet));
    }

    // ─── Lines ──────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("an entry: side and coin, fill and risk, stop and take as signed moves")
    void entryLine() {
        assertEquals("🟢 <b>Вход LONG TIA</b>\nцена 0.5075 · риск $0.72\nстоп −12.9% · тейк +22.6%",
                TradeNotifier.entryHtml("TIAUSDT", Side.LONG, 0.5075, 0.4420, 0.6222, 0.72));
        assertEquals("🟢 <b>Вход SHORT BTC</b>\nцена 86437\nстоп +5.0% · тейк —",
                TradeNotifier.entryHtml("BTCUSDT", Side.SHORT, 86437, 90758.85, Double.NaN, Double.NaN));
    }

    @Test
    @DisplayName("a take after an announced entry: ≈ P&L, move, time held, entry → exit")
    void takeAfterEntry() {
        TradeNotifier n = notifier("");
        n.entry("TIAUSDT", Side.LONG, 0.5, 10, 0.45, 0.6, 0.5, T0);
        n.exit("TIAUSDT", TradeNotifier.ExitKind.TAKE, 0.6, 10, T0.plus(Duration.ofHours(31)), "");

        assertEquals(2, sent.size());
        assertEquals("✅ <b>Тейк TIA ≈+$1.00</b> (+20.0%)\nдержал 1 д 7 ч · 0.5 → 0.6", sent.get(1));
    }

    @Test
    @DisplayName("a short's stop is a loss, and says it is a short")
    void shortStop() {
        TradeNotifier n = notifier("");
        n.entry("SOLUSDT", Side.SHORT, 100, 0.1, 105, 91.25, 0.5, T0);
        n.exit("SOLUSDT", TradeNotifier.ExitKind.STOP, 105, 0.1, T0.plus(Duration.ofHours(2)), "");

        assertEquals("🛑 <b>Стоп SOL ≈−$0.50</b> (−5.0%)\nшорт · держал 2 ч · 100 → 105", sent.get(1));
    }

    @Test
    @DisplayName("a stop whose order was unreadable is priced at the stop level; size and time from /book's view")
    void stopWithoutPriceUsesTheLevel() {
        TradeNotifier n = notifier("");
        n.observe(List.of(new OperatorSnapshot.Position("ADAUSDT", Side.LONG, 10, 0.5, 0.48, -0.2, 0.45, 0.6,
                T0, true)));
        n.exit("ADAUSDT", TradeNotifier.ExitKind.STOP, Double.NaN, Double.NaN, T0.plus(Duration.ofMinutes(50)), "");

        assertEquals("🛑 <b>Стоп ADA ≈−$0.50</b> (−10.0%)\nдержал 50 мин · 0.5 → 0.45", sent.get(0));
    }

    @Test
    @DisplayName("an exit with nothing known still arrives, and says what it does not know")
    void exitWithNothingKnown() {
        TradeNotifier n = notifier("");
        n.exit("ZECUSDT", TradeNotifier.ExitKind.HAND, Double.NaN, Double.NaN, T0, "");
        n.exit("A<B>USDT", TradeNotifier.ExitKind.LIQUIDATION, 1.5, Double.NaN, T0, "");
        n.exit("XUSDT", TradeNotifier.ExitKind.OTHER, 2, 1, T0, "manual <console>");

        assertEquals("✋ <b>Закрыта в приложении ZEC</b>\nцена выхода неизвестна", sent.get(0));
        assertEquals("🔴 <b>Ликвидация A&lt;B&gt;</b>\nвыход 1.5\n<i>биржа закрыла сама — проверь счёт</i>", sent.get(1));
        assertTrue(sent.get(2).endsWith("<i>причина: manual &lt;console&gt;</i>"), sent.get(2));
    }

    @Test
    @DisplayName("a trim is a warning line, and the position stays remembered for its real exit")
    void partialKeepsThePosition() {
        TradeNotifier n = notifier("");
        n.entry("TAOUSDT", Side.LONG, 300, 2, 270, 330, 1, T0);
        n.partial("TAOUSDT", T0.plusSeconds(60));
        n.exit("TAOUSDT", TradeNotifier.ExitKind.TAKE, 330, 1, T0.plusSeconds(120), "");

        assertEquals("🟠 <b>Частичный выход TAO</b>\nпозиция уменьшилась, остаток под стопом", sent.get(1));
        assertTrue(sent.get(2).startsWith("✅ <b>Тейк TAO ≈+$30.00</b> (+10.0%)"), sent.get(2));
    }

    @Test
    @DisplayName("close reasons and exit causes map onto the chat's vocabulary")
    void kinds() {
        assertEquals(TradeNotifier.ExitKind.MANUAL, TradeNotifier.ExitKind.ofCloseReason("operator via Telegram"));
        assertEquals(TradeNotifier.ExitKind.CLOSE_ALL, TradeNotifier.ExitKind.ofCloseReason(OperatorChannel.CLOSE_ALL_REASON));
        assertEquals(TradeNotifier.ExitKind.TIME, TradeNotifier.ExitKind.ofCloseReason("max-hold"));
        assertEquals(TradeNotifier.ExitKind.TREND, TradeNotifier.ExitKind.ofCloseReason("trend-exited"));
        assertEquals(TradeNotifier.ExitKind.TREND, TradeNotifier.ExitKind.ofCloseReason("dip-exited"));
        assertEquals(TradeNotifier.ExitKind.KILL_SWITCH, TradeNotifier.ExitKind.ofCloseReason("daily-loss-kill-switch"));
        assertEquals(TradeNotifier.ExitKind.STOP_REPAIR, TradeNotifier.ExitKind.ofCloseReason("stop-repair"));
        assertEquals(TradeNotifier.ExitKind.OTHER, TradeNotifier.ExitKind.ofCloseReason("manual"));
        assertEquals(TradeNotifier.ExitKind.TAKE, TradeNotifier.ExitKind.ofCause("take-profit"));
        assertEquals(TradeNotifier.ExitKind.STOP, TradeNotifier.ExitKind.ofCause("stop-loss"));
        assertEquals(TradeNotifier.ExitKind.HAND, TradeNotifier.ExitKind.ofCause("hand-close"));
        assertEquals(TradeNotifier.ExitKind.LIQUIDATION, TradeNotifier.ExitKind.ofCause("liquidation"));
        assertEquals(TradeNotifier.ExitKind.CLOSED_EARLY, TradeNotifier.ExitKind.ofCause("closed-early"));
        assertEquals(TradeNotifier.ExitKind.UNEXPLAINED, TradeNotifier.ExitKind.ofCause("unexplained"));
        assertEquals(TradeNotifier.ExitKind.UNEXPLAINED, TradeNotifier.ExitKind.ofCause(null));
    }

    @Test
    @DisplayName("a delivery that throws never reaches the loop that announced the trade")
    void failuresStayInside() {
        TradeNotifier n = new TradeNotifier(html -> { throw new IllegalStateException("telegram down"); }, null);
        assertDoesNotThrow(() -> n.entry("TIAUSDT", Side.LONG, 0.5, 10, 0.45, 0.6, 0.5, T0));
        assertDoesNotThrow(() -> n.exit("TIAUSDT", TradeNotifier.ExitKind.TAKE, 0.6, 10, T0, ""));
        assertDoesNotThrow(() -> n.partial("TIAUSDT", T0));
        assertDoesNotThrow(() -> n.observe(null));
    }

    // ─── Wiring helpers in TestnetBot ───────────────────────────────────────────────────────

    @Test
    @DisplayName("the loop's helpers: a proven cause, a trim, a commanded close, an empty close, no notifier")
    void loopHelpers() {
        TradeNotifier n = notifier("");
        TestnetBot.notifyExchangeExit(n, "VVVUSDT", "take-profit", "23.368", "0.041");
        TestnetBot.notifyExchangeExit(n, "VVVUSDT", "partial-exit", "", "");
        TestnetBot.notifyClose(n, "ADAUSDT", "max-hold", new ExecutionCoordinator.CloseReport("ADAUSDT", true,
                new BigDecimal("10"), new BigDecimal("0.61"), "ok"));
        TestnetBot.notifyClose(n, "XRPUSDT", "operator via Telegram", new ExecutionCoordinator.CloseReport("XRPUSDT",
                true, BigDecimal.ZERO, BigDecimal.ZERO, "already flat"));

        assertEquals(3, sent.size(), "an 'already flat' close is not a fill and not a line: " + sent);
        assertTrue(sent.get(0).startsWith("✅ <b>Тейк VVV</b>\nвыход 23.368"), sent.get(0));
        assertTrue(sent.get(1).startsWith("🟠 <b>Частичный выход VVV</b>"), sent.get(1));
        assertTrue(sent.get(2).startsWith("⏱ <b>По времени ADA</b>"), sent.get(2));
        assertDoesNotThrow(() -> TestnetBot.notifyExchangeExit(null, "X", "take-profit", "1", "1"));
        assertDoesNotThrow(() -> TestnetBot.notifyClose(null, "X", "max-hold", null));
    }

    // ─── Quiet hours ────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("QUIET_HOURS parses 23-08 and 23:30-07:15; blank and off are no window; nonsense is refused")
    void quietParsing() {
        assertEquals("23:00–08:00", TradeNotifier.QuietHours.parse("23-08").label());
        assertEquals("23:30–07:15", TradeNotifier.QuietHours.parse(" 23:30 - 07:15 ").label());
        assertEquals("22:00–00:00", TradeNotifier.QuietHours.parse("22-24").label());
        assertNull(TradeNotifier.QuietHours.parse(""));
        assertNull(TradeNotifier.QuietHours.parse(null));
        assertNull(TradeNotifier.QuietHours.parse("off"));
        assertThrows(RuntimeException.class, () -> TradeNotifier.QuietHours.parse("late"));
        assertThrows(RuntimeException.class, () -> TradeNotifier.QuietHours.parse("8-8"));
        assertThrows(RuntimeException.class, () -> TradeNotifier.QuietHours.parse("25-08"));
    }

    @Test
    @DisplayName("TRADE_NOTIFY off means no notifier; a bad QUIET_HOURS costs the quiet, not the lines")
    void environment() {
        assertNull(TradeNotifier.fromEnvironmentOrNull(Map.of("TRADE_NOTIFY", "off")::get, sent::add));
        assertNull(TradeNotifier.fromEnvironmentOrNull(Map.of("TRADE_NOTIFY", "0")::get, sent::add));
        TradeNotifier on = TradeNotifier.fromEnvironmentOrNull(Map.<String, String>of()::get, sent::add);
        assertNotNull(on);
        assertNull(on.quietHours());
        TradeNotifier typo = TradeNotifier.fromEnvironmentOrNull(Map.of("QUIET_HOURS", "nights")::get, sent::add);
        assertNotNull(typo);
        assertNull(typo.quietHours());
        TradeNotifier quiet = TradeNotifier.fromEnvironmentOrNull(
                Map.of("TRADE_NOTIFY", "on", "QUIET_HOURS", "23-08")::get, sent::add);
        assertEquals("23:00–08:00", quiet.quietHours().label());
    }

    @Test
    @DisplayName("a window across midnight holds the night's lines and sends one summary at its end")
    void quietAcrossMidnight() {
        TradeNotifier n = notifier("23-08");
        n.entry("TIAUSDT", Side.LONG, 0.5, 10, 0.45, 0.6, 0.5, Instant.parse("2026-09-22T21:30:00Z"));   // 23:30
        n.exit("TIAUSDT", TradeNotifier.ExitKind.TAKE, 0.6, 10, Instant.parse("2026-09-23T03:00:00Z"), "");  // 05:00
        n.exit("ADAUSDT", TradeNotifier.ExitKind.HAND, Double.NaN, Double.NaN, Instant.parse("2026-09-23T04:00:00Z"), "");
        n.tick(Instant.parse("2026-09-23T05:59:00Z"));   // 07:59

        assertTrue(sent.isEmpty(), "nothing inside the window: " + sent);
        assertEquals(3, n.heldCount());

        n.tick(Instant.parse("2026-09-23T06:00:00Z"));   // 08:00
        assertEquals(1, sent.size());
        String summary = sent.get(0);
        assertTrue(summary.startsWith("🌙 <b>За ночь: 1 вход, 2 выхода</b>"), summary);
        assertTrue(summary.contains("💰 ≈+$1.00 по закрытым (не все с ценой)"), summary);
        assertTrue(summary.contains("\n23:30 🟢 Вход LONG TIA"), summary);
        assertTrue(summary.contains("\n05:00 ✅ Тейк TIA ≈+$1.00"), summary);
        assertTrue(summary.contains("\n06:00 ✋ Закрыта в приложении ADA"), summary);
        assertEquals(0, n.heldCount());

        n.tick(Instant.parse("2026-09-23T06:01:00Z"));
        assertEquals(1, sent.size(), "one summary, not one per tick");
    }

    @Test
    @DisplayName("the morning's first trade does not overtake the night: summary first, then the trade")
    void morningTradeFlushesFirst() {
        TradeNotifier n = notifier("23-08");
        n.partial("TAOUSDT", Instant.parse("2026-09-23T01:00:00Z"));
        n.entry("TIAUSDT", Side.LONG, 0.5, 10, 0.45, 0.6, 0.5, Instant.parse("2026-09-23T06:05:00Z"));

        assertEquals(2, sent.size());
        assertTrue(sent.get(0).startsWith("🌙 <b>За ночь: 1 событие</b>"), sent.get(0));
        assertTrue(sent.get(1).startsWith("🟢 <b>Вход LONG TIA</b>"), sent.get(1));
    }

    @Test
    @DisplayName("autumn DST: 01-04 is the owner's 01:00-04:00, an hour longer in UTC that night")
    void quietOverAutumnDst() {
        // 25.10.2026 01:00 UTC: 03:00 CEST becomes 02:00 CET.
        TradeNotifier n = notifier("01-04");
        n.exit("ADAUSDT", TradeNotifier.ExitKind.HAND, 1, 1, Instant.parse("2026-10-25T00:30:00Z"), "");  // 02:30 CEST
        n.tick(Instant.parse("2026-10-25T01:30:00Z"));   // 02:30 CET, the repeated hour
        n.tick(Instant.parse("2026-10-25T02:59:00Z"));   // 03:59 CET
        assertTrue(sent.isEmpty(), sent.toString());
        n.tick(Instant.parse("2026-10-25T03:00:00Z"));   // 04:00 CET
        assertEquals(1, sent.size());
        assertTrue(sent.get(0).contains("\n02:30 ✋ Закрыта в приложении ADA"), sent.get(0));
    }

    @Test
    @DisplayName("spring DST: 23-08 ends at 08:00 summer time, 06:00 UTC, not winter's 07:00")
    void quietOverSpringDst() {
        // 29.03.2026 01:00 UTC: 02:00 CET becomes 03:00 CEST.
        TradeNotifier n = notifier("23-08");
        n.exit("ADAUSDT", TradeNotifier.ExitKind.HAND, 1, 1, Instant.parse("2026-03-29T00:30:00Z"), "");  // 01:30 CET
        n.tick(Instant.parse("2026-03-29T05:59:00Z"));   // 07:59 CEST
        assertTrue(sent.isEmpty(), sent.toString());
        n.tick(Instant.parse("2026-03-29T06:00:00Z"));   // 08:00 CEST
        assertEquals(1, sent.size());
    }

    @Test
    @DisplayName("a daytime window is 'тихие часы', and a very long night keeps a count of what it cut")
    void daytimeWindowAndOverflow() {
        TradeNotifier n = notifier("12-14");
        for (int i = 0; i < TradeNotifier.MAX_HELD + 5; i++) {
            n.partial("C" + i + "USDT", Instant.parse("2026-09-23T10:30:00Z"));   // 12:30
        }
        n.tick(Instant.parse("2026-09-23T12:00:00Z"));   // 14:00

        String summary = sent.get(0);
        assertTrue(summary.startsWith("🌙 <b>За тихие часы: " + (TradeNotifier.MAX_HELD) + " событий</b>"), summary);
        assertTrue(summary.contains("… и ещё " + (TradeNotifier.MAX_HELD - TradeNotifier.SUMMARY_LINES + 5)), summary);
        assertTrue(summary.length() < TgFormat.MAX_MESSAGE_CHARS, "one message: " + summary.length());
    }

    @Test
    @DisplayName("outside the window lines go at once")
    void outsideTheWindow() {
        TradeNotifier n = notifier("23-08");
        n.entry("TIAUSDT", Side.LONG, 0.5, 10, 0.45, 0.6, 0.5, T0);
        assertEquals(1, sent.size());
        assertEquals(0, n.heldCount());
    }
}
