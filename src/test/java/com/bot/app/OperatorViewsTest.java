package com.bot.app;

import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.risk.ExposureBook;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The operator's screens as pure functions: the first line is always the worst true state, the
 * tables fit a phone, and nothing the loop did not know is invented.
 */
class OperatorViewsTest {

    private static final Instant NOON = Instant.parse("2026-09-23T10:00:00Z");   // 12:00 in Warsaw

    private static OperatorSnapshot.Position adaLong() {
        // Entered 100, now 101, stop 95, take 110, 14 hours ago.
        return new OperatorSnapshot.Position("ADAUSDT", Side.LONG, 2, 100, 101, 2, 95, 110,
                NOON.minusSeconds(14 * 3600), true);
    }

    private static OperatorSnapshot snap(boolean hold, boolean contactLost, boolean blind, boolean kill,
                                         List<OperatorSnapshot.Position> rows, OperatorSnapshot.Pnl pnl) {
        return new OperatorSnapshot(NOON, NOON.minusSeconds(3 * 86400 + 3600), "20260923-053822Z", false, kill,
                hold ? 720_000L : 0L, contactLost, blind, blind ? 10 : 0, 0, NOON.minusSeconds(25),
                new OperatorSnapshot.Account(143.10, 120.70, 1.23), rows, true, pnl, List.of());
    }

    // ─── The first line ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("the headline is the worst true state: loop, exchange, observe, halt, daily limit, green")
    void headlineOrder() {
        Optional<String> none = Optional.empty();
        assertEquals("🟢 <b>Торгует</b>", OperatorViews.stateLine(snap(false, false, false, false, List.of(), null), none, NOON));
        assertTrue(OperatorViews.stateLine(null, none, NOON).startsWith("🟠 <b>Запуск</b>"));
        assertTrue(OperatorViews.stateLine(snap(false, false, false, true, List.of(), null), none, NOON)
                .startsWith("⏸ <b>Дневной лимит убытка</b>"));
        assertTrue(OperatorViews.stateLine(snap(false, false, false, true, List.of(), null), Optional.of("drift"), NOON)
                .startsWith("⏸ <b>Халт:</b> drift"));
        assertTrue(OperatorViews.stateLine(snap(false, false, false, false, List.of(), null),
                Optional.of(TestnetBot.OBSERVE_HALT_REASON), NOON).startsWith("👁 <b>Наблюдение</b>"));
        assertEquals("🔴 <b>Биржа не отвечает</b> — пауза ещё 12 мин",
                OperatorViews.stateLine(snap(true, false, false, false, List.of(), null), Optional.of("drift"), NOON));
        assertTrue(OperatorViews.stateLine(snap(false, true, false, false, List.of(), null), none, NOON).startsWith("🔴"));
        assertTrue(OperatorViews.stateLine(snap(false, false, true, false, List.of(), null), none, NOON)
                .contains("10 сверок подряд"));
        assertEquals("🔴 <b>Цикл молчит 5 мин</b>",
                OperatorViews.stateLine(snap(false, false, false, false, List.of(), null), none, NOON.plusSeconds(300)));
    }

    @Test
    @DisplayName("an observe-only venue never reads 🟢, whatever the latch holds - empty after /resume, or a drift")
    void observeFlagOutranksTheLatch() {
        OperatorSnapshot s = snap(false, false, false, false, List.of(), null);
        OperatorSnapshot observe = new OperatorSnapshot(s.at(), s.startedAt(), s.buildStamp(), true,
                false, 0L, false, false, 0, 0, s.lastReconcileOkAt(), s.account(), List.of(), true, null, List.of());

        assertEquals("👁 <b>Наблюдение</b> — входы выключены",
                OperatorViews.stateLine(observe, Optional.empty(), NOON), "the gap before the loop re-latches");
        List<String> drift = OperatorViews.states(observe, Optional.of("reconciliation drift: X"), NOON);
        assertEquals(List.of("👁 <b>Наблюдение</b> — входы выключены", "⏸ <b>Халт:</b> reconciliation drift: X"), drift);
        List<String> latched = OperatorViews.states(observe, Optional.of(TestnetBot.OBSERVE_HALT_REASON), NOON);
        assertEquals(1, latched.size(), "flag and latch together are said once: " + latched);
    }

    @Test
    @DisplayName("the panel file's state code is the headline /status shows, from the same list (24.09)")
    void stateCodeFollowsTheHeadline() {
        Optional<String> none = Optional.empty();
        OperatorSnapshot green = snap(false, false, false, false, List.of(), null);
        assertEquals("starting", OperatorViews.stateCode(null, none, NOON));
        assertEquals("trading", OperatorViews.stateCode(green, none, NOON));
        assertEquals("kill_switch", OperatorViews.stateCode(snap(false, false, false, true, List.of(), null), none, NOON));
        assertEquals("halt", OperatorViews.stateCode(snap(false, false, false, true, List.of(), null), Optional.of("drift"), NOON));
        assertEquals("observe", OperatorViews.stateCode(green, Optional.of(TestnetBot.OBSERVE_HALT_REASON), NOON));
        assertEquals("exchange_hold", OperatorViews.stateCode(snap(true, true, true, true, List.of(), null), Optional.of("x"), NOON));
        assertEquals("contact_lost", OperatorViews.stateCode(snap(false, true, true, false, List.of(), null), none, NOON));
        assertEquals("blind", OperatorViews.stateCode(snap(false, false, true, false, List.of(), null), none, NOON));
        assertEquals("loop_silent", OperatorViews.stateCode(green, none, NOON.plusSeconds(300)));
    }

    @Test
    @DisplayName("a tripped daily limit says how far the day fell when the switch knows (24.09)")
    void killSwitchLineNamesTheLoss() {
        OperatorSnapshot s = snap(false, false, false, true, List.of(), null);
        OperatorSnapshot withFigures = new OperatorSnapshot(s.at(), s.startedAt(), s.buildStamp(), false, true, 0L,
                false, false, 0, 0, s.lastReconcileOkAt(), s.account(), List.of(), true, null, List.of(),
                new OperatorSnapshot.KillSwitch(true, 0.03, 145.86, 0.0337, Instant.parse("2026-09-24T00:00:00Z")));
        assertEquals("⏸ <b>Дневной лимит убытка</b> −3.4% — входов нет",
                OperatorViews.stateLine(withFigures, Optional.empty(), NOON));
        assertEquals("⏸ <b>Дневной лимит убытка</b> — входов нет", OperatorViews.stateLine(s, Optional.empty(), NOON),
                "without the figures: no number invented");
    }

    @Test
    @DisplayName("KillSwitch.of: day start and loss from the switch, the next UTC midnight when tripped, NaN before the first balance")
    void killSwitchFigures() {
        com.bot.risk.DailyLossKillSwitch.Status tripped = new com.bot.risk.DailyLossKillSwitch.Status(true, "limit",
                java.time.LocalDate.parse("2026-09-23"), 145.86, -4.0, -0.915, -4.915, false);
        OperatorSnapshot.KillSwitch k = OperatorSnapshot.KillSwitch.of(tripped, 0.03);
        assertTrue(k.tripped());
        assertEquals(0.03, k.limitFrac());
        assertEquals(145.86, k.dayStartBalance());
        assertEquals(4.915 / 145.86, k.dayLossFrac(), 1e-12);
        assertEquals(Instant.parse("2026-09-24T00:00:00Z"), k.resumesAt());

        com.bot.risk.DailyLossKillSwitch.Status fresh = new com.bot.risk.DailyLossKillSwitch.Status(false, "",
                java.time.LocalDate.parse("2026-09-23"), 0.0, 0, 0, 0, false);
        OperatorSnapshot.KillSwitch unknown = OperatorSnapshot.KillSwitch.of(fresh, 0.03);
        assertTrue(Double.isNaN(unknown.dayStartBalance()), "no balance yet today is not $0");
        assertTrue(Double.isNaN(unknown.dayLossFrac()));
        assertNull(unknown.resumesAt(), "not tripped: nothing to resume");
        assertNull(OperatorSnapshot.KillSwitch.of(null, 0.03));
    }

    @Test
    @DisplayName("/help lists control and the fallback screen only (24.09)")
    void helpIsControlOnly() {
        String h = OperatorViews.help();
        for (String c : List.of("/status", "/halt", "/resume", "/close", "/menu")) assertTrue(h.contains(c), c);
        for (String c : List.of("/book", "/pnl", "/queue", "/why")) assertFalse(h.contains(c), c);
        assertTrue(h.endsWith(OperatorViews.PANEL_HINT), h);
    }

    // ─── /status ────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("/status: state first, then every other true state, then the account in a phone-width table")
    void statusScreen() {
        String s = OperatorViews.status(snap(true, false, false, false, List.of(adaLong()), null),
                Optional.of("operator via Telegram"), NOON.plusSeconds(12));
        String[] lines = s.split("\n");
        assertTrue(lines[0].startsWith("🔴 <b>Биржа не отвечает</b>"), s);
        assertEquals("⏸ Халт: operator via Telegram", lines[1], "the halt is not hidden behind the red headline");
        assertTrue(s.contains("Кошелёк    $143.10"), s);
        assertTrue(s.contains("Маржа      $23.63"), s);   // 143.10 + 1.23 - 120.70
        assertTrue(s.contains("Нереал.    +$1.23"), s);
        assertTrue(s.contains("Позиции    1 · L1 S0"), s);
        assertTrue(s.contains("Сверка     37 с назад"), s);
        assertTrue(s.contains("Работает   3 д 1 ч"), s);
        assertTrue(s.contains("Сборка     20260923-053822Z"), s);
        assertTrue(s.contains("<i>Данные 12 с назад · 12:00</i>\n"), s);
        // 24.09: the one fallback screen ends by saying where the full panel is.
        assertTrue(s.endsWith(OperatorViews.PANEL_HINT), s);
        assertTrue(TgFormat.plain(s).endsWith("Полная панель — кнопка 📊 Панель\nв лаб-боте (когда ноутбук включён)"), s);
        for (String line : TgFormat.plain(s).split("\n")) {
            assertTrue(line.length() <= 40, "fits a phone: " + line);
        }
    }

    @Test
    @DisplayName("/status before the loop has published says so instead of inventing numbers")
    void statusBeforeFirstPublish() {
        String s = OperatorViews.status(null, Optional.empty(), NOON);
        assertTrue(s.startsWith("🟠 <b>Запуск</b>"), s);
        assertFalse(s.contains("$"), s);
        assertTrue(s.endsWith(OperatorViews.PANEL_HINT), s);
    }

    @Test
    @DisplayName("/status names the coins whose closes are retrying - /queue is no longer advertised (24.09)")
    void statusNamesRetryingCloses() {
        OperatorSnapshot base = snap(false, false, false, false, List.of(adaLong()), null);
        OperatorSnapshot s = new OperatorSnapshot(base.at(), base.startedAt(), base.buildStamp(), false, false, 0L,
                false, false, 0, 1, base.lastReconcileOkAt(), base.account(), base.positions(), true, null,
                List.of(new OperatorSnapshot.QueuedClose("ADAUSDT", "stop", 1, 5, NOON.plusSeconds(30))));
        String out = OperatorViews.status(s, Optional.empty(), NOON);
        assertTrue(out.contains("🟠 Закрытия повторяются: ADA"), out);
        assertFalse(out.contains("/queue"), out);
    }

    // ─── /book ──────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("/book: P&L now, signed distance to stop and take, hours held - 34 columns wide")
    void bookScreen() {
        OperatorSnapshot.Position shortOne = new OperatorSnapshot.Position("1000PEPEUSDT", Side.SHORT, 10, 0.010,
                0.0099, 0.001, 0.0105, 0.0090, null, true);
        String b = OperatorViews.book(snap(false, false, false, false, List.of(adaLong(), shortOne), null), NOON);
        assertTrue(b.startsWith("📒 <b>Книга: 2 позиции</b>\n<pre>"), b);
        String row = OperatorViews.bookRow(adaLong(), NOON);
        assertEquals("ADA      L  +1.0%  −5.9%  +8.9% 14", row);
        assertEquals(34, row.length());
        assertEquals("1000PEPE S  +1.0%  +6.1%  −9.1%  —", OperatorViews.bookRow(shortOne, NOON));
        assertEquals(34, TgFormat.plain(OperatorViews.bookHeader()).length());
        assertTrue(b.contains("P&amp;L"), "the header's & is escaped: " + b);
        assertTrue(b.contains("💰 Нереал.: +$2.00"), b);
    }

    @Test
    @DisplayName("/book escapes a symbol with markup in it, and says 'empty' for an empty book")
    void bookEscapingAndEmpty() {
        OperatorSnapshot.Position odd = new OperatorSnapshot.Position("A<B>&USDT", Side.LONG, 1, 1, 1, 0,
                Double.NaN, Double.NaN, null, true);
        String b = OperatorViews.book(snap(false, false, false, false, List.of(odd), null), NOON);
        assertTrue(b.contains("A&lt;B&gt;&amp;"), b);
        assertFalse(b.contains("A<B>"), b);
        assertTrue(OperatorViews.book(snap(false, false, false, false, List.of(), null), NOON)
                .startsWith("📒 <b>Книга пуста</b>"));
    }

    // ─── /pnl ───────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("/pnl: today first with its share of the balance, then 7 and 30 days, net, with its age")
    void pnlScreen() {
        OperatorSnapshot.Pnl pnl = new OperatorSnapshot.Pnl(1.43, -0.40, 5.10, NOON.minusSeconds(180), null);
        String p = OperatorViews.pnl(snap(false, false, false, false, List.of(), pnl), NOON);
        assertTrue(p.startsWith("💰 <b>Сегодня +$1.43</b> (+1.0%)"), p);
        assertTrue(p.contains("7 дней       −$0.40"), p);
        assertTrue(p.contains("30 дней      +$5.10"), p);
        assertTrue(p.contains("комиссии + фандинг"), p);
        assertTrue(p.endsWith("<i>Посчитано 3 мин назад · 11:57</i>"), p);
    }

    @Test
    @DisplayName("/pnl says 'stale' after a failed refresh and 'not yet' before the first")
    void pnlStaleAndMissing() {
        OperatorSnapshot.Pnl stale = new OperatorSnapshot.Pnl(1.0, 1.0, 1.0, NOON.minusSeconds(900), NOON.minusSeconds(60));
        assertTrue(OperatorViews.pnl(snap(false, false, false, false, List.of(), stale), NOON)
                .contains("🟠 Обновить не удалось в 11:59 — цифры от 11:45"));
        OperatorSnapshot.Pnl never = new OperatorSnapshot.Pnl(Double.NaN, Double.NaN, Double.NaN, null, NOON);
        assertTrue(OperatorViews.pnl(snap(false, false, false, false, List.of(), never), NOON).startsWith("🔴"));
        assertTrue(OperatorViews.pnl(snap(false, false, false, false, List.of(), null), NOON).startsWith("💰 <b>P&amp;L ещё не посчитан"));
    }

    // ─── The rows behind /book ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("rows come from the exchange (mark from its unrealised PnL), the book's stop, the journal's take")
    void rowsJoinTheThreeSources() {
        PositionSnapshot ada = new PositionSnapshot("ADAUSDT", new BigDecimal("2"), new BigDecimal("100"), 3, true,
                new BigDecimal("2"), BigDecimal.ZERO);
        PositionSnapshot xrp = new PositionSnapshot("XRPUSDT", new BigDecimal("-2"), new BigDecimal("100"), 3, true,
                new BigDecimal("2"), BigDecimal.ZERO);
        List<ExposureBook.OpenPosition> book = List.of(
                new ExposureBook.OpenPosition("ADAUSDT", Side.LONG, new BigDecimal("2"), 100, 200, 10, Optional.of("s1")),
                new ExposureBook.OpenPosition("XRPUSDT", Side.SHORT, new BigDecimal("2"), 100, 200, 0, Optional.empty()));
        Map<String, TradeJournal.OpenMark> marks = Map.of(
                "ADAUSDT", new TradeJournal.OpenMark("LONG", NOON.minusSeconds(3600), Double.NaN, 110),
                // A stale row from an older LONG on a coin now held SHORT must not date or price it.
                "XRPUSDT", new TradeJournal.OpenMark("LONG", NOON.minusSeconds(99_999), 90, 120));

        List<OperatorSnapshot.Position> rows = OperatorSnapshot.positions(List.of(xrp, ada), book, marks);

        OperatorSnapshot.Position a = rows.get(0);
        assertEquals("ADAUSDT", a.symbol(), "sorted by symbol");
        assertEquals(101.0, a.mark(), 1e-9);
        assertEquals(95.0, a.stop(), 1e-9, "no journal stop: the book's $10 at risk over 2 units");
        assertEquals(110.0, a.take(), 1e-9);
        assertEquals(1, a.hoursHeld(NOON));
        assertTrue(a.stopOnRecord());

        OperatorSnapshot.Position x = rows.get(1);
        assertEquals(99.0, x.mark(), 1e-9, "a short earns as the price falls");
        assertEquals(1.0, x.pnlPct(), 1e-9);
        assertTrue(Double.isNaN(x.stop()) && Double.isNaN(x.take()));
        assertNull(x.openedAt());
        assertFalse(x.stopOnRecord());

        List<OperatorSnapshot.Position> bookOnly = OperatorSnapshot.positions(null, book, Map.of());
        assertEquals(2, bookOnly.size());
        assertTrue(Double.isNaN(bookOnly.get(0).mark()), "no exchange read: no price is invented");
    }

    // ─── The build stamp ────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("the build stamp comes from the image's file, else the environment, else 'unknown'")
    void buildStamp(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("BUILD_STAMP");
        assertEquals("unknown", TestnetBot.buildStamp(file, null));
        assertEquals("env-1", TestnetBot.buildStamp(file, " env-1 "));
        Files.writeString(file, "20260923-053822Z\n");
        assertEquals("20260923-053822Z", TestnetBot.buildStamp(file, "env-1"));
    }
}
