package com.bot.exec;

import com.bot.core.Side;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The guards added by the 03.09 audit: an empty read, a re-latched halt, and an intended entry. */
class ReconcilerGuardsTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();
    private final IdempotentOrderPlacer placer = new IdempotentOrderPlacer(exchange, 3, 2, 0, ExecFixtures.NO_SLEEP);
    private final Reconciler reconciler = new Reconciler(exchange, engine, halt, alerts, placer);

    private void bookOne(String symbol, String qty, String entry) {
        exchange.plantPosition(symbol, qty, entry);
        engine.book().open(new ExposureBook.OpenPosition(symbol, Side.LONG, new BigDecimal(qty),
                Double.parseDouble(entry), Double.parseDouble(qty) * Double.parseDouble(entry), 49.2,
                Optional.of("bt-s0-" + symbol)));
    }

    private int criticals() {
        return (int) alerts.messages.stream().filter(m -> m.startsWith("CRITICAL")).count();
    }

    @Test
    @DisplayName("an empty position list while the account still reports open PnL is an unconfirmed read, not an exit")
    void emptyReadWithOpenPnlIsNotBelieved() {
        bookOne("BTCUSDT", "0.041", "64000");
        exchange.clearPosition("BTCUSDT");          // the listing comes back empty...
        exchange.setUnrealizedPnl("-1.50");          // ...while the account says something is open

        assertThrows(ExchangeException.class, () -> reconciler.reconcile(ExecFixtures.NOON));

        assertTrue(engine.book().hasPosition("BTCUSDT"), "the book (and so the ledger) is untouched");
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("a single stop-out with the PnL at zero is still absorbed on the first pass")
    void singleStopOutIsAbsorbedAtOnce() {
        bookOne("BTCUSDT", "0.041", "64000");
        exchange.clearPosition("BTCUSDT");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.healthy(), report.describe());
        assertFalse(engine.book().hasPosition("BTCUSDT"));
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("a whole book vanishing at once needs a second empty read before it is believed")
    void wholeBookGoneNeedsASecondRead() {
        bookOne("BTCUSDT", "0.041", "64000");
        bookOne("ETHUSDT", "1.5", "3000");
        bookOne("SOLUSDT", "10", "150");
        for (String s : new String[] {"BTCUSDT", "ETHUSDT", "SOLUSDT"}) exchange.clearPosition(s);

        assertThrows(ExchangeException.class, () -> reconciler.reconcile(ExecFixtures.NOON));
        assertEquals(3, engine.book().openCount(), "first empty read: nothing erased");

        Reconciler.Report second = reconciler.reconcile(ExecFixtures.NOON.plusSeconds(30));
        assertTrue(second.healthy(), second.describe());
        assertEquals(0, engine.book().openCount(), "second empty read in a row: believed");
    }

    @Test
    @DisplayName("after the operator clears the halt, the same drift is announced again at once")
    void driftAnnouncesAgainAfterResume() {
        exchange.plantPosition("BTCUSDT", "0.5", "64000");     // foreign: nothing on the book, no intent

        reconciler.reconcile(ExecFixtures.NOON);
        assertTrue(halt.isHalted());
        assertEquals(1, criticals());

        reconciler.reconcile(ExecFixtures.NOON.plusSeconds(30));
        assertEquals(1, criticals(), "inside the throttle window the same drift stays quiet");

        halt.clear();
        reconciler.reconcile(ExecFixtures.NOON.plusSeconds(60));
        assertTrue(halt.isHalted(), "the drift is still there, so it re-latches");
        assertEquals(2, criticals(), "and the operator who just cleared it hears about it immediately");
    }

    @Test
    @DisplayName("a position matching a recorded entry intent is adopted with its planned stop and repaired")
    void intendedEntryIsAdoptedAndProtected() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        EntryIntents intents = EntryIntents.inMemory();
        intents.record(plan, ExecFixtures.NOON.toEpochMilli());
        // The process died between the fill and the stop: the exchange holds the position, the
        // book knows nothing, no stop rests.
        exchange.plantPosition("BTCUSDT", plan.quantity().toPlainString(), plan.entryPrice().toPlainString());
        reconciler.withEntryIntents(intents);
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, engine, halt, alerts);
        reconciler.withStopRepair(coordinator::closeOut);

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON.plusSeconds(30));

        assertTrue(report.healthy(), report.describe());
        assertFalse(halt.isHalted(), "ours, so no halt");
        ExposureBook.OpenPosition booked = engine.book().get("BTCUSDT").orElseThrow();
        assertTrue(booked.riskUsd() > 0, "risk comes from the planned stop distance");
        assertTrue(booked.protectiveStopId().isPresent(), "the missing stop was placed");
        assertTrue(exchange.openOrders("BTCUSDT").stream()
                .anyMatch(o -> o.type() == OrderType.STOP_MARKET && o.isWorking()), "a STOP_MARKET rests");
        assertTrue(intents.get("BTCUSDT", ExecFixtures.NOON.toEpochMilli()).isEmpty(), "the intent is spent");
        assertTrue(alerts.sawCritical("Stop was missing"), alerts.messages.toString());
    }

    @Test
    @DisplayName("a hand trade with a foreign stop id is never repaired or closed by the bot")
    void foreignStopIdIsNotOurs() {
        exchange.plantPosition("ETHUSDT", "1.0", "3000");
        engine.book().open(new ExposureBook.OpenPosition("ETHUSDT", Side.LONG, BigDecimal.ONE, 3000, 3000, 30.0,
                Optional.of("web_abc123")));      // adopted from the app: risk on record, id not ours
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, engine, halt, alerts);
        reconciler.withStopRepair(coordinator::closeOut);

        Reconciler.Report report = reconciler.reconcile(Instant.parse("2026-08-09T12:00:30Z"));

        assertFalse(report.healthy(), "a naked hand position is reported, not repaired");
        assertTrue(report.describe().contains("not this process's to repair"), report.describe());
        assertFalse(exchange.openPositions().isEmpty(), "and certainly not closed");
    }
}
