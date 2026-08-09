package com.bot.exec;

import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Closing out a symbol on request.
 *
 * <p>The ordering under test is the mirror of opening: there the stop goes on before anything else,
 * here it comes off only after the position is confirmed gone. Cancelling first would leave the
 * position naked for as long as the close takes — which is the same defect, in reverse.
 */
class ClosePathTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();

    private ExecutionCoordinator coordinator() {
        return ExecFixtures.coordinator(exchange, engine, halt, alerts);
    }

    private ExecutionCoordinator.Report openOne() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        return coordinator().execute(plan);
    }

    @Test
    @DisplayName("a close flattens the position and only then cancels the protective orders")
    void closeFlattensThenCancels() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        assertTrue(exchange.order(stopId).orElseThrow().isWorking());

        ExecutionCoordinator.CloseReport report = coordinator().closeOut("BTCUSDT", "close-1");

        assertTrue(report.flat(), report.note());
        assertEquals(0, report.closedQuantity().compareTo(new BigDecimal("0.041")));
        assertTrue(exchange.openPositions().isEmpty(), "the position should be gone");
        assertFalse(exchange.order(stopId).orElseThrow().isWorking(),
                "the stop is an orphan once the position is flat, so it goes — but only then");
        assertFalse(engine.book().hasPosition("BTCUSDT"));
        assertFalse(halt.isHalted(), "an ordinary close is not a reason to stop trading");
    }

    @Test
    @DisplayName("closing an already-flat symbol cancels leftovers and reports flat")
    void closingWhenAlreadyFlatIsSafe() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        exchange.clearPosition("BTCUSDT");   // the stop fired while the bot was not looking

        ExecutionCoordinator.CloseReport report = coordinator().closeOut("BTCUSDT", "close-2");

        assertTrue(report.flat());
        assertEquals(0, report.closedQuantity().compareTo(BigDecimal.ZERO));
        assertFalse(exchange.order(stopId).orElseThrow().isWorking(),
                "orders left behind by a position that no longer exists are unmanaged risk");
    }

    @Test
    @DisplayName("a close that fills only partly keeps the stop and halts")
    void partialCloseKeepsTheStop() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        exchange.fillRatio = 0.5;

        ExecutionCoordinator.CloseReport report = coordinator().closeOut("BTCUSDT", "close-3");

        assertFalse(report.flat());
        assertTrue(report.note().contains("still open"), report.note());
        assertTrue(exchange.order(stopId).orElseThrow().isWorking(),
                "half the position is still there — removing its stop would be the worst possible move");
        assertTrue(halt.isHalted());
        assertTrue(alerts.sawCritical("Partial close"), alerts.messages.toString());
    }

    @Test
    @DisplayName("a refused close keeps everything in place and halts")
    void refusedCloseKeepsEverything() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        exchange.placementFailure = request -> ExchangeException.refused(
                "ReduceOnly Order is rejected.", 400,
                com.bot.exec.binance.BinanceErrorCodes.REDUCE_ONLY_REJECT);

        ExecutionCoordinator.CloseReport report = coordinator().closeOut("BTCUSDT", "close-4");

        assertFalse(report.flat());
        assertFalse(exchange.openPositions().isEmpty(), "the position is still open");
        assertTrue(exchange.order(stopId).orElseThrow().isWorking(), "and still protected");
        assertTrue(halt.isHalted());
        assertTrue(alerts.sawCritical("Close failed"), alerts.messages.toString());
    }

    @Test
    @DisplayName("a halt does not block a close — it only blocks opening")
    void haltDoesNotBlockClosing() throws Exception {
        openOne();
        halt.halt("something went wrong elsewhere", ExecFixtures.NOON);

        ExecutionCoordinator.CloseReport report = coordinator().closeOut("BTCUSDT", "close-5");

        assertTrue(report.flat(),
                "a lock that seals positions in is not a safety feature — it is a way to be unable "
                        + "to exit a losing trade");
        assertTrue(exchange.openPositions().isEmpty());
    }

    @Test
    @DisplayName("closing the same request twice does not send a second order")
    void closeIsIdempotent() throws Exception {
        openOne();
        coordinator().closeOut("BTCUSDT", "close-6");
        int callsAfterFirst = exchange.placeOrderCalls;

        // The row was re-delivered; the symbol is already flat, so nothing further is sent.
        ExecutionCoordinator.CloseReport again = coordinator().closeOut("BTCUSDT", "close-6");

        assertTrue(again.flat());
        assertEquals(callsAfterFirst, exchange.placeOrderCalls,
                "a redelivered close must not open an opposite position");
    }
}
