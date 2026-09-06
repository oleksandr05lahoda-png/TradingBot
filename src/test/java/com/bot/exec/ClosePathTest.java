package com.bot.exec;

import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Closing out a symbol on request. The stop comes off only after the position is confirmed gone;
 * cancelling first would leave the position naked for as long as the close takes.
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
    @DisplayName("an already-flat close leaves the book entry for the reconciler, which journals the exchange-side exit")
    void alreadyFlatLeavesTheExitToTheReconciler() throws Exception {
        openOne();
        exchange.clearPosition("BTCUSDT");   // the stop fired, or a close whose response was lost filled

        ExecutionCoordinator.CloseReport report = coordinator().closeOut("BTCUSDT", "close-7");

        assertTrue(report.flat());
        assertTrue(engine.book().hasPosition("BTCUSDT"),
                "closing the book here erased the exit from the journal: no 'close' row (nothing "
                        + "filled) and no ghost for the reconciler to record either");

        java.util.List<String> exits = new java.util.ArrayList<>();
        Reconciler reconciler = new Reconciler(exchange, engine, halt, alerts,
                new IdempotentOrderPlacer(exchange, 1, 1, 0, ExecFixtures.NO_SLEEP));
        reconciler.onExchangeExit((symbol, detail) -> exits.add(symbol));
        reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(java.util.List.of("BTCUSDT"), exits, "the exit must reach the journal once");
        assertFalse(engine.book().hasPosition("BTCUSDT"));
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("a take filling between the read and the send trims the reduce-only close: flat, not a partial-close halt")
    void trimmedCloseAfterATakeIsAFullClose() throws Exception {
        DelegatingExchange venue = new DelegatingExchange() {
            @Override public OrderStatus placeOrder(OrderRequest request) {
                if (request.purpose() != com.bot.exec.OrderTypes.OrderPurpose.EMERGENCY_CLOSE) {
                    return delegate.placeOrder(request);
                }
                // The take leg filled the whole position a second before this send. Binance trims
                // a reduce-only market order to what is left, which is nothing: no error, a
                // response with executedQty 0 against a symbol that is now flat.
                delegate.clearPosition(request.symbol());
                return new OrderStatus(request.clientOrderId(), 99L, request.symbol(),
                        com.bot.exec.OrderTypes.OrderState.EXPIRED, com.bot.exec.OrderTypes.OrderType.MARKET,
                        request.quantity(), BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                        true, false, delegate.serverTimeMillis());
            }
        };
        ExecutionCoordinator coordinator = new ExecutionCoordinator(venue, engine,
                new IdempotentOrderPlacer(venue, 3, 2, 0, ExecFixtures.NO_SLEEP), halt, alerts,
                ExecutionCoordinator.Settings.defaults(), ExecFixtures.CLOCK, ExecFixtures.NO_SLEEP);
        TradePlan plan = ExecFixtures.approvedPlan(engine, venue.fetchFilters("BTCUSDT"));
        ExecutionCoordinator.Report opened = coordinator.execute(plan);
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();

        ExecutionCoordinator.CloseReport report = coordinator.closeOut("BTCUSDT", "close-8");

        assertTrue(report.flat(), report.note());
        assertFalse(halt.isHalted(), "a symbol that IS flat must not latch a partial-close halt");
        assertFalse(alerts.sawCritical("Partial close"), alerts.messages.toString());
        assertFalse(engine.book().hasPosition("BTCUSDT"));
        assertFalse(venue.delegate.order(stopId).orElseThrow().isWorking(), "leftovers cancelled");
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
    @DisplayName("a refused close keeps everything in place and reports it — the latch belongs to the retries")
    void refusedCloseKeepsEverything() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        exchange.placementFailure = request -> ExchangeException.refused(
                "ReduceOnly Order is rejected.", 400,
                com.bot.exec.binance.BinanceErrorCodes.REDUCE_ONLY_REJECT);

        ExecutionCoordinator.CloseReport report = coordinator().closeOut("BTCUSDT", "close-4");

        assertFalse(report.flat());
        assertTrue(report.note().contains("refused"), report.note());
        assertFalse(exchange.openPositions().isEmpty(), "the position is still open");
        assertTrue(exchange.order(stopId).orElseThrow().isWorking(), "and still protected");
        // One refused attempt is not an incident: the caller retries with backoff and latches the
        // halt only when the retries are spent (audit 03.09). Halting here stood the scanner down
        // for the life of the process over a 429 the second attempt sailed through.
        assertFalse(halt.isHalted(), "a single refusal must not latch the halt");
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

    @Test
    @DisplayName("a close whose response is lost but whose position reads flat is a close, not a refusal")
    void ambiguousCloseThatFlattenedIsReportedAsClosed() throws Exception {
        ExecutionCoordinator.Report opened = openOne();
        String stopId = opened.protectiveStop().orElseThrow().clientOrderId();
        // The order executes, the response is lost, and the placer's probes see nothing.
        exchange.loseNextResponse = true;
        exchange.hideNextQueries = 5;

        ExecutionCoordinator.CloseReport report = coordinator().closeOut("BTCUSDT", "close-amb");

        assertTrue(report.flat(), "the position is flat on the exchange: " + report.note());
        assertFalse(report.note().contains("still in place"), report.note());
        assertEquals(0, report.closedQuantity().compareTo(new BigDecimal("0.041")));
        assertFalse(engine.book().hasPosition("BTCUSDT"), "the book follows the confirmed flat read");
        assertFalse(exchange.order(stopId).orElseThrow().isWorking(), "the orphaned stop is cancelled");
        assertFalse(halt.isHalted());
    }
}
