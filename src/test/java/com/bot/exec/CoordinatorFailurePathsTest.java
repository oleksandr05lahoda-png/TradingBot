package com.bot.exec;

import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.OrderTypes.OrderPurpose;
import com.bot.exec.binance.BinanceErrorCodes;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The paths where something goes wrong <i>after</i> money is committed — each one a failure that
 * could leave a leveraged position open while the report, the book or the operator says otherwise.
 */
class CoordinatorFailurePathsTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();

    private TradePlan plan() {
        return ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
    }

    private ExecutionCoordinator coordinator() {
        return ExecFixtures.coordinator(exchange, engine, halt, alerts);
    }

    @Test
    @DisplayName("a refused protective stop with a clean unwind refuses the symbol without halting")
    void refusedStopWithCleanUnwindDoesNotHalt() throws Exception {
        TradePlan plan = plan();
        exchange.placementFailure = request -> request.purpose() == OrderPurpose.STOP_LOSS
                ? ExchangeException.refused("Precision is over the maximum defined for this asset.",
                        400, BinanceErrorCodes.BAD_PRECISION)
                : null;

        ExecutionCoordinator.Report report = coordinator().execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.ABORTED_UNPROTECTED, report.outcome());
        assertTrue(report.mayHaveOpenedUnknownRisk());
        assertTrue(alerts.sawCritical("Protective stop could not be placed"), alerts.messages.toString());
        assertTrue(exchange.openPositions().isEmpty(),
                "a position that cannot be protected must be closed, not kept");
        // The unwind is confirmed flat, so the account is exactly as if the signal had been
        // refused outright. One symbol with unplaceable stops must not stop every other symbol
        // (seen live 14.08: stale conditional orders on a venue that cannot enumerate them).
        assertFalse(halt.isHalted(),
                "a clean unwind leaves nothing at risk, so the machine keeps trading other symbols");
    }

    @Test
    @DisplayName("a stop refusal whose close only partly fills is reported as still open, and halts")
    void refusedStopWithPartialCloseIsReportedHonestly() throws Exception {
        TradePlan plan = plan();
        exchange.placementFailure = request -> request.purpose() == OrderPurpose.STOP_LOSS
                ? ExchangeException.refused("rejected", 400, BinanceErrorCodes.NEW_ORDER_REJECTED)
                : null;
        exchange.fillRatio = 0.5;   // the entry AND the emergency close both fill only half

        ExecutionCoordinator.Report report = coordinator().execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.ABORTED_UNPROTECTED, report.outcome());
        assertTrue(alerts.sawCritical("Naked position"), alerts.messages.toString());
        assertTrue(report.note().contains("REMAINS OPEN AND UNPROTECTED"), report.note());
        assertFalse(exchange.openPositions().isEmpty(),
                "the residual really is there — the point is that the report says so");
        assertTrue(halt.isHalted(),
                "an unprotected remainder on the exchange is exactly what the halt latch is for");
    }

    @Test
    @DisplayName("an entry whose fate cannot be established halts instead of being logged and forgotten")
    void ambiguousEntryHalts() throws Exception {
        TradePlan plan = plan();
        // Every send fails ambiguously and the order never becomes visible: the placer exhausts its
        // probes and resends and gives up without ever learning what happened.
        exchange.placementFailure = request -> request.purpose() == OrderPurpose.ENTRY
                ? ExchangeException.ambiguous("connection reset", null)
                : null;

        ExecutionCoordinator.Report report = coordinator().execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.UNKNOWN_AFTER_SEND, report.outcome());
        assertTrue(report.mayHaveOpenedUnknownRisk(),
                "'unknown' must not read as 'nothing happened' — a position may be live");
        assertFalse(report.opened());
        assertTrue(halt.isHalted(), "the loop must not open the next position on top of an unknown one");
        assertTrue(alerts.sawCritical("Entry outcome unknown"), alerts.messages.toString());
        assertEquals(0, engine.book().openCount());
    }

    @Test
    @DisplayName("an entry the exchange definitely refused is an ordinary refusal, not a halt")
    void definitelyRefusedEntryDoesNotHalt() throws Exception {
        TradePlan plan = plan();
        exchange.placementFailure = request -> request.purpose() == OrderPurpose.ENTRY
                ? ExchangeException.refused("Margin is insufficient.", 400,
                        BinanceErrorCodes.MARGIN_NOT_SUFFICIENT)
                : null;

        ExecutionCoordinator.Report report = coordinator().execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.REFUSED, report.outcome());
        assertFalse(halt.isHalted(), "nothing landed, so there is nothing to stop trading about");
        assertTrue(exchange.openPositions().isEmpty());
    }

    @Test
    @DisplayName("a slippage abort whose close fails keeps the protective stop and halts")
    void slippageAbortWithFailedCloseKeepsTheStop() throws Exception {
        TradePlan plan = plan();
        exchange.fillPrice = new BigDecimal("64400.0");   // beyond the 20% risk overrun
        exchange.placementFailure = request -> request.purpose() == OrderPurpose.EMERGENCY_CLOSE
                ? ExchangeException.refused("ReduceOnly Order Failed.", 400,
                        BinanceErrorCodes.REDUCE_ONLY_MARGIN_CHECK_FAILED)
                : null;

        ExecutionCoordinator.Report report = coordinator().execute(plan);
        OrderStatus stop = report.protectiveStop().orElseThrow();

        assertEquals(ExecutionCoordinator.Outcome.ABORTED_ON_SLIPPAGE, report.outcome());
        assertTrue(exchange.order(stop.clientOrderId()).orElseThrow().isWorking(),
                "the close failed, so the position is still open — cancelling its stop would strip "
                        + "the protection off a position the code knows is live");
        assertTrue(halt.isHalted());
        assertTrue(alerts.sawCritical("Slippage abort could not close"), alerts.messages.toString());
    }

    @Test
    @DisplayName("a slippage abort whose close only partly fills keeps the stop and halts")
    void slippageAbortWithPartialCloseKeepsTheStop() throws Exception {
        TradePlan plan = plan();
        // Half of 0.041 is 0.020, so the fill has to be further out than in the full-size case to
        // still breach the budget: 0.020 x |66,000 - 62,800| = $64 against a planned $49.20.
        exchange.fillPrice = new BigDecimal("66000.0");
        exchange.fillRatio = 0.5;

        ExecutionCoordinator.Report report = coordinator().execute(plan);
        OrderStatus stop = report.protectiveStop().orElseThrow();

        assertEquals(ExecutionCoordinator.Outcome.ABORTED_ON_SLIPPAGE, report.outcome());
        assertTrue(report.note().contains("is still open"), report.note());
        assertTrue(exchange.order(stop.clientOrderId()).orElseThrow().isWorking());
        assertTrue(halt.isHalted());
        assertTrue(engine.book().hasPosition("BTCUSDT"),
                "the book must keep the position it could not close, or the next signal sizes "
                        + "against exposure that is still there");
    }

    @Test
    @DisplayName("a successful slippage abort does cancel the now-orphaned stop")
    void successfulSlippageAbortCancelsTheStop() throws Exception {
        TradePlan plan = plan();
        exchange.fillPrice = new BigDecimal("64400.0");

        ExecutionCoordinator.Report report = coordinator().execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.ABORTED_ON_SLIPPAGE, report.outcome());
        assertTrue(report.protectiveStop().isEmpty(),
                "the position is flat, so the stop was cancelled and the report does not claim one");
        assertTrue(exchange.openPositions().isEmpty());
        assertFalse(engine.book().hasPosition("BTCUSDT"));
        assertFalse(halt.isHalted(), "a clean abort is not a reason to stop trading");
    }

    @Test
    @DisplayName("cancelQuietly reports whether the order is actually gone")
    void cancelQuietlyDistinguishesOutcomes() {
        IdempotentOrderPlacer placer =
                new IdempotentOrderPlacer(exchange, 1, 1, 0, ExecFixtures.NO_SLEEP);

        assertTrue(placer.cancelQuietly("BTCUSDT", "bt-e0-nothing-here"),
                "an order the exchange has never heard of is not working, so this is a true 'gone'");

        exchange.cancelFailure = ExchangeException.refused("service unavailable", 500, 0);
        assertFalse(placer.cancelQuietly("BTCUSDT", "bt-e0-nothing-here"),
                "a failed cancel must not read as a successful one — the order may still be live");
    }
}
