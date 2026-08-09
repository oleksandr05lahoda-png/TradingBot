package com.bot.exec;

import com.bot.core.Side;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reconciliation: the exchange wins, every disagreement halts trading, and a position with no stop
 * is the loudest disagreement of all.
 */
class ReconciliationDriftTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();
    private final Reconciler reconciler = new Reconciler(exchange, engine, halt, alerts,
            new IdempotentOrderPlacer(exchange, 1, 1, 0, ExecFixtures.NO_SLEEP));

    @Test
    @DisplayName("an empty exchange and an empty book converge quietly")
    void emptyConverges() {
        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);
        assertTrue(report.converged(), report.describe());
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("a position the book does not know about is drift, and it halts trading")
    void unknownPositionHalts() {
        exchange.plantPosition("BTCUSDT", "0.500", "64000");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertFalse(report.converged());
        assertEquals(Reconciler.Drift.Kind.UNKNOWN_POSITION, report.drifts().get(0).kind());
        assertTrue(halt.isHalted(), "an unexplained position must stop new risk being taken");
        assertTrue(alerts.sawCritical("Reconciliation drift"), alerts.messages.toString());
    }

    @Test
    @DisplayName("the exchange's truth replaces the local book, it does not merge with it")
    void exchangeTruthReplacesTheBook() {
        exchange.plantPosition("BTCUSDT", "0.500", "64000");
        reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(1, engine.book().openCount());
        ExposureBook.OpenPosition adopted = engine.book().get("BTCUSDT").orElseThrow();
        assertEquals(Side.LONG, adopted.side());
        assertEquals(0, adopted.quantity().compareTo(new BigDecimal("0.500")));
        assertEquals(32_000.0, adopted.notionalUsd(), 1e-6);
    }

    @Test
    @DisplayName("a position the book holds but the exchange has closed is drift")
    void ghostPositionIsDrift() {
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2));

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(Reconciler.Drift.Kind.GHOST_POSITION, report.drifts().get(0).kind());
        assertEquals(0, engine.book().openCount(), "the book must follow the exchange, which is flat");
        assertTrue(halt.isHalted());
    }

    @Test
    @DisplayName("the same symbol in the opposite direction is a side mismatch, not a size difference")
    void sideMismatchIsItsOwnKind() {
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.SHORT,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2));
        exchange.plantPosition("BTCUSDT", "0.041", "64000");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.drifts().stream().anyMatch(d -> d.kind() == Reconciler.Drift.Kind.SIDE_MISMATCH),
                report.describe());
        assertEquals(Side.LONG, engine.book().get("BTCUSDT").orElseThrow().side());
    }

    @Test
    @DisplayName("a size difference beyond tolerance is drift")
    void quantityMismatchIsDrift() {
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2));
        exchange.plantPosition("BTCUSDT", "0.082", "64000");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.drifts().stream().anyMatch(d -> d.kind() == Reconciler.Drift.Kind.QUANTITY_MISMATCH),
                report.describe());
        assertEquals(0, engine.book().get("BTCUSDT").orElseThrow().quantity()
                .compareTo(new BigDecimal("0.082")));
    }

    @Test
    @DisplayName("an open position with no working stop is the drift that matters most")
    void positionWithoutAStopIsCritical() throws Exception {
        // Execute normally, then cancel the stop behind the bot's back — the state a crash between
        // "entry filled" and "stop placed" would leave.
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, engine, halt, alerts);
        ExecutionCoordinator.Report execution = coordinator.execute(plan);
        exchange.cancelOrder("BTCUSDT", execution.protectiveStop().orElseThrow().clientOrderId());

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.drifts().stream()
                        .anyMatch(d -> d.kind() == Reconciler.Drift.Kind.POSITION_WITHOUT_STOP),
                report.describe());
        assertTrue(report.drifts().stream().anyMatch(Reconciler.Drift::isCritical));
        assertTrue(halt.isHalted());
    }

    @Test
    @DisplayName("a converged pass leaves a protected position alone")
    void protectedPositionConverges() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, engine, halt, alerts);
        coordinator.execute(plan);

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.converged(), report.describe());
        assertFalse(halt.isHalted());
        assertEquals(1, report.exchangePositions());
    }

    @Test
    @DisplayName("realised PnL for the daily limit is re-seeded from the exchange, not from local memory")
    void realisedPnlIsSeededFromTheExchange() {
        exchange.setWalletBalance("9500");
        exchange.setRealizedPnlToday(-500);

        reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(-500.0, engine.killSwitch().evaluate(ExecFixtures.NOON).realizedPnl(), 1e-9,
                "a restart must not be able to clear the day's loss");
    }

    @Test
    @DisplayName("bootstrap refuses to start trading when the start-up state does not converge")
    void bootstrapRefusesOnDrift() {
        exchange.plantPosition("ETHUSDT", "-2.0", "3000");
        assertFalse(reconciler.bootstrap(ExecFixtures.NOON));
        assertTrue(halt.isHalted());
    }
}
