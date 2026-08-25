package com.bot.exec;

import com.bot.core.Side;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

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
    @DisplayName("a stop-out is absorbed and announced, never halted on — 24/7 depends on this")
    void ghostPositionIsDrift() {
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, Optional.empty()));

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(Reconciler.Drift.Kind.GHOST_POSITION, report.drifts().get(0).kind());
        assertEquals(0, engine.book().openCount(), "the book must follow the exchange, which is flat");
        assertFalse(halt.isHalted(),
                "an exchange-side exit is the machine WORKING; halting on it froze the bot on 21.08");
        assertFalse(report.healthy() && report.converged(), "the exit must still be reported");
    }

    @Test
    @DisplayName("the same symbol in the opposite direction is a side mismatch, not a size difference")
    void sideMismatchIsItsOwnKind() {
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.SHORT,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, Optional.empty()));
        exchange.plantPosition("BTCUSDT", "0.041", "64000");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.drifts().stream().anyMatch(d -> d.kind() == Reconciler.Drift.Kind.SIDE_MISMATCH),
                report.describe());
        assertEquals(Side.LONG, engine.book().get("BTCUSDT").orElseThrow().side());
    }

    @Test
    @DisplayName("a position that GREW behind the bot's back is critical: someone else is trading here")
    void positionThatGrewHalts() {
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, Optional.empty()));
        exchange.plantPosition("BTCUSDT", "0.082", "64000");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.drifts().stream().anyMatch(d -> d.kind() == Reconciler.Drift.Kind.POSITION_GREW),
                report.describe());
        assertTrue(halt.isHalted(), "risk this process did not take must stop new entries");
        assertEquals(0, engine.book().get("BTCUSDT").orElseThrow().quantity()
                .compareTo(new BigDecimal("0.082")));
    }

    @Test
    @DisplayName("a position that shrank is a partial exit: realigned and announced, not halted on")
    void partialExitDoesNotHalt() throws Exception {
        // A properly protected position whose TP leg then fills half of it on the exchange.
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecFixtures.coordinator(exchange, engine, halt, alerts).execute(plan);
        BigDecimal half = engine.book().get("BTCUSDT").orElseThrow()
                .quantity().divide(new BigDecimal("2"));
        exchange.plantPosition("BTCUSDT", half.toPlainString(), "64000");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.drifts().stream().anyMatch(d -> d.kind() == Reconciler.Drift.Kind.QUANTITY_MISMATCH),
                report.describe());
        assertFalse(halt.isHalted(), "the closePosition stop still covers the remainder");
        assertEquals(0, engine.book().get("BTCUSDT").orElseThrow().quantity().compareTo(half));
    }

    @Test
    @DisplayName("a boot over a stop-out that happened while the process was down still starts")
    void bootstrapSurvivesAnExchangeSideExit() {
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, Optional.empty()));
        // Exchange is flat: the stop fired overnight. Boot must absorb this, not refuse to trade.
        assertTrue(reconciler.bootstrap(ExecFixtures.NOON),
                "a finished trade while the process was down is history, not danger");
        assertFalse(halt.isHalted());
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
    @DisplayName("the exchange's own liquidation price is measured against the resting stop")
    void liquidationBufferIsRecheckedAgainstTheExchange() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, engine, halt, alerts);
        coordinator.execute(plan);

        // Entry 64,000, stop 62,800. A liquidation price of 63,300 leaves only
        // (62,800 - 63,300) ... on the wrong side entirely: the stop is now BEYOND liquidation.
        exchange.reportedLiquidationPrice = new BigDecimal("63300.0");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.drifts().stream()
                        .anyMatch(d -> d.kind() == Reconciler.Drift.Kind.LIQUIDATION_BUFFER_BREACHED),
                report.describe());
        assertTrue(halt.isHalted(),
                "a stop that is no longer inside liquidation is not a situation to keep trading through");
    }

    @Test
    @DisplayName("a comfortable liquidation price raises nothing")
    void healthyLiquidationBufferIsQuiet() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecFixtures.coordinator(exchange, engine, halt, alerts).execute(plan);

        // Liquidation at 50,000: the stop at 62,800 leaves (62,800-50,000)/(64,000-50,000) = 91%.
        exchange.reportedLiquidationPrice = new BigDecimal("50000.0");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.converged(), report.describe());
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("a venue that cannot list conditional orders still confirms the stop by name")
    void stopIsConfirmedByNameWhenItCannotBeListed() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator.Report execution =
                ExecFixtures.coordinator(exchange, engine, halt, alerts).execute(plan);
        String stopId = execution.protectiveStop().orElseThrow().clientOrderId();
        exchange.listsConditionalOrders = false;   // demo-fapi: accepted, resting, invisible to the listing

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.converged(), report.describe());
        assertFalse(halt.isHalted());
        assertEquals(Optional.of(stopId), engine.book().get("BTCUSDT").orElseThrow().protectiveStopId(),
                "the id must survive the pass, or the next one has nothing to ask about");
    }

    @Test
    @DisplayName("a stop cancelled behind the bot's back is caught by name, not by the listing")
    void cancelledStopIsCaughtByName() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator.Report execution =
                ExecFixtures.coordinator(exchange, engine, halt, alerts).execute(plan);
        exchange.cancelOrder("BTCUSDT", execution.protectiveStop().orElseThrow().clientOrderId());
        exchange.listsConditionalOrders = false;

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.drifts().stream()
                        .anyMatch(d -> d.kind() == Reconciler.Drift.Kind.POSITION_WITHOUT_STOP),
                "this is the gap the named check exists to close: " + report.describe());
        assertTrue(halt.isHalted());
    }

    @Test
    @DisplayName("on a listing venue, a stop the listing missed but the name confirms is not an emergency")
    void listingLagIsCheckedByNameBeforeAlarming() throws Exception {
        // The race of 22.08's audit: positions read first, orders seconds later, and the stop
        // is momentarily absent from the listing while still answering by name.
        DelegatingExchange lagging = new DelegatingExchange() {
            @Override public java.util.List<ExchangeSnapshots.OrderStatus> openOrders(String symbol) {
                return delegate.openOrders(symbol).stream()
                        .filter(o -> o.type() != OrderTypes.OrderType.STOP_MARKET).toList();
            }
        };
        TradePlan plan = ExecFixtures.approvedPlan(engine, lagging.delegate.fetchFilters("BTCUSDT"));
        ExecFixtures.coordinator(lagging.delegate, engine, halt, alerts).execute(plan);
        Reconciler viaLagging = new Reconciler(lagging, engine, halt, alerts,
                new IdempotentOrderPlacer(lagging, 1, 1, 0, ExecFixtures.NO_SLEEP));

        Reconciler.Report report = viaLagging.reconcile(ExecFixtures.NOON);

        assertTrue(report.converged(), report.describe());
        assertFalse(halt.isHalted(), "a stop that answers by name is protection, whatever the listing says");
    }

    @Test
    @DisplayName("on a listing venue, a position with no recorded stop id and no listed stop is still naked")
    void listingVenueWithoutAnyEvidenceStillAlarms() {
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, Optional.empty()));
        exchange.plantPosition("BTCUSDT", "0.041", "64000");

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.drifts().stream().anyMatch(d -> d.kind() == Reconciler.Drift.Kind.POSITION_WITHOUT_STOP),
                report.describe());
        assertTrue(halt.isHalted());
    }

    @Test
    @DisplayName("a stop the venue retired while a take-profit fills gets the same one pass of grace")
    void expiredStopBehindATakeProfitIsGivenOnePass() throws Exception {
        // STXUSDT, 25.08 06:08:58: the take triggered, Binance retired the stop the same instant,
        // and the reconciler read the gap sixteen seconds before the close landed for +$1.19.
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator.Report execution =
                ExecFixtures.coordinator(exchange, engine, halt, alerts).execute(plan);
        exchange.listsConditionalOrders = false;
        exchange.setOrderState(execution.protectiveStop().orElseThrow().clientOrderId(),
                OrderTypes.OrderState.EXPIRED);

        Reconciler.Report first = reconciler.reconcile(ExecFixtures.NOON);
        assertTrue(first.converged(), "one pass of grace: the position is on its way out — " + first.describe());
        assertFalse(halt.isHalted(), "a winning take-profit must not freeze the machine");

        // Still open a pass later with no stop: that IS the emergency.
        Reconciler.Report second = reconciler.reconcile(ExecFixtures.NOON);
        assertTrue(second.drifts().stream()
                        .anyMatch(d -> d.kind() == Reconciler.Drift.Kind.POSITION_WITHOUT_STOP),
                second.describe());
        assertTrue(halt.isHalted());
    }

    @Test
    @DisplayName("a stop that has just triggered gets one pass of grace, then counts as naked")
    void triggeredStopIsGivenOnePassThenReported() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator.Report execution =
                ExecFixtures.coordinator(exchange, engine, halt, alerts).execute(plan);
        exchange.listsConditionalOrders = false;
        // Fired, but the position is still there: normal for a second, an emergency if it persists.
        exchange.setOrderState(execution.protectiveStop().orElseThrow().clientOrderId(),
                OrderTypes.OrderState.FILLED);

        Reconciler.Report first = reconciler.reconcile(ExecFixtures.NOON);
        assertTrue(first.converged(),
                "a stop-out in flight must not raise a critical alert: " + first.describe());

        Reconciler.Report second = reconciler.reconcile(ExecFixtures.NOON.plusSeconds(30));
        assertTrue(second.drifts().stream()
                        .anyMatch(d -> d.kind() == Reconciler.Drift.Kind.POSITION_WITHOUT_STOP),
                "a trigger whose close never landed leaves a naked position: " + second.describe());
        assertTrue(halt.isHalted());
    }

    @Test
    @DisplayName("a stop status this build cannot parse is ignorance, not a naked position")
    void unreadableStopStatusIsNotReportedAsNaked() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator.Report execution =
                ExecFixtures.coordinator(exchange, engine, halt, alerts).execute(plan);
        exchange.listsConditionalOrders = false;
        exchange.setOrderState(execution.protectiveStop().orElseThrow().clientOrderId(),
                OrderTypes.OrderState.UNKNOWN);

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.converged(), "a vocabulary change must not become a false emergency: "
                + report.describe());
    }

    @Test
    @DisplayName("with no stop id on record the reconciler stays quiet rather than crying wolf")
    void positionWithNoRecordedStopIsNotDeclaredNaked() {
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, Optional.empty()));
        exchange.plantPosition("BTCUSDT", "0.041", "64000");
        exchange.listsConditionalOrders = false;

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(report.converged(),
                "nothing is known either way, so nothing is claimed: " + report.describe());
    }

    @Test
    @DisplayName("a working order still inside the grace window is re-inspected on the next pass")
    void orphanGraceWindowIsRevisited() throws Exception {
        // An order young enough to be a race on this pass. Its symbol holds no position, so without
        // carry-over it would drop off the inspection list and never be looked at again.
        exchange.placeOrder(OrderRequest.limitEntry("BTCUSDT", com.bot.exec.OrderTypes.OrderSide.BUY,
                new java.math.BigDecimal("0.001"), new java.math.BigDecimal("60000.0"),
                com.bot.exec.OrderTypes.TimeInForce.GTC,
                ClientOrderIdFactory.create("stale-signal", com.bot.exec.OrderTypes.OrderPurpose.ENTRY, 0)));
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, Optional.empty()));

        // Pass 1: the book knows the symbol, so it is inspected; the order is younger than the grace.
        reconciler.reconcile(ExecFixtures.NOON);
        engine.book().close("BTCUSDT");   // whatever the book thought is gone now

        // Pass 2 is where the old code lost sight of the symbol entirely. Now it is carried over,
        // and by this point the order is older than the 60s grace window.
        Reconciler.Report second = reconciler.reconcile(ExecFixtures.NOON.plusSeconds(3600));

        assertTrue(second.drifts().stream().anyMatch(d -> d.kind() == Reconciler.Drift.Kind.ORPHAN_ORDER),
                "an order that was too young on one pass must still be reachable on the next: "
                        + second.describe());
    }

    @Test
    @DisplayName("bootstrap refuses to start trading when the start-up state does not converge")
    void bootstrapRefusesOnDrift() {
        exchange.plantPosition("ETHUSDT", "-2.0", "3000");
        assertFalse(reconciler.bootstrap(ExecFixtures.NOON));
        assertTrue(halt.isHalted());
    }
}
