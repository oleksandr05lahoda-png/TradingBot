package com.bot.exec;

import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The missing-stop repair (28.08): a booked position found without a stop gets the stop re-placed
 * from the book's own record, or is closed reduce-only — the halt latch alone left the position
 * riding naked until a human typed /close. Only a position this bot sized is touched; an adopted
 * one (riskUsd 0) keeps the old halt-only behaviour.
 */
class ReconcilerStopRepairTest {

    private static final String STOP_ID = ClientOrderIdFactory.create("old-signal",
            OrderTypes.OrderPurpose.STOP_LOSS, 0);

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();

    /** LONG 0.041 @ 64000, risk $49.2 → recorded stop distance $1200 → stop at 62800. */
    private void bookedPosition(Optional<String> stopId) {
        exchange.plantPosition("BTCUSDT", "0.041", "64000");
        engine.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, stopId));
    }

    private Reconciler reconcilerWith(ExchangePort port, Reconciler.PositionCloser closer) {
        Reconciler reconciler = new Reconciler(port, engine, halt, alerts,
                new IdempotentOrderPlacer(port, 1, 1, 0, ExecFixtures.NO_SLEEP));
        if (closer != null) reconciler.withStopRepair(closer);
        return reconciler;
    }

    @Test
    @DisplayName("a naked booked position gets its stop re-placed from the book's record — no halt")
    void nakedPositionGetsStopReplaced() {
        bookedPosition(Optional.of(STOP_ID));   // recorded, but nothing rests on the exchange

        List<String> closes = new ArrayList<>();
        Reconciler reconciler = reconcilerWith(exchange, (symbol, requestId) -> {
            closes.add(symbol);
            throw new IllegalStateException("the close path must not run when the stop can be placed");
        });

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(Reconciler.Drift.Kind.STOP_REPLACED, report.drifts().get(0).kind(),
                report.describe());
        assertTrue(report.healthy(), "a repaired stop is not a halt-worthy drift");
        assertFalse(halt.isHalted());
        assertTrue(closes.isEmpty());

        String newStopId = engine.book().get("BTCUSDT").orElseThrow().protectiveStopId().orElseThrow();
        assertNotEquals(STOP_ID, newStopId, "the replacement is a fresh order, not the dead id");
        OrderStatus placed = exchange.order(newStopId).orElseThrow();
        assertTrue(placed.isWorking());
        assertEquals(OrderType.STOP_MARKET, placed.type());
        assertTrue(placed.reduceOnly() || placed.closePosition(),
                "a replacement stop must not be able to flip the position");
        assertEquals(0, placed.stopPrice().compareTo(new BigDecimal("62800.0")),
                "entry − riskUsd/quantity reconstructs the recorded stop, got " + placed.stopPrice());
        assertTrue(alerts.sawCritical("re-placed"), alerts.messages.toString());
    }

    @Test
    @DisplayName("when the venue refuses the stop, the position is closed reduce-only — no halt")
    void refusedStopFallsBackToClose() throws Exception {
        bookedPosition(Optional.of(STOP_ID));
        exchange.placementFailure = request ->
                request.type() == OrderType.STOP_MARKET
                        ? ExchangeException.refused("conditional order cap", 400, -4045)
                        : null;

        List<String> closes = new ArrayList<>();
        Reconciler reconciler = reconcilerWith(exchange, (symbol, requestId) -> {
            closes.add(symbol + "/" + requestId);
            exchange.clearPosition(symbol);
            engine.registerClose(symbol);
            return new ExecutionCoordinator.CloseReport(symbol, true,
                    new BigDecimal("0.041"), new BigDecimal("63000"), "closed reduce-only in full");
        });

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(List.of(Reconciler.Drift.Kind.UNPROTECTED_CLOSED),
                report.drifts().stream().map(Reconciler.Drift::kind).toList(), report.describe());
        assertTrue(report.healthy(), report.describe());
        assertFalse(halt.isHalted(), "closed flat is a resolved emergency, not a latched one");
        assertEquals(1, closes.size());
        assertTrue(alerts.sawCritical("Unprotected position closed"), alerts.messages.toString());
    }

    @Test
    @DisplayName("stop refused AND close failing is the old emergency: drift, alert, halt")
    void bothLegsFailingStillHalts() {
        bookedPosition(Optional.of(STOP_ID));
        exchange.placementFailure = request ->
                request.type() == OrderType.STOP_MARKET
                        ? ExchangeException.refused("conditional order cap", 400, -4045)
                        : null;

        Reconciler reconciler = reconcilerWith(exchange, (symbol, requestId) -> {
            throw ExchangeException.refused("close also refused", 400, -2019);
        });

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(Reconciler.Drift.Kind.POSITION_WITHOUT_STOP, report.drifts().get(0).kind());
        assertFalse(report.healthy());
        assertTrue(halt.isHalted());
    }

    @Test
    @DisplayName("an adopted position (riskUsd 0) is never repaired or closed — halt, as before")
    void adoptedPositionIsNotTouched() {
        // Planted on the exchange only: the pass adopts it with no stop distance on record.
        exchange.plantPosition("ETHUSDT", "0.500", "3000");

        List<String> closes = new ArrayList<>();
        Reconciler reconciler = reconcilerWith(exchange, (symbol, requestId) -> {
            closes.add(symbol);
            return new ExecutionCoordinator.CloseReport(symbol, true,
                    BigDecimal.ZERO, BigDecimal.ZERO, "should not happen");
        });

        Reconciler.Report report = reconciler.reconcile(ExecFixtures.NOON);

        assertFalse(report.healthy());
        assertTrue(halt.isHalted(), "a position this bot cannot price is not its to fix");
        assertTrue(closes.isEmpty(), "flattening what the owner may hold by hand is not our call");
    }

    @Test
    @DisplayName("three passes of an unreadable stop stop being caution and trigger the repair")
    void unconfirmableStopEscalatesOnTheThirdPass() {
        bookedPosition(Optional.of(STOP_ID));

        // The recorded id is unreadable (network-shaped failure); everything else answers.
        DelegatingExchange flaky = new DelegatingExchange() {
            @Override public Optional<OrderStatus> queryOrder(String symbol, String clientOrderId) {
                if (STOP_ID.equals(clientOrderId)) {
                    throw ExchangeException.ambiguous("timeout reading the stop", null);
                }
                return exchange.queryOrder(symbol, clientOrderId);
            }
            @Override public ExchangeSnapshots.AccountSnapshot fetchAccount() { return exchange.fetchAccount(); }
            @Override public List<ExchangeSnapshots.PositionSnapshot> openPositions() { return exchange.openPositions(); }
            @Override public List<OrderStatus> openOrders(String symbol) { return List.of(); }
            @Override public boolean canListConditionalOrders() { return false; }
            @Override public double fetchRealizedPnlSince(long sinceEpochMs) { return 0; }
            @Override public com.bot.core.InstrumentFilters fetchFilters(String symbol) { return exchange.fetchFilters(symbol); }
            @Override public OrderStatus placeOrder(OrderRequest request) { return exchange.placeOrder(request); }
        };

        Reconciler reconciler = reconcilerWith(flaky, (symbol, requestId) -> {
            throw new IllegalStateException("the close path must not run when the stop can be placed");
        });

        assertTrue(reconciler.reconcile(ExecFixtures.NOON).converged(), "pass 1 is caution");
        assertTrue(reconciler.reconcile(ExecFixtures.NOON.plusSeconds(30)).converged(), "pass 2 too");

        Reconciler.Report third = reconciler.reconcile(ExecFixtures.NOON.plusSeconds(60));
        assertEquals(Reconciler.Drift.Kind.STOP_REPLACED, third.drifts().get(0).kind(),
                "silent-forever was the audit's finding #11; three passes is the limit — "
                        + third.describe());
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("an unreadable stop whose replacement is ALSO refused halts — it is never market-closed")
    void unconfirmableStopIsNeverFlattenedOnReadFailuresAlone() {
        bookedPosition(Optional.of(STOP_ID));

        // The same venue outage that makes the stop unreadable also refuses the replacement.
        DelegatingExchange broken = new DelegatingExchange() {
            @Override public Optional<OrderStatus> queryOrder(String symbol, String clientOrderId) {
                throw ExchangeException.ambiguous("timeout reading the stop", null);
            }
            @Override public OrderStatus placeOrder(OrderRequest request) {
                throw ExchangeException.refused("venue refusing conditional orders", 400, -4045);
            }
            @Override public ExchangeSnapshots.AccountSnapshot fetchAccount() { return exchange.fetchAccount(); }
            @Override public List<ExchangeSnapshots.PositionSnapshot> openPositions() { return exchange.openPositions(); }
            @Override public List<OrderStatus> openOrders(String symbol) { return List.of(); }
            @Override public boolean canListConditionalOrders() { return false; }
            @Override public double fetchRealizedPnlSince(long sinceEpochMs) { return 0; }
            @Override public com.bot.core.InstrumentFilters fetchFilters(String symbol) { return exchange.fetchFilters(symbol); }
        };

        List<String> closes = new ArrayList<>();
        Reconciler reconciler = reconcilerWith(broken, (symbol, requestId) -> {
            closes.add(symbol);
            return new ExecutionCoordinator.CloseReport(symbol, true,
                    new BigDecimal("0.041"), new BigDecimal("63000"), "closed");
        });

        reconciler.reconcile(ExecFixtures.NOON);
        reconciler.reconcile(ExecFixtures.NOON.plusSeconds(30));
        Reconciler.Report third = reconciler.reconcile(ExecFixtures.NOON.plusSeconds(60));

        assertEquals(Reconciler.Drift.Kind.POSITION_WITHOUT_STOP, third.drifts().get(0).kind());
        assertTrue(halt.isHalted(), "a stop never proven gone is a case for a human, not a market close");
        assertTrue(closes.isEmpty(),
                "turning three unreadable answers into a realised loss is the opposite of a repair");
    }

    @Test
    @DisplayName("the replacement is placed BEFORE the old id is retired, and the old id is retired")
    void oldStopIdIsCancelledAfterTheReplacementRests() {
        bookedPosition(Optional.of(STOP_ID));
        // The old stop is genuinely alive on the venue, which cannot list conditional orders and
        // whose query keeps failing — the exact case where the repair would park a duplicate.
        exchange.placeOrder(OrderRequest.protectiveStop("BTCUSDT",
                OrderTypes.OrderSide.SELL, new BigDecimal("62800.0"), STOP_ID));
        assertTrue(exchange.order(STOP_ID).orElseThrow().isWorking());

        DelegatingExchange unreadableStop = new DelegatingExchange() {
            @Override public Optional<OrderStatus> queryOrder(String symbol, String clientOrderId) {
                if (STOP_ID.equals(clientOrderId)) {
                    throw ExchangeException.ambiguous("timeout reading the stop", null);
                }
                return exchange.queryOrder(symbol, clientOrderId);
            }
            @Override public OrderStatus placeOrder(OrderRequest request) { return exchange.placeOrder(request); }
            @Override public void cancelOrder(String symbol, String clientOrderId) {
                exchange.cancelOrder(symbol, clientOrderId);
            }
            @Override public ExchangeSnapshots.AccountSnapshot fetchAccount() { return exchange.fetchAccount(); }
            @Override public List<ExchangeSnapshots.PositionSnapshot> openPositions() { return exchange.openPositions(); }
            @Override public List<OrderStatus> openOrders(String symbol) { return List.of(); }
            @Override public boolean canListConditionalOrders() { return false; }
            @Override public double fetchRealizedPnlSince(long sinceEpochMs) { return 0; }
            @Override public com.bot.core.InstrumentFilters fetchFilters(String symbol) { return exchange.fetchFilters(symbol); }
        };

        Reconciler reconciler = reconcilerWith(unreadableStop, (symbol, requestId) -> {
            throw new IllegalStateException("must not close");
        });
        reconciler.reconcile(ExecFixtures.NOON);
        reconciler.reconcile(ExecFixtures.NOON.plusSeconds(30));
        reconciler.reconcile(ExecFixtures.NOON.plusSeconds(60));

        String newStopId = engine.book().get("BTCUSDT").orElseThrow().protectiveStopId().orElseThrow();
        assertNotEquals(STOP_ID, newStopId);
        assertTrue(exchange.order(newStopId).orElseThrow().isWorking(), "the replacement rests");
        assertFalse(exchange.order(STOP_ID).orElseThrow().isWorking(),
                "the duplicate must not be left parked against the venue's conditional-order cap");
    }

    @Test
    @DisplayName("a position that left on its own before the repair ran is not reported as a close")
    void alreadyGoneIsNotReportedAsAnUnprotectedClose() {
        bookedPosition(Optional.of(STOP_ID));
        exchange.placementFailure = request ->
                request.type() == OrderType.STOP_MARKET
                        ? ExchangeException.refused("conditional order cap", 400, -4045)
                        : null;

        Reconciler reconciler = reconcilerWith(exchange, (symbol, requestId) -> {
            exchange.clearPosition(symbol);
            engine.registerClose(symbol);
            return new ExecutionCoordinator.CloseReport(symbol, true,
                    BigDecimal.ZERO, BigDecimal.ZERO, "already flat");
        });

        reconciler.reconcile(ExecFixtures.NOON);

        assertFalse(alerts.sawCritical("Unprotected position closed"),
                "nothing was closed, so nothing may be announced as closed: " + alerts.messages);
        assertTrue(alerts.sawWarning("already gone"), alerts.messages.toString());
    }

    @Test
    @DisplayName("a repair close reaches the trade journal — the one exit no operator typed")
    void repairCloseIsObserved() {
        bookedPosition(Optional.of(STOP_ID));
        exchange.placementFailure = request ->
                request.type() == OrderType.STOP_MARKET
                        ? ExchangeException.refused("conditional order cap", 400, -4045)
                        : null;

        List<String> observed = new ArrayList<>();
        Reconciler reconciler = reconcilerWith(exchange, (symbol, requestId) -> {
            exchange.clearPosition(symbol);
            engine.registerClose(symbol);
            return new ExecutionCoordinator.CloseReport(symbol, true,
                    new BigDecimal("0.041"), new BigDecimal("63000"), "closed reduce-only in full");
        });
        reconciler.onRepairClose((requestId, symbol, report) -> observed.add(symbol));

        reconciler.reconcile(ExecFixtures.NOON);

        assertEquals(List.of("BTCUSDT"), observed,
                "an exit missing from the journal makes the forward record lie about its own costs");
    }
}
