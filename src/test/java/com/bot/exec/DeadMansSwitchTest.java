package com.bot.exec;

import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.OrderTypes.OrderPurpose;
import com.bot.exec.OrderTypes.OrderSide;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dead-man's switch. Binance's countdownCancelAll cancels every open order on a symbol, so arming
 * it over an open position would schedule the deletion of that position's protective stop; the
 * negative tests here are the ones that matter.
 */
class DeadMansSwitchTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();

    private DeadMansSwitch switchWith(long countdownMs, long silenceMs) {
        return new DeadMansSwitch(exchange, engine, halt, alerts, countdownMs, silenceMs);
    }

    private void openBook(String symbol) {
        engine.book().open(new ExposureBook.OpenPosition(symbol, Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, java.util.Optional.empty()));
    }

    @Test
    @DisplayName("a symbol with a resting entry and no position gets the exchange-side countdown")
    void restingEntryIsArmed() {
        switchWith(120_000, 90_000).heartbeat(ExecFixtures.NOON, Set.of("BTCUSDT"));
        assertEquals(120_000L, exchange.deadMansCountdownFor("BTCUSDT"));
    }

    @Test
    @DisplayName("a symbol holding a position is NEVER armed: the countdown would delete its stop")
    void positionHoldingSymbolIsNeverArmed() {
        openBook("BTCUSDT");

        DeadMansSwitch deadMansSwitch = switchWith(120_000, 90_000);
        // Even when the caller asks for it, because the position takes precedence.
        deadMansSwitch.heartbeat(ExecFixtures.NOON, Set.of("BTCUSDT"));

        assertNull(exchange.deadMansCountdownFor("BTCUSDT"),
                "countdownCancelAll cancels every open order on the symbol, protective stop included");
        assertTrue(deadMansSwitch.armedSymbols().isEmpty());
    }

    @Test
    @DisplayName("a symbol is disarmed as soon as its entry becomes a position")
    void armedSymbolIsDisarmedOnceThePositionExists() {
        DeadMansSwitch deadMansSwitch = switchWith(120_000, 90_000);
        deadMansSwitch.heartbeat(ExecFixtures.NOON, Set.of("BTCUSDT"));
        assertEquals(120_000L, exchange.deadMansCountdownFor("BTCUSDT"));

        openBook("BTCUSDT");   // the entry filled; a stop is now resting on that symbol
        deadMansSwitch.heartbeat(ExecFixtures.NOON.plusSeconds(30), Set.of("BTCUSDT"));

        assertEquals(0L, exchange.deadMansCountdownFor("BTCUSDT"),
                "leaving the countdown running would schedule the deletion of the new stop");
        assertTrue(deadMansSwitch.armedSymbols().isEmpty());
    }

    @Test
    @DisplayName("nothing is armed when there are no resting entries")
    void nothingArmedWhenNothingIsResting() {
        switchWith(120_000, 90_000).heartbeat(ExecFixtures.NOON);
        assertNull(exchange.deadMansCountdownFor("BTCUSDT"));
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("prolonged loss of contact halts new risk and alerts")
    void lostContactHaltsAndAlerts() {
        openBook("BTCUSDT");
        UnreachableExchange unreachable = new UnreachableExchange();
        DeadMansSwitch deadMansSwitch = new DeadMansSwitch(unreachable, engine, halt, alerts, 120_000, 90_000);

        Instant start = ExecFixtures.NOON;
        deadMansSwitch.heartbeat(start);                      // first failure starts the clock
        assertFalse(halt.isHalted(), "one failed heartbeat is not a lost connection");

        deadMansSwitch.heartbeat(start.plusSeconds(120));      // beyond the 90s tolerance

        assertTrue(deadMansSwitch.isDegraded());
        assertTrue(halt.isHalted());
        assertTrue(alerts.sawCritical("contact lost"), alerts.messages.toString());
    }

    @Test
    @DisplayName("the degraded path cancels exposure-increasing orders and leaves protective ones alone")
    void degradedPathSparesReduceOnlyOrders() throws Exception {
        // A real position with a real protective stop and exits, plus a resting entry on another
        // symbol — then contact is lost.
        RiskEngine liveEngine = ExecFixtures.engine();
        TradePlan plan = ExecFixtures.approvedPlan(liveEngine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, liveEngine, halt, alerts);
        ExecutionCoordinator.Report report = coordinator.execute(plan);
        String stopId = report.protectiveStop().orElseThrow().clientOrderId();

        // A resting, exposure-increasing entry on a symbol with no position.
        String restingEntryId = ClientOrderIdFactory.create("other-signal", OrderPurpose.ENTRY, 0);
        exchange.placeOrder(OrderRequest.limitEntry("ETHUSDT", OrderSide.BUY,
                new BigDecimal("0.001"), new BigDecimal("3000.0"), OrderTypes.TimeInForce.GTC,
                restingEntryId));
        assertTrue(exchange.order(restingEntryId).orElseThrow().isWorking());

        ArmFailingExchange armFails = new ArmFailingExchange(exchange);
        DeadMansSwitch deadMansSwitch = new DeadMansSwitch(armFails, liveEngine, halt, alerts, 120_000, 90_000);
        deadMansSwitch.heartbeat(ExecFixtures.NOON, Set.of("ETHUSDT"));
        deadMansSwitch.heartbeat(ExecFixtures.NOON.plusSeconds(120), Set.of("ETHUSDT"));

        assertTrue(deadMansSwitch.isDegraded());
        assertFalse(exchange.order(restingEntryId).orElseThrow().isWorking(),
                "an exposure-increasing order that could fill unmanaged should have been cancelled");
        assertTrue(exchange.order(stopId).orElseThrow().isWorking(),
                "the protective stop must survive: it is the thing keeping the position survivable");
        assertTrue(report.takeProfitOrders().stream()
                        .allMatch(tp -> exchange.order(tp.clientOrderId()).orElseThrow().isWorking()),
                "reduce-only exits must survive too");
    }

    @Test
    @DisplayName("positions are never closed by the switch, only orders")
    void positionsAreNotTouched() throws Exception {
        RiskEngine liveEngine = ExecFixtures.engine();
        TradePlan plan = ExecFixtures.approvedPlan(liveEngine, exchange.fetchFilters("BTCUSDT"));
        ExecFixtures.coordinator(exchange, liveEngine, halt, alerts).execute(plan);
        BigDecimal before = exchange.openPositions().get(0).signedQuantity();

        ArmFailingExchange armFails = new ArmFailingExchange(exchange);
        DeadMansSwitch deadMansSwitch = new DeadMansSwitch(armFails, liveEngine, halt, alerts, 120_000, 90_000);
        deadMansSwitch.heartbeat(ExecFixtures.NOON, Set.of("BTCUSDT"));
        deadMansSwitch.heartbeat(ExecFixtures.NOON.plusSeconds(120), Set.of("BTCUSDT"));

        assertEquals(0, exchange.openPositions().get(0).signedQuantity().compareTo(before),
                "force-closing a position is a trading decision; a watchdog does not get to make it");
    }

    @Test
    @DisplayName("a countdown shorter than the local tolerance is refused at construction")
    void countdownMustOutlastTheLocalTolerance() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> switchWith(30_000, 90_000));
        assertTrue(thrown.getMessage().contains("must outlast"), thrown.getMessage());
    }

    @Test
    @DisplayName("shutdown disarms every countdown")
    void shutdownDisarms() {
        DeadMansSwitch deadMansSwitch = switchWith(120_000, 90_000);
        deadMansSwitch.heartbeat(ExecFixtures.NOON, Set.of("BTCUSDT"));
        deadMansSwitch.disarmAll();
        assertEquals(0L, exchange.deadMansCountdownFor("BTCUSDT"));
        assertTrue(deadMansSwitch.armedSymbols().isEmpty());
    }

    /** Nothing reaches the exchange at all — the "alive but blind" case. */
    private static final class UnreachableExchange extends DelegatingExchange {
        @Override public void armDeadMansSwitch(String symbol, long countdownMillis) {
            throw ExchangeException.ambiguous("no route to host", null);
        }

        @Override public long serverTimeMillis() {
            throw ExchangeException.ambiguous("no route to host", null);
        }
    }

    /** Arming fails, but the rest of the exchange (shared with the test) still answers. */
    private static final class ArmFailingExchange extends DelegatingExchange {
        private final FakeExchange shared;

        ArmFailingExchange(FakeExchange shared) { this.shared = shared; }

        @Override public void armDeadMansSwitch(String symbol, long countdownMillis) {
            throw ExchangeException.ambiguous("no route to host", null);
        }

        @Override public long serverTimeMillis() {
            throw ExchangeException.ambiguous("no route to host", null);
        }

        @Override public java.util.List<OrderStatus> openOrders(String symbol) {
            return shared.openOrders(symbol);
        }

        @Override public void cancelOrder(String symbol, String clientOrderId) {
            shared.cancelOrder(symbol, clientOrderId);
        }
    }
}
