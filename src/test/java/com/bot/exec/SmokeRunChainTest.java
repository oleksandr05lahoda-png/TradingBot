package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.risk.MarginTierTable;
import com.bot.risk.RiskDecision;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import com.bot.risk.TradeRequest;
import com.bot.signal.ManualTestnetInput;
import com.bot.signal.Signal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whole chain the definition of done names, asserted end to end against an in-memory exchange:
 *
 * <pre>
 *   typed signal -> size from the stop -> entry order -> stop and take-profits placed
 *                -> reconciliation converges
 * </pre>
 *
 * <p>This is the same wiring {@code TestnetBot} assembles, minus the socket. Running it against the
 * real testnet exercises the adapter as well; running it here proves the protocol, deterministically
 * and on every build.
 */
class SmokeRunChainTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();

    @Test
    @DisplayName("a typed line becomes a sized, stopped, targeted position and the state reconciles")
    void manualSignalToReconciledPosition() throws Exception {
        // 1 ─ A signal arrives the only way one can: somebody typed it.
        Signal signal;
        try (ManualTestnetInput input = ManualTestnetInput.fromReader(
                new StringReader("BTCUSDT LONG entry=64000 stop=62800 lev=3 id=smoke-1"),
                Clock.fixed(ExecFixtures.NOON, java.time.ZoneOffset.UTC), 3)) {
            List<Signal> polled = input.poll();
            assertEquals(1, polled.size());
            signal = polled.get(0);
        }

        // 2 ─ The gate sizes it from the stop and nothing else.
        InstrumentFilters filters = exchange.fetchFilters(signal.symbol());
        MarginTierTable tiers = exchange.fetchMarginTiers(signal.symbol());
        double equity = exchange.fetchAccount().equityUsd();
        assertEquals(10_000.0, equity);

        RiskDecision decision = engine.evaluate(new TradeRequest(signal.id(), signal.symbol(),
                signal.side(), signal.entryPrice(), signal.structuralStopPrice(), signal.atr(),
                signal.leverage(), filters, tiers), equity, ExecFixtures.NOON);

        TradePlan plan = decision.planOrThrow();
        assertEquals(0, plan.quantity().compareTo(new BigDecimal("0.041")));
        assertEquals(49.2, plan.riskUsd(), 1e-9);
        assertTrue(plan.riskFractionOfBalance() <= 0.005 + 1e-12, "risk is within the 0.5% budget");
        assertTrue(plan.liquidationBufferFraction() >= 0.30, "the stop is inside liquidation with room");

        // 3 ─ Execution: entry, then the protective stop, then the reduce-only exits.
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, engine, halt, alerts);
        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.FILLED, report.outcome());
        assertEquals(0, report.filledQuantity().compareTo(new BigDecimal("0.041")));

        OrderStatus stop = report.protectiveStop().orElseThrow();
        assertEquals(OrderType.STOP_MARKET, stop.type());
        assertTrue(stop.closePosition());
        assertEquals(0, stop.stopPrice().compareTo(new BigDecimal("62800.0")));

        assertEquals(2, report.takeProfitOrders().size(), "1.5R and 2R");
        assertTrue(report.takeProfitOrders().stream().allMatch(OrderStatus::reduceOnly));
        assertEquals(0, report.takeProfitOrders().get(0).stopPrice().compareTo(new BigDecimal("65800.0")));
        assertEquals(0, report.takeProfitOrders().get(1).stopPrice().compareTo(new BigDecimal("66400.0")));
        BigDecimal exitTotal = report.takeProfitOrders().stream()
                .map(OrderStatus::originalQuantity)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        assertEquals(0, exitTotal.compareTo(report.filledQuantity()), "the exits close the whole position");

        // 4 ─ The dead-man's switch is armed on the exchange side.
        assertEquals(ExecutionCoordinator.Settings.defaults().deadMansSwitchCountdownMs(),
                exchange.deadMansCountdownFor("BTCUSDT"));

        // 5 ─ Reconciliation converges: the exchange and the book agree, nothing is halted.
        Reconciler reconciler = new Reconciler(exchange, engine, halt, alerts,
                new IdempotentOrderPlacer(exchange, 1, 1, 0, ExecFixtures.NO_SLEEP));
        Reconciler.Report reconciliation = reconciler.reconcile(ExecFixtures.NOON);

        assertTrue(reconciliation.converged(), reconciliation.describe());
        assertFalse(halt.isHalted());
        assertEquals(1, reconciliation.exchangePositions());
        assertTrue(alerts.messages.isEmpty(), "a clean run should raise nothing: " + alerts.messages);
    }

    @Test
    @DisplayName("a signal the gate refuses reaches the exchange as nothing at all")
    void refusedSignalPlacesNoOrders() {
        // A stop 30% below entry at 5x cannot satisfy the liquidation buffer.
        RiskDecision decision = engine.evaluate(new TradeRequest("smoke-2", "BTCUSDT",
                com.bot.core.Side.LONG, 64_000, java.util.OptionalDouble.of(44_800),
                java.util.OptionalDouble.empty(), 5,
                exchange.fetchFilters("BTCUSDT"), exchange.fetchMarginTiers("BTCUSDT")),
                10_000, ExecFixtures.NOON);

        assertTrue(decision instanceof RiskDecision.Rejected, "expected a refusal, got " + decision);
        assertEquals(0, exchange.placeOrderCalls, "a refused signal must never reach the exchange");
        assertTrue(exchange.openPositions().isEmpty());
    }

    @Test
    @DisplayName("a halted bot still refuses to open, and the refusal costs no orders")
    void haltedBotOpensNothing() throws Exception {
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        halt.halt("operator stopped trading", ExecFixtures.NOON);

        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, engine, halt, alerts);
        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.REFUSED, report.outcome());
        assertEquals(0, exchange.placeOrderCalls);
    }
}
