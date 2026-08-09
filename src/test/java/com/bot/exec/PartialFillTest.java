package com.bot.exec;

import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Partial fills: the position that exists is not the position that was approved, and everything
 * downstream has to be sized from what filled.
 *
 * <p>A stop sized for the intended quantity leaves the difference unprotected while every log line
 * says the trade is covered — which is the failure mode that looks healthiest right up until it
 * isn't.
 */
class PartialFillTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();

    @Test
    @DisplayName("the exits are sized from what filled, not from what was asked for")
    void exitsFollowTheFilledQuantity() throws Exception {
        exchange.fillRatio = 0.5;

        RiskEngine engine = ExecFixtures.engine();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator =
                ExecFixtures.coordinator(exchange, engine, new TradingHalt(), alerts);

        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.PARTIALLY_FILLED, report.outcome());
        assertEquals(0, report.filledQuantity().compareTo(new BigDecimal("0.020")),
                "half of 0.041 floored to the 0.001 lot is 0.020");

        BigDecimal exitTotal = report.takeProfitOrders().stream()
                .map(OrderStatus::originalQuantity)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        assertEquals(0, exitTotal.compareTo(report.filledQuantity()),
                "the exits cover " + exitTotal + " but only " + report.filledQuantity() + " filled");
        assertTrue(report.takeProfitOrders().stream().allMatch(OrderStatus::reduceOnly),
                "every exit must be reduce-only");
    }

    @Test
    @DisplayName("the exposure book records the filled size, never the intended one")
    void bookRecordsTheFilledSize() throws Exception {
        exchange.fillRatio = 0.5;

        RiskEngine engine = ExecFixtures.engine();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator =
                ExecFixtures.coordinator(exchange, engine, new TradingHalt(), alerts);

        coordinator.execute(plan);

        assertEquals(1, engine.book().openCount());
        assertEquals(0, engine.book().get("BTCUSDT").orElseThrow().quantity()
                .compareTo(new BigDecimal("0.020")));
        assertEquals(0.020 * 64_000, engine.book().longExposureUsd(), 1e-6);
    }

    @Test
    @DisplayName("the protective stop closes the whole position, so it stays correct as exits fill")
    void protectiveStopUsesClosePosition() throws Exception {
        exchange.fillRatio = 0.5;

        RiskEngine engine = ExecFixtures.engine();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator =
                ExecFixtures.coordinator(exchange, engine, new TradingHalt(), alerts);

        ExecutionCoordinator.Report report = coordinator.execute(plan);
        OrderStatus stop = report.protectiveStop().orElseThrow();

        assertEquals(OrderType.STOP_MARKET, stop.type());
        assertTrue(stop.closePosition(),
                "a fixed-quantity stop goes stale the moment a take-profit leg reduces the position");
        assertEquals(0, stop.stopPrice().compareTo(plan.stopPrice()));
    }

    @Test
    @DisplayName("nothing filled means nothing placed: no stop, no exits, no position")
    void nothingFilledMeansNothingToClean() throws Exception {
        exchange.fillRatio = 0.0;

        RiskEngine engine = ExecFixtures.engine();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator =
                ExecFixtures.coordinator(exchange, engine, new TradingHalt(), alerts);

        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.NOT_FILLED, report.outcome());
        assertTrue(report.protectiveStop().isEmpty());
        assertTrue(report.takeProfitOrders().isEmpty());
        assertEquals(0, engine.book().openCount());
        assertTrue(exchange.openPositions().isEmpty());
    }

    @Test
    @DisplayName("a fill far enough from plan to break the risk budget closes the position again")
    void slippageBeyondToleranceAbortsTheTrade() throws Exception {
        RiskEngine engine = ExecFixtures.engine();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));

        // Planned entry 64,000 with the stop at 62,800: R = 1,200 and the budget is $49.20.
        // Filling at 64,400 makes the real distance 1,600 — a third more risk than approved.
        exchange.fillPrice = new BigDecimal("64400.0");

        ExecutionCoordinator coordinator =
                ExecFixtures.coordinator(exchange, engine, new TradingHalt(), alerts);
        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.ABORTED_ON_SLIPPAGE, report.outcome());
        assertTrue(alerts.sawCritical("risk budget"), alerts.messages.toString());
        assertTrue(exchange.openPositions().isEmpty(),
                "the position should have been closed reduce-only, not kept and hoped about");
        assertFalse(engine.book().hasPosition("BTCUSDT"));
    }

    @Test
    @DisplayName("slippage within tolerance is accepted and the exits are re-projected from the real fill")
    void slippageWithinToleranceIsAccepted() throws Exception {
        RiskEngine engine = ExecFixtures.engine();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));

        // Filling at 64,100 makes R = 1,300, about 8% more risk than planned — inside the 20% tolerance.
        exchange.fillPrice = new BigDecimal("64100.0");

        ExecutionCoordinator coordinator =
                ExecFixtures.coordinator(exchange, engine, new TradingHalt(), alerts);
        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.FILLED, report.outcome());
        // 1.5R from the real fill of 64,100 is 64,100 + 1.5 * 1,300 = 66,050, not the planned 65,800.
        assertEquals(0, report.takeProfitOrders().get(0).stopPrice().compareTo(new BigDecimal("66050.0")),
                "exits must be measured from the price that actually filled");
    }
}
