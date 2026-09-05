package com.bot.exec;

import com.bot.exec.OrderTypes.OrderType;
import com.bot.risk.DailyLossKillSwitch;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskConfig;
import com.bot.risk.RiskEngine;
import com.bot.risk.TakeProfitPolicy;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A take-profit leg that never reaches the exchange must reach the owner. Two legs, one of them
 * priced outside the venue's PRICE_FILTER: the valid leg rests, the invalid one is skipped, and
 * that skip used to be a log line only — the "NO take-profit" alert stays quiet because one leg
 * was placed.
 */
class TakeProfitAlertTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();

    private static RiskEngine engineWith(TakeProfitPolicy policy) {
        RiskConfig config = RiskConfig.defaults().withTakeProfitPolicy(policy);
        return new RiskEngine(config, new ExposureBook(), new DailyLossKillSwitch(config.dailyLossFractionLimit()));
    }

    @Test
    @DisplayName("a leg the validator refuses raises the same alert as a leg the venue refuses")
    void refusedLegIsAlerted() throws Exception {
        // 1.5R is a normal target; 1,000,000R puts the price past the fake's maxPrice of 1e9.
        RiskEngine engine = engineWith(new TakeProfitPolicy(List.of(
                new TakeProfitPolicy.Leg(1.5, 0.5), new TakeProfitPolicy.Leg(1_000_000, 0.5))));
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, engine, halt, alerts);

        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.FILLED, report.outcome(), report.note());
        assertEquals(1, report.takeProfitOrders().size(), "the valid leg rests");
        assertEquals(1, exchange.openOrders("BTCUSDT").stream()
                .filter(o -> o.type() == OrderType.TAKE_PROFIT_MARKET && o.isWorking()).count());
        assertTrue(alerts.sawWarning("Take-profit leg not placed"), alerts.messages.toString());
        assertFalse(alerts.sawWarning("Position has NO take-profit"), "one leg did rest");
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("two good legs raise no take-profit alert at all")
    void goodLegsAreQuiet() throws Exception {
        RiskEngine engine = engineWith(TakeProfitPolicy.standard());
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(exchange, engine, halt, alerts);

        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(2, report.takeProfitOrders().size());
        assertFalse(alerts.sawWarning("Take-profit leg not placed"), alerts.messages.toString());
        assertFalse(alerts.sawWarning("Position has NO take-profit"));
    }
}
