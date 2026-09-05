package com.bot.exec;

import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.exec.OrderTypes.TimeInForce;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The limit-orphan gap of the 30.08 backlog: a limit entry that rests past its window, whose
 * cancel then fails, used to be reported "not filled" with its intent wiped — and a fill an hour
 * later was a foreign position the reconciler halted on. Now the intent outlives the report.
 */
class LimitEntryIntentTest {

    private static final ExecutionCoordinator.Settings LIMIT =
            new ExecutionCoordinator.Settings(OrderType.LIMIT, TimeInForce.GTC, 2, 500, 0.20);

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();
    private final IdempotentOrderPlacer placer = new IdempotentOrderPlacer(exchange, 3, 2, 0, ExecFixtures.NO_SLEEP);
    private final Reconciler reconciler = new Reconciler(exchange, engine, halt, alerts, placer);

    private ExecutionCoordinator coordinator() {
        return ExecFixtures.coordinator(exchange, engine, halt, alerts, LIMIT);
    }

    @Test
    @DisplayName("a resting limit cancelled cleanly is not filled and its intent is spent")
    void cleanCancelSpendsTheIntent() throws Exception {
        ExecutionCoordinator coordinator = coordinator();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));

        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.NOT_FILLED, report.outcome(), report.note());
        assertTrue(coordinator.intents().get("BTCUSDT", ExecFixtures.NOON.toEpochMilli()).isEmpty(),
                "nothing rests, nothing to remember");
        assertTrue(exchange.openOrders("BTCUSDT").stream().noneMatch(OrderStatus::isWorking));
        assertEquals(0, engine.book().openCount());
    }

    @Test
    @DisplayName("a resting limit whose cancel fails keeps its intent and says so")
    void failedCancelKeepsTheIntent() throws Exception {
        ExecutionCoordinator coordinator = coordinator();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        exchange.cancelFailure = ExchangeException.refused("service unavailable", 500, 0);

        ExecutionCoordinator.Report report = coordinator.execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.NOT_FILLED, report.outcome(), report.note());
        assertTrue(report.entryOrder().orElseThrow().isWorking(), "the order still rests");
        assertTrue(coordinator.intents().get("BTCUSDT", ExecFixtures.NOON.toEpochMilli()).isPresent(),
                "the intent must outlive the report while the order can still fill");
        assertTrue(alerts.sawWarning("Entry still resting"), alerts.messages.toString());
        assertFalse(halt.isHalted(), "nothing is at risk yet");
        assertEquals(0, engine.book().openCount());
    }

    @Test
    @DisplayName("the late fill of an uncancellable limit is adopted with its planned stop")
    void lateFillIsAdoptedAndProtected() throws Exception {
        ExecutionCoordinator coordinator = coordinator();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        exchange.cancelFailure = ExchangeException.refused("service unavailable", 500, 0);
        ExecutionCoordinator.Report report = coordinator.execute(plan);
        String entryId = report.entryOrder().orElseThrow().clientOrderId();

        // An hour later the market comes back to the price and the resting limit fills.
        exchange.cancelFailure = null;
        exchange.setOrderState(entryId, OrderState.FILLED);
        exchange.plantPosition("BTCUSDT", plan.quantity().toPlainString(), plan.entryPrice().toPlainString());
        reconciler.withEntryIntents(coordinator.intents());
        reconciler.withStopRepair(coordinator::closeOut);

        Reconciler.Report pass = reconciler.reconcile(ExecFixtures.NOON.plusSeconds(3600));

        assertTrue(pass.healthy(), pass.describe());
        assertFalse(halt.isHalted(), "ours by intent, never a foreign position");
        ExposureBook.OpenPosition booked = engine.book().get("BTCUSDT").orElseThrow();
        assertTrue(booked.riskUsd() > 0, "risk from the planned stop distance");
        assertTrue(booked.protectiveStopId().isPresent(), "the stop that was meant is now placed");
        assertTrue(exchange.openOrders("BTCUSDT").stream()
                .anyMatch(o -> o.type() == OrderType.STOP_MARKET && o.isWorking()));
        assertTrue(coordinator.intents().get("BTCUSDT", ExecFixtures.NOON.toEpochMilli()).isEmpty(),
                "spent once the position is protected");
    }

    @Test
    @DisplayName("the reconciler cancels the leftover entry out of grace and spends the intent one pass later")
    void reconcilerCancelsTheLeftoverAndSpendsTheIntent() throws Exception {
        ExecutionCoordinator coordinator = coordinator();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        exchange.cancelFailure = ExchangeException.refused("service unavailable", 500, 0);
        ExecutionCoordinator.Report report = coordinator.execute(plan);
        String entryId = report.entryOrder().orElseThrow().clientOrderId();
        exchange.cancelFailure = null;
        reconciler.withEntryIntents(coordinator.intents());

        // Inside the orphan grace: the symbol is inspected because of the intent, the order is
        // left alone, the intent is kept — a fill may still be coming.
        Instant first = ExecFixtures.NOON.plusSeconds(30);
        reconciler.reconcile(first);
        assertTrue(exchange.openOrders("BTCUSDT").stream().anyMatch(o -> o.clientOrderId().equals(entryId) && o.isWorking()),
                "inside grace the order rests");
        assertTrue(coordinator.intents().get("BTCUSDT", first.toEpochMilli()).isPresent());

        // Out of grace: the entry is cancelled as an orphan. The intent survives THIS pass — the
        // cancel may have raced a fill — and goes on the next one, which reads the symbol flat
        // with nothing working.
        exchange.ageOrder(entryId, 120_000L);
        Instant second = first.plusSeconds(30);
        Reconciler.Report pass = reconciler.reconcile(second);
        assertTrue(exchange.openOrders("BTCUSDT").stream().noneMatch(OrderStatus::isWorking),
                "out of grace the leftover entry is cancelled: " + pass.describe());
        assertTrue(coordinator.intents().get("BTCUSDT", second.toEpochMilli()).isPresent(),
                "one pass of lag: a fill between the read and the cancel must still find its intent");

        Instant third = second.plusSeconds(30);
        reconciler.reconcile(third);
        assertTrue(coordinator.intents().get("BTCUSDT", third.toEpochMilli()).isEmpty(),
                "flat with nothing working: the intent is spent");
        assertFalse(halt.isHalted());
        assertEquals(0, engine.book().openCount());
    }
}
