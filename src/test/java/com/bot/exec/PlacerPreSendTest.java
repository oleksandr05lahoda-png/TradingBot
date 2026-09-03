package com.bot.exec;

import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The placer's pre-send look-up is a READ. Its failure used to escape as if the SEND had failed:
 * a timeout on the stop's id check market-closed the position just opened, one on the entry's
 * latched an "outcome unknown" halt over nothing (audit 03.09).
 */
class PlacerPreSendTest {

    /** Throws from queryOrder for ids with a given prefix, a bounded number of times. */
    static final class FlakyQueries extends DelegatingExchange {
        int throwsLeft;
        String onlyForPrefix;

        @Override public Optional<OrderStatus> queryOrder(String symbol, String clientOrderId) {
            if (throwsLeft > 0 && (onlyForPrefix == null || clientOrderId.startsWith(onlyForPrefix))) {
                throwsLeft--;
                throw ExchangeException.ambiguous("timeout on GET /fapi/v1/order — the request may have been executed", null);
            }
            return delegate.queryOrder(symbol, clientOrderId);
        }
    }

    private final FlakyQueries exchange = new FlakyQueries();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();

    private ExecutionCoordinator coordinator() {
        // Two probes per look-up, as in the shared fixture; three stop attempts in the coordinator.
        IdempotentOrderPlacer placer = new IdempotentOrderPlacer(exchange, 3, 2, 0, ExecFixtures.NO_SLEEP);
        return new ExecutionCoordinator(exchange, engine, placer, halt, alerts,
                ExecutionCoordinator.Settings.defaults(), ExecFixtures.CLOCK, ExecFixtures.NO_SLEEP);
    }

    private TradePlan plan() {
        return ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
    }

    @Test
    @DisplayName("a transient timeout on the stop's pre-send look-up is retried, not turned into a market close")
    void stopPreCheckTimeoutIsRetried() throws Exception {
        exchange.onlyForPrefix = "bt-s";
        exchange.throwsLeft = 2;          // both probes of the first attempt fail; the retry succeeds

        ExecutionCoordinator.Report report = coordinator().execute(plan());

        assertEquals(ExecutionCoordinator.Outcome.FILLED, report.outcome(), report.note());
        assertTrue(report.protectiveStop().isPresent(), "the stop rests after the retry");
        assertFalse(exchange.openPositions().isEmpty(), "the position was NOT unwound");
        assertFalse(halt.isHalted());
        assertFalse(alerts.sawCritical("Protective stop could not be placed"), alerts.messages.toString());
    }

    @Test
    @DisplayName("a timeout on the entry's pre-send look-up is a refusal with nothing sent — no halt")
    void entryPreCheckTimeoutIsRefusedNotHalted() throws Exception {
        exchange.onlyForPrefix = "bt-e";
        exchange.throwsLeft = 10;

        ExecutionCoordinator.Report report = coordinator().execute(plan());

        assertEquals(ExecutionCoordinator.Outcome.REFUSED, report.outcome(), report.note());
        assertTrue(report.note().contains("nothing was sent"), report.note());
        assertFalse(halt.isHalted(), "nothing was sent, so nothing is unknown");
        assertTrue(exchange.openPositions().isEmpty());
        assertEquals(0, exchange.delegate.placeOrderCalls, "no order may be sent without a successful look-up");
        assertTrue(coordinator().intents().get("BTCUSDT", ExecFixtures.NOON.toEpochMilli()).isEmpty(),
                "a refused entry leaves no intent behind");
    }

    @Test
    @DisplayName("a stop whose look-up never recovers is still unwound — the invariant holds")
    void persistentStopPreCheckFailureUnwinds() throws Exception {
        exchange.onlyForPrefix = "bt-s";
        exchange.throwsLeft = 100;

        ExecutionCoordinator.Report report = coordinator().execute(plan());

        assertEquals(ExecutionCoordinator.Outcome.ABORTED_UNPROTECTED, report.outcome(), report.note());
        assertTrue(exchange.openPositions().isEmpty(), "no position lives without a stop");
        assertTrue(alerts.sawCritical("Protective stop could not be placed"), alerts.messages.toString());
    }

    @Test
    @DisplayName("an old terminal order under the same id is refused, not adopted as today's fill")
    void staleTerminalOrderIsRefused() throws Exception {
        TradePlan plan = plan();
        ExecutionCoordinator first = coordinator();
        assertEquals(ExecutionCoordinator.Outcome.FILLED, first.execute(plan).outcome());
        // Forget the book and pretend a later process replays the same signal much later.
        engine.registerClose("BTCUSDT");
        exchange.delegate.clearPosition("BTCUSDT");
        String entryId = ClientOrderIdFactory.create(plan.signalId(), OrderTypes.OrderPurpose.ENTRY, 0);
        OrderStatus old = exchange.delegate.order(entryId).orElseThrow();
        exchange.delegate.setOrderState(entryId, old.state());   // re-stamps updateTime to the fake clock
        // The fake clock is fixed, so age the order by hand: 11 minutes before "now".
        exchange.delegate.ageOrder(entryId, IdempotentOrderPlacer.STALE_TERMINAL_MS + 60_000L);

        ExecutionCoordinator.Report replay = coordinator().execute(plan);

        assertEquals(ExecutionCoordinator.Outcome.REFUSED, replay.outcome(), replay.note());
        assertTrue(replay.note().contains("executed before"), replay.note());
        assertTrue(exchange.openPositions().isEmpty(), "a replay must not open a second position");
        assertFalse(halt.isHalted());
    }
}
