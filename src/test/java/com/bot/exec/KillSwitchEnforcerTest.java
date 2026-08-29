package com.bot.exec;

import com.bot.core.Side;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The daily loss limit's teeth (28.08): a trip closes the whole book reduce-only instead of only
 * refusing new entries while the open losers run on. With 15 slots at 0.5% risk each, "-3% stops
 * the day" without this really meant -7.5%.
 */
class KillSwitchEnforcerTest {

    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final RiskEngine engine = ExecFixtures.engine();

    private final List<String> closed = new ArrayList<>();
    private final List<String> journaled = new ArrayList<>();

    private void openBook(String... symbols) {
        for (String symbol : symbols) {
            engine.book().open(new ExposureBook.OpenPosition(symbol, Side.LONG,
                    new BigDecimal("0.041"), 64_000, 2_624, 49.2, Optional.of("stop-" + symbol)));
        }
    }

    /**
     * Down 10% of the day's opening balance — far past the 3% default limit. The flat observation
     * first is what makes it TODAY's loss: the switch measures the move from where the day began,
     * so a book that was already under water when the day started does not trip on its own.
     */
    private void tripTheDay(Instant now) {
        engine.killSwitch().observeBalance(1_000, now);
        engine.killSwitch().observeOpenUnrealizedPnl(0, now);
        engine.killSwitch().observeOpenUnrealizedPnl(-100, now);
    }

    private Reconciler.PositionCloser flatteningCloser() {
        return (symbol, requestId) -> {
            closed.add(symbol + "/" + requestId);
            engine.registerClose(symbol);
            return new ExecutionCoordinator.CloseReport(symbol, true,
                    new BigDecimal("0.041"), new BigDecimal("63000"), "closed reduce-only in full");
        };
    }

    private KillSwitchEnforcer enforcer(Reconciler.PositionCloser closer, KillSwitchEnforcer.Action action) {
        return new KillSwitchEnforcer(engine, closer, alerts, action,
                (requestId, symbol, report) -> journaled.add(symbol));
    }

    private void openAdopted(String symbol) {
        // riskUsd 0 is how an adopted position is booked: the exchange stores no intended stop.
        engine.book().open(new ExposureBook.OpenPosition(symbol, Side.LONG,
                new BigDecimal("0.500"), 3_000, 1_500, 0.0, Optional.empty()));
    }

    @Test
    @DisplayName("armed switch: nothing happens, nothing is closed")
    void armedDoesNothing() throws Exception {
        openBook("BTCUSDT", "ETHUSDT");
        engine.killSwitch().observeBalance(1_000, ExecFixtures.NOON);

        enforcer(flatteningCloser(), KillSwitchEnforcer.Action.FLATTEN).enforce(ExecFixtures.NOON);

        assertTrue(closed.isEmpty());
        assertEquals(2, engine.book().openCount());
        assertTrue(alerts.messages.isEmpty(), alerts.messages.toString());
    }

    @Test
    @DisplayName("a trip closes every booked position reduce-only and journals each close")
    void tripFlattensTheBook() throws Exception {
        openBook("BTCUSDT", "ETHUSDT", "SOLUSDT");
        tripTheDay(ExecFixtures.NOON);

        enforcer(flatteningCloser(), KillSwitchEnforcer.Action.FLATTEN).enforce(ExecFixtures.NOON);

        assertEquals(3, closed.size(), closed.toString());
        assertEquals(0, engine.book().openCount(), "the limit is a ceiling, not a commentary");
        assertEquals(3, journaled.size());
        assertTrue(alerts.sawCritical("closing the book"), alerts.messages.toString());
        assertTrue(alerts.sawWarning("Book closed"), alerts.messages.toString());
    }

    @Test
    @DisplayName("a failed close is retried on the next pass with a fresh request id")
    void failedCloseRetriesNextPass() throws Exception {
        openBook("BTCUSDT");
        tripTheDay(ExecFixtures.NOON);

        List<String> requestIds = new ArrayList<>();
        boolean[] failFirst = {true};
        KillSwitchEnforcer enforcer = enforcer((symbol, requestId) -> {
            requestIds.add(requestId);
            if (failFirst[0]) {
                failFirst[0] = false;
                throw ExchangeException.refused("throttled", 429, 0);
            }
            engine.registerClose(symbol);
            return new ExecutionCoordinator.CloseReport(symbol, true,
                    new BigDecimal("0.041"), new BigDecimal("63000"), "closed reduce-only in full");
        }, KillSwitchEnforcer.Action.FLATTEN);

        enforcer.enforce(ExecFixtures.NOON);
        assertEquals(1, engine.book().openCount(), "first attempt failed; position still booked");

        enforcer.enforce(ExecFixtures.NOON.plusSeconds(30));
        assertEquals(0, engine.book().openCount());
        assertEquals(2, requestIds.size());
        assertFalse(requestIds.get(0).equals(requestIds.get(1)),
                "a retry must not adopt a stale terminal order under the same id: " + requestIds);
    }

    @Test
    @DisplayName("after the attempt budget the enforcer stands down loudly and stops hammering")
    void givesUpAfterTheBudget() throws Exception {
        openBook("BTCUSDT");
        tripTheDay(ExecFixtures.NOON);

        int[] calls = {0};
        KillSwitchEnforcer enforcer = enforcer((symbol, requestId) -> {
            calls[0]++;
            throw ExchangeException.refused("always refused", 400, -2019);
        }, KillSwitchEnforcer.Action.FLATTEN);

        for (int i = 0; i < KillSwitchEnforcer.MAX_FLATTEN_ATTEMPTS + 3; i++) {
            enforcer.enforce(ExecFixtures.NOON.plusSeconds(30L * i));
        }

        assertEquals(KillSwitchEnforcer.MAX_FLATTEN_ATTEMPTS, calls[0],
                "the budget bounds the hammering");
        assertTrue(alerts.sawCritical("could not flatten"), alerts.messages.toString());
    }

    @Test
    @DisplayName("KILL_SWITCH_ACTION=halt-only keeps the old behaviour: alert once, close nothing")
    void haltOnlyClosesNothing() throws Exception {
        openBook("BTCUSDT", "ETHUSDT");
        tripTheDay(ExecFixtures.NOON);

        KillSwitchEnforcer enforcer = enforcer(flatteningCloser(), KillSwitchEnforcer.Action.HALT_ONLY);
        enforcer.enforce(ExecFixtures.NOON);
        enforcer.enforce(ExecFixtures.NOON.plusSeconds(30));

        assertTrue(closed.isEmpty());
        assertEquals(2, engine.book().openCount());
        assertEquals(1, alerts.messages.stream().filter(m -> m.contains("Daily loss limit hit")).count(),
                "announced once, not every pass: " + alerts.messages);
    }

    @Test
    @DisplayName("an adopted position is never flattened — the owner's hand trade is not ours to close")
    void adoptedPositionsAreLeftAlone() throws Exception {
        openBook("BTCUSDT");        // sized by this bot
        openAdopted("ETHUSDT");     // adopted from the exchange, riskUsd 0
        tripTheDay(ExecFixtures.NOON);

        enforcer(flatteningCloser(), KillSwitchEnforcer.Action.FLATTEN).enforce(ExecFixtures.NOON);

        assertEquals(List.of("BTCUSDT/ks-2026-08-09-BTCUSDT-" + ExecFixtures.NOON.getEpochSecond() + "-a1"),
                closed, "only what this bot sized may be market-closed");
        assertTrue(engine.book().hasPosition("ETHUSDT"),
                "flattening what the owner may be holding by hand is not this process's call");
        assertTrue(alerts.messages.stream().anyMatch(m -> m.contains("NOT closed") && m.contains("ETHUSDT")),
                "the trip alert must name what it deliberately left alone: " + alerts.messages);
    }

    @Test
    @DisplayName("a book of nothing but adopted positions closes nothing and says so")
    void onlyAdoptedMeansNothingToClose() throws Exception {
        openAdopted("ETHUSDT");
        tripTheDay(ExecFixtures.NOON);

        enforcer(flatteningCloser(), KillSwitchEnforcer.Action.FLATTEN).enforce(ExecFixtures.NOON);

        assertTrue(closed.isEmpty());
        assertTrue(engine.book().hasPosition("ETHUSDT"));
        assertTrue(alerts.sawCritical("Nothing this bot opened"), alerts.messages.toString());
    }

    @Test
    @DisplayName("the request id carries a run stamp, so a same-day restart cannot repeat it")
    void requestIdIsUniquePerRun() throws Exception {
        openBook("BTCUSDT");
        tripTheDay(ExecFixtures.NOON);
        enforcer(flatteningCloser(), KillSwitchEnforcer.Action.FLATTEN).enforce(ExecFixtures.NOON);

        // A fresh process on the same UTC day: attempts restarts at 1, but the run stamp differs,
        // so the id cannot collide with the pre-crash attempt 1 and be adopted as already-filled.
        RiskEngine restarted = ExecFixtures.engine();
        restarted.book().open(new ExposureBook.OpenPosition("BTCUSDT", Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2, Optional.of("stop-BTCUSDT")));
        Instant later = ExecFixtures.NOON.plusSeconds(240);
        restarted.killSwitch().observeBalance(1_000, later);
        restarted.killSwitch().observeOpenUnrealizedPnl(0, later);
        restarted.killSwitch().observeOpenUnrealizedPnl(-100, later);
        List<String> afterRestart = new ArrayList<>();
        new KillSwitchEnforcer(restarted, (symbol, requestId) -> {
            afterRestart.add(requestId);
            restarted.registerClose(symbol);
            return new ExecutionCoordinator.CloseReport(symbol, true,
                    new BigDecimal("0.041"), new BigDecimal("63000"), "closed reduce-only in full");
        }, alerts, KillSwitchEnforcer.Action.FLATTEN, null).enforce(later);

        assertEquals(1, afterRestart.size());
        assertFalse(closed.get(0).endsWith(afterRestart.get(0)),
                "the same id would make the placer adopt the pre-crash order instead of sending: "
                        + closed + " vs " + afterRestart);
    }

    @Test
    @DisplayName("the UTC rollover re-arms the enforcer for the next day")
    void rolloverRearms() throws Exception {
        openBook("BTCUSDT");
        tripTheDay(ExecFixtures.NOON);

        KillSwitchEnforcer enforcer = enforcer(flatteningCloser(), KillSwitchEnforcer.Action.FLATTEN);
        enforcer.enforce(ExecFixtures.NOON);
        assertEquals(1, closed.size());

        // Next UTC day: the switch self-clears, the enforcer must follow.
        Instant tomorrow = ExecFixtures.NOON.plusSeconds(86_400);
        engine.killSwitch().observeBalance(1_000, tomorrow);
        enforcer.enforce(tomorrow);
        assertFalse(engine.killSwitch().isTripped(tomorrow));

        openBook("ETHUSDT");
        engine.killSwitch().observeOpenUnrealizedPnl(0, tomorrow);
        engine.killSwitch().observeOpenUnrealizedPnl(-100, tomorrow);
        enforcer.enforce(tomorrow.plusSeconds(30));
        assertEquals(2, closed.size(), "a fresh trip on a fresh day closes again: " + closed);
    }
}
