package com.bot.risk;

import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The daily loss limit: when it trips, what it counts, and how long it stays tripped. */
class DailyLossKillSwitchTest {

    private static final Instant MORNING = Instant.parse("2026-08-09T08:00:00Z");
    private static final Instant EVENING = Instant.parse("2026-08-09T23:59:59Z");
    private static final Instant NEXT_DAY = Instant.parse("2026-08-10T00:00:01Z");

    @Test
    @DisplayName("a loss below the limit does not trip it")
    void lossBelowLimitDoesNotTrip() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03);
        killSwitch.observeBalance(10_000, MORNING);
        killSwitch.seedRealizedPnl(-200, MORNING);
        assertFalse(killSwitch.isTripped(MORNING));
    }

    @Test
    @DisplayName("crossing the limit trips it")
    void crossingTheLimitTrips() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03);
        killSwitch.observeBalance(10_000, MORNING);
        killSwitch.seedRealizedPnl(-301, MORNING);

        DailyLossKillSwitch.Status status = killSwitch.evaluate(MORNING);
        assertTrue(status.tripped());
        assertTrue(status.drawdownFraction() > 0.03);
        assertTrue(status.reason().contains("daily loss"), status.reason());
    }

    @Test
    @DisplayName("it stays tripped for the rest of the UTC day even if the account recovers")
    void itLatchesForTheRestOfTheDay() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03);
        killSwitch.observeBalance(10_000, MORNING);
        killSwitch.seedRealizedPnl(-400, MORNING);
        assertTrue(killSwitch.isTripped(MORNING));

        killSwitch.seedRealizedPnl(+1_000, EVENING);
        assertTrue(killSwitch.isTripped(EVENING),
                "a limit that un-trips when the number improves is a limit the same losing session "
                        + "will test again and again");
    }

    @Test
    @DisplayName("it resets at the UTC day boundary")
    void itResetsAtMidnightUtc() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03);
        killSwitch.observeBalance(10_000, MORNING);
        killSwitch.seedRealizedPnl(-400, MORNING);
        assertTrue(killSwitch.isTripped(EVENING));

        killSwitch.observeBalance(9_600, NEXT_DAY);
        assertFalse(killSwitch.isTripped(NEXT_DAY));
        DailyLossKillSwitch.Status status = killSwitch.evaluate(NEXT_DAY);
        assertEquals(0.0, status.realizedPnl(), 1e-9);
        assertEquals(9_600, status.dayStartBalance(), 1e-9,
                "the new day's limit is a percentage of the new day's opening balance");
    }

    @Test
    @DisplayName("an open drawdown blocks new entries before it is realised")
    void openDrawdownCountsAgainstTheLimit() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03);
        killSwitch.observeBalance(10_000, MORNING);
        killSwitch.observeOpenUnrealizedPnl(0, MORNING);        // the day starts flat
        killSwitch.observeOpenUnrealizedPnl(-350, MORNING);     // and moves against us today
        assertTrue(killSwitch.isTripped(MORNING),
                "a position sitting at a loss larger than the daily cap must stop new entries now, "
                        + "not once it closes");
    }

    @Test
    @DisplayName("a loss carried in from yesterday is not today's loss")
    void carriedOverOpenLossDoesNotTripToday() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03);
        killSwitch.observeBalance(10_000, MORNING);
        // The book was already deep under water when this day - or this process - began.
        killSwitch.observeOpenUnrealizedPnl(-900, MORNING);
        assertFalse(killSwitch.isTripped(MORNING),
                "yesterday's paper loss is not today's; counting it would market-close the whole "
                        + "book seconds after a restart, on a day that has not moved yet");

        killSwitch.observeOpenUnrealizedPnl(-1_250, MORNING);   // -350 more, made today
        assertTrue(killSwitch.isTripped(MORNING),
                "the move made TODAY still counts, measured from where the day started");
    }

    @Test
    @DisplayName("the baseline is re-taken at the UTC rollover, not carried across it")
    void baselineResetsWithTheDay() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03);
        killSwitch.observeBalance(10_000, MORNING);
        killSwitch.observeOpenUnrealizedPnl(-900, MORNING);

        killSwitch.observeBalance(10_000, NEXT_DAY);
        killSwitch.observeOpenUnrealizedPnl(-900, NEXT_DAY);    // unchanged: the new day's zero
        assertFalse(killSwitch.isTripped(NEXT_DAY));

        killSwitch.observeOpenUnrealizedPnl(-1_260, NEXT_DAY);
        assertTrue(killSwitch.isTripped(NEXT_DAY),
                "each day measures from its own start, so the same book can trip on a later day");
    }

    @Test
    @DisplayName("an open winner cannot mask a realised loss")
    void openWinnerCannotOffsetARealisedLoss() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03);
        killSwitch.observeBalance(10_000, MORNING);
        killSwitch.seedRealizedPnl(-400, MORNING);
        killSwitch.observeOpenUnrealizedPnl(+5_000, MORNING);
        assertTrue(killSwitch.isTripped(MORNING),
                "paper gains evaporate; a realised -4% is -4% whatever the screen says");
    }

    @Test
    @DisplayName("realised PnL can be reseeded from the exchange after a restart")
    void seedingFromTheExchangeSurvivesARestart() {
        // A fresh process: nothing in memory, but the exchange's ledger still knows about the day.
        DailyLossKillSwitch afterRestart = new DailyLossKillSwitch(0.03);
        afterRestart.observeBalance(10_000, MORNING);
        afterRestart.seedRealizedPnl(-450, MORNING);
        assertTrue(afterRestart.isTripped(MORNING),
                "a restart must not be a way to clear the day's loss");
    }

    @Test
    @DisplayName("a manual trip is honoured and carries its reason")
    void manualTrip() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03);
        killSwitch.observeBalance(10_000, MORNING);
        killSwitch.trip("reconciliation drift on BTCUSDT", MORNING);

        DailyLossKillSwitch.Status status = killSwitch.evaluate(MORNING);
        assertTrue(status.tripped());
        assertEquals("reconciliation drift on BTCUSDT", status.reason());
    }

    @Test
    @DisplayName("the engine refuses every new position while the switch is tripped")
    void engineRefusesWhileTripped() {
        RiskEngine engine = RiskFixtures.engine();
        engine.killSwitch().observeBalance(10_000, MORNING);
        engine.killSwitch().seedRealizedPnl(-400, MORNING);

        RiskDecision decision = engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, MORNING);
        RiskDecision.Rejected rejected = assertInstanceOf(RiskDecision.Rejected.class, decision);
        assertEquals(RejectReason.TRADING_HALTED, rejected.reason());
    }

    @Test
    @DisplayName("the engine trades again on the next UTC day")
    void engineResumesNextDay() {
        RiskEngine engine = RiskFixtures.engine();
        engine.killSwitch().observeBalance(10_000, MORNING);
        engine.killSwitch().seedRealizedPnl(-400, MORNING);
        assertInstanceOf(RiskDecision.Rejected.class, engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, EVENING));

        assertInstanceOf(RiskDecision.Approved.class, engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 9_600, NEXT_DAY));
    }
}
