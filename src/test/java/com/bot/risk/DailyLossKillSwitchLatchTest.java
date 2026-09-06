package com.bot.risk;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The 03.09 additions: a trip that survives a restart, a breach that must persist, a stale feed that only ratchets. */
class DailyLossKillSwitchLatchTest {

    private static final Instant NOON = Instant.parse("2026-08-09T12:00:00Z");

    @TempDir Path dir;

    @Test
    @DisplayName("a trip is written down and comes back after a same-day restart")
    void tripSurvivesRestart() {
        Path latch = dir.resolve("killswitch-latch.json");
        DailyLossKillSwitch first = new DailyLossKillSwitch(0.03).withLatchFile(latch, NOON);
        first.observeBalance(1000, NOON);
        first.observeOpenUnrealizedPnl(0, NOON);
        first.seedRealizedPnl(-40, NOON);
        assertTrue(first.isTripped(NOON));

        DailyLossKillSwitch restarted = new DailyLossKillSwitch(0.03)
                .withLatchFile(latch, NOON.plusSeconds(600));
        assertTrue(restarted.isTripped(NOON.plusSeconds(600)), "the restart must not un-trip the day");
        assertTrue(restarted.evaluate(NOON.plusSeconds(600)).reason().contains("restored"));
    }

    @Test
    @DisplayName("yesterday's latch is ignored, and the rollover removes it")
    void latchFromAnotherDayIsIgnored() {
        Path latch = dir.resolve("killswitch-latch.json");
        DailyLossKillSwitch first = new DailyLossKillSwitch(0.03).withLatchFile(latch, NOON);
        first.observeBalance(1000, NOON);
        first.seedRealizedPnl(-40, NOON);
        assertTrue(first.isTripped(NOON));

        Instant tomorrow = NOON.plusSeconds(86_400);
        DailyLossKillSwitch next = new DailyLossKillSwitch(0.03).withLatchFile(latch, tomorrow);
        assertFalse(next.isTripped(tomorrow));
    }

    @Test
    @DisplayName("with a confirmation window, one breached sample blocks nothing; a persisting one latches")
    void breachMustPersist() {
        DailyLossKillSwitch sw = new DailyLossKillSwitch(0.03).withConfirmationWindowMs(20_000);
        sw.observeBalance(1000, NOON);
        sw.observeOpenUnrealizedPnl(0, NOON);
        sw.observeOpenUnrealizedPnl(-35, NOON.plusSeconds(1));          // a wick

        assertFalse(sw.evaluate(NOON.plusSeconds(1)).tripped(), "first sight: confirming, not latched");
        assertFalse(sw.evaluate(NOON.plusSeconds(10)).tripped());
        assertTrue(sw.evaluate(NOON.plusSeconds(21)).tripped(), "20 s of breach: latched");
    }

    @Test
    @DisplayName("a breach that clears inside the window restarts the count")
    void clearedBreachRestartsTheWindow() {
        DailyLossKillSwitch sw = new DailyLossKillSwitch(0.03).withConfirmationWindowMs(20_000);
        sw.observeBalance(1000, NOON);
        sw.observeOpenUnrealizedPnl(0, NOON);
        sw.observeOpenUnrealizedPnl(-35, NOON.plusSeconds(1));
        assertFalse(sw.evaluate(NOON.plusSeconds(1)).tripped());
        sw.observeOpenUnrealizedPnl(-5, NOON.plusSeconds(10));           // the wick reverted
        assertFalse(sw.evaluate(NOON.plusSeconds(10)).tripped());
        sw.observeOpenUnrealizedPnl(-35, NOON.plusSeconds(15));
        assertFalse(sw.evaluate(NOON.plusSeconds(15)).tripped(), "the NEW breach starts its own clock");
        assertFalse(sw.evaluate(NOON.plusSeconds(25)).tripped(), "only 10 s into it");
        assertTrue(sw.evaluate(NOON.plusSeconds(36)).tripped());
    }

    @Test
    @DisplayName("the default window is zero: the first breached evaluation latches, as before")
    void defaultLatchesAtOnce() {
        DailyLossKillSwitch sw = new DailyLossKillSwitch(0.03);
        sw.observeBalance(1000, NOON);
        sw.seedRealizedPnl(-40, NOON);
        assertTrue(sw.isTripped(NOON));
    }

    @Test
    @DisplayName("while the realised feed is stale, a stop-out cannot improve the measured day")
    void staleFeedRatchets() {
        DailyLossKillSwitch sw = new DailyLossKillSwitch(0.03);
        sw.observeBalance(1000, NOON);
        sw.seedRealizedPnl(0, NOON);
        sw.observeOpenUnrealizedPnl(0, NOON);
        sw.observeOpenUnrealizedPnl(-25, NOON.plusSeconds(30));
        assertEquals(-25, sw.evaluate(NOON.plusSeconds(30)).effectivePnl(), 1e-9);

        sw.markRealizedStale(true);
        sw.observeOpenUnrealizedPnl(0, NOON.plusSeconds(60));            // the loser stopped out: open loss gone
        assertEquals(-25, sw.evaluate(NOON.plusSeconds(60)).effectivePnl(), 1e-9,
                "the realised half never arrived, so the day may not read better than it did");

        sw.markRealizedStale(false);
        sw.seedRealizedPnl(-25, NOON.plusSeconds(90));                   // the feed is back with the truth
        assertEquals(-25, sw.evaluate(NOON.plusSeconds(90)).effectivePnl(), 1e-9);
    }

    @Test
    @DisplayName("while a breach is confirming, the entry gate is already shut")
    void confirmingBreachClosesTheEntryGate() {
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(0.03).withConfirmationWindowMs(20_000L);
        killSwitch.observeBalance(1000, NOON);
        killSwitch.observeOpenUnrealizedPnl(0, NOON);
        killSwitch.seedRealizedPnl(-40, NOON);
        DailyLossKillSwitch.Status status = killSwitch.evaluate(NOON);
        assertFalse(status.tripped(), "the window delays the flatten");
        assertTrue(status.confirming());

        RiskEngine engine = new RiskEngine(RiskConfig.defaults(), new ExposureBook(), killSwitch);
        RiskDecision decision = engine.evaluate(
                RiskFixtures.request(com.bot.core.Side.LONG, 64_000, 62_800, 3), 1000, NOON.plusSeconds(2));
        RiskDecision.Rejected rejected = org.junit.jupiter.api.Assertions.assertInstanceOf(
                RiskDecision.Rejected.class, decision);
        assertEquals(RejectReason.TRADING_HALTED, rejected.reason());
        assertFalse(killSwitch.evaluate(NOON.plusSeconds(2)).tripped(), "still confirming, not yet tripped");
    }

    @Test
    @DisplayName("the rollover baseline survives a restart, so a carried-in loser is still credited back")
    void rolloverBaselineSurvivesRestart() {
        Path latch = dir.resolve("killswitch-latch.json");
        Instant yesterday = Instant.parse("2026-08-08T23:00:00Z");
        Instant dayStart = Instant.parse("2026-08-09T00:10:00Z");
        DailyLossKillSwitch first = new DailyLossKillSwitch(0.03).withLatchFile(latch, yesterday);
        first.observeBalance(1000, yesterday);
        first.observeOpenUnrealizedPnl(java.util.Map.of("AAA", -10.0), yesterday);
        first.observeBalance(1000, dayStart);
        first.observeOpenUnrealizedPnl(java.util.Map.of("AAA", -40.0), dayStart);   // the rollover baseline
        assertFalse(first.isTripped(dayStart));

        // A deploy at 00:40. AAA stops out at -50 at 14:00: only -10 of it is today's.
        DailyLossKillSwitch restarted = new DailyLossKillSwitch(0.03)
                .withLatchFile(latch, dayStart.plusSeconds(1800));
        restarted.observeBalance(1000, dayStart.plusSeconds(1800));
        restarted.observeOpenUnrealizedPnl(java.util.Map.of("AAA", -40.0), dayStart.plusSeconds(1800));
        Instant afternoon = Instant.parse("2026-08-09T14:00:00Z");
        restarted.seedRealizedPnl(-50, afternoon);
        restarted.observeOpenUnrealizedPnl(java.util.Map.of(), afternoon);

        DailyLossKillSwitch.Status status = restarted.evaluate(afternoon);
        assertEquals(-10.0, status.effectivePnl(), 1e-9, "yesterday's -40 must stay credited after the restart");
        assertFalse(status.tripped());
    }
}
