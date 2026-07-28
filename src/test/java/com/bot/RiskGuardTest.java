package com.bot;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * project_state id=25 — the four refusals the gate exists for.
 *
 * Every test uses an isolated guard (no disk persistence), so none of them inherits or clobbers
 * the running bot's ./data/riskguard.csv and none depends on what the last trading day left behind.
 *
 * These assert REFUSALS specifically. The failure that matters here is a gate that says yes when it
 * cannot know — that is the shape of id=2, id=22, id=28 and id=29 alike.
 */
class RiskGuardTest {

    private static final String SYM = "BTCUSDT";
    private static final double BALANCE = 1_000.0;

    /** A guard whose BTC feed is fresh, so staleness does not mask the check under test. */
    private RiskGuard guardWithFreshBtcFeed() {
        RiskGuard g = RiskGuard.newIsolatedForTest();
        g.updateBtcPrice(60_000.0);
        return g;
    }

    @Test
    @DisplayName("daily loss limit breached -> order refused")
    void dailyLossLimitBlocks() {
        RiskGuard g = guardWithFreshBtcFeed();

        // Establish today's starting balance, then realize a loss past the 10% default cap.
        assertTrue(g.canTrade(SYM, BALANCE, 10.0).allowed,
                "a clean guard with a fresh feed and no exposure should allow");

        g.recordTradeOpened(SYM, 100.0);
        g.recordTradeClosed(SYM, -150.0);   // -15% of the 1000 day-start balance

        RiskGuard.Decision d = g.canTrade("ETHUSDT", BALANCE, 10.0);
        assertFalse(d.allowed, "15% realized daily loss must block, cap is 10%");
        assertTrue(d.reason.contains("daily loss"), "reason should name the daily loss limit, got: " + d.reason);
    }

    @Test
    @DisplayName("aggregate exposure cap reached -> order refused")
    void aggregateExposureCapBlocks() {
        // Isolated from the position COUNT cap on purpose: RG_MAX_CONCURRENT_POSITIONS defaults to
        // 1, so with any position already booked the count would block first and this test would
        // pass for the wrong reason. Exposure here comes from the incoming order alone.
        // Default RG_MAX_AGGREGATE_NOTIONAL_PCT is 60% => $600 on a $1000 balance.
        RiskGuard g = guardWithFreshBtcFeed();

        RiskGuard.Decision over = g.canTrade("ETHUSDT", BALANCE, 610.0);
        assertFalse(over.allowed, "610 of 1000 is 61%, above the 60% aggregate cap");
        assertTrue(over.reason.contains("aggregate"),
                "reason should name the aggregate cap, got: " + over.reason);

        assertTrue(g.canTrade("ETHUSDT", BALANCE, 590.0).allowed,
                "590 of 1000 is 59% and must still pass");

        // Booked exposure counts too, not just the incoming order.
        RiskGuard g2 = guardWithFreshBtcFeed();
        g2.recordTradeOpened("SOLUSDT", 550.0);
        RiskGuard.Decision withOpen = g2.canTrade("ETHUSDT", BALANCE, 100.0);   // 550 + 100 = 65%
        assertFalse(withOpen.allowed, "open notional must count toward the cap");
        assertTrue(withOpen.reason.contains("aggregate"),
                "aggregate check runs before the count cap, got: " + withOpen.reason);
    }

    @Test
    @DisplayName("balance read failed -> order refused, not treated as zero")
    void unknownBalanceBlocks() {
        RiskGuard g = guardWithFreshBtcFeed();

        for (double bad : new double[]{0.0, -1.0, Double.NaN}) {
            RiskGuard.Decision d = g.canTrade(SYM, bad, 10.0);
            assertFalse(d.allowed, "balance " + bad + " is unknown, not permission to trade");
            assertTrue(d.reason.contains("balance"), "reason should name the balance, got: " + d.reason);
        }
    }

    @Test
    @DisplayName("BTC feed stale or absent -> order refused")
    void staleBtcFeedBlocks() {
        // Never fed: an unfed crash detector is not a calm market.
        RiskGuard never = RiskGuard.newIsolatedForTest();
        RiskGuard.Decision absent = never.canTrade(SYM, BALANCE, 10.0);
        assertFalse(absent.allowed, "a crash detector that was never fed must refuse");
        assertTrue(absent.reason.contains("BTC"), "reason should name the BTC feed, got: " + absent.reason);

        // Fed, but longer ago than RG_BTC_FEED_MAX_AGE_MS (10 min default).
        RiskGuard stale = RiskGuard.newIsolatedForTest();
        stale.updateBtcPriceAt(60_000.0, System.currentTimeMillis() - 11 * 60_000L);
        RiskGuard.Decision old = stale.canTrade(SYM, BALANCE, 10.0);
        assertFalse(old.allowed, "an 11-minute-old BTC sample is past the 10-minute limit");
        assertTrue(old.reason.contains("BTC"), "reason should name the BTC feed, got: " + old.reason);
    }
}
