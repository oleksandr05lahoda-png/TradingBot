package com.bot.paper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * v2 differs from v1 in exactly one condition, and the difference is a PREDICATE, not just a
 * number: v1 exits at a rate AT OR BELOW its entry threshold, v2 exits only at a rate STRICTLY
 * below zero. 1452 of the 129238 dev-period settlements are exactly zero, so conflating the two
 * would silently test a rule nobody registered.
 */
class FundingCarryV2Test {

    private static final long H = 3_600_000L;
    private final CarryPaperExecutor exec = new CarryPaperExecutor();

    private static Bar bar(long openMs, double px) {
        return new Bar("BTCUSDT", openMs, H, px, px, px, px, 1.0);
    }

    private static void flat(List<Bar> perp, List<Bar> spot, int n, double spotPx, double basisBp) {
        double perpPx = spotPx * (1 + basisBp / 10_000.0);
        for (int i = 0; i <= n; i++) {
            perp.add(bar(i * H, perpPx));
            spot.add(bar(i * H, spotPx));
        }
    }

    @Test
    @DisplayName("v2 parameters: only the funding exit differs from v1")
    void onlyTheFundingExitDiffers() {
        FundingCarryV1 v1 = new FundingCarryV1();
        FundingCarryV2 v2 = new FundingCarryV2();

        assertEquals(v1.name(), v2.name(), "same family, so the same sleeve name");
        assertNotEquals(v1.version(), v2.version());
        assertEquals("v2", v2.version());

        assertEquals(FundingCarryV1.ENTRY_FUNDING, FundingCarryV2.ENTRY_FUNDING, 0.0);
        assertEquals(FundingCarryV1.EXIT_BASIS_BP, FundingCarryV2.EXIT_BASIS_BP, 0.0);
        assertEquals(v1.maxHoldBars(), v2.maxHoldBars());

        assertEquals(0.0, FundingCarryV2.EXIT_FUNDING, 0.0, "the mechanism's own boundary");
        assertNotEquals(FundingCarryV1.EXIT_FUNDING, FundingCarryV2.EXIT_FUNDING);
    }

    @Test
    @DisplayName("a settlement of exactly zero does NOT end a v2 position")
    void exactlyZeroDoesNotExit() {
        // Zero pays nothing in either direction: the reason to hold has stopped earning, not
        // reversed. Under v1's AT_OR_BELOW predicate this same settlement would have exited.
        CarryPosition v2pos = new CarryPosition("BTCUSDT", H, 0.0, FundingCarryV2.EXIT_FUNDING,
                CarryPosition.FundingExit.STRICTLY_BELOW, 1.0, "t");
        assertFalse(v2pos.fundingExitTriggered(0.0), "zero must not trigger a v2 exit");
        assertTrue(v2pos.fundingExitTriggered(-1e-9), "any negative rate must trigger it");
        assertFalse(v2pos.fundingExitTriggered(0.0001));

        CarryPosition v1pos = new CarryPosition("BTCUSDT", H, 0.0, FundingCarryV1.EXIT_FUNDING,
                CarryPosition.FundingExit.AT_OR_BELOW, 1.0, "t");
        assertTrue(v1pos.fundingExitTriggered(FundingCarryV1.EXIT_FUNDING),
                "v1 exits AT its threshold — unchanged, so v1's recorded result stays reproducible");
    }

    @Test
    @DisplayName("v2 holds through a decaying positive rate that would have ended v1")
    void v2HoldsWhereV1Exited() {
        List<Bar> perp = new ArrayList<>(), spot = new ArrayList<>();
        flat(perp, spot, 30, 1000.0, 20.0);

        // Rate decays 9bp -> 2bp -> 0.5bp but never turns negative.
        List<PaperExecutor.FundingPoint> funding = List.of(
                new PaperExecutor.FundingPoint(2 * H, 0.0009),
                new PaperExecutor.FundingPoint(10 * H, 0.0002),
                new PaperExecutor.FundingPoint(18 * H, 0.00005));

        CarryPosition v2pos = new CarryPosition("BTCUSDT", H, Double.NaN, FundingCarryV2.EXIT_FUNDING,
                CarryPosition.FundingExit.STRICTLY_BELOW, 1.0, "t");
        CarryPaperExecutor.Fill v2f = exec.simulate(v2pos, perp, spot, funding, 25);
        assertSame(CarryPaperExecutor.ExitReason.time_stop, v2f.exitReason,
                "v2 must reach the horizon when the rate never turns negative");

        CarryPosition v1pos = new CarryPosition("BTCUSDT", H, Double.NaN, FundingCarryV1.EXIT_FUNDING,
                CarryPosition.FundingExit.AT_OR_BELOW, 1.0, "t");
        CarryPaperExecutor.Fill v1f = exec.simulate(v1pos, perp, spot, funding, 25);
        assertSame(CarryPaperExecutor.ExitReason.funding_flipped, v1f.exitReason,
                "v1 exits as soon as the rate is back at 5bp or below");
        assertTrue(v1f.exitBarMs < v2f.exitBarMs, "v1 exits earlier than v2 on identical data");
    }

    @Test
    @DisplayName("v2 exits when the rate genuinely turns negative")
    void v2ExitsOnTrueSignFlip() {
        List<Bar> perp = new ArrayList<>(), spot = new ArrayList<>();
        flat(perp, spot, 30, 1000.0, 20.0);

        List<PaperExecutor.FundingPoint> funding = List.of(
                new PaperExecutor.FundingPoint(2 * H, 0.0009),
                new PaperExecutor.FundingPoint(10 * H, 0.0),        // zero: must NOT exit
                new PaperExecutor.FundingPoint(18 * H, -0.0003));   // negative: must exit

        CarryPosition pos = new CarryPosition("BTCUSDT", H, Double.NaN, FundingCarryV2.EXIT_FUNDING,
                CarryPosition.FundingExit.STRICTLY_BELOW, 1.0, "t");
        CarryPaperExecutor.Fill f = exec.simulate(pos, perp, spot, funding, 25);

        assertSame(CarryPaperExecutor.ExitReason.funding_flipped, f.exitReason);
        assertEquals(17 * H, f.exitBarMs,
                "the settlement at 18H belongs to the bar (17H, 18H] that it accrued over");
    }
}
