package com.bot.risk;

import com.bot.core.InstrumentFilters;
import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step 9b: one lot step up when the floored size would be refused by the exchange minimum, only
 * inside an armed tolerance. Off by default, so every other test keeps the never-round-up world.
 * Numbers: $100 balance, 0.5% = $0.50 of risk, entry 2.0, a 1-unit lot, $5 minimum notional.
 */
class LotRoundUpTest {

    /** Lot 1, $5 minimum: at entry 2.0 the floor lands on 2 units = $4 and is refused. */
    private static InstrumentFilters coarse() {
        return InstrumentFilters.of("COARSEUSDT", "0.01", "1", "5");
    }

    private static RiskDecision decide(double stop, double tolerance) {
        RiskEngine engine = RiskFixtures.engine().withLotRoundUpTolerance(tolerance);
        return engine.evaluate(RiskFixtures.request(Side.LONG, 2.0, stop, 2, coarse()), 100, RiskFixtures.NOON);
    }

    @Test
    @DisplayName("off: the floored size under $5 is refused, exactly as before")
    void offRefuses() {
        // stop 1.82 -> R 0.18 -> ideal 2.78 -> floor 2 -> $4.00 < $5
        RiskDecision.Rejected rejected = assertInstanceOf(RiskDecision.Rejected.class, decide(1.82, 0.0));
        assertEquals(RejectReason.BELOW_MIN_NOTIONAL, rejected.reason());
    }

    @Test
    @DisplayName("on: one step up inside the tolerance is approved, and the overrun is stated")
    void oneStepUpInsideToleranceIsApproved() {
        // floor 2 ($4) -> 3 units = $6 notional, risk 3 x 0.18 = $0.54 = +8% over $0.50
        TradePlan plan = assertInstanceOf(RiskDecision.Approved.class, decide(1.82, 0.20)).plan();
        assertEquals(0, plan.quantity().compareTo(new BigDecimal("3")));
        assertEquals(0.54, plan.riskUsd(), 1e-9);
        assertTrue(plan.riskUsd() <= 0.50 * 1.20 + 1e-9, "never more than the armed tolerance");
        assertTrue(plan.sizingNote().contains("rounded UP 1 lot step(s)"), plan.sizingNote());
    }

    @Test
    @DisplayName("on: a step that would overrun the tolerance is still refused")
    void oneStepUpBeyondToleranceIsRefused() {
        // stop 1.78 -> R 0.22 -> ideal 2.27 -> floor 2 ($4) -> 3 units risk $0.66 = +32% > 20%
        RiskDecision.Rejected rejected = assertInstanceOf(RiskDecision.Rejected.class, decide(1.78, 0.20));
        assertEquals(RejectReason.BELOW_MIN_NOTIONAL, rejected.reason());
    }

    @Test
    @DisplayName("on: a size that already clears the minimum is never rounded up")
    void noRoundUpWhenNotNeeded() {
        // stop 1.875 -> quantized to the 0.01 tick towards entry = 1.88 -> R 0.12 -> ideal 4.17 ->
        // floor 4 -> $8 >= $5: untouched, and the risk stays UNDER the $0.50 budget (4 x 0.12).
        TradePlan plan = assertInstanceOf(RiskDecision.Approved.class, decide(1.875, 0.20)).plan();
        assertEquals(0, plan.quantity().compareTo(new BigDecimal("4")));
        assertTrue(plan.riskUsd() <= 0.50 + 1e-9, "no round-up means no overrun: " + plan.riskUsd());
        assertTrue(!plan.sizingNote().contains("rounded UP"), plan.sizingNote());
    }

    /** Lot 1 worth $0.50: $5 needs 10 units, and one step from the floor never gets there. */
    private static InstrumentFilters cents() {
        return InstrumentFilters.of("CENTSUSDT", "0.01", "1", "5");
    }

    private static RiskDecision decideCents(double stop, double tolerance) {
        RiskEngine engine = RiskFixtures.engine().withLotRoundUpTolerance(tolerance);
        return engine.evaluate(RiskFixtures.request(Side.LONG, 0.5, stop, 2, cents()), 100, RiskFixtures.NOON);
    }

    @Test
    @DisplayName("several lot steps to the minimum: a coin one step could never clear is sized to $5")
    void roundsAllTheWayToTheMinimum() {
        // stop 0.44 -> R 0.06 -> ideal 8.33 -> floor 8 ($4.00, refused); one step = 9 ($4.50, still
        // refused - the rule before 05.09); the minimum is 10 units = $5.00, risk 0.60 = +20%.
        TradePlan plan = assertInstanceOf(RiskDecision.Approved.class, decideCents(0.44, 0.20)).plan();
        assertEquals(0, plan.quantity().compareTo(new BigDecimal("10")));
        assertEquals(0.60, plan.riskUsd(), 1e-9);
        assertTrue(plan.sizingNote().contains("rounded UP 2 lot step(s)"), plan.sizingNote());
    }

    @Test
    @DisplayName("the minimum beyond the tolerance is still refused, however many steps it is")
    void minimumBeyondToleranceIsRefused() {
        // stop 0.43 -> R 0.07 -> ideal 7.14 -> floor 7 ($3.50); minimum 10 units risk 0.70 = +40% > 20%
        RiskDecision.Rejected rejected = assertInstanceOf(RiskDecision.Rejected.class, decideCents(0.43, 0.20));
        assertEquals(RejectReason.BELOW_MIN_NOTIONAL, rejected.reason());
        // ...and approved once the operator widens the dial to 0.5
        TradePlan plan = assertInstanceOf(RiskDecision.Approved.class, decideCents(0.43, 0.50)).plan();
        assertEquals(0, plan.quantity().compareTo(new BigDecimal("10")));
        assertEquals(0.70, plan.riskUsd(), 1e-9);
    }

    @Test
    @DisplayName("a tolerance of 1.0 lets a floor-constrained coin run up to the 1% cap and no further")
    void fullToleranceStopsAtTheCap() {
        // coarse: stop 1.70 -> R 0.30 -> ideal 1.67 -> floor 1 ($2); minimum 3 units risk 0.90 = +80%,
        // inside 1.0 and under the $1.00 cap -> approved
        TradePlan plan = assertInstanceOf(RiskDecision.Approved.class, decide(1.70, 1.0)).plan();
        assertEquals(0, plan.quantity().compareTo(new BigDecimal("3")));
        assertEquals(0.90, plan.riskUsd(), 1e-9);
        // stop 1.60 -> R 0.40 -> minimum 3 units risk 1.20 > the $1.00 cap -> refused even at 1.0
        RiskDecision.Rejected rejected = assertInstanceOf(RiskDecision.Rejected.class, decide(1.60, 1.0));
        assertEquals(RejectReason.BELOW_MIN_NOTIONAL, rejected.reason());
    }

    @Test
    @DisplayName("a tolerance above 1.0 is refused by the engine itself")
    void toleranceAboveOneIsRefused() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> RiskFixtures.engine().withLotRoundUpTolerance(1.5));
    }

    @Test
    @DisplayName("the 1% hard cap binds even inside a generous tolerance")
    void hardCapStillBinds() {
        // $100 balance at 1% cap = $1.00 max risk. Stop 1.60 -> R 0.40 -> ideal 1.25 -> floor 1 ($2, refused)
        // -> 2 units risk $0.80 = +60%: beyond a 0.5 tolerance? 0.80 <= 0.50*1.5 = 0.75 is false -> refused.
        RiskDecision.Rejected rejected = assertInstanceOf(RiskDecision.Rejected.class, decide(1.60, 0.5));
        assertEquals(RejectReason.BELOW_MIN_NOTIONAL, rejected.reason());
    }
}
