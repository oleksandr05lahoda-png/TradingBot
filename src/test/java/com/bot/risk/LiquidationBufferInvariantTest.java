package com.bot.risk;

import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The liquidation-buffer invariant: the stop sits strictly between entry and liquidation, and leaves
 * at least 30% of that distance unused.
 */
class LiquidationBufferInvariantTest {

    private static final MarginTierTable TIERS = MarginTierTable.conservativeDefault();

    @Test
    @DisplayName("buffer is |stop - liq| / |entry - liq|")
    void bufferIsTheUnusedShareOfTheDistance() {
        // Long at 100,000, 5x, 1 unit -> liq = 1,586,000/19 = 83,473.68 (see LiquidationCalculatorTest).
        // entry - liq = 314,000/19 and stop - liq = 124,000/19, so the buffer is exactly 124/314 = 62/157.
        LiquidationSafety.Buffer buffer = LiquidationSafety.evaluateForPosition(
                Side.LONG, 100_000, 90_000, 1.0, 5, TIERS, 0.0);
        assertEquals(62.0 / 157.0, buffer.fraction(), 1e-12);
        assertTrue(buffer.stopInsideLiquidation());
        assertTrue(buffer.satisfies(RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION));
    }

    @Test
    @DisplayName("a stop closer than 30% of the way is refused")
    void stopTooCloseToLiquidationFails() {
        // stop - liq = 67,000/19, so the buffer is 67/314 = 21.3%.
        LiquidationSafety.Buffer buffer = LiquidationSafety.evaluateForPosition(
                Side.LONG, 100_000, 87_000, 1.0, 5, TIERS, 0.0);
        assertEquals(67.0 / 314.0, buffer.fraction(), 1e-12);
        assertFalse(buffer.satisfies(RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION));
    }

    @Test
    @DisplayName("a stop beyond the liquidation price scores zero, not a large number")
    void stopBeyondLiquidationIsNotSafe() {
        LiquidationSafety.Buffer buffer = LiquidationSafety.evaluateForPosition(
                Side.LONG, 100_000, 80_000, 1.0, 5, TIERS, 0.0);
        assertFalse(buffer.stopInsideLiquidation(),
                "a stop below the liquidation price would never be reached — the position is gone first");
        assertEquals(0.0, buffer.fraction());
        assertFalse(buffer.satisfies(RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION));
    }

    @Test
    @DisplayName("the engine refuses a plan whose stop is too near liquidation, and says what would pass")
    void engineRefusesAndSuggestsALowerLeverage() {
        // Sizes to 0.035 BTC / $3,500 notional: the first bracket, 1% maintenance. There 5x puts
        // liquidation 19.14% below entry, so a stop 14% below leaves 26.9% of the distance: too thin.
        RiskEngine engine = RiskFixtures.engine();
        RiskDecision decision = engine.evaluate(
                RiskFixtures.request(Side.LONG, 100_000, 86_000, 5), 100_000, RiskFixtures.NOON);

        RiskDecision.Rejected rejected = assertInstanceOf(RiskDecision.Rejected.class, decision);
        assertEquals(RejectReason.LIQUIDATION_BUFFER, rejected.reason());
        assertTrue(rejected.detail().contains("passes at") || rejected.detail().contains("no leverage"),
                "the refusal should tell the operator what would work: " + rejected.detail());
    }

    @Test
    @DisplayName("the same trade at lower leverage passes")
    void lowerLeverageRestoresTheBuffer() {
        // Same trade at 2x: liquidation drops to 49.4% below entry and the buffer becomes 71.7%.
        RiskEngine engine = RiskFixtures.engine();
        RiskDecision decision = engine.evaluate(
                RiskFixtures.request(Side.LONG, 100_000, 86_000, 2), 100_000, RiskFixtures.NOON);
        assertInstanceOf(RiskDecision.Approved.class, decision);
    }

    @Test
    @DisplayName("a config may tighten the buffer above 30% but never below it")
    void bufferFloorCannotBeLowered() {
        RiskConfig tighter = new RiskConfig(0.005, 5, 1.0, Double.POSITIVE_INFINITY, 2.0, 2.0, 3, 0.03,
                0.50, 0.5, 0.0005, 2.0, TakeProfitPolicy.standard());
        assertEquals(0.50, tighter.minLiquidationBufferFraction());

        IllegalArgumentException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new RiskConfig(0.005, 5, 1.0, Double.POSITIVE_INFINITY, 2.0, 2.0, 3, 0.03,
                        0.29, 0.5, 0.0005, 2.0, TakeProfitPolicy.standard()));
        assertTrue(thrown.getMessage().contains("below the hard floor"), thrown.getMessage());
    }

    @Test
    @DisplayName("INVARIANT: no plan the engine ever approves violates the buffer")
    void noApprovedPlanEverViolatesTheInvariant() {
        Random random = new Random(RiskFixtures.seed() + 11);
        int approved = 0;

        for (int i = 0; i < 8_000; i++) {
            RiskEngine engine = RiskFixtures.engine();
            double balance = 500 + random.nextDouble() * 500_000;
            double entry = 10 + random.nextDouble() * 90_000;
            double stopFraction = 0.002 + random.nextDouble() * 0.35;
            int leverage = 1 + random.nextInt(RiskConstants.MAX_LEVERAGE);
            Side side = random.nextBoolean() ? Side.LONG : Side.SHORT;
            double stop = side == Side.LONG ? entry * (1 - stopFraction) : entry * (1 + stopFraction);

            RiskDecision decision = engine.evaluate(
                    RiskFixtures.request(side, entry, stop, leverage), balance, RiskFixtures.NOON);
            if (!(decision instanceof RiskDecision.Approved a)) continue;
            approved++;
            TradePlan plan = a.plan();

            double liq = plan.liquidationPrice();
            double stopPrice = plan.stopPrice().doubleValue();
            double entryPrice = plan.entryPrice().doubleValue();

            boolean inside = plan.side() == Side.LONG
                    ? liq < stopPrice && stopPrice < entryPrice
                    : entryPrice < stopPrice && stopPrice < liq;
            assertTrue(inside, "approved plan has its stop outside the liquidation price: " + plan);

            double measured = Math.abs(stopPrice - liq) / Math.abs(entryPrice - liq);
            assertTrue(measured >= RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION - 1e-9,
                    "approved plan has a buffer of " + measured + ": " + plan);
            assertEquals(measured, plan.liquidationBufferFraction(), 1e-9,
                    "the plan reports a buffer it does not have: " + plan);
        }
        assertTrue(approved > 200,
                "only " + approved + " plans were approved out of 8000 — the sweep is not exercising "
                        + "the approval path, so the invariant above proves little");
    }

    @Test
    @DisplayName("highestSafeLeverage reports a leverage that genuinely passes")
    void highestSafeLeverageIsHonest() {
        int safe = LiquidationSafety.highestSafeLeverage(Side.LONG, 100_000, 87_000, 1.0,
                RiskConstants.MAX_LEVERAGE, TIERS, 0.0, RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION);
        assertTrue(safe >= 1 && safe <= RiskConstants.MAX_LEVERAGE, "got " + safe);
        assertTrue(LiquidationSafety.evaluateForPosition(Side.LONG, 100_000, 87_000, 1.0, safe, TIERS, 0.0)
                        .satisfies(RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION),
                safe + "x was reported as safe but does not satisfy the buffer");
        if (safe < RiskConstants.MAX_LEVERAGE) {
            assertFalse(LiquidationSafety.evaluateForPosition(Side.LONG, 100_000, 87_000, 1.0, safe + 1, TIERS, 0.0)
                            .satisfies(RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION),
                    (safe + 1) + "x also passes, so " + safe + "x was not the highest");
        }
    }
}
