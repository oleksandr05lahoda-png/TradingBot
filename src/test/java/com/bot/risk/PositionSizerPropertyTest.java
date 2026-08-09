package com.bot.risk;

import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for the sizing rule. The seed comes from {@code -Dbot.test.seed} (set by the
 * Gradle build), so a failing case from CI replays exactly.
 *
 * <p>Asserted against {@link PositionSizer}, the unclamped rule, not {@link RiskEngine}: the engine's
 * ceilings can legitimately reduce a size, which would give every property an escape clause. That
 * those ceilings only ever reduce is asserted in {@link RiskEngineLimitsTest}.
 */
class PositionSizerPropertyTest {

    private static final int CASES = 20_000;

    @Test
    @DisplayName("property: money at risk is exactly balance x riskFraction, for every input")
    void riskInDollarsIsInvariant() {
        Random random = new Random(RiskFixtures.seed());
        for (int i = 0; i < CASES; i++) {
            double balance = 10 + random.nextDouble() * 1_000_000;
            double riskFraction = 1e-5 + random.nextDouble() * (RiskConstants.MAX_RISK_FRACTION_PER_TRADE - 1e-5);
            double entry = 0.00001 + random.nextDouble() * 100_000;
            double stopDistance = entry * (0.0001 + random.nextDouble() * 0.5);
            Side side = random.nextBoolean() ? Side.LONG : Side.SHORT;
            double stop = side == Side.LONG ? entry - stopDistance : entry + stopDistance;
            if (stop <= 0) continue;

            double qty = PositionSizer.quantityForRisk(balance, riskFraction, entry, stop);
            double risk = PositionSizer.riskUsd(qty, entry, stop);
            double expected = balance * riskFraction;

            assertEquals(expected, risk, expected * 1e-9,
                    "risk drifted from the budget at case " + i + " (seed " + RiskFixtures.seed()
                            + "): balance=" + balance + " riskFraction=" + riskFraction
                            + " entry=" + entry + " stop=" + stop);
        }
    }

    @Test
    @DisplayName("property: a farther stop always produces a strictly smaller size")
    void fartherStopMeansSmallerSize() {
        Random random = new Random(RiskFixtures.seed() + 1);
        for (int i = 0; i < CASES; i++) {
            double balance = 100 + random.nextDouble() * 500_000;
            double riskFraction = 0.001 + random.nextDouble() * 0.009;
            double entry = 1 + random.nextDouble() * 50_000;
            double nearDistance = entry * (0.001 + random.nextDouble() * 0.1);
            double farDistance = nearDistance * (1.0001 + random.nextDouble() * 3);
            Side side = random.nextBoolean() ? Side.LONG : Side.SHORT;

            double nearStop = side == Side.LONG ? entry - nearDistance : entry + nearDistance;
            double farStop = side == Side.LONG ? entry - farDistance : entry + farDistance;
            if (nearStop <= 0 || farStop <= 0) continue;

            double nearQty = PositionSizer.quantityForRisk(balance, riskFraction, entry, nearStop);
            double farQty = PositionSizer.quantityForRisk(balance, riskFraction, entry, farStop);

            assertTrue(farQty < nearQty,
                    "a wider stop did not shrink the position at case " + i + " (seed " + RiskFixtures.seed()
                            + "): near=" + nearQty + " far=" + farQty);
        }
    }

    @Test
    @DisplayName("property: size scales linearly with balance")
    void sizeScalesWithBalance() {
        Random random = new Random(RiskFixtures.seed() + 2);
        for (int i = 0; i < CASES; i++) {
            double balance = 100 + random.nextDouble() * 100_000;
            double multiplier = 1.1 + random.nextDouble() * 20;
            double riskFraction = 0.001 + random.nextDouble() * 0.009;
            double entry = 1 + random.nextDouble() * 20_000;
            double stop = entry * (1 - (0.001 + random.nextDouble() * 0.2));

            double small = PositionSizer.quantityForRisk(balance, riskFraction, entry, stop);
            double large = PositionSizer.quantityForRisk(balance * multiplier, riskFraction, entry, stop);

            assertEquals(small * multiplier, large, small * multiplier * 1e-9,
                    "size is not linear in balance at case " + i + " (seed " + RiskFixtures.seed() + ")");
        }
    }

    @Test
    @DisplayName("property: direction does not change the size for the same stop distance")
    void longAndShortSizeIdenticallyForTheSameDistance() {
        Random random = new Random(RiskFixtures.seed() + 3);
        for (int i = 0; i < CASES; i++) {
            double balance = 100 + random.nextDouble() * 100_000;
            double riskFraction = 0.001 + random.nextDouble() * 0.009;
            double entry = 10 + random.nextDouble() * 20_000;
            double distance = entry * (0.001 + random.nextDouble() * 0.3);

            double longQty = PositionSizer.quantityForRisk(balance, riskFraction, entry, entry - distance);
            double shortQty = PositionSizer.quantityForRisk(balance, riskFraction, entry, entry + distance);

            assertEquals(longQty, shortQty, longQty * 1e-12,
                    "long and short sized differently for the same distance at case " + i);
        }
    }

    @Test
    @DisplayName("leverage is not an input to sizing and cannot become one")
    void sizingHasNoLeverageParameter() {
        boolean anyMethodMentionsLeverage = java.util.Arrays.stream(PositionSizer.class.getDeclaredMethods())
                .anyMatch(m -> m.getName().toLowerCase(java.util.Locale.ROOT).contains("leverage"));
        assertTrue(!anyMethodMentionsLeverage,
                "PositionSizer grew a leverage-aware method. Size comes from the stop; leverage only "
                        + "moves the liquidation price.");
    }

    @Test
    @DisplayName("a risk fraction above the hard cap is refused, not clamped")
    void riskFractionAboveHardCapIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> PositionSizer.quantityForRisk(
                10_000, RiskConstants.MAX_RISK_FRACTION_PER_TRADE + 0.0001, 100, 95));
    }

    @Test
    @DisplayName("a stop equal to the entry is refused: no finite size carries the intended risk")
    void zeroStopDistanceIsRefused() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> PositionSizer.quantityForRisk(10_000, 0.005, 100, 100));
        assertTrue(thrown.getMessage().contains("risk per unit is zero"), thrown.getMessage());
    }

    @Test
    @DisplayName("unusable inputs are refused rather than coerced")
    void failClosedOnUnusableInputs() {
        assertThrows(IllegalArgumentException.class, () -> PositionSizer.quantityForRisk(Double.NaN, 0.005, 100, 95));
        assertThrows(IllegalArgumentException.class, () -> PositionSizer.quantityForRisk(0, 0.005, 100, 95));
        assertThrows(IllegalArgumentException.class, () -> PositionSizer.quantityForRisk(1000, 0.005, 100, Double.NaN));
        assertThrows(IllegalArgumentException.class,
                () -> PositionSizer.quantityForRisk(1000, 0.005, Double.POSITIVE_INFINITY, 95));
    }

    @Test
    @DisplayName("worked example from the README stays true")
    void readmeExample() {
        // balance $10,000, risk 0.5%, entry 64,000, stop 62,800 -> R = 1,200, qty = 50 / 1200
        double qty = PositionSizer.quantityForRisk(10_000, 0.005, 64_000, 62_800);
        assertEquals(0.0416666667, qty, 1e-9);
        assertEquals(50.0, PositionSizer.riskUsd(qty, 64_000, 62_800), 1e-9);
    }
}
