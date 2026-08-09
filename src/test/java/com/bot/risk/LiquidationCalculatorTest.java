package com.bot.risk;

import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The liquidation price, checked against numbers worked by hand from Binance's formula rather than
 * against whatever the implementation happens to produce.
 *
 * <p>Every expected value below is derived in the comment above it, so a future change that alters
 * the arithmetic has to argue with the derivation and not just re-record a new number.
 */
class LiquidationCalculatorTest {

    private static final MarginTierTable TIERS = MarginTierTable.conservativeDefault();
    private static final double NO_FEE = 0.0;

    @Test
    @DisplayName("long, 1 unit at 100000, 5x: liq = (WB + cum - q*EP) / (q*(MMR - 1))")
    void longWorkedExample() {
        // notional 100,000 -> bracket 25k..100k: MMR 5%, cum 700. WB = 100,000/5 = 20,000.
        // liq = (20000 + 700 - 100000) / (1 * (0.05 - 1)) = -79,300 / -0.95 = 83,473.6842...
        // Bracket at the liquidation price (notional 83,474) is the same one, so it converges at once.
        double liq = LiquidationCalculator.isolatedLiquidationPrice(Side.LONG, 100_000, 1.0, 5, TIERS, NO_FEE);
        assertEquals(83_473.6842105263, liq, 1e-6);
    }

    @Test
    @DisplayName("short, 1 unit at 100000, 5x: the bracket at liquidation differs, so the solver iterates")
    void shortWorkedExampleCrossesABracket() {
        // First pass with the entry bracket (MMR 5%, cum 700):
        //   liq = (20000 + 700 + 100000) / (1 * (0.05 + 1)) = 120,700 / 1.05 = 114,952.38
        // At 114,952 the notional falls in the 100k..250k bracket (MMR 10%, cum 5,700), so re-solve:
        //   liq = (20000 + 5700 + 100000) / (1 * (0.10 + 1)) = 125,700 / 1.10 = 114,272.7272...
        // 114,273 is still inside 100k..250k, so that is the fixed point.
        double liq = LiquidationCalculator.isolatedLiquidationPrice(Side.SHORT, 100_000, 1.0, 5, TIERS, NO_FEE);
        assertEquals(114_272.7272727273, liq, 1e-6);
    }

    @Test
    @DisplayName("the entry fee moves liquidation towards entry, never away")
    void takerFeeTightensTheLiquidationPrice() {
        double withoutFee = LiquidationCalculator.isolatedLiquidationPrice(
                Side.LONG, 100_000, 1.0, 5, TIERS, 0.0);
        double withFee = LiquidationCalculator.isolatedLiquidationPrice(
                Side.LONG, 100_000, 1.0, 5, TIERS, RiskConstants.DEFAULT_TAKER_FEE_FRACTION);
        // WB drops by 100,000 * 0.0005 = 50, so liq = (19950 + 700 - 100000) / -0.95 = 83,526.3157...
        assertEquals(83_526.3157894737, withFee, 1e-6);
        assertTrue(withFee > withoutFee, "the fee should bring liquidation closer to a long's entry");
    }

    @Test
    @DisplayName("it is not the naive entry x (1 - 1/leverage), and it is always the safer side of it")
    void isNotNaiveInverseLeverage() {
        Random random = new Random(RiskFixtures.seed() + 7);
        for (int i = 0; i < 2_000; i++) {
            double entry = 10 + random.nextDouble() * 50_000;
            double qty = 0.001 + random.nextDouble() * 5;
            int leverage = 1 + random.nextInt(RiskConstants.MAX_LEVERAGE);
            double notional = entry * qty;
            if (leverage > TIERS.maxLeverageAt(notional)) continue;

            double liq = LiquidationCalculator.isolatedLiquidationPrice(
                    Side.LONG, entry, qty, leverage, TIERS, NO_FEE);
            double naive = entry * (1 - 1.0 / leverage);
            assertTrue(liq >= naive - 1e-6,
                    "the real liquidation price must never be further from entry than the naive one "
                            + "(entry=" + entry + " qty=" + qty + " lev=" + leverage + "): real=" + liq
                            + " naive=" + naive);
        }
    }

    @Test
    @DisplayName("a long at 1x has no reachable liquidation price above zero")
    void longAtOneTimesLeverageCannotLiquidate() {
        double liq = LiquidationCalculator.isolatedLiquidationPrice(Side.LONG, 1_000, 1.0, 1, TIERS, NO_FEE);
        assertEquals(0.0, liq, 0.0);
    }

    @Test
    @DisplayName("property: liquidation moves towards entry as leverage rises")
    void higherLeverageMeansCloserLiquidation() {
        Random random = new Random(RiskFixtures.seed() + 8);
        for (int i = 0; i < 2_000; i++) {
            double entry = 100 + random.nextDouble() * 10_000;
            double qty = 0.01 + random.nextDouble() * 2;
            double notional = entry * qty;
            int cap = Math.min(RiskConstants.MAX_LEVERAGE, TIERS.maxLeverageAt(notional));
            if (cap < 2) continue;

            Side side = random.nextBoolean() ? Side.LONG : Side.SHORT;
            double previousDistance = Double.MAX_VALUE;
            for (int leverage = 1; leverage <= cap; leverage++) {
                double liq = LiquidationCalculator.isolatedLiquidationPrice(
                        side, entry, qty, leverage, TIERS, NO_FEE);
                double distance = Math.abs(entry - liq);
                assertTrue(distance <= previousDistance + 1e-6,
                        "raising leverage from " + (leverage - 1) + " to " + leverage
                                + " moved liquidation further away (side=" + side + ")");
                previousDistance = distance;
            }
        }
    }

    @Test
    @DisplayName("a long liquidates below entry and a short above, always")
    void liquidationIsAlwaysOnTheLosingSide() {
        Random random = new Random(RiskFixtures.seed() + 9);
        for (int i = 0; i < 5_000; i++) {
            double entry = 0.01 + random.nextDouble() * 30_000;
            double qty = 0.001 + random.nextDouble() * 10;
            int leverage = 1 + random.nextInt(RiskConstants.MAX_LEVERAGE);
            double notional = entry * qty;
            if (leverage > TIERS.maxLeverageAt(notional)) continue;

            double longLiq = LiquidationCalculator.isolatedLiquidationPrice(
                    Side.LONG, entry, qty, leverage, TIERS, NO_FEE);
            double shortLiq = LiquidationCalculator.isolatedLiquidationPrice(
                    Side.SHORT, entry, qty, leverage, TIERS, NO_FEE);
            assertTrue(longLiq <= entry, "long liquidation " + longLiq + " is above entry " + entry);
            assertTrue(shortLiq >= entry, "short liquidation " + shortLiq + " is below entry " + entry);
        }
    }

    @Test
    @DisplayName("a fee larger than the initial margin is refused, not silently negative")
    void feeExceedingInitialMarginIsRefused() {
        // 1x leverage with a 1% fee still leaves margin; construct the impossible case directly.
        assertThrows(IllegalArgumentException.class,
                () -> LiquidationCalculator.isolatedWalletBalanceAtOpen(1_000, 200, 0.01));
    }

    @Test
    @DisplayName("a bracket whose maintenance margin exceeds the initial margin clamps to entry, not past it")
    void instantlyLiquidatableClampsToEntry() {
        // A single bracket at 30% maintenance with 3x leverage (33% initial margin) is nearly
        // liquidatable at open; at 25% initial margin (4x) it already is.
        MarginTierTable harsh = new MarginTierTable(List.of(
                new MarginTier(0, Double.POSITIVE_INFINITY, 0.30, 0, 5)));
        double liq = LiquidationCalculator.isolatedLiquidationPrice(Side.LONG, 1_000, 1.0, 4, harsh, NO_FEE);
        assertEquals(1_000.0, liq, 1e-9,
                "an already-liquidatable position should report liquidation at entry so the buffer "
                        + "check refuses it, rather than throwing from inside a pricing routine");
    }
}
