package com.bot.risk;

import com.bot.core.Preconditions;
import com.bot.core.Side;

/**
 * The invariant that decides whether a sized trade may exist at all: the stop must sit strictly
 * between the entry and the liquidation price, and it must leave at least
 * {@link RiskConstants#MIN_LIQUIDATION_BUFFER_FRACTION} of the entry-to-liquidation distance unused.
 *
 * <pre>{@code   buffer = |stop - liq| / |entry - liq|   >=  0.30 }</pre>
 *
 * <p>The stop may travel at most 70% of the way to liquidation. The margin is this wide because the
 * two fire on <i>different prices</i> — the stop on its own trigger price, liquidation on the
 * exchange's mark — and on a thin book those disagree by more than people expect. A stop that is
 * merely "before" liquidation gets overtaken by a mark excursion, and the position closes on the
 * exchange's terms: the whole isolated margin, not the planned R.
 */
public final class LiquidationSafety {

    private LiquidationSafety() {}

    /**
     * @param liquidationPrice       price at which the position liquidates ({@code 0.0} = unreachable)
     * @param fraction               {@code |stop - liq| / |entry - liq|}, 0 when the distance is degenerate
     * @param stopInsideLiquidation  whether the stop sits strictly between entry and liquidation
     */
    public record Buffer(double liquidationPrice, double fraction, boolean stopInsideLiquidation) {

        public boolean satisfies(double minFraction) {
            return stopInsideLiquidation && fraction >= minFraction;
        }

        @Override public String toString() {
            return String.format("liq=%.8g buffer=%.1f%%%s",
                    liquidationPrice, fraction * 100, stopInsideLiquidation ? "" : " STOP-OUTSIDE-LIQ");
        }
    }

    /** Measures the buffer between a stop and a liquidation price. Pure geometry, no policy. */
    public static Buffer evaluate(Side side, double entryPrice, double stopPrice, double liquidationPrice) {
        Preconditions.notNull(side, "side");
        Preconditions.positiveFinite(entryPrice, "entryPrice");
        Preconditions.positiveFinite(stopPrice, "stopPrice");
        Preconditions.nonNegativeFinite(liquidationPrice, "liquidationPrice");

        boolean inside = side == Side.LONG
                ? liquidationPrice < stopPrice && stopPrice < entryPrice
                : entryPrice < stopPrice && stopPrice < liquidationPrice;

        double entryToLiq = Math.abs(entryPrice - liquidationPrice);
        double fraction = entryToLiq <= 0 ? 0.0 : Math.abs(stopPrice - liquidationPrice) / entryToLiq;
        return new Buffer(liquidationPrice, inside ? fraction : 0.0, inside);
    }

    /** Convenience: compute the liquidation price and measure the buffer in one call. */
    public static Buffer evaluateForPosition(Side side,
                                             double entryPrice,
                                             double stopPrice,
                                             double quantity,
                                             int leverage,
                                             MarginTierTable tiers,
                                             double takerFeeFraction) {
        double liq = LiquidationCalculator.isolatedLiquidationPrice(
                side, entryPrice, quantity, leverage, tiers, takerFeeFraction);
        return evaluate(side, entryPrice, stopPrice, liq);
    }

    /**
     * The highest leverage from 1 to {@code maxLeverage} at which this exact position still satisfies
     * the buffer, or 0 if none does.
     *
     * <p>Leverage does not change the position size — size comes from the stop — so this only moves
     * the liquidation price. It exists to turn a refusal into an actionable one: "rejected, but 2x
     * would pass" is a fact the operator can act on, and it is diagnostics, not an automatic retry.
     */
    public static int highestSafeLeverage(Side side,
                                          double entryPrice,
                                          double stopPrice,
                                          double quantity,
                                          int maxLeverage,
                                          MarginTierTable tiers,
                                          double takerFeeFraction,
                                          double minBufferFraction) {
        int cap = Math.min(maxLeverage, RiskConstants.MAX_LEVERAGE);
        for (int lev = cap; lev >= 1; lev--) {
            try {
                if (evaluateForPosition(side, entryPrice, stopPrice, quantity, lev, tiers, takerFeeFraction)
                        .satisfies(minBufferFraction)) {
                    return lev;
                }
            } catch (IllegalArgumentException e) {
                // This leverage cannot even be opened (fee exceeds initial margin). Try a lower one.
            }
        }
        return 0;
    }
}
