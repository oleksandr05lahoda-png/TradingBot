package com.bot.risk;

import com.bot.core.Preconditions;
import com.bot.core.Side;

/**
 * The stop must sit strictly between entry and liquidation, leaving at least
 * {@link RiskConstants#MIN_LIQUIDATION_BUFFER_FRACTION} of the entry-to-liquidation distance unused.
 *
 * <pre>{@code   buffer = |stop - liq| / |entry - liq|   >=  0.30 }</pre>
 *
 * <p>This wide because the two fire on <i>different prices</i> — the stop on its own trigger,
 * liquidation on the exchange's mark. A stop merely "before" liquidation is overtaken by a mark
 * excursion, closing the position for the whole isolated margin rather than the planned R.
 */
public final class LiquidationSafety {

    private LiquidationSafety() {}

    /**
     * {@code liquidationPrice} of {@code 0.0} means unreachable;
     * {@code fraction = |stop - liq| / |entry - liq|}, and 0 when that distance is degenerate.
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
     * the buffer, or 0 if none does. Leverage does not change size — it only moves the liquidation
     * price — and this is diagnostics for the rejection message, not an automatic retry.
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
