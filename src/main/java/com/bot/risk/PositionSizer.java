package com.bot.risk;

import com.bot.core.Preconditions;

/**
 * Position size from the stop distance, and nothing else.
 *
 * <pre>{@code   qty = balance * riskFraction / |entry - stop| }</pre>
 *
 * <p>The direction of that arrow is the point: the stop is chosen first, and the size is whatever
 * makes the loss at that stop equal the budget. Deriving the stop from a desired size is how a
 * position ends up with a stop where it is affordable rather than where it means something — so no
 * method here takes a size and returns a stop.
 *
 * <p>Pure and <b>unclamped</b>: the result always risks exactly {@code balance * riskFraction}.
 * Ceilings are {@link RiskEngine}'s job and only ever reduce. Keeping them apart is what lets
 * risk-is-constant be tested as an absolute rather than "unless some cap bit".
 *
 * <p>Leverage is not an input. It changes only how much margin is locked up, and therefore where the
 * position liquidates.
 */
public final class PositionSizer {

    private PositionSizer() {}

    /**
     * @param balanceUsd   account balance the risk fraction applies to
     * @param riskFraction fraction of balance to put at risk, e.g. 0.005 for 0.5%
     * @param entryPrice   intended entry
     * @param stopPrice    stop, on the losing side of the entry
     * @return quantity in base units; exactly {@code balanceUsd * riskFraction} is lost if the stop fills
     */
    public static double quantityForRisk(double balanceUsd, double riskFraction, double entryPrice, double stopPrice) {
        Preconditions.positiveFinite(balanceUsd, "balanceUsd");
        Preconditions.inClosedRange(riskFraction, 0.0, RiskConstants.MAX_RISK_FRACTION_PER_TRADE, "riskFraction");
        Preconditions.require(riskFraction > 0, "riskFraction must be positive, got " + riskFraction);
        Preconditions.positiveFinite(entryPrice, "entryPrice");
        Preconditions.positiveFinite(stopPrice, "stopPrice");

        double stopDistance = Math.abs(entryPrice - stopPrice);
        Preconditions.require(stopDistance > 0,
                "entry and stop are the same price (" + entryPrice + ") — the risk per unit is zero, "
                        + "so no finite size carries the intended risk");

        double qty = balanceUsd * riskFraction / stopDistance;
        Preconditions.require(Double.isFinite(qty) && qty > 0,
                "sizing produced a non-usable quantity: " + qty);
        return qty;
    }

    /** Money at risk if the stop fills at its trigger price: {@code qty * |entry - stop|}. */
    public static double riskUsd(double quantity, double entryPrice, double stopPrice) {
        Preconditions.positiveFinite(quantity, "quantity");
        Preconditions.positiveFinite(entryPrice, "entryPrice");
        Preconditions.positiveFinite(stopPrice, "stopPrice");
        return quantity * Math.abs(entryPrice - stopPrice);
    }

    /** Risk as a fraction of balance, the inverse of {@link #quantityForRisk}. */
    public static double riskFractionOf(double quantity, double entryPrice, double stopPrice, double balanceUsd) {
        Preconditions.positiveFinite(balanceUsd, "balanceUsd");
        return riskUsd(quantity, entryPrice, stopPrice) / balanceUsd;
    }
}
