package com.bot.risk;

import com.bot.core.Preconditions;

/**
 * Position size from the stop distance and nothing else:
 * {@code qty = balance * riskFraction / |entry - stop|}. No method here takes a size and returns a
 * stop — that puts the stop where it is affordable rather than where it means something. Unclamped:
 * ceilings are {@link RiskEngine}'s job. Leverage is not an input; it only moves liquidation.
 */
public final class PositionSizer {

    private PositionSizer() {}

    /** Quantity in base units that loses exactly {@code balanceUsd * riskFraction} if the stop fills. */
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
}
