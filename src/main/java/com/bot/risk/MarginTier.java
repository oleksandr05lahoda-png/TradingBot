package com.bot.risk;

import com.bot.core.Preconditions;

/**
 * One maintenance-margin bracket as Binance returns it from {@code GET /fapi/v1/leverageBracket}
 * (payload keys: notionalFloor, notionalCap, maintMarginRatio, cum, initialLeverage). Maintenance
 * margin is piecewise linear, not a flat percentage:
 * {@code maintenanceMargin = notional * maintenanceMarginRate - maintenanceAmount}. The subtracted
 * {@code maintenanceAmount} ("cum") keeps it continuous across boundaries; a bare percentage
 * overstates the requirement above the first bracket.
 */
public record MarginTier(
        double notionalFloor,
        double notionalCap,
        double maintenanceMarginRate,
        double maintenanceAmount,
        int maxLeverage) {

    public MarginTier {
        Preconditions.nonNegativeFinite(notionalFloor, "notionalFloor");
        Preconditions.require(notionalCap > notionalFloor,
                "notionalCap " + notionalCap + " must exceed notionalFloor " + notionalFloor);
        // Strictly below 1: at 1.0 the long denominator q*(MMR-1) is zero and the clamp would read
        // the resulting non-finite price as a permissive "unreachable".
        Preconditions.require(maintenanceMarginRate > 0 && maintenanceMarginRate < 1.0,
                "maintenanceMarginRate must be in (0, 1), got " + maintenanceMarginRate);
        Preconditions.nonNegativeFinite(maintenanceAmount, "maintenanceAmount");
        Preconditions.positive(maxLeverage, "maxLeverage");
    }

    public boolean contains(double notional) {
        return notional > notionalFloor - 1e-9 && notional <= notionalCap;
    }

    /** Maintenance margin required for {@code notional}, assuming it falls inside this bracket. */
    public double maintenanceMargin(double notional) {
        return notional * maintenanceMarginRate - maintenanceAmount;
    }
}
