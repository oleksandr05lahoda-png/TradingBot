package com.bot.risk;

import com.bot.core.Preconditions;

/**
 * One maintenance-margin bracket, shaped as Binance returns it from
 * {@code GET /fapi/v1/leverageBracket}. Maintenance margin is piecewise linear, not a flat percentage:
 *
 * <pre>{@code   maintenanceMargin = notional * maintenanceMarginRate - maintenanceAmount }</pre>
 *
 * The subtracted {@code maintenanceAmount} ("cum" in the payload) keeps the function continuous
 * across boundaries; a bare percentage overstates the requirement above the first bracket.
 *
 * @param notionalFloor         lower bound of the bracket, inclusive ({@code bracket.notionalFloor})
 * @param notionalCap           upper bound, inclusive ({@code bracket.notionalCap})
 * @param maintenanceMarginRate {@code bracket.maintMarginRatio}, e.g. 0.025 for 2.5%
 * @param maintenanceAmount     {@code bracket.cum}
 * @param maxLeverage           {@code bracket.initialLeverage} — the exchange's own cap in this bracket
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
        // Strictly below 1: at exactly 1.0 the long denominator q*(MMR - 1) is zero and the solve
        // returns a non-finite price, which the clamp would turn into a permissive "unreachable".
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
