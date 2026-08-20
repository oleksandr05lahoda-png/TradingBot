package com.bot.risk;

import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Liquidation price for a single <b>isolated</b> USDⓈ-M position, from Binance's published formula
 * {@code liq = (WB + cum - side*q*EP) / (q*MMR - side*q)} — side = +1 long / -1 short, q = base
 * units, EP = entry, MMR/cum = the governing bracket's maintenance rate and amount, WB = isolated
 * wallet balance (Binance's TMM and UPNL terms are 0 in isolated mode). Not
 * {@code entry * (1 - 1/leverage)}, which ignores maintenance margin. The bracket depends on
 * notional and notional on price, so the solver iterates to a fixed point, taking the price nearer
 * entry if it oscillates.
 */
public final class LiquidationCalculator {

    private static final int MAX_BRACKET_ITERATIONS = 8;

    private LiquidationCalculator() {}

    /** Initial margin minus the entry fee, which on an isolated position comes out of that same margin. */
    public static double isolatedWalletBalanceAtOpen(double notional, int leverage, double takerFeeFraction) {
        Preconditions.positiveFinite(notional, "notional");
        Preconditions.positive(leverage, "leverage");
        Preconditions.inClosedRange(takerFeeFraction, 0.0, 0.01, "takerFeeFraction");
        double initialMargin = notional / leverage;
        double entryFee = notional * takerFeeFraction;
        double wb = initialMargin - entryFee;
        Preconditions.require(wb > 0,
                "entry fee " + entryFee + " exceeds the initial margin " + initialMargin
                        + " — leverage is too high for this notional to be openable at all");
        return wb;
    }

    /** Liquidation mark price, re-solving the bracket until it settles; {@code 0.0} = unreachable long (1x). */
    public static double isolatedLiquidationPrice(Side side,
                                                  double entryPrice,
                                                  double quantity,
                                                  int leverage,
                                                  MarginTierTable tiers,
                                                  double takerFeeFraction) {
        Preconditions.notNull(side, "side");
        Preconditions.positiveFinite(entryPrice, "entryPrice");
        Preconditions.positiveFinite(quantity, "quantity");
        Preconditions.positive(leverage, "leverage");
        Preconditions.notNull(tiers, "tiers");

        double notionalAtEntry = entryPrice * quantity;
        double walletBalance = isolatedWalletBalanceAtOpen(notionalAtEntry, leverage, takerFeeFraction);

        MarginTier tier = tiers.tierFor(notionalAtEntry);
        Set<MarginTier> visited = new LinkedHashSet<>();
        double price = solve(side, entryPrice, quantity, walletBalance, tier);

        for (int i = 0; i < MAX_BRACKET_ITERATIONS; i++) {
            if (!visited.add(tier)) break;
            MarginTier atLiquidation = tiers.tierFor(Math.max(0.0, price) * quantity);
            if (atLiquidation.equals(tier)) return clampToReachable(side, entryPrice, price);
            if (visited.contains(atLiquidation)) {
                // Oscillating: take the solved PRICE nearer entry — with calibrated maintenance
                // amounts the higher-rate bracket is not the safer one.
                double a = solve(side, entryPrice, quantity, walletBalance, tier);
                double b = solve(side, entryPrice, quantity, walletBalance, atLiquidation);
                double nearer = side == Side.LONG ? Math.max(a, b) : Math.min(a, b);
                return clampToReachable(side, entryPrice, nearer);
            }
            tier = atLiquidation;
            price = solve(side, entryPrice, quantity, walletBalance, tier);
        }
        return clampToReachable(side, entryPrice, price);
    }

    private static double solve(Side side, double entryPrice, double quantity, double walletBalance, MarginTier tier) {
        int s = side.sign();
        double numerator = walletBalance + tier.maintenanceAmount() - s * quantity * entryPrice;
        // q*(MMR - 1) long, q*(MMR + 1) short; MarginTier bounds MMR to (0, 1) so neither is zero.
        double denominator = quantity * tier.maintenanceMarginRate() - s * quantity;
        return numerator / denominator;
    }

    /**
     * A long solving at or below zero reports {@code 0.0} (ordinary at 1x). A price on the wrong side
     * of entry is liquidatable at open; clamping to entry zeroes the buffer so it is refused readably.
     */
    private static double clampToReachable(Side side, double entryPrice, double price) {
        Preconditions.require(Double.isFinite(price) || side == Side.LONG,
                "liquidation price solved to " + price + " — inputs are inconsistent");
        if (side == Side.LONG) {
            if (!Double.isFinite(price) || price < 0.0) return 0.0;
            return Math.min(price, entryPrice);
        }
        return Math.max(price, entryPrice);
    }
}
