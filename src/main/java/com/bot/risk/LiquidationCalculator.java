package com.bot.risk;

import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Liquidation price for a single <b>isolated</b> USDⓈ-M position, from Binance's published formula.
 *
 * <h2>The formula</h2>
 * Binance's general form is
 *
 * <pre>{@code
 *   liq = (WB - TMM1 + UPNL1 + cumB - SUM(Position_i * EP_i * Side_i))
 *       / (SUM(Position_i * MMR_i) - SUM(Position_i * Side_i))
 * }</pre>
 *
 * with, per Binance, <i>"In isolated margin mode, WB is isolatedWalletBalance of the isolated
 * position, TMM = 0, UPNL = 0"</i>. One isolated position therefore collapses the sums to a single
 * term and leaves
 *
 * <pre>{@code   liq = (WB + cum - side * q * EP) / (q * MMR - side * q) }</pre>
 *
 * where {@code side} is +1 long / -1 short, {@code q} the position size in base units, {@code EP}
 * the entry price, {@code MMR} the maintenance margin rate of the governing bracket and {@code cum}
 * that bracket's maintenance amount.
 *
 * <h2>Why it is that, derived rather than copied</h2>
 * Liquidation is the price at which equity meets the maintenance requirement:
 * {@code WB + uPnL = MM}. For a long, {@code uPnL = q(P - EP)} and {@code MM = q*P*MMR - cum}, so
 * {@code WB + q(P - EP) = q*P*MMR - cum}, which rearranges to
 * {@code P = (WB + cum - q*EP) / (q(MMR - 1))}. For a short, {@code uPnL = q(EP - P)} gives
 * {@code P = (WB + cum + q*EP) / (q(MMR + 1))}. Both are the single expression above. The
 * derivation is written out because a formula that is only copied cannot be checked.
 *
 * <h2>This is not 1/leverage</h2>
 * The naive {@code entry * (1 - 1/leverage)} ignores maintenance margin entirely and therefore puts
 * liquidation <i>further</i> from entry than it is — for a long, it is always the more optimistic
 * number. Being optimistic about liquidation distance is the one direction that costs the whole
 * isolated margin rather than the planned R.
 *
 * <h2>Bracket selection</h2>
 * The governing bracket depends on notional, and notional depends on price, so the bracket at entry
 * is not necessarily the bracket at liquidation. The solver iterates: solve with the entry bracket,
 * re-select the bracket at the resulting price, solve again, until it stops moving. If it oscillates
 * between two brackets it settles on the one with the higher maintenance rate — the answer nearer to
 * entry, which is the conservative side to land on.
 */
public final class LiquidationCalculator {

    private static final int MAX_BRACKET_ITERATIONS = 8;

    private LiquidationCalculator() {}

    /**
     * Isolated wallet balance for a freshly opened position: the initial margin, minus the entry fee,
     * which on an isolated position is deducted from that same isolated margin.
     *
     * <p>Ignoring the fee inflates the wallet balance and pushes the projected liquidation price
     * away from entry. The correction is small — 0.05% of notional — but it is small in the
     * dangerous direction, so it is applied rather than waved away.
     */
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

    /**
     * Liquidation price for an isolated position opened at {@code entryPrice} with {@code quantity}
     * base units at {@code leverage}.
     *
     * @return the mark price at which the position liquidates, or {@code 0.0} for a long whose
     *         liquidation price is not reachable above zero (which happens at 1x)
     */
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
                // Oscillating between two brackets. Settle on whichever SOLVED PRICE is nearer the
                // entry — for a long the higher of the two, for a short the lower. Comparing the
                // maintenance RATES instead would be wrong: because the maintenance amount is
                // calibrated for continuity, the higher-rate bracket demands *less* maintenance
                // margin below its own floor, which puts liquidation further away. Being wrong
                // towards "closer" only ever rejects a trade; being wrong towards "further" lets a
                // bad one through.
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

    /** {@code liq = (WB + cum - side*q*EP) / (q*MMR - side*q)}. */
    private static double solve(Side side, double entryPrice, double quantity, double walletBalance, MarginTier tier) {
        int s = side.sign();
        double numerator = walletBalance + tier.maintenanceAmount() - s * quantity * entryPrice;
        double denominator = quantity * tier.maintenanceMarginRate() - s * quantity;
        // denominator is q*(MMR - 1) < 0 for a long and q*(MMR + 1) > 0 for a short. MarginTier
        // requires MMR strictly inside (0, 1), so neither form can be zero for a positive quantity.
        return numerator / denominator;
    }

    /**
     * Two ends of the range need naming.
     *
     * <p>A long's liquidation price comes out at or below zero when the margin is large enough that
     * price would hit zero first — the ordinary case at 1x. That is reported as {@code 0.0},
     * "not reachable", rather than as a negative price.
     *
     * <p>At the other end, the solved price can land on the wrong side of entry. That is not a bug
     * in the arithmetic: it says the initial margin is already at or below the maintenance
     * requirement, i.e. the position would be liquidatable the moment it opened. It is clamped to
     * the entry price, which drives the liquidation buffer to zero and makes
     * {@link LiquidationSafety} refuse the trade with a reason the operator can read — far better
     * than an exception thrown from inside a pricing routine.
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
