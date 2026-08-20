package com.bot.risk;

import com.bot.core.Preconditions;

import java.util.OptionalDouble;

/**
 * Volatility targeting (Moreira &amp; Muir, "Volatility-Managed Portfolios", 2017): scale exposure
 * by {@code min(1, targetVol / realizedVol)}, so when the market runs hotter than the target the
 * position shrinks in proportion. This is an overlay, not a strategy — it has no opinion on
 * direction and no edge of its own; it only narrows the distribution of outcomes per trade.
 *
 * <p>Two deliberate asymmetries, both towards less risk:
 * <ul>
 *   <li><b>Capped at 1.0.</b> Calm markets never lever the position UP. The academic form scales
 *       both ways; an automated bot gets the de-risking half only, because the
 *       cost of the cap is forgone size while the cost of its absence is a levered position sized
 *       off a vol estimate that may simply be stale.</li>
 *   <li><b>Fail-open to 1.0.</b> No estimate, a zero, a negative number, NaN, infinity — anything
 *       unusable multiplies by exactly 1.0 and the bot behaves as if the overlay did not exist.
 *       Fail-closed is the rule for sizing INPUTS ({@link PositionSizer} refuses NaN), but this is
 *       an advisory reduction on top of an already-budgeted size: a broken volatility feed must
 *       degrade to yesterday's behaviour, never become a new way to refuse or distort a trade.</li>
 * </ul>
 */
public final class VolTargetOverlay {

    private VolTargetOverlay() {}

    /**
     * The factor the risk fraction is multiplied by, always in {@code (0, 1]}.
     *
     * @param targetVolFraction   desired daily volatility as a fraction of price; a config value,
     *                            validated by {@link RiskConfig}, so unusable values throw here
     * @param realizedVolFraction measured daily volatility, or empty when unknown; unusable values
     *                            fail OPEN to 1.0 — see the class comment for why the two differ
     */
    public static double multiplier(double targetVolFraction, OptionalDouble realizedVolFraction) {
        Preconditions.positiveFinite(targetVolFraction, "targetVolFraction");
        Preconditions.notNull(realizedVolFraction, "realizedVolFraction");

        if (realizedVolFraction.isEmpty()) {
            return 1.0; // no data is not high vol and not low vol — it is no overlay
        }
        double realized = realizedVolFraction.getAsDouble();
        if (!Double.isFinite(realized) || realized <= 0) {
            // A non-positive or non-finite "measurement" is a broken feed, not a calm market.
            // Guarding here also keeps targetVol / realized from dividing by zero.
            return 1.0;
        }
        return Math.min(1.0, targetVolFraction / realized);
    }
}
