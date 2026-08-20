package com.bot.risk;

import com.bot.core.Preconditions;

import java.util.OptionalDouble;

/**
 * Volatility targeting (Moreira &amp; Muir, 2017): scale exposure by
 * {@code min(1, targetVol / realizedVol)}, so a hotter market shrinks the position. An overlay, not
 * a strategy — no view on direction, no edge of its own. Two asymmetries, both towards less risk.
 * <b>Capped at 1.0</b>: calm markets never lever UP, since a stale estimate would then size a levered
 * position. <b>Fail-open to 1.0</b>: no estimate, zero, negative, NaN or infinity all multiply by
 * exactly 1.0, so a broken feed degrades to pre-overlay behaviour instead of becoming a new way to
 * refuse a trade — unlike sizing INPUTS, which fail closed ({@link PositionSizer}).
 */
public final class VolTargetOverlay {

    private VolTargetOverlay() {}

    /**
     * The factor the risk fraction is multiplied by, always in {@code (0, 1]}. An unusable target
     * throws (it is validated config); an unusable realized value fails open to 1.0.
     */
    public static double multiplier(double targetVolFraction, OptionalDouble realizedVolFraction) {
        Preconditions.positiveFinite(targetVolFraction, "targetVolFraction");
        Preconditions.notNull(realizedVolFraction, "realizedVolFraction");

        if (realizedVolFraction.isEmpty()) {
            return 1.0; // no data is not high vol and not low vol — it is no overlay
        }
        double realized = realizedVolFraction.getAsDouble();
        if (!Double.isFinite(realized) || realized <= 0) {
            // A broken feed, not a calm market; also keeps target/realized from dividing by zero.
            return 1.0;
        }
        return Math.min(1.0, targetVolFraction / realized);
    }
}
