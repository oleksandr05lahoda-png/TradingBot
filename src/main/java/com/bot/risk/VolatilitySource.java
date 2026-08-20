package com.bot.risk;

import java.util.OptionalDouble;

/**
 * Where the vol-targeting overlay learns how rough the market is. An interface rather than a lookup
 * inside {@link RiskEngine} for the same reason balance and time are arguments to
 * {@link RiskEngine#evaluate}: the gate does no I/O, so a decision stays reproducible and tests can
 * hand it any regime. Absence is a first-class answer — never fabricate a number to avoid empty.
 */
@FunctionalInterface
public interface VolatilitySource {

    /** Realized daily vol of {@code symbol} as a fraction of price (0.03 = 3% daily sigma), or empty. */
    OptionalDouble realizedDailyVolFraction(String symbol);

    /** The fail-open default: knows nothing, so the overlay multiplies by exactly 1.0 and sizing is unchanged. */
    static VolatilitySource none() {
        return symbol -> OptionalDouble.empty();
    }
}
