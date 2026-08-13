package com.bot.risk;

import java.util.OptionalDouble;

/**
 * Where the vol-targeting overlay learns how rough the market currently is. An interface rather
 * than a lookup inside {@link RiskEngine} for the same reason balance and time are arguments to
 * {@link RiskEngine#evaluate}: the gate does no I/O of its own, so a sizing decision stays
 * reproducible and the tests can hand it any volatility regime without a market data stack.
 *
 * <p>Absence is a first-class answer. {@code OptionalDouble.empty()} means "I do not know", and the
 * overlay treats it as multiplier 1.0 — the bot behaves exactly as if the overlay did not exist.
 * An implementation must never fabricate a number to avoid returning empty.
 */
@FunctionalInterface
public interface VolatilitySource {

    /**
     * Realized daily volatility of {@code symbol} as a fraction of price (e.g. 0.03 for a 3%
     * daily standard deviation of returns), or empty when no estimate is available.
     */
    OptionalDouble realizedDailyVolFraction(String symbol);

    /**
     * The fail-open default: knows nothing, so the overlay multiplies by exactly 1.0 and sizing is
     * bit-for-bit what it was before the overlay existed. This is the wiring until a measured
     * volatility feed earns its place.
     */
    static VolatilitySource none() {
        return symbol -> OptionalDouble.empty();
    }
}
