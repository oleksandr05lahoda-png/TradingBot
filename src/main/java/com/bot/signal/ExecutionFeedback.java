package com.bot.signal;

import com.bot.core.Preconditions;

import java.math.BigDecimal;

/**
 * What actually happened to a signal, sent back to the source that produced it.
 *
 * <p>Without this the queue is one-way: a row records that an order was sent and nothing more, so
 * the difference between the price a sleeve assumed and the price it got is never written down.
 * That difference is the execution cost, and every gate in the lab currently treats it as a
 * constant — 20bp here, 40bp there — because there has never been anything to measure it with.
 *
 * <p>Deliberately a plain value type in the {@code signal} package rather than a reference to the
 * execution layer's report: a source must not need to know that {@code com.bot.exec} exists.
 *
 * @param clientOrderId    the deterministic id the order carried
 * @param filledQuantity   what the exchange filled, which is not what was requested
 * @param averageFillPrice weighted average fill price; compare with the signal's intended entry
 * @param note             short human-readable outcome, for the operator rather than for arithmetic
 */
public record ExecutionFeedback(
        String clientOrderId,
        BigDecimal filledQuantity,
        BigDecimal averageFillPrice,
        String note) {

    public ExecutionFeedback {
        Preconditions.notBlank(clientOrderId, "clientOrderId");
        Preconditions.notNull(filledQuantity, "filledQuantity");
        Preconditions.notNull(averageFillPrice, "averageFillPrice");
        Preconditions.notNull(note, "note");
    }

    /** Slippage against the intended entry, in basis points. Positive means worse than intended. */
    public double slippageBpAgainst(double intendedEntry, boolean isLong) {
        if (intendedEntry <= 0 || averageFillPrice.signum() <= 0) return 0.0;
        double filled = averageFillPrice.doubleValue();
        double signed = isLong ? filled - intendedEntry : intendedEntry - filled;
        return signed / intendedEntry * 10_000.0;
    }
}
