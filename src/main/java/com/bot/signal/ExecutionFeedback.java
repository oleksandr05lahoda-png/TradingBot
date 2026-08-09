package com.bot.signal;

import com.bot.core.Preconditions;

import java.math.BigDecimal;

/**
 * What actually happened to a signal, sent back to the source that produced it. The gap between the
 * intended entry and {@code averageFillPrice} is the only measurement of realised execution cost.
 *
 * <p>A plain value type rather than a reference to the execution layer's report: a source must not
 * need to know that {@code com.bot.exec} exists.
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
