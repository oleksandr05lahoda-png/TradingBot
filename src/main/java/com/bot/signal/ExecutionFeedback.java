package com.bot.signal;

import com.bot.core.Preconditions;

import java.math.BigDecimal;

/**
 * What happened to a signal, reported back to its source. The gap between intended entry and
 * {@code averageFillPrice} is the only measurement of realised execution cost. A plain value type,
 * not the execution layer's report: a source must not need to know {@code com.bot.exec} exists.
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
