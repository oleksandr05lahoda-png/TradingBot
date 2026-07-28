package com.bot.paper;

/**
 * A pre-registered prediction. Immutable — once a hypothesis has emitted one, nothing may adjust
 * it in the light of what happened next.
 *
 * {@code signalBarCloseMs} is the close of the bar the decision was made ON. The executor enters at
 * the OPEN of the first bar strictly after it, so this field is what makes the no-look-ahead rule
 * checkable after the fact — and it is re-checked in the database by
 * paper_signals_entry_after_signal_ck.
 */
public final class Signal {
    public enum Side { LONG, SHORT }

    public final String symbol;
    public final Side   side;
    public final long   signalBarCloseMs;
    public final double stopPrice;
    /** May be 0 when the hypothesis has no target and relies on stop / time-stop only. */
    public final double targetPrice;
    public final double sizeMultiplier;
    public final String rationale;

    public Signal(String symbol, Side side, long signalBarCloseMs,
                  double stopPrice, double targetPrice, double sizeMultiplier, String rationale) {
        if (symbol == null || symbol.isBlank()) throw new IllegalArgumentException("symbol required");
        if (side == null) throw new IllegalArgumentException("side required");
        if (stopPrice <= 0) throw new IllegalArgumentException("stopPrice must be positive");
        if (targetPrice < 0) throw new IllegalArgumentException("targetPrice must be >= 0");
        if (sizeMultiplier <= 0) throw new IllegalArgumentException("sizeMultiplier must be positive");
        this.symbol           = symbol;
        this.side             = side;
        this.signalBarCloseMs = signalBarCloseMs;
        this.stopPrice        = stopPrice;
        this.targetPrice      = targetPrice;
        this.sizeMultiplier   = sizeMultiplier;
        this.rationale        = rationale == null ? "" : rationale;
    }

    @Override public String toString() {
        return side + " " + symbol + " @signalClose=" + signalBarCloseMs
                + " stop=" + stopPrice + " target=" + targetPrice;
    }
}
