package com.bot.paper;

/**
 * A delta-neutral carry position: SHORT perp, LONG spot, equal notional.
 *
 * A separate type from {@link Signal} on purpose. Signal requires a stop price, and a
 * delta-neutral pair has no stop — its risk is basis divergence and margin on the perp leg, not
 * distance to a level. Passing a fabricated stop just to satisfy that constructor would put a lie
 * in the data model, and the journal would then record a protective level that never existed.
 *
 * Exit conditions are part of the pre-registered claim, not something chosen once the outcome is
 * visible, so they live here alongside the entry.
 */
public final class CarryPosition {

    public final String symbol;
    public final long   signalBarCloseMs;

    /**
     * Exit when the basis falls to or below this, in basis points. {@code NaN} disables the rule.
     * The carry is earned while the perp trades above spot; once that premium is gone there is
     * nothing left to harvest and only cost remains.
     */
    public final double exitBasisBp;

    /** Exit at the first funding settlement that turns negative — the trade would start paying. */
    public final boolean exitOnFundingFlip;

    public final double sizeMultiplier;
    public final String rationale;

    public CarryPosition(String symbol, long signalBarCloseMs, double exitBasisBp,
                         boolean exitOnFundingFlip, double sizeMultiplier, String rationale) {
        if (symbol == null || symbol.isBlank()) throw new IllegalArgumentException("symbol required");
        if (sizeMultiplier <= 0) throw new IllegalArgumentException("sizeMultiplier must be positive");
        this.symbol            = symbol;
        this.signalBarCloseMs  = signalBarCloseMs;
        this.exitBasisBp       = exitBasisBp;
        this.exitOnFundingFlip = exitOnFundingFlip;
        this.sizeMultiplier    = sizeMultiplier;
        this.rationale         = rationale == null ? "" : rationale;
    }

    @Override public String toString() {
        return "CARRY " + symbol + " @signalClose=" + signalBarCloseMs
                + " exitBasisBp=" + exitBasisBp + " exitOnFundingFlip=" + exitOnFundingFlip;
    }
}
