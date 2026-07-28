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

    /** How {@link #exitFundingBelow} is compared. The two readings are different claims. */
    public enum FundingExit {
        /** {@code rate <= level}. "The rate fell back to the level that justified the entry." */
        AT_OR_BELOW,
        /** {@code rate < level}. "The rate crossed the level." Used with level 0 for a true sign flip. */
        STRICTLY_BELOW
    }

    /**
     * Exit at the first funding settlement meeting {@link #exitFundingMode} against this level.
     * {@code NaN} disables the rule.
     */
    public final double exitFundingBelow;

    /**
     * Strictness matters and is not a detail: 1452 of the 129238 dev-period settlements are exactly
     * zero. Under AT_OR_BELOW with level 0 those would end the position, but a zero rate pays
     * nothing in either direction — the reason for holding has not disappeared, it has merely
     * stopped earning. A version claiming "exit when funding turns negative" must therefore use
     * STRICTLY_BELOW, or it would be testing a different rule than the one registered.
     */
    public final FundingExit exitFundingMode;

    public final double sizeMultiplier;
    public final String rationale;

    public CarryPosition(String symbol, long signalBarCloseMs, double exitBasisBp,
                         double exitFundingBelow, FundingExit exitFundingMode,
                         double sizeMultiplier, String rationale) {
        if (symbol == null || symbol.isBlank()) throw new IllegalArgumentException("symbol required");
        if (sizeMultiplier <= 0) throw new IllegalArgumentException("sizeMultiplier must be positive");
        if (!Double.isNaN(exitFundingBelow) && exitFundingMode == null) {
            throw new IllegalArgumentException("exitFundingMode required when a funding exit is set");
        }
        this.symbol            = symbol;
        this.signalBarCloseMs  = signalBarCloseMs;
        this.exitBasisBp       = exitBasisBp;
        this.exitFundingBelow  = exitFundingBelow;
        this.exitFundingMode   = exitFundingMode;
        this.sizeMultiplier    = sizeMultiplier;
        this.rationale         = rationale == null ? "" : rationale;
    }

    /** True when this settlement ends the position under the registered funding rule. */
    boolean fundingExitTriggered(double rate) {
        if (Double.isNaN(exitFundingBelow)) return false;
        return exitFundingMode == FundingExit.STRICTLY_BELOW
                ? rate <  exitFundingBelow
                : rate <= exitFundingBelow;
    }

    @Override public String toString() {
        return "CARRY " + symbol + " @signalClose=" + signalBarCloseMs
                + " exitBasisBp=" + exitBasisBp
                + " exitFunding=" + exitFundingMode + " " + exitFundingBelow;
    }
}
