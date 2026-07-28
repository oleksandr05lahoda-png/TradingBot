package com.bot.paper;

import java.util.List;

/**
 * A falsifiable claim about delta-neutral carry. Parallel to {@link Hypothesis}, not a subtype:
 * it emits {@link CarryPosition}, which has no stop, and it needs both legs to decide.
 *
 * The two snapshots are taken at the SAME instant and cut by the same rule, so a carry hypothesis
 * has no more access to the future than a directional one. They are separate arguments rather than
 * one merged map so that perp and spot can never be confused for each other.
 */
public interface CarryHypothesis {

    String name();

    /**
     * Changes on ANY edit to parameters or logic — the partial unique index gives each version
     * exactly one holdout run, ever.
     */
    String version();

    /**
     * Holding window in bars, owned by the hypothesis rather than passed on the command line.
     *
     * The horizon is part of the claim: "collect carry for 19 days" and "collect carry for 3 days"
     * are different predictions, and a --hold argument would let them share a version and therefore
     * share the single holdout run that version is allowed.
     */
    int maxHoldBars();

    /**
     * @param perp bars for the perp leg, plus the funding settlements announced by {@code asOfMs}
     * @param spot bars for the spot-proxy leg, taken at the same instant
     */
    List<CarryPosition> evaluate(MarketSnapshot perp, MarketSnapshot spot);
}
