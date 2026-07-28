package com.bot.paper;

import java.util.List;

/**
 * A falsifiable claim about the market, in a form the harness can score.
 *
 * The harness is EMPTY on purpose: no implementation of this interface ships here. Whatever the
 * first real hypothesis turns out to be, it is scored by the same PaperExecutor as every later one,
 * so backtest and forward numbers are comparable as numbers rather than by eye.
 */
public interface Hypothesis {

    /** Stable sleeve name. Written to paper_signals.hypothesis_name. */
    String name();

    /**
     * Changes on ANY edit to parameters or logic. Written to paper_signals.hypothesis_version.
     *
     * This is not bookkeeping: the partial unique index on (hypothesis_name, hypothesis_version)
     * where mode='backtest_holdout' means a version gets exactly one holdout run, ever. Bumping the
     * version to get a second look is allowed and honest — it just costs another entry in the
     * multiple-testing denominator, which is precisely the price it should cost.
     */
    String version();

    /**
     * Decide from what is known at {@code snap.asOfMs()}. Returning an empty list is normal and
     * expected most of the time.
     *
     * Implementations must derive everything from the snapshot. There is no way to reach newer
     * data through it, so a correct implementation cannot peek even by accident.
     */
    List<Signal> evaluate(MarketSnapshot snap);
}
