package com.bot.paper;

import java.util.ArrayList;
import java.util.List;

/**
 * Delta-neutral funding carry, v1. Parameters are those pre-registered by the operator from the
 * dev-period funding distribution (project_state id=46) and are NOT to be adjusted after seeing a
 * result — a threshold moved to improve a number stops measuring anything.
 *
 * <pre>
 *   universe   all symbols present in the run's fixed universe
 *   entry      funding rate above 5bp per 8h settlement
 *   hold       57 settlements = 19 days = 114 bars of 4h
 *   exit       time stop at 57 settlements
 *              | funding falls back to or below the entry threshold
 *              | basis converges
 * </pre>
 *
 * <h3>What this measures, and what it does not</h3>
 * The dev figure that motivated these parameters, +1.806%, is COLLECTED FUNDING ONLY. It contains
 * no basis drift and no fees. This harness charges four fills at 0.20% round-trip and prices both
 * legs, so the measured result can be far lower or negative: entering while the basis is wide and
 * exiting after it has converged loses on the pair precisely while the funding is being collected.
 * Those two effects are correlated, not independent, and separating them is the entire point of
 * running this.
 *
 * <h3>Two decisions the specification left open</h3>
 * 1. ENTRY IS AN EVENT, NOT A STATE. A position opens on the settlement that crosses the threshold
 *    from below, not on every bar while the rate stays above it. Read as a state, a single
 *    high-funding episode would emit a position on every bar for its whole duration — the same
 *    episode counted dozens of times, each overlapping the last, which inflates the sample without
 *    adding observations and makes any aggregate statistic meaningless.
 * 2. "CONVERGED" IS ZERO. The specification named basis_converged without a level, so the
 *    definitional one is used: the perp no longer trades above the spot. This is a definition, not
 *    a tuned value, and it is stated here so it cannot later be mistaken for one.
 */
public final class FundingCarryV1 implements CarryHypothesis {

    /** Entry: a settlement richer than this. 5bp per 8h. */
    static final double ENTRY_FUNDING = 0.0005;
    /** Exit: funding back at or below the level that justified the entry. */
    static final double EXIT_FUNDING  = ENTRY_FUNDING;
    /** Exit: perp no longer above spot. Definitional, not fitted. */
    static final double EXIT_BASIS_BP = 0.0;
    /** 57 settlements x 8h = 456h = 114 bars of 4h. */
    static final int    HOLD_BARS     = 114;

    @Override public String name()    { return "funding_carry"; }
    @Override public String version() { return "v1"; }
    @Override public int maxHoldBars(){ return HOLD_BARS; }

    @Override
    public List<CarryPosition> evaluate(MarketSnapshot perp, MarketSnapshot spot) {
        List<CarryPosition> out = new ArrayList<>();

        for (String symbol : perp.symbols()) {
            // Both legs must be priced at this instant, or there is no hedge to enter.
            Bar pb = perp.lastClosed(symbol);
            Bar sb = spot.lastClosed(symbol);
            if (pb == null || sb == null || pb.closeMs != sb.closeMs) continue;

            List<PaperExecutor.FundingPoint> f = perp.funding(symbol);
            if (f.size() < 2) continue;                    // need a previous rate to see a crossing

            PaperExecutor.FundingPoint last = f.get(f.size() - 1);
            PaperExecutor.FundingPoint prev = f.get(f.size() - 2);

            // The crossing must have happened in the bar that just closed, otherwise the same
            // episode would re-emit on every subsequent bar until the rate falls back.
            boolean crossedInThisBar = last.timeMs > pb.openMs && last.timeMs <= pb.closeMs;
            boolean crossedUp = last.rate > ENTRY_FUNDING && prev.rate <= ENTRY_FUNDING;
            if (!crossedInThisBar || !crossedUp) continue;

            out.add(new CarryPosition(symbol, pb.closeMs, EXIT_BASIS_BP, EXIT_FUNDING, 1.0,
                    String.format("funding %.5f crossed %.5f at %d", last.rate, ENTRY_FUNDING, last.timeMs)));
        }
        return out;
    }
}
