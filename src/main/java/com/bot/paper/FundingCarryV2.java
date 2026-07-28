package com.bot.paper;

import java.util.ArrayList;
import java.util.List;

/**
 * Delta-neutral funding carry, v2 — the LAST version of this family (project_state id=49,
 * registered before the run).
 *
 * Exactly ONE condition differs from {@link FundingCarryV1}: the funding exit is a TRUE sign flip,
 * {@code rate < 0}, instead of the rate falling back to the entry threshold. Everything else is
 * identical — same frozen universe, same entry at a crossing above 5bp, same 57-settlement horizon,
 * same 0bp basis convergence, same dev window.
 *
 * <h3>Why this is a re-specification and not a fitted parameter</h3>
 * v1's exit threshold equalled its entry threshold, and a rate above 5bp rarely survives one
 * settlement — so v1 never held longer than a single funding period: time_stop fired zero times out
 * of 1061 episodes, median hold 4 hours against a registered horizon of 19 days. The claim "collect
 * elevated funding for 19 days" was therefore never executed. That inconsistency follows from
 * reading the specification, not from reading the result, which is what makes changing it legitimate
 * here and would not make a third version legitimate.
 *
 * Zero is the mechanism's own boundary, not a chosen level: below it the position PAYS funding and
 * the reason to hold has gone. The comparison is STRICT — 1452 of the 129238 dev-period settlements
 * are exactly zero, and a zero rate pays nothing in either direction, so it does not end the trade.
 *
 * <h3>Limit</h3>
 * v1 tested the reading "exit when the rate falls back"; v2 tests "hold to the horizon". Together
 * they exhaust the readings of the registered hypothesis. A v3 would be parameter search — the
 * netfiles history repeating — so if v2 is negative the family is closed.
 */
public final class FundingCarryV2 implements CarryHypothesis {

    /** Entry: a settlement richer than this. 5bp per 8h. Same as v1. */
    static final double ENTRY_FUNDING = 0.0005;
    /** Exit: the rate turns negative. THE one change from v1. */
    static final double EXIT_FUNDING  = 0.0;
    /** Exit: perp no longer above spot. Definitional, not fitted. Same as v1. */
    static final double EXIT_BASIS_BP = 0.0;
    /** 57 settlements x 8h = 456h = 114 bars of 4h. Same as v1. */
    static final int    HOLD_BARS     = 114;

    @Override public String name()    { return "funding_carry"; }
    @Override public String version() { return "v2"; }
    @Override public int maxHoldBars(){ return HOLD_BARS; }

    @Override
    public List<CarryPosition> evaluate(MarketSnapshot perp, MarketSnapshot spot) {
        List<CarryPosition> out = new ArrayList<>();

        for (String symbol : perp.symbols()) {
            Bar pb = perp.lastClosed(symbol);
            Bar sb = spot.lastClosed(symbol);
            if (pb == null || sb == null || pb.closeMs != sb.closeMs) continue;

            List<PaperExecutor.FundingPoint> f = perp.funding(symbol);
            if (f.size() < 2) continue;

            PaperExecutor.FundingPoint last = f.get(f.size() - 1);
            PaperExecutor.FundingPoint prev = f.get(f.size() - 2);

            // Entry is an EVENT: the crossing must fall in the bar that just closed, or one episode
            // above the threshold would emit a position on every bar for its whole duration.
            boolean crossedInThisBar = last.timeMs > pb.openMs && last.timeMs <= pb.closeMs;
            boolean crossedUp = last.rate > ENTRY_FUNDING && prev.rate <= ENTRY_FUNDING;
            if (!crossedInThisBar || !crossedUp) continue;

            out.add(new CarryPosition(symbol, pb.closeMs, EXIT_BASIS_BP, EXIT_FUNDING,
                    CarryPosition.FundingExit.STRICTLY_BELOW, 1.0,
                    String.format("funding %.5f crossed %.5f at %d", last.rate, ENTRY_FUNDING, last.timeMs)));
        }
        return out;
    }
}
