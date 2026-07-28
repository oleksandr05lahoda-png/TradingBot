package com.bot.paper;

import java.util.List;

/**
 * Accounting for delta-neutral carry: SHORT perp against LONG spot, equal notional.
 *
 * Sits BESIDE {@link PaperExecutor} rather than inside it. The directional executor is correct for
 * one-legged hypotheses and bending it around a second leg would risk both. What is shared is the
 * discipline — point-in-time snapshots, funding from real settlements, two-step journalling, the
 * dev/holdout split — not the exit arithmetic.
 *
 * <h3>Why the P&amp;L is computed from four prices and not from the basis drift</h3>
 * To first order the pair returns {@code -(basis_exit - basis_entry)}. That approximation drops a
 * term of order {@code r * basis_exit}, where r is the spot's own move over the hold. With a 5%
 * spot move and a 5bp exit basis that discarded term is 0.25bp — five percent of the very signal
 * being measured. So the legs are summed exactly, and the basis drift is reported alongside as a
 * diagnostic rather than used as the result.
 *
 * <h3>Costs</h3>
 * FOUR fills, not two: open short perp, open long spot, close each. At
 * {@link PaperExecutor#PAPER_TAKER_FEE} per fill that is 0.20% round-trip, against a carry that
 * typically earns single-digit basis points per 8h settlement. This multiplier decides whether the
 * whole family is viable, so it is a named constant and it is charged in full.
 * Slippage is charged on all four fills, always against us.
 */
public final class CarryPaperExecutor {

    /** Open perp, open spot, close perp, close spot. The number that decides carry's viability. */
    public static final int CARRY_FILLS = 4;

    public enum ExitReason {
        /** Held to the end of the pre-registered window. */
        time_stop,
        /** Basis fell to or below the pre-registered exit level — nothing left to harvest. */
        basis_converged,
        /** A funding settlement turned negative — the position would start paying. */
        funding_flipped
    }

    /** Resolved outcome. All returns are fractions of notional. */
    public static final class Fill {
        public final long   entryBarOpenMs;
        public final double perpEntryPx;   // after slippage
        public final double spotEntryPx;   // after slippage
        public final long   exitBarMs;
        public final double perpExitPx;
        public final double spotExitPx;
        public final ExitReason exitReason;

        public final double basisEntryBp;
        public final double basisExitBp;
        /** {@code -(basisExit - basisEntry)} in fractional terms. Diagnostic only — see class doc. */
        public final double basisDrift;

        public final double retGross;      // exact sum of both legs
        public final double fees;          // CARRY_FILLS * PAPER_TAKER_FEE
        /** Signed as a COST, matching paper_signals.funding. A collecting carry makes this negative. */
        public final double funding;
        public final double retNet;

        Fill(long entryBarOpenMs, double perpEntryPx, double spotEntryPx,
             long exitBarMs, double perpExitPx, double spotExitPx, ExitReason exitReason,
             double basisEntryBp, double basisExitBp, double retGross, double fees, double funding) {
            this.entryBarOpenMs = entryBarOpenMs;
            this.perpEntryPx = perpEntryPx;
            this.spotEntryPx = spotEntryPx;
            this.exitBarMs = exitBarMs;
            this.perpExitPx = perpExitPx;
            this.spotExitPx = spotExitPx;
            this.exitReason = exitReason;
            this.basisEntryBp = basisEntryBp;
            this.basisExitBp = basisExitBp;
            this.basisDrift = -(basisExitBp - basisEntryBp) / 10_000.0;
            this.retGross = retGross;
            this.fees = fees;
            this.funding = funding;
            this.retNet = retGross - fees - funding;
        }
    }

    /**
     * Resolve one carry position.
     *
     * @param pos          the pre-registered position
     * @param perpSeries   perp bars (klines_4h), ascending
     * @param spotSeries   spot-proxy bars (index_klines_4h), ascending, same interval
     * @param funding      funding settlements for the symbol, ascending
     * @param maxHoldBars  bars to hold before the time stop
     * @return the fill, or null when either leg is missing a bar it needs. A carry that cannot be
     *         priced on BOTH legs is not a trade, and filling in one leg would invent the hedge.
     */
    public Fill simulate(CarryPosition pos, List<Bar> perpSeries, List<Bar> spotSeries,
                         List<PaperExecutor.FundingPoint> funding, int maxHoldBars) {
        if (pos == null || perpSeries == null || spotSeries == null || maxHoldBars <= 0) return null;

        int entryIdx = firstIndexAtOrAfter(perpSeries, pos.signalBarCloseMs);
        if (entryIdx < 0) return null;

        Bar perpEntryBar = perpSeries.get(entryIdx);
        // Same rule as the directional path: the entry bar must be the immediately following one.
        // Across a hole in the series this would otherwise enter a full interval or more late — a
        // different trade from the pre-registered one, and a row the database rejects.
        if (perpEntryBar.openMs - pos.signalBarCloseMs
                >= perpEntryBar.closeMs - perpEntryBar.openMs) {
            return null;
        }
        Bar spotEntryBar = findByOpenMs(spotSeries, perpEntryBar.openMs);
        if (spotEntryBar == null) return null;          // no hedge available at entry — not a trade

        double slip = PaperExecutor.PAPER_SLIPPAGE_BP / 10_000.0;

        // Entry at the OPEN of the first bar strictly after the signal bar, on BOTH legs.
        // Short perp sells (fills lower), long spot buys (fills higher). Both against us.
        double perpEntryPx = perpEntryBar.open * (1.0 - slip);
        double spotEntryPx = spotEntryBar.open * (1.0 + slip);
        double basisEntryBp = basisBp(perpEntryBar.open, spotEntryBar.open);

        int lastIdx = Math.min(entryIdx + maxHoldBars - 1, perpSeries.size() - 1);
        for (int i = entryIdx; i <= lastIdx; i++) {
            Bar pb = perpSeries.get(i);
            Bar sb = findByOpenMs(spotSeries, pb.openMs);
            if (sb == null) continue;                   // gap on the hedge leg — cannot evaluate here

            // Funding first: a settlement landing inside this bar that turns negative ends the
            // trade at this bar's close, because from here on the position pays instead of earning.
            //
            // The window is half-open, (openMs, closeMs]. Binance settles at 00:00/08:00/16:00 UTC,
            // which land exactly on 4h bar boundaries, so a settlement at time T belongs to the bar
            // that ENDS at T — the bar over which it accrued — not to the one starting there.
            if (!Double.isNaN(pos.exitFundingBelow) && funding != null) {
                for (PaperExecutor.FundingPoint fp : funding) {
                    if (fp.timeMs > pb.openMs && fp.timeMs <= pb.closeMs
                            && pos.fundingExitTriggered(fp.rate)) {
                        return build(pos, perpEntryBar, perpEntryPx, spotEntryPx, basisEntryBp,
                                pb, pb.close, sb.close, ExitReason.funding_flipped, slip, funding);
                    }
                }
            }
            if (!Double.isNaN(pos.exitBasisBp)) {
                double b = basisBp(pb.close, sb.close);
                if (b <= pos.exitBasisBp) {
                    return build(pos, perpEntryBar, perpEntryPx, spotEntryPx, basisEntryBp,
                            pb, pb.close, sb.close, ExitReason.basis_converged, slip, funding);
                }
            }
        }

        int tsIdx = entryIdx + maxHoldBars;
        if (tsIdx >= perpSeries.size()) return null;    // window not finished — leave unresolved
        Bar tsPerp = perpSeries.get(tsIdx);
        Bar tsSpot = findByOpenMs(spotSeries, tsPerp.openMs);
        if (tsSpot == null) return null;
        return build(pos, perpEntryBar, perpEntryPx, spotEntryPx, basisEntryBp,
                tsPerp, tsPerp.open, tsSpot.open, ExitReason.time_stop, slip, funding);
    }

    private Fill build(CarryPosition pos, Bar entryBar, double perpEntryPx, double spotEntryPx,
                       double basisEntryBp, Bar exitBar, double rawPerpExit, double rawSpotExit,
                       ExitReason reason, double slip, List<PaperExecutor.FundingPoint> funding) {

        // Closing: buy back the perp (pay more), sell the spot (get less).
        double perpExitPx = rawPerpExit * (1.0 + slip);
        double spotExitPx = rawSpotExit * (1.0 - slip);

        // EXACT, leg by leg. Short perp profits when the price falls; long spot profits when it rises.
        double perpLeg = (perpEntryPx - perpExitPx) / perpEntryPx;
        double spotLeg = (spotExitPx - spotEntryPx) / spotEntryPx;
        double retGross = perpLeg + spotLeg;

        double fees = CARRY_FILLS * PaperExecutor.PAPER_TAKER_FEE;

        // The SHORT perp leg collects funding when the rate is positive, so a positive rate is a
        // negative cost. The spot leg has no funding. Signed to match paper_signals.funding, where
        // positive always means "it cost us" — the same convention PaperExecutor uses.
        //
        // Window is half-open, (entryOpen, exitInstant]. The start is exclusive because a
        // settlement exactly on the entry bar's open accrued before we held the position.
        //
        // THE END IS THE INSTANT THE EXIT PRICE COMES FROM, not simply the exit bar's close. A
        // time stop is priced at that bar's OPEN, so the window has to stop there too; running it
        // to the close credited up to four more hours of funding than the position was held for.
        // Measured before the fix: 22 of 84 time_stop episodes carried an extra settlement, +0.0057%
        // on that group. Small, and it flattered the result — which is why it goes regardless of size.
        // The other two exits are priced at the bar's CLOSE, so for them the close IS the instant.
        long exitInstantMs = reason == ExitReason.time_stop ? exitBar.openMs : exitBar.closeMs;

        double fundingCost = 0.0;
        if (funding != null) {
            for (PaperExecutor.FundingPoint fp : funding) {
                if (fp.timeMs > entryBar.openMs && fp.timeMs <= exitInstantMs) {
                    fundingCost -= fp.rate;
                }
            }
        }

        return new Fill(entryBar.openMs, perpEntryPx, spotEntryPx,
                exitBar.openMs, perpExitPx, spotExitPx, reason,
                basisEntryBp, basisBp(rawPerpExit, rawSpotExit), retGross, fees, fundingCost);
    }

    /** Basis in basis points, on raw prices: (perp - spot) / spot. */
    static double basisBp(double perp, double spot) {
        if (spot <= 0) return Double.NaN;
        return (perp - spot) / spot * 10_000.0;
    }

    private static int firstIndexAtOrAfter(List<Bar> series, long openMs) {
        for (int i = 0; i < series.size(); i++) {
            if (series.get(i).openMs >= openMs) return i;
        }
        return -1;
    }

    /** The hedge leg's bar for the same instant, or null when that leg has a gap there. */
    private static Bar findByOpenMs(List<Bar> series, long openMs) {
        int lo = 0, hi = series.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long v = series.get(mid).openMs;
            if (v == openMs) return series.get(mid);
            if (v < openMs) lo = mid + 1; else hi = mid - 1;
        }
        return null;
    }
}
