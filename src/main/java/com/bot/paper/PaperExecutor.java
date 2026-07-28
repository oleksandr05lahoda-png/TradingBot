package com.bot.paper;

import java.util.List;

/**
 * The accounting engine. ONE implementation, shared by HistoricalDriver and ForwardDriver, so a
 * backtest number and a forward number are comparable as numbers rather than by eye.
 *
 * DELIBERATELY KNOWS NOTHING ABOUT RISK LIMITS. It never consults RiskGuard, never applies the
 * aggregate cap and never applies a position count. A cap here would drop signals that happened to
 * arrive while exposure was high, and the journal would then contain only the signals that occurred
 * when the book was empty — a systematically selected sample, and the selection correlates with
 * exactly the market conditions being measured. The harness measures the edge of a SIGNAL;
 * portfolio constraints are applied afterwards, arithmetically, over the recorded outcomes.
 * RiskGuard belongs on the execution path in SupabaseSignalBridge, and stays there.
 *
 * Costs are charged the same way in both modes:
 *   - taker fee {@link #PAPER_TAKER_FEE} per leg, so 0.10% round-trip;
 *   - {@link #PAPER_SLIPPAGE_BP} of slippage per leg, always against us;
 *   - funding from the REAL funding_history rows that fall inside the holding period.
 * Whether a level was touched is decided on the RAW bar high/low against the RAW stop/target —
 * slippage moves the price we get filled at, not the question of whether the market reached it.
 */
public final class PaperExecutor {

    /** Taker fee per leg. Round-trip is twice this. */
    public static final double PAPER_TAKER_FEE   = 0.0005;   // 0.05%
    /** Slippage per leg in basis points, charged against us on both entry and exit. */
    public static final double PAPER_SLIPPAGE_BP = 2.0;      // 0.02%

    /** A funding settlement, as stored in funding_history. */
    public static final class FundingPoint {
        public final long   timeMs;
        public final double rate;
        public FundingPoint(long timeMs, double rate) { this.timeMs = timeMs; this.rate = rate; }
    }

    public enum ExitReason { stop, target, time_stop }

    /** The resolved outcome of one signal. All returns are fractions of notional, not percent. */
    public static final class Fill {
        public final long   entryBarOpenMs;
        public final double entryPx;      // after slippage — what we actually paid
        public final long   exitBarMs;
        public final double exitPx;       // after slippage
        public final ExitReason exitReason;
        public final double retGross;
        public final double fees;
        public final double funding;      // signed as a COST: positive means it cost us
        public final double retNet;

        Fill(long entryBarOpenMs, double entryPx, long exitBarMs, double exitPx,
             ExitReason exitReason, double retGross, double fees, double funding) {
            this.entryBarOpenMs = entryBarOpenMs;
            this.entryPx = entryPx;
            this.exitBarMs = exitBarMs;
            this.exitPx = exitPx;
            this.exitReason = exitReason;
            this.retGross = retGross;
            this.fees = fees;
            this.funding = funding;
            this.retNet = retGross - fees - funding;
        }
    }

    /**
     * Resolve one signal against the bars that followed it.
     *
     * @param signal       the pre-registered prediction
     * @param series       the symbol's bars, ascending. Bars at or before the signal bar's close
     *                     are ignored; the entry is the OPEN of the first bar STRICTLY after it.
     * @param funding      funding settlements for the symbol, ascending. May be empty.
     * @param maxHoldBars  bars to hold before the time stop fires
     * @return the fill, or null when there is no bar after the signal — an unresolvable signal is
     *         not a trade, and inventing one would fabricate a return
     */
    public Fill simulate(Signal signal, List<Bar> series, List<FundingPoint> funding, int maxHoldBars) {
        if (signal == null || series == null || maxHoldBars <= 0) return null;

        // The first bar STRICTLY after the signal bar. Bars are contiguous, so that bar's open
        // coincides with the signal bar's close: openMs >= signalBarCloseMs selects it and cannot
        // select the signal bar itself, whose open is one interval earlier.
        int entryIdx = -1;
        for (int i = 0; i < series.size(); i++) {
            if (series.get(i).openMs >= signal.signalBarCloseMs) { entryIdx = i; break; }
        }
        if (entryIdx < 0) return null;                 // nothing after the signal bar — no trade

        Bar entryBar = series.get(entryIdx);
        boolean isLong = signal.side == Signal.Side.LONG;
        double slip = PAPER_SLIPPAGE_BP / 10_000.0;

        // Entry is the OPEN of that bar, never the close of the signal bar.
        double rawEntry = entryBar.open;
        double entryPx  = isLong ? rawEntry * (1.0 + slip) : rawEntry * (1.0 - slip);

        int lastIdx = Math.min(entryIdx + maxHoldBars - 1, series.size() - 1);
        for (int i = entryIdx; i <= lastIdx; i++) {
            Bar b = series.get(i);
            boolean stopHit = isLong ? b.low <= signal.stopPrice : b.high >= signal.stopPrice;
            boolean tgtHit  = signal.targetPrice > 0
                    && (isLong ? b.high >= signal.targetPrice : b.low <= signal.targetPrice);

            // Stop is tested first and returns immediately, so a bar that touched BOTH levels
            // resolves as a stop. OHLC cannot tell us which came first, and assuming the target
            // would flatter every result that ever straddled a bar.
            if (stopHit) {
                return build(signal, entryBar, entryPx, b, signal.stopPrice, ExitReason.stop,
                        isLong, slip, funding);
            }
            if (tgtHit) {
                return build(signal, entryBar, entryPx, b, signal.targetPrice, ExitReason.target,
                        isLong, slip, funding);
            }
        }

        // Time stop: the OPEN of the bar after the holding window.
        int timeStopIdx = entryIdx + maxHoldBars;
        if (timeStopIdx >= series.size()) return null;  // window not finished yet — leave unresolved
        Bar tsBar = series.get(timeStopIdx);
        return build(signal, entryBar, entryPx, tsBar, tsBar.open, ExitReason.time_stop,
                isLong, slip, funding);
    }

    private Fill build(Signal signal, Bar entryBar, double entryPx, Bar exitBar, double rawExit,
                       ExitReason reason, boolean isLong, double slip, List<FundingPoint> funding) {
        double exitPx = isLong ? rawExit * (1.0 - slip) : rawExit * (1.0 + slip);

        double retGross = isLong
                ? (exitPx - entryPx) / entryPx
                : (entryPx - exitPx) / entryPx;

        double fees = 2.0 * PAPER_TAKER_FEE;

        // Every settlement inside the holding window is charged. A long pays a positive rate; a
        // short receives it, so the sign flips. Held over 25h across 8h settlements => 3 accruals.
        double fundingCost = 0.0;
        if (funding != null) {
            long from = entryBar.openMs;
            long to   = exitBar.closeMs;
            for (FundingPoint fp : funding) {
                if (fp.timeMs >= from && fp.timeMs <= to) {
                    fundingCost += isLong ? fp.rate : -fp.rate;
                }
            }
        }

        return new Fill(entryBar.openMs, entryPx, exitBar.openMs, exitPx,
                reason, retGross, fees, fundingCost);
    }
}
