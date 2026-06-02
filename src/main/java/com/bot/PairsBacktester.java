package com.bot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * [v84.0] PAIRS STAT-ARB BACKTEST — market-neutral mean reversion on the
 * log-spread of two correlated symbols.
 *
 * WHY a separate engine: the main DecisionEngine is single-leg directional and
 * proven edgeless (VCB/MR/funding-fade all failed under scrutiny). This class is
 * fully self-contained so every line is auditable for two things:
 *   1) NO lookahead — the z-score at bar i uses ONLY the trailing window
 *      [i-lookback, i-1]; the entry/exit decision at bar i uses bar i's CLOSE
 *      (trade-at-close), never any future bar.
 *   2) HONEST costs — costRoundTripPct is subtracted once per trade and is meant
 *      to cover BOTH legs (open+close of leg A and leg B): ~2x a single-leg
 *      round trip. Funding is omitted (for correlated pairs the two legs' funding
 *      largely nets; holds are short) — a small optimistic simplification, noted.
 *
 * THESIS: price is non-stationary (trends) so price mean-reversion fails. But the
 * spread of two cointegrated assets is ~stationary → reverts → tradeable, and is
 * market-neutral (survives BTC dumps, which killed the directional strategies).
 *
 * PnL: long-spread (pos=+1) = long A / short B with equal $ notional →
 * return ≈ Δ(lnA) − Δ(lnB) = Δspread. So gross% = pos × (spread_exit − spread_entry) × 100.
 */
public final class PairsBacktester {

    private PairsBacktester() {}

    public static final class Trade {
        public final long   entryTime;
        public final double entryZ;     // |z| at entry — confidence/tier axis
        public final double netPnlPct;  // % after the 2-leg round-trip cost
        public Trade(long entryTime, double entryZ, double netPnlPct) {
            this.entryTime = entryTime; this.entryZ = entryZ; this.netPnlPct = netPnlPct;
        }
    }

    public static final class Result {
        public final String name;
        public final List<Trade> trades = new ArrayList<>();
        public double netPnLPct = 0.0;
        public int    wins = 0;
        public Result(String name) { this.name = name; }
    }

    /**
     * @param lookback          z-score window in bars (e.g. 96 = 1 day on 15m)
     * @param entryZ            enter when |z| >= entryZ (e.g. 2.0)
     * @param exitZ             exit when |z| <= exitZ (reverted, e.g. 0.5)
     * @param stopZ             exit when |z| >= stopZ (diverged/stop, e.g. 3.5)
     * @param timeStopBars      force-exit after this many bars
     * @param costRoundTripPct  total % cost for the 2-leg round trip (e.g. 0.35)
     */
    public static Result run(String name,
                             List<TradingCore.Candle> a,
                             List<TradingCore.Candle> b,
                             int lookback, double entryZ, double exitZ, double stopZ,
                             int timeStopBars, double costRoundTripPct) {
        Result r = new Result(name);
        if (a == null || b == null) return r;

        // Align B's closes by openTime so we only use bars that exist in BOTH series.
        Map<Long, Double> bClose = new HashMap<>(b.size() * 2);
        for (TradingCore.Candle c : b) bClose.put(c.openTime, c.close);

        List<Double> spread = new ArrayList<>(a.size());
        List<Long>   times  = new ArrayList<>(a.size());
        for (TradingCore.Candle c : a) {                 // a is oldest-first (chronological)
            Double pb = bClose.get(c.openTime);
            if (pb == null || pb <= 0 || c.close <= 0) continue;
            spread.add(Math.log(c.close) - Math.log(pb));
            times.add(c.openTime);
        }
        int n = spread.size();
        if (n < lookback + 10) return r;

        int    pos = 0;            // +1 long spread, -1 short spread, 0 flat
        double entrySpread = 0.0, entryZabs = 0.0;
        long   entryT = 0L;
        int    barsHeld = 0;

        for (int i = lookback; i < n; i++) {
            // Mean/sd from the STRICTLY PAST window [i-lookback, i-1] — no lookahead.
            double mean = 0.0;
            for (int k = i - lookback; k < i; k++) mean += spread.get(k);
            mean /= lookback;
            double var = 0.0;
            for (int k = i - lookback; k < i; k++) { double d = spread.get(k) - mean; var += d * d; }
            double sd = Math.sqrt(var / lookback);
            if (sd <= 1e-9) continue;

            double cur = spread.get(i);
            double z   = (cur - mean) / sd;

            if (pos == 0) {
                if (z >= entryZ) {                 // spread too high → short spread
                    pos = -1; entrySpread = cur; entryZabs = z;  entryT = times.get(i); barsHeld = 0;
                } else if (z <= -entryZ) {          // spread too low → long spread
                    pos = +1; entrySpread = cur; entryZabs = -z; entryT = times.get(i); barsHeld = 0;
                }
            } else {
                barsHeld++;
                boolean exit = Math.abs(z) <= exitZ || Math.abs(z) >= stopZ || barsHeld >= timeStopBars;
                if (exit) {
                    double gross = pos * (cur - entrySpread) * 100.0;
                    double net   = gross - costRoundTripPct;
                    r.trades.add(new Trade(entryT, entryZabs, net));
                    r.netPnLPct += net;
                    if (net > 0.05) r.wins++;
                    pos = 0;
                }
            }
        }
        return r;
    }
}
