package com.bot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * [v86.9] CROSS-SECTIONAL MOMENTUM backtest — relative-strength, market-neutral.
 *
 * EDGE SOURCE: dispersion, NOT direction. Even when the whole market drifts with no
 * tradeable absolute trend (exactly where the directional TREND strategy sits flat),
 * SOME coins fall harder and SOME hold up. Rank the universe by recent return, LONG
 * the top-K strongest and SHORT the bottom-K weakest. PnL = the spread between winners
 * and losers, which exists in chop and in down markets — so it produces signals when
 * the trend strategy cannot. This is the answer to "the market is dead, nothing fires".
 *
 * Why it is testable now: needs only price history (already fetched). The estimate is
 * OPTIMISTIC — it ignores funding paid on the short legs and borrow cost — so if even
 * this is thin/negative, cross-sectional momentum is NOT worth building live. If it is
 * a solid market-neutral spread that holds across both time-halves, it is the real
 * second edge to build.
 *
 * No lookahead: ranking at rebalance t uses the return over [t-lookback, t] (strictly
 * past); realized leg returns use the forward window [t, t+rebalance].
 */
public final class CrossSectionalMomentumBacktester {

    private CrossSectionalMomentumBacktester() {}

    public static final class Result {
        public int    rebalances, wins, symbolsUsed;
        public double grossPct, netPct, avgPerRebalance, daysCovered;
        public double firstHalfPct, secondHalfPct;
        public double longLegPct, shortLegPct;
    }

    private static final class Row {
        final double pastRet, fwdRet;
        Row(double pastRet, double fwdRet) { this.pastRet = pastRet; this.fwdRet = fwdRet; }
    }

    /**
     * @param series      symbol -> hourly candles (ascending time)
     * @param lookbackHrs ranking lookback in hours (e.g. 168 = 7d)
     * @param rebalHrs    rebalance period in hours (e.g. 24 = daily)
     * @param topK        LONG top-K strongest, SHORT bottom-K weakest
     * @param costPct     round-trip turnover cost charged per rebalance, % (e.g. 0.20)
     */
    public static Result run(Map<String, List<com.bot.TradingCore.Candle>> series,
                             int lookbackHrs, int rebalHrs, int topK, double costPct) {
        Result r = new Result();
        if (series == null || topK < 1 || series.size() < 2 * topK) return r;

        // Per-symbol time->close maps; establish the common time grid.
        Map<String, TreeMap<Long, Double>> closes = new HashMap<>();
        long gridStart = Long.MAX_VALUE, gridEnd = Long.MIN_VALUE;
        for (Map.Entry<String, List<com.bot.TradingCore.Candle>> e : series.entrySet()) {
            List<com.bot.TradingCore.Candle> cs = e.getValue();
            if (cs == null || cs.size() < 2) continue;
            TreeMap<Long, Double> m = new TreeMap<>();
            for (com.bot.TradingCore.Candle c : cs) if (c.close > 0) m.put(c.openTime, c.close);
            if (m.size() < 2) continue;
            closes.put(e.getKey(), m);
            gridStart = Math.min(gridStart, m.firstKey());
            gridEnd   = Math.max(gridEnd,   m.lastKey());
        }
        if (closes.size() < 2 * topK) return r;

        final long H = 3_600_000L;
        long lookMs = (long) lookbackHrs * H;
        long rebMs  = (long) rebalHrs   * H;
        long firstRebal = gridStart + lookMs;
        long lastRebal  = gridEnd - rebMs;          // need a full forward window
        if (lastRebal <= firstRebal) return r;
        long midTime = (firstRebal + lastRebal) / 2L;

        for (long t = firstRebal; t <= lastRebal; t += rebMs) {
            List<Row> rows = new ArrayList<>();
            for (TreeMap<Long, Double> m : closes.values()) {
                Double pPast = floor(m, t - lookMs);
                Double pNow  = floor(m, t);
                Double pFwd  = floor(m, t + rebMs);
                if (pPast == null || pNow == null || pFwd == null || pPast <= 0 || pNow <= 0) continue;
                rows.add(new Row(pNow / pPast - 1.0, pFwd / pNow - 1.0));
            }
            if (rows.size() < 2 * topK) continue;
            rows.sort((a, b) -> Double.compare(b.pastRet, a.pastRet));   // strongest first

            double longSum = 0, shortSum = 0;
            for (int i = 0; i < topK; i++) {
                longSum  += rows.get(i).fwdRet;                          // long the strongest
                shortSum += rows.get(rows.size() - 1 - i).fwdRet;        // short the weakest
            }
            double longAvg  = longSum  / topK;
            double shortAvg = shortSum / topK;
            // Market-neutral: long leg earns +fwd, short leg earns -fwd. Spread = the edge.
            double gross = (longAvg - shortAvg) * 100.0;
            double net   = gross - costPct;

            r.grossPct    += gross;
            r.netPct      += net;
            r.longLegPct  += longAvg * 100.0;
            r.shortLegPct += (-shortAvg) * 100.0;
            r.rebalances++;
            if (net > 0) r.wins++;
            if (t <= midTime) r.firstHalfPct += net; else r.secondHalfPct += net;
        }

        r.symbolsUsed     = closes.size();
        r.avgPerRebalance = r.rebalances > 0 ? r.netPct / r.rebalances : 0.0;
        r.daysCovered     = (gridEnd - gridStart) / 86_400_000.0;
        return r;
    }

    private static Double floor(TreeMap<Long, Double> m, long t) {
        Map.Entry<Long, Double> e = m.floorEntry(t);
        return e == null ? null : e.getValue();
    }
}
