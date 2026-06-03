package com.bot;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * [v85.0] CARRY (basis / funding harvest) BACKTEST — delta-neutral.
 *
 * EDGE SOURCE: the funding PAYMENT itself (a real cash flow), NOT price prediction.
 * Position when funding is positive = long spot + short perp → delta-neutral, so the
 * two legs' price moves cancel and you simply COLLECT funding each 8h. This is
 * market-neutral (survives BTC dumps that killed every directional strategy) and
 * structural (a cash flow exists whether or not you can "predict" anything).
 *
 * WHY this is testable now without a spot leg: in delta-neutral carry the price PnL
 * is ~0 by construction, so the yield is determined by FUNDING HISTORY ALONE — which
 * the bot already fetches. We don't need spot prices to ESTIMATE the edge; we need
 * them only to EXECUTE it live later.
 *
 * Model (positive-carry only — the no-borrow, retail-accessible version):
 *   - enter when prior funding > minRate (long spot + short perp), collect funding each event
 *   - exit when funding drops below minRate; pay a round-trip cost per cycle (2 legs in+out)
 *   - NO lookahead: the enter/exit decision at event i uses funding known at i-1.
 *
 * This estimate is OPTIMISTIC (ignores basis-convergence risk, spot trading fees, and
 * execution slippage on the legs). So: if even this is thin/negative, carry is not worth
 * the spot integration. If it's a solid market-neutral yield, it's the real edge to build.
 */
public final class CarryBacktester {

    private CarryBacktester() {}

    public static final class Result {
        public final String symbol;
        public int    events, cycles;
        public double grossPct, netPct, annualizedPct, daysCovered;
        public Result(String symbol) { this.symbol = symbol; }
    }

    /**
     * @param funding    sorted {timestampMs -> fundingRate} (0.0001 = 0.01%/8h)
     * @param minRate    only hold carry when funding above this (e.g. 0.00005 = 0.005%/8h)
     * @param rtCostPct  round-trip cost (%) to open+close both legs of one carry cycle (e.g. 0.30)
     */
    public static Result run(String symbol, TreeMap<Long, Double> funding,
                             double minRate, double rtCostPct) {
        Result r = new Result(symbol);
        if (funding == null || funding.size() < 6) return r;

        List<Long>   ts = new ArrayList<>(funding.keySet());
        List<Double> fr = new ArrayList<>(funding.values());
        int n = fr.size();

        int pos = 0;                 // 1 = holding carry (collecting), 0 = flat
        double net = 0.0, gross = 0.0;
        int cycles = 0;

        for (int i = 1; i < n; i++) {
            // Decision uses funding known at i-1 (strictly past) → no lookahead.
            boolean wantIn = fr.get(i - 1) > minRate;
            if (wantIn && pos == 0) {            // open carry
                pos = 1; net -= rtCostPct / 2.0;
            } else if (!wantIn && pos == 1) {    // close carry
                pos = 0; net -= rtCostPct / 2.0; cycles++;
            }
            if (pos == 1) {                      // collect this settlement's funding
                double f = fr.get(i) * 100.0;
                net   += f;
                gross += f;
            }
        }
        if (pos == 1) { net -= rtCostPct / 2.0; cycles++; }  // close final open position

        r.events   = n;
        r.cycles   = cycles;
        r.grossPct = gross;
        r.netPct   = net;
        double ms = ts.get(n - 1) - ts.get(0);
        r.daysCovered = ms / 86_400_000.0;
        if (r.daysCovered > 0.5) r.annualizedPct = net * (365.0 / r.daysCovered);
        return r;
    }
}
