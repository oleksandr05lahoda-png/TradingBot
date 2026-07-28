package com.bot.paper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Everything a hypothesis is allowed to see at one instant, and nothing else.
 *
 * The no-look-ahead rule is enforced HERE, at construction: a bar enters the snapshot only when
 * {@code bar.closeMs <= asOfMs}. There is deliberately no method that returns anything newer, no
 * accessor that takes a timestamp, and no handle on the underlying series — so a hypothesis cannot
 * reach the future even by mistake, and a reviewer does not have to trust that it did not.
 *
 * That is the difference between "we are careful" and "it cannot happen". The same rule is checked
 * a second time in the database (paper_signals_entry_after_signal_ck), because one guard that
 * everybody trusts is how this project got id=2, id=22 and id=28.
 */
public final class MarketSnapshot {

    private final long asOfMs;
    private final Map<String, List<Bar>> bars;
    private final Map<String, List<PaperExecutor.FundingPoint>> funding;

    private MarketSnapshot(long asOfMs, Map<String, List<Bar>> bars,
                           Map<String, List<PaperExecutor.FundingPoint>> funding) {
        this.asOfMs = asOfMs;
        this.bars = bars;
        this.funding = funding;
    }

    /**
     * Build a snapshot as of {@code asOfMs} from complete series. Bars still forming, or belonging
     * to the future entirely, are dropped — not hidden behind an accessor that could be bypassed.
     *
     * @param fullSeries symbol -> bars in ascending openMs order
     */
    public static MarketSnapshot asOf(long asOfMs, Map<String, List<Bar>> fullSeries) {
        return asOf(asOfMs, fullSeries, Collections.emptyMap());
    }

    /**
     * Snapshot including funding settlements, cut by the SAME rule: a settlement is visible only
     * when {@code timeMs <= asOfMs}. Funding is the entry signal for carry hypotheses, so letting a
     * future settlement leak in here would be look-ahead of the most direct kind — deciding to
     * collect a rate that has not been announced yet.
     */
    public static MarketSnapshot asOf(long asOfMs, Map<String, List<Bar>> fullSeries,
                                      Map<String, List<PaperExecutor.FundingPoint>> fullFunding) {
        Map<String, List<PaperExecutor.FundingPoint>> visibleFunding = new LinkedHashMap<>();
        for (Map.Entry<String, List<PaperExecutor.FundingPoint>> e : fullFunding.entrySet()) {
            List<PaperExecutor.FundingPoint> src = e.getValue();
            int lo = 0, hi = src.size();
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (src.get(mid).timeMs <= asOfMs) lo = mid + 1; else hi = mid;
            }
            visibleFunding.put(e.getKey(), Collections.unmodifiableList(src.subList(0, lo)));
        }
        Map<String, List<Bar>> visible = new LinkedHashMap<>();
        for (Map.Entry<String, List<Bar>> e : fullSeries.entrySet()) {
            List<Bar> src = e.getValue();
            int cut = firstIndexClosingAfter(src, asOfMs);
            // A VIEW, not a copy: a historical run builds a snapshot per bar per symbol, and
            // copying would make that quadratic — roughly 3e9 element copies over 49 symbols and
            // 1300 days. The caller must not mutate the source lists afterwards; the driver builds
            // them once and hands over unmodifiable lists.
            visible.put(e.getKey(), Collections.unmodifiableList(src.subList(0, cut)));
        }
        return new MarketSnapshot(asOfMs, Collections.unmodifiableMap(visible),
                Collections.unmodifiableMap(visibleFunding));
    }

    /** Index of the first bar that is NOT yet closed at asOfMs. Binary search; series is ascending. */
    private static int firstIndexClosingAfter(List<Bar> src, long asOfMs) {
        int lo = 0, hi = src.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (src.get(mid).closeMs <= asOfMs) lo = mid + 1; else hi = mid;
        }
        return lo;
    }

    /** The instant this snapshot is taken at. Nothing in it closed later than this. */
    public long asOfMs() { return asOfMs; }

    public Set<String> symbols() { return bars.keySet(); }

    /** Closed bars for a symbol, oldest first. Empty when the symbol has no history yet. */
    public List<Bar> bars(String symbol) {
        List<Bar> b = bars.get(symbol);
        return b == null ? Collections.emptyList() : b;
    }

    /** Funding settlements already announced at asOfMs, oldest first. */
    public List<PaperExecutor.FundingPoint> funding(String symbol) {
        List<PaperExecutor.FundingPoint> f = funding.get(symbol);
        return f == null ? Collections.emptyList() : f;
    }

    /** The latest announced settlement, or null when none has been announced yet. */
    public PaperExecutor.FundingPoint lastFunding(String symbol) {
        List<PaperExecutor.FundingPoint> f = funding(symbol);
        return f.isEmpty() ? null : f.get(f.size() - 1);
    }

    /** The most recently CLOSED bar, or null when the symbol has none yet. */
    public Bar lastClosed(String symbol) {
        List<Bar> b = bars(symbol);
        return b.isEmpty() ? null : b.get(b.size() - 1);
    }
}
