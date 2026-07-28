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

    private MarketSnapshot(long asOfMs, Map<String, List<Bar>> bars) {
        this.asOfMs = asOfMs;
        this.bars = bars;
    }

    /**
     * Build a snapshot as of {@code asOfMs} from complete series. Bars still forming, or belonging
     * to the future entirely, are dropped — not hidden behind an accessor that could be bypassed.
     *
     * @param fullSeries symbol -> bars in ascending openMs order
     */
    public static MarketSnapshot asOf(long asOfMs, Map<String, List<Bar>> fullSeries) {
        Map<String, List<Bar>> visible = new LinkedHashMap<>();
        for (Map.Entry<String, List<Bar>> e : fullSeries.entrySet()) {
            List<Bar> src = e.getValue();
            List<Bar> dst = new ArrayList<>(Math.min(src.size(), 512));
            for (Bar b : src) {
                if (b.closeMs <= asOfMs) dst.add(b);
                else break;                       // ascending order: everything after is newer
            }
            visible.put(e.getKey(), Collections.unmodifiableList(dst));
        }
        return new MarketSnapshot(asOfMs, Collections.unmodifiableMap(visible));
    }

    /** The instant this snapshot is taken at. Nothing in it closed later than this. */
    public long asOfMs() { return asOfMs; }

    public Set<String> symbols() { return bars.keySet(); }

    /** Closed bars for a symbol, oldest first. Empty when the symbol has no history yet. */
    public List<Bar> bars(String symbol) {
        List<Bar> b = bars.get(symbol);
        return b == null ? Collections.emptyList() : b;
    }

    /** The most recently CLOSED bar, or null when the symbol has none yet. */
    public Bar lastClosed(String symbol) {
        List<Bar> b = bars(symbol);
        return b.isEmpty() ? null : b.get(b.size() - 1);
    }
}
