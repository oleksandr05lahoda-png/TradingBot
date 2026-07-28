package com.bot.paper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The database constraint paper_signals_entry_after_signal_ck, expressed in Java:
 *
 * <pre>   0 &lt;= entry_bar_open_ms - signal_bar_close_ms &lt; bar_interval_ms</pre>
 *
 * Both executors pick the entry bar with the same rule, so both are covered here. The directional
 * path had the identical latent defect — it simply had not reached the database yet — and only the
 * carry run exposed it.
 *
 * The point of duplicating the constraint here is that the code and the schema drifted apart once
 * already: the original check compared TIMESTAMPS while the code advanced BARS, and under
 * closeMs = openMs + intervalMs a bar's close and the next bar's open carry the same timestamp. A
 * unit test fails in seconds; the schema version failed only after loading 49 symbols.
 */
class EntryTimingInvariantTest {

    private static final long H4 = 4 * 3_600_000L;

    private static Bar bar(long openMs, double px) {
        return new Bar("BTCUSDT", openMs, H4, px, px * 1.001, px * 0.999, px, 1.0);
    }

    private static List<Bar> flatSeries(int n, double px) {
        List<Bar> s = new ArrayList<>();
        for (int i = 0; i < n; i++) s.add(bar(i * H4, px));
        return s;
    }

    /** Exactly what the database enforces. */
    private static void assertSatisfiesDbConstraint(long signalBarCloseMs, long entryBarOpenMs) {
        long delta = entryBarOpenMs - signalBarCloseMs;
        assertTrue(delta >= 0,
                "entry must not precede the signal (look-ahead): delta=" + delta);
        assertTrue(delta < H4,
                "entry must be the IMMEDIATE next bar, not a later one: delta=" + delta
                        + " interval=" + H4);
    }

    @Test
    @DisplayName("directional: entry lands on the immediate next bar")
    void directionalEntryTiming() {
        List<Bar> series = flatSeries(12, 100.0);

        // Signal decided on the bar that closes at 3*H4; the next bar opens at that same instant.
        for (int signalBar = 1; signalBar <= 6; signalBar++) {
            long signalClose = series.get(signalBar).closeMs;
            Signal s = new Signal("BTCUSDT", Signal.Side.LONG, signalClose, 1.0, 0.0, 1.0, "t");
            PaperExecutor.Fill f = new PaperExecutor().simulate(s, series, List.of(), 3);
            assertTrue(f != null, "expected a fill for signal bar " + signalBar);
            assertSatisfiesDbConstraint(signalClose, f.entryBarOpenMs);
        }
    }

    @Test
    @DisplayName("carry: entry lands on the immediate next bar, on both legs")
    void carryEntryTiming() {
        List<Bar> perp = flatSeries(12, 100.2);
        List<Bar> spot = flatSeries(12, 100.0);

        for (int signalBar = 1; signalBar <= 6; signalBar++) {
            long signalClose = perp.get(signalBar).closeMs;
            CarryPosition p = new CarryPosition("BTCUSDT", signalClose,
                    Double.NaN, Double.NaN, 1.0, "t");
            CarryPaperExecutor.Fill f =
                    new CarryPaperExecutor().simulate(p, perp, spot, List.of(), 3);
            assertTrue(f != null, "expected a fill for signal bar " + signalBar);
            assertSatisfiesDbConstraint(signalClose, f.entryBarOpenMs);
        }
    }

    @Test
    @DisplayName("a gap in the series still cannot produce a delayed entry that the DB would reject")
    void gapDoesNotProduceDelayedEntry() {
        // Bars 0,1,2 then a hole where bar 3 would be, resuming at bar 4. A signal on bar 2 has no
        // immediately following bar, so the entry would land a full interval late — which the
        // database now rejects. The executor must not silently produce such a fill.
        List<Bar> series = new ArrayList<>();
        series.add(bar(0, 100));
        series.add(bar(H4, 100));
        series.add(bar(2 * H4, 100));
        series.add(bar(4 * H4, 100));      // gap: 3*H4 missing
        series.add(bar(5 * H4, 100));
        series.add(bar(6 * H4, 100));

        long signalClose = series.get(2).closeMs;   // 3*H4
        Signal s = new Signal("BTCUSDT", Signal.Side.LONG, signalClose, 1.0, 0.0, 1.0, "t");
        PaperExecutor.Fill f = new PaperExecutor().simulate(s, series, List.of(), 2);

        if (f != null) {
            long delta = f.entryBarOpenMs - signalClose;
            assertTrue(delta >= 0 && delta < H4,
                    "a fill produced across a data gap would be rejected by the database: delta="
                            + delta + " — the executor should refuse rather than emit it");
        }
    }
}
