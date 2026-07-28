package com.bot.paper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The pre-registered parameters, and the two readings the specification left open. */
class FundingCarryV1Test {

    private static final long H4 = 4 * 3_600_000L;

    private final FundingCarryV1 h = new FundingCarryV1();

    private static Bar bar(long openMs, double px) {
        return new Bar("BTCUSDT", openMs, H4, px, px, px, px, 1.0);
    }

    private static Map<String, List<Bar>> series(double px, int n) {
        List<Bar> b = new ArrayList<>();
        for (int i = 0; i < n; i++) b.add(bar(i * H4, px));
        return Map.of("BTCUSDT", b);
    }

    @Test
    @DisplayName("parameters are the pre-registered ones")
    void parametersArePreRegistered() {
        assertEquals("funding_carry", h.name());
        assertEquals("v1", h.version());
        assertEquals(0.0005, FundingCarryV1.ENTRY_FUNDING, 0.0);
        assertEquals(0.0005, FundingCarryV1.EXIT_FUNDING, 0.0);
        assertEquals(114, h.maxHoldBars(), "57 settlements x 8h = 456h = 114 bars of 4h");
        assertEquals(0.0, FundingCarryV1.EXIT_BASIS_BP, 0.0, "converged = perp no longer above spot");
    }

    @Test
    @DisplayName("entry fires on the crossing bar only, not on every bar above the threshold")
    void entryIsAnEventNotAState() {
        Map<String, List<Bar>> perp = series(1002.0, 6);
        Map<String, List<Bar>> spot = series(1000.0, 6);

        // Rate crosses 5bp upward at 2*H4 and stays high afterwards.
        List<PaperExecutor.FundingPoint> f = List.of(
                new PaperExecutor.FundingPoint(1 * H4, 0.0002),
                new PaperExecutor.FundingPoint(2 * H4, 0.0009),
                new PaperExecutor.FundingPoint(3 * H4, 0.0009),
                new PaperExecutor.FundingPoint(4 * H4, 0.0009));
        Map<String, List<PaperExecutor.FundingPoint>> fm = Map.of("BTCUSDT", f);

        int emitted = 0;
        for (int i = 1; i <= 5; i++) {
            long t = i * H4;   // each bar's close
            List<CarryPosition> out = h.evaluate(
                    MarketSnapshot.asOf(t, perp, fm), MarketSnapshot.asOf(t, spot));
            emitted += out.size();
        }
        assertEquals(1, emitted,
                "one episode above the threshold must produce ONE position, not one per bar");
    }

    @Test
    @DisplayName("no entry while the rate stays below the threshold")
    void noEntryBelowThreshold() {
        Map<String, List<Bar>> perp = series(1002.0, 6);
        Map<String, List<Bar>> spot = series(1000.0, 6);
        Map<String, List<PaperExecutor.FundingPoint>> fm = Map.of("BTCUSDT", List.of(
                new PaperExecutor.FundingPoint(1 * H4, 0.0002),
                new PaperExecutor.FundingPoint(2 * H4, 0.0005),   // equal, not above
                new PaperExecutor.FundingPoint(3 * H4, 0.0004)));

        int emitted = 0;
        for (int i = 1; i <= 4; i++) {
            long t = i * H4;
            emitted += h.evaluate(MarketSnapshot.asOf(t, perp, fm),
                    MarketSnapshot.asOf(t, spot)).size();
        }
        assertEquals(0, emitted, "5bp exactly is not above 5bp");
    }

    @Test
    @DisplayName("a settlement not yet announced cannot trigger an entry")
    void cannotSeeFutureFunding() {
        Map<String, List<Bar>> perp = series(1002.0, 6);
        Map<String, List<Bar>> spot = series(1000.0, 6);
        Map<String, List<PaperExecutor.FundingPoint>> fm = Map.of("BTCUSDT", List.of(
                new PaperExecutor.FundingPoint(1 * H4, 0.0002),
                new PaperExecutor.FundingPoint(4 * H4, 0.0009)));   // the crossing, later

        // Evaluated at the close of bar 2, well before that settlement exists.
        List<CarryPosition> out = h.evaluate(
                MarketSnapshot.asOf(2 * H4, perp, fm), MarketSnapshot.asOf(2 * H4, spot));
        assertTrue(out.isEmpty(), "the snapshot must not expose an unannounced settlement");
    }

    @Test
    @DisplayName("no position without both legs priced at the same instant")
    void needsBothLegs() {
        Map<String, List<Bar>> perp = series(1002.0, 6);
        Map<String, List<Bar>> spot = Map.of("BTCUSDT", List.<Bar>of());   // spot leg missing
        Map<String, List<PaperExecutor.FundingPoint>> fm = Map.of("BTCUSDT", List.of(
                new PaperExecutor.FundingPoint(1 * H4, 0.0002),
                new PaperExecutor.FundingPoint(2 * H4, 0.0009)));

        assertTrue(h.evaluate(MarketSnapshot.asOf(2 * H4, perp, fm),
                MarketSnapshot.asOf(2 * H4, spot)).isEmpty());
    }
}
