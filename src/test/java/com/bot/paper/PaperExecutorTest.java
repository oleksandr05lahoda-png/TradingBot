package com.bot.paper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The accounting rules the whole harness rests on. Every number below is worked out by hand in the
 * test, not read back from the implementation — a test that recomputes the code's own arithmetic
 * proves only that the code is self-consistent.
 */
class PaperExecutorTest {

    private static final long H  = 3_600_000L;
    private static final double EPS = 1e-9;

    private final PaperExecutor exec = new PaperExecutor();

    private static Bar bar(long openMs, double o, double h, double l, double c) {
        return new Bar("BTCUSDT", openMs, H, o, h, l, c, 1.0);
    }

    @Test
    @DisplayName("entry is the OPEN of the next bar, never the close of the signal bar")
    void entryIsNextBarOpen() {
        List<Bar> series = new ArrayList<>();
        series.add(bar(0, 100, 105,  99, 104));   // signal bar: closes at 1h with close=104
        series.add(bar(H, 110, 115, 109, 112));   // next bar opens at 110 — a visible gap
        series.add(bar(2 * H, 112, 118, 111, 117));
        series.add(bar(3 * H, 117, 119, 116, 118));  // the time-stop bar must exist, or the
                                                     // signal is correctly left unresolved

        Signal s = new Signal("BTCUSDT", Signal.Side.LONG, H, 50.0, 0.0, 1.0, "t");
        PaperExecutor.Fill f = exec.simulate(s, series, List.of(), 2);

        double slip = PaperExecutor.PAPER_SLIPPAGE_BP / 10_000.0;
        assertEquals(110.0 * (1 + slip), f.entryPx, EPS,
                "must fill at the next bar's OPEN plus slippage");
        assertNotEquals(104.0, f.entryPx, "must NOT fill at the signal bar's close");
        assertEquals(H, f.entryBarOpenMs, "entry bar is the one opening at the signal bar's close");
    }

    @Test
    @DisplayName("stop and target inside one bar resolves as the STOP")
    void bothLevelsInOneBarIsAStop() {
        List<Bar> series = new ArrayList<>();
        series.add(bar(0, 100, 100, 100, 100));
        // Next bar spans 80..130, straddling both a stop at 90 and a target at 120.
        series.add(bar(H, 100, 130, 80, 125));
        series.add(bar(2 * H, 125, 126, 124, 125));

        Signal s = new Signal("BTCUSDT", Signal.Side.LONG, H, 90.0, 120.0, 1.0, "t");
        PaperExecutor.Fill f = exec.simulate(s, series, List.of(), 3);

        assertSame(PaperExecutor.ExitReason.stop, f.exitReason,
                "OHLC cannot order the two touches, so the pessimistic one must win");
        double slip = PaperExecutor.PAPER_SLIPPAGE_BP / 10_000.0;
        assertEquals(90.0 * (1 - slip), f.exitPx, EPS, "filled at the stop, slipped against us");
    }

    @Test
    @DisplayName("25h hold charges exactly the three 8h fundings inside the window")
    void fundingAccruesPerEightHourSettlement() {
        List<Bar> series = new ArrayList<>();
        series.add(bar(0, 100, 100, 100, 100));                 // signal bar
        for (int i = 1; i <= 30; i++) {                          // flat bars, nothing triggers
            series.add(bar(i * H, 100, 100.5, 99.5, 100));
        }

        List<PaperExecutor.FundingPoint> funding = List.of(
                new PaperExecutor.FundingPoint(-1 * H, 0.001),   // before entry — must not count
                new PaperExecutor.FundingPoint(9 * H, 0.001),
                new PaperExecutor.FundingPoint(17 * H, 0.001),
                new PaperExecutor.FundingPoint(25 * H, 0.001),
                new PaperExecutor.FundingPoint(40 * H, 0.001));  // after exit — must not count

        // Entry at the bar opening 1h, held 25 bars => time stop at the bar opening 26h.
        Signal s = new Signal("BTCUSDT", Signal.Side.LONG, H, 1.0, 0.0, 1.0, "t");
        PaperExecutor.Fill f = exec.simulate(s, series, funding, 25);

        assertSame(PaperExecutor.ExitReason.time_stop, f.exitReason);
        assertEquals(0.003, f.funding, EPS, "three settlements at 0.1% each, paid by the long");
    }

    @Test
    @DisplayName("ret_net = ret_gross - fees - funding, checked on a hand-computed case")
    void retNetArithmetic() {
        List<Bar> series = new ArrayList<>();
        series.add(bar(0, 100, 100, 100, 100));
        series.add(bar(H, 100, 100, 100, 100));      // entry bar, opens at 100
        series.add(bar(2 * H, 100, 120, 100, 110));  // target 110 touched here

        List<PaperExecutor.FundingPoint> funding =
                List.of(new PaperExecutor.FundingPoint(2 * H, 0.0004));

        Signal s = new Signal("BTCUSDT", Signal.Side.LONG, H, 90.0, 110.0, 1.0, "t");
        PaperExecutor.Fill f = exec.simulate(s, series, funding, 5);

        double slip    = PaperExecutor.PAPER_SLIPPAGE_BP / 10_000.0;   // 0.0002
        double entryPx = 100.0 * (1 + slip);                           // 100.02
        double exitPx  = 110.0 * (1 - slip);                           // 109.978
        double gross   = (exitPx - entryPx) / entryPx;
        double fees    = 2 * PaperExecutor.PAPER_TAKER_FEE;            // 0.001
        double funded  = 0.0004;

        assertEquals(gross, f.retGross, EPS);
        assertEquals(fees, f.fees, EPS);
        assertEquals(funded, f.funding, EPS);
        assertEquals(gross - fees - funded, f.retNet, EPS);
    }

    @Test
    @DisplayName("funding is signed by SIDE: long pays what short receives, exactly")
    void fundingIsSignedBySide() {
        // project_state id=43: funding on this universe is positive 72.5% of the time, averaging
        // 3.85% annualised. If shorts were charged instead of credited they would be understated by
        // about 7.7% a year — enough to invert the sign of any result with a small edge. So this
        // asserts the symmetry directly rather than trusting the comment above the loop.
        List<Bar> series = new ArrayList<>();
        series.add(bar(0, 100, 100, 100, 100));
        for (int i = 1; i <= 12; i++) series.add(bar(i * H, 100, 100.5, 99.5, 100));

        List<PaperExecutor.FundingPoint> positiveRate =
                List.of(new PaperExecutor.FundingPoint(9 * H, 0.0010));   // longs pay 10bp

        Signal longS  = new Signal("BTCUSDT", Signal.Side.LONG,  H,  1.0, 0.0, 1.0, "t");
        Signal shortS = new Signal("BTCUSDT", Signal.Side.SHORT, H, 999.0, 0.0, 1.0, "t");

        PaperExecutor.Fill fl = exec.simulate(longS,  series, positiveRate, 10);
        PaperExecutor.Fill fs = exec.simulate(shortS, series, positiveRate, 10);

        assertEquals(0.0010, fl.funding, EPS, "the long PAYS a positive rate: a positive cost");
        assertEquals(-0.0010, fs.funding, EPS, "the short RECEIVES it: a negative cost");
        assertEquals(-fl.funding, fs.funding, EPS, "equal magnitude, opposite sign");
        assertNotEquals(fl.funding, fs.funding, "funding must not be charged to both sides alike");
    }

    @Test
    @DisplayName("a settlement exactly on the entry bar's open is not ours")
    void settlementAtEntryOpenIsExcluded() {
        // It accrued over the interval before the position existed. Binance settles on 4h
        // boundaries, so this lands on a real bar edge rather than a contrived one.
        List<Bar> series = new ArrayList<>();
        series.add(bar(0, 100, 100, 100, 100));
        for (int i = 1; i <= 6; i++) series.add(bar(i * H, 100, 100.5, 99.5, 100));

        Signal s = new Signal("BTCUSDT", Signal.Side.LONG, H, 1.0, 0.0, 1.0, "t");
        PaperExecutor.Fill f = exec.simulate(s, series,
                List.of(new PaperExecutor.FundingPoint(H, 0.0010)), 4);   // exactly at entry open

        assertEquals(0.0, f.funding, EPS, "must not charge a settlement from before the entry");
    }

    @Test
    @DisplayName("a signal with no bar after it is not a trade")
    void unresolvableSignalReturnsNull() {
        List<Bar> series = new ArrayList<>();
        series.add(bar(0, 100, 100, 100, 100));

        Signal s = new Signal("BTCUSDT", Signal.Side.LONG, H, 90.0, 110.0, 1.0, "t");
        assertNull(exec.simulate(s, series, List.of(), 5),
                "inventing an entry would fabricate a return");
    }
}
