package com.bot.paper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Carry accounting. Every expected number is worked out here from first principles — a test that
 * re-runs the implementation's own formula proves only self-consistency.
 */
class CarryPaperExecutorTest {

    private static final long H = 3_600_000L;
    private static final double EPS = 1e-9;
    private static final double SLIP = PaperExecutor.PAPER_SLIPPAGE_BP / 10_000.0;  // 0.0002

    private final CarryPaperExecutor exec = new CarryPaperExecutor();

    private static Bar bar(String sym, long openMs, double o, double c) {
        return new Bar(sym, openMs, H, o, Math.max(o, c), Math.min(o, c), c, 1.0);
    }

    /** Flat series where perp sits a fixed number of bp above spot. */
    private static void flat(List<Bar> perp, List<Bar> spot, int bars, double spotPx, double basisBp) {
        double perpPx = spotPx * (1 + basisBp / 10_000.0);
        for (int i = 0; i <= bars; i++) {
            perp.add(bar("BTCUSDT", i * H, perpPx, perpPx));
            spot.add(bar("BTCUSDT", i * H, spotPx, spotPx));
        }
    }

    @Test
    @DisplayName("fees are exactly FOUR fills, not two")
    void feesAreFourFills() {
        List<Bar> perp = new ArrayList<>(), spot = new ArrayList<>();
        flat(perp, spot, 10, 1000.0, 10.0);

        CarryPosition pos = new CarryPosition("BTCUSDT", H, Double.NaN, Double.NaN, null, 1.0, "t");
        CarryPaperExecutor.Fill f = exec.simulate(pos, perp, spot, List.of(), 5);

        assertEquals(4, CarryPaperExecutor.CARRY_FILLS);
        assertEquals(4 * PaperExecutor.PAPER_TAKER_FEE, f.fees, EPS);
        assertEquals(0.0020, f.fees, EPS, "0.20% round-trip — the number that decides carry");
        // Twice what a one-legged trade pays.
        assertEquals(2 * (2 * PaperExecutor.PAPER_TAKER_FEE), f.fees, EPS);
    }

    @Test
    @DisplayName("25h hold collects exactly the three 8h settlements, as INCOME for the short perp")
    void fundingIsCollectedThreeTimes() {
        List<Bar> perp = new ArrayList<>(), spot = new ArrayList<>();
        flat(perp, spot, 40, 1000.0, 10.0);

        List<PaperExecutor.FundingPoint> funding = List.of(
                new PaperExecutor.FundingPoint(-1 * H, 0.0001),   // before entry — excluded
                new PaperExecutor.FundingPoint(9 * H, 0.0001),
                new PaperExecutor.FundingPoint(17 * H, 0.0001),
                new PaperExecutor.FundingPoint(25 * H, 0.0001),
                new PaperExecutor.FundingPoint(40 * H, 0.0001));  // after exit — excluded

        // Entry at the bar opening 1h, 25 bars held => time stop at the bar opening 26h.
        CarryPosition pos = new CarryPosition("BTCUSDT", H, Double.NaN, Double.NaN, null, 1.0, "t");
        CarryPaperExecutor.Fill f = exec.simulate(pos, perp, spot, funding, 25);

        assertSame(CarryPaperExecutor.ExitReason.time_stop, f.exitReason);
        // Short perp COLLECTS a positive rate, so the cost is negative: three settlements at 1bp.
        assertEquals(-0.0003, f.funding, EPS,
                "collected 3 x 0.01% — stored as a negative cost, matching paper_signals.funding");
    }

    @Test
    @DisplayName("basis drift is signed so that a widening basis LOSES money")
    void basisDriftSign() {
        // Spot flat at 1000. Perp starts 20bp rich and ends 60bp rich: the short perp leg loses.
        List<Bar> perp = new ArrayList<>(), spot = new ArrayList<>();
        double spotPx = 1000.0;
        double perpIn  = spotPx * (1 + 20.0 / 10_000.0);   // 1002
        double perpOut = spotPx * (1 + 60.0 / 10_000.0);   // 1006
        perp.add(bar("BTCUSDT", 0, perpIn, perpIn));
        perp.add(bar("BTCUSDT", H, perpIn, perpIn));        // entry bar
        perp.add(bar("BTCUSDT", 2 * H, perpOut, perpOut));  // time-stop bar
        for (int i = 0; i <= 2; i++) spot.add(bar("BTCUSDT", i * H, spotPx, spotPx));

        CarryPosition pos = new CarryPosition("BTCUSDT", H, Double.NaN, Double.NaN, null, 1.0, "t");
        CarryPaperExecutor.Fill f = exec.simulate(pos, perp, spot, List.of(), 1);

        assertEquals(20.0, f.basisEntryBp, 1e-6);
        assertEquals(60.0, f.basisExitBp, 1e-6);
        assertTrue(f.basisDrift < 0, "a basis that widened must show a negative drift");
        assertEquals(-0.0040, f.basisDrift, 1e-9, "-(60bp - 20bp) = -40bp");
        assertTrue(f.retGross < 0, "widening basis loses on the pair, got " + f.retGross);
    }

    @Test
    @DisplayName("gross P&L is the exact sum of both legs, hand-computed")
    void grossIsExactTwoLegSum() {
        // Spot rises 1000 -> 1050 while the basis narrows from 20bp to 5bp.
        double s0 = 1000.0, s1 = 1050.0;
        double p0 = s0 * (1 + 20.0 / 10_000.0);            // 1002
        double p1 = s1 * (1 +  5.0 / 10_000.0);            // 1050.525

        List<Bar> perp = new ArrayList<>(), spot = new ArrayList<>();
        perp.add(bar("BTCUSDT", 0, p0, p0));
        perp.add(bar("BTCUSDT", H, p0, p0));
        perp.add(bar("BTCUSDT", 2 * H, p1, p1));
        spot.add(bar("BTCUSDT", 0, s0, s0));
        spot.add(bar("BTCUSDT", H, s0, s0));
        spot.add(bar("BTCUSDT", 2 * H, s1, s1));

        CarryPosition pos = new CarryPosition("BTCUSDT", H, Double.NaN, Double.NaN, null, 1.0, "t");
        CarryPaperExecutor.Fill f = exec.simulate(pos, perp, spot, List.of(), 1);

        double perpEntry = p0 * (1 - SLIP);   // sold the perp
        double spotEntry = s0 * (1 + SLIP);   // bought the spot
        double perpExit  = p1 * (1 + SLIP);   // bought it back
        double spotExit  = s1 * (1 - SLIP);   // sold it
        double expected  = (perpEntry - perpExit) / perpEntry + (spotExit - spotEntry) / spotEntry;

        assertEquals(perpEntry, f.perpEntryPx, EPS);
        assertEquals(spotEntry, f.spotEntryPx, EPS);
        assertEquals(expected, f.retGross, EPS);
        assertEquals(expected - f.fees - f.funding, f.retNet, EPS);

        // And the first-order shortcut really is only an approximation: it differs from the exact
        // figure by more than a tenth of a basis point here, which is why the legs are summed.
        assertTrue(Math.abs(f.basisDrift - f.retGross) > 1e-5,
                "drift=" + f.basisDrift + " exact=" + f.retGross);
    }

    @Test
    @DisplayName("exits when the basis converges to the pre-registered level")
    void exitsOnBasisConvergence() {
        List<Bar> perp = new ArrayList<>(), spot = new ArrayList<>();
        double s = 1000.0;
        for (int i = 0; i <= 6; i++) spot.add(bar("BTCUSDT", i * H, s, s));
        perp.add(bar("BTCUSDT", 0, s * 1.0030, s * 1.0030));
        perp.add(bar("BTCUSDT", H, s * 1.0030, s * 1.0030));       // entry, 30bp
        perp.add(bar("BTCUSDT", 2 * H, s * 1.0020, s * 1.0020));   // 20bp
        perp.add(bar("BTCUSDT", 3 * H, s * 1.0004, s * 1.0004));   // 4bp -> at/below 5bp
        for (int i = 4; i <= 6; i++) perp.add(bar("BTCUSDT", i * H, s, s));

        CarryPosition pos = new CarryPosition("BTCUSDT", H, 5.0, Double.NaN, null, 1.0, "t");
        CarryPaperExecutor.Fill f = exec.simulate(pos, perp, spot, List.of(), 5);

        assertSame(CarryPaperExecutor.ExitReason.basis_converged, f.exitReason);
        assertEquals(3 * H, f.exitBarMs, "must exit on the bar where the basis reached the level");
    }

    @Test
    @DisplayName("exits at the first negative funding settlement")
    void exitsOnFundingFlip() {
        List<Bar> perp = new ArrayList<>(), spot = new ArrayList<>();
        flat(perp, spot, 10, 1000.0, 30.0);

        List<PaperExecutor.FundingPoint> funding = List.of(
                new PaperExecutor.FundingPoint(2 * H, 0.0001),
                new PaperExecutor.FundingPoint(4 * H, -0.0002));   // the flip

        CarryPosition pos = new CarryPosition("BTCUSDT", H, Double.NaN, -0.00001, CarryPosition.FundingExit.AT_OR_BELOW, 1.0, "t");
        CarryPaperExecutor.Fill f = exec.simulate(pos, perp, spot, funding, 8);

        assertSame(CarryPaperExecutor.ExitReason.funding_flipped, f.exitReason);
        // The settlement at exactly 4H belongs to the bar (3H, 4H] — the one it accrued over —
        // so the trade ends on the bar opening at 3H, not the one opening at 4H. Binance settles
        // on 4h boundaries, so this boundary rule decides a real case, not a corner one.
        assertEquals(3 * H, f.exitBarMs);
        // Collected +1bp, then paid 2bp on the settlement that ended it: net cost +1bp.
        assertEquals(0.0001, f.funding, EPS);
    }

    @Test
    @DisplayName("no hedge bar means no trade — a single leg is not a carry")
    void missingHedgeLegIsNotATrade() {
        List<Bar> perp = new ArrayList<>(), spot = new ArrayList<>();
        for (int i = 0; i <= 5; i++) perp.add(bar("BTCUSDT", i * H, 1002, 1002));
        spot.add(bar("BTCUSDT", 0, 1000, 1000));   // spot stops before the entry bar

        CarryPosition pos = new CarryPosition("BTCUSDT", H, Double.NaN, Double.NaN, null, 1.0, "t");
        assertNull(exec.simulate(pos, perp, spot, List.of(), 3),
                "filling one leg would invent a hedge that was never there");
    }
}
