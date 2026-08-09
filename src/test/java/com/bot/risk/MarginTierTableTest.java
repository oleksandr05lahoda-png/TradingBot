package com.bot.risk;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validation of the maintenance-margin table.
 *
 * <p>The continuity check is the point. Maintenance margin is {@code notional * rate - amount}, and
 * the amount exists precisely so the function does not step at a bracket boundary. A table whose
 * amounts do not produce continuity is a table with a wrong number in it, and a wrong maintenance
 * margin is a wrong liquidation price — which is a wrong answer to the only question that decides
 * whether a stop is real.
 */
class MarginTierTableTest {

    @Test
    @DisplayName("the bundled default is continuous at every boundary")
    void defaultTableIsContinuous() {
        MarginTierTable table = MarginTierTable.conservativeDefault();
        for (double boundary : new double[]{5_000, 25_000, 100_000, 250_000, 1_000_000}) {
            double below = table.maintenanceMargin(boundary);
            double above = table.maintenanceMargin(boundary + 0.01);
            assertEquals(below, above, 0.01,
                    "maintenance margin steps at a notional of " + boundary);
        }
    }

    @Test
    @DisplayName("maintenance margin is never negative and never decreases")
    void maintenanceMarginIsMonotonic() {
        MarginTierTable table = MarginTierTable.conservativeDefault();
        double previous = -1;
        for (double notional = 0; notional < 3_000_000; notional += 977) {
            double mm = table.maintenanceMargin(notional);
            assertTrue(mm >= -1e-9, "negative maintenance margin at " + notional + ": " + mm);
            assertTrue(mm >= previous - 1e-9, "maintenance margin fell at " + notional);
            previous = mm;
        }
    }

    @Test
    @DisplayName("a discontinuous table is refused at construction")
    void discontinuousTableIsRefused() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new MarginTierTable(List.of(
                        new MarginTier(0, 5_000, 0.010, 0, 25),
                        // cum should be 75 for continuity at 5,000; 0 makes the requirement jump.
                        new MarginTier(5_000, Double.POSITIVE_INFINITY, 0.025, 0, 20))));
        assertTrue(thrown.getMessage().contains("discontinuous"), thrown.getMessage());
    }

    @Test
    @DisplayName("a table with a gap between brackets is refused")
    void nonContiguousTableIsRefused() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new MarginTierTable(List.of(
                        new MarginTier(0, 5_000, 0.010, 0, 25),
                        new MarginTier(6_000, Double.POSITIVE_INFINITY, 0.025, 100, 20))));
        assertTrue(thrown.getMessage().contains("not contiguous"), thrown.getMessage());
    }

    @Test
    @DisplayName("a table that does not cover every notional is refused")
    void boundedTopBracketIsRefused() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new MarginTierTable(List.of(new MarginTier(0, 5_000, 0.010, 0, 25))));
        assertTrue(thrown.getMessage().contains("unbounded"), thrown.getMessage());
    }

    @Test
    @DisplayName("a table that does not start at zero is refused")
    void tableMustStartAtZero() {
        assertThrows(IllegalArgumentException.class,
                () -> new MarginTierTable(List.of(
                        new MarginTier(100, Double.POSITIVE_INFINITY, 0.010, 0, 25))));
    }

    @Test
    @DisplayName("bracket boundaries are inclusive at the top, as Binance defines them")
    void bracketBoundariesAreInclusiveAtTheTop() {
        MarginTierTable table = MarginTierTable.conservativeDefault();
        assertEquals(0.010, table.tierFor(5_000).maintenanceMarginRate());
        assertEquals(0.025, table.tierFor(5_000.01).maintenanceMarginRate());
        assertEquals(0.025, table.tierFor(25_000).maintenanceMarginRate());
        assertEquals(0.050, table.tierFor(25_000.01).maintenanceMarginRate());
        assertEquals(0.250, table.tierFor(50_000_000).maintenanceMarginRate());
    }

    @Test
    @DisplayName("leverage caps fall as notional grows")
    void leverageCapsFallWithNotional() {
        MarginTierTable table = MarginTierTable.conservativeDefault();
        assertEquals(25, table.maxLeverageAt(1_000));
        assertEquals(10, table.maxLeverageAt(50_000));
        assertEquals(5, table.maxLeverageAt(200_000));
        assertEquals(1, table.maxLeverageAt(5_000_000));
    }

    @Test
    @DisplayName("a maintenance rate of 1.0 is refused, because it would zero the long denominator")
    void maintenanceRateOfOneIsRefused() {
        // liq for a long divides by q*(MMR - 1). At MMR = 1 that is zero, the solve returns
        // -Infinity, and the non-finite result would be clamped to 0.0 — the sentinel meaning
        // "liquidation unreachable". The most dangerous possible bracket would produce the most
        // permissive possible answer, silently. It is refused at construction instead.
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new MarginTier(0, Double.POSITIVE_INFINITY, 1.0, 0, 1));
        assertTrue(thrown.getMessage().contains("must be in (0, 1)"), thrown.getMessage());

        assertThrows(IllegalArgumentException.class,
                () -> new MarginTier(0, Double.POSITIVE_INFINITY, 0.0, 0, 1));
    }

    @Test
    @DisplayName("a table whose leverage caps rise with notional is refused")
    void risingLeverageCapIsRefused() {
        assertThrows(IllegalArgumentException.class,
                () -> new MarginTierTable(List.of(
                        new MarginTier(0, 5_000, 0.010, 0, 5),
                        new MarginTier(5_000, Double.POSITIVE_INFINITY, 0.025, 75, 20))));
    }
}
