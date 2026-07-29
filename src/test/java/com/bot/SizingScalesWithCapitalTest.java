package com.bot;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * project_state id=53 — every ceiling in the sizing path must be a share of capital.
 *
 * The defect this pins down: EXEC_MAX_NOTIONAL_USD was a flat $6, so a $10000 account and a $20
 * account were capped identically. It is also why the boot banner could advertise risk=2.0%/trade
 * while the real ceiling was around $0.12 (id=32b) — a percentage and an absolute figure were
 * fighting, and the absolute one won.
 *
 * These tests need no API keys and touch no network: since id=32a moved the account-mode mutation
 * out of the constructor, building the executor is side-effect free.
 */
class SizingScalesWithCapitalTest {

    private static final double EPS = 1e-9;

    private final BinanceTradeExecutor ex = BinanceTradeExecutor.getInstance();

    @Test
    @DisplayName("notional ceiling scales exactly proportionally with the balance")
    void notionalCeilingIsProportional() {
        double atHundred    = ex.plannedNotionalCeilingUsd(100.0);
        double atThousand   = ex.plannedNotionalCeilingUsd(1_000.0);
        double atTenThousand= ex.plannedNotionalCeilingUsd(10_000.0);

        assertTrue(atHundred > 0, "a positive balance must permit a positive ceiling");
        assertEquals(10.0, atThousand / atHundred, EPS,
                "10x the capital must give exactly 10x the ceiling");
        assertEquals(100.0, atTenThousand / atHundred, EPS,
                "100x the capital must give exactly 100x the ceiling");

        // The specific defect: a flat $6 would have made all three equal.
        assertTrue(atTenThousand > 1_000.0,
                "a $10000 account must not be capped at a small absolute figure, got " + atTenThousand);
    }

    @Test
    @DisplayName("the ceiling is a fixed fraction of capital, whatever the capital is")
    void ceilingIsAConstantFraction() {
        double fraction = ex.plannedNotionalCeilingUsd(1_000.0) / 1_000.0;
        for (double balance : new double[]{50, 137.42, 1_000, 12_345.67, 1_000_000}) {
            assertEquals(fraction, ex.plannedNotionalCeilingUsd(balance) / balance, EPS,
                    "fraction must not depend on the balance, broke at " + balance);
        }
        assertEquals(ex.getMaxNotionalHardPct() / 100.0, fraction, EPS,
                "and that fraction is the declared hard ceiling");
    }

    @Test
    @DisplayName("realized risk ceiling scales with capital too, at a fixed stop distance")
    void realizedRiskScales() {
        double slDist = 0.02;   // 2% stop
        double atThousand    = ex.realizedRiskCeilingUsd(1_000.0, slDist);
        double atTenThousand = ex.realizedRiskCeilingUsd(10_000.0, slDist);

        assertTrue(atThousand > 0);
        assertEquals(10.0, atTenThousand / atThousand, EPS,
                "10x the capital must risk 10x the dollars at the same stop distance");

        // Under the old flat $6 cap this was 6 * 0.02 = $0.12 regardless of balance.
        assertTrue(atTenThousand > 1.0,
                "a $10000 account risking 2% on a 2% stop cannot be bounded near a dollar, got "
                        + atTenThousand);
    }

    @Test
    @DisplayName("zero and negative balances yield no room to trade")
    void nonPositiveBalanceGivesNoCeiling() {
        assertEquals(0.0, ex.plannedNotionalCeilingUsd(0.0), EPS);
        assertTrue(ex.plannedNotionalCeilingUsd(-1.0) <= 0.0,
                "a negative balance must not produce a positive allowance");
    }

    @Test
    @DisplayName("the hard ceiling is not below the normal cap")
    void hardCeilingIsTheLooserOfTheTwo() {
        assertTrue(ex.getMaxNotionalHardPct() >= ex.getMaxNotionalPct(),
                "the hard ceiling exists to bound the case where the exchange minimum pushes the "
                        + "normal cap upward, so it cannot sit below it: hard="
                        + ex.getMaxNotionalHardPct() + " normal=" + ex.getMaxNotionalPct());
    }
}
