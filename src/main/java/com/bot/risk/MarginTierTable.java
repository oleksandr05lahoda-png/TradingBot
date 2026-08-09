package com.bot.risk;

import com.bot.core.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The maintenance-margin brackets for one symbol, validated as a whole.
 *
 * <p>The constructor rejects anything that is not a usable maintenance-margin function: brackets
 * starting at zero, contiguous, covering every notional, rates non-decreasing — and the check that
 * actually catches bad data, a <b>continuous</b> margin at every boundary. If
 * {@code cap * rate_i - cum_i} differs from {@code cap * rate_(i+1) - cum_(i+1)}, a number is wrong,
 * and a wrong maintenance margin is a wrong liquidation price.
 *
 * <p>Normally built from {@code GET /fapi/v1/leverageBracket}; {@link #conservativeDefault()} lets
 * the risk core run without a network call and is stricter than BTCUSDT's real brackets.
 */
public final class MarginTierTable {

    private final List<MarginTier> tiers;

    public MarginTierTable(List<MarginTier> tiers) {
        Preconditions.notNull(tiers, "tiers");
        Preconditions.require(!tiers.isEmpty(), "margin tier table must not be empty");

        List<MarginTier> sorted = new ArrayList<>(tiers);
        sorted.sort(Comparator.comparingDouble(MarginTier::notionalFloor));

        Preconditions.require(sorted.get(0).notionalFloor() == 0.0,
                "the first bracket must start at notional 0, got " + sorted.get(0).notionalFloor());
        Preconditions.require(Double.isInfinite(sorted.get(sorted.size() - 1).notionalCap()),
                "the last bracket must be unbounded (cap = +Infinity) so every notional is covered");

        for (int i = 0; i < sorted.size() - 1; i++) {
            MarginTier lo = sorted.get(i);
            MarginTier hi = sorted.get(i + 1);
            Preconditions.require(lo.notionalCap() == hi.notionalFloor(),
                    "brackets are not contiguous: " + lo.notionalCap() + " then " + hi.notionalFloor());
            Preconditions.require(hi.maintenanceMarginRate() >= lo.maintenanceMarginRate(),
                    "maintenance margin rate must not decrease as notional grows");
            Preconditions.require(hi.maxLeverage() <= lo.maxLeverage(),
                    "max leverage must not increase as notional grows");

            double boundary = lo.notionalCap();
            double below = lo.maintenanceMargin(boundary);
            double above = hi.maintenanceMargin(boundary);
            double tolerance = 1e-6 * Math.max(1.0, Math.abs(below));
            Preconditions.require(Math.abs(below - above) <= tolerance,
                    String.format("maintenance margin is discontinuous at notional %.2f: %.10f below, "
                            + "%.10f above — check maintenanceAmount (cum)", boundary, below, above));
        }

        this.tiers = List.copyOf(sorted);
    }

    public List<MarginTier> tiers() { return tiers; }

    /** The bracket that governs {@code notional}. */
    public MarginTier tierFor(double notional) {
        Preconditions.nonNegativeFinite(notional, "notional");
        for (MarginTier t : tiers) {
            if (t.contains(notional)) return t;
        }
        // Unreachable while the constructor's coverage invariant holds; kept as a loud failure
        // rather than a silent fall-through to the first bracket.
        throw new IllegalStateException("no margin bracket covers notional " + notional);
    }

    /** Maintenance margin required to hold {@code notional}. */
    public double maintenanceMargin(double notional) {
        return tierFor(notional).maintenanceMargin(notional);
    }

    /** The exchange's own leverage cap at {@code notional}. Independent of {@link RiskConstants#MAX_LEVERAGE}. */
    public int maxLeverageAt(double notional) {
        return tierFor(notional).maxLeverage();
    }

    /**
     * A generic USDⓈ-M bracket table, the shape Binance publishes for mid-cap perpetuals. Used only
     * when the real brackets have not been fetched. It is on the strict side of the real ones for
     * majors, so a plan approved against it stays approved against the exchange's own numbers.
     *
     * <p>Continuity holds at every boundary — 5k, 25k, 100k, 250k and 1M — which the constructor
     * re-checks on every instantiation.
     */
    public static MarginTierTable conservativeDefault() {
        return new MarginTierTable(List.of(
                new MarginTier(0,          5_000,             0.010,       0,       25),
                new MarginTier(5_000,      25_000,            0.025,      75,       20),
                new MarginTier(25_000,     100_000,           0.050,     700,       10),
                new MarginTier(100_000,    250_000,           0.100,   5_700,        5),
                new MarginTier(250_000,    1_000_000,         0.125,  11_950,        4),
                new MarginTier(1_000_000,  Double.POSITIVE_INFINITY,
                                                              0.250, 136_950,        1)));
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder("MarginTierTable[");
        for (MarginTier t : tiers) {
            sb.append(String.format("%.0f..%.0f:%.3f%%/-%.0f ",
                    t.notionalFloor(), t.notionalCap(), t.maintenanceMarginRate() * 100, t.maintenanceAmount()));
        }
        return sb.append(']').toString();
    }
}
