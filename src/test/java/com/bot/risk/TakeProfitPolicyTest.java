package com.bot.risk;

import com.bot.core.InstrumentFilters;
import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exits expressed in R, and the lot arithmetic that decides whether they can be sent at all. */
class TakeProfitPolicyTest {

    private final InstrumentFilters filters = RiskFixtures.btcFilters();

    @Test
    @DisplayName("the default is half at 1.5R and half at 2R")
    void defaultPolicyIsTheBriefsDefault() {
        TakeProfitPolicy policy = TakeProfitPolicy.standard();
        assertEquals(2, policy.legs().size());
        assertEquals(1.5, policy.legs().get(0).rMultiple());
        assertEquals(0.5, policy.legs().get(0).fractionOfPosition());
        assertEquals(2.0, policy.legs().get(1).rMultiple());
        assertEquals(1.0, policy.coveredFraction(), 1e-12);
    }

    @Test
    @DisplayName("R multiples become prices measured from the entry-to-stop distance")
    void rMultiplesBecomePrices() {
        // entry 64,000, stop 62,800 -> R = 1,200. 1.5R = 65,800, 2R = 66,400.
        List<TakeProfitPolicy.ProjectedLeg> legs = TakeProfitPolicy.standard()
                .project(Side.LONG, 64_000, 62_800, new BigDecimal("1.000"), filters);

        assertEquals(2, legs.size());
        assertEquals(0, legs.get(0).price().compareTo(new BigDecimal("65800")));
        assertEquals(0, legs.get(1).price().compareTo(new BigDecimal("66400")));
    }

    @Test
    @DisplayName("a short's targets sit below the entry")
    void shortTargetsAreBelowEntry() {
        List<TakeProfitPolicy.ProjectedLeg> legs = TakeProfitPolicy.standard()
                .project(Side.SHORT, 64_000, 65_200, new BigDecimal("1.000"), filters);
        assertEquals(0, legs.get(0).price().compareTo(new BigDecimal("62200")));   // 64,000 - 1.5 * 1,200
        assertEquals(0, legs.get(1).price().compareTo(new BigDecimal("61600")));   // 64,000 - 2 * 1,200
    }

    @Test
    @DisplayName("the legs sum to exactly the position, with the remainder on the last one")
    void legsSumToThePosition() {
        // 0.003 split 50/50 is 0.0015 each, which is not a multiple of the 0.001 lot step.
        BigDecimal position = new BigDecimal("0.003");
        List<TakeProfitPolicy.ProjectedLeg> legs = TakeProfitPolicy.standard()
                .project(Side.LONG, 64_000, 62_800, position, filters);

        BigDecimal total = legs.stream()
                .map(TakeProfitPolicy.ProjectedLeg::quantity)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        assertEquals(0, total.compareTo(position),
                "the exits must close the whole position; dust left over is a position the bot thinks is flat");
        assertEquals(0, legs.get(0).quantity().compareTo(new BigDecimal("0.001")));
        assertEquals(0, legs.get(1).quantity().compareTo(new BigDecimal("0.002")));
    }

    @Test
    @DisplayName("a position too small to split collapses to one leg at the nearer target")
    void tooSmallToSplitCollapsesToTheNearerTarget() {
        List<TakeProfitPolicy.ProjectedLeg> legs = TakeProfitPolicy.standard()
                .project(Side.LONG, 64_000, 62_800, new BigDecimal("0.001"), filters);

        assertEquals(1, legs.size());
        assertEquals(1.5, legs.get(0).rMultiple(),
                "collapsing towards the nearer target reduces exposure sooner, which is the direction "
                        + "a risk layer should err in");
        assertEquals(0, legs.get(0).quantity().compareTo(new BigDecimal("0.001")));
    }

    @Test
    @DisplayName("a policy covering less than the whole position leaves a runner")
    void partialCoverageLeavesARunner() {
        TakeProfitPolicy policy = new TakeProfitPolicy(List.of(new TakeProfitPolicy.Leg(2.0, 0.5)));
        List<TakeProfitPolicy.ProjectedLeg> legs =
                policy.project(Side.LONG, 64_000, 62_800, new BigDecimal("0.100"), filters);

        assertEquals(1, legs.size());
        assertEquals(0, legs.get(0).quantity().compareTo(new BigDecimal("0.050")),
                "half the position should be covered, the rest rides to the stop");
    }

    @Test
    @DisplayName("legs must be ordered by strictly increasing R")
    void legsMustBeOrdered() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new TakeProfitPolicy(List.of(
                        new TakeProfitPolicy.Leg(2.0, 0.5),
                        new TakeProfitPolicy.Leg(1.5, 0.5))));
        assertTrue(thrown.getMessage().contains("increasing R"), thrown.getMessage());
    }

    @Test
    @DisplayName("fractions summing above 1 would close more than the position and are refused")
    void fractionsCannotExceedThePosition() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new TakeProfitPolicy(List.of(
                        new TakeProfitPolicy.Leg(1.5, 0.6),
                        new TakeProfitPolicy.Leg(2.0, 0.6))));
        assertTrue(thrown.getMessage().contains("more than the position"), thrown.getMessage());
    }

    @Test
    @DisplayName("no leg is emitted when even the whole position is below the exchange's minimum lot")
    void nothingIsEmittedWhenTheLotIsUnsendable() {
        // minQty 0.01 with a 0.001 step: a 0.005 position cannot be closed by an order at all.
        InstrumentFilters coarseMinimum = new InstrumentFilters("ODDUSDT",
                new BigDecimal("0.01"), new BigDecimal("0.01"), new BigDecimal("1000000"),
                new BigDecimal("0.001"), new BigDecimal("0.010"), new BigDecimal("1000"),
                new BigDecimal("1000"), new BigDecimal("5"), 2, 3);

        List<TakeProfitPolicy.ProjectedLeg> legs = TakeProfitPolicy.standard()
                .project(Side.LONG, 100, 98, new BigDecimal("0.005"), coarseMinimum);
        assertTrue(legs.isEmpty(),
                "an unsendable leg must not be produced — it would be rejected by the exchange while "
                        + "the code believed an exit was in place");
    }

    @Test
    @DisplayName("minimum notional is not applied to exits: the exchange exempts reduce-only orders")
    void minNotionalDoesNotBlockASmallReduceOnlyExit() {
        // 0.001 at 103 is a notional of about $0.10, far under the $5 floor — and still sendable,
        // because Binance exempts reduce-only orders from MIN_NOTIONAL.
        InstrumentFilters tiny = InstrumentFilters.of("TINYUSDT", "0.01", "0.001", "5");
        List<TakeProfitPolicy.ProjectedLeg> legs = TakeProfitPolicy.standard()
                .project(Side.LONG, 100, 98, new BigDecimal("0.001"), tiny);
        assertEquals(1, legs.size());
        assertEquals(1.5, legs.get(0).rMultiple());
    }
}
