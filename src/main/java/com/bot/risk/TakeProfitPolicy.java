package com.bot.risk;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Where the exits sit, expressed in R — multiples of the distance from entry to stop — and never in
 * absolute prices. R is the only unit in which a target is comparable across symbols and volatility
 * regimes; "take profit at +2%" means something different on BTC and on a fresh listing, "take
 * profit at 2R" does not.
 *
 * <p>The default is the one the brief specifies: half the position at 1.5R, the rest at 2R.
 *
 * <p>Every leg produced here is a <b>reduce-only</b> close. That is not a preference — an exit order
 * that is not reduce-only will open a position in the opposite direction if it races a stop that has
 * already flattened the book.
 */
public record TakeProfitPolicy(List<Leg> legs) {

    /**
     * @param rMultiple          distance from entry in units of {@code |entry - stop|}
     * @param fractionOfPosition share of the filled quantity this leg closes
     */
    public record Leg(double rMultiple, double fractionOfPosition) {
        public Leg {
            Preconditions.positiveFinite(rMultiple, "rMultiple");
            Preconditions.require(fractionOfPosition > 0 && fractionOfPosition <= 1.0,
                    "fractionOfPosition must be in (0, 1], got " + fractionOfPosition);
        }
    }

    /** A projected leg: an actual price and an actual, lot-aligned quantity. */
    public record ProjectedLeg(double rMultiple, BigDecimal price, BigDecimal quantity) {}

    public TakeProfitPolicy {
        Preconditions.notNull(legs, "legs");
        Preconditions.require(!legs.isEmpty(), "at least one take-profit leg is required");
        double total = 0;
        double previousR = 0;
        for (Leg leg : legs) {
            Preconditions.require(leg.rMultiple() > previousR,
                    "take-profit legs must be ordered by strictly increasing R, got "
                            + leg.rMultiple() + " after " + previousR);
            previousR = leg.rMultiple();
            total += leg.fractionOfPosition();
        }
        Preconditions.require(total <= 1.0 + 1e-9,
                "take-profit fractions sum to " + total + ", which would close more than the position");
        legs = List.copyOf(legs);
    }

    /** Half at 1.5R, half at 2R — the brief's default. */
    public static TakeProfitPolicy standard() {
        return new TakeProfitPolicy(List.of(new Leg(1.5, 0.5), new Leg(2.0, 0.5)));
    }

    /** Single leg closing the whole position at {@code rMultiple}. */
    public static TakeProfitPolicy single(double rMultiple) {
        return new TakeProfitPolicy(List.of(new Leg(rMultiple, 1.0)));
    }

    /** Sum of all leg fractions; less than 1 leaves a runner with no target. */
    public double coveredFraction() {
        return legs.stream().mapToDouble(Leg::fractionOfPosition).sum();
    }

    /**
     * Turns the policy into orders that the exchange will actually accept for {@code totalQuantity}.
     *
     * <p>Three things happen here that a naive {@code qty * fraction} does not do:
     * <ol>
     *   <li>Each leg's quantity is floored to the lot step, and the <b>last</b> leg absorbs the
     *       rounding remainder, so the legs sum to exactly the quantity being closed rather than to
     *       "almost" it — an unclosed dust remainder is a position the bot believes is flat.</li>
     *   <li>If any leg comes out below the exchange's minimum lot, the split is abandoned as a whole
     *       rather than patched. Patching would mean pushing the unsendable share into a later leg,
     *       which quietly moves size <i>further out</i> — the opposite of what a risk layer should do
     *       when it cannot do what was asked.</li>
     *   <li>The fallback is a single leg at the <b>nearest</b> R holding the whole quantity.
     *       Collapsing towards the nearer target reduces exposure sooner, which is the direction to
     *       err in. If even that is unsendable the result is empty, and the stop is the only exit.</li>
     * </ol>
     *
     * <p>Minimum <i>notional</i> is deliberately not applied to these legs. Binance exempts
     * reduce-only orders from it — its own {@code -4164} message reads "unless you choose reduce
     * only" — and every leg produced here is reduce-only, so applying the floor would refuse exits
     * the exchange would have accepted.
     *
     * @param totalQuantity lot-aligned quantity actually held (the filled size, never the intended one)
     */
    public List<ProjectedLeg> project(Side side,
                                      double entryPrice,
                                      double stopPrice,
                                      BigDecimal totalQuantity,
                                      InstrumentFilters filters) {
        Preconditions.notNull(side, "side");
        Preconditions.positiveFinite(entryPrice, "entryPrice");
        Preconditions.positiveFinite(stopPrice, "stopPrice");
        Preconditions.notNull(totalQuantity, "totalQuantity");
        Preconditions.notNull(filters, "filters");
        Preconditions.require(totalQuantity.signum() > 0, "totalQuantity must be positive");

        double r = Math.abs(entryPrice - stopPrice);
        Preconditions.require(r > 0, "entry and stop are equal, so R is zero and no target is definable");

        double covered = coveredFraction();
        BigDecimal quantityToClose = totalQuantity
                .multiply(BigDecimal.valueOf(covered))
                .min(totalQuantity);
        BigDecimal alignedToClose = filters.quantizeQuantityDown(quantityToClose.doubleValue());
        if (alignedToClose.signum() <= 0) return List.of();

        List<ProjectedLeg> split = new ArrayList<>();
        BigDecimal remaining = alignedToClose;
        boolean splitIsSendable = true;

        for (int i = 0; i < legs.size() && splitIsSendable; i++) {
            Leg leg = legs.get(i);
            boolean last = i == legs.size() - 1;
            BigDecimal legPrice = priceFor(side, entryPrice, r, leg.rMultiple(), filters);

            BigDecimal legQty = last
                    ? remaining
                    : filters.quantizeQuantityDown(
                            alignedToClose.multiply(BigDecimal.valueOf(leg.fractionOfPosition() / covered))
                                    .doubleValue())
                        .min(remaining);

            if (!isSendable(legQty, filters)) {
                splitIsSendable = false;
                break;
            }
            split.add(new ProjectedLeg(leg.rMultiple(), legPrice, legQty));
            remaining = remaining.subtract(legQty);
        }

        if (splitIsSendable && remaining.signum() == 0) {
            return List.copyOf(split);
        }

        Leg nearest = legs.get(0);
        BigDecimal nearestPrice = priceFor(side, entryPrice, r, nearest.rMultiple(), filters);
        return isSendable(alignedToClose, filters)
                ? List.of(new ProjectedLeg(nearest.rMultiple(), nearestPrice, alignedToClose))
                : List.of();
    }

    private static BigDecimal priceFor(Side side, double entryPrice, double r, double rMultiple,
                                       InstrumentFilters filters) {
        double price = entryPrice + side.sign() * rMultiple * r;
        Preconditions.require(price > 0, "take-profit at " + rMultiple + "R would be at or below zero");
        return filters.quantizeTakeProfitPrice(side, price);
    }

    private static boolean isSendable(BigDecimal quantity, InstrumentFilters filters) {
        return quantity.signum() > 0 && filters.isQuantityInRange(quantity, true);
    }
}
