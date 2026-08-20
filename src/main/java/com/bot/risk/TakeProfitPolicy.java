package com.bot.risk;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Where the exits sit, in R (multiples of entry-to-stop), never absolute prices — "+2%" means
 * different things on BTC and on a fresh listing. Every leg is <b>reduce-only</b>: one that is not
 * would open an opposite position if it raced a stop that already flattened.
 */
public record TakeProfitPolicy(List<Leg> legs) {

    /** {@code rMultiple} is a distance from entry in units of {@code |entry - stop|}. */
    public record Leg(double rMultiple, double fractionOfPosition) {
        public Leg {
            Preconditions.positiveFinite(rMultiple, "rMultiple");
            Preconditions.require(fractionOfPosition > 0 && fractionOfPosition <= 1.0,
                    "fractionOfPosition must be in (0, 1], got " + fractionOfPosition);
        }
    }

    /** A leg with a real tick-aligned price and a real lot-aligned quantity. */
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

    /** Half at 1.5R, half at 2R. */
    public static TakeProfitPolicy standard() {
        return new TakeProfitPolicy(List.of(new Leg(1.5, 0.5), new Leg(2.0, 0.5)));
    }

    public static TakeProfitPolicy single(double rMultiple) {
        return new TakeProfitPolicy(List.of(new Leg(rMultiple, 1.0)));
    }

    /** Sum of all leg fractions; less than 1 leaves a runner with no target. */
    public double coveredFraction() {
        return legs.stream().mapToDouble(Leg::fractionOfPosition).sum();
    }

    /**
     * Orders the exchange will accept for {@code totalQuantity} (the filled size, never the intended
     * one), floored to the lot step with the <b>last</b> leg absorbing the remainder — dust left
     * unclosed is a position the bot believes is flat. A leg below the minimum lot abandons the split
     * rather than patching it, since patching moves size <i>further out</i>; the fallback is one leg
     * at the <b>nearest</b> R, and empty means the stop is the only exit. Minimum <i>notional</i> is
     * deliberately not applied — Binance exempts reduce-only orders ({@code -4164}), as all of these are.
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
