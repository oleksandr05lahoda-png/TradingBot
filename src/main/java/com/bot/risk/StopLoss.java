package com.bot.risk;

import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.util.OptionalDouble;

/**
 * A stop price and where it came from. {@link TradePlan} has no overload without one and every
 * implementation validates its own geometry, so a position without a stop cannot be constructed.
 * {@link Structural} (from the signal) is preferred; {@link AtrFallback} only when none came.
 */
public sealed interface StopLoss permits StopLoss.Structural, StopLoss.AtrFallback {

    /** Always finite, positive and on the losing side of the entry. */
    double price();

    Side side();

    double entryPrice();

    String origin();

    /** Entry to stop, in quote currency per unit of base. */
    default double distance() {
        return Math.abs(entryPrice() - price());
    }

    default double distanceFraction() {
        return distance() / entryPrice();
    }

    static StopLoss structural(Side side, double entryPrice, double stopPrice) {
        return new Structural(side, entryPrice, stopPrice);
    }

    /** A volatility fallback: {@code entry - sign * atr * multiplier}. */
    static StopLoss atrFallback(Side side, double entryPrice, double atr, double multiplier) {
        return new AtrFallback(side, entryPrice, atr, multiplier);
    }

    /**
     * The structural level if the signal supplied one, else the ATR fallback, else throws. A level on
     * the wrong side of entry throws rather than falling back — that is a broken signal producer.
     */
    static StopLoss resolve(Side side,
                            double entryPrice,
                            OptionalDouble structuralStopPrice,
                            OptionalDouble atr,
                            double atrMultiplier) {
        Preconditions.notNull(structuralStopPrice, "structuralStopPrice");
        Preconditions.notNull(atr, "atr");
        if (structuralStopPrice.isPresent()) {
            return structural(side, entryPrice, structuralStopPrice.getAsDouble());
        }
        if (atr.isPresent()) {
            return atrFallback(side, entryPrice, atr.getAsDouble(), atrMultiplier);
        }
        throw new IllegalArgumentException(
                "no stop available: the signal carried neither a structural stop nor an ATR value. "
                        + "A position without a stop is not a position this system can hold.");
    }

    record Structural(Side side, double entryPrice, double price) implements StopLoss {
        public Structural {
            Preconditions.notNull(side, "side");
            Preconditions.positiveFinite(entryPrice, "entryPrice");
            Preconditions.positiveFinite(price, "stopPrice");
            Preconditions.require(side.isValidStopGeometry(entryPrice, price),
                    "structural stop " + price + " is on the wrong side of entry " + entryPrice
                            + " for a " + side + " position");
        }

        @Override public String origin() { return "structural"; }
    }

    record AtrFallback(Side side, double entryPrice, double atr, double multiplier, double price)
            implements StopLoss {

        AtrFallback(Side side, double entryPrice, double atr, double multiplier) {
            this(side, entryPrice, atr, multiplier, computePrice(side, entryPrice, atr, multiplier));
        }

        public AtrFallback {
            Preconditions.notNull(side, "side");
            Preconditions.positiveFinite(entryPrice, "entryPrice");
            Preconditions.positiveFinite(atr, "atr");
            Preconditions.positiveFinite(multiplier, "atrMultiplier");
            Preconditions.positiveFinite(price, "derived stop price");
            Preconditions.require(side.isValidStopGeometry(entryPrice, price),
                    "ATR fallback stop " + price + " is on the wrong side of entry " + entryPrice
                            + " for a " + side + " position");
            // Canonical ctor is reachable directly, so re-derive: a mislabelled "ATR stop" misleads the log.
            double expected = computePrice(side, entryPrice, atr, multiplier);
            Preconditions.require(Math.abs(price - expected) <= 1e-9 * Math.max(1.0, expected),
                    "ATR fallback price " + price + " does not match the ATR distance " + expected);
        }

        private static double computePrice(Side side, double entryPrice, double atr, double multiplier) {
            Preconditions.notNull(side, "side");
            double p = entryPrice - side.sign() * atr * multiplier;
            Preconditions.require(p > 0,
                    "ATR fallback would put the stop at or below zero (entry=" + entryPrice
                            + ", atr=" + atr + ", multiplier=" + multiplier + ")");
            return p;
        }

        @Override public String origin() {
            return String.format("atr(%.6g x%.2f)", atr, multiplier);
        }
    }
}
