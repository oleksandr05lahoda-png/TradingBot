package com.bot.risk;

import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.util.OptionalDouble;

/**
 * A stop price together with where it came from. This type is the reason a position without a stop
 * cannot be constructed: {@link TradePlan} takes a {@code StopLoss}, there is no overload without
 * one, and every implementation validates its own geometry, so an "empty" stop cannot be smuggled
 * in as a null, a zero or a NaN.
 *
 * <p>Two origins, no more: {@link Structural}, a level that arrived with the signal (preferred —
 * something outside the bot picked it), and {@link AtrFallback}, a volatility distance used only
 * when no structural level came. The fallback is not a strategy: it decides no direction, no entry
 * and not whether to trade.
 */
public sealed interface StopLoss permits StopLoss.Structural, StopLoss.AtrFallback {

    /** The stop price. Always finite, positive, and on the losing side of the entry. */
    double price();

    /** The position direction this stop was validated against. */
    Side side();

    /** The entry price this stop was validated against. */
    double entryPrice();

    /** Short, loggable description of where the level came from. */
    String origin();

    /** Distance from entry to stop, in quote currency per unit of base. Always positive. */
    default double distance() {
        return Math.abs(entryPrice() - price());
    }

    /** Distance as a fraction of the entry price. */
    default double distanceFraction() {
        return distance() / entryPrice();
    }

    // ─── Factories ───────────────────────────────────────────────────────────────────────────

    /** A level supplied by the signal. */
    static StopLoss structural(Side side, double entryPrice, double stopPrice) {
        return new Structural(side, entryPrice, stopPrice);
    }

    /**
     * A volatility fallback: {@code entry - sign * atr * multiplier}. Used only when the signal
     * carried no structural level.
     */
    static StopLoss atrFallback(Side side, double entryPrice, double atr, double multiplier) {
        return new AtrFallback(side, entryPrice, atr, multiplier);
    }

    /**
     * Picks the structural level when the signal supplied a usable one, otherwise the ATR fallback.
     *
     * <p>"Usable" means it passes the same geometry validation as any other stop. A structural level
     * on the wrong side of the entry is not silently repaired and not silently swapped for the ATR
     * stop — it throws, because a signal that says "LONG at 100, stop at 105" is not a stop that
     * needs fixing, it is a producer that is broken and must be found.
     *
     * @throws IllegalArgumentException if neither a structural level nor an ATR value is present
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

    // ─── Implementations ─────────────────────────────────────────────────────────────────────

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
            // The canonical constructor is reachable directly, so the stored price is re-derived
            // rather than trusted: an "ATR stop" whose price is not the ATR distance is a lie the
            // audit trail would repeat.
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
