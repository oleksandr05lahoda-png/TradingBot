package com.bot.signal;

import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.risk.RiskConstants;

import java.time.Instant;
import java.util.OptionalDouble;

/**
 * An instruction to consider a trade, from outside this system.
 *
 * <p>What a signal is allowed to carry is deliberately narrow: a symbol, a direction, an entry, an
 * optional structural stop, an optional volatility figure and a requested leverage. It carries no
 * size — size is derived from the stop by {@link com.bot.risk.PositionSizer} and is not negotiable —
 * and no confidence, score or urgency, because nothing downstream would be allowed to act on those.
 *
 * <p>A signal is a <i>request</i>, not a decision. {@link com.bot.risk.RiskEngine} decides.
 *
 * @param id                   stable identity; the client order id is derived from it, so the same
 *                             signal replayed after a crash produces the same order rather than a second one
 * @param structuralStopPrice  a level the producer chose; preferred over the ATR fallback
 * @param atr                  volatility for the fallback stop when no structural level was supplied
 */
public record Signal(
        String id,
        String symbol,
        Side side,
        double entryPrice,
        OptionalDouble structuralStopPrice,
        OptionalDouble atr,
        int leverage,
        Instant createdAt) {

    public Signal {
        Preconditions.notBlank(id, "id");
        Preconditions.notBlank(symbol, "symbol");
        Preconditions.notNull(side, "side");
        Preconditions.positiveFinite(entryPrice, "entryPrice");
        Preconditions.notNull(structuralStopPrice, "structuralStopPrice");
        Preconditions.notNull(atr, "atr");
        Preconditions.positive(leverage, "leverage");
        Preconditions.require(leverage <= RiskConstants.MAX_LEVERAGE,
                "signal asks for " + leverage + "x, above the hard cap " + RiskConstants.MAX_LEVERAGE + "x");
        Preconditions.notNull(createdAt, "createdAt");
        if (structuralStopPrice.isPresent()) {
            Preconditions.positiveFinite(structuralStopPrice.getAsDouble(), "structuralStopPrice");
        }
        if (atr.isPresent()) {
            Preconditions.positiveFinite(atr.getAsDouble(), "atr");
        }
        Preconditions.require(structuralStopPrice.isPresent() || atr.isPresent(),
                "signal " + id + " carries neither a structural stop nor an ATR: there is nothing to "
                        + "size from, and a position without a stop is not one this system can hold");
    }

    public boolean hasStructuralStop() { return structuralStopPrice.isPresent(); }

    @Override public String toString() {
        return String.format("Signal[%s %s %s entry=%.8g stop=%s atr=%s lev=%dx]",
                id, side, symbol, entryPrice,
                structuralStopPrice.isPresent() ? String.format("%.8g", structuralStopPrice.getAsDouble()) : "-",
                atr.isPresent() ? String.format("%.8g", atr.getAsDouble()) : "-",
                leverage);
    }
}
