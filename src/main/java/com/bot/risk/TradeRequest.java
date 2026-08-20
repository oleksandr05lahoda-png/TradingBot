package com.bot.risk;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.util.OptionalDouble;

/**
 * What the risk engine is asked to approve, and all {@link RiskEngine#evaluate} depends on. Note the
 * absence of any size: quantity is derived from the stop and the balance, not negotiated.
 *
 * @param signalId            stable identity of the signal; the client order id derives from it
 * @param structuralStopPrice level that arrived with the signal, if any — preferred over ATR
 * @param leverage            requested leverage; capped, never used to size
 */
public record TradeRequest(
        String signalId,
        String symbol,
        Side side,
        double entryPrice,
        OptionalDouble structuralStopPrice,
        OptionalDouble atr,
        int leverage,
        InstrumentFilters filters,
        MarginTierTable marginTiers) {

    public TradeRequest {
        Preconditions.notBlank(signalId, "signalId");
        Preconditions.notBlank(symbol, "symbol");
        Preconditions.notNull(side, "side");
        Preconditions.positiveFinite(entryPrice, "entryPrice");
        Preconditions.notNull(structuralStopPrice, "structuralStopPrice");
        Preconditions.notNull(atr, "atr");
        Preconditions.positive(leverage, "leverage");
        Preconditions.notNull(filters, "filters");
        Preconditions.notNull(marginTiers, "marginTiers");
        Preconditions.require(symbol.equals(filters.symbol()),
                "filters are for " + filters.symbol() + " but the request is for " + symbol);
    }
}
