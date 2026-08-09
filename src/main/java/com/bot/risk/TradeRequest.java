package com.bot.risk;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.core.Side;

import java.util.OptionalDouble;

/**
 * What the risk engine is asked to approve. Everything the decision depends on is in here — there is
 * no ambient state, no lookup, and nothing fetched during evaluation, which is what makes
 * {@link RiskEngine#evaluate} reproducible from a log line.
 *
 * <p>Note what is <b>absent</b>: any notion of size. A caller cannot ask for a quantity, because the
 * quantity is derived from the stop and the balance and is not open to negotiation.
 *
 * @param signalId            stable identity of the originating signal; the client order id derives from it
 * @param structuralStopPrice level that arrived with the signal, if any — preferred over ATR
 * @param atr                 volatility used for the fallback stop when no structural level arrived
 * @param leverage            requested leverage; capped, never used to size
 * @param filters             the symbol's live exchange filters
 * @param marginTiers         the symbol's live maintenance-margin brackets
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
