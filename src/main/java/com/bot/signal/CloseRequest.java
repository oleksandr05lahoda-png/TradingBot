package com.bot.signal;

import com.bot.core.Preconditions;

import java.time.Instant;

/**
 * An instruction to close whatever is open on a symbol.
 *
 * <p>Separate from {@link Signal} on purpose. A signal is a request to take risk and goes through
 * the whole gate — sizing, stops, liquidation buffer, exposure caps. A close only ever gives risk
 * back, so it goes through none of them, and in particular it is <b>not</b> blocked by a trading
 * halt. A halt that sealed positions in would not be a safety feature; the previous generation of
 * this codebase made exactly that mistake, and time-stops silently stopped working for weeks.
 *
 * @param id     stable identity; the client order id of the closing order derives from it
 * @param symbol the symbol to flatten
 * @param reason why, for the audit trail — a time stop, an operator, a kill switch
 */
public record CloseRequest(String id, String symbol, String reason, Instant createdAt) {

    public CloseRequest {
        Preconditions.notBlank(id, "id");
        Preconditions.notBlank(symbol, "symbol");
        Preconditions.notNull(reason, "reason");
        Preconditions.notNull(createdAt, "createdAt");
    }

    @Override public String toString() {
        return "CloseRequest[" + id + " " + symbol + " — " + reason + "]";
    }
}
