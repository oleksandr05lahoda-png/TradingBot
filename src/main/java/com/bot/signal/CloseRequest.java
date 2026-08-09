package com.bot.signal;

import com.bot.core.Preconditions;

import java.time.Instant;

/**
 * An instruction to close whatever is open on a symbol; its id derives the closing order's client
 * order id. Separate from {@link Signal} because a close only gives risk back: it skips the risk
 * gate entirely and is <b>not</b> blocked by a trading halt, which would otherwise seal positions
 * in and silently break time stops.
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
