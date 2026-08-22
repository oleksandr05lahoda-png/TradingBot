package com.bot.exec;

import com.bot.core.Preconditions;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * One-way latch that stops new risk being taken. It stops <b>opening</b>, never closing — the close
 * path never consults it, so a halt can never seal a losing position in. Only an operator clears it.
 */
public final class TradingHalt {

    private static final Logger LOG = Logger.getLogger(TradingHalt.class.getName());

    private record State(String reason, Instant at) {}

    private final AtomicReference<State> state = new AtomicReference<>();

    /** Stops new positions. Repeated calls keep the first reason. */
    public void halt(String reason, Instant at) {
        Preconditions.notBlank(reason, "reason");
        Preconditions.notNull(at, "at");
        if (state.compareAndSet(null, new State(reason, at))) {
            LOG.severe("[TradingHalt] HALTED at " + at + ": " + reason
                    + " — no new positions will be opened until an operator clears this");
        }
    }

    public boolean isHalted() { return state.get() != null; }

    public Optional<String> reason() {
        State s = state.get();
        return s == null ? Optional.empty() : Optional.of(s.reason());
    }

    /**
     * Explicit operator reset — the Telegram {@code /resume} command is its only caller. The
     * "HALT CLEARED at" wording is read by the scanner, which otherwise stands down on the
     * "HALTED at" line for the rest of the process's life.
     */
    public void clear() {
        State previous = state.getAndSet(null);
        if (previous != null) {
            LOG.warning("[TradingHalt] HALT CLEARED at " + Instant.now()
                    + " by operator; previous halt was: " + previous.reason());
        }
    }
}
