package com.bot.exec;

import com.bot.core.Preconditions;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * A one-way latch that stops new risk being taken, tripped by anything meaning the bot no longer
 * knows what is true. It stops <b>opening</b>, never closing — the close path never consults it, so
 * a halt can never seal a losing position in. Clearing it is an explicit operator act.
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

    /** Explicit operator reset. Deliberately not called from anywhere else in this codebase. */
    public void clear() {
        State previous = state.getAndSet(null);
        if (previous != null) {
            LOG.warning("[TradingHalt] cleared by operator; previous halt was: " + previous.reason());
        }
    }
}
