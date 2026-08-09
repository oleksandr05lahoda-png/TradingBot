package com.bot.exec;

import com.bot.core.Preconditions;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * A one-way latch that stops new risk being taken.
 *
 * <p>Tripped by anything meaning the bot no longer knows what is true: reconciliation drift, a fired
 * dead-man's switch, an unestablishable order. Clearing it is an explicit operator act —
 * {@link #clear()} is never called from a retry or a timer, because a halt that heals itself hides
 * what it was raised about.
 *
 * <p>The asymmetry matters, and the previous generation got it wrong: a halt stops <b>opening</b>,
 * never closing. A lock that seals positions in is not a safety feature, it is a way to be unable to
 * exit a losing trade — so the close path never consults this latch.
 */
public final class TradingHalt {

    private static final Logger LOG = Logger.getLogger(TradingHalt.class.getName());

    private record State(String reason, Instant at) {}

    private final AtomicReference<State> state = new AtomicReference<>();

    /** Stops new positions. Repeated calls keep the first reason — the earliest cause is the useful one. */
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

    public Optional<Instant> haltedAt() {
        State s = state.get();
        return s == null ? Optional.empty() : Optional.of(s.at());
    }

    /** Explicit operator reset. Deliberately not called from anywhere else in this codebase. */
    public void clear() {
        State previous = state.getAndSet(null);
        if (previous != null) {
            LOG.warning("[TradingHalt] cleared by operator; previous halt was: " + previous.reason());
        }
    }
}
