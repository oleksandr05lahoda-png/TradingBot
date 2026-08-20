package com.bot.signal;

import java.util.List;

/**
 * The <b>only</b> way a trade idea enters this system. A source transports a decision made
 * elsewhere — it never computes an indicator, reads a candle or picks a direction. Exactly two
 * implementations, no third; {@code SignalSourceImplementationsTest} enforces it.
 */
public interface SignalSource extends AutoCloseable {

    String name();

    /**
     * Signals available now, oldest first; never blocks indefinitely. Fail-closed: an unreachable
     * backing store must throw, because empty is indistinguishable from "no signals".
     */
    List<Signal> poll() throws Exception;

    /** Closes, oldest first. Drained <b>before</b> {@link #poll()}: giving risk back precedes taking more. */
    default List<CloseRequest> pollCloses() throws Exception { return List.of(); }

    default void onClosed(CloseRequest request, ExecutionFeedback feedback) throws Exception {}

    default void onAccepted(Signal signal, ExecutionFeedback feedback) throws Exception {}

    /** Called when the risk gate refused the signal, with its reason. */
    default void onRejected(Signal signal, String reason) throws Exception {}

    @Override default void close() throws Exception {}
}
