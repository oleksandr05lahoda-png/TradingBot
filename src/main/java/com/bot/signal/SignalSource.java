package com.bot.signal;

import java.util.List;

/**
 * The <b>only</b> way a trade idea enters this system. A source transports a decision made
 * elsewhere; it does not compute an indicator, read a candle or pick a direction — one that needed
 * market data to answer {@link #poll()} would be a strategy wearing this interface as a costume.
 * Exactly two implementations, no third, and {@code SignalSourceImplementationsTest} enforces it.
 */
public interface SignalSource extends AutoCloseable {

    /** Short name for logs and for the boot banner. */
    String name();

    /**
     * Signals available right now, oldest first; never blocks indefinitely. Must be fail-closed: a
     * source that cannot reach its backing store throws, because returning empty would be
     * indistinguishable from "no signals".
     */
    List<Signal> poll() throws Exception;

    /**
     * Instructions to close positions, oldest first. Drained <b>before</b> {@link #poll()} on every
     * cycle, so giving risk back always takes precedence over taking more.
     */
    default List<CloseRequest> pollCloses() throws Exception { return List.of(); }

    /** Called once a close has been executed, with what the exchange actually did. */
    default void onClosed(CloseRequest request, ExecutionFeedback feedback) throws Exception {}

    /** Called once a signal became a position, with what the exchange actually did. */
    default void onAccepted(Signal signal, ExecutionFeedback feedback) throws Exception {}

    /** Called when the gate refused a signal, with the reason. */
    default void onRejected(Signal signal, String reason) throws Exception {}

    @Override default void close() throws Exception {}
}
