package com.bot.signal;

import java.util.List;

/**
 * The <b>only</b> way a trade idea enters this system.
 *
 * <p>Exactly two implementations, no third: {@link ManualTestnetInput} (an operator typing) and
 * {@link SupabaseQueueSource} (an external queue). A source transports a decision made elsewhere; it
 * does not compute an indicator, read a candle or pick a direction. One that needed market data to
 * answer {@link #poll()} would be a strategy wearing this interface as a costume — and since sizing,
 * stops and exposure all sit downstream, it would inherit every safety property and look exactly
 * like an operator typing. {@code SignalSourceImplementationsTest} enforces the count.
 */
public interface SignalSource extends AutoCloseable {

    /** Short name for logs and for the boot banner. */
    String name();

    /**
     * Returns the signals available right now, oldest first, and never blocks indefinitely.
     *
     * <p>Implementations must be fail-closed: a source that cannot reach its backing store throws.
     * Returning an empty list on failure would be indistinguishable from "no signals", and "the
     * queue looked empty" is not a thing a trading loop should ever conclude by accident.
     */
    List<Signal> poll() throws Exception;

    /**
     * Called once a signal became a position, carrying what the exchange actually did. The feedback
     * is the only place the realised execution cost is recorded — see {@link ExecutionFeedback}.
     */
    default void onAccepted(Signal signal, ExecutionFeedback feedback) throws Exception {}

    /** Called when the gate refused a signal, with the reason. */
    default void onRejected(Signal signal, String reason) throws Exception {}

    @Override default void close() throws Exception {}
}
