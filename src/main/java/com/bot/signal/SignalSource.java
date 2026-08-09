package com.bot.signal;

import java.util.List;

/**
 * The <b>only</b> way a trade idea enters this system.
 *
 * <p>There are exactly two implementations and there is no third:
 * {@link ManualTestnetInput} (an operator typing) and {@link SupabaseQueueSource} (an external
 * queue). No implementation of this interface computes an indicator, reads a candle or decides a
 * direction — a source transports a decision that was made elsewhere. If a future implementation
 * would need market data to answer {@link #poll()}, it is a strategy wearing this interface as a
 * costume, and it does not belong here.
 *
 * <p>Why the constraint is structural rather than a comment: with sizing, stops, liquidation and
 * exposure all downstream of the gate, a strategy added here would inherit every safety property
 * automatically and would look, to the rest of the system, exactly like an operator typing. That is
 * precisely the disguise this project cannot afford, because no strategy in it has ever passed its
 * own validation gates.
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

    /** Called once the gate approved a signal and an order carrying {@code clientOrderId} was sent. */
    default void onAccepted(Signal signal, String clientOrderId) throws Exception {}

    /** Called when the gate refused a signal, with the reason. */
    default void onRejected(Signal signal, String reason) throws Exception {}

    @Override default void close() throws Exception {}
}
