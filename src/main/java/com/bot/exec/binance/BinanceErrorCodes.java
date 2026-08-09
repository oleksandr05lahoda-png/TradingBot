package com.bot.exec.binance;

/**
 * Binance USDⓈ-M futures error codes this system reacts to, taken from the published error-code
 * reference rather than from memory.
 *
 * <p>Only codes that change behaviour are listed. A code that is merely logged does not need a name,
 * and a list of every code the API can emit would rot faster than it would help.
 */
public final class BinanceErrorCodes {

    private BinanceErrorCodes() {}

    /** {@code -1021} Timestamp outside {@code recvWindow}: the local clock has drifted. */
    public static final int TIMESTAMP_OUT_OF_RECV_WINDOW = -1021;

    /** {@code -1111} Precision is over the maximum defined for this asset. */
    public static final int BAD_PRECISION = -1111;

    /** {@code -2010} New order rejected. */
    public static final int NEW_ORDER_REJECTED = -2010;

    /** {@code -2011} Cancel rejected because the order was not in the book. */
    public static final int CANCEL_REJECTED_UNKNOWN_ORDER = -2011;

    /** {@code -2013} Order does not exist. Returned when querying an id the exchange never saw. */
    public static final int NO_SUCH_ORDER = -2013;

    /** {@code -2019} Margin is insufficient. */
    public static final int MARGIN_NOT_SUFFICIENT = -2019;

    /** {@code -2022} ReduceOnly order is rejected — usually because the position is already flat. */
    public static final int REDUCE_ONLY_REJECT = -2022;

    /**
     * {@code -4116} clientOrderId is duplicated. The single most useful code in this file: it is the
     * exchange enforcing idempotency on the bot's behalf, and it means the order already exists.
     */
    public static final int DUPLICATED_CLIENT_ORDER_ID = -4116;

    /** {@code -4118} ReduceOnly order failed — check the existing position and open orders. */
    public static final int REDUCE_ONLY_MARGIN_CHECK_FAILED = -4118;

    /** {@code -4164} Order notional is below the symbol's minimum. */
    public static final int MIN_NOTIONAL = -4164;

    /** {@code -4046} No need to change margin type — it is already what was asked for. */
    public static final int NO_NEED_TO_CHANGE_MARGIN_TYPE = -4046;

    /**
     * {@code -4028} Leverage is not valid.
     *
     * <p>Named here so it is not mistaken for a benign "already set" code, which is what an earlier
     * version of this file called it. It is a genuine refusal — the requested leverage is outside
     * what the exchange permits — and it must propagate, because a position opened at a leverage
     * other than the one the liquidation buffer was computed for is a position whose stop was
     * validated against the wrong liquidation price.
     */
    public static final int INVALID_LEVERAGE = -4028;

    /**
     * True when the code means "the state you asked for is already the state" — a success, not a
     * failure. Only the margin-type code qualifies: Binance answers a redundant leverage change with
     * HTTP 200, so there is no leverage equivalent to swallow.
     */
    public static boolean isBenignAlreadyInDesiredState(int code) {
        return code == NO_NEED_TO_CHANGE_MARGIN_TYPE;
    }

    /** True when the code means the order simply is not there — cancelling it is a no-op, not an error. */
    public static boolean isOrderAbsent(int code) {
        return code == NO_SUCH_ORDER || code == CANCEL_REJECTED_UNKNOWN_ORDER;
    }
}
