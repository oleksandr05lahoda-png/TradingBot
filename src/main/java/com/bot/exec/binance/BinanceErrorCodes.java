package com.bot.exec.binance;

/** Binance USDⓈ-M futures error codes the adapter reacts to, from the published reference. */
public final class BinanceErrorCodes {

    private BinanceErrorCodes() {}

    /** Timestamp outside {@code recvWindow}: the local clock has drifted. */
    public static final int TIMESTAMP_OUT_OF_RECV_WINDOW = -1021;

    public static final int BAD_PRECISION = -1111;

    public static final int NEW_ORDER_REJECTED = -2010;

    /** Cancel rejected: the order was not in the book. */
    public static final int CANCEL_REJECTED_UNKNOWN_ORDER = -2011;

    /** Returned when querying an id the exchange never saw. */
    public static final int NO_SUCH_ORDER = -2013;

    public static final int MARGIN_NOT_SUFFICIENT = -2019;

    /** ReduceOnly rejected — usually because the position is already flat. */
    public static final int REDUCE_ONLY_REJECT = -2022;

    /** Duplicated clientOrderId, i.e. the order already exists. */
    public static final int DUPLICATED_CLIENT_ORDER_ID = -4116;

    /** ReduceOnly failed the margin check — inspect the existing position and open orders. */
    public static final int REDUCE_ONLY_MARGIN_CHECK_FAILED = -4118;

    /** Trigger order sent to the plain endpoint; conditionals moved to algoOrder in December 2025. */
    public static final int ORDER_TYPE_NEEDS_ALGO_ENDPOINT = -4120;

    public static final int MIN_NOTIONAL = -4164;

    /** Margin type is already what was asked for. */
    public static final int NO_NEED_TO_CHANGE_MARGIN_TYPE = -4046;

    /** A refusal, not "already set": it must propagate, or the liq buffer assumed other leverage. */
    public static final int INVALID_LEVERAGE = -4028;

    /** Only margin type qualifies; Binance answers a redundant leverage change with HTTP 200. */
    public static boolean isBenignAlreadyInDesiredState(int code) {
        return code == NO_NEED_TO_CHANGE_MARGIN_TYPE;
    }

    /** Order is not there — cancelling it is a no-op, not an error. */
    public static boolean isOrderAbsent(int code) {
        return code == NO_SUCH_ORDER || code == CANCEL_REJECTED_UNKNOWN_ORDER;
    }
}
