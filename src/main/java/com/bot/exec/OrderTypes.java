package com.bot.exec;

import com.bot.core.Side;

/**
 * The exchange-facing vocabulary of an order. {@link OrderSide} is deliberately not
 * {@link com.bot.core.Side}: one enum for both eventually closes a long by buying more of it.
 */
public final class OrderTypes {

    private OrderTypes() {}

    public enum OrderSide {
        BUY, SELL;

        public static OrderSide toOpen(Side direction) {
            return direction == Side.LONG ? BUY : SELL;
        }

        public static OrderSide toClose(Side direction) {
            return direction == Side.LONG ? SELL : BUY;
        }
    }

    /** The subset of Binance USDⓈ-M order types this system uses. */
    public enum OrderType {
        MARKET,
        LIMIT,
        /** Conditional market order — the protective stop. Triggers on {@link WorkingType}. */
        STOP_MARKET,
        /** Conditional market order on the winning side — the reduce-only exits. */
        TAKE_PROFIT_MARKET;

        /**
         * Since December 2025 Binance refuses these on {@code POST /fapi/v1/order} with {@code -4120}
         * and requires {@code /fapi/v1/algoOrder}: different parameter names, separate id space.
         */
        public boolean isConditional() {
            return this == STOP_MARKET || this == TAKE_PROFIT_MARKET;
        }
    }

    public enum TimeInForce {
        GTC,
        IOC
    }

    /**
     * {@link #MARK_PRICE} is the deliberate default for stops — liquidation is decided on the mark
     * price, while {@link #CONTRACT_PRICE} follows last traded price and wicks far more on a thin book.
     */
    public enum WorkingType {
        MARK_PRICE, CONTRACT_PRICE
    }

    /** Order lifecycle. An HTTP 200 only means accepted, which is {@link #NEW}, not filled. */
    public enum OrderState {
        NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED, UNKNOWN;

        public boolean isTerminal() {
            return this == FILLED || this == CANCELED || this == REJECTED || this == EXPIRED;
        }

        public boolean isWorking() {
            return this == NEW || this == PARTIALLY_FILLED;
        }
    }

    /** Drives the deterministic client order id and the "anything but an entry only reduces" rule. */
    public enum OrderPurpose {
        ENTRY(false),
        STOP_LOSS(true),
        TAKE_PROFIT(true),
        EMERGENCY_CLOSE(true);

        private final boolean closing;

        OrderPurpose(boolean closing) { this.closing = closing; }

        public boolean isClosing() { return closing; }
    }
}
