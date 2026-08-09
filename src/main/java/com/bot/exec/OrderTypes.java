package com.bot.exec;

import com.bot.core.Side;

/**
 * The exchange-facing vocabulary of an order. {@link OrderSide} is deliberately a different type
 * from {@link com.bot.core.Side}: a LONG is opened with BUY and closed with SELL, and one enum for
 * both eventually closes a long by buying more of it with no complaint from the compiler.
 */
public final class OrderTypes {

    private OrderTypes() {}

    public enum OrderSide {
        BUY, SELL;

        /** The side that opens a position in {@code direction}. */
        public static OrderSide toOpen(Side direction) {
            return direction == Side.LONG ? BUY : SELL;
        }

        /** The side that reduces or closes a position in {@code direction}. */
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
         * True for trigger order types. Since December 2025 Binance refuses these on
         * {@code POST /fapi/v1/order} with {@code -4120} and requires {@code /fapi/v1/algoOrder},
         * which uses different parameter names and a separate id space.
         */
        public boolean isConditional() {
            return this == STOP_MARKET || this == TAKE_PROFIT_MARKET;
        }
    }

    public enum TimeInForce {
        /** Rests until cancelled. */
        GTC,
        /** Fills what it can immediately, cancels the rest. */
        IOC,
        /** All or nothing, immediately. */
        FOK
    }

    /**
     * Which price triggers a conditional order. {@link #MARK_PRICE} is the deliberate default for
     * protective stops, because liquidation is decided on the mark price; {@link #CONTRACT_PRICE}
     * triggers on last traded price, which wicks far more on a thin book.
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

    /**
     * What an order is for. Drives the deterministic client order id and lets
     * {@link PreTradeValidator} enforce that anything but an entry can only reduce a position.
     */
    public enum OrderPurpose {
        ENTRY(false),
        STOP_LOSS(true),
        TAKE_PROFIT(true),
        EMERGENCY_CLOSE(true);

        private final boolean closing;

        OrderPurpose(boolean closing) { this.closing = closing; }

        /** True when this order may only ever shrink a position. */
        public boolean isClosing() { return closing; }
    }
}
