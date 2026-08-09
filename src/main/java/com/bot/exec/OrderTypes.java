package com.bot.exec;

import com.bot.core.Side;

/**
 * The exchange-facing vocabulary of an order, kept in one file because these enums only mean
 * anything together.
 *
 * <p>{@link OrderSide} is deliberately a different type from {@link com.bot.core.Side}. A LONG
 * position is opened with BUY and closed with SELL; a system that uses one enum for both eventually
 * closes a long by buying more of it, and the compiler has nothing to say about it.
 */
public final class OrderTypes {

    private OrderTypes() {}

    /** BUY or SELL, as the exchange means it. */
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

    /** The subset of Binance USDⓈ-M order types this system uses. Nothing here is exotic on purpose. */
    public enum OrderType {
        /** Immediate execution, no price guarantee. Used for entries where a fill matters more than a tick. */
        MARKET,
        /** Price guaranteed, fill not. */
        LIMIT,
        /** Conditional market order — the protective stop. Triggers on {@link WorkingType}. */
        STOP_MARKET,
        /** Conditional market order on the winning side — the reduce-only exits. */
        TAKE_PROFIT_MARKET
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
     * Which price triggers a conditional order.
     *
     * <p>{@link #MARK_PRICE} is the deliberate default for protective stops: liquidation is decided
     * on the mark price, so a stop that triggers on anything else is measuring a different number
     * from the one that can end the position. {@link #CONTRACT_PRICE} triggers on last traded price,
     * which on a thin testnet book wicks around far more.
     */
    public enum WorkingType {
        MARK_PRICE, CONTRACT_PRICE
    }

    /**
     * Order lifecycle. Note that "sent successfully" is not in this list: an HTTP 200 means the
     * exchange accepted the request, and {@link #NEW} is as far as that gets you.
     */
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
     * {@link PreTradeValidator} enforce that everything which is not an entry can only ever reduce
     * a position.
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
