package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.exec.OrderTypes.OrderPurpose;
import com.bot.exec.OrderTypes.OrderSide;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.exec.OrderTypes.TimeInForce;
import com.bot.exec.OrderTypes.WorkingType;

import java.math.BigDecimal;
import java.util.regex.Pattern;

/**
 * One order, fully specified; the constructor enforces the field combinations the exchange accepts.
 * An order whose {@link OrderPurpose#isClosing()} is true must carry {@code reduceOnly} or
 * {@code closePosition} (mutually exclusive at Binance): without either, a close that races an
 * already-triggered stop opens a fresh position in the opposite direction.
 *
 * @param quantity      base units; must be {@code null} exactly when {@code closePosition} is true
 * @param price         limit price; required for {@link OrderType#LIMIT}, null otherwise
 * @param stopPrice     trigger price; required for the conditional types, null otherwise
 * @param closePosition Binance's close-all flag, inherently reduce-only
 */
public record OrderRequest(
        String symbol,
        OrderSide side,
        OrderType type,
        BigDecimal quantity,
        BigDecimal price,
        BigDecimal stopPrice,
        boolean reduceOnly,
        boolean closePosition,
        TimeInForce timeInForce,
        WorkingType workingType,
        String clientOrderId,
        OrderPurpose purpose) {

    /** Binance USDⓈ-M {@code newClientOrderId} pattern, verbatim from the API reference. */
    public static final Pattern CLIENT_ORDER_ID = Pattern.compile("^[\\.A-Z\\:/a-z0-9_-]{1,36}$");

    public OrderRequest {
        Preconditions.notBlank(symbol, "symbol");
        Preconditions.notNull(side, "side");
        Preconditions.notNull(type, "type");
        Preconditions.notNull(purpose, "purpose");
        Preconditions.notBlank(clientOrderId, "clientOrderId");
        Preconditions.require(CLIENT_ORDER_ID.matcher(clientOrderId).matches(),
                "clientOrderId \"" + clientOrderId + "\" does not match the exchange pattern "
                        + CLIENT_ORDER_ID.pattern());

        Preconditions.require(!(reduceOnly && closePosition),
                "reduceOnly and closePosition are mutually exclusive — the exchange rejects both together");
        if (purpose.isClosing()) {
            Preconditions.require(reduceOnly || closePosition,
                    purpose + " must be reduce-only or closePosition; a closing order that is neither "
                            + "will open the opposite position if it races the stop");
        } else {
            Preconditions.require(!reduceOnly && !closePosition,
                    purpose + " must not be reduce-only — it is meant to open a position");
        }

        if (closePosition) {
            Preconditions.require(quantity == null,
                    "closePosition closes whatever is there; a quantity would be ignored, so passing "
                            + "one means the caller believes something untrue");
        } else {
            Preconditions.notNull(quantity, "quantity");
            Preconditions.require(quantity.signum() > 0, "quantity must be positive, got " + quantity);
        }

        switch (type) {
            case LIMIT -> {
                Preconditions.notNull(price, "price is required for a LIMIT order");
                Preconditions.notNull(timeInForce, "timeInForce is required for a LIMIT order");
                Preconditions.require(stopPrice == null, "stopPrice is meaningless on a LIMIT order");
            }
            case MARKET -> {
                Preconditions.require(price == null, "price is meaningless on a MARKET order");
                Preconditions.require(stopPrice == null, "stopPrice is meaningless on a MARKET order");
            }
            case STOP_MARKET, TAKE_PROFIT_MARKET -> {
                Preconditions.notNull(stopPrice, "stopPrice is required for " + type);
                Preconditions.require(stopPrice.signum() > 0, "stopPrice must be positive");
                Preconditions.require(price == null, "price is meaningless on " + type);
                Preconditions.notNull(workingType, "workingType is required for " + type);
            }
        }
    }

    /** True when this order can only ever shrink a position. */
    public boolean isStrictlyReducing() {
        return reduceOnly || closePosition;
    }

    // ─── Builders for the four shapes this system actually sends ─────────────────────────────

    public static OrderRequest marketEntry(String symbol, OrderSide side, BigDecimal quantity, String clientOrderId) {
        return new OrderRequest(symbol, side, OrderType.MARKET, quantity, null, null,
                false, false, null, null, clientOrderId, OrderPurpose.ENTRY);
    }

    public static OrderRequest limitEntry(String symbol, OrderSide side, BigDecimal quantity,
                                          BigDecimal price, TimeInForce tif, String clientOrderId) {
        return new OrderRequest(symbol, side, OrderType.LIMIT, quantity, price, null,
                false, false, tif, null, clientOrderId, OrderPurpose.ENTRY);
    }

    /**
     * The protective stop, as {@code closePosition=true}: it stays correct after a take-profit leg
     * has shrunk the position, where a fixed-quantity stop would protect a size that no longer exists.
     */
    public static OrderRequest protectiveStop(String symbol, OrderSide closingSide,
                                              BigDecimal stopPrice, String clientOrderId) {
        return new OrderRequest(symbol, closingSide, OrderType.STOP_MARKET, null, null, stopPrice,
                false, true, null, WorkingType.MARK_PRICE, clientOrderId, OrderPurpose.STOP_LOSS);
    }

    public static OrderRequest takeProfit(String symbol, OrderSide closingSide, BigDecimal quantity,
                                          BigDecimal stopPrice, String clientOrderId) {
        return new OrderRequest(symbol, closingSide, OrderType.TAKE_PROFIT_MARKET, quantity, null, stopPrice,
                true, false, null, WorkingType.MARK_PRICE, clientOrderId, OrderPurpose.TAKE_PROFIT);
    }

    public static OrderRequest emergencyClose(String symbol, OrderSide closingSide,
                                              BigDecimal quantity, String clientOrderId) {
        return new OrderRequest(symbol, closingSide, OrderType.MARKET, quantity, null, null,
                true, false, null, null, clientOrderId, OrderPurpose.EMERGENCY_CLOSE);
    }
}
