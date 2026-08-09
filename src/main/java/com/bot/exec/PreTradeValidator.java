package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.exec.OrderTypes.OrderType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks an order against the symbol's filters <b>before</b> it is sent.
 *
 * <p>A stop that comes back {@code -1111 BAD_PRECISION} was not placed, and the position it was to
 * protect is open either way — so the failure that matters is not "rejected" but "naked for the
 * seconds it took to work out why". Everything checkable locally is checked locally.
 *
 * <p>The reduce-only rule is here rather than only in {@link OrderRequest} because this is the one
 * gate every order passes through. Twice is deliberate.
 */
public final class PreTradeValidator {

    private PreTradeValidator() {}

    public record Result(List<String> violations) {
        public Result {
            violations = List.copyOf(violations);
        }

        public boolean ok() { return violations.isEmpty(); }

        public String describe() { return String.join("; ", violations); }
    }

    /**
     * @param referencePrice price used for the minimum-notional check on orders that carry no price
     *                       of their own (a MARKET entry); typically the intended entry
     */
    public static Result validate(OrderRequest order, InstrumentFilters filters, BigDecimal referencePrice) {
        Preconditions.notNull(order, "order");
        Preconditions.notNull(filters, "filters");
        Preconditions.notNull(referencePrice, "referencePrice");

        List<String> violations = new ArrayList<>();

        if (!order.symbol().equals(filters.symbol())) {
            violations.add("filters are for " + filters.symbol() + " but the order is for " + order.symbol());
            return new Result(violations);
        }

        boolean marketExecution = order.type() == OrderType.MARKET
                || order.type() == OrderType.STOP_MARKET
                || order.type() == OrderType.TAKE_PROFIT_MARKET;

        if (order.quantity() != null) {
            BigDecimal qty = order.quantity();
            if (!filters.isQuantityOnStep(qty)) {
                violations.add("quantity " + qty.toPlainString()
                        + " is not a multiple of stepSize " + filters.stepSize().toPlainString());
            }
            if (!filters.isQuantityInRange(qty, marketExecution)) {
                violations.add("quantity " + qty.toPlainString() + " is outside ["
                        + filters.minQty().toPlainString() + ", "
                        + (marketExecution ? filters.marketMaxQty() : filters.maxQty()).toPlainString() + "]");
            }
        }

        for (BigDecimal price : new BigDecimal[]{order.price(), order.stopPrice()}) {
            if (price == null) continue;
            if (!filters.isPriceOnTick(price)) {
                violations.add("price " + price.toPlainString()
                        + " is not a multiple of tickSize " + filters.tickSize().toPlainString());
            }
            if (!filters.isPriceInRange(price)) {
                violations.add("price " + price.toPlainString() + " is outside PRICE_FILTER ["
                        + filters.minPrice().toPlainString() + ", " + filters.maxPrice().toPlainString() + "]");
            }
        }

        // MIN_NOTIONAL. Binance exempts reduce-only orders from it, so a take-profit leg that is
        // small in absolute terms is still sendable; entries are not exempt.
        if (order.quantity() != null && !order.isStrictlyReducing()) {
            BigDecimal notionalPrice = order.price() != null ? order.price() : referencePrice;
            if (!filters.meetsMinNotional(notionalPrice, order.quantity())) {
                violations.add("notional " + notionalPrice.multiply(order.quantity()).toPlainString()
                        + " is below MIN_NOTIONAL " + filters.minNotional().toPlainString());
            }
        }

        if (order.purpose().isClosing() && !order.isStrictlyReducing()) {
            violations.add("closing order " + order.clientOrderId()
                    + " is neither reduceOnly nor closePosition");
        }

        return new Result(violations);
    }

    /** Validates and throws on the first failure. Used on the path where there is nothing to fall back to. */
    public static void validateOrThrow(OrderRequest order, InstrumentFilters filters, BigDecimal referencePrice) {
        Result result = validate(order, filters, referencePrice);
        if (!result.ok()) {
            throw new IllegalArgumentException(
                    "order " + order.clientOrderId() + " would be rejected by the exchange: " + result.describe());
        }
    }
}
