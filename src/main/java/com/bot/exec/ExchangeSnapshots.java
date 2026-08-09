package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.OrderTypes.OrderType;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * What the exchange says is true. These types are read-only views of the exchange's own state, and
 * they are the authority whenever they disagree with anything the bot remembers.
 */
public final class ExchangeSnapshots {

    private ExchangeSnapshots() {}

    /**
     * The state of one order as the exchange reports it.
     *
     * <p>{@code executedQuantity} is the number that matters, and it is not implied by
     * {@code state}: an order can sit in {@link OrderState#PARTIALLY_FILLED} for a long time, and a
     * {@link OrderState#CANCELED} order may still have filled part of the way before it was pulled.
     * Sizing the protective stop off the intended quantity rather than this one leaves the
     * difference unprotected.
     *
     * @param averagePrice weighted average fill price, {@code ZERO} while nothing has filled
     */
    public record OrderStatus(
            String clientOrderId,
            long exchangeOrderId,
            String symbol,
            OrderState state,
            OrderType type,
            BigDecimal originalQuantity,
            BigDecimal executedQuantity,
            BigDecimal averagePrice,
            BigDecimal stopPrice,
            boolean reduceOnly,
            boolean closePosition,
            long updateTimeMs) {

        public OrderStatus {
            Preconditions.notBlank(clientOrderId, "clientOrderId");
            Preconditions.notBlank(symbol, "symbol");
            Preconditions.notNull(state, "state");
            Preconditions.notNull(originalQuantity, "originalQuantity");
            Preconditions.notNull(executedQuantity, "executedQuantity");
            Preconditions.notNull(averagePrice, "averagePrice");
            Preconditions.require(executedQuantity.signum() >= 0, "executedQuantity must not be negative");
        }

        public boolean hasFill() { return executedQuantity.signum() > 0; }

        /** Filled but not completely — the case that silently changes the risk of the position. */
        public boolean isPartial() {
            return hasFill() && executedQuantity.compareTo(originalQuantity) < 0;
        }

        public boolean isWorking() { return state.isWorking(); }
    }

    /**
     * A position as the exchange reports it.
     *
     * @param signedQuantity positive for a long, negative for a short, zero when flat. Signed
     *                       because that is how the exchange models it, and translating too early
     *                       is how a short becomes a long in local state
     */
    public record PositionSnapshot(
            String symbol,
            BigDecimal signedQuantity,
            BigDecimal entryPrice,
            int leverage,
            boolean isolated,
            BigDecimal unrealizedPnl,
            BigDecimal liquidationPrice) {

        public PositionSnapshot {
            Preconditions.notBlank(symbol, "symbol");
            Preconditions.notNull(signedQuantity, "signedQuantity");
            Preconditions.notNull(entryPrice, "entryPrice");
            Preconditions.notNull(unrealizedPnl, "unrealizedPnl");
            Preconditions.notNull(liquidationPrice, "liquidationPrice");
        }

        public boolean isFlat() { return signedQuantity.signum() == 0; }

        public BigDecimal absoluteQuantity() { return signedQuantity.abs(); }

        /** Empty when flat — a flat position has no direction, and inventing one is a bug generator. */
        public Optional<Side> direction() {
            int sign = signedQuantity.signum();
            if (sign == 0) return Optional.empty();
            return Optional.of(sign > 0 ? Side.LONG : Side.SHORT);
        }
    }

    /**
     * Account-level margin state.
     *
     * @param availableBalance what may still be committed as margin
     * @param walletBalance    total, including margin already locked
     */
    public record AccountSnapshot(
            BigDecimal walletBalance,
            BigDecimal availableBalance,
            BigDecimal totalUnrealizedPnl,
            long exchangeTimeMs) {

        public AccountSnapshot {
            Preconditions.notNull(walletBalance, "walletBalance");
            Preconditions.notNull(availableBalance, "availableBalance");
            Preconditions.notNull(totalUnrealizedPnl, "totalUnrealizedPnl");
        }

        /**
         * The balance the risk budget is a percentage of: wallet balance plus open PnL, i.e. what the
         * account is actually worth right now. Using wallet balance alone would let a losing open
         * position keep sizing new trades as if the loss had not happened.
         */
        public double equityUsd() {
            return walletBalance.add(totalUnrealizedPnl).doubleValue();
        }
    }
}
