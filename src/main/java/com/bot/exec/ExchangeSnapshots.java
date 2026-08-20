package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.OrderTypes.OrderType;

import java.math.BigDecimal;
import java.util.Optional;

/** Read-only views of the exchange's own state; the authority whenever the bot's memory disagrees. */
public final class ExchangeSnapshots {

    private ExchangeSnapshots() {}

    /**
     * One order as the exchange reports it. {@code executedQuantity} is not implied by {@code state}
     * — a {@link OrderState#CANCELED} order may still have filled part of the way — and protective
     * stops must be sized off it, not off the intended quantity. {@code averagePrice} is
     * {@code ZERO} until something fills.
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


        public boolean isWorking() { return state.isWorking(); }
    }

    /** A position as the exchange reports it; {@code signedQuantity} is negative for a short. */
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

        /** Empty when flat. */
        public Optional<Side> direction() {
            int sign = signedQuantity.signum();
            if (sign == 0) return Optional.empty();
            return Optional.of(sign > 0 ? Side.LONG : Side.SHORT);
        }
    }

    /** {@code availableBalance} excludes margin already locked; {@code walletBalance} includes it. */
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

        /** The balance the risk budget is a fraction of; wallet alone ignores an open loss. */
        public double equityUsd() {
            return walletBalance.add(totalUnrealizedPnl).doubleValue();
        }
    }
}
