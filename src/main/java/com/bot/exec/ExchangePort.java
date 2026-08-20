package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.risk.MarginTierTable;

import java.util.List;
import java.util.Optional;

/**
 * The seam between the core and any exchange; HTTP, signing, JSON and Binance's error codes all live
 * behind it in {@code com.bot.exec.binance}. Every method may throw {@link ExchangeException};
 * callers must check {@link ExchangeException#ambiguous()} before deciding to retry.
 */
public interface ExchangePort extends AutoCloseable {

    /** Host this port actually sends to; the pinning itself lives in {@code BinanceVenue}. */
    String endpointHost();

    /** Exchange clock, in epoch milliseconds. Used to detect the drift that invalidates signatures. */
    long serverTimeMillis();

    AccountSnapshot fetchAccount();

    /** Never cached across a run without refreshing. */
    InstrumentFilters fetchFilters(String symbol);

    /** Maintenance-margin brackets, for the liquidation calculation. */
    MarginTierTable fetchMarginTiers(String symbol);

    /** Realised PnL booked since {@code sinceEpochMs}, used to reseed the daily loss limit after a restart. */
    double fetchRealizedPnlSince(long sinceEpochMs);

    /** Idempotent: already-isolated is a success, not an error. */
    void ensureIsolatedMargin(String symbol);

    /** Idempotent. */
    void setLeverage(String symbol, int leverage);

    /** A successful return means accepted, not filled — read the returned status for what happened. */
    OrderStatus placeOrder(OrderRequest request);

    /** Empty when the exchange has never heard of the id. */
    Optional<OrderStatus> queryOrder(String symbol, String clientOrderId);

    /** Every order still working on the symbol, including ones this process did not create. */
    List<OrderStatus> openOrders(String symbol);

    /**
     * False means conditional (trigger) orders never appear in {@link #openOrders}, so the absence of
     * a stop proves nothing: the reconciler asks {@link #queryOrder} for it by name instead.
     */
    default boolean canListConditionalOrders() { return true; }

    List<PositionSnapshot> openPositions();

    /** Cancelling an unknown or already-finished order is not an error. */
    void cancelOrder(String symbol, String clientOrderId);

    /** Every working order on the symbol. Does not touch positions. */
    void cancelAllOpenOrders(String symbol);

    /**
     * Not re-called within {@code countdownMillis}, the exchange cancels every open order on the
     * symbol itself; positions are untouched. Pass 0 to disarm.
     */
    void armDeadMansSwitch(String symbol, long countdownMillis);

    @Override void close();
}
