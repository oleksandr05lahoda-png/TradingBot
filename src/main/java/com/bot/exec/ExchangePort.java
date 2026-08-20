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
 * behind it in {@code com.bot.exec.binance}.
 *
 * <p>Every method may throw {@link ExchangeException}. Callers must distinguish
 * {@link ExchangeException#ambiguous()} from a plain refusal before deciding to retry.
 */
public interface ExchangePort extends AutoCloseable {

    /**
     * Host this port actually sends to. Shown in the boot banner and in adapter error messages;
     * venue/host pinning itself lives in {@code BinanceVenue}.
     */
    String endpointHost();

    /** Exchange clock, in epoch milliseconds. Used to detect the drift that invalidates signatures. */
    long serverTimeMillis();

    /** Wallet balance, available margin and open PnL. */
    AccountSnapshot fetchAccount();

    /** Live precision and size filters for one symbol. Never cached across a run without refreshing. */
    InstrumentFilters fetchFilters(String symbol);

    /** Live maintenance-margin brackets for one symbol, for the liquidation calculation. */
    MarginTierTable fetchMarginTiers(String symbol);

    /** Realised PnL booked since {@code sinceEpochMs}, used to reseed the daily loss limit after a restart. */
    double fetchRealizedPnlSince(long sinceEpochMs);

    /** Switches the symbol to isolated margin. Idempotent: already-isolated is a success, not an error. */
    void ensureIsolatedMargin(String symbol);

    /** Sets leverage for the symbol. Idempotent. */
    void setLeverage(String symbol, int leverage);

    /**
     * Sends one order. A successful return means the exchange accepted the request — not that it
     * filled. Read {@link #queryOrder} or the returned status for what actually happened.
     */
    OrderStatus placeOrder(OrderRequest request);

    /** Looks an order up by its client order id. Empty when the exchange has never heard of it. */
    Optional<OrderStatus> queryOrder(String symbol, String clientOrderId);

    /** Every order still working on the symbol, including ones this process did not create. */
    List<OrderStatus> openOrders(String symbol);

    /**
     * Whether {@link #openOrders} can see conditional (trigger) orders.
     *
     * <p>False means a protective stop may exist and not appear in the list, so the absence of one
     * proves nothing. The reconciler must not call a position naked on that basis — a check that
     * cries wolf on every healthy position is worse than no check, because it trains the operator to
     * ignore the one that matters. It asks {@link #queryOrder} for the stop by name instead, which
     * these venues do answer.
     */
    default boolean canListConditionalOrders() { return true; }

    /** Every non-flat position on the account. */
    List<PositionSnapshot> openPositions();

    /** Cancels one order by client order id. Cancelling an unknown or finished order is not an error. */
    void cancelOrder(String symbol, String clientOrderId);

    /** Cancels every working order on the symbol. Does not touch positions. */
    void cancelAllOpenOrders(String symbol);

    /**
     * If this is not called again within {@code countdownMillis}, the exchange cancels every open
     * order on the symbol by itself; positions are untouched. Pass 0 to disarm.
     */
    void armDeadMansSwitch(String symbol, long countdownMillis);

    @Override void close();
}
