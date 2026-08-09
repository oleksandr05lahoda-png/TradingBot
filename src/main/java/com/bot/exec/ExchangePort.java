package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.risk.MarginTierTable;

import java.util.List;
import java.util.Optional;

/**
 * The seam between this system and any exchange. The core — sizing, stops, liquidation, exposure,
 * reconciliation logic — depends on this interface and on nothing else; HTTP, signing, JSON, rate
 * limit headers and Binance's error codes all live behind it in
 * {@code com.bot.exec.binance}.
 *
 * <p>That is what makes the execution path testable without a network: the tests drive a fake that
 * implements exactly these methods including their failure modes — duplicate client order ids,
 * partial fills, lost responses — which are near-impossible to provoke on demand against a real
 * endpoint.
 *
 * <p>Every method may throw {@link ExchangeException}. Callers must distinguish
 * {@link ExchangeException#ambiguous()} from a plain refusal before deciding to retry.
 */
public interface ExchangePort extends AutoCloseable {

    /** Host this port actually sends to. Used by the boot banner and the testnet assertions. */
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

    /** Every non-flat position on the account. */
    List<PositionSnapshot> openPositions();

    /** Cancels one order by client order id. Cancelling an unknown or finished order is not an error. */
    void cancelOrder(String symbol, String clientOrderId);

    /** Cancels every working order on the symbol. Does not touch positions. */
    void cancelAllOpenOrders(String symbol);

    /**
     * Arms the exchange-side dead-man's switch for {@code symbol}: if this method is not called
     * again within {@code countdownMillis}, the exchange cancels every open order on that symbol by
     * itself. Positions are untouched.
     *
     * <p>Server-side is the point. A client-side watchdog dies in the same crash that stranded the
     * orders; this one keeps counting on the exchange's machine. Pass 0 to disarm.
     */
    void armDeadMansSwitch(String symbol, long countdownMillis);

    @Override void close();
}
