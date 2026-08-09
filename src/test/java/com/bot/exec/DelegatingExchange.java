package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.risk.MarginTierTable;

import java.util.List;
import java.util.Optional;

/**
 * A pass-through {@link ExchangePort} over a {@link FakeExchange}, so a test that needs one method
 * to misbehave overrides that one method and inherits working behaviour for everything else.
 */
class DelegatingExchange implements ExchangePort {

    protected final FakeExchange delegate = new FakeExchange();

    @Override public String endpointHost() { return delegate.endpointHost(); }

    @Override public long serverTimeMillis() { return delegate.serverTimeMillis(); }

    @Override public AccountSnapshot fetchAccount() { return delegate.fetchAccount(); }

    @Override public InstrumentFilters fetchFilters(String symbol) { return delegate.fetchFilters(symbol); }

    @Override public MarginTierTable fetchMarginTiers(String symbol) { return delegate.fetchMarginTiers(symbol); }

    @Override public double fetchRealizedPnlSince(long sinceEpochMs) {
        return delegate.fetchRealizedPnlSince(sinceEpochMs);
    }

    @Override public void ensureIsolatedMargin(String symbol) { delegate.ensureIsolatedMargin(symbol); }

    @Override public void setLeverage(String symbol, int leverage) { delegate.setLeverage(symbol, leverage); }

    @Override public OrderStatus placeOrder(OrderRequest request) { return delegate.placeOrder(request); }

    @Override public Optional<OrderStatus> queryOrder(String symbol, String clientOrderId) {
        return delegate.queryOrder(symbol, clientOrderId);
    }

    @Override public List<OrderStatus> openOrders(String symbol) { return delegate.openOrders(symbol); }

    @Override public List<PositionSnapshot> openPositions() { return delegate.openPositions(); }

    @Override public void cancelOrder(String symbol, String clientOrderId) {
        delegate.cancelOrder(symbol, clientOrderId);
    }

    @Override public void cancelAllOpenOrders(String symbol) { delegate.cancelAllOpenOrders(symbol); }

    @Override public void armDeadMansSwitch(String symbol, long countdownMillis) {
        delegate.armDeadMansSwitch(symbol, countdownMillis);
    }

    @Override public void close() { delegate.close(); }
}
