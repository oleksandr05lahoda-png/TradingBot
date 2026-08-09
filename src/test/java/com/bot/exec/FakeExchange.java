package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.exec.OrderTypes.OrderSide;
import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.exec.binance.BinanceErrorCodes;
import com.bot.risk.MarginTierTable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An in-memory exchange with the failure modes that matter.
 *
 * <p>The point of the port/adapter split is exactly this class. Duplicate client order ids, lost
 * responses, partial fills and positions that change behind the bot's back are the situations the
 * execution layer exists to survive, and provoking any of them against a real endpoint is somewhere
 * between hard and impossible. Here they are one method call.
 *
 * <p>Its behaviour deliberately mirrors Binance's where that matters: a duplicate client order id
 * raises {@code -4116}, an unknown order query returns empty rather than throwing, a market order
 * fills at {@link #fillPrice} for {@link #fillRatio} of its size, and a conditional order rests.
 */
final class FakeExchange implements ExchangePort {

    private final Map<String, OrderStatus> ordersByClientId = new LinkedHashMap<>();
    private final Map<String, BigDecimal> positions = new LinkedHashMap<>();
    private final Map<String, BigDecimal> entryPrices = new LinkedHashMap<>();
    private final Map<String, Long> deadMansCountdowns = new LinkedHashMap<>();

    private InstrumentFilters filters = InstrumentFilters.of("BTCUSDT", "0.10", "0.001", "5");
    private MarginTierTable tiers = MarginTierTable.conservativeDefault();
    private BigDecimal walletBalance = new BigDecimal("10000");
    private BigDecimal unrealizedPnl = BigDecimal.ZERO;
    private double realizedPnlToday = 0;

    /** Fraction of a market order that fills. 1.0 = full, 0.5 = half. */
    double fillRatio = 1.0;
    /** Price a market order fills at; null means "the order's own price or the last set entry". */
    BigDecimal fillPrice = new BigDecimal("64000.0");
    /** When true, the next placeOrder records the order and then throws an ambiguous failure. */
    boolean loseNextResponse = false;
    /** When non-null, the next placeOrder throws this instead of doing anything. */
    ExchangeException failNextPlaceWith = null;
    /** Makes the next N queryOrder calls report "no such order", as a propagation delay would. */
    int hideNextQueries = 0;

    int placeOrderCalls = 0;
    int queryOrderCalls = 0;
    int cancelAllCalls = 0;
    long orderIdSequence = 1;

    // ─── Test controls ───────────────────────────────────────────────────────────────────────

    void setFilters(InstrumentFilters filters) { this.filters = filters; }

    void setTiers(MarginTierTable tiers) { this.tiers = tiers; }

    void setWalletBalance(String balance) { this.walletBalance = new BigDecimal(balance); }

    void setUnrealizedPnl(String pnl) { this.unrealizedPnl = new BigDecimal(pnl); }

    void setRealizedPnlToday(double pnl) { this.realizedPnlToday = pnl; }

    /** Plants a position the bot does not know about, as a manual trade or a missed fill would. */
    void plantPosition(String symbol, String signedQuantity, String entryPrice) {
        positions.put(symbol, new BigDecimal(signedQuantity));
        entryPrices.put(symbol, new BigDecimal(entryPrice));
    }

    void clearPosition(String symbol) {
        positions.remove(symbol);
        entryPrices.remove(symbol);
    }

    Long deadMansCountdownFor(String symbol) { return deadMansCountdowns.get(symbol); }

    List<OrderStatus> allOrders() { return List.copyOf(ordersByClientId.values()); }

    Optional<OrderStatus> order(String clientOrderId) {
        return Optional.ofNullable(ordersByClientId.get(clientOrderId));
    }

    // ─── ExchangePort ────────────────────────────────────────────────────────────────────────

    @Override public String endpointHost() { return "fake.local"; }

    @Override public long serverTimeMillis() { return 1_754_740_800_000L; }

    @Override public AccountSnapshot fetchAccount() {
        return new AccountSnapshot(walletBalance, walletBalance, unrealizedPnl, serverTimeMillis());
    }

    @Override public InstrumentFilters fetchFilters(String symbol) { return filters; }

    @Override public MarginTierTable fetchMarginTiers(String symbol) { return tiers; }

    @Override public double fetchRealizedPnlSince(long sinceEpochMs) { return realizedPnlToday; }

    @Override public void ensureIsolatedMargin(String symbol) { }

    @Override public void setLeverage(String symbol, int leverage) { }

    @Override public OrderStatus placeOrder(OrderRequest request) {
        placeOrderCalls++;

        if (failNextPlaceWith != null) {
            ExchangeException failure = failNextPlaceWith;
            failNextPlaceWith = null;
            throw failure;
        }
        if (ordersByClientId.containsKey(request.clientOrderId())) {
            throw ExchangeException.refused("clientOrderId is duplicated", 400,
                    BinanceErrorCodes.DUPLICATED_CLIENT_ORDER_ID);
        }

        OrderStatus status = simulate(request);
        ordersByClientId.put(request.clientOrderId(), status);
        applyFill(request, status);

        if (loseNextResponse) {
            loseNextResponse = false;
            // The order landed; the response did not. This is the case a blind retry doubles.
            throw ExchangeException.ambiguous("response lost after the order was accepted", null);
        }
        return status;
    }

    private OrderStatus simulate(OrderRequest request) {
        long id = orderIdSequence++;
        BigDecimal requested = request.quantity() == null ? BigDecimal.ZERO : request.quantity();

        if (request.type() == OrderType.MARKET) {
            BigDecimal executed = filters.quantizeQuantityDown(requested.doubleValue() * fillRatio);
            OrderState state = executed.compareTo(requested) >= 0
                    ? OrderState.FILLED
                    : (executed.signum() > 0 ? OrderState.PARTIALLY_FILLED : OrderState.EXPIRED);
            return new OrderStatus(request.clientOrderId(), id, request.symbol(), state, request.type(),
                    requested, executed, executed.signum() > 0 ? fillPrice : BigDecimal.ZERO,
                    BigDecimal.ZERO, request.reduceOnly(), request.closePosition(), serverTimeMillis());
        }
        // LIMIT and the conditional types rest until something triggers them.
        return new OrderStatus(request.clientOrderId(), id, request.symbol(), OrderState.NEW, request.type(),
                requested, BigDecimal.ZERO, BigDecimal.ZERO,
                request.stopPrice() == null ? BigDecimal.ZERO : request.stopPrice(),
                request.reduceOnly(), request.closePosition(), serverTimeMillis());
    }

    private void applyFill(OrderRequest request, OrderStatus status) {
        if (status.executedQuantity().signum() <= 0) return;
        BigDecimal signed = request.side() == OrderSide.BUY
                ? status.executedQuantity() : status.executedQuantity().negate();
        BigDecimal updated = positions.getOrDefault(request.symbol(), BigDecimal.ZERO).add(signed);
        if (updated.signum() == 0) {
            positions.remove(request.symbol());
            entryPrices.remove(request.symbol());
        } else {
            positions.put(request.symbol(), updated);
            entryPrices.putIfAbsent(request.symbol(), status.averagePrice());
        }
    }

    @Override public Optional<OrderStatus> queryOrder(String symbol, String clientOrderId) {
        queryOrderCalls++;
        if (hideNextQueries > 0) {
            hideNextQueries--;
            return Optional.empty();
        }
        return Optional.ofNullable(ordersByClientId.get(clientOrderId));
    }

    @Override public List<OrderStatus> openOrders(String symbol) {
        List<OrderStatus> out = new ArrayList<>();
        for (OrderStatus o : ordersByClientId.values()) {
            if (o.symbol().equals(symbol) && o.isWorking()) out.add(o);
        }
        return out;
    }

    @Override public List<PositionSnapshot> openPositions() {
        List<PositionSnapshot> out = new ArrayList<>();
        for (Map.Entry<String, BigDecimal> e : positions.entrySet()) {
            if (e.getValue().signum() == 0) continue;
            out.add(new PositionSnapshot(e.getKey(), e.getValue(),
                    entryPrices.getOrDefault(e.getKey(), fillPrice), 3, true,
                    unrealizedPnl, BigDecimal.ZERO));
        }
        return out;
    }

    @Override public void cancelOrder(String symbol, String clientOrderId) {
        OrderStatus existing = ordersByClientId.get(clientOrderId);
        if (existing == null || !existing.isWorking()) return;
        ordersByClientId.put(clientOrderId, new OrderStatus(existing.clientOrderId(),
                existing.exchangeOrderId(), existing.symbol(), OrderState.CANCELED, existing.type(),
                existing.originalQuantity(), existing.executedQuantity(), existing.averagePrice(),
                existing.stopPrice(), existing.reduceOnly(), existing.closePosition(), serverTimeMillis()));
    }

    @Override public void cancelAllOpenOrders(String symbol) {
        cancelAllCalls++;
        for (OrderStatus o : List.copyOf(ordersByClientId.values())) {
            if (o.symbol().equals(symbol)) cancelOrder(symbol, o.clientOrderId());
        }
    }

    @Override public void armDeadMansSwitch(String symbol, long countdownMillis) {
        deadMansCountdowns.put(symbol, countdownMillis);
    }

    @Override public void close() { }
}
