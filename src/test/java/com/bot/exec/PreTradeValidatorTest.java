package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.exec.OrderTypes.OrderPurpose;
import com.bot.exec.OrderTypes.OrderSide;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.exec.OrderTypes.TimeInForce;
import com.bot.exec.OrderTypes.WorkingType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Everything checkable before an order is sent, checked before it is sent. */
class PreTradeValidatorTest {

    private final InstrumentFilters btc = InstrumentFilters.of("BTCUSDT", "0.10", "0.001", "5");
    private final BigDecimal reference = new BigDecimal("64000.0");

    private static String id(OrderPurpose purpose) {
        return ClientOrderIdFactory.create("sig-1", purpose, 0);
    }

    @Test
    @DisplayName("a well-formed entry passes")
    void wellFormedEntryPasses() {
        OrderRequest order = OrderRequest.marketEntry("BTCUSDT", OrderSide.BUY,
                new BigDecimal("0.041"), id(OrderPurpose.ENTRY));
        assertTrue(PreTradeValidator.validate(order, btc, reference).ok());
    }

    @Test
    @DisplayName("a quantity off the lot step is caught locally, not by a -1111 from the exchange")
    void offStepQuantityIsCaught() {
        OrderRequest order = OrderRequest.marketEntry("BTCUSDT", OrderSide.BUY,
                new BigDecimal("0.0415"), id(OrderPurpose.ENTRY));
        PreTradeValidator.Result result = PreTradeValidator.validate(order, btc, reference);
        assertFalse(result.ok());
        assertTrue(result.describe().contains("stepSize"), result.describe());
    }

    @Test
    @DisplayName("a price off the tick is caught locally")
    void offTickPriceIsCaught() {
        OrderRequest order = OrderRequest.protectiveStop("BTCUSDT", OrderSide.SELL,
                new BigDecimal("62800.037"), id(OrderPurpose.STOP_LOSS));
        PreTradeValidator.Result result = PreTradeValidator.validate(order, btc, reference);
        assertFalse(result.ok());
        assertTrue(result.describe().contains("tickSize"), result.describe());
    }

    @Test
    @DisplayName("an entry below the minimum notional is caught; a reduce-only exit is exempt")
    void minNotionalAppliesToEntriesOnly() {
        // 0.001 at a reference price of 1,000 is a notional of $1, under the $5 floor.
        BigDecimal cheapReference = new BigDecimal("1000.0");
        OrderRequest entry = OrderRequest.marketEntry("BTCUSDT", OrderSide.BUY,
                new BigDecimal("0.001"), id(OrderPurpose.ENTRY));
        assertFalse(PreTradeValidator.validate(entry, btc, cheapReference).ok());

        OrderRequest exit = OrderRequest.takeProfit("BTCUSDT", OrderSide.SELL,
                new BigDecimal("0.001"), new BigDecimal("1100.0"), id(OrderPurpose.TAKE_PROFIT));
        assertTrue(PreTradeValidator.validate(exit, btc, cheapReference).ok(),
                "Binance exempts reduce-only orders from MIN_NOTIONAL — refusing them here would "
                        + "block exits the exchange would have accepted");
    }

    @Test
    @DisplayName("filters for the wrong symbol are refused outright")
    void wrongSymbolFilters() {
        OrderRequest order = OrderRequest.marketEntry("ETHUSDT", OrderSide.BUY,
                new BigDecimal("0.041"), id(OrderPurpose.ENTRY));
        PreTradeValidator.Result result = PreTradeValidator.validate(order, btc, reference);
        assertFalse(result.ok());
        assertTrue(result.describe().contains("but the order is for ETHUSDT"), result.describe());
    }

    @Test
    @DisplayName("a closing order that is neither reduce-only nor closePosition cannot be built at all")
    void closingOrdersMustBeStrictlyReducing() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new OrderRequest("BTCUSDT", OrderSide.SELL, OrderType.TAKE_PROFIT_MARKET,
                        new BigDecimal("0.041"), null, new BigDecimal("66400.0"),
                        false, false, null, WorkingType.MARK_PRICE,
                        id(OrderPurpose.TAKE_PROFIT), OrderPurpose.TAKE_PROFIT));
        assertTrue(thrown.getMessage().contains("reduce-only or closePosition"), thrown.getMessage());
    }

    @Test
    @DisplayName("reduceOnly and closePosition together are refused, as the exchange refuses them")
    void reduceOnlyAndClosePositionAreExclusive() {
        assertThrows(IllegalArgumentException.class,
                () -> new OrderRequest("BTCUSDT", OrderSide.SELL, OrderType.STOP_MARKET,
                        null, null, new BigDecimal("62800.0"),
                        true, true, null, WorkingType.MARK_PRICE,
                        id(OrderPurpose.STOP_LOSS), OrderPurpose.STOP_LOSS));
    }

    @Test
    @DisplayName("an entry cannot be marked reduce-only")
    void entriesCannotBeReduceOnly() {
        assertThrows(IllegalArgumentException.class,
                () -> new OrderRequest("BTCUSDT", OrderSide.BUY, OrderType.MARKET,
                        new BigDecimal("0.041"), null, null,
                        true, false, null, null, id(OrderPurpose.ENTRY), OrderPurpose.ENTRY));
    }

    @Test
    @DisplayName("closePosition orders carry no quantity, because one would be ignored")
    void closePositionCarriesNoQuantity() {
        assertThrows(IllegalArgumentException.class,
                () -> new OrderRequest("BTCUSDT", OrderSide.SELL, OrderType.STOP_MARKET,
                        new BigDecimal("0.041"), null, new BigDecimal("62800.0"),
                        false, true, null, WorkingType.MARK_PRICE,
                        id(OrderPurpose.STOP_LOSS), OrderPurpose.STOP_LOSS));
    }

    @Test
    @DisplayName("a LIMIT order without a price or a time-in-force is refused")
    void limitOrdersNeedPriceAndTif() {
        assertThrows(IllegalArgumentException.class,
                () -> OrderRequest.limitEntry("BTCUSDT", OrderSide.BUY, new BigDecimal("0.041"),
                        null, TimeInForce.GTC, id(OrderPurpose.ENTRY)));
        assertThrows(IllegalArgumentException.class,
                () -> OrderRequest.limitEntry("BTCUSDT", OrderSide.BUY, new BigDecimal("0.041"),
                        new BigDecimal("64000.0"), null, id(OrderPurpose.ENTRY)));
    }

    @Test
    @DisplayName("protective stops trigger on the mark price, which is what liquidation uses")
    void stopsTriggerOnMarkPrice() {
        OrderRequest stop = OrderRequest.protectiveStop("BTCUSDT", OrderSide.SELL,
                new BigDecimal("62800.0"), id(OrderPurpose.STOP_LOSS));
        assertEquals(WorkingType.MARK_PRICE, stop.workingType(),
                "a stop measured against a different price from the one that liquidates the position "
                        + "is measuring the wrong number");
    }

    @Test
    @DisplayName("generated client order ids satisfy the exchange's pattern and stay unique per purpose")
    void clientOrderIdsAreValidAndDistinct() {
        Set<String> seen = new HashSet<>();
        for (OrderPurpose purpose : OrderPurpose.values()) {
            for (int index = 0; index < 10; index++) {
                String generated = ClientOrderIdFactory.create("signal-with-a-fairly-long-identifier-42",
                        purpose, index);
                assertTrue(OrderRequest.CLIENT_ORDER_ID.matcher(generated).matches(),
                        generated + " does not match " + OrderRequest.CLIENT_ORDER_ID.pattern());
                assertTrue(generated.length() <= 36, generated + " is longer than 36 characters");
                assertTrue(seen.add(generated), "duplicate client order id: " + generated);
            }
        }
    }

    @Test
    @DisplayName("an index outside the supported range is refused rather than silently wrapped")
    void clientOrderIdIndexIsBounded() {
        assertThrows(IllegalArgumentException.class,
                () -> ClientOrderIdFactory.create("sig", OrderPurpose.TAKE_PROFIT, 100));
        assertThrows(IllegalArgumentException.class,
                () -> ClientOrderIdFactory.create("sig", OrderPurpose.TAKE_PROFIT, -1));
    }
}
