package com.bot.exec;

import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.OrderTypes.OrderPurpose;
import com.bot.exec.OrderTypes.OrderSide;
import com.bot.exec.binance.BinanceErrorCodes;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Idempotency: the same logical order, sent again, must not become a second position. Assertions are
 * on the resulting position, not the return value — a doubled exposure returns a fine-looking order.
 */
class IdempotentResubmitTest {

    private final FakeExchange exchange = new FakeExchange();
    private final IdempotentOrderPlacer placer =
            new IdempotentOrderPlacer(exchange, 3, 2, 0, ExecFixtures.NO_SLEEP);

    private OrderRequest entry() {
        return OrderRequest.marketEntry("BTCUSDT", OrderSide.BUY, new BigDecimal("0.041"),
                ClientOrderIdFactory.create("sig-1", OrderPurpose.ENTRY, 0));
    }

    @Test
    @DisplayName("a lost response does not create a second order or a second position")
    void lostResponseDoesNotDoubleThePosition() throws Exception {
        exchange.loseNextResponse = true;

        OrderStatus result = placer.place(entry());

        assertEquals(1, exchange.placeOrderCalls,
                "the order was sent more than once after an ambiguous failure");
        assertTrue(result.hasFill());
        assertEquals(1, exchange.openPositions().size());
        assertEquals(0, exchange.openPositions().get(0).signedQuantity().compareTo(new BigDecimal("0.041")),
                "the position doubled: the ambiguous failure was retried blindly");
    }

    @Test
    @DisplayName("a lost response whose probes see nothing is NOT resent: a filled market id is reusable on Binance")
    void lostResponseWithBlindProbesIsNotResent() throws Exception {
        // Binance semantics: the first attempt FILLED, so its id is no longer among the open orders
        // and a second POST is accepted — the -4116 backstop the older test leans on does not exist
        // for a filled market order. The pre-send look-up and both probes miss the order, as they
        // do when the matching engine lags or the reads are 429'd.
        exchange.filledIdsReusable = true;
        exchange.loseNextResponse = true;
        exchange.hideNextQueries = 3;

        ExchangeException thrown = assertThrows(ExchangeException.class, () -> placer.place(entry()));

        assertTrue(thrown.ambiguous(), "the caller must hear 'unknown', never 'refused': " + thrown.getMessage());
        assertEquals(1, exchange.placeOrderCalls, "the market entry was sent again after an ambiguous send");
        assertEquals(1, exchange.openPositions().size());
        assertEquals(0, exchange.openPositions().get(0).signedQuantity().compareTo(new BigDecimal("0.041")),
                "the position doubled: a MARKET entry was resent on ignorance");
    }

    @Test
    @DisplayName("a reduce-only close IS resent after an ambiguous send: a duplicate cannot over-close")
    void reduceOnlyCloseIsStillResent() throws Exception {
        exchange.filledIdsReusable = true;
        placer.place(entry());
        OrderRequest close = OrderRequest.emergencyClose("BTCUSDT", OrderSide.SELL, new BigDecimal("0.041"),
                ClientOrderIdFactory.create("sig-1", OrderPurpose.EMERGENCY_CLOSE, 0));
        // The first send dies before the exchange records anything; the probes see nothing. For a
        // reducing order the placer must still try again — a flat symbol would answer -2022.
        exchange.failNextPlaceWith = ExchangeException.ambiguous("timeout", null);
        exchange.hideNextQueries = 3;

        OrderStatus result = placer.place(close);

        assertTrue(result.hasFill());
        assertTrue(exchange.openPositions().isEmpty(), "the close was not resent");
    }

    @Test
    @DisplayName("sending the identical request again is a no-op")
    void resendingIsANoOp() throws Exception {
        OrderStatus first = placer.place(entry());
        int callsAfterFirst = exchange.placeOrderCalls;

        OrderStatus second = placer.place(entry());

        assertEquals(callsAfterFirst, exchange.placeOrderCalls,
                "the second send reached the exchange; the query-before-send step did not run");
        assertEquals(first.exchangeOrderId(), second.exchangeOrderId());
        assertEquals(1, exchange.openPositions().size());
        assertEquals(0, exchange.openPositions().get(0).signedQuantity().compareTo(new BigDecimal("0.041")));
    }

    @Test
    @DisplayName("the exchange's own duplicate rejection is adopted, not treated as a failure")
    void duplicateRejectionIsAdopted() throws Exception {
        // A previous process — or a parallel deployment — already placed this exact order, and this
        // placer never saw it. The forced -4116 stands in for the exchange refusing the second send.
        exchange.placeOrder(entry());
        exchange.hideNextQueries = 1;   // the placer's query-before-send misses it
        exchange.failNextPlaceWith = ExchangeException.refused(
                "clientOrderId is duplicated", 400, BinanceErrorCodes.DUPLICATED_CLIENT_ORDER_ID);

        OrderStatus adopted = placer.place(entry());

        assertTrue(adopted.hasFill(), "the pre-existing order should have been adopted with its fill");
        assertEquals(1, exchange.openPositions().size(),
                "a duplicate rejection must not become a second position");
        assertEquals(0, exchange.openPositions().get(0).signedQuantity().compareTo(new BigDecimal("0.041")));
    }

    @Test
    @DisplayName("a definite refusal is propagated and never retried")
    void definiteRefusalIsNotRetried() {
        exchange.failNextPlaceWith = ExchangeException.refused(
                "Margin is insufficient.", 400, BinanceErrorCodes.MARGIN_NOT_SUFFICIENT);

        ExchangeException thrown = assertThrows(ExchangeException.class, () -> placer.place(entry()));

        assertEquals(BinanceErrorCodes.MARGIN_NOT_SUFFICIENT, thrown.exchangeCode());
        assertEquals(1, exchange.placeOrderCalls,
                "a refusal with an error code was retried; retrying a refusal only collects another one");
    }

    @Test
    @DisplayName("client order ids are a pure function of the order, not of when it was sent")
    void clientOrderIdsAreDeterministic() {
        String first = ClientOrderIdFactory.create("sig-1", OrderPurpose.ENTRY, 0);
        String again = ClientOrderIdFactory.create("sig-1", OrderPurpose.ENTRY, 0);
        assertEquals(first, again, "ids must be reproducible across processes and restarts");

        assertTrue(!first.equals(ClientOrderIdFactory.create("sig-1", OrderPurpose.STOP_LOSS, 0)));
        assertTrue(!first.equals(ClientOrderIdFactory.create("sig-2", OrderPurpose.ENTRY, 0)));
        assertTrue(!first.equals(ClientOrderIdFactory.create("sig-1", OrderPurpose.ENTRY, 1)));
    }

    @Test
    @DisplayName("re-executing the same plan does not open a second position")
    void reExecutingAPlanIsSafe() throws Exception {
        RiskEngine engine = ExecFixtures.engine();
        TradePlan plan = ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
        ExecutionCoordinator coordinator = ExecFixtures.coordinator(
                exchange, engine, new TradingHalt(), new ExecFixtures.RecordingAlerts());

        ExecutionCoordinator.Report first = coordinator.execute(plan);
        assertEquals(ExecutionCoordinator.Outcome.FILLED, first.outcome());
        BigDecimal positionAfterFirst = exchange.openPositions().get(0).signedQuantity();

        // A crash-and-replay: the same plan arrives again from the same signal.
        engine.registerClose(plan.symbol());
        ExecutionCoordinator.Report second = coordinator.execute(plan);

        assertEquals(0, exchange.openPositions().get(0).signedQuantity().compareTo(positionAfterFirst),
                "replaying a plan doubled the position; the deterministic ids did not hold");
        assertEquals(Side.LONG, second.plan().side());
    }
}
