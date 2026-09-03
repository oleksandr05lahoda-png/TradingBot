package com.bot.exec.binance;

import com.bot.exec.ExchangeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The transport's verdict on a non-2xx answer decides whether an order that DID execute can be
 * dropped as "never landed". Binance says "execution status unknown" in words (-1007, -1006) and
 * delivers them with a 4xx; by HTTP status alone they read as refusals (audit 03.09).
 */
class BinanceErrorClassificationTest {

    private static final String TIMEOUT_BODY = "{\"code\":-1007,\"msg\":\"Timeout waiting for response "
            + "from backend server. Send status unknown; execution status unknown.\"}";

    @Test
    @DisplayName("-1007 on a 4xx is ambiguous: the placer must probe, never conclude 'did not land'")
    void executionUnknownOnFourHundredIsAmbiguous() {
        ExchangeException e = BinanceFuturesAdapter.classifyError("POST", "/fapi/v1/order", 400, TIMEOUT_BODY);
        assertTrue(e.ambiguous(), e.toString());
        assertFalse(e.retryableWithoutRisk(), "an ambiguous send is never a free retry");
    }

    @Test
    @DisplayName("-1006 is the same verdict")
    void unexpectedResponseIsAmbiguous() {
        ExchangeException e = BinanceFuturesAdapter.classifyError("POST", "/fapi/v1/algoOrder", 400,
                "{\"code\":-1006,\"msg\":\"An unexpected response was received from the message bus. "
                        + "Execution status unknown.\"}");
        assertTrue(e.ambiguous(), e.toString());
    }

    @Test
    @DisplayName("a coded refusal stays a refusal, with its code and status intact")
    void codedRefusalStaysRefused() {
        ExchangeException e = BinanceFuturesAdapter.classifyError("POST", "/fapi/v1/algoOrder", 400,
                "{\"code\":-2021,\"msg\":\"Order would immediately trigger.\"}");
        assertFalse(e.ambiguous());
        assertEquals(-2021, e.exchangeCode());
        assertEquals(400, e.httpStatus());
        assertFalse(e.retryableWithoutRisk(), "a coded refusal is not retried blindly");
    }

    @Test
    @DisplayName("every 5xx is ambiguous whatever the body says")
    void fiveHundredIsAmbiguous() {
        assertTrue(BinanceFuturesAdapter.classifyError("POST", "/fapi/v1/order", 502, "<html>bad gateway</html>")
                .ambiguous());
    }

    @Test
    @DisplayName("a non-JSON 4xx body is a refusal carrying the raw text")
    void nonJsonBodyIsRefused() {
        ExchangeException e = BinanceFuturesAdapter.classifyError("GET", "/fapi/v1/order", 403, "forbidden");
        assertFalse(e.ambiguous());
        assertTrue(e.getMessage().contains("forbidden"));
        assertEquals(0, e.exchangeCode());
    }

    @Test
    @DisplayName("'never sent' is the one refusal a caller may retry without doubling anything")
    void neverSentIsRetryable() {
        ExchangeException e = ExchangeException.neverSent("could not connect", null);
        assertFalse(e.ambiguous());
        assertTrue(e.retryableWithoutRisk());
        assertTrue(ExchangeException.refused("rate limited", 429, 0).retryableWithoutRisk(),
                "a rate limit is slept out by the limiter before the next send");
        assertFalse(ExchangeException.refused("dup", 400, BinanceErrorCodes.DUPLICATED_CLIENT_ORDER_ID)
                .retryableWithoutRisk());
    }
}
