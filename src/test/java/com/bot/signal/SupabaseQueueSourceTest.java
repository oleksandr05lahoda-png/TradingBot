package com.bot.signal;

import com.bot.core.Side;
import com.bot.risk.RiskConstants;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The queue's row-to-signal contract, tested without a queue.
 *
 * <p>The leverage case is the one that matters: the two permitted signal sources must agree about
 * what a bad row means. An earlier version clamped an over-leveraged row down to the maximum while
 * the manual input refused the identical mistake — so a queue could publish 20x rows indefinitely
 * with nothing in the logs to say the request had ever been made.
 */
class SupabaseQueueSourceTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-09T12:00:00Z"), ZoneOffset.UTC);

    private final SupabaseQueueSource source =
            new SupabaseQueueSource("https://example.invalid", "not-a-real-key", 10, 3, CLOCK);

    private static JSONObject row(String json) {
        return new JSONObject(json);
    }

    @Test
    @DisplayName("a well-formed row becomes a signal")
    void wellFormedRow() {
        Signal signal = source.toSignal(row("""
                {"id": 42, "symbol": "btcusdt", "side": "long", "entry": 64000, "sl": 62800, "leverage": 3}"""));

        assertEquals("sbq-42", signal.id());
        assertEquals("BTCUSDT", signal.symbol());
        assertEquals(Side.LONG, signal.side());
        assertEquals(64_000.0, signal.entryPrice());
        assertEquals(62_800.0, signal.structuralStopPrice().orElseThrow());
        assertEquals(3, signal.leverage());
    }

    @Test
    @DisplayName("a missing leverage falls back to the configured default")
    void leverageFallsBackToTheDefault() {
        Signal signal = source.toSignal(row("""
                {"id": 7, "symbol": "ETHUSDT", "side": "SHORT", "entry": 3120, "atr": 45}"""));
        assertEquals(3, signal.leverage());
        assertTrue(signal.structuralStopPrice().isEmpty());
        assertEquals(45.0, signal.atr().orElseThrow());
    }

    @Test
    @DisplayName("excessive leverage is refused, not silently clamped")
    void excessiveLeverageIsRefused() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> source.toSignal(row("""
                        {"id": 9, "symbol": "BTCUSDT", "side": "LONG", "entry": 64000, "sl": 62800,
                         "leverage": 20}""")));
        assertTrue(thrown.getMessage().contains("outside [1, " + RiskConstants.MAX_LEVERAGE + "]"),
                thrown.getMessage());
    }

    @Test
    @DisplayName("a row with neither a stop nor an ATR is refused")
    void rowWithNoStopSourceIsRefused() {
        assertThrows(IllegalArgumentException.class,
                () -> source.toSignal(row("""
                        {"id": 11, "symbol": "BTCUSDT", "side": "LONG", "entry": 64000}""")));
    }

    @Test
    @DisplayName("a null stop column is treated as absent, not as zero")
    void nullStopIsAbsent() {
        Signal signal = source.toSignal(row("""
                {"id": 12, "symbol": "BTCUSDT", "side": "LONG", "entry": 64000, "sl": null, "atr": 500}"""));
        assertTrue(signal.structuralStopPrice().isEmpty());
        assertEquals(500.0, signal.atr().orElseThrow());
    }

    @Test
    @DisplayName("an unknown side is refused rather than guessed")
    void unknownSideIsRefused() {
        assertThrows(IllegalArgumentException.class,
                () -> source.toSignal(row("""
                        {"id": 13, "symbol": "BTCUSDT", "side": "SIDEWAYS", "entry": 1, "sl": 0.5}""")));
    }
}
