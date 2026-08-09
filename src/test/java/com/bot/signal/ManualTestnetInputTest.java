package com.bot.signal;

import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The manual input format, and what it refuses. */
class ManualTestnetInputTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-09T12:00:00Z"), ZoneOffset.UTC);

    @Test
    @DisplayName("a structural stop line parses into a signal")
    void parsesAStructuralStopLine() {
        Signal signal = ManualTestnetInput.parse("BTCUSDT LONG entry=64000 stop=62800 lev=3", CLOCK, 2, 1);

        assertEquals("BTCUSDT", signal.symbol());
        assertEquals(Side.LONG, signal.side());
        assertEquals(64_000.0, signal.entryPrice());
        assertEquals(62_800.0, signal.structuralStopPrice().orElseThrow());
        assertTrue(signal.atr().isEmpty());
        assertEquals(3, signal.leverage());
    }

    @Test
    @DisplayName("an ATR line parses, and leverage falls back to the configured default")
    void parsesAnAtrLine() {
        Signal signal = ManualTestnetInput.parse("ethusdt short entry=3120 atr=45", CLOCK, 2, 7);

        assertEquals("ETHUSDT", signal.symbol());
        assertEquals(Side.SHORT, signal.side());
        assertTrue(signal.structuralStopPrice().isEmpty());
        assertEquals(45.0, signal.atr().orElseThrow());
        assertEquals(2, signal.leverage());
    }

    @Test
    @DisplayName("a line with neither a stop nor an ATR is refused at the point of typing")
    void refusesALineWithNoStopSource() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> ManualTestnetInput.parse("BTCUSDT LONG entry=64000", CLOCK, 2, 1));
        assertTrue(thrown.getMessage().contains("stop= or atr="), thrown.getMessage());
    }

    @Test
    @DisplayName("malformed input is refused with a message naming the problem")
    void refusesMalformedInput() {
        assertThrows(IllegalArgumentException.class,
                () -> ManualTestnetInput.parse("BTCUSDT SIDEWAYS entry=1 stop=0.5", CLOCK, 2, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ManualTestnetInput.parse("BTCUSDT LONG entry=abc stop=1", CLOCK, 2, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ManualTestnetInput.parse("BTCUSDT LONG entry=1 stop=0.5 wat=9", CLOCK, 2, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ManualTestnetInput.parse("BTCUSDT LONG", CLOCK, 2, 1));
    }

    @Test
    @DisplayName("an explicit id is used verbatim, so a replayed script is one trade and not two")
    void explicitIdIsUsedVerbatim() {
        Signal signal = ManualTestnetInput.parse(
                "BTCUSDT LONG entry=64000 stop=62800 id=morning-btc", CLOCK, 2, 5);
        assertEquals("morning-btc", signal.id());
    }

    @Test
    @DisplayName("reading a script skips blanks and comments and keeps order")
    void readsAScript() throws Exception {
        String script = String.join("\n",
                "# a comment",
                "",
                "BTCUSDT LONG entry=64000 stop=62800",
                "   ",
                "ETHUSDT SHORT entry=3120 atr=45 lev=2",
                "GARBAGE",
                "SOLUSDT LONG entry=150 stop=145");

        try (ManualTestnetInput input = ManualTestnetInput.fromReader(new StringReader(script), CLOCK, 3)) {
            List<Signal> signals = input.poll();

            assertEquals(3, signals.size(), "the malformed line should be skipped, not fatal");
            assertEquals("BTCUSDT", signals.get(0).symbol());
            assertEquals("ETHUSDT", signals.get(1).symbol());
            assertEquals("SOLUSDT", signals.get(2).symbol());
            assertTrue(input.poll().isEmpty(), "a drained script yields nothing further");
        }
    }

    @Test
    @DisplayName("a CLOSE line becomes a close request, not a signal")
    void closeLineIsRouted() throws Exception {
        String script = String.join("\n",
                "BTCUSDT LONG entry=64000 stop=62800",
                "CLOSE ETHUSDT reason=time-stop",
                "SOLUSDT LONG entry=150 stop=145");

        try (ManualTestnetInput input = ManualTestnetInput.fromReader(new StringReader(script), CLOCK, 3)) {
            List<CloseRequest> closes = input.pollCloses();
            List<Signal> signals = input.poll();

            assertEquals(1, closes.size());
            assertEquals("ETHUSDT", closes.get(0).symbol());
            assertEquals("time-stop", closes.get(0).reason());

            assertEquals(2, signals.size(), "the two open lines are still signals");
            assertEquals("BTCUSDT", signals.get(0).symbol());
            assertEquals("SOLUSDT", signals.get(1).symbol());
        }
    }

    @Test
    @DisplayName("a CLOSE line with no symbol is refused")
    void closeNeedsASymbol() {
        assertThrows(IllegalArgumentException.class,
                () -> ManualTestnetInput.parseClose("CLOSE", CLOCK, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ManualTestnetInput.parseClose("CLOSE BTCUSDT wat=1", CLOCK, 1));
    }

    @Test
    @DisplayName("generated ids are distinct per line so two identical trades stay two trades")
    void generatedIdsAreDistinctPerLine() throws Exception {
        String script = "BTCUSDT LONG entry=64000 stop=62800\nBTCUSDT LONG entry=64000 stop=62800";
        try (ManualTestnetInput input = ManualTestnetInput.fromReader(new StringReader(script), CLOCK, 3)) {
            List<Signal> signals = input.poll();
            assertEquals(2, signals.size());
            assertFalse(signals.get(0).id().equals(signals.get(1).id()));
        }
    }

}
