package com.bot.risk;

import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stops: the two origins, the geometry each must satisfy, and the refusal when neither is available.
 */
class StopLossTest {

    @Test
    @DisplayName("a structural stop is preferred over the ATR fallback")
    void structuralWinsOverAtr() {
        StopLoss stop = StopLoss.resolve(Side.LONG, 100, OptionalDouble.of(95), OptionalDouble.of(3), 2.0);
        assertInstanceOf(StopLoss.Structural.class, stop);
        assertEquals(95.0, stop.price());
        assertEquals("structural", stop.origin());
    }

    @Test
    @DisplayName("the ATR fallback is used only when no structural level arrived")
    void atrUsedWhenNoStructuralLevel() {
        StopLoss stop = StopLoss.resolve(Side.LONG, 100, OptionalDouble.empty(), OptionalDouble.of(3), 2.0);
        assertInstanceOf(StopLoss.AtrFallback.class, stop);
        assertEquals(94.0, stop.price(), 1e-12);   // 100 - 3 * 2
        assertEquals(6.0, stop.distance(), 1e-12);
    }

    @Test
    @DisplayName("a short's ATR stop sits above entry")
    void atrFallbackForShort() {
        StopLoss stop = StopLoss.resolve(Side.SHORT, 100, OptionalDouble.empty(), OptionalDouble.of(3), 2.0);
        assertEquals(106.0, stop.price(), 1e-12);
    }

    @Test
    @DisplayName("neither a structural level nor an ATR means no trade, not a default stop")
    void noStopMeansRefusal() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> StopLoss.resolve(Side.LONG, 100, OptionalDouble.empty(), OptionalDouble.empty(), 2.0));
        assertTrue(thrown.getMessage().contains("without a stop"), thrown.getMessage());
    }

    @Test
    @DisplayName("a structural level on the wrong side throws rather than falling back to ATR")
    void badStructuralGeometryIsNotSilentlyRepaired() {
        // A producer that sends "LONG at 100, stop at 105" is broken. Quietly substituting the ATR
        // stop would hide that and keep trading on its other output.
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> StopLoss.resolve(Side.LONG, 100, OptionalDouble.of(105), OptionalDouble.of(3), 2.0));
        assertTrue(thrown.getMessage().contains("wrong side of entry"), thrown.getMessage());
    }

    @Test
    @DisplayName("an ATR wide enough to put the stop at or below zero is refused")
    void atrCannotProduceANonPositiveStop() {
        assertThrows(IllegalArgumentException.class,
                () -> StopLoss.atrFallback(Side.LONG, 100, 60, 2.0));
    }

    @Test
    @DisplayName("an ATR stop cannot claim a price that is not the ATR distance")
    void atrPriceIsReDerivedNotTrusted() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new StopLoss.AtrFallback(Side.LONG, 100, 3, 2.0, 97.0));
        assertTrue(thrown.getMessage().contains("does not match the ATR distance"), thrown.getMessage());
    }

    @Test
    @DisplayName("distance and distance-as-a-fraction agree with the prices")
    void distanceAccessors() {
        StopLoss stop = StopLoss.structural(Side.SHORT, 200, 210);
        assertEquals(10.0, stop.distance(), 1e-12);
        assertEquals(0.05, stop.distanceFraction(), 1e-12);
    }

    @Test
    @DisplayName("the sealed hierarchy has exactly the two permitted origins")
    void exactlyTwoOrigins() {
        Class<?>[] permitted = StopLoss.class.getPermittedSubclasses();
        assertEquals(2, permitted.length,
                "StopLoss grew a third origin. Stops come from the signal or from volatility; a third "
                        + "kind would be a strategy decision made inside the risk core.");
    }
}
