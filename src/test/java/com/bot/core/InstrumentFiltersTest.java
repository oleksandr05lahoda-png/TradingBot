package com.bot.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tick and lot arithmetic — the quiet source of rejected orders. */
class InstrumentFiltersTest {

    private final InstrumentFilters btc = InstrumentFilters.of("BTCUSDT", "0.10", "0.001", "5");

    @Test
    @DisplayName("quantity always rounds down to the lot step")
    void quantityRoundsDown() {
        assertEquals(0, btc.quantizeQuantityDown(0.0419999).compareTo(new BigDecimal("0.041")));
        assertEquals(0, btc.quantizeQuantityDown(0.0410000).compareTo(new BigDecimal("0.041")));
        assertEquals(0, btc.quantizeQuantityDown(0.0009999).compareTo(BigDecimal.ZERO));
    }

    @Test
    @DisplayName("PROPERTY: quantisation never rounds a size up, which would exceed the intended risk")
    void quantisationNeverIncreasesSize() {
        Random random = new Random(4242);
        for (int i = 0; i < 50_000; i++) {
            double raw = random.nextDouble() * 1_000;
            BigDecimal quantized = btc.quantizeQuantityDown(raw);
            assertTrue(quantized.doubleValue() <= raw + 1e-12,
                    "rounding produced a larger size than requested: " + raw + " -> " + quantized);
            assertTrue(btc.isQuantityOnStep(quantized), quantized + " is not on the lot step");
        }
    }

    @Test
    @DisplayName("a long's stop rounds up to the tick, a short's rounds down — both towards entry")
    void stopsRoundTowardsEntry() {
        assertEquals(0, btc.quantizeStopPrice(Side.LONG, 62_800.037).compareTo(new BigDecimal("62800.1")));
        assertEquals(0, btc.quantizeStopPrice(Side.SHORT, 65_200.037).compareTo(new BigDecimal("65200.0")));
    }

    @Test
    @DisplayName("take-profits round towards entry, to the nearer and more reachable tick")
    void takeProfitsRoundTowardsEntry() {
        assertEquals(0, btc.quantizeTakeProfitPrice(Side.LONG, 65_800.09).compareTo(new BigDecimal("65800.0")));
        assertEquals(0, btc.quantizeTakeProfitPrice(Side.SHORT, 62_199.91).compareTo(new BigDecimal("62200.0")));
    }

    @Test
    @DisplayName("quantised prices really are multiples of the tick, without floating point residue")
    void quantisedPricesAreExact() {
        Random random = new Random(99);
        for (int i = 0; i < 20_000; i++) {
            double raw = 1 + random.nextDouble() * 100_000;
            BigDecimal price = btc.quantizePrice(raw, RoundingMode.FLOOR);
            assertTrue(btc.isPriceOnTick(price), price + " is not a multiple of " + btc.tickSize());
            assertTrue(price.doubleValue() <= raw + 1e-9);
        }
    }

    @Test
    @DisplayName("minimum notional is enforced against price times quantity")
    void minimumNotional() {
        assertTrue(btc.meetsMinNotional(new BigDecimal("64000"), new BigDecimal("0.001")));
        assertFalse(btc.meetsMinNotional(new BigDecimal("1000"), new BigDecimal("0.001")));
    }

    @Test
    @DisplayName("smallestTradableQuantity clears both the lot minimum and the notional minimum")
    void smallestTradableQuantityIsActuallyTradable() {
        InstrumentFilters cheap = InstrumentFilters.of("CHEAPUSDT", "0.0001", "0.1", "5");
        BigDecimal smallest = cheap.smallestTradableQuantity(2.0);
        assertTrue(cheap.isQuantityOnStep(smallest), smallest + " is not on the lot step");
        assertTrue(cheap.isQuantityInRange(smallest, true));
        assertTrue(cheap.meetsMinNotional(new BigDecimal("2.0"), smallest),
                smallest + " at 2.0 does not clear the $5 minimum notional");
    }

    @Test
    @DisplayName("filters whose precision cannot represent their own tick are refused")
    void inconsistentPrecisionIsRefused() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new InstrumentFilters("BADUSDT",
                        new BigDecimal("0.001"), new BigDecimal("0.001"), new BigDecimal("100000"),
                        new BigDecimal("0.001"), new BigDecimal("0.001"), new BigDecimal("1000"),
                        new BigDecimal("1000"), new BigDecimal("5"),
                        2,   // pricePrecision 2 cannot express a 0.001 tick
                        3));
        assertTrue(thrown.getMessage().contains("cannot represent tickSize"), thrown.getMessage());
    }

    @Test
    @DisplayName("MARKET orders honour the separate market lot ceiling")
    void marketLotCeilingIsSeparate() {
        InstrumentFilters filters = new InstrumentFilters("CAPUSDT",
                new BigDecimal("0.1"), new BigDecimal("0.1"), new BigDecimal("100000"),
                new BigDecimal("0.001"), new BigDecimal("0.001"), new BigDecimal("1000"),
                new BigDecimal("10"), new BigDecimal("5"), 1, 3);

        assertTrue(filters.isQuantityInRange(new BigDecimal("50"), false), "50 is fine as a limit order");
        assertFalse(filters.isQuantityInRange(new BigDecimal("50"), true),
                "50 exceeds MARKET_LOT_SIZE and must be refused for a market order");
    }
}
