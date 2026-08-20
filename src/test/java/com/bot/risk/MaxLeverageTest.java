package com.bot.risk;

import com.bot.core.Side;
import com.bot.signal.ManualInput;
import com.bot.signal.Signal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.List;
import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The leverage ceiling, with one test per layer that could be used to get around it: config, signal,
 * typed line, plan constructor, and the exchange's own bracket cap.
 */
class MaxLeverageTest {

    @Test
    @DisplayName("the hard constant is 5")
    void hardConstantIsFive() {
        assertEquals(5, RiskConstants.MAX_LEVERAGE);
    }

    @Test
    @DisplayName("a config cannot be built above the hard cap")
    void configCannotExceedHardCap() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> RiskConfig.defaults().withMaxLeverage(RiskConstants.MAX_LEVERAGE + 1));
        assertTrue(thrown.getMessage().contains("exceeds the hard cap"), thrown.getMessage());
    }

    @Test
    @DisplayName("the engine refuses a request above the configured cap")
    void engineRefusesRequestAboveCap() {
        RiskEngine engine = RiskFixtures.engine(RiskConfig.defaults().withMaxLeverage(3));
        RiskDecision decision = engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 4), 10_000, RiskFixtures.NOON);

        RiskDecision.Rejected rejected = assertInstanceOf(RiskDecision.Rejected.class, decision);
        assertEquals(RejectReason.LEVERAGE_ABOVE_MAX, rejected.reason());
    }

    @Test
    @DisplayName("a signal cannot carry leverage above the hard cap")
    void signalCannotCarryExcessiveLeverage() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new Signal("s", "BTCUSDT", Side.LONG, 100, OptionalDouble.of(95),
                        OptionalDouble.empty(), RiskConstants.MAX_LEVERAGE + 1, RiskFixtures.NOON));
        assertTrue(thrown.getMessage().contains("hard cap"), thrown.getMessage());
    }

    @Test
    @DisplayName("a typed line asking for excessive leverage is rejected, not clamped")
    void manualInputRefusesExcessiveLeverage() {
        Clock clock = Clock.fixed(RiskFixtures.NOON, ZoneOffset.UTC);
        assertThrows(IllegalArgumentException.class,
                () -> ManualInput.parse("BTCUSDT LONG entry=64000 stop=62800 lev=20", clock, 3, 1));
    }

    @Test
    @DisplayName("a plan cannot be constructed above the hard cap, even from inside the risk package")
    void planConstructorRefusesExcessiveLeverage() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> new TradePlan(
                "sig", "BTCUSDT", Side.LONG,
                new BigDecimal("64000.0"), new BigDecimal("62800.0"),
                StopLoss.structural(Side.LONG, 64_000, 62_800),
                new BigDecimal("0.041"), RiskConstants.MAX_LEVERAGE + 1,
                2624.0, 437.3, 50.0, 0.005, 55_000, 0.4, List.of(),
                RiskFixtures.btcFilters(), "test"));
        assertTrue(thrown.getMessage().contains("outside [1, 5]"), thrown.getMessage());
    }

    @Test
    @DisplayName("the exchange's own bracket cap wins when it is stricter than 5x")
    void exchangeBracketCapIsHonoured() {
        // The conservative table allows only 4x above a notional of 250,000, and 1x above 1,000,000.
        MarginTierTable tiers = MarginTierTable.conservativeDefault();
        assertEquals(5, tiers.maxLeverageAt(200_000));
        assertEquals(4, tiers.maxLeverageAt(500_000));
        assertEquals(1, tiers.maxLeverageAt(2_000_000));

        RiskEngine engine = RiskFixtures.engine(RiskConfig.defaults()
                .withMaxNotionalPerTrade(100.0, Double.POSITIVE_INFINITY)
                .withExposureFractions(100.0, 100.0));
        // $2m balance, 0.5% risk, a very tight stop -> a notional big enough to leave the 5x bracket.
        RiskDecision decision = engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 63_900, 5), 2_000_000, RiskFixtures.NOON);

        RiskDecision.Rejected rejected = assertInstanceOf(RiskDecision.Rejected.class, decision);
        assertEquals(RejectReason.LEVERAGE_ABOVE_EXCHANGE_BRACKET, rejected.reason());
    }
}
