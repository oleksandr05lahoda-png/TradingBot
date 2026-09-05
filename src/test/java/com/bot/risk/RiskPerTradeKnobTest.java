package com.bot.risk;

import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The one knob that moves the monthly figure: risk per trade, bounded by the 1% hard cap. */
class RiskPerTradeKnobTest {

    @Test
    @DisplayName("1% is accepted and doubles the size the stop implies")
    void onePercentDoublesTheSize() {
        RiskConfig config = RiskConfig.defaults().withRiskFractionPerTrade(0.01);
        TradePlan plan = assertInstanceOf(RiskDecision.Approved.class, RiskFixtures.engine(config)
                .evaluate(RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, RiskFixtures.NOON)).plan();
        // $10,000 x 1% = $100 of risk over R = 1,200 -> 0.0833, floored to 0.083 BTC (default 0.5% gives 0.041).
        assertEquals(0, plan.quantity().compareTo(new BigDecimal("0.083")));
        assertEquals(99.6, plan.riskUsd(), 1e-9);
    }

    @Test
    @DisplayName("above the hard cap the config refuses to exist at all")
    void aboveTheCapIsRefused() {
        assertThrows(IllegalArgumentException.class,
                () -> RiskConfig.defaults().withRiskFractionPerTrade(0.02));
    }

    @Test
    @DisplayName("the default stays 0.5%")
    void defaultIsHalfAPercent() {
        assertEquals(0.005, RiskConfig.defaults().riskFractionPerTrade(), 1e-12);
    }
}
