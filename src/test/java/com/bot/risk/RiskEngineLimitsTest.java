package com.bot.risk;

import com.bot.core.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.OptionalDouble;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The gate's limits: each one refuses, and the ceilings can only ever shrink a position. */
class RiskEngineLimitsTest {

    private static RiskDecision.Rejected reject(RiskDecision decision) {
        return assertInstanceOf(RiskDecision.Rejected.class, decision);
    }

    private static TradePlan approve(RiskDecision decision) {
        return assertInstanceOf(RiskDecision.Approved.class, decision).plan();
    }

    @Test
    @DisplayName("a healthy trade is approved with the size the stop implies")
    void healthyTradeIsApproved() {
        // $10,000 at 0.5% = $50 of risk; R = 1,200 -> 0.041666 BTC, floored to the 0.001 lot = 0.041.
        TradePlan plan = approve(RiskFixtures.engine()
                .evaluate(RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, RiskFixtures.NOON));

        assertEquals(0, plan.quantity().compareTo(new BigDecimal("0.041")));
        assertEquals(49.2, plan.riskUsd(), 1e-9);          // 0.041 * 1,200
        assertTrue(plan.riskUsd() <= 50.0, "lot rounding must never round the risk up");
        assertEquals(2624.0, plan.notionalUsd(), 1e-9);    // 0.041 * 64,000
        assertEquals(2, plan.takeProfits().size());
        assertEquals("structural", plan.stop().origin());
    }

    @Test
    @DisplayName("an unreadable balance refuses rather than being treated as zero")
    void unreadableBalanceFailsClosed() {
        for (double balance : new double[]{Double.NaN, 0, -1, Double.POSITIVE_INFINITY}) {
            RiskDecision.Rejected rejected = reject(RiskFixtures.engine()
                    .evaluate(RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), balance, RiskFixtures.NOON));
            assertEquals(RejectReason.INVALID_INPUT, rejected.reason(), "balance " + balance);
        }
    }

    @Test
    @DisplayName("a signal with neither a stop nor an ATR is refused")
    void noStopIsRefused() {
        TradeRequest request = new TradeRequest("sig", "BTCUSDT", Side.LONG, 64_000,
                OptionalDouble.empty(), OptionalDouble.empty(), 3,
                RiskFixtures.btcFilters(), MarginTierTable.conservativeDefault());
        assertEquals(RejectReason.NO_STOP_AVAILABLE,
                reject(RiskFixtures.engine().evaluate(request, 10_000, RiskFixtures.NOON)).reason());
    }

    @Test
    @DisplayName("a second position on the same symbol is refused")
    void oneSymbolOnePosition() {
        RiskEngine engine = RiskFixtures.engine();
        TradePlan first = approve(engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, RiskFixtures.NOON));
        engine.registerFill(first, first.quantity(), 64_000, "bt-s0-fixture");

        assertEquals(RejectReason.POSITION_ALREADY_OPEN, reject(engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, RiskFixtures.NOON)).reason());
    }

    @Test
    @DisplayName("the concurrent position limit refuses the one that would exceed it")
    void concurrentPositionLimit() {
        RiskEngine engine = RiskFixtures.engine(RiskConfig.defaults().withMaxConcurrentPositions(1));
        TradePlan first = approve(engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, RiskFixtures.NOON));
        engine.registerFill(first, first.quantity(), 64_000, "bt-s0-fixture");

        TradeRequest other = new TradeRequest("sig-2", "COARSEUSDT", Side.LONG, 1_000,
                OptionalDouble.of(950), OptionalDouble.empty(), 3,
                RiskFixtures.coarseFilters(), MarginTierTable.conservativeDefault());
        assertEquals(RejectReason.MAX_CONCURRENT_POSITIONS,
                reject(engine.evaluate(other, 10_000, RiskFixtures.NOON)).reason());
    }

    @Test
    @DisplayName("long and short exposure are capped separately and never netted")
    void longAndShortExposureAreSeparate() {
        // A long at the cap must not create room for another long, but must leave shorts untouched.
        RiskEngine engine = RiskFixtures.engine(RiskConfig.defaults()
                .withExposureFractions(0.30, 2.0)
                .withMaxConcurrentPositions(5));

        TradePlan first = approve(engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, RiskFixtures.NOON));
        engine.registerFill(first, first.quantity(), 64_000, "bt-s0-fixture");
        assertEquals(2624.0, engine.book().longExposureUsd(), 1e-9);
        assertEquals(0.0, engine.book().shortExposureUsd(), 1e-9);

        // Long headroom is now 10,000 * 0.30 - 2,624 = 376, so the next long is squeezed into it.
        // Unconstrained it would be $500 of notional (5 units at 100).
        TradeRequest anotherLong = new TradeRequest("sig-long-2", "COARSEUSDT", Side.LONG, 100,
                OptionalDouble.of(90), OptionalDouble.empty(), 3,
                RiskFixtures.coarseFilters(), MarginTierTable.conservativeDefault());
        TradePlan squeezed = approve(engine.evaluate(anotherLong, 10_000, RiskFixtures.NOON));
        assertTrue(squeezed.notionalUsd() <= 376.0,
                "the second long should be limited to the remaining long headroom, got "
                        + squeezed.notionalUsd());
        assertTrue(squeezed.sizingNote().contains("LONG exposure headroom"), squeezed.sizingNote());

        // The same trade as a short is untouched by the long book: 5 units at 100 = $500.
        TradeRequest aShort = new TradeRequest("sig-short", "COARSEUSDT", Side.SHORT, 100,
                OptionalDouble.of(110), OptionalDouble.empty(), 2,
                RiskFixtures.coarseFilters(), MarginTierTable.conservativeDefault());
        TradePlan shortPlan = approve(engine.evaluate(aShort, 10_000, RiskFixtures.NOON));
        assertEquals(500.0, shortPlan.notionalUsd(), 1e-9,
                "a short must not be shrunk by long exposure — they are separate books");
    }

    @Test
    @DisplayName("the per-trade notional cap reduces the size instead of moving the stop")
    void perTradeNotionalCapReducesSize() {
        RiskEngine engine = RiskFixtures.engine(RiskConfig.defaults()
                .withMaxNotionalPerTrade(0.10, Double.POSITIVE_INFINITY));

        TradePlan plan = approve(engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, RiskFixtures.NOON));

        assertTrue(plan.notionalUsd() <= 1_000.0 + 1e-9,
                "notional " + plan.notionalUsd() + " exceeds the 10% cap");
        assertTrue(plan.riskUsd() < 50.0, "the cap should have reduced the risk below the budget");
        assertEquals(0, plan.stopPrice().compareTo(new BigDecimal("62800.0")),
                "a notional cap must never move the stop — only the size");
        assertTrue(plan.sizingNote().contains("per-trade notional cap"), plan.sizingNote());
    }

    @Test
    @DisplayName("an absolute notional ceiling applies alongside the fractional one")
    void absoluteNotionalCeilingAlsoApplies() {
        RiskEngine engine = RiskFixtures.engine(RiskConfig.defaults()
                .withMaxNotionalPerTrade(10.0, 500.0));
        TradePlan plan = approve(engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, RiskFixtures.NOON));
        assertTrue(plan.notionalUsd() <= 500.0 + 1e-9, "notional " + plan.notionalUsd());
    }

    @Test
    @DisplayName("PROPERTY: no ceiling ever increases the size or the risk")
    void ceilingsOnlyEverReduce() {
        Random random = new Random(RiskFixtures.seed() + 21);
        int checked = 0;

        for (int i = 0; i < 6_000; i++) {
            double balance = 1_000 + random.nextDouble() * 200_000;
            double entry = 100 + random.nextDouble() * 60_000;
            double stopFraction = 0.005 + random.nextDouble() * 0.15;
            double stop = entry * (1 - stopFraction);
            int leverage = 1 + random.nextInt(RiskConstants.MAX_LEVERAGE);

            RiskConfig config = RiskConfig.defaults()
                    .withMaxNotionalPerTrade(0.05 + random.nextDouble() * 2.0, Double.POSITIVE_INFINITY);
            RiskDecision decision = RiskFixtures.engine(config)
                    .evaluate(RiskFixtures.request(Side.LONG, entry, stop, leverage), balance, RiskFixtures.NOON);
            if (!(decision instanceof RiskDecision.Approved approved)) continue;
            checked++;

            TradePlan plan = approved.plan();
            double unclampedQty = PositionSizer.quantityForRisk(
                    balance, config.riskFractionPerTrade(),
                    plan.entryPrice().doubleValue(), plan.stopPrice().doubleValue());

            assertTrue(plan.quantity().doubleValue() <= unclampedQty + 1e-12,
                    "a ceiling produced a LARGER position than the stop implied: " + plan);
            assertTrue(plan.riskUsd() <= balance * config.riskFractionPerTrade() * (1 + 1e-9),
                    "approved plan risks more than the budget: " + plan);
            assertTrue(plan.riskFractionOfBalance() <= RiskConstants.MAX_RISK_FRACTION_PER_TRADE + 1e-12,
                    "approved plan is above the hard risk cap: " + plan);
        }
        assertTrue(checked > 500, "only " + checked + " plans were approved; the sweep proves little");
    }

    @Test
    @DisplayName("a position too small for the exchange's minimum lot is refused with an actionable message")
    void belowMinimumLotIsRefusedUsefully() {
        RiskDecision.Rejected rejected = reject(RiskFixtures.engine()
                .evaluate(RiskFixtures.request(Side.LONG, 64_000, 32_000, 2), 100, RiskFixtures.NOON));
        assertEquals(RejectReason.BELOW_MIN_QUANTITY, rejected.reason());
        assertTrue(rejected.detail().contains("balance of about"),
                "the refusal should say what balance would make this tradable: " + rejected.detail());
    }

    @Test
    @DisplayName("margin utilisation caps how much of the balance one position may lock up")
    void marginUtilisationIsCapped() {
        RiskEngine engine = RiskFixtures.engine();
        TradePlan plan = approve(engine.evaluate(
                RiskFixtures.request(Side.LONG, 64_000, 62_800, 3), 10_000, RiskFixtures.NOON));
        assertTrue(plan.initialMarginUsd() <= 10_000 * 0.50,
                "initial margin " + plan.initialMarginUsd() + " exceeds half the balance");
        assertEquals(plan.notionalUsd() / 3, plan.initialMarginUsd(), 1e-9);
    }

    @Test
    @DisplayName("the tick-aligned stop is the one every number is computed from")
    void riskIsComputedFromTheTickAlignedStop() {
        // 62,800.037 is not a multiple of the 0.10 tick; it rounds towards entry, to 62,800.10.
        TradeRequest request = new TradeRequest("sig", "BTCUSDT", Side.LONG, 64_000,
                OptionalDouble.of(62_800.037), OptionalDouble.empty(), 3,
                RiskFixtures.btcFilters(), MarginTierTable.conservativeDefault());
        TradePlan plan = approve(RiskFixtures.engine().evaluate(request, 10_000, RiskFixtures.NOON));

        assertEquals(0, plan.stopPrice().compareTo(new BigDecimal("62800.1")));
        assertEquals(62_800.037, plan.stop().price(), 1e-9, "the source level is preserved for the audit trail");
        assertEquals(plan.quantity().doubleValue() * (64_000 - 62_800.1), plan.riskUsd(), 1e-9);
    }
}
