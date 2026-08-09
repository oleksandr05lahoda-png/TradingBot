package com.bot.risk;

import com.bot.core.InstrumentFilters;
import com.bot.core.Side;

import java.time.Instant;
import java.util.OptionalDouble;

/** Shared builders for the risk tests, so each test states only the thing it is about. */
final class RiskFixtures {

    static final Instant NOON = Instant.parse("2026-08-09T12:00:00Z");

    private RiskFixtures() {}

    /** BTC-like: 0.10 tick, 0.001 lot, $5 minimum notional. */
    static InstrumentFilters btcFilters() {
        return InstrumentFilters.of("BTCUSDT", "0.10", "0.001", "5");
    }

    /** A coarse instrument, for the cases where lot alignment is the point. */
    static InstrumentFilters coarseFilters() {
        return InstrumentFilters.of("COARSEUSDT", "1", "1", "5");
    }

    static TradeRequest request(Side side, double entry, double stop, int leverage) {
        return request(side, entry, stop, leverage, btcFilters());
    }

    static TradeRequest request(Side side, double entry, double stop, int leverage, InstrumentFilters filters) {
        return new TradeRequest("sig-1", filters.symbol(), side, entry,
                OptionalDouble.of(stop), OptionalDouble.empty(), leverage,
                filters, MarginTierTable.conservativeDefault());
    }

    static RiskEngine engine(RiskConfig config) {
        return new RiskEngine(config, new ExposureBook(), new DailyLossKillSwitch(config.dailyLossFractionLimit()));
    }

    static RiskEngine engine() {
        return engine(RiskConfig.defaults());
    }

    /** The seed the property tests draw from; overridable with {@code -Dbot.test.seed=...}. */
    static long seed() {
        return Long.parseLong(System.getProperty("bot.test.seed", "20260809"));
    }
}
