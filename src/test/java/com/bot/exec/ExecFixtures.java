package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.core.Side;
import com.bot.risk.DailyLossKillSwitch;
import com.bot.risk.ExposureBook;
import com.bot.risk.MarginTierTable;
import com.bot.risk.RiskConfig;
import com.bot.risk.RiskDecision;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import com.bot.risk.TradeRequest;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

/** Wiring shared by the execution tests, so each test states only what it is about. */
final class ExecFixtures {

    static final Instant NOON = Instant.parse("2026-08-09T12:00:00Z");
    static final Clock CLOCK = Clock.fixed(NOON, ZoneOffset.UTC);

    /** Never actually sleeps: the execution tests must not spend wall-clock time waiting. */
    static final RateLimiter.Sleeper NO_SLEEP = millis -> { };

    private ExecFixtures() {}

    /** Records what was alerted, so a test can assert that a failure was reported and not swallowed. */
    static final class RecordingAlerts implements AlertSink {
        final List<String> messages = new ArrayList<>();

        @Override public void alert(Severity severity, String title, String message) {
            messages.add(severity + " " + title + " :: " + message);
        }

        boolean sawCritical(String fragment) {
            return messages.stream().anyMatch(m -> m.startsWith("CRITICAL") && m.contains(fragment));
        }
    }

    static RiskEngine engine() {
        RiskConfig config = RiskConfig.defaults();
        return new RiskEngine(config, new ExposureBook(),
                new DailyLossKillSwitch(config.dailyLossFractionLimit()));
    }

    /** A plan the gate approved: LONG BTCUSDT, entry 64,000, stop 62,800, 3x, on a $10,000 balance. */
    static TradePlan approvedPlan(RiskEngine engine, InstrumentFilters filters) {
        TradeRequest request = new TradeRequest("sig-1", "BTCUSDT", Side.LONG, 64_000,
                OptionalDouble.of(62_800), OptionalDouble.empty(), 3,
                filters, MarginTierTable.conservativeDefault());
        RiskDecision decision = engine.evaluate(request, 10_000, NOON);
        if (decision instanceof RiskDecision.Rejected rejected) {
            throw new IllegalStateException("fixture plan was rejected: " + rejected);
        }
        return decision.planOrThrow();
    }

    static ExecutionCoordinator coordinator(FakeExchange exchange, RiskEngine engine,
                                            TradingHalt halt, AlertSink alerts) {
        return coordinator(exchange, engine, halt, alerts, ExecutionCoordinator.Settings.defaults());
    }

    static ExecutionCoordinator coordinator(FakeExchange exchange, RiskEngine engine, TradingHalt halt,
                                            AlertSink alerts, ExecutionCoordinator.Settings settings) {
        IdempotentOrderPlacer placer = new IdempotentOrderPlacer(exchange, 3, 2, 0, NO_SLEEP);
        return new ExecutionCoordinator(exchange, engine, placer, halt, alerts, settings, CLOCK, NO_SLEEP);
    }
}
