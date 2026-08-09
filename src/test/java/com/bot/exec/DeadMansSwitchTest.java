package com.bot.exec;

import com.bot.core.Side;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The dead-man's switch: armed on the exchange, disarmed when flat, and loud when contact is lost. */
class DeadMansSwitchTest {

    private final FakeExchange exchange = new FakeExchange();
    private final ExecFixtures.RecordingAlerts alerts = new ExecFixtures.RecordingAlerts();
    private final TradingHalt halt = new TradingHalt();
    private final RiskEngine engine = ExecFixtures.engine();

    private DeadMansSwitch switchWith(long countdownMs, long silenceMs) {
        return new DeadMansSwitch(exchange, engine, halt, alerts, countdownMs, silenceMs);
    }

    private void openBook(String symbol) {
        engine.book().open(new ExposureBook.OpenPosition(symbol, Side.LONG,
                new BigDecimal("0.041"), 64_000, 2_624, 49.2));
    }

    @Test
    @DisplayName("a heartbeat arms the countdown for every symbol holding a position")
    void heartbeatArmsHeldSymbols() {
        openBook("BTCUSDT");
        switchWith(120_000, 90_000).heartbeat(ExecFixtures.NOON);
        assertEquals(120_000L, exchange.deadMansCountdownFor("BTCUSDT"));
    }

    @Test
    @DisplayName("a symbol that closed is disarmed, so a stale countdown cannot cancel a future position's orders")
    void closedSymbolsAreDisarmed() {
        openBook("BTCUSDT");
        DeadMansSwitch deadMansSwitch = switchWith(120_000, 90_000);
        deadMansSwitch.heartbeat(ExecFixtures.NOON);

        engine.book().close("BTCUSDT");
        deadMansSwitch.heartbeat(ExecFixtures.NOON.plusSeconds(30));

        assertEquals(0L, exchange.deadMansCountdownFor("BTCUSDT"), "the countdown should be cancelled");
    }

    @Test
    @DisplayName("nothing is armed when nothing is open")
    void nothingArmedWhenFlat() {
        switchWith(120_000, 90_000).heartbeat(ExecFixtures.NOON);
        assertNull(exchange.deadMansCountdownFor("BTCUSDT"));
        assertFalse(halt.isHalted());
    }

    @Test
    @DisplayName("prolonged loss of contact halts new risk, alerts, and tries a local cancel")
    void lostContactHaltsAndAlerts() {
        openBook("BTCUSDT");
        FailingArmExchange failing = new FailingArmExchange();
        DeadMansSwitch deadMansSwitch = new DeadMansSwitch(failing, engine, halt, alerts, 120_000, 90_000);

        Instant start = ExecFixtures.NOON;
        deadMansSwitch.heartbeat(start);                        // first failure starts the clock
        assertFalse(halt.isHalted(), "one failed heartbeat is not a lost connection");

        deadMansSwitch.heartbeat(start.plusSeconds(120));        // beyond the 90s tolerance

        assertTrue(deadMansSwitch.isDegraded());
        assertTrue(halt.isHalted());
        assertTrue(alerts.sawCritical("contact lost"), alerts.messages.toString());
        assertEquals(1, failing.cancelAllCalls, "a local cancel should at least be attempted");
    }

    @Test
    @DisplayName("positions are never closed by the switch, only orders")
    void positionsAreNotTouched() {
        openBook("BTCUSDT");
        exchange.plantPosition("BTCUSDT", "0.041", "64000");
        switchWith(120_000, 90_000).heartbeat(ExecFixtures.NOON);
        assertEquals(1, exchange.openPositions().size(),
                "force-closing a position is a trading decision; a watchdog does not get to make it");
    }

    @Test
    @DisplayName("a countdown shorter than the local tolerance is refused at construction")
    void countdownMustOutlastTheLocalTolerance() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> switchWith(30_000, 90_000));
        assertTrue(thrown.getMessage().contains("must outlast"), thrown.getMessage());
    }

    @Test
    @DisplayName("shutdown disarms every countdown")
    void shutdownDisarms() {
        openBook("BTCUSDT");
        DeadMansSwitch deadMansSwitch = switchWith(120_000, 90_000);
        deadMansSwitch.heartbeat(ExecFixtures.NOON);
        deadMansSwitch.disarmAll();
        assertEquals(0L, exchange.deadMansCountdownFor("BTCUSDT"));
    }

    /** An exchange that is reachable for everything except arming — the "alive but blind" case. */
    private static final class FailingArmExchange extends DelegatingExchange {
        int cancelAllCalls = 0;

        @Override public void armDeadMansSwitch(String symbol, long countdownMillis) {
            throw ExchangeException.ambiguous("no route to host", null);
        }

        @Override public void cancelAllOpenOrders(String symbol) {
            cancelAllCalls++;
        }
    }
}
