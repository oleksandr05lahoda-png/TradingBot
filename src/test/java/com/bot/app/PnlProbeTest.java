package com.bot.app;

import com.bot.exec.ExchangePort;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** /pnl's figures: computed on the loop at most every 10 minutes, and a failure only makes them stale. */
class PnlProbeTest {

    private static final Instant T = Instant.parse("2026-09-23T10:00:00Z");

    /** Only the two methods the probe may call; anything else is a test failure. */
    private static final class Port {
        final List<Long> sinces = new ArrayList<>();
        long holdMs = 0;
        boolean fail = false;

        ExchangePort proxy() {
            return (ExchangePort) Proxy.newProxyInstance(ExchangePort.class.getClassLoader(),
                    new Class<?>[] {ExchangePort.class}, (self, method, args) -> switch (method.getName()) {
                        case "heldByExchangeForMillis" -> holdMs;
                        case "fetchRealizedPnlSince" -> {
                            if (fail) throw new IllegalStateException("HTTP 503");
                            sinces.add((Long) args[0]);
                            yield (double) sinces.size();
                        }
                        default -> throw new AssertionError("the probe must not call " + method.getName());
                    });
        }
    }

    @Test
    @DisplayName("today is Warsaw's day; 7 and 30 days are rolling; a second call inside 10 min fetches nothing")
    void cadenceAndWindows() {
        Port port = new Port();
        PnlProbe probe = new PnlProbe(port.proxy());

        OperatorSnapshot.Pnl p = probe.refreshIfDue(T);
        assertEquals(List.of(Instant.parse("2026-09-22T22:00:00Z").toEpochMilli(),
                T.minusSeconds(7 * 86400).toEpochMilli(), T.minusSeconds(30 * 86400).toEpochMilli()), port.sinces);
        assertEquals(1.0, p.today());
        assertEquals(3.0, p.month());
        assertEquals(T, p.computedAt());
        assertNull(p.failedAt());

        assertEquals(p, probe.refreshIfDue(T.plusSeconds(599)));
        assertEquals(3, port.sinces.size(), "no call inside the ten minutes");
        probe.refreshIfDue(T.plusSeconds(600));
        assertEquals(6, port.sinces.size());
    }

    @Test
    @DisplayName("a failed refresh keeps the last figures and says when it failed; an exchange hold skips the call")
    void failureIsStalenessOnly() {
        Port port = new Port();
        PnlProbe probe = new PnlProbe(port.proxy());
        probe.refreshIfDue(T);

        port.fail = true;
        OperatorSnapshot.Pnl stale = probe.refreshIfDue(T.plusSeconds(600));
        assertEquals(1.0, stale.today());
        assertEquals(T, stale.computedAt());
        assertEquals(T.plusSeconds(600), stale.failedAt());

        PnlProbe fresh = new PnlProbe(port.proxy());
        OperatorSnapshot.Pnl never = fresh.refreshIfDue(T);
        assertFalse(never.known());

        Port held = new Port();
        held.holdMs = 60_000;
        assertNull(new PnlProbe(held.proxy()).refreshIfDue(T));
        assertTrue(held.sinces.isEmpty(), "nothing asked of a venue that told us to wait");
    }

    @Test
    @DisplayName("Warsaw midnight: 23:30 local is still the 23rd, 00:30 local is the 24th")
    void warsawDay() {
        assertEquals(Instant.parse("2026-09-22T22:00:00Z"), PnlProbe.startOfWarsawDay(Instant.parse("2026-09-23T21:30:00Z")));
        assertEquals(Instant.parse("2026-09-23T22:00:00Z"), PnlProbe.startOfWarsawDay(Instant.parse("2026-09-23T22:30:00Z")));
        assertEquals(Instant.parse("2026-12-01T23:00:00Z"), PnlProbe.startOfWarsawDay(Instant.parse("2026-12-02T08:00:00Z")));
    }
}
