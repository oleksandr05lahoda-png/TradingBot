package com.bot.exec;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Rate limiting, driven by a fake clock so the test costs no wall-clock time. A {@code 418} while a
 * position is open leaves leveraged exposure the bot cannot act on until the ban expires.
 */
class RateLimiterTest {

    /** A clock the test advances by hand; sleeping simply moves it forward. */
    private static final class FakeTime {
        final AtomicLong nowMs = new AtomicLong(1_000_000);
        long sleptTotalMs = 0;

        RateLimiter.Sleeper sleeper() {
            return millis -> {
                sleptTotalMs += millis;
                nowMs.addAndGet(millis);
            };
        }
    }

    @Test
    @DisplayName("requests inside the budget do not wait")
    void withinBudgetDoesNotWait() throws Exception {
        FakeTime time = new FakeTime();
        RateLimiter limiter = new RateLimiter(100, 10, 50, time.nowMs::get, time.sleeper());

        for (int i = 0; i < 20; i++) limiter.acquire(5, false);

        assertEquals(0, time.sleptTotalMs, "the limiter slept while it still had budget");
        assertEquals(100, limiter.usedWeight(time.nowMs.get()));
    }

    @Test
    @DisplayName("exceeding the weight budget waits until the window has room")
    void weightBudgetIsEnforced() throws Exception {
        FakeTime time = new FakeTime();
        RateLimiter limiter = new RateLimiter(100, 100, 100, time.nowMs::get, time.sleeper());

        for (int i = 0; i < 20; i++) limiter.acquire(5, false);
        limiter.acquire(5, false);   // the 21st must wait for the first to age out

        assertTrue(time.sleptTotalMs >= 60_000,
                "the limiter did not wait for the sliding minute to free up, it slept "
                        + time.sleptTotalMs + "ms");
    }

    @Test
    @DisplayName("the order-rate budget is separate from the weight budget")
    void orderRateIsSeparate() throws Exception {
        FakeTime time = new FakeTime();
        // Plenty of weight, but only 3 orders per 10 seconds.
        RateLimiter limiter = new RateLimiter(10_000, 3, 1_000, time.nowMs::get, time.sleeper());

        for (int i = 0; i < 3; i++) limiter.acquire(1, true);
        assertEquals(0, time.sleptTotalMs);

        limiter.acquire(1, true);
        assertTrue(time.sleptTotalMs >= 10_000,
                "the fourth order in a 10-second window should have waited, slept " + time.sleptTotalMs);
        assertEquals(4, limiter.ordersInLastMinute());
    }

    @Test
    @DisplayName("a ban stops everything until it expires, retries included")
    void banStopsEverything() throws Exception {
        FakeTime time = new FakeTime();
        RateLimiter limiter = new RateLimiter(10_000, 1_000, 10_000, time.nowMs::get, time.sleeper());

        limiter.observeBan(30_000);
        assertTrue(limiter.isBanned());

        limiter.acquire(1, false);

        assertTrue(time.sleptTotalMs >= 30_000,
                "requests continued during a ban, which is how a 429 becomes a 418");
        assertFalse(limiter.isBanned());
    }

    @Test
    @DisplayName("the exchange's reported weight supersedes the local tally when it is higher")
    void exchangeHeaderWinsWhenHigher() throws Exception {
        FakeTime time = new FakeTime();
        RateLimiter limiter = new RateLimiter(1_000, 100, 1_000, time.nowMs::get, time.sleeper());

        limiter.acquire(10, false);
        assertEquals(10, limiter.usedWeight(time.nowMs.get()));

        // Another process shares this IP and has spent far more than this limiter knows about.
        limiter.observeUsedWeight(800);
        assertEquals(800, limiter.usedWeight(time.nowMs.get()));

        // A lower report does not lower the local tally — the local one is a floor, not a guess.
        limiter.observeUsedWeight(5);
        assertEquals(10, limiter.usedWeight(time.nowMs.get()));
    }

    @Test
    @DisplayName("the window slides rather than resetting on a boundary")
    void windowSlides() throws Exception {
        FakeTime time = new FakeTime();
        RateLimiter limiter = new RateLimiter(10, 100, 1_000, time.nowMs::get, time.sleeper());

        for (int i = 0; i < 10; i++) limiter.acquire(1, false);
        assertEquals(10, limiter.usedWeight(time.nowMs.get()));

        time.nowMs.addAndGet(59_000);
        assertEquals(10, limiter.usedWeight(time.nowMs.get()), "nothing has aged out yet");

        time.nowMs.addAndGet(2_000);
        assertEquals(0, limiter.usedWeight(time.nowMs.get()), "the whole burst is now older than a minute");
    }
}
