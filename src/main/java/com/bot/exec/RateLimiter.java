package com.bot.exec;

import com.bot.core.Preconditions;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.LongSupplier;
import java.util.logging.Logger;

/**
 * Keeps this process inside Binance's two independent budgets: request <b>weight</b> per minute per
 * IP, and <b>order count</b> per 10 seconds and per minute per account.
 *
 * <p>Being banned is not an inconvenience here, it is a risk event. A {@code 418} while a position is
 * open means the bot cannot place a stop, cannot amend one, and cannot close — it holds leveraged
 * exposure it has no way to act on, for as long as the ban lasts (Binance escalates repeat offences
 * from two minutes to three days). So the budgets are enforced <i>before</i> sending rather than
 * discovered from the response.
 *
 * <p>Both budgets are sliding windows rather than fixed buckets: a fixed bucket that resets on the
 * minute lets a burst spend a full minute's budget in the last second of one window and the first of
 * the next, which is exactly the pattern that trips the limiter it was meant to respect.
 *
 * <p>The exchange's own accounting wins over the local estimate: {@link #observeUsedWeight} folds in
 * the {@code X-MBX-USED-WEIGHT-1M} header, because other processes may share the IP.
 *
 * <p>Time and sleeping are injected so the whole class is testable without waiting.
 */
public final class RateLimiter {

    private static final Logger LOG = Logger.getLogger(RateLimiter.class.getName());

    /** Sleeps for a number of milliseconds. Real implementation blocks; tests advance a clock. */
    @FunctionalInterface
    public interface Sleeper {
        void sleepMillis(long millis) throws InterruptedException;
    }

    private record Entry(long atMs, int amount) {}

    private final int weightPerMinute;
    private final int ordersPer10s;
    private final int ordersPerMinute;
    private final LongSupplier nowMs;
    private final Sleeper sleeper;

    private final Deque<Entry> weightWindow = new ArrayDeque<>();
    private final Deque<Entry> orderWindow = new ArrayDeque<>();
    private long bannedUntilMs = 0;
    private int exchangeReportedWeight = 0;
    private long exchangeReportedAtMs = 0;

    /** Binance USDⓈ-M defaults: 2400 weight/min, 300 orders/10s, 1200 orders/min. */
    public static RateLimiter binanceDefaults() {
        return new RateLimiter(2400, 300, 1200, System::currentTimeMillis, Thread::sleep);
    }

    public RateLimiter(int weightPerMinute, int ordersPer10s, int ordersPerMinute,
                       LongSupplier nowMs, Sleeper sleeper) {
        this.weightPerMinute = Preconditions.positive(weightPerMinute, "weightPerMinute");
        this.ordersPer10s = Preconditions.positive(ordersPer10s, "ordersPer10s");
        this.ordersPerMinute = Preconditions.positive(ordersPerMinute, "ordersPerMinute");
        this.nowMs = Preconditions.notNull(nowMs, "nowMs");
        this.sleeper = Preconditions.notNull(sleeper, "sleeper");
    }

    /**
     * Reserves {@code weight} of request budget, waiting if the window is full.
     *
     * @param isOrder true for endpoints that also consume the separate order-rate budget
     */
    public void acquire(int weight, boolean isOrder) throws InterruptedException {
        Preconditions.positive(weight, "weight");
        while (true) {
            long waitMs;
            // The wait is computed under the lock; the waiting itself happens outside it. Sleeping
            // while holding the monitor would block observeBan and observeUsedWeight for the whole
            // wait — up to a full ban — so a thread that had just learned of a 418 could not record
            // it, and the waiting thread would wake up and send anyway.
            synchronized (this) {
                long now = nowMs.getAsLong();
                prune(now);
                waitMs = waitRequired(now, weight, isOrder);
                if (waitMs <= 0) {
                    weightWindow.addLast(new Entry(now, weight));
                    if (isOrder) orderWindow.addLast(new Entry(now, 1));
                    return;
                }
            }
            LOG.fine("[RateLimiter] holding " + waitMs + "ms before a weight-" + weight + " request");
            sleeper.sleepMillis(Math.min(waitMs, 5_000L));
        }
    }

    /** Milliseconds this request must wait, or 0 when it may go now. Caller holds the monitor. */
    private long waitRequired(long now, int weight, boolean isOrder) {
        if (now < bannedUntilMs) {
            return bannedUntilMs - now;
        }
        long waitMs = 0;
        if (usedWeight(now) + weight > weightPerMinute) {
            waitMs = Math.max(waitMs, millisUntilRoomInWindow(weightWindow, now, 60_000L));
        }
        if (isOrder) {
            if (count(orderWindow, now, 10_000L) + 1 > ordersPer10s) {
                waitMs = Math.max(waitMs, millisUntilRoomInWindow(orderWindow, now, 10_000L));
            }
            if (count(orderWindow, now, 60_000L) + 1 > ordersPerMinute) {
                waitMs = Math.max(waitMs, millisUntilRoomInWindow(orderWindow, now, 60_000L));
            }
        }
        return waitMs;
    }

    /**
     * Folds in the exchange's own view of used weight from {@code X-MBX-USED-WEIGHT-1M}. It supersedes
     * the local estimate whenever it is higher, since the IP may be shared with another process whose
     * requests this limiter never saw.
     */
    public synchronized void observeUsedWeight(int usedWeight1m) {
        if (usedWeight1m <= 0) return;
        this.exchangeReportedWeight = usedWeight1m;
        this.exchangeReportedAtMs = nowMs.getAsLong();
    }

    /**
     * Records a {@code 429} or {@code 418}. Everything stops until the ban expires — including, and
     * especially, retries, which are what turn a 429 into a 418.
     */
    public synchronized void observeBan(long retryAfterMillis) {
        long until = nowMs.getAsLong() + Math.max(1_000L, retryAfterMillis);
        if (until > bannedUntilMs) {
            bannedUntilMs = until;
            LOG.warning("[RateLimiter] rate limited by the exchange — holding all requests for "
                    + (retryAfterMillis / 1000) + "s");
        }
    }

    public synchronized boolean isBanned() {
        return nowMs.getAsLong() < bannedUntilMs;
    }

    /** Weight used in the trailing minute: the larger of the local tally and the exchange's. */
    public synchronized int usedWeight(long now) {
        int local = sum(weightWindow, now, 60_000L);
        boolean headerFresh = exchangeReportedAtMs > 0 && now - exchangeReportedAtMs < 60_000L;
        return headerFresh ? Math.max(local, exchangeReportedWeight) : local;
    }

    public synchronized int ordersInLast10s() { return count(orderWindow, nowMs.getAsLong(), 10_000L); }

    public synchronized int ordersInLastMinute() { return count(orderWindow, nowMs.getAsLong(), 60_000L); }

    private void prune(long now) {
        while (!weightWindow.isEmpty() && now - weightWindow.peekFirst().atMs() >= 60_000L) weightWindow.removeFirst();
        while (!orderWindow.isEmpty() && now - orderWindow.peekFirst().atMs() >= 60_000L) orderWindow.removeFirst();
    }

    private static int sum(Deque<Entry> window, long now, long spanMs) {
        int total = 0;
        for (Entry e : window) {
            if (now - e.atMs() < spanMs) total += e.amount();
        }
        return total;
    }

    private static int count(Deque<Entry> window, long now, long spanMs) {
        return sum(window, now, spanMs);
    }

    /** How long until the oldest entry inside {@code spanMs} falls out of the window. */
    private static long millisUntilRoomInWindow(Deque<Entry> window, long now, long spanMs) {
        for (Entry e : window) {
            if (now - e.atMs() < spanMs) return spanMs - (now - e.atMs()) + 1;
        }
        return 1;
    }
}
