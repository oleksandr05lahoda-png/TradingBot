package com.bot.exec;

import com.bot.core.Preconditions;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.LongSupplier;
import java.util.logging.Logger;

/**
 * Keeps this process inside Binance's two independent budgets: request <b>weight</b> per minute per
 * IP, and <b>order count</b> per 10s and per minute per account. Enforced before sending — a ban
 * blocks placing or amending a stop while it lasts (Binance escalates 2 minutes → 3 days). Sliding
 * windows, not fixed buckets, or a burst spends a full minute's budget across a bucket boundary.
 */
public final class RateLimiter {

    private static final Logger LOG = Logger.getLogger(RateLimiter.class.getName());

    /** Injected so tests need not wait: the real implementation blocks, tests advance a clock. */
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

    /** Reserves {@code weight}, waiting if the window is full; {@code isOrder} also spends order-rate. */
    public void acquire(int weight, boolean isOrder) throws InterruptedException {
        Preconditions.positive(weight, "weight");
        while (true) {
            long waitMs;
            // Wait computed under the lock, slept outside it: holding the monitor would block
            // observeBan, so a 418 seen by another thread could not be recorded before this one sends.
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

    /** Folds in {@code X-MBX-USED-WEIGHT-1M}, which wins when higher: the IP may be shared. */
    public synchronized void observeUsedWeight(int usedWeight1m) {
        if (usedWeight1m <= 0) return;
        this.exchangeReportedWeight = usedWeight1m;
        this.exchangeReportedAtMs = nowMs.getAsLong();
    }

    /** A hold at least this long is an incident, not a throttle: the operator is told. */
    static final long HOLD_ALERT_THRESHOLD_MS = 5 * 60_000L;

    private volatile java.util.function.LongConsumer holdListener;

    /** Called with the hold length when the exchange imposes one of {@link #HOLD_ALERT_THRESHOLD_MS} or more. */
    public void onHold(java.util.function.LongConsumer listener) {
        this.holdListener = listener;
    }

    /** Records a {@code 429}/{@code 418}; everything stops until it expires — retries turn a 429 into a 418. */
    public void observeBan(long retryAfterMillis) {
        boolean longer;
        synchronized (this) {
            long until = nowMs.getAsLong() + Math.max(1_000L, retryAfterMillis);
            longer = until > bannedUntilMs;
            if (longer) {
                bannedUntilMs = until;
                LOG.warning("[RateLimiter] rate limited by the exchange — holding all requests for "
                        + (retryAfterMillis / 1000) + "s");
            }
        }
        // Outside the monitor: the listener sends a Telegram message, and a blocked monitor would
        // stall every thread waiting to send. A 6-hour IP ban at boot (22.08) was a WARNING in a
        // log nobody was reading; the operator found out from the exchange, not from the bot.
        java.util.function.LongConsumer l = holdListener;
        if (longer && l != null && retryAfterMillis >= HOLD_ALERT_THRESHOLD_MS) {
            try {
                l.accept(retryAfterMillis);
            } catch (RuntimeException e) {
                LOG.warning("[RateLimiter] hold listener failed: " + e.getMessage());
            }
        }
    }

    public synchronized boolean isBanned() {
        return nowMs.getAsLong() < bannedUntilMs;
    }

    /** Milliseconds the exchange has told us to stay silent; 0 when free to send. */
    public synchronized long heldForMillis() {
        return Math.max(0L, bannedUntilMs - nowMs.getAsLong());
    }

    /** Weight used in the trailing minute: the larger of the local tally and the exchange's. */
    public synchronized int usedWeight(long now) {
        int local = sum(weightWindow, now, 60_000L);
        boolean headerFresh = exchangeReportedAtMs > 0 && now - exchangeReportedAtMs < 60_000L;
        return headerFresh ? Math.max(local, exchangeReportedWeight) : local;
    }

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

    private static long millisUntilRoomInWindow(Deque<Entry> window, long now, long spanMs) {
        for (Entry e : window) {
            if (now - e.atMs() < spanMs) return spanMs - (now - e.atMs()) + 1;
        }
        return 1;
    }
}
