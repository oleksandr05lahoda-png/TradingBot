package com.bot.app;

import com.bot.core.Preconditions;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Trade lines on their way to Telegram, sent from a thread of their own. The main loop announces
 * an entry the moment it fills, and a Bot API call there - 15 s of timeout, a retry sleep - would
 * sit between that fill and the next close (the same reason AlertSink got its worker on 03.09).
 * Bounded: a Telegram outage must not grow the heap; a dropped line is logged, and the journal
 * still carries the trade.
 */
final class TgOutbox implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(TgOutbox.class.getName());

    static final int CAPACITY = 256;
    static final int ATTEMPTS = 3;

    /**
     * How one delivery went. {@code status}/{@code body} are Telegram's last refusal (0 / "" when
     * no answer came at all), so a 429 can be waited out for as long as Telegram itself asks.
     */
    record Attempt(boolean delivered, int status, String body) {
        static final Attempt DELIVERED = new Attempt(true, 200, "");
        /** The network failed or the call was interrupted: nothing to read, retry on the usual pause. */
        static final Attempt NO_ANSWER = new Attempt(false, 0, "");

        static Attempt of(boolean delivered) {
            return delivered ? DELIVERED : NO_ANSWER;
        }

        /** The chat id or the token is wrong: no retry can fix it (AlertSink's rule since 06.09). */
        boolean refusedForGood() {
            return status == 401 || status == 403 || status == 404;
        }
    }

    /** One delivery. Never expected to throw, but may. */
    @FunctionalInterface
    interface Delivery {
        Attempt deliver(String html);
    }

    /** The retry pause, so a test does not wait for real. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long ms) throws InterruptedException;
    }

    private final Delivery delivery;
    private final Sleeper sleeper;
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(CAPACITY);
    /** Accepted and not yet finished, in flight included - what the shutdown flush waits on. */
    private final AtomicInteger pending = new AtomicInteger();
    private volatile boolean shuttingDown;
    private Thread worker;

    TgOutbox(Delivery delivery, Sleeper sleeper) {
        this.delivery = Preconditions.notNull(delivery, "delivery");
        this.sleeper = Preconditions.notNull(sleeper, "sleeper");
    }

    /** Never blocks: a full queue drops the line with a log entry. */
    void offer(String html) {
        if (html == null || html.isEmpty()) return;
        pending.incrementAndGet();
        if (!queue.offer(html)) {
            pending.decrementAndGet();
            LOG.warning("[Notify] Telegram outbox is full - a trade line dropped (the journal has it)");
        }
    }

    void start() {
        if (worker != null) return;
        worker = new Thread(() -> {
            while (true) {
                String html;
                try {
                    html = queue.take();
                } catch (InterruptedException e) {
                    return;
                }
                deliverOne(html);
            }
        }, "telegram-trades");
        worker.setDaemon(true);
        worker.start();
        // The last exit before a stop is worth a short wait; the retry pauses are skipped meanwhile.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown = true;
            flush(5_000L);
        }, "telegram-trades-flush"));
    }

    /** For a test, or with no worker: sends everything queued, on the caller's thread. */
    int drainNow() {
        int n = 0;
        String html;
        while ((html = queue.poll()) != null) {
            deliverOne(html);
            n++;
        }
        return n;
    }

    private void deliverOne(String html) {
        try {
            int attempts = ATTEMPTS;
            boolean floodRetried = false;
            for (int attempt = 1; attempt <= attempts; attempt++) {
                Attempt result;
                try {
                    result = delivery.deliver(html);
                } catch (RuntimeException e) {
                    LOG.warning("[Notify] trade line not sent: " + e.getMessage());
                    result = Attempt.NO_ANSWER;
                }
                if (result == null) result = Attempt.NO_ANSWER;
                if (result.delivered()) return;
                if (result.refusedForGood()) {
                    // Six seconds of pauses per line cannot fix a wrong chat id or a rotated token.
                    LOG.warning("[Notify] trade line REFUSED, HTTP " + result.status()
                            + " - check TELEGRAM_CHAT_ID and TELEGRAM_BOT_TOKEN (the journal has it)");
                    return;
                }
                long pause = 2_000L * attempt;
                if (result.status() == 429) {
                    // Flood control names its own wait; a fixed 2 s / 4 s ran out inside a longer one
                    // and lost the line (a closed book's 15 exits beside the kill switch's alerts).
                    if (shuttingDown) return;
                    pause = OperatorChannel.retryAfterMillis(result.body());
                    if (!floodRetried) {
                        floodRetried = true;
                        attempts++;
                    }
                    LOG.warning("[Notify] Telegram flood control, waiting " + pause + " ms (attempt "
                            + attempt + "/" + attempts + ")");
                }
                if (attempt < attempts && !shuttingDown) {
                    try {
                        sleeper.sleep(pause);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            LOG.warning("[Notify] trade line given up after " + attempts + " attempts (the journal has it)");
        } finally {
            pending.decrementAndGet();
        }
    }

    void flush(long maxWaitMs) {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (pending.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    int pending() {
        return pending.get();
    }

    @Override public void close() {
        if (worker != null) worker.interrupt();
    }
}
