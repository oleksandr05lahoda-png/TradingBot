package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Makes sure a bot that stops running does not leave <b>exposure-increasing</b> orders behind it —
 * and, just as importantly, does not take its own protection down with it.
 *
 * <h2>Why "cancel all working orders" is the wrong rule here</h2>
 * The obvious reading of a dead-man's switch is "if the bot goes quiet, cancel everything it left
 * on the book". For this system that reading is actively harmful, and the reason is worth being
 * explicit about.
 *
 * <p>The orders this bot leaves resting fall into two categories with opposite risk signs:
 * <ul>
 *   <li>a resting <b>entry</b> order is exposure-increasing. If it fills while the process is dead,
 *       it creates a leveraged position with nothing protecting it. Cancelling it is exactly right.</li>
 *   <li>the protective <b>stop</b> and the reduce-only exits can only ever shrink a position. If they
 *       fill while the process is dead, they do the job they were placed for. Cancelling them strips
 *       the protection off an open position at the precise moment nobody is watching it — which is
 *       the opposite of what a safety mechanism should do.</li>
 * </ul>
 *
 * <p>Binance's {@code countdownCancelAll} cannot tell the two apart: it cancels every open order on
 * the symbol. So it is armed only for a symbol that has a resting entry and <b>no position</b>, and
 * a symbol that holds a position is explicitly <b>disarmed</b>. Arming it over an open position
 * would schedule the deletion of that position's stop.
 *
 * <p>Server-side is still the point where it does apply: a watchdog thread inside this process dies
 * in the same crash, OOM or container eviction that stranded the order. The countdown keeps running
 * on the exchange's machine, which is the one place a local failure cannot reach.
 *
 * <h2>The local half</h2>
 * A different failure is "the process is alive but cannot reach the exchange". There is nothing to
 * cancel with in that state, so what it does instead is stop taking new risk —
 * {@link TradingHalt} after {@code maxSilenceMillis} without contact — and say so loudly. It also
 * makes a best-effort pass to cancel <b>only the non-reducing</b> working orders, for the same
 * reason as above.
 *
 * <h2>Choosing the countdown</h2>
 * Arm with roughly twice the heartbeat interval. A 30s heartbeat with a 120s countdown survives
 * three consecutive failed heartbeats before the exchange acts.
 */
public final class DeadMansSwitch {

    private static final Logger LOG = Logger.getLogger(DeadMansSwitch.class.getName());

    private final ExchangePort port;
    private final RiskEngine engine;
    private final TradingHalt halt;
    private final AlertSink alerts;
    private final long countdownMillis;
    private final long maxSilenceMillis;

    /** Symbols whose countdown is currently armed on the exchange. */
    private final Set<String> armed = ConcurrentHashMap.newKeySet();

    private volatile long lastSuccessMs = 0;
    private volatile boolean degraded = false;

    public DeadMansSwitch(ExchangePort port, RiskEngine engine, TradingHalt halt, AlertSink alerts,
                          long countdownMillis, long maxSilenceMillis) {
        this.port = Preconditions.notNull(port, "port");
        this.engine = Preconditions.notNull(engine, "engine");
        this.halt = Preconditions.notNull(halt, "halt");
        this.alerts = Preconditions.notNull(alerts, "alerts");
        Preconditions.require(countdownMillis > 0, "countdownMillis must be positive");
        Preconditions.require(maxSilenceMillis > 0, "maxSilenceMillis must be positive");
        Preconditions.require(countdownMillis >= maxSilenceMillis,
                "the exchange-side countdown (" + countdownMillis + "ms) must outlast the local "
                        + "tolerance for silence (" + maxSilenceMillis + "ms), otherwise the exchange "
                        + "cancels orders before this process has even noticed it is out of touch");
        this.countdownMillis = countdownMillis;
        this.maxSilenceMillis = maxSilenceMillis;
    }

    /** Heartbeat with no resting entries — the default flow, where entries are market orders. */
    public void heartbeat(Instant now) {
        heartbeat(now, Set.of());
    }

    /**
     * One heartbeat.
     *
     * @param symbolsWithRestingEntries symbols carrying an exposure-increasing order that has not
     *                                  filled yet. These get the exchange-side countdown. Symbols
     *                                  holding a position never do — see the class javadoc.
     */
    public void heartbeat(Instant now, Set<String> symbolsWithRestingEntries) {
        Preconditions.notNull(now, "now");
        Preconditions.notNull(symbolsWithRestingEntries, "symbolsWithRestingEntries");
        long nowMs = now.toEpochMilli();

        Set<String> held = engine.book().all().stream()
                .map(ExposureBook.OpenPosition::symbol)
                .collect(Collectors.toUnmodifiableSet());

        Set<String> shouldBeArmed = new LinkedHashSet<>(symbolsWithRestingEntries);
        shouldBeArmed.removeAll(held);

        boolean allContacted = true;

        for (String symbol : shouldBeArmed) {
            try {
                port.armDeadMansSwitch(symbol, countdownMillis);
                armed.add(symbol);
            } catch (RuntimeException e) {
                allContacted = false;
                LOG.warning("[DeadMansSwitch] could not arm " + symbol + ": " + e.getMessage());
            }
        }

        // Anything armed that should not be: the entry filled (so the symbol now holds a position
        // and a stop), or it was cancelled. Disarm, so a countdown left running cannot delete the
        // protection that was placed after it.
        for (String symbol : List.copyOf(armed)) {
            if (shouldBeArmed.contains(symbol)) continue;
            try {
                port.armDeadMansSwitch(symbol, 0);
                armed.remove(symbol);
            } catch (RuntimeException e) {
                allContacted = false;
                LOG.warning("[DeadMansSwitch] could not disarm " + symbol + ": " + e.getMessage());
            }
        }

        // Liveness is measured even when there is nothing to arm: the point of the local half is to
        // notice that the exchange has gone away while positions are open, and an idle heartbeat
        // that never talks to the exchange would notice nothing.
        if (shouldBeArmed.isEmpty() && armed.isEmpty()) {
            try {
                port.serverTimeMillis();
            } catch (RuntimeException e) {
                allContacted = false;
                LOG.warning("[DeadMansSwitch] liveness probe failed: " + e.getMessage());
            }
        }

        if (allContacted) {
            lastSuccessMs = nowMs;
            if (degraded) {
                degraded = false;
                alerts.info("Exchange contact restored",
                        "dead-man's switch heartbeat is succeeding again; the trading halt still "
                                + "needs an explicit operator reset");
            }
            return;
        }

        if (lastSuccessMs == 0) lastSuccessMs = nowMs;
        long silentFor = nowMs - lastSuccessMs;
        if (silentFor > maxSilenceMillis && !degraded) {
            degraded = true;
            String message = "no successful contact with the exchange for " + (silentFor / 1000)
                    + "s while holding " + held.size() + " position(s). New risk is stopped. Any "
                    + "resting entry order is covered by the exchange-side countdown, which fires "
                    + "within " + (countdownMillis / 1000) + "s of the last successful arm. "
                    + "Protective stops and positions are deliberately left alone.";
            LOG.severe("[DeadMansSwitch] " + message);
            alerts.critical("Dead-man's switch: contact lost", message);
            halt.halt("exchange contact lost for " + (silentFor / 1000) + "s", now);
            cancelNonReducingOrdersWhereStillPossible(shouldBeArmed);
        }
    }

    /** Disarms every countdown. Called on a clean shutdown. */
    public void disarmAll() {
        for (String symbol : List.copyOf(armed)) {
            try {
                port.armDeadMansSwitch(symbol, 0);
            } catch (RuntimeException e) {
                LOG.fine("[DeadMansSwitch] disarm of " + symbol + " failed: " + e.getMessage());
            }
            armed.remove(symbol);
        }
    }

    public boolean isDegraded() { return degraded; }

    /** Symbols currently carrying an exchange-side countdown. */
    public Set<String> armedSymbols() { return Set.copyOf(armed); }

    /**
     * Best effort, and deliberately selective: only orders that could <i>increase</i> exposure are
     * cancelled. A reduce-only stop is the thing keeping an open position survivable, and a
     * connectivity problem is not a reason to remove it.
     */
    private void cancelNonReducingOrdersWhereStillPossible(Set<String> symbols) {
        for (String symbol : symbols) {
            try {
                for (OrderStatus order : port.openOrders(symbol)) {
                    if (!order.isWorking() || order.reduceOnly() || order.closePosition()) continue;
                    port.cancelOrder(symbol, order.clientOrderId());
                    LOG.warning("[DeadMansSwitch] locally cancelled exposure-increasing order "
                            + order.clientOrderId() + " on " + symbol);
                }
            } catch (RuntimeException e) {
                LOG.warning("[DeadMansSwitch] local cancel on " + symbol + " also failed ("
                        + e.getMessage() + ") — relying on the exchange-side countdown");
            }
        }
    }
}
