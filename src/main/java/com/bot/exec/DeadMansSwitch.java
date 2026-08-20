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
 * Cancels exposure-increasing orders left behind by a dead process, without stripping protection off
 * open positions.
 *
 * <p>Binance's {@code countdownCancelAll} cancels every open order on the symbol and cannot tell a
 * resting entry from a protective stop, so it is armed only for a symbol with a resting entry and
 * <b>no position</b>; a symbol holding a position is explicitly disarmed. Arm with roughly twice the
 * heartbeat interval, so a few consecutive failures do not trip it.
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
     * @param symbolsWithRestingEntries symbols carrying an unfilled exposure-increasing order. These
     *                                  get the exchange-side countdown; symbols holding a position
     *                                  never do.
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

        // Disarm the rest: a countdown left running would delete protection placed after it.
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

        // Probe even with nothing to arm, otherwise an idle heartbeat never notices a lost exchange.
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
            // The countdown clause is conditional on purpose: with market entries (the only kind
            // today) nothing is ever armed, and an alert promising an exchange-side countdown
            // that does not exist would misdirect the operator during an outage.
            String message = "no successful contact with the exchange for " + (silentFor / 1000)
                    + "s while holding " + held.size() + " position(s). New risk is stopped. "
                    + (armed.isEmpty()
                            ? "No resting entry orders were armed, so there is nothing for the "
                                    + "exchange-side countdown to cancel. "
                            : "Resting entry orders are covered by the exchange-side countdown, "
                                    + "which fires within " + (countdownMillis / 1000)
                                    + "s of the last successful arm. ")
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
     * Best effort, and deliberately selective: cancelling reduce-only stops would strip an open
     * position of the protection keeping it survivable.
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
