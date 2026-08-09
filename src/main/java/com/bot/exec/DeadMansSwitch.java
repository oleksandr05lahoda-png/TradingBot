package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Makes sure a bot that stops running does not leave live orders behind it.
 *
 * <p>The mechanism is Binance's own {@code POST /fapi/v1/countdownCancelAll}: arm a countdown for a
 * symbol, and if it is not re-armed before the countdown expires, the <b>exchange</b> cancels every
 * open order on that symbol. Positions are untouched — which is the correct division. Cancelling a
 * dead bot's resting orders removes risk it can no longer manage; force-closing its positions would
 * be a trading decision made by a watchdog, at whatever price the book happens to offer.
 *
 * <p>Server-side is the entire point. A watchdog thread inside this process dies in the same crash,
 * OOM or container eviction that stranded the orders. The countdown keeps running on the exchange's
 * machine, which is the one place a local failure cannot reach.
 *
 * <p>The local half handles a different failure: the process is alive but cannot reach the exchange.
 * There is nothing to cancel with in that state, so what it does instead is stop taking new risk —
 * {@link TradingHalt} after {@code maxSilenceMillis} without a successful re-arm — and say so
 * loudly. The exchange-side countdown covers the rest.
 *
 * <h2>Choosing the countdown</h2>
 * Arm with roughly twice the heartbeat interval. A 30s heartbeat with a 120s countdown survives three
 * consecutive failed heartbeats before the exchange acts, which is enough to ride out a transient
 * network problem and short enough that a genuinely dead process is disarmed within two minutes.
 */
public final class DeadMansSwitch {

    private static final Logger LOG = Logger.getLogger(DeadMansSwitch.class.getName());

    private final ExchangePort port;
    private final RiskEngine engine;
    private final TradingHalt halt;
    private final AlertSink alerts;
    private final long countdownMillis;
    private final long maxSilenceMillis;

    private final ConcurrentHashMap<String, Long> lastArmedAtMs = new ConcurrentHashMap<>();
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

    /**
     * One heartbeat. Re-arms the countdown for every symbol currently carrying a position, and
     * disarms symbols that no longer do.
     *
     * <p>Call this on a timer at roughly half {@code countdownMillis}. It is safe to call more often.
     */
    public void heartbeat(Instant now) {
        Preconditions.notNull(now, "now");
        long nowMs = now.toEpochMilli();
        Set<String> live = engine.book().all().stream()
                .map(ExposureBook.OpenPosition::symbol)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());

        boolean allArmed = true;
        for (String symbol : live) {
            try {
                port.armDeadMansSwitch(symbol, countdownMillis);
                lastArmedAtMs.put(symbol, nowMs);
            } catch (RuntimeException e) {
                allArmed = false;
                LOG.warning("[DeadMansSwitch] could not re-arm " + symbol + ": " + e.getMessage());
            }
        }

        // Symbols that closed since the last heartbeat: disarm, so a countdown left running does not
        // cancel orders belonging to a future position on the same symbol.
        for (String symbol : List.copyOf(lastArmedAtMs.keySet())) {
            if (live.contains(symbol)) continue;
            try {
                port.armDeadMansSwitch(symbol, 0);
                lastArmedAtMs.remove(symbol);
            } catch (RuntimeException e) {
                LOG.warning("[DeadMansSwitch] could not disarm " + symbol + ": " + e.getMessage());
            }
        }

        if (allArmed) {
            lastSuccessMs = nowMs;
            if (degraded) {
                degraded = false;
                alerts.info("Exchange contact restored", "dead-man's switch re-armed for " + live);
            }
            return;
        }

        if (lastSuccessMs == 0) lastSuccessMs = nowMs;
        long silentFor = nowMs - lastSuccessMs;
        if (silentFor > maxSilenceMillis && !degraded) {
            degraded = true;
            String message = "no successful contact with the exchange for " + (silentFor / 1000)
                    + "s while holding " + live.size() + " position(s). The exchange-side countdown will "
                    + "cancel working orders on its own within " + (countdownMillis / 1000)
                    + "s of the last successful arm. Positions are not touched.";
            LOG.severe("[DeadMansSwitch] " + message);
            alerts.critical("Dead-man's switch: contact lost", message);
            halt.halt("exchange contact lost for " + (silentFor / 1000) + "s", now);
            cancelLocallyWhereStillPossible(live);
        }
    }

    /** Disarms every countdown. Called on a clean shutdown, where orders are cancelled deliberately. */
    public void disarmAll() {
        for (String symbol : List.copyOf(lastArmedAtMs.keySet())) {
            try {
                port.armDeadMansSwitch(symbol, 0);
            } catch (RuntimeException e) {
                LOG.fine("[DeadMansSwitch] disarm of " + symbol + " failed: " + e.getMessage());
            }
            lastArmedAtMs.remove(symbol);
        }
    }

    public boolean isDegraded() { return degraded; }

    /**
     * Best effort only. If the exchange were reachable this would not be running, so a failure here
     * is expected rather than exceptional — the exchange-side countdown is the mechanism that is
     * actually relied on.
     */
    private void cancelLocallyWhereStillPossible(Set<String> symbols) {
        for (String symbol : symbols) {
            try {
                port.cancelAllOpenOrders(symbol);
                LOG.warning("[DeadMansSwitch] locally cancelled working orders on " + symbol);
            } catch (RuntimeException e) {
                LOG.warning("[DeadMansSwitch] local cancel on " + symbol + " also failed ("
                        + e.getMessage() + ") — relying on the exchange-side countdown");
            }
        }
    }
}
