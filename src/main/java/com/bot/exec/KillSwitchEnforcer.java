package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.risk.DailyLossKillSwitch;
import com.bot.risk.ExposureBook;
import com.bot.risk.RiskEngine;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.logging.Logger;

/**
 * Gives the daily loss limit an action, not just a verdict. Until 28.08 a tripped
 * {@link DailyLossKillSwitch} only refused NEW entries while the open losers ran on to their stops:
 * with 15 slots at 0.5% risk each, "-3% stops the day" really meant -7.5%. On the trip this closes
 * the whole book reduce-only through the same path as an operator {@code /close} — stops and takes
 * are cancelled with each position, closes ignore the halt latch by design — so the limit means
 * what it says. Failed closes are retried on later passes; the positions keep their exchange-side
 * stops in the meantime, so the worst case is the old behaviour, never worse.
 *
 * <p>{@code KILL_SWITCH_ACTION=halt-only} restores the pre-28.08 behaviour (block entries, close
 * nothing) for an operator who prefers the old shape. The default is to flatten.
 */
public final class KillSwitchEnforcer {

    private static final Logger LOG = Logger.getLogger(KillSwitchEnforcer.class.getName());

    public enum Action { FLATTEN, HALT_ONLY }

    static final int MAX_FLATTEN_ATTEMPTS = 5;

    private final RiskEngine engine;
    private final Reconciler.PositionCloser closer;
    private final AlertSink alerts;
    private final Action action;
    private final CloseObserver closedListener;

    private LocalDate announcedDay;
    private int attempts;
    private boolean gaveUp;
    /**
     * Seconds since the epoch of this process's first trip, mixed into every request id. The attempt
     * counter lives in memory, so after a same-day restart it starts at 1 again and would mint the
     * id a pre-crash attempt already used — {@link IdempotentOrderPlacer} would then adopt that
     * stale FILLED order instead of sending, and a partially-closed position would be read as flat.
     */
    private long runStampSeconds;

    public KillSwitchEnforcer(RiskEngine engine, Reconciler.PositionCloser closer, AlertSink alerts,
                              Action action, CloseObserver closedListener) {
        this.engine = Preconditions.notNull(engine, "engine");
        this.closer = Preconditions.notNull(closer, "closer");
        this.alerts = Preconditions.notNull(alerts, "alerts");
        this.action = Preconditions.notNull(action, "action");
        this.closedListener = closedListener;
    }

    /**
     * Called once per reconcile pass, right after the kill switch was fed fresh numbers. Does
     * nothing while the switch is armed; on a trip closes every booked position reduce-only,
     * retrying on later passes until the book is flat or {@link #MAX_FLATTEN_ATTEMPTS} is spent.
     */
    public void enforce(Instant now) throws InterruptedException {
        Preconditions.notNull(now, "now");
        DailyLossKillSwitch.Status status = engine.killSwitch().evaluate(now);
        if (!status.tripped()) {
            // Armed again — the UTC rollover cleared the trip; the next one starts fresh.
            attempts = 0;
            gaveUp = false;
            announcedDay = null;
            return;
        }

        LocalDate day = LocalDate.ofInstant(now, ZoneOffset.UTC);
        boolean firstSight = !day.equals(announcedDay);
        announcedDay = day;

        if (action == Action.HALT_ONLY) {
            if (firstSight) {
                alerts.critical("Daily loss limit hit", status.reason()
                        + ". KILL_SWITCH_ACTION=halt-only: open positions are NOT closed and run "
                        + "to their exchange stops; no new entries until 00:00 UTC.");
            }
            return;
        }
        if (runStampSeconds == 0) runStampSeconds = now.getEpochSecond();

        // Only what this bot sized. A position with no recorded risk was adopted from the exchange —
        // the owner's own hand trade or a hedge — and market-closing it is not this process's call,
        // exactly as Reconciler.handleMissingStop refuses to. They keep their own protection.
        List<ExposureBook.OpenPosition> book = engine.book().all();
        List<ExposureBook.OpenPosition> open = book.stream().filter(p -> p.riskUsd() > 0).toList();
        List<String> leftAlone = book.stream()
                .filter(p -> p.riskUsd() <= 0)
                .map(ExposureBook.OpenPosition::symbol)
                .toList();
        if (open.isEmpty()) {
            if (firstSight) {
                alerts.critical("Daily loss limit hit", status.reason()
                        + ". Nothing this bot opened is left to close" + describeLeftAlone(leftAlone)
                        + "; no new entries until 00:00 UTC.");
            }
            return;
        }
        if (gaveUp) return;

        attempts++;
        if (runStampSeconds == 0) runStampSeconds = now.getEpochSecond();
        if (firstSight) {
            alerts.critical("Daily loss limit hit — closing the book", status.reason()
                    + ". Closing " + open.size() + " position(s) reduce-only so the day stops at "
                    + "the limit instead of running every stop out" + describeLeftAlone(leftAlone)
                    + "; no new entries until 00:00 UTC.");
        }

        int closed = 0;
        int failed = 0;
        for (ExposureBook.OpenPosition position : open) {
            // Run stamp + attempt number keep a retry from adopting a stale terminal order under the
            // same id; reduce-only makes an accidental duplicate harmless — it cannot over-close.
            String requestId = "ks-" + day + "-" + position.symbol()
                    + "-" + runStampSeconds + "-a" + attempts;
            try {
                ExecutionCoordinator.CloseReport report = closer.close(position.symbol(), requestId);
                if (report.flat()) {
                    closed++;
                    if (closedListener != null) {
                        try {
                            closedListener.closed(requestId, position.symbol(), report);
                        } catch (RuntimeException e) {
                            LOG.fine("[KillSwitchEnforcer] closed listener failed: " + e.getMessage());
                        }
                    }
                } else {
                    failed++;
                    LOG.severe("[KillSwitchEnforcer] " + position.symbol()
                            + " did not close: " + report.note());
                }
            } catch (RuntimeException e) {
                failed++;
                LOG.severe("[KillSwitchEnforcer] close of " + position.symbol()
                        + " failed: " + e.getMessage());
            }
        }

        if (failed == 0) {
            alerts.warning("Book closed on the daily loss limit",
                    closed + " position(s) closed reduce-only" + describeLeftAlone(leftAlone)
                            + "; trading resumes at 00:00 UTC");
            return;
        }
        if (attempts >= MAX_FLATTEN_ATTEMPTS) {
            gaveUp = true;
            alerts.critical("Kill switch could not flatten the book",
                    failed + " position(s) refused to close after " + attempts + " attempt(s). "
                            + "They keep their exchange-side stops; close them by hand (/close). "
                            + "No further attempts today.");
        } else {
            LOG.warning("[KillSwitchEnforcer] " + failed + " close(s) failed on attempt " + attempts
                    + "/" + MAX_FLATTEN_ATTEMPTS + " — retrying next pass");
        }
    }

    /** Names what the flatten deliberately did not touch, so silence is never mistaken for coverage. */
    private static String describeLeftAlone(List<String> leftAlone) {
        if (leftAlone.isEmpty()) return "";
        return ". NOT closed (adopted from the exchange, not opened by this bot — they keep their "
                + "own protection): " + String.join(", ", leftAlone);
    }
}
