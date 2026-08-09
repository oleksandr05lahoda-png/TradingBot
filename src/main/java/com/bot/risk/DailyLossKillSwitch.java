package com.bot.risk;

import com.bot.core.Preconditions;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.logging.Logger;

/**
 * The daily loss limit, and the latch it trips.
 *
 * <p>Two design decisions carry the safety here.
 *
 * <p><b>It latches.</b> Once the day's loss crosses the limit, trading stops until the next UTC day
 * begins — it does not resume because the next mark tick made the number look better. A limit that
 * un-trips is a limit that gets tested repeatedly by the same losing session, which is precisely the
 * session it exists to end.
 *
 * <p><b>Open drawdown counts, but only against you.</b> The effective day PnL is
 * {@code realised + min(0, unrealised)}. A position sitting at -8% must block new entries before it
 * closes, otherwise the cap only ever notices losses that have already been taken. The
 * {@code min(0, ...)} is the other half: an open <i>winner</i> must not offset a realised loss.
 * Paper gains evaporate; a realised -3% is -3% whatever the screen says. So the unrealised term can
 * only tighten the gate, never loosen it.
 *
 * <p>State is in memory and is <b>seeded from the exchange</b> at start-up rather than from a local
 * file — see {@link #seedRealizedPnl}. A restart mid-drawdown must not clear the day's loss, and the
 * exchange's income ledger is the only account of it that a crashed process cannot have corrupted.
 */
public final class DailyLossKillSwitch {

    private static final Logger LOG = Logger.getLogger(DailyLossKillSwitch.class.getName());

    /**
     * @param tripped           whether trading is stopped
     * @param reason            human-readable cause, empty when not tripped
     * @param utcDay            the UTC day this state belongs to
     * @param dayStartBalance   balance at the first observation of the day
     * @param realizedPnl       realised PnL booked today
     * @param openUnrealizedPnl unrealised PnL of everything currently open
     * @param effectivePnl      {@code realizedPnl + min(0, openUnrealizedPnl)}
     */
    public record Status(
            boolean tripped,
            String reason,
            LocalDate utcDay,
            double dayStartBalance,
            double realizedPnl,
            double openUnrealizedPnl,
            double effectivePnl) {

        public double drawdownFraction() {
            return dayStartBalance > 0 ? Math.max(0.0, -effectivePnl / dayStartBalance) : 0.0;
        }
    }

    private final double dailyLossFractionLimit;

    private LocalDate utcDay;
    private double dayStartBalance;
    private double realizedPnl;
    private double openUnrealizedPnl;
    private boolean tripped;
    private String reason = "";

    public DailyLossKillSwitch(double dailyLossFractionLimit) {
        Preconditions.require(dailyLossFractionLimit > 0 && dailyLossFractionLimit < 1.0,
                "dailyLossFractionLimit must be in (0, 1), got " + dailyLossFractionLimit);
        this.dailyLossFractionLimit = dailyLossFractionLimit;
    }

    /**
     * Feeds the current balance. The first call of each UTC day fixes that day's starting balance,
     * which is what the limit is a percentage of.
     */
    public synchronized void observeBalance(double balanceUsd, Instant now) {
        Preconditions.positiveFinite(balanceUsd, "balanceUsd");
        rolloverIfNewDay(now, balanceUsd);
        if (dayStartBalance <= 0) dayStartBalance = balanceUsd;
    }

    /** Books a realised PnL event. Positive for a win, negative for a loss. */
    public synchronized void recordRealizedPnl(double pnlUsd, Instant now) {
        Preconditions.finite(pnlUsd, "pnlUsd");
        rolloverIfNewDay(now, 0);
        realizedPnl += pnlUsd;
    }

    /**
     * Replaces today's realised PnL wholesale. This is how the process recovers after a restart:
     * the caller sums the exchange's own realised-PnL ledger since UTC midnight and hands it over,
     * so a crash cannot be used — accidentally or otherwise — to reset the day's loss to zero.
     */
    public synchronized void seedRealizedPnl(double realizedPnlToday, Instant now) {
        Preconditions.finite(realizedPnlToday, "realizedPnlToday");
        rolloverIfNewDay(now, 0);
        this.realizedPnl = realizedPnlToday;
        LOG.info(String.format("[KillSwitch] seeded realised PnL for %s from the exchange: $%+.2f",
                utcDay, realizedPnlToday));
    }

    /** Current unrealised PnL across everything open. Only its negative part is ever used. */
    public synchronized void observeOpenUnrealizedPnl(double pnlUsd, Instant now) {
        Preconditions.finite(pnlUsd, "pnlUsd");
        rolloverIfNewDay(now, 0);
        this.openUnrealizedPnl = pnlUsd;
    }

    /** Trips the latch by hand — used by the reconciler on drift and by the operator. */
    public synchronized void trip(String why, Instant now) {
        rolloverIfNewDay(now, 0);
        if (!tripped) {
            tripped = true;
            reason = Preconditions.notBlank(why, "why");
            LOG.warning("[KillSwitch] TRIPPED: " + reason + " — no new positions until 00:00 UTC");
        }
    }

    /**
     * Evaluates the limit and latches if it is breached. Call before every gate decision; it is the
     * evaluation itself that rolls the day over.
     */
    public synchronized Status evaluate(Instant now) {
        rolloverIfNewDay(now, 0);
        double effective = realizedPnl + Math.min(0.0, openUnrealizedPnl);
        if (!tripped && dayStartBalance > 0) {
            double drawdown = -effective / dayStartBalance;
            if (drawdown >= dailyLossFractionLimit) {
                tripped = true;
                reason = String.format(
                        "daily loss %.2f%% of the day's starting balance $%.2f (realised $%+.2f, open $%+.2f) "
                                + "reached the %.2f%% limit",
                        drawdown * 100, dayStartBalance, realizedPnl, openUnrealizedPnl,
                        dailyLossFractionLimit * 100);
                LOG.warning("[KillSwitch] TRIPPED: " + reason + " — no new positions until 00:00 UTC");
            }
        }
        return new Status(tripped, reason, utcDay, dayStartBalance, realizedPnl, openUnrealizedPnl, effective);
    }

    public synchronized boolean isTripped(Instant now) {
        return evaluate(now).tripped();
    }

    private void rolloverIfNewDay(Instant now, double balanceForNewDay) {
        Preconditions.notNull(now, "now");
        LocalDate today = LocalDate.ofInstant(now, ZoneOffset.UTC);
        if (utcDay == null) {
            utcDay = today;
            dayStartBalance = balanceForNewDay;
            return;
        }
        if (!today.equals(utcDay)) {
            LOG.info(String.format("[KillSwitch] UTC day rollover %s -> %s (realised $%+.2f, tripped=%s)",
                    utcDay, today, realizedPnl, tripped));
            utcDay = today;
            realizedPnl = 0;
            openUnrealizedPnl = 0;
            tripped = false;
            reason = "";
            dayStartBalance = balanceForNewDay;
        }
    }
}
