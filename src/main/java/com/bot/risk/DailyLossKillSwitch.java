package com.bot.risk;

import com.bot.core.Preconditions;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.logging.Logger;

/**
 * The daily loss limit, latched: once crossed, trading stops until the next UTC day rather than
 * resuming on a friendlier mark tick. Effective PnL is {@code realised + min(0, unrealised)}, so an
 * open loser tightens the gate but an open winner cannot offset a realised loss. Seeded from the
 * exchange at start-up, never a local file, so a restart mid-drawdown cannot clear the day's loss.
 */
public final class DailyLossKillSwitch {

    private static final Logger LOG = Logger.getLogger(DailyLossKillSwitch.class.getName());

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
    /**
     * Open PnL as it stood when this day began, so only TODAY's movement counts against the limit.
     * Without it a book carried into a new day brings yesterday's paper loss with it: realised
     * resets at midnight but unrealised does not, and the switch reads an old loss as a fresh one.
     * Harmless while the trip only blocked entries; with KILL_SWITCH_ACTION=flatten it would
     * market-close the whole book seconds after a restart (29.08 audit, before first deploy).
     */
    private double openUnrealizedAtDayStart;
    private boolean baselineSet;
    private boolean tripped;
    private String reason = "";

    public DailyLossKillSwitch(double dailyLossFractionLimit) {
        Preconditions.require(dailyLossFractionLimit > 0 && dailyLossFractionLimit < 1.0,
                "dailyLossFractionLimit must be in (0, 1), got " + dailyLossFractionLimit);
        this.dailyLossFractionLimit = dailyLossFractionLimit;
    }

    /** Feeds the current balance; the first call of each UTC day fixes that day's starting balance. */
    public synchronized void observeBalance(double balanceUsd, Instant now) {
        Preconditions.positiveFinite(balanceUsd, "balanceUsd");
        rolloverIfNewDay(now, balanceUsd);
        if (dayStartBalance <= 0) dayStartBalance = balanceUsd;
    }

    /** Replaces today's realised PnL from the exchange's own ledger since UTC midnight. */
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
        if (!baselineSet) {
            // First sight this day - or this process. Whatever is open right now is where the day
            // starts from. After a mid-day restart that forgives damage done before the restart;
            // the realised half still carries it, read from the exchange ledger since midnight.
            openUnrealizedAtDayStart = pnlUsd;
            baselineSet = true;
            LOG.info(String.format("[KillSwitch] day-start open PnL baseline for %s: $%+.2f",
                    utcDay, pnlUsd));
        }
    }

    /** Operator hook. Unlike {@code exec.TradingHalt} (drift, operator-cleared) this self-clears at UTC rollover. */
    public synchronized void trip(String why, Instant now) {
        rolloverIfNewDay(now, 0);
        if (!tripped) {
            tripped = true;
            reason = Preconditions.notBlank(why, "why");
            LOG.warning("[KillSwitch] TRIPPED: " + reason + " — no new positions until 00:00 UTC");
        }
    }

    /** Evaluates the limit and latches if breached; this call is also what rolls the UTC day over. */
    public synchronized Status evaluate(Instant now) {
        rolloverIfNewDay(now, 0);
        // Only the move made SINCE the day began counts; a book carried in brings its own history.
        double openToday = openUnrealizedPnl - openUnrealizedAtDayStart;
        double effective = realizedPnl + Math.min(0.0, openToday);
        if (!tripped && dayStartBalance > 0) {
            double drawdown = -effective / dayStartBalance;
            if (drawdown >= dailyLossFractionLimit) {
                tripped = true;
                reason = String.format(
                        "daily loss %.2f%% of the day's starting balance $%.2f (realised $%+.2f, open "
                                + "$%+.2f against $%+.2f at the day's start) reached the %.2f%% limit",
                        drawdown * 100, dayStartBalance, realizedPnl, openUnrealizedPnl,
                        openUnrealizedAtDayStart, dailyLossFractionLimit * 100);
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
            // The new day inherits whatever is open; the next observation re-fixes the baseline.
            openUnrealizedAtDayStart = 0;
            baselineSet = false;
            tripped = false;
            reason = "";
            dayStartBalance = balanceForNewDay;
        }
    }
}
