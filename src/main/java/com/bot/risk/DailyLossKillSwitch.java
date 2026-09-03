package com.bot.risk;

import com.bot.core.Preconditions;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.logging.Logger;

/**
 * The daily loss limit, latched: once crossed, trading stops until the next UTC day rather than
 * resuming on a friendlier mark tick. Effective PnL is {@code realised + min(0, unrealised)}, so an
 * open loser tightens the gate but an open winner cannot offset a realised loss. Seeded from the
 * exchange at start-up, never a local file, so a restart mid-drawdown cannot clear the day's loss.
 *
 * <p>Two additions (audit 03.09). A trip is written to a small latch file, because a trip driven by
 * OPEN loss did not survive a restart: the open-PnL baseline was re-taken at the current (already
 * lost) level, the day un-tripped, a half-done flatten stopped and entries resumed. And a breach
 * may be required to persist for a short confirmation window before it latches, so one 30 s
 * mark-price wick cannot market-close the whole book — the window is 0 unless the assembly sets it.
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
    private boolean seededThisDay;

    /** Breach must persist this long before it latches; 0 = latch on first sight (the default). */
    private long confirmationWindowMs = 0;
    private Instant firstBreachAt;

    /**
     * While the realised feed is stale the effective figure may only worsen: a stop-out during the
     * outage removes its open loss at once and delivers its realised loss never, which used to
     * move the measured day TOWARD zero.
     */
    private boolean realizedStale;
    private double effectiveFloorWhileStale = Double.POSITIVE_INFINITY;
    /** The last effective figure computed, so a feed that goes stale ratchets from where it was. */
    private double lastEffective = 0.0;

    private Path latchFile;

    public DailyLossKillSwitch(double dailyLossFractionLimit) {
        Preconditions.require(dailyLossFractionLimit > 0 && dailyLossFractionLimit < 1.0,
                "dailyLossFractionLimit must be in (0, 1), got " + dailyLossFractionLimit);
        this.dailyLossFractionLimit = dailyLossFractionLimit;
    }

    /** A breach must hold for this long before the latch; the next reconcile pass confirms it. */
    public synchronized DailyLossKillSwitch withConfirmationWindowMs(long millis) {
        Preconditions.require(millis >= 0, "confirmation window must not be negative");
        this.confirmationWindowMs = millis;
        return this;
    }

    /**
     * Where a trip is written so a restart on the same UTC day comes back tripped. Loaded now: a
     * latch for today re-trips immediately; one for another day is ignored.
     */
    public synchronized DailyLossKillSwitch withLatchFile(Path path, Instant now) {
        this.latchFile = Preconditions.notNull(path, "path");
        rolloverIfNewDay(now, 0);
        if (!Files.exists(path)) return this;
        try {
            JSONObject root = new JSONObject(Files.readString(path, StandardCharsets.UTF_8));
            LocalDate day = LocalDate.parse(root.getString("day"));
            if (day.equals(utcDay)) {
                tripped = true;
                reason = root.optString("reason", "tripped before a restart") + " [restored from "
                        + path.getFileName() + "]";
                LOG.warning("[KillSwitch] restored today's TRIP from " + path + ": " + reason);
            }
        } catch (RuntimeException | IOException e) {
            LOG.warning("[KillSwitch] latch file " + path + " is unreadable (" + e.getMessage() + ") — ignored");
        }
        return this;
    }

    /** Feeds the current balance; the first call of each UTC day fixes that day's starting balance. */
    public synchronized void observeBalance(double balanceUsd, Instant now) {
        Preconditions.positiveFinite(balanceUsd, "balanceUsd");
        rolloverIfNewDay(now, balanceUsd);
        if (dayStartBalance <= 0) dayStartBalance = balanceUsd;
    }

    /**
     * Replaces today's realised PnL from the exchange's own ledger since UTC midnight. Called every
     * pass on purpose — exchange-side stop fills must count — but logged only when the figure moves:
     * an unconditional INFO here was 99% of the log file (audit 03.09).
     */
    public synchronized void seedRealizedPnl(double realizedPnlToday, Instant now) {
        Preconditions.finite(realizedPnlToday, "realizedPnlToday");
        rolloverIfNewDay(now, 0);
        boolean moved = !seededThisDay || Math.abs(realizedPnlToday - this.realizedPnl) >= 0.005;
        this.realizedPnl = realizedPnlToday;
        seededThisDay = true;
        String line = String.format("[KillSwitch] seeded realised PnL for %s from the exchange: $%+.2f",
                utcDay, realizedPnlToday);
        if (moved) LOG.info(line); else LOG.fine(line);
    }

    /** The realised feed is (not) answering; while stale the effective figure only ratchets down. */
    public synchronized void markRealizedStale(boolean stale) {
        if (stale && !realizedStale) {
            // From here the day may only read worse than it last did, until the feed is back.
            effectiveFloorWhileStale = lastEffective;
        }
        realizedStale = stale;
        if (!stale) effectiveFloorWhileStale = Double.POSITIVE_INFINITY;
    }

    /** Current unrealised PnL across everything open. Only its negative part is ever used. */
    public synchronized void observeOpenUnrealizedPnl(double pnlUsd, Instant now) {
        Preconditions.finite(pnlUsd, "pnlUsd");
        rolloverIfNewDay(now, 0);
        this.openUnrealizedPnl = pnlUsd;
        if (!baselineSet) {
            // First sight this day - or this process. Whatever is open right now is where the day
            // starts from. After a mid-day restart that forgives damage done before the restart;
            // the realised half still carries it, read from the exchange ledger since midnight —
            // and a trip taken before the restart comes back from the latch file.
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
            persistLatch(now);
        }
    }

    /** Evaluates the limit and latches if breached; this call is also what rolls the UTC day over. */
    public synchronized Status evaluate(Instant now) {
        rolloverIfNewDay(now, 0);
        // Only the move made SINCE the day began counts; a book carried in brings its own history.
        double openToday = openUnrealizedPnl - openUnrealizedAtDayStart;
        double effective = realizedPnl + Math.min(0.0, openToday);
        if (realizedStale) {
            effective = Math.min(effective, effectiveFloorWhileStale);
            effectiveFloorWhileStale = effective;
        }
        lastEffective = effective;
        if (!tripped && dayStartBalance > 0) {
            double drawdown = -effective / dayStartBalance;
            if (drawdown >= dailyLossFractionLimit) {
                String description = String.format(
                        "daily loss %.2f%% of the day's starting balance $%.2f (realised $%+.2f, open "
                                + "$%+.2f against $%+.2f at the day's start) reached the %.2f%% limit",
                        drawdown * 100, dayStartBalance, realizedPnl, openUnrealizedPnl,
                        openUnrealizedAtDayStart, dailyLossFractionLimit * 100);
                if (firstBreachAt == null) firstBreachAt = now;
                long held = now.toEpochMilli() - firstBreachAt.toEpochMilli();
                if (held >= confirmationWindowMs) {
                    tripped = true;
                    reason = description;
                    LOG.warning("[KillSwitch] TRIPPED: " + reason + " — no new positions until 00:00 UTC");
                    persistLatch(now);
                } else {
                    LOG.warning(String.format("[KillSwitch] breach observed (%s) — confirming for %d s "
                            + "before the latch", description, (confirmationWindowMs - held) / 1000));
                }
            } else {
                firstBreachAt = null;
            }
        }
        return new Status(tripped, reason, utcDay, dayStartBalance, realizedPnl, openUnrealizedPnl, effective);
    }

    public synchronized boolean isTripped(Instant now) {
        return evaluate(now).tripped();
    }

    private void persistLatch(Instant now) {
        if (latchFile == null) return;
        try {
            String body = new JSONObject()
                    .put("day", utcDay.toString())
                    .put("reason", reason)
                    .put("at", now.toString())
                    .toString();
            Path tmp = latchFile.resolveSibling(latchFile.getFileName() + ".tmp");
            Files.writeString(tmp, body, StandardCharsets.UTF_8);
            Files.move(tmp, latchFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOG.warning("[KillSwitch] could not persist the trip to " + latchFile + ": " + e.getMessage());
        }
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
            seededThisDay = false;
            tripped = false;
            reason = "";
            firstBreachAt = null;
            effectiveFloorWhileStale = Double.POSITIVE_INFINITY;
            dayStartBalance = balanceForNewDay;
            if (latchFile != null) {
                try {
                    Files.deleteIfExists(latchFile);
                } catch (IOException e) {
                    LOG.fine("[KillSwitch] stale latch file not removed: " + e.getMessage());
                }
            }
        }
    }
}
