package com.bot.app;

import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.risk.ExposureBook;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What the main loop last saw, handed to the operator's Telegram thread. Immutable: the loop builds
 * a fresh one after every reconcile pass and the command thread only reads it, so /status, /book
 * and /pnl never call the exchange and never wait on the loop. Everything the loop did not know is
 * {@code null} or {@code NaN}, and the views say "—" rather than invent a number.
 *
 * @param at                    when the loop built it; a stale one means the loop is parked or stuck
 * @param lastReconcileOkAt     the last pass that completed, or null before the first
 * @param account               the pass's account read, or null before the first
 * @param positions             from the exchange when {@code positionsFromExchange}, else the bot's book
 * @param pnl                   realised figures, or null before the first attempt
 * @param closeQueue            closes that did not confirm flat and wait for their next attempt
 * @param killSwitch            the daily limit's own figures, or null when the loop did not read them
 */
record OperatorSnapshot(
        Instant at,
        Instant startedAt,
        String buildStamp,
        boolean observeOnly,
        boolean killSwitchTripped,
        long exchangeHoldMs,
        boolean contactLost,
        boolean blind,
        int reconcileFailures,
        int pendingCloses,
        Instant lastReconcileOkAt,
        Account account,
        List<Position> positions,
        boolean positionsFromExchange,
        Pnl pnl,
        List<QueuedClose> closeQueue,
        KillSwitch killSwitch) {

    OperatorSnapshot {
        positions = positions == null ? List.of() : List.copyOf(positions);
        closeQueue = closeQueue == null ? List.of() : List.copyOf(closeQueue);
        buildStamp = buildStamp == null || buildStamp.isBlank() ? "unknown" : buildStamp.trim();
    }

    /** Without the daily limit's figures: the screens only ever needed its tripped flag. */
    OperatorSnapshot(Instant at, Instant startedAt, String buildStamp, boolean observeOnly, boolean killSwitchTripped,
                     long exchangeHoldMs, boolean contactLost, boolean blind, int reconcileFailures, int pendingCloses,
                     Instant lastReconcileOkAt, Account account, List<Position> positions,
                     boolean positionsFromExchange, Pnl pnl, List<QueuedClose> closeQueue) {
        this(at, startedAt, buildStamp, observeOnly, killSwitchTripped, exchangeHoldMs, contactLost, blind,
                reconcileFailures, pendingCloses, lastReconcileOkAt, account, positions, positionsFromExchange, pnl,
                closeQueue, null);
    }

    /**
     * The daily loss limit as the switch itself reads it, for the owner's panel (24.09: "my budget
     * is not shown", and a paused bot said nothing about how far the day had fallen). NaN / null
     * where the switch does not know yet - before the first balance of the day, for instance.
     *
     * @param dayLossFrac today's loss as a fraction of the day's start (0.0337 = −3.37%); 0 on a green day
     * @param resumesAt   when a trip lifts by itself (the next UTC midnight); null when not tripped
     */
    record KillSwitch(boolean tripped, double limitFrac, double dayStartBalance, double dayLossFrac,
                      Instant resumesAt) {

        static KillSwitch of(com.bot.risk.DailyLossKillSwitch.Status s, double limitFrac) {
            if (s == null) return null;
            boolean known = s.dayStartBalance() > 0;
            Instant resumes = s.tripped() && s.utcDay() != null
                    ? s.utcDay().plusDays(1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant() : null;
            return new KillSwitch(s.tripped(), limitFrac, known ? s.dayStartBalance() : Double.NaN,
                    known ? s.drawdownFraction() : Double.NaN, resumes);
        }
    }

    /**
     * The same snapshot with the hold length read NOW. During a hold the loop parks inside the rate
     * limiter and publishes nothing - for 17 hours once - so the one number that explains the
     * silence has to come from the port's own clock, which is local state, not an exchange call.
     */
    OperatorSnapshot withExchangeHold(long holdMs) {
        return new OperatorSnapshot(at, startedAt, buildStamp, observeOnly, killSwitchTripped, holdMs,
                contactLost, blind, reconcileFailures, pendingCloses, lastReconcileOkAt, account, positions,
                positionsFromExchange, pnl, closeQueue, killSwitch);
    }

    /**
     * A close in the loop's retry queue: what it is, how many attempts it has spent of how many,
     * and when the next one is due. The queue lives in memory, so a restart empties it.
     */
    record QueuedClose(String symbol, String reason, int attemptsSpent, int maxAttempts, Instant nextTryAt) {}

    /**
     * Wallet includes locked margin; available excludes it.
     *
     * @param at when the reconciler read it; null when not known
     */
    record Account(double wallet, double available, double unrealized, Instant at) {

        Account(double wallet, double available, double unrealized) {
            this(wallet, available, unrealized, null);
        }

        static Account of(AccountSnapshot a) {
            return of(a, null);
        }

        static Account of(AccountSnapshot a, Instant at) {
            return a == null ? null : new Account(a.walletBalance().doubleValue(),
                    a.availableBalance().doubleValue(), a.totalUnrealizedPnl().doubleValue(), at);
        }

        /** Wallet plus open P&amp;L - Binance's "margin balance", what the account is worth now. */
        double marginBalance() {
            return wallet + unrealized;
        }

        /** Margin in use: equity minus what is still free. */
        double marginInUse() {
            return Math.max(0.0, wallet + unrealized - available);
        }
    }

    /**
     * One open position. {@code mark} is derived from the exchange's own unrealised PnL - the
     * positionRisk read the reconciler already makes - so no price fetch was added for it.
     */
    record Position(String symbol, Side side, double quantity, double entry, double mark,
                    double unrealizedUsd, double stop, double take, Instant openedAt, boolean stopOnRecord) {

        /** Price move since entry in the position's favour, percent. */
        double pnlPct() {
            if (!(entry > 0) || !(mark > 0)) return Double.NaN;
            return (mark / entry - 1.0) * 100.0 * side.sign();
        }

        /** How far the price must move to reach the stop, signed: a long's stop reads negative. */
        double toStopPct() {
            return move(stop);
        }

        /** How far the price must move to reach the take, signed: a long's take reads positive. */
        double toTakePct() {
            return move(take);
        }

        private double move(double level) {
            if (!(mark > 0) || !(level > 0)) return Double.NaN;
            return (level / mark - 1.0) * 100.0;
        }

        /** Whole hours held, or -1 when the journal did not say when it opened. */
        long hoursHeld(Instant now) {
            if (openedAt == null || now == null) return -1;
            return Math.max(0, Duration.between(openedAt, now).toHours());
        }
    }

    /**
     * Net realised result - trades, commissions and funding together, as the exchange's income
     * ledger totals them (the port gives no split). {@code failedAt} set means the latest attempt
     * failed and the figures, if any, are from {@code computedAt}.
     */
    record Pnl(double today, double week, double month, Instant computedAt, Instant failedAt) {

        boolean known() {
            return computedAt != null;
        }
    }

    /**
     * Rows for /book: the exchange's positions (entry, and mark via unrealised PnL), the book's stop
     * record, the journal's take and open time. The journal only fills in when its side agrees with
     * the exchange - a stale row from an older trade on the same coin must not date a new one.
     * Without an exchange read, the book alone, marked as such, with no prices.
     */
    static List<Position> positions(List<PositionSnapshot> live, List<ExposureBook.OpenPosition> book,
                                    Map<String, TradeJournal.OpenMark> marks) {
        Map<String, ExposureBook.OpenPosition> booked = new LinkedHashMap<>();
        if (book != null) for (ExposureBook.OpenPosition p : book) booked.put(p.symbol(), p);
        Map<String, TradeJournal.OpenMark> journal = marks == null ? Map.of() : marks;
        List<Position> out = new ArrayList<>();
        if (live != null) {
            for (PositionSnapshot p : live) {
                if (p.isFlat()) continue;
                Side side = p.direction().orElseThrow();
                double qty = p.absoluteQuantity().doubleValue();
                double entry = p.entryPrice().doubleValue();
                double upnl = p.unrealizedPnl().doubleValue();
                double signedQty = p.signedQuantity().doubleValue();
                double mark = entry > 0 && signedQty != 0 ? entry + upnl / signedQty : Double.NaN;
                out.add(row(p.symbol(), side, qty, entry, mark, upnl, booked.get(p.symbol()), journal.get(p.symbol())));
            }
        } else {
            for (ExposureBook.OpenPosition b : booked.values()) {
                out.add(row(b.symbol(), b.side(), b.quantity().doubleValue(), b.entryPrice(),
                        Double.NaN, Double.NaN, b, journal.get(b.symbol())));
            }
        }
        out.sort(Comparator.comparing(Position::symbol));
        return out;
    }

    private static Position row(String symbol, Side side, double qty, double entry, double mark, double upnl,
                                ExposureBook.OpenPosition booked, TradeJournal.OpenMark mark0) {
        TradeJournal.OpenMark j = mark0 != null && side.name().equals(mark0.side()) ? mark0 : null;
        double stop = j != null ? j.stopPrice() : Double.NaN;
        if (Double.isNaN(stop) && booked != null && booked.side() == side && booked.riskUsd() > 0
                && booked.quantity().signum() > 0) {
            // The book keeps the stop as dollars at risk for its size; the distance is the same thing.
            double distance = booked.riskUsd() / booked.quantity().doubleValue();
            stop = booked.entryPrice() - side.sign() * distance;
            if (!(stop > 0)) stop = Double.NaN;
        }
        return new Position(symbol, side, qty, entry, mark, upnl, stop,
                j != null ? j.takePrice() : Double.NaN,
                j != null ? j.openedAt() : null,
                booked != null && booked.protectiveStopId().isPresent());
    }
}
