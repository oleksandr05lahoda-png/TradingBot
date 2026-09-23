package com.bot.app;

import com.bot.exec.ExchangePort;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Logger;

/**
 * The /pnl figures, computed on the MAIN LOOP at most every ten minutes and published with their
 * age. The Telegram thread never calls the exchange; a failure here only makes the figures stale.
 *
 * <p>Weight: three {@code /fapi/v1/income} reads (weight 30 each, one page each on this account)
 * every ten minutes - 90 per 10 min against Binance's 2400 per minute. Skipped entirely during an
 * exchange hold, where a call would only park the loop behind the rate limiter.
 */
final class PnlProbe {

    private static final Logger LOG = Logger.getLogger(PnlProbe.class.getName());

    static final Duration REFRESH = Duration.ofMinutes(10);

    private final ExchangePort port;
    private Instant lastAttempt;
    private OperatorSnapshot.Pnl last;

    PnlProbe(ExchangePort port) {
        this.port = port;
    }

    /** The latest figures, refreshed first when due. Never throws into the loop. */
    OperatorSnapshot.Pnl refreshIfDue(Instant now) {
        if (lastAttempt != null && Duration.between(lastAttempt, now).compareTo(REFRESH) < 0) return last;
        try {
            if (port.heldByExchangeForMillis() > 0) return last;
        } catch (RuntimeException e) {
            return last;
        }
        lastAttempt = now;
        try {
            double today = port.fetchRealizedPnlSince(startOfWarsawDay(now).toEpochMilli());
            double week = port.fetchRealizedPnlSince(now.minus(Duration.ofDays(7)).toEpochMilli());
            double month = port.fetchRealizedPnlSince(now.minus(Duration.ofDays(30)).toEpochMilli());
            last = new OperatorSnapshot.Pnl(today, week, month, now, null);
        } catch (RuntimeException e) {
            LOG.warning("[Operator] P&L refresh failed, /pnl shows the previous figures: " + e.getMessage());
            last = last == null
                    ? new OperatorSnapshot.Pnl(Double.NaN, Double.NaN, Double.NaN, null, now)
                    : new OperatorSnapshot.Pnl(last.today(), last.week(), last.month(), last.computedAt(), now);
        }
        return last;
    }

    /** Midnight on the owner's clock: his "today" is Warsaw's, not the exchange's UTC day. */
    static Instant startOfWarsawDay(Instant now) {
        return now.atZone(TgFormat.WARSAW).truncatedTo(ChronoUnit.DAYS).toInstant();
    }
}
