package com.bot.paper;

/**
 * One OHLCV bar. Immutable.
 *
 * {@code openMs} is the bar's open time as stored in klines_4h / klines_1h (ts_ms).
 * {@code closeMs} is the instant the bar is FINISHED, i.e. openMs + interval. A bar counts as
 * "known" at time T only when {@code closeMs <= T} — a bar that is still forming has a close, high
 * and low that can still change, and letting a hypothesis read those is look-ahead.
 */
public final class Bar {
    public final String symbol;
    public final long   openMs;
    public final long   closeMs;
    public final double open;
    public final double high;
    public final double low;
    public final double close;
    public final double volume;

    public Bar(String symbol, long openMs, long intervalMs,
               double open, double high, double low, double close, double volume) {
        if (intervalMs <= 0) throw new IllegalArgumentException("intervalMs must be positive");
        this.symbol  = symbol;
        this.openMs  = openMs;
        this.closeMs = openMs + intervalMs;
        this.open    = open;
        this.high    = high;
        this.low     = low;
        this.close   = close;
        this.volume  = volume;
    }

    @Override public String toString() {
        return symbol + "@" + openMs + " o=" + open + " h=" + high + " l=" + low + " c=" + close;
    }
}
