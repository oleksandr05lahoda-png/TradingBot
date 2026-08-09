package com.bot.exec;

/**
 * A failure that came from, or on the way to, the exchange.
 *
 * <p>{@link #ambiguous} is the distinction that matters. A request that was refused with an error
 * code definitely did not execute. A request that timed out, or died mid-flight, may have executed
 * perfectly with only the response lost — and blindly retrying that one is how an account ends up
 * with two positions where it planned one. Callers use this flag to decide between "retry" and
 * "ask the exchange what happened first".
 */
public class ExchangeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int httpStatus;
    private final int exchangeCode;
    private final boolean ambiguous;

    public ExchangeException(String message, int httpStatus, int exchangeCode, boolean ambiguous, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
        this.exchangeCode = exchangeCode;
        this.ambiguous = ambiguous;
    }

    public static ExchangeException refused(String message, int httpStatus, int exchangeCode) {
        return new ExchangeException(message, httpStatus, exchangeCode, false, null);
    }

    /** The outcome is unknown: the request may or may not have been executed. */
    public static ExchangeException ambiguous(String message, Throwable cause) {
        return new ExchangeException(message, 0, 0, true, cause);
    }

    public int httpStatus() { return httpStatus; }

    /** Binance's numeric error code, or 0 when the failure never reached it. */
    public int exchangeCode() { return exchangeCode; }

    /** True when the request may have taken effect despite the failure. */
    public boolean ambiguous() { return ambiguous; }

    @Override public String toString() {
        return "ExchangeException[http=" + httpStatus + " code=" + exchangeCode
                + " ambiguous=" + ambiguous + "] " + getMessage();
    }
}
