package com.bot.exec;

/**
 * A failure from, or on the way to, the exchange. A coded refusal definitely did not execute; an
 * {@link #ambiguous} failure may have, so callers must ask the exchange rather than retry blindly.
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

    public static ExchangeException ambiguous(String message, Throwable cause) {
        return new ExchangeException(message, 0, 0, true, cause);
    }

    public int httpStatus() { return httpStatus; }

    /** Binance's numeric error code, or 0 when the failure never reached it. */
    public int exchangeCode() { return exchangeCode; }

    public boolean ambiguous() { return ambiguous; }

    @Override public String toString() {
        return "ExchangeException[http=" + httpStatus + " code=" + exchangeCode
                + " ambiguous=" + ambiguous + "] " + getMessage();
    }
}
