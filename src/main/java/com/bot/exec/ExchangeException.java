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

    /**
     * A request that provably never left this host — DNS, connection refused, connect timeout — or
     * a read this process refused to act on before sending anything. Not ambiguous: nothing can
     * have executed. {@code httpStatus} 0 and code 0 are the signature callers may retry on.
     */
    public static ExchangeException neverSent(String message, Throwable cause) {
        return new ExchangeException(message, 0, 0, false, cause);
    }

    /**
     * True when a plain retry cannot double anything: the request never reached the exchange
     * (status 0) or the exchange refused to even look at it (429/418, which the limiter now sleeps
     * out before the next send). A coded refusal such as -2021 or -4164 is not retryable.
     */
    public boolean retryableWithoutRisk() {
        return !ambiguous && exchangeCode == 0
                && (httpStatus == 0 || httpStatus == 429 || httpStatus == 418);
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
