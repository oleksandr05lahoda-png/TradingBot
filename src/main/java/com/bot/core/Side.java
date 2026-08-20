package com.bot.core;

/**
 * Position direction, deliberately not the exchange's BUY/SELL order side — a LONG opens with BUY
 * and closes with SELL, and conflating the two is how a close opens the opposite position.
 */
public enum Side {
    LONG(+1),
    SHORT(-1);

    private final int sign;

    Side(int sign) { this.sign = sign; }

    /** +1 for LONG, -1 for SHORT; the liquidation formula uses it directly. */
    public int sign() { return sign; }

    /** True when the stop is on the losing side of entry: below for a LONG, above for a SHORT. */
    public boolean isValidStopGeometry(double entryPrice, double stopPrice) {
        return this == LONG ? stopPrice < entryPrice : stopPrice > entryPrice;
    }

    /** True when the take-profit is on the winning side of entry. */
    public boolean isValidTakeProfitGeometry(double entryPrice, double takeProfitPrice) {
        return this == LONG ? takeProfitPrice > entryPrice : takeProfitPrice < entryPrice;
    }
}
