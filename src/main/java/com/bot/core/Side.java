package com.bot.core;

/**
 * Direction of a position. Deliberately separate from the exchange's BUY/SELL order side:
 * a LONG position is opened with BUY and closed with SELL, so conflating the two is how a
 * close ends up opening the opposite side.
 */
public enum Side {
    LONG(+1),
    SHORT(-1);

    private final int sign;

    Side(int sign) { this.sign = sign; }

    /** +1 for LONG, -1 for SHORT. Used directly by the liquidation formula. */
    public int sign() { return sign; }

    public Side opposite() { return this == LONG ? SHORT : LONG; }

    /** True when the stop is on the losing side of entry: below for a LONG, above for a SHORT. */
    public boolean isValidStopGeometry(double entryPrice, double stopPrice) {
        return this == LONG ? stopPrice < entryPrice : stopPrice > entryPrice;
    }

    /** True when the take-profit is on the winning side of entry. */
    public boolean isValidTakeProfitGeometry(double entryPrice, double takeProfitPrice) {
        return this == LONG ? takeProfitPrice > entryPrice : takeProfitPrice < entryPrice;
    }
}
