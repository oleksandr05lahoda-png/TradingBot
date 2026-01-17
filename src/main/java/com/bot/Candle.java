package com.bot;

public class Candle {
    public final long openTime;
    public final double open, high, low, close, volume, quoteAssetVolume;
    public final long closeTime;

    public Candle(long openTime, double open, double high, double low,
                  double close, double volume, double quoteAssetVolume, long closeTime) {
        this.openTime = openTime;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.quoteAssetVolume = quoteAssetVolume;
        this.closeTime = closeTime;
    }

    public boolean isBull() { return close > open; }
    public boolean isBear() { return close < open; }
}
