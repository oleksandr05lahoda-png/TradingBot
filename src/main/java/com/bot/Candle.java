package com.bot;

import java.time.Instant;

public class Candle {
    public final Instant openTime;
    public final double open;
    public final double high;
    public final double low;
    public final double close;
    public final double volume;
    public final double quoteVolume;
    public final long closeTime;

    public Candle(long openTimeMillis, double open, double high, double low, double close, double volume, double quoteVolume, long closeTime) {
        this.openTime = Instant.ofEpochMilli(openTimeMillis);
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.quoteVolume = quoteVolume;
        this.closeTime = closeTime;
    }

    // Упрощённый конструктор для кода, где раньше вызывалось new Candle(long, double, ...)
    public Candle(long openTimeMillis, double open, double high, double low, double close, double volume) {
        this.openTime = Instant.ofEpochMilli(openTimeMillis);
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.quoteVolume = 0.0;
        this.closeTime = openTimeMillis;
    }

    // Вспомогательный метод для получения времени открытия в миллисекундах
    public long getOpenTimeMillis() {
        return openTime.toEpochMilli();
    }

    // Методы для проверки свечей, которые используются в SignalSender
    public boolean isBull() {
        return close > open;
    }

    public boolean isBear() {
        return close < open;
    }
}
