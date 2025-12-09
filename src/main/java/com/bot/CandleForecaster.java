package com.bot;
import java.util.*;
import java.util.stream.*;

public class CandleForecaster {

    private final int lookback; // сколько свечей учитываем для прогноза

    public CandleForecaster(int lookback) {
        this.lookback = lookback;
    }

    /**
     * Прогноз следующей цены на основе исторических цен
     * Можно тут внедрить ML/регрессию или просто расширенную линейную аппроксимацию
     */
    public Optional<Double> predictNextPrice(List<Double> closes) {
        if (closes.size() < 3) return Optional.empty();

        int n = Math.min(closes.size(), lookback);
        List<Double> recent = closes.subList(closes.size() - n, closes.size());

        // Простая линейная аппроксимация с ускорением
        double last = recent.get(n - 1);
        double prev = recent.get(n - 2);
        double prevPrev = recent.get(n - 3);

        double speed = last - prev;
        double accel = (last - prev) - (prev - prevPrev);

        double predicted = last + speed + accel; // прогноз с учетом ускорения
        return Optional.of(predicted);
    }

    /**
     * Прогноз следующей свечи (Candle)
     */
    public Optional<Candle> predictNextCandle(List<Candle> recentCandles) {
        if (recentCandles.size() < 3) return Optional.empty();

        int n = Math.min(recentCandles.size(), lookback);
        List<Candle> recent = recentCandles.subList(recentCandles.size() - n, recentCandles.size());

        Candle lastCandle = recent.get(recent.size() - 1);
        Candle prevCandle = recent.get(recent.size() - 2);
        Candle prevPrevCandle = recent.get(recent.size() - 3);

        double speed = lastCandle.close - prevCandle.close;
        double accel = (lastCandle.close - prevCandle.close) - (prevCandle.close - prevPrevCandle.close);

        double predictedOpen = lastCandle.close;
        double predictedClose = lastCandle.close + speed + accel;
        double predictedHigh = Math.max(predictedOpen, predictedClose) + Math.abs(accel) * 0.5;
        double predictedLow = Math.min(predictedOpen, predictedClose) - Math.abs(accel) * 0.5;
        long candleDuration = lastCandle.closeTime - lastCandle.getOpenTimeMillis();
        if (candleDuration <= 0) candleDuration = 60_000;

        long predictedTime = System.currentTimeMillis() + candleDuration;
        Candle predictedCandle = new Candle(
                predictedTime,
                predictedOpen,
                predictedHigh,
                predictedLow,
                predictedClose,
                lastCandle.volume,
                lastCandle.quoteVolume, // совпадает с полем
                predictedTime
        );

        return Optional.of(predictedCandle);
    }

    /**
     * Возвращает вероятность движения вверх/вниз на основе линейного прогноза
     */
    public double predictProbability(List<Double> closes) {
        Optional<Double> predictedOpt = predictNextPrice(closes);
        if (predictedOpt.isEmpty()) return 0.5; // нейтрально

        double predicted = predictedOpt.get();
        double last = closes.get(closes.size() - 1);
        double diffPct = (predicted - last) / (last + 1e-12);

        // Простой перевод изменения в вероятность: 0-1
        double prob = 0.5 + diffPct * 50; // если diffPct = 0.01 -> +0.5% к вероятности
        return Math.max(0.0, Math.min(1.0, prob));
    }
}
