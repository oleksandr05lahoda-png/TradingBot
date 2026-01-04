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

        double emaFast = Indicator.calcEMA(
                closes.stream().mapToDouble(Double::doubleValue).toArray(), 9);
        double emaSlow = Indicator.calcEMA(
                closes.stream().mapToDouble(Double::doubleValue).toArray(), 21);

        double trend = emaFast - emaSlow;
        double predicted = last + trend * 0.3;

        return Optional.of(predicted);
    }
}
