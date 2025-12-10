package com.bot;

import java.util.List;

public class FuturePredictor {

    // --- Старый метод: принимает List<Candle> ---
    public static double predictNextPrice(List<Candle> candles) {
        int n = candles.size();
        if (n < 5) return candles.get(n - 1).close;

        double alpha = 2.0 / (n + 1);
        double ema = candles.get(0).close;

        for (int i = 1; i < n; i++) {
            ema = alpha * candles.get(i).close + (1 - alpha) * ema;
        }
        return ema;
    }

    // --- Старый метод: принимает массив double[] ---
    public static double predictNextPrice(double[] closes) {
        int n = closes.length;
        if (n < 5) return closes[n - 1];

        double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;

        for (int i = 0; i < n; i++) {
            double x = i;
            double y = closes[i];
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumXX += x * x;
        }

        double b = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
        double a = (sumY - b * sumX) / n;
        return a + b * n;
    }

    // --- Новый метод: прогноз на N свечей вперёд с порогом ---
    // futureCandles: количество свечей для прогноза
    // minPctChange: минимальное относительное изменение цены для генерации сигнала (например 0.005 = 0.5%)
    public static double predictNextPrice(double[] closes, int futureCandles, double minPctChange) {
        int n = closes.length;
        if (n < 5) return closes[n - 1];

        // Линейная регрессия
        double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
        for (int i = 0; i < n; i++) {
            double x = i;
            double y = closes[i];
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumXX += x * x;
        }
        double b = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
        double a = (sumY - b * sumX) / n;

        double forecastPrice = a + b * (n + futureCandles - 1);

        // Проверяем, чтобы изменение было значимое
        double lastPrice = closes[n - 1];
        double diffPct = (forecastPrice - lastPrice) / lastPrice;
        if (Math.abs(diffPct) < minPctChange) {
            // если прогноз меньше порога, оставляем текущую цену
            forecastPrice = lastPrice;
        }

        return forecastPrice;
    }
}
