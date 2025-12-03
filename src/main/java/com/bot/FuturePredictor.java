package com.bot;

import java.util.List;

public class FuturePredictor {

    // Существующий метод
    public static double predictNextPrice(List<Candle> candles) {
        int n = candles.size();
        if (n < 5) return candles.get(n - 1).close;

        double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;

        for (int i = 0; i < n; i++) {
            double x = i;
            double y = candles.get(i).close;
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumXX += x * x;
        }

        double b = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
        double a = (sumY - b * sumX) / n;
        return a + b * n;
    }

    // Новый метод: принимает массив double[]
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
}
