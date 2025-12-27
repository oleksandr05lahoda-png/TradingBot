package com.bot;
public class FuturePredictor {


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

        double lastPrice = closes[n - 1];

// --- Фильтр флэта: если наклон почти нулевой, рынок мёртв ---
        if (Math.abs(b) < lastPrice * 0.0002) {
            return lastPrice;
        }

        double forecastPrice = a + b * (n + futureCandles - 1);
        double diffPct = (forecastPrice - lastPrice) / lastPrice;
        if (Math.abs(diffPct) < minPctChange) {
            forecastPrice = lastPrice;
        }

        return forecastPrice;
    }
}
