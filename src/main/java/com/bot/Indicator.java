package com.bot;
public class Indicator {

    // EMA
    public static double calcEMA(double[] closes, int period) {
        if (closes.length < period) return closes[closes.length - 1];
        double alpha = 2.0 / (period + 1);
        double ema = closes[0];
        for (int i = 1; i < closes.length; i++) {
            ema = alpha * closes[i] + (1 - alpha) * ema;
        }
        return ema;
    }

    // Пример: RSI
    public static double calcRSI(double[] closes, int period) {
        if (closes.length < period + 1) return 50.0;
        double gain = 0, loss = 0;
        for (int i = closes.length - period; i < closes.length; i++) {
            double diff = closes[i] - closes[i - 1];
            if (diff > 0) gain += diff;
            else loss -= diff;
        }
        double rs = (loss == 0) ? 100 : gain / loss;
        return 100 - (100 / (1 + rs));
    }
}
