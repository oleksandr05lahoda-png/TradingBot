package com.bot;

import com.bot.SignalSender.Candle;
import java.util.List;

public class MultiTFAnalyzer {

    // Возвращает 1 = LONG, -1 = SHORT, 0 = нейтрально
    public static int multiTFConfirm(int dir1h, int dir15m, int dir5m) {
        int score = 0;
        score += dir1h * 3;
        score += dir15m * 2;
        score += dir5m * 1;
        if (score > 2) return 1;
        if (score < -2) return -1;
        return 0;
    }

    public static int emaDirection(List<Candle> candles, int shortP, int longP, double hysteresis) {
        if (candles == null || candles.size() < longP + 2) return 0;
        List<Double> closes = candles.stream().map(c -> c.close).toList();
        double s = SignalSender.ema(closes, shortP);
        double l = SignalSender.ema(closes, longP);
        if (s > l * (1 + hysteresis)) return 1;
        if (s < l * (1 - hysteresis)) return -1;
        return 0;
    }
}
