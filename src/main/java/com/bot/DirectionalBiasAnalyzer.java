package com.bot;

import com.bot.SignalSender.Candle;

import java.util.List;

public class DirectionalBiasAnalyzer {

    public enum Bias {
        BULLISH,
        BEARISH,
        NEUTRAL
    }

    public Bias detectBias(
            List<Candle> c15m,
            List<Candle> c5m,
            double vwap15
    ) {
        if (c15m.size() < 30 || c5m.size() < 20) return Bias.NEUTRAL;

        Candle last5 = c5m.get(c5m.size() - 1);
        double price = last5.close;

        // 1️⃣ VWAP displacement (ключевое)
        double vwapDev = (price - vwap15) / vwap15;

        // 2️⃣ Структура последних откатов
        int higherLows = 0;
        int lowerHighs = 0;

        for (int i = c5m.size() - 6; i < c5m.size() - 1; i++) {
            Candle prev = c5m.get(i - 1);
            Candle cur = c5m.get(i);
            if (cur.low > prev.low) higherLows++;
            if (cur.high < prev.high) lowerHighs++;
        }

        // 3️⃣ Импульс теряет силу?
        double lastBody = Math.abs(last5.close - last5.open);
        double avgBody = c5m.subList(c5m.size() - 10, c5m.size())
                .stream()
                .mapToDouble(c -> Math.abs(c.close - c.open))
                .average()
                .orElse(lastBody);

        boolean impulseFading = lastBody < avgBody * 0.55;

        // === ЛОГИКА ===

        // ЯВНЫЙ ШОРТ-БИАС
        if (vwapDev < -0.002 && lowerHighs >= 3 && impulseFading) {
            return Bias.BEARISH;
        }

        // ЯВНЫЙ ЛОНГ-БИАС
        if (vwapDev > 0.002 && higherLows >= 3 && impulseFading) {
            return Bias.BULLISH;
        }

        return Bias.NEUTRAL;
    }
}
