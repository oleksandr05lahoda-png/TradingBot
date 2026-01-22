package com.bot;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TradingCore {

    // ===================== CANDLE =====================
    public static class Candle {
        public final long openTime;
        public final long closeTime;
        public final double open;
        public final double high;
        public final double low;
        public final double close;
        public final double volume;
        public final double quoteAssetVolume;

        public Candle(long openTime,
                      double open,
                      double high,
                      double low,
                      double close,
                      double volume,
                      double quoteAssetVolume,
                      long closeTime) {
            this.openTime = openTime;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.quoteAssetVolume = quoteAssetVolume;
            this.closeTime = closeTime;
        }
    }

    // ===================== RISK ENGINE =====================
    public static class RiskEngine {

        private final double minRiskPct; // минимальный риск от цены (например 0.005 = 0.5%)

        public RiskEngine(double minRiskPct) {
            this.minRiskPct = minRiskPct;
        }

        public static class TradeSignal {
            public String symbol;
            public String side;     // LONG / SHORT
            public double entry;
            public double stop;
            public double take;
            public double confidence;
            public String reason;
        }

        public TradeSignal applyRisk(
                String symbol,
                String side,
                double entry,
                double atr,
                double confidence,
                String reason
        ) {
            TradeSignal ts = new TradeSignal();
            ts.symbol = symbol;
            ts.side = side;
            ts.entry = entry;
            ts.confidence = confidence;
            ts.reason = reason;

            // базовый риск через ATR
            double risk = atr * 1.2;

            // защита от микроскопического ATR
            double minRisk = entry * minRiskPct;
            if (risk < minRisk) {
                risk = minRisk;
            }

            if ("LONG".equalsIgnoreCase(side)) {
                ts.stop = entry - risk;
                ts.take = entry + risk * 2.5;
            } else {
                ts.stop = entry + risk;
                ts.take = entry - risk * 2.5;
            }

            return ts;
        }
    }

    // ===================== ADAPTIVE BRAIN =====================
    public static class AdaptiveBrain {

        private final Map<String, Double> symbolBias = new ConcurrentHashMap<>();
        private final Map<String, Integer> streaks = new ConcurrentHashMap<>();

        /**
         * Основной метод, который ты используешь ВЕЗДЕ
         */
        public double applyAllAdjustments(
                String strategy,
                String symbol,
                double baseConfidence
        ) {
            double conf = baseConfidence;

            conf = adapt(strategy, conf);
            conf += symbolBias(symbol);
            conf += sessionBoost();

            return clamp(conf, 0.0, 0.95);
        }

        /**
         * Адаптация под стратегию (можно расширять)
         */
        public double adapt(String strategy, double confidence) {
            if ("ELITE5".equalsIgnoreCase(strategy)) {
                return confidence + 0.02;
            }
            return confidence;
        }

        /**
         * Усиление / ослабление по истории символа
         */
        private double symbolBias(String symbol) {
            return symbolBias.getOrDefault(symbol, 0.0);
        }

        /**
         * Лёгкий буст, если серия удачная
         */
        public double sessionBoost() {
            int streak = streaks.values().stream().mapToInt(i -> i).sum();
            if (streak > 3) return 0.03;
            if (streak < -3) return -0.03;
            return 0.0;
        }

        /**
         * Вызывай после закрытия сделки
         */
        public void registerResult(String symbol, boolean win) {
            streaks.merge(symbol, win ? 1 : -1, Integer::sum);
            symbolBias.merge(symbol, win ? 0.01 : -0.01, Double::sum);
        }

        private double clamp(double v, double min, double max) {
            return Math.max(min, Math.min(max, v));
        }
    }
}
