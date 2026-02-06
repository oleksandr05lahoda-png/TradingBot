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

        public Candle(long openTime, double open, double high, double low,
                      double close, double volume, double quoteAssetVolume, long closeTime) {
            this.openTime = openTime;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.quoteAssetVolume = quoteAssetVolume;
            this.closeTime = closeTime;
        }

        @Override
        public String toString() {
            return String.format("Candle[%d - %d | O: %.4f H: %.4f L: %.4f C: %.4f V: %.4f]",
                    openTime, closeTime, open, high, low, close, volume);
        }
    }

    // ===================== SIDE =====================
    public enum Side {
        LONG,
        SHORT
    }

    // ===================== COIN TYPE =====================
    public enum CoinType {
        TOP,
        ALT,
        MEME
    }

    // ===================== RISK ENGINE =====================
    public static class RiskEngine {

        private final double minRiskPct; // минимальный риск от цены

        public RiskEngine(double minRiskPct) {
            this.minRiskPct = minRiskPct;
        }

        public static class TradeSignal {
            public final String symbol;
            public final Side side;
            public final double entry;
            public final double stop;
            public final double take;
            public final double confidence;
            public final String reason;
            public final CoinType type;

            public TradeSignal(String symbol, Side side, double entry,
                               double stop, double take, double confidence,
                               String reason, CoinType type) {
                this.symbol = symbol;
                this.side = side;
                this.entry = entry;
                this.stop = stop;
                this.take = take;
                this.confidence = confidence;
                this.reason = reason;
                this.type = type;
            }

            @Override
            public String toString() {
                return String.format("TradeSignal[%s | %s | %s | Entry: %.4f Stop: %.4f Take: %.4f Conf: %.2f Reason: %s]",
                        symbol, type, side, entry, stop, take, confidence, reason);
            }
        }

        public TradeSignal applyRisk(String symbol, Side side, double entry, double atr,
                                     double confidence, String reason, CoinType type) {

            double riskMultiplier = switch (type) {
                case TOP -> 1.0;
                case ALT -> 1.2;
                case MEME -> 1.5;
            };

            double takeMultiplier = switch (type) {
                case TOP -> 2.5;
                case ALT -> 3.0;
                case MEME -> 4.0;
            };

            double risk = Math.max(atr * riskMultiplier, entry * minRiskPct);
            double stop = side == Side.LONG ? entry - risk : entry + risk;
            double take = side == Side.LONG ? entry + risk * takeMultiplier : entry - risk * takeMultiplier;

            return new TradeSignal(symbol, side, entry, stop, take, confidence, reason, type);
        }
    }

    // ===================== ADAPTIVE BRAIN =====================
    public static class AdaptiveBrain {

        private final Map<String, Double> symbolBias = new ConcurrentHashMap<>();
        private final Map<String, Integer> streaks = new ConcurrentHashMap<>();

        /**
         * Корректирует уверенность сигнала с учетом стратегии, монеты, объема и текущих сессий.
         */
        public double applyAllAdjustments(String strategy, String symbol, double baseConfidence,
                                          CoinType type, boolean highVol, boolean lowVol) {
            double conf = baseConfidence;

            conf += adaptStrategy(strategy);
            conf += symbolBias.getOrDefault(symbol, 0.0);
            conf += sessionBoost();
            conf += switch (type) {
                case TOP -> 0.00;
                case ALT -> 0.02;
                case MEME -> 0.04;
            };

            if (highVol) conf += 0.03;
            if (lowVol) conf -= 0.02;

            return clamp(conf, 0.0, 0.95);
        }

        private double adaptStrategy(String strategy) {
            return "ELITE5".equalsIgnoreCase(strategy) ? 0.02 : 0.0;
        }

        private double sessionBoost() {
            int totalStreak = streaks.values().stream().mapToInt(i -> i).sum();
            if (totalStreak > 3) return 0.03;
            if (totalStreak < -3) return -0.03;
            return 0.0;
        }

        public void registerResult(String symbol, boolean win) {
            streaks.merge(symbol, win ? 1 : -1, Integer::sum);
            symbolBias.merge(symbol, win ? 0.01 : -0.01, Double::sum);
        }

        private double clamp(double value, double min, double max) {
            return Math.max(min, Math.min(max, value));
        }
    }
}
