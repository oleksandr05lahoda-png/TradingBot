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

    // ===================== SIDE =====================
    public enum Side {
        LONG,
        SHORT
    }

    // ===================== RISK ENGINE =====================
    public static class RiskEngine {

        private final double minRiskPct; // минимальный риск от цены (например 0.005 = 0.5%)

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

            public TradeSignal(String symbol, Side side, double entry,
                               double stop, double take, double confidence, String reason) {
                this.symbol = symbol;
                this.side = side;
                this.entry = entry;
                this.stop = stop;
                this.take = take;
                this.confidence = confidence;
                this.reason = reason;
            }

            @Override
            public String toString() {
                return "TradeSignal{" +
                        "symbol='" + symbol + '\'' +
                        ", side=" + side +
                        ", entry=" + entry +
                        ", stop=" + stop +
                        ", take=" + take +
                        ", confidence=" + confidence +
                        ", reason='" + reason + '\'' +
                        '}';
            }
        }

        public TradeSignal applyRisk(String symbol, Side side, double entry, double atr, double confidence, String reason) {

            double risk = atr * 1.2;
            double minRisk = entry * minRiskPct;
            if (risk < minRisk) risk = minRisk;

            double stop, take;
            if (side == Side.LONG) {
                stop = entry - risk;
                take = entry + risk * 2.5;
            } else {
                stop = entry + risk;
                take = entry - risk * 2.5;
            }

            return new TradeSignal(symbol, side, entry, stop, take, confidence, reason);
        }
    }

    // ===================== ADAPTIVE BRAIN =====================
    public static class AdaptiveBrain {

        private final Map<String, Double> symbolBias = new ConcurrentHashMap<>();
        private final Map<String, Integer> streaks = new ConcurrentHashMap<>();

        public double applyAllAdjustments(String strategy, String symbol, double baseConfidence) {
            double conf = baseConfidence;
            conf = adaptStrategy(strategy, conf);
            conf += getSymbolBias(symbol);
            conf += sessionBoost();
            return clamp(conf, 0.0, 0.95);
        }

        private double adaptStrategy(String strategy, double confidence) {
            if ("ELITE5".equalsIgnoreCase(strategy)) {
                return confidence + 0.02;
            }
            return confidence;
        }

        private double getSymbolBias(String symbol) {
            return symbolBias.getOrDefault(symbol, 0.0);
        }

        private double sessionBoost() {
            int streak = streaks.values().stream().mapToInt(i -> i).sum();
            if (streak > 3) return 0.03;
            if (streak < -3) return -0.03;
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
