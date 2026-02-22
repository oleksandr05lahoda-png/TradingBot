package com.bot;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TradingCore {

    /* ============================================================
       CANDLE
       ============================================================ */

    public static class Candle {
        public final long openTime;
        public final double open;
        public final double high;
        public final double low;
        public final double close;
        public final double volume;
        public final double qvol; // <-- добавляем сюда
        public final long closeTime;

        public Candle(long openTime, double open, double high, double low, double close,
                      double volume, double qvol, long closeTime) {
            this.openTime = openTime;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.qvol = qvol;
            this.closeTime = closeTime;
        }
    }

    /* ============================================================
       SIDE / TYPE
       ============================================================ */

    public enum Side { LONG, SHORT }

    public enum CoinType { TOP, ALT, MEME }

    /* ============================================================
       RISK ENGINE (PRO LEVEL)
       ============================================================ */

    public static final class RiskEngine {

        private final double minRiskPct;
        private final double maxRiskPct;
        private final double minRR;

        public RiskEngine(double minRiskPct,
                          double maxRiskPct,
                          double minRR) {

            this.minRiskPct = minRiskPct;
            this.maxRiskPct = maxRiskPct;
            this.minRR = minRR;
        }

        public static final class TradeSignal {
            public final String symbol;
            public final Side side;
            public final double entry;
            public final double stop;
            public final double take;
            public final double rr;
            public final double confidence;
            public final String reason;
            public final CoinType type;

            public TradeSignal(String symbol,
                               Side side,
                               double entry,
                               double stop,
                               double take,
                               double rr,
                               double confidence,
                               String reason,
                               CoinType type) {

                this.symbol = symbol;
                this.side = side;
                this.entry = entry;
                this.stop = stop;
                this.take = take;
                this.rr = rr;
                this.confidence = confidence;
                this.reason = reason;
                this.type = type;
            }
        }

        public TradeSignal applyRisk(String symbol,
                                     Side side,
                                     double entry,
                                     double atr,
                                     double confidence,
                                     String reason,
                                     CoinType type) {

            /* ==== VOLATILITY ADJUSTMENT ==== */

            double atrPct = atr / entry;

            atrPct = clamp(atrPct, minRiskPct, maxRiskPct);

            double typeRiskMultiplier = switch (type) {
                case TOP -> 1.0;
                case ALT -> 1.25;
                case MEME -> 1.6;
            };

            double riskPct = atrPct * typeRiskMultiplier;

            riskPct = clamp(riskPct, minRiskPct, maxRiskPct);

            /* ==== RR BASED ON CONFIDENCE ==== */

            double rr =
                    confidence > 0.85 ? 3.2 :
                            confidence > 0.75 ? 2.6 :
                                    confidence > 0.65 ? 2.2 :
                                            1.8;

            rr = Math.max(rr, minRR);

            /* ==== STOP / TAKE ==== */

            double stop, take;

            if (side == Side.LONG) {
                stop = entry * (1 - riskPct);
                take = entry * (1 + riskPct * rr);
            } else {
                stop = entry * (1 + riskPct);
                take = entry * (1 - riskPct * rr);
            }

            return new TradeSignal(
                    symbol,
                    side,
                    entry,
                    stop,
                    take,
                    rr,
                    confidence,
                    reason,
                    type
            );
        }

        private double clamp(double v, double min, double max) {
            return Math.max(min, Math.min(max, v));
        }
    }

    /* ============================================================
       ADAPTIVE BRAIN (SAFE VERSION)
       ============================================================ */

    public static final class AdaptiveBrain {

        private static final double MAX_BIAS = 0.08;
        private static final double DECAY = 0.98;

        private final Map<String, Double> symbolBias = new ConcurrentHashMap<>();
        private final Map<String, Integer> streaks = new ConcurrentHashMap<>();

        public double applyAllAdjustments(String strategy,
                                          String symbol,
                                          double baseConfidence,
                                          CoinType type,
                                          boolean highVol,
                                          boolean lowVol) {

            double conf = baseConfidence;

            conf += strategyBoost(strategy);
            conf += symbolBias.getOrDefault(symbol, 0.0);
            conf += typeBoost(type);

            if (highVol) conf += 0.02;
            if (lowVol) conf -= 0.02;

            return clamp(conf, 0.40, 0.95);
        }

        private double strategyBoost(String strategy) {
            if ("ELITE5".equalsIgnoreCase(strategy))
                return 0.02;
            return 0.0;
        }

        private double typeBoost(CoinType type) {
            return switch (type) {
                case TOP -> 0.00;
                case ALT -> 0.015;
                case MEME -> 0.03;
            };
        }

        public void registerResult(String symbol, boolean win) {

            /* ==== STREAK CONTROL ==== */

            streaks.merge(symbol, win ? 1 : -1, Integer::sum);

            streaks.compute(symbol, (s, val) -> {
                if (val == null) return 0;
                if (val > 5) return 5;
                if (val < -5) return -5;
                return val;
            });

            /* ==== BIAS CONTROL ==== */

            symbolBias.merge(symbol,
                    win ? 0.01 : -0.01,
                    Double::sum);

            symbolBias.compute(symbol, (s, val) -> {
                if (val == null) return 0.0;
                val *= DECAY; // плавное затухание
                return clamp(val, -MAX_BIAS, MAX_BIAS);
            });
        }

        private double clamp(double v, double min, double max) {
            return Math.max(min, Math.min(max, v));
        }
    }
}