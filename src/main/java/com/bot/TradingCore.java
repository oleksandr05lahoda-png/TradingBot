package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TradingCore {

    // ================== 1) Candle ==================
    public static class Candle {
        public final long openTime;
        public final double open, high, low, close, volume, quoteAssetVolume;
        public final long closeTime;

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

        public boolean isBull() {
            return close > open;
        }

        public boolean isBear() {
            return close < open;
        }
    }

    // ================== 1.1) TA helpers ==================
    public static class TA {

        public static double ema(List<Double> values, int period) {
            if (values == null || values.size() < period) return 0;
            double k = 2.0 / (period + 1);
            double ema = values.get(0);
            for (int i = 1; i < values.size(); i++)
                ema = values.get(i) * k + ema * (1 - k);
            return ema;
        }

        public static double rsi(List<Double> closes, int period) {
            if (closes == null || closes.size() < period + 1) return 50;

            double gain = 0, loss = 0;
            for (int i = closes.size() - period - 1; i < closes.size() - 1; i++) {
                double d = closes.get(i + 1) - closes.get(i);
                if (d > 0) gain += d;
                else loss -= d;
            }
            if (loss == 0) return 100;
            double rs = gain / loss;
            return 100 - (100 / (1 + rs));
        }

        public static double atr(List<Candle> candles, int period) {
            if (candles == null || candles.size() < period) return 0;
            double sum = 0;
            for (int i = candles.size() - period; i < candles.size(); i++) {
                sum += (candles.get(i).high - candles.get(i).low);
            }
            return sum / period;
        }

        public static int marketStructure(List<Candle> candles) {
            if (candles == null || candles.size() < 3) return 0;
            Candle a = candles.get(candles.size() - 3);
            Candle b = candles.get(candles.size() - 2);
            Candle c = candles.get(candles.size() - 1);

            if (b.high > a.high && b.high > c.high) return -1; // swing high
            if (b.low < a.low && b.low < c.low) return 1;      // swing low
            return 0;
        }
    }

    // ================== 2) RiskEngine ==================
    public static class RiskEngine {

        private final double riskPerTrade;

        public RiskEngine(double riskPerTrade) {
            this.riskPerTrade = riskPerTrade;
        }

        public double calcSize(double balance, double entry, double stop) {
            double risk = Math.abs(entry - stop);
            double moneyRisk = balance * riskPerTrade;
            return moneyRisk / (risk == 0 ? 1 : risk);
        }

        public TradeSignal applyRisk(String symbol, String side,
                                     double entryPrice, double atr,
                                     double confidence, String reason) {

            TradeSignal s = new TradeSignal();
            s.symbol = symbol;
            s.side = side;
            s.entry = entryPrice;
            s.confidence = confidence;
            s.reason = reason;

            // корректная оценка риска через ATR + confidence
            double risk = atr * (confidence > 0.65 ? 0.9 : 1.2);

            // минимальный риск
            if (risk < entryPrice * 0.001)
                risk = entryPrice * 0.001;

            if (side.equalsIgnoreCase("LONG")) {
                s.stop = entryPrice - risk;
                s.take = entryPrice + risk * 2;
            } else {
                s.stop = entryPrice + risk;
                s.take = entryPrice - risk * 2;
            }

            return s;
        }

        public static class TradeSignal {
            public String symbol;
            public String side;
            public double entry, stop, take, confidence;
            public String reason;

            @Override
            public String toString() {
                return String.format("%s %s @%.4f Stop: %.4f Take: %.4f Conf: %.2f Reason: %s",
                        symbol, side, entry, stop, take, confidence, reason);
            }
        }
    }

    // ================== 3) AdaptiveBrain ==================
    public static class AdaptiveBrain {

        private static class OutcomeStat {
            int wins = 0;
            int losses = 0;
        }

        private final Map<String, OutcomeStat> strategyStats = new ConcurrentHashMap<>();

        public void recordOutcome(String strategy, boolean win) {
            strategyStats.putIfAbsent(strategy, new OutcomeStat());
            OutcomeStat s = strategyStats.get(strategy);
            if (win) s.wins++;
            else s.losses++;
        }

        private double winrate(String strategy) {
            OutcomeStat s = strategyStats.get(strategy);
            if (s == null) return 0.5;
            int total = s.wins + s.losses;
            if (total < 20) return 0.5;
            return (double) s.wins / total;
        }

        public double adaptConfidence(String strategy, double baseConf) {
            double wr = winrate(strategy);
            if (wr > 0.60) return Math.min(0.85, baseConf + 0.05);
            if (wr < 0.45) return Math.max(0.55, baseConf - 0.05);
            return baseConf;
        }

        private final Map<String, Deque<Long>> impulseHistory = new ConcurrentHashMap<>();

        public double impulsePenalty(String pair) {
            impulseHistory.putIfAbsent(pair, new ArrayDeque<>());
            Deque<Long> q = impulseHistory.get(pair);

            long now = System.currentTimeMillis();
            q.addLast(now);
            while (!q.isEmpty() && now - q.peekFirst() > 20 * 60_000)
                q.pollFirst();

            if (q.size() >= 3) return -0.07;
            if (q.size() == 2) return -0.03;
            return 0.0;
        }

        public enum Regime { TREND, RANGE, CHAOS }

        public Regime detectRegime(List<Candle> candles) {
            if (candles == null || candles.size() < 30) return Regime.CHAOS;

            double atr = TA.atr(candles, 14);
            double atrPrev = TA.atr(candles.subList(0, candles.size() - 5), 14);

            int structure = TA.marketStructure(candles);

            if (atr > atrPrev * 1.2 && structure != 0) return Regime.TREND;
            if (atr < atrPrev * 0.8 && structure == 0) return Regime.RANGE;

            return Regime.CHAOS;
        }

        public double sessionBoost() {
            int h = LocalTime.now(ZoneOffset.UTC).getHour();
            if ((h >= 7 && h <= 10) || (h >= 13 && h <= 16))
                return 0.05;
            return 0.0;
        }

        public double applyAllAdjustments(String strategy, String pair, double baseConf) {
            double conf = baseConf;
            conf = adaptConfidence(strategy, conf);
            conf += sessionBoost();
            conf += impulsePenalty(pair);
            conf = Math.max(0.45, Math.min(0.85, conf));
            return conf;
        }
    }
}
