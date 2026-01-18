package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DecisionEngineMerged {

    // ================== Candle helpers (TA) ==================
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

        public static double atr(List<TradingCore.Candle> candles, int period) {
            if (candles == null || candles.size() < period) return 0;
            double sum = 0;
            for (int i = candles.size() - period; i < candles.size(); i++)
                sum += (candles.get(i).high - candles.get(i).low);
            return sum / period;
        }

        public static int marketStructure(List<TradingCore.Candle> candles) {
            if (candles == null || candles.size() < 3) return 0;
            TradingCore.Candle a = candles.get(candles.size() - 3);
            TradingCore.Candle b = candles.get(candles.size() - 2);
            TradingCore.Candle c = candles.get(candles.size() - 1);

            if (b.high > a.high && b.high > c.high) return -1; // swing high
            if (b.low < a.low && b.low < c.low) return 1;      // swing low
            return 0;
        }
    }

    // ================== AdaptiveBrain (из SuperBrain) ==================
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

        public Regime detectRegime(List<TradingCore.Candle> candles) {
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
    }

    private final AdaptiveBrain adaptive = new AdaptiveBrain();

    // ================== TradeIdea ==================
    public static class TradeIdea {
        public String symbol;
        public String side;
        public double entry;
        public double atr;
        public double confidence;
        public String reason;
    }

    // ================== MultiTF helpers ==================
    private int emaDirection(List<TradingCore.Candle> candles, int fast, int slow) {
        List<Double> closes = candles.stream().map(c -> c.close).collect(Collectors.toList());
        double emaFast = TA.ema(closes, fast);
        double emaSlow = TA.ema(closes, slow);
        if (emaFast > emaSlow) return 1;
        if (emaFast < emaSlow) return -1;
        return 0;
    }

    private int multiTFConfirm(List<TradingCore.Candle> c5m, List<TradingCore.Candle> c15m, List<TradingCore.Candle> c1h) {
        int dir5 = emaDirection(c5m, 20, 50);
        int dir15 = emaDirection(c15m, 20, 50);
        int dir1h = (c1h != null && c1h.size() >= 50) ? emaDirection(c1h, 20, 50) : 0;

        // Вес 5m=1, 15m=2, 1h=3
        return dir5 * 1 + dir15 * 2 + dir1h * 3;
    }

    // ================== EVALUATE ==================
    public Optional<TradeIdea> evaluate(
            String symbol,
            List<TradingCore.Candle> candles5m,
            List<TradingCore.Candle> candles15m,
            List<TradingCore.Candle> candles1h
    ) {

        if (candles5m == null || candles15m == null) return Optional.empty();
        if (candles5m.size() < 50 || candles15m.size() < 50)
            return Optional.empty();

        if (candles1h != null && candles1h.size() < 50)
            candles1h = null;

        List<Double> closes5 = candles5m.stream().map(c -> c.close).collect(Collectors.toList());
        List<Double> closes15 = candles15m.stream().map(c -> c.close).collect(Collectors.toList());

        double rsi5 = TA.rsi(closes5, 14);
        double rsi15 = TA.rsi(closes15, 14);

        double atr5 = TA.atr(candles5m, 14);
        double atrAvg = TA.atr(candles5m.subList(0, candles5m.size() - 5), 14);

        int contextDir =
                TA.ema(closes15, 20) > TA.ema(closes15, 50) ? 1 :
                        TA.ema(closes15, 20) < TA.ema(closes15, 50) ? -1 : 0;

        if (contextDir == 0) return Optional.empty();

        AdaptiveBrain.Regime regime = adaptive.detectRegime(candles5m);
        if (regime == AdaptiveBrain.Regime.CHAOS) return Optional.empty();

        TradingCore.Candle last = candles5m.get(candles5m.size() - 1);
        TradingCore.Candle prev = candles5m.get(candles5m.size() - 2);

        boolean structureBreakUp =
                prev.close < prev.open &&
                        last.close > prev.high &&
                        (last.high - last.low) < atr5 * 1.2;

        boolean structureBreakDown =
                prev.close > prev.open &&
                        last.close < prev.low &&
                        (last.high - last.low) < atr5 * 1.2;

        boolean pullbackLong =
                prev.close < prev.open &&
                        last.close > prev.close &&
                        last.close < prev.high;

        boolean pullbackShort =
                prev.close > prev.open &&
                        last.close < prev.close &&
                        last.close > prev.low;

        boolean atrCompression = atr5 < atrAvg * 0.85;
        if (atr5 > atrAvg * 1.5) return Optional.empty();

        // Убираем резкие свечи
        double lastRange = last.high - last.low;
        if (lastRange > atr5 * 2.0) return Optional.empty();

        if (contextDir == 1 && rsi15 > 70) return Optional.empty();
        if (contextDir == -1 && rsi15 < 30) return Optional.empty();

        if (contextDir == 1 && rsi5 > 75) return Optional.empty();
        if (contextDir == -1 && rsi5 < 25) return Optional.empty();

        double confidence = 0.30;

        int mtf = multiTFConfirm(candles5m, candles15m, candles1h);
        confidence += mtf * 0.15;

        if (rsi5 < 25 || rsi5 > 75) confidence -= 0.05;
        else confidence += 0.05;

        if (atr5 < atrAvg * 0.85) confidence += 0.05;

        confidence += adaptive.sessionBoost();
        confidence += adaptive.impulsePenalty(symbol);

        // адаптация по winrate
        confidence = adaptive.adaptConfidence("MTF", confidence);

        confidence = Math.max(0.50, Math.min(0.90, confidence));

        if (contextDir == 1 &&
                atrCompression &&
                rsi5 > 35 && rsi5 < 50 &&
                (structureBreakUp || pullbackLong)) {

            TradeIdea idea = new TradeIdea();
            idea.symbol = symbol;
            idea.side = "LONG";
            idea.entry = last.close;
            idea.atr = atr5;
            idea.confidence = confidence;
            idea.reason = "MTF structure LONG";
            return Optional.of(idea);
        }

        if (contextDir == -1 &&
                atrCompression &&
                rsi5 < 65 && rsi5 > 50 &&
                (structureBreakDown || pullbackShort)) {

            TradeIdea idea = new TradeIdea();
            idea.symbol = symbol;
            idea.side = "SHORT";
            idea.entry = last.close;
            idea.atr = atr5;
            idea.confidence = confidence;
            idea.reason = "MTF structure SHORT";
            return Optional.of(idea);
        }

        return Optional.empty();
    }


    // ================== BACKTEST ==================

    public static class BacktestResult {
        public int trades = 0;
        public int wins = 0;
        public int losses = 0;
        public double totalPnL = 0;
        public double maxDrawdown = 0;

        @Override
        public String toString() {
            double winrate = trades == 0 ? 0 : (double) wins / trades;
            return "BacktestResult{" +
                    "trades=" + trades +
                    ", wins=" + wins +
                    ", losses=" + losses +
                    ", winrate=" + String.format("%.2f", winrate * 100) + "%" +
                    ", totalPnL=" + String.format("%.4f", totalPnL) +
                    ", maxDrawdown=" + String.format("%.4f", maxDrawdown) +
                    '}';
        }
    }

    /**
     * Backtest based on fixed stop/take
     * Stop = entry - ATR
     * Take = entry + ATR*2
     */
    public BacktestResult backtest(
            String symbol,
            List<TradingCore.Candle> candles5m,
            List<TradingCore.Candle> candles15m,
            List<TradingCore.Candle> candles1h
    ) {

        BacktestResult result = new BacktestResult();
        if (candles5m == null || candles15m == null) return result;
        if (candles5m.size() < 60 || candles15m.size() < 60) return result;

        // step by step through candles
        for (int i = 60; i < candles5m.size(); i++) {

            List<TradingCore.Candle> slice5 = candles5m.subList(0, i);
            List<TradingCore.Candle> slice15 = candles15m.size() >= i ? candles15m.subList(0, i / 3) : candles15m;
            List<TradingCore.Candle> slice1h = (candles1h != null && candles1h.size() >= i / 12) ? candles1h.subList(0, i / 12) : null;

            Optional<TradeIdea> ideaOpt = evaluate(symbol, slice5, slice15, slice1h);

            if (ideaOpt.isEmpty()) continue;

            TradeIdea idea = ideaOpt.get();

            double stop = idea.side.equals("LONG") ? idea.entry - idea.atr : idea.entry + idea.atr;
            double take = idea.side.equals("LONG") ? idea.entry + idea.atr * 2 : idea.entry - idea.atr * 2;

            // simulate next candle only
            TradingCore.Candle next = candles5m.get(i);
            boolean win = idea.side.equals("LONG")
                    ? (next.high >= take)
                    : (next.low <= take);

            boolean loss = idea.side.equals("LONG")
                    ? (next.low <= stop)
                    : (next.high >= stop);

            // if both hit, assume worst case: stop first
            if (win && loss) win = false;

            result.trades++;
            if (win) result.wins++;
            else if (loss) result.losses++;
            result.totalPnL += win ? (take - idea.entry) : (idea.entry - stop);

            // drawdown simple calc
            double equity = result.totalPnL;
            if (equity < result.maxDrawdown) result.maxDrawdown = equity;
        }

        return result;
    }
}
