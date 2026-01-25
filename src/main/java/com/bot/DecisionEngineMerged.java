package com.bot;

import java.util.*;
import java.util.stream.Collectors;

public class DecisionEngineMerged {

    public static class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double stopLoss;
        public final double takeProfit1;
        public final double takeProfit2;
        public final double probability;
        public final double atr;
        public final String confidence;
        public final String context;

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double entry,
                         double stopLoss,
                         double tp1,
                         double tp2,
                         double probability,
                         double atr,
                         String confidence,
                         String context) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stopLoss = stopLoss;
            this.takeProfit1 = tp1;
            this.takeProfit2 = tp2;
            this.probability = probability;
            this.atr = atr;
            this.confidence = confidence;
            this.context = context;
        }
    }

    // ===================== MAIN =====================
    public List<TradeIdea> evaluateAll(String symbol,
                                       List<TradingCore.Candle> c5,
                                       List<TradingCore.Candle> c15,
                                       List<TradingCore.Candle> c1h) {

        if (!valid(c5, 20)) return Collections.emptyList(); // меньше минимальных свечей

        double atr = SignalSender.atr(c5, 14);
        if (atr <= 0) return Collections.emptyList();

        MarketContext ctx = buildMarketContext(c15, c1h);
        List<TradeIdea> ideas = new ArrayList<>();
        TradingCore.Candle last = last(c5);
        double entry = last.close;

        // ===== EARLY REVERSAL =====
        if (ctx.trend1h == Trend.UP && liquiditySweepHigh(c5, atr) && rejectionDown(c5)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.6, "HTF reversal DOWN"));
        }
        if (ctx.trend1h == Trend.DOWN && liquiditySweepLow(c5, atr) && rejectionUp(c5)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.6, "HTF reversal UP"));
        }

        // ===== COMPRESSION → EXPANSION =====
        if (volatilityCompression(c5, atr) && impulseUp(c5) && ctx.trend15 != Trend.DOWN) {
            ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.58, "Compression → expansion UP"));
        }
        if (volatilityCompression(c5, atr) && impulseDown(c5) && ctx.trend15 != Trend.UP) {
            ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.58, "Compression → expansion DOWN"));
        }

        // ===== MOMENTUM FLIP =====
        if (momentumFlipUp(c5) && ctx.trend1h != Trend.DOWN) {
            ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.55, "Momentum flip UP"));
        }
        if (momentumFlipDown(c5) && ctx.trend1h != Trend.UP) {
            ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.55, "Momentum flip DOWN"));
        }

        // ===== TREND CONTINUATION =====
        if (ctx.trend15 == Trend.UP && continuationUp(c5)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.52, "Trend continuation UP"));
        }
        if (ctx.trend15 == Trend.DOWN && continuationDown(c5)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.52, "Trend continuation DOWN"));
        }

        // ===== FILTER + SORT =====
        return ideas.stream()
                .filter(i -> i.probability >= 0.5) // ниже порога для большего числа сигналов
                .sorted(Comparator.comparingDouble(i -> -i.probability))
                .limit(10) // больше сигналов для анализа
                .collect(Collectors.toList());
    }

    // ===================== BUILD IDEA =====================
    private TradeIdea buildIdea(String symbol,
                                TradingCore.Side side,
                                double entry,
                                double atr,
                                double probability,
                                String reason) {

        double stop, tp1, tp2;
        if (side == TradingCore.Side.LONG) {
            stop = entry - atr * 0.6; // чуть свободнее стоп
            tp1 = entry + atr * 1.2;
            tp2 = entry + atr * 2.0;
        } else {
            stop = entry + atr * 0.6;
            tp1 = entry - atr * 1.2;
            tp2 = entry - atr * 2.0;
        }

        return new TradeIdea(
                symbol,
                side,
                entry,
                stop,
                tp1,
                tp2,
                probability,
                atr,
                mapConfidence(probability),
                reason
        );
    }

    // ===================== LOGIC =====================
    private boolean liquiditySweepHigh(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        double prevHigh = recentHigh(c, 5); // меньше свечей для частых сигналов
        return l.high > prevHigh && (l.high - l.close) > atr * 0.2;
    }

    private boolean liquiditySweepLow(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        double prevLow = recentLow(c, 5);
        return l.low < prevLow && (l.close - l.low) > atr * 0.2;
    }

    private boolean rejectionDown(List<TradingCore.Candle> c) {
        TradingCore.Candle l = last(c);
        return l.close < (l.high + l.low) / 2;
    }

    private boolean rejectionUp(List<TradingCore.Candle> c) {
        TradingCore.Candle l = last(c);
        return l.close > (l.high + l.low) / 2;
    }

    private boolean volatilityCompression(List<TradingCore.Candle> c, double atr) {
        double range = last(c).high - last(c).low;
        return range < atr * 0.85; // смягчено
    }

    private boolean impulseUp(List<TradingCore.Candle> c) {
        return last(c).close > recentHigh(c, 2); // меньше свечей
    }

    private boolean impulseDown(List<TradingCore.Candle> c) {
        return last(c).close < recentLow(c, 2);
    }

    private boolean momentumFlipUp(List<TradingCore.Candle> c) {
        return last(c).close > c.get(Math.max(c.size() - 3, 0)).close;
    }

    private boolean momentumFlipDown(List<TradingCore.Candle> c) {
        return last(c).close < c.get(Math.max(c.size() - 3, 0)).close;
    }

    private boolean continuationUp(List<TradingCore.Candle> c) {
        return last(c).close > recentHigh(c, 3); // меньше свечей
    }

    private boolean continuationDown(List<TradingCore.Candle> c) {
        return last(c).close < recentLow(c, 3);
    }

    // ===================== HELPERS =====================
    private double recentHigh(List<TradingCore.Candle> c, int n) {
        return c.subList(Math.max(0, c.size() - n), c.size())
                .stream().mapToDouble(x -> x.high).max().orElse(last(c).high);
    }

    private double recentLow(List<TradingCore.Candle> c, int n) {
        return c.subList(Math.max(0, c.size() - n), c.size())
                .stream().mapToDouble(x -> x.low).min().orElse(last(c).low);
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    private String mapConfidence(double p) {
        if (p >= 0.65) return "[S]";
        if (p >= 0.58) return "[M]";
        return "[W]";
    }

    // ===================== MARKET CONTEXT =====================
    private MarketContext buildMarketContext(List<TradingCore.Candle> c15, List<TradingCore.Candle> c1h) {
        Trend t1h = trend(c1h);
        Trend t15 = trend(c15);
        double strength = trendStrength(c1h);
        boolean late = isTrendLate(c1h);
        return new MarketContext(t1h, t15, strength, late);
    }

    private Trend trend(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(Math.max(0, c.size() - 50));
        TradingCore.Candle l = last(c);
        double pct = (l.close - f.close) / f.close;
        if (pct > 0.003) return Trend.UP;
        if (pct < -0.003) return Trend.DOWN;
        return Trend.FLAT;
    }

    private double trendStrength(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(Math.max(0, c.size() - 80));
        TradingCore.Candle l = last(c);
        return Math.min(Math.abs((l.close - f.close) / f.close) * 7, 1.0);
    }

    private boolean isTrendLate(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(Math.max(0, c.size() - 100));
        TradingCore.Candle l = last(c);
        return Math.abs((l.close - f.close) / f.close) > 0.07;
    }

    private static class MarketContext {
        Trend trend1h;
        Trend trend15;
        double strength;
        boolean late;

        MarketContext(Trend t1h, Trend t15, double strength, boolean late) {
            this.trend1h = t1h;
            this.trend15 = t15;
            this.strength = strength;
            this.late = late;
        }
    }

    private enum Trend {UP, DOWN, FLAT}
}
