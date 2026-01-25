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
        public final String reason;

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double entry,
                         double stopLoss,
                         double tp1,
                         double tp2,
                         double probability,
                         double atr,
                         String confidence,
                         String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stopLoss = stopLoss;
            this.takeProfit1 = tp1;
            this.takeProfit2 = tp2;
            this.probability = probability;
            this.atr = atr;
            this.confidence = confidence;
            this.reason = reason;
        }
    }

    // ===================== MAIN =====================
    public List<TradeIdea> evaluateAll(String symbol,
                                       List<TradingCore.Candle> c5,
                                       List<TradingCore.Candle> c15,
                                       List<TradingCore.Candle> c1h) {

        if (!valid(c5, 20)) return Collections.emptyList();

        double atr = SignalSender.atr(c5, 14);
        if (atr <= 0) return Collections.emptyList();

        MarketContext ctx = buildMarketContext(c15, c1h);
        if (ctx.late) return Collections.emptyList(); // блокируем слишком поздний тренд

        List<TradeIdea> ideas = new ArrayList<>();
        TradingCore.Candle last = last(c5);
        double entry = last.close;

        // ===== EARLY REVERSAL =====
        if (ctx.trend1h == Trend.UP && ctx.trend15 != Trend.DOWN && liquiditySweepHigh(c5, atr) && rejectionDown(c5, atr)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.65, "HTF reversal DOWN"));
        }
        if (ctx.trend1h == Trend.DOWN && ctx.trend15 != Trend.UP && liquiditySweepLow(c5, atr) && rejectionUp(c5, atr)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.65, "HTF reversal UP"));
        }

        // ===== COMPRESSION → EXPANSION =====
        if (volatilityCompression(c5, atr) && impulseUp(c5, atr) && ctx.trend15 != Trend.DOWN) {
            ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.6, "Compression → expansion UP"));
        }
        if (volatilityCompression(c5, atr) && impulseDown(c5, atr) && ctx.trend15 != Trend.UP) {
            ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.6, "Compression → expansion DOWN"));
        }

        // ===== MOMENTUM FLIP =====
        if (momentumFlipUp(c5, atr) && ctx.trend1h != Trend.DOWN) {
            ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.6, "Momentum flip UP"));
        }
        if (momentumFlipDown(c5, atr) && ctx.trend1h != Trend.UP) {
            ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.6, "Momentum flip DOWN"));
        }

        // ===== TREND CONTINUATION =====
        if (ctx.trend15 == Trend.UP && continuationUp(c5)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.58, "Trend continuation UP"));
        }
        if (ctx.trend15 == Trend.DOWN && continuationDown(c5)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.58, "Trend continuation DOWN"));
        }

        // ===== FILTER + SORT =====
        return ideas.stream()
                .filter(i -> i.probability >= 0.6) // повышенный порог
                .sorted(Comparator.comparingDouble(i -> -i.probability))
                .limit(10)
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
            stop = entry - atr * 0.6;
            tp1 = entry + atr * 1.2;
            tp2 = entry + atr * 2.0;
        } else {
            stop = entry + atr * 0.6;
            tp1 = entry - atr * 1.2;
            tp2 = entry - atr * 2.0;
        }

        return new TradeIdea(
                symbol, side, entry, stop, tp1, tp2, probability, atr, mapConfidence(probability), reason
        );
    }

    // ===================== LOGIC =====================
    private boolean liquiditySweepHigh(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        double prevHigh = recentHigh(c, 10);
        return l.high > prevHigh && (l.high - l.close) > atr * 0.2;
    }

    private boolean liquiditySweepLow(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        double prevLow = recentLow(c, 10);
        return l.low < prevLow && (l.close - l.low) > atr * 0.2;
    }

    private boolean rejectionDown(TradingCore.Candle l, double atr) {
        return l.close < (l.high + l.low) / 2 - atr * 0.05;
    }

    private boolean rejectionDown(List<TradingCore.Candle> c, double atr) {
        return rejectionDown(last(c), atr);
    }

    private boolean rejectionUp(TradingCore.Candle l, double atr) {
        return l.close > (l.high + l.low) / 2 + atr * 0.05;
    }

    private boolean rejectionUp(List<TradingCore.Candle> c, double atr) {
        return rejectionUp(last(c), atr);
    }

    private boolean volatilityCompression(List<TradingCore.Candle> c, double atr) {
        double range = last(c).high - last(c).low;
        return range < atr * 0.85;
    }

    private boolean impulseUp(List<TradingCore.Candle> c, double atr) {
        return last(c).close > recentHigh(c, 3) + atr * 0.1;
    }

    private boolean impulseDown(List<TradingCore.Candle> c, double atr) {
        return last(c).close < recentLow(c, 3) - atr * 0.1;
    }

    private boolean momentumFlipUp(List<TradingCore.Candle> c, double atr) {
        return last(c).close > c.get(Math.max(c.size() - 3, 0)).close + atr * 0.05;
    }

    private boolean momentumFlipDown(List<TradingCore.Candle> c, double atr) {
        return last(c).close < c.get(Math.max(c.size() - 3, 0)).close - atr * 0.05;
    }

    private boolean continuationUp(List<TradingCore.Candle> c) {
        return last(c).close > recentHigh(c, 5);
    }

    private boolean continuationDown(List<TradingCore.Candle> c) {
        return last(c).close < recentLow(c, 5);
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
        if (p >= 0.6) return "[M]";
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
