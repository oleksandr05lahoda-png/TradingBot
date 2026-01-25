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

        if (!valid(c5, 50) || !valid(c15, 50) || !valid(c1h, 50)) {
            return Collections.emptyList();
        }

        double atr = SignalSender.atr(c5, 14);
        if (atr <= 0) return Collections.emptyList();

        MarketContext ctx = buildMarketContext(c15, c1h);
        if (ctx.late) return Collections.emptyList(); // блокируем слишком поздний тренд

        // если тренд не определен — не торгуем
        if (ctx.trend1h == Trend.FLAT && ctx.trend15 == Trend.FLAT) {
            return Collections.emptyList();
        }

        List<TradeIdea> ideas = new ArrayList<>();
        TradingCore.Candle last = last(c5);
        double entry = last.close;

        // ===== VOLUME FILTER =====
        if (!volumeAboveAverage(c5)) {
            return Collections.emptyList();
        }

        // ===== EMA FILTER (5m) =====
        EMAContext emaCtx = calculateEMA(c5);

        // ===== TREND FILTER =====
        // Если 1h UP => ищем LONG, если 1h DOWN => ищем SHORT
        // Если 1h FLAT => смотрим 15m
        Trend mainTrend = ctx.trend1h != Trend.FLAT ? ctx.trend1h : ctx.trend15;

        // ===== STRATEGIES =====

        // 1) TREND FOLLOW (EMA + PRICE ABOVE/BELOW EMA)
        if (mainTrend == Trend.UP && emaCtx.priceAboveAll) {
            if (trendContinuationUp(c5, atr)) {
                ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.62, "Trend follow UP (EMA+breakout)"));
            }
            if (pullbackLong(c5, emaCtx)) {
                ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.58, "Pullback UP (EMA bounce)"));
            }
        }

        if (mainTrend == Trend.DOWN && emaCtx.priceBelowAll) {
            if (trendContinuationDown(c5, atr)) {
                ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.62, "Trend follow DOWN (EMA+breakdown)"));
            }
            if (pullbackShort(c5, emaCtx)) {
                ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.58, "Pullback DOWN (EMA bounce)"));
            }
        }

        // 2) REVERSAL (если 15m/1h резко перегибается)
        if (ctx.trend1h == Trend.UP && reversalShort(c5, atr)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.SHORT, entry, atr, 0.60, "Reversal SHORT (HTF rejection)"));
        }
        if (ctx.trend1h == Trend.DOWN && reversalLong(c5, atr)) {
            ideas.add(buildIdea(symbol, TradingCore.Side.LONG, entry, atr, 0.60, "Reversal LONG (HTF rejection)"));
        }

        // ===== FILTER + SORT =====
        return ideas.stream()
                .filter(i -> i.probability >= 0.55)
                .sorted(Comparator.comparingDouble(i -> -i.probability))
                .limit(5)
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
        double risk = atr * 0.7;

        if (side == TradingCore.Side.LONG) {
            stop = entry - risk;
            tp1 = entry + risk * 1.5;
            tp2 = entry + risk * 2.6;
        } else {
            stop = entry + risk;
            tp1 = entry - risk * 1.5;
            tp2 = entry - risk * 2.6;
        }

        return new TradeIdea(
                symbol, side, entry, stop, tp1, tp2, probability, atr, mapConfidence(probability), reason
        );
    }

    // ===================== STRATEGIES =====================

    // TREND FOLLOW (breakout)
    private boolean trendContinuationUp(List<TradingCore.Candle> c, double atr) {
        return last(c).close > recentHigh(c, 8) + atr * 0.05;
    }

    private boolean trendContinuationDown(List<TradingCore.Candle> c, double atr) {
        return last(c).close < recentLow(c, 8) - atr * 0.05;
    }

    // PULLBACK (bounce from EMA21)
    private boolean pullbackLong(List<TradingCore.Candle> c, EMAContext ema) {
        double close = last(c).close;
        return close > ema.ema21 && close < ema.ema9 && (ema.ema9 - close) / ema.ema9 < 0.012;
    }

    private boolean pullbackShort(List<TradingCore.Candle> c, EMAContext ema) {
        double close = last(c).close;
        return close < ema.ema21 && close > ema.ema9 && (close - ema.ema9) / ema.ema9 < 0.012;
    }

    // REVERSAL
    private boolean reversalShort(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return liquiditySweepHigh(c, atr) && rejectionDown(c, atr);
    }

    private boolean reversalLong(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return liquiditySweepLow(c, atr) && rejectionUp(c, atr);
    }

    // ===================== HELPERS =====================

    private boolean volumeAboveAverage(List<TradingCore.Candle> c) {
        int n = Math.min(c.size(), 50);
        double avg = c.subList(c.size() - n, c.size()).stream()
                .mapToDouble(x -> x.volume).average().orElse(0);
        return last(c).volume >= avg * 1.05; // минимум 5% выше среднего
    }

    private EMAContext calculateEMA(List<TradingCore.Candle> c) {
        List<Double> closes = new ArrayList<>();
        for (TradingCore.Candle candle : c) closes.add(candle.close);

        double ema9 = ema(closes, 9);
        double ema21 = ema(closes, 21);
        double ema50 = ema(closes, 50);

        double price = last(c).close;

        return new EMAContext(
                ema9, ema21, ema50,
                price > ema9 && price > ema21 && price > ema50,
                price < ema9 && price < ema21 && price < ema50
        );
    }

    private double ema(List<Double> series, int period) {
        double k = 2.0 / (period + 1);
        double ema = series.get(series.size() - period); // стартовое значение
        for (int i = series.size() - period + 1; i < series.size(); i++) {
            ema = series.get(i) * k + ema * (1 - k);
        }
        return ema;
    }

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

    private static class EMAContext {
        double ema9;
        double ema21;
        double ema50;
        boolean priceAboveAll;
        boolean priceBelowAll;

        EMAContext(double ema9, double ema21, double ema50, boolean priceAboveAll, boolean priceBelowAll) {
            this.ema9 = ema9;
            this.ema21 = ema21;
            this.ema50 = ema50;
            this.priceAboveAll = priceAboveAll;
            this.priceBelowAll = priceBelowAll;
        }
    }
}
