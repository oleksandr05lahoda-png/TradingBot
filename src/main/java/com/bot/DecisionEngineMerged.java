package com.bot;

import java.util.*;
import java.util.stream.Collectors;

public class DecisionEngineMerged {

    /* ========================== MODEL ========================== */

    public static class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double stop;
        public final double tp1;
        public final double tp2;
        public final double probability;
        public final double atr;
        public final String reason;

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double entry,
                         double stop,
                         double tp1,
                         double tp2,
                         double probability,
                         double atr,
                         String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.tp1 = tp1;
            this.tp2 = tp2;
            this.probability = probability;
            this.atr = atr;
            this.reason = reason;
        }
    }

    /* ========================== MAIN ========================== */

    public List<TradeIdea> evaluate(String symbol,
                                    List<TradingCore.Candle> c5,
                                    List<TradingCore.Candle> c15,
                                    List<TradingCore.Candle> c1h) {

        if (!valid(c5, 120) || !valid(c15, 120) || !valid(c1h, 120))
            return Collections.emptyList();

        double atr = SignalSender.atr(c5, 14);
        if (atr <= 0) return Collections.emptyList();

        Trend t1h = trend(c1h);
        Trend t15 = trend(c15);
        Trend bias = t1h != Trend.FLAT ? t1h : t15;
        if (bias == Trend.FLAT) return Collections.emptyList();

        EMA ema = emaContext(c5);
        TradingCore.Candle last = last(c5);

        List<TradeIdea> ideas = new ArrayList<>();

        /* ========================== TREND FOLLOW ========================== */
        if (!isTrendExhausted(c1h, atr)) {

            if (bias == Trend.UP && ema.bullish) {
                scoreAndAdd(ideas, buildLong(symbol, last.close, atr),
                        scoreTrend(c5, atr, true),
                        "Trend follow LONG");
            }

            if (bias == Trend.DOWN && ema.bearish) {
                scoreAndAdd(ideas, buildShort(symbol, last.close, atr),
                        scoreTrend(c5, atr, false),
                        "Trend follow SHORT");
            }
        }

        /* ========================== PULLBACK ========================== */
        if (bias == Trend.UP && pullbackLong(c5, ema)) {
            scoreAndAdd(ideas, buildLong(symbol, last.close, atr),
                    scorePullback(c5, atr),
                    "Pullback LONG");
        }

        if (bias == Trend.DOWN && pullbackShort(c5, ema)) {
            scoreAndAdd(ideas, buildShort(symbol, last.close, atr),
                    scorePullback(c5, atr),
                    "Pullback SHORT");
        }

        /* ========================== REVERSAL ========================== */
        if (isHTFExhaustion(c1h, atr)) {

            if (sweepHigh(c5, atr) && rejectionDown(c5, atr)) {
                scoreAndAdd(ideas, buildShort(symbol, last.close, atr),
                        scoreReversal(c5),
                        "Early reversal SHORT");
            }

            if (sweepLow(c5, atr) && rejectionUp(c5, atr)) {
                scoreAndAdd(ideas, buildLong(symbol, last.close, atr),
                        scoreReversal(c5),
                        "Early reversal LONG");
            }
        }

        return ideas.stream()
                .filter(i -> i.probability >= 0.58)
                .sorted(Comparator.comparingDouble(i -> -i.probability))
                .limit(4)
                .collect(Collectors.toList());
    }

    /* ========================== SCORING ========================== */

    private void scoreAndAdd(List<TradeIdea> ideas, TradeIdea base, double score, String reason) {
        if (score < 0.55) return;

        ideas.add(new TradeIdea(
                base.symbol,
                base.side,
                base.entry,
                base.stop,
                base.tp1,
                base.tp2,
                score,
                base.atr,
                reason + " | score=" + String.format("%.2f", score)
        ));
    }

    private double scoreTrend(List<TradingCore.Candle> c, double atr, boolean up) {
        double s = 0.50;
        if (volumeBoost(c)) s += 0.05;
        if (impulse(c, atr)) s += 0.05;
        if (structureBreak(c, up)) s += 0.05;
        return s;
    }

    private double scorePullback(List<TradingCore.Candle> c, double atr) {
        double s = 0.48;
        if (volumeDry(c)) s += 0.06;
        if (impulse(c, atr)) s += 0.04;
        return s;
    }

    private double scoreReversal(List<TradingCore.Candle> c) {
        double s = 0.52;
        if (volumeClimax(c)) s += 0.08;
        if (rangeExpansion(c)) s += 0.04;
        return s;
    }

    /* ========================== BUILD ========================== */

    private TradeIdea buildLong(String symbol, double entry, double atr) {
        double risk = atr * 0.65;
        return new TradeIdea(symbol, TradingCore.Side.LONG,
                entry, entry - risk,
                entry + risk * 1.7,
                entry + risk * 3.0,
                0, atr, "");
    }

    private TradeIdea buildShort(String symbol, double entry, double atr) {
        double risk = atr * 0.65;
        return new TradeIdea(symbol, TradingCore.Side.SHORT,
                entry, entry + risk,
                entry - risk * 1.7,
                entry - risk * 3.0,
                0, atr, "");
    }

    /* ========================== CONDITIONS ========================== */

    private boolean pullbackLong(List<TradingCore.Candle> c, EMA e) {
        double p = last(c).close;
        return p > e.ema21 && p < e.ema9;
    }

    private boolean pullbackShort(List<TradingCore.Candle> c, EMA e) {
        double p = last(c).close;
        return p < e.ema21 && p > e.ema9;
    }

    private boolean sweepHigh(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return l.high > recentHigh(c, 20) && (l.high - l.close) > atr * 0.2;
    }

    private boolean sweepLow(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return l.low < recentLow(c, 20) && (l.close - l.low) > atr * 0.2;
    }

    private boolean rejectionDown(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return l.close < (l.high + l.low) / 2;
    }

    private boolean rejectionUp(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return l.close > (l.high + l.low) / 2;
    }

    /* ========================== CONTEXT ========================== */

    private boolean isHTFExhaustion(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 80).close) / atr > 6;
    }

    private boolean isTrendExhausted(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 120).close) / atr > 8;
    }

    private Trend trend(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(c.size() - 80);
        TradingCore.Candle l = last(c);
        double p = (l.close - f.close) / f.close;
        if (p > 0.003) return Trend.UP;
        if (p < -0.003) return Trend.DOWN;
        return Trend.FLAT;
    }

    /* ========================== EMA ========================== */

    private EMA emaContext(List<TradingCore.Candle> c) {
        List<Double> cl = c.stream().map(x -> x.close).toList();
        double e9 = ema(cl, 9);
        double e21 = ema(cl, 21);
        double e50 = ema(cl, 50);
        double p = last(c).close;

        return new EMA(
                e9, e21, e50,
                p > e9 && e9 > e21,
                p < e9 && e9 < e21
        );
    }

    private double ema(List<Double> s, int p) {
        double k = 2.0 / (p + 1);
        double e = s.get(s.size() - p);
        for (int i = s.size() - p + 1; i < s.size(); i++)
            e = s.get(i) * k + e * (1 - k);
        return e;
    }

    /* ========================== UTILS ========================== */

    private boolean volumeBoost(List<TradingCore.Candle> c) {
        return last(c).volume > avgVol(c) * 1.2;
    }

    private boolean volumeDry(List<TradingCore.Candle> c) {
        return last(c).volume < avgVol(c) * 0.9;
    }

    private boolean volumeClimax(List<TradingCore.Candle> c) {
        return last(c).volume > avgVol(c) * 1.6;
    }

    private boolean impulse(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 5).close) > atr;
    }

    private boolean rangeExpansion(List<TradingCore.Candle> c) {
        return (last(c).high - last(c).low) >
                (avgRange(c) * 1.4);
    }

    private boolean structureBreak(List<TradingCore.Candle> c, boolean up) {
        return up
                ? last(c).close > recentHigh(c, 10)
                : last(c).close < recentLow(c, 10);
    }

    private double avgRange(List<TradingCore.Candle> c) {
        return c.stream().skip(c.size() - 20)
                .mapToDouble(x -> x.high - x.low)
                .average().orElse(0);
    }

    private double avgVol(List<TradingCore.Candle> c) {
        return c.stream().skip(c.size() - 30)
                .mapToDouble(x -> x.volume)
                .average().orElse(0);
    }

    private double recentHigh(List<TradingCore.Candle> c, int n) {
        return c.subList(c.size() - n, c.size())
                .stream().mapToDouble(x -> x.high).max().orElse(0);
    }

    private double recentLow(List<TradingCore.Candle> c, int n) {
        return c.subList(c.size() - n, c.size())
                .stream().mapToDouble(x -> x.low).min().orElse(0);
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    /* ========================== STRUCT ========================== */

    private enum Trend {UP, DOWN, FLAT}

    private static class EMA {
        double ema9, ema21, ema50;
        boolean bullish, bearish;

        EMA(double e9, double e21, double e50, boolean bull, boolean bear) {
            this.ema9 = e9;
            this.ema21 = e21;
            this.ema50 = e50;
            this.bullish = bull;
            this.bearish = bear;
        }
    }
}
