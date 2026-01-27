package com.bot;

import java.util.*;
import java.util.stream.Collectors;

public class DecisionEngineMerged {

    /* ========================== MODEL ========================== */

    public static class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, tp1, tp2;
        public final double probability, atr;
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
            return List.of();

        double atr = SignalSender.atr(c5, 14);
        if (atr <= 0) return List.of();

        MarketMode mode = marketMode(c1h, c15, atr);
        EMA ema = emaContext(c5);
        TradingCore.Candle last = last(c5);

        List<TradeIdea> ideas = new ArrayList<>();

        /* ========================== TREND ========================== */
        if (mode == MarketMode.TREND) {

            if (ema.bullish) {
                ideas.add(scored(buildLong(symbol, last.close, atr),
                        scoreTrend(c5, atr, true),
                        "TREND LONG"));
            }

            if (ema.bearish) {
                ideas.add(scored(buildShort(symbol, last.close, atr),
                        scoreTrend(c5, atr, false),
                        "TREND SHORT"));
            }
        }

        /* ========================== PULLBACK ========================== */
        if (mode == MarketMode.EXHAUSTION) {

            if (pullbackLong(c5, ema)) {
                ideas.add(scored(buildLong(symbol, last.close, atr),
                        scorePullback(c5),
                        "PULLBACK LONG"));
            }

            if (pullbackShort(c5, ema)) {
                ideas.add(scored(buildShort(symbol, last.close, atr),
                        scorePullback(c5),
                        "PULLBACK SHORT"));
            }
        }

        /* ========================== REVERSAL ========================== */
        if (mode == MarketMode.REVERSAL) {

            if (sweepHigh(c5, atr) && rejectionDown(c5)) {
                ideas.add(scored(buildShort(symbol, last.close, atr),
                        scoreReversal(c5),
                        "REVERSAL SHORT"));
            }

            if (sweepLow(c5, atr) && rejectionUp(c5)) {
                ideas.add(scored(buildLong(symbol, last.close, atr),
                        scoreReversal(c5),
                        "REVERSAL LONG"));
            }
        }

        return ideas.stream()
                .filter(i -> i.probability >= 0.60)
                .sorted(Comparator.comparingDouble(i -> -i.probability))
                .limit(2)
                .collect(Collectors.toList());
    }

    /* ========================== MARKET MODE ========================== */

    private MarketMode marketMode(List<TradingCore.Candle> c1h,
                                  List<TradingCore.Candle> c15,
                                  double atr) {

        double htfMove = Math.abs(last(c1h).close - c1h.get(c1h.size() - 80).close) / atr;
        double mtfMove = Math.abs(last(c15).close - c15.get(c15.size() - 40).close) / atr;

        if (htfMove > 7.5 && mtfMove < 2.0)
            return MarketMode.REVERSAL;

        if (htfMove > 6.0)
            return MarketMode.EXHAUSTION;

        return MarketMode.TREND;
    }

    /* ========================== SCORING ========================== */

    private TradeIdea scored(TradeIdea base, double score, String reason) {
        return new TradeIdea(
                base.symbol,
                base.side,
                base.entry,
                base.stop,
                base.tp1,
                base.tp2,
                Math.min(score, 0.95),
                base.atr,
                reason + " | p=" + String.format("%.2f", score)
        );
    }

    private double scoreTrend(List<TradingCore.Candle> c, double atr, boolean up) {
        double s = 0.58;
        if (volumeBoost(c)) s += 0.06;
        if (impulse(c, atr)) s += 0.06;
        if (structureBreak(c, up)) s += 0.05;
        return s;
    }

    private double scorePullback(List<TradingCore.Candle> c) {
        double s = 0.60;
        if (volumeDry(c)) s += 0.06;
        return s;
    }

    private double scoreReversal(List<TradingCore.Candle> c) {
        double s = 0.62;
        if (volumeClimax(c)) s += 0.08;
        if (rangeExpansion(c)) s += 0.05;
        return s;
    }

    /* ========================== BUILD ========================== */

    private TradeIdea buildLong(String s, double e, double atr) {
        double r = atr * 0.6;
        return new TradeIdea(s, TradingCore.Side.LONG,
                e, e - r, e + r * 1.6, e + r * 2.8, 0, atr, "");
    }

    private TradeIdea buildShort(String s, double e, double atr) {
        double r = atr * 0.6;
        return new TradeIdea(s, TradingCore.Side.SHORT,
                e, e + r, e - r * 1.6, e - r * 2.8, 0, atr, "");
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
        return l.high > recentHigh(c, 20) && (l.high - l.close) > atr * 0.25;
    }

    private boolean sweepLow(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return l.low < recentLow(c, 20) && (l.close - l.low) > atr * 0.25;
    }

    private boolean rejectionDown(List<TradingCore.Candle> c) {
        TradingCore.Candle l = last(c);
        return l.close < (l.high + l.low) / 2;
    }

    private boolean rejectionUp(List<TradingCore.Candle> c) {
        TradingCore.Candle l = last(c);
        return l.close > (l.high + l.low) / 2;
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
                p > e9 && e9 > e21 && e21 > e50,
                p < e9 && e9 < e21 && e21 < e50
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
        return last(c).volume > avgVol(c) * 1.3;
    }

    private boolean volumeDry(List<TradingCore.Candle> c) {
        return last(c).volume < avgVol(c) * 0.85;
    }

    private boolean volumeClimax(List<TradingCore.Candle> c) {
        return last(c).volume > avgVol(c) * 1.7;
    }

    private boolean impulse(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 5).close) > atr * 1.1;
    }

    private boolean rangeExpansion(List<TradingCore.Candle> c) {
        return (last(c).high - last(c).low) > avgRange(c) * 1.5;
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

    private enum MarketMode { TREND, EXHAUSTION, REVERSAL }

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
