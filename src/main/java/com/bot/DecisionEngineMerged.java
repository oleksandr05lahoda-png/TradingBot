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

        MarketMode mode = marketMode(c1h, c15, c5, atr);
        EMA ema = emaContext(c5);
        TradingCore.Candle last = last(c5);

        List<TradeIdea> ideas = new ArrayList<>();

        // ===== TREND + PULLBACK CORE =====
        if (mode == MarketMode.TREND || mode == MarketMode.HIGH_VOL_TREND) {

            if (ema.bullish) {
                ideas.add(scored(buildLong(symbol, last.close, atr),
                        scoreTrend(c5, atr, true, mode),
                        "TREND LONG"));
            }

            if (ema.bearish) {
                ideas.add(scored(buildShort(symbol, last.close, atr),
                        scoreTrend(c5, atr, false, mode),
                        "TREND SHORT"));
            }

            if (pullbackLong(c5, ema)) {
                ideas.add(scored(buildLong(symbol, last.close, atr),
                        scorePullback(c5, atr),
                        "PULLBACK LONG"));
            }

            if (pullbackShort(c5, ema)) {
                ideas.add(scored(buildShort(symbol, last.close, atr),
                        scorePullback(c5, atr),
                        "PULLBACK SHORT"));
            }
        }

        // ===== MOMENTUM IMPULSE =====
        if (impulseBreakout(c5, atr)) {
            if (ema.bullish) {
                ideas.add(scored(buildLong(symbol, last.close, atr),
                        scoreMomentum(c5, atr),
                        "MOMENTUM LONG"));
            }
            if (ema.bearish) {
                ideas.add(scored(buildShort(symbol, last.close, atr),
                        scoreMomentum(c5, atr),
                        "MOMENTUM SHORT"));
            }
        }

        // ===== RANGE FADE =====
        if (mode == MarketMode.RANGE) {
            if (rangeFadeShort(c5, atr)) {
                ideas.add(scored(buildShort(symbol, last.close, atr),
                        scoreRange(c5),
                        "RANGE FADE SHORT"));
            }

            if (rangeFadeLong(c5, atr)) {
                ideas.add(scored(buildLong(symbol, last.close, atr),
                        scoreRange(c5),
                        "RANGE FADE LONG"));
            }
        }

        // ===== REVERSAL / LIQUIDITY SWEEP =====
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

        double minProb = adaptiveThreshold(mode);

        return ideas.stream()
                .filter(i -> i.probability >= minProb)
                .sorted(Comparator.comparingDouble(i -> -i.probability))
                .limit(5) // чуть больше сигналов для выбора
                .collect(Collectors.toList());
    }

    /* ========================== MARKET MODE ========================== */

    private MarketMode marketMode(List<TradingCore.Candle> c1h,
                                  List<TradingCore.Candle> c15,
                                  List<TradingCore.Candle> c5,
                                  double atr) {

        double htfMove = Math.abs(last(c1h).close - c1h.get(c1h.size() - 80).close) / atr;
        double mtfMove = Math.abs(last(c15).close - c15.get(c15.size() - 40).close) / atr;

        double atr5 = avgRange(c5) / atr;
        boolean highVol = atr5 > 1.4;
        boolean compress = rangeCompression(c5);

        if (htfMove > 7.0 && mtfMove < 2.0)
            return MarketMode.REVERSAL;

        if (compress)
            return MarketMode.RANGE;

        if (highVol)
            return MarketMode.HIGH_VOL_TREND;

        return MarketMode.TREND;
    }

    /* ========================== ADAPTIVE THRESHOLD ========================== */

    private double adaptiveThreshold(MarketMode mode) {
        return switch (mode) {
            case HIGH_VOL_TREND -> 0.50;
            case TREND -> 0.48;
            case RANGE -> 0.50;
            case REVERSAL -> 0.52;
        };
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

    private double scoreTrend(List<TradingCore.Candle> c, double atr, boolean up, MarketMode mode) {
        double s = 0.55;
        if (volumeBoost(c)) s += 0.06;
        if (impulse(c, atr)) s += 0.05;
        if (structureBreak(c, up)) s += 0.05;
        if (mode == MarketMode.HIGH_VOL_TREND) s += 0.03;
        return s;
    }

    private double scorePullback(List<TradingCore.Candle> c, double atr) {
        double s = 0.52;
        if (volumeDry(c)) s += 0.04;
        if (smallRange(c, atr)) s += 0.03;
        return s;
    }

    private double scoreMomentum(List<TradingCore.Candle> c, double atr) {
        double s = 0.53;
        if (volumeClimax(c)) s += 0.07;
        if (rangeExpansion(c)) s += 0.05;
        return s;
    }

    private double scoreRange(List<TradingCore.Candle> c) {
        double s = 0.55;
        if (volumeDry(c)) s += 0.03;
        return s;
    }

    private double scoreReversal(List<TradingCore.Candle> c) {
        double s = 0.54;
        if (volumeClimax(c)) s += 0.06;
        if (rangeExpansion(c)) s += 0.04;
        return s;
    }

    /* ========================== BUILD ========================== */

    private TradeIdea buildLong(String s, double e, double atr) {
        double r = atr * 0.55;
        return new TradeIdea(s, TradingCore.Side.LONG,
                e, e - r, e + r * 1.5, e + r * 2.5, 0, atr, "");
    }

    private TradeIdea buildShort(String s, double e, double atr) {
        double r = atr * 0.55;
        return new TradeIdea(s, TradingCore.Side.SHORT,
                e, e + r, e - r * 1.5, e - r * 2.5, 0, atr, "");
    }

    /* ========================== CONDITIONS ========================== */

    private boolean pullbackLong(List<TradingCore.Candle> c, EMA e) {
        double p = last(c).close;
        return p >= e.ema21 && p <= e.ema9;
    }

    private boolean pullbackShort(List<TradingCore.Candle> c, EMA e) {
        double p = last(c).close;
        return p <= e.ema21 && p >= e.ema9;
    }

    private boolean impulseBreakout(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 3).close) > atr * 0.9;
    }

    private boolean rangeFadeShort(List<TradingCore.Candle> c, double atr) {
        return last(c).high > recentHigh(c, 20) - atr * 0.25;
    }

    private boolean rangeFadeLong(List<TradingCore.Candle> c, double atr) {
        return last(c).low < recentLow(c, 20) + atr * 0.25;
    }

    private boolean sweepHigh(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return l.high > recentHigh(c, 20) && (l.high - l.close) > atr * 0.20;
    }

    private boolean sweepLow(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return l.low < recentLow(c, 20) && (l.close - l.low) > atr * 0.20;
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
        return last(c).volume > avgVol(c) * 1.25;
    }

    private boolean volumeDry(List<TradingCore.Candle> c) {
        return last(c).volume < avgVol(c) * 0.85;
    }

    private boolean volumeClimax(List<TradingCore.Candle> c) {
        return last(c).volume > avgVol(c) * 1.6;
    }

    private boolean impulse(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 5).close) > atr * 1.0;
    }

    private boolean rangeExpansion(List<TradingCore.Candle> c) {
        return (last(c).high - last(c).low) > avgRange(c) * 1.4;
    }

    private boolean smallRange(List<TradingCore.Candle> c, double atr) {
        return (last(c).high - last(c).low) < atr * 0.8;
    }

    private boolean structureBreak(List<TradingCore.Candle> c, boolean up) {
        return up
                ? last(c).close > recentHigh(c, 12)
                : last(c).close < recentLow(c, 12);
    }

    private boolean rangeCompression(List<TradingCore.Candle> c) {
        return avgRange(c) < avgRange(c.subList(0, c.size() - 20)) * 0.75;
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

    private enum MarketMode {
        TREND,
        HIGH_VOL_TREND,
        RANGE,
        REVERSAL
    }

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
