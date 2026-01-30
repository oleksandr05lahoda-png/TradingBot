package com.bot;

import java.util.*;
import java.util.concurrent.*;

public class DecisionEngineMerged {

    /* ========================== MODEL ========================== */

    public enum SignalGrade { A, B }

    public static class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double probability;
        public final double atr;
        public final SignalGrade grade;
        public final String reason;

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double entry,
                         double stop,
                         double take,
                         double probability,
                         double atr,
                         SignalGrade grade,
                         String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.probability = probability;
            this.atr = atr;
            this.grade = grade;
            this.reason = reason;
        }
    }

    /* ========================== CONFIG ========================== */

    private static final int MAX_COINS = 100;
    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    // 🔓 РАННИЙ вход, не душим рынок
    private static final double MIN_ATR_PCT = 0.0009;

    // 🎯 Фьючерсный риск
    private static final double STOP_ATR_MULT = 0.8;
    private static final double RR_A = 1.4;
    private static final double RR_B = 1.25;

    /* ========================== MAIN ========================== */

    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> c5m,
                                    Map<String, List<TradingCore.Candle>> c15m,
                                    Map<String, List<TradingCore.Candle>> c1h) {

        List<TradeIdea> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        int processed = 0;

        for (String s : symbols) {
            if (processed++ >= MAX_COINS) break;

            List<TradingCore.Candle> m5 = c5m.get(s);
            List<TradingCore.Candle> m15 = c15m.get(s);
            List<TradingCore.Candle> h1 = c1h.get(s);

            if (!valid(m5, 120) || !valid(m15, 80) || !valid(h1, 60)) continue;

            double atr = atr(m5, 14);
            double price = last(m5).close;
            if (atr <= 0 || atr / price < MIN_ATR_PCT) continue;

            EMA e5 = emaContext(m5);
            EMA e15 = emaContext(m15);
            EMA e1h = emaContext(h1);

            MarketMode mode = marketMode(m5);

            checkMomentum(s, m5, e5, e15, e1h, atr, out, now);
            checkPullback(s, m5, e5, e15, e1h, atr, out, now);

            if (mode == MarketMode.RANGE)
                checkRange(s, m5, atr, out, now);
        }

        out.sort((a, b) -> Double.compare(b.probability, a.probability));
        return out;
    }

    /* ========================== SIGNALS ========================== */

    private void checkMomentum(String s, List<TradingCore.Candle> c,
                               EMA e5, EMA e15, EMA e1h,
                               double atr, List<TradeIdea> out, long now) {

        if (!impulse(c, atr)) return;

        if (e5.bullish && e15.bullish && !cooldowned(s, "MOMO_L", now)) {
            double p = scoreMomentum(c) + htfBias(e1h, true);
            build(out, s, TradingCore.Side.LONG, c, atr, p, "MOMENTUM");
            mark(s, "MOMO_L", now);
        }

        if (e5.bearish && e15.bearish && !cooldowned(s, "MOMO_S", now)) {
            double p = scoreMomentum(c) + htfBias(e1h, false);
            build(out, s, TradingCore.Side.SHORT, c, atr, p, "MOMENTUM");
            mark(s, "MOMO_S", now);
        }
    }

    private void checkPullback(String s, List<TradingCore.Candle> c,
                               EMA e5, EMA e15, EMA e1h,
                               double atr, List<TradeIdea> out, long now) {

        double price = last(c).close;

        if (price > e5.ema21 && price < e5.ema9 && e15.bullish &&
                !cooldowned(s, "PB_L", now)) {
            double p = scorePullback(c) + htfBias(e1h, true);
            build(out, s, TradingCore.Side.LONG, c, atr, p, "PULLBACK");
            mark(s, "PB_L", now);
        }

        if (price < e5.ema21 && price > e5.ema9 && e15.bearish &&
                !cooldowned(s, "PB_S", now)) {
            double p = scorePullback(c) + htfBias(e1h, false);
            build(out, s, TradingCore.Side.SHORT, c, atr, p, "PULLBACK");
            mark(s, "PB_S", now);
        }
    }

    private void checkRange(String s, List<TradingCore.Candle> c,
                            double atr, List<TradeIdea> out, long now) {

        TradingCore.Candle l = last(c);

        if (l.high > recentHigh(c, 20) - atr * 0.25 &&
                !cooldowned(s, "R_S", now)) {
            double p = scoreRange(c);
            build(out, s, TradingCore.Side.SHORT, c, atr, p, "RANGE");
            mark(s, "R_S", now);
        }

        if (l.low < recentLow(c, 20) + atr * 0.25 &&
                !cooldowned(s, "R_L", now)) {
            double p = scoreRange(c);
            build(out, s, TradingCore.Side.LONG, c, atr, p, "RANGE");
            mark(s, "R_L", now);
        }
    }

    /* ========================== BUILD ========================== */

    private void build(List<TradeIdea> out, String s,
                       TradingCore.Side side,
                       List<TradingCore.Candle> c,
                       double atr, double p, String tag) {

        SignalGrade g = grade(p);
        if (g == null) return;

        double r = atr * STOP_ATR_MULT;
        double entry = last(c).close;

        double stop = side == TradingCore.Side.LONG
                ? entry - r
                : entry + r;

        double take = side == TradingCore.Side.LONG
                ? entry + r * (g == SignalGrade.A ? RR_A : RR_B)
                : entry - r * (g == SignalGrade.A ? RR_A : RR_B);

        out.add(new TradeIdea(
                s, side, entry, stop, take,
                Math.min(p, 0.72), atr, g, tag + " " + g
        ));
    }

    /* ========================== GRADING ========================== */

    private SignalGrade grade(double p) {
        if (p >= 0.62) return SignalGrade.A;
        if (p >= 0.55) return SignalGrade.B;
        return null;
    }

    /* ========================== SCORING ========================== */

    private double scoreMomentum(List<TradingCore.Candle> c) {
        double s = 0.53;
        if (rangeExpansion(c)) s += 0.06;
        if (volumeClimax(c)) s += 0.06;
        return s;
    }

    private double scorePullback(List<TradingCore.Candle> c) {
        double s = 0.51;
        if (volumeDry(c)) s += 0.04;
        if (smallRange(c)) s += 0.03;
        return s;
    }

    private double scoreRange(List<TradingCore.Candle> c) {
        double s = 0.52;
        if (volumeDry(c)) s += 0.03;
        return s;
    }

    /* ========================== HTF ========================== */

    private double htfBias(EMA htf, boolean longSide) {
        if (longSide && htf.bullish) return 0.03;
        if (!longSide && htf.bearish) return 0.03;
        if (longSide && htf.bearish) return -0.03;
        if (!longSide && htf.bullish) return -0.03;
        return 0;
    }

    /* ========================== UTILS ========================== */

    private boolean cooldowned(String s, String k, long n) {
        return cooldown.containsKey(s + k) && n - cooldown.get(s + k) < 60_000;
    }

    private void mark(String s, String k, long n) {
        cooldown.put(s + k, n);
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    private double atr(List<TradingCore.Candle> c, int n) {
        double s = 0;
        for (int i = c.size() - n; i < c.size(); i++)
            s += c.get(i).high - c.get(i).low;
        return s / n;
    }

    private boolean impulse(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 3).close) > atr * 0.45;
    }

    private boolean rangeExpansion(List<TradingCore.Candle> c) {
        return (last(c).high - last(c).low) > avgRange(c, 20) * 1.35;
    }

    private boolean smallRange(List<TradingCore.Candle> c) {
        return (last(c).high - last(c).low) < avgRange(c, 14) * 0.9;
    }

    private boolean volumeDry(List<TradingCore.Candle> c) {
        return last(c).volume < avgVol(c, 30) * 0.85;
    }

    private boolean volumeClimax(List<TradingCore.Candle> c) {
        return last(c).volume > avgVol(c, 30) * 1.5;
    }

    private double avgRange(List<TradingCore.Candle> c, int n) {
        double s = 0;
        for (int i = c.size() - n; i < c.size(); i++)
            s += c.get(i).high - c.get(i).low;
        return s / n;
    }

    private double avgVol(List<TradingCore.Candle> c, int n) {
        double s = 0;
        for (int i = c.size() - n; i < c.size(); i++)
            s += c.get(i).volume;
        return s / n;
    }

    private double recentHigh(List<TradingCore.Candle> c, int n) {
        double m = -1e9;
        for (int i = c.size() - n; i < c.size(); i++)
            m = Math.max(m, c.get(i).high);
        return m;
    }

    private double recentLow(List<TradingCore.Candle> c, int n) {
        double m = 1e9;
        for (int i = c.size() - n; i < c.size(); i++)
            m = Math.min(m, c.get(i).low);
        return m;
    }

    /* ========================== EMA ========================== */

    private EMA emaContext(List<TradingCore.Candle> c) {
        double e9 = ema(c, 9);
        double e21 = ema(c, 21);
        double e50 = ema(c, 50);
        double p = last(c).close;

        return new EMA(
                e9, e21, e50,
                p > e9 && e9 > e21 && e21 > e50,
                p < e9 && e9 < e21 && e21 < e50
        );
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private MarketMode marketMode(List<TradingCore.Candle> c) {
        return avgRange(c, 20) < avgRange(c, 60) * 0.7
                ? MarketMode.RANGE
                : MarketMode.TREND;
    }

    private enum MarketMode { TREND, RANGE }

    private static class EMA {
        double ema9, ema21, ema50;
        boolean bullish, bearish;

        EMA(double e9, double e21, double e50, boolean b, boolean s) {
            this.ema9 = e9;
            this.ema21 = e21;
            this.ema50 = e50;
            this.bullish = b;
            this.bearish = s;
        }
    }
}
