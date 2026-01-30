package com.bot;

import java.util.*;
import java.util.concurrent.*;

public class DecisionEngineMerged {

    /* ========================== MODEL ========================== */

    public enum SignalGrade { A, B } // C убрал, слабые сигналы не берем

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

    /* ========================== CONSTANTS ========================== */

    private static final int MAX_COINS = 100;
    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    private static final double MIN_ATR_PCT = 0.0015; // фильтр флэта
    private static final double STOP_ATR_MULT = 1.2;
    private static final double RR_A = 1.7;
    private static final double RR_B = 1.4;

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
            if (atr <= 0 || atr / last(m5).close < MIN_ATR_PCT) continue;

            EMA e5 = emaContext(m5);
            EMA e15 = emaContext(m15);
            EMA e1h = emaContext(h1);

            MarketMode mode = marketMode(m5, m15, atr);

            // ===== MOMENTUM =====
            checkMomentum(s, m5, e5, e15, e1h, atr, mode, out, now);

            // ===== PULLBACK =====
            checkPullback(s, m5, e5, e15, e1h, atr, mode, out, now);

            // ===== RANGE =====
            if (mode == MarketMode.RANGE) {
                checkRange(s, m5, atr, out, now);
            }
        }

        out.sort((a, b) -> Double.compare(b.probability, a.probability));
        return out;
    }

    /* ========================== SIGNALS ========================== */

    private void checkMomentum(String s, List<TradingCore.Candle> c,
                               EMA e5, EMA e15, EMA e1h,
                               double atr, MarketMode mode,
                               List<TradeIdea> out, long now) {

        if (!impulse(c, atr)) return;

        if (e5.bullish && e15.bullish && !cooldowned(s, "MOMO_L", now, 120_000)) {
            double p = scoreMomentum(c, atr) + htfBias(e1h, true);
            buildAndAdd(out, s, TradingCore.Side.LONG, c, atr, p, "MOMENTUM");
            mark(s, "MOMO_L", now);
        }

        if (e5.bearish && e15.bearish && !cooldowned(s, "MOMO_S", now, 120_000)) {
            double p = scoreMomentum(c, atr) + htfBias(e1h, false);
            buildAndAdd(out, s, TradingCore.Side.SHORT, c, atr, p, "MOMENTUM");
            mark(s, "MOMO_S", now);
        }
    }

    private void checkPullback(String s, List<TradingCore.Candle> c,
                               EMA e5, EMA e15, EMA e1h,
                               double atr, MarketMode mode,
                               List<TradeIdea> out, long now) {

        double price = last(c).close;

        if (price > e5.ema21 && price < e5.ema9 && e15.bullish &&
                !cooldowned(s, "PB_L", now, 120_000)) {
            double p = scorePullback(c, atr) + htfBias(e1h, true);
            buildAndAdd(out, s, TradingCore.Side.LONG, c, atr, p, "PULLBACK");
            mark(s, "PB_L", now);
        }

        if (price < e5.ema21 && price > e5.ema9 && e15.bearish &&
                !cooldowned(s, "PB_S", now, 120_000)) {
            double p = scorePullback(c, atr) + htfBias(e1h, false);
            buildAndAdd(out, s, TradingCore.Side.SHORT, c, atr, p, "PULLBACK");
            mark(s, "PB_S", now);
        }
    }

    private void checkRange(String s, List<TradingCore.Candle> c,
                            double atr, List<TradeIdea> out, long now) {

        TradingCore.Candle l = last(c);

        if (l.high > recentHigh(c, 20) - atr * 0.25 &&
                !cooldowned(s, "R_S", now, 60_000)) {
            double p = scoreRange(c);
            buildAndAdd(out, s, TradingCore.Side.SHORT, c, atr, p, "RANGE");
            mark(s, "R_S", now);
        }

        if (l.low < recentLow(c, 20) + atr * 0.25 &&
                !cooldowned(s, "R_L", now, 60_000)) {
            double p = scoreRange(c);
            buildAndAdd(out, s, TradingCore.Side.LONG, c, atr, p, "RANGE");
            mark(s, "R_L", now);
        }
    }

    /* ========================== BUILD ========================== */

    private void buildAndAdd(List<TradeIdea> out, String s,
                             TradingCore.Side side,
                             List<TradingCore.Candle> c,
                             double atr, double p, String tag) {

        SignalGrade g = grade(p);
        if (g == null) return;

        double r = atr * STOP_ATR_MULT;

        double entry = last(c).close;
        double stop = side == TradingCore.Side.LONG ? entry - r : entry + r;
        double take = side == TradingCore.Side.LONG
                ? entry + r * (g == SignalGrade.A ? RR_A : RR_B)
                : entry - r * (g == SignalGrade.A ? RR_A : RR_B);

        out.add(new TradeIdea(s, side, entry, stop, take,
                Math.min(p, 0.75), atr, g, tag + " " + g));
    }

    /* ========================== GRADING ========================== */

    private SignalGrade grade(double p) {
        if (p >= 0.65) return SignalGrade.A;
        if (p >= 0.58) return SignalGrade.B;
        return null;
    }

    /* ========================== SCORING ========================== */

    private double scoreMomentum(List<TradingCore.Candle> c, double atr) {
        double s = 0.52;
        if (rangeExpansion(c)) s += 0.06;
        if (volumeClimax(c)) s += 0.07;
        return s;
    }

    private double scorePullback(List<TradingCore.Candle> c, double atr) {
        double s = 0.50;
        if (volumeDry(c)) s += 0.04;
        if (smallRange(c, atr)) s += 0.03;
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
        if (longSide && htf.bearish) return -0.04;
        if (!longSide && htf.bullish) return -0.04;
        return 0;
    }

    /* ========================== MARKET MODE ========================== */

    private MarketMode marketMode(List<TradingCore.Candle> c5,
                                  List<TradingCore.Candle> c15,
                                  double atr) {
        boolean compress = avgRange(c5, 20) < avgRange(c5, 60) * 0.7;
        return compress ? MarketMode.RANGE : MarketMode.TREND;
    }

    /* ========================== UTILS ========================== */

    private boolean cooldowned(String s, String k, long n, long ms) {
        return cooldown.containsKey(s + k) && n - cooldown.get(s + k) < ms;
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
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++)
            sum += c.get(i).high - c.get(i).low;
        return sum / n;
    }

    private boolean impulse(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 3).close) > atr * 0.8;
    }

    private boolean rangeExpansion(List<TradingCore.Candle> c) {
        TradingCore.Candle l = last(c);
        return (l.high - l.low) > avgRange(c, 20) * 1.4;
    }

    private boolean smallRange(List<TradingCore.Candle> c, double atr) {
        return (last(c).high - last(c).low) < atr * 0.8;
    }

    private boolean volumeDry(List<TradingCore.Candle> c) {
        return last(c).volume < avgVol(c, 30) * 0.85;
    }

    private boolean volumeClimax(List<TradingCore.Candle> c) {
        return last(c).volume > avgVol(c, 30) * 1.6;
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
        return new EMA(e9, e21, e50,
                p > e9 && e9 > e21 && e21 > e50,
                p < e9 && e9 < e21 && e21 < e50);
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
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
