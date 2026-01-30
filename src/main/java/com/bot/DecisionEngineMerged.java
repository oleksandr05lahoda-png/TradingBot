package com.bot;

import java.util.*;
import java.util.concurrent.*;

public final class DecisionEngineMerged {

    /* ======================= CORE MODEL ======================= */

    public enum SignalGrade { A, B }

    public enum SignalAuthority {
        FINAL // стоп и тейк НЕ МЕНЯЮТСЯ
    }

    public static final class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double stop;
        public final double take;
        public final double probability;
        public final double atr;
        public final SignalGrade grade;
        public final SignalAuthority authority;
        public final String reason;

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double entry,
                         double stop,
                         double take,
                         double probability,
                         double atr,
                         SignalGrade grade,
                         SignalAuthority authority,
                         String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.probability = probability;
            this.atr = atr;
            this.grade = grade;
            this.authority = authority;
            this.reason = reason;
        }
    }

    /* ======================= CONFIG ======================= */

    private static final int MAX_COINS = 80;
    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    private static final double MIN_ATR_PCT = 0.0010;
    private static final double STOP_ATR = 0.7;
    private static final double RR_A = 2.2;
    private static final double RR_B = 1.7;
    private static final double MAX_EXTENSION_ATR = 1.0;
    private static final double HARD_MIN_STOP_PCT = 0.0009;

    /* ======================= MAIN ======================= */

    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> c5,
                                    Map<String, List<TradingCore.Candle>> c15,
                                    Map<String, List<TradingCore.Candle>> c1h) {

        List<TradeIdea> result = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String s : symbols) {
            if (scanned++ >= MAX_COINS) break;

            List<TradingCore.Candle> m5 = c5.get(s);
            List<TradingCore.Candle> m15 = c15.get(s);
            List<TradingCore.Candle> h1 = c1h.get(s);

            if (!valid(m5, 120) || !valid(m15, 80) || !valid(h1, 60)) continue;

            double price = last(m5).close;
            double atr = atr(m5, 14);
            if (atr / price < MIN_ATR_PCT) continue;

            EMA e5 = emaContext(m5);
            EMA e15 = emaContext(m15);
            EMA e1h = emaContext(h1);

            if (overextended(price, e5.ema21, atr)) continue;

            scanMomentum(s, m5, e15, e1h, atr, result, now);
            scanPullback(s, m5, e5, e15, e1h, atr, result, now);
        }

        result.sort((a, b) -> Double.compare(b.probability, a.probability));
        return result;
    }

    /* ======================= SETUPS ======================= */

    private void scanMomentum(String s, List<TradingCore.Candle> c,
                              EMA e15, EMA e1h,
                              double atr, List<TradeIdea> out, long now) {

        if (!impulse(c, atr)) return;

        if (e15.bullish && !cooldowned(s, "MOMO_L", now)) {
            double p = momentumScore(c) + htfBias(e1h, true);
            build(out, s, TradingCore.Side.LONG, c, atr, p, "EARLY_MOMENTUM");
            mark(s, "MOMO_L", now);
        }

        if (e15.bearish && !cooldowned(s, "MOMO_S", now)) {
            double p = momentumScore(c) + htfBias(e1h, false);
            build(out, s, TradingCore.Side.SHORT, c, atr, p, "EARLY_MOMENTUM");
            mark(s, "MOMO_S", now);
        }
    }

    private void scanPullback(String s, List<TradingCore.Candle> c,
                              EMA e5, EMA e15, EMA e1h,
                              double atr, List<TradeIdea> out, long now) {

        double price = last(c).close;

        if (price < e5.ema21 && e15.bullish && !cooldowned(s, "PB_L", now)) {
            double p = pullbackScore(c) + htfBias(e1h, true);
            build(out, s, TradingCore.Side.LONG, c, atr, p, "PULLBACK_CONT");
            mark(s, "PB_L", now);
        }

        if (price > e5.ema21 && e15.bearish && !cooldowned(s, "PB_S", now)) {
            double p = pullbackScore(c) + htfBias(e1h, false);
            build(out, s, TradingCore.Side.SHORT, c, atr, p, "PULLBACK_CONT");
            mark(s, "PB_S", now);
        }
    }

    /* ======================= BUILD ======================= */

    private void build(List<TradeIdea> out,
                       String s,
                       TradingCore.Side side,
                       List<TradingCore.Candle> c,
                       double atr,
                       double p,
                       String tag) {

        SignalGrade g = grade(p);
        if (g == null) return;

        double price = last(c).close;
        double risk = Math.max(atr * STOP_ATR, price * HARD_MIN_STOP_PCT);

        double entry = price + (side == TradingCore.Side.LONG ? atr * 0.05 : -atr * 0.05);
        double stop = side == TradingCore.Side.LONG ? entry - risk : entry + risk;
        double take = side == TradingCore.Side.LONG
                ? entry + risk * (g == SignalGrade.A ? RR_A : RR_B)
                : entry - risk * (g == SignalGrade.A ? RR_A : RR_B);

        out.add(new TradeIdea(
                s, side, entry, stop, take,
                clamp(p, 0.50, 0.72),
                atr,
                g,
                SignalAuthority.FINAL,
                tag + " " + g
        ));
    }

    /* ======================= SCORING ======================= */

    private double momentumScore(List<TradingCore.Candle> c) {
        double s = 0.54;
        if (rangeExpansion(c)) s += 0.05;
        if (volumeClimax(c)) s += 0.05;
        return s;
    }

    private double pullbackScore(List<TradingCore.Candle> c) {
        double s = 0.53;
        if (volumeDry(c)) s += 0.04;
        return s;
    }

    private double htfBias(EMA htf, boolean isLong) {
        if (isLong && htf.bullish) return 0.04;
        if (!isLong && htf.bearish) return 0.04;
        return -0.03;
    }

    /* ======================= FILTERS ======================= */

    private boolean impulse(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 2).close) > atr * 0.3;
    }

    private boolean overextended(double price, double ema21, double atr) {
        return Math.abs(price - ema21) > atr * MAX_EXTENSION_ATR;
    }

    /* ======================= GRADING ======================= */

    private SignalGrade grade(double p) {
        if (p >= 0.62) return SignalGrade.A;
        if (p >= 0.56) return SignalGrade.B;
        return null;
    }

    /* ======================= UTILS ======================= */

    private boolean cooldowned(String s, String k, long n) {
        return cooldown.containsKey(s + k) && n - cooldown.get(s + k) < 55_000;
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

    private boolean rangeExpansion(List<TradingCore.Candle> c) {
        return (last(c).high - last(c).low) > avgRange(c, 20) * 1.3;
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

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    private static final class EMA {
        final double ema9, ema21, ema50;
        final boolean bullish, bearish;

        EMA(double e9, double e21, double e50, boolean b, boolean s) {
            this.ema9 = e9;
            this.ema21 = e21;
            this.ema50 = e50;
            this.bullish = b;
            this.bearish = s;
        }
    }
}
