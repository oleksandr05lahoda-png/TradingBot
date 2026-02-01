package com.bot;

import java.util.*;
import java.util.concurrent.*;

public final class DecisionEngineMerged {

    /* ======================= CORE MODEL ======================= */
    public enum SignalGrade { A, B }
    public enum SignalAuthority { FINAL }

    public static final class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double probability, atr;
        public final SignalGrade grade;
        public final SignalAuthority authority;
        public final String reason, coinType;

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double entry,
                         double stop,
                         double take,
                         double probability,
                         double atr,
                         SignalGrade grade,
                         SignalAuthority authority,
                         String reason,
                         String coinType) {
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
            this.coinType = coinType;
        }
    }

    /* ======================= CONFIG ======================= */
    private static final int MAX_COINS = 70;
    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    private static final double MIN_ATR_PCT = 0.0016;   // 🔥 снижено — фьючи живут
    private static final double STOP_ATR = 0.55;
    private static final double RR_A = 1.8;
    private static final double RR_B = 1.4;
    private static final double HARD_MIN_STOP = 0.0012;

    /* ======================= MAIN ======================= */
    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> c15,
                                    Map<String, List<TradingCore.Candle>> c1h,
                                    Map<String, String> coinTypes) {

        List<TradeIdea> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String s : symbols) {
            if (scanned++ >= MAX_COINS) break;

            var m15 = c15.get(s);
            var h1  = c1h.get(s);
            if (!valid(m15, 60) || !valid(h1, 40)) continue;

            double price = last(m15).close;
            double atr = atr(m15, 14);
            if (atr / price < MIN_ATR_PCT) continue;

            EMA e15 = emaContext(m15);
            EMA e1h = emaContext(h1);

            scan(s, m15, atr, e15, e1h, out, now, coinTypes.getOrDefault(s, "ALT"));
        }

        out.sort((a, b) -> Double.compare(b.probability, a.probability));
        return out;
    }

    /* ======================= CORE SCAN ======================= */
    private void scan(String s,
                      List<TradingCore.Candle> c,
                      double atr,
                      EMA e15,
                      EMA e1h,
                      List<TradeIdea> out,
                      long now,
                      String type) {

        double base = 0.50;

        base += trendScore(e15, e1h);
        base += momentumScore(c);
        base += volumeScore(c);
        base += structureScore(c);

        if (base < 0.54) return; // 🔥 единственный жёсткий порог

        TradingCore.Side side =
                e15.bullish ? TradingCore.Side.LONG :
                        e15.bearish ? TradingCore.Side.SHORT : null;

        if (side == null) return;
        if (cooldowned(s, side.name(), now)) return;

        build(out, s, side, c, atr, base, type);
        mark(s, side.name(), now);
    }

    /* ======================= BUILD ======================= */
    private void build(List<TradeIdea> out,
                       String s,
                       TradingCore.Side side,
                       List<TradingCore.Candle> c,
                       double atr,
                       double p,
                       String type) {

        SignalGrade g = p >= 0.63 ? SignalGrade.A : SignalGrade.B;

        double price = last(c).close;
        double risk = Math.max(atr * STOP_ATR, price * HARD_MIN_STOP);

        double entry = price;
        double stop = side == TradingCore.Side.LONG ? entry - risk : entry + risk;
        double take = side == TradingCore.Side.LONG
                ? entry + risk * (g == SignalGrade.A ? RR_A : RR_B)
                : entry - risk * (g == SignalGrade.A ? RR_A : RR_B);

        out.add(new TradeIdea(
                s, side, entry, stop, take,
                clamp(p, 0.55, 0.78),
                atr, g, SignalAuthority.FINAL,
                "15M FUTURES " + g + " CONF",
                type
        ));
    }

    /* ======================= SCORING ======================= */
    private double trendScore(EMA ltf, EMA htf) {
        double s = 0;
        if (ltf.bullish) s += 0.04;
        if (ltf.bearish) s += 0.04;
        if (ltf.bullish && htf.bullish) s += 0.04;
        if (ltf.bearish && htf.bearish) s += 0.04;
        return s;
    }

    private double momentumScore(List<TradingCore.Candle> c) {
        double body = Math.abs(last(c).close - last(c).open);
        double range = last(c).high - last(c).low;
        return body / range > 0.55 ? 0.05 : 0.0;
    }

    private double volumeScore(List<TradingCore.Candle> c) {
        return last(c).volume > avgVol(c, 20) ? 0.04 : 0.0;
    }

    private double structureScore(List<TradingCore.Candle> c) {
        return rangeExpansion(c) ? 0.04 : 0.0;
    }

    /* ======================= UTILS ======================= */
    private boolean cooldowned(String s, String k, long n) {
        return cooldown.containsKey(s + k) && n - cooldown.get(s + k) < 90_000;
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
        return (last(c).high - last(c).low) > avgRange(c, 20) * 1.2;
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
        double e9 = ema(c, 9), e21 = ema(c, 21), e50 = ema(c, 50);
        return new EMA(e9, e21, e50,
                e9 > e21 && e21 > e50,
                e9 < e21 && e21 < e50);
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
            this.ema9 = e9; this.ema21 = e21; this.ema50 = e50;
            this.bullish = b; this.bearish = s;
        }
    }
}
