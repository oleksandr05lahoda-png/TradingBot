package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DecisionEngineMerged {

    /* ============================ TA ============================ */
    public static class TA {

        public static double ema(List<Double> v, int p) {
            if (v == null || v.size() < p) return 0;
            double k = 2.0 / (p + 1);
            double e = v.get(0);
            for (int i = 1; i < v.size(); i++)
                e = v.get(i) * k + e * (1 - k);
            return e;
        }

        public static double rsi(List<Double> c, int p) {
            if (c == null || c.size() < p + 1) return 50;
            double g = 0, l = 0;
            for (int i = c.size() - p - 1; i < c.size() - 1; i++) {
                double d = c.get(i + 1) - c.get(i);
                if (d > 0) g += d;
                else l -= d;
            }
            if (l == 0) return 100;
            double rs = g / l;
            return 100 - (100 / (1 + rs));
        }

        public static double atr(List<TradingCore.Candle> c, int p) {
            if (c == null || c.size() < p + 1) return 0;
            double sum = 0;
            for (int i = c.size() - p; i < c.size(); i++) {
                TradingCore.Candle cur = c.get(i);
                TradingCore.Candle prev = c.get(i - 1);
                double tr = Math.max(
                        cur.high - cur.low,
                        Math.max(Math.abs(cur.high - prev.close),
                                Math.abs(cur.low - prev.close))
                );
                sum += tr;
            }
            return sum / p;
        }
    }

    /* ======================= ADAPTIVE CORE ====================== */
    public static class AdaptiveBrain {

        private static class Stat { int w, l; }

        private final Map<String, Stat> stats = new ConcurrentHashMap<>();
        private final Map<String, Deque<Long>> impulse = new ConcurrentHashMap<>();

        public void record(String k, boolean win) {
            stats.putIfAbsent(k, new Stat());
            if (win) stats.get(k).w++;
            else stats.get(k).l++;
        }

        private double wr(String k) {
            Stat s = stats.get(k);
            if (s == null) return 0.5;
            int t = s.w + s.l;
            if (t < 40) return 0.5;
            return (double) s.w / t;
        }

        public double adapt(String k, double c) {
            double w = wr(k);
            if (w > 0.60) return Math.min(0.85, c + 0.04);
            if (w < 0.45) return Math.max(0.45, c - 0.04);
            return c;
        }

        public double impulsePenalty(String s) {
            impulse.putIfAbsent(s, new ArrayDeque<>());
            Deque<Long> q = impulse.get(s);
            long now = System.currentTimeMillis();
            q.addLast(now);
            while (!q.isEmpty() && now - q.peekFirst() > 20 * 60_000)
                q.pollFirst();
            return q.size() >= 4 ? -0.04 : 0;
        }

        public double sessionBoost() {
            int h = LocalTime.now(ZoneOffset.UTC).getHour();
            return (h >= 7 && h <= 10) || (h >= 13 && h <= 16) ? 0.04 : 0;
        }
    }

    private final AdaptiveBrain adaptive = new AdaptiveBrain();

    /* ============================ DTO =========================== */
    public static class TradeIdea {
        public String symbol, side, reason;
        public double entry, atr, confidence;
    }

    /* ========================= MULTI TF ========================= */
    private int emaDir(List<TradingCore.Candle> c, int f, int s) {
        if (c == null || c.size() < s + 10) return 0;
        List<Double> cl = c.subList(c.size() - 60, c.size())
                .stream().map(x -> x.close).collect(Collectors.toList());
        return Double.compare(TA.ema(cl, f), TA.ema(cl, s));
    }

    private int mtfScore(List<TradingCore.Candle> c5,
                         List<TradingCore.Candle> c15,
                         List<TradingCore.Candle> c1h) {
        return emaDir(c5, 20, 50)
                + emaDir(c15, 20, 50) * 2
                + (c1h != null ? emaDir(c1h, 20, 50) * 3 : 0);
    }

    /* ============================ CORE ========================== */
    public Optional<TradeIdea> evaluate(
            String symbol,
            List<TradingCore.Candle> c5,
            List<TradingCore.Candle> c15,
            List<TradingCore.Candle> c1h) {

        if (c5 == null || c15 == null || c5.size() < 80 || c15.size() < 80)
            return Optional.empty();

        TradingCore.Candle last = c5.get(c5.size() - 1);
        TradingCore.Candle prev = c5.get(c5.size() - 2);

        List<Double> cl5 = c5.stream().map(x -> x.close).collect(Collectors.toList());
        List<Double> cl15 = c15.stream().map(x -> x.close).collect(Collectors.toList());

        double rsi5 = TA.rsi(cl5, 14);
        double rsi15 = TA.rsi(cl15, 14);
        double atr = TA.atr(c5, 14);
        double rsiPrevLocal = TA.rsi(cl5.subList(0, cl5.size() - 1), 14);

        boolean bearishDiv =
                last.close > prev.close &&
                        rsi5 < rsiPrevLocal &&
                        rsi5 > 55;

        boolean bullishDiv =
                last.close < prev.close &&
                        rsi5 > rsiPrevLocal &&
                        rsi5 < 45;

        int context =
                TA.ema(cl15, 20) > TA.ema(cl15, 50) ? 1 :
                        TA.ema(cl15, 20) < TA.ema(cl15, 50) ? -1 : 0;

        int mtf = mtfScore(c5, c15, c1h);
        boolean longSig =
                (bullishDiv || rsi5 < 35 || mtf > 0) &&
                        (last.high - last.low) < atr * 2.2;

        boolean shortSig =
                (bearishDiv || rsi5 > 65 || mtf < 0) &&
                        (last.high - last.low) < atr * 2.2;


        if (!longSig && !shortSig) return Optional.empty();

        String side = longSig ? "LONG" : "SHORT";

        double conf = 0.55;

        // RSI penalties instead of hard filters
        if (side.equals("LONG")) {
            if (rsi5 > 80) conf -= 0.03;
            if (rsi15 > 70) conf -= 0.03;
        } else {
            if (rsi5 < 25) conf -= 0.05;
            if (rsi15 < 30) conf -= 0.05;
        }
        conf += Math.min(0.10, Math.abs(mtf) * 0.03);
        conf += adaptive.sessionBoost();
        conf += adaptive.impulsePenalty(symbol + side);
        if (side.equals("LONG") && context > 0) conf += 0.05;
        if (side.equals("SHORT") && context < 0) conf += 0.05;
        conf = adaptive.adapt("CORE", conf);
        conf = Math.max(0.47, Math.min(0.85, conf));

        TradeIdea t = new TradeIdea();
        t.symbol = symbol;
        t.side = side;
        t.entry = last.close;
        t.atr = atr;
        t.confidence = conf;
        t.reason = "RSI divergence + exhaustion (5-bar forecast)";

        return Optional.of(t);
    }
}
