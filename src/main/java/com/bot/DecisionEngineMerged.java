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
                        Math.max(
                                Math.abs(cur.high - prev.close),
                                Math.abs(cur.low - prev.close)
                        )
                );
                sum += tr;
            }
            return sum / p;
        }
    }

    /* ======================= ADAPTIVE ========================== */
    private final Map<String, Deque<Long>> impulse = new ConcurrentHashMap<>();

    private double impulsePenalty(String key) {
        impulse.putIfAbsent(key, new ArrayDeque<>());
        Deque<Long> q = impulse.get(key);
        long now = System.currentTimeMillis();
        q.addLast(now);
        while (!q.isEmpty() && now - q.peekFirst() > 15 * 60_000)
            q.pollFirst();
        return q.size() >= 3 ? -0.06 : 0;
    }

    private double sessionBoost() {
        int h = LocalTime.now(ZoneOffset.UTC).getHour();
        return (h >= 7 && h <= 10) || (h >= 13 && h <= 16) ? 0.04 : 0;
    }

    /* ============================ DTO =========================== */
    public static class TradeIdea {
        public String symbol, side, reason;
        public double entry, atr, confidence;
    }

    /* ======================= MARKET REGIME ===================== */
    enum Regime { TREND, RANGE, DEAD }

    private Regime detectRegime(List<TradingCore.Candle> c15) {
        List<Double> cl = c15.stream().map(x -> x.close).toList();

        double atr = TA.atr(c15, 14);
        double ema20 = TA.ema(cl, 20);
        double ema50 = TA.ema(cl, 50);
        double slope = ema20 - TA.ema(cl.subList(0, cl.size() - 10), 20);

        if (atr <= 0) return Regime.DEAD;
        if (Math.abs(slope) < atr * 0.04) return Regime.RANGE;
        return Regime.TREND;
    }

    /* ========================= TREND BIAS ====================== */
    private int trendBias(List<TradingCore.Candle> c15, List<TradingCore.Candle> c1h) {
        List<Double> cl15 = c15.stream().map(x -> x.close).toList();
        int dir = Double.compare(
                TA.ema(cl15, 20),
                TA.ema(cl15, 50)
        );
        if (c1h != null && c1h.size() > 60) {
            List<Double> cl1h = c1h.stream().map(x -> x.close).toList();
            dir += Double.compare(
                    TA.ema(cl1h, 20),
                    TA.ema(cl1h, 50)
            );
        }
        return dir;
    }

    /* ============================ CORE ========================== */
    public Optional<TradeIdea> evaluate(
            String symbol,
            List<TradingCore.Candle> c5,
            List<TradingCore.Candle> c15,
            List<TradingCore.Candle> c1h) {

        if (c5 == null || c15 == null || c5.size() < 120 || c15.size() < 120)
            return Optional.empty();

        Regime regime = detectRegime(c15);
        if (regime != Regime.TREND) return Optional.empty();

        int bias = trendBias(c15, c1h);
        if (bias == 0) return Optional.empty();

        TradingCore.Candle last = c5.get(c5.size() - 1);
        TradingCore.Candle prev = c5.get(c5.size() - 2);

        List<Double> cl5 = c5.stream().map(x -> x.close).collect(Collectors.toList());

        double atr = TA.atr(c5, 14);
        if (atr <= 0) return Optional.empty();

        double body = Math.abs(last.close - last.open);
        boolean impulse =
                body > atr * 0.6 &&
                        (last.high - last.low) < atr * 1.8;

        double rsi5 = TA.rsi(cl5, 14);

        boolean longSig =
                bias > 0 &&
                        impulse &&
                        rsi5 < 72;

        boolean shortSig =
                bias < 0 &&
                        impulse &&
                        rsi5 > 28;

        if (!longSig && !shortSig) return Optional.empty();

        String side = longSig ? "LONG" : "SHORT";

        double conf = 0.60;
        conf += sessionBoost();
        conf += impulsePenalty(symbol + side);
        conf = Math.max(0.50, Math.min(0.85, conf));

        TradeIdea t = new TradeIdea();
        t.symbol = symbol;
        t.side = side;
        t.entry = last.close;
        t.atr = atr;
        t.confidence = conf;
        t.reason = "TREND+IMPULSE+ATR";

        return Optional.of(t);
    }
}
