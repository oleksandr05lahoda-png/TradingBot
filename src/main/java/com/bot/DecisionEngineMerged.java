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

    /* ======================= ADAPTIVE CORE ====================== */
    public static class AdaptiveBrain {

        private static class Stat { int w, l; }

        private final Map<String, Stat> stats = new ConcurrentHashMap<>();
        private final Map<String, Deque<Long>> impulse = new ConcurrentHashMap<>();

        public double adapt(String k, double c) {
            Stat s = stats.get(k);
            if (s == null) return c;
            int t = s.w + s.l;
            if (t < 40) return c;
            double wr = (double) s.w / t;
            if (wr > 0.60) return Math.min(0.75, c + 0.02);
            if (wr < 0.45) return Math.max(0.55, c - 0.02);
            return c;
        }

        public double impulsePenalty(String key) {
            impulse.putIfAbsent(key, new ArrayDeque<>());
            Deque<Long> q = impulse.get(key);
            long now = System.currentTimeMillis();
            q.addLast(now);
            while (!q.isEmpty() && now - q.peekFirst() > 20 * 60_000)
                q.pollFirst();
            return q.size() >= 3 ? -0.05 : 0;
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

    /* ========================= REGIME ========================== */
    enum Regime { TREND, RANGE, DEAD }

    private Regime detectRegime(List<TradingCore.Candle> c15) {
        List<Double> cl = c15.stream().map(x -> x.close).toList();

        double atrNow  = TA.atr(c15, 14);
        double atrPrev = TA.atr(c15.subList(0, c15.size() - 10), 14);

        double ema20 = TA.ema(cl, 20);
        double ema50 = TA.ema(cl, 50);
        double slope = ema20 - TA.ema(
                cl.subList(0, cl.size() - 10), 20
        );

        if (atrNow < atrPrev * 0.8) return Regime.DEAD;
        if (Math.abs(slope) < atrNow * 0.05) return Regime.RANGE;
        return Regime.TREND;
    }

    /* ============================ CORE ========================== */
    public Optional<TradeIdea> evaluate(
            String symbol,
            List<TradingCore.Candle> c5,
            List<TradingCore.Candle> c15,
            List<TradingCore.Candle> c1h) {

        if (c5 == null || c15 == null || c1h == null ||
                c5.size() < 60 || c15.size() < 60 || c1h.size() < 60)
            return Optional.empty();

        if (detectRegime(c15) != Regime.TREND)
            return Optional.empty();

        TradingCore.Candle last = c5.get(c5.size() - 1);
        TradingCore.Candle prev = c5.get(c5.size() - 2);

        List<Double> cl5  = c5.stream().map(x -> x.close).toList();
        List<Double> cl15 = c15.stream().map(x -> x.close).toList();
        List<Double> cl1h = c1h.stream().map(x -> x.close).toList();

        double ema5_20   = TA.ema(cl5, 20);
        double ema15_20  = TA.ema(cl15, 20);
        double ema15_50  = TA.ema(cl15, 50);
        double ema1h_20  = TA.ema(cl1h, 20);
        double ema1h_50  = TA.ema(cl1h, 50);

        boolean trendUp =
                ema1h_20 > ema1h_50 &&
                        ema15_20 > ema15_50;

        boolean trendDown =
                ema1h_20 < ema1h_50 &&
                        ema15_20 < ema15_50;

        if (!trendUp && !trendDown)
            return Optional.empty();

        double atr = TA.atr(c5, 14);

        if (atr < last.close * 0.002)
            return Optional.empty();

        boolean pullbackLong =
                trendUp &&
                        last.low <= ema5_20 &&
                        prev.isBear() &&
                        last.isBull() &&
                        (last.high - last.low) < atr * 1.2;

        boolean pullbackShort =
                trendDown &&
                        last.high >= ema5_20 &&
                        prev.isBull() &&
                        last.isBear() &&
                        (last.high - last.low) < atr * 1.2;

        if (!pullbackLong && !pullbackShort)
            return Optional.empty();

        String side = pullbackLong ? "LONG" : "SHORT";

        double conf = 0.55;
        conf += Math.abs(ema15_20 - ema15_50) > atr * 0.5 ? 0.05 : 0;
        conf += adaptive.sessionBoost();
        conf += adaptive.impulsePenalty(symbol + side);
        conf = adaptive.adapt("PULLBACK", conf);
        conf = Math.max(0.55, Math.min(0.75, conf));

        TradeIdea t = new TradeIdea();
        t.symbol = symbol;
        t.side = side;
        t.entry = last.close;
        t.atr = atr;
        t.confidence = conf;
        t.reason = "HTF trend + pullback continuation";

        return Optional.of(t);
    }
}
