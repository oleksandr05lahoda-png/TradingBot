package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    /* ===================== MODELS ===================== */

    public enum SignalGrade { A, B }

    public static final class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final double atr;
        public final SignalGrade grade;
        public final String reason;
        public final String coinType;

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double entry,
                         double stop,
                         double take,
                         double confidence,
                         double atr,
                         SignalGrade grade,
                         String reason,
                         String coinType) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.atr = atr;
            this.grade = grade;
            this.reason = reason;
            this.coinType = coinType;
        }
    }

    /* ===================== CONFIG ===================== */

    private static final int MIN_BARS = 160;

    private static final double RR_A = 2.4;
    private static final double RR_B = 1.6;

    private static final long COOLDOWN_MS = 10 * 60_000;

    private static final Map<String, Long> cooldown =
            new ConcurrentHashMap<>();

    /* ===================== ENTRY ===================== */

    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> m15,
                                    Map<String, List<TradingCore.Candle>> h1,
                                    Map<String, String> coinTypes) {

        List<TradeIdea> result = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (String symbol : symbols) {

            List<TradingCore.Candle> c15 = m15.get(symbol);
            List<TradingCore.Candle> c1h = h1.get(symbol);

            if (!valid(c15, MIN_BARS) || !valid(c1h, MIN_BARS))
                continue;

            HTFBias bias = detectHTFBias(c1h);
            if (bias == HTFBias.NONE)
                continue;

            TradeIdea idea = scan(
                    symbol,
                    c15,
                    c1h,
                    bias,
                    coinTypes.getOrDefault(symbol, "ALT"),
                    now
            );

            if (idea != null)
                result.add(idea);
        }

        result.sort(
                Comparator.comparingDouble((TradeIdea t) -> t.confidence)
                        .reversed()
        );

        return result;
    }

    /* ===================== CORE ===================== */

    private TradeIdea scan(String symbol,
                           List<TradingCore.Candle> m15,
                           List<TradingCore.Candle> h1,
                           HTFBias bias,
                           String type,
                           long now) {

        TradingCore.Side side =
                (bias == HTFBias.BULL)
                        ? TradingCore.Side.LONG
                        : TradingCore.Side.SHORT;

        String key = symbol + "_" + side;
        if (cooldown.containsKey(key)
                && now - cooldown.get(key) < COOLDOWN_MS)
            return null;

        if (!pullbackStructure(m15, side))
            return null;

        double atr = trueATR(m15, 14);
        double adx = adx(m15, 14);

        if (adx < 13)
            return null;

        double entry = last(m15).close;

        double riskMultiplier =
                (adx > 22) ? 0.70 :
                        (adx > 18) ? 0.60 :
                                0.50;

        double risk = atr * riskMultiplier;

        double stop = (side == TradingCore.Side.LONG)
                ? entry - risk
                : entry + risk;

        double confidence =
                computeConfidence(m15, h1, side, type, adx);

        SignalGrade grade =
                confidence >= 0.70
                        ? SignalGrade.A
                        : SignalGrade.B;

        double rr =
                (grade == SignalGrade.A)
                        ? RR_A
                        : RR_B;

        double take =
                (side == TradingCore.Side.LONG)
                        ? entry + risk * rr
                        : entry - risk * rr;

        cooldown.put(key, now);

        return new TradeIdea(
                symbol,
                side,
                entry,
                stop,
                take,
                confidence,
                atr,
                grade,
                "HTF ALIGNED PULLBACK",
                type
        );
    }

    /* ===================== HTF BIAS ===================== */

    private enum HTFBias { BULL, BEAR, NONE }

    private HTFBias detectHTFBias(List<TradingCore.Candle> h1) {

        double ema50 = ema(h1, 50);
        double ema200 = ema(h1, 200);
        double price = last(h1).close;

        if (price > ema50 && ema50 > ema200)
            return HTFBias.BULL;

        if (price < ema50 && ema50 < ema200)
            return HTFBias.BEAR;

        return HTFBias.NONE;
    }

    /* ===================== STRUCTURE ===================== */

    private boolean pullbackStructure(List<TradingCore.Candle> c,
                                      TradingCore.Side side) {

        int n = c.size();

        TradingCore.Candle a = c.get(n - 3);
        TradingCore.Candle b = c.get(n - 2);
        TradingCore.Candle d = c.get(n - 1);

        if (side == TradingCore.Side.LONG)
            return d.close > b.high
                    && b.low > a.low;

        return d.close < b.low
                && b.high < a.high;
    }

    /* ===================== CONFIDENCE ===================== */

    private double computeConfidence(List<TradingCore.Candle> m15,
                                     List<TradingCore.Candle> h1,
                                     TradingCore.Side side,
                                     String type,
                                     double adx) {

        double structureScore = structureStrength(m15);
        double trendScore = trendAcceleration(h1);
        double volatilityScore =
                Math.min(0.12, trueATR(m15, 14) / last(m15).close * 40);

        double adxScore = Math.min(0.15, adx / 100.0);

        double conf =
                0.50 +
                        structureScore * 0.15 +
                        trendScore * 0.18 +
                        volatilityScore +
                        adxScore;

        if ("TOP".equals(type)) conf += 0.03;
        if ("MEME".equals(type)) conf -= 0.04;

        return clamp(conf, 0.48, 0.93);
    }

    /* ===================== STRUCTURE SCORE ===================== */

    private double structureStrength(List<TradingCore.Candle> c) {

        TradingCore.Candle last = last(c);
        TradingCore.Candle prev = c.get(c.size() - 2);

        double body = Math.abs(last.close - last.open);
        double range = last.high - last.low;

        if (range == 0) return 0;

        double bodyPower = body / range;
        double momentum =
                Math.abs(last.close - prev.close) / range;

        return clamp(
                bodyPower * 0.6 + momentum * 0.4,
                0.0,
                1.0
        );
    }

    /* ===================== INDICATORS ===================== */

    private double trueATR(List<TradingCore.Candle> c, int n) {

        double sum = 0;

        for (int i = c.size() - n; i < c.size(); i++) {

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

        return sum / n;
    }

    private double adx(List<TradingCore.Candle> c, int n) {

        double move = 0;

        for (int i = c.size() - n; i < c.size() - 1; i++)
            move += Math.abs(
                    c.get(i + 1).close - c.get(i).close
            );

        return (move / n) / trueATR(c, n) * 25.0;
    }

    private double trendAcceleration(List<TradingCore.Candle> c) {

        double e21 = ema(c, 21);
        double e50 = ema(c, 50);

        return clamp(
                Math.abs(e21 - e50) / e50 * 6,
                0,
                1
        );
    }

    /* ===================== MATH ===================== */

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> c, int n) {
        return c != null && c.size() >= n;
    }

    private double ema(List<TradingCore.Candle> c, int p) {

        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;

        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);

        return e;
    }

    private double clamp(double v,
                         double min,
                         double max) {
        return Math.max(min,
                Math.min(max, v));
    }
}
