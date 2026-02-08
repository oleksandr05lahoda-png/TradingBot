package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PROFESSIONAL FUTURES DECISION ENGINE
 * - No prediction
 * - No flip signals
 * - HTF controls regime
 * - LTF only entry timing
 */
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

    private static final int MAX_COINS = 120;

    private static final double STOP_ATR = 0.55;
    private static final double RR_A = 2.2;
    private static final double RR_B = 1.6;

    private static final long COOLDOWN_MS = 20 * 60_000; // 20 мин
    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    /* ===================== ENTRY ===================== */

    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> m15,
                                    Map<String, List<TradingCore.Candle>> h1,
                                    Map<String, String> coinTypes) {

        List<TradeIdea> result = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String symbol : symbols) {
            if (scanned++ >= MAX_COINS) break;

            List<TradingCore.Candle> c15 = m15.get(symbol);
            List<TradingCore.Candle> c1h = h1.get(symbol);

            if (!valid(c15, 50) || !valid(c1h, 50)) continue;

            MarketRegime regime = detectRegime(c1h);
            if (regime == MarketRegime.RANGE) continue;

            TradeIdea idea = scan(symbol, c15, c1h, regime,
                    coinTypes.getOrDefault(symbol, "ALT"), now);

            if (idea != null) result.add(idea);
        }

        result.sort(Comparator.comparingDouble((TradeIdea t) -> t.confidence).reversed());
        return result;
    }

    /* ===================== CORE LOGIC ===================== */

    private TradeIdea scan(String s,
                           List<TradingCore.Candle> m15,
                           List<TradingCore.Candle> h1,
                           MarketRegime regime,
                           String type,
                           long now) {

        TradingCore.Side side =
                regime == MarketRegime.BULL ? TradingCore.Side.LONG : TradingCore.Side.SHORT;

        String key = s + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < COOLDOWN_MS) return null;

        if (!structureConfirmed(m15, side)) return null;
        if (isExhaustion(m15)) return null;

        double atr = atr(m15, 14);
        double entry = last(m15).close;
        double risk = atr * STOP_ATR;

        double stop = side == TradingCore.Side.LONG ? entry - risk : entry + risk;
        double take = side == TradingCore.Side.LONG
                ? entry + risk * RR_A
                : entry - risk * RR_A;

        double confidence = computeConfidence(m15, h1, regime, type);

        SignalGrade grade = confidence >= 0.68 ? SignalGrade.A : SignalGrade.B;
        if (grade == SignalGrade.B) {
            take = side == TradingCore.Side.LONG
                    ? entry + risk * RR_B
                    : entry - risk * RR_B;
        }

        cooldown.put(key, now);

        return new TradeIdea(
                s,
                side,
                entry,
                stop,
                take,
                confidence,
                atr,
                grade,
                "HTF " + regime + " | STRUCTURE ENTRY",
                type
        );
    }

    /* ===================== MARKET REGIME ===================== */

    private enum MarketRegime { BULL, BEAR, RANGE }

    private MarketRegime detectRegime(List<TradingCore.Candle> h1) {
        double e50 = ema(h1, 50);
        double e200 = ema(h1, 200);
        double price = last(h1).close;

        if (price > e50 && e50 > e200) return MarketRegime.BULL;
        if (price < e50 && e50 < e200) return MarketRegime.BEAR;
        return MarketRegime.RANGE;
    }

    /* ===================== FILTERS ===================== */

    private boolean structureConfirmed(List<TradingCore.Candle> c, TradingCore.Side side) {
        TradingCore.Candle a = c.get(c.size() - 3);
        TradingCore.Candle b = c.get(c.size() - 2);
        TradingCore.Candle d = c.get(c.size() - 1);

        if (side == TradingCore.Side.LONG)
            return d.close > b.high && b.low > a.low;
        else
            return d.close < b.low && b.high < a.high;
    }

    private boolean isExhaustion(List<TradingCore.Candle> c) {
        TradingCore.Candle k = last(c);
        double atr = atr(c, 14);
        double range = k.high - k.low;
        return range > atr * 1.8;
    }

    /* ===================== CONFIDENCE ===================== */

    private double computeConfidence(List<TradingCore.Candle> m15,
                                     List<TradingCore.Candle> h1,
                                     MarketRegime regime,
                                     String type) {

        double conf = 0.50;

        conf += structureStrength(m15) * 0.15;
        conf += trendStrength(h1) * 0.20;

        if ("TOP".equals(type)) conf += 0.03;
        if ("MEME".equals(type)) conf -= 0.03;

        return clamp(conf, 0.45, 0.85);
    }

    private double structureStrength(List<TradingCore.Candle> c) {
        TradingCore.Candle k = last(c);
        double body = Math.abs(k.close - k.open);
        double range = k.high - k.low;
        return range == 0 ? 0 : body / range;
    }

    private double trendStrength(List<TradingCore.Candle> c) {
        double e50 = ema(c, 50);
        double e200 = ema(c, 200);
        return Math.min(1.0, Math.abs(e50 - e200) / e200 * 5);
    }

    /* ===================== MATH ===================== */

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> c, int n) {
        return c != null && c.size() >= n;
    }

    private double atr(List<TradingCore.Candle> c, int n) {
        double s = 0;
        for (int i = c.size() - n; i < c.size(); i++)
            s += c.get(i).high - c.get(i).low;
        return s / n;
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
}
