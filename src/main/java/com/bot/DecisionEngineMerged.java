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

    private static final int MIN_BARS = 180; // чуть меньше, чтобы ловились новые монеты
    private static final double RR_A = 2.2;
    private static final double RR_B = 1.5;

    private static final long COOLDOWN_MS = 15 * 60_000; // более частые сигналы
    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

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

            MarketRegime regime = detectRegime(c1h);
            // Убрали жесткую фильтрацию RANGE, теперь генерируем и в боковиках
            TradeIdea idea = scan(symbol, c15, c1h,
                    regime,
                    coinTypes.getOrDefault(symbol, "ALT"),
                    now);

            if (idea != null)
                result.add(idea);
        }

        result.sort(Comparator.comparingDouble((TradeIdea t) -> t.confidence).reversed());
        return result;
    }

    /* ===================== CORE ===================== */

    private TradeIdea scan(String s,
                           List<TradingCore.Candle> m15,
                           List<TradingCore.Candle> h1,
                           MarketRegime regime,
                           String type,
                           long now) {

        TradingCore.Side side =
                (regime == MarketRegime.BEAR) ? TradingCore.Side.SHORT : TradingCore.Side.LONG;

        String key = s + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < COOLDOWN_MS)
            return null;

        if (!structureConfirmed(m15, side))
            return null;

        double atr = trueATR(m15, 14);
        double adx = adx(m15, 14);

        double entry = last(m15).close;

        // 🔥 динамический стоп и риск
        double riskMultiplier = (adx > 20) ? 0.65 : 0.50;
        double risk = atr * riskMultiplier;

        double stop = (side == TradingCore.Side.LONG) ? entry - risk : entry + risk;

        double confidence = computeConfidence(m15, h1, regime, type, adx);

        SignalGrade grade = confidence >= 0.68 ? SignalGrade.A : SignalGrade.B;

        double rr = (grade == SignalGrade.A) ? RR_A : RR_B;
        double take = (side == TradingCore.Side.LONG) ? entry + risk * rr : entry - risk * rr;

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
                "REGIME " + regime + " | TREND ENTRY",
                type
        );
    }

    /* ===================== REGIME ===================== */

    private enum MarketRegime { BULL, BEAR, RANGE }

    private MarketRegime detectRegime(List<TradingCore.Candle> h1) {
        double e50 = ema(h1, 50);
        double e200 = ema(h1, 200);
        double price = last(h1).close;
        double adx = adx(h1, 14);

        if (price > e50 && e50 > e200) return MarketRegime.BULL;
        if (price < e50 && e50 < e200) return MarketRegime.BEAR;

        return MarketRegime.RANGE;
    }

    private double structureStrength(List<TradingCore.Candle> c) {
        TradingCore.Candle last = last(c);
        TradingCore.Candle prev = c.get(c.size() - 2);

        double body = Math.abs(last.close - last.open);
        double range = last.high - last.low;
        if (range == 0) return 0;

        double bodyPower = body / range;
        double momentum = Math.abs(last.close - prev.close) / range;

        return clamp((bodyPower * 0.6 + momentum * 0.4), 0.0, 1.0);
    }

    /* ===================== CONFIDENCE ===================== */

    private double computeConfidence(List<TradingCore.Candle> m15,
                                     List<TradingCore.Candle> h1,
                                     MarketRegime regime,
                                     String type,
                                     double adx) {
        double conf = 0.50;
        conf += structureStrength(m15) * 0.15;
        conf += trendAcceleration(h1) * 0.20;
        conf += Math.min(0.10, adx / 100.0);

        if ("TOP".equals(type)) conf += 0.03;
        if ("MEME".equals(type)) conf -= 0.04;

        return clamp(conf, 0.45, 0.92);
    }

    /* ===================== FILTERS ===================== */

    private boolean structureConfirmed(List<TradingCore.Candle> c, TradingCore.Side side) {
        TradingCore.Candle a = c.get(c.size() - 3);
        TradingCore.Candle b = c.get(c.size() - 2);
        TradingCore.Candle d = c.get(c.size() - 1);

        if (side == TradingCore.Side.LONG)
            return d.close > b.high && b.low > a.low;

        return d.close < b.low && b.high < a.high;
    }

    /* ===================== INDICATORS ===================== */

    private double trueATR(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++) {
            TradingCore.Candle cur = c.get(i);
            TradingCore.Candle prev = c.get(i - 1);
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close), Math.abs(cur.low - prev.close)));
            sum += tr;
        }
        return sum / n;
    }

    private double adx(List<TradingCore.Candle> c, int n) {
        double move = 0;
        for (int i = c.size() - n; i < c.size() - 1; i++)
            move += Math.abs(c.get(i + 1).close - c.get(i).close);
        return (move / n) / trueATR(c, n) * 25.0;
    }

    private double trendAcceleration(List<TradingCore.Candle> c) {
        double e21 = ema(c, 21);
        double e50 = ema(c, 50);
        return Math.min(1.0, Math.abs(e21 - e50) / e50 * 6);
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

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}
