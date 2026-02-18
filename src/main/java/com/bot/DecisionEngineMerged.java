package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    /* ===================== MODELS ===================== */

    public enum SignalGrade { A, B }

    private enum Regime { COMPRESSION, EXPANSION, TREND, EXHAUSTION }

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
    private static final double RR_A = 2.6;
    private static final double RR_B = 1.8;
    private static final long COOLDOWN_MS = 60 * 60_000; // один сигнал в час

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

            if (!valid(c15, MIN_BARS) || !valid(c1h, MIN_BARS)) continue;

            Regime regime = detectRegime(c15);
            HTFBias bias = detectHTFBias(c1h);

            TradeIdea idea = generateIdea(symbol, c15, regime, bias,
                    coinTypes.getOrDefault(symbol, "ALT"), now);

            if (idea != null) result.add(idea);
        }

        result.sort(Comparator.comparingDouble((TradeIdea t) -> t.confidence).reversed());
        return result;
    }

    /* ===================== CORE ===================== */

    private TradeIdea generateIdea(String symbol,
                                   List<TradingCore.Candle> c15,
                                   Regime regime,
                                   HTFBias bias,
                                   String type,
                                   long now) {

        double atr = trueATR(c15, 14);
        double entry = last(c15).close;

        TradingCore.Side side = null;
        String reason = null;

        switch (regime) {
            case COMPRESSION -> {
                if (breakoutUp(c15)) { side = TradingCore.Side.LONG; reason = "COMPRESSION BREAKOUT UP"; }
                else if (breakoutDown(c15)) { side = TradingCore.Side.SHORT; reason = "COMPRESSION BREAKOUT DOWN"; }
            }
            case TREND -> {
                if (trendContinuationLong(c15)) side = TradingCore.Side.LONG;
                else if (trendContinuationShort(c15)) side = TradingCore.Side.SHORT;
                if (side != null) reason = "TREND CONTINUATION";
            }
            case EXHAUSTION -> {
                if (reversalSignal(c15, TradingCore.Side.LONG)) side = TradingCore.Side.LONG;
                else if (reversalSignal(c15, TradingCore.Side.SHORT)) side = TradingCore.Side.SHORT;
                if (side != null) reason = "REVERSAL SETUP";
            }
            case EXPANSION -> {
                // Простой ранний вход при импульсе
                if (trendContinuationLong(c15)) side = TradingCore.Side.LONG;
                else if (trendContinuationShort(c15)) side = TradingCore.Side.SHORT;
                if (side != null) reason = "EXPANSION EARLY ENTRY";
            }
        }

        if (side == null) return null;
        if (!htfAligned(side, bias)) return null;

        String key = symbol + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < COOLDOWN_MS) return null;

        double confidence = computeProfessionalConfidence(c15, regime, type);
        double risk = atr * riskMultiplier(type);

        double stop = side == TradingCore.Side.LONG ? entry - risk : entry + risk;
        double rr = confidence > 0.72 ? RR_A : RR_B;
        double take = side == TradingCore.Side.LONG ? entry + risk * rr : entry - risk * rr;

        cooldown.put(key, now);

        return new TradeIdea(symbol, side, entry, stop, take, confidence,
                atr, confidence > 0.72 ? SignalGrade.A : SignalGrade.B,
                reason, type);
    }

    /* ===================== REGIME ===================== */

    private Regime detectRegime(List<TradingCore.Candle> c) {
        double atr = trueATR(c, 14);
        double prevAtr = trueATR(c.subList(0, c.size() - 1), 14);
        boolean volumeSpike = volumeSpike(c);

        if (atr < prevAtr * 0.85) return Regime.COMPRESSION;
        if (volumeSpike && atr > prevAtr * 1.1) return Regime.EXPANSION;
        if (trendStrength(c) > 0.7) return Regime.TREND;
        return Regime.EXHAUSTION;
    }

    /* ===================== STRATEGIES ===================== */

    private boolean breakoutUp(List<TradingCore.Candle> c) {
        double high = highestHigh(c, 12);
        return last(c).close > high;
    }

    private boolean breakoutDown(List<TradingCore.Candle> c) {
        double low = lowestLow(c, 12);
        return last(c).close < low;
    }

    private boolean trendContinuationLong(List<TradingCore.Candle> c) {
        return ema(c, 21) > ema(c, 50) && last(c).close > ema(c, 21);
    }

    private boolean trendContinuationShort(List<TradingCore.Candle> c) {
        return ema(c, 21) < ema(c, 50) && last(c).close < ema(c, 21);
    }

    private boolean reversalSignal(List<TradingCore.Candle> c, TradingCore.Side side) {
        TradingCore.Candle last = last(c);
        double body = Math.abs(last.close - last.open);
        double range = last.high - last.low;
        if (range == 0) return false;
        double exhaustion = body / range;
        return side == TradingCore.Side.LONG
                ? exhaustion < 0.25 && last.close > last.open
                : exhaustion < 0.25 && last.close < last.open;
    }

    /* ===================== CONFIDENCE ===================== */

    private double computeProfessionalConfidence(List<TradingCore.Candle> c, Regime regime, String type) {
        double momentum = trendStrength(c);
        double volatility = trueATR(c, 14) / last(c).close * 40;

        double base = switch (regime) {
            case COMPRESSION -> 0.62;
            case EXPANSION -> 0.75;
            case TREND -> 0.70;
            case EXHAUSTION -> 0.58;
        };

        double conf = base + momentum * 0.15 + volatility * 0.08;
        if ("TOP".equals(type)) conf += 0.02;
        if ("MEME".equals(type)) conf += 0.04;

        // ADX фильтр для точности сигналов
        double adxValue = adx(c, 14);
        conf *= (adxValue > 20 ? 1.0 : 0.85);

        return clamp(conf, 0.50, 0.92);
    }

    /* ===================== HELPERS ===================== */

    private boolean htfAligned(TradingCore.Side side, HTFBias bias) {
        if (bias == HTFBias.NONE) return true;
        return (bias == HTFBias.BULL && side == TradingCore.Side.LONG)
                || (bias == HTFBias.BEAR && side == TradingCore.Side.SHORT);
    }

    private double riskMultiplier(String type) {
        return switch (type) {
            case "TOP" -> 0.9;
            case "ALT" -> 1.2;
            case "MEME" -> 1.6;
            default -> 1.2;
        };
    }

    private enum HTFBias { BULL, BEAR, NONE }

    private HTFBias detectHTFBias(List<TradingCore.Candle> c) {
        double ema50 = ema(c, 50);
        double ema200 = ema(c, 200);
        double price = last(c).close;
        if (price > ema50 && ema50 > ema200) return HTFBias.BULL;
        if (price < ema50 && ema50 < ema200) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    private double highestHigh(List<TradingCore.Candle> c, int n) {
        return c.subList(c.size() - n, c.size()).stream().mapToDouble(cd -> cd.high).max().orElse(0);
    }

    private double lowestLow(List<TradingCore.Candle> c, int n) {
        return c.subList(c.size() - n, c.size()).stream().mapToDouble(cd -> cd.low).min().orElse(0);
    }

    private boolean volumeSpike(List<TradingCore.Candle> c) {
        int n = c.size();
        double avg = c.subList(n - 20, n - 1).stream().mapToDouble(cd -> cd.volume).average().orElse(0);
        return last(c).volume > avg * 1.5;
    }

    private double trendStrength(List<TradingCore.Candle> c) {
        double e21 = ema(c, 21);
        double e50 = ema(c, 50);
        return clamp(Math.abs(e21 - e50) / e50 * 8, 0, 1);
    }

    private double trueATR(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++) {
            TradingCore.Candle cur = c.get(i);
            TradingCore.Candle prev = c.get(i - 1);
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));
            sum += tr;
        }
        return sum / n;
    }

    private double adx(List<TradingCore.Candle> c, int n) {
        double move = 0;
        for (int i = c.size() - n; i < c.size() - 1; i++) {
            move += Math.abs(c.get(i + 1).close - c.get(i).close);
        }
        return move / n / trueATR(c, n) * 25.0;
    }

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
