package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class Elite5MinAnalyzer {

    /* ================= CONFIG ================= */
    private static final int MIN_M15 = 200;
    private static final int MIN_H1  = 200;
    private static final int MAX_SYMBOLS = 70;

    private static final double MIN_CONFIDENCE = 0.62;
    private static final double MIN_ATR_PCT    = 0.0020;
    private static final long BASE_COOLDOWN = 8 * 60_000;

    private final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    /* ================= OUTPUT ================= */
    public static final class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final String reason;

        public TradeSignal(String symbol,
                           TradingCore.Side side,
                           double entry,
                           double stop,
                           double take,
                           double confidence,
                           String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.reason = reason;
        }
    }

    /* ================= MAIN ANALYSIS ================= */
    public List<TradeSignal> analyze(List<String> symbols,
                                     Map<String, List<TradingCore.Candle>> m15,
                                     Map<String, List<TradingCore.Candle>> h1) {

        List<TradeSignal> result = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String symbol : symbols) {
            if (scanned++ >= MAX_SYMBOLS) break;

            List<TradingCore.Candle> tf15 = m15.get(symbol);
            List<TradingCore.Candle> tf1h = h1.get(symbol);

            if (!valid(tf15, MIN_M15) || !valid(tf1h, MIN_H1)) continue;
            if (!volatilityOk(tf15)) continue;

            TradingCore.Side bias = detectHTFBias(tf1h);
            if (bias == null) continue;

            TradeSignal signal = buildSignal(symbol, tf15, bias, tf1h, now);
            if (signal != null) result.add(signal);
        }

        result.sort(Comparator.comparingDouble((TradeSignal s) -> s.confidence).reversed());
        return result;
    }

    /* ================= SIGNAL BUILDING ================= */
    private TradeSignal buildSignal(String symbol,
                                    List<TradingCore.Candle> m15,
                                    TradingCore.Side bias,
                                    List<TradingCore.Candle> h1,
                                    long now) {

        double price = last(m15).close;
        double atr = atr(m15, 14);
        double adx = adx(m15, 14);
        double rsi = rsi(m15, 14);
        double vol = relativeVolume(m15);

        TradingCore.Side side = null;
        String reason = null;

        // ================= STRATEGIES =================
        // 1️⃣ Early Breakout
        if (earlyBreakout(m15, bias) && adx > 16 && !overextendedMove(m15)) {
            side = bias; reason = "Early Breakout";
        }

        // 2️⃣ Pullback Entry
        if (side == null && adx > 18 && pullbackZone(m15, bias) && !overextendedMove(m15)) {
            side = bias; reason = "Trend Pullback";
        }

        // 3️⃣ Reversal/Divergence
        if (side == null) {
            if (bullishDivergence(m15) && rsi < 38 && bias == TradingCore.Side.LONG) {
                side = TradingCore.Side.LONG; reason = "Bullish Divergence";
            }
            if (bearishDivergence(m15) && rsi > 62 && bias == TradingCore.Side.SHORT) {
                side = TradingCore.Side.SHORT; reason = "Bearish Divergence";
            }
        }

        if (side == null) return null;

        // ================= COOLDOWN =================
        String key = symbol + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < BASE_COOLDOWN) return null;

        // ================= ALIGNMENT CHECK =================
        if (!isAlignedWithHTF(side, h1)) return null;

        // ================= CONFIDENCE =================
        double confidence = calculateConfidence(m15, adx, rsi, vol, bias);
        if (confidence < MIN_CONFIDENCE) return null;

        // ================= RISK/REWARD =================
        double risk = atr * 1.1;
        double rr = confidence > 0.75 ? 3.0 : 2.2;
        double stop = side == TradingCore.Side.LONG ? price - risk : price + risk;
        double take = side == TradingCore.Side.LONG ? price + risk * rr : price - risk * rr;

        cooldown.put(key, now);

        return new TradeSignal(symbol, side, price, stop, take,
                clamp(confidence, 0.55, 0.95), reason);
    }

    /* ================= ALIGNMENT ================= */
    private boolean isAlignedWithHTF(TradingCore.Side side, List<TradingCore.Candle> h1) {
        if (!valid(h1, MIN_H1)) return true;
        double ema50 = ema(h1, 50);
        double ema200 = ema(h1, 200);
        return side == TradingCore.Side.LONG ? ema50 > ema200 : ema50 < ema200;
    }

    /* ================= STRATEGIES ================= */
    private boolean earlyBreakout(List<TradingCore.Candle> c, TradingCore.Side side) {
        int n = c.size();
        TradingCore.Candle last = last(c);
        TradingCore.Candle prev = c.get(n - 2);
        return side == TradingCore.Side.LONG ? last.close > prev.high : last.close < prev.low;
    }

    private boolean overextendedMove(List<TradingCore.Candle> c) {
        int n = c.size();
        int strong = 0;
        for (int i = n - 4; i < n - 1; i++) {
            double body = Math.abs(c.get(i + 1).close - c.get(i + 1).open);
            double range = c.get(i + 1).high - c.get(i + 1).low;
            if (range > 0 && body / range > 0.7) strong++;
        }
        return strong >= 3;
    }

    private boolean pullbackZone(List<TradingCore.Candle> c, TradingCore.Side side) {
        double ema21 = ema(c, 21);
        double price = last(c).close;
        return side == TradingCore.Side.LONG ? price <= ema21 * 1.006 : price >= ema21 * 0.994;
    }

    private boolean bullishDivergence(List<TradingCore.Candle> c) {
        int n = c.size(); if (n < 20) return false;
        double low1 = c.get(n - 3).low;
        double low2 = c.get(n - 1).low;
        double rsi1 = rsi(c.subList(0, n - 2), 14);
        double rsi2 = rsi(c, 14);
        return low2 < low1 && rsi2 > rsi1;
    }

    private boolean bearishDivergence(List<TradingCore.Candle> c) {
        int n = c.size(); if (n < 20) return false;
        double high1 = c.get(n - 3).high;
        double high2 = c.get(n - 1).high;
        double rsi1 = rsi(c.subList(0, n - 2), 14);
        double rsi2 = rsi(c, 14);
        return high2 > high1 && rsi2 < rsi1;
    }

    /* ================= CONFIDENCE ================= */
    private double calculateConfidence(List<TradingCore.Candle> m15,
                                       double adx,
                                       double rsi,
                                       double vol,
                                       TradingCore.Side bias) {
        double trend = trendStrength(m15);
        double momentum = momentumScore(m15);
        double conf = 0.58 + trend * 0.2 + momentum * 0.15 + (adx / 40.0) * 0.12 + (vol - 1) * 0.05;
        if (bias == TradingCore.Side.LONG && trend > 0.6) conf += 0.05;
        if (bias == TradingCore.Side.SHORT && trend > 0.6) conf += 0.05;
        return clamp(conf, 0.55, 0.95);
    }

    /* ================= INDICATORS ================= */
    private double atr(List<TradingCore.Candle> c, int p) {
        double sum = 0;
        for (int i = c.size() - p; i < c.size(); i++)
            sum += c.get(i).high - c.get(i).low;
        return sum / p;
    }

    private double adx(List<TradingCore.Candle> c, int p) {
        double move = 0;
        for (int i = c.size() - p; i < c.size() - 1; i++)
            move += Math.abs(c.get(i + 1).close - c.get(i).close);
        return (move / p) / atr(c, p) * 25.0;
    }

    private double rsi(List<TradingCore.Candle> c, int p) {
        double gain = 0, loss = 0;
        for (int i = c.size() - p; i < c.size() - 1; i++) {
            double diff = c.get(i + 1).close - c.get(i).close;
            if (diff > 0) gain += diff; else loss -= diff;
        }
        if (loss == 0) return 100;
        return 100 - (100 / (1 + gain / loss));
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private double trendStrength(List<TradingCore.Candle> c) {
        double e21 = ema(c, 21);
        double e50 = ema(c, 50);
        return clamp(Math.abs(e21 - e50) / e50 * 6, 0, 1);
    }

    private double momentumScore(List<TradingCore.Candle> c) {
        double move = 0;
        for (int i = c.size() - 6; i < c.size() - 1; i++)
            move += Math.abs(c.get(i + 1).close - c.get(i).close);
        return clamp(move / 6.0 / atr(c, 14), 0, 1);
    }

    private double relativeVolume(List<TradingCore.Candle> c) {
        int n = c.size();
        double avg = c.subList(n - 20, n - 1).stream().mapToDouble(cd -> cd.volume).average().orElse(0);
        return avg == 0 ? 1 : last(c).volume / avg;
    }

    private boolean volatilityOk(List<TradingCore.Candle> c) {
        double a = atr(c, 14);
        double price = last(c).close;
        return (a / price) > MIN_ATR_PCT;
    }

    private TradingCore.Side detectHTFBias(List<TradingCore.Candle> c) {
        double e50 = ema(c, 50);
        double e200 = ema(c, 200);
        double price = last(c).close;
        if (price > e50 && e50 > e200) return TradingCore.Side.LONG;
        if (price < e50 && e50 < e200) return TradingCore.Side.SHORT;
        return null;
    }

    private static boolean valid(List<?> l, int min) {
        return l != null && l.size() >= min;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}