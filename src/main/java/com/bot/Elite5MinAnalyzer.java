package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class Elite5MinAnalyzer {

    /* ================= CONFIG ================= */
    private static final int MIN_M15 = 150;       // минимальные свечи для анализа
    private static final int MIN_H1 = 150;
    private static final int MAX_SYMBOLS = 70;    // топ-70 монет
    private static final double MIN_CONFIDENCE = 0.50; // минимальная уверенность сигнала
    private static final double MIN_ATR_PCT = 0.0015;  // минимальная волатильность для сигнала
    private static final long BASE_COOLDOWN = 10 * 60_000; // кулдаун 10 минут

    private final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    /* ================= OUTPUT ================= */
    public static final class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final String reason;
        public final String grade;

        public TradeSignal(String symbol, TradingCore.Side side, double entry, double stop, double take,
                           double confidence, String reason, String grade) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.reason = reason;
            this.grade = grade;
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
            if (bias == null) bias = TradingCore.Side.LONG; // смягчённый вариант

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
        if (earlyBreakout(m15, bias) && adx > 14 && !overextendedMove(m15)) {
            side = bias;
            reason = "Early Breakout";
        }

        if (side == null && adx > 16 && pullbackZone(m15, bias) && !overextendedMove(m15)) {
            side = bias;
            reason = "Trend Pullback";
        }

        if (side == null) {
            if (bullishDivergence(m15) && rsi < 45 && bias == TradingCore.Side.LONG) {
                side = TradingCore.Side.LONG;
                reason = "Bullish Divergence";
            } else if (bearishDivergence(m15) && rsi > 55 && bias == TradingCore.Side.SHORT) {
                side = TradingCore.Side.SHORT;
                reason = "Bearish Divergence";
            }
        }

        if (side == null) {
            double high = highest(m15, 15);
            double low = lowest(m15, 15);
            if (bias == TradingCore.Side.LONG && price <= low * 1.005) {
                side = TradingCore.Side.LONG;
                reason = "Range Support";
            } else if (bias == TradingCore.Side.SHORT && price >= high * 0.995) {
                side = TradingCore.Side.SHORT;
                reason = "Range Resistance";
            }
        }

        if (side == null) return null;

        // ================= COOLDOWN =================
        String key = symbol + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < BASE_COOLDOWN) return null;

        // ================= ALIGNMENT =================
        if (!isAlignedWithHTF(side, h1)) return null;

        // ================= CONFIDENCE =================
        double confidence = calculateConfidence(m15, adx, rsi, vol, bias);
        String grade = confidence > 0.75 ? "A" : confidence > 0.62 ? "B" : "C";
        if (grade.equals("C") && confidence < MIN_CONFIDENCE) return null;

        // ================= RISK / REWARD =================
        double risk = atr * 1.0;
        double rr = confidence > 0.72 ? 2.8 : 2.2;
        double stop = side == TradingCore.Side.LONG ? price - risk : price + risk;
        double take = side == TradingCore.Side.LONG ? price + risk * rr : price - risk * rr;

        cooldown.put(key, now);
        return new TradeSignal(symbol, side, price, stop, take, confidence, reason, grade);
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
        return side == TradingCore.Side.LONG ? last.close > prev.high * 1.002 : last.close < prev.low * 0.998;
    }

    private boolean overextendedMove(List<TradingCore.Candle> c) {
        int n = c.size();
        int strong = 0;
        for (int i = n - 3; i < n - 1; i++) {
            double body = Math.abs(c.get(i + 1).close - c.get(i + 1).open);
            double range = c.get(i + 1).high - c.get(i + 1).low;
            if (range > 0 && body / range > 0.65) strong++;
        }
        return strong >= 2;
    }

    private boolean pullbackZone(List<TradingCore.Candle> c, TradingCore.Side side) {
        double ema21 = ema(c, 21);
        double price = last(c).close;
        return side == TradingCore.Side.LONG ? price <= ema21 * 1.012 : price >= ema21 * 0.988;
    }

    private boolean bullishDivergence(List<TradingCore.Candle> c) {
        int n = c.size();
        if (n < 20) return false;
        double low1 = c.get(n - 3).low, low2 = c.get(n - 1).low;
        double rsi1 = rsi(c.subList(0, n - 2), 14), rsi2 = rsi(c, 14);
        return low2 < low1 && rsi2 > rsi1;
    }

    private boolean bearishDivergence(List<TradingCore.Candle> c) {
        int n = c.size();
        if (n < 20) return false;
        double high1 = c.get(n - 3).high, high2 = c.get(n - 1).high;
        double rsi1 = rsi(c.subList(0, n - 2), 14), rsi2 = rsi(c, 14);
        return high2 > high1 && rsi2 < rsi1;
    }

    /* ================= CONFIDENCE ================= */
    private double calculateConfidence(List<TradingCore.Candle> m15, double adx, double rsi, double vol, TradingCore.Side bias) {
        double structure = Math.abs(ema(m15, 21) - ema(m15, 50)) / ema(m15, 50);
        double base = 0.6 + structure * 0.15 + (adx / 50.0) * 0.1 + Math.min((vol - 1) * 0.05, 0.08);
        return clamp(base, MIN_CONFIDENCE, 0.95);
    }

    /* ================= INDICATORS ================= */
    private double atr(List<TradingCore.Candle> c, int p) {
        double sum = 0;
        for (int i = c.size() - p; i < c.size(); i++) sum += c.get(i).high - c.get(i).low;
        return sum / p;
    }

    private double adx(List<TradingCore.Candle> c, int p) {
        double move = 0;
        for (int i = c.size() - p; i < c.size() - 1; i++) move += Math.abs(c.get(i + 1).close - c.get(i).close);
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
        for (int i = c.size() - p + 1; i < c.size(); i++) e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private double highest(List<TradingCore.Candle> c, int n) {
        return c.subList(c.size() - n, c.size()).stream().mapToDouble(cd -> cd.high).max().orElse(0);
    }

    private double lowest(List<TradingCore.Candle> c, int n) {
        return c.subList(c.size() - n, c.size()).stream().mapToDouble(cd -> cd.low).min().orElse(0);
    }

    private double relativeVolume(List<TradingCore.Candle> c) {
        int n = c.size();
        double avg = c.subList(Math.max(0, n - 20), n - 1).stream().mapToDouble(cd -> cd.volume).average().orElse(1);
        return last(c).volume / avg;
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

    /* ================= UTILS ================= */
    private static boolean valid(List<?> l, int min) { return l != null && l.size() >= min; }
    private TradingCore.Candle last(List<TradingCore.Candle> c) { return c.get(c.size() - 1); }
    private double clamp(double v, double min, double max) { return Math.max(min, Math.min(max, v)); }
}