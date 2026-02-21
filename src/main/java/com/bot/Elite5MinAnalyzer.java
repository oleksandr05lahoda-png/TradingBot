package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class Elite5MinAnalyzer {

    /* ================= CONFIG ================= */
    private static final int MIN_M15 = 150;
    private static final int MIN_H1  = 200;
    private static final int MAX_SYMBOLS = 70;

    private static final double MIN_CONFIDENCE = 0.55;
    private static final double MIN_ATR_PCT = 0.0015;
    private static final long BASE_COOLDOWN = 10 * 60_000;

    private final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    /* ================= OUTPUT ================= */
    public static final class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final String reason;
        public final String grade;

        public TradeSignal(String symbol,
                           TradingCore.Side side,
                           double entry,
                           double stop,
                           double take,
                           double confidence,
                           String reason,
                           String grade) {
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

    /* ================= MAIN ================= */
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

    /* ================= SIGNAL BUILD ================= */
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

        /* ========= LONG ========= */
        if (bias == TradingCore.Side.LONG) {

            if (earlyBreakout(m15, TradingCore.Side.LONG) && adx > 14 && !overextendedMove(m15)) {
                side = TradingCore.Side.LONG;
                reason = "Early Breakout";
            }

            if (side == null && adx > 16 && pullbackZone(m15, TradingCore.Side.LONG) && !overextendedMove(m15)) {
                side = TradingCore.Side.LONG;
                reason = "Trend Pullback";
            }

            if (side == null && bullishDivergence(m15) && rsi < 42) {
                side = TradingCore.Side.LONG;
                reason = "Bullish Divergence";
            }
        }

        /* ========= SHORT (без агрессивных контртрендов) ========= */
        if (bias == TradingCore.Side.SHORT) {

            if (rsi > 50 && price < ema(m15, 21) && !bullishExhaustion(m15)) {

                if (earlyBreakout(m15, TradingCore.Side.SHORT) && adx > 16) {
                    side = TradingCore.Side.SHORT;
                    reason = "Early Breakdown";
                }

                if (side == null && pullbackZone(m15, TradingCore.Side.SHORT) && adx > 18) {
                    side = TradingCore.Side.SHORT;
                    reason = "Bear Pullback";
                }

                if (side == null && bearishDivergence(m15) && rsi > 55) {
                    side = TradingCore.Side.SHORT;
                    reason = "Bearish Divergence";
                }
            }
        }

        if (side == null) return null;

        /* ========= COOLDOWN ========= */
        String key = symbol + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < BASE_COOLDOWN)
            return null;

        /* ========= HTF ALIGN ========= */
        if (!isAlignedWithHTF(side, h1)) return null;

        /* ========= CONFIDENCE ========= */
        double confidence = calculateConfidence(m15, adx, rsi, vol);
        if (confidence < MIN_CONFIDENCE) return null;

        String grade = confidence > 0.75 ? "A" : confidence > 0.62 ? "B" : "C";

        /* ========= RISK ========= */
        double risk = atr;
        double rr = confidence > 0.72 ? 2.8 : 2.2;

        double stop = side == TradingCore.Side.LONG ? price - risk : price + risk;
        double take = side == TradingCore.Side.LONG ? price + risk * rr : price - risk * rr;

        cooldown.put(key, now);

        return new TradeSignal(symbol, side, price, stop, take, confidence, reason, grade);
    }

    /* ================= ANTI EXHAUSTION ================= */
    private boolean bullishExhaustion(List<TradingCore.Candle> c) {
        int n = c.size();
        int strongGreen = 0;

        for (int i = n - 3; i < n; i++) {
            double body = c.get(i).close - c.get(i).open;
            double range = c.get(i).high - c.get(i).low;
            if (body > 0 && range > 0 && body / range > 0.6)
                strongGreen++;
        }
        return strongGreen >= 2;
    }

    private boolean overextendedMove(List<TradingCore.Candle> c) {
        double atr = atr(c, 14);
        double move = Math.abs(last(c).close - c.get(c.size() - 4).close);
        return move > atr * 2.5;
    }

    /* ================= HTF ================= */
    private TradingCore.Side detectHTFBias(List<TradingCore.Candle> c) {
        double e50 = ema(c, 50);
        double e200 = ema(c, 200);
        double price = last(c).close;

        if (price > e50 && e50 > e200)
            return TradingCore.Side.LONG;

        if (price < e50 && e50 < e200 && rsi(c, 14) < 48)
            return TradingCore.Side.SHORT;

        return null;
    }

    private boolean isAlignedWithHTF(TradingCore.Side side, List<TradingCore.Candle> h1) {
        double ema50 = ema(h1, 50);
        double ema200 = ema(h1, 200);
        return side == TradingCore.Side.LONG ? ema50 > ema200 : ema50 < ema200;
    }

    /* ================= INDICATORS ================= */

    private double atr(List<TradingCore.Candle> c, int p) {
        if (c.size() < p + 1) return 0;
        double sum = 0;
        for (int i = c.size() - p; i < c.size(); i++)
            sum += (c.get(i).high - c.get(i).low);
        return sum / p;
    }

    private double adx(List<TradingCore.Candle> c, int p) {
        if (c.size() < p + 1) return 0;
        double move = 0;
        for (int i = c.size() - p; i < c.size() - 1; i++)
            move += Math.abs(c.get(i + 1).close - c.get(i).close);
        double atr = atr(c, p);
        if (atr == 0) return 0;
        return (move / p) / atr * 25.0;
    }

    private double rsi(List<TradingCore.Candle> c, int p) {
        if (c.size() < p + 1) return 50;
        double gain = 0, loss = 0;
        for (int i = c.size() - p; i < c.size() - 1; i++) {
            double diff = c.get(i + 1).close - c.get(i).close;
            if (diff > 0) gain += diff;
            else loss -= diff;
        }
        if (loss == 0) return 100;
        return 100 - (100 / (1 + gain / loss));
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        if (c.size() < p) return last(c).close;
        double k = 2.0 / (p + 1);
        double ema = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            ema = c.get(i).close * k + ema * (1 - k);
        return ema;
    }

    private boolean earlyBreakout(List<TradingCore.Candle> c, TradingCore.Side side) {
        int n = c.size();
        TradingCore.Candle last = last(c);
        TradingCore.Candle prev = c.get(n - 2);

        return side == TradingCore.Side.LONG
                ? last.close > prev.high * 1.002
                : last.close < prev.low * 0.998;
    }

    private boolean pullbackZone(List<TradingCore.Candle> c, TradingCore.Side side) {
        double ema21 = ema(c, 21);
        double price = last(c).close;

        return side == TradingCore.Side.LONG
                ? price <= ema21 * 1.008
                : price >= ema21 * 0.992;
    }

    private boolean bullishDivergence(List<TradingCore.Candle> c) {
        int n = c.size();
        if (n < 20) return false;

        double low1 = c.get(n - 3).low;
        double low2 = c.get(n - 1).low;

        return low2 < low1 && rsi(c, 14) > rsi(c.subList(0, n - 2), 14);
    }

    private boolean bearishDivergence(List<TradingCore.Candle> c) {
        int n = c.size();
        if (n < 20) return false;

        double high1 = c.get(n - 3).high;
        double high2 = c.get(n - 1).high;

        return high2 > high1 && rsi(c, 14) < rsi(c.subList(0, n - 2), 14);
    }

    private double relativeVolume(List<TradingCore.Candle> c) {
        int n = c.size();
        if (n < 2) return 1;

        double avg = c.subList(Math.max(0, n - 20), n - 1)
                .stream()
                .mapToDouble(cd -> cd.volume)
                .average()
                .orElse(1);

        return avg == 0 ? 1 : last(c).volume / avg;
    }

    private boolean volatilityOk(List<TradingCore.Candle> c) {
        double atr = atr(c, 14);
        double price = last(c).close;
        return price > 0 && (atr / price) > MIN_ATR_PCT;
    }

    private static boolean valid(List<?> l, int min) {
        return l != null && l.size() >= min;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private double calculateConfidence(List<TradingCore.Candle> c,
                                       double adx,
                                       double rsi,
                                       double vol) {

        double structure = Math.abs(ema(c, 21) - ema(c, 50)) / ema(c, 50);

        double base = 0.6
                + structure * 0.15
                + (adx / 50.0) * 0.1
                + Math.min((vol - 1) * 0.05, 0.08);

        return Math.max(0.55, Math.min(0.95, base));
    }
}