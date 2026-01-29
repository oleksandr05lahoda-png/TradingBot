package com.bot;

import java.util.*;
import java.util.concurrent.*;

public class DecisionEngineMerged {

    /* ========================== MODEL ========================== */
    public static class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, tp1, tp2;
        public final double probability, atr;
        public final String reason;

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double entry,
                         double stop,
                         double tp1,
                         double tp2,
                         double probability,
                         double atr,
                         String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.tp1 = tp1;
            this.tp2 = tp2;
            this.probability = probability;
            this.atr = atr;
            this.reason = reason;
        }
    }

    /* ========================== CONSTANTS ========================== */
    private static final long COOLDOWN_MS = 5 * 60_000; // 5 минут
    private static final int MAX_COINS = 100;

    /* ========================== ANTI-DUPLICATION ========================== */
    private final Map<String, Long> lastLongSignal = new ConcurrentHashMap<>();
    private final Map<String, Long> lastShortSignal = new ConcurrentHashMap<>();

    /* ========================== MAIN ========================== */
    public List<TradeIdea> evaluateAll(List<String> symbols,
                                       Map<String, List<TradingCore.Candle>> candles5m,
                                       Map<String, List<TradingCore.Candle>> candles15m,
                                       Map<String, List<TradingCore.Candle>> candles1h) {

        List<TradeIdea> allIdeas = new ArrayList<>();
        long now = System.currentTimeMillis();

        int count = 0;
        for (String symbol : symbols) {
            if (count >= MAX_COINS) break;
            List<TradingCore.Candle> c5 = candles5m.get(symbol);
            List<TradingCore.Candle> c15 = candles15m.get(symbol);
            List<TradingCore.Candle> c1h = candles1h.get(symbol);

            if (!valid(c5, 120) || !valid(c15, 120) || !valid(c1h, 120)) continue;
            double atr = SignalSender.atr(c5, 14);
            if (atr <= 0) continue;

            MarketMode mode = marketMode(c1h, c15, c5, atr);
            EMA ema5 = emaContext(c5);
            EMA ema15 = emaContext(c15);
            EMA ema1h = emaContext(c1h);
            double minProb = adaptiveThreshold(mode);

            // ===== TREND & PULLBACK =====
            generateTrendPullback(symbol, c5, ema5, ema15, ema1h, mode, atr, allIdeas, now, minProb);

            // ===== MOMENTUM / IMPULSE =====
            generateMomentum(symbol, c5, ema5, ema15, atr, allIdeas, now, minProb);

            // ===== RANGE FADE =====
            generateRange(symbol, c5, atr, mode, allIdeas, now, minProb);

            // ===== REVERSAL / LIQUIDITY SWEEP =====
            generateReversal(symbol, c5, atr, mode, allIdeas, now, minProb);

            count++;
        }

        allIdeas.sort((a, b) -> Double.compare(b.probability, a.probability));
        return allIdeas;
    }

    /* ========================== TREND & PULLBACK ========================== */
    private void generateTrendPullback(String symbol, List<TradingCore.Candle> c5, EMA ema5, EMA ema15, EMA ema1h,
                                       MarketMode mode, double atr, List<TradeIdea> ideas, long now, double minProb) {

        // TREND SIGNALS – редкие
        if (ema5.bullish && ema15.bullish && ema1h.bullish && !recentCooldown(lastLongSignal, symbol, now)) {
            double score = scoreTrend(c5, atr, true, mode);
            if (score >= 0.60) {
                ideas.add(scored(buildLong(symbol, last(c5).close, atr), score, "TREND LONG"));
                lastLongSignal.put(symbol, now);
            }
        }
        if (ema5.bearish && ema15.bearish && ema1h.bearish && !recentCooldown(lastShortSignal, symbol, now)) {
            double score = scoreTrend(c5, atr, false, mode);
            if (score >= 0.60) {
                ideas.add(scored(buildShort(symbol, last(c5).close, atr), score, "TREND SHORT"));
                lastShortSignal.put(symbol, now);
            }
        }

        // PULLBACK – частые сигналы
        if (pullbackLong(c5, ema5) && ema15.bullish && !recentCooldown(lastLongSignal, symbol, now)) {
            double score = scorePullback(c5, atr);
            if (score >= minProb) {
                ideas.add(scored(buildLong(symbol, last(c5).close, atr), score, "PULLBACK LONG"));
                lastLongSignal.put(symbol, now);
            }
        }
        if (pullbackShort(c5, ema5) && ema15.bearish && !recentCooldown(lastShortSignal, symbol, now)) {
            double score = scorePullback(c5, atr);
            if (score >= minProb) {
                ideas.add(scored(buildShort(symbol, last(c5).close, atr), score, "PULLBACK SHORT"));
                lastShortSignal.put(symbol, now);
            }
        }
    }

    /* ========================== MOMENTUM ========================== */
    private void generateMomentum(String symbol, List<TradingCore.Candle> c5, EMA ema5, EMA ema15,
                                  double atr, List<TradeIdea> ideas, long now, double minProb) {

        if (!impulseBreakout(c5, atr)) return;

        if (ema5.bullish && ema15.bullish && !recentCooldown(lastLongSignal, symbol, now)) {
            double score = scoreMomentum(c5, atr);
            if (score >= minProb) {
                ideas.add(scored(buildLong(symbol, last(c5).close, atr), score, "MOMENTUM LONG"));
                lastLongSignal.put(symbol, now);
            }
        }
        if (ema5.bearish && ema15.bearish && !recentCooldown(lastShortSignal, symbol, now)) {
            double score = scoreMomentum(c5, atr);
            if (score >= minProb) {
                ideas.add(scored(buildShort(symbol, last(c5).close, atr), score, "MOMENTUM SHORT"));
                lastShortSignal.put(symbol, now);
            }
        }
    }

    /* ========================== RANGE ========================== */
    private void generateRange(String symbol, List<TradingCore.Candle> c5, double atr, MarketMode mode,
                               List<TradeIdea> ideas, long now, double minProb) {

        if (mode != MarketMode.RANGE) return;

        if (rangeFadeShort(c5, atr) && !recentCooldown(lastShortSignal, symbol, now)) {
            double score = scoreRange(c5);
            if (score >= minProb) {
                ideas.add(scored(buildShort(symbol, last(c5).close, atr), score, "RANGE FADE SHORT"));
                lastShortSignal.put(symbol, now);
            }
        }
        if (rangeFadeLong(c5, atr) && !recentCooldown(lastLongSignal, symbol, now)) {
            double score = scoreRange(c5);
            if (score >= minProb) {
                ideas.add(scored(buildLong(symbol, last(c5).close, atr), score, "RANGE FADE LONG"));
                lastLongSignal.put(symbol, now);
            }
        }
    }

    /* ========================== REVERSAL ========================== */
    private void generateReversal(String symbol, List<TradingCore.Candle> c5, double atr, MarketMode mode,
                                  List<TradeIdea> ideas, long now, double minProb) {

        if (mode != MarketMode.REVERSAL) return;

        if (sweepHigh(c5, atr) && rejectionDown(c5) && !recentCooldown(lastShortSignal, symbol, now)) {
            double score = scoreReversal(c5);
            if (score >= minProb) {
                ideas.add(scored(buildShort(symbol, last(c5).close, atr), score, "REVERSAL SHORT"));
                lastShortSignal.put(symbol, now);
            }
        }
        if (sweepLow(c5, atr) && rejectionUp(c5) && !recentCooldown(lastLongSignal, symbol, now)) {
            double score = scoreReversal(c5);
            if (score >= minProb) {
                ideas.add(scored(buildLong(symbol, last(c5).close, atr), score, "REVERSAL LONG"));
                lastLongSignal.put(symbol, now);
            }
        }
    }

    /* ========================== MARKET MODE ========================== */
    private MarketMode marketMode(List<TradingCore.Candle> c1h,
                                  List<TradingCore.Candle> c15,
                                  List<TradingCore.Candle> c5,
                                  double atr) {

        double htfMove = Math.abs(last(c1h).close - c1h.get(c1h.size() - 80).close) / atr;
        double mtfMove = Math.abs(last(c15).close - c15.get(c15.size() - 40).close) / atr;

        boolean highVol = avgRange(c5, 20) / atr > 1.4;
        boolean compress = rangeCompression(c5);

        if (htfMove > 7.0 && mtfMove < 2.0) return MarketMode.REVERSAL;
        if (compress) return MarketMode.RANGE;
        if (highVol) return MarketMode.HIGH_VOL_TREND;
        return MarketMode.TREND;
    }

    /* ========================== ADAPTIVE THRESHOLD ========================== */
    private double adaptiveThreshold(MarketMode mode) {
        return switch (mode) {
            case HIGH_VOL_TREND -> 0.50;
            case TREND -> 0.58;
            case RANGE -> 0.50;
            case REVERSAL -> 0.52;
        };
    }

    /* ========================== SCORING ========================== */
    private TradeIdea scored(TradeIdea base, double score, String reason) {
        return new TradeIdea(
                base.symbol,
                base.side,
                base.entry,
                base.stop,
                base.tp1,
                base.tp2,
                Math.min(score, 0.95),
                base.atr,
                reason + " | p=" + String.format("%.2f", score)
        );
    }

    private double scoreTrend(List<TradingCore.Candle> c, double atr, boolean up, MarketMode mode) {
        double s = 0.55;
        if (volumeBoost(c)) s += 0.06;
        if (impulse(c, atr)) s += 0.05;
        if (structureBreak(c, up)) s += 0.05;
        if (mode == MarketMode.HIGH_VOL_TREND) s += 0.03;
        return s;
    }

    private double scorePullback(List<TradingCore.Candle> c, double atr) {
        double s = 0.52;
        if (volumeDry(c)) s += 0.04;
        if (smallRange(c, atr)) s += 0.03;
        return s;
    }

    private double scoreMomentum(List<TradingCore.Candle> c, double atr) {
        double s = 0.53;
        if (volumeClimax(c)) s += 0.07;
        if (rangeExpansion(c)) s += 0.05;
        return s;
    }

    private double scoreRange(List<TradingCore.Candle> c) {
        double s = 0.55;
        if (volumeDry(c)) s += 0.03;
        return s;
    }

    private double scoreReversal(List<TradingCore.Candle> c) {
        double s = 0.54;
        if (volumeClimax(c)) s += 0.06;
        if (rangeExpansion(c)) s += 0.04;
        return s;
    }

    /* ========================== BUILD SIGNALS ========================== */
    private TradeIdea buildLong(String s, double e, double atr) {
        double r = atr * 0.55;
        return new TradeIdea(s, TradingCore.Side.LONG, e, e - r, e + r * 1.5, e + r * 2.5, 0, atr, "");
    }

    private TradeIdea buildShort(String s, double e, double atr) {
        double r = atr * 0.55;
        return new TradeIdea(s, TradingCore.Side.SHORT, e, e + r, e - r * 1.5, e - r * 2.5, 0, atr, "");
    }

    /* ========================== CONDITIONS ========================== */
    private boolean pullbackLong(List<TradingCore.Candle> c, EMA e) {
        double p = last(c).close;
        return p >= e.ema21 && p <= e.ema9;
    }

    private boolean pullbackShort(List<TradingCore.Candle> c, EMA e) {
        double p = last(c).close;
        return p <= e.ema21 && p >= e.ema9;
    }

    private boolean impulseBreakout(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 3).close) > atr * 0.9;
    }

    private boolean rangeFadeShort(List<TradingCore.Candle> c, double atr) {
        return last(c).high > recentHigh(c, 20) - atr * 0.25;
    }

    private boolean rangeFadeLong(List<TradingCore.Candle> c, double atr) {
        return last(c).low < recentLow(c, 20) + atr * 0.25;
    }

    private boolean sweepHigh(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return l.high > recentHigh(c, 20) && (l.high - l.close) > atr * 0.20;
    }

    private boolean sweepLow(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return l.low < recentLow(c, 20) && (l.close - l.low) > atr * 0.20;
    }

    private boolean rejectionDown(List<TradingCore.Candle> c) {
        TradingCore.Candle l = last(c);
        return l.close < (l.high + l.low) / 2;
    }

    private boolean rejectionUp(List<TradingCore.Candle> c) {
        TradingCore.Candle l = last(c);
        return l.close > (l.high + l.low) / 2;
    }

    /* ========================== EMA ========================== */
    private EMA emaContext(List<TradingCore.Candle> c) {
        double e9 = ema(c, 9);
        double e21 = ema(c, 21);
        double e50 = ema(c, 50);
        double p = last(c).close;
        return new EMA(e9, e21, e50, p > e9 && e9 > e21 && e21 > e50, p < e9 && e9 < e21 && e21 < e50);
    }

    private double ema(List<TradingCore.Candle> c, int period) {
        double k = 2.0 / (period + 1);
        double e = c.get(c.size() - period).close;
        for (int i = c.size() - period + 1; i < c.size(); i++) e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    /* ========================== UTILS ========================== */
    private boolean volumeBoost(List<TradingCore.Candle> c) {
        return last(c).volume > avgVol(c, 30) * 1.25;
    }

    private boolean volumeDry(List<TradingCore.Candle> c) {
        return last(c).volume < avgVol(c, 30) * 0.85;
    }

    private boolean volumeClimax(List<TradingCore.Candle> c) {
        return last(c).volume > avgVol(c, 30) * 1.6;
    }

    private boolean impulse(List<TradingCore.Candle> c, double atr) {
        return Math.abs(last(c).close - c.get(c.size() - 5).close) > atr;
    }

    private boolean rangeExpansion(List<TradingCore.Candle> c) {
        TradingCore.Candle l = last(c);
        return (l.high - l.low) > avgRange(c, 20) * 1.4;
    }

    private boolean smallRange(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        return (l.high - l.low) < atr * 0.8;
    }

    private boolean structureBreak(List<TradingCore.Candle> c, boolean up) {
        TradingCore.Candle l = last(c);
        return up ? l.close > recentHigh(c, 12) : l.close < recentLow(c, 12);
    }

    private boolean rangeCompression(List<TradingCore.Candle> c) {
        return avgRange(c, 20) < avgRange(c, c.size() - 20) * 0.75;
    }

    private double avgRange(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++) sum += c.get(i).high - c.get(i).low;
        return sum / n;
    }

    private double avgVol(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++) sum += c.get(i).volume;
        return sum / n;
    }

    private double recentHigh(List<TradingCore.Candle> c, int n) {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = c.size() - n; i < c.size(); i++) if (c.get(i).high > max) max = c.get(i).high;
        return max;
    }

    private double recentLow(List<TradingCore.Candle> c, int n) {
        double min = Double.POSITIVE_INFINITY;
        for (int i = c.size() - n; i < c.size(); i++) if (c.get(i).low < min) min = c.get(i).low;
        return min;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    private boolean recentCooldown(Map<String, Long> map, String symbol, long now) {
        return map.containsKey(symbol) && now - map.get(symbol) < COOLDOWN_MS;
    }

    /* ========================== STRUCT ========================== */
    private enum MarketMode { TREND, HIGH_VOL_TREND, RANGE, REVERSAL }

    private static class EMA {
        double ema9, ema21, ema50;
        boolean bullish, bearish;

        EMA(double e9, double e21, double e50, boolean bull, boolean bear) {
            this.ema9 = e9; this.ema21 = e21; this.ema50 = e50; this.bullish = bull; this.bearish = bear;
        }
    }
}
