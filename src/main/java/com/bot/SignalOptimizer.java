package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SignalOptimizer Ultimate для интеграции с EliteM15Analyzer и DecisionEngineMerged.
 * Поддерживает микро-тренды, импульсы, направление сигнала и согласование с HTF.
 */
public final class SignalOptimizer {

    /* ================= CONFIG ================= */
    private static final int MAX_TICKS = 60;           // последние тики для анализа микро-тренда
    private static final double ALPHA = 0.35;          // сглаживание скорости и ускорения
    private static final double IMPULSE_DEAD = 0.0003;
    private static final double IMPULSE_STRONG = 0.0015;

    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();
    private final TradingCore.AdaptiveBrain adaptiveBrain;

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque,
                           TradingCore.AdaptiveBrain adaptiveBrain) {
        this.tickPriceDeque = tickPriceDeque;
        this.adaptiveBrain = adaptiveBrain;
    }

    /* ================= MICRO TREND ================= */
    public static final class MicroTrendResult {
        public final double speed;
        public final double accel;
        public final double avg;

        public MicroTrendResult(double speed, double accel, double avg) {
            this.speed = speed;
            this.accel = accel;
            this.avg = avg;
        }
    }

    public MicroTrendResult computeMicroTrend(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < 6) return new MicroTrendResult(0, 0, 0);

        List<Double> prices = new ArrayList<>(dq);
        int size = prices.size();
        int n = Math.min(size, MAX_TICKS);

        double speed = 0, accel = 0;
        for (int i = size - n + 1; i < size; i++) {
            double diff = prices.get(i) - prices.get(i - 1);
            double prevSpeed = speed;
            speed = ALPHA * diff + (1 - ALPHA) * speed;
            accel = ALPHA * (speed - prevSpeed) + (1 - ALPHA) * accel;
        }

        double avg = prices.subList(size - n, size).stream().mapToDouble(d -> d).average()
                .orElse(prices.get(size - 1));
        MicroTrendResult result = new MicroTrendResult(speed, accel, avg);
        microTrendCache.put(symbol, result);
        return result;
    }

    /* ================= CONFIDENCE ================= */
    public double adjustConfidence(Elite5MinAnalyzer.TradeSignal s, double baseConfidence) {
        double conf = baseConfidence;
        MicroTrendResult mt = microTrendCache.get(s.symbol);

        if (mt != null) {
            double impulse = Math.abs(mt.speed) + Math.abs(mt.accel);

            if (impulse < IMPULSE_DEAD) conf *= 0.78;        // рынок мёртв
            else if (impulse > IMPULSE_STRONG) conf += 0.06; // сильный импульс

            if ((mt.speed > 0 && s.side == TradingCore.Side.SHORT) ||
                    (mt.speed < 0 && s.side == TradingCore.Side.LONG)) {
                conf *= 0.85; // против микро-тренда → штраф
            }
        }

        // согласование с AdaptiveBrain
        if (adaptiveBrain != null) {
            conf = adaptiveBrain.applyAllAdjustments(
                    "ELITE15", s.symbol, conf,
                    TradingCore.CoinType.TOP, true, false
            );
        }

        return clamp(conf, 0.50, 0.97);
    }

    /* ================= STOP / TAKE ================= */
    public Elite5MinAnalyzer.TradeSignal withAdjustedStopTake(Elite5MinAnalyzer.TradeSignal s, double atr) {
        double volPct = clamp(atr / s.entry, 0.008, 0.04);
        double rr = s.confidence > 0.75 ? 2.5 : s.confidence > 0.65 ? 2.0 : 1.6;

        double stop, take;
        if (s.side == TradingCore.Side.LONG) {
            stop = s.entry * (1 - volPct);
            take = s.entry * (1 + volPct * rr);
        } else {
            stop = s.entry * (1 + volPct);
            take = s.entry * (1 - volPct * rr);
        }

        return new Elite5MinAnalyzer.TradeSignal(
                s.symbol,
                s.side,
                s.entry,
                stop,
                take,
                s.confidence,
                s.reason,
                s.grade
        );
    }

    /* ================= HTF ALIGNMENT ================= */
    public boolean alignsWithDecisionEngine(DecisionEngineMerged.TradeIdea idea, List<TradingCore.Candle> h1) {
        if (h1 == null || h1.size() < 200) return true;
        double ema50 = ema(h1, 50);
        double ema200 = ema(h1, 200);
        return idea.side == TradingCore.Side.LONG ? ema50 > ema200 : ema50 < ema200;
    }

    /* ================= UTIL ================= */
    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }
}