package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class SignalOptimizer {

    /* ================= CONFIG ================= */

    private static final int MIN_TICKS = 12;
    private static final int MAX_TICKS = 150;

    private static final double EMA_ALPHA = 0.32;

    private static final double STRONG_IMPULSE = 0.0015;
    private static final double WEAK_IMPULSE = 0.0006;

    private static final double MAX_CONF = 95.0;
    private static final double MIN_CONF = 54.0;

    private static final double MAX_IMPULSE_CAP = 0.005;

    /* ================= STATE ================= */

    private final Map<String, Deque<Double>> tickPriceDeque;

    private final Map<String, MicroTrendResult> microTrendCache =
            new ConcurrentHashMap<>();

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    /* ================= DATA ================= */

    public static final class MicroTrendResult {

        public final double speed;
        public final double accel;
        public final double avg;
        public final double impulse;

        public MicroTrendResult(double speed,
                                double accel,
                                double avg) {

            this.speed = speed;
            this.accel = accel;
            this.avg = avg;

            double rawImpulse =
                    Math.abs(speed) * 1.15 +
                            Math.abs(accel) * 0.75;
            this.impulse = Math.min(rawImpulse, MAX_IMPULSE_CAP);
        }
    }

    private static final MicroTrendResult ZERO =
            new MicroTrendResult(0,0,0);

    /* ================= MICRO TREND ================= */

    public MicroTrendResult computeMicroTrend(String symbol) {

        MicroTrendResult cached = microTrendCache.get(symbol);

        Deque<Double> dq = tickPriceDeque.get(symbol);

        if (dq == null || dq.size() < MIN_TICKS)
            return ZERO;

        List<Double> buffer = new ArrayList<>(MAX_TICKS);

        synchronized (dq) {

            Iterator<Double> it = dq.descendingIterator();

            while (it.hasNext() && buffer.size() < MAX_TICKS)
                buffer.add(it.next());
        }

        if (buffer.size() < MIN_TICKS)
            return ZERO;

        Collections.reverse(buffer);

        double speed = 0.0;
        double accel = 0.0;

        double prev = buffer.get(0);
        double sum = prev;

        for (int i = 1; i < buffer.size(); i++) {

            double price = buffer.get(i);

            double diff = (price - prev) / Math.max(prev, 1e-9);

            double prevSpeed = speed;

            speed = EMA_ALPHA * diff + (1 - EMA_ALPHA) * speed;

            accel = EMA_ALPHA * (speed - prevSpeed) + (1 - EMA_ALPHA) * accel;

            prev = price;

            sum += price;
        }

        double avg = sum / buffer.size();

        MicroTrendResult result = new MicroTrendResult(speed, accel, avg);
        microTrendCache.put(symbol, result);
        return result;
    }

    /* ================= CONFIDENCE ADJUSTMENT ================= */

    public double adjustConfidence(com.bot.DecisionEngineMerged.TradeIdea signal) {

        MicroTrendResult mt = computeMicroTrend(signal.symbol);

        double confidence = signal.probability;

        if (mt == null || mt.impulse < 1e-7)
            return confidence;

        boolean isLong =
                signal.side == com.bot.TradingCore.Side.LONG;

        boolean trendUp =
                mt.speed > 0 || mt.accel > 0;

        double trendAlignment =
                (isLong && trendUp) || (!isLong && !trendUp)
                        ? 1.0
                        : -1.0;

        double range = STRONG_IMPULSE - WEAK_IMPULSE;

        if (range <= 0)
            return confidence;

        double impulseNorm =
                Math.min(
                        Math.max(mt.impulse - WEAK_IMPULSE, 0.0) / range,
                        1.0
                );

        double factor =
                1.0 + trendAlignment * (0.03 + 0.09 * impulseNorm);

        confidence *= factor;

        /* ===== Direction Strength Bonus ===== */

        double directionStrength =
                Math.abs(mt.speed) / Math.max(Math.abs(mt.avg), 1e-9);
        /* ===== MICRO REVERSAL FILTER ===== */

        if (mt.speed * mt.accel < 0 && Math.abs(mt.accel) > Math.abs(mt.speed)) {

            confidence -= 4.0;
        }
        if (directionStrength > 0.0018)
            confidence += 2.5;

        return clamp(confidence, MIN_CONF, MAX_CONF);
    }

    /* ================= SIGNAL WRAPPER ================= */

    public com.bot.DecisionEngineMerged.TradeIdea withAdjustedConfidence(
            com.bot.DecisionEngineMerged.TradeIdea signal) {

        double newConfidence =
                adjustConfidence(signal);

        return new com.bot.DecisionEngineMerged.TradeIdea(
                signal.symbol,
                signal.side,
                signal.price,
                signal.stop,
                signal.take,
                newConfidence,
                signal.flags
        );
    }

    /* ================= CACHE CLEANUP ================= */

    public void clearCacheForSymbol(String symbol) {
        microTrendCache.remove(symbol);
    }

    public void clearAllCache() {
        microTrendCache.clear();
    }

    /* ================= UTIL ================= */

    private static double clamp(double v,
                                double min,
                                double max) {

        return Math.max(min, Math.min(max, v));
    }
}