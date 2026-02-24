package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Professional Signal Optimizer
 * Без блокировок, без залипаний, для бесконечного 15m цикла.
 */
public final class SignalOptimizer {

    /* ================= CONFIG ================= */

    private static final int MIN_TICKS = 8;
    private static final int MAX_TICKS = 120;
    private static final double EMA_ALPHA = 0.32;

    private static final double STRONG_IMPULSE = 0.0015;
    private static final double WEAK_IMPULSE = 0.0006;

    private static final double MAX_CONF = 0.97;
    private static final double MIN_CONF = 0.40;

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
            this.impulse = Math.abs(speed) + Math.abs(accel);
        }
    }

    private static final MicroTrendResult ZERO =
            new MicroTrendResult(0, 0, 0);

    /* ================= MICRO TREND ================= */

    public MicroTrendResult computeMicroTrend(String symbol) {

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
            double diff = price - prev;

            double prevSpeed = speed;

            speed = EMA_ALPHA * diff +
                    (1 - EMA_ALPHA) * speed;

            accel = EMA_ALPHA * (speed - prevSpeed) +
                    (1 - EMA_ALPHA) * accel;

            prev = price;
            sum += price;
        }

        MicroTrendResult result =
                new MicroTrendResult(
                        speed,
                        accel,
                        sum / buffer.size());

        microTrendCache.put(symbol, result);

        return result;
    }

    /* ================= CONFIDENCE ================= */

    public double adjustConfidence(
            DecisionEngineMerged.TradeIdea signal) {

        MicroTrendResult mt =
                computeMicroTrend(signal.symbol);

        double confidence = signal.probability;

        if (mt == ZERO)
            return confidence;

        boolean isLong =
                signal.side == TradingCore.Side.LONG;

        boolean trendUp = mt.speed > 0;

        double adjustment = 0.0;

        if (mt.impulse > STRONG_IMPULSE) {
            adjustment = ((isLong && trendUp) ||
                    (!isLong && !trendUp))
                    ? 0.07 : -0.06;

        } else if (mt.impulse > WEAK_IMPULSE) {
            adjustment = ((isLong && trendUp) ||
                    (!isLong && !trendUp))
                    ? 0.03 : -0.03;
        }

        confidence += adjustment;

        return clamp(confidence, MIN_CONF, MAX_CONF);
    }

    /* ================= STOP / TAKE ================= */

    public DecisionEngineMerged.TradeIdea withAdjustedStopTake(
            DecisionEngineMerged.TradeIdea signal,
            double atr) {

        double newConfidence =
                adjustConfidence(signal);

        double volatilityPct =
                clamp(atr / signal.price,
                        0.006,
                        0.035);

        double rr =
                newConfidence > 0.85 ? 3.2 :
                        newConfidence > 0.75 ? 2.6 :
                                newConfidence > 0.65 ? 2.1 :
                                        1.7;

        double stop;
        double take;

        if (signal.side == TradingCore.Side.LONG) {

            stop = signal.price *
                    (1 - volatilityPct);

            take = signal.price *
                    (1 + volatilityPct * rr);

        } else {

            stop = signal.price *
                    (1 + volatilityPct);

            take = signal.price *
                    (1 - volatilityPct * rr);
        }

        return new DecisionEngineMerged.TradeIdea(
                signal.symbol,
                signal.side,
                signal.price,
                stop,
                take,
                newConfidence,
                signal.reason
        );
    }

    /* ================= CLEANUP ================= */

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