package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SignalOptimizerMerged
 * Работает поверх DecisionEngineMerged.TradeIdea
 * Добавляет микро-тренды и динамическое корректирование confidence / stop / take.
 */
public final class SignalOptimizer {

    /* ================= CONFIG ================= */
    private static final int MIN_TICKS = 8;
    private static final int MAX_TICKS = 100;
    private static final double EMA_ALPHA = 0.32;
    private static final double STRONG_IMPULSE = 0.0015;
    private static final double WEAK_IMPULSE = 0.0006;
    private static final double MAX_CONF = 0.97;
    private static final double MIN_CONF = 0.40;
    private static final long SIGNAL_REFRESH_MS = 15 * 60_000; // 15 минут

    /* ================= STATE ================= */
    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSignalTimestamp = new ConcurrentHashMap<>();

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    /* ================= DATA CLASS ================= */
    public static final class MicroTrendResult {
        public final double speed;
        public final double accel;
        public final double avg;
        public final double impulse;

        public MicroTrendResult(double speed, double accel, double avg) {
            this.speed = speed;
            this.accel = accel;
            this.avg = avg;
            this.impulse = Math.abs(speed) + Math.abs(accel);
        }
    }

    private static final class MicroTrendResultZero {
        private static final MicroTrendResult INSTANCE = new MicroTrendResult(0, 0, 0);
    }

    /* ================= MICRO TREND CALCULATION ================= */
    public MicroTrendResult computeMicroTrend(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < MIN_TICKS)
            return MicroTrendResultZero.INSTANCE;

        double speed = 0.0;
        double accel = 0.0;
        double prevPrice = 0.0;
        int processed = 0;
        double sum = 0.0;

        List<Double> buffer = new ArrayList<>(MAX_TICKS);
        Iterator<Double> it = dq.descendingIterator();
        while (it.hasNext() && buffer.size() < MAX_TICKS) buffer.add(it.next());
        Collections.reverse(buffer);

        for (double price : buffer) {
            if (processed == 0) {
                prevPrice = price;
                sum += price;
                processed++;
                continue;
            }
            double diff = price - prevPrice;
            double prevSpeed = speed;
            speed = EMA_ALPHA * diff + (1 - EMA_ALPHA) * speed;
            accel = EMA_ALPHA * (speed - prevSpeed) + (1 - EMA_ALPHA) * accel;
            prevPrice = price;
            sum += price;
            processed++;
        }

        MicroTrendResult result = new MicroTrendResult(speed, accel, sum / processed);
        microTrendCache.put(symbol, result);
        return result;
    }

    /* ================= CONFIDENCE ADJUSTMENT ================= */
    public double adjustConfidence(DecisionEngineMerged.TradeIdea signal) {
        long now = System.currentTimeMillis();
        Long last = lastSignalTimestamp.get(signal.symbol);
        if (last != null && now - last < SIGNAL_REFRESH_MS) return signal.confidence;
        lastSignalTimestamp.put(signal.symbol, now);

        MicroTrendResult mt = microTrendCache.get(signal.symbol);
        if (mt == null) mt = MicroTrendResultZero.INSTANCE;

        double confidence = signal.confidence;

        // Коррекция в зависимости от микро-тренда
        boolean isLong = signal.side == TradingCore.Side.LONG;
        boolean trendUp = mt.speed > 0;

        if (mt.impulse > STRONG_IMPULSE) {
            confidence += ((isLong && trendUp) || (!isLong && !trendUp)) ? 0.07 : -0.06;
        } else if (mt.impulse > WEAK_IMPULSE) {
            confidence += ((isLong && trendUp) || (!isLong && !trendUp)) ? 0.03 : -0.03;
        }

        // Дополнительные мелкие корректировки
        confidence = clamp(confidence, MIN_CONF, MAX_CONF);
        return confidence;
    }

    /* ================= STOP / TAKE ADJUSTMENT ================= */
    public DecisionEngineMerged.TradeIdea withAdjustedStopTake(
            DecisionEngineMerged.TradeIdea signal,
            double atr) {

        double newConfidence = adjustConfidence(signal);
        double volatilityPct = clamp(atr / signal.entry, 0.006, 0.035);

        double rr = newConfidence > 0.85 ? 3.2 :
                newConfidence > 0.75 ? 2.6 :
                        newConfidence > 0.65 ? 2.1 : 1.7;

        double stop = signal.side == TradingCore.Side.LONG ?
                signal.entry * (1 - volatilityPct) :
                signal.entry * (1 + volatilityPct);
        double take = signal.side == TradingCore.Side.LONG ?
                signal.entry * (1 + volatilityPct * rr) :
                signal.entry * (1 - volatilityPct * rr);

        return new DecisionEngineMerged.TradeIdea(
                signal.symbol,
                signal.side,
                signal.entry,
                stop,
                take,
                newConfidence,
                signal.grade,
                signal.reason
        );
    }

    /* ================= HELPERS ================= */
    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}