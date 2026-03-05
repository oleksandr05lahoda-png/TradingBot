package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Professional Signal Optimizer (Enhanced & Corrected)
 * Поддержка микро-трендов, адаптивного confidence, безопасное кэширование,
 * контроль импульсов и флагов для интеграции с DecisionEngineMerged.
 */
public final class SignalOptimizer {

    /* ================= CONFIG ================= */
    private static final int MIN_TICKS = 8;
    private static final int MAX_TICKS = 200; // увеличено для лучшего анализа
    private static final double EMA_ALPHA = 0.32;

    private static final double BASE_STRONG_IMPULSE = 0.0015;
    private static final double BASE_WEAK_IMPULSE = 0.0006;

    private static final double MAX_CONF = 95.0;
    private static final double MIN_CONF = 40.0;

    private static final int MICRO_CACHE_LIMIT = 500; // увеличен лимит кэша

    /* ================= STATE ================= */
    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    /* ================= DATA ================= */
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

        public boolean isZero() {
            return speed == 0.0 && accel == 0.0 && avg == 0.0;
        }
    }

    private static final MicroTrendResult ZERO = new MicroTrendResult(0, 0, 0);

    /* ================= MICRO TREND ================= */
    public MicroTrendResult computeMicroTrend(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < MIN_TICKS)
            return ZERO;

        List<Double> buffer = new ArrayList<>(MAX_TICKS);

        synchronized (dq) {
            Iterator<Double> it = dq.descendingIterator();
            while (it.hasNext() && buffer.size() < MAX_TICKS) {
                buffer.add(it.next());
            }
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

            speed = EMA_ALPHA * diff + (1 - EMA_ALPHA) * speed;
            accel = EMA_ALPHA * (speed - prevSpeed) + (1 - EMA_ALPHA) * accel;

            prev = price;
            sum += price;
        }

        MicroTrendResult result = new MicroTrendResult(speed, accel, sum / buffer.size());

        // --- кэширование с безопасным лимитом
        if (microTrendCache.size() > MICRO_CACHE_LIMIT) {
            Iterator<String> keys = microTrendCache.keySet().iterator();
            if (keys.hasNext()) microTrendCache.remove(keys.next());
        }
        microTrendCache.put(symbol, result);

        return result;
    }

    /* ================= ADAPTIVE IMPULSE ================= */
    private double adaptiveStrongImpulse(List<Double> buffer) {
        if (buffer == null || buffer.size() < 10) return BASE_STRONG_IMPULSE;
        double std = stdDev(buffer);
        return Math.max(BASE_STRONG_IMPULSE, std * 1.1);
    }

    private double adaptiveWeakImpulse(List<Double> buffer) {
        if (buffer == null || buffer.size() < 10) return BASE_WEAK_IMPULSE;
        double std = stdDev(buffer);
        return Math.max(BASE_WEAK_IMPULSE, std * 0.45);
    }

    private double stdDev(List<Double> buffer) {
        double avg = buffer.stream().mapToDouble(d -> d).average().orElse(0.0);
        double sumSq = buffer.stream().mapToDouble(d -> (d - avg) * (d - avg)).sum();
        return Math.sqrt(sumSq / buffer.size());
    }

    /* ================= CONFIDENCE ================= */
    public double adjustConfidence(DecisionEngineMerged.TradeIdea signal) {
        if (signal == null) return MIN_CONF;

        MicroTrendResult mt = computeMicroTrend(signal.symbol);
        double confidence = signal.probability;

        if (mt.isZero()) return clamp(confidence, MIN_CONF, MAX_CONF);

        boolean isLong = signal.side == TradingCore.Side.LONG;
        boolean trendUp = mt.speed > 0;

        double adjustment = 0.0;

        // Подготовка буфера для адаптивного импульса
        List<Double> buffer = new ArrayList<>(tickPriceDeque.getOrDefault(signal.symbol, new ConcurrentLinkedDeque<>()));
        double strongImpulse = adaptiveStrongImpulse(buffer);
        double weakImpulse = adaptiveWeakImpulse(buffer);

        // Основная адаптивная коррекция
        if (mt.impulse > strongImpulse) {
            adjustment = ((isLong && trendUp) || (!isLong && !trendUp)) ? 0.07 : -0.06;
        } else if (mt.impulse > weakImpulse) {
            adjustment = ((isLong && trendUp) || (!isLong && !trendUp)) ? 0.03 : -0.03;
        }

        // Учитываем DecisionEngine флаги
        if (signal.flags != null && signal.flags.contains("impulse:true")) {
            adjustment += ((isLong && trendUp) || (!isLong && !trendUp)) ? 0.015 : -0.015;
        }

        confidence += adjustment;

        return clamp(confidence, MIN_CONF, MAX_CONF);
    }

    /* ================= STOP / TAKE ================= */
    public DecisionEngineMerged.TradeIdea withAdjustedConfidence(DecisionEngineMerged.TradeIdea signal) {
        if (signal == null) return null;
        double newConfidence = adjustConfidence(signal);
        return new DecisionEngineMerged.TradeIdea(
                signal.symbol,
                signal.side,
                signal.price,
                signal.stop,
                signal.take,
                newConfidence,
                signal.flags
        );
    }

    /* ================= UTILS ================= */
    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}