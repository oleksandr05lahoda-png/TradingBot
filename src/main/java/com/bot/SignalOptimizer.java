package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Professional Signal Optimizer
 * Без блокировок, без залипаний, для бесконечного 15m цикла.
 */
public final class SignalOptimizer {

    /* ================= CONFIG ================= */

    private static final int MIN_TICKS = 12;
    private static final int MAX_TICKS = 120;
    private static final double EMA_ALPHA = 0.32;

    private static final double STRONG_IMPULSE = 0.0015;
    private static final double WEAK_IMPULSE = 0.0006;

    private static final double MAX_CONF = 95.0;
    private static final double MIN_CONF = 54.0; // синхронизировано с DecisionEngineMerged

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

            speed = EMA_ALPHA * diff + (1 - EMA_ALPHA) * speed;
            accel = EMA_ALPHA * (speed - prevSpeed) + (1 - EMA_ALPHA) * accel;

            prev = price;
            sum += price;
        }

        MicroTrendResult result = new MicroTrendResult(speed, accel, sum / buffer.size());
        microTrendCache.put(symbol, result);
        return result;
    }

    public double adjustConfidence(com.bot.DecisionEngineMerged.TradeIdea signal) {

        MicroTrendResult mt = computeMicroTrend(signal.symbol);
        double confidence = signal.probability;

        if (mt == null || mt.impulse < 1e-7)  // нулевой тренд → не меняем
            return confidence;

        boolean isLong = signal.side == com.bot.TradingCore.Side.LONG;
        boolean trendUp = mt.speed > 0;

        // Вычисляем выравнивание тренда: +1 если сигнал совпадает с трендом, -1 если против
        double trendAlignment = (isLong && trendUp) || (!isLong && !trendUp) ? 1.0 : -1.0;

        // Нормируем импульс в диапазон 0…1 относительно WEAK и STRONG
        double impulseNorm = Math.min(Math.max(mt.impulse - WEAK_IMPULSE, 0.0) / (STRONG_IMPULSE - WEAK_IMPULSE), 1.0);

        // Плавная коррекция: от ±2% при слабом импульсе до ±8% при сильном
        double factor = 1.0 + trendAlignment * (0.02 + 0.06 * impulseNorm);

        confidence *= factor;

        return clamp(confidence, MIN_CONF, MAX_CONF);
    }

    public com.bot.DecisionEngineMerged.TradeIdea withAdjustedConfidence(
            com.bot.DecisionEngineMerged.TradeIdea signal) {

        double newConfidence = adjustConfidence(signal);

        return new com.bot.DecisionEngineMerged.TradeIdea(
                signal.symbol,
                signal.side,
                signal.price,
                signal.stop,     // НЕ трогаем стоп
                signal.take,     // НЕ трогаем тейк
                newConfidence,
                signal.flags
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

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}