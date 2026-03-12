package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class SignalOptimizer {

    /* ================= CONFIG ================= */

    private static final int MIN_TICKS = 8;      // Снижено с 12
    private static final int MAX_TICKS = 150;

    private static final double EMA_ALPHA = 0.35;

    private static final double STRONG_IMPULSE = 0.0012;  // Более чувствительно
    private static final double WEAK_IMPULSE = 0.0004;

    private static final double MAX_CONF = 92.0;
    private static final double MIN_CONF = 54.0;

    private static final double MAX_IMPULSE_CAP = 0.006;

    /* ================= STATE ================= */

    private final Map<String, Deque<Double>> tickPriceDeque;

    private final Map<String, MicroTrendResult> microTrendCache =
            new ConcurrentHashMap<>();

    // Кэш последних цен для fallback когда нет тиков
    private final Map<String, List<Double>> priceFallback = new ConcurrentHashMap<>();

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    /* ================= DATA ================= */

    public static final class MicroTrendResult {

        public final double speed;
        public final double accel;
        public final double avg;
        public final double impulse;
        public final boolean fromTicks;  // true = реальные тики, false = fallback

        public MicroTrendResult(double speed,
                                double accel,
                                double avg,
                                boolean fromTicks) {

            this.speed = speed;
            this.accel = accel;
            this.avg = avg;
            this.fromTicks = fromTicks;

            double rawImpulse =
                    Math.abs(speed) * 1.20 +
                            Math.abs(accel) * 0.80;
            this.impulse = Math.min(rawImpulse, MAX_IMPULSE_CAP);
        }

        // Совместимость
        public MicroTrendResult(double speed, double accel, double avg) {
            this(speed, accel, avg, true);
        }
    }

    private static final MicroTrendResult ZERO =
            new MicroTrendResult(0, 0, 0, false);

    /* ================= MICRO TREND ================= */

    public MicroTrendResult computeMicroTrend(String symbol) {

        Deque<Double> dq = tickPriceDeque.get(symbol);

        // Если есть тики - используем их
        if (dq != null && dq.size() >= MIN_TICKS) {
            return computeFromTicks(symbol, dq);
        }

        // Fallback: используем кэш цен из свечей
        List<Double> fallback = priceFallback.get(symbol);
        if (fallback != null && fallback.size() >= 5) {
            return computeFromPrices(fallback, false);
        }

        return ZERO;
    }

    private MicroTrendResult computeFromTicks(String symbol, Deque<Double> dq) {
        List<Double> buffer = new ArrayList<>(MAX_TICKS);

        synchronized (dq) {
            Iterator<Double> it = dq.descendingIterator();
            while (it.hasNext() && buffer.size() < MAX_TICKS) {
                buffer.add(it.next());
            }
        }

        if (buffer.size() < MIN_TICKS) {
            return ZERO;
        }

        Collections.reverse(buffer);
        MicroTrendResult result = computeFromPrices(buffer, true);
        microTrendCache.put(symbol, result);
        return result;
    }

    private MicroTrendResult computeFromPrices(List<Double> prices, boolean fromTicks) {
        if (prices.size() < 3) return ZERO;

        double speed = 0.0;
        double accel = 0.0;

        double prev = prices.get(0);
        double sum = prev;

        for (int i = 1; i < prices.size(); i++) {

            double price = prices.get(i);
            double diff = (price - prev) / Math.max(prev, 1e-9);

            double prevSpeed = speed;
            speed = EMA_ALPHA * diff + (1 - EMA_ALPHA) * speed;
            accel = EMA_ALPHA * (speed - prevSpeed) + (1 - EMA_ALPHA) * accel;

            prev = price;
            sum += price;
        }

        double avg = sum / prices.size();

        return new MicroTrendResult(speed, accel, avg, fromTicks);
    }

    /* ================= UPDATE FALLBACK FROM CANDLES ================= */

    /**
     * Обновить fallback данные из свечей (вызывается из SignalSender)
     */
    public void updateFromCandles(String symbol, List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 10) return;

        List<Double> prices = new ArrayList<>();
        int start = Math.max(0, candles.size() - 20);

        for (int i = start; i < candles.size(); i++) {
            com.bot.TradingCore.Candle c = candles.get(i);
            // Используем типичную цену: (H+L+C)/3
            double tp = (c.high + c.low + c.close) / 3.0;
            prices.add(tp);
        }

        priceFallback.put(symbol, prices);
    }

    /* ================= CONFIDENCE ADJUSTMENT ================= */

    public double adjustConfidence(com.bot.DecisionEngineMerged.TradeIdea signal) {

        MicroTrendResult mt = computeMicroTrend(signal.symbol);

        double confidence = signal.probability;

        // Если нет данных микротренда - возвращаем как есть
        if (mt == null || mt.impulse < 1e-8) {
            return confidence;
        }

        boolean isLong = signal.side == com.bot.TradingCore.Side.LONG;

        boolean trendUp = mt.speed > 0 || mt.accel > 0;

        double trendAlignment =
                (isLong && trendUp) || (!isLong && !trendUp)
                        ? 1.0
                        : -1.0;

        double range = STRONG_IMPULSE - WEAK_IMPULSE;
        if (range <= 0) return confidence;

        double impulseNorm = Math.min(
                Math.max(mt.impulse - WEAK_IMPULSE, 0.0) / range,
                1.0
        );

        // Бонус/штраф за направление
        double factor = 1.0 + trendAlignment * (0.04 + 0.10 * impulseNorm);
        confidence *= factor;

        /* ===== Direction Strength Bonus ===== */
        double directionStrength = Math.abs(mt.speed) / Math.max(Math.abs(mt.avg), 1e-9);

        if (directionStrength > 0.0015) {
            confidence += 2.8;
        }

        /* ===== MICRO REVERSAL FILTER ===== */
        // Если скорость и ускорение разнонаправлены = возможный разворот
        if (mt.speed * mt.accel < 0 && Math.abs(mt.accel) > Math.abs(mt.speed) * 0.8) {
            confidence -= 3.5;
        }

        /* ===== MOMENTUM CONFIRMATION ===== */
        // Сильный моментум в направлении сигнала
        if (Math.abs(mt.speed) > 0.001 && trendAlignment > 0) {
            confidence += 1.5;
        }

        // Дополнительный бонус если данные из реальных тиков
        if (mt.fromTicks && impulseNorm > 0.5) {
            confidence += 1.0;
        }

        return clamp(confidence, MIN_CONF, MAX_CONF);
    }

    /* ================= SIGNAL WRAPPER ================= */

    public com.bot.DecisionEngineMerged.TradeIdea withAdjustedConfidence(
            com.bot.DecisionEngineMerged.TradeIdea signal) {

        double newConfidence = adjustConfidence(signal);

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

    /* ================= PUMP DETECTION HELPER ================= */

    /**
     * Быстрая проверка на микро-памп по тикам
     */
    public boolean detectMicroPump(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < 20) return false;

        List<Double> recent = new ArrayList<>();
        synchronized (dq) {
            Iterator<Double> it = dq.descendingIterator();
            while (it.hasNext() && recent.size() < 30) {
                recent.add(it.next());
            }
        }

        if (recent.size() < 20) return false;

        Collections.reverse(recent);

        // Движение за последние 10 тиков
        double first = recent.get(recent.size() - 15);
        double last = recent.get(recent.size() - 1);
        double movePct = Math.abs(last - first) / first;

        // Памп если движение > 0.5% за короткий период
        return movePct > 0.005;
    }

    /* ================= CACHE CLEANUP ================= */

    public void clearCacheForSymbol(String symbol) {
        microTrendCache.remove(symbol);
        priceFallback.remove(symbol);
    }

    public void clearAllCache() {
        microTrendCache.clear();
        priceFallback.clear();
    }

    /* ================= UTIL ================= */

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}
