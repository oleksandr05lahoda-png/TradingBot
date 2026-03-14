package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class SignalOptimizer {

    /* ================= CONFIG ================= */

    private static final int MIN_TICKS = 6;       // Снижено с 8
    private static final int MAX_TICKS = 200;     // Увеличено

    private static final double EMA_ALPHA = 0.40;  // Более responsive

    private static final double STRONG_IMPULSE = 0.0010;   // Более чувствительно
    private static final double WEAK_IMPULSE = 0.0003;

    private static final double MAX_CONF = 92.0;
    private static final double MIN_CONF = 52.0;

    private static final double MAX_IMPULSE_CAP = 0.008;

    // Новые параметры
    private static final double MOMENTUM_THRESHOLD = 0.0008;
    private static final double ACCELERATION_THRESHOLD = 0.0004;
    private static final int MOMENTUM_WINDOW = 15;

    /* ================= STATE ================= */

    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();
    private final Map<String, List<Double>> priceFallback = new ConcurrentHashMap<>();

    // Новое: история momentum для детекции exhaustion
    private final Map<String, Deque<Double>> momentumHistory = new ConcurrentHashMap<>();

    // Интеграция с PumpHunter
    private PumpHunter pumpHunter;

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    public void setPumpHunter(PumpHunter pumpHunter) {
        this.pumpHunter = pumpHunter;
    }

    /* ================= DATA ================= */

    public static final class MicroTrendResult {

        public final double speed;          // Скорость движения
        public final double accel;          // Ускорение
        public final double avg;            // Средняя цена
        public final double impulse;        // Общая сила импульса
        public final boolean fromTicks;     // true = реальные тики
        public final double momentum;       // Momentum (новое)
        public final double smoothSpeed;    // Сглаженная скорость (новое)
        public final boolean isExhausted;   // Признак истощения (новое)

        public MicroTrendResult(double speed, double accel, double avg,
                                boolean fromTicks, double momentum,
                                double smoothSpeed, boolean isExhausted) {
            this.speed = speed;
            this.accel = accel;
            this.avg = avg;
            this.fromTicks = fromTicks;
            this.momentum = momentum;
            this.smoothSpeed = smoothSpeed;
            this.isExhausted = isExhausted;

            double rawImpulse =
                    Math.abs(speed) * 1.15 +
                            Math.abs(accel) * 0.75 +
                            Math.abs(momentum) * 0.45;
            this.impulse = Math.min(rawImpulse, MAX_IMPULSE_CAP);
        }

        // Совместимость со старым кодом
        public MicroTrendResult(double speed, double accel, double avg, boolean fromTicks) {
            this(speed, accel, avg, fromTicks, 0, speed, false);
        }

        public MicroTrendResult(double speed, double accel, double avg) {
            this(speed, accel, avg, true, 0, speed, false);
        }
    }

    private static final MicroTrendResult ZERO =
            new MicroTrendResult(0, 0, 0, false, 0, 0, false);

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
            return computeFromPrices(symbol, fallback, false);
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
        MicroTrendResult result = computeFromPrices(symbol, buffer, true);
        microTrendCache.put(symbol, result);
        return result;
    }

    private MicroTrendResult computeFromPrices(String symbol, List<Double> prices, boolean fromTicks) {
        if (prices.size() < 3) return ZERO;

        double speed = 0.0;
        double accel = 0.0;

        double prev = prices.get(0);
        double sum = prev;

        // Для momentum calculation
        List<Double> returns = new ArrayList<>();

        for (int i = 1; i < prices.size(); i++) {
            double price = prices.get(i);
            double diff = (price - prev) / Math.max(prev, 1e-9);

            returns.add(diff);

            double prevSpeed = speed;
            speed = EMA_ALPHA * diff + (1 - EMA_ALPHA) * speed;
            accel = EMA_ALPHA * (speed - prevSpeed) + (1 - EMA_ALPHA) * accel;

            prev = price;
            sum += price;
        }

        double avg = sum / prices.size();

        // === Новое: Momentum calculation ===
        double momentum = 0;
        if (returns.size() >= 5) {
            // Суммируем последние returns
            int momWindow = Math.min(MOMENTUM_WINDOW, returns.size());
            double momSum = 0;
            for (int i = returns.size() - momWindow; i < returns.size(); i++) {
                momSum += returns.get(i);
            }
            momentum = momSum / momWindow;
        }

        // === Новое: Smooth speed (двойное сглаживание) ===
        double smoothSpeed = speed;
        if (returns.size() >= 10) {
            double longSpeed = 0;
            double longAlpha = 0.15;
            for (double r : returns) {
                longSpeed = longAlpha * r + (1 - longAlpha) * longSpeed;
            }
            smoothSpeed = (speed + longSpeed) / 2;
        }

        // === Новое: Exhaustion detection ===
        boolean isExhausted = detectExhaustion(symbol, speed, accel, momentum);

        // Сохраняем momentum в историю
        Deque<Double> momHistory = momentumHistory.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        momHistory.addLast(momentum);
        while (momHistory.size() > 100) momHistory.removeFirst();

        return new MicroTrendResult(speed, accel, avg, fromTicks, momentum, smoothSpeed, isExhausted);
    }

    /**
     * Детекция истощения momentum
     */
    private boolean detectExhaustion(String symbol, double speed, double accel, double momentum) {
        Deque<Double> momHistory = momentumHistory.get(symbol);
        if (momHistory == null || momHistory.size() < 10) return false;

        // Условия истощения:
        // 1. Momentum падает (accel противоположен speed)
        // 2. Текущий momentum меньше среднего исторического
        // 3. Speed замедляется

        boolean speedAccelDiverge = speed * accel < 0;

        List<Double> recentMom = new ArrayList<>(momHistory);
        double avgMom = recentMom.stream().mapToDouble(d -> Math.abs(d)).average().orElse(0);

        boolean momentumWeakening = Math.abs(momentum) < avgMom * 0.6;

        return speedAccelDiverge && momentumWeakening && Math.abs(accel) > ACCELERATION_THRESHOLD;
    }

    /* ================= UPDATE FALLBACK FROM CANDLES ================= */

    public void updateFromCandles(String symbol, List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 10) return;

        List<Double> prices = new ArrayList<>();
        int start = Math.max(0, candles.size() - 30);  // Увеличено с 20

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

        // === 1. Trend Alignment ===
        boolean trendUp = mt.speed > 0 || mt.smoothSpeed > 0;
        double trendAlignment = (isLong && trendUp) || (!isLong && !trendUp) ? 1.0 : -1.0;

        double range = STRONG_IMPULSE - WEAK_IMPULSE;
        if (range <= 0) return confidence;

        double impulseNorm = Math.min(
                Math.max(mt.impulse - WEAK_IMPULSE, 0.0) / range,
                1.0
        );

        // Бонус/штраф за направление
        double factor = 1.0 + trendAlignment * (0.05 + 0.12 * impulseNorm);
        confidence *= factor;

        // === 2. Momentum Strength Bonus ===
        double momentumStrength = Math.abs(mt.momentum);
        if (momentumStrength > MOMENTUM_THRESHOLD) {
            boolean momAligned = (isLong && mt.momentum > 0) || (!isLong && mt.momentum < 0);
            if (momAligned) {
                confidence += 3.5 + Math.min(momentumStrength * 500, 4.0);
            } else {
                confidence -= 2.5;
            }
        }

        // === 3. Direction Strength Bonus ===
        double directionStrength = Math.abs(mt.smoothSpeed) / Math.max(Math.abs(mt.avg), 1e-9);
        if (directionStrength > 0.0012) {
            confidence += 3.0;
        }

        // === 4. MICRO REVERSAL FILTER ===
        // Если скорость и ускорение разнонаправлены = возможный разворот
        if (mt.speed * mt.accel < 0 && Math.abs(mt.accel) > Math.abs(mt.speed) * 0.7) {
            confidence -= 4.0;
        }

        // === 5. EXHAUSTION FILTER ===
        if (mt.isExhausted) {
            // При истощении снижаем confidence для продолжения тренда
            confidence -= 5.0;
        }

        // === 6. MOMENTUM CONFIRMATION ===
        if (Math.abs(mt.smoothSpeed) > 0.0008 && trendAlignment > 0) {
            confidence += 2.0;
        }

        // === 7. Tick Data Quality Bonus ===
        if (mt.fromTicks && impulseNorm > 0.4) {
            confidence += 1.5;
        }

        // === 8. PumpHunter Integration ===
        if (pumpHunter != null) {
            PumpHunter.PumpEvent pump = pumpHunter.getRecentPump(signal.symbol);
            if (pump != null && pump.strength > 0.5) {
                // Если есть активный памп в нашем направлении - бонус
                boolean pumpAligned = (isLong && pump.isBullish()) || (!isLong && pump.isBearish());
                if (pumpAligned) {
                    confidence += 4.0 + pump.strength * 5.0;
                } else {
                    // Памп против нас - штраф
                    confidence -= 3.0;
                }
            }
        }

        return clamp(confidence, MIN_CONF, MAX_CONF);
    }

    /* ================= ADVANCED ANALYSIS ================= */

    /**
     * Расширенный анализ микро-структуры
     */
    public MicroAnalysis analyzeMicroStructure(String symbol,
                                               List<com.bot.TradingCore.Candle> c1m,
                                               List<com.bot.TradingCore.Candle> c5m) {

        MicroTrendResult mt = computeMicroTrend(symbol);

        MicroAnalysis analysis = new MicroAnalysis();
        analysis.microTrend = mt;

        if (c1m != null && c1m.size() >= 20) {
            // Анализ последних 1M свечей
            analysis.recentMomentum = calculateCandleMomentum(c1m, 10);
            analysis.volumeTrend = calculateVolumeTrend(c1m, 15);
            analysis.priceAcceleration = calculatePriceAcceleration(c1m, 8);
        }

        if (c5m != null && c5m.size() >= 15) {
            // Анализ 5M свечей
            analysis.mediumMomentum = calculateCandleMomentum(c5m, 8);
        }

        // Multi-timeframe alignment
        if (mt != null) {
            boolean microUp = mt.smoothSpeed > 0;
            boolean m1Up = analysis.recentMomentum > 0;
            boolean m5Up = analysis.mediumMomentum > 0;

            int alignScore = 0;
            if (microUp) alignScore++;
            if (m1Up) alignScore++;
            if (m5Up) alignScore++;

            analysis.mtfAlignment = (alignScore >= 2) ? 1 : (alignScore == 0) ? -1 : 0;
            analysis.isFullyAligned = alignScore == 3 || alignScore == 0;
        }

        return analysis;
    }

    public static class MicroAnalysis {
        public MicroTrendResult microTrend;
        public double recentMomentum;
        public double mediumMomentum;
        public double volumeTrend;
        public double priceAcceleration;
        public int mtfAlignment;  // 1 = все вверх, -1 = все вниз, 0 = mixed
        public boolean isFullyAligned;
    }

    private double calculateCandleMomentum(List<com.bot.TradingCore.Candle> candles, int period) {
        if (candles.size() < period + 1) return 0;

        int n = candles.size();
        double sum = 0;

        for (int i = n - period; i < n; i++) {
            com.bot.TradingCore.Candle c = candles.get(i);
            sum += c.close - c.open;
        }

        double avgPrice = candles.get(n - 1).close;
        return sum / (avgPrice * period);
    }

    private double calculateVolumeTrend(List<com.bot.TradingCore.Candle> candles, int period) {
        if (candles.size() < period + 5) return 0;

        int n = candles.size();

        // Средний объём первой половины периода
        double firstHalfVol = 0;
        int halfPeriod = period / 2;
        for (int i = n - period; i < n - halfPeriod; i++) {
            firstHalfVol += candles.get(i).volume;
        }
        firstHalfVol /= halfPeriod;

        // Средний объём второй половины
        double secondHalfVol = 0;
        for (int i = n - halfPeriod; i < n; i++) {
            secondHalfVol += candles.get(i).volume;
        }
        secondHalfVol /= halfPeriod;

        // Тренд объёма: положительный = растёт
        return (secondHalfVol - firstHalfVol) / Math.max(firstHalfVol, 1);
    }

    private double calculatePriceAcceleration(List<com.bot.TradingCore.Candle> candles, int period) {
        if (candles.size() < period + 2) return 0;

        int n = candles.size();

        // Движение первой половины
        int half = period / 2;
        double move1 = candles.get(n - period + half - 1).close - candles.get(n - period).close;

        // Движение второй половины
        double move2 = candles.get(n - 1).close - candles.get(n - half).close;

        double avgPrice = candles.get(n - 1).close;

        // Ускорение = изменение скорости
        return (move2 - move1) / (avgPrice * half);
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
                signal.flags,
                signal.fundingRate,
                signal.oiChange,
                signal.htfBias
        );
    }

    /* ================= PUMP DETECTION HELPER ================= */

    /**
     * Быстрая проверка на микро-памп по тикам
     */
    public boolean detectMicroPump(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < 25) return false;

        List<Double> recent = new ArrayList<>();
        synchronized (dq) {
            Iterator<Double> it = dq.descendingIterator();
            while (it.hasNext() && recent.size() < 40) {
                recent.add(it.next());
            }
        }

        if (recent.size() < 25) return false;

        Collections.reverse(recent);

        // Движение за последние 15 тиков
        double first = recent.get(recent.size() - 20);
        double last = recent.get(recent.size() - 1);
        double movePct = Math.abs(last - first) / first;

        // Памп если движение > 0.4% за короткий период
        return movePct > 0.004;
    }

    /**
     * Детекция momentum divergence
     */
    public boolean detectMomentumDivergence(String symbol,
                                            List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 30) return false;

        MicroTrendResult mt = computeMicroTrend(symbol);
        if (mt == null) return false;

        int n = candles.size();

        // Тренд цены (последние 15 баров)
        double priceMove = candles.get(n - 1).close - candles.get(n - 15).close;
        boolean priceUp = priceMove > 0;

        // Тренд momentum
        boolean momUp = mt.momentum > 0;

        // Дивергенция = цена идёт в одном направлении, momentum в другом
        return priceUp != momUp && Math.abs(mt.momentum) > MOMENTUM_THRESHOLD * 0.5;
    }

    /* ================= CACHE CLEANUP ================= */

    public void clearCacheForSymbol(String symbol) {
        microTrendCache.remove(symbol);
        priceFallback.remove(symbol);
        momentumHistory.remove(symbol);
    }

    public void clearAllCache() {
        microTrendCache.clear();
        priceFallback.clear();
        momentumHistory.clear();
    }

    /* ================= UTIL ================= */

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}
