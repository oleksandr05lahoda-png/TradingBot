package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class SignalOptimizer {

    // === УЛУЧШЕНО: Более чувствительные пороги ===
    private static final int MIN_TICKS = 5;
    private static final int MAX_TICKS = 200;
    private static final double EMA_ALPHA = 0.45;

    private static final double STRONG_IMPULSE = 0.0025;   // Более чувствительно
    private static final double WEAK_IMPULSE = 0.0002;

    private static final double MAX_CONF = 88.0;
    private static final double MIN_CONF = 50.0;

    private static final double MAX_IMPULSE_CAP = 0.010;

    private static final double MOMENTUM_THRESHOLD = 0.0020;
    private static final double ACCELERATION_THRESHOLD = 0.0005;
    private static final int MOMENTUM_WINDOW = 15;

    // === НОВОЕ: Anti-lag параметры ===
    private static final double EARLY_DETECTION_SENSITIVITY = 1.5;  // 1.5x обычного
    private static final double REVERSAL_SENSITIVITY = 1.3;

    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();
    private final Map<String, List<Double>> priceFallback = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>> momentumHistory = new ConcurrentHashMap<>();

    private com.bot.PumpHunter pumpHunter;

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    public void setPumpHunter(com.bot.PumpHunter pumpHunter) {
        this.pumpHunter = pumpHunter;
    }

    public static final class MicroTrendResult {

        public final double speed;
        public final double accel;
        public final double avg;
        public final double impulse;
        public final boolean fromTicks;
        public final double momentum;
        public final double smoothSpeed;
        public final boolean isExhausted;

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

        public MicroTrendResult(double speed, double accel, double avg, boolean fromTicks) {
            this(speed, accel, avg, fromTicks, 0, speed, false);
        }

        public MicroTrendResult(double speed, double accel, double avg) {
            this(speed, accel, avg, true, 0, speed, false);
        }
    }

    private static final MicroTrendResult ZERO =
            new MicroTrendResult(0, 0, 0, false, 0, 0, false);

    public MicroTrendResult computeMicroTrend(String symbol) {

        Deque<Double> dq = tickPriceDeque.get(symbol);

        if (dq != null && dq.size() >= MIN_TICKS) {
            return computeFromTicks(symbol, dq);
        }

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

        // === Momentum calculation ===
        double momentum = 0;
        if (returns.size() >= 5) {
            int momWindow = Math.min(MOMENTUM_WINDOW, returns.size());
            double momSum = 0;
            for (int i = returns.size() - momWindow; i < returns.size(); i++) {
                momSum += returns.get(i);
            }
            momentum = momSum / momWindow;
        }

        // === Smooth speed ===
        double smoothSpeed = speed;
        if (returns.size() >= 10) {
            double longSpeed = 0;
            double longAlpha = 0.15;
            for (double r : returns) {
                longSpeed = longAlpha * r + (1 - longAlpha) * longSpeed;
            }
            smoothSpeed = (speed + longSpeed) / 2;
        }

        // === Exhaustion detection ===
        boolean isExhausted = detectExhaustion(symbol, speed, accel, momentum);

        // === Сохраняем momentum ===
        Deque<Double> momHistory = momentumHistory.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        momHistory.addLast(momentum);
        while (momHistory.size() > 100) momHistory.removeFirst();

        return new MicroTrendResult(speed, accel, avg, fromTicks, momentum, smoothSpeed, isExhausted);
    }

    private boolean detectExhaustion(String symbol, double speed, double accel, double momentum) {
        Deque<Double> momHistory = momentumHistory.get(symbol);
        if (momHistory == null || momHistory.size() < 8) return false;  // БЫЛО 10 → 8

        // === РАННЯЯ ДЕТЕКЦИЯ ===
        List<Double> recentMom = new ArrayList<>(momHistory);

        // Проверка: speed и accel разнонаправлены (начинается слабение)
        boolean diverging = speed * accel < 0 && Math.abs(accel) > 0.00015;  // БЫЛО 0.6*speed

        // Momentum падает 2 раза подряд = сигнал разворота
        boolean momentumFalling = false;
        if (recentMom.size() >= 3) {
            double last = Math.abs(recentMom.get(recentMom.size()-1));
            double prev = Math.abs(recentMom.get(recentMom.size()-2));
            double prev2 = Math.abs(recentMom.get(recentMom.size()-3));
            momentumFalling = (last < prev * 0.75) && (prev < prev2 * 0.75);
        }

        // Не требуем полного слабления - раннее предупреждение
        return diverging || momentumFalling;
    }
    public void updateFromCandles(String symbol, List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 10) return;

        List<Double> prices = new ArrayList<>();
        int start = Math.max(0, candles.size() - 30);

        for (int i = start; i < candles.size(); i++) {
            com.bot.TradingCore.Candle c = candles.get(i);
            double tp = (c.high + c.low + c.close) / 3.0;
            prices.add(tp);
        }

        priceFallback.put(symbol, prices);
    }

    /**
     * КРИТИЧНО: Корректировка confidence с учётом anti-lag параметров
     */
    public double adjustConfidence(com.bot.DecisionEngineMerged.TradeIdea signal) {

        MicroTrendResult mt = computeMicroTrend(signal.symbol);
        double confidence = signal.probability;

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

        double factor = 1.0 + trendAlignment * (0.06 + 0.15 * impulseNorm);
        confidence *= factor;

        // === 2. Momentum Strength Bonus (УСИЛЕНО) ===
        double momentumStrength = Math.abs(mt.momentum);
        if (momentumStrength > MOMENTUM_THRESHOLD) {
            boolean momAligned = (isLong && mt.momentum > 0) || (!isLong && mt.momentum < 0);
            if (momAligned) {
                confidence += 4.5 + Math.min(momentumStrength * 600, 5.5);  // +0.5 к обычному
            } else {
                confidence -= 3.5;  // +0.5 к штрафу
            }
        }

        // === 3. Direction Strength Bonus ===
        double directionStrength = Math.abs(mt.smoothSpeed) / Math.max(Math.abs(mt.avg), 1e-9);
        if (directionStrength > 0.0014) {
            confidence += 4.0;  // +1.0 к обычному
        }

        // === 4. MICRO REVERSAL FILTER (УСИЛЕНО) ===
        if (mt.speed * mt.accel < 0 && Math.abs(mt.accel) > Math.abs(mt.speed) * 0.7) {
            confidence -= 5.5;  // +1.5 к штрафу
        }

        // === 5. EXHAUSTION FILTER (КРИТИЧНО!) ===
        if (mt.isExhausted) {
            confidence -= 6.0;  // +2.0 к штрафу - большой штраф
        }

        // === 6. MOMENTUM CONFIRMATION ===
        if (Math.abs(mt.smoothSpeed) > 0.0010 && trendAlignment > 0) {
            confidence += 3.0;  // +1.0 к обычному
        }

        // === 7. Tick Data Quality Bonus ===
        if (mt.fromTicks && impulseNorm > 0.4) {
            confidence += 2.5;  // +1.0 к обычному
        }

        // === 8. PumpHunter Integration ===
        if (pumpHunter != null) {
            com.bot.PumpHunter.PumpEvent pump = pumpHunter.getRecentPump(signal.symbol);
            if (pump != null && pump.strength > 0.5) {
                boolean pumpAligned = (isLong && pump.isBullish()) || (!isLong && pump.isBearish());
                if (pumpAligned) {
                    confidence += 5.5 + pump.strength * 6.0;  // +1.5 к бонусу
                } else {
                    confidence -= 4.0;  // +1.0 к штрафу
                }
            }
        }

        // === 9. НОВОЕ: Anti-lag bonus (ловим движение рано) ===
        if (mt.impulse > STRONG_IMPULSE * EARLY_DETECTION_SENSITIVITY && trendAlignment > 0) {
            confidence += 6.0;  // Сильный бонус за раннее детектирование
        }

        // === 10. НОВОЕ: Reversal warning (блокируем плохие сигналы) ===
        if (mt.isExhausted && Math.abs(mt.momentum) < momentumStrength * 0.5) {
            confidence -= 8.0;  // БЫЛО -8.0 → -15.0 (сильнее штрафуем)
        }
        if (mt.isExhausted && trendAlignment < 0) {
            confidence -= 40.0;  // УНИЧТОЖАЕМ уверенность. Сигнал против тренда на истощении должен умереть.
        }

        if (confidence > 85.0) {
            // Искусственно прижимаем всё, что выше 85%, чтобы 92% появлялись только на идеальных сетапах
            confidence = 85.0 + (confidence - 85.0) * 0.2;
        }

        if (mt != null && mt.momentum != 0) {
            Deque<Double> momHist = momentumHistory.get(signal.symbol);
            if (momHist != null && momHist.size() >= 5) {
                List<Double> recentMom = new ArrayList<>(momHist);

                // Берём последние 5 значений momentum
                double lastMom = Math.abs(recentMom.get(recentMom.size()-1));
                double prevMom = Math.abs(recentMom.get(recentMom.size()-2));
                double prev2Mom = Math.abs(recentMom.get(recentMom.size()-3));

                // КЛЮЧЕВОЙ ПРИЗНАК: momentum падает 2 раза подряд = конец тренда!
                boolean momentumFalling = lastMom < prevMom * 0.70 && prevMom < prev2Mom * 0.70;

                // Плюс: speed и accel разнонаправлены
                boolean speedAccelDiverge = mt.speed * mt.accel < 0 && Math.abs(mt.accel) > 0.00018;

                if ((momentumFalling || speedAccelDiverge) && Math.abs(mt.smoothSpeed) > 0.0005) {
                    confidence -= 8.0;  // СИЛЬНЫЙ штраф за усталость тренда!
                    System.out.println("[Exhaustion] " + signal.symbol + " momentum fading, confidence -8%");
                }
            }
        }

        if (trendAlignment < 0 && impulseNorm > 0.5 && mt.isExhausted) {
            confidence -= 12.0;  // Очень сильный штраф
            System.out.println("[ContraTrend] Exhaustion detected, blocking");
        }

        return clamp(confidence, MIN_CONF, MAX_CONF);
    }

    /**
     * Создание нового сигнала с корректировкой confidence
     */
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

    public MicroAnalysis analyzeMicroStructure(String symbol,
                                               List<com.bot.TradingCore.Candle> c1m,
                                               List<com.bot.TradingCore.Candle> c5m) {

        MicroTrendResult mt = computeMicroTrend(symbol);

        MicroAnalysis analysis = new MicroAnalysis();
        analysis.microTrend = mt;

        if (c1m != null && c1m.size() >= 20) {
            analysis.recentMomentum = calculateCandleMomentum(c1m, 10);
            analysis.volumeTrend = calculateVolumeTrend(c1m, 15);
            analysis.priceAcceleration = calculatePriceAcceleration(c1m, 8);
        }

        if (c5m != null && c5m.size() >= 15) {
            analysis.mediumMomentum = calculateCandleMomentum(c5m, 8);
        }

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
        public int mtfAlignment;
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

        double firstHalfVol = 0;
        int halfPeriod = period / 2;
        for (int i = n - period; i < n - halfPeriod; i++) {
            firstHalfVol += candles.get(i).volume;
        }
        firstHalfVol /= halfPeriod;

        double secondHalfVol = 0;
        for (int i = n - halfPeriod; i < n; i++) {
            secondHalfVol += candles.get(i).volume;
        }
        secondHalfVol /= halfPeriod;

        return (secondHalfVol - firstHalfVol) / Math.max(firstHalfVol, 1);
    }

    private double calculatePriceAcceleration(List<com.bot.TradingCore.Candle> candles, int period) {
        if (candles.size() < period + 2) return 0;

        int n = candles.size();

        int half = period / 2;
        double move1 = candles.get(n - period + half - 1).close - candles.get(n - period).close;

        double move2 = candles.get(n - 1).close - candles.get(n - half).close;

        double avgPrice = candles.get(n - 1).close;

        return (move2 - move1) / (avgPrice * half);
    }

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

        double first = recent.get(recent.size() - 20);
        double last = recent.get(recent.size() - 1);
        double movePct = Math.abs(last - first) / first;

        return movePct > 0.005;  // 0.5% за короткий период
    }

    public boolean detectMomentumDivergence(String symbol,
                                            List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 30) return false;

        MicroTrendResult mt = computeMicroTrend(symbol);
        if (mt == null) return false;

        int n = candles.size();

        double priceMove = candles.get(n - 1).close - candles.get(n - 15).close;
        boolean priceUp = priceMove > 0;

        boolean momUp = mt.momentum > 0;

        return priceUp != momUp && Math.abs(mt.momentum) > MOMENTUM_THRESHOLD * 0.5;
    }

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

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}