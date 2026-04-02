package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║        SignalOptimizer — GODBOT EDITION v5.0                        ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  ИСПРАВЛЕНИЯ v5.0:                                                   ║
 * ║                                                                      ║
 * ║  [FIX-BUG-2] УБРАН MAX_IMPULSE_CAP = 0.010 (BUG #2 из аудита)     ║
 * ║    Было: любое движение > 1% обрезалось капом                        ║
 * ║    Проблема: монета +4% = монета +1% для бота — одинаково           ║
 * ║    Бот не видел разницы между нормальным ростом и сильным пампом    ║
 * ║    Стало: адаптивный кап на основе реального ATR символа            ║
 * ║    - Базовый кап: 4× STRONG_IMPULSE (0.010 → 0.040)                ║
 * ║    - Если pumpHunter видит активный памп — кап 0.080                ║
 * ║    - Нормализация теперь относительная (vs исторический ATR)        ║
 * ║                                                                      ║
 * ║  [FIX-OPT] Улучшенный adjustConfidence:                             ║
 * ║    - Логарифмическая нормализация импульса (не линейная)             ║
 * ║    - Кап confidence оставлен на уровне 85.0 (ограничивает жадность) ║
 * ║    - Штрафы за исхождение сохранены, но не дублируются              ║
 * ║                                                                      ║
 * ║  СОХРАНЕНО: MicroTrendResult / MicroAnalysis / PumpHunter hookup    ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 */
public final class SignalOptimizer {

    private static final int    MIN_TICKS  = 5;
    private static final int    MAX_TICKS  = 200;
    private static final double EMA_ALPHA  = 0.45;

    private static final double STRONG_IMPULSE = 0.0025;
    private static final double WEAK_IMPULSE   = 0.0002;

    private static final double MAX_CONF = 85.0;  // [v11.0] was 88 — unrealistic
    private static final double MIN_CONF = 50.0;

    // [FIX-BUG-2] Убран хардкодный кап 0.010 (1%).
    // Теперь кап динамический — задаётся в computeAdaptiveImpulseCap()
    // Диапазон: 0.020..0.080 в зависимости от режима рынка
    private static final double IMPULSE_CAP_BASE   = 0.040;  // базовый (было 0.010 = 4× занижено)
    private static final double IMPULSE_CAP_PUMP   = 0.080;  // при активном пампе
    private static final double IMPULSE_CAP_MIN    = 0.020;  // минимальный (тихий рынок)

    private static final double MOMENTUM_THRESHOLD     = 0.0020;
    private static final double ACCELERATION_THRESHOLD = 0.0005;
    private static final int    MOMENTUM_WINDOW        = 15;

    private static final double EARLY_DETECTION_SENSITIVITY = 1.5;
    private static final double REVERSAL_SENSITIVITY        = 1.3;

    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();
    private final Map<String, List<Double>> priceFallback       = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>> momentumHistory    = new ConcurrentHashMap<>();

    // [FIX-BUG-2] История ATR по символам для адаптивного капа
    private final Map<String, Deque<Double>> symbolAtrHistory   = new ConcurrentHashMap<>();
    private static final int ATR_HISTORY_SIZE = 96;

    private com.bot.PumpHunter pumpHunter;

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    public void setPumpHunter(com.bot.PumpHunter pumpHunter) {
        this.pumpHunter = pumpHunter;
    }

    // ══════════════════════════════════════════════════════════════
    //  MicroTrendResult
    // ══════════════════════════════════════════════════════════════

    public static final class MicroTrendResult {

        public final double speed;
        public final double accel;
        public final double avg;
        public final double impulse;
        public final boolean fromTicks;
        public final double momentum;
        public final double smoothSpeed;
        public final boolean isExhausted;

        // [FIX-BUG-2] impulse теперь НЕ ограничен хардкодным 0.010
        // Ограничение происходит снаружи через computeAdaptiveImpulseCap
        public MicroTrendResult(double speed, double accel, double avg,
                                boolean fromTicks, double momentum,
                                double smoothSpeed, boolean isExhausted,
                                double impulseCap) {
            this.speed = speed;
            this.accel = accel;
            this.avg = avg;
            this.fromTicks = fromTicks;
            this.momentum = momentum;
            this.smoothSpeed = smoothSpeed;
            this.isExhausted = isExhausted;

            double rawImpulse =
                    Math.abs(speed)    * 1.15 +
                            Math.abs(accel)    * 0.75 +
                            Math.abs(momentum) * 0.45;
            // Кап задаётся снаружи через impulseCap
            this.impulse = Math.min(rawImpulse, impulseCap);
        }

        /** Обратная совместимость — используем базовый кап */
        public MicroTrendResult(double speed, double accel, double avg,
                                boolean fromTicks, double momentum,
                                double smoothSpeed, boolean isExhausted) {
            this(speed, accel, avg, fromTicks, momentum, smoothSpeed, isExhausted, IMPULSE_CAP_BASE);
        }

        public MicroTrendResult(double speed, double accel, double avg, boolean fromTicks) {
            this(speed, accel, avg, fromTicks, 0, speed, false, IMPULSE_CAP_BASE);
        }

        public MicroTrendResult(double speed, double accel, double avg) {
            this(speed, accel, avg, true, 0, speed, false, IMPULSE_CAP_BASE);
        }
    }

    private static final MicroTrendResult ZERO =
            new MicroTrendResult(0, 0, 0, false, 0, 0, false);

    // ══════════════════════════════════════════════════════════════
    //  [FIX-BUG-2] Адаптивный кап импульса
    //  Учитывает: активный памп / исторический ATR / базовый уровень
    // ══════════════════════════════════════════════════════════════

    private double computeAdaptiveImpulseCap(String symbol) {
        // Если PumpHunter видит активный памп — расширяем кап
        if (pumpHunter != null) {
            com.bot.PumpHunter.PumpEvent pump = pumpHunter.getRecentPump(symbol);
            if (pump != null && pump.strength > 0.65) {
                return IMPULSE_CAP_PUMP;
            }
        }

        // Если есть история ATR — используем адаптивный кап
        Deque<Double> atrHist = symbolAtrHistory.get(symbol);
        if (atrHist != null && atrHist.size() >= 20) {
            List<Double> sorted = new ArrayList<>(atrHist);
            Collections.sort(sorted);
            double medianAtr = sorted.get(sorted.size() / 2);
            // Кап = 4× медианный ATR (было захардкоджено 1% = 1× ATR для большинства монет)
            return Math.max(IMPULSE_CAP_MIN, Math.min(IMPULSE_CAP_PUMP, medianAtr * 4.0));
        }

        return IMPULSE_CAP_BASE;
    }

    // ══════════════════════════════════════════════════════════════
    //  COMPUTE MICRO TREND
    // ══════════════════════════════════════════════════════════════

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

        // [v34.0 FIX] RACE CONDITION: synchronized(ConcurrentLinkedDeque) is an anti-pattern.
        // ConcurrentLinkedDeque is lock-free — external sync doesn't prevent concurrent adds.
        // Fix: use toArray() for atomic snapshot, then iterate the copy.
        try {
            Object[] snapshot = dq.toArray();
            int start = Math.max(0, snapshot.length - MAX_TICKS);
            for (int i = start; i < snapshot.length; i++) {
                buffer.add((Double) snapshot[i]);
            }
        } catch (Exception e) {
            // Defensive: if deque is modified during toArray, use fallback
            return ZERO;
        }

        if (buffer.size() < MIN_TICKS) {
            return ZERO;
        }

        // [v34.0] No reverse needed — toArray() returns forward chronological order
        MicroTrendResult result = computeFromPrices(symbol, buffer, true);
        microTrendCache.put(symbol, result);
        return result;
    }

    private MicroTrendResult computeFromPrices(String symbol, List<Double> prices, boolean fromTicks) {
        if (prices.size() < 3) return ZERO;

        double speed = 0.0;
        double accel = 0.0;

        double prev = prices.get(0);
        double sum  = prev;

        List<Double> returns = new ArrayList<>();

        for (int i = 1; i < prices.size(); i++) {
            double price = prices.get(i);
            double diff  = (price - prev) / Math.max(prev, 1e-9);

            returns.add(diff);

            double prevSpeed = speed;
            speed = EMA_ALPHA * diff + (1 - EMA_ALPHA) * speed;
            accel = EMA_ALPHA * (speed - prevSpeed) + (1 - EMA_ALPHA) * accel;

            prev = price;
            sum += price;
        }

        double avg = sum / prices.size();

        // Momentum
        double momentum = 0;
        if (returns.size() >= 5) {
            int momWindow = Math.min(MOMENTUM_WINDOW, returns.size());
            double momSum = 0;
            for (int i = returns.size() - momWindow; i < returns.size(); i++) {
                momSum += returns.get(i);
            }
            momentum = momSum / momWindow;
        }

        // Smooth speed
        double smoothSpeed = speed;
        if (returns.size() >= 10) {
            double longSpeed = 0;
            double longAlpha = 0.15;
            for (double r : returns) {
                longSpeed = longAlpha * r + (1 - longAlpha) * longSpeed;
            }
            smoothSpeed = (speed + longSpeed) / 2;
        }

        // Exhaustion detection
        boolean isExhausted = detectExhaustion(symbol, speed, accel, momentum);

        // Сохраняем momentum историю
        Deque<Double> momHistory = momentumHistory.computeIfAbsent(symbol, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        momHistory.addLast(momentum);
        while (momHistory.size() > 100) momHistory.removeFirst();

        // [FIX-BUG-2] Используем адаптивный кап
        double adaptiveCap = computeAdaptiveImpulseCap(symbol);

        return new MicroTrendResult(speed, accel, avg, fromTicks, momentum, smoothSpeed, isExhausted, adaptiveCap);
    }

    private boolean detectExhaustion(String symbol, double speed, double accel, double momentum) {
        Deque<Double> momHistory = momentumHistory.get(symbol);
        if (momHistory == null || momHistory.size() < 8) return false;

        List<Double> recentMom = new ArrayList<>(momHistory);

        // Speed и accel разнонаправлены
        boolean diverging = speed * accel < 0 && Math.abs(accel) > 0.00015;

        // Momentum падает 2 раза подряд
        boolean momentumFalling = false;
        if (recentMom.size() >= 3) {
            double last  = Math.abs(recentMom.get(recentMom.size() - 1));
            double prev  = Math.abs(recentMom.get(recentMom.size() - 2));
            double prev2 = Math.abs(recentMom.get(recentMom.size() - 3));
            momentumFalling = (last < prev * 0.75) && (prev < prev2 * 0.75);
        }

        return diverging || momentumFalling;
    }

    // ══════════════════════════════════════════════════════════════
    //  UPDATE FROM CANDLES
    // ══════════════════════════════════════════════════════════════

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

        // [FIX-BUG-2] Обновляем историю ATR для адаптивного капа
        if (candles.size() >= 15) {
            double atrPct = computeAtrPct(candles);
            if (atrPct > 0) {
                Deque<Double> atrHist = symbolAtrHistory.computeIfAbsent(symbol, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
                atrHist.addLast(atrPct);
                if (atrHist.size() > ATR_HISTORY_SIZE) atrHist.removeFirst();
            }
        }
    }

    /** [v23.0] Uses Wilder's ATR for consistency */
    private double computeAtrPct(List<com.bot.TradingCore.Candle> candles) {
        int n = candles.size();
        if (n < 15) return 0;
        double atr = com.bot.TradingCore.atr(candles, 14);
        double price = candles.get(n - 1).close;
        return price > 0 ? atr / price : 0;
    }

    // ══════════════════════════════════════════════════════════════
    //  ADJUST CONFIDENCE
    //  [FIX-BUG-2] Нормализация импульса теперь логарифмическая
    //  Это позволяет корректно различать +1% и +4% движения
    // ══════════════════════════════════════════════════════════════

    /**
     * [v23.0] RESTORED micro-momentum adjustConfidence.
     * v18.0 killed this method → bot stopped reacting to tick-level acceleration.
     * Now restored with TAMED influence: ±8 max (was ±30+).
     * This lets the bot "feel" the market moving in real-time.
     */
    public double adjustConfidence(com.bot.DecisionEngineMerged.TradeIdea signal) {
        MicroTrendResult mt = computeMicroTrend(signal.symbol);
        if (mt == null || mt.impulse < WEAK_IMPULSE) return signal.probability;

        double conf = signal.probability;
        boolean isLong = signal.side == com.bot.TradingCore.Side.LONG;
        boolean impulseAligned = (isLong && mt.smoothSpeed > 0) || (!isLong && mt.smoothSpeed < 0);
        boolean impulseOpposed = (isLong && mt.smoothSpeed < 0) || (!isLong && mt.smoothSpeed > 0);

        // Aligned micro-momentum → small boost (max +5)
        if (impulseAligned && mt.impulse > STRONG_IMPULSE) {
            double boost = Math.min(5.0, Math.log1p(mt.impulse / STRONG_IMPULSE) * 3.0);
            conf += boost;
        }
        // Opposed micro-momentum → penalty (max -8)
        if (impulseOpposed && mt.impulse > STRONG_IMPULSE) {
            double penalty = Math.min(8.0, Math.log1p(mt.impulse / STRONG_IMPULSE) * 4.0);
            conf -= penalty;
        }
        // Acceleration bonus: price accelerating in our direction
        if (impulseAligned && Math.abs(mt.accel) > ACCELERATION_THRESHOLD) {
            conf += Math.min(3.0, Math.abs(mt.accel) / ACCELERATION_THRESHOLD * 1.5);
        }
        // Exhaustion penalty: micro-trend is exhausted
        if (mt.isExhausted && impulseAligned) {
            conf -= 4.0;
        }

        return Math.max(MIN_CONF, Math.min(MAX_CONF, conf));
    }

    // ══════════════════════════════════════════════════════════════
    //  WITH ADJUSTED CONFIDENCE
    // ══════════════════════════════════════════════════════════════

    /**
     * [v23.0] Rebuild TradeIdea with micro-momentum adjusted confidence.
     */
    public com.bot.DecisionEngineMerged.TradeIdea withAdjustedConfidence(
            com.bot.DecisionEngineMerged.TradeIdea signal) {
        double adjusted = adjustConfidence(signal);
        if (Math.abs(adjusted - signal.probability) < 0.5) return signal;
        List<String> nf = new java.util.ArrayList<>(signal.flags);
        nf.add("μ" + String.format("%+.0f", adjusted - signal.probability));
        return new com.bot.DecisionEngineMerged.TradeIdea(
                signal.symbol, signal.side, signal.price, signal.stop, signal.take,
                signal.rr, adjusted, nf, signal.fundingRate, signal.fundingDelta,
                signal.oiChange, signal.htfBias, signal.category,
                signal.forecast, signal.tp1Mult, signal.tp2Mult, signal.tp3Mult);
    }

    // ══════════════════════════════════════════════════════════════
    //  MICRO ANALYSIS
    // ══════════════════════════════════════════════════════════════

    public MicroAnalysis analyzeMicroStructure(String symbol,
                                               List<com.bot.TradingCore.Candle> c1m,
                                               List<com.bot.TradingCore.Candle> c5m) {

        MicroTrendResult mt = computeMicroTrend(symbol);

        MicroAnalysis analysis = new MicroAnalysis();
        analysis.microTrend = mt;

        if (c1m != null && c1m.size() >= 20) {
            analysis.recentMomentum   = calculateCandleMomentum(c1m, 10);
            analysis.volumeTrend      = calculateVolumeTrend(c1m, 15);
            analysis.priceAcceleration = calculatePriceAcceleration(c1m, 8);
        }

        if (c5m != null && c5m.size() >= 15) {
            analysis.mediumMomentum = calculateCandleMomentum(c5m, 8);
        }

        if (mt != null) {
            boolean microUp = mt.smoothSpeed > 0;
            boolean m1Up    = analysis.recentMomentum > 0;
            boolean m5Up    = analysis.mediumMomentum > 0;

            int alignScore = 0;
            if (microUp) alignScore++;
            if (m1Up)    alignScore++;
            if (m5Up)    alignScore++;

            analysis.mtfAlignment  = (alignScore >= 2) ? 1 : (alignScore == 0) ? -1 : 0;
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
        public int    mtfAlignment;
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

        // [v34.0 FIX] Race condition: use toArray() snapshot instead of synchronized
        List<Double> recent = new ArrayList<>();
        try {
            Object[] snapshot = dq.toArray();
            int start = Math.max(0, snapshot.length - 40);
            for (int i = start; i < snapshot.length; i++) {
                recent.add((Double) snapshot[i]);
            }
        } catch (Exception e) {
            return false;
        }

        if (recent.size() < 25) return false;
        // [v34.0] Already in forward chronological order from toArray()

        double first = recent.get(recent.size() - 20);
        double last  = recent.get(recent.size() - 1);
        double movePct = Math.abs(last - first) / first;

        return movePct > 0.005;
    }

    public boolean detectMomentumDivergence(String symbol,
                                            List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 30) return false;

        MicroTrendResult mt = computeMicroTrend(symbol);
        if (mt == null) return false;

        int n = candles.size();
        double priceMove = candles.get(n - 1).close - candles.get(n - 15).close;
        boolean priceUp  = priceMove > 0;
        boolean momUp    = mt.momentum > 0;

        return priceUp != momUp && Math.abs(mt.momentum) > MOMENTUM_THRESHOLD * 0.5;
    }

    public void clearCacheForSymbol(String symbol) {
        microTrendCache.remove(symbol);
        priceFallback.remove(symbol);
        momentumHistory.remove(symbol);
        symbolAtrHistory.remove(symbol);
    }

    public void clearAllCache() {
        microTrendCache.clear();
        priceFallback.clear();
        momentumHistory.clear();
        symbolAtrHistory.clear();
    }

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}