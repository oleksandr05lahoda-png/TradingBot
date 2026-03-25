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

    private double computeAtrPct(List<com.bot.TradingCore.Candle> candles) {
        int n = candles.size();
        int period = Math.min(14, n - 1);
        if (period <= 0) return 0;
        double sum = 0;
        for (int i = n - period; i < n; i++) {
            com.bot.TradingCore.Candle cur  = candles.get(i);
            com.bot.TradingCore.Candle prev = candles.get(i - 1);
            sum += Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low  - prev.close)));
        }
        double atr = sum / period;
        double price = candles.get(n - 1).close;
        return price > 0 ? atr / price : 0;
    }

    // ══════════════════════════════════════════════════════════════
    //  ADJUST CONFIDENCE
    //  [FIX-BUG-2] Нормализация импульса теперь логарифмическая
    //  Это позволяет корректно различать +1% и +4% движения
    // ══════════════════════════════════════════════════════════════

    /**
     * [v7.0] TAMED adjustConfidence — уважает кластерную калибровку из DecisionEngine.
     *
     * Принцип: DecisionEngine даёт калиброванную probability (50-85%).
     * Optimizer корректирует на основе микро-тренда, НО:
     * - Максимальное ОБЩЕЕ изменение: ±12 единиц (было ±30+)
     * - Потолок: 85% (совпадает с DecisionEngine)
     * - Нет стекинга бонусов — каждый блок дополняет, но не перезаписывает
     * - Exhaustion = единственный случай сильного штрафа (-15 max)
     */
    public double adjustConfidence(com.bot.DecisionEngineMerged.TradeIdea signal) {
        // [v18.0 REFACTOR] Bypassed arbitrary confidence scaling.
        // The ForecastEngine and cluster models in DecisionEngineMerged now provide
        // the final probability. SignalOptimizer is relegated to micro-trend tracking.
        return signal.probability;
    }

    // ══════════════════════════════════════════════════════════════
    //  WITH ADJUSTED CONFIDENCE
    // ══════════════════════════════════════════════════════════════

    /**
     * [v14.0 FIX] Пересоздание TradeIdea с сохранением ВСЕХ полей.
     * БЫЛО: 3-arg конструктор терял rr, fundingDelta, category, forecast.
     */
    public com.bot.DecisionEngineMerged.TradeIdea withAdjustedConfidence(
            com.bot.DecisionEngineMerged.TradeIdea signal) {
        // [v18.0] Return unchanged signal since scaling is removed
        return signal;
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

    // ══════════════════════════════════════════════════════════════
    //  [v20.0] EARLY BOS REVERSAL DETECTION AFTER DUMP/PUMP
    //
    //  Problem: Bot waits for 15m candle close to generate signals.
    //  During fast dumps/pumps, by the time 15m closes, the move is DONE
    //  and the reversal has already started.
    //
    //  Solution: After PumpHunter detects a strong dump, we scan 1m candles
    //  for Break of Structure (BOS) — the first higher low after a series
    //  of lower lows = the dump is exhausting and reversal is starting.
    //
    //  This gives us an EARLY signal to either:
    //  1. Block late SHORT entries (dump already done)
    //  2. Provide early LONG reversal signal
    // ══════════════════════════════════════════════════════════════

    /**
     * Detects a Break of Structure (BOS) on 1m candles after a dump.
     * BOS = first higher low after 3+ lower lows → sellers losing control.
     *
     * @return EarlyReversalResult with detection info
     */
    public EarlyReversalResult detectEarlyReversalAfterDump(
            String symbol,
            List<com.bot.TradingCore.Candle> c1m,
            List<com.bot.TradingCore.Candle> c5m) {

        EarlyReversalResult result = new EarlyReversalResult();

        // Need PumpHunter to know if a dump just happened
        if (pumpHunter == null || c1m == null || c1m.size() < 30) {
            return result;
        }

        // Check if PumpHunter recently detected a dump (sell climax or strong dump)
        com.bot.PumpHunter.PumpEvent recentPump = pumpHunter.getRecentPump(symbol);
        boolean recentDump = recentPump != null && recentPump.isBearish()
                && recentPump.strength > 0.50
                && System.currentTimeMillis() - recentPump.timestamp < 10 * 60_000;

        boolean recentPumpUp = recentPump != null && recentPump.isBullish()
                && recentPump.strength > 0.50
                && System.currentTimeMillis() - recentPump.timestamp < 10 * 60_000;

        if (!recentDump && !recentPumpUp) {
            return result;
        }

        int n = c1m.size();

        if (recentDump) {
            // Look for BOS LONG — higher low after lower lows
            result = detectBosAfterDump(c1m, n);
            if (result.detected) {
                result.side = "LONG";
                result.flags.add("BOS_LONG_1M");
                // Check 5m confirmation
                if (c5m != null && c5m.size() >= 10) {
                    int n5 = c5m.size();
                    com.bot.TradingCore.Candle last5 = c5m.get(n5 - 1);
                    // If 5m candle is green (close > open) = confirmation of reversal
                    if (last5.close > last5.open) {
                        result.confidence += 0.15;
                        result.flags.add("5M_GREEN_CONFIRM");
                    }
                }
            }
        } else {
            // Look for BOS SHORT — lower high after higher highs
            result = detectBosAfterPump(c1m, n);
            if (result.detected) {
                result.side = "SHORT";
                result.flags.add("BOS_SHORT_1M");
                if (c5m != null && c5m.size() >= 10) {
                    int n5 = c5m.size();
                    com.bot.TradingCore.Candle last5 = c5m.get(n5 - 1);
                    if (last5.close < last5.open) {
                        result.confidence += 0.15;
                        result.flags.add("5M_RED_CONFIRM");
                    }
                }
            }
        }

        return result;
    }

    private EarlyReversalResult detectBosAfterDump(List<com.bot.TradingCore.Candle> c1m, int n) {
        EarlyReversalResult result = new EarlyReversalResult();

        // Find consecutive lower lows in the last 15 bars
        int lowerLowCount = 0;
        double lowestLow = Double.MAX_VALUE;
        int lowestLowIdx = -1;

        for (int i = Math.max(1, n - 15); i < n; i++) {
            if (c1m.get(i).low < c1m.get(i - 1).low) {
                lowerLowCount++;
                if (c1m.get(i).low < lowestLow) {
                    lowestLow = c1m.get(i).low;
                    lowestLowIdx = i;
                }
            }
        }

        // Need at least 3 lower lows to qualify as a downtrend
        if (lowerLowCount < 3 || lowestLowIdx < 0 || lowestLowIdx >= n - 1) {
            return result;
        }

        // Now check if AFTER the lowest low, we see a higher low = BOS
        boolean higherLowFound = false;
        for (int i = lowestLowIdx + 1; i < n; i++) {
            if (c1m.get(i).low > lowestLow) {
                // Check if we also have a green candle (bullish reversal bar)
                if (c1m.get(i).close > c1m.get(i).open) {
                    higherLowFound = true;
                    result.flags.add("HL_BAR_" + (n - i) + "_AGO");
                }
            }
        }

        if (higherLowFound) {
            result.detected = true;
            result.confidence = 0.55 + Math.min(0.20, lowerLowCount * 0.04);
            // More lower lows before the BOS = stronger reversal signal
        }

        return result;
    }

    private EarlyReversalResult detectBosAfterPump(List<com.bot.TradingCore.Candle> c1m, int n) {
        EarlyReversalResult result = new EarlyReversalResult();

        // Find consecutive higher highs in the last 15 bars
        int higherHighCount = 0;
        double highestHigh = Double.MIN_VALUE;
        int highestHighIdx = -1;

        for (int i = Math.max(1, n - 15); i < n; i++) {
            if (c1m.get(i).high > c1m.get(i - 1).high) {
                higherHighCount++;
                if (c1m.get(i).high > highestHigh) {
                    highestHigh = c1m.get(i).high;
                    highestHighIdx = i;
                }
            }
        }

        if (higherHighCount < 3 || highestHighIdx < 0 || highestHighIdx >= n - 1) {
            return result;
        }

        boolean lowerHighFound = false;
        for (int i = highestHighIdx + 1; i < n; i++) {
            if (c1m.get(i).high < highestHigh) {
                if (c1m.get(i).close < c1m.get(i).open) {
                    lowerHighFound = true;
                    result.flags.add("LH_BAR_" + (n - i) + "_AGO");
                }
            }
        }

        if (lowerHighFound) {
            result.detected = true;
            result.confidence = 0.55 + Math.min(0.20, higherHighCount * 0.04);
        }

        return result;
    }

    /**
     * [v20.0] Result of early reversal detection after dump/pump.
     */
    public static class EarlyReversalResult {
        public boolean detected = false;
        public String side = "NONE";       // "LONG" or "SHORT" or "NONE"
        public double confidence = 0.0;     // 0.0 .. 1.0
        public List<String> flags = new ArrayList<>();

        @Override
        public String toString() {
            return String.format("EarlyReversal{detected=%s side=%s conf=%.2f flags=%s}",
                    detected, side, confidence, flags);
        }
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