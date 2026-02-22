package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SignalOptimizer — улучшенный класс для оценки сигналов Elite5MinAnalyzer
 * с микротрендом, EMA скорости/ускорения, импульсами и адаптивной корректировкой confidence.
 */
public final class SignalOptimizer {

    /* ================= CONFIG ================= */
    private static final int MAX_TICKS = 80;
    private static final int MIN_TICKS = 8;

    private static final double EMA_ALPHA = 0.32;
    private static final double STRONG_IMPULSE = 0.0015;
    private static final double WEAK_IMPULSE   = 0.0006;

    private static final double MAX_CONF = 0.97;
    private static final double MIN_CONF = 0.40;

    /* ================= STATE ================= */
    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();
    private final TradingCore.AdaptiveBrain adaptiveBrain;

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque,
                           TradingCore.AdaptiveBrain adaptiveBrain) {
        this.tickPriceDeque = tickPriceDeque;
        this.adaptiveBrain = adaptiveBrain;
    }

    /* ================= DATA CLASS ================= */
    public static final class MicroTrendResult {
        public final double speed;      // EMA скорости изменения цены
        public final double accel;      // EMA ускорения (изменение скорости)
        public final double avg;        // Среднее по буферу
        public final double impulse;    // Сумма абсолютных значений speed и accel

        public MicroTrendResult(double speed, double accel, double avg) {
            this.speed = speed;
            this.accel = accel;
            this.avg = avg;
            this.impulse = Math.abs(speed) + Math.abs(accel);
        }
    }

    private static final class MicroTrendResultZero {
        private static final MicroTrendResult INSTANCE =
                new MicroTrendResult(0, 0, 0);
    }

    /* ============================================================
       MICRO TREND CALCULATION (EMA SPEED + EMA ACCELERATION)
       ============================================================ */
    public MicroTrendResult computeMicroTrend(String symbol) {

        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < MIN_TICKS)
            return MicroTrendResultZero.INSTANCE;

        double speed = 0.0;
        double accel = 0.0;
        double prevPrice = 0.0;
        double sum = 0.0;
        int processed = 0;

        // собираем последние MAX_TICKS цены
        Iterator<Double> it = dq.descendingIterator();
        List<Double> buffer = new ArrayList<>(MAX_TICKS);
        while (it.hasNext() && buffer.size() < MAX_TICKS) {
            buffer.add(it.next());
        }
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

        double avg = sum / processed;

        MicroTrendResult result = new MicroTrendResult(speed, accel, avg);
        microTrendCache.put(symbol, result);

        return result;
    }

    /* ============================================================
       CONFIDENCE ADJUSTMENT
       ============================================================ */
    public double adjustConfidence(Elite5MinAnalyzer.TradeSignal signal) {

        MicroTrendResult mt = microTrendCache.get(signal.symbol);
        if (mt == null) return signal.confidence;

        double confidence = signal.confidence;

        boolean isLong = signal.side == TradingCore.Side.LONG;
        boolean trendUp = mt.speed > 0;

        // ===== IMPULSE LOGIC =====
        if (mt.impulse > STRONG_IMPULSE) {
            if ((isLong && trendUp) || (!isLong && !trendUp)) confidence += 0.07;
            else confidence -= 0.06;
        } else if (mt.impulse > WEAK_IMPULSE) {
            if ((isLong && trendUp) || (!isLong && !trendUp)) confidence += 0.03;
            else confidence -= 0.03;
        }

        // ===== ADAPTIVE BRAIN =====
        if (adaptiveBrain != null) {
            confidence = adaptiveBrain.applyAllAdjustments(
                    "ELITE5",
                    signal.symbol,
                    confidence,
                    detectTradingCoreCoinType(signal),
                    true,
                    false
            );
        }

        return clamp(confidence, MIN_CONF, MAX_CONF);
    }

    /* ============================================================
       STOP / TAKE OPTIMIZATION (ATR ADAPTIVE)
       ============================================================ */
    public Elite5MinAnalyzer.TradeSignal withAdjustedStopTake(
            Elite5MinAnalyzer.TradeSignal signal,
            double atr) {

        double newConfidence = adjustConfidence(signal);

        double volatilityPct = clamp(atr / signal.entry, 0.006, 0.035);

        double rr = newConfidence > 0.85 ? 3.2 :
                newConfidence > 0.75 ? 2.6 :
                        newConfidence > 0.65 ? 2.1 : 1.7;

        double stop, take;

        if (signal.side == TradingCore.Side.LONG) {
            stop = signal.entry * (1 - volatilityPct);
            take = signal.entry * (1 + volatilityPct * rr);
        } else {
            stop = signal.entry * (1 + volatilityPct);
            take = signal.entry * (1 - volatilityPct * rr);
        }

        return new Elite5MinAnalyzer.TradeSignal(
                signal.symbol,
                signal.side,
                signal.entry,
                stop,
                take,
                newConfidence,
                signal.grade,
                signal.reason,
                convertToEliteCoinType(detectTradingCoreCoinType(signal))
        );
    }

    /* ============================================================
       HELPERS
       ============================================================ */
    private Elite5MinAnalyzer.CoinType convertToEliteCoinType(TradingCore.CoinType type) {
        return switch (type) {
            case TOP  -> Elite5MinAnalyzer.CoinType.TOP;
            case ALT  -> Elite5MinAnalyzer.CoinType.ALT;
            case MEME -> Elite5MinAnalyzer.CoinType.MEME;
        };
    }

    private TradingCore.CoinType detectTradingCoreCoinType(
            Elite5MinAnalyzer.TradeSignal s) {

        String sym = s.symbol.toUpperCase();

        if (sym.contains("PEPE") || sym.contains("DOGE") || sym.contains("SHIB"))
            return TradingCore.CoinType.MEME;

        if (sym.contains("BTC") || sym.contains("ETH"))
            return TradingCore.CoinType.TOP;

        return TradingCore.CoinType.ALT;
    }

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}