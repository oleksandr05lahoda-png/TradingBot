package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class SignalOptimizer {

    private static final int MAX_TICKS = 60;
    private static final double ALPHA = 0.35;
    private static final double IMPULSE_STRONG = 0.0014;

    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();
    private final TradingCore.AdaptiveBrain adaptiveBrain;

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque,
                           TradingCore.AdaptiveBrain adaptiveBrain) {
        this.tickPriceDeque = tickPriceDeque;
        this.adaptiveBrain = adaptiveBrain;
    }

    public static final class MicroTrendResult {
        public final double speed, accel, avg;

        public MicroTrendResult(double speed, double accel, double avg) {
            this.speed = speed;
            this.accel = accel;
            this.avg = avg;
        }

        @Override
        public String toString() {
            return String.format("MicroTrend[speed=%.6f, accel=%.6f, avg=%.6f]", speed, accel, avg);
        }
    }

    /** Вычисляет микротренд на основании последних MAX_TICKS тиков */
    public MicroTrendResult computeMicroTrend(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < 6) return new MicroTrendResult(0, 0, 0);

        List<Double> prices = new ArrayList<>(dq);
        int size = prices.size();
        int n = Math.min(size, MAX_TICKS);

        double speed = 0, accel = 0;
        for (int i = size - n + 1; i < size; i++) {
            double diff = prices.get(i) - prices.get(i - 1);
            double prevSpeed = speed;
            speed = ALPHA * diff + (1 - ALPHA) * speed;
            accel = ALPHA * (speed - prevSpeed) + (1 - ALPHA) * accel;
        }

        double avg = prices.subList(size - n, size).stream().mapToDouble(d -> d).average()
                .orElse(prices.get(size - 1));

        MicroTrendResult result = new MicroTrendResult(speed, accel, avg);
        microTrendCache.put(symbol, result);
        return result;
    }

    /** Корректировка confidence с учетом микротренда и адаптивного мозга */
    public double adjustConfidence(Elite5MinAnalyzer.TradeSignal s) {
        double conf = s.confidence;
        MicroTrendResult mt = microTrendCache.get(s.symbol);
        if (mt != null) {
            double impulse = Math.abs(mt.speed) + Math.abs(mt.accel);
            if (impulse > IMPULSE_STRONG) conf += 0.05; // бонус за сильный микротренд
        }

        if (adaptiveBrain != null) {
            conf = adaptiveBrain.applyAllAdjustments(
                    "ELITE5",
                    s.symbol,
                    conf,
                    detectTradingCoreCoinType(s),
                    true,
                    false
            );
        }

        return clamp(conf, 0.40, 0.97);
    }

    /** Пересчитывает stop и take под ATR и confidence */
    public Elite5MinAnalyzer.TradeSignal withAdjustedStopTake(
            Elite5MinAnalyzer.TradeSignal s, double atr) {

        double volPct = clamp(atr / s.entry, 0.007, 0.045); // корректировка под волатильность
        double rr = s.confidence > 0.80 ? 2.8 :
                s.confidence > 0.70 ? 2.3 :
                        s.confidence > 0.60 ? 1.9 : 1.6;

        double stop = s.side == TradingCore.Side.LONG ? s.entry * (1 - volPct)
                : s.entry * (1 + volPct);
        double take = s.side == TradingCore.Side.LONG ? s.entry * (1 + volPct * rr)
                : s.entry * (1 - volPct * rr);

        return new Elite5MinAnalyzer.TradeSignal(
                s.symbol,
                s.side,
                s.entry,
                stop,
                take,
                adjustConfidence(s),
                s.grade,
                s.reason,
                convertToEliteCoinType(detectTradingCoreCoinType(s))
        );
    }

    private Elite5MinAnalyzer.CoinType convertToEliteCoinType(TradingCore.CoinType type) {
        return switch (type) {
            case TOP -> Elite5MinAnalyzer.CoinType.TOP;
            case ALT -> Elite5MinAnalyzer.CoinType.ALT;
            case MEME -> Elite5MinAnalyzer.CoinType.MEME;
        };
    }

    private TradingCore.CoinType detectTradingCoreCoinType(Elite5MinAnalyzer.TradeSignal s) {
        String sym = s.symbol.toUpperCase();
        if (sym.contains("MEME")) return TradingCore.CoinType.MEME;
        if (sym.contains("ALT")) return TradingCore.CoinType.ALT;
        return TradingCore.CoinType.TOP;
    }

    /** Безопасный clamp */
    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}