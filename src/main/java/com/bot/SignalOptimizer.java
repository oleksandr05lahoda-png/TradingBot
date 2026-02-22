package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class SignalOptimizer {

    /* ================= CONFIG ================= */
    private static final int MAX_TICKS = 100;
    private static final int MIN_TICKS = 8;

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
    private final TradingCore.AdaptiveBrain adaptiveBrain;

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque,
                           TradingCore.AdaptiveBrain adaptiveBrain) {
        this.tickPriceDeque = tickPriceDeque;
        this.adaptiveBrain = adaptiveBrain;
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

    /* ============================================================
       MICRO TREND CALCULATION (EMA SPEED + EMA ACCELERATION + REVERSAL DETECTION)
       ============================================================ */
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
       CONFIDENCE ADJUSTMENT (IMPROVED FOR REVERSALS AND TYPES)
       ============================================================ */
    public double adjustConfidence(Elite5MinAnalyzer.TradeSignal signal) {

        // Проверка на тайминг обновления сигнала
        long now = System.currentTimeMillis();
        Long last = lastSignalTimestamp.get(signal.symbol);
        if (last != null && now - last < SIGNAL_REFRESH_MS) {
            return signal.confidence;
        }
        lastSignalTimestamp.put(signal.symbol, now);

        MicroTrendResult mt = microTrendCache.get(signal.symbol);
        if (mt == null) return signal.confidence;

        double confidence = signal.confidence;
        boolean isLong = signal.side == TradingCore.Side.LONG;
        boolean trendUp = mt.speed > 0;

        Elite5MinAnalyzer.CoinType type = detectEliteCoinType(signal);

        // ===== IMPULSE / REVERSAL LOGIC =====
        double reversalFactor = computeReversalFactor(signal, mt, type);

        confidence += reversalFactor;

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

    private double computeReversalFactor(Elite5MinAnalyzer.TradeSignal signal,
                                         MicroTrendResult mt,
                                         Elite5MinAnalyzer.CoinType type) {
        boolean isLong = signal.side == TradingCore.Side.LONG;
        boolean trendUp = mt.speed > 0;

        double factor = 0.0;

        // Сильный импульс
        if (mt.impulse > STRONG_IMPULSE) {
            if ((isLong && trendUp) || (!isLong && !trendUp)) factor += 0.07;
            else factor -= 0.06;
        }
        // Слабый импульс
        else if (mt.impulse > WEAK_IMPULSE) {
            if ((isLong && trendUp) || (!isLong && !trendUp)) factor += 0.03;
            else factor -= 0.03;
        }

        // Альткойны: ранние развороты по локальной дивергенции
        if (type == Elite5MinAnalyzer.CoinType.ALT && Math.signum(mt.speed) != Math.signum(mt.accel)) {
            factor += 0.05; // умный разворот
        }

        // MEME: быстрые входы по ускорению
        if (type == Elite5MinAnalyzer.CoinType.MEME && Math.abs(mt.accel) > 0.0008) {
            factor += 0.04;
        }

        return factor;
    }

    /* ============================================================
       STOP / TAKE OPTIMIZATION (ATR + DYNAMIC RR)
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
                detectEliteCoinType(signal)
        );
    }

    /* ============================================================
       HELPERS
       ============================================================ */
    private Elite5MinAnalyzer.CoinType detectEliteCoinType(Elite5MinAnalyzer.TradeSignal s) {
        String sym = s.symbol.toUpperCase();
        if (sym.contains("PEPE") || sym.contains("DOGE") || sym.contains("SHIB"))
            return Elite5MinAnalyzer.CoinType.MEME;
        if (sym.contains("BTC") || sym.contains("ETH"))
            return Elite5MinAnalyzer.CoinType.TOP;
        return Elite5MinAnalyzer.CoinType.ALT;
    }

    private TradingCore.CoinType detectTradingCoreCoinType(Elite5MinAnalyzer.TradeSignal s) {
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