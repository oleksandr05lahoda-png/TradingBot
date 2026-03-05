package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * InstitutionalSignalCore - профессиональный менеджер сигналов для торгового бота
 * Управление множественными сигналами, динамическое exposure, TTL, история сделок,
 * скоры символов, winrate, уникальность сигналов, контроль реверсов.
 */
public final class InstitutionalSignalCore {

    /* =========================================================
       CONFIGURATION
       ========================================================= */

    private final int maxGlobalSignals;        // Максимум активных сигналов глобально
    private final int maxSignalsPerSymbol;     // Максимум сигналов на один символ
    private final double maxPortfolioExposure; // Максимум общего exposure
    private final double minConfidence;        // Минимальная вероятность для принятия сигнала
    private final double minSignalDiff;        // Минимальная дистанция между сигналами по цене
    private final long signalTtlMs;            // Время жизни сигнала в миллисекундах
    private final double scoreThreshold;       // Минимальный score символа для разрешения сигнала

    public InstitutionalSignalCore(int maxGlobalSignals,
                                   int maxSignalsPerSymbol,
                                   double maxPortfolioExposure,
                                   double minConfidence,
                                   double minSignalDiff,
                                   long signalTtlMs,
                                   double scoreThreshold) {

        this.maxGlobalSignals = maxGlobalSignals;
        this.maxSignalsPerSymbol = maxSignalsPerSymbol;
        this.maxPortfolioExposure = maxPortfolioExposure;
        this.minConfidence = minConfidence;
        this.minSignalDiff = minSignalDiff;
        this.signalTtlMs = signalTtlMs;
        this.scoreThreshold = scoreThreshold;
    }

    /* =========================================================
       STATE
       ========================================================= */

    private final Map<String, List<ActiveSignal>> activeSignals = new ConcurrentHashMap<>();
    private final Map<String, List<ClosedTrade>> history = new ConcurrentHashMap<>();
    private final Map<String, Double> symbolScore = new ConcurrentHashMap<>();
    private final Map<String, List<Double>> symbolConfidenceHistory = new ConcurrentHashMap<>();

    private volatile double currentExposure = 0.0;

    /* =========================================================
       MODELS
       ========================================================= */

    public static final class ActiveSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double probability;
        public final long timestamp;
        public final double atr;             // ATR на момент сигнала
        public final List<String> flags;     // Флаги сигнала для оптимизации

        public ActiveSignal(String symbol,
                            TradingCore.Side side,
                            double entry,
                            double probability,
                            long timestamp,
                            double atr,
                            List<String> flags) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.probability = probability;
            this.timestamp = timestamp;
            this.atr = atr;
            this.flags = flags != null ? flags : List.of();
        }
    }

    public static final class ClosedTrade {
        public final String symbol;
        public final double pnl;
        public final long duration;
        public final long closeTimestamp;
        public final TradingCore.Side side;
        public final double entry;
        public final double exit;

        public ClosedTrade(String symbol,
                           double pnl,
                           long duration,
                           long closeTimestamp,
                           TradingCore.Side side,
                           double entry,
                           double exit) {
            this.symbol = symbol;
            this.pnl = pnl;
            this.duration = duration;
            this.closeTimestamp = closeTimestamp;
            this.side = side;
            this.entry = entry;
            this.exit = exit;
        }
    }

    /* =========================================================
       MAIN FILTER
       ========================================================= */

    public synchronized boolean allowSignal(DecisionEngineMerged.TradeIdea signal) {

        cleanupExpiredSignals();

        if (signal == null) return false;

        if (signal.probability < minConfidence + 2.0)
            return false;

        if (getActiveSignalsCount() >= maxGlobalSignals)
            return false;

        if (currentExposure >= maxPortfolioExposure)
            return false;

        double score = symbolScore.getOrDefault(signal.symbol, 0.0);
        if (score < scoreThreshold)
            return false;

        List<ActiveSignal> list =
                activeSignals.getOrDefault(signal.symbol, List.of());

        if (list.size() >= maxSignalsPerSymbol)
            return false;

        for (ActiveSignal a : list) {

            if (a.side != signal.side)
                return false;

            double diff = Math.abs(a.entry - signal.price) / a.entry;
            if (diff < minSignalDiff)
                return false;

            if (Math.abs(a.probability - signal.probability) < 0.015)
                return false;
        }

        double estimatedExposure = estimateExposure(signal);
        if (currentExposure + estimatedExposure > maxPortfolioExposure)
            return false;

        return true;
    }

    /* =========================================================
       REGISTER SIGNAL
       ========================================================= */

    public synchronized void registerSignal(DecisionEngineMerged.TradeIdea signal, double atr) {

        long now = System.currentTimeMillis();

        ActiveSignal active = new ActiveSignal(
                signal.symbol,
                signal.side,
                signal.price,
                signal.probability,
                now,
                atr,
                signal.flags
        );

        activeSignals
                .computeIfAbsent(signal.symbol,
                        k -> new CopyOnWriteArrayList<>())
                .add(active);

        currentExposure = clamp(
                currentExposure + estimateExposure(signal),
                0.0,
                maxPortfolioExposure
        );

        // История confidence
        symbolConfidenceHistory
                .computeIfAbsent(signal.symbol, k -> new ArrayList<>())
                .add(signal.probability);
    }

    /* =========================================================
       CLOSE TRADE
       ========================================================= */

    public synchronized void closeTrade(String symbol,
                                        TradingCore.Side side,
                                        double entry,
                                        double exit) {

        List<ActiveSignal> list = activeSignals.get(symbol);
        if (list == null) return;

        long now = System.currentTimeMillis();

        Iterator<ActiveSignal> it = list.iterator();
        while (it.hasNext()) {
            ActiveSignal s = it.next();
            if (s.side != side || s.entry != entry) continue;

            double pnlPercent = (side == TradingCore.Side.LONG) ?
                    (exit - entry) / entry * 100 :
                    (entry - exit) / entry * 100;

            history
                    .computeIfAbsent(symbol,
                            k -> new CopyOnWriteArrayList<>())
                    .add(new ClosedTrade(
                            symbol,
                            pnlPercent,
                            now - s.timestamp,
                            now,
                            side,
                            entry,
                            exit));

            currentExposure = clamp(
                    currentExposure - estimateExposure(s),
                    0.0,
                    maxPortfolioExposure
            );

            updateSymbolScore(symbol, pnlPercent);

            it.remove();
        }

        if (list.isEmpty())
            activeSignals.remove(symbol);
    }

    /* =========================================================
       AUTO CLEANUP
       ========================================================= */

    private void cleanupExpiredSignals() {

        long now = System.currentTimeMillis();

        for (Iterator<Map.Entry<String,
                List<ActiveSignal>>> it =
             activeSignals.entrySet().iterator();
             it.hasNext();) {

            Map.Entry<String, List<ActiveSignal>> e = it.next();

            List<ActiveSignal> list = e.getValue();

            list.removeIf(s -> now - s.timestamp > signalTtlMs);

            if (list.isEmpty())
                it.remove();
        }

        recalcExposure();
    }

    private void recalcExposure() {

        double exposure = 0.0;

        for (List<ActiveSignal> list : activeSignals.values()) {
            for (ActiveSignal s : list)
                exposure += estimateExposure(s);
        }

        currentExposure = clamp(exposure, 0.0, maxPortfolioExposure);
    }

    /* =========================================================
       SYMBOL PERFORMANCE SCORE
       ========================================================= */

    private void updateSymbolScore(String symbol,
                                   double pnl) {

        double delta =
                pnl > 0 ? 0.02 :
                        pnl < 0 ? -0.025 :
                                -0.005;

        symbolScore.merge(symbol, delta, Double::sum);

        symbolScore.compute(symbol,
                (k, v) -> clamp(v, -0.40, 0.40));
    }

    /* =========================================================
       EXPOSURE MODEL
       ========================================================= */

    private double estimateExposure(DecisionEngineMerged.TradeIdea s) {

        if (s.probability >= 85) return 0.05;
        if (s.probability >= 75) return 0.035;
        if (s.probability >= 65) return 0.025;
        return 0.02;
    }

    private double estimateExposure(ActiveSignal s) {

        if (s.probability >= 85) return 0.05;
        if (s.probability >= 75) return 0.035;
        if (s.probability >= 65) return 0.025;
        return 0.02;
    }

    /* =========================================================
       STATS API
       ========================================================= */

    public synchronized int getActiveSignalsCount() {
        return activeSignals.values()
                .stream()
                .mapToInt(List::size)
                .sum();
    }

    public synchronized double getCurrentExposure() {
        return currentExposure;
    }

    public double getSymbolScore(String symbol) {
        return symbolScore.getOrDefault(symbol, 0.0);
    }

    public List<ClosedTrade> getHistory(String symbol) {
        return history.getOrDefault(symbol, List.of());
    }

    public double getWinRate(String symbol) {
        List<ClosedTrade> h = history.get(symbol);
        if (h == null || h.isEmpty()) return 0.0;
        long wins = h.stream().filter(t -> t.pnl > 0).count();
        return (double) wins / h.size();
    }

    public List<Double> getConfidenceHistory(String symbol) {
        return symbolConfidenceHistory.getOrDefault(symbol, List.of());
    }

    /* =========================================================
       UTILS
       ========================================================= */

    private static double clamp(double v,
                                double min,
                                double max) {
        return Math.max(min, Math.min(max, v));
    }
}