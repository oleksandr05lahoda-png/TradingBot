package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class InstitutionalSignalCore {

    /* =========================================================
       CONFIGURATION
       ========================================================= */

    private final int maxGlobalSignals;
    private final int maxSignalsPerSymbol;
    private final double maxPortfolioExposure;
    private final double minConfidence;
    private final double minSignalDiff;
    private final long signalTtlMs;

    public InstitutionalSignalCore(int maxGlobalSignals,
                                   int maxSignalsPerSymbol,
                                   double maxPortfolioExposure,
                                   double minConfidence,
                                   double minSignalDiff,
                                   long signalTtlMs) {

        this.maxGlobalSignals = maxGlobalSignals;
        this.maxSignalsPerSymbol = maxSignalsPerSymbol;
        this.maxPortfolioExposure = maxPortfolioExposure;
        this.minConfidence = minConfidence;
        this.minSignalDiff = minSignalDiff;
        this.signalTtlMs = signalTtlMs;
    }

    /* =========================================================
       STATE
       ========================================================= */

    private final Map<String, List<ActiveSignal>> activeSignals = new ConcurrentHashMap<>();
    private final Map<String, List<ClosedTrade>> history = new ConcurrentHashMap<>();
    private final Map<String, Double> symbolScore = new ConcurrentHashMap<>();

    private volatile double currentExposure = 0.0;

    /* =========================================================
       MODELS
       ========================================================= */

    public static final class ActiveSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double confidence;
        public final long timestamp;

        public ActiveSignal(String symbol,
                            TradingCore.Side side,
                            double entry,
                            double confidence,
                            long timestamp) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.confidence = confidence;
            this.timestamp = timestamp;
        }
    }

    public static final class ClosedTrade {
        public final String symbol;
        public final double pnl;
        public final long duration;

        public ClosedTrade(String symbol,
                           double pnl,
                           long duration) {
            this.symbol = symbol;
            this.pnl = pnl;
            this.duration = duration;
        }
    }

    /* =========================================================
       MAIN FILTER
       ========================================================= */

    public synchronized boolean allowSignal(DecisionEngineMerged.TradeIdea signal) {

        cleanupExpiredSignals();

        if (signal == null) return false;

        if (signal.confidence < minConfidence)
            return false;

        if (getActiveSignalsCount() >= maxGlobalSignals)
            return false;

        if (currentExposure >= maxPortfolioExposure)
            return false;

        double score = symbolScore.getOrDefault(signal.symbol, 0.0);
        if (score < -0.20)
            return false;

        List<ActiveSignal> list =
                activeSignals.getOrDefault(signal.symbol, List.of());

        if (list.size() >= maxSignalsPerSymbol)
            return false;

        for (ActiveSignal a : list) {

            if (a.side != signal.side)
                return false;

            double diff = Math.abs(a.entry - signal.entry) / a.entry;
            if (diff < minSignalDiff)
                return false;

            if (Math.abs(a.confidence - signal.confidence) < 0.015)
                return false;
        }

        double estimatedExposure = estimateExposure(signal);
        if (currentExposure + estimatedExposure > maxPortfolioExposure)
            return false;

        return true;
    }

    /* =========================================================
       REGISTER
       ========================================================= */

    public synchronized void registerSignal(DecisionEngineMerged.TradeIdea signal) {

        long now = System.currentTimeMillis();

        ActiveSignal active = new ActiveSignal(
                signal.symbol,
                signal.side,
                signal.entry,
                signal.confidence,
                now
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
    }

    /* =========================================================
       CLOSE TRADE
       ========================================================= */

    public synchronized void closeTrade(String symbol,
                                        double pnlPercent) {

        List<ActiveSignal> list = activeSignals.remove(symbol);
        if (list == null) return;

        long now = System.currentTimeMillis();

        for (ActiveSignal s : list) {

            history
                    .computeIfAbsent(symbol,
                            k -> new CopyOnWriteArrayList<>())
                    .add(new ClosedTrade(
                            symbol,
                            pnlPercent,
                            now - s.timestamp));

            currentExposure = clamp(
                    currentExposure - estimateExposure(s),
                    0.0,
                    maxPortfolioExposure);

            updateSymbolScore(symbol, pnlPercent);
        }
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
                pnl > 0 ? 0.025 :
                        pnl < 0 ? -0.035 :
                                -0.01;

        symbolScore.merge(symbol, delta, Double::sum);

        symbolScore.compute(symbol,
                (k, v) -> clamp(v, -0.40, 0.40));
    }

    /* =========================================================
       EXPOSURE MODEL
       ========================================================= */

    private double estimateExposure(
            DecisionEngineMerged.TradeIdea s) {

        if (s.confidence > 0.85) return 0.05;
        if (s.confidence > 0.75) return 0.035;
        if (s.confidence > 0.65) return 0.025;
        return 0.02;
    }

    private double estimateExposure(ActiveSignal s) {

        if (s.confidence > 0.85) return 0.05;
        if (s.confidence > 0.75) return 0.035;
        if (s.confidence > 0.65) return 0.025;
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

    /* =========================================================
       UTIL
       ========================================================= */

    private static double clamp(double v,
                                double min,
                                double max) {
        return Math.max(min, Math.min(max, v));
    }
}