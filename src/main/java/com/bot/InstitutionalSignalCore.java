package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class InstitutionalSignalCore {

    /* =========================================================
       CONFIG
       ========================================================= */

    private final int maxGlobalSignals;
    private final int maxSignalsPerSymbol;
    private final double maxPortfolioExposure;
    private final long cooldownMs;
    private final double minConfidence;
    private final double minSignalDiff;

    public InstitutionalSignalCore(int maxGlobalSignals,
                                   int maxSignalsPerSymbol,
                                   double maxPortfolioExposure,
                                   long cooldownMs,
                                   double minConfidence,
                                   double minSignalDiff) {

        this.maxGlobalSignals = maxGlobalSignals;
        this.maxSignalsPerSymbol = maxSignalsPerSymbol;
        this.maxPortfolioExposure = maxPortfolioExposure;
        this.cooldownMs = cooldownMs;
        this.minConfidence = minConfidence;
        this.minSignalDiff = minSignalDiff;
    }

    /* =========================================================
       STATE STORAGE
       ========================================================= */

    private final Map<String, List<ActiveSignal>> activeSignals = new ConcurrentHashMap<>();
    private final Map<String, Long> cooldownMap = new ConcurrentHashMap<>();
    private final Map<String, List<ClosedTrade>> history = new ConcurrentHashMap<>();
    private final Map<String, Double> symbolScore = new ConcurrentHashMap<>();

    private double currentExposure = 0.0;

    /* =========================================================
       DATA MODELS
       ========================================================= */

    public static final class ActiveSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double confidence;
        public final long time;

        public ActiveSignal(String symbol,
                            TradingCore.Side side,
                            double entry,
                            double confidence,
                            long time) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.confidence = confidence;
            this.time = time;
        }
    }

    public static final class ClosedTrade {
        public final String symbol;
        public final double pnl;
        public final long duration;

        public ClosedTrade(String symbol, double pnl, long duration) {
            this.symbol = symbol;
            this.pnl = pnl;
            this.duration = duration;
        }
    }

    /* =========================================================
       MAIN FILTER
       ========================================================= */

    public boolean allowSignal(DecisionEngineMerged.TradeIdea signal) {

        long now = System.currentTimeMillis();

        /* ---------- confidence filter ---------- */

        if (signal.confidence < minConfidence)
            return false;

        /* ---------- global limit ---------- */

        int totalSignals = activeSignals.values().stream()
                .mapToInt(List::size)
                .sum();

        if (totalSignals >= maxGlobalSignals)
            return false;

        /* ---------- cooldown ---------- */

        Long last = cooldownMap.get(signal.symbol);
        if (last != null && now - last < cooldownMs)
            return false;

        /* ---------- symbol performance bias ---------- */

        double score = symbolScore.getOrDefault(signal.symbol, 0.0);
        if (score < -0.15)
            return false;

        /* ---------- exposure limit ---------- */

        if (currentExposure >= maxPortfolioExposure)
            return false;

        /* ---------- per symbol limit ---------- */

        List<ActiveSignal> list = activeSignals.get(signal.symbol);
        if (list != null && list.size() >= maxSignalsPerSymbol)
            return false;

        /* ---------- conflict resolver ---------- */

        if (list != null) {
            for (ActiveSignal a : list) {

                if (a.side != signal.side)
                    return false;

                double diff = Math.abs(a.entry - signal.entry) / a.entry;

                if (diff < minSignalDiff)
                    return false;

                if (Math.abs(a.confidence - signal.confidence) < 0.02)
                    return false;
            }
        }

        return true;
    }

    /* =========================================================
       REGISTER SIGNAL
       ========================================================= */

    public void registerSignal(DecisionEngineMerged.TradeIdea signal) {

        long now = System.currentTimeMillis();

        ActiveSignal s = new ActiveSignal(
                signal.symbol,
                signal.side,
                signal.entry,
                signal.confidence,
                now
        );

        activeSignals
                .computeIfAbsent(signal.symbol, k -> new ArrayList<>())
                .add(s);

        cooldownMap.put(signal.symbol, now);

        currentExposure += estimateExposure(signal);
    }

    /* =========================================================
       CLOSE TRADE
       ========================================================= */

    public void closeTrade(String symbol, double pnlPercent) {

        List<ActiveSignal> list = activeSignals.remove(symbol);
        if (list == null) return;

        long now = System.currentTimeMillis();

        for (ActiveSignal s : list) {

            history
                    .computeIfAbsent(symbol, k -> new ArrayList<>())
                    .add(new ClosedTrade(
                            symbol,
                            pnlPercent,
                            now - s.time
                    ));

            currentExposure -= estimateExposure(s);

            updateSymbolScore(symbol, pnlPercent);
        }
    }

    /* =========================================================
       SYMBOL ADAPTIVE SCORE
       ========================================================= */

    private void updateSymbolScore(String symbol, double pnl) {

        double delta = pnl > 0 ? 0.02 : -0.03;

        symbolScore.merge(symbol, delta, Double::sum);

        symbolScore.compute(symbol, (k,v)->{
            if(v==null) return 0.0;
            return clamp(v, -0.30, 0.30);
        });
    }

    /* =========================================================
       EXPOSURE MODEL
       ========================================================= */

    private double estimateExposure(DecisionEngineMerged.TradeIdea s) {

        double base = 0.02;

        if (s.confidence > 0.85) base = 0.05;
        else if (s.confidence > 0.75) base = 0.035;
        else if (s.confidence > 0.65) base = 0.025;

        return base;
    }

    private double estimateExposure(ActiveSignal s) {

        double base = 0.02;

        if (s.confidence > 0.85) base = 0.05;
        else if (s.confidence > 0.75) base = 0.035;
        else if (s.confidence > 0.65) base = 0.025;

        return base;
    }

    /* =========================================================
       CLEANUP
       ========================================================= */

    public void cleanupOldSignals(long maxLifetimeMs) {

        long now = System.currentTimeMillis();

        for (Iterator<Map.Entry<String,List<ActiveSignal>>> it = activeSignals.entrySet().iterator(); it.hasNext();) {

            Map.Entry<String,List<ActiveSignal>> e = it.next();

            e.getValue().removeIf(s -> now - s.time > maxLifetimeMs);

            if (e.getValue().isEmpty())
                it.remove();
        }
    }

    /* =========================================================
       STATS API
       ========================================================= */

    public double getSymbolScore(String symbol) {
        return symbolScore.getOrDefault(symbol,0.0);
    }

    public int getActiveSignalsCount() {
        return activeSignals.values().stream().mapToInt(List::size).sum();
    }

    public double getCurrentExposure() {
        return currentExposure;
    }

    public List<ClosedTrade> getHistory(String symbol) {
        return history.getOrDefault(symbol, List.of());
    }

    /* ========================================================= */

    private static double clamp(double v,double min,double max){
        return Math.max(min,Math.min(max,v));
    }
}