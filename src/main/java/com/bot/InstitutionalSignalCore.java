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
        public final com.bot.TradingCore.Side side;
        public final double entry;
        public final double probability;
        public final long timestamp;

        public ActiveSignal(String symbol,
                            com.bot.TradingCore.Side side,
                            double entry,
                            double probability,
                            long timestamp) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.probability = probability;
            this.timestamp = timestamp;
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
    public synchronized boolean allowSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {

        cleanupExpiredSignals();
        if (getActiveSignalsCount() >= maxGlobalSignals) {
            System.out.println("[DEBUG " + getTime() + "] Global signal limit reached → rejected");
            return false;
        }
        if (signal == null) {
            System.out.println("[DEBUG " + getTime() + "] Signal null → rejected");            return false;
        }
        if (signal.probability < minConfidence) {
            System.out.println("[DEBUG " + getTime() + "] Signal " + signal.symbol +
                    " probability " + signal.probability +
                    " < minConfidence " + minConfidence + " → rejected");
            return false;
        }

        List<ActiveSignal> list = activeSignals.computeIfAbsent(
                signal.symbol,
                k -> new CopyOnWriteArrayList<>()
        );

        // Проверка схожих сигналов
        for (ActiveSignal a : list) {
            double priceDiff = Math.abs(a.entry - signal.price)/a.entry;
            double probDiff = Math.abs(a.probability - signal.probability);

            // Сигнал почти такой же как существующий
            if (a.side == signal.side && priceDiff < minSignalDiff && probDiff < 1.5) {
                System.out.println("[DEBUG] Signal too similar → rejected");
                return false;
            }

            // Сигнал с другой стороны, но цена слишком близка
            if (a.side != signal.side && priceDiff < minSignalDiff) {
                System.out.println("[DEBUG] Opposite side, price too close → rejected");
                return false;
            }
        }

        double score = symbolScore.getOrDefault(signal.symbol, 0.0);
        if (score < -0.35) {
            System.out.println("[DEBUG] SymbolScore " + score +
                    " < -0.35 for " + signal.symbol + " → rejected");
            return false;
        }

        if (list.size() >= maxSignalsPerSymbol) {
            System.out.println("[DEBUG] Active signals for " + signal.symbol +
                    " size " + list.size() + " >= maxSignalsPerSymbol " + maxSignalsPerSymbol + " → rejected");
            return false;
        }
        double estimatedExposure = estimateExposure(signal);
        if (currentExposure + estimatedExposure > maxPortfolioExposure) {
            System.out.println("[DEBUG] EstimatedExposure " + estimatedExposure +
                    " + currentExposure " + currentExposure +
                    " > maxPortfolioExposure " + maxPortfolioExposure + " → rejected");
            return false;
        }

        System.out.println("[DEBUG] Signal " + signal.symbol + " allowed! prob=" + signal.probability);
        return true;
    }

    /* =========================================================
       REGISTER / UPDATE
       ========================================================= */
    public synchronized void registerSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        long now = System.currentTimeMillis();

        ActiveSignal active = new ActiveSignal(
                signal.symbol,
                signal.side,
                signal.price,
                signal.probability,
                now
        );

        activeSignals.compute(signal.symbol, (sym, lst) -> {
            if (lst == null) lst = new CopyOnWriteArrayList<>();
            // заменяем дублирующий сигнал
            lst.removeIf(s -> s.side == signal.side && Math.abs(s.entry - signal.price)/s.entry < minSignalDiff);
            lst.add(active);
            return lst;
        });
        currentExposure = clamp(currentExposure + estimateExposure(signal), 0.0, maxPortfolioExposure);
    }

    /* =========================================================
       CLOSE TRADE
       ========================================================= */
    public synchronized void closeTrade(String symbol, double pnlPercent) {
        List<ActiveSignal> list = activeSignals.remove(symbol);
        if (list == null) return;

        long now = System.currentTimeMillis();

        for (ActiveSignal s : list) {
            history.computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>())
                    .add(new ClosedTrade(symbol, pnlPercent, now - s.timestamp));

            currentExposure = clamp(currentExposure - estimateExposure(s), 0.0, maxPortfolioExposure);
            updateSymbolScore(symbol, pnlPercent);
        }
    }

    /* =========================================================
       AUTO CLEANUP
       ========================================================= */
    private void cleanupExpiredSignals() {
        long now = System.currentTimeMillis();

        for (Iterator<Map.Entry<String, List<ActiveSignal>>> it = activeSignals.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, List<ActiveSignal>> e = it.next();
            List<ActiveSignal> list = e.getValue();
            list.removeIf(s -> now - s.timestamp > signalTtlMs);
            if (list.isEmpty()) it.remove();
        }

        recalcExposure();
    }

    private void recalcExposure() {
        double exposure = 0.0;
        for (List<ActiveSignal> list : activeSignals.values())
            for (ActiveSignal s : list)
                exposure += estimateExposure(s);

        currentExposure = clamp(exposure, 0.0, maxPortfolioExposure);
    }

    /* =========================================================
       SYMBOL PERFORMANCE SCORE
       ========================================================= */
    private void updateSymbolScore(String symbol, double pnl) {
        double delta = pnl > 0 ? 0.012 : pnl < 0 ? -0.015 : -0.004;
        symbolScore.merge(symbol, delta, Double::sum);
        symbolScore.compute(symbol, (k, v) -> clamp(v, -0.40, 0.40));
    }

    /* =========================================================
       EXPOSURE MODEL
       ========================================================= */
    private double estimateExposure(com.bot.DecisionEngineMerged.TradeIdea s) {
        if (s.probability >= 82) return 0.05;
        if (s.probability >= 75) return 0.035;
        if (s.probability >= 68) return 0.025;
        return 0.02;
    }

    private double estimateExposure(ActiveSignal s) {
        if (s.probability >= 82) return 0.05;
        if (s.probability >= 75) return 0.035;
        if (s.probability >= 68) return 0.025;
        return 0.02;
    }

    /* =========================================================
       STATS API
       ========================================================= */
    public synchronized int getActiveSignalsCount() {
        return activeSignals.values().stream().mapToInt(List::size).sum();
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
    private static String getTime() {
        // локальное время системы в формате HH:mm:ss
        return java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
    }
    /* =========================================================
       UTIL
       ========================================================= */
    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}