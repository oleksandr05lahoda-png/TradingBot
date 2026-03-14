package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class InstitutionalSignalCore {

    /* =========================================================
    CONFIGURATION (УЛУЧШЕННЫЕ ПАРАМЕТРЫ)
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

    // Дефолтный конструктор с оптимальными параметрами
    public InstitutionalSignalCore() {
        this.maxGlobalSignals = 12;          // было 20 - меньше но качественнее
        this.maxSignalsPerSymbol = 2;
        this.maxPortfolioExposure = 0.40;    // 40%
        this.minConfidence = 58.0;           // было 52 - выше порог
        this.minSignalDiff = 0.003;          // 0.3% минимум между сигналами
        this.signalTtlMs = 15 * 60_000;      // было 10 сек - теперь 15 минут!
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

        if (signal == null) {
            System.out.println("[ISC " + getTime() + "] Signal null → rejected");
            return false;
        }

// Глобальный лимит
        if (getActiveSignalsCount() >= maxGlobalSignals) {
            System.out.println("[ISC " + getTime() + "] Global limit " + maxGlobalSignals + " reached → rejected");
            return false;
        }

// Минимальная уверенность
        if (signal.probability < minConfidence) {
            System.out.println("[ISC " + getTime() + "] " + signal.symbol +
                    " prob " + signal.probability + " < " + minConfidence + " → rejected");
            return false;
        }

        List<ActiveSignal> list = activeSignals.computeIfAbsent(
                signal.symbol,
                k -> new CopyOnWriteArrayList<>()
        );

// Проверка похожих сигналов
        for (ActiveSignal a : list) {
            double priceDiff = Math.abs(a.entry - signal.price) / a.entry;
            double probDiff = Math.abs(a.probability - signal.probability);

// Сигнал почти такой же как существующий
            if (a.side == signal.side && priceDiff < minSignalDiff && probDiff < 3) {
                System.out.println("[ISC] " + signal.symbol + " too similar to existing → rejected");
                return false;
            }

// Противоположный сигнал слишком близко по цене
            if (a.side != signal.side && priceDiff < minSignalDiff * 1.5) {
                System.out.println("[ISC] " + signal.symbol + " opposite too close → rejected");
                return false;
            }
        }

// Проверка score символа
        double score = symbolScore.getOrDefault(signal.symbol, 0.0);
        if (score < -0.30) {
            double winRate = getWinRate(signal.symbol);
            if (winRate < 0.38) {
                System.out.println("[ISC] " + signal.symbol + " bad history (score=" +
                        String.format("%.2f", score) + ", wr=" +
                        String.format("%.2f", winRate) + ") → rejected");
                return false;
            }
        }

// Лимит на символ
        if (list.size() >= maxSignalsPerSymbol) {
            System.out.println("[ISC] " + signal.symbol + " has " + list.size() +
                    " active signals (max=" + maxSignalsPerSymbol + ") → rejected");
            return false;
        }

// Exposure check
        double estimatedExposure = estimateExposure(signal);
        if (currentExposure + estimatedExposure > maxPortfolioExposure) {
            System.out.println("[ISC] Portfolio exposure " +
                    String.format("%.2f", currentExposure) + " + " +
                    String.format("%.2f", estimatedExposure) + " > " +
                    String.format("%.2f", maxPortfolioExposure) + " → rejected");
            return false;
        }

        System.out.println("[ISC " + getTime() + "] ✓ " + signal.symbol +
                " " + signal.side + " prob=" + signal.probability + " → ALLOWED");
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
// Удаляем старый похожий сигнал
            lst.removeIf(s -> s.side == signal.side &&
                    Math.abs(s.entry - signal.price) / s.entry < minSignalDiff);
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
    AUTO CLEANUP (ИСПРАВЛЕННЫЙ - без iterator.remove())
    ========================================================= */
    private void cleanupExpiredSignals() {
        long now = System.currentTimeMillis();

        for (Iterator<Map.Entry<String, List<ActiveSignal>>> it = activeSignals.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, List<ActiveSignal>> e = it.next();
            List<ActiveSignal> list = e.getValue();

// Используем removeIf вместо iterator.remove() - FIX для CopyOnWriteArrayList
            list.removeIf(s -> {
                if (now - s.timestamp > signalTtlMs) {
                    currentExposure = clamp(currentExposure - estimateExposure(s), 0.0, maxPortfolioExposure);
                    return true;
                }
                return false;
            });

            if (list.isEmpty()) {
                it.remove();
            }
        }

        recalcExposure();
    }

    private void recalcExposure() {
        double exposure = 0.0;
        for (List<ActiveSignal> list : activeSignals.values()) {
            for (ActiveSignal s : list) {
                exposure += estimateExposure(s);
            }
        }
        currentExposure = clamp(exposure, 0.0, maxPortfolioExposure);
    }

    /* =========================================================
    SYMBOL PERFORMANCE SCORE
    ========================================================= */
    private void updateSymbolScore(String symbol, double pnl) {
        double delta = pnl > 0 ? 0.015 : pnl < 0 ? -0.018 : -0.003;
        symbolScore.merge(symbol, delta, Double::sum);
        symbolScore.compute(symbol, (k, v) -> clamp(v, -0.45, 0.45));
    }

    private double estimateExposure(com.bot.DecisionEngineMerged.TradeIdea s) {
        double p = s.probability;
        if (p >= 85) return 0.055;
        if (p >= 78) return 0.045;
        if (p >= 70) return 0.035;
        if (p >= 62) return 0.028;
        return 0.020;
    }

    private double estimateExposure(ActiveSignal s) {
        double p = s.probability;
        if (p >= 85) return 0.055;
        if (p >= 78) return 0.045;
        if (p >= 70) return 0.035;
        if (p >= 62) return 0.028;
        return 0.020;
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
        if (h == null || h.isEmpty()) return 0.5; // Нейтральный если нет истории
        long wins = h.stream().filter(t -> t.pnl > 0).count();
        return (double) wins / h.size();
    }

    private static String getTime() {
        return java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    /* =========================================================
    UTIL
    ========================================================= */
    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}
