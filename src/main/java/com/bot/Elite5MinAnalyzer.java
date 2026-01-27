package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Elite5MinAnalyzer {

    private final DecisionEngineMerged decisionEngine;
    private final RiskEngine riskEngine;
    private final AdaptiveBrain brain;

    private final Map<String, Long> lastSignalTime = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();

    public Elite5MinAnalyzer(DecisionEngineMerged decisionEngine, double minRiskPct) {
        this.decisionEngine = decisionEngine;
        this.riskEngine = new RiskEngine(minRiskPct);
        this.brain = new AdaptiveBrain();

        cleaner.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            lastSignalTime.entrySet().removeIf(e -> now - e.getValue() > 30 * 60_000);
        }, 10, 10, TimeUnit.MINUTES);
    }

    // ================== TRADE SIGNAL ==================
    public static class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double stop;
        public final double take;
        public final String confidence;
        public final String reason;

        public TradeSignal(String symbol, TradingCore.Side side, double entry,
                           double stop, double take, String confidence, String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.reason = reason;
        }

        @Override
        public String toString() {
            return String.format("%s → %s | Entry: %.5f Stop: %.5f Take: %.5f %s | %s",
                    symbol, side, entry, stop, take, confidence, reason);
        }
    }

    // ================== MAIN ANALYSIS ==================
    public List<TradeSignal> analyze(String symbol,
                                     List<TradingCore.Candle> c5m,
                                     List<TradingCore.Candle> c15m,
                                     List<TradingCore.Candle> c1h) {

        if (c5m == null || c15m == null || c1h == null) return Collections.emptyList();

        // Получаем идеи от DecisionEngineMerged
        List<DecisionEngineMerged.TradeIdea> ideas = decisionEngine.evaluate(symbol, c5m, c15m, c1h);
        if (ideas.isEmpty()) return Collections.emptyList();

        // Определяем тренд по старшему таймфрейму
        TradingCore.Side trend = determineTrend(c1h, c15m);

        long now = System.currentTimeMillis();
        long minInterval = dynamicSignalInterval(symbol);
        if (lastSignalTime.containsKey(symbol) && now - lastSignalTime.get(symbol) < minInterval) {
            return Collections.emptyList();
        }

        // Сортировка идей по вероятности
        ideas.sort(Comparator.comparingDouble(i -> -i.probability));

        List<TradeSignal> result = new ArrayList<>();

        for (DecisionEngineMerged.TradeIdea idea : ideas) {

            // Синхронизация с трендом
            if (trend != null && idea.side != trend) continue; // отбрасываем сигналы против тренда

            double adjustedProb = brain.applyAllAdjustments("ELITE5", symbol, idea.probability);

            if (adjustedProb < 0.50) continue;

            RiskEngine.TradeSignal riskSig = riskEngine.applyRisk(
                    idea.symbol, idea.side, idea.entry, idea.atr, adjustedProb, idea.reason
            );

            TradeSignal ts = new TradeSignal(
                    symbol,
                    idea.side,
                    riskSig.entry,
                    riskSig.stop,
                    riskSig.take,
                    mapConfidence(adjustedProb),
                    idea.reason
            );

            result.add(ts);

            if (result.size() >= dynamicMaxSignals()) break;
        }

        if (!result.isEmpty()) {
            lastSignalTime.put(symbol, now);
        }

        return result;
    }

    private TradingCore.Side determineTrend(List<TradingCore.Candle> c1h, List<TradingCore.Candle> c15m) {
        // простой тренд по закрытию: UP, DOWN или null (flat)
        double diffH = c1h.get(c1h.size() - 1).close - c1h.get(c1h.size() - 80).close;
        if (diffH > 0.003 * c1h.get(c1h.size() - 80).close) return TradingCore.Side.LONG;
        if (diffH < -0.003 * c1h.get(c1h.size() - 80).close) return TradingCore.Side.SHORT;

        double diff15 = c15m.get(c15m.size() - 1).close - c15m.get(c15m.size() - 80).close;
        if (diff15 > 0.003 * c15m.get(c15m.size() - 80).close) return TradingCore.Side.LONG;
        if (diff15 < -0.003 * c15m.get(c15m.size() - 80).close) return TradingCore.Side.SHORT;

        return null; // flat
    }

    private String mapConfidence(double probability) {
        if (probability >= 0.70) return "[S]";
        if (probability >= 0.55) return "[M]";
        return "[W]";
    }

    // ================== RISK ENGINE ==================
    public static class RiskEngine {
        private final double minRiskPct;

        public RiskEngine(double minRiskPct) {
            this.minRiskPct = minRiskPct;
        }

        public static class TradeSignal {
            public final String symbol;
            public final double entry, stop, take;
            public final double confidence;
            public final String reason;

            public TradeSignal(String symbol, double entry, double stop, double take,
                               double confidence, String reason) {
                this.symbol = symbol;
                this.entry = entry;
                this.stop = stop;
                this.take = take;
                this.confidence = confidence;
                this.reason = reason;
            }
        }

        public TradeSignal applyRisk(String symbol, TradingCore.Side side,
                                     double entry, double atr,
                                     double confidence, String reason) {

            double riskMultiplier = confidence >= 0.65 ? 0.85 : 1.1;
            double risk = atr * riskMultiplier;
            double minRisk = entry * minRiskPct;
            if (risk < minRisk) risk = minRisk;

            double takeMultiplier = confidence >= 0.65 ? 2.8 : 2.2;

            double stop, take;
            if (side == TradingCore.Side.LONG) {
                stop = entry - risk;
                take = entry + risk * takeMultiplier;
            } else {
                stop = entry + risk;
                take = entry - risk * takeMultiplier;
            }

            return new TradeSignal(symbol, entry, stop, take, confidence, reason);
        }
    }

    // ================== ADAPTIVE BRAIN ==================
    public static class AdaptiveBrain {

        private static class OutcomeStat { int wins = 0, losses = 0; }

        private final Map<String, OutcomeStat> stats = new ConcurrentHashMap<>();
        private final Map<String, Deque<Long>> impulseHistory = new ConcurrentHashMap<>();

        public void recordOutcome(String strategy, boolean win) {
            stats.putIfAbsent(strategy, new OutcomeStat());
            OutcomeStat s = stats.get(strategy);
            if (win) s.wins++;
            else s.losses++;
        }

        private double winrate(String strategy) {
            OutcomeStat s = stats.get(strategy);
            if (s == null) return 0.5;
            int total = s.wins + s.losses;
            if (total < 20) return 0.5;
            return (double) s.wins / total;
        }

        private double adaptConfidence(String strategy, double baseConf) {
            double wr = winrate(strategy);
            if (wr > 0.60) return Math.min(0.90, baseConf + 0.06);
            if (wr < 0.45) return Math.max(0.50, baseConf - 0.06);
            return baseConf;
        }

        private double impulsePenalty(String pair) {
            impulseHistory.putIfAbsent(pair, new ConcurrentLinkedDeque<>());
            Deque<Long> q = impulseHistory.get(pair);
            long now = System.currentTimeMillis();
            q.addLast(now);
            while (!q.isEmpty() && now - q.peekFirst() > 20 * 60_000) q.pollFirst();
            if (q.size() >= 3) return -0.08;
            if (q.size() == 2) return -0.04;
            return 0.0;
        }

        private double sessionBoost() {
            int hour = LocalTime.now(ZoneOffset.UTC).getHour();
            if ((hour >= 7 && hour < 11) || (hour >= 13 && hour < 17)) return 0.06;
            return 0.0;
        }

        public double applyAllAdjustments(String strategy, String pair, double baseConf) {
            double conf = adaptConfidence(strategy, baseConf);
            conf += sessionBoost();
            conf += impulsePenalty(pair);
            return Math.max(0.45, Math.min(0.92, conf));
        }
    }

    private long dynamicSignalInterval(String symbol) { return 5 * 60_000; }

    private int dynamicMaxSignals() {
        int hour = LocalTime.now(ZoneOffset.UTC).getHour();
        if ((hour >= 7 && hour < 11) || (hour >= 13 && hour < 17)) return 4;
        return 3;
    }

    public void shutdown() { cleaner.shutdown(); }
}
