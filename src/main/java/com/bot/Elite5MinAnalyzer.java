package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public class Elite5MinAnalyzer {

    private final DecisionEngineMerged decisionEngine;
    private final RiskEngine riskEngine;
    private final AdaptiveBrain brain;
    private final Set<String> recentSignals = ConcurrentHashMap.newKeySet(); // для фильтра дублей

    public Elite5MinAnalyzer(DecisionEngineMerged decisionEngine, double minRiskPct) {
        this.decisionEngine = decisionEngine;
        this.riskEngine = new RiskEngine(minRiskPct);
        this.brain = new AdaptiveBrain();
    }

    // ================== TRADE SIGNAL ==================
    public static class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double stop;
        public final double take;
        public final String confidence; // [S], [M], [W]
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
    public List<TradeSignal> analyzeAll(List<String> symbols,
                                        Map<String, List<TradingCore.Candle>> candles5m,
                                        Map<String, List<TradingCore.Candle>> candles15m,
                                        Map<String, List<TradingCore.Candle>> candles1h) {

        List<TradeSignal> signals = new ArrayList<>();

        for (String sym : symbols) {
            List<TradingCore.Candle> c5 = candles5m.get(sym);
            List<TradingCore.Candle> c15 = candles15m.get(sym);
            List<TradingCore.Candle> c1h = candles1h.get(sym);

            if (c5 == null || c15 == null || c1h == null) continue;

            Optional<DecisionEngineMerged.TradeIdea> ideaOpt =
                    decisionEngine.evaluate(sym, c5, c15, c1h);

            if (ideaOpt.isEmpty()) continue;
            DecisionEngineMerged.TradeIdea idea = ideaOpt.get();

            // ---- адаптивная уверенность ----
            double baseProb = idea.probability;
            double adjustedProb = brain.applyAllAdjustments("ELITE5", sym, baseProb);

            if (adjustedProb < 0.55) continue; // фильтр слабых сигналов

            // ---- применение риск-движка ----
            RiskEngine.TradeSignal riskSig = riskEngine.applyRisk(
                    idea.symbol, idea.side, idea.entry, idea.atr, adjustedProb, idea.context);

            TradeSignal ts = new TradeSignal(
                    sym, idea.side, riskSig.entry, riskSig.stop, riskSig.take,
                    mapConfidence(adjustedProb), idea.context
            );

            // ---- фильтр дублей ----
            String key = sym + "_" + idea.side;
            if (!recentSignals.contains(key)) {
                recentSignals.add(key);
                signals.add(ts);
            }
        }

        // ---- очистка старых сигналов через 10 минут ----
        ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();
        cleaner.schedule(() -> recentSignals.clear(), 10, TimeUnit.MINUTES);

        return signals;
    }

    private String mapConfidence(double probability) {
        if (probability >= 0.75) return "[S]";
        if (probability >= 0.60) return "[M]";
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

            double risk = atr * (confidence > 0.65 ? 0.9 : 1.1);
            double minRisk = entry * minRiskPct;
            if (risk < minRisk) risk = minRisk;

            double stop, take;
            if (side == TradingCore.Side.LONG) {
                stop = entry - risk;
                take = entry + risk * 2.5;
            } else {
                stop = entry + risk;
                take = entry - risk * 2.5;
            }

            return new TradeSignal(symbol, entry, stop, take, confidence, reason);
        }
    }

    // ================== ADAPTIVE BRAIN ==================
    public static class AdaptiveBrain {

        private static class OutcomeStat {
            int wins = 0;
            int losses = 0;
        }

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
            if (wr > 0.60) return Math.min(0.85, baseConf + 0.05);
            if (wr < 0.45) return Math.max(0.55, baseConf - 0.05);
            return baseConf;
        }

        private double impulsePenalty(String pair) {
            impulseHistory.putIfAbsent(pair, new ConcurrentLinkedDeque<>());
            Deque<Long> q = impulseHistory.get(pair);
            long now = System.currentTimeMillis();
            q.addLast(now);
            while (!q.isEmpty() && now - q.peekFirst() > 20 * 60_000) q.pollFirst();
            if (q.size() >= 3) return -0.07;
            if (q.size() == 2) return -0.03;
            return 0.0;
        }

        private double sessionBoost() {
            int hour = LocalTime.now(ZoneOffset.UTC).getHour();
            if ((hour >= 7 && hour < 11) || (hour >= 13 && hour < 17)) return 0.05;
            return 0.0;
        }

        public double applyAllAdjustments(String strategy, String pair, double baseConf) {
            double conf = adaptConfidence(strategy, baseConf);
            conf += sessionBoost();
            conf += impulsePenalty(pair);
            return Math.max(0.45, Math.min(0.90, conf));
        }
    }
}
