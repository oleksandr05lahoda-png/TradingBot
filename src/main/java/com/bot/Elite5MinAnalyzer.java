package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Elite5MinAnalyzer {

    private final DecisionEngineMerged decisionEngine;
    private final RiskEngine riskEngine;
    private final AdaptiveBrain brain;

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
            return "TradeSignal{" +
                    "symbol='" + symbol + '\'' +
                    ", side=" + side +
                    ", entry=" + entry +
                    ", stop=" + stop +
                    ", take=" + take +
                    ", confidence=" + confidence +
                    ", reason='" + reason + '\'' +
                    '}';
        }
    }

    // ================== MAIN ANALYSIS ==================
    public Optional<TradeSignal> analyze(
            String symbol,
            List<TradingCore.Candle> candles5m,
            List<TradingCore.Candle> candles15m,
            List<TradingCore.Candle> candles1h
    ) {
        Optional<DecisionEngineMerged.TradeIdea> ideaOpt =
                decisionEngine.evaluate(symbol, candles5m, candles15m, candles1h);

        if (ideaOpt.isEmpty()) return Optional.empty();

        DecisionEngineMerged.TradeIdea idea = ideaOpt.get();

        // ---- adjust confidence using AdaptiveBrain ----
        double baseProb = idea.probability;
        double adjustedProb = brain.applyAllAdjustments("ELITE5", symbol, baseProb);

        // ---- map probability to confidence string ----
        String conf = adjustedProb >= 0.70 ? "[S]" :
                adjustedProb >= 0.58 ? "[M]" : "[W]";

        // ---- apply risk engine ----
        RiskEngine.TradeSignal riskSignal = riskEngine.applyRisk(
                idea.symbol,
                idea.side,
                idea.entry,
                idea.atr,
                adjustedProb,
                idea.context
        );

        // ---- build final signal ----
        TradeSignal finalSignal = new TradeSignal(
                idea.symbol,
                idea.side,
                riskSignal.entry,
                riskSignal.stop,
                riskSignal.take,
                conf,
                idea.context
        );

        return Optional.of(finalSignal);
    }

    // ================== RISK ENGINE ==================
    public static class RiskEngine {

        private final double minRiskPct;

        public RiskEngine(double minRiskPct) {
            this.minRiskPct = minRiskPct;
        }

        public static class TradeSignal {
            public final String symbol;
            public final double entry;
            public final double stop;
            public final double take;
            public final double confidence;
            public final String reason;

            public TradeSignal(String symbol, double entry, double stop,
                               double take, double confidence, String reason) {
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

            // адаптивный риск по ATR и уверенности
            double risk = atr * (confidence > 0.65 ? 0.9 : 1.2);
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

            while (!q.isEmpty() && now - q.peekFirst() > 20 * 60_000)
                q.pollFirst();

            if (q.size() >= 3) return -0.07;
            if (q.size() == 2) return -0.03;
            return 0.0;
        }

        private double sessionBoost() {
            int hour = LocalTime.now(ZoneOffset.UTC).getHour();
            if ((hour >= 7 && hour < 11) || (hour >= 13 && hour < 17))
                return 0.05;
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
