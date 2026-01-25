package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Elite5MinAnalyzer {

    private final DecisionEngineMerged decisionEngine;
    private final RiskEngine riskEngine;
    private final AdaptiveBrain brain;

    // Чтобы не спамить один и тот же символ слишком часто
    private final Map<String, Long> lastSignalTime = new ConcurrentHashMap<>();

    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();

    public Elite5MinAnalyzer(DecisionEngineMerged decisionEngine, double minRiskPct) {
        this.decisionEngine = decisionEngine;
        this.riskEngine = new RiskEngine(minRiskPct);
        this.brain = new AdaptiveBrain();

        // чистим старые таймстампы каждые 10 минут
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

        // Получаем все идеи от DecisionEngineMerged
        List<DecisionEngineMerged.TradeIdea> ideas = decisionEngine.evaluateAll(symbol, c5m, c15m, c1h);
        if (ideas.isEmpty()) return Collections.emptyList();

        // Фильтр дублей по времени: один символ не чаще 8-10 минут
        long now = System.currentTimeMillis();
        if (lastSignalTime.containsKey(symbol) && now - lastSignalTime.get(symbol) < 8 * 60_000) {
            return Collections.emptyList();
        }

        // Сортируем идеи по вероятности
        ideas.sort(Comparator.comparingDouble(i -> -i.probability));

        List<TradeSignal> result = new ArrayList<>();

        for (DecisionEngineMerged.TradeIdea idea : ideas) {

            // Адаптация confidence
            double adjustedProb = brain.applyAllAdjustments("ELITE5", symbol, idea.probability);

            // минимальный порог 0.52 (чтобы сигналы были чаще, но не фейковые)
            if (adjustedProb < 0.52) continue;

            // RiskEngine
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

            // если нашли 2-3 сигнала — выходим (чтобы не спамить)
            if (result.size() >= 3) break;
        }

        if (!result.isEmpty()) {
            lastSignalTime.put(symbol, now);
        }

        return result;
    }

    private String mapConfidence(double probability) {
        if (probability >= 0.65) return "[S]";
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

            // Более агрессивный риск при сильных сигналах
            double riskMultiplier = confidence >= 0.65 ? 0.85 : 1.05;
            double risk = atr * riskMultiplier;

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
            if (wr < 0.45) return Math.max(0.50, baseConf - 0.05);
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

    // ================== CLEANUP ==================
    public void shutdown() {
        cleaner.shutdown();
    }
}
