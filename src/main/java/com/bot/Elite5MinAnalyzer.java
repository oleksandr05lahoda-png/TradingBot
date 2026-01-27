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
            lastSignalTime.entrySet().removeIf(e -> now - e.getValue() > 20 * 60_000);
        }, 10, 10, TimeUnit.MINUTES);
    }

    /* ================= TRADE SIGNAL ================= */

    public static class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double stop;
        public final double take;
        public final String confidence;
        public final String mode;
        public final String reason;

        public TradeSignal(String symbol, TradingCore.Side side,
                           double entry, double stop, double take,
                           String confidence, String mode, String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.mode = mode;
            this.reason = reason;
        }
    }

    /* ================= MAIN ================= */

    public List<TradeSignal> analyze(String symbol,
                                     List<TradingCore.Candle> c5,
                                     List<TradingCore.Candle> c15,
                                     List<TradingCore.Candle> c1h) {

        if (c5 == null || c15 == null || c1h == null) return List.of();

        List<DecisionEngineMerged.TradeIdea> ideas =
                decisionEngine.evaluate(symbol, c5, c15, c1h);

        if (ideas.isEmpty()) return List.of();

        TradingCore.Side htfTrend = detectTrend(c1h, c15);
        long now = System.currentTimeMillis();

        if (lastSignalTime.containsKey(symbol)
                && now - lastSignalTime.get(symbol) < dynamicCooldown(symbol)) {
            return List.of();
        }

        ideas.sort(Comparator.comparingDouble(i -> -i.probability));

        List<TradeSignal> result = new ArrayList<>();

        for (DecisionEngineMerged.TradeIdea idea : ideas) {

            boolean counterTrend =
                    htfTrend != null && idea.side != htfTrend;

            TradeMode mode = classifyMode(idea, counterTrend);

            double conf = brain.applyAllAdjustments("ELITE5", symbol, idea.probability);

            double minConf = switch (mode) {
                case TREND -> 0.52;
                case PULLBACK -> 0.55;
                case REVERSAL -> 0.62;
            };

            if (conf < minConf) continue;

            RiskEngine.TradeSignal r =
                    riskEngine.applyRisk(idea, conf, mode);

            result.add(new TradeSignal(
                    symbol,
                    idea.side,
                    r.entry,
                    r.stop,
                    r.take,
                    mapConfidence(conf),
                    mode.name(),
                    idea.reason
            ));

            if (result.size() >= maxSignals()) break;
        }

        if (!result.isEmpty()) {
            lastSignalTime.put(symbol, now);
        }

        return result;
    }

    /* ================= MODE ================= */

    enum TradeMode {
        TREND,
        PULLBACK,
        REVERSAL
    }

    private TradeMode classifyMode(DecisionEngineMerged.TradeIdea idea, boolean counterTrend) {
        if (idea.reason.contains("reversal")) return TradeMode.REVERSAL;
        if (counterTrend) return TradeMode.REVERSAL;
        if (idea.reason.contains("Pullback")) return TradeMode.PULLBACK;
        return TradeMode.TREND;
    }

    /* ================= TREND ================= */

    private TradingCore.Side detectTrend(List<TradingCore.Candle> c1h,
                                         List<TradingCore.Candle> c15) {

        double h = c1h.get(c1h.size() - 1).close
                - c1h.get(c1h.size() - 80).close;

        if (Math.abs(h) > 0.004 * c1h.get(c1h.size() - 80).close)
            return h > 0 ? TradingCore.Side.LONG : TradingCore.Side.SHORT;

        double m = c15.get(c15.size() - 1).close
                - c15.get(c15.size() - 80).close;

        if (Math.abs(m) > 0.003 * c15.get(c15.size() - 80).close)
            return m > 0 ? TradingCore.Side.LONG : TradingCore.Side.SHORT;

        return null;
    }

    /* ================= RISK ================= */

    static class RiskEngine {

        private final double minRiskPct;

        RiskEngine(double minRiskPct) {
            this.minRiskPct = minRiskPct;
        }

        static class TradeSignal {
            double entry, stop, take;
            TradeSignal(double e, double s, double t) {
                entry = e; stop = s; take = t;
            }
        }

        TradeSignal applyRisk(DecisionEngineMerged.TradeIdea i,
                              double conf, TradeMode mode) {

            double atr = i.atr;
            double risk = Math.max(atr * 0.7, i.entry * minRiskPct);

            double tpMult = switch (mode) {
                case TREND -> conf > 0.65 ? 3.0 : 2.6;
                case PULLBACK -> 2.2;
                case REVERSAL -> 1.4;
            };

            double stop, take;
            if (i.side == TradingCore.Side.LONG) {
                stop = i.entry - risk;
                take = i.entry + risk * tpMult;
            } else {
                stop = i.entry + risk;
                take = i.entry - risk * tpMult;
            }

            return new TradeSignal(i.entry, stop, take);
        }
    }

    /* ================= BRAIN ================= */

    static class AdaptiveBrain {

        private final Map<String, Integer> streak = new ConcurrentHashMap<>();

        double applyAllAdjustments(String strat, String pair, double base) {

            int s = streak.getOrDefault(pair, 0);
            double adj = base;

            if (s >= 2) adj += 0.05;
            if (s <= -2) adj -= 0.06;

            int h = LocalTime.now(ZoneOffset.UTC).getHour();
            if (h >= 7 && h <= 17) adj += 0.04;

            return Math.max(0.45, Math.min(0.92, adj));
        }
    }

    /* ================= UTILS ================= */

    private long dynamicCooldown(String symbol) {
        return 2 * 60_000;
    }

    private int maxSignals() {
        int h = LocalTime.now(ZoneOffset.UTC).getHour();
        return (h >= 7 && h <= 17) ? 4 : 3;
    }

    private String mapConfidence(double p) {
        if (p >= 0.70) return "[S]";
        if (p >= 0.58) return "[M]";
        return "[W]";
    }

    public void shutdown() {
        cleaner.shutdown();
    }
}
