package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

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

        // чистка старых сигналов каждые 10 минут
        cleaner.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            lastSignalTime.entrySet().removeIf(e -> now - e.getValue() > 30 * 60_000);
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

    /* ================= MARKET CONTEXT ================= */

    static class MarketContext {
        boolean tradable;
        boolean highVol;
        boolean lowVol;
        boolean compressed;
        boolean impulse;
        boolean microTrendUp;
        boolean microTrendDown;
        double atrPct;
        double rangePct;
    }

    /* ================= MAIN ================= */

    public List<TradeSignal> analyze(String symbol,
                                     List<TradingCore.Candle> c5,
                                     List<TradingCore.Candle> c15,
                                     List<TradingCore.Candle> c1h) {

        if (!valid(c5, 120) || !valid(c15, 120) || !valid(c1h, 120))
            return List.of();

        MarketContext ctx = buildMarketContext(c5);
        if (!ctx.tradable) return List.of();

        List<DecisionEngineMerged.TradeIdea> ideas =
                decisionEngine.evaluate(symbol, c5, c15, c1h);

        if (ideas.isEmpty()) return List.of();

        TradingCore.Side htfTrend = detectTrend(c1h, c15);
        long now = System.currentTimeMillis();

        if (lastSignalTime.containsKey(symbol)
                && now - lastSignalTime.get(symbol) < dynamicCooldown(ctx)) {
            return List.of();
        }

        // сортировка по вероятности
        ideas.sort(Comparator.comparingDouble(i -> -i.probability));

        List<TradeSignal> result = new ArrayList<>();

        for (DecisionEngineMerged.TradeIdea idea : ideas) {

            boolean counterTrend = htfTrend != null && idea.side != htfTrend;

            TradeMode mode = classifyMode(idea, counterTrend, ctx);

            double conf = brain.applyAllAdjustments("ELITE5", symbol, idea.probability);

            conf = applyContextBoost(conf, ctx, idea, counterTrend);

            double minConf = dynamicMinConfidence(mode, ctx);

            if (conf < minConf) continue;

            RiskEngine.TradeSignal r = riskEngine.applyRisk(idea, conf, mode);

            result.add(new TradeSignal(
                    symbol,
                    idea.side,
                    r.entry,
                    r.stop,
                    r.take,
                    mapConfidence(conf),
                    mode.name(),
                    idea.reason + contextReason(ctx)
            ));
        }

        // сохраняем время сигнала
        if (!result.isEmpty()) lastSignalTime.put(symbol, now);

        return result;
    }

    /* ================= CONTEXT ENGINE ================= */

    private MarketContext buildMarketContext(List<TradingCore.Candle> c5) {
        MarketContext ctx = new MarketContext();

        double atr = atr(c5, 14);
        double price = last(c5).close;

        double atrPct = atr / price;
        double avgRange = avgRange(c5, 20);
        double currRange = last(c5).high - last(c5).low;

        double emaFastSlope = emaSlope(c5, 9, 5);

        ctx.atrPct = atrPct;
        ctx.rangePct = currRange / price;

        ctx.highVol = atrPct > 0.0022;
        ctx.lowVol = atrPct < 0.0015;

        ctx.compressed = currRange < avgRange * 0.75;
        ctx.impulse = currRange > avgRange * 1.5;

        ctx.microTrendUp = emaFastSlope > 0;
        ctx.microTrendDown = emaFastSlope < 0;

        ctx.tradable = true;

        return ctx;
    }

    /* ================= MODE ================= */

    enum TradeMode {
        TREND,
        PULLBACK,
        REVERSAL,
        BREAKOUT
    }

    private TradeMode classifyMode(DecisionEngineMerged.TradeIdea idea,
                                   boolean counterTrend,
                                   MarketContext ctx) {

        if (ctx.impulse && ctx.compressed) return TradeMode.BREAKOUT;
        if (counterTrend) return TradeMode.REVERSAL;
        if (idea.reason.toLowerCase().contains("pullback")) return TradeMode.PULLBACK;
        return TradeMode.TREND;
    }

    /* ================= ADAPTIVE LOGIC ================= */

    private double applyContextBoost(double conf,
                                     MarketContext ctx,
                                     DecisionEngineMerged.TradeIdea idea,
                                     boolean counterTrend) {

        double c = conf;

        if (ctx.highVol) c += 0.05;
        if (ctx.impulse) c += 0.04;
        if (ctx.compressed) c += 0.03;

        if (counterTrend) c -= 0.04;
        if (ctx.lowVol) c -= 0.08;

        return clamp(c, 0.45, 0.95);
    }

    private double dynamicMinConfidence(TradeMode mode, MarketContext ctx) {
        double base = switch (mode) {
            case TREND -> 0.48;
            case PULLBACK -> 0.50;
            case BREAKOUT -> 0.52;
            case REVERSAL -> 0.56;
        };

        if (ctx.highVol) base -= 0.02;
        if (ctx.lowVol) base += 0.04;

        return base;
    }

    /* ================= TREND ================= */

    private TradingCore.Side detectTrend(List<TradingCore.Candle> c1h,
                                         List<TradingCore.Candle> c15) {

        double h = c1h.get(c1h.size() - 1).close
                - c1h.get(c1h.size() - 80).close;

        if (Math.abs(h) > 0.003 * c1h.get(c1h.size() - 80).close)
            return h > 0 ? TradingCore.Side.LONG : TradingCore.Side.SHORT;

        double m = c15.get(c15.size() - 1).close
                - c15.get(c15.size() - 80).close;

        if (Math.abs(m) > 0.002 * c15.get(c15.size() - 80).close)
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
            double risk = Math.max(atr * 0.55, i.entry * minRiskPct);

            double tpMult = switch (mode) {
                case TREND -> conf > 0.63 ? 3.0 : 2.5;
                case PULLBACK -> 2.2;
                case BREAKOUT -> 3.3;
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
            if (s <= -2) adj -= 0.05;

            int h = LocalTime.now(ZoneOffset.UTC).getHour();
            if (h >= 7 && h <= 17) adj += 0.03;

            return Math.max(0.45, Math.min(0.95, adj));
        }
    }

    /* ================= UTILS ================= */

    private long dynamicCooldown(MarketContext ctx) {
        if (ctx.highVol) return 50_000;
        if (ctx.compressed) return 80_000;
        return 90_000;
    }

    private String mapConfidence(double p) {
        if (p >= 0.70) return "[S]";
        if (p >= 0.55) return "[M]";
        return "[W]";
    }

    private String contextReason(MarketContext ctx) {
        StringBuilder sb = new StringBuilder(" |CTX:");
        if (ctx.highVol) sb.append("HV ");
        if (ctx.compressed) sb.append("COMP ");
        if (ctx.impulse) sb.append("IMP ");
        return sb.toString();
    }

    private double atr(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++) sum += c.get(i).high - c.get(i).low;
        return sum / n;
    }

    private double avgRange(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++) sum += c.get(i).high - c.get(i).low;
        return sum / n;
    }

    private double emaSlope(List<TradingCore.Candle> c, int p, int bars) {
        double e1 = ema(c, p, bars);
        double e2 = ema(c, p, bars + 5);
        return e1 - e2;
    }

    private double ema(List<TradingCore.Candle> c, int p, int back) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - back - p).close;
        for (int i = c.size() - back - p + 1; i < c.size() - back; i++) {
            e = c.get(i).close * k + e * (1 - k);
        }
        return e;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    public void shutdown() {
        cleaner.shutdown();
    }
}
