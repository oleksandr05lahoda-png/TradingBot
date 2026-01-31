package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public class Elite5MinAnalyzer {

    private final DecisionEngineMerged decisionEngine;
    private final RiskEngine riskEngine;
    private final AdaptiveBrain brain;

    private final Map<String, Long> lastSignal = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();
    private static final int MAX_COINS = 100;

    public Elite5MinAnalyzer(DecisionEngineMerged engine, double minRiskPct) {
        this.decisionEngine = engine;
        this.riskEngine = new RiskEngine(minRiskPct);
        this.brain = new AdaptiveBrain();

        // Очистка старых сигналов каждые 10 минут
        cleaner.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            lastSignal.entrySet().removeIf(e -> now - e.getValue() > 25 * 60_000);
        }, 10, 10, TimeUnit.MINUTES);
    }

    /* ================= SIGNAL ================= */
    public static class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final String mode;
        public final String reason;
        public final String coinType;

        public TradeSignal(String s, TradingCore.Side side,
                           double e, double sl, double tp,
                           double conf, String mode, String reason,
                           String coinType) {
            this.symbol = s;
            this.side = side;
            this.entry = e;
            this.stop = sl;
            this.take = tp;
            this.confidence = conf;
            this.mode = mode;
            this.reason = reason;
            this.coinType = coinType;
        }
    }

    /* ================= ANALYZE ================= */
    public List<TradeSignal> analyze(
            List<String> symbols,
            Map<String, List<TradingCore.Candle>> c15,
            Map<String, List<TradingCore.Candle>> c1h,
            Map<String, String> coinTypes
    ) {
        List<TradeSignal> allSignals = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String symbol : symbols) {
            if (scanned++ >= MAX_COINS) break;

            List<TradingCore.Candle> m15 = c15.get(symbol);
            List<TradingCore.Candle> h1 = c1h.get(symbol);
            if (!valid(m15, 50) || !valid(h1, 40)) continue;

            MarketContext ctx = MarketContext.build(m15, h1);
            if (!ctx.tradable) continue;

            String type = coinTypes.getOrDefault(symbol, "ALT");

            // Получаем идеи для конкретного типа монеты
            List<DecisionEngineMerged.TradeIdea> ideas =
                    decisionEngine.evaluate(List.of(symbol),
                            Map.of(symbol, m15),
                            Map.of(symbol, h1),
                            Map.of(symbol, type));

            // Сортировка по вероятности
            ideas.sort(Comparator.comparingDouble(i -> -i.probability));

            for (DecisionEngineMerged.TradeIdea i : ideas) {
                TradeMode mode = TradeMode.classify(i, ctx);

                long cd = dynamicCooldown(ctx, mode, i.grade, type);
                if (lastSignal.containsKey(symbol) && now - lastSignal.get(symbol) < cd)
                    continue;

                double conf = brain.adjust(symbol, i.probability, ctx, mode, type);
                if (conf < mode.minConfidence(ctx, type)) continue;

                RiskEngine.Result r = riskEngine.apply(i, conf, mode, ctx, type);

                allSignals.add(new TradeSignal(
                        symbol,
                        i.side,
                        r.entry,
                        r.stop,
                        r.take,
                        conf,
                        mode.name(),
                        i.reason + ctx.describe(),
                        type
                ));

                lastSignal.put(symbol, now);
            }
        }

        allSignals.sort((a, b) -> Double.compare(b.confidence, a.confidence));
        return allSignals;
    }

    /* ================= MODES ================= */
    enum TradeMode {
        TREND, CONTINUATION, PULLBACK, BREAKOUT, EXHAUSTION;

        static TradeMode classify(DecisionEngineMerged.TradeIdea i, MarketContext ctx) {
            if (ctx.exhaustion) return EXHAUSTION;
            if (ctx.impulse && ctx.compressed) return BREAKOUT;
            if (ctx.strongTrend && !ctx.pullback) return CONTINUATION;
            if (ctx.pullback) return PULLBACK;
            return TREND;
        }

        double minConfidence(MarketContext ctx, String type) {
            double base = switch (this) {
                case TREND -> 0.46;
                case CONTINUATION -> 0.44;
                case PULLBACK -> 0.50;
                case BREAKOUT -> 0.52;
                case EXHAUSTION -> 0.58;
            };
            // Разные пороги для типов монет
            if ("TOP".equals(type)) base -= 0.03;
            if ("MEME".equals(type)) base += 0.02;
            if (ctx.highVol) base -= 0.03;
            if (ctx.lowVol) base += 0.04;
            return base;
        }
    }

    /* ================= MARKET CONTEXT ================= */
    static class MarketContext {
        boolean tradable;
        boolean highVol, lowVol;
        boolean compressed, impulse;
        boolean strongTrend;
        boolean pullback;
        boolean exhaustion;
        double atr, atrPct;
        double emaSlope;
        double rsi;

        static MarketContext build(List<TradingCore.Candle> c15, List<TradingCore.Candle> c1h) {
            MarketContext c = new MarketContext();
            double price = last(c15).close;
            c.atr = atr(c15, 14);
            c.atrPct = c.atr / price;

            c.emaSlope = emaSlope(c15, 21, 5);
            c.rsi = rsi(c15, 14);

            double range = last(c15).high - last(c15).low;
            double avg = avgRange(c15, 20);

            c.highVol = c.atrPct > 0.0022;
            c.lowVol = c.atrPct < 0.0014;
            c.compressed = range < avg * 0.75;
            c.impulse = range > avg * 1.6;

            c.strongTrend = Math.abs(c.emaSlope) > price * 0.0008;
            c.pullback = c.strongTrend && c.rsi < 45 && c.rsi > 35;
            c.exhaustion = (c.rsi > 72 || c.rsi < 28) && c.impulse;

            c.tradable = !c.lowVol || c.impulse;
            return c;
        }

        String describe() {
            StringBuilder sb = new StringBuilder(" |CTX:");
            if (highVol) sb.append("HV ");
            if (compressed) sb.append("COMP ");
            if (impulse) sb.append("IMP ");
            if (strongTrend) sb.append("TREND ");
            if (pullback) sb.append("PB ");
            if (exhaustion) sb.append("EX ");
            return sb.toString();
        }
    }

    /* ================= RISK ================= */
    static class RiskEngine {
        private final double minRiskPct;

        RiskEngine(double minRiskPct) {
            this.minRiskPct = minRiskPct;
        }

        static class Result {
            double entry, stop, take;
        }

        Result apply(DecisionEngineMerged.TradeIdea i,
                     double conf,
                     TradeMode mode,
                     MarketContext ctx,
                     String type) {

            Result r = new Result();

            double riskMultiplier = switch (type) {
                case "MEME" -> 0.75;
                case "ALT" -> 1.0;
                default -> 1.1; // TOP
            };

            double risk = Math.max(i.atr * 0.6 * riskMultiplier, i.entry * minRiskPct);

            double tpMult = switch (mode) {
                case CONTINUATION -> conf > 0.65 ? 3.2 : 2.6;
                case TREND -> 2.4;
                case PULLBACK -> 2.1;
                case BREAKOUT -> 3.5;
                case EXHAUSTION -> 1.6;
            };

            r.entry = i.entry;
            r.stop = i.side == TradingCore.Side.LONG ? i.entry - risk : i.entry + risk;
            r.take = i.side == TradingCore.Side.LONG ? i.entry + risk * tpMult : i.entry - risk * tpMult;

            return r;
        }
    }

    /* ================= ADAPTIVE ================= */
    static class AdaptiveBrain {
        private final Map<String, Integer> streak = new ConcurrentHashMap<>();

        double adjust(String pair, double base,
                      MarketContext ctx, TradeMode mode, String type) {
            int s = streak.getOrDefault(pair, 0);
            double c = base;

            // Стрик позитив / негатив
            if (s >= 2) c += 0.04;
            if (s <= -2) c -= 0.06;

            // Сильный тренд + continuation
            if (ctx.strongTrend && mode == TradeMode.CONTINUATION) c += 0.05;

            // Тип монеты
            if ("TOP".equals(type)) c += 0.02;
            if ("MEME".equals(type)) c -= 0.02;

            // Время дня
            int h = LocalTime.now(ZoneOffset.UTC).getHour();
            if (h >= 7 && h <= 18) c += 0.02;

            return clamp(c, 0.42, 0.95);
        }
    }

    /* ================= UTILS ================= */
    private static long dynamicCooldown(MarketContext ctx, TradeMode m,
                                        DecisionEngineMerged.SignalGrade g, String type) {
        long base = ctx.highVol ? 45_000 : 80_000;
        if (m == TradeMode.CONTINUATION) base *= 0.6;
        if (g == DecisionEngineMerged.SignalGrade.A) base *= 0.7;
        if ("MEME".equals(type)) base *= 0.85;
        if ("TOP".equals(type)) base *= 1.2;
        return base;
    }

    private static boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    private static TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private static double atr(List<TradingCore.Candle> c, int p) {
        double s = 0;
        for (int i = c.size() - p; i < c.size(); i++)
            s += c.get(i).high - c.get(i).low;
        return s / p;
    }

    private static double avgRange(List<TradingCore.Candle> c, int p) {
        double s = 0;
        for (int i = c.size() - p; i < c.size(); i++)
            s += c.get(i).high - c.get(i).low;
        return s / p;
    }

    private static double emaSlope(List<TradingCore.Candle> c, int period, int back) {
        return ema(c, period, back) - ema(c, period, back + 5);
    }

    private static double ema(List<TradingCore.Candle> c, int period, int back) {
        double k = 2.0 / (period + 1);
        double e = c.get(c.size() - back - period).close;
        for (int i = c.size() - back - period + 1; i < c.size() - back; i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private static double rsi(List<TradingCore.Candle> c, int p) {
        double g = 0, l = 0;
        for (int i = c.size() - p; i < c.size(); i++) {
            double d = c.get(i).close - c.get(i - 1).close;
            if (d > 0) g += d;
            else l -= d;
        }
        if (l == 0) return 100;
        double rs = g / l;
        return 100 - (100 / (1 + rs));
    }

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    public void shutdown() {
        cleaner.shutdown();
    }
}
