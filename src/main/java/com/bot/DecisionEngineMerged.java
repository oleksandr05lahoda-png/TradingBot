package com.bot;

import java.util.List;
import java.util.Optional;

public class DecisionEngineMerged {

    // ===================== TRADE IDEA =====================
    public static class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry;
        public final double stopLoss;
        public final double takeProfit1;
        public final double takeProfit2;
        public final double probability;
        public final double atr;
        public final String confidence;
        public final String context;

        public TradeIdea(
                String symbol,
                TradingCore.Side side,
                double entry,
                double sl,
                double tp1,
                double tp2,
                double prob,
                double atr,
                String confidence,
                String ctx
        ) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stopLoss = sl;
            this.takeProfit1 = tp1;
            this.takeProfit2 = tp2;
            this.probability = prob;
            this.atr = atr;
            this.confidence = confidence;
            this.context = ctx;
        }
    }

    // ===================== MAIN =====================
    public Optional<TradeIdea> evaluate(
            String symbol,
            List<TradingCore.Candle> c5,
            List<TradingCore.Candle> c15,
            List<TradingCore.Candle> c1h
    ) {
        if (!valid(c5, 50) || !valid(c15, 50) || !valid(c1h, 80))
            return Optional.empty();

        double atr = SignalSender.atr(c5, 14);
        if (atr <= 0) return Optional.empty();

        MarketContext ctx = buildMarketContext(c15, c1h);

        Setup setup = detectSetup(c5, atr, ctx);
        if (setup == Setup.NONE)
            return Optional.empty();

        TradeRisk risk = buildRisk(c5, atr, setup);
        double probability = estimateProbability(ctx, setup, risk);

        String confidence = mapConfidence(probability);

        return Optional.of(new TradeIdea(
                symbol,
                setup.side,
                last(c5).close,
                risk.stop,
                risk.tp1,
                risk.tp2,
                probability,
                atr,
                confidence,
                setup.reason + " | " + ctx.explain()
        ));
    }

    // ===================== SETUP =====================
    private Setup detectSetup(List<TradingCore.Candle> c5, double atr, MarketContext ctx) {
        TradingCore.Candle l = last(c5);

        // ==== REVERSAL ====
        boolean reversalTop =
                ctx.trend1h == Trend.UP &&
                        ctx.strength > 0.5 &&
                        sweepHigh(c5, atr * 0.7) &&
                        momentumShiftDown(c5);

        boolean reversalBottom =
                ctx.trend1h == Trend.DOWN &&
                        ctx.strength > 0.5 &&
                        sweepLow(c5, atr * 0.7) &&
                        momentumShiftUp(c5);

        if (reversalTop)
            return new Setup(TradingCore.Side.SHORT, "HTF exhaustion reversal");

        if (reversalBottom)
            return new Setup(TradingCore.Side.LONG, "HTF exhaustion reversal");

        // ==== TREND CONTINUATION ====
        if (!ctx.late) {
            if (ctx.trend1h == Trend.UP && continuationUp(c5))
                return new Setup(TradingCore.Side.LONG, "Trend continuation");

            if (ctx.trend1h == Trend.DOWN && continuationDown(c5))
                return new Setup(TradingCore.Side.SHORT, "Trend continuation");
        }

        // ==== MINI SIGNALS (быстрые движения) ====
        if (breakoutUp(c5))
            return new Setup(TradingCore.Side.LONG, "Quick breakout up");

        if (breakoutDown(c5))
            return new Setup(TradingCore.Side.SHORT, "Quick breakout down");

        return Setup.NONE;
    }

    // ===================== RISK =====================
    private TradeRisk buildRisk(List<TradingCore.Candle> c5, double atr, Setup setup) {
        TradingCore.Candle l = last(c5);
        double entry = l.close;

        double stop, tp1, tp2;

        if (setup.side == TradingCore.Side.LONG) {
            stop = recentSwingLow(c5) - atr * 0.3;
            tp1 = entry + atr * 1.2;
            tp2 = entry + atr * 2.2;
        } else {
            stop = recentSwingHigh(c5) + atr * 0.3;
            tp1 = entry - atr * 1.2;
            tp2 = entry - atr * 2.2;
        }

        double rr = Math.abs(tp2 - entry) / Math.abs(entry - stop);
        return new TradeRisk(stop, tp1, tp2, rr);
    }

    // ===================== PROBABILITY =====================
    private double estimateProbability(MarketContext ctx, Setup setup, TradeRisk risk) {
        double p = 0.55;

        if (setup.reason.contains("reversal")) p += 0.10;
        if (setup.reason.contains("continuation")) p += 0.08;
        if (setup.reason.contains("breakout")) p += 0.06;

        if (ctx.trend15 == ctx.trend1h) p += 0.05;
        if (ctx.late) p -= 0.05;

        if (risk.rr < 1.0) p -= 0.08;
        else if (risk.rr > 2.0) p += 0.05;

        return Math.min(Math.max(p, 0.50), 0.90);
    }

    private String mapConfidence(double probability) {
        if (probability >= 0.70) return "[S]";
        if (probability >= 0.58) return "[M]";
        return "[W]";
    }

    // ===================== CONTEXT =====================
    private MarketContext buildMarketContext(List<TradingCore.Candle> c15, List<TradingCore.Candle> c1h) {
        Trend t1h = trend(c1h);
        Trend t15 = trend(c15);
        double strength = trendStrength(c1h);
        boolean late = isTrendLate(c1h);

        return new MarketContext(t1h, t15, strength, late);
    }

    // ===================== HELPERS =====================
    private boolean sweepLow(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        TradingCore.Candle p = c.get(Math.max(0, c.size() - 3));
        return l.low < p.low && (l.high - l.low) > atr;
    }

    private boolean sweepHigh(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        TradingCore.Candle p = c.get(Math.max(0, c.size() - 3));
        return l.high > p.high && (l.high - l.low) > atr;
    }

    private boolean momentumShiftDown(List<TradingCore.Candle> c) {
        return last(c).close < c.get(Math.max(0, c.size() - 4)).close;
    }

    private boolean momentumShiftUp(List<TradingCore.Candle> c) {
        return last(c).close > c.get(Math.max(0, c.size() - 4)).close;
    }

    private boolean continuationUp(List<TradingCore.Candle> c) {
        return last(c).close > c.get(Math.max(0, c.size() - 2)).high;
    }

    private boolean continuationDown(List<TradingCore.Candle> c) {
        return last(c).close < c.get(Math.max(0, c.size() - 2)).low;
    }

    private boolean breakoutUp(List<TradingCore.Candle> c) {
        return last(c).close > recentHigh(c, 5);
    }

    private boolean breakoutDown(List<TradingCore.Candle> c) {
        return last(c).close < recentLow(c, 5);
    }

    private double recentSwingLow(List<TradingCore.Candle> c) {
        return c.subList(Math.max(0, c.size() - 10), c.size())
                .stream().mapToDouble(x -> x.low).min().orElse(last(c).low);
    }

    private double recentSwingHigh(List<TradingCore.Candle> c) {
        return c.subList(Math.max(0, c.size() - 10), c.size())
                .stream().mapToDouble(x -> x.high).max().orElse(last(c).high);
    }

    private double recentHigh(List<TradingCore.Candle> c, int lookback) {
        return c.subList(Math.max(0, c.size() - lookback), c.size())
                .stream().mapToDouble(x -> x.high).max().orElse(last(c).high);
    }

    private double recentLow(List<TradingCore.Candle> c, int lookback) {
        return c.subList(Math.max(0, c.size() - lookback), c.size())
                .stream().mapToDouble(x -> x.low).min().orElse(last(c).low);
    }

    private Trend trend(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(Math.max(0, c.size() - 80));
        TradingCore.Candle l = last(c);
        double pct = (l.close - f.close) / f.close;

        if (pct > 0.004) return Trend.UP;
        if (pct < -0.004) return Trend.DOWN;
        return Trend.FLAT;
    }

    private double trendStrength(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(Math.max(0, c.size() - 100));
        TradingCore.Candle l = last(c);
        return Math.min(Math.abs((l.close - f.close) / f.close) * 7, 1.0);
    }

    private boolean isTrendLate(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(Math.max(0, c.size() - 120));
        TradingCore.Candle l = last(c);
        return Math.abs((l.close - f.close) / f.close) > 0.06;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    // ===================== STRUCTS =====================
    private static class MarketContext {
        Trend trend1h;
        Trend trend15;
        double strength;
        boolean late;

        MarketContext(Trend t1, Trend t15, double s, boolean late) {
            this.trend1h = t1;
            this.trend15 = t15;
            this.strength = s;
            this.late = late;
        }

        String explain() {
            return "1H=" + trend1h +
                    " 15M=" + trend15 +
                    " strength=" + String.format("%.2f", strength) +
                    (late ? " late" : "");
        }
    }

    private static class TradeRisk {
        double stop, tp1, tp2, rr;

        TradeRisk(double s, double t1, double t2, double rr) {
            this.stop = s;
            this.tp1 = t1;
            this.tp2 = t2;
            this.rr = rr;
        }
    }

    private static class Setup {
        TradingCore.Side side;
        String reason;

        Setup(TradingCore.Side s, String r) {
            side = s;
            reason = r;
        }

        static final Setup NONE = null;
    }

    private enum Trend {UP, DOWN, FLAT}
}
