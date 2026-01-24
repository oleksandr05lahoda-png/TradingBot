package com.bot;

import java.util.List;
import java.util.Optional;

public class DecisionEngineMerged {

    public static class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;   // <- единственный Side в проекте
        public final double entry;
        public final double stopLoss;
        public final double takeProfit1;
        public final double takeProfit2;
        public final double probability;
        public final double atr;
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
        if (!valid(c5, 120) || !valid(c15, 120) || !valid(c1h, 150))
            return Optional.empty();

        double atr = SignalSender.atr(c5, 14);
        if (atr <= 0) return Optional.empty();

        MarketContext ctx = buildMarketContext(c15, c1h);

        Direction direction = chooseDirection(ctx);
        if (direction == Direction.NONE)
            return Optional.empty();

        Setup setup = detectSetup(c5, atr, ctx, direction);
        if (setup == Setup.NONE)
            return Optional.empty();

        TradeRisk risk = buildRisk(c5, atr, setup);
        if (risk.rr < 2.0)
            return Optional.empty();

        double probability = estimateProbability(ctx, setup);
        if (probability < 0.65)
            return Optional.empty();

        return Optional.of(new TradeIdea(
                symbol,
                setup.side,
                last(c5).close,
                risk.stop,
                risk.tp1,
                risk.tp2,
                probability,
                atr,
                ctx.explain() + " | " + setup
        ));
    }

    // ===================== DIRECTION =====================
    private Direction chooseDirection(MarketContext ctx) {

        if (ctx.late) return Direction.NONE;
        if (ctx.trend1h == Trend.UP) return Direction.LONG;
        if (ctx.trend1h == Trend.FLAT) return Direction.NONE;
        if (ctx.trend1h == Trend.DOWN && ctx.strength > 0.5) return Direction.SHORT;

        return Direction.NONE;
    }

    // ===================== SETUP =====================
    private Setup detectSetup(
            List<TradingCore.Candle> c5,
            double atr,
            MarketContext ctx,
            Direction direction
    ) {
        TradingCore.Candle l = last(c5);

        boolean liquiditySweepDown =
                sweepLow(c5, atr) && l.close > l.open;

        boolean continuationUp =
                momentumContinuation(c5) && l.close > l.open;

        boolean exhaustionTop =
                sweepHigh(c5, atr) && l.close < l.open;

        if (direction == Direction.LONG &&
                (liquiditySweepDown || continuationUp))
            return new Setup(TradingCore.Side.LONG);

        if (direction == Direction.SHORT &&
                exhaustionTop)
            return new Setup(TradingCore.Side.SHORT);

        return Setup.NONE;
    }

    // ===================== RISK =====================
    private TradeRisk buildRisk(
            List<TradingCore.Candle> c5,
            double atr,
            Setup setup
    ) {
        TradingCore.Candle l = last(c5);
        double entry = l.close;

        double stop, tp1, tp2;

        if (setup.side == TradingCore.Side.LONG) {
            stop = recentSwingLow(c5) - atr * 0.2;
            tp1 = entry + atr * 1.8;
            tp2 = entry + atr * 3.5;
        } else {
            stop = recentSwingHigh(c5) + atr * 0.2;
            tp1 = entry - atr * 1.8;
            tp2 = entry - atr * 3.5;
        }

        double rr = Math.abs(tp2 - entry) / Math.abs(entry - stop);
        return new TradeRisk(stop, tp1, tp2, rr);
    }

    // ===================== PROBABILITY =====================
    private double estimateProbability(
            MarketContext ctx,
            Setup setup
    ) {
        double p = 0.58;

        if (setup.side == TradingCore.Side.LONG && ctx.trend1h == Trend.UP) p += 0.15;
        if (setup.side == TradingCore.Side.LONG && ctx.trend15 == Trend.UP) p += 0.10;

        if (setup.side == TradingCore.Side.SHORT) p -= 0.12;

        if (ctx.strength > 0.7) p += 0.05;
        if (ctx.late) p -= 0.08;

        return Math.min(p, 0.85);
    }

    // ===================== CONTEXT =====================
    private MarketContext buildMarketContext(
            List<TradingCore.Candle> c15,
            List<TradingCore.Candle> c1h
    ) {
        Trend t1h = trend(c1h);
        Trend t15 = trend(c15);
        double strength = trendStrength(c1h);
        boolean late = isTrendLate(c1h);

        return new MarketContext(t1h, t15, strength, late);
    }

    // ===================== HELPERS =====================
    private boolean sweepLow(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        TradingCore.Candle p = c.get(c.size() - 3);
        return l.low < p.low && (l.high - l.low) > atr * 0.8;
    }

    private boolean sweepHigh(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle l = last(c);
        TradingCore.Candle p = c.get(c.size() - 3);
        return l.high > p.high && (l.high - l.low) > atr * 0.8;
    }

    private boolean momentumContinuation(List<TradingCore.Candle> c) {
        TradingCore.Candle a = c.get(c.size() - 4);
        TradingCore.Candle b = last(c);
        return b.close > a.close;
    }

    private double recentSwingLow(List<TradingCore.Candle> c) {
        return c.subList(c.size() - 12, c.size())
                .stream().mapToDouble(x -> x.low).min().orElse(last(c).low);
    }

    private double recentSwingHigh(List<TradingCore.Candle> c) {
        return c.subList(c.size() - 12, c.size())
                .stream().mapToDouble(x -> x.high).max().orElse(last(c).high);
    }

    private Trend trend(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(c.size() - 80);
        TradingCore.Candle l = last(c);
        double pct = (l.close - f.close) / f.close;

        if (pct > 0.006) return Trend.UP;
        if (pct < -0.006) return Trend.DOWN;
        return Trend.FLAT;
    }

    private double trendStrength(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(c.size() - 100);
        TradingCore.Candle l = last(c);
        return Math.min(Math.abs((l.close - f.close) / f.close) * 8, 1.0);
    }

    private boolean isTrendLate(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(c.size() - 120);
        TradingCore.Candle l = last(c);
        return Math.abs((l.close - f.close) / f.close) > 0.07;
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

        Setup(TradingCore.Side s) {
            side = s;
        }

        static final Setup NONE = null;

        public String toString() {
            return side.name();
        }
    }

    private enum Direction {LONG, SHORT, NONE}

    private enum Trend {UP, DOWN, FLAT}
}
