package com.bot;

import java.util.List;
import java.util.Optional;

public class DecisionEngineMerged {

    // ================== TRADE IDEA ==================
    public static class TradeIdea {
        public final String symbol;
        public final String side;
        public final double entry;
        public final double atr;
        public final double confidence;
        public final String reason;

        public TradeIdea(String s, String side, double e, double a, double c, String r) {
            this.symbol = s;
            this.side = side;
            this.entry = e;
            this.atr = a;
            this.confidence = c;
            this.reason = r;
        }

        public static TradeIdea longIdea(String s, double e, double a, double c, String r) {
            return new TradeIdea(s, "LONG", e, a, c, r);
        }

        public static TradeIdea shortIdea(String s, double e, double a, double c, String r) {
            return new TradeIdea(s, "SHORT", e, a, c, r);
        }
    }

    // ================== MAIN ==================
    public Optional<TradeIdea> evaluate(
            String symbol,
            List<TradingCore.Candle> c5,
            List<TradingCore.Candle> c15,
            List<TradingCore.Candle> c1h
    ) {
        if (!valid(c5, 50) || !valid(c15, 50) || !valid(c1h, 50))
            return Optional.empty();

        TradingCore.Candle last = last(c5);
        double atr = SignalSender.atr(c5, 14);
        if (atr <= 0) return Optional.empty();

        // === 1. АНТИ-DUMP / АНТИ-PUMP ===
        if (isClimacticCandle(c5, atr))
            return Optional.empty();

        // === 2. СТРУКТУРА ===
        MarketStructure structure = detectStructure(c5);

        // === 3. ТРЕНДЫ ===
        Trend t1h = detectTrend(c1h);
        Trend t15 = detectTrend(c15);

        // === 4. ЛИКВИДНОСТЬ ===
        LiquidityEvent liquidity = detectLiquiditySweep(c5, atr);

        // === 5. ENTRY ===
        Entry entry = resolveEntry(c5, structure, liquidity);
        if (entry == Entry.NONE)
            return Optional.empty();

        // === 6. ФИЛЬТР ПО HTF ===
        if (!trendAllows(entry, t1h))
            return Optional.empty();

        // === 7. CONFIDENCE ===
        double conf = buildConfidence(entry, structure, liquidity, t1h, t15, atr);
        if (conf < 0.65)
            return Optional.empty();

        return Optional.of(
                entry == Entry.LONG
                        ? TradeIdea.longIdea(symbol, last.close, atr, conf,
                        "Structure + Liquidity + HTF safe")
                        : TradeIdea.shortIdea(symbol, last.close, atr, conf,
                        "Structure + Liquidity + HTF safe")
        );
    }

    // ================== STRUCTURE ==================
    private MarketStructure detectStructure(List<TradingCore.Candle> c) {
        TradingCore.Candle a = c.get(c.size() - 6);
        TradingCore.Candle b = c.get(c.size() - 3);
        TradingCore.Candle l = last(c);

        if (l.high > b.high && b.high > a.high) return MarketStructure.HH;
        if (l.low < b.low && b.low < a.low) return MarketStructure.LL;

        return MarketStructure.RANGE;
    }

    // ================== LIQUIDITY ==================
    private LiquidityEvent detectLiquiditySweep(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle last = last(c);
        TradingCore.Candle prev = c.get(c.size() - 2);

        if (last.low < prev.low && (prev.high - prev.low) < atr)
            return LiquidityEvent.SELL_SIDE_SWEEP;

        if (last.high > prev.high && (prev.high - prev.low) < atr)
            return LiquidityEvent.BUY_SIDE_SWEEP;

        return LiquidityEvent.NONE;
    }

    // ================== ENTRY ==================
    private Entry resolveEntry(
            List<TradingCore.Candle> c,
            MarketStructure s,
            LiquidityEvent l
    ) {
        TradingCore.Candle last = last(c);

        if (l == LiquidityEvent.SELL_SIDE_SWEEP && s != MarketStructure.LL)
            return Entry.LONG;

        if (l == LiquidityEvent.BUY_SIDE_SWEEP && s != MarketStructure.HH)
            return Entry.SHORT;

        if (last.close > last.open && s == MarketStructure.HH)
            return Entry.LONG;

        if (last.close < last.open && s == MarketStructure.LL)
            return Entry.SHORT;

        return Entry.NONE;
    }

    // ================== CONFIDENCE ==================
    private double buildConfidence(
            Entry e,
            MarketStructure s,
            LiquidityEvent l,
            Trend t1h,
            Trend t15,
            double atr
    ) {
        double c = 0.55;

        if (l != LiquidityEvent.NONE) c += 0.10;
        if ((e == Entry.LONG && s == MarketStructure.HH) ||
                (e == Entry.SHORT && s == MarketStructure.LL))
            c += 0.10;

        if ((e == Entry.LONG && t1h == Trend.UP) ||
                (e == Entry.SHORT && t1h == Trend.DOWN))
            c += 0.10;

        if ((e == Entry.LONG && t15 == Trend.UP) ||
                (e == Entry.SHORT && t15 == Trend.DOWN))
            c += 0.05;

        return Math.min(c, 0.85);
    }

    // ================== SAFETY ==================
    private boolean isClimacticCandle(List<TradingCore.Candle> c, double atr) {
        TradingCore.Candle last = last(c);
        return (last.high - last.low) > atr * 1.9;
    }

    private boolean trendAllows(Entry e, Trend t) {
        if (e == Entry.LONG) return t != Trend.DOWN;
        if (e == Entry.SHORT) return t != Trend.UP;
        return false;
    }

    private Trend detectTrend(List<TradingCore.Candle> c) {
        TradingCore.Candle f = c.get(c.size() - 30);
        TradingCore.Candle l = last(c);
        double pct = (l.close - f.close) / f.close;

        if (pct > 0.005) return Trend.UP;
        if (pct < -0.005) return Trend.DOWN;
        return Trend.FLAT;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    // ================== ENUMS ==================
    private enum Trend { UP, DOWN, FLAT }
    private enum Entry { LONG, SHORT, NONE }
    private enum MarketStructure { HH, LL, RANGE }
    private enum LiquidityEvent { BUY_SIDE_SWEEP, SELL_SIDE_SWEEP, NONE }
}
