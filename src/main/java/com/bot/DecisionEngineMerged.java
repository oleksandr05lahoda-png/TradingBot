package com.bot;

import java.util.List;
import java.util.Optional;

public class DecisionEngineMerged {

    // ================== TRADE IDEA ==================
    public static class TradeIdea {
        public final String symbol;
        public final String side; // "LONG" | "SHORT"
        public final double entry;
        public final double atr;
        public final double confidence;
        public final String reason;

        public TradeIdea(
                String symbol,
                String side,
                double entry,
                double atr,
                double confidence,
                String reason
        ) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.atr = atr;
            this.confidence = confidence;
            this.reason = reason;
        }

        public static TradeIdea longIdea(
                String symbol,
                double entry,
                double atr,
                double confidence,
                String reason
        ) {
            return new TradeIdea(symbol, "LONG", entry, atr, confidence, reason);
        }

        public static TradeIdea shortIdea(
                String symbol,
                double entry,
                double atr,
                double confidence,
                String reason
        ) {
            return new TradeIdea(symbol, "SHORT", entry, atr, confidence, reason);
        }
    }

    // ================== MAIN ENTRY POINT ==================
    public Optional<TradeIdea> evaluate(
            String symbol,
            List<TradingCore.Candle> c5m,
            List<TradingCore.Candle> c15m,
            List<TradingCore.Candle> c1h
    ) {
        // --- базовая валидация
        if (!valid(c5m, 30) || !valid(c15m, 30) || !valid(c1h, 30))
            return Optional.empty();

        TradingCore.Candle last5 = last(c5m);

        double atr5 = SignalSender.atr(c5m, 14);
        if (atr5 <= 0)
            return Optional.empty();

        Trend trend1h = detectTrend(c1h);
        Trend trend15m = detectTrend(c15m);
        Entry entry5m = detectImpulseEntry(c5m, atr5);

        boolean impulse = entry5m == Entry.IMPULSE_LONG || entry5m == Entry.IMPULSE_SHORT;

// если не импульс — fallback на старую логику
        if (!impulse) {
            entry5m = detectEntry(c5m);
        }


        if (entry5m == Entry.NONE)
            return Optional.empty();

        if (!impulse && trend1h == Trend.NONE)
            return Optional.empty();

        if (!trendAligned(trend1h, trend15m, entry5m))
            return Optional.empty();

        double confidence = buildConfidence(trend1h, trend15m, entry5m, atr5);

        if (confidence < 0.60)
            return Optional.empty();

        String side = entry5m == Entry.LONG ? "LONG" : "SHORT";

        return Optional.of(
                side.equals("LONG")
                        ? TradeIdea.longIdea(
                        symbol,
                        last5.close,
                        atr5,
                        confidence,
                        "HTF trend + MTF momentum + 5m breakout"
                )
                        : TradeIdea.shortIdea(
                        symbol,
                        last5.close,
                        atr5,
                        confidence,
                        "HTF trend + MTF momentum + 5m breakdown"
                )
        );
    }

    // ================== TREND (HTF / MTF) ==================
    private Trend detectTrend(List<TradingCore.Candle> candles) {
        int lookback = 25;
        TradingCore.Candle first = candles.get(candles.size() - lookback);
        TradingCore.Candle last = candles.get(candles.size() - 1);

        double pct = (last.close - first.close) / (first.close + 1e-12);

        if (pct > 0.004) return Trend.UP;
        if (pct < -0.004) return Trend.DOWN;
        return Trend.NONE;
    }

    // ================== ENTRY (5m) ==================
    private Entry detectEntry(List<TradingCore.Candle> c5m) {
        TradingCore.Candle prev = c5m.get(c5m.size() - 2);
        TradingCore.Candle last = c5m.get(c5m.size() - 1);

        // импульс + пробой
        if (last.close > prev.high && last.close > last.open)
            return Entry.LONG;

        if (last.close < prev.low && last.close < last.open)
            return Entry.SHORT;

        return Entry.NONE;
    }

    // ================== ALIGNMENT ==================
    private boolean trendAligned(
            Trend t1h,
            Trend t15m,
            Entry entry
    ) {
        if (entry == Entry.LONG)
            return t1h == Trend.UP && t15m != Trend.DOWN;

        if (entry == Entry.SHORT)
            return t1h == Trend.DOWN && t15m != Trend.UP;

        return false;
    }

    // ================== CONFIDENCE ==================
    private double buildConfidence(
            Trend t1h,
            Trend t15m,
            Entry entry,
            double atr
    ) {
        double conf = 0.50;

        // HTF alignment
        conf += 0.15;

        // MTF confirmation
        if ((entry == Entry.LONG && t15m == Trend.UP) ||
                (entry == Entry.SHORT && t15m == Trend.DOWN)) {
            conf += 0.10;
        }

        // volatility presence
        if (atr > 0)
            conf += 0.05;

        return Math.min(conf, 0.85);
    }

    // ================== UTILS ==================
    private boolean valid(List<?> list, int min) {
        return list != null && list.size() >= min;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> list) {
        return list.get(list.size() - 1);
    }

    // ================== ENUMS ==================
    private enum Trend {
        UP, DOWN, NONE
    }

    private enum Entry {
        LONG, SHORT, IMPULSE_LONG, IMPULSE_SHORT, NONE
    }
    private Entry detectImpulseEntry(List<TradingCore.Candle> c5m, double atr) {
        TradingCore.Candle last = c5m.get(c5m.size() - 1);

        double body = Math.abs(last.close - last.open);
        double range = last.high - last.low;

        // защита
        if (atr <= 0) return Entry.NONE;

        // 🔥 импульсный шорт
        if (
                last.close < last.open &&           // красная
                        body > atr * 1.2 &&                 // тело больше ATR
                        range > atr * 1.5                  // свеча аномальная
        ) {
            return Entry.IMPULSE_SHORT;
        }

        // 🔥 импульсный лонг
        if (
                last.close > last.open &&
                        body > atr * 1.2 &&
                        range > atr * 1.5
        ) {
            return Entry.IMPULSE_LONG;
        }

        return Entry.NONE;
    }
}
