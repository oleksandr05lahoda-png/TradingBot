package com.bot;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DecisionEngineV2 {

    // ================== TradeIdea ==================
    public static class TradeIdea {
        public String symbol;
        public String side;
        public double entry;
        public double atr;
        public double confidence;
        public String reason;

        public static TradeIdea longIdea(String s, double e, double atr, String r, double conf) {
            TradeIdea i = new TradeIdea();
            i.symbol = s;
            i.side = "LONG";
            i.entry = e;
            i.atr = atr;
            i.reason = r;
            i.confidence = conf;
            return i;
        }

        public static TradeIdea shortIdea(String s, double e, double atr, String r, double conf) {
            TradeIdea i = new TradeIdea();
            i.symbol = s;
            i.side = "SHORT";
            i.entry = e;
            i.atr = atr;
            i.reason = r;
            i.confidence = conf;
            return i;
        }
    }

    // ================== Technical Analysis ==================
    public static class TA {

        public static double ema(List<Double> values, int period) {
            if (values.isEmpty()) return 0;
            double k = 2.0 / (period + 1);
            double ema = values.get(0);
            for (int i = 1; i < values.size(); i++) {
                ema = values.get(i) * k + ema * (1 - k);
            }
            return ema;
        }

        public static double rsi(List<Double> closes, int period) {
            if (closes.size() <= period) return 50.0;
            double gain = 0, loss = 0;
            for (int i = closes.size() - period; i < closes.size() - 1; i++) {
                double diff = closes.get(i + 1) - closes.get(i);
                if (diff > 0) gain += diff;
                else loss -= diff;
            }
            if (loss == 0) return 100.0;
            double rs = gain / loss;
            return 100.0 - (100.0 / (1.0 + rs));
        }

        public static double atr(List<Candle> candles, int period) {
            if (candles.size() <= period) return 0;
            double sum = 0;
            for (int i = candles.size() - period; i < candles.size(); i++) {
                Candle c = candles.get(i);
                sum += (c.high - c.low);
            }
            return sum / period;
        }
    }

    // ================== MAIN EVALUATE ==================
    public Optional<TradeIdea> evaluate(String symbol,
                                        List<Candle> candles5m,
                                        List<Candle> candles15m,
                                        List<Candle> candles1h,
                                        List<Candle> candles4h) {

        if (candles5m.size() < 50 || candles15m.size() < 50)
            return Optional.empty();

        // ===== 15m CONTEXT =====
        List<Double> closes15 = candles15m.stream().map(c -> c.close).collect(Collectors.toList());
        double emaFast15 = TA.ema(closes15, 20);
        double emaSlow15 = TA.ema(closes15, 50);

        int contextDir = 0;
        if (emaFast15 > emaSlow15) contextDir = 1;
        if (emaFast15 < emaSlow15) contextDir = -1;
        if (contextDir == 0) return Optional.empty();

        // ===== HTF CONFIRMATION =====
        if (!candles1h.isEmpty()) {
            List<Double> closes1h = candles1h.stream().map(c -> c.close).collect(Collectors.toList());
            double emaFast1h = TA.ema(closes1h, 20);
            double emaSlow1h = TA.ema(closes1h, 50);

            if ((contextDir == 1 && emaFast1h < emaSlow1h) ||
                    (contextDir == -1 && emaFast1h > emaSlow1h))
                return Optional.empty();
        }

        // ===== 5m STATE =====
        List<Double> closes5 = candles5m.stream().map(c -> c.close).collect(Collectors.toList());
        double rsi5 = TA.rsi(closes5, 14);
        double atr5 = TA.atr(candles5m, 14);

        Candle last = candles5m.get(candles5m.size() - 1);
        Candle prev = candles5m.get(candles5m.size() - 2);

        boolean impulseUp = last.close > last.open && last.close > prev.high;
        boolean impulseDown = last.close < last.open && last.close < prev.low;

        // ===== ATR FALLING (ANTI-LATE ENTRY) =====
        double atrPrev1 = TA.atr(candles5m.subList(0, candles5m.size() - 1), 14);
        double atrPrev2 = TA.atr(candles5m.subList(0, candles5m.size() - 2), 14);

        boolean atrFalling = atr5 < atrPrev1 && atrPrev1 < atrPrev2;

        // ===== POST-CRASH REVERSAL (LONG ONLY) =====
        double rangeSum = 0;
        for (int i = candles5m.size() - 6; i < candles5m.size(); i++) {
            Candle c = candles5m.get(i);
            rangeSum += (c.high - c.low);
        }
        double avgRange = rangeSum / 6.0;

        boolean flatAfterDump = avgRange < atr5 * 0.6;
        boolean bullishClose =
                last.close > last.open &&
                        last.close > candles5m.get(candles5m.size() - 3).close;

        if (contextDir == 1 && flatAfterDump && bullishClose) {
            return Optional.of(
                    TradeIdea.longIdea(
                            symbol,
                            last.close,
                            atr5,
                            "Post-crash reversal",
                            0.65
                    )
            );
        }

        // ===== NOISE FILTER =====
        if (rsi5 > 40 && rsi5 < 60)
            return Optional.empty();

        double candleRange = last.high - last.low;
        if (candleRange > 1.5 * atr5)
            return Optional.empty();

        // ===== CONFIDENCE =====
        double confidence = 0.60;
        if (impulseUp || impulseDown) confidence += 0.10;
        if (rsi5 < 20 || rsi5 > 80) confidence += 0.05;
        if (contextDir == 1 && last.close > last.open) confidence += 0.05;
        if (contextDir == -1 && last.close < last.open) confidence += 0.05;
        confidence = Math.min(confidence, 0.75);

        // ===== TREND PULLBACK =====
        if (contextDir == 1 &&
                rsi5 > 35 && rsi5 < 50 &&
                impulseUp) {

            return Optional.of(
                    TradeIdea.longIdea(
                            symbol,
                            last.close,
                            atr5,
                            "Trend pullback (15m up)",
                            confidence
                    )
            );
        }

        if (contextDir == -1 &&
                rsi5 < 65 && rsi5 > 50 &&
                impulseDown &&
                !atrFalling) {

            return Optional.of(
                    TradeIdea.shortIdea(
                            symbol,
                            last.close,
                            atr5,
                            "Trend pullback (15m down)",
                            confidence
                    )
            );
        }

        return Optional.empty();
    }

    // ===== OVERLOAD =====
    public Optional<TradeIdea> evaluate(String symbol,
                                        List<Candle> candles5m,
                                        List<Candle> candles15m) {
        return evaluate(symbol, candles5m, candles15m, List.of(), List.of());
    }
}
