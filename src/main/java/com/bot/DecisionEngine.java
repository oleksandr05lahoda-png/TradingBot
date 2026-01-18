package com.bot;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DecisionEngine {

    // ================== TradeIdea ==================
    public static class TradeIdea {
        public String symbol;
        public String side;
        public double entry;
        public double atr;
        public double confidence;
        public String reason;

        public static TradeIdea longIdea(String s, double e, double atr, String r, double conf) {
            TradeIdea t = new TradeIdea();
            t.symbol = s;
            t.side = "LONG";
            t.entry = e;
            t.atr = atr;
            t.reason = r;
            t.confidence = conf;
            return t;
        }

        public static TradeIdea shortIdea(String s, double e, double atr, String r, double conf) {
            TradeIdea t = new TradeIdea();
            t.symbol = s;
            t.side = "SHORT";
            t.entry = e;
            t.atr = atr;
            t.reason = r;
            t.confidence = conf;
            return t;
        }
    }

    // ================== MarketState ==================
    public static class MarketState {
        public enum Bias { LONG, SHORT, NEUTRAL }
        public final Bias bias;
        public MarketState(Bias bias) { this.bias = bias; }
    }

    // ================== EVALUATE ==================
    public Optional<TradeIdea> evaluate(
            String symbol,
            List<Candle> candles5m,
            List<Candle> candles15m,
            MarketState market
    ) {
        if (candles5m.size() < 50 || candles15m.size() < 50)
            return Optional.empty();

        if (market.bias == MarketState.Bias.NEUTRAL)
            return Optional.empty();

        double rsi5 = TechnicalAnalysis.rsi(
                candles5m.stream().map(c -> c.close).collect(Collectors.toList()),
                14
        );

        double atr5 = TechnicalAnalysis.atr(candles5m, 14);

        Candle last = candles5m.get(candles5m.size() - 1);
        Candle prev = candles5m.get(candles5m.size() - 2);

        // ===== STRUCTURE BREAK (ANTI-LATE) =====
        boolean structureBreakUp =
                prev.close < prev.open &&
                        last.close > prev.high &&
                        (last.high - last.low) < atr5 * 1.2;

        boolean structureBreakDown =
                prev.close > prev.open &&
                        last.close < prev.low &&
                        (last.high - last.low) < atr5 * 1.2;

        boolean trend = TechnicalAnalysis.isTrend(candles15m);

        // ===== RSI HARD BLOCK =====
        if (market.bias == MarketState.Bias.SHORT && rsi5 < 30) return Optional.empty();
        if (market.bias == MarketState.Bias.LONG  && rsi5 > 70) return Optional.empty();

        double confidence = 0.55;
        if (trend) confidence += 0.05;
        if (structureBreakUp || structureBreakDown) confidence += 0.05;
        confidence = Math.min(confidence, 0.75);

        // ===== ENTRY =====
        if (market.bias == MarketState.Bias.LONG &&
                rsi5 > 35 && rsi5 < 50 &&
                structureBreakUp) {

            return Optional.of(
                    TradeIdea.longIdea(symbol, last.close, atr5, "Structure pullback LONG", confidence)
            );
        }

        if (market.bias == MarketState.Bias.SHORT &&
                rsi5 < 65 && rsi5 > 50 &&
                structureBreakDown) {

            return Optional.of(
                    TradeIdea.shortIdea(symbol, last.close, atr5, "Structure pullback SHORT", confidence)
            );
        }

        return Optional.empty();
    }

    // ================== TechnicalAnalysis ==================
    public static class TechnicalAnalysis {

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
            double sum = 0;
            for (int i = candles.size() - period; i < candles.size(); i++) {
                Candle c = candles.get(i);
                sum += (c.high - c.low);
            }
            return sum / period;
        }

        public static double ema(List<Double> values, int period) {
            double k = 2.0 / (period + 1);
            double ema = values.get(0);
            for (int i = 1; i < values.size(); i++)
                ema = values.get(i) * k + ema * (1 - k);
            return ema;
        }

        public static boolean isTrend(List<Candle> candles) {
            if (candles.size() < 30) return false;
            List<Double> closes = candles.stream().map(c -> c.close).toList();
            double fast = ema(closes, 10);
            double slow = ema(closes, 30);
            return Math.abs(fast - slow) / slow > 0.0015;
        }
    }
}
