package com.bot;

import com.bot.Candle;
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
    public class MarketState {

        public enum Bias {
            LONG,
            SHORT,
            NEUTRAL
        }

        public final Bias bias;

        public MarketState(Bias bias) {
            this.bias = bias;
        }
    }

    // ================== Evaluate logic ==================
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
                candles5m.stream()
                        .map(c -> c.close)
                        .collect(Collectors.toList()),
                14
        );

        double atr5 = TechnicalAnalysis.atr(candles5m, 14);

        Candle last = candles5m.get(candles5m.size() - 1);
        Candle prev = candles5m.get(candles5m.size() - 2);

        boolean impulseUp = last.close > last.open && last.close > prev.high;
        boolean impulseDown = last.close < last.open && last.close < prev.low;

        double confidence = 0.6;

        if (market.bias == MarketState.Bias.LONG && rsi5 > 35 && rsi5 < 50 && impulseUp)
            return Optional.of(
                    TradeIdea.longIdea(symbol, last.close, atr5, "Trend pullback", confidence)
            );

        if (market.bias == MarketState.Bias.SHORT && rsi5 < 65 && rsi5 > 50 && impulseDown)
            return Optional.of(
                    TradeIdea.shortIdea(symbol, last.close, atr5, "Trend pullback", confidence)
            );

        return Optional.empty();
    }

    // ================== TechnicalAnalysis ==================
    public static class TechnicalAnalysis {

        // ===== RSI =====
        public static double rsi(List<Double> closes, int period) {
            if (closes.size() <= period) return 50.0;

            double gain = 0;
            double loss = 0;

            for (int i = closes.size() - period; i < closes.size() - 1; i++) {
                double diff = closes.get(i + 1) - closes.get(i);
                if (diff > 0) gain += diff;
                else loss -= diff;
            }

            if (loss == 0) return 100.0;

            double rs = gain / loss;
            return 100.0 - (100.0 / (1.0 + rs));
        }

        // ===== ATR =====
        public static double atr(List<Candle> candles, int period) {
            if (candles.size() <= period) return 0;

            double sum = 0;
            for (int i = candles.size() - period; i < candles.size(); i++) {
                Candle c = candles.get(i);
                sum += (c.high - c.low);
            }
            return sum / period;
        }

        // ===== EMA =====
        public static double ema(List<Double> values, int period) {
            if (values.isEmpty()) return 0;

            double k = 2.0 / (period + 1);
            double ema = values.get(0);

            for (int i = 1; i < values.size(); i++) {
                ema = values.get(i) * k + ema * (1 - k);
            }
            return ema;
        }

        // ===== TREND CHECK =====
        public static boolean isTrend(List<Candle> candles) {
            if (candles.size() < 20) return false;

            double emaFast = ema(
                    candles.stream().map(c -> c.close).toList(), 10);
            double emaSlow = ema(
                    candles.stream().map(c -> c.close).toList(), 30);

            return Math.abs(emaFast - emaSlow) / emaSlow > 0.001;
        }
    }
}
