package com.bot;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DecisionEngineV2 {

    // ================== TradeIdea ==================
    public static class TradeIdea {
        public String symbol;
        public String side;
        public double entry;     // реальный вход (после пробоя)
        public double trigger;   // уровень пробоя
        public double atr;
        public double confidence;
        public String reason;

        public static TradeIdea longIdea(String s, double trigger, double atr, String r, double conf) {
            TradeIdea i = new TradeIdea();
            i.symbol = s;
            i.side = "LONG";
            i.trigger = trigger;
            i.entry = trigger;  // entry будет подтверждаться в SignalSender
            i.atr = atr;
            i.reason = r;
            i.confidence = conf;
            return i;
        }

        public static TradeIdea shortIdea(String s, double trigger, double atr, String r, double conf) {
            TradeIdea i = new TradeIdea();
            i.symbol = s;
            i.side = "SHORT";
            i.trigger = trigger;
            i.entry = trigger;
            i.atr = atr;
            i.reason = r;
            i.confidence = conf;
            return i;
        }
    }

    // ================== TA ==================
    public static class TA {

        public static double ema(List<Double> values, int period) {
            double k = 2.0 / (period + 1);
            double ema = values.get(0);
            for (int i = 1; i < values.size(); i++)
                ema = values.get(i) * k + ema * (1 - k);
            return ema;
        }

        public static double rsi(List<Double> closes, int period) {
            if (closes.size() <= period) return 50;

            double gain = 0, loss = 0;
            for (int i = closes.size() - period; i < closes.size() - 1; i++) {
                double d = closes.get(i + 1) - closes.get(i);
                if (d > 0) gain += d;
                else loss -= d;
            }

            double avgGain = gain / period;
            double avgLoss = loss / period;

            if (avgLoss == 0) return 100;
            double rs = avgGain / avgLoss;
            return 100 - (100 / (1 + rs));
        }

        public static double atr(List<Candle> candles, int period) {
            if (candles.size() <= period) return 0;

            double sum = 0;
            for (int i = candles.size() - period; i < candles.size(); i++) {
                Candle c = candles.get(i);
                Candle prev = candles.get(i - 1);

                double tr = Math.max(c.high - c.low,
                        Math.max(Math.abs(c.high - prev.close), Math.abs(c.low - prev.close)));

                sum += tr;
            }
            return sum / period;
        }
    }

    // ================== EVALUATE ==================
    public Optional<TradeIdea> evaluate(
            String symbol,
            List<Candle> candles5m,
            List<Candle> candles15m,
            List<Candle> candles1h
    ) {
        if (candles5m.size() < 50 || candles15m.size() < 50)
            return Optional.empty();

        // ===== 15m CONTEXT =====
        List<Double> closes15 = candles15m.stream().map(c -> c.close).collect(Collectors.toList());
        int contextDir =
                TA.ema(closes15, 20) > TA.ema(closes15, 50) ? 1 :
                        TA.ema(closes15, 20) < TA.ema(closes15, 50) ? -1 : 0;

        if (contextDir == 0) return Optional.empty();

        double confidence = 0.60;

        // ===== HTF BIAS ONLY =====
        if (!candles1h.isEmpty()) {
            List<Double> closes1h = candles1h.stream().map(c -> c.close).toList();
            boolean htfConflict =
                    (contextDir == 1 && TA.ema(closes1h, 20) < TA.ema(closes1h, 50)) ||
                            (contextDir == -1 && TA.ema(closes1h, 20) > TA.ema(closes1h, 50));
            if (htfConflict) confidence -= 0.10;
        }

        // ===== 5m STATE =====
        List<Double> closes5 = candles5m.stream().map(c -> c.close).toList();
        double rsi5 = TA.rsi(closes5, 14);
        double atr5 = TA.atr(candles5m, 14);

        Candle last = candles5m.get(candles5m.size() - 1);
        Candle prev = candles5m.get(candles5m.size() - 2);

        boolean structureBreakUp =
                prev.close < prev.open &&
                        last.close > prev.high &&
                        (last.high - last.low) < atr5 * 1.2;

        boolean structureBreakDown =
                prev.close > prev.open &&
                        last.close < prev.low &&
                        (last.high - last.low) < atr5 * 1.2;

        // ===== ATR PHASE =====
        double atrAvg = TA.atr(candles5m.subList(0, candles5m.size() - 5), 14);
        boolean atrCompression = atr5 < atrAvg * 0.85;
        if (atr5 > atrAvg * 1.5) return Optional.empty();

        // ===== HARD RSI BLOCK =====
        if (contextDir == 1 && rsi5 > 75) return Optional.empty();
        if (contextDir == -1 && rsi5 < 25) return Optional.empty();

        confidence = Math.min(confidence + 0.10, 0.75);

        // ===== ENTRY =====
        double triggerLong = prev.high + atr5 * 0.02;
        double triggerShort = prev.low - atr5 * 0.02;

        // защита от нулевого триггера
        if (triggerLong <= 0 || triggerShort <= 0) return Optional.empty();

        if (contextDir == 1 &&
                atrCompression &&
                rsi5 > 35 && rsi5 < 50 &&
                structureBreakUp) {

            return Optional.of(
                    TradeIdea.longIdea(symbol, triggerLong, atr5, "MTF structure LONG", confidence)
            );
        }

        if (contextDir == -1 &&
                atrCompression &&
                rsi5 < 65 && rsi5 > 50 &&
                structureBreakDown) {

            return Optional.of(
                    TradeIdea.shortIdea(symbol, triggerShort, atr5, "MTF structure SHORT", confidence)
            );
        }

        return Optional.empty();
    }
}
