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
    public static class TechnicalAnalysis {
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

    // ================== Main Evaluate ==================
    // минимально 3 таймфрейма: 5m, 15m, 1h. 4h опционально.
    public Optional<TradeIdea> evaluate(String symbol,
                                        List<Candle> candles5m,
                                        List<Candle> candles15m,
                                        List<Candle> candles1h,
                                        List<Candle> candles4h) {
        if (candles5m.size() < 50 || candles15m.size() < 50) return Optional.empty();

        // --- Контекст 15m ---
        List<Double> closes15 = candles15m.stream().map(c -> c.close).collect(Collectors.toList());
        double emaFast15 = TechnicalAnalysis.ema(closes15, 20);
        double emaSlow15 = TechnicalAnalysis.ema(closes15, 50);
        int contextDir = 0;
        if (emaFast15 > emaSlow15) contextDir = 1;
        if (emaFast15 < emaSlow15) contextDir = -1;
        if (contextDir == 0) return Optional.empty();

        // --- Multi-TF фильтр ---
        if (!candles1h.isEmpty() && !candles4h.isEmpty()) {
            List<Double> closes1h = candles1h.stream().map(c -> c.close).collect(Collectors.toList());
            List<Double> closes4h = candles4h.stream().map(c -> c.close).collect(Collectors.toList());
            double emaFast1h = TechnicalAnalysis.ema(closes1h, 20);
            double emaSlow1h = TechnicalAnalysis.ema(closes1h, 50);
            double emaFast4h = TechnicalAnalysis.ema(closes4h, 20);
            double emaSlow4h = TechnicalAnalysis.ema(closes4h, 50);

            if ((contextDir == 1 && (emaFast1h < emaSlow1h || emaFast4h < emaSlow4h)) ||
                    (contextDir == -1 && (emaFast1h > emaSlow1h || emaFast4h > emaSlow4h))) {
                return Optional.empty();
            }
        }

        // --- Состояние 5m ---
        List<Double> closes5 = candles5m.stream().map(c -> c.close).collect(Collectors.toList());
        double rsi5 = TechnicalAnalysis.rsi(closes5, 14);
        double atr5 = TechnicalAnalysis.atr(candles5m, 14);

        Candle last = candles5m.get(candles5m.size() - 1);
        Candle prev = candles5m.get(candles5m.size() - 2);

        boolean impulseUp = last.close > last.open && last.close > prev.high;
        boolean impulseDown = last.close < last.open && last.close < prev.low;

        // --- Фильтр флетового рынка ---
        if (rsi5 > 40 && rsi5 < 60) return Optional.empty();

        // --- Фильтр шумных свечей ---
        double range = last.high - last.low;
        if (range > 1.5 * atr5) return Optional.empty();

        // --- Динамический confidence ---
        double confidence = 0.6;
        if (impulseUp || impulseDown) confidence += 0.1;
        if (rsi5 < 20 || rsi5 > 80) confidence += 0.05;
        if (contextDir == 1 && last.close > last.open) confidence += 0.05;
        if (contextDir == -1 && last.close < last.open) confidence += 0.05;
        confidence = Math.min(confidence, 0.75);

        // --- Сетап: Trend Pullback ---
        if (contextDir == 1 && rsi5 > 35 && rsi5 < 50 && impulseUp) {
            return Optional.of(TradeIdea.longIdea(symbol, last.close, atr5, "Trend pullback (15m up)", confidence));
        }

        if (contextDir == -1 && rsi5 < 65 && rsi5 > 50 && impulseDown) {
            return Optional.of(TradeIdea.shortIdea(symbol, last.close, atr5, "Trend pullback (15m down)", confidence));
        }

        return Optional.empty();
    }

    // перегрузка без 4h для SignalSender
    public Optional<TradeIdea> evaluate(String symbol,
                                        List<Candle> candles5m,
                                        List<Candle> candles15m) {
        return evaluate(symbol, candles5m, candles15m, List.of(), List.of());
    }
}
