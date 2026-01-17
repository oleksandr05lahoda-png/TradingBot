package com.bot;

import com.bot.SignalSender.Candle;
import java.util.List;
import java.util.Optional;

public class DecisionEngineV2 {

    public Optional<TradeIdea> evaluate(
            String symbol,
            List<Candle> candles5m,
            List<Candle> candles15m
    ) {
        if (candles5m.size() < 50 || candles15m.size() < 50) {
            return Optional.empty();
        }

        // === 1. КОНТЕКСТ 15m (ГЛАВНОЕ) ===
        double emaFast15 = SignalSender.ema(
                candles15m.stream().map(c -> c.close).toList(), 20);
        double emaSlow15 = SignalSender.ema(
                candles15m.stream().map(c -> c.close).toList(), 50);

        int contextDir = 0;
        if (emaFast15 > emaSlow15) contextDir = 1;
        if (emaFast15 < emaSlow15) contextDir = -1;

        if (contextDir == 0) return Optional.empty();

        // === 2. СОСТОЯНИЕ 5m ===
        double rsi5 = SignalSender.rsi(
                candles5m.stream().map(c -> c.close).toList(),
                14
        );
        double atr5 = SignalSender.atr(candles5m, 14);

        Candle last = candles5m.get(candles5m.size() - 1);
        Candle prev = candles5m.get(candles5m.size() - 2);

        boolean impulseUp = last.close > last.open && last.close > prev.high;
        boolean impulseDown = last.close < last.open && last.close < prev.low;

        // === 3. СЕТАП: TREND PULLBACK ===
        if (contextDir == 1) {
            // LONG ТОЛЬКО ЕСЛИ КОНТЕКСТ LONG
            if (rsi5 > 35 && rsi5 < 50 && impulseUp) {
                return Optional.of(
                        TradeIdea.longIdea(symbol, last.close, atr5,
                                "Trend pullback (15m up)")
                );
            }
        }

        if (contextDir == -1) {
            // SHORT ТОЛЬКО ЕСЛИ КОНТЕКСТ SHORT
            if (rsi5 < 65 && rsi5 > 50 && impulseDown) {
                return Optional.of(
                        TradeIdea.shortIdea(symbol, last.close, atr5,
                                "Trend pullback (15m down)")
                );
            }
        }

        return Optional.empty();
    }

    // === ВНУТРЕННЯЯ МОДЕЛЬ ИДЕИ (НЕ СИГНАЛ) ===
    public static class TradeIdea {
        public String symbol;
        public String side;
        public double entry;
        public double atr;
        public double confidence;
        public String reason;

        static TradeIdea longIdea(String s, double e, double atr, String r) {
            TradeIdea i = new TradeIdea();
            i.symbol = s;
            i.side = "LONG";
            i.entry = e;
            i.atr = atr;
            i.reason = r;
            i.confidence = 0.62;
            return i;
        }

        static TradeIdea shortIdea(String s, double e, double atr, String r) {
            TradeIdea i = new TradeIdea();
            i.symbol = s;
            i.side = "SHORT";
            i.entry = e;
            i.atr = atr;
            i.reason = r;
            i.confidence = 0.62;
            return i;
        }
    }
}
