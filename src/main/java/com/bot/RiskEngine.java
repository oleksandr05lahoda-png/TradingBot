package com.bot;

public class RiskEngine {

    private final double maxLeverage;

    // Конструктор RiskEngine
    public RiskEngine(double maxLeverage) {
        this.maxLeverage = maxLeverage;
    }

    // Внутренний класс TradeSignal
    public static class TradeSignal {
        public String symbol;
        public String side;      // "LONG" или "SHORT"
        public double entry;
        public double stop;
        public double take;
        public double confidence;
        public String reason;

        @Override
        public String toString() {
            return String.format("%s %s @%.4f, Stop: %.4f, Take: %.4f, Conf: %.2f, Reason: %s",
                    symbol, side, entry, stop, take, confidence, reason);
        }
    }

    // Метод для расчёта уровней риска
    public TradeSignal applyRisk(String symbol, String side, double entryPrice, double atr, double confidence, String reason) {
        TradeSignal s = new TradeSignal();
        s.symbol = symbol;
        s.side = side;
        s.entry = entryPrice;
        s.confidence = confidence;
        s.reason = reason;

        double risk = atr * 1.0; // базовый риск на ATR
        if (risk < entryPrice * 0.001) risk = entryPrice * 0.001; // минимальный стоп

        if (side.equalsIgnoreCase("LONG")) {
            s.stop = entryPrice - risk;
            s.take = entryPrice + risk * 2.0; // отношение риск:прибыль 1:2
        } else {
            s.stop = entryPrice + risk;
            s.take = entryPrice - risk * 2.0;
        }

        return s;
    }
}
