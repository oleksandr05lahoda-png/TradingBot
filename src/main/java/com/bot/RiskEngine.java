package com.bot;

import com.bot.Candle;

public class RiskEngine {

    private final double riskPerTrade;

    public RiskEngine(double riskPerTrade) {
        this.riskPerTrade = riskPerTrade;
    }

    public double calcSize(double balance, double entry, double stop) {
        double risk = Math.abs(entry - stop);
        double moneyRisk = balance * riskPerTrade;
        return moneyRisk / (risk == 0 ? 1 : risk);
    }

    public TradeSignal applyRisk(String symbol, String side,
                                 double entryPrice, double atr,
                                 double confidence, String reason) {

        TradeSignal s = new TradeSignal();
        s.symbol = symbol;
        s.side = side;
        s.entry = entryPrice;
        s.confidence = confidence;
        s.reason = reason;

        double risk = atr * (confidence > 0.65 ? 0.9 : 1.2);

        if (side.equalsIgnoreCase("LONG")) {
            s.entry = entryPrice;
            s.stop = entryPrice - risk;
            s.take = entryPrice + risk * 2;
        } else {
            s.entry = entryPrice;
            s.stop = entryPrice + risk;
            s.take = entryPrice - risk * 2;
        }
        if (risk < entryPrice * 0.001)
            risk = entryPrice * 0.001;
        if (side.equalsIgnoreCase("LONG")) {
            s.stop = entryPrice - risk;
            s.take = entryPrice + risk * 2;
        } else {
            s.stop = entryPrice + risk;
            s.take = entryPrice - risk * 2;
        }
        return s;
    }

    public static class TradeSignal {
        public String symbol;
        public String side;
        public double entry, stop, take, confidence;
        public String reason;
        @Override
        public String toString() {
            return String.format("%s %s @%.4f Stop: %.4f Take: %.4f Conf: %.2f Reason: %s", symbol, side, entry, stop, take, confidence, reason);
        }
    }
}
