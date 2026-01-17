package com.bot;

public class PositionSizer {

    private final double riskPerTrade; // например 0.5% = 0.005

    public PositionSizer(double riskPerTrade) {
        this.riskPerTrade = riskPerTrade;
    }

    public double calcSize(double balance, double entry, double stop) {
        double risk = Math.abs(entry - stop);
        double moneyRisk = balance * riskPerTrade;
        return moneyRisk / risk;
    }
}
