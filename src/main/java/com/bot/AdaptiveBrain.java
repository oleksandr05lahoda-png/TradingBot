package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AdaptiveBrain {

    // ====== 1. FEEDBACK / OUTCOME ======
    private static class OutcomeStat {
        int wins = 0;
        int losses = 0;
    }

    private final Map<String, OutcomeStat> strategyStats = new ConcurrentHashMap<>();

    public void recordOutcome(String strategy, boolean win) {
        strategyStats.putIfAbsent(strategy, new OutcomeStat());
        OutcomeStat s = strategyStats.get(strategy);
        if (win) s.wins++; else s.losses++;
    }

    private double winrate(String strategy) {
        OutcomeStat s = strategyStats.get(strategy);
        if (s == null) return 0.5;
        int total = s.wins + s.losses;
        if (total < 20) return 0.5;
        return (double) s.wins / total;
    }
    // ====== 2. ADAPTIVE CONFIDENCE ======
    public double adaptConfidence(String strategy, double baseConf) {
        double wr = winrate(strategy);
        if (wr > 0.60) return Math.min(0.85, baseConf + 0.05);
        if (wr < 0.45) return Math.max(0.55, baseConf - 0.05);
        return baseConf;
    }
    // ====== 3. IMPULSE SESSION CONTROL ======
    private final Map<String, Deque<Long>> impulseHistory = new ConcurrentHashMap<>();

    public double impulsePenalty(String pair) {
        impulseHistory.putIfAbsent(pair, new ArrayDeque<>());
        Deque<Long> q = impulseHistory.get(pair);

        long now = System.currentTimeMillis();
        q.addLast(now);
        while (!q.isEmpty() && now - q.peekFirst() > 20 * 60_000)
            q.pollFirst();

        if (q.size() >= 3) return -0.07;
        if (q.size() == 2) return -0.03;
        return 0.0;
    }

    // ====== 4. MARKET REGIME ======
    public enum Regime { TREND, RANGE, CHAOS }

    public Regime detectRegime(List<Candle> candles) {
        if (candles == null || candles.size() < 30) return Regime.CHAOS;

        double atr = SignalSender.atr(candles, 14);
        double atrPrev = SignalSender.atr(candles.subList(0, candles.size() - 5), 14);

        int structure = SignalSender.marketStructure(candles);

        if (atr > atrPrev * 1.2 && structure != 0) return Regime.TREND;
        if (atr < atrPrev * 0.8 && structure == 0) return Regime.RANGE;

        return Regime.CHAOS;
    }

    // ====== 5. SESSION BOOST ======
    public double sessionBoost() {
        int h = LocalTime.now(ZoneOffset.UTC).getHour();
        // London + NY
        if ((h >= 7 && h <= 10) || (h >= 13 && h <= 16))
            return 0.05;
        return 0.0;
    }
}
