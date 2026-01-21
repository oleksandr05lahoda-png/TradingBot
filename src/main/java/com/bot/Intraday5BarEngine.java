package com.bot;

import java.util.*;
import java.util.stream.Collectors;

public class Intraday5BarEngine {

    public Optional<DecisionEngineMerged.TradeIdea> evaluate(
            String symbol,
            List<TradingCore.Candle> c5,
            List<TradingCore.Candle> c15
    ) {

        if (c5 == null || c5.size() < 30 || c15 == null)
            return Optional.empty();

        TradingCore.Candle last = c5.get(c5.size() - 1);

        // ===== 1. HTF bias (чтобы не шортить сильный лонг)
        List<Double> cl15 = c15.stream().map(x -> x.close).collect(Collectors.toList());
        int htfBias = Double.compare(
                DecisionEngineMerged.TA.ema(cl15, 20),
                DecisionEngineMerged.TA.ema(cl15, 50)
        );

        // ===== 2. Micro momentum (5 свечей)
        int bull = 0, bear = 0;
        for (int i = c5.size() - 5; i < c5.size(); i++) {
            if (c5.get(i).close > c5.get(i).open) bull++;
            else bear++;
        }

        // ===== 3. EMA velocity
        List<Double> cl5 = c5.stream().map(x -> x.close).collect(Collectors.toList());
        double emaNow = DecisionEngineMerged.TA.ema(cl5, 9);
        double emaPrev = DecisionEngineMerged.TA.ema(cl5.subList(0, cl5.size() - 5), 9);
        double emaVel = emaNow - emaPrev;

        // ===== 4. ATR filter
        double atr = DecisionEngineMerged.TA.atr(c5, 14);
        double range = last.high - last.low;
        if (range > atr * 1.3) return Optional.empty();

        // ===== 5. Score
        double score = 0.0;
        if (bull >= 4 && emaVel < 0) score += 0.40;
        if (bear >= 4 && emaVel > 0) score += 0.40;
        if (Math.abs(emaVel) > atr * 0.15) score += 0.20;
        if (htfBias != 0) score += 0.10;

        if (score < 0.60) return Optional.empty();

        boolean exhaustionUp =
                bull >= 4 &&
                        emaVel < 0;

        boolean exhaustionDown =
                bear >= 4 &&
                        emaVel > 0;

        String side;
        if (exhaustionUp) side = "SHORT";
        else if (exhaustionDown) side = "LONG";
        else return Optional.empty();

        DecisionEngineMerged.TradeIdea t = new DecisionEngineMerged.TradeIdea();
        t.symbol = symbol;
        t.side = side;
        t.entry = last.close;
        t.atr = atr;
        t.confidence = Math.min(0.75, score);
        t.reason = "5-bar momentum forecast";

        return Optional.of(t);
    }
}
