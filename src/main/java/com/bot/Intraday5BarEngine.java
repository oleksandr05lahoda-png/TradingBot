package com.bot;

import java.util.*;
import java.util.stream.Collectors;

public class Intraday5BarEngine {

    public Optional<DecisionEngineMerged.TradeIdea> evaluate(
            String symbol,
            List<TradingCore.Candle> c5,
            List<TradingCore.Candle> c15
    ) {

        // ===== базовые проверки
        if (c5 == null || c15 == null || c5.size() < 30 || c15.size() < 50)
            return Optional.empty();

        TradingCore.Candle last = c5.get(c5.size() - 1);

        // ===== 1. HTF bias (15m)
        List<Double> cl15 = c15.stream()
                .map(x -> x.close)
                .collect(Collectors.toList());

        int htfBias = Integer.signum(Double.compare(
                DecisionEngineMerged.TA.ema(cl15, 20),
                DecisionEngineMerged.TA.ema(cl15, 50)
        ));

        // ===== 2. Micro momentum (последние 5 свечей)
        int bull = 0, bear = 0;
        for (int i = c5.size() - 5; i < c5.size(); i++) {
            if (c5.get(i).close > c5.get(i).open) bull++;
            else bear++;
        }

        // ===== 3. EMA velocity (ускорение)
        List<Double> cl5 = c5.stream()
                .map(x -> x.close)
                .collect(Collectors.toList());

        double emaNow  = DecisionEngineMerged.TA.ema(cl5, 9);
        double emaPrev = DecisionEngineMerged.TA.ema(
                cl5.subList(0, cl5.size() - 5), 9
        );
        double emaVel = emaNow - emaPrev;

        // ===== 4. ATR фильтр (не лезем в хай-волу)
        double atr = DecisionEngineMerged.TA.atr(c5, 14);
        double range = last.high - last.low;
        if (range > atr * 1.6)
            return Optional.empty();

        // ===== 5. Exhaustion (ОСНОВА стратегии)
        boolean exhaustionDown = bear >= 4 && emaVel > 0; // падали, но EMA растёт
        boolean exhaustionUp   = bull >= 4 && emaVel < 0; // росли, но EMA падает

        if (!exhaustionUp && !exhaustionDown)
            return Optional.empty();

        String side = exhaustionDown ? "LONG" : "SHORT";

        // ===== 6. Score (вероятность на 5 баров)
        double score = 0.55;

        // сила истощения
        if (bear == 5 || bull == 5) score += 0.15;
        else score += 0.10;

        // ускорение
        if (Math.abs(emaVel) > atr * 0.15)
            score += 0.15;

        // HTF bias — бонус или мягкий штраф
        if (htfBias != 0) {
            if ((side.equals("LONG") && htfBias > 0) ||
                    (side.equals("SHORT") && htfBias < 0)) {
                score += 0.15;
            } else {
                score -= 0.10;
            }
        }

        // финальный фильтр
        if (score < 0.50)
            return Optional.empty();

        // ===== 7. TradeIdea
        DecisionEngineMerged.TradeIdea t =
                new DecisionEngineMerged.TradeIdea();

        t.symbol = symbol;
        t.side = side;
        t.entry = last.close;
        t.atr = atr;
        t.confidence = Math.max(0.50, Math.min(0.85, score));
        t.reason = "5-bar exhaustion momentum forecast";

        return Optional.of(t);
    }
}
