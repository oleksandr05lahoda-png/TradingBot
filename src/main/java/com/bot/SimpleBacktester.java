package com.bot;

import java.util.*;

public final class SimpleBacktester {

    private final DecisionEngineMerged engine = new DecisionEngineMerged();

    public static final class BacktestResult {
        public int    total, wins, losses;
        public double winRate, avgRR, totalPnL;
        public String symbol;

        public BacktestResult(String symbol) { this.symbol = symbol; }

        @Override
        public String toString() {
            return String.format(
                    "[Backtest %s] Total=%d Wins=%d WinRate=%.1f%% AvgRR=%.2f TotalPnL=%.2f%%",
                    symbol, total, wins, winRate * 100, avgRR, totalPnL);
        }
    }

    /**
     * Прогоняет скользящее окно 200 свечей по всей истории.
     * Для каждого сигнала проверяет следующие 10 свечей — достигнут SL или TP.
     *
     * @param symbol  символ (для логов)
     * @param m15     история 15m (минимум 400 свечей для смысла)
     * @param h1      история 1h (синхронизируется пропорционально)
     */
    public BacktestResult run(String symbol,
                              List<TradingCore.Candle> m15,
                              List<TradingCore.Candle> h1) {
        BacktestResult res  = new BacktestResult(symbol);
        final int WINDOW    = 200;
        final int FORWARD   = 10; // свечей вперёд для проверки

        if (m15 == null || m15.size() < WINDOW + FORWARD) {
            System.out.println("[Backtest] Not enough data for " + symbol);
            return res;
        }

        for (int i = WINDOW; i < m15.size() - FORWARD; i++) {
            List<TradingCore.Candle> slice15 = m15.subList(i - WINDOW, i);
            List<TradingCore.Candle> sliceH1 = getH1Slice(h1, i, m15);

            DecisionEngineMerged.TradeIdea idea;
            try {
                idea = engine.analyze(symbol, List.of(), List.of(), slice15, sliceH1, List.of(),
                        DecisionEngineMerged.CoinCategory.TOP);
            } catch (Exception e) {
                continue;
            }
            if (idea == null) continue;

            boolean win = false, touched = false;

            for (int j = i; j < Math.min(i + FORWARD, m15.size()); j++) {
                TradingCore.Candle c = m15.get(j);
                if (idea.side == TradingCore.Side.LONG) {
                    if (c.low  <= idea.stop) { touched = true; win = false; break; }
                    if (c.high >= idea.tp1)  { touched = true; win = true;  break; }
                } else {
                    if (c.high >= idea.stop) { touched = true; win = false; break; }
                    if (c.low  <= idea.tp1)  { touched = true; win = true;  break; }
                }
            }
            if (!touched) continue;

            res.total++;
            double rr = Math.abs(idea.tp1 - idea.price) /
                    Math.max(Math.abs(idea.stop - idea.price), 1e-9);
            if (win) { res.wins++;   res.totalPnL += rr;  res.avgRR += rr; }
            else     { res.losses++; res.totalPnL -= 1.0; }
        }

        if (res.total > 0) {
            res.winRate = (double) res.wins / res.total;
            res.avgRR   = res.wins > 0 ? res.avgRR / res.wins : 0;
        }
        return res;
    }

    /**
     * Параметрический прогон для поиска оптимального minConfidence.
     * Перебирает conf от 52 до 72 с шагом 2 и печатает результаты.
     */
    public void sweepConfidence(String symbol,
                                List<TradingCore.Candle> m15,
                                List<TradingCore.Candle> h1) {
        System.out.println("=== Confidence Sweep for " + symbol + " ===");
        for (double conf = 52; conf <= 72; conf += 2) {
            final double c = conf;
            // Каждый раз новый движок
            DecisionEngineMerged eng = new DecisionEngineMerged();
            BacktestResult res = new BacktestResult(symbol);
            res.total = 0; res.wins = 0; res.losses = 0;

            final int WINDOW = 200, FORWARD = 10;
            if (m15 == null || m15.size() < WINDOW + FORWARD) break;

            for (int i = WINDOW; i < m15.size() - FORWARD; i++) {
                List<TradingCore.Candle> s15 = m15.subList(i - WINDOW, i);
                List<TradingCore.Candle> sH1 = getH1Slice(h1, i, m15);
                DecisionEngineMerged.TradeIdea idea;
                try {
                    idea = eng.analyze(symbol, List.of(), List.of(), s15, sH1, List.of(),
                            DecisionEngineMerged.CoinCategory.TOP);
                } catch (Exception e) { continue; }
                if (idea == null || idea.probability < c) continue;

                boolean win = false, touched = false;
                for (int j = i; j < Math.min(i + FORWARD, m15.size()); j++) {
                    TradingCore.Candle cv = m15.get(j);
                    if (idea.side == TradingCore.Side.LONG) {
                        if (cv.low  <= idea.stop) { touched = true; break; }
                        if (cv.high >= idea.tp1)  { touched = true; win = true; break; }
                    } else {
                        if (cv.high >= idea.stop) { touched = true; break; }
                        if (cv.low  <= idea.tp1)  { touched = true; win = true; break; }
                    }
                }
                if (!touched) continue;
                res.total++;
                if (win) res.wins++; else res.losses++;
                res.totalPnL += win ? 1 : -1;
            }
            if (res.total > 0) res.winRate = (double) res.wins / res.total;
            System.out.printf("  conf>=%.0f: total=%d winRate=%.1f%% pnl=%.1f%n",
                    c, res.total, res.winRate * 100, res.totalPnL);
        }
    }

    private List<TradingCore.Candle> getH1Slice(
            List<TradingCore.Candle> h1,
            int m15Index,
            List<TradingCore.Candle> m15) {
        if (h1 == null || h1.isEmpty()) return List.of();
        // 4 свечи 15m = 1 свеча 1h
        int h1Index = m15Index / 4;
        int start   = Math.max(0, h1Index - 150);
        int end     = Math.min(h1.size(), h1Index);
        return end > start ? h1.subList(start, end) : List.of();
    }
}