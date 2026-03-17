package com.bot;

import java.util.*;

/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║  SimpleBacktester — ИСПРАВЛЕННАЯ ВЕРСИЯ                         ║
 * ║                                                                  ║
 * ║  ИСПРАВЛЕНО:                                                     ║
 * ║  1. run() передавал List.of() вместо реальных c1/c5             ║
 * ║     Теперь передаёт реальные срезы — результаты ТОЧНЫЕ          ║
 * ║  2. sweepConfidence() — добавлен расчёт avgRR и                  ║
 * ║     Expected Value (EV = winRate × avgRR - lossRate)             ║
 * ║  3. Новый метод sweepCooldown() — находит оптимальный кулдаун   ║
 * ║  4. Новый метод findBestParams() — перебирает conf + порог      ║
 * ║  5. Backtestный отчёт содержит по-символьную статистику         ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */
public final class SimpleBacktester {

    private final DecisionEngineMerged engine = new DecisionEngineMerged();

    // ── Результат бэктеста ─────────────────────────────────────────

    public static final class BacktestResult {
        public int    total, wins, losses;
        public double winRate, avgRR, totalPnL, ev;
        public String symbol;

        public BacktestResult(String symbol) { this.symbol = symbol; }

        @Override
        public String toString() {
            return String.format(
                    "[Backtest %s] Total=%d Wins=%d WinRate=%.1f%% AvgRR=%.2f TotalPnL=%.2f%% EV=%.3f",
                    symbol, total, wins, winRate * 100, avgRR, totalPnL, ev);
        }

        /** Expected Value — сколько зарабатываем в среднем на одну сделку */
        public void calcEV() {
            double lossRate = 1.0 - winRate;
            ev = winRate * avgRR - lossRate;
        }
    }

    /**
     * Прогоняет скользящее окно по всей истории.
     * ИСПРАВЛЕНО: теперь использует реальные c1/c5 срезы.
     *
     * @param symbol  символ
     * @param m1      история 1m (опционально, может быть null)
     * @param m5      история 5m (опционально, может быть null)
     * @param m15     история 15m (минимум 400 свечей)
     * @param h1      история 1h
     */
    public BacktestResult run(String symbol,
                              List<TradingCore.Candle> m1,
                              List<TradingCore.Candle> m5,
                              List<TradingCore.Candle> m15,
                              List<TradingCore.Candle> h1) {
        BacktestResult res  = new BacktestResult(symbol);
        final int WINDOW    = 200;
        final int FORWARD   = 10;

        if (m15 == null || m15.size() < WINDOW + FORWARD) {
            System.out.println("[Backtest] Not enough m15 data for " + symbol
                    + " (need " + (WINDOW + FORWARD) + ", got " + (m15 == null ? 0 : m15.size()) + ")");
            return res;
        }

        for (int i = WINDOW; i < m15.size() - FORWARD; i++) {
            List<TradingCore.Candle> slice15 = m15.subList(i - WINDOW, i);
            List<TradingCore.Candle> sliceH1 = getH1Slice(h1, i, m15);

            // ИСПРАВЛЕНО: передаём реальные срезы m1/m5 (не List.of())
            List<TradingCore.Candle> sliceM1 = getM1Slice(m1, i, m15);
            List<TradingCore.Candle> sliceM5 = getM5Slice(m5, i, m15);

            DecisionEngineMerged.TradeIdea idea;
            try {
                idea = engine.analyze(symbol,
                        sliceM1,   // c1 — реальные 1-минутные свечи
                        sliceM5,   // c5 — реальные 5-минутные свечи
                        slice15,
                        sliceH1,
                        List.of(), // c2h — нет в бэктесте
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
            if (win) {
                res.wins++;
                res.totalPnL += rr;
                res.avgRR    += rr;
            } else {
                res.losses++;
                res.totalPnL -= 1.0;
            }
        }

        if (res.total > 0) {
            res.winRate = (double) res.wins / res.total;
            res.avgRR   = res.wins > 0 ? res.avgRR / res.wins : 0;
            res.calcEV();
        }
        return res;
    }

    /**
     * Обратная совместимость — без m1/m5.
     */
    public BacktestResult run(String symbol,
                              List<TradingCore.Candle> m15,
                              List<TradingCore.Candle> h1) {
        return run(symbol, null, null, m15, h1);
    }

    /**
     * Параметрический sweep по minConfidence.
     * ИСПРАВЛЕНО: добавлен расчёт avgRR и EV (Expected Value).
     * EV > 0 означает прибыльную стратегию.
     */
    public void sweepConfidence(String symbol,
                                List<TradingCore.Candle> m1,
                                List<TradingCore.Candle> m5,
                                List<TradingCore.Candle> m15,
                                List<TradingCore.Candle> h1) {
        System.out.println("=== Confidence Sweep для " + symbol + " ===");
        System.out.printf("%-8s %-8s %-10s %-8s %-8s %-8s%n",
                "conf", "total", "winRate%", "avgRR", "pnl", "EV");

        for (double conf = 50; conf <= 80; conf += 2) {
            final double c = conf;
            BacktestResult res  = new BacktestResult(symbol);
            final int WINDOW    = 200, FORWARD = 10;
            if (m15 == null || m15.size() < WINDOW + FORWARD) break;

            for (int i = WINDOW; i < m15.size() - FORWARD; i++) {
                List<TradingCore.Candle> s15 = m15.subList(i - WINDOW, i);
                List<TradingCore.Candle> sH1 = getH1Slice(h1, i, m15);
                List<TradingCore.Candle> sM1 = getM1Slice(m1, i, m15);
                List<TradingCore.Candle> sM5 = getM5Slice(m5, i, m15);

                DecisionEngineMerged eng2 = new DecisionEngineMerged();
                DecisionEngineMerged.TradeIdea idea;
                try {
                    idea = eng2.analyze(symbol, sM1, sM5, s15, sH1, List.of(),
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
                double rr = Math.abs(idea.tp1 - idea.price) /
                        Math.max(Math.abs(idea.stop - idea.price), 1e-9);
                if (win) { res.wins++; res.totalPnL += rr; res.avgRR += rr; }
                else     { res.losses++; res.totalPnL -= 1.0; }
            }

            if (res.total > 0) {
                res.winRate = (double) res.wins / res.total;
                res.avgRR   = res.wins > 0 ? res.avgRR / res.wins : 0;
                res.calcEV();
            }

            // Маркируем строки с положительным EV
            String marker = (res.ev > 0.2) ? " ★★" : (res.ev > 0) ? " ★" : "";
            System.out.printf("%-8.0f %-8d %-10.1f %-8.2f %-8.1f %-8.3f%s%n",
                    c, res.total, res.winRate * 100, res.avgRR, res.totalPnL, res.ev, marker);
        }
    }

    /**
     * Sweep по кулдауну — находит оптимальный COOLDOWN_TOP.
     * Перебирает от 2 до 15 минут.
     */
    public void sweepCooldown(String symbol,
                              List<TradingCore.Candle> m15,
                              List<TradingCore.Candle> h1) {
        System.out.println("=== Cooldown Sweep для " + symbol + " ===");
        System.out.printf("%-10s %-8s %-10s %-8s %-8s%n",
                "cooldownM", "total", "winRate%", "pnl", "EV");

        final int WINDOW = 200, FORWARD = 10;
        if (m15 == null || m15.size() < WINDOW + FORWARD) return;

        for (int cdMin = 2; cdMin <= 15; cdMin++) {
            // Симулируем кулдаун вручную
            BacktestResult res = new BacktestResult(symbol);
            Map<String, Long> cdMap = new HashMap<>();
            long cdMs = cdMin * 60_000L;

            for (int i = WINDOW; i < m15.size() - FORWARD; i++) {
                List<TradingCore.Candle> s15 = m15.subList(i - WINDOW, i);
                List<TradingCore.Candle> sH1 = getH1Slice(h1, i, m15);

                DecisionEngineMerged.TradeIdea idea;
                try {
                    idea = new DecisionEngineMerged().analyze(symbol,
                            List.of(), List.of(), s15, sH1, List.of(),
                            DecisionEngineMerged.CoinCategory.TOP);
                } catch (Exception e) { continue; }
                if (idea == null) continue;

                // Проверяем кулдаун вручную
                String key = symbol + "_" + idea.side;
                long nowSim = m15.get(i).closeTime;
                Long lastCd = cdMap.get(key);
                if (lastCd != null && nowSim - lastCd < cdMs) continue;
                cdMap.put(key, nowSim);

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
                double rr = Math.abs(idea.tp1 - idea.price) /
                        Math.max(Math.abs(idea.stop - idea.price), 1e-9);
                if (win) { res.wins++; res.totalPnL += rr; res.avgRR += rr; }
                else     { res.losses++; res.totalPnL -= 1.0; }
            }

            if (res.total > 0) {
                res.winRate = (double) res.wins / res.total;
                res.avgRR   = res.wins > 0 ? res.avgRR / res.wins : 0;
                res.calcEV();
            }

            String marker = (res.ev > 0.2) ? " ★★ OPTIMAL" : (res.ev > 0) ? " ★" : "";
            System.out.printf("%-10d %-8d %-10.1f %-8.1f %-8.3f%s%n",
                    cdMin, res.total, res.winRate * 100, res.totalPnL, res.ev, marker);
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  ХЕЛПЕРЫ для синхронизации таймфреймов
    // ─────────────────────────────────────────────────────────────

    private List<TradingCore.Candle> getH1Slice(
            List<TradingCore.Candle> h1,
            int m15Index,
            List<TradingCore.Candle> m15) {
        if (h1 == null || h1.isEmpty()) return List.of();
        int h1Index = m15Index / 4;
        int start   = Math.max(0, h1Index - 150);
        int end     = Math.min(h1.size(), h1Index);
        return end > start ? h1.subList(start, end) : List.of();
    }

    /**
     * Синхронизируем 1m срез: 15 свечей 1m на каждую свечу 15m.
     * m15Index=200 → берём m1 диапазон [200*15 - 300 .. 200*15].
     */
    private List<TradingCore.Candle> getM1Slice(
            List<TradingCore.Candle> m1,
            int m15Index,
            List<TradingCore.Candle> m15) {
        if (m1 == null || m1.isEmpty()) return List.of();
        int m1End   = Math.min(m1.size(), m15Index * 15);
        int m1Start = Math.max(0, m1End - 300); // ~20 баров 15m назад
        return m1End > m1Start ? m1.subList(m1Start, m1End) : List.of();
    }

    /**
     * Синхронизируем 5m срез: 3 свечи 5m на каждую свечу 15m.
     */
    private List<TradingCore.Candle> getM5Slice(
            List<TradingCore.Candle> m5,
            int m15Index,
            List<TradingCore.Candle> m15) {
        if (m5 == null || m5.isEmpty()) return List.of();
        int m5End   = Math.min(m5.size(), m15Index * 3);
        int m5Start = Math.max(0, m5End - 120);
        return m5End > m5Start ? m5.subList(m5Start, m5End) : List.of();
    }
}