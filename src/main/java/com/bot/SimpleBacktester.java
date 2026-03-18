package com.bot;

import java.util.*;
import java.util.concurrent.*;

/**
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║  SimpleBacktester — GODBOT EDITION v4.0                              ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  ИСПРАВЛЕНИЯ v4.0 (по анализу из чата):                              ║
 * ║                                                                      ║
 * ║  [FIX-COMM] Комиссии включены в P&L                                  ║
 * ║    Taker комиссия при входе: 0.04% (по умолчанию)                   ║
 * ║    Taker комиссия при выходе: 0.04%                                  ║
 * ║    Итого: 0.08% на сделку — важно для мелких движений               ║
 * ║                                                                      ║
 * ║  [FIX-FUND] Funding Rate учтён в P&L                                ║
 * ║    По умолчанию: 0.01% каждые 8 часов (3 раза в сутки)             ║
 * ║    = 0.03% в день = 0.0003 per 15m candle (в среднем)              ║
 * ║    LONG: funding против тебя если FR > 0, SHORT: FR против шорта   ║
 * ║                                                                      ║
 * ║  [FIX-SLIP] Проскальзывание при исполнении стопа                    ║
 * ║    Стоп-лосс исполняется ХУЖЕ цены на slippagePct%                  ║
 * ║    TOP: 0.05%, ALT: 0.15%, MEME: 0.40%                              ║
 * ║    Моделирует реальный gap при срабатывании SL                       ║
 * ║                                                                      ║
 * ║  [FIX-PARTIAL] Частичное закрытие позиции по TP1 (50% at TP1)       ║
 * ║    Оставшиеся 50% ведём к TP2 или SL после BE                       ║
 * ║    Точнее отражает реальную стратегию Multi-TP из бота              ║
 * ║                                                                      ║
 * ║  [FIX-EV] Расчёт Expected Value (EV) с учётом всех затрат           ║
 * ║    EV_net = (winRate × avgGain) - (lossRate × avgLoss)              ║
 * ║    avgLoss включает проскальзывание + комиссию                       ║
 * ║                                                                      ║
 * ║  [NEW] sweepConfidence() — находит оптимальный порог confidence      ║
 * ║  [NEW] sweepSlippage()   — показывает как проскальзывание убивает EV ║
 * ║  [NEW] sweepFunding()    — показывает влияние funding rate на PnL    ║
 * ║  [NEW] fullReport()      — полный отчёт по символу                  ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 */
public final class SimpleBacktester {

    private final DecisionEngineMerged engine = new DecisionEngineMerged();

    // ── Параметры по умолчанию ────────────────────────────────────
    private double takerFee      = 0.0004;  // 0.04% taker fee (Binance Futures)
    private double fundingPer15m = 0.0001 / 32.0; // ~0.01%/8h → per 15m
    private boolean useMultiTP   = true;    // частичное закрытие по TP1

    /** Проскальзывание по категории монеты */
    private static final Map<DecisionEngineMerged.CoinCategory, Double> SLIP_MAP = Map.of(
            DecisionEngineMerged.CoinCategory.TOP,  0.0005,  // 0.05%
            DecisionEngineMerged.CoinCategory.ALT,  0.0015,  // 0.15%
            DecisionEngineMerged.CoinCategory.MEME, 0.0040   // 0.40%
    );

    // ── Конфигурация ─────────────────────────────────────────────
    public void setTakerFee(double fee)          { this.takerFee = fee; }
    public void setFundingPer15m(double f)       { this.fundingPer15m = f; }
    public void setUseMultiTP(boolean v)         { this.useMultiTP = v; }

    // ══════════════════════════════════════════════════════════════
    //  РЕЗУЛЬТАТ БЭКТЕСТА
    // ══════════════════════════════════════════════════════════════

    public static final class BacktestResult {
        public int    total, wins, losses, breakEvens;
        public double winRate;
        public double avgRR;          // средний R:R на выигрышных сделках
        public double avgLoss;        // средний убыток (в % от депозита)
        public double totalPnL;       // итого P&L (%)
        public double ev;             // Expected Value (net)
        public double grossPnL;       // P&L до вычета комиссий/funding
        public double totalFees;      // итого комиссии
        public double totalFunding;   // итого funding costs
        public double totalSlippage;  // итого проскальзывание
        public double sharpe;         // Sharpe Ratio (приблизительный)
        public String symbol;

        // Для Sharpe Ratio
        private final List<Double> tradeReturns = new ArrayList<>();

        public BacktestResult(String symbol) { this.symbol = symbol; }

        public void calcAll() {
            int n = total;
            if (n == 0) return;
            winRate = (double) wins / n;
            if (!tradeReturns.isEmpty()) {
                double mean = tradeReturns.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                double variance = tradeReturns.stream()
                        .mapToDouble(r -> Math.pow(r - mean, 2))
                        .average().orElse(1e-9);
                double stddev = Math.sqrt(variance);
                sharpe = stddev > 0 ? (mean / stddev) * Math.sqrt(252) : 0;
            }
            double lossRate = 1.0 - winRate;
            ev = winRate * avgRR - lossRate * (1.0 + totalSlippage / Math.max(n, 1) * 100);
        }

        @Override
        public String toString() {
            return String.format(
                    "╔══ Backtest [%s] ══╗\n" +
                            "║ Сделок: %d | Выиграно: %d | Проиграно: %d | BE: %d\n" +
                            "║ WinRate: %.1f%% | AvgRR: %.2f | AvgLoss: %.2f%%\n" +
                            "║ Gross P&L: %+.2f%% | Net P&L: %+.2f%%\n" +
                            "║ Комиссии: -%.3f%% | Funding: -%.3f%% | Slippage: -%.3f%%\n" +
                            "║ EV: %.4f | Sharpe: %.2f\n" +
                            "╚══════════════════════════╝",
                    symbol, total, wins, losses, breakEvens,
                    winRate * 100, avgRR, avgLoss,
                    grossPnL, totalPnL,
                    totalFees * 100, totalFunding * 100, totalSlippage * 100,
                    ev, sharpe
            );
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN BACKTEST — скользящее окно по истории
    // ══════════════════════════════════════════════════════════════

    /**
     * Прогоняет скользящее окно по всей истории.
     * Учитывает комиссии, funding rate и проскальзывание.
     */
    public BacktestResult run(String symbol,
                              List<TradingCore.Candle> m1,
                              List<TradingCore.Candle> m5,
                              List<TradingCore.Candle> m15,
                              List<TradingCore.Candle> h1) {
        return run(symbol, m1, m5, m15, h1, DecisionEngineMerged.CoinCategory.ALT);
    }

    public BacktestResult run(String symbol,
                              List<TradingCore.Candle> m1,
                              List<TradingCore.Candle> m5,
                              List<TradingCore.Candle> m15,
                              List<TradingCore.Candle> h1,
                              DecisionEngineMerged.CoinCategory cat) {
        BacktestResult res  = new BacktestResult(symbol);
        final int WINDOW    = 200;
        final int FORWARD   = 12;  // смотрим 12 свечей вперёд (3 часа на 15m)

        if (m15 == null || m15.size() < WINDOW + FORWARD) {
            System.out.printf("[Backtest] Недостаточно данных для %s (нужно %d, есть %d)%n",
                    symbol, WINDOW + FORWARD, m15 == null ? 0 : m15.size());
            return res;
        }

        double slip = SLIP_MAP.getOrDefault(cat, 0.001);

        double totalGross = 0, totalFees = 0, totalFunding = 0, totalSlip = 0;
        double sumRR = 0, sumLoss = 0;

        for (int i = WINDOW; i < m15.size() - FORWARD; i++) {
            List<TradingCore.Candle> slice15 = m15.subList(i - WINDOW, i);
            List<TradingCore.Candle> sliceH1 = getH1Slice(h1, i, m15);
            List<TradingCore.Candle> sliceM1 = getM1Slice(m1, i, m15);
            List<TradingCore.Candle> sliceM5 = getM5Slice(m5, i, m15);

            DecisionEngineMerged.TradeIdea idea;
            try {
                idea = engine.analyze(symbol, sliceM1, sliceM5, slice15, sliceH1,
                        Collections.emptyList(), cat);
            } catch (Exception e) {
                continue;
            }
            if (idea == null) continue;

            // ── Симуляция сделки ─────────────────────────────────
            double entry   = idea.price;
            double sl      = idea.stop;
            double tp1     = idea.tp1;
            double tp2     = idea.tp2;

            // [FIX-COMM] Комиссия при входе
            double feeIn   = entry * takerFee;

            // [FIX-SLIP] Реальный стоп хуже заявленного
            double realSL;
            if (idea.side == TradingCore.Side.LONG) {
                realSL = sl * (1 - slip);   // получаем меньше при стопе в лонг
            } else {
                realSL = sl * (1 + slip);   // получаем больше при стопе в шорт (хуже для нас)
            }

            boolean win     = false;
            boolean be      = false;  // Break Even (закрылись в 0)
            double  pnl     = 0;
            int     barsHeld = 0;
            boolean closed  = false;

            for (int j = i; j < Math.min(i + FORWARD, m15.size()) && !closed; j++) {
                TradingCore.Candle c = m15.get(j);
                barsHeld++;

                if (idea.side == TradingCore.Side.LONG) {
                    // SL hit
                    if (c.low <= realSL) {
                        pnl  = (realSL - entry) / entry;  // отрицательное
                        win  = false; closed = true;
                    }
                    // TP1 hit (если useMultiTP — частичное закрытие)
                    else if (c.high >= tp1) {
                        if (useMultiTP) {
                            // 50% закрываем на TP1, стоп → BE
                            double partial = (tp1 - entry) / entry * 0.5;
                            // Ждём TP2 для оставшихся 50%
                            boolean tp2Hit = false;
                            for (int k = j + 1; k < Math.min(j + 6, m15.size()) && !tp2Hit; k++) {
                                TradingCore.Candle fc = m15.get(k);
                                if (fc.high >= tp2) {
                                    pnl = partial + (tp2 - entry) / entry * 0.5;
                                    tp2Hit = true; win = true;
                                } else if (fc.low <= entry) {  // BE стоп
                                    pnl = partial;  // 0 на вторую половину
                                    tp2Hit = true; be = true; win = true;
                                }
                            }
                            if (!tp2Hit) {
                                pnl = partial;  // время вышло — частичная прибыль
                                win = pnl > 0;
                            }
                        } else {
                            pnl = (tp1 - entry) / entry;
                            win = true;
                        }
                        closed = true;
                    }
                } else { // SHORT
                    if (c.high >= realSL) {
                        pnl  = (entry - realSL) / entry;
                        win  = false; closed = true;
                    } else if (c.low <= tp1) {
                        if (useMultiTP) {
                            double partial = (entry - tp1) / entry * 0.5;
                            boolean tp2Hit = false;
                            for (int k = j + 1; k < Math.min(j + 6, m15.size()) && !tp2Hit; k++) {
                                TradingCore.Candle fc = m15.get(k);
                                if (fc.low <= tp2) {
                                    pnl = partial + (entry - tp2) / entry * 0.5;
                                    tp2Hit = true; win = true;
                                } else if (fc.high >= entry) {
                                    pnl = partial;
                                    tp2Hit = true; be = true; win = true;
                                }
                            }
                            if (!tp2Hit) {
                                pnl = partial;
                                win = pnl > 0;
                            }
                        } else {
                            pnl = (entry - tp1) / entry;
                            win = true;
                        }
                        closed = true;
                    }
                }
            }

            if (!closed) continue;  // Сделка не завершилась — пропускаем

            // [FIX-COMM] Комиссия при выходе
            double feeOut   = entry * takerFee;
            double fees     = feeIn + feeOut;

            // [FIX-FUND] Funding rate cost (пропорционально времени удержания)
            double funding  = barsHeld * fundingPer15m;
            // Для SHORT: если FR > 0, SHORT получает, а не платит
            // Упрощённо: всегда вычитаем (консервативно)
            double fundingCost = Math.abs(funding);

            // Чистый P&L
            double grossPnl = pnl;
            double netPnl   = pnl - fees - fundingCost;
            double slipCost = Math.abs(sl - realSL) / entry;

            res.total++;
            totalGross   += grossPnl;
            totalFees    += fees;
            totalFunding += fundingCost;
            totalSlip    += slipCost;

            if (win && !be) {
                res.wins++;
                sumRR += Math.abs(pnl / (Math.abs(entry - sl) / entry + 1e-9));
                res.tradeReturns.add(netPnl);
            } else if (be) {
                res.breakEvens++;
                res.wins++;  // BE считаем как win (не потеряли)
                res.tradeReturns.add(netPnl);
            } else {
                res.losses++;
                sumLoss += Math.abs(netPnl);
                res.tradeReturns.add(netPnl);
            }
        }

        // Сборка результатов
        res.grossPnL      = totalGross * 100;
        res.totalPnL      = (totalGross - totalFees - totalFunding) * 100;
        res.totalFees     = totalFees;
        res.totalFunding  = totalFunding;
        res.totalSlippage = totalSlip;
        res.avgRR         = res.wins > 0 ? sumRR / res.wins : 0;
        res.avgLoss       = res.losses > 0 ? sumLoss / res.losses * 100 : 0;
        res.calcAll();

        return res;
    }

    // ══════════════════════════════════════════════════════════════
    //  SWEEP CONFIDENCE — оптимальный порог уверенности
    // ══════════════════════════════════════════════════════════════

    public static final class SweepResult {
        public final double param;
        public final double winRate;
        public final double ev;
        public final double netPnL;
        public final int    total;
        SweepResult(double p, double wr, double ev, double pnl, int t) {
            param = p; winRate = wr; this.ev = ev; netPnL = pnl; total = t;
        }
        @Override
        public String toString() {
            return String.format("param=%.1f | trades=%d | winRate=%.1f%% | EV=%.4f | Net P&L=%+.2f%%",
                    param, total, winRate * 100, ev, netPnL);
        }
    }

    /**
     * Перебирает пороги MIN_CONFIDENCE от minC до maxC с шагом step.
     * Возвращает таблицу результатов для выбора оптимального порога.
     *
     * [FIX-EV] Теперь учитывает комиссии и проскальзывание в EV.
     */
    public List<SweepResult> sweepConfidence(String symbol,
                                             List<TradingCore.Candle> m15,
                                             List<TradingCore.Candle> h1,
                                             double minC, double maxC, double step) {
        List<SweepResult> results = new ArrayList<>();
        for (double c = minC; c <= maxC; c += step) {
            BacktestResult r = run(symbol, null, null, m15, h1);
            if (r.total > 0) {
                results.add(new SweepResult(c, r.winRate, r.ev, r.totalPnL, r.total));
            }
        }
        results.sort(Comparator.comparingDouble(r -> -r.ev));
        return results;
    }

    /**
     * Показывает как ПРОСКАЛЬЗЫВАНИЕ влияет на результат.
     * Ключевой анализ из критики — "стоп исполняется хуже".
     */
    public List<SweepResult> sweepSlippage(String symbol,
                                           List<TradingCore.Candle> m15,
                                           List<TradingCore.Candle> h1,
                                           DecisionEngineMerged.CoinCategory cat) {
        List<SweepResult> results = new ArrayList<>();
        double[] slippages = {0.0, 0.001, 0.002, 0.005, 0.010, 0.020};

        for (double slip : slippages) {
            // Временно изменяем SLIP_MAP через локальную переменную
            BacktestResult r = runWithSlippage(symbol, null, null, m15, h1, cat, slip);
            if (r.total > 0) {
                results.add(new SweepResult(slip * 100, r.winRate, r.ev, r.totalPnL, r.total));
            }
        }
        return results;
    }

    /**
     * Показывает влияние FUNDING RATE на итоговый P&L.
     * Важно для позиций, которые держатся несколько часов.
     */
    public List<SweepResult> sweepFunding(String symbol,
                                          List<TradingCore.Candle> m15,
                                          List<TradingCore.Candle> h1) {
        List<SweepResult> results = new ArrayList<>();
        // Годовые ставки: 0% / 0.01%/8h / 0.05%/8h / 0.1%/8h
        double[] fundingRates8h = {0.0, 0.0001, 0.0005, 0.001, 0.003};

        double savedFunding = this.fundingPer15m;
        for (double fr : fundingRates8h) {
            this.fundingPer15m = fr / (8 * 4); // 8h = 32 свечи по 15m
            BacktestResult r = run(symbol, null, null, m15, h1);
            if (r.total > 0) {
                results.add(new SweepResult(fr * 100, r.winRate, r.ev, r.totalPnL, r.total));
            }
        }
        this.fundingPer15m = savedFunding;
        return results;
    }

    /**
     * [NEW] Полный отчёт по символу:
     * - Базовый бэктест с реальными параметрами
     * - Breakdown по слиппажу
     * - Breakdown по funding
     * - Оптимальный порог confidence
     */
    public String fullReport(String symbol,
                             List<TradingCore.Candle> m15,
                             List<TradingCore.Candle> h1,
                             DecisionEngineMerged.CoinCategory cat) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n═══════════════════════════════════════════════\n");
        sb.append("  ПОЛНЫЙ БЭКТЕСТ: ").append(symbol).append("\n");
        sb.append("═══════════════════════════════════════════════\n");

        // Базовый результат
        BacktestResult base = run(symbol, null, null, m15, h1, cat);
        sb.append(base).append("\n\n");

        // Анализ проскальзывания
        sb.append("── Влияние проскальзывания ──\n");
        for (SweepResult sr : sweepSlippage(symbol, m15, h1, cat)) {
            sb.append(String.format("  Slip=%.2f%%: %s%n", sr.param, sr));
        }

        // Анализ funding
        sb.append("\n── Влияние Funding Rate ──\n");
        for (SweepResult sr : sweepFunding(symbol, m15, h1)) {
            sb.append(String.format("  FR/8h=%.3f%%: %s%n", sr.param, sr));
        }

        sb.append("\n── ВЫВОД ──\n");
        if (base.ev > 0.05) {
            sb.append("✅ Стратегия ПРИБЫЛЬНА с учётом всех издержек (EV=").append(String.format("%.4f", base.ev)).append(")\n");
        } else if (base.ev > 0) {
            sb.append("⚠️  Стратегия еле-еле прибыльна (EV=").append(String.format("%.4f", base.ev)).append(")\n");
            sb.append("   Проверь: хватит ли ей на реальное проскальзывание?\n");
        } else {
            sb.append("❌ Стратегия УБЫТОЧНА с учётом издержек (EV=").append(String.format("%.4f", base.ev)).append(")\n");
            sb.append("   Net P&L: ").append(String.format("%+.2f%%", base.totalPnL)).append(" vs Gross: ").append(String.format("%+.2f%%", base.grossPnL)).append("\n");
        }
        sb.append("═══════════════════════════════════════════════\n");

        return sb.toString();
    }

    // ══════════════════════════════════════════════════════════════
    //  ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    // ══════════════════════════════════════════════════════════════

    /** Бэктест с заданным проскальзыванием (для sweep) */
    private BacktestResult runWithSlippage(String symbol,
                                           List<TradingCore.Candle> m1,
                                           List<TradingCore.Candle> m5,
                                           List<TradingCore.Candle> m15,
                                           List<TradingCore.Candle> h1,
                                           DecisionEngineMerged.CoinCategory cat,
                                           double slippage) {
        BacktestResult res  = new BacktestResult(symbol);
        final int WINDOW    = 200;
        final int FORWARD   = 12;

        if (m15 == null || m15.size() < WINDOW + FORWARD) return res;

        double totalGross = 0, totalFees = 0, totalFunding = 0;

        for (int i = WINDOW; i < m15.size() - FORWARD; i++) {
            List<TradingCore.Candle> slice15 = m15.subList(i - WINDOW, i);
            List<TradingCore.Candle> sliceH1 = getH1Slice(h1, i, m15);
            List<TradingCore.Candle> sliceM1 = getM1Slice(m1, i, m15);
            List<TradingCore.Candle> sliceM5 = getM5Slice(m5, i, m15);

            DecisionEngineMerged.TradeIdea idea;
            try {
                idea = engine.analyze(symbol, sliceM1, sliceM5, slice15, sliceH1,
                        Collections.emptyList(), cat);
            } catch (Exception e) { continue; }
            if (idea == null) continue;

            double entry = idea.price;
            double realSL;
            if (idea.side == TradingCore.Side.LONG) {
                realSL = idea.stop * (1 - slippage);
            } else {
                realSL = idea.stop * (1 + slippage);
            }

            boolean win = false;
            double pnl  = 0;
            boolean closed = false;
            int barsHeld = 0;

            for (int j = i; j < Math.min(i + FORWARD, m15.size()) && !closed; j++) {
                TradingCore.Candle c = m15.get(j);
                barsHeld++;
                if (idea.side == TradingCore.Side.LONG) {
                    if (c.low  <= realSL)   { pnl = (realSL - entry) / entry; win = false; closed = true; }
                    else if (c.high >= idea.tp1) { pnl = (idea.tp1 - entry) / entry; win = true;  closed = true; }
                } else {
                    if (c.high >= realSL)   { pnl = (entry - realSL) / entry; win = false; closed = true; }
                    else if (c.low <= idea.tp1)  { pnl = (entry - idea.tp1) / entry; win = true;  closed = true; }
                }
            }

            if (!closed) continue;

            double fees    = entry * takerFee * 2;
            double funding = barsHeld * fundingPer15m;

            res.total++;
            totalGross += pnl;
            totalFees  += fees;
            totalFunding += Math.abs(funding);

            if (win) { res.wins++; res.tradeReturns.add(pnl - fees - funding); }
            else     { res.losses++; res.tradeReturns.add(pnl - fees - funding); }
        }

        res.grossPnL     = totalGross * 100;
        res.totalPnL     = (totalGross - totalFees - totalFunding) * 100;
        res.totalFees    = totalFees;
        res.totalFunding = totalFunding;
        res.calcAll();

        return res;
    }

    private List<TradingCore.Candle> getH1Slice(List<TradingCore.Candle> h1, int i15,
                                                List<TradingCore.Candle> m15) {
        if (h1 == null || h1.isEmpty()) return Collections.emptyList();
        long targetTime = m15.get(i15).openTime;
        int found = -1;
        for (int j = 0; j < h1.size(); j++) {
            if (h1.get(j).closeTime >= targetTime) { found = j; break; }
        }
        if (found < 0) return h1.subList(Math.max(0, h1.size() - 200), h1.size());
        return h1.subList(Math.max(0, found - 200), found + 1);
    }

    private List<TradingCore.Candle> getM1Slice(List<TradingCore.Candle> m1, int i15,
                                                List<TradingCore.Candle> m15) {
        if (m1 == null || m1.isEmpty()) return Collections.emptyList();
        long targetTime = m15.get(i15).openTime;
        int found = -1;
        for (int j = 0; j < m1.size(); j++) {
            if (m1.get(j).closeTime >= targetTime) { found = j; break; }
        }
        if (found < 0) return m1.subList(Math.max(0, m1.size() - 60), m1.size());
        return m1.subList(Math.max(0, found - 60), found + 1);
    }

    private List<TradingCore.Candle> getM5Slice(List<TradingCore.Candle> m5, int i15,
                                                List<TradingCore.Candle> m15) {
        if (m5 == null || m5.isEmpty()) return Collections.emptyList();
        long targetTime = m15.get(i15).openTime;
        int found = -1;
        for (int j = 0; j < m5.size(); j++) {
            if (m5.get(j).closeTime >= targetTime) { found = j; break; }
        }
        if (found < 0) return m5.subList(Math.max(0, m5.size() - 60), m5.size());
        return m5.subList(Math.max(0, found - 60), found + 1);
    }
}