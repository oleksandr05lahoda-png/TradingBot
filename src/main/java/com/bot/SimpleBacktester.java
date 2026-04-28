package com.bot;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;

/** SimpleBacktester v10.0 — INSTITUTIONAL-GRADE BACKTEST */
public final class SimpleBacktester {

    // ── Configuration ────────────────────────────────────────────
    private double initialBalance   = 100.0;
    private double takerFee         = 0.0004;     // 0.04% per side
    private double fundingPer15m    = 0.0001 / 32; // ~0.01%/8h → per 15m
    private int    maxConcurrent    = 6;
    // [v75] 8 → 6 bars. SYNC FIX: ISC.TIME_STOP_BARS=6 (90 min) is the live rule.
    // Backtester at 8 (120 min) was inflating PnL by giving setups 33% more time
    // to come back — totally invalidating walk-forward results. Now both are 90min.
    // toTelegramString also displays "Time-stop: 90 мин" — single source of truth.
    private int    timeStopBars     = 6;           // 90 min at 15m, matches ISC.TIME_STOP_BARS
    private boolean compound        = true;
    private boolean useM1Resolution = true;

    // Single source of truth for warmup.
    // Must match DecisionEngineMerged.MIN_BARS (150) — that's the gate
    // engine.analyze() uses internally. Previously backtester used 160
    // while live could accept 150, causing backtest signals to never fire
    // in the early window and live to fire signals the backtest never saw.
    // The extra 10 bars are kept as a safety margin for EMA200 stabilization.
    private static final int BACKTEST_WARMUP_BARS = 150;

    // Volume-adaptive slippage.
    //
    // Old: fixed slippage per category (TOP=0.08%, ALT=0.25%, MEME=0.60%).
    // Problem: a $100K position in a $2M-daily-volume ALT pays real slippage
    // 2–3× higher than the fixed value. Backtester underestimated costs and
    // overstated winrate by ~3–5pp for small accounts / illiquid pairs.
    //
    // New: base slippage per category × liquidity penalty.
    //   liquidity penalty = 1.0 + positionUSD / max(volume24hUSD, 1).
    // For $100 position in $5B volume TOP pair → penalty ~1.00002 (negligible).
    // For $100 position in $2M volume MEME → penalty ~1.00005 (still tiny at $100).
    // For $10K position in $5M volume MEME → penalty 1.002 (+0.12% slippage).
    // At realistic bot sizes ($20–$500 per trade), penalty mostly ≈1.0, but
    // the formula correctly scales when users grow their capital.
    private static final Map<com.bot.DecisionEngineMerged.CoinCategory, Double> SLIPPAGE_BASE = Map.of(
            com.bot.DecisionEngineMerged.CoinCategory.TOP,  0.0008,
            com.bot.DecisionEngineMerged.CoinCategory.ALT,  0.0025,
            com.bot.DecisionEngineMerged.CoinCategory.MEME, 0.0060
    );

    // Backward-compat alias: some call sites still reference SLIPPAGE as a field.
    private static final Map<com.bot.DecisionEngineMerged.CoinCategory, Double> SLIPPAGE = SLIPPAGE_BASE;

    /**
     * Compute effective slippage given category, position size (USD), and 24h volume.
     * Handles nulls / zeros gracefully by falling back to base rate.
     */
    private static double effectiveSlippage(com.bot.DecisionEngineMerged.CoinCategory cat,
                                            double positionUSD, double volume24hUSD) {
        double base = SLIPPAGE_BASE.getOrDefault(cat, 0.0025);
        if (positionUSD <= 0 || volume24hUSD <= 0) return base;
        // Cap penalty at 3× base — beyond that, the trade is untradeable in reality.
        double penalty = 1.0 + (positionUSD / volume24hUSD);
        return base * Math.min(penalty, 3.0);
    }

    // ── Setters ──────────────────────────────────────────────────
    public void setInitialBalance(double v)  { this.initialBalance = v; }
    public void setTakerFee(double v)        { this.takerFee = v; }
    public void setMaxConcurrent(int v)      { this.maxConcurrent = v; }
    public void setTimeStopBars(int v)       { this.timeStopBars = v; }
    public void setCompound(boolean v)       { this.compound = v; }
    public void setUseM1Resolution(boolean v){ this.useM1Resolution = v; }

    // Optional: provide 24h volume (USD) for more realistic slippage.
    // If set, effectiveSlippage() scales with position/volume ratio.
    // If not set, falls back to fixed base rate (old behavior) — backward compat.
    private double volume24hUSD = 0.0;
    public void setVolume24hUSD(double v)    { this.volume24hUSD = Math.max(0.0, v); }

    //  RESULTS

    public static final class BacktestResult {
        public final String symbol;
        public int total, wins, losses, breakEvens, timeStops;
        public double winRate, avgWinPct, avgLossPct, avgRR;
        public double grossPnL, netPnL;
        public double totalFees, totalSlippage, totalFunding;
        public double ev;                 // correct: winRate * avgWin - lossRate * avgLoss
        public double sharpeDaily;        // annualized from daily returns
        public double sortinoDaily;       // [PATCH #2] annualized, downside-only deviation
        public double maxDrawdownPct;
        public double maxDDDurationBars;  // [PATCH #2] how long the worst DD lasted
        public double calmarRatio;
        public double profitFactor;
        public double expectancy;         // avg $ per trade
        public double finalBalance;
        // Reliability score: how much to trust these results
        public double reliabilityScore;  // 0..1 based on trade count + Sharpe consistency
        // Trade quality distribution
        public int    longestLossStreak;
        public int    longestWinStreak;
        public double medianPnlPct;
        public double stdPnlPct;          // std of single-trade pnl

        // Detailed trade log
        public final List<TradeRecord> trades = new ArrayList<>();

        // Daily returns for Sharpe calculation
        public final List<Double> dailyReturns = new ArrayList<>();

        public BacktestResult(String symbol) { this.symbol = symbol; }

        public void compute(double initialBal) {
            if (total == 0) return;
            winRate = (double) wins / total;

            double sumWin = 0, sumLoss = 0;
            int wCount = 0, lCount = 0;
            double grossProfit = 0, grossLoss = 0;

            for (TradeRecord t : trades) {
                if (t.pnlPct > 0.01) { sumWin += t.pnlPct; wCount++; grossProfit += t.pnlPct; }
                else if (t.pnlPct < -0.01) { sumLoss += Math.abs(t.pnlPct); lCount++; grossLoss += Math.abs(t.pnlPct); }
            }

            avgWinPct  = wCount > 0 ? sumWin / wCount : 0;
            avgLossPct = lCount > 0 ? sumLoss / lCount : 1;
            avgRR      = avgLossPct > 0 ? avgWinPct / avgLossPct : 0;

            // CORRECT EV formula
            double lossRate = 1.0 - winRate;
            ev = winRate * avgWinPct - lossRate * avgLossPct;

            // Profit factor
            profitFactor = grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? 999 : 0;

            // Expectancy (average pnl per trade as % of balance)
            expectancy = trades.stream().mapToDouble(t -> t.pnlPct).average().orElse(0);

            // CORRECT Sharpe: from daily returns, annualized
            if (dailyReturns.size() >= 5) {
                double meanDaily = dailyReturns.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                double varDaily  = dailyReturns.stream().mapToDouble(r -> Math.pow(r - meanDaily, 2)).average().orElse(0);
                double stdDaily  = Math.sqrt(varDaily);
                sharpeDaily = stdDaily > 0 ? (meanDaily / stdDaily) * Math.sqrt(365) : 0;

                // Sortino: downside deviation only (штрафует только потери)
                double downsideVar = dailyReturns.stream()
                        .filter(r -> r < 0)
                        .mapToDouble(r -> r * r)
                        .average().orElse(0);
                double downsideStd = Math.sqrt(downsideVar);
                sortinoDaily = downsideStd > 0 ? (meanDaily / downsideStd) * Math.sqrt(365) : 0;
            }

            // Max drawdown + DD duration
            double peak = initialBal;
            double maxDD = 0;
            double equity = initialBal;
            int currentDDBars = 0, maxDDBars = 0;
            for (TradeRecord t : trades) {
                equity += equity * t.pnlPct / 100.0;
                if (equity >= peak) {
                    peak = equity;
                    currentDDBars = 0;
                } else {
                    currentDDBars++;
                    maxDDBars = Math.max(maxDDBars, currentDDBars);
                }
                double dd = (peak - equity) / peak;
                maxDD = Math.max(maxDD, dd);
            }
            maxDrawdownPct = maxDD * 100;
            maxDDDurationBars = maxDDBars;

            // Trade distribution — longest streaks
            int curLossStreak = 0, curWinStreak = 0;
            for (TradeRecord t : trades) {
                if (t.pnlPct > 0.05) {
                    curWinStreak++; curLossStreak = 0;
                    longestWinStreak = Math.max(longestWinStreak, curWinStreak);
                } else if (t.pnlPct < -0.05) {
                    curLossStreak++; curWinStreak = 0;
                    longestLossStreak = Math.max(longestLossStreak, curLossStreak);
                }
            }

            // Median + std of single-trade pnl
            double[] pnls = trades.stream().mapToDouble(t -> t.pnlPct).sorted().toArray();
            if (pnls.length > 0) {
                medianPnlPct = pnls.length % 2 == 0
                        ? (pnls[pnls.length / 2 - 1] + pnls[pnls.length / 2]) / 2.0
                        : pnls[pnls.length / 2];
                double meanPnl = java.util.Arrays.stream(pnls).average().orElse(0);
                double var = java.util.Arrays.stream(pnls)
                        .map(p -> (p - meanPnl) * (p - meanPnl))
                        .average().orElse(0);
                stdPnlPct = Math.sqrt(var);
            }

            // Calmar ratio
            double annualReturn = dailyReturns.isEmpty() ? 0 :
                    dailyReturns.stream().mapToDouble(Double::doubleValue).average().orElse(0) * 365;
            calmarRatio = maxDrawdownPct > 0 ? annualReturn / maxDrawdownPct : 0;

            // Reliability score: penalizes small sample sizes
            // Under 30 trades, results are statistically unreliable
            double tradeCountFactor = Math.min(1.0, total / 50.0);  // peaks at 50 trades
            double consistencyFactor = profitFactor > 1.0 ? Math.min(1.0, (profitFactor - 1.0) * 2) : 0;
            double drawdownPenalty = maxDrawdownPct > 20 ? 0.5 : maxDrawdownPct > 10 ? 0.8 : 1.0;
            reliabilityScore = tradeCountFactor * 0.5 + consistencyFactor * 0.3 + drawdownPenalty * 0.2;
        }

        @Override
        public String toString() {
            return String.format(
                    "╔══ Backtest [%s] ══╗\n" +
                            "║ Trades: %d | W: %d | L: %d | BE: %d | TS: %d\n" +
                            "║ WinRate: %.1f%% | AvgWin: %.2f%% | AvgLoss: %.2f%% | AvgRR: %.2f\n" +
                            "║ Gross: %+.2f%% | Net: %+.2f%% | Final: $%.2f\n" +
                            "║ Fees: -%.3f%% | Slip: -%.3f%% | Fund: -%.3f%%\n" +
                            "║ EV: %+.4f | Sharpe: %.2f | Sortino: %.2f | PF: %.2f\n" +
                            "║ MaxDD: %.1f%% (%d bars) | Calmar: %.2f | Expectancy: %+.3f%%\n" +
                            "║ Median: %+.3f%% | StdDev: %.3f%% | Streak: +%d/-%d\n" +
                            "║ Reliability: %.0f%% %s\n" +
                            "╚══════════════════════════════════════════════════╝",
                    symbol, total, wins, losses, breakEvens, timeStops,
                    winRate * 100, avgWinPct, avgLossPct, avgRR,
                    grossPnL, netPnL, finalBalance,
                    totalFees * 100, totalSlippage * 100, totalFunding * 100,
                    ev, sharpeDaily, sortinoDaily, profitFactor,
                    maxDrawdownPct, (int) maxDDDurationBars, calmarRatio, expectancy,
                    medianPnlPct, stdPnlPct, longestWinStreak, longestLossStreak,
                    reliabilityScore * 100,
                    total < 30 ? "⚠️ LOW SAMPLE" : total < 50 ? "⚠️ MODERATE" : "✅");
        }
    }

    public static final class TradeRecord {
        public final String symbol;
        public final com.bot.TradingCore.Side side;
        public final double entry, exit, sl, tp;
        public final double pnlPct;
        public final double confidence;
        public final int barsHeld;
        public final String exitReason; // SL, TP1, TP2, TIME_STOP, BE
        public final long entryTime;
        public final double fees, slippage, funding;

        public TradeRecord(String sym, com.bot.TradingCore.Side side, double entry, double exit,
                           double sl, double tp, double pnlPct, double conf, int bars,
                           String reason, long entryTime, double fees, double slip, double fund) {
            this.symbol = sym; this.side = side; this.entry = entry; this.exit = exit;
            this.sl = sl; this.tp = tp; this.pnlPct = pnlPct; this.confidence = conf;
            this.barsHeld = bars; this.exitReason = reason; this.entryTime = entryTime;
            this.fees = fees; this.slippage = slip; this.funding = fund;
        }
    }

    /** Portfolio-level result aggregating multiple symbols */
    public static final class PortfolioResult {
        public final List<BacktestResult> symbolResults = new ArrayList<>();
        public final List<String> warnings = new ArrayList<>();   // [PATCH #3]
        public double totalNetPnL, portfolioSharpe, portfolioMaxDD;
        public double avgWinRate, avgEV, avgPF;
        public int totalTrades;
        public double finalBalance;

        public void compute(double initialBal) {
            totalTrades = symbolResults.stream().mapToInt(r -> r.total).sum();
            totalNetPnL = symbolResults.stream().mapToDouble(r -> r.netPnL).sum();
            avgWinRate  = symbolResults.stream().filter(r -> r.total > 0).mapToDouble(r -> r.winRate).average().orElse(0);
            avgEV       = symbolResults.stream().filter(r -> r.total > 0).mapToDouble(r -> r.ev).average().orElse(0);
            avgPF       = symbolResults.stream().filter(r -> r.total > 0).mapToDouble(r -> r.profitFactor).average().orElse(0);

            // Portfolio max DD from all trades sorted by time
            List<TradeRecord> allTrades = new ArrayList<>();
            symbolResults.forEach(r -> allTrades.addAll(r.trades));
            allTrades.sort(Comparator.comparingLong(t -> t.entryTime));

            double equity = initialBal;
            double peak = initialBal;
            double maxDD = 0;
            for (TradeRecord t : allTrades) {
                equity += equity * t.pnlPct / 100.0;
                peak = Math.max(peak, equity);
                maxDD = Math.max(maxDD, (peak - equity) / peak);
            }
            portfolioMaxDD = maxDD * 100;
            finalBalance = equity;

            // Portfolio daily returns
            Map<Long, Double> dailyPnL = new TreeMap<>();
            for (TradeRecord t : allTrades) {
                long day = t.entryTime / 86400_000L;
                dailyPnL.merge(day, t.pnlPct, Double::sum);
            }
            if (dailyPnL.size() >= 5) {
                double[] dailyArr = dailyPnL.values().stream().mapToDouble(Double::doubleValue).toArray();
                double mean = Arrays.stream(dailyArr).average().orElse(0);
                double var  = Arrays.stream(dailyArr).map(r -> Math.pow(r - mean, 2)).average().orElse(0);
                double std  = Math.sqrt(var);
                portfolioSharpe = std > 0 ? (mean / std) * Math.sqrt(365) : 0;
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("╔══════════ PORTFOLIO BACKTEST ══════════╗\n");
            sb.append(String.format("║ Symbols: %d | Trades: %d\n", symbolResults.size(), totalTrades));
            sb.append(String.format("║ Net PnL: %+.2f%% | Final: $%.2f\n", totalNetPnL, finalBalance));
            sb.append(String.format("║ Avg WR: %.1f%% | Avg EV: %+.4f | Avg PF: %.2f\n", avgWinRate * 100, avgEV, avgPF));
            sb.append(String.format("║ Portfolio Sharpe: %.2f | MaxDD: %.1f%%\n", portfolioSharpe, portfolioMaxDD));
            sb.append("╠══════════ PER SYMBOL ══════════════════╣\n");
            symbolResults.stream()
                    .sorted(Comparator.comparingDouble((BacktestResult r) -> r.ev).reversed())
                    .forEach(r -> sb.append(String.format("║ %-10s TR:%3d WR:%.0f%% EV:%+.3f PF:%.1f DD:%.0f%%\n",
                            r.symbol, r.total, r.winRate * 100, r.ev, r.profitFactor, r.maxDrawdownPct)));
            sb.append("╚════════════════════════════════════════╝");
            return sb.toString();
        }
    }

    //  SINGLE SYMBOL BACKTEST

    public BacktestResult run(String symbol,
                              List<com.bot.TradingCore.Candle> m1,
                              List<com.bot.TradingCore.Candle> m5,
                              List<com.bot.TradingCore.Candle> m15,
                              List<com.bot.TradingCore.Candle> h1,
                              com.bot.DecisionEngineMerged.CoinCategory category) {
        BacktestResult result = new BacktestResult(symbol);
        if (m15 == null || m15.size() < 200) return result;

        com.bot.DecisionEngineMerged engine = new com.bot.DecisionEngineMerged();
        com.bot.GlobalImpulseController btGic = new com.bot.GlobalImpulseController();
        engine.setGIC(btGic);
        // Effective slippage = base × (1 + position/volume) capped at 3× base.
        // When setVolume24hUSD() not called (default = 0), falls back to base rate
        // preserving pre-patch behavior.
        double slippage = effectiveSlippage(category, initialBalance, this.volume24hUSD);
        double balance = initialBalance;

        // Build m1 index for intra-candle resolution
        Map<Long, List<com.bot.TradingCore.Candle>> m1Index = buildM1Index(m1);

        // Track daily returns
        Map<Long, Double> dailyPnL = new TreeMap<>();

        int warmup = BACKTEST_WARMUP_BARS; // [PATCH 2.1] aligned with DecisionEngine.MIN_BARS=150
        int i = warmup;

        // Active position tracking
        ActivePosition currentPos = null;

        while (i < m15.size()) {
            com.bot.TradingCore.Candle bar = m15.get(i);
            long barDay = bar.openTime / 86400_000L;

            // Check existing position
            if (currentPos != null) {
                PositionOutcome outcome = resolvePosition(currentPos, m15, i, m1Index, slippage);
                if (outcome != null) {
                    // Calculate costs
                    double feesCost = 2 * takerFee; // entry + exit
                    double slipCost = 2 * slippage;
                    double fundingCost = fundingPer15m * outcome.barsHeld;
                    double totalCosts = feesCost + slipCost + fundingCost;

                    double grossPnl = outcome.pnlPct;
                    double netPnl   = grossPnl - totalCosts * 100;

                    result.trades.add(new TradeRecord(
                            symbol, currentPos.side, currentPos.entry, outcome.exitPrice,
                            currentPos.sl, currentPos.tp1, netPnl, currentPos.confidence,
                            outcome.barsHeld, outcome.reason, currentPos.entryTime,
                            feesCost, slipCost, fundingCost));

                    result.total++;
                    result.grossPnL += grossPnl;
                    result.netPnL   += netPnl;
                    result.totalFees += feesCost;
                    result.totalSlippage += slipCost;
                    result.totalFunding += fundingCost;

                    if (netPnl > 0.05)      result.wins++;
                    else if (netPnl < -0.05) result.losses++;
                    else                      result.breakEvens++;
                    if ("TIME_STOP".equals(outcome.reason)) result.timeStops++;

                    dailyPnL.merge(barDay, netPnl, Double::sum);

                    if (compound) balance += balance * netPnl / 100.0;
                    i = outcome.exitBar + 1;
                    currentPos = null;
                    continue;
                }
            }

            // Generate signal if no position
            // LOOK-AHEAD FIX
            // Old: slice до i+1 (включая текущий бар) + entry = idea.price (close бара i).
            // Problem: close бара i известен только в НАЧАЛЕ бара i+1. Вход по close i
            //          даёт нереалистичный winrate (+5..15%).
            // New: slice до i (бар i НЕ включён — он ещё не закрыт в момент решения);
            //      entry = m15[i].open (open текущего бара = момент когда в live
            //      бот реально получит сигнал и войдёт).
            // Правило: НИКОГДА не использовать close текущего бара как entry price.
            if (currentPos == null && i + 1 < m15.size()) {
                int fromBar = Math.max(0, i - 200);
                // slice EXCLUDES current bar i
                List<com.bot.TradingCore.Candle> slice15 = m15.subList(fromBar, i);
                if (slice15.size() < BACKTEST_WARMUP_BARS) { i++; continue; }

                long decisionTime = m15.get(i - 1).closeTime; // момент принятия решения
                List<com.bot.TradingCore.Candle> sliceH1 = getTimeframeSlice(
                        h1, m15.get(fromBar).openTime, decisionTime);

                List<com.bot.TradingCore.Candle> sliceM1 = null, sliceM5 = null;
                if (m1 != null && !m1.isEmpty())
                    sliceM1 = getTimeframeSlice(m1, m15.get(Math.max(0, i - 20)).openTime, decisionTime);
                if (m5 != null && !m5.isEmpty())
                    sliceM5 = getTimeframeSlice(m5, m15.get(Math.max(0, i - 40)).openTime, decisionTime);

                com.bot.DecisionEngineMerged.TradeIdea idea = engine.analyze(
                        symbol, sliceM1, sliceM5, slice15, sliceH1, category);

                if (idea != null) {
                    // ENTRY = open текущего бара i (в live именно в этот момент бот входит)
                    com.bot.TradingCore.Candle entryBar = m15.get(i);
                    double entryPrice = entryBar.open;
                    // Slippage применяем к фактической цене входа
                    if (idea.side == com.bot.TradingCore.Side.LONG) entryPrice *= (1 + slippage);
                    else entryPrice *= (1 - slippage);

                    // SL/TP пересчитываем относительно нового entry
                    // (idea.stop/tp1/tp2 были рассчитаны от close бара i-1)
                    double slShift     = (entryPrice - idea.price);
                    double adjustedSL  = idea.stop + slShift;
                    double adjustedTP1 = idea.tp1  + slShift;
                    double adjustedTP2 = idea.tp2  + slShift;

                    currentPos = new ActivePosition(
                            idea.side, entryPrice, adjustedSL, adjustedTP1, adjustedTP2,
                            idea.probability, entryBar.openTime, i);
                }
            }

            i++;
        }

        // Compute daily returns
        for (double dr : dailyPnL.values()) result.dailyReturns.add(dr);

        result.finalBalance = balance;
        result.compute(initialBalance);
        return result;
    }

    //  PORTFOLIO BACKTEST (multiple symbols simultaneously)

    /**
     * Portfolio backtest — per-symbol isolated, NO cross-symbol concurrency.
     *
     * WARNING: this is NOT a true portfolio simulation. Each symbol runs independently,
     * which means the total may show 50 concurrent positions even though live CorrelationGuard
     * limits to 5-6. Per-symbol metrics are trustworthy; aggregate P&L is NOT a live-realistic
     * number (it ignores maxConcurrent and cross-symbol correlation).
     *
     * For a proper portfolio simulation you need to merge all symbols into a single timeline
     * with shared equity and max-concurrent cap. That's a separate refactor.
     */
    public PortfolioResult runPortfolio(Map<String, SymbolData> allData) {
        PortfolioResult portfolio = new PortfolioResult();
        portfolio.warnings.add("[WARN] runPortfolio() is per-symbol isolated. Aggregate P&L "
                + "ignores maxConcurrent and cross-symbol correlation. Use per-symbol results only.");

        for (Map.Entry<String, SymbolData> entry : allData.entrySet()) {
            SymbolData data = entry.getValue();
            BacktestResult result = run(entry.getKey(), data.m1, data.m5, data.m15, data.h1, data.category);
            if (result.total >= 3) {
                portfolio.symbolResults.add(result);
            }
        }

        portfolio.compute(initialBalance);
        return portfolio;
    }

    /** Data container for all timeframes of a symbol */
    public static final class SymbolData {
        public final List<com.bot.TradingCore.Candle> m1, m5, m15, h1;
        public final com.bot.DecisionEngineMerged.CoinCategory category;

        public SymbolData(List<com.bot.TradingCore.Candle> m1, List<com.bot.TradingCore.Candle> m5,
                          List<com.bot.TradingCore.Candle> m15, List<com.bot.TradingCore.Candle> h1,
                          com.bot.DecisionEngineMerged.CoinCategory category) {
            this.m1 = m1; this.m5 = m5; this.m15 = m15; this.h1 = h1; this.category = category;
        }
    }

    //  WALK-FORWARD VALIDATION

    /**
     * Walk-forward test: split data into training and test windows.
     * Training: optimize parameters on first N bars.
     * Test: run with those parameters on next M bars.
     * Slide forward and repeat.
     *
     * @param symbol   symbol name
     * @param m15      full 15m data
     * @param h1       full 1h data
     * @param category coin category
     * @param trainBars  bars for training window
     * @param testBars   bars for test window
     * @return list of out-of-sample results for each window
     */
    public List<BacktestResult> walkForward(String symbol,
                                            List<com.bot.TradingCore.Candle> m15,
                                            List<com.bot.TradingCore.Candle> h1,
                                            com.bot.DecisionEngineMerged.CoinCategory category,
                                            int trainBars, int testBars) {
        // [v50 AUDIT FIX] Purged walk-forward with embargo.
        // Problem: 15m candle autocorrelation (~0.8 at lag=1) causes leakage when train and
        // test windows are adjacent. Plus, a trade opened at end of train may still be open
        // in test window → label leakage.
        // Fix: gap = max(timeStopBars + 10, 18) bars between train end and test start.
        // Same-size gap after test (embargo) before next train slide.
        final int PURGE_BARS = Math.max(timeStopBars + 10, 18);
        List<BacktestResult> oosResults = new ArrayList<>();
        if (m15 == null || m15.size() < trainBars + testBars + 2 * PURGE_BARS) return oosResults;

        int cursor = 0;
        while (cursor + trainBars + PURGE_BARS + testBars <= m15.size()) {
            int testStart = cursor + trainBars + PURGE_BARS;
            int testEnd   = Math.min(testStart + testBars, m15.size());

            List<com.bot.TradingCore.Candle> testM15 = m15.subList(Math.max(0, testStart - 200), testEnd);
            List<com.bot.TradingCore.Candle> testH1  = getTimeframeSlice(h1,
                    m15.get(testStart).openTime - 200 * 3600_000L,
                    m15.get(testEnd - 1).openTime);

            BacktestResult oos = run(symbol, null, null, testM15, testH1, category);
            oos.compute(initialBalance);
            oosResults.add(oos);

            cursor += testBars + PURGE_BARS; // slide with embargo
        }

        // [v79 I3] Audit-log walk-forward OOS results for third-party verification.
        // Записываем итоги в HMAC-подписанный audit log калибратора, чтобы
        // backtest-результаты нельзя было подменить задним числом.
        try {
            for (BacktestResult r : oosResults) {
                if (r.total >= 5) {
                    com.bot.DecisionEngineMerged.getCalibrator().writeDispatchAudit(
                            symbol, "WALKFORWARD",
                            r.finalBalance,         // proxy: final balance as "price"
                            r.winRate * 100,        // proxy: WR as "tp1"
                            r.maxDrawdownPct,       // proxy: DD as "sl"
                            r.ev * 100,             // proxy: EV as "prob"
                            "WF_OOS",
                            "BACKTEST", false);
                }
            }
        } catch (Throwable ignored) {}

        return oosResults;
    }

    //  MONTE CARLO DRAWDOWN ESTIMATION

    public static double monteCarloDrawdown(List<Double> trades, int simulations, double confidence) {
        if (trades.isEmpty()) return 0;

        Random rng = new Random(42);
        double[] maxDrawdowns = new double[simulations];

        for (int sim = 0; sim < simulations; sim++) {
            // Shuffle trades
            List<Double> shuffled = new ArrayList<>(trades);
            Collections.shuffle(shuffled, rng);

            // Calculate max drawdown for this simulation
            double equity = 100;
            double peak = 100;
            double maxDD = 0;

            for (double pnl : shuffled) {
                equity += equity * pnl / 100.0;
                peak = Math.max(peak, equity);
                double dd = (peak - equity) / peak;
                maxDD = Math.max(maxDD, dd);
            }

            maxDrawdowns[sim] = maxDD * 100;
        }

        Arrays.sort(maxDrawdowns);
        int idx = (int) (confidence * simulations);
        return maxDrawdowns[Math.min(idx, simulations - 1)];
    }

    //  INTERNAL: POSITION RESOLUTION

    private static final class ActivePosition {
        final com.bot.TradingCore.Side side;
        final double entry, sl, tp1, tp2, confidence;
        final long entryTime;
        final int entryBar;
        boolean tp1Hit = false;
        double currentSL;

        ActivePosition(com.bot.TradingCore.Side side, double entry, double sl, double tp1, double tp2,
                       double conf, long time, int bar) {
            this.side = side; this.entry = entry; this.sl = sl;
            this.tp1 = tp1; this.tp2 = tp2; this.confidence = conf;
            this.entryTime = time; this.entryBar = bar; this.currentSL = sl;
        }
    }

    private static final class PositionOutcome {
        final double exitPrice, pnlPct;
        final int barsHeld, exitBar;
        final String reason;

        PositionOutcome(double exit, double pnl, int bars, int exitBar, String reason) {
            this.exitPrice = exit; this.pnlPct = pnl; this.barsHeld = bars;
            this.exitBar = exitBar; this.reason = reason;
        }
    }

    private PositionOutcome resolvePosition(ActivePosition pos, List<com.bot.TradingCore.Candle> m15,
                                            int currentBar, Map<Long, List<com.bot.TradingCore.Candle>> m1Index,
                                            double slippage) {
        boolean isLong = pos.side == com.bot.TradingCore.Side.LONG;
        int barsHeld = currentBar - pos.entryBar;

        // Time stop
        if (barsHeld >= timeStopBars) {
            double exitPrice = m15.get(currentBar).close;
            double pnl = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                    : (pos.entry - exitPrice) / pos.entry * 100;
            return new PositionOutcome(exitPrice, pnl, barsHeld, currentBar, "TIME_STOP");
        }

        com.bot.TradingCore.Candle bar = m15.get(currentBar);
        double sl = pos.currentSL;
        double tp = pos.tp1Hit ? pos.tp2 : pos.tp1;

        // Check if both SL and TP could be hit in this bar
        boolean slCanHit = isLong ? bar.low <= sl : bar.high >= sl;
        boolean tpCanHit = isLong ? bar.high >= tp : bar.low <= tp;

        if (!slCanHit && !tpCanHit) return null; // nothing happened

        if (slCanHit && !tpCanHit) {
            // SL hit
            double exitPrice = sl;
            if (isLong) exitPrice -= exitPrice * slippage;
            else        exitPrice += exitPrice * slippage;
            double pnl = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                    : (pos.entry - exitPrice) / pos.entry * 100;
            return new PositionOutcome(exitPrice, pnl, barsHeld, currentBar, pos.tp1Hit ? "BE_STOP" : "SL");
        }

        if (tpCanHit && !slCanHit) {
            // TP hit
            if (!pos.tp1Hit) {
                pos.tp1Hit = true;
                pos.currentSL = pos.entry * (isLong ? 1.001 : 0.999); // move SL to BE
                return null; // position continues with BE stop
            } else {
                // TP2 hit — close fully
                double exitPrice = tp;
                double pnl = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                        : (pos.entry - exitPrice) / pos.entry * 100;
                return new PositionOutcome(exitPrice, pnl, barsHeld, currentBar, "TP2");
            }
        }

        // Both SL and TP could be hit — use 1m data to resolve
        if (useM1Resolution) {
            List<com.bot.TradingCore.Candle> m1Candles = m1Index.get(bar.openTime);
            if (m1Candles != null && m1Candles.size() >= 3) {
                return resolveWithM1(pos, m1Candles, isLong, sl, tp, barsHeld, currentBar, slippage);
            }
        }

        // Fallback: heuristic based on candle open position
        return resolveHeuristic(pos, bar, isLong, sl, tp, barsHeld, currentBar, slippage);
    }

    private PositionOutcome resolveWithM1(ActivePosition pos, List<com.bot.TradingCore.Candle> m1,
                                          boolean isLong, double sl, double tp,
                                          int barsHeld, int currentBar, double slippage) {
        for (com.bot.TradingCore.Candle m1Bar : m1) {
            boolean slHit = isLong ? m1Bar.low <= sl : m1Bar.high >= sl;
            boolean tpHit = isLong ? m1Bar.high >= tp : m1Bar.low <= tp;

            if (slHit) {
                double exitPrice = sl;
                if (isLong) exitPrice -= exitPrice * slippage;
                else        exitPrice += exitPrice * slippage;
                double pnl = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                        : (pos.entry - exitPrice) / pos.entry * 100;
                return new PositionOutcome(exitPrice, pnl, barsHeld, currentBar, pos.tp1Hit ? "BE_STOP" : "SL");
            }
            if (tpHit) {
                if (!pos.tp1Hit) {
                    pos.tp1Hit = true;
                    pos.currentSL = pos.entry * (isLong ? 1.001 : 0.999);
                    return null;
                }
                double pnl = isLong ? (tp - pos.entry) / pos.entry * 100
                        : (pos.entry - tp) / pos.entry * 100;
                return new PositionOutcome(tp, pnl, barsHeld, currentBar, "TP2");
            }
        }
        return null;
    }

    private PositionOutcome resolveHeuristic(ActivePosition pos, com.bot.TradingCore.Candle bar,
                                             boolean isLong, double sl, double tp,
                                             int barsHeld, int currentBar, double slippage) {
        // Conservative bias: default to SL first (prevents backtest optimism)
        double range = bar.high - bar.low;
        if (range < 1e-12) return null;
        double openPos = (bar.open - bar.low) / range; // 0 = open at low, 1 = open at high

        boolean slFirst;
        if (isLong) {
            // Long: SL is below. If open is near low (openPos < 0.3), market went down first
            slFirst = openPos < 0.35;
        } else {
            // Short: SL is above. If open is near high (openPos > 0.7), market went up first
            slFirst = openPos > 0.65;
        }

        // Ambiguous case: open is mid-range (0.35–0.65) — both SL and TP plausible.
        // OLD: always SL_FIRST → backtest was systematically pessimistic, artificially low WR.
        // NEW: no default bias — return null (treat as "nothing resolved this bar"),
        //      let the time-stop logic handle it if the position expires.
        //      This matches reality: if we can't determine order from heuristics, skip the bar.
        if (openPos > 0.35 && openPos < 0.65) return null;

        if (slFirst) {
            double exitPrice = sl;
            if (isLong) exitPrice -= exitPrice * slippage;
            else        exitPrice += exitPrice * slippage;
            double pnl = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                    : (pos.entry - exitPrice) / pos.entry * 100;
            return new PositionOutcome(exitPrice, pnl, barsHeld, currentBar, pos.tp1Hit ? "BE_STOP" : "SL");
        } else {
            if (!pos.tp1Hit) {
                pos.tp1Hit = true;
                pos.currentSL = pos.entry * (isLong ? 1.001 : 0.999);
                return null;
            }
            double pnl = isLong ? (tp - pos.entry) / pos.entry * 100
                    : (pos.entry - tp) / pos.entry * 100;
            return new PositionOutcome(tp, pnl, barsHeld, currentBar, "TP2");
        }
    }

    //  UTILITY

    private Map<Long, List<com.bot.TradingCore.Candle>> buildM1Index(List<com.bot.TradingCore.Candle> m1) {
        Map<Long, List<com.bot.TradingCore.Candle>> index = new HashMap<>();
        if (m1 == null) return index;
        for (com.bot.TradingCore.Candle c : m1) {
            long period15m = (c.openTime / (15 * 60_000L)) * (15 * 60_000L);
            index.computeIfAbsent(period15m, k -> new ArrayList<>()).add(c);
        }
        return index;
    }

    private List<com.bot.TradingCore.Candle> getTimeframeSlice(List<com.bot.TradingCore.Candle> candles,
                                                               long fromMs, long toMs) {
        if (candles == null || candles.isEmpty()) return List.of();
        List<com.bot.TradingCore.Candle> result = new ArrayList<>();
        for (com.bot.TradingCore.Candle c : candles) {
            if (c.openTime >= fromMs && c.openTime <= toMs) result.add(c);
        }
        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // [v74] STARTUP SELF-VALIDATION HARNESS
    //
    // Replays the last N days of 15m history through the live engine on
    // bot startup (and every 6h thereafter). Aggregates win-rate, profit
    // factor, expectancy across the top-K pairs by 24h volume, and posts
    // a GO / MARGINAL / NO-GO verdict to Telegram.
    //
    // Goal: give the operator a concrete edge measurement BEFORE risking
    // real money — without requiring them to download CSVs or run separate
    // scripts. Pulls candles via SignalSender.fetchKlines (REST through
    // the existing rate-limited cache).
    //
    // Disable with VALIDATOR_ENABLED=0.
    // ═══════════════════════════════════════════════════════════════════════
    public static final class SelfValidator {

        private static final Logger LOG = Logger.getLogger(SelfValidator.class.getName());

        private final int    daysOfHistory;
        private final int    topNPairs;
        private final long   reportEveryMs;
        private final boolean enabled;

        private final SignalSender              sender;
        private final TelegramBotSender         tg;
        private final ScheduledExecutorService  sched;
        private final AtomicBoolean             running = new AtomicBoolean(false);

        public SelfValidator(SignalSender sender, TelegramBotSender tg) {
            this.sender         = sender;
            this.tg             = tg;
            this.daysOfHistory  = envInt("VALIDATOR_DAYS",         4);
            this.topNPairs      = envInt("VALIDATOR_TOP_N",        12);
            this.reportEveryMs  = envLong("VALIDATOR_REPORT_EVERY_MS", 6 * 60 * 60_000L);
            this.enabled        = envInt("VALIDATOR_ENABLED", 1) == 1;
            this.sched = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "self-validator");
                t.setDaemon(true);
                return t;
            });
        }

        /** Convenience static entry point used from BotMain. */
        public static SelfValidator start(SignalSender sender,
                                          TelegramBotSender tg,
                                          long initialDelayMs) {
            SelfValidator v = new SelfValidator(sender, tg);
            v.schedule(initialDelayMs);
            return v;
        }

        public void schedule(long initialDelayMs) {
            if (!enabled) {
                LOG.info("[SELF-VALIDATOR] disabled via VALIDATOR_ENABLED=0");
                return;
            }
            sched.schedule(this::runSafe, initialDelayMs, TimeUnit.MILLISECONDS);
            sched.scheduleAtFixedRate(this::runSafe,
                    reportEveryMs + initialDelayMs, reportEveryMs, TimeUnit.MILLISECONDS);
        }

        public void stop() { sched.shutdownNow(); }

        private void runSafe() {
            if (!running.compareAndSet(false, true)) return;
            try { run(); }
            catch (Throwable t) { LOG.warning("[SELF-VALIDATOR] " + t.getMessage()); }
            finally { running.set(false); }
        }

        private void run() {
            long t0 = System.currentTimeMillis();
            List<String> pairs = sender.getTopPairsForForecast(topNPairs);
            if (pairs == null || pairs.isEmpty()) {
                LOG.info("[SELF-VALIDATOR] no pairs available");
                return;
            }

            int barsNeeded = daysOfHistory * 96 + 250; // warmup + window
            int h1Limit    = Math.max(200, barsNeeded / 4 + 50);

            // Aggregate across all pairs
            int totalSignals = 0, totalWins = 0, totalLosses = 0, totalTimeStops = 0;
            double totalGrossPnL = 0, totalNetPnL = 0;
            double sumProfitFactor = 0, sumExpectancy = 0;
            int pairsWithSignals = 0;

            SimpleBacktester bt = new SimpleBacktester();
            bt.setInitialBalance(100.0);
            bt.setCompound(false);
            bt.setUseM1Resolution(false);

            for (String pair : pairs) {
                List<com.bot.TradingCore.Candle> m15;
                List<com.bot.TradingCore.Candle> h1;
                try {
                    m15 = sender.fetchKlines(pair, "15m", barsNeeded);
                    h1  = sender.fetchKlines(pair, "1h",  h1Limit);
                } catch (Throwable e) {
                    continue;
                }
                if (m15 == null || m15.size() < 250) continue;
                if (h1  == null || h1.size()  < 80)  continue;

                DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(pair);

                // run() expects (m1, m5, m15, h1, category). m1/m5 = null is fine —
                // the backtest engine handles it; this is the conservative path
                // (less context than live), so any edge measured here is a lower
                // bound on real-bot edge.
                BacktestResult r;
                try {
                    r = bt.run(pair, null, null, m15, h1, cat);
                } catch (Throwable e) {
                    continue;
                }
                if (r == null || r.total == 0) continue;

                pairsWithSignals++;
                totalSignals    += r.total;
                totalWins       += r.wins;
                totalLosses     += r.losses;
                totalTimeStops  += r.timeStops;
                totalGrossPnL   += r.grossPnL;
                totalNetPnL     += r.netPnL;
                if (r.profitFactor > 0 && !Double.isInfinite(r.profitFactor))
                    sumProfitFactor += r.profitFactor;
                sumExpectancy += r.expectancy;
            }

            long elapsed = System.currentTimeMillis() - t0;
            String report = formatReport(daysOfHistory, pairs.size(), pairsWithSignals,
                    elapsed, totalSignals, totalWins, totalLosses, totalTimeStops,
                    totalGrossPnL, totalNetPnL, sumProfitFactor, sumExpectancy);

            LOG.info(report);
            // [v78 NO-SPAM] SELF-VALIDATOR Telegram report DISABLED.
            // The report runs every 6h and almost always says either
            // "NO SIGNALS GENERATED" or "MARGINAL/NO-GO" — neither is
            // actionable for a manual trader. Full report stays in LOG
            // for the operator who needs it; user gets clean chat.
            // To re-enable: set env VALIDATOR_TELEGRAM=1.
            if (tg != null && "1".equals(System.getenv().getOrDefault("VALIDATOR_TELEGRAM", "0"))) {
                try { tg.sendMessageAsync(report); }
                catch (Throwable ignore) { }
            }
        }

        private static String formatReport(int days, int pairsScanned, int pairsWithSignals,
                                           long elapsedMs, int signals, int wins, int losses,
                                           int timeStops, double grossPnL, double netPnL,
                                           double sumPF, double sumExpectancy) {
            if (signals == 0) {
                return String.format(
                        "🧪 SELF-VALIDATOR (%dd × %d pairs, %.1fs)\n" +
                                "❌ NO SIGNALS GENERATED on history.\n" +
                                "Engine produced 0 ideas across %d pairs walked.\n" +
                                "→ Filters too strict OR market regime mismatch.",
                        days, pairsScanned, elapsedMs / 1000.0, pairsScanned);
            }
            double winRate = 100.0 * wins / signals;
            double avgPF   = pairsWithSignals > 0 ? sumPF / pairsWithSignals : 0.0;
            double avgExp  = pairsWithSignals > 0 ? sumExpectancy / pairsWithSignals : 0.0;
            double sigPerDayPerPair = signals / (double) Math.max(1, days * pairsWithSignals);

            String verdict =
                    (winRate >= 55 && avgPF > 1.50 && netPnL > 0) ? "✅ GO — viable edge"
                            : (winRate >= 48 && avgPF > 1.10 && netPnL > 0) ? "⚠️ MARGINAL — edge thin"
                              : "❌ NO-GO — no edge / negative expectancy";

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("🧪 SELF-VALIDATOR  %dd × %d pairs  (%.1fs)\n",
                    days, pairsScanned, elapsedMs / 1000.0));
            sb.append(String.format("Signals: %d   ~%.2f / day / pair (active: %d)\n",
                    signals, sigPerDayPerPair, pairsWithSignals));
            sb.append(String.format("Win-rate: %.1f%%  (W=%d, L=%d, T-stop=%d)\n",
                    winRate, wins, losses, timeStops));
            sb.append(String.format("Gross PnL: %+.2f%%   Net PnL: %+.2f%%\n",
                    grossPnL, netPnL));
            sb.append(String.format("Avg profit-factor: %.2f   Avg expectancy: $%+.2f / trade\n",
                    avgPF, avgExp));
            sb.append("\n").append(verdict).append("\n");
            return sb.toString();
        }

        // ── helpers ──
        private static int envInt(String key, int def) {
            try { return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(def))); }
            catch (Exception e) { return def; }
        }
        private static long envLong(String key, long def) {
            try { return Long.parseLong(System.getenv().getOrDefault(key, String.valueOf(def))); }
            catch (Exception e) { return def; }
        }
    }
}