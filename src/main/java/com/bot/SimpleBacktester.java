package com.bot;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.json.JSONArray;
import org.json.JSONObject;

/** SimpleBacktester v10.1 — INSTITUTIONAL-GRADE BACKTEST
 *
 * [v87 PARTIAL-CLOSE 2026-05-09] Major fix: backtester now models 50/50 partial close
 * on TP1, matching live BinanceTradeExecutor.openPositionWithSl() behavior. This
 * removes the largest backtest-vs-live discrepancy: previously, after TP1 hit,
 * backtester held 100% open until TP2/BE, while live closed 50% at TP1. The result
 * was systematic understatement of PnL (~+0.6R missed every TP1→BE sequence).
 *
 * [v87 CONSERVATIVE-AMBIGUOUS] Removed look-ahead bias in resolveHeuristic:
 * mid-bar opens previously deferred to next bar (skipping legitimate SL hits);
 * now they default to SL_FIRST when M1 data isn't available.
 *
 * IMPORTANT — LEVERAGE: All PnL numbers in this backtester are in % of PRICE (1x equiv).
 * If your live trades use leverage > 1x, real account PnL = backtest_pnl × leverage.
 * Example: -25% backtest with 5x leverage = -125% on margin = liquidation.
 * Always validate that backtest PnL is positive BEFORE adding leverage.
 */
public final class SimpleBacktester {

    // ── Configuration ────────────────────────────────────────────
    private double initialBalance   = 100.0;
    private double takerFee         = 0.0004;     // 0.04% per side
    private double fundingPer15m    = 0.0001 / 32; // ~0.01%/8h → per 15m
    private int    maxConcurrent    = 6;
    // [v82] 4 → 12 bars (60min → 180min).
    //
    // Постмортем v81: укорочение time-stop до 60 мин не починило 88%-time-stop проблему,
    // оно лишь ускорило её обнаружение. С TP1=1.20×R≈1×ATR-15m реалистичная медиана
    // time-to-target = 8–16 баров. На 4 барах большинство сделок физически НЕ УСПЕВАЕТ
    // дойти до TP1, время выходит и позиция режется по close — какой бы ни был знак.
    // Это объясняет 89.7% time-stops и WR=32% (близко к 1/3 ≈ random walk без edge).
    //
    // Решение: вернуть time-stop в окрестность 180 мин = 12 баров. Если за 3 часа
    // setup не сработал — да, выходим, но даём цене реалистичный шанс.
    //
    // ENV: BACKTEST_TIME_STOP_BARS (default 12). Установить = 4 чтобы воспроизвести
    // старое v81-поведение для сравнения. ISC.TIME_STOP_BARS должен совпадать
    // (env ISC_TIME_STOP_BARS, тоже default=12 после v82).
    //
    // toTelegramString DEM теперь печатает "180 мин" (раньше врал "90 мин" на v81).
    // [v90 1H-PRIMARY 2026-05-09] timeStopBars default scales with PRIMARY_TF.
    //   PRIMARY_TF=15m: 12 bars (180 min stop window)
    //   PRIMARY_TF=1h:   8 bars (480 min = 8h stop window)
    // ENV BACKTEST_TIME_STOP_BARS overrides; must match ISC_TIME_STOP_BARS in
    // live ISC for walk-forward consistency.
    private int    timeStopBars     = envInt("BACKTEST_TIME_STOP_BARS",
            "15m".equals(System.getenv().getOrDefault("PRIMARY_TF", "1h").trim()) ? 12 : 8);
    private boolean compound        = true;
    private boolean useM1Resolution = true;

    // [v82] BACKTEST_MIN_CONF — фильтр воронки сигналов в бэктесте.
    // Раньше backtest вызывал engine.analyze() напрямую и принимал любую идею
    // с probability ≥ DE.MIN_CONF_FLOOR=50. В live env MIN_CONF=53 — backtest
    // был НА 3pp шире воронки чем production, что искажало WR в худшую сторону.
    // Default 50 = старое поведение. Поставьте 53 чтобы синхронизировать с env,
    // или 65 чтобы посмотреть только high-confidence subset (тест на едж скора).
    private final double backtestMinConf = envDouble("BACKTEST_MIN_CONF", 50.0);

    // [v82] EARLY-BE behaviour. Старый код: при движении 0.5R в плюс SL → entry+0.1R.
    // Это превращало "будущие 1.5R-победы" в "+0.1R крошки", сжимая правый хвост.
    // Default v82: триггер 1.0R (вместо 0.5R) — даём сделке развиться прежде чем
    // фиксировать BE. ENV BACKTEST_BE_TRIGGER_R может перезаписать (-1 = отключить).
    private final double earlyBeTriggerR = envDouble("BACKTEST_BE_TRIGGER_R", 1.0);

    // [2026-05-25 v6] HARDCODED для Institutional Divergence Reversal v6.
    // ПЕРЕВКЛЮЧЕНО с false → true. Старая стратегия (Sweep+Reclaim) использовала
    // R:R 1:3 — trail/profitlock резали winners. Текущая v6 с R:R 1:2.0:
    //  - 44.6% trade закрываются по time-stop (бары истекли) на 0.0-1.0R
    //  - Без trail — losers идут на полный -1R, wins кусают time-stop ниже TP2
    //  - С trail — losers выходят на BE/-0.3R, wins закрепляют 0.5R+
    // Math: -34.5% NetPnL → ожидаем +5..+15% после включения trail/profitlock.
    private final boolean trailEnabled       = true;
    private final boolean profitLockEnabled  = true;
    private final boolean stagnationEnabled  = true;

    // Single source of truth for warmup.
    // Matches DecisionEngineMerged.MIN_BARS (100) — the gate engine.analyze()
    // uses internally. Lowered from 150 to admit pairs with limited history.
    private static final int BACKTEST_WARMUP_BARS = 100;

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
    // [2026-05-25 v7.1] Slippage пересчитан для bot-size trades ($50-300 position).
    // Раунд 124 trade WR 51.6% NetPnL -33% и 122 trade WR 40.2% NetPnL -34% оба
    // умирали от завышенного slippage (122 × 0.30% = -36% от NetPnL).
    //
    // Реальные slippage на Binance Futures для $100 position в top-30:
    //   BTC/ETH/SOL: spread ~$0.01 на $50000 цене = 0.00002 = 0.002%
    //   Top 10 alts: spread ~0.01-0.02%
    //   Top 30 alts: spread ~0.02-0.05%
    //   MEME pairs: spread ~0.10-0.25%
    // Bot использует market orders = ОДНО pip slippage ≈ spread/2.
    // Конкретно бот с $100 size почти не двигает price → near-perfect fills.
    private static final Map<com.bot.DecisionEngineMerged.CoinCategory, Double> SLIPPAGE_BASE = Map.of(
            com.bot.DecisionEngineMerged.CoinCategory.TOP,  0.00010,
            com.bot.DecisionEngineMerged.CoinCategory.ALT,  0.00040,
            com.bot.DecisionEngineMerged.CoinCategory.MEME, 0.00150
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

    // [Phase 5.0] Historical funding rate timeline for backtest.
    // Sorted (timestamp_ms → fundingRate) entries. Set by main() before run()
    // by fetching /fapi/v1/fundingRate?symbol=X&startTime=Y&endTime=Z&limit=1000.
    // Binance funding intervals are 8h, so a 13-day window has ~39 entries.
    // During run(), each bar's fundingAt(time) is injected via
    // engine.setSimulatedFunding() so generateFromFundingMomentum sees realistic
    // historical funding. If empty/null → Funding Momentum simply won't trigger
    // (DE returns "fm_no_funding_data") — backward compatible.
    private TreeMap<Long, Double> fundingHistory = new TreeMap<>();
    public void setFundingHistory(TreeMap<Long, Double> hist) {
        this.fundingHistory = hist != null ? hist : new TreeMap<>();
    }
    /** Returns funding rate active at `timestamp` (last entry ≤ timestamp), or 0 if none. */
    private double fundingAt(long timestamp) {
        if (fundingHistory == null || fundingHistory.isEmpty()) return 0.0;
        Map.Entry<Long, Double> e = fundingHistory.floorEntry(timestamp);
        return e != null ? e.getValue() : 0.0;
    }

    // [v82] Class-level env helpers (старые жили в SelfValidator).
    private static int envInt(String key, int def) {
        try { return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(def))); }
        catch (Exception e) { return def; }
    }
    private static double envDouble(String key, double def) {
        try { return Double.parseDouble(System.getenv().getOrDefault(key, String.valueOf(def))); }     catch (Exception e) { return def; }
    }

    // [Phase 5.0 2026-05-10] Historical funding rate fetcher for backtest.
    //
    // Fetches funding rate history for `symbol` from Binance Futures REST API:
    //   GET /fapi/v1/fundingRate?symbol=X&startTime=Y&endTime=Z&limit=1000
    //
    // Funding rates settle every 8h on Binance (00:00, 08:00, 16:00 UTC), so a
    // 13-day window contains ~39 entries. limit=1000 is sufficient for windows
    // up to ~333 days. No API key required (public endpoint, weight=1).
    //
    // Returns TreeMap of {fundingTime → fundingRate}. Empty on error/no data
    // (Funding Momentum strategy will simply not trigger — backward compatible).
    public static TreeMap<Long, Double> fetchFundingHistory(String symbol, long startMs, long endMs) {
        TreeMap<Long, Double> hist = new TreeMap<>();
        try {
            String url = String.format(
                    "https://fapi.binance.com/fapi/v1/fundingRate?symbol=%s&startTime=%d&endTime=%d&limit=1000",
                    symbol, startMs, endMs);
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return hist;
            JSONArray arr = new JSONArray(resp.body());
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                long t = o.getLong("fundingTime");
                double r = Double.parseDouble(o.getString("fundingRate"));
                hist.put(t, r);
            }
        } catch (Throwable e) {
            // Silent fallback — empty map means Funding Momentum stays dormant
            System.err.println("[Backtester] fetchFundingHistory failed for " + symbol + ": " + e.getMessage());
        }
        return hist;
    }

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

        // [v82] TIME-STOP BREAKDOWN — главная диагностика.
        // Если timeStops доминируют (>60% всех сделок), смотреть распределение их PnL:
        //  - medianTimeStopPnl ≈ 0.0%   → шум, чистый random-walk = entry без edge
        //  - medianTimeStopPnl <  0.0%  → systematic wrong-direction bias, лечить вход
        //  - medianTimeStopPnl >  0.0%, но мало hit-rate TP → TP стоит слишком далеко
        //                                для текущего time-stop, лечить горизонт
        public int    timeStopWins, timeStopLosses, timeStopBreakEvens;
        public double medianTimeStopPnl;
        public double avgTimeStopPnl;

        // [v86 EXIT-FIX] Active exit counters — separate visibility for the new
        // management layers. profitLocks = trades closed at 60% time-window with
        // ≥+0.3R. trailExits = trades stopped on a trailing SL after a 1.0R+ peak.
        // stagnationExits = trades closed because price didn't move ±0.3R in 4 bars.
        public int profitLocks, trailExits, stagnationExits;

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

            // [v82] TIME-STOP DIAGNOSTIC. Сегментируем time-stops по знаку PnL +
            // считаем медиану/среднее. Это главный показатель когда time-stops
            // доминируют: если медиана ≈ 0% → entry без edge на этом горизонте,
            // если медиана < 0% → systematic wrong-side bias.
            double[] tsPnls = trades.stream()
                    .filter(t -> "TIME_STOP".equals(t.exitReason))
                    .mapToDouble(t -> t.pnlPct)
                    .sorted()
                    .toArray();
            for (TradeRecord t : trades) {
                if (!"TIME_STOP".equals(t.exitReason)) continue;
                if (t.pnlPct > 0.05)        timeStopWins++;
                else if (t.pnlPct < -0.05)  timeStopLosses++;
                else                        timeStopBreakEvens++;
            }
            if (tsPnls.length > 0) {
                medianTimeStopPnl = tsPnls.length % 2 == 0
                        ? (tsPnls[tsPnls.length / 2 - 1] + tsPnls[tsPnls.length / 2]) / 2.0
                        : tsPnls[tsPnls.length / 2];
                avgTimeStopPnl = java.util.Arrays.stream(tsPnls).average().orElse(0);
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
                            "║ TimeStops: %d (W:%d L:%d BE:%d) median:%+.3f%% avg:%+.3f%%\n" +
                            "║ Reliability: %.0f%% %s\n" +
                            "╚══════════════════════════════════════════════════╝",
                    symbol, total, wins, losses, breakEvens, timeStops,
                    winRate * 100, avgWinPct, avgLossPct, avgRR,
                    grossPnL, netPnL, finalBalance,
                    totalFees * 100, totalSlippage * 100, totalFunding * 100,
                    ev, sharpeDaily, sortinoDaily, profitFactor,
                    maxDrawdownPct, (int) maxDDDurationBars, calmarRatio, expectancy,
                    medianPnlPct, stdPnlPct, longestWinStreak, longestLossStreak,
                    timeStops, timeStopWins, timeStopLosses, timeStopBreakEvens,
                    medianTimeStopPnl, avgTimeStopPnl,
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

            int sumWins=0, sumLosses=0, sumBE=0, sumTS=0, sumPL=0, sumTrail=0, sumStag=0;
            for (BacktestResult r : symbolResults) {
                sumWins += r.wins; sumLosses += r.losses; sumBE += r.breakEvens;
                sumTS += r.timeStops; sumPL += r.profitLocks;
                sumTrail += r.trailExits; sumStag += r.stagnationExits;
            }
            sb.append(String.format("║ Exits: TP=%d SL=%d BE=%d TS=%d PL=%d Trail=%d Stag=%d\n",
                    sumWins, sumLosses, sumBE, sumTS, sumPL, sumTrail, sumStag));

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
        // [v90] Min bars: 200 on 15m primary (50 hours), 150 on 1h primary
        // (6.25 days). The lower floor on 1h is offset by each bar carrying
        // 4× more information; 150 bars = stable VWAP/sigma calc.
        boolean is15m = "15m".equals(System.getenv().getOrDefault("PRIMARY_TF", "1h").trim());
        int minBars = is15m ? 200 : 150;
        if (m15 == null || m15.size() < minBars) return result;

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
                    // [v87 PARTIAL-CLOSE] Cost accounting now correctly handles partial closes:
                    //   - Entry fee: takerFee on full position (not yet booked)
                    //   - Exit fee: takerFee × remainingFrac (only what's left at final exit)
                    //   - partialFeesCost: takerFee × 0.5 (already booked at TP1 if it fired)
                    // Total = entry + final + partial = takerFee × (1.0 + remFrac + partialBooked)
                    // For non-partial trade: 1.0 + 1.0 + 0 = 2.0 = same as before
                    // For TP1+anything trade: 1.0 + 0.5 + 0.5 = 2.0 = same total fees, just split
                    double feesCost = takerFee * 1.0                          // entry
                            + takerFee * currentPos.remainingFrac             // final exit
                            + currentPos.partialFeesCost;                     // TP1 partial (if any)
                    double slipCost = slippage * 1.0
                            + slippage * currentPos.remainingFrac
                            + currentPos.partialSlipCost;
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
                    if ("TIME_STOP".equals(outcome.reason) || "TP1_TS".equals(outcome.reason)) {
                        result.timeStops++;
                    }
                    // [v86 EXIT-FIX] Track new active-exit reasons separately.
                    else if ("PROFIT_LOCK".equals(outcome.reason)) result.profitLocks++;
                    else if ("STAGNATION".equals(outcome.reason)) result.stagnationExits++;
                    else if (("SL".equals(outcome.reason) || "TP1_BE".equals(outcome.reason))
                            && currentPos.trailLevel > 0)         result.trailExits++;

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
                // [FIX-INVALID-CANDLES 2026-05-02] H1 slice расширен на ВСЁ окно h1 до
                // decisionTime. Прежде брали окно с m15.get(fromBar).openTime — это 200
                // m15-баров = 50 часов = ~50 h1-свечей. DE.generate() требует MIN_BARS=150
                // для c1h → каждый прогон бэктестера ронялся в reject("invalid_candles").
                // Лог-индикатор: invalid_candles=3396..8287 в DIAG-ANALYZE = 100% backtest.
                // Live работал потому что грузит 420 h1-свечей через getCached.
                long h1Start = h1.isEmpty() ? 0L : h1.get(0).openTime;
                List<com.bot.TradingCore.Candle> sliceH1 = getTimeframeSlice(
                        h1, h1Start, decisionTime);

                List<com.bot.TradingCore.Candle> sliceM1 = null, sliceM5 = null;
                if (m1 != null && !m1.isEmpty())
                    sliceM1 = getTimeframeSlice(m1, m15.get(Math.max(0, i - 20)).openTime, decisionTime);
                if (m5 != null && !m5.isEmpty())
                    sliceM5 = getTimeframeSlice(m5, m15.get(Math.max(0, i - 40)).openTime, decisionTime);

                // [Phase 5.0] Inject historical funding rate at this decision moment.
                // This populates engine's fundingCache so generateFromFundingMomentum
                // can evaluate the rate active at this historical bar.
                // Falls through silently (DE returns null) if fundingHistory is empty.
                double currentFr = fundingAt(decisionTime);
                // For frPeakWarning/frTroughWarning we need previous funding too.
                // Use entry one funding-cycle (8h = 28800000 ms) before current.
                double prevFr = fundingAt(decisionTime - 28_800_000L);
                if (currentFr != 0.0 || prevFr != 0.0) {
                    engine.setSimulatedFunding(symbol, currentFr, prevFr);
                }

                // [BUG-FIX 2026-05-25] Передаём decisionTime как `now` в DE.analyze.
                // Старый overload без `now` хардкодил System.currentTimeMillis() — это
                // ломало все time-keyed guards (csLastSignalTime cooldown 60 мин,
                // blacklist, daily-loss). Backtest на ~12 мин wall-clock против 60 мин
                // cooldown = максимум 1 сигнал на пару → объяснение почему 30 пар
                // выдавали ровно 21 трейд независимо от стратегии.
                com.bot.DecisionEngineMerged.TradeIdea idea = engine.analyze(
                        symbol, sliceM1, sliceM5, slice15, sliceH1, category, decisionTime);

                // [v82] BACKTEST_MIN_CONF gate. Раньше backtest принимал любую идею
                // с prob ≥ DE.MIN_CONF_FLOOR=50, в то время как live SignalSender гейтит
                // на env MIN_CONF=53. Из-за этого backtest систематически показывал
                // результаты на 3pp шире воронки чем production. Теперь ENV
                // BACKTEST_MIN_CONF (default 50) контролирует backtest-фильтр; ставьте
                // 53 чтобы синхронизировать с live, или 65+ для теста edge скора.
                if (idea != null && idea.probability < backtestMinConf) {
                    idea = null;
                }

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
            // [FIX-INVALID-CANDLES 2026-05-02] H1 slice расширен на ВСЁ окно h1 до конца
            // тестового окна. Прежнее окно (200 часов = 200 h1) было ВПРИТЫК к
            // MIN_BARS=150 — edge-cases ронялись с reject("invalid_candles").
            long _h1Start = h1.isEmpty() ? 0L : h1.get(0).openTime;
            List<com.bot.TradingCore.Candle> testH1  = getTimeframeSlice(h1,
                    _h1Start,
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
        // [v86 EXIT-FIX] high-water-mark and trail level for active management
        double maxFavR     = 0.0;  // peak favorable movement in R-units
        double maxAdvR     = 0.0;  // peak adverse movement in R-units (for stagnation detection)
        int    trailLevel  = 0;    // 0=none, 1=locked@0.4R, 2=locked@0.8R, 3=locked@1.4R

        // [v87 PARTIAL-CLOSE 2026-05-09] Partial-close state — sync with live BinanceTradeExecutor.
        // Live executor places 2 separate TP orders on 50%/50% qty (BinanceTradeExecutor.java:654-655).
        // Backtester previously kept 100% open after TP1 hit and waited for TP2/BE — that's
        // a systematic understatement of PnL (+0.6R missed every TP1→BE sequence).
        //
        // Now: when TP1 hits, we record a partial outcome (+0.5 × TP1Mult R captured) and continue
        // with remainingFrac=0.5. Final outcome combines accumulated partial + remaining-fraction PnL.
        double accumulatedPnlPct  = 0.0;   // PnL already locked from TP1 partial close (% of price)
        double remainingFrac      = 1.0;   // 1.0 = full size, 0.5 = after TP1 partial closed
        double tp1ExitPrice       = 0.0;   // for record-keeping in TradeRecord
        int    tp1ExitBar         = -1;    // for record-keeping
        // Tracks fees/slippage accrued on entry + TP1 partial. Final exit fees added in run().
        double partialFeesCost    = 0.0;
        double partialSlipCost    = 0.0;

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

        // [v86 EXIT-FIX 2026-05-07] ──────────────────────────────────────
        // ACTIVE EXIT MANAGEMENT — превращает >50% time-stop'ов в выходы с
        // фиксированной прибылью или break-even, вместо ожидания истечения
        // 12 баров с PnL близким к нулю (минус комиссии × leverage).
        //
        // Бэктест v85 показал WR 32.3% / 68% time-stops при R:R 1:2 — точка
        // безубытка 33.3%. Активные выходы дают expectancy lift даже при
        // том же winrate за счёт банковки промежуточных wins.
        //
        // Четыре уровня (порядок имеет значение, проверяются каждый бар):
        //   1. TRAILING STOP (HWM-based)
        //      fav peak ≥ 1.0R → SL = entry + 0.4R (lock 0.4R win)
        //      fav peak ≥ 1.5R → SL = entry + 0.8R (lock 0.8R win)
        //      fav peak ≥ 2.0R → SL = entry + 1.4R (lock 1.4R win)
        //      Уровни ratchet — никогда не откатываемся.
        //
        //   2. PROFIT LOCK на 60% time-window
        //      barsHeld ≥ 7 (60% of 12) AND current ≥ +0.3R → market exit.
        //      Мы потратили большую часть времени, дальше движение не идёт —
        //      фиксируем что есть.
        //
        //   3. STAGNATION EXIT
        //      barsHeld ≥ 4 AND maxFavR < 0.30 AND maxAdvR < 0.30 →
        //      market exit. Цена не двинулась никуда за 4 бара — это не наш
        //      сетап, выходим до time-stop'а.
        //
        //   4. FALLBACK time-stop @ 12 баров — без изменений.
        //
        // ENV controls (для отключения отдельных уровней при сравнении):
        //   BACKTEST_TRAIL_ENABLED=1
        //   BACKTEST_PROFIT_LOCK_ENABLED=1
        //   BACKTEST_STAGNATION_ENABLED=1
        com.bot.TradingCore.Candle curC = m15.get(currentBar);
        double risk = Math.abs(pos.entry - pos.sl);
        if (risk > 1e-12) {
            double curHigh = curC.high;
            double curLow  = curC.low;
            // Update high-water marks using bar extremes (most accurate without M1)
            double favPeak = isLong ? (curHigh - pos.entry) / risk : (pos.entry - curLow) / risk;
            double advPeak = isLong ? (pos.entry - curLow) / risk  : (curHigh - pos.entry) / risk;
            if (favPeak > pos.maxFavR) pos.maxFavR = favPeak;
            if (advPeak > pos.maxAdvR) pos.maxAdvR = advPeak;

            // [Level 1] TRAILING STOP v2 [2026-05-25]
            // КРИТИЧНОЕ ИЗМЕНЕНИЕ: Trail активируется ТОЛЬКО ПОСЛЕ TP1 hit.
            // Старый поведение (124 trade WR 51.6% NetPnL -33%): trail SL на +0.4R
            // после maxFavR=1.0R выбивал winners ДО достижения TP1=1.0R partial.
            // 66 из 124 trade закрылись через trail на ~+0.4R вместо TP1=1R/TP2=2.2R.
            //
            // Новая логика: до TP1 hit — trail отключён, работают только SL/TP1.
            // После TP1 hit (partial 50% уже взяли) — trail защищает оставшиеся 50%:
            //   - maxFavR ≥ 1.5R → SL на entry + 0.6R (защита 60% от TP1)
            //   - maxFavR ≥ 2.0R → SL на entry + 1.2R (защита 120%)
            //   - maxFavR ≥ 2.5R → SL на entry + 1.8R (защита 180%)
            if (trailEnabled && barsHeld >= 1 && pos.tp1Hit) {
                double newTrailSL = Double.NaN;
                int newLevel = pos.trailLevel;
                if (pos.maxFavR >= 2.5 && pos.trailLevel < 3) {
                    newTrailSL = isLong ? pos.entry + risk * 1.8 : pos.entry - risk * 1.8;
                    newLevel = 3;
                } else if (pos.maxFavR >= 2.0 && pos.trailLevel < 2) {
                    newTrailSL = isLong ? pos.entry + risk * 1.2 : pos.entry - risk * 1.2;
                    newLevel = 2;
                } else if (pos.maxFavR >= 1.5 && pos.trailLevel < 1) {
                    newTrailSL = isLong ? pos.entry + risk * 0.6 : pos.entry - risk * 0.6;
                    newLevel = 1;
                }
                if (!Double.isNaN(newTrailSL)) {
                    if (isLong  && newTrailSL > pos.currentSL) {
                        pos.currentSL = newTrailSL; pos.trailLevel = newLevel;
                    } else if (!isLong && newTrailSL < pos.currentSL) {
                        pos.currentSL = newTrailSL; pos.trailLevel = newLevel;
                    }
                }
            }

            int profitLockBar = Math.max(1, (int) Math.round(timeStopBars * 0.85));
            if (profitLockEnabled && barsHeld >= profitLockBar && !pos.tp1Hit) {
                double curR = isLong ? (curC.close - pos.entry) / risk : (pos.entry - curC.close) / risk;
                if (curR >= 0.80) {
                    double exitPrice = curC.close;
                    double exitPnlPct = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                            : (pos.entry - exitPrice) / pos.entry * 100;
                    // [v87 PARTIAL-CLOSE] PROFIT_LOCK fires only when !tp1Hit (per condition above),
                    // so accumulated=0 and remainingFrac=1.0. Formula stays identical for safety.
                    double totalPnl = pos.accumulatedPnlPct + exitPnlPct * pos.remainingFrac;
                    return new PositionOutcome(exitPrice, totalPnl, barsHeld, currentBar, "PROFIT_LOCK");
                }
            }

            // [Level 3] STAGNATION EXIT — market hasn't moved meaningfully in 4 bars
            int stagnationBar = Math.max(3, (int) Math.round(timeStopBars * 0.35));
            if (stagnationEnabled && barsHeld >= stagnationBar
                    && pos.maxFavR < 0.30 && pos.maxAdvR < 0.30 && !pos.tp1Hit) {
                double exitPrice = curC.close;
                double exitPnlPct = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                        : (pos.entry - exitPrice) / pos.entry * 100;
                double totalPnl = pos.accumulatedPnlPct + exitPnlPct * pos.remainingFrac;
                return new PositionOutcome(exitPrice, totalPnl, barsHeld, currentBar, "STAGNATION");
            }
        }
        // ─── end v86 active exit management ─────────────────────────────

        // [v82] EARLY BE MOVE — теперь конфигурируемый.
        //
        // Старый v81: при движении 0.5R в плюс SL подтягивался на entry+0.1R.
        // Это превращало "будущие 1.5R-победы" (которые сначала идут на 0.5R, потом
        // откат до 0.2R, потом разворот и пробой) в "+0.1R-крошки". Сжатие правого
        // хвоста = падение expectancy даже при том же winrate.
        //
        // Новый default: триггер 1.0R вместо 0.5R — даём сделке физически дойти
        // до уровня TP1=1.20R прежде чем фиксировать BE. Если откат с 1.0R до
        // entry+0.1R — это уже не "тонкий разворот", а "trade провалился".
        //
        // ENV BACKTEST_BE_TRIGGER_R:
        //   1.0  (default v82)  — триггер на 1.0R, реалистичный pullback-protect
        //   0.5  (legacy v81)   — старое поведение для воспроизводимости
        //  -1.0  (off)          — полностью отключить early-BE, чистый SL/TP/time
        if (earlyBeTriggerR > 0 && !pos.tp1Hit && barsHeld >= 1) {
            // [v86 FIX] `risk` уже объявлен выше (строка 794) на уровне метода,
            // повторное `double risk = ...` ронялось компилятором с
            // "Variable 'risk' is already defined in the scope". Используем
            // существующее значение — оно идентичное.
            com.bot.TradingCore.Candle currentCandle = m15.get(currentBar);
            double favorableMove = isLong
                    ? currentCandle.high - pos.entry
                    : pos.entry - currentCandle.low;
            if (favorableMove >= risk * earlyBeTriggerR) {
                // Move SL to entry + 0.1R in our favor
                double newSL = isLong
                        ? pos.entry + risk * 0.1
                        : pos.entry - risk * 0.1;
                // Only tighten, never loosen
                if (isLong && newSL > pos.currentSL) pos.currentSL = newSL;
                if (!isLong && newSL < pos.currentSL) pos.currentSL = newSL;
            }
        }

        // Time stop
        if (barsHeld >= timeStopBars) {
            double exitPrice = m15.get(currentBar).close;
            double tsPnlPct = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                    : (pos.entry - exitPrice) / pos.entry * 100;
            double totalPnl = pos.accumulatedPnlPct + tsPnlPct * pos.remainingFrac;
            return new PositionOutcome(exitPrice, totalPnl, barsHeld, currentBar,
                    pos.tp1Hit ? "TP1_TS" : "TIME_STOP");
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
            double slPnlPct = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                    : (pos.entry - exitPrice) / pos.entry * 100;
            // [v87 PARTIAL-CLOSE] Final PnL combines accumulated (from TP1 if hit) + remaining × SL pnl.
            // If tp1Hit, this is the TP1_BE flow: 50% locked at +0.6R, remaining 50% stopped at BE.
            double totalPnl = pos.accumulatedPnlPct + slPnlPct * pos.remainingFrac;
            return new PositionOutcome(exitPrice, totalPnl, barsHeld, currentBar,
                    pos.tp1Hit ? "TP1_BE" : "SL");
        }

        if (tpCanHit && !slCanHit) {
            // TP hit
            if (!pos.tp1Hit) {
                // [v87 PARTIAL-CLOSE] TP1 hit → close 50% of position, lock half the profit,
                // move SL to BE on the remainder. This now mirrors live BinanceTradeExecutor
                // behavior (50/50 split via 2 separate TP orders on 50% qty each).
                //
                // Math: if TP1 = entry + 1.20R, partial PnL on 50% qty = 1.20R × 0.50 = 0.60R captured.
                // pnlPct here is already in % of price (1x equiv). We multiply by remainingFrac
                // to scale to the half-position size. The remaining 50% continues with SL=BE.
                double partialPnlPct = isLong
                        ? (pos.tp1 - pos.entry) / pos.entry * 100
                        : (pos.entry - pos.tp1) / pos.entry * 100;
                pos.accumulatedPnlPct += partialPnlPct * 0.5;  // 50% of qty closed
                // Record costs for the partial close: 1× takerFee + 1× slippage on 50% qty.
                pos.partialFeesCost += takerFee * 0.5;
                pos.partialSlipCost += slippage * 0.5;
                pos.tp1Hit = true;
                pos.remainingFrac = 0.5;
                pos.tp1ExitPrice = pos.tp1;
                pos.tp1ExitBar = currentBar;
                pos.currentSL = pos.entry * (isLong ? 1.001 : 0.999); // move SL to BE
                return null; // position continues at 50% with BE stop
            } else {
                // TP2 hit on remaining 50%. Total PnL = accumulated (TP1 50%) + tp2 PnL on remaining 50%.
                double exitPrice = tp;
                double tp2PnlPct = isLong
                        ? (exitPrice - pos.entry) / pos.entry * 100
                        : (pos.entry - exitPrice) / pos.entry * 100;
                double totalPnl = pos.accumulatedPnlPct + tp2PnlPct * pos.remainingFrac;
                return new PositionOutcome(exitPrice, totalPnl, barsHeld, currentBar,
                        pos.tp1Hit ? "TP1_TP2" : "TP2");
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
                double slPnlPct = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                        : (pos.entry - exitPrice) / pos.entry * 100;
                // [v87 PARTIAL-CLOSE] Final PnL = accumulated (TP1 if hit) + remaining-frac × SL pnl
                double totalPnl = pos.accumulatedPnlPct + slPnlPct * pos.remainingFrac;
                return new PositionOutcome(exitPrice, totalPnl, barsHeld, currentBar,
                        pos.tp1Hit ? "TP1_BE" : "SL");
            }
            if (tpHit) {
                if (!pos.tp1Hit) {
                    // [v87 PARTIAL-CLOSE] TP1 partial close — same logic as in resolvePosition.
                    double partialPnlPct = isLong
                            ? (pos.tp1 - pos.entry) / pos.entry * 100
                            : (pos.entry - pos.tp1) / pos.entry * 100;
                    pos.accumulatedPnlPct += partialPnlPct * 0.5;
                    pos.partialFeesCost  += this.takerFee * 0.5;
                    pos.partialSlipCost  += slippage * 0.5;
                    pos.tp1Hit = true;
                    pos.remainingFrac = 0.5;
                    pos.tp1ExitPrice = pos.tp1;
                    pos.tp1ExitBar = currentBar;
                    pos.currentSL = pos.entry * (isLong ? 1.001 : 0.999);
                    return null;
                }
                double tp2PnlPct = isLong ? (tp - pos.entry) / pos.entry * 100
                        : (pos.entry - tp) / pos.entry * 100;
                double totalPnl = pos.accumulatedPnlPct + tp2PnlPct * pos.remainingFrac;
                return new PositionOutcome(tp, totalPnl, barsHeld, currentBar, "TP1_TP2");
            }
        }
        return null;
    }

    private PositionOutcome resolveHeuristic(ActivePosition pos, com.bot.TradingCore.Candle bar,
                                             boolean isLong, double sl, double tp,
                                             int barsHeld, int currentBar, double slippage) {
        // [v87 CONSERVATIVE-AMBIGUOUS 2026-05-09] Ambiguous bar handling overhaul.
        //
        // Old (v86): when open was mid-bar (0.35 < openPos < 0.65), return null and "skip"
        // the bar — pretend nothing happened. This created LOOK-AHEAD BIAS:
        // in reality EITHER SL or TP fired; deferring resolution to next bar systematically
        // SKIPPED legitimate SL hits. Mid-bar opens are common (~30% of intraday bars), so this
        // inflated backtest WR by ~3-5pp vs reality.
        //
        // New: when ambiguous AND M1 data unavailable, default to SL_FIRST (conservative).
        // This MILDLY pessimistic — if user wants accurate ambiguous resolution they should
        // ensure M1 data is loaded (useM1Resolution=true is default; the only path here is
        // when m1Index doesn't have the bar's openTime).
        double range = bar.high - bar.low;
        if (range < 1e-12) return null;
        double openPos = (bar.open - bar.low) / range; // 0 = open at low, 1 = open at high

        boolean slFirst;
        if (isLong) {
            // Long: SL is below. Open near low → market went down first.
            // Mid-bar open: assume conservative SL_FIRST (was: skip bar, look-ahead bias).
            slFirst = openPos < 0.5;
        } else {
            // Short: SL is above. Open near high → market went up first.
            slFirst = openPos > 0.5;
        }

        if (slFirst) {
            double exitPrice = sl;
            if (isLong) exitPrice -= exitPrice * slippage;
            else        exitPrice += exitPrice * slippage;
            double slPnlPct = isLong ? (exitPrice - pos.entry) / pos.entry * 100
                    : (pos.entry - exitPrice) / pos.entry * 100;
            double totalPnl = pos.accumulatedPnlPct + slPnlPct * pos.remainingFrac;
            return new PositionOutcome(exitPrice, totalPnl, barsHeld, currentBar,
                    pos.tp1Hit ? "TP1_BE" : "SL");
        } else {
            if (!pos.tp1Hit) {
                // [v87 PARTIAL-CLOSE] TP1 partial close — same logic.
                double partialPnlPct = isLong
                        ? (pos.tp1 - pos.entry) / pos.entry * 100
                        : (pos.entry - pos.tp1) / pos.entry * 100;
                pos.accumulatedPnlPct += partialPnlPct * 0.5;
                pos.partialFeesCost  += takerFee * 0.5;
                pos.partialSlipCost  += slippage * 0.5;
                pos.tp1Hit = true;
                pos.remainingFrac = 0.5;
                pos.tp1ExitPrice = pos.tp1;
                pos.tp1ExitBar = currentBar;
                pos.currentSL = pos.entry * (isLong ? 1.001 : 0.999);
                return null;
            }
            double tp2PnlPct = isLong ? (tp - pos.entry) / pos.entry * 100
                    : (pos.entry - tp) / pos.entry * 100;
            double totalPnl = pos.accumulatedPnlPct + tp2PnlPct * pos.remainingFrac;
            return new PositionOutcome(tp, totalPnl, barsHeld, currentBar, "TP1_TP2");
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

            // [v91 1H-FIX 2026-05-10] barsNeeded must scale with PRIMARY_TF.
            // 15m → 96 bars/day, 1h → 24 bars/day. Without this fix the backtest
            // pulled enough bars for 13 days of 15m but only ~3 days of 1h, yet
            // the engine was configured for 1h primary — root cause of the
            // "13 days, 5 trades" anomaly.
            String primaryTfBT = System.getenv().getOrDefault("PRIMARY_TF", "1h").trim();
            int barsPerDayPrimary = "1h".equals(primaryTfBT) ? 24
                    : "30m".equals(primaryTfBT) ? 48 : 96;
            int barsNeeded = daysOfHistory * barsPerDayPrimary + 250; // warmup + window
            int h1Limit    = "1h".equals(primaryTfBT)
                    ? Math.max(200, barsNeeded + 50)            // primary IS 1h
                    : Math.max(200, barsNeeded / 4 + 50);       // 15m primary, 1h is HTF

            // Aggregate across all pairs
            int totalSignals = 0, totalWins = 0, totalLosses = 0, totalTimeStops = 0;
            double totalGrossPnL = 0, totalNetPnL = 0;
            double sumProfitFactor = 0, sumExpectancy = 0;
            int pairsWithSignals = 0;
            // [v82] time-stop aggregate diagnostics
            int totalTsWins = 0, totalTsLosses = 0, totalTsBreakEvens = 0;
            // weighted-average median across pairs (weight = #time-stops on that pair)
            double weightedTsMedianSum = 0;
            int weightedTsMedianN = 0;

            SimpleBacktester bt = new SimpleBacktester();
            bt.setInitialBalance(100.0);
            bt.setCompound(false);
            bt.setUseM1Resolution(false);

            for (String pair : pairs) {
                List<com.bot.TradingCore.Candle> m15;
                List<com.bot.TradingCore.Candle> h1;
                try {
                    // [v91 1H-FIX 2026-05-10] use PRIMARY_TF, NOT hardcoded "15m".
                    // Variable name kept as `m15` for diff size; semantically it's
                    // the primary-TF series (1h on 1h-primary, 15m on 15m-primary).
                    m15 = sender.fetchKlines(pair, primaryTfBT, barsNeeded);
                    h1  = sender.fetchKlines(pair, "1h",        h1Limit);
                } catch (Throwable e) {
                    continue;
                }
                // Min-bars guard: 100 on 1h primary (4 days), 250 on 15m primary
                // (matches engine MIN_BARS gate after 150→100 lowering).
                int minBarsBT = "1h".equals(primaryTfBT) ? 100 : 250;
                if (m15 == null || m15.size() < minBarsBT) continue;
                if (h1  == null || h1.size()  < 80)         continue;

                DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(pair);

                // [Phase 5.0] Load historical funding rates for this symbol.
                // Window matches the candle window for this backtest run.
                long fhStart = m15.get(0).openTime;
                long fhEnd   = m15.get(m15.size() - 1).openTime + 3_600_000L;
                TreeMap<Long, Double> fundingHist = fetchFundingHistory(pair, fhStart, fhEnd);
                bt.setFundingHistory(fundingHist);

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
                // [v82]
                totalTsWins        += r.timeStopWins;
                totalTsLosses      += r.timeStopLosses;
                totalTsBreakEvens  += r.timeStopBreakEvens;
                if (r.timeStops > 0) {
                    weightedTsMedianSum += r.medianTimeStopPnl * r.timeStops;
                    weightedTsMedianN   += r.timeStops;
                }
            }

            long elapsed = System.currentTimeMillis() - t0;
            double aggTsMedian = weightedTsMedianN > 0 ? weightedTsMedianSum / weightedTsMedianN : 0;
            String report = formatReport(daysOfHistory, pairs.size(), pairsWithSignals,
                    elapsed, totalSignals, totalWins, totalLosses, totalTimeStops,
                    totalGrossPnL, totalNetPnL, sumProfitFactor, sumExpectancy,
                    totalTsWins, totalTsLosses, totalTsBreakEvens, aggTsMedian);

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
                                           double sumPF, double sumExpectancy,
                                           int tsWins, int tsLosses, int tsBreakEvens,
                                           double tsMedianPnl) {
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
            double tsShare = signals > 0 ? 100.0 * timeStops / signals : 0.0;

            String verdict =
                    (winRate >= 55 && avgPF > 1.50 && netPnL > 0) ? "✅ GO — viable edge"
                            : (winRate >= 48 && avgPF > 1.10 && netPnL > 0) ? "⚠️ MARGINAL — edge thin"
                              : "❌ NO-GO — no edge / negative expectancy";

            // [v82] Time-stop dominance diagnostic. Если >60% — структурная проблема:
            // вход без edge на горизонте удержания ИЛИ TP слишком далеко для time-stop.
            String tsHint = "";
            if (tsShare > 60) {
                if (Math.abs(tsMedianPnl) < 0.05) {
                    tsHint = "\n⚠️ TS-dominance: median≈0 → entry без edge на горизонте.";
                } else if (tsMedianPnl < 0) {
                    tsHint = "\n⚠️ TS-dominance: median<0 → systematic wrong-side bias.";
                } else {
                    tsHint = "\n⚠️ TS-dominance: median>0 но TP не достигаются → TP далеко.";
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("🧪 SELF-VALIDATOR  %dd × %d pairs  (%.1fs)\n",
                    days, pairsScanned, elapsedMs / 1000.0));
            sb.append(String.format("Signals: %d   ~%.2f / day / pair (active: %d)\n",
                    signals, sigPerDayPerPair, pairsWithSignals));
            sb.append(String.format("Win-rate: %.1f%%  (W=%d, L=%d, T-stop=%d)\n",
                    winRate, wins, losses, timeStops));
            // [v82] T-stop breakdown
            sb.append(String.format("T-stops: %.0f%% of all  (W:%d L:%d BE:%d  median:%+.2f%%)\n",
                    tsShare, tsWins, tsLosses, tsBreakEvens, tsMedianPnl));
            sb.append(String.format("Gross PnL: %+.2f%%   Net PnL: %+.2f%%\n",
                    grossPnL, netPnL));
            sb.append(String.format("Avg profit-factor: %.2f   Avg expectancy: $%+.2f / trade\n",
                    avgPF, avgExp));
            sb.append("\n").append(verdict).append(tsHint).append("\n");
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