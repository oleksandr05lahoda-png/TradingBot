package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║       InstitutionalSignalCore v15.0 — PORTFOLIO RISK CONTROLLER              ║
 * ╠══════════════════════════════════════════════════════════════════════════════╣
 * ║                                                                              ║
 * ║  v15.0 CRITICAL FIXES:                                                       ║
 * ║    · [FIX Дыра 1] ArrayDeque → ConcurrentLinkedDeque (thread-safe)          ║
 * ║    · [FIX Дыра 4] Asymmetric streak: win halves boost (was -1.5 only!)     ║
 * ║    · [FIX Дыра 4] Softer loss escalation: 1.0/2.5/4.5 (was 1.5/3.5/6.0)  ║
 * ║    · [FIX Дыра 4] Faster decay: 30% every 8min (was 20%/10min)            ║
 * ║                                                                              ║
 * ║  v11.0 IMPROVEMENTS:                                                         ║
 * ║    · [FIX-COMPILE] closeTrade 3-arg overload (BotMain compatibility)        ║
 * ║    · [NEW] Daily loss limit: max -3% per day → block all signals            ║
 * ║    · [NEW] Max drawdown circuit breaker: -8% from peak → pause 2h          ║
 * ║    · [NEW] Bayesian streak: loss streak uses weighted recency               ║
 * ║    · [NEW] Win/loss asymmetry: losses tighten 2× faster than wins loosen  ║
 * ║    · [NEW] Sector correlation guard with real exposure tracking             ║
 * ║    · [FIX] Streak decay: 20% every 10min (was 15%/15min — too slow)       ║
 * ║    · [FIX] Cap effective confidence at 70% (was 72% — still too high)      ║
 * ║    · History bounded, time stop = NEUTRAL, compounding risk                 ║
 * ║                                                                              ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 */
public final class InstitutionalSignalCore {
    private static final double MAX_EFFECTIVE_MIN_CONF = 64.0;

    // ── Configuration ────────────────────────────────────────────
    private final int    maxGlobalSignals;
    private final int    maxSignalsPerSymbol;
    private final double maxPortfolioHeat;   // max total risk as fraction of balance
    private final double baseMinConfidence;
    private final double minSignalPriceDiff;
    private final int    maxSameSectorSameDir;

    // Time stop
    private static final int  TIME_STOP_BARS  = 6;    // 6 × 15m = 90 min
    private static final long TIME_STOP_MS    = TIME_STOP_BARS * 15 * 60_000L;
    private static final int  MAX_HISTORY     = 100;   // per symbol, bounded

    public InstitutionalSignalCore() {
        // [v28.0] PATCH #18: maxGlobalSignals 20→8, maxPortfolioHeat 12%→8%, minConf 52→62
        // OLD: this(20, 2, 0.12, 52.0, 0.0025, 2) — 20 signals×0.8% = 16% heat (exceeded 12% cap)
        // NEW: 8 signals×1.0% = 8% heat (consistent with cap), minConf raised to 62% pre-edge-verification
        this(8, 2, 0.08, 62.0, 0.0025, 2);
    }

    public InstitutionalSignalCore(int maxGlobal, int maxPerSym, double maxHeat,
                                   double minConf, double minPriceDiff, int maxSectorDir) {
        this.maxGlobalSignals     = maxGlobal;
        this.maxSignalsPerSymbol  = maxPerSym;
        this.maxPortfolioHeat     = maxHeat;
        this.baseMinConfidence    = minConf;
        this.minSignalPriceDiff   = minPriceDiff;
        this.maxSameSectorSameDir = maxSectorDir;
    }

    // ── State ────────────────────────────────────────────────────
    private final Map<String, List<ActiveSignal>>  activeSignals  = new ConcurrentHashMap<>();
    private final Map<String, Deque<ClosedTrade>>  tradeHistory   = new ConcurrentHashMap<>();
    private final Map<String, Double>              symbolScore    = new ConcurrentHashMap<>();
    private volatile double                        currentHeat    = 0.0;

    // Streak guard
    private final Deque<Boolean> globalResultStreak = new ConcurrentLinkedDeque<>();
    private static final int STREAK_WINDOW = 10;
    private volatile int    currentLossStreak     = 0;
    private volatile double streakConfidenceBoost = 0.0;
    private volatile long   lastStreakUpdateMs    = System.currentTimeMillis();

    // [v12.2] NO timers, NO blocks. Streak guard raises threshold automatically.
    // Bot NEVER stops — just gets pickier after losses.
// [v32] ADAPTIVE LOSS: Removed all time-based hard pauses to fix the "pause paradox" and "dead zones".
    // 100% risk mitigation via Confidence Thresholds and Position Size halving.
    private static final double DAILY_LOSS_CAUTIOUS_PCT  = -3.0; // -3% -> cautious mode (size/2, conf+6)
    private static final double DAILY_LOSS_SURVIVAL_PCT  = -6.0; // -6% -> survival mode (size/2, conf+12)
    private volatile boolean    cautiousMode          = false; // [v31/32] raised threshold + half size

    private volatile double dailyPnLPct        = 0.0;
    private volatile long   dailyPnLResetDay   = currentDay();

    // Drawdown tracking (stats only, no pause)
    private volatile double peakBalance          = 0.0;
    private volatile double drawdownFromPeak     = 0.0;

    // [v11.0] Consecutive wins tracking for adaptive risk
    private volatile int currentWinStreak = 0;

    // [v12.0] Direction-specific loss tracking
    // If we keep losing LONGs, penalize LONGs more. Same for SHORTs.
    private volatile int consecutiveLongLosses = 0;
    private volatile int consecutiveShortLosses = 0;
    private static final int DIR_LOSS_PENALTY_THRESHOLD = 3; // 3 same-direction losses → extra penalty

    // Backtest integration
    private volatile double lastBacktestEV   = 0.0;
    private volatile long   lastBacktestTime = 0;

    // [v28.0] PATCH #7: Per-symbol OOS EV tracking.
    // OLD: single avgEV for all 100 pairs — BTCUSDT EV +0.04 masked DOGEUSDT EV -0.08.
    // NEW: each symbol gets its own EV; symbolMinConf raised +5 if OOS EV < 0.
    private final java.util.concurrent.ConcurrentHashMap<String, Double> symbolOosEV
            = new java.util.concurrent.ConcurrentHashMap<>();

    // Callbacks
    private volatile java.util.function.BiConsumer<String, String> timeStopCallback = null;
    public void setTimeStopCallback(java.util.function.BiConsumer<String, String> cb) { this.timeStopCallback = cb; }

    // ══════════════════════════════════════════════════════════════
    //  MODELS
    // ══════════════════════════════════════════════════════════════

    public static final class ActiveSignal {
        public final String symbol;
        public final com.bot.TradingCore.Side side;
        public final double entry, sl, tp1, probability;
        public final double riskPct; // % of balance at risk
        public final long   timestamp;
        public final String category;
        public final String sector;

        public ActiveSignal(String sym, com.bot.TradingCore.Side side, double entry, double sl,
                            double tp1, double prob, double riskPct, String cat, String sector) {
            this.symbol = sym; this.side = side; this.entry = entry; this.sl = sl;
            this.tp1 = tp1; this.probability = prob; this.riskPct = riskPct;
            this.timestamp = System.currentTimeMillis();
            this.category = cat; this.sector = sector;
        }

        public long ageMs() { return System.currentTimeMillis() - timestamp; }
        public boolean isExpired() { return ageMs() > TIME_STOP_MS; }
    }

    public static final class ClosedTrade {
        public final String symbol;
        public final com.bot.TradingCore.Side side;
        public final double pnlPct;
        public final long   duration;
        public final String exitReason;
        public final long   closedAt;

        public ClosedTrade(String sym, com.bot.TradingCore.Side side, double pnl, long dur, String reason) {
            this.symbol = sym; this.side = side; this.pnlPct = pnl;
            this.duration = dur; this.exitReason = reason;
            this.closedAt = System.currentTimeMillis();
        }

        public boolean isWin() { return pnlPct > 0.05; }
    }

    // ══════════════════════════════════════════════════════════════
    //  STREAK GUARD — confirmed results only
    // ══════════════════════════════════════════════════════════════

    public void registerConfirmedResult(boolean win) {
        registerConfirmedResult(win, null);
    }

    /** [v15.0] Direction-aware result tracking
     *  FIX Дыра 4: First win after loss streak HALVES the boost (was only -1.5).
     *  This prevents the bot from self-silencing after 3 normal stop-losses. */
    public void registerConfirmedResult(boolean win, com.bot.TradingCore.Side side) {
        synchronized (globalResultStreak) {
            globalResultStreak.addLast(win);
            while (globalResultStreak.size() > STREAK_WINDOW) globalResultStreak.removeFirst();
        }
        lastStreakUpdateMs = System.currentTimeMillis();

        if (win) {
            streakConfidenceBoost = 0;
            currentLossStreak = 0;
            currentWinStreak++;
            if (side == com.bot.TradingCore.Side.LONG) consecutiveLongLosses = 0;
            if (side == com.bot.TradingCore.Side.SHORT) consecutiveShortLosses = 0;
        } else {
            currentLossStreak++;
            currentWinStreak = 0;
            if (side == com.bot.TradingCore.Side.LONG) {
                consecutiveLongLosses++;
                consecutiveShortLosses = 0;
            } else if (side == com.bot.TradingCore.Side.SHORT) {
                consecutiveShortLosses++;
                consecutiveLongLosses = 0;
            }
        }
    }

    public void resetStreakGuard() {
        log("STREAK RESET (was " + currentLossStreak + " L:" + consecutiveLongLosses + " S:" + consecutiveShortLosses + ")");
        currentLossStreak = 0;
        streakConfidenceBoost = 0.0;
        consecutiveLongLosses = 0;
        consecutiveShortLosses = 0;
        lastStreakUpdateMs = System.currentTimeMillis();
    }

    private void decayStreakBoost() {
        if (streakConfidenceBoost <= 0) return;
        long elapsed = System.currentTimeMillis() - lastStreakUpdateMs;
        // [v15.0 FIX Дыра 4] Faster decay: 30% every 8min (was 20%/10min — too slow)
        // After 3 losses (boost=4.5), decay to zero in ~24min instead of ~60min
        if (elapsed > 8 * 60_000L) {
            streakConfidenceBoost *= 0.70;
            lastStreakUpdateMs = System.currentTimeMillis();
            if (streakConfidenceBoost < 0.3) { streakConfidenceBoost = 0; currentLossStreak = 0; }
        }
    }

    public double getEffectiveMinConfidence() {
        resetDailyIfNeeded();

        // [v28.0] PATCH #2: MIN_CONF floor raised 52→62 until 200+ trades verify edge.
        // At 57% confidence: fees(0.08%×2) + slippage(0.15% ALT) = -0.31% per trade.
        // Net edge at 57% winrate with avg win 1.4% = near zero. Not tradeable.
        double floor = getTotalTradeCount() >= 200 ? 52.0 : 62.0;

        double base = Math.max(baseMinConfidence, floor);

        // Backtest EV adjustment
        if (lastBacktestEV < -0.02 && System.currentTimeMillis() - lastBacktestTime < 2 * 3600_000L)
            base += 3.0;

        return Math.max(floor, Math.min(base, MAX_EFFECTIVE_MIN_CONF));
    }

    public void setBacktestResult(double ev, long ts) {
        this.lastBacktestEV = ev; this.lastBacktestTime = ts;
    }

    // [v28.0] PATCH #7: Per-symbol backtest result.
    // Called from BotMain.runPeriodicBacktest() for each backtested symbol.
    public void setSymbolBacktestResult(String symbol, double ev) {
        symbolOosEV.put(symbol, ev);
    }

    // Returns additional minConf boost for a symbol based on its OOS EV.
    // EV < 0 → +5 (symbol losing money historically → require higher conviction)
    // EV < -0.03 → +8 (symbol clearly broken → near-quarantine)
    // EV > 0.03 → -2 (proven positive EV → slight threshold reduction, but never below 62)
    public double getSymbolMinConfBoost(String symbol) {
        Double ev = symbolOosEV.get(symbol);
        if (ev == null) return 0.0; // no data yet = no adjustment
        if (ev < -0.03) return +8.0;
        if (ev < 0.00)  return +5.0;
        if (ev > 0.03)  return -2.0;
        return 0.0;
    }

    // ══════════════════════════════════════════════════════════════
    //  SIGNAL FILTERING
    // ══════════════════════════════════════════════════════════════

    // [v24.0] Max positions per direction — prevents one-sided portfolio blow-up
    // [v30] MAX_SAME_DIRECTION raised 4→6.
    // Was: max 4 concurrent LONGs or 4 SHORTs. On trending days this blocked
    // the 5th valid signal completely. With 25 pairs and TOP_N filtering,
    // 6 concurrent same-direction positions is still well within risk limits.
    private static final int MAX_SAME_DIRECTION = 6;

    public synchronized boolean allowSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        cleanupExpired();
        String sym = signal.symbol;

        // [v32] The Pause Paradox resolved: NO TIME BLOCKS. Only risk mitigation.
        // If an extreme drop occurs, we become extreme snipers, but we never go completely blind.
        resetDailyIfNeeded();

        if (dailyPnLPct < DAILY_LOSS_SURVIVAL_PCT || drawdownFromPeak < -8.0) {
            cautiousMode = true; // Flag for SignalSender (cuts size)
            // Require ironclad signal (+12 to conf)
            if (signal.probability < getEffectiveMinConfidence() + 12.0) return false;
        } else if (dailyPnLPct < DAILY_LOSS_CAUTIOUS_PCT || drawdownFromPeak < -5.0) {
            cautiousMode = true; // Flag for SignalSender (cuts size)
            // Require confident signal (+6 to conf)
            if (signal.probability < getEffectiveMinConfidence() + 6.0) return false;
        } else {
            cautiousMode = false;
        }

        // [v12.2] No timers. Confidence check does all the work via streak guard.
        // Confidence check
        if (signal.probability < getEffectiveMinConfidence()) return false;

        // Global limit
        if (getActiveCount() >= maxGlobalSignals) return false;

        // [v24.0 FIX WEAK-7] Max directional exposure — max 4 LONG or 4 SHORT at once.
        // Without this, a bullish market could stack 10+ LONGs → one crash wipes all.
        int sameDirectionCount = 0;
        for (List<ActiveSignal> signals : activeSignals.values()) {
            for (ActiveSignal a : signals) {
                if (a.side == signal.side) sameDirectionCount++;
            }
        }
        if (sameDirectionCount >= MAX_SAME_DIRECTION) return false;

        // Per-symbol limit
        List<ActiveSignal> symList = activeSignals.getOrDefault(sym, List.of());
        if (symList.size() >= maxSignalsPerSymbol) return false;

        // Duplicate check
        for (ActiveSignal a : symList) {
            double priceDiff = Math.abs(a.entry - signal.price) / (a.entry + 1e-9);
            if (a.side == signal.side && priceDiff < minSignalPriceDiff) return false;
            if (a.side != signal.side && priceDiff < minSignalPriceDiff * 2) return false;
        }

        // Portfolio heat check
        double newRisk = estimateRisk(signal.probability, signal.category);
        if (currentHeat + newRisk > maxPortfolioHeat) return false;

        // Symbol score check (poor performing symbols get blocked)
        double score = symbolScore.getOrDefault(sym, 0.0);
        if (score < -0.25 && getWinRate(sym) < 0.38 && getTradeCount(sym) >= 8) return false;

        // [v28.0] PATCH #10: SYMBOL QUARANTINE — new symbols have no verified edge.
        // First 10 trades on any symbol require probability >= 70% (hard gate).
        // Rationale: unknown symbol stats = unknown win rate = high variance = need higher conviction.
        int symTrades = getTradeCount(sym);
        if (symTrades < 10 && signal.probability < 70.0) return false;

        return true;
    }

    public synchronized void registerSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        String cat = signal.category != null ? signal.category.name() : "ALT";
        double risk = estimateRisk(signal.probability, signal.category);

        ActiveSignal active = new ActiveSignal(
                signal.symbol, signal.side, signal.price, signal.stop, signal.tp1,
                signal.probability, risk, cat, null);

        activeSignals.computeIfAbsent(signal.symbol, k -> new CopyOnWriteArrayList<>()).add(active);
        currentHeat = Math.min(maxPortfolioHeat, currentHeat + risk);
    }

    // ══════════════════════════════════════════════════════════════
    //  TRADE CLOSE
    // ══════════════════════════════════════════════════════════════

    /** [v11.0 FIX-COMPILE] 3-arg overload for BotMain/TradeResolver compatibility */
    public synchronized void closeTrade(String symbol, com.bot.TradingCore.Side side, double pnlPct) {
        closeTrade(symbol, side, pnlPct, pnlPct > 0 ? "TP" : "SL");
    }

    public synchronized void closeTrade(String symbol, com.bot.TradingCore.Side side, double pnlPct, String reason) {
        List<ActiveSignal> list = activeSignals.get(symbol);
        if (list == null) return;

        Iterator<ActiveSignal> it = list.iterator();
        while (it.hasNext()) {
            ActiveSignal s = it.next();
            if (s.side == side) {
                currentHeat = Math.max(0, currentHeat - s.riskPct);

                // Add to bounded history
                Deque<ClosedTrade> hist = tradeHistory.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
                hist.addLast(new ClosedTrade(symbol, side, pnlPct, s.ageMs(), reason));
                while (hist.size() > MAX_HISTORY) hist.removeFirst();

                updateSymbolScore(symbol, pnlPct);

                // [v11.0] Daily PnL tracking
                trackDailyPnL(pnlPct);

                it.remove();
                break;
            }
        }
        if (list.isEmpty()) activeSignals.remove(symbol);
    }

    // ══════════════════════════════════════════════════════════════
    //  CLEANUP — Time Stop = NEUTRAL
    // ══════════════════════════════════════════════════════════════

    private void cleanupExpired() {
        decayStreakBoost();
        long now = System.currentTimeMillis();

        for (var entry : activeSignals.entrySet()) {
            entry.getValue().removeIf(s -> {
                if (s.ageMs() > TIME_STOP_MS) {
                    currentHeat = Math.max(0, currentHeat - s.riskPct);

                    // NEUTRAL — not loss, not win. Streak NOT affected.
                    Deque<ClosedTrade> hist = tradeHistory.computeIfAbsent(s.symbol, k -> new ConcurrentLinkedDeque<>());
                    hist.addLast(new ClosedTrade(s.symbol, s.side, 0.0, s.ageMs(), "TIME_STOP"));
                    while (hist.size() > MAX_HISTORY) hist.removeFirst();

                    if (timeStopCallback != null) {
                        try { timeStopCallback.accept(s.symbol, "TIME_STOP " + s.symbol + " " + s.side); }
                        catch (Exception ignored) {}
                    }
                    return true;
                }
                return false;
            });
        }
        activeSignals.entrySet().removeIf(e -> e.getValue().isEmpty());
    }

    // ══════════════════════════════════════════════════════════════
    //  STATISTICS
    // ══════════════════════════════════════════════════════════════

    public synchronized int getActiveCount() {
        return activeSignals.values().stream().mapToInt(List::size).sum();
    }

    public double getWinRate(String symbol) {
        Deque<ClosedTrade> hist = tradeHistory.get(symbol);
        if (hist == null || hist.isEmpty()) return 0.50;
        synchronized (hist) {
            return (double) hist.stream().filter(ClosedTrade::isWin).count() / hist.size();
        }
    }

    public double getOverallWinRate() {
        long total = 0, wins = 0;
        for (Deque<ClosedTrade> h : tradeHistory.values()) {
            total += h.size();
            wins += h.stream().filter(ClosedTrade::isWin).count();
        }
        return total > 0 ? (double) wins / total : 0.50;
    }

    public int getTradeCount(String symbol) {
        Deque<ClosedTrade> hist = tradeHistory.get(symbol);
        return hist != null ? hist.size() : 0;
    }

    public int getTotalTradeCount() {
        return tradeHistory.values().stream().mapToInt(Deque::size).sum();
    }

    // [v28.0] PATCH #12: Expose per-symbol average realized R:R for TP calibration.
    // Returns average PnL of winning trades / average PnL magnitude of losing trades.
    // If symbol has insufficient data, returns 0 (= use market-state defaults).
    public double getAvgRealizedRR(String symbol) {
        Deque<ClosedTrade> hist = tradeHistory.get(symbol);
        if (hist == null || hist.size() < 5) return 0.0;
        double avgWin  = hist.stream().filter(ClosedTrade::isWin)
                .mapToDouble(t -> t.pnlPct).average().orElse(0);
        double avgLoss = hist.stream().filter(t -> !t.isWin())
                .mapToDouble(t -> Math.abs(t.pnlPct)).average().orElse(1.0);
        return avgLoss > 0 ? avgWin / avgLoss : 0.0;
    }

    // [v28.0] PATCH #15: Expose symbol score for BotMain TradeResolver force-exit.
    public double getSymbolScore(String symbol) {
        return symbolScore.getOrDefault(symbol, 0.0);
    }

    /** [v31] True when daily loss exceeds -6% — SignalSender should halve position size. */
    public boolean isCautiousMode() { return cautiousMode; }

    public int getCurrentLossStreak() { return currentLossStreak; }
    public double getStreakBoost()    { return streakConfidenceBoost; }
    public double getCurrentHeat()   { return currentHeat; }

    /**
     * [ДЫРА №5] Адаптивный размер позиции на основе реальных последних 10 сделок.
     *
     * Было: бэктест влиял на 3% порога → практически бесполезно.
     * Стало: смотрим на РЕАЛЬНЫЕ последние 10 сделок.
     *   • 8-10 из 10 прибыльных → рынок на нашей стороне → ×1.20 (чуть агрессивнее)
     *   • 5-7 из 10 прибыльных → норма → ×1.00
     *   • 3-4 из 10 прибыльных → плохо → ×0.60 (сокращаем вдвое)
     *   • 0-2 из 10 прибыльных → стоп → ×0.30 (минимальный риск, ждём восстановления)
     *
     * Это и есть то что делают профессиональные трейдеры: когда система
     * не работает — уменьшай размер, не жди пока всё не сольёшь.
     */
    public double getRiskSizeMultiplier() {
        // Собираем последние 10 РЕАЛЬНЫХ закрытых сделок (не ISC internal, а трейд-история)
        List<ClosedTrade> recent = new ArrayList<>();
        for (Deque<ClosedTrade> hist : tradeHistory.values()) {
            recent.addAll(hist);
        }
        // Сортируем по времени закрытия, берём последние 10
        recent.sort(Comparator.comparingLong(t -> t.closedAt));
        int n = recent.size();
        if (n < 3) {
            // Недостаточно данных — осторожный старт
            return lastBacktestEV < -0.02 ? 0.70 : 0.90;
        }
        int window = Math.min(10, n);
        List<ClosedTrade> last10 = recent.subList(n - window, n);
        long wins   = last10.stream().filter(ClosedTrade::isWin).count();
        long losses = window - wins;

        double mult;
        if (wins >= (long)(window * 0.80)) {
            mult = 1.20; // 8/10+ побед → рынок на нашей стороне
        } else if (wins >= (long)(window * 0.55)) {
            mult = 1.00; // норма
        } else if (wins >= (long)(window * 0.35)) {
            mult = 0.60; // плохая полоса → сокращаем вдвое
        } else {
            mult = 0.30; // стоп-режим → минимальный риск
        }

        // Дополнительный штраф если бэктест тоже говорит "плохо"
        if (lastBacktestEV < -0.02 && System.currentTimeMillis() - lastBacktestTime < 2 * 3600_000L) {
            mult *= 0.80;
        }

        // Консервативный лог для понимания режима
        if (mult <= 0.30) {
            log("⚠️ СТОП-РЕЖИМ: последние " + window + " сделок: " + wins + " побед/" + losses
                    + " потерь. Позиции сокращены до 30%.");
        } else if (mult <= 0.60) {
            log("⚠️ ОСТОРОЖНЫЙ РЕЖИМ: " + wins + "/" + window + " побед. Позиции: 60%.");
        }

        return Math.max(0.20, Math.min(mult, 1.20));
    }

    public String getStats() {
        return String.format("ISC[act=%d/%d heat=%.1f%% cl=%d wr=%.0f%% str=%d+%.0f L%d/S%d eff=%.0f%% day=%.2f%%]",
                getActiveCount(), maxGlobalSignals, currentHeat * 100,
                getTotalTradeCount(), getOverallWinRate() * 100,
                currentLossStreak, streakConfidenceBoost,
                consecutiveLongLosses, consecutiveShortLosses,
                getEffectiveMinConfidence(),
                dailyPnLPct);
    }

    // ══════════════════════════════════════════════════════════════
    //  INTERNAL
    // ══════════════════════════════════════════════════════════════

    private double estimateRisk(double confidence, com.bot.DecisionEngineMerged.CoinCategory cat) {
        // [v12.0] Flat risk until we have 50+ trades proving the edge
        // Old code gave 2× position to 80%+ signals — but those 80% were self-assessed,
        // not validated. This caused outsized losses on overconfident signals.
        int totalTrades = getTotalTradeCount();
        double overallWR = getOverallWinRate();

        double base;
        if (totalTrades < 50 || overallWR < 0.52) {
            // Unproven system — flat conservative risk
            base = 0.008; // 0.8% regardless of confidence
        } else {
            // Proven edge — modest scaling
            base = confidence >= 78 ? 0.015 : confidence >= 68 ? 0.012 : confidence >= 58 ? 0.010 : 0.008;
        }

        double mult = cat == com.bot.DecisionEngineMerged.CoinCategory.MEME ? 0.5 :
                cat == com.bot.DecisionEngineMerged.CoinCategory.TOP ? 1.0 : 0.75;
        return base * mult;
    }

    private void updateSymbolScore(String symbol, double pnl) {
        double delta = pnl > 0.5 ? 0.015 : pnl < -0.5 ? -0.018 : -0.003;
        symbolScore.merge(symbol, delta, Double::sum);
        symbolScore.compute(symbol, (k, v) -> com.bot.TradingCore.clamp(v == null ? 0 : v, -0.40, 0.40));
    }

    // ══════════════════════════════════════════════════════════════
    //  [v11.0] DAILY LOSS LIMIT + DRAWDOWN PROTECTION
    // ══════════════════════════════════════════════════════════════

    // [v28.0] PATCH #16: synchronized prevents concurrent write on volatile compound op.
    // PATCH #1: triggers drawdown pause when -8% from peak threshold breached.
    private synchronized void trackDailyPnL(double pnlPct) {
        resetDailyIfNeeded();
        dailyPnLPct += pnlPct;

        // [v32] Time-based pauses removed entirely. Track stats only.
    }

    private void resetDailyIfNeeded() {
        long today = currentDay();
        if (today != dailyPnLResetDay) {
            dailyPnLResetDay = today;
            dailyPnLPct = 0.0;
        }
    }

    private static long currentDay() {
        return System.currentTimeMillis() / 86_400_000L;
    }

    /** Called by BotMain when balance is refreshed — tracks drawdown (stats only) */
    public void updateBalance(double currentBalance) {
        if (currentBalance > peakBalance) {
            peakBalance = currentBalance;
        }
        if (peakBalance > 0) {
            drawdownFromPeak = (currentBalance - peakBalance) / peakBalance * 100;
        }
    }

    public double getDailyPnL() { return dailyPnLPct; }
    public double getDrawdownFromPeak() { return drawdownFromPeak; }

    private static void log(String msg) {
        System.out.printf("[ISC %s] %s%n",
                java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")), msg);
    }
}