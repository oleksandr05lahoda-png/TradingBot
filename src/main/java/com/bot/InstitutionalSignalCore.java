package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * InstitutionalSignalCore v18.0 — PORTFOLIO RISK CONTROLLER
 *
 * Назначение: risk-governor + cooldown manager.
 * НЕ quality-filter (это делает DecisionEngine + Dispatcher).
 *
 * v18.0 changes:
 *  - Smoothed thresholds for flat-market signal generation
 *  - Removed dead version-history comments (kept WHY-rationale)
 *  - Tightened daily-loss → size-only penalty (no signal blocks)
 *  - Synced with DE MIN_CONF_FLOOR=50, Dispatcher floor=53
 */
public final class InstitutionalSignalCore {

    // [v71] MAX_EFFECTIVE_MIN_CONF 60→56. Синхронизация с DE.MIN_CONF_FLOOR=52
    // и SignalSender.MIN_CONF=53. Старый потолок 60 не давал ISC снизить порог
    // достаточно даже при отличном track record — daily penalty +2 двигал base
    // выше потолка и порог застревал на 60 (выше чем фильтр в SignalSender).
    private static final double MAX_EFFECTIVE_MIN_CONF = 56.0;

    // ── Configuration ────────────────────────────────────────────
    private final int    maxGlobalSignals;
    private final int    maxSignalsPerSymbol;
    private final double maxPortfolioHeat;
    private final double baseMinConfidence;
    private final double minSignalPriceDiff;
    private final int    maxSameSectorSameDir;

    // Time stop
    private static final int  TIME_STOP_BARS  = 6;    // 6 × 15m = 90 min
    private static final long TIME_STOP_MS    = TIME_STOP_BARS * 15 * 60_000L;
    private static final int  MAX_HISTORY     = 100;

    public InstitutionalSignalCore() {
        // [v71] baseMinConfidence 53→50. Синхронизация с DE.MIN_CONF_FLOOR=52
        // (margin -2: ISC умышленно слегка ниже DE чтобы не дублировать фильтр).
        // Реальный quality control делается в Dispatcher cold-start gate (53/57).
        this(
                envInt("ISC_MAX_GLOBAL_SIGNALS", 6),
                envInt("ISC_MAX_SIGNALS_PER_SYMBOL", 1),
                envDouble("ISC_MAX_PORTFOLIO_HEAT", 0.06),
                envDouble("ISC_BASE_MIN_CONF", 50.0),
                envDouble("ISC_MIN_SIGNAL_PRICE_DIFF", 0.003),
                envInt("ISC_MAX_SAME_SECTOR_DIR", 2)
        );
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

    //  SIGNAL STATE MANAGER — Anti-bipolar + SL Cooldown
    //
    //  PROBLEM A — Bipolar trades: bot was sending LONG and then SHORT
    //  on the same symbol while the first trade was still open.
    //  Result: hedged position = pure fee loss, no directional edge.
    //
    //  PROBLEM B — Falling knife re-entries: after SL hit, next signal
    //  on the same symbol could fire within the next 1-minute cycle,
    //  catching the continuation of the same dump.
    //
    //  SOLUTION:
    //    · activeSymbols tracks which symbols currently have ANY open trade.
    //    · Any new signal on an active symbol → HARD DROP.
    //    · After SL exit: 30-min cooldown. After TP exit: 5-min cooldown.

    /** Symbols with an open tracked trade (any direction). Symbol → entry timestamp. */
    private final Map<String, Long> activeSymbols = new ConcurrentHashMap<>();

    /** Post-exit cooldowns. Symbol → earliest time a new signal is allowed (epoch ms). */
    private final Map<String, Long> symbolCooldownUntil = new ConcurrentHashMap<>();

    // Global signal rate limit — defends against signal floods.
    // [v18] 12 → 16 per 2h. Allows more signals when market is active.
    private final ConcurrentLinkedDeque<Long> globalSignalTimestamps = new ConcurrentLinkedDeque<>();
    private static final int  MAX_GLOBAL_SIGNALS_2H = envInt("ISC_MAX_GLOBAL_2H", 16);
    private static final long GLOBAL_WINDOW_2H_MS   = 2L * 60 * 60_000L;

    // Daily kill-switch threshold. At -5% daily PnL, block ALL new signals
    // until next UTC day. resetDailyIfNeeded() will clear dailyPnLPct automatically.
    private static final double DAILY_KILL_SWITCH_PCT = -0.05;

    // DAILY SIGNAL LIMIT PER SYMBOL — prevents "milking" one volatile coin all day.
    // [v18] 4 → 5 per 8h (slightly more headroom in trending day).
    private static final int    MAX_SIGNALS_PER_SYMBOL_8H = 5;
    private static final long   SIGNAL_WINDOW_8H_MS       = 8 * 60 * 60_000L;
    private final Map<String, Deque<Long>> symbolSignalTimestamps = new ConcurrentHashMap<>();

    private void pruneOldSignalTimestamps(Deque<Long> timestamps) {
        long cutoff = System.currentTimeMillis() - SIGNAL_WINDOW_8H_MS;
        while (!timestamps.isEmpty() && timestamps.peekFirst() < cutoff) {
            timestamps.pollFirst();
        }
    }

    private boolean symbolDailyLimitReached(String symbol) {
        Deque<Long> timestamps = symbolSignalTimestamps.get(symbol);
        if (timestamps == null) return false;
        pruneOldSignalTimestamps(timestamps);
        return timestamps.size() >= MAX_SIGNALS_PER_SYMBOL_8H;
    }

    private void recordSymbolSignal(String symbol) {
        Deque<Long> timestamps = symbolSignalTimestamps.computeIfAbsent(symbol,
                k -> new ConcurrentLinkedDeque<>());
        timestamps.addLast(System.currentTimeMillis());
        pruneOldSignalTimestamps(timestamps);
    }

    /** SL cooldown — 30 min, blocks falling-knife re-entry. */
    private static final long SL_COOLDOWN_MS  = 30 * 60_000L;
    /** TP cooldown — 20 min, synced with BotMain.setSignalCooldown(20min). */
    private static final long TP_COOLDOWN_MS  = 20 * 60_000L;
    /** TIME_STOP cooldown — 45 min. Pair that didn't move in 90 min needs more time off. */
    private static final long TIME_STOP_COOLDOWN_MS = 45 * 60_000L;

    //  TIME-STOP CHAIN GUARD
    //  After 2 TS in a row pauses (symbol, side) for 4h.
    //  After 3 TS pauses entire symbol for 8h.
    //  Any decisive close (TP / SL) resets the chain.
    private static final int  TS_CHAIN_TIER1_THRESHOLD = 2;
    private static final int  TS_CHAIN_TIER2_THRESHOLD = 3;
    private static final long TS_CHAIN_TIER1_PAUSE_MS  = 4L * 60 * 60_000L;
    private static final long TS_CHAIN_TIER2_PAUSE_MS  = 8L * 60 * 60_000L;
    private final Map<String, java.util.concurrent.atomic.AtomicInteger> tsChainCounters
            = new ConcurrentHashMap<>();
    private final Map<String, Long> tsChainSidePause   = new ConcurrentHashMap<>();
    private final Map<String, Long> tsChainSymbolPause = new ConcurrentHashMap<>();

    public void markSymbolActive(String symbol) {
        activeSymbols.put(symbol, System.currentTimeMillis());
    }

    public void markSymbolClosed(String symbol, boolean wasSL) {
        activeSymbols.remove(symbol);
        long cooldown = wasSL ? SL_COOLDOWN_MS : TP_COOLDOWN_MS;
        symbolCooldownUntil.put(symbol, System.currentTimeMillis() + cooldown);
        log((wasSL ? "SL_COOLDOWN" : "TP_COOLDOWN") + " " + symbol
                + " for " + (cooldown / 60_000) + "min");
    }

    /**
     * Central gate: is a new signal for this symbol allowed RIGHT NOW?
     * Returns false if symbol has any active trade OR is within post-SL cooldown.
     */
    public boolean isSymbolAvailable(String symbol) {
        if (activeSymbols.containsKey(symbol)) return false;
        Long until = symbolCooldownUntil.get(symbol);
        if (until != null && System.currentTimeMillis() < until) return false;
        return true;
    }

    //  CHAIN-GUARD API

    private static String chainKey(String symbol, com.bot.TradingCore.Side side) {
        return symbol + "|" + side.name();
    }

    public int recordTimeStopChain(String symbol, com.bot.TradingCore.Side side) {
        if (symbol == null || side == null) return 0;
        String k = chainKey(symbol, side);
        int n = tsChainCounters.computeIfAbsent(k,
                x -> new java.util.concurrent.atomic.AtomicInteger(0)).incrementAndGet();
        long now = System.currentTimeMillis();
        if (n >= TS_CHAIN_TIER2_THRESHOLD) {
            tsChainSymbolPause.put(symbol, now + TS_CHAIN_TIER2_PAUSE_MS);
            tsChainCounters.get(k).set(0);
        } else if (n >= TS_CHAIN_TIER1_THRESHOLD) {
            tsChainSidePause.put(k, now + TS_CHAIN_TIER1_PAUSE_MS);
        }
        return n;
    }

    public void resetTimeStopChain(String symbol, com.bot.TradingCore.Side side) {
        if (symbol == null || side == null) return;
        var c = tsChainCounters.get(chainKey(symbol, side));
        if (c != null) c.set(0);
    }

    public boolean isPausedByChain(String symbol, com.bot.TradingCore.Side side) {
        if (symbol == null || side == null) return false;
        long now = System.currentTimeMillis();
        Long sym = tsChainSymbolPause.get(symbol);
        if (sym != null) {
            if (now < sym) return true;
            tsChainSymbolPause.remove(symbol);
        }
        Long sd = tsChainSidePause.get(chainKey(symbol, side));
        if (sd != null) {
            if (now < sd) return true;
            tsChainSidePause.remove(chainKey(symbol, side));
        }
        return false;
    }

    public long chainPauseMinutesLeft(String symbol, com.bot.TradingCore.Side side) {
        if (!isPausedByChain(symbol, side)) return 0;
        long now = System.currentTimeMillis();
        long sym = tsChainSymbolPause.getOrDefault(symbol, 0L);
        long sd  = tsChainSidePause.getOrDefault(chainKey(symbol, side), 0L);
        return Math.max(0, (Math.max(sym, sd) - now) / 60_000L);
    }

    /**
     * Returns the number of symbols currently in cooldown or active.
     * Used by Watchdog to distinguish a genuine signal drought from healthy
     * post-signal cooldown.
     */
    public int getCooldownedSymbolCount() {
        long now = System.currentTimeMillis();
        int count = activeSymbols.size();
        for (Map.Entry<String, Long> e : symbolCooldownUntil.entrySet()) {
            if (e.getValue() > now && !activeSymbols.containsKey(e.getKey())) {
                count++;
            }
        }
        return count;
    }

    //  DRAWDOWN MANAGER — Soft Circuit Breaker
    //
    //  Track consecutive confirmed SL exits (not chandelier/trail).
    //  3+ consecutive SLs → REDUCED_RISK mode (size ×0.5).
    //  5+ consecutive SLs → DEEP_REDUCED_RISK mode (size ×0.25).
    //  Any TP or Chandelier profit → reset counter, exit REDUCED_RISK.

    private final java.util.concurrent.atomic.AtomicInteger consecutiveSlCount
            = new java.util.concurrent.atomic.AtomicInteger(0);
    private static final int REDUCED_RISK_THRESHOLD      = 3;
    private static final int DEEP_REDUCED_RISK_THRESHOLD = 5;

    public void recordConsecutiveSL() {
        int c = consecutiveSlCount.incrementAndGet();
        if (c >= DEEP_REDUCED_RISK_THRESHOLD) {
            log("🔴 DEEP_REDUCED_RISK: " + c + " consecutive SLs — size ×0.25");
        } else if (c >= REDUCED_RISK_THRESHOLD) {
            log("🟡 REDUCED_RISK: " + c + " consecutive SLs — size ×0.5");
        }
    }

    public void resetConsecutiveSL() {
        int prev = consecutiveSlCount.getAndSet(0);
        if (prev >= REDUCED_RISK_THRESHOLD) {
            log("✅ REDUCED_RISK reset after profitable exit (was " + prev + " SLs)");
        }
    }

    public double getReducedRiskMultiplier() {
        int c = consecutiveSlCount.get();
        if (c >= DEEP_REDUCED_RISK_THRESHOLD) return 0.25;
        if (c >= REDUCED_RISK_THRESHOLD)      return 0.50;
        return 1.0;
    }

    public String getReducedRiskFlag() {
        int c = consecutiveSlCount.get();
        if (c >= DEEP_REDUCED_RISK_THRESHOLD) return "🔴DEEP_REDUCED_RISK";
        if (c >= REDUCED_RISK_THRESHOLD)      return "🟡REDUCED_RISK";
        return "";
    }

    // Streak guard
    private final Deque<Boolean> globalResultStreak = new ConcurrentLinkedDeque<>();
    private static final int STREAK_WINDOW = 10;
    private volatile int    currentLossStreak     = 0;
    private volatile double streakConfidenceBoost = 0.0;
    private volatile long   lastStreakUpdateMs    = System.currentTimeMillis();

    // Daily loss → POSITION SIZE ONLY, never blocks signals (prevents 20h droughts).
    private static final double DAILY_LOSS_CAUTIOUS_PCT  = -3.0; // size ×0.5
    private static final double DAILY_LOSS_SURVIVAL_PCT  = -6.0; // size ×0.25 (NO BLOCK)
    private volatile boolean    cautiousMode          = false;

    private volatile double dailyPnLPct        = 0.0;
    private volatile long   dailyPnLResetDay   = currentDay();

    // Drawdown tracking (stats only)
    private volatile double peakBalance          = 0.0;
    private volatile double drawdownFromPeak     = 0.0;

    private volatile int currentWinStreak = 0;

    // Direction-specific loss tracking
    private volatile int consecutiveLongLosses = 0;
    private volatile int consecutiveShortLosses = 0;
    private static final int DIR_LOSS_PENALTY_THRESHOLD = 3;

    // Backtest integration
    private volatile double lastBacktestEV   = 0.0;
    private volatile long   lastBacktestTime = 0;

    // Per-symbol OOS EV tracking (separates BTC EV from DOGE EV).
    private final ConcurrentHashMap<String, Double> symbolOosEV = new ConcurrentHashMap<>();

    // Callbacks
    private volatile java.util.function.BiConsumer<String, String> timeStopCallback = null;
    public void setTimeStopCallback(java.util.function.BiConsumer<String, String> cb) {
        this.timeStopCallback = cb;
    }

    //  MODELS

    public static final class ActiveSignal {
        public final String symbol;
        public final com.bot.TradingCore.Side side;
        public final double entry, sl, tp1, probability;
        public final double riskPct;
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

    //  STREAK GUARD — confirmed results only

    public void registerConfirmedResult(boolean win) {
        registerConfirmedResult(win, null);
    }

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
            if (side == com.bot.TradingCore.Side.LONG)  consecutiveLongLosses  = 0;
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
        log("STREAK RESET (was " + currentLossStreak
                + " L:" + consecutiveLongLosses + " S:" + consecutiveShortLosses + ")");
        currentLossStreak = 0;
        streakConfidenceBoost = 0.0;
        consecutiveLongLosses = 0;
        consecutiveShortLosses = 0;
        lastStreakUpdateMs = System.currentTimeMillis();
    }

    private void decayStreakBoost() {
        if (streakConfidenceBoost <= 0) return;
        long elapsed = System.currentTimeMillis() - lastStreakUpdateMs;
        // Decay 30% every 8min — after 3 losses (boost=4.5) decays to zero in ~24min
        if (elapsed > 8 * 60_000L) {
            streakConfidenceBoost *= 0.70;
            lastStreakUpdateMs = System.currentTimeMillis();
            if (streakConfidenceBoost < 0.3) {
                streakConfidenceBoost = 0;
                currentLossStreak = 0;
            }
        }
    }

    public double getEffectiveMinConfidence() {
        resetDailyIfNeeded();

        // [v71] floor 50→47, milestones 46→44 / 48→45.
        // Синхронизация с DE.MIN_CONF_FLOOR=52: ISC должен ВСЕГДА быть ниже DE
        // на 3-5pt margin, иначе становится дублирующим фильтром. Реальная
        // защита качества — Dispatcher cold-start gate (53/57) и калибратор.
        double floor = 47.0;
        int totalTrades = getTotalTradeCount();
        double overallWr = getOverallWinRate();
        if (totalTrades >= 500 && overallWr >= 0.58) floor = 44.0;
        else if (totalTrades >= 200 && overallWr >= 0.55) floor = 45.0;

        double base = Math.max(baseMinConfidence, floor);

        // Daily loss penalties — soft (size already cut elsewhere)
        if (dailyPnLPct <= DAILY_LOSS_SURVIVAL_PCT) {
            base += 2.0;
        } else if (dailyPnLPct <= DAILY_LOSS_CAUTIOUS_PCT) {
            base += 1.0;
        }

        if (lastBacktestEV < -0.02 && System.currentTimeMillis() - lastBacktestTime < 2 * 3600_000L)
            base += 2.0;

        return Math.max(floor, Math.min(base, MAX_EFFECTIVE_MIN_CONF));
    }

    public void setBacktestResult(double ev, long ts) {
        this.lastBacktestEV = ev;
        this.lastBacktestTime = ts;
    }

    public void setSymbolBacktestResult(String symbol, double ev) {
        symbolOosEV.put(symbol, ev);
    }

    //  SIGNAL FILTERING

    // Max positions per direction (signal-mode only — we don't auto-trade).
    private static final int MAX_SAME_DIRECTION = envInt("ISC_MAX_SAME_DIRECTION", 10);

    // Rate-limit BIPOLAR BLOCK logs: one log per symbol per 60s to avoid Railway log flood.
    private final Map<String, Long> bipolarLogThrottle = new ConcurrentHashMap<>();
    private static final long BIPOLAR_LOG_THROTTLE_MS = 60_000L;

    /** Periodic cleanup — call from BotMain every cycle. */
    public synchronized void periodicCleanup() {
        cleanupExpired();
    }

    public synchronized boolean allowSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        cleanupExpired();

        // Daily kill-switch: hard stop at -5% daily DD until next UTC day.
        if (dailyPnLPct <= DAILY_KILL_SWITCH_PCT) {
            long now = System.currentTimeMillis();
            Long lastLog = bipolarLogThrottle.get("__daily_kill__");
            if (lastLog == null || (now - lastLog) > BIPOLAR_LOG_THROTTLE_MS) {
                bipolarLogThrottle.put("__daily_kill__", now);
                log(String.format("🛑 DAILY_KILL_SWITCH: DD=%.2f%% → no new signals until next UTC day",
                        dailyPnLPct * 100));
            }
            return false;
        }

        // Global rate limit: max MAX_GLOBAL_SIGNALS_2H signals in rolling 2h.
        long nowMs = System.currentTimeMillis();
        while (!globalSignalTimestamps.isEmpty()
                && nowMs - globalSignalTimestamps.peekFirst() > GLOBAL_WINDOW_2H_MS) {
            globalSignalTimestamps.pollFirst();
        }
        if (globalSignalTimestamps.size() >= MAX_GLOBAL_SIGNALS_2H) {
            Long lastLog = bipolarLogThrottle.get("__global_rate__");
            if (lastLog == null || (nowMs - lastLog) > BIPOLAR_LOG_THROTTLE_MS) {
                bipolarLogThrottle.put("__global_rate__", nowMs);
                log("⚠ GLOBAL_RATE_LIMIT reached: " + globalSignalTimestamps.size()
                        + "/" + MAX_GLOBAL_SIGNALS_2H + " signals in last 2h — auto-throttle");
            }
            return false;
        }

        //  [SCANNER MODE] allowSignal() — minimal gate for manual-trading mode.
        //  REMOVED (was blocking real signals when bot is signal-only):
        //    · Portfolio heat cap, Global signal limit, Same-direction limit,
        //    · Directional loss guard, Daily signal limit per symbol.
        //  These were checking VIRTUAL portfolio state — meaningless without auto-execution.
        //
        //  KEPT (signal quality protection):
        //    · Bipolar guard — no LONG→SHORT on same symbol while open
        //    · Cooldown guard — 20-min cooldown after each signal

        if (!isSymbolAvailable(signal.symbol)) {
            Long until = symbolCooldownUntil.get(signal.symbol);
            long now2 = System.currentTimeMillis();
            Long lastLog = bipolarLogThrottle.get(signal.symbol);
            if (lastLog == null || (now2 - lastLog) > BIPOLAR_LOG_THROTTLE_MS) {
                bipolarLogThrottle.put(signal.symbol, now2);
                if (activeSymbols.containsKey(signal.symbol)) {
                    log("🚫 BIPOLAR BLOCK: " + signal.symbol + " already active");
                } else if (until != null) {
                    long remainSec = (until - now2) / 1000;
                    log("⏳ COOLDOWN BLOCK: " + signal.symbol + " cooldown " + remainSec + "s left");
                }
            }
            return false;
        }

        cautiousMode = dailyPnLPct <= DAILY_LOSS_CAUTIOUS_PCT;

        return true;
    }

    /**
     * Sets cooldown on a symbol after a signal is sent.
     * Replaces markSymbolActive() + (wait for TradeResolver) → markSymbolClosed() chain.
     */
    public synchronized void setSignalCooldown(String symbol, long durationMs) {
        activeSymbols.remove(symbol);
        long until = System.currentTimeMillis() + durationMs;
        symbolCooldownUntil.put(symbol, until);
        log("⏱ SIGNAL_COOLDOWN: " + symbol + " → " + (durationMs / 60_000) + " мин");
    }

    public synchronized void registerSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        double estRisk = estimateRisk(signal.probability, signal.category);
        // Per-signal factor logging — record which indicators voted for this signal
        String factorLog = "";
        if (signal.forecast != null && signal.forecast.factorScores != null) {
            StringBuilder fl = new StringBuilder();
            for (var e : signal.forecast.factorScores.entrySet()) {
                if (Math.abs(e.getValue()) > 0.08) {
                    if (fl.length() > 0) fl.append(",");
                    fl.append(e.getKey()).append("=").append(String.format("%.2f", e.getValue()));
                }
            }
            factorLog = fl.toString();
        }
        ActiveSignal act = new ActiveSignal(signal.symbol, signal.side, signal.price, signal.stop,
                signal.tp1, signal.probability, estRisk, signal.category.name(), factorLog);

        activeSignals.computeIfAbsent(signal.symbol, k -> new CopyOnWriteArrayList<>()).add(act);
        currentHeat += estRisk;
        markSymbolActive(signal.symbol);
        recordSymbolSignal(signal.symbol);
        globalSignalTimestamps.addLast(System.currentTimeMillis());

        if (!factorLog.isEmpty()) {
            log(String.format("[SIGNAL] %s %s prob=%.0f%% factors=[%s]",
                    signal.symbol, signal.side, signal.probability, factorLog));
        }
    }

    /**
     * Undo a registerSignal() call — used by R:R gate in SignalSender when a trade
     * passes allowSignal() but fails the hard R:R check after adaptive TP calibration.
     */
    public synchronized void unregisterSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        List<ActiveSignal> list = activeSignals.get(signal.symbol);
        if (list == null) return;
        list.removeIf(s -> s.side == signal.side &&
                Math.abs(s.entry - signal.price) < signal.price * 0.001);
        if (list.isEmpty()) activeSignals.remove(signal.symbol);
        double estRisk = estimateRisk(signal.probability, signal.category);
        currentHeat = Math.max(0, currentHeat - estRisk);
        activeSymbols.remove(signal.symbol);
        log("[UNREGISTER] " + signal.symbol + " " + signal.side + " — R:R gate rollback");
    }

    //  TRADE CLOSE

    public synchronized void closeTrade(String symbol, com.bot.TradingCore.Side side, double pnlPct) {
        closeTrade(symbol, side, pnlPct, pnlPct > 0 ? "TP" : "SL");
    }

    public synchronized void closeTrade(String symbol, com.bot.TradingCore.Side side,
                                        double pnlPct, String reason) {
        List<ActiveSignal> list = activeSignals.get(symbol);
        if (list == null) return;

        ActiveSignal matched = null;
        for (ActiveSignal s : list) {
            if (s.side == side) {
                matched = s;
                break;
            }
        }
        if (matched != null) {
            currentHeat = Math.max(0, currentHeat - matched.riskPct);

            String outcome = pnlPct > 0.05 ? "WIN" : pnlPct < -0.05 ? "LOSS" : "FLAT";
            if (!matched.sector.isEmpty()) {
                log(String.format("[P&L_ATTR] %s %s %s pnl=%+.2f%% dur=%dm factors=[%s]",
                        symbol, side, outcome, pnlPct, matched.ageMs() / 60_000, matched.sector));
            }

            Deque<ClosedTrade> hist = tradeHistory.computeIfAbsent(symbol,
                    k -> new ConcurrentLinkedDeque<>());
            hist.addLast(new ClosedTrade(symbol, side, pnlPct, matched.ageMs(), reason));
            while (hist.size() > MAX_HISTORY) hist.removeFirst();

            updateSymbolScore(symbol, pnlPct);
            trackDailyPnL(pnlPct);

            list.remove(matched);
        }
        if (list.isEmpty()) activeSignals.remove(symbol);

        boolean wasSL = "SL".equals(reason) || "HIT_SL".equals(reason) || "SCORE_EXIT".equals(reason);
        boolean wasProfit = pnlPct > 0.05;
        markSymbolClosed(symbol, wasSL);
        resetTimeStopChain(symbol, side);
        if (wasSL)          recordConsecutiveSL();
        else if (wasProfit) resetConsecutiveSL();
    }

    //  CLEANUP — Time Stop = NEUTRAL (chain-counted)

    private void cleanupExpired() {
        decayStreakBoost();
        long now = System.currentTimeMillis();

        for (var entry : activeSignals.entrySet()) {
            entry.getValue().removeIf(s -> {
                if (s.ageMs() > TIME_STOP_MS) {
                    currentHeat = Math.max(0, currentHeat - s.riskPct);

                    activeSymbols.remove(s.symbol);
                    symbolCooldownUntil.put(s.symbol,
                            System.currentTimeMillis() + TIME_STOP_COOLDOWN_MS);

                    Deque<ClosedTrade> hist = tradeHistory.computeIfAbsent(s.symbol,
                            k -> new ConcurrentLinkedDeque<>());
                    hist.addLast(new ClosedTrade(s.symbol, s.side, 0.0, s.ageMs(), "TIME_STOP"));
                    while (hist.size() > MAX_HISTORY) hist.removeFirst();

                    int chainLen = recordTimeStopChain(s.symbol, s.side);
                    long pauseMin = chainPauseMinutesLeft(s.symbol, s.side);
                    log("⏱ TIME_STOP: " + s.symbol + " " + s.side
                            + " — unlocked, 45min cooldown"
                            + (chainLen >= TS_CHAIN_TIER1_THRESHOLD
                            ? " · TS-CHAIN=" + chainLen + " pause=" + pauseMin + "min"
                            : ""));

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

        // Safety net: purge stale activeSymbols entries
        long staleThreshold = TIME_STOP_MS + 5 * 60_000L;
        activeSymbols.entrySet().removeIf(e ->
                (now - e.getValue()) > staleThreshold &&
                        !activeSignals.containsKey(e.getKey()));
    }

    //  STATISTICS

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

    public java.util.Set<String> getTradeHistorySymbols() {
        return java.util.Collections.unmodifiableSet(tradeHistory.keySet());
    }

    /**
     * Per-symbol average realized R:R for TP calibration.
     * Returns avg PnL of wins / avg PnL magnitude of losses.
     * If symbol has insufficient data, returns 0 (= use market-state defaults).
     */
    public double getAvgRealizedRR(String symbol) {
        Deque<ClosedTrade> hist = tradeHistory.get(symbol);
        if (hist == null || hist.size() < 5) return 0.0;
        double avgWin  = hist.stream().filter(ClosedTrade::isWin)
                .mapToDouble(t -> t.pnlPct).average().orElse(0);
        double avgLoss = hist.stream().filter(t -> !t.isWin())
                .mapToDouble(t -> Math.abs(t.pnlPct)).average().orElse(1.0);
        return avgLoss > 0 ? avgWin / avgLoss : 0.0;
    }

    public double getSymbolScore(String symbol) {
        return symbolScore.getOrDefault(symbol, 0.0);
    }

    /** True when daily loss exceeds -3% (cautious) OR -6% (survival). Both modes reduce size. */
    public boolean isCautiousMode() {
        resetDailyIfNeeded();
        return dailyPnLPct <= DAILY_LOSS_CAUTIOUS_PCT;
    }

    /** True only in deep survival mode (day <= -6%). Position size ×0.25. */
    public boolean isSurvivalMode() {
        resetDailyIfNeeded();
        return dailyPnLPct <= DAILY_LOSS_SURVIVAL_PCT;
    }

    public int getCurrentLossStreak() { return currentLossStreak; }
    public double getStreakBoost()    { return streakConfidenceBoost; }
    public double getCurrentHeat()    { return currentHeat; }

    /**
     * Adaptive position size based on REAL last 10 closed trades.
     *   • 8-10 of 10 wins → ×1.20
     *   • 5-7  of 10 wins → ×1.00 (norm)
     *   • 3-4  of 10 wins → ×0.60 (cut)
     *   • 0-2  of 10 wins → ×0.30 (survival, never zero — keep edge tested)
     */
    public double getRiskSizeMultiplier() {
        resetDailyIfNeeded();
        if (dailyPnLPct <= DAILY_LOSS_SURVIVAL_PCT) {
            return 0.25; // -6% day → minimum size, but keep trading
        }

        List<ClosedTrade> recent = new ArrayList<>();
        for (Deque<ClosedTrade> hist : tradeHistory.values()) {
            recent.addAll(hist);
        }
        recent.sort(Comparator.comparingLong(t -> t.closedAt));
        int n = recent.size();
        if (n < 3) {
            return lastBacktestEV < -0.02 ? 0.70 : 0.90;
        }
        int window = Math.min(10, n);
        List<ClosedTrade> last10 = recent.subList(n - window, n);
        long wins   = last10.stream().filter(ClosedTrade::isWin).count();
        long losses = window - wins;

        double mult;
        if (wins >= (long)(window * 0.80)) {
            mult = 1.20;
        } else if (wins >= (long)(window * 0.55)) {
            mult = 1.00;
        } else if (wins >= (long)(window * 0.35)) {
            mult = dailyPnLPct <= DAILY_LOSS_CAUTIOUS_PCT ? 0.40 : 0.60;
        } else {
            mult = dailyPnLPct <= DAILY_LOSS_CAUTIOUS_PCT ? 0.25 : 0.30;
        }

        if (lastBacktestEV < -0.02
                && System.currentTimeMillis() - lastBacktestTime < 2 * 3600_000L) {
            mult *= 0.80;
        }

        if (mult <= 0.30) {
            log("⚠️ СТОП-РЕЖИМ: последние " + window + " сделок: " + wins + " побед/"
                    + losses + " потерь. Позиции: " + (int)(mult*100) + "%.");
        } else if (mult <= 0.60) {
            log("⚠️ ОСТОРОЖНЫЙ РЕЖИМ: " + wins + "/" + window + " побед. Позиции: "
                    + (int)(mult*100) + "%.");
        }

        return Math.max(0.20, Math.min(mult, 1.20));
    }

    public String getStats() {
        String rrFlag = getReducedRiskFlag();
        return String.format(
                "ISC[act=%d/%d heat=%.1f%% cl=%d wr=%.0f%% str=%d+%.0f L%d/S%d eff=%.0f%% day=%.2f%% SL_streak=%d%s]",
                getActiveCount(), maxGlobalSignals, currentHeat * 100,
                getTotalTradeCount(), getOverallWinRate() * 100,
                currentLossStreak, streakConfidenceBoost,
                consecutiveLongLosses, consecutiveShortLosses,
                getEffectiveMinConfidence(),
                dailyPnLPct,
                consecutiveSlCount.get(),
                rrFlag.isEmpty() ? "" : " " + rrFlag);
    }

    //  INTERNAL

    private double estimateRisk(double confidence, com.bot.DecisionEngineMerged.CoinCategory cat) {
        // Flat conservative risk until we have 50+ trades proving the edge.
        int totalTrades = getTotalTradeCount();
        double overallWR = getOverallWinRate();

        double base;
        if (totalTrades < 50 || overallWR < 0.52) {
            base = 0.008; // 0.8% regardless of confidence
        } else {
            base = confidence >= 78 ? 0.015
                    : confidence >= 68 ? 0.012
                      : confidence >= 58 ? 0.010
                        : 0.008;
        }

        double mult = cat == com.bot.DecisionEngineMerged.CoinCategory.MEME ? 0.5 :
                cat == com.bot.DecisionEngineMerged.CoinCategory.TOP  ? 1.0 : 0.75;
        return base * mult;
    }

    private void updateSymbolScore(String symbol, double pnl) {
        double delta = pnl > 0.5 ? 0.015 : pnl < -0.5 ? -0.018 : -0.003;
        symbolScore.merge(symbol, delta, Double::sum);
        symbolScore.compute(symbol, (k, v) ->
                com.bot.TradingCore.clamp(v == null ? 0 : v, -0.40, 0.40));

        // Auto-soft-blacklist: symbols with persistent low WR get a hard conf boost,
        // making them functionally untradeable until WR recovers.
        checkAutoBlacklist(symbol);
    }

    // Thread-safe auto-blacklist registry (symbol → soft-blocked)
    private final ConcurrentHashMap<String, Boolean> autoBlacklist = new ConcurrentHashMap<>();

    // Three-tier auto-blacklist for early intervention.
    // Tier 1: 5+ trades, WR < 30% → soft block (confidence boost)
    // Tier 2: 15+ trades, WR < 35% → soft block continues
    // Tier 3: 20+ trades, WR < 25% → HARD block (no signals at all, requires p<0.05 binomial test)
    private static final double AUTO_BLACKLIST_WR_THRESHOLD  = 0.30;
    private static final double AUTO_BLACKLIST_WR_RECOVERY   = 0.45;
    private static final int    AUTO_BLACKLIST_MIN_TRADES    = 5;
    private static final int    AUTO_BLACKLIST_TIGHT_TRADES  = 15;
    private static final double AUTO_BLACKLIST_TIGHT_WR      = 0.35;
    private static final int    AUTO_BLACKLIST_HARD_TRADES   = 20;
    private static final double AUTO_BLACKLIST_HARD_WR       = 0.25;
    private static final double AUTO_BLACKLIST_CONF_BOOST    = 20.0;

    // Hard blacklist — completely blocks signal generation for the symbol.
    private final ConcurrentHashMap<String, Long> hardBlacklist = new ConcurrentHashMap<>();
    private static final long HARD_BLACKLIST_DURATION_MS = 48L * 60 * 60 * 1000L;

    private void checkAutoBlacklist(String symbol) {
        int count = getTradeCount(symbol);
        if (count < AUTO_BLACKLIST_MIN_TRADES) return;

        double wr = getWinRate(symbol);
        boolean currentlyBlocked = autoBlacklist.getOrDefault(symbol, false);

        boolean shouldHardBlock = count >= AUTO_BLACKLIST_HARD_TRADES && wr < AUTO_BLACKLIST_HARD_WR;
        boolean shouldSoftBlock;
        if (count >= AUTO_BLACKLIST_TIGHT_TRADES) {
            shouldSoftBlock = wr < AUTO_BLACKLIST_TIGHT_WR;
        } else {
            shouldSoftBlock = wr < AUTO_BLACKLIST_WR_THRESHOLD;
        }

        // Binomial p-value test for hard-block.
        // Null hypothesis: true WR >= 0.40. Only hard-block if we can reject at p<0.05.
        // Prevents false-positives on noise (on 20 trades, 25% WR can occur by chance even if true WR = 50%).
        int wins = (int) Math.round(wr * count);
        double pValue = shouldHardBlock ? binomialCdf(wins, count, 0.40) : 1.0;
        boolean hardBlockStatisticallyValid = shouldHardBlock && pValue < 0.05;

        if (hardBlockStatisticallyValid) {
            hardBlacklist.put(symbol, System.currentTimeMillis() + HARD_BLACKLIST_DURATION_MS);
            autoBlacklist.put(symbol, true);
            log(String.format("[AUTO_BLACKLIST_HARD] %s WR=%.0f%% after %d trades (p=%.3f) → HARD-blocked 48h",
                    symbol, wr * 100, count, pValue));
        } else if (!currentlyBlocked && shouldSoftBlock) {
            autoBlacklist.put(symbol, true);
            log(String.format("[AUTO_BLACKLIST] %s WR=%.0f%% after %d trades → soft-blocked",
                    symbol, wr * 100, count));
        } else if (currentlyBlocked && wr > AUTO_BLACKLIST_WR_RECOVERY && !isHardBlacklisted(symbol)) {
            autoBlacklist.remove(symbol);
            log(String.format("[AUTO_BLACKLIST] %s WR=%.0f%% recovered → unblocked",
                    symbol, wr * 100));
        }
    }

    /**
     * Binomial CDF P(X <= k | n, p) — log-space computation to avoid overflow.
     */
    private static double binomialCdf(int k, int n, double p) {
        if (k < 0 || n <= 0 || p < 0 || p > 1) return 1.0;
        if (k >= n) return 1.0;
        double sum = 0;
        double logP = Math.log(p), log1mP = Math.log(1 - p);
        double logCoeff = 0;
        for (int i = 0; i <= k; i++) {
            if (i > 0) logCoeff += Math.log((double)(n - i + 1) / i);
            double logTerm = logCoeff + i * logP + (n - i) * log1mP;
            sum += Math.exp(logTerm);
        }
        return Math.min(1.0, sum);
    }

    /** Hard blacklist check — completely blocks signal generation. */
    public boolean isHardBlacklisted(String symbol) {
        Long until = hardBlacklist.get(symbol);
        if (until == null) return false;
        if (System.currentTimeMillis() > until) {
            hardBlacklist.remove(symbol);
            autoBlacklist.remove(symbol);
            return false;
        }
        return true;
    }

    /** OOS EV-based confidence boost. */
    public double getSymbolMinConfBoost(String symbol) {
        if (autoBlacklist.getOrDefault(symbol, false)) {
            return AUTO_BLACKLIST_CONF_BOOST;
        }
        Double ev = symbolOosEV.get(symbol);
        if (ev == null) return 0.0;
        if (ev < -0.05) return +8.0;
        if (ev < -0.02) return +5.0;
        if (ev < 0.00)  return +3.0;
        if (ev > 0.04)  return -3.0;
        if (ev > 0.02)  return -2.0;
        return 0.0;
    }

    public boolean isAutoBlacklisted(String symbol) {
        return autoBlacklist.getOrDefault(symbol, false);
    }

    public java.util.Set<String> getAutoBlacklist() {
        return java.util.Collections.unmodifiableSet(autoBlacklist.keySet());
    }

    //  DAILY LOSS LIMIT + DRAWDOWN PROTECTION

    private synchronized void trackDailyPnL(double pnlPct) {
        resetDailyIfNeeded();
        dailyPnLPct += pnlPct;
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

    private static int envInt(String key, int fallback) {
        try {
            return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(fallback)));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static double envDouble(String key, double fallback) {
        try {
            return Double.parseDouble(System.getenv().getOrDefault(key, String.valueOf(fallback)));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    /** Called by BotMain when balance is refreshed — tracks drawdown (stats only). */
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
                java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")),
                msg);
    }
}