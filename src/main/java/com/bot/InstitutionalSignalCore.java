package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║       InstitutionalSignalCore v17.0 — PORTFOLIO RISK CONTROLLER              ║
 * ╠══════════════════════════════════════════════════════════════════════════════╣
 * ║                                                                              ║
 * ║  v17.0 CRITICAL FIXES (BIPOLAR BLOCK ROOT CAUSE):                           ║
 * ║    · [FIX ROOT] cleanupExpired() now calls activeSymbols.remove(symbol)     ║
 * ║      on TIME_STOP. Without this, symbols locked in activeSymbols FOREVER.   ║
 * ║      This was the ROOT CAUSE of:                                             ║
 * ║        – "BIPOLAR BLOCK: X already active" spamming 100s of times/min      ║
 * ║        – Signal drought growing worse the longer the bot ran                ║
 * ║        – wr=0% because all closes were TIME_STOP, never real TP/SL         ║
 * ║    · [FIX] Safety purge: stale activeSymbols entries > TIME_STOP+5min      ║
 * ║      removed even if no matching activeSignal (handles race conditions).    ║
 * ║    · [FIX LOG] BIPOLAR BLOCK log throttled: 1 log/symbol/60s              ║
 * ║      was: every allowSignal() call → Railway log flood → $$ cost           ║
 * ║                                                                              ║
 * ║  v16.0 CRITICAL FIXES (SIGNAL DROUGHT FIX):                                 ║
 * ║    · [FIX ROOT] Removed hard return false in allowSignal() for daily loss   ║
 * ║    · [FIX CONF] getEffectiveMinConfidence() daily loss penalty: +12→+3     ║
 * ║    · [FIX CONF] MAX_EFFECTIVE_MIN_CONF: 75→68                              ║
 * ║    · [FIX SYMBOL] getSymbolMinConfBoost(): +8/+5→+4/+3                     ║
 * ║                                                                              ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 */
public final class InstitutionalSignalCore {
    // [v16.0 FIX] Reduced from 75→68. At 75% the bot requires near-perfect confluence
    // which almost never happens → 20+ hour droughts. 68% is high-conviction but achievable.
    private static final double MAX_EFFECTIVE_MIN_CONF = 68.0;

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
        // [v38.0] Tightened defaults: MAX_GLOBAL 12→6, MAX_SECTOR 3→2
        // Aligned with CorrelationGuard MAX_TOTAL=6
        this(
                envInt("ISC_MAX_GLOBAL_SIGNALS", 6),
                envInt("ISC_MAX_SIGNALS_PER_SYMBOL", 1),  // [v38.0] 2→1: no stacking
                envDouble("ISC_MAX_PORTFOLIO_HEAT", 0.06), // [v38.0] 8%→6%
                envDouble("ISC_BASE_MIN_CONF", 65.0),      // [v38.0] 62→65: higher bar
                envDouble("ISC_MIN_SIGNAL_PRICE_DIFF", 0.003), // [v38.0] 0.25%→0.3%
                envInt("ISC_MAX_SAME_SECTOR_DIR", 2)       // [v38.0] 3→2
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

    // ══════════════════════════════════════════════════════════════
    //  [v17.0 §1] SIGNAL STATE MANAGER — Anti-bipolar + SL Cooldown
    //
    //  PROBLEM A — Bipolar trades: bot was sending LONG and then SHORT
    //  (or vice versa) on the same symbol while the first trade was still
    //  open. Result: hedged position = pure fee loss, no directional edge.
    //
    //  PROBLEM B — Falling knife re-entries: after SL hit, next signal
    //  on the same symbol could fire within the next 1-minute cycle,
    //  catching the continuation of the same dump.
    //
    //  SOLUTION:
    //    · activeSymbols tracks which symbols currently have ANY open trade.
    //    · Any new signal on an active symbol → HARD DROP (no matter direction).
    //    · After SL exit: 30-min cooldown before symbol can fire again.
    //    · After TP exit: 5-min cooldown (short pause, not a full lockout).
    // ══════════════════════════════════════════════════════════════

    /** Symbols with an open tracked trade (any direction). Symbol → entry timestamp. */
    private final Map<String, Long> activeSymbols = new ConcurrentHashMap<>();

    /** Post-exit cooldowns. Symbol → earliest time a new signal is allowed (epoch ms). */
    private final Map<String, Long> symbolCooldownUntil = new ConcurrentHashMap<>();

    // [v34.0] DAILY SIGNAL LIMIT PER SYMBOL — prevents "milking" one volatile coin all day.
    // Problem: BULLAUSDT fires 12 signals in 6 hours — all correlated, same pattern = one big bet.
    // Fix: max 4 signals per symbol per rolling 8h window. Forces diversification.
    private static final int    MAX_SIGNALS_PER_SYMBOL_8H = 4;
    private static final long   SIGNAL_WINDOW_8H_MS       = 8 * 60 * 60_000L;
    private final Map<String, Deque<Long>> symbolSignalTimestamps = new ConcurrentHashMap<>();

    /** Prune timestamps older than the 8h window from the deque. */
    private void pruneOldSignalTimestamps(Deque<Long> timestamps) {
        long cutoff = System.currentTimeMillis() - SIGNAL_WINDOW_8H_MS;
        while (!timestamps.isEmpty() && timestamps.peekFirst() < cutoff) {
            timestamps.pollFirst();
        }
    }

    /** Check if symbol has exceeded its daily signal quota */
    private boolean symbolDailyLimitReached(String symbol) {
        Deque<Long> timestamps = symbolSignalTimestamps.get(symbol);
        if (timestamps == null) return false;
        pruneOldSignalTimestamps(timestamps); // [FIX #18] single prune location
        return timestamps.size() >= MAX_SIGNALS_PER_SYMBOL_8H;
    }

    /** Record a signal timestamp for daily quota tracking */
    private void recordSymbolSignal(String symbol) {
        Deque<Long> timestamps = symbolSignalTimestamps.computeIfAbsent(symbol,
                k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        timestamps.addLast(System.currentTimeMillis());
        pruneOldSignalTimestamps(timestamps); // [FIX #18] reuse same prune helper
    }

    /** SL cooldown: 2 × 15m candles = 30 minutes (prevents "falling knife" re-entry). */
    private static final long SL_COOLDOWN_MS  = 30 * 60_000L;

    /** [v34.0 FIX] TP cooldown: 15min (was 5min — caused same-coin spam loop).
     *  After TP, coin is still volatile. 5min = bot re-enters same move.
     *  15min = 1 full 15m candle resets structure. */
    private static final long TP_COOLDOWN_MS  = 15 * 60_000L;

    /** Register symbol as having an open trade. Called from registerSignal(). */
    public void markSymbolActive(String symbol) {
        activeSymbols.put(symbol, System.currentTimeMillis());
    }

    /** Release symbol after trade close. Applies SL or TP cooldown. */
    public void markSymbolClosed(String symbol, boolean wasSL) {
        activeSymbols.remove(symbol);
        long cooldown = wasSL ? SL_COOLDOWN_MS : TP_COOLDOWN_MS;
        symbolCooldownUntil.put(symbol, System.currentTimeMillis() + cooldown);
        log((wasSL ? "SL_COOLDOWN" : "TP_COOLDOWN") + " " + symbol
                + " for " + (cooldown / 60_000) + "min");
    }

    /**
     * §1 — Central gate: is a new signal for this symbol allowed RIGHT NOW?
     *
     * Returns false (BLOCK) if:
     *   a) Symbol has any currently active trade (bipolar prevention)
     *   b) Symbol is within its post-SL cooldown window
     */
    public boolean isSymbolAvailable(String symbol) {
        // (a) Bipolar guard
        if (activeSymbols.containsKey(symbol)) return false;
        // (b) Cooldown guard
        Long until = symbolCooldownUntil.get(symbol);
        if (until != null && System.currentTimeMillis() < until) return false;
        return true;
    }

    // ══════════════════════════════════════════════════════════════
    //  [v17.0 §4] DRAWDOWN MANAGER — Soft Circuit Breaker
    //
    //  PROBLEM: Disabling the hard signal block (v16.0) fixes droughts
    //  but leaves the bot fully exposed during prolonged loss streaks.
    //  We need a middle ground: signals keep flowing, but with REDUCED_RISK
    //  flag so position sizing drops automatically.
    //
    //  LOGIC:
    //    · Track consecutive confirmed SL exits (not chandelier/trail).
    //    · 3+ consecutive SLs → REDUCED_RISK mode (size ×0.5 additional).
    //    · 5+ consecutive SLs → DEEP_REDUCED_RISK mode (size ×0.25 additional).
    //    · Any TP or Chandelier profit → reset counter, exit REDUCED_RISK.
    //    · Mode surfaced via getReducedRiskMultiplier() to SignalSender.
    // ══════════════════════════════════════════════════════════════

    // [PATCH B5 v33] RACE CONDITION FIX — consecutiveSlCount.
    // volatile int + ++ operator is NOT atomic: read-increment-write = 3 ops.
    // Two parallel SL closures (different symbols closing simultaneously in TradeResolver)
    // can both read the same value, both increment, and write the same +1 result.
    // Net effect: one SL is silently lost from the counter → DrawdownManager under-counts.
    // At REDUCED_RISK_THRESHOLD=3: missing 1 count means bot stays at full risk 1 SL too long.
    // Fix: AtomicInteger.incrementAndGet() is a single CAS operation — always correct.
    private final java.util.concurrent.atomic.AtomicInteger consecutiveSlCount
            = new java.util.concurrent.atomic.AtomicInteger(0);
    private static final int REDUCED_RISK_THRESHOLD      = 3;
    private static final int DEEP_REDUCED_RISK_THRESHOLD = 5;

    /** Called by BotMain.runTradeResolver() on every SL hit. */
    public void recordConsecutiveSL() {
        int c = consecutiveSlCount.incrementAndGet();
        if (c >= DEEP_REDUCED_RISK_THRESHOLD) {
            log("🔴 DEEP_REDUCED_RISK: " + c + " consecutive SLs — size ×0.25");
        } else if (c >= REDUCED_RISK_THRESHOLD) {
            log("🟡 REDUCED_RISK: " + c + " consecutive SLs — size ×0.5");
        }
    }

    /** Called by BotMain.runTradeResolver() on any profitable exit (TP / Chandelier / Trail+). */
    public void resetConsecutiveSL() {
        int prev = consecutiveSlCount.getAndSet(0);
        if (prev >= REDUCED_RISK_THRESHOLD) {
            log("✅ REDUCED_RISK reset after profitable exit (was " + prev + " SLs)");
        }
    }

    /**
     * §4 — Size multiplier from DrawdownManager.
     * Applied ADDITIONALLY on top of daily-loss multiplier.
     * Returns: 1.0 normal | 0.5 reduced | 0.25 deep_reduced.
     */
    public double getReducedRiskMultiplier() {
        int c = consecutiveSlCount.get();
        if (c >= DEEP_REDUCED_RISK_THRESHOLD) return 0.25;
        if (c >= REDUCED_RISK_THRESHOLD)      return 0.50;
        return 1.0;
    }

    /** Returns the REDUCED_RISK flag string to embed in signal flags. Empty if normal. */
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

    // [v16.0 FIX ROOT CAUSE] Daily loss now affects ONLY position size — NEVER blocks signals.
    // OLD behaviour (v15): -6% daily → allowSignal() hard return false → 20h signal drought.
    // NEW behaviour (v16): -6% daily → position size ×0.25 via getRiskSizeMultiplier().
    // The bot keeps sending signals with tiny size instead of going completely silent.
    // Confidence threshold penalty softened: +12→+3, +6→+1.5 (see getEffectiveMinConfidence).
    private static final double DAILY_LOSS_CAUTIOUS_PCT  = -3.0; // -3% → cautious: size ×0.5
    private static final double DAILY_LOSS_SURVIVAL_PCT  = -6.0; // -6% → survival: size ×0.25 (NO BLOCK)
    private volatile boolean    cautiousMode          = false; // position size only — never blocks signals

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

        // [v50] Floor lowered 62→58 to match expanded confidence range.
        // With the new wider cluster-base scores (40-78), 62 floor was blocking
        // valid 3-cluster signals. At 58 we let through moderate-conviction setups
        // that can still have positive EV due to better entry timing.
        double floor = getTotalTradeCount() >= 200 ? 50.0 : 58.0;

        double base = Math.max(baseMinConfidence, floor);

        // [v16.0 FIX] Daily loss penalties DRASTICALLY reduced.
        // OLD: -6% → +12pts (62+12=74%), -3% → +6pts (62+6=68%) → both exceed MAX_EFFECTIVE_MIN_CONF
        // and combine with qualityPenalty/symbolBoost to reach 80%+ → physically impossible to pass.
        // NEW: -6% → +3pts (62+3=65%), -3% → +1.5pts (62+1.5=63.5%).
        // Risk is managed by getRiskSizeMultiplier() reducing position size, NOT by silencing signals.
        if (dailyPnLPct <= DAILY_LOSS_SURVIVAL_PCT) {
            base += 3.0;   // was +12.0 — caused 20h droughts
        } else if (dailyPnLPct <= DAILY_LOSS_CAUTIOUS_PCT) {
            base += 1.5;   // was +6.0
        }

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

    // [v16.0 FIX] Penalties halved: +8/+5 → +4/+3. Old values on top of raised daily-loss
    // thresholds pushed total required confidence to 80%+, making some symbols untradeable.
    // Still penalises bad symbols — just doesn't completely quarantine them.
    // [v43] getSymbolMinConfBoost() moved to auto-blacklist section below (includes WR-based blocking)

    // ══════════════════════════════════════════════════════════════
    //  SIGNAL FILTERING
    // ══════════════════════════════════════════════════════════════

    // [v24.0] Max positions per direction — prevents one-sided portfolio blow-up
    // [v30] MAX_SAME_DIRECTION raised 4→6.
    // Was: max 4 concurrent LONGs or 4 SHORTs. On trending days this blocked
    // the 5th valid signal completely. With 25 pairs and TOP_N filtering,
    // 6 concurrent same-direction positions is still well within risk limits.
    private static final int MAX_SAME_DIRECTION = envInt("ISC_MAX_SAME_DIRECTION", 10);

    // [v17.0] Rate-limit BIPOLAR BLOCK logs: one log per symbol per 60s to avoid Railway log flood.
    private final Map<String, Long> bipolarLogThrottle = new ConcurrentHashMap<>();
    private static final long BIPOLAR_LOG_THROTTLE_MS = 60_000L;

    /** [FIX #15] Periodic cleanup — call this from BotMain every cycle or every few minutes.
     *  cleanupExpired() was previously only reachable inside allowSignal() (synchronized).
     *  If no new signals arrive (signal drought), activeSymbols never gets cleaned → stuck symbols. */
    public synchronized void periodicCleanup() {
        cleanupExpired();
    }

    public synchronized boolean allowSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        cleanupExpired();

        // ╔══════════════════════════════════════════════════════════════╗
        // ║  [SCANNER MODE v2.0] allowSignal() — упрощён до минимума   ║
        // ║                                                              ║
        // ║  УДАЛЕНО (блокировало сигналы при ручной торговле):         ║
        // ║  · Portfolio heat cap (currentHeat > maxPortfolioHeat)      ║
        // ║  · Global signal limit (maxGlobalSignals)                   ║
        // ║  · Same direction limit (MAX_SAME_DIRECTION)                ║
        // ║  · Directional loss guard (consecutiveLongLosses)           ║
        // ║  · Daily signal limit per symbol (symbolDailyLimitReached)  ║
        // ║                                                              ║
        // ║  ВСЕ ЭТИ ПРОВЕРКИ были для ВИРТУАЛЬНОГО ПОРТФЕЛЯ.          ║
        // ║  При ручной торговле без автоисполнения — они блокировали   ║
        // ║  сигналы основываясь на позициях, которых нет в реальности. ║
        // ║                                                              ║
        // ║  ОСТАВЛЕНО (защита качества сигналов):                      ║
        // ║  · Bipolar guard — нет LONG→SHORT на одной монете           ║
        // ║  · Cooldown guard — кулдаун 20 мин после сигнала            ║
        // ║  Эти проверки работают от реального состояния (setSignalCooldown),
        // ║  не от виртуального портфеля.                               ║
        // ╚══════════════════════════════════════════════════════════════╝

        // Единственная проверка: кулдаун / биполярная защита
        if (!isSymbolAvailable(signal.symbol)) {
            Long until = symbolCooldownUntil.get(signal.symbol);
            // [v17.0] Rate-limit log: max once per 60s per symbol (was every call = Railway spam)
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

        // Daily loss mode — обновляем флаг (влияет только на размер позиции в будущей автоторговле)
        cautiousMode = dailyPnLPct <= DAILY_LOSS_CAUTIOUS_PCT;

        return true;
    }

    /**
     * [SCANNER MODE v2.0] Устанавливает кулдаун на монету после отправки сигнала.
     * Заменяет пару markSymbolActive() + (ждём TradeResolver) → markSymbolClosed().
     *
     * Механика:
     *   1. Убираем символ из activeSymbols (больше не "в сделке")
     *   2. Ставим кулдаун symbolCooldownUntil на durationMs вперёд
     *   3. isSymbolAvailable() вернёт false пока кулдаун не истечёт
     *
     * @param symbol    Монета (напр. "BTCUSDT")
     * @param durationMs Длительность кулдауна в мс (20 * 60_000L = 20 мин)
     */
    public synchronized void setSignalCooldown(String symbol, long durationMs) {
        activeSymbols.remove(symbol); // снимаем "в активной сделке"
        long until = System.currentTimeMillis() + durationMs;
        symbolCooldownUntil.put(symbol, until);
        log("⏱ SIGNAL_COOLDOWN: " + symbol + " → " + (durationMs / 60_000) + " мин");
    }

    public synchronized void registerSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        double estRisk = estimateRisk(signal.probability, signal.category);
        // [v38.0] Per-signal factor logging — record which indicators voted for this signal
        String factorLog = "";
        if (signal.forecast != null && signal.forecast.factorScores != null) {
            StringBuilder fl = new StringBuilder();
            for (var e : signal.forecast.factorScores.entrySet()) {
                if (Math.abs(e.getValue()) > 0.08) { // only log significant factors
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

        // [v38.0] Log signal factors for post-hoc analysis
        if (!factorLog.isEmpty()) {
            log(String.format("[SIGNAL] %s %s prob=%.0f%% factors=[%s]",
                    signal.symbol, signal.side, signal.probability, factorLog));
        }
    }

    /**
     * [PATCH B4 v33] UNREGISTER SIGNAL — undo a registerSignal() call.
     * Used by the R:R gate in SignalSender when a trade passes allowSignal()
     * but then fails the hard R:R >= 1.80 check (after adaptive TP calibration).
     * Without this, the symbol stays "active" and heat is inflated permanently.
     */
    public synchronized void unregisterSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        List<ActiveSignal> list = activeSignals.get(signal.symbol);
        if (list == null) return;
        list.removeIf(s -> s.side == signal.side &&
                Math.abs(s.entry - signal.price) < signal.price * 0.001);
        if (list.isEmpty()) activeSignals.remove(signal.symbol);
        // Release heat and bipolar lock so the symbol is immediately available again
        double estRisk = estimateRisk(signal.probability, signal.category);
        currentHeat = Math.max(0, currentHeat - estRisk);
        activeSymbols.remove(signal.symbol);
        log("[UNREGISTER] " + signal.symbol + " " + signal.side + " — R:R gate rollback");
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

        ActiveSignal matched = null;
        for (ActiveSignal s : list) {
            if (s.side == side) {
                matched = s;
                break;
            }
        }
        if (matched != null) {
            currentHeat = Math.max(0, currentHeat - matched.riskPct);

            // [v38.0] P&L ATTRIBUTION — log which factors led to this outcome
            // After 500+ trades, analyze: which factors correlate with wins vs losses
            String outcome = pnlPct > 0.05 ? "WIN" : pnlPct < -0.05 ? "LOSS" : "FLAT";
            if (!matched.sector.isEmpty()) { // sector field stores factor log in v38
                log(String.format("[P&L_ATTR] %s %s %s pnl=%+.2f%% dur=%dm factors=[%s]",
                        symbol, side, outcome, pnlPct, matched.ageMs() / 60_000, matched.sector));
            }

            // Add to bounded history
            Deque<ClosedTrade> hist = tradeHistory.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
            hist.addLast(new ClosedTrade(symbol, side, pnlPct, matched.ageMs(), reason));
            while (hist.size() > MAX_HISTORY) hist.removeFirst();

            updateSymbolScore(symbol, pnlPct);
            trackDailyPnL(pnlPct);

            list.remove(matched);
        }
        if (list.isEmpty()) activeSignals.remove(symbol);

        // [v17.0 §1] Release symbol with appropriate cooldown
        // [v17.0 §4] Update consecutive SL counter for DrawdownManager
        boolean wasSL = "SL".equals(reason) || "HIT_SL".equals(reason) || "SCORE_EXIT".equals(reason);
        boolean wasProfit = pnlPct > 0.05;
        markSymbolClosed(symbol, wasSL);
        if (wasSL)      recordConsecutiveSL();
        else if (wasProfit) resetConsecutiveSL();
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

                    // [v17.0 CRITICAL FIX] Release from activeSymbols on expiry.
                    // BUG: activeSymbols was NEVER cleaned on TIME_STOP — only activeSignals was.
                    // Result: every symbol that ever got a signal stayed in activeSymbols forever.
                    // isSymbolAvailable() checks activeSymbols first → permanent BIPOLAR BLOCK.
                    // This was the ROOT CAUSE of:
                    //   1. Endless "BIPOLAR BLOCK: X already active" log spam
                    //   2. Signal drought growing worse the longer the bot ran
                    //   3. wr=0% cl=46 — all "closes" were TIME_STOP because signals never unlocked
                    // Fix: apply TP_COOLDOWN after TIME_STOP (not SL_COOLDOWN — it wasn't a loss).
                    activeSymbols.remove(s.symbol);
                    symbolCooldownUntil.put(s.symbol,
                            System.currentTimeMillis() + TP_COOLDOWN_MS); // 15min cooldown after expiry

                    // NEUTRAL — not loss, not win. Streak NOT affected.
                    Deque<ClosedTrade> hist = tradeHistory.computeIfAbsent(s.symbol, k -> new ConcurrentLinkedDeque<>());
                    hist.addLast(new ClosedTrade(s.symbol, s.side, 0.0, s.ageMs(), "TIME_STOP"));
                    while (hist.size() > MAX_HISTORY) hist.removeFirst();

                    log("⏱ TIME_STOP: " + s.symbol + " " + s.side + " — unlocked, 15min cooldown");

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

        // [v17.0 FIX] Purge stale activeSymbols entries that have no matching activeSignal.
        // Safety net: if a symbol ended up in activeSymbols without a signal (e.g. race condition),
        // it stays locked forever. Remove any entry older than TIME_STOP_MS + 5min buffer.
        long staleThreshold = TIME_STOP_MS + 5 * 60_000L;
        activeSymbols.entrySet().removeIf(e ->
                (now - e.getValue()) > staleThreshold &&
                        !activeSignals.containsKey(e.getKey()));
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

    /** [v43] Returns all symbols that have trade history — used for WR metrics logging in BotMain. */
    public java.util.Set<String> getTradeHistorySymbols() {
        return java.util.Collections.unmodifiableSet(tradeHistory.keySet());
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

    /** [v16.0] True when daily loss exceeds -3% (cautious) OR -6% (survival).
     *  Both modes reduce position size. Survival additionally uses getRiskSizeMultiplier() → 0.25. */
    public boolean isCautiousMode() {
        resetDailyIfNeeded();
        return dailyPnLPct <= DAILY_LOSS_CAUTIOUS_PCT;
    }

    /** [v16.0] True only in deep survival mode (day <= -6%). Position size ×0.25. */
    public boolean isSurvivalMode() {
        resetDailyIfNeeded();
        return dailyPnLPct <= DAILY_LOSS_SURVIVAL_PCT;
    }

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
        // [v16.0] Survival mode (day <= -6%) → position size ×0.25 regardless of streak.
        // This replaces the old hard signal block with a meaningful risk reduction.
        resetDailyIfNeeded();
        if (dailyPnLPct <= DAILY_LOSS_SURVIVAL_PCT) {
            return 0.25; // lost ≥6% today — trade with minimum size, but keep trading
        }
        if (dailyPnLPct <= DAILY_LOSS_CAUTIOUS_PCT) {
            // Further constrained by trade history below, but floor at 0.40
            // (was 0.50 — extra caution when we've already lost 3%+)
        }

        // Собираем последние 10 РЕАЛЬНЫХ закрытых сделок (не ISC internal, а трейд-история)
        List<ClosedTrade> recent = new ArrayList<>();
        for (Deque<ClosedTrade> hist : tradeHistory.values()) {
            recent.addAll(hist);
        }
        // Сортируем по времени закрытия, берём последние 10
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

        if (lastBacktestEV < -0.02 && System.currentTimeMillis() - lastBacktestTime < 2 * 3600_000L) {
            mult *= 0.80;
        }

        if (mult <= 0.30) {
            log("⚠️ СТОП-РЕЖИМ: последние " + window + " сделок: " + wins + " побед/"
                    + losses + " потерь. Позиции сокращены до " + (int)(mult*100) + "%.");
        } else if (mult <= 0.60) {
            log("⚠️ ОСТОРОЖНЫЙ РЕЖИМ: " + wins + "/" + window + " побед. Позиции: "
                    + (int)(mult*100) + "%.");
        }

        return Math.max(0.20, Math.min(mult, 1.20));
    }

    public String getStats() {
        String rrFlag = getReducedRiskFlag();
        return String.format("ISC[act=%d/%d heat=%.1f%% cl=%d wr=%.0f%% str=%d+%.0f L%d/S%d eff=%.0f%% day=%.2f%% SL_streak=%d%s]",
                getActiveCount(), maxGlobalSignals, currentHeat * 100,
                getTotalTradeCount(), getOverallWinRate() * 100,
                currentLossStreak, streakConfidenceBoost,
                consecutiveLongLosses, consecutiveShortLosses,
                getEffectiveMinConfidence(),
                dailyPnLPct,
                consecutiveSlCount.get(),
                rrFlag.isEmpty() ? "" : " " + rrFlag);
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

        // [v43 PATCH FIX #6] Auto-soft-blacklist: symbols with persistent low win-rate
        // get a hard confidence boost, making them functionally un-tradeable.
        //
        // Why soft-blacklist instead of hard block?
        //   Hard block in ISC = no override possible. Soft block via minConf boost means:
        //   - if the symbol genuinely recovers WR (regime change), it can re-qualify
        //   - the boost applies per-symbol, not globally
        //   - getSymbolMinConfBoost() already exists and is checked in allowSignal()
        //
        // Trigger: win-rate < 25% AND at least 5 trades (statistically meaningful).
        // Effect: +20 minConf boost → effectively requires 80%+ signal confidence → untradeable.
        // Recovery: boost removed when WR recovers above 40%.
        checkAutoBlacklist(symbol);
    }

    // [v43] Thread-safe auto-blacklist registry (symbol → soft-blocked)
    private final java.util.concurrent.ConcurrentHashMap<String, Boolean> autoBlacklist
            = new java.util.concurrent.ConcurrentHashMap<>();
    // [v51] TIGHTER DYNAMIC BLACKLIST — multi-tier WR enforcement.
    // Old: single 25% threshold after 5 trades = too lenient (lost capital before block).
    // New: 3 tiers based on trade count for early intervention.
    // [v51] Three-tier auto-blacklist for early intervention.
    // Tier 1: 5+ trades, WR < 30% → soft block (confidence boost)
    // Tier 2: 15+ trades, WR < 35% → soft block continues
    // Tier 3: 20+ trades, WR < 25% → HARD block (no signals at all)
    private static final double AUTO_BLACKLIST_WR_THRESHOLD  = 0.30;
    private static final double AUTO_BLACKLIST_WR_RECOVERY   = 0.45;
    private static final int    AUTO_BLACKLIST_MIN_TRADES    = 5;
    private static final int    AUTO_BLACKLIST_TIGHT_TRADES  = 15;
    private static final double AUTO_BLACKLIST_TIGHT_WR      = 0.35;
    private static final int    AUTO_BLACKLIST_HARD_TRADES   = 20;
    private static final double AUTO_BLACKLIST_HARD_WR       = 0.25;
    private static final double AUTO_BLACKLIST_CONF_BOOST    = 20.0;

    // [v51] Hard blacklist — completely blocks signal generation for the symbol.
    private final java.util.concurrent.ConcurrentHashMap<String, Long> hardBlacklist
            = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long HARD_BLACKLIST_DURATION_MS = 48L * 60 * 60 * 1000L;

    private void checkAutoBlacklist(String symbol) {
        int count = getTradeCount(symbol);
        if (count < AUTO_BLACKLIST_MIN_TRADES) return;

        double wr = getWinRate(symbol);
        boolean currentlyBlocked = autoBlacklist.getOrDefault(symbol, false);

        // [v51] Three-tier blocking
        boolean shouldHardBlock = count >= AUTO_BLACKLIST_HARD_TRADES && wr < AUTO_BLACKLIST_HARD_WR;
        boolean shouldSoftBlock;
        if (count >= AUTO_BLACKLIST_TIGHT_TRADES) {
            shouldSoftBlock = wr < AUTO_BLACKLIST_TIGHT_WR;
        } else {
            shouldSoftBlock = wr < AUTO_BLACKLIST_WR_THRESHOLD;
        }

        if (shouldHardBlock) {
            hardBlacklist.put(symbol, System.currentTimeMillis() + HARD_BLACKLIST_DURATION_MS);
            autoBlacklist.put(symbol, true);
            log(String.format("[AUTO_BLACKLIST_HARD] %s WR=%.0f%% after %d trades → HARD-blocked 48h",
                    symbol, wr * 100, count));
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

    /** [v51] Hard blacklist check — completely blocks signal generation. */
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

    /** [v51] Stricter OOS EV-based confidence boost. */
    public double getSymbolMinConfBoost(String symbol) {
        if (autoBlacklist.getOrDefault(symbol, false)) {
            return AUTO_BLACKLIST_CONF_BOOST;
        }
        Double ev = symbolOosEV.get(symbol);
        if (ev == null) return 0.0;
        if (ev < -0.05) return +8.0;  // strong negative EV → strong penalty
        if (ev < -0.02) return +5.0;  // was +4
        if (ev < 0.00)  return +3.0;
        if (ev > 0.04)  return -3.0;  // strong positive → small bonus
        if (ev > 0.02)  return -2.0;
        return 0.0;
    }

    /** [v43] Expose auto-blacklist status for logging/metrics */
    public boolean isAutoBlacklisted(String symbol) {
        return autoBlacklist.getOrDefault(symbol, false);
    }

    /** [v43] Get full auto-blacklist for reporting */
    public java.util.Set<String> getAutoBlacklist() {
        return java.util.Collections.unmodifiableSet(autoBlacklist.keySet());
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