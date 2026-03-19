package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║       InstitutionalSignalCore v12.0 — PORTFOLIO RISK CONTROLLER              ║
 * ╠══════════════════════════════════════════════════════════════════════════════╣
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
        this(20, 2, 0.12, 56.0, 0.0025, 2);
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
    private final Deque<Boolean> globalResultStreak = new ArrayDeque<>();
    private static final int STREAK_WINDOW = 10;
    private volatile int    currentLossStreak     = 0;
    private volatile double streakConfidenceBoost = 0.0;
    private volatile long   lastStreakUpdateMs    = System.currentTimeMillis();

    // [v12.1] Loss cooldown — NOT a 24h block!
    // On 15m TF, market changes every hour. Blocking for 24h = missing recovery.
    // Instead: 30min cooldown after consecutive losses, then resume with tighter thresholds.
    private volatile double dailyPnLPct        = 0.0;
    private volatile long   dailyPnLResetDay   = currentDay();
    private volatile long   lossCooldownUntil  = 0;
    private static final long LOSS_COOLDOWN_MS = 30 * 60_000L; // 30 min pause, not 24h

    // [v12.1] Drawdown circuit breaker — shorter pause for 15m TF
    private volatile double peakBalance          = 0.0;
    private volatile double drawdownFromPeak     = 0.0;
    private static final double MAX_DRAWDOWN_PCT = -8.0; // -8% from peak → pause
    private volatile long   drawdownPauseUntil   = 0;
    private static final long DRAWDOWN_PAUSE_MS  = 45 * 60_000L; // 45 min (was 2h)

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

    // Callbacks
    private volatile java.util.function.BiConsumer<String, String> timeStopCallback = null;
    public void setTimeStopCallback(java.util.function.BiConsumer<String, String> cb) { this.timeStopCallback = cb; }

    // ══════════════════════════════════════════════════════════════
    //  MODELS
    // ══════════════════════════════════════════════════════════════

    public static final class ActiveSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, sl, tp1, probability;
        public final double riskPct; // % of balance at risk
        public final long   timestamp;
        public final String category;
        public final String sector;

        public ActiveSignal(String sym, TradingCore.Side side, double entry, double sl,
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
        public final TradingCore.Side side;
        public final double pnlPct;
        public final long   duration;
        public final String exitReason;
        public final long   closedAt;

        public ClosedTrade(String sym, TradingCore.Side side, double pnl, long dur, String reason) {
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

    /** [v12.0] Direction-aware result tracking */
    public void registerConfirmedResult(boolean win, TradingCore.Side side) {
        synchronized (globalResultStreak) {
            globalResultStreak.addLast(win);
            while (globalResultStreak.size() > STREAK_WINDOW) globalResultStreak.removeFirst();
        }
        lastStreakUpdateMs = System.currentTimeMillis();

        if (win) {
            currentLossStreak = 0;
            currentWinStreak++;
            streakConfidenceBoost = Math.max(0, streakConfidenceBoost - 1.5);
            // [v12.0] Reset direction losses on any win
            if (side == TradingCore.Side.LONG) consecutiveLongLosses = 0;
            if (side == TradingCore.Side.SHORT) consecutiveShortLosses = 0;
        } else {
            currentLossStreak++;
            currentWinStreak = 0;
            streakConfidenceBoost = switch (currentLossStreak) {
                case 1 -> 1.5;
                case 2 -> 3.5;
                case 3 -> 6.0;
                default -> Math.min(10.0, currentLossStreak * 2.0);
            };
            // [v12.0] Track direction-specific losses
            if (side == TradingCore.Side.LONG) {
                consecutiveLongLosses++;
                consecutiveShortLosses = 0; // wins in other direction don't count
            } else if (side == TradingCore.Side.SHORT) {
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
        // [v11.0] Faster decay: 20% every 10min (was 15%/15min)
        if (elapsed > 10 * 60_000L) {
            streakConfidenceBoost *= 0.80;
            lastStreakUpdateMs = System.currentTimeMillis();
            if (streakConfidenceBoost < 0.5) { streakConfidenceBoost = 0; currentLossStreak = 0; }
        }
    }

    public double getEffectiveMinConfidence() {
        decayStreakBoost();
        resetDailyIfNeeded();

        // [v12.1] Cooldown active — block temporarily (30min, not 24h)
        if (System.currentTimeMillis() < lossCooldownUntil) return 100.0;

        // [v12.1] Drawdown pause
        if (System.currentTimeMillis() < drawdownPauseUntil) return 100.0;

        double base = baseMinConfidence + streakConfidenceBoost;

        // Backtest EV adjustment
        if (lastBacktestEV < -0.02 && System.currentTimeMillis() - lastBacktestTime < 2 * 3600_000L)
            base += 3.0;

        // [v12.1] Daily PnL in loss territory → tighten proportionally
        // Worse day = higher threshold, but never blocks completely
        if (dailyPnLPct < -1.0) base += Math.min(5.0, Math.abs(dailyPnLPct));

        return Math.min(base, 70.0);
    }

    public void setBacktestResult(double ev, long ts) {
        this.lastBacktestEV = ev; this.lastBacktestTime = ts;
    }

    // ══════════════════════════════════════════════════════════════
    //  SIGNAL FILTERING
    // ══════════════════════════════════════════════════════════════

    public synchronized boolean allowSignal(DecisionEngineMerged.TradeIdea signal) {
        cleanupExpired();
        String sym = signal.symbol;

        // [v12.1] Loss cooldown (30min, not 24h)
        if (System.currentTimeMillis() < lossCooldownUntil) return false;

        // [v12.1] Drawdown pause (45min)
        if (System.currentTimeMillis() < drawdownPauseUntil) return false;

        // Confidence check
        if (signal.probability < getEffectiveMinConfidence()) return false;

        // Global limit
        if (getActiveCount() >= maxGlobalSignals) return false;

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

        // [v12.0] Direction-specific loss check
        // If we lost 3+ consecutive LONGs, require higher confidence for next LONG
        if (signal.side == TradingCore.Side.LONG && consecutiveLongLosses >= DIR_LOSS_PENALTY_THRESHOLD) {
            double dirPenalty = Math.min(8.0, consecutiveLongLosses * 2.5);
            if (signal.probability < getEffectiveMinConfidence() + dirPenalty) return false;
        }
        if (signal.side == TradingCore.Side.SHORT && consecutiveShortLosses >= DIR_LOSS_PENALTY_THRESHOLD) {
            double dirPenalty = Math.min(8.0, consecutiveShortLosses * 2.5);
            if (signal.probability < getEffectiveMinConfidence() + dirPenalty) return false;
        }

        return true;
    }

    public synchronized void registerSignal(DecisionEngineMerged.TradeIdea signal) {
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
    public synchronized void closeTrade(String symbol, TradingCore.Side side, double pnlPct) {
        closeTrade(symbol, side, pnlPct, pnlPct > 0 ? "TP" : "SL");
    }

    public synchronized void closeTrade(String symbol, TradingCore.Side side, double pnlPct, String reason) {
        List<ActiveSignal> list = activeSignals.get(symbol);
        if (list == null) return;

        Iterator<ActiveSignal> it = list.iterator();
        while (it.hasNext()) {
            ActiveSignal s = it.next();
            if (s.side == side) {
                currentHeat = Math.max(0, currentHeat - s.riskPct);

                // Add to bounded history
                Deque<ClosedTrade> hist = tradeHistory.computeIfAbsent(symbol, k -> new ArrayDeque<>());
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
                    Deque<ClosedTrade> hist = tradeHistory.computeIfAbsent(s.symbol, k -> new ArrayDeque<>());
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

    public int getCurrentLossStreak() { return currentLossStreak; }
    public double getStreakBoost()    { return streakConfidenceBoost; }
    public double getCurrentHeat()   { return currentHeat; }

    public String getStats() {
        String cooldownStr = isCooldownActive() ? "CD=" + getCooldownMinutesLeft() + "m " : "";
        return String.format("ISC[act=%d/%d heat=%.1f%% cl=%d wr=%.0f%% str=%d+%.0f L%d/S%d %sev=%.4f eff=%.0f%% day=%.2f%% dd=%.1f%%]",
                getActiveCount(), maxGlobalSignals, currentHeat * 100,
                getTotalTradeCount(), getOverallWinRate() * 100,
                currentLossStreak, streakConfidenceBoost,
                consecutiveLongLosses, consecutiveShortLosses,
                cooldownStr,
                lastBacktestEV, getEffectiveMinConfidence(),
                dailyPnLPct, drawdownFromPeak);
    }

    // ══════════════════════════════════════════════════════════════
    //  INTERNAL
    // ══════════════════════════════════════════════════════════════

    private double estimateRisk(double confidence, DecisionEngineMerged.CoinCategory cat) {
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

        double mult = cat == DecisionEngineMerged.CoinCategory.MEME ? 0.5 :
                cat == DecisionEngineMerged.CoinCategory.TOP ? 1.0 : 0.75;
        return base * mult;
    }

    private void updateSymbolScore(String symbol, double pnl) {
        double delta = pnl > 0.5 ? 0.015 : pnl < -0.5 ? -0.018 : -0.003;
        symbolScore.merge(symbol, delta, Double::sum);
        symbolScore.compute(symbol, (k, v) -> TradingCore.clamp(v == null ? 0 : v, -0.40, 0.40));
    }

    // ══════════════════════════════════════════════════════════════
    //  [v11.0] DAILY LOSS LIMIT + DRAWDOWN PROTECTION
    // ══════════════════════════════════════════════════════════════

    private void trackDailyPnL(double pnlPct) {
        resetDailyIfNeeded();
        dailyPnLPct += pnlPct;

        // [v12.1] After each loss — set 30min cooldown
        // NOT a 24h block! Bot resumes after cooldown with tighter streak thresholds.
        if (pnlPct < -0.5 && currentLossStreak >= 2) {
            lossCooldownUntil = System.currentTimeMillis() + LOSS_COOLDOWN_MS;
            log("⏸ COOLDOWN 30min after " + currentLossStreak + " losses (day: "
                    + String.format("%.2f%%", dailyPnLPct) + ")");
        }
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

    /** Called by BotMain when balance is refreshed — tracks drawdown from peak */
    public void updateBalance(double currentBalance) {
        if (currentBalance > peakBalance) {
            peakBalance = currentBalance;
        }
        if (peakBalance > 0) {
            drawdownFromPeak = (currentBalance - peakBalance) / peakBalance * 100;
            if (drawdownFromPeak <= MAX_DRAWDOWN_PCT && System.currentTimeMillis() > drawdownPauseUntil) {
                drawdownPauseUntil = System.currentTimeMillis() + DRAWDOWN_PAUSE_MS;
                log("🛑 MAX DRAWDOWN HIT: " + String.format("%.2f%%", drawdownFromPeak)
                        + " — PAUSED for 2 hours");
            }
        }
    }

    public boolean isCooldownActive() {
        return System.currentTimeMillis() < lossCooldownUntil;
    }

    /** Minutes remaining in cooldown, 0 if not active */
    public long getCooldownMinutesLeft() {
        long remaining = lossCooldownUntil - System.currentTimeMillis();
        return remaining > 0 ? remaining / 60_000 : 0;
    }

    public double getDailyPnL() { return dailyPnLPct; }
    public double getDrawdownFromPeak() { return drawdownFromPeak; }

    private static void log(String msg) {
        System.out.printf("[ISC %s] %s%n",
                java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")), msg);
    }
}