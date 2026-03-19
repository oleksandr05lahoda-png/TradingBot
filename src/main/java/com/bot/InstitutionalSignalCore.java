package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║       InstitutionalSignalCore v9.0 — CRITICAL FIXES                     ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  [FIX-STREAK] TIME_STOP ≠ loss. cleanupExpired НЕ кормит streak guard ║
 * ║  [FIX-DECAY]  streakConfBoost *= 0.85 каждые 15 мин бездействия       ║
 * ║  [FIX-CAP]    effectiveMinConf cap = 72% (было 78% — бот умирал)      ║
 * ║  [FIX-RESET]  resetStreakGuard() для Watchdog self-healing             ║
 * ║  [FIX-SOFT]   Streak penalties мягче: +2/+4/+7 (было +3/+6/+10)      ║
 * ║  [FIX-FEEDBACK] registerConfirmedResult() — ТОЛЬКО подтверждённые      ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public final class InstitutionalSignalCore {

    private final int    maxGlobalSignals;
    private final int    maxSignalsPerSymbol;
    private final double maxPortfolioExposure;
    private final double minConfidence;
    private final double minSignalDiff;
    private final long   signalTtlMs;

    public InstitutionalSignalCore() {
        this.maxGlobalSignals     = 30;
        this.maxSignalsPerSymbol  = 2;
        this.maxPortfolioExposure = 0.50;
        this.minConfidence        = 56.0;
        this.minSignalDiff        = 0.0025;
        // [v10.0] CRITICAL FIX: TTL was 15 min, TIME_STOP was 90 min.
        // TTL killed signals after 15 min → TradeResolver couldn't find them
        // when TP/SL hit at 30+ min → history/winRate never updated.
        // Fix: TTL = TIME_STOP. Signal lives until resolved or timed out.
        this.signalTtlMs          = TIME_STOP_MS;
    }

    public InstitutionalSignalCore(int maxGlobal, int maxPerSym, double maxExposure,
                                   double minConf, double minDiff, long ttlMs) {
        this.maxGlobalSignals = maxGlobal; this.maxSignalsPerSymbol = maxPerSym;
        this.maxPortfolioExposure = maxExposure; this.minConfidence = minConf;
        this.minSignalDiff = minDiff; this.signalTtlMs = ttlMs;
    }

    // ── State ──────────────────────────────────────────────────────
    private final Map<String, List<ActiveSignal>> activeSignals = new ConcurrentHashMap<>();
    private final Map<String, List<ClosedTrade>>  history       = new ConcurrentHashMap<>();
    private final Map<String, Double>             symbolScore   = new ConcurrentHashMap<>();
    private final Map<String, Long>               symbolLastWin = new ConcurrentHashMap<>();
    private volatile double currentExposure = 0.0;

    private static final int  TIME_STOP_BARS_15M = 6;
    private static final long TIME_STOP_MS = TIME_STOP_BARS_15M * 15 * 60_000L;

    private volatile java.util.function.BiConsumer<String, String> timeStopCallback = null;
    public void setTimeStopCallback(java.util.function.BiConsumer<String, String> cb) {
        this.timeStopCallback = cb;
    }

    // ════════════════════════════════════════════════════════════════
    //  [v9.0] STREAK GUARD — CONFIRMED RESULTS ONLY
    // ════════════════════════════════════════════════════════════════

    private final Deque<Boolean> globalResultStreak = new java.util.ArrayDeque<>();
    private static final int STREAK_WINDOW = 10;
    private volatile int    currentLossStreak     = 0;
    private volatile double streakConfidenceBoost = 0.0;
    private volatile long   lastStreakUpdateMs    = System.currentTimeMillis();

    private volatile double lastBacktestEV   = 0.0;
    private volatile long   lastBacktestTime = 0;

    public void setBacktestResult(double ev, long ts) {
        this.lastBacktestEV = ev; this.lastBacktestTime = ts;
        log("Backtest EV: " + String.format("%.4f", ev));
    }
    public double getLastBacktestEV() { return lastBacktestEV; }

    /**
     * [v9.0] ТОЛЬКО TradeResolver / UDS вызывает это.
     * TIME_STOP НЕ вызывает — это ключевой фикс.
     */
    public void registerConfirmedResult(boolean win) {
        globalResultStreak.addLast(win);
        while (globalResultStreak.size() > STREAK_WINDOW) globalResultStreak.removeFirst();
        lastStreakUpdateMs = System.currentTimeMillis();

        if (win) {
            currentLossStreak = 0;
            streakConfidenceBoost = 0.0;
        } else {
            currentLossStreak++;
            streakConfidenceBoost = switch (currentLossStreak) {
                case 1 -> 0.0;
                case 2 -> 2.0;   // мягче: было 3
                case 3 -> 4.0;   // мягче: было 6
                default -> 7.0;  // мягче: было 10
            };
            if (streakConfidenceBoost > 0)
                log("STREAK: " + currentLossStreak + " losses → conf+" + String.format("%.0f", streakConfidenceBoost));
        }
    }

    /** [v9.0] Watchdog self-heal */
    public void resetStreakGuard() {
        log("STREAK RESET (was streak=" + currentLossStreak + " boost=" + String.format("%.0f", streakConfidenceBoost) + ")");
        currentLossStreak = 0;
        streakConfidenceBoost = 0.0;
        lastStreakUpdateMs = System.currentTimeMillis();
    }

    /** [v9.0] Decay: -15% каждые 15 мин бездействия */
    private void decayStreakBoost() {
        if (streakConfidenceBoost <= 0) return;
        long elapsed = System.currentTimeMillis() - lastStreakUpdateMs;
        if (elapsed > 15 * 60_000L) {
            streakConfidenceBoost *= 0.85;
            lastStreakUpdateMs = System.currentTimeMillis();
            if (streakConfidenceBoost < 0.5) { streakConfidenceBoost = 0; currentLossStreak = 0; }
            log("STREAK DECAY → boost=" + String.format("%.1f", streakConfidenceBoost));
        }
    }

    public double getEffectiveMinConfidence() {
        decayStreakBoost();
        double base = minConfidence + streakConfidenceBoost;
        if (lastBacktestEV < -0.02 && System.currentTimeMillis() - lastBacktestTime < 2*60*60_000L)
            base += 3.0;
        return Math.min(base, 72.0); // [v9.0] cap 72 (было 78)
    }

    public int getCurrentLossStreak() { return currentLossStreak; }
    public double getStreakConfBoost() { return streakConfidenceBoost; }

    // ── Models ─────────────────────────────────────────────────────

    public static final class ActiveSignal {
        public final String symbol;
        public final com.bot.TradingCore.Side side;
        public final double entry, tp1, stop, probability;
        public final long timestamp;
        public final String category;

        public ActiveSignal(String sym, com.bot.TradingCore.Side side, double entry,
                            double tp1, double stop, double prob, long ts, String cat) {
            this.symbol=sym; this.side=side; this.entry=entry; this.tp1=tp1;
            this.stop=stop; this.probability=prob; this.timestamp=ts; this.category=cat;
        }
        public ActiveSignal(String sym, com.bot.TradingCore.Side side, double entry,
                            double prob, long ts, String cat) {
            this(sym, side, entry, 0, 0, prob, ts, cat);
        }
        public long ageMs() { return System.currentTimeMillis() - timestamp; }
        public boolean isTimeExpired() { return ageMs() > TIME_STOP_MS; }
        public boolean isStalled(double currentPrice) {
            if (tp1==0 || ageMs() < 3*15*60_000L) return false;
            double dist = Math.abs(tp1 - entry);
            double prog = side==com.bot.TradingCore.Side.LONG ? currentPrice-entry : entry-currentPrice;
            return prog / (dist+1e-9) < 0.25;
        }
    }

    public static final class ClosedTrade {
        public final String symbol; public final com.bot.TradingCore.Side side;
        public final double pnlPct; public final long duration, closedAt;
        public ClosedTrade(String sym, com.bot.TradingCore.Side side, double pnl, long dur) {
            this.symbol=sym; this.side=side; this.pnlPct=pnl; this.duration=dur;
            this.closedAt=System.currentTimeMillis();
        }
        public boolean isWin() { return pnlPct > 0; }
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN FILTER
    // ══════════════════════════════════════════════════════════════

    public synchronized boolean allowSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        cleanupExpiredSignals();
        if (signal == null) return false;
        String sym = signal.symbol;

        double effMin = getEffectiveMinConfidence();
        if (signal.probability < effMin) {
            log(sym + " prob=" + signal.probability + " < effMin=" + String.format("%.1f", effMin) + " → reject");
            return false;
        }
        if (getActiveSignalsCount() >= maxGlobalSignals) return false;

        double score = symbolScore.getOrDefault(sym, 0.0);
        if (score < -0.28 && getWinRate(sym) < 0.36 && getHistory(sym).size() >= 5) return false;

        List<ActiveSignal> list = activeSignals.computeIfAbsent(sym, k -> new CopyOnWriteArrayList<>());
        for (ActiveSignal a : list) {
            double pd = Math.abs(a.entry - signal.price) / (a.entry + 1e-9);
            if (a.side == signal.side && pd < minSignalDiff && Math.abs(a.probability - signal.probability) < 4)
                return false;
            if (a.side != signal.side && pd < minSignalDiff * 1.8) return false;
        }
        if (list.size() >= maxSignalsPerSymbol) return false;

        double est = estimateExposure(signal.probability, signal.category != null ? signal.category.name() : "ALT");
        if (currentExposure + est > maxPortfolioExposure) return false;

        log("✓ " + sym + " " + signal.side + " prob=" + signal.probability);
        return true;
    }

    public synchronized void registerSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        String cat = signal.category != null ? signal.category.name() : "ALT";
        ActiveSignal a = new ActiveSignal(signal.symbol, signal.side, signal.price,
                signal.tp1, signal.stop, signal.probability, System.currentTimeMillis(), cat);
        activeSignals.compute(signal.symbol, (s, lst) -> {
            if (lst == null) lst = new CopyOnWriteArrayList<>();
            lst.removeIf(x -> x.side == signal.side && Math.abs(x.entry - signal.price)/(x.entry+1e-9) < minSignalDiff);
            lst.add(a); return lst;
        });
        currentExposure = clamp(currentExposure + estimateExposure(signal.probability, cat), 0, maxPortfolioExposure);
    }

    public synchronized void closeTrade(String symbol, com.bot.TradingCore.Side side, double pnlPct) {
        List<ActiveSignal> list = activeSignals.get(symbol);
        if (list == null) return;
        long now = System.currentTimeMillis();
        List<ActiveSignal> rm = new ArrayList<>();
        for (ActiveSignal s : list) {
            if (s.side == side) {
                history.computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>())
                        .add(new ClosedTrade(symbol, side, pnlPct, now - s.timestamp));
                currentExposure = clamp(currentExposure - estimateExposure(s.probability, s.category), 0, maxPortfolioExposure);
                updateSymbolScore(symbol, pnlPct);
                if (pnlPct > 0) symbolLastWin.put(symbol, now);
                rm.add(s);
                // [v9.0] НЕ вызываем registerConfirmedResult здесь!
                // Вызывается из BotMain.runTradeResolver() или UDS handler
            }
        }
        list.removeAll(rm);
        if (list.isEmpty()) activeSignals.remove(symbol);
        recalcExposure();
    }

    // ══════════════════════════════════════════════════════════════
    //  CLEANUP — [v10.0] TTL = TIME_STOP = 90 min
    // ══════════════════════════════════════════════════════════════

    private void cleanupExpiredSignals() {
        long now = System.currentTimeMillis();
        decayStreakBoost();

        for (Iterator<Map.Entry<String, List<ActiveSignal>>> it = activeSignals.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, List<ActiveSignal>> e = it.next();
            String sym = e.getKey();
            e.getValue().removeIf(s -> {
                // [v10.0] TTL = TIME_STOP = 90 min. Signal lives until resolved or expired.
                boolean expired = now - s.timestamp > signalTtlMs;
                if (expired) {
                    currentExposure = clamp(currentExposure - estimateExposure(s.probability, s.category), 0, maxPortfolioExposure);
                    history.computeIfAbsent(sym, k -> new CopyOnWriteArrayList<>())
                            .add(new ClosedTrade(sym, s.side, 0.0, now - s.timestamp));
                    updateSymbolScore(sym, 0.0);
                    // NEUTRAL — не loss, не win. Streak guard НЕ трогаем.
                    if (timeStopCallback != null) {
                        try { timeStopCallback.accept(sym,
                                "⏱ *TIME STOP* " + sym + " " + s.side + " — neutral"); }
                        catch (Exception ignored) {}
                    }
                    log("EXPIRED (neutral): " + sym + " " + s.side + " age=" + (now - s.timestamp)/60_000 + "min");
                    return true;
                }
                return false;
            });
            if (e.getValue().isEmpty()) it.remove();
        }
        recalcExposure();
    }

    private void recalcExposure() {
        double exp = 0;
        for (List<ActiveSignal> l : activeSignals.values())
            for (ActiveSignal s : l) exp += estimateExposure(s.probability, s.category);
        currentExposure = clamp(exp, 0, maxPortfolioExposure);
    }

    private void updateSymbolScore(String symbol, double pnl) {
        double d = pnl > 0.5 ? 0.016 : pnl < -0.5 ? -0.020 : -0.005;
        symbolScore.merge(symbol, d, Double::sum);
        symbolScore.compute(symbol, (k,v) -> clamp(v==null?0:v, -0.50, 0.50));
    }

    private double estimateExposure(double prob, String cat) {
        double base = prob>=85?0.060 : prob>=78?0.050 : prob>=70?0.038 : prob>=62?0.028 : 0.020;
        double m = "MEME".equals(cat)?0.75 : "TOP".equals(cat)?1.0 : 0.90;
        return base * m;
    }
    private double estimateExposure(double p, Object c) { return estimateExposure(p, c!=null?c.toString():"ALT"); }

    // ══════════════════════════════════════════════════════════════
    //  PUBLIC API
    // ══════════════════════════════════════════════════════════════

    public synchronized int getActiveSignalsCount() {
        return activeSignals.values().stream().mapToInt(List::size).sum();
    }
    public synchronized double getCurrentExposure() { return currentExposure; }
    public double getSymbolScore(String s) { return symbolScore.getOrDefault(s, 0.0); }
    public List<ClosedTrade> getHistory(String s) { return history.getOrDefault(s, List.of()); }
    public int getTotalClosedTrades() { return history.values().stream().mapToInt(List::size).sum(); }
    public double getWinRate(String s) {
        List<ClosedTrade> h = history.get(s);
        if (h==null||h.isEmpty()) return 0.50;
        return (double)h.stream().filter(ClosedTrade::isWin).count() / h.size();
    }
    public double getOverallWinRate() {
        long t=0,w=0;
        for (List<ClosedTrade> l:history.values()) { t+=l.size(); w+=l.stream().filter(ClosedTrade::isWin).count(); }
        return t==0?0.50:(double)w/t;
    }
    public List<Map.Entry<String, Double>> getTopPerformers() {
        return symbolScore.entrySet().stream().sorted(Map.Entry.<String,Double>comparingByValue().reversed()).limit(5).collect(Collectors.toList());
    }

    public synchronized String getStats() {
        return String.format("ISC[act=%d/%d exp=%.1f%% cl=%d wr=%.0f%% str=%d+%.0f ev=%.4f eff=%.0f%%]",
                getActiveSignalsCount(), maxGlobalSignals, currentExposure*100,
                getTotalClosedTrades(), getOverallWinRate()*100,
                currentLossStreak, streakConfidenceBoost, lastBacktestEV, getEffectiveMinConfidence());
    }

    public synchronized String getFullStats() {
        StringBuilder sb = new StringBuilder("📊 *ISC v9.0*\n");
        sb.append(String.format("Active: %d/%d | Exp: %.1f%% | Closed: %d | WR: %.0f%%\n",
                getActiveSignalsCount(), maxGlobalSignals, currentExposure*100, getTotalClosedTrades(), getOverallWinRate()*100));
        sb.append(String.format("Streak: %d | Conf+%.0f | EffConf: %.0f%%\n",
                currentLossStreak, streakConfidenceBoost, getEffectiveMinConfidence()));
        return sb.toString();
    }

    private static double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
    private static void log(String msg) {
        System.out.println("[ISC " + java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")) + "] " + msg);
    }
}