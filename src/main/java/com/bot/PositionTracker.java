package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * PositionTracker v2.0 — institutional-grade position lifecycle tracker.
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  v2.0 changes (BE + real PnL + accurate reason classification):     │
 * │                                                                      │
 * │  1. REAL PnL from Binance fills (/fapi/v1/userTrades VWAP),         │
 * │     instead of "guess slPrice and hope". Fixes: bot showing −$X     │
 * │     when user actually closed in profit, and TP1+SL combos being    │
 * │     reported as pure SL_HIT.                                        │
 * │                                                                      │
 * │  2. MOVE-TO-BREAKEVEN after TP1 detection. When position size       │
 * │     drops ≤55% of initial (TP1 50%-close fired), tracker:           │
 * │       a) cancels the OLD SL via targeted DELETE                     │
 * │          /fapi/v1/algoOrder?algoId=… (NOT cancelAllOpenOrders —     │
 * │          that would also kill TP2).                                 │
 * │       b) places a NEW SL at entry ± BE_OFFSET_PCT (default 0.05%    │
 * │          to cover round-trip fees).                                 │
 * │       c) updates Tracked.slOrderId / slPrice / beActivated=true.    │
 * │     EV impact: roughly +0.50R per BE-protected trade vs naked SL.   │
 * │                                                                      │
 * │  3. ACCURATE reason classification: TP2_HIT / TP1_THEN_SL /         │
 * │     TP1_THEN_BE / SL_HIT / TIME_STOP / MANUAL — based on real exit  │
 * │     price proximity to actual SL/TP1/TP2 levels with 0.1% tolerance │
 * │     (handles slippage/wicks).                                       │
 * │                                                                      │
 * │  4. ORPHAN ALGO CLEANUP — when position closes naturally (SL or     │
 * │     TP fired), kill BOTH algo + plain order queues. Without this,   │
 * │     leftover TPs on a closed position can spuriously trigger later  │
 * │     when a stray ticker hits the trigger price.                     │
 * │                                                                      │
 * │  5. STARTUP RECONCILE — read /fapi/v2/positionRisk, warn loudly via │
 * │     Telegram if open positions exist that we don't know about.      │
 * │     We do NOT auto-adopt them (no recoverable entry/SL info).       │
 * │     User can manually close them.                                    │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * env:
 *   PT_TIME_STOP_MS       = 10800000  (180min — match ISC.TIME_STOP_BARS)
 *   PT_POLL_INTERVAL_MS   = 30000     (30s — balance between responsiveness/rate-limit)
 *   PT_BE_OFFSET_PCT      = 0.05      (BE SL above/below entry by this %, covers fees)
 *   PT_BE_ENABLED         = 1         (set 0 to disable breakeven feature entirely)
 *   PT_BE_TRIGGER_FRAC    = 0.55      (BE fires if remaining qty ≤ this fraction of initial)
 *   PT_REAL_PNL_ENABLED   = 1         (set 0 to fall back to old slPrice guess)
 */
public final class PositionTracker {

    private static final Logger LOG = Logger.getLogger("PositionTracker");

    private final BinanceTradeExecutor executor;
    private final RiskGuard            riskGuard;
    private TelegramBotSender          telegram; // injected by BotMain

    // env-configurable
    private final long    TIME_STOP_MS;
    private final long    POLL_INTERVAL_MS;
    private final double  BE_OFFSET_PCT;       // 0.05 → 0.05%
    private final boolean BE_ENABLED;
    private final double  BE_TRIGGER_FRAC;     // 0.55 → fire BE if remaining ≤ 55% of initial
    private final boolean REAL_PNL_ENABLED;

    // Active risk-management toggles. Mirror of SimpleBacktester flags so
    // live and backtest behave identically.
    //   PT_TRAIL_ENABLED       — multi-level ratchet trail (1.0R→0.4R, 1.5R→0.8R, 2.0R→1.4R)
    //   PT_PROFIT_LOCK_ENABLED — at 60% time-window with ≥+0.3R, market-close
    //   PT_STAGNATION_ENABLED  — at 35% time-window with no movement ±0.3R, market-close
    private final boolean TRAIL_ENABLED;
    private final boolean PROFIT_LOCK_ENABLED;
    private final boolean STAGNATION_ENABLED;

    /** Symbol → tracked position state. */
    private final Map<String, Tracked> tracked = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler;
    private volatile boolean started = false;

    private static final PositionTracker INSTANCE = new PositionTracker();
    public static PositionTracker getInstance() { return INSTANCE; }

    private PositionTracker() {
        this.executor  = BinanceTradeExecutor.getInstance();
        this.riskGuard = RiskGuard.getInstance();
        this.telegram  = null;
        this.TIME_STOP_MS      = envLong("PT_TIME_STOP_MS", 180 * 60_000L);
        this.POLL_INTERVAL_MS  = envLong("PT_POLL_INTERVAL_MS", 30_000L);
        this.BE_OFFSET_PCT     = envDouble("PT_BE_OFFSET_PCT", 0.05);
        this.BE_ENABLED        = "1".equals(System.getenv().getOrDefault("PT_BE_ENABLED", "1"));
        this.BE_TRIGGER_FRAC   = envDouble("PT_BE_TRIGGER_FRAC", 0.55);
        this.REAL_PNL_ENABLED  = "1".equals(System.getenv().getOrDefault("PT_REAL_PNL_ENABLED", "1"));
        this.TRAIL_ENABLED       =
                !"0".equals(System.getenv().getOrDefault("PT_TRAIL_ENABLED",       "1"));
        this.PROFIT_LOCK_ENABLED =
                !"0".equals(System.getenv().getOrDefault("PT_PROFIT_LOCK_ENABLED", "1"));
        this.STAGNATION_ENABLED  =
                !"0".equals(System.getenv().getOrDefault("PT_STAGNATION_ENABLED",  "1"));

        LOG.info(String.format("[Tracker] init v2.0+v86: poll=%dms timeStop=%dmin "
                        + "BE=%s offset=%.2f%% trigger=%.2f realPnL=%s "
                        + "trail=%s profitLock=%s stagnation=%s",
                POLL_INTERVAL_MS, TIME_STOP_MS / 60_000L,
                BE_ENABLED ? "ON" : "OFF", BE_OFFSET_PCT, BE_TRIGGER_FRAC,
                REAL_PNL_ENABLED ? "ON" : "OFF",
                TRAIL_ENABLED ? "ON" : "OFF",
                PROFIT_LOCK_ENABLED ? "ON" : "OFF",
                STAGNATION_ENABLED ? "ON" : "OFF"));
    }

    public void setTelegram(TelegramBotSender tg) { this.telegram = tg; }

    public void start() {
        if (started) return;
        synchronized (this) {
            if (started) return;
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "PositionTracker-poll");
                t.setDaemon(true);
                return t;
            });
            scheduler.schedule(this::reconcileAtStartup, 5, TimeUnit.SECONDS);
            scheduler.scheduleAtFixedRate(this::pollAll,
                    POLL_INTERVAL_MS, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            // Periodic orphan reconcile (every 15min). pollAll only iterates
            // tracked.keySet() — runtime-orphans (exchange positions not in
            // tracked) need this scan to be detected.
            scheduler.scheduleAtFixedRate(this::reconcileAtStartup,
                    15, 15, TimeUnit.MINUTES);
            started = true;
            LOG.info("[Tracker] started");
        }
    }

    public void stop() {
        if (!started) return;
        try {
            scheduler.shutdown();
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        started = false;
        LOG.info("[Tracker] stopped");
    }

    // ─── Public: trackOpened (full + legacy overload) ─────────────────

    /**
     * Full v2.0 signature. Call this from BotMain after a successful open.
     *
     * @param tp1OrderId  algoId of TP1 algo order (empty if no TP1 placed)
     * @param tp2OrderId  algoId of TP2 algo order (empty if no TP2 placed)
     */
    public void trackOpened(String symbol, boolean isLong, double entry, double qty,
                            double slPrice, double tp1Price, double tp2Price,
                            double notionalUsd, String entryOrderId,
                            String slOrderId, String tp1OrderId, String tp2OrderId) {
        Tracked t = new Tracked();
        t.symbol = symbol;
        t.isLong = isLong;
        t.entry  = entry;
        t.initialQty = qty;
        t.qty    = qty;
        t.slPrice = slPrice;
        t.initialSlPrice = slPrice;  // [v86] immutable snapshot for R calculation
        t.tp1Price = tp1Price;
        t.tp2Price = tp2Price;
        t.notionalUsd = notionalUsd;
        t.entryOrderId = entryOrderId;
        t.slOrderId    = slOrderId;
        t.tp1OrderId   = tp1OrderId == null ? "" : tp1OrderId;
        t.tp2OrderId   = tp2OrderId == null ? "" : tp2OrderId;
        t.openedAtMs   = System.currentTimeMillis();
        t.beActivated  = false;
        tracked.put(symbol, t);
        LOG.info(String.format(
                "[Tracker] now watching %s %s qty=%.6f entry=%.6f sl=%.6f "
                        + "tp1=%.6f tp2=%.6f tp1Id=%s tp2Id=%s",
                symbol, isLong ? "LONG" : "SHORT", qty, entry, slPrice,
                tp1Price, tp2Price, t.tp1OrderId, t.tp2OrderId));
    }

    /**
     * @deprecated v1.0 signature. Kept for compile compatibility — calls the
     * full overload with empty TP IDs (so BE detection only uses size heuristic).
     * BotMain should switch to the full signature to enable proper BE.
     */
    @Deprecated
    public void trackOpened(String symbol, boolean isLong, double entry, double qty,
                            double slPrice, double notionalUsd,
                            String entryOrderId, String slOrderId) {
        trackOpened(symbol, isLong, entry, qty, slPrice,
                /*tp1*/ 0.0, /*tp2*/ 0.0, notionalUsd,
                entryOrderId, slOrderId, "", "");
    }

    public boolean isTracking(String symbol) { return tracked.containsKey(symbol); }
    public int trackedCount() { return tracked.size(); }

    // ─── Internal: polling loop ───────────────────────────────────────

    private void pollAll() {
        if (tracked.isEmpty()) return;
        for (String symbol : new java.util.ArrayList<>(tracked.keySet())) {
            try {
                pollOne(symbol);
            } catch (Throwable t) {
                LOG.warning("[Tracker] poll " + symbol + " error: " + t.getMessage());
            }
        }
    }

    private void pollOne(String symbol) {
        Tracked t = tracked.get(symbol);
        if (t == null) return;

        long now = System.currentTimeMillis();
        double exchangeQty = executor.fetchPositionAmount(symbol);
        double absQty = Math.abs(exchangeQty);
        boolean stillOpen = absQty > 1e-9;

        if (!stillOpen) {
            handlePositionClosed(t, now);
            return;
        }

        // ── Position still open ──
        // 1) Time-stop check first (it's terminal — no point doing BE on a stopping position)
        long age = now - t.openedAtMs;
        if (age >= TIME_STOP_MS) {
            LOG.info("[Tracker] " + symbol + " hit time-stop ("
                    + (age / 60_000L) + "min) — closing");
            boolean closed = executor.closePosition(symbol, "time-stop");
            if (closed) {
                // closePosition() already cancelled algo orders. Now compute PnL
                // off the most recent reduceOnly fills (the close we just sent).
                handlePositionClosedAfterForce(t, "TIME_STOP");
            }
            return;
        }

        // 2) Detect TP1 partial close → move SL to breakeven
        if (BE_ENABLED && !t.beActivated && t.initialQty > 0) {
            double remainingFrac = absQty / t.initialQty;
            if (remainingFrac <= BE_TRIGGER_FRAC) {
                attemptBreakeven(t, absQty, remainingFrac);
            }
        }

        // 3) Active risk management — fetch mark price, update HWM, apply
        //    trailing stop / profit-lock / stagnation exit.
        //    All four actions short-circuit on success; not combined in one poll.
        if (TRAIL_ENABLED || PROFIT_LOCK_ENABLED || STAGNATION_ENABLED) {
            applyActiveManagement(t, age);
        }

        // 4) Update tracked qty so next poll has fresh number
        t.qty = absQty;
    }

    /**
     * Active risk management for an open position.
     *
     * Reads current mark price, updates favorable/adverse high-water-marks
     * in R-units, then evaluates three exit triggers in priority order:
     *
     *   1. PROFIT LOCK — at ≥60% of time-window with ≥+0.3R, market-close.
     *      Locks in any positive ground when the trade can't make TP1 in time.
     *
     *   2. STAGNATION  — at ≥35% of time-window with neither side hitting 0.3R,
     *      market-close. Trade is dead; releasing capital for next setup.
     *
     *   3. TRAIL       — multi-level ratchet. Move SL closer to spot as the trade
     *      goes our way. Replaces the 1-shot BE-on-TP1 with continuous protection.
     *
     * Exits via {@link BinanceTradeExecutor#closePosition} for (1)+(2),
     * or via {@link BinanceTradeExecutor#replaceSlWithBreakeven} (which is
     * actually a generic SL-replace) for (3). All three are guarded by env
     * flags for surgical A/B testing.
     */
    private void applyActiveManagement(Tracked t, long age) {
        double mark = executor.fetchMarkPrice(t.symbol);
        if (mark <= 0) return; // API hiccup — try again next poll

        double risk = Math.abs(t.entry - t.initialSlPrice);
        if (risk <= 1e-12) return; // can't compute R without sane initial SL

        // Compute current R (signed: positive = our way, negative = adverse)
        double curR = t.isLong ? (mark - t.entry) / risk : (t.entry - mark) / risk;
        // Update HWM. We only have point-in-time mark, so peaks are approximate
        // (better than nothing — backtester uses bar high/low which is more accurate).
        if (curR > 0 && curR > t.maxFavR) t.maxFavR = curR;
        if (curR < 0 && -curR > t.maxAdvR) t.maxAdvR = -curR;

        // Window position: 0.0 = just opened, 1.0 = at time-stop.
        double agePct = (double) age / Math.max(1L, TIME_STOP_MS);

        if (PROFIT_LOCK_ENABLED && !t.beActivated && agePct >= 0.85 && curR >= 0.80) {
            LOG.info(String.format(
                    "[Tracker] %s PROFIT_LOCK: age=%.0f%% curR=%+.2f maxFav=%.2f → market close",
                    t.symbol, agePct * 100, curR, t.maxFavR));
            boolean closed = executor.closePosition(t.symbol, "profit-lock");
            if (closed) {
                if (telegram != null) {
                    telegram.sendMessageAsync(String.format(
                            "🎯 *PROFIT LOCK* %s\n" +
                                    "Прошло %.0f%% времени, фиксируем +%.2fR.\n" +
                                    "_Market close: цена %.6f_",
                            t.symbol, agePct * 100, curR, mark));
                }
                handlePositionClosedAfterForce(t, "PROFIT_LOCK");
            }
            return;
        }

        // ── STAGNATION ─────────────────────────────────────────────────
        // Past 35% of window, neither favorable nor adverse exceeded 0.3R —
        // trade is dead, release capital for next setup. Skip if BE already moved
        // (we have a partial fill in profit; let TP2 / trail handle).
        if (STAGNATION_ENABLED && !t.beActivated
                && agePct >= 0.35
                && t.maxFavR < 0.30 && t.maxAdvR < 0.30) {
            LOG.info(String.format(
                    "[Tracker] %s STAGNATION: age=%.0f%% maxFav=%.2f maxAdv=%.2f → market close",
                    t.symbol, agePct * 100, t.maxFavR, t.maxAdvR));
            boolean closed = executor.closePosition(t.symbol, "stagnation");
            if (closed) {
                if (telegram != null) {
                    telegram.sendMessageAsync(String.format(
                            "💤 *STAGNATION* %s\n" +
                                    "Прошло %.0f%% времени, движение ±%.2fR — закрываем.\n" +
                                    "_Освобождаем капитал для следующего сетапа_",
                            t.symbol, agePct * 100, Math.max(t.maxFavR, t.maxAdvR)));
                }
                handlePositionClosedAfterForce(t, "STAGNATION");
            }
            return;
        }

        // ── TRAILING STOP ──────────────────────────────────────────────
        // Multi-level ratchet. Each level is one-way — once we move SL to
        // entry+0.4R, we never go back to entry+0.0R. trailLevel records
        // the highest level reached so we don't churn the SL order book.
        if (TRAIL_ENABLED) {
            double newTrailSl = Double.NaN;
            int newLevel = t.trailLevel;
            if (t.maxFavR >= 2.0 && t.trailLevel < 3) {
                newTrailSl = t.isLong ? t.entry + risk * 1.4 : t.entry - risk * 1.4;
                newLevel = 3;
            } else if (t.maxFavR >= 1.5 && t.trailLevel < 2) {
                newTrailSl = t.isLong ? t.entry + risk * 0.8 : t.entry - risk * 0.8;
                newLevel = 2;
            } else if (t.maxFavR >= 1.0 && t.trailLevel < 1) {
                newTrailSl = t.isLong ? t.entry + risk * 0.4 : t.entry - risk * 0.4;
                newLevel = 1;
            }
            if (!Double.isNaN(newTrailSl)) {
                // Sanity: only tighten SL toward us, never loosen.
                boolean tighter = t.isLong ? newTrailSl > t.slPrice : newTrailSl < t.slPrice;
                if (tighter) {
                    LOG.info(String.format(
                            "[Tracker] %s TRAIL L%d: maxFav=%.2fR new SL %.6f (was %.6f)",
                            t.symbol, newLevel, t.maxFavR, newTrailSl, t.slPrice));
                    String newSlAlgoId = executor.replaceSlWithBreakeven(
                            t.symbol, t.isLong, t.slOrderId, newTrailSl);
                    if (newSlAlgoId != null) {
                        String oldId = t.slOrderId;
                        t.slOrderId  = newSlAlgoId;
                        t.slPrice    = newTrailSl;
                        t.trailLevel = newLevel;
                        if (telegram != null) {
                            double lockedR = newLevel == 3 ? 1.4 : (newLevel == 2 ? 0.8 : 0.4);
                            telegram.sendMessageAsync(String.format(
                                    "🪜 *TRAIL L%d* %s — SL подтянут\n" +
                                            "maxFav=%.2fR → SL зафиксировал +%.1fR прибыли\n" +
                                            "Новый SL: %.6f (был %.6f)\n" +
                                            "_oldId=%s newId=%s_",
                                    newLevel, t.symbol, t.maxFavR, lockedR,
                                    newTrailSl, t.slPrice, oldId, newSlAlgoId));
                        }
                    } else {
                        // Don't bump trailLevel — retry on next poll. But cap retries
                        // implicitly by not flooding logs (replaceSl already logs SEVERE).
                        LOG.warning("[Tracker] " + t.symbol
                                + " TRAIL L" + newLevel + " failed — will retry next poll");
                    }
                }
            }
        }
    }

    // ─── Position-closed handling ─────────────────────────────────────

    /**
     * Called when poll detects exchange-side qty == 0 (natural close: SL/TP/manual).
     * Computes real PnL from /fapi/v1/userTrades (VWAP of closing fills),
     * classifies the close reason, cleans up orphan algo orders, notifies.
     */
    private void handlePositionClosed(Tracked t, long now) {
        // Get real exit price from Binance fills (or fallback to slPrice)
        double exitPrice = computeRealExitPrice(t);
        double pnl = computePnl(t, exitPrice);
        String reason = classifyCloseReason(t, exitPrice, pnl);

        // Cleanup orphan algo orders (e.g., remaining TP after SL fired,
        // or remaining SL after TP2 fired). Idempotent.
        try {
            executor.cancelAllOrdersOnSymbol(t.symbol);
        } catch (Throwable cleanupEx) {
            LOG.warning("[Tracker] orphan cleanup failed for " + t.symbol + ": "
                    + cleanupEx.getMessage());
        }

        tracked.remove(t.symbol);
        riskGuard.recordTradeClosed(t.symbol, pnl);
        sendCloseNotification(t, exitPrice, pnl, reason);
        LOG.info(String.format("[Tracker] CLOSED %s pnl=$%+.4f exit=%.6f reason=%s "
                        + "(entry=%.6f sl=%.6f tp1=%.6f tp2=%.6f BE=%s)",
                t.symbol, pnl, exitPrice, reason, t.entry, t.slPrice,
                t.tp1Price, t.tp2Price, t.beActivated ? "yes" : "no"));
    }

    /**
     * Called after we forced a close (time-stop). Real fills should be there
     * already since we sent a MARKET reduce. Use same real-fetch logic.
     */
    private void handlePositionClosedAfterForce(Tracked t, String forcedReason) {
        double exitPrice = computeRealExitPrice(t);
        double pnl = computePnl(t, exitPrice);
        tracked.remove(t.symbol);
        riskGuard.recordTradeClosed(t.symbol, pnl);
        sendCloseNotification(t, exitPrice, pnl, forcedReason);
        LOG.info(String.format("[Tracker] FORCED-CLOSE %s pnl=$%+.4f exit=%.6f reason=%s",
                t.symbol, pnl, exitPrice, forcedReason));
    }

    /**
     * Fetch the real VWAP of closing fills since position was opened.
     * Falls back to slPrice if API fails or returns nothing usable.
     *
     * Key correctness point: even after partial fills (TP1 + SL), this returns
     * the PROFIT-WEIGHTED average of all closing prices, so pnl reflects
     * (TP1_price × half_qty + SL_price × half_qty) correctly — instead of
     * mis-attributing the entire qty to slPrice.
     */
    private double computeRealExitPrice(Tracked t) {
        if (!REAL_PNL_ENABLED) return t.slPrice;
        try {
            double real = executor.fetchRealizedClosingPrice(
                    t.symbol, t.isLong, t.openedAtMs);
            if (real > 0) return real;
            LOG.fine("[Tracker] real exit price returned 0 for " + t.symbol
                    + " — falling back to slPrice");
        } catch (Throwable ex) {
            LOG.warning("[Tracker] real exit price fetch failed for " + t.symbol
                    + ": " + ex.getMessage() + " — falling back to slPrice");
        }
        return t.slPrice;
    }

    private double computePnl(Tracked t, double exitPrice) {
        double diff = t.isLong ? (exitPrice - t.entry) : (t.entry - exitPrice);
        // Use initialQty (the size we actually opened) so PnL covers the whole trade
        // including any partial closes — we approximate with full qty × diff because
        // when both TP1 and SL fire, exitPrice IS already the qty-weighted VWAP.
        return diff * t.initialQty;
    }

    /**
     * Classify the close reason by comparing real exit price to known levels
     * (SL, TP1, TP2, BE). Tolerance: 0.1% — handles slippage and wick fills.
     *
     * Priority order: TP2 > TP1 > BE_SL > SL > MANUAL
     */
    private String classifyCloseReason(Tracked t, double exitPrice, double pnl) {
        if (exitPrice <= 0) return pnl > 0 ? "PROFIT_UNKNOWN" : "LOSS_UNKNOWN";
        // Category-aware slippage tolerance for SL classification:
        //   TOP coins (BTC/ETH/SOL/BNB):  0.20% — tight books
        //   MEME / volatile:               0.80% — wide books, frequent gaps
        //   ALT default:                   0.50% — typical Binance perp slippage
        // Without this, normal-slippage SL fills get misclassified as MANUAL_LOSS.
        double tol;
        String catUpper = t.symbol == null ? "" : t.symbol.toUpperCase();
        if (catUpper.startsWith("BTC") || catUpper.startsWith("ETH")
                || catUpper.startsWith("SOL") || catUpper.startsWith("BNB")) {
            tol = 0.0020;
        } else if (catUpper.contains("PEPE") || catUpper.contains("DOGE")
                || catUpper.contains("SHIB") || catUpper.contains("FLOKI")
                || catUpper.contains("BONK") || catUpper.contains("WIF")
                || catUpper.contains("MEME")) {
            tol = 0.0080;
        } else {
            tol = 0.0050; // ALT default
        }

        // Helper: is exitPrice near level (within tolerance)?
        java.util.function.DoubleFunction<Boolean> near = level ->
                level > 0 && Math.abs(exitPrice - level) / level < tol;

        if (near.apply(t.tp2Price)) return "TP2_HIT";
        if (near.apply(t.tp1Price)) return "TP1_THEN_SL"; // TP1 fired and we ran SL on remainder
        if (t.beActivated && near.apply(t.slPrice)) return "TP1_THEN_BE";
        if (near.apply(t.slPrice)) return "SL_HIT";

        // Extended check: did exit price OVERSHOOT SL with slippage?
        // Common case: market gapped through SL, executed past the level.
        // Direction: for LONG, SL is below entry → overshoot means exitPrice < slPrice.
        //            for SHORT, SL is above entry → overshoot means exitPrice > slPrice.
        boolean overshootSl = (t.isLong && t.slPrice > 0 && exitPrice < t.slPrice)
                || (!t.isLong && t.slPrice > 0 && exitPrice > t.slPrice);
        if (overshootSl && pnl < 0) return "SL_SLIPPED";

        // Symmetric check: did exit price OVERSHOOT TP1/TP2 (rare, market gapped through)?
        boolean overshootTp1 = t.tp1Price > 0
                && ((t.isLong && exitPrice > t.tp1Price)
                || (!t.isLong && exitPrice < t.tp1Price));
        boolean overshootTp2 = t.tp2Price > 0
                && ((t.isLong && exitPrice > t.tp2Price)
                || (!t.isLong && exitPrice < t.tp2Price));
        if (overshootTp2 && pnl > 0) return "TP2_SLIPPED";
        if (overshootTp1 && pnl > 0) return "TP1_SLIPPED";

        // Truly out-of-band close — likely manual close or forced kill-switch
        if (pnl > 0) return "MANUAL_PROFIT";
        if (pnl < 0) return "MANUAL_LOSS";
        return "FLAT_CLOSE";
    }

    // ─── Move-to-breakeven ────────────────────────────────────────────

    /**
     * After detecting TP1 partial close, replace the original SL (still set
     * at the loss level) with a new SL at entry ± BE_OFFSET_PCT.
     *
     * Failure mode handling: if we can't cancel the old SL, we DON'T leave
     * the position unprotected — we abort the BE move and log SEVERE. The
     * old SL stays valid until either it fires or TP2 fires.
     */
    private void attemptBreakeven(Tracked t, double absQty, double remainingFrac) {
        try {
            double offset = BE_OFFSET_PCT / 100.0; // 0.05% → 0.0005
            double newSlPrice = t.isLong
                    ? t.entry * (1.0 + offset)
                    : t.entry * (1.0 - offset);

            LOG.info(String.format(
                    "[Tracker] %s TP1 detected (remaining=%.4f initial=%.4f frac=%.2f) "
                            + "→ moving SL to BE at %.8f (was %.8f)",
                    t.symbol, absQty, t.initialQty, remainingFrac, newSlPrice, t.slPrice));

            String newSlAlgoId = executor.replaceSlWithBreakeven(
                    t.symbol, t.isLong, t.slOrderId, newSlPrice);

            if (newSlAlgoId != null) {
                String oldSlId = t.slOrderId;
                t.slOrderId   = newSlAlgoId;
                t.slPrice     = newSlPrice;
                t.beActivated = true;
                if (telegram != null) {
                    telegram.sendMessageAsync(String.format(
                            "🛡 *TP1 hit на %s* — SL переведён в безубыток\n" +
                                    "Старый SL: %.8f → новый SL (BE+%.2f%%): %.8f\n" +
                                    "Осталось qty: %.6f (%.0f%% от исходного)\n" +
                                    "_oldId=%s newId=%s_",
                            t.symbol, t.entry, BE_OFFSET_PCT, newSlPrice,
                            absQty, remainingFrac * 100.0,
                            oldSlId, newSlAlgoId));
                }
            } else {
                // BE move failed. The OLD SL may or may not still be alive
                // (depends on whether cancel succeeded before placement failed).
                // Mark beActivated=true anyway so we don't retry every poll —
                // retry storms could compound the problem.
                t.beActivated = true;
                LOG.severe("[Tracker] " + t.symbol
                        + " BE move FAILED — SL state may be inconsistent. "
                        + "Position will close on TP2 or original SL if still active.");
                if (telegram != null) {
                    telegram.sendMessageAsync(String.format(
                            "⚠️ *BE move FAILED на %s*\n" +
                                    "TP1 сработал, но переставить SL в безубыток не удалось.\n" +
                                    "Проверь позицию вручную. Логи в Railway.",
                            t.symbol));
                }
            }
        } catch (Throwable ex) {
            LOG.severe("[Tracker] attemptBreakeven exception " + t.symbol + ": "
                    + ex.getMessage());
            t.beActivated = true; // don't retry storm
        }
    }

    // ─── Startup reconcile ────────────────────────────────────────────

    /**
     * [HOLE-3 FIX 2026-05-08] On bot start: enumerate all open futures positions.
     *
     * Old behaviour: just WARN about orphans, leave position naked. This is
     * dangerous — Railway/cloud рестарт во время открытой позиции с висящим
     * SL на бирже = SL остаётся, ок. Но если SL не успел встать (наша 5s naked
     * window после MARKET) и бот рестартанул — позиция БЕЗ СТОПА на бирже.
     *
     * New behaviour:
     *   1. Перечисляем все открытые позиции через /fapi/v2/positionRisk
     *   2. Для orphans (которых tracker не знает) проверяем есть ли SL на бирже
     *      через /fapi/v1/openOrders + algo orders
     *   3. Если SL есть — оставляем (трейд под защитой, просто без tracker'а)
     *   4. Если SL НЕТ — это naked position, ОПАСНО, авто-закрываем через
     *      executor.closeOrphanPosition()
     *   5. Шлём отчёт в Telegram с детализацией: "закрыто N, оставлено M"
     *
     * Поведение можно отключить через env AUTO_CLOSE_ORPHANS=0 (по умолчанию 1).
     * При =0 возвращается старое поведение (только warn).
     */
    private void reconcileAtStartup() {
        if (!executor.isReady()) return;
        boolean autoClose = !"0".equals(System.getenv().getOrDefault("AUTO_CLOSE_ORPHANS", "1"));
        try {
            JSONArray positions = executor.fetchAllOpenPositionsRaw();
            if (positions == null) {
                LOG.info("[Tracker] startup reconcile: positionRisk fetch returned null (skipped)");
                return;
            }
            int orphanWithSl = 0;
            int orphanNakedClosed = 0;
            int orphanNakedFailed = 0;
            StringBuilder report = new StringBuilder();
            for (int i = 0; i < positions.length(); i++) {
                JSONObject p = positions.getJSONObject(i);
                double amt = p.optDouble("positionAmt", 0);
                if (Math.abs(amt) < 1e-9) continue;
                String sym = p.optString("symbol", "?");
                double entry = p.optDouble("entryPrice", 0);
                double mark  = p.optDouble("markPrice", 0);
                double upnl  = p.optDouble("unRealizedProfit", 0);
                if (tracked.containsKey(sym)) continue; // not orphan

                boolean hasSl = executor.hasActiveStopLoss(sym);
                if (hasSl) {
                    orphanWithSl++;
                    report.append(String.format(
                            "  • %s amt=%.6f entry=%.6f mark=%.6f uPnL=$%+.4f [SL on exchange ✓]\n",
                            sym, amt, entry, mark, upnl));
                } else if (autoClose) {
                    LOG.severe("[Tracker] NAKED orphan " + sym + " amt=" + amt
                            + " — auto-closing for safety");
                    boolean ok = executor.closeOrphanPosition(sym, amt);
                    if (ok) {
                        orphanNakedClosed++;
                        report.append(String.format(
                                "  • %s amt=%.6f uPnL=$%+.4f [NAKED → CLOSED ✓]\n",
                                sym, amt, upnl));
                    } else {
                        orphanNakedFailed++;
                        report.append(String.format(
                                "  • %s amt=%.6f uPnL=$%+.4f [NAKED → CLOSE FAILED ❌ MANUAL]\n",
                                sym, amt, upnl));
                    }
                } else {
                    orphanNakedFailed++;
                    report.append(String.format(
                            "  • %s amt=%.6f uPnL=$%+.4f [NAKED — auto-close disabled]\n",
                            sym, amt, upnl));
                }
            }
            int total = orphanWithSl + orphanNakedClosed + orphanNakedFailed;
            if (total > 0) {
                String msg = String.format(
                        "⚠️ *Startup reconcile: %d orphan position(s)*\n" +
                                "withSL=%d  closed=%d  manual=%d\n%s",
                        total, orphanWithSl, orphanNakedClosed, orphanNakedFailed,
                        report.toString());
                LOG.warning("[Tracker] " + msg.replace("\n", " | "));
                if (telegram != null) telegram.sendMessageAsync(msg);
            } else {
                LOG.info("[Tracker] startup reconcile: clean (no orphan positions)");
            }

            // After reconcile: cancel any orphan orders account-wide and
            // re-attempt ONE_WAY mode. The boot-time call may have failed
            // silently if orders were hanging. Now positions are dealt with,
            // sweep any lingering orders and retry the position-mode switch.
            try {
                int cancelled = executor.cancelAllOrdersAccountWide();
                if (cancelled > 0) {
                    LOG.info("[Tracker] post-reconcile: cancelled "
                            + cancelled + " orphan orders account-wide");
                }
                executor.ensureCleanAccountAndOneWayMode();
            } catch (Throwable t) {
                LOG.warning("[Tracker] post-reconcile cleanup failed: " + t.getMessage());
            }
        } catch (Throwable t) {
            LOG.warning("[Tracker] reconcile error: " + t.getMessage());
        }
    }

    // ─── Notifications ────────────────────────────────────────────────

    private void sendCloseNotification(Tracked t, double exitPrice, double pnl, String reason) {
        if (telegram == null) return;
        try {
            String emoji = pnl > 0 ? "✅" : pnl < 0 ? "❌" : "⚪";
            String beTag = t.beActivated ? " [BE-armed]" : "";
            String msg = String.format(
                    "%s *Позиция закрыта*%s\n" +
                            "%s %s | qty=%.6f\n" +
                            "Entry: %.6f → Exit (real): %.6f\n" +
                            "PnL: $%+.4f | Reason: %s\n" +
                            "Длительность: %d мин",
                    emoji, beTag, t.symbol, t.isLong ? "LONG" : "SHORT", t.initialQty,
                    t.entry, exitPrice, pnl, reason,
                    (System.currentTimeMillis() - t.openedAtMs) / 60_000L);
            telegram.sendMessageAsync(msg);
        } catch (Throwable ignored) {}
    }

    // ─── State holder ─────────────────────────────────────────────────
    private static final class Tracked {
        String symbol;
        boolean isLong;
        double entry;
        double initialQty;   // qty at open (immutable for the lifetime of the position)
        double qty;          // current remaining qty (updated each poll)
        double slPrice;      // current SL (changes after BE move)
        double tp1Price;
        double tp2Price;
        double notionalUsd;
        String entryOrderId;
        String slOrderId;    // current SL algoId (changes after BE move)
        String tp1OrderId;
        String tp2OrderId;
        long   openedAtMs;
        boolean beActivated; // true once BE move was attempted (success or failure)

        // Active management state.
        double initialSlPrice;       // saved at trackOpened — needed to compute R reliably
        double maxFavR     = 0.0;    // peak favorable movement in R-units (HWM)
        double maxAdvR     = 0.0;    // peak adverse movement in R-units (for stagnation detection)
        int    trailLevel  = 0;      // 0=none, 1=locked@0.4R, 2=locked@0.8R, 3=locked@1.4R
    }

    // ─── Env helpers ──────────────────────────────────────────────────
    private static long envLong(String k, long d) {
        try { return Long.parseLong(System.getenv().getOrDefault(k, String.valueOf(d))); }
        catch (Exception e) { return d; }
    }
    private static double envDouble(String k, double d) {
        try { return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(d))); }
        catch (Exception e) { return d; }
    }
}