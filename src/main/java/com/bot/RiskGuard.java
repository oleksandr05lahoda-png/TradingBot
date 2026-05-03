package com.bot;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * RiskGuard v1.0 — pre-trade safety layer.
 *
 * ┌────────────────────────────────────────────────────────────────────┐
 * │  Назначение: ПЕРЕД исполнением каждой сделки спросить у RiskGuard  │
 * │  "можно?" через canTrade(). Если нельзя — он скажет почему,        │
 * │  и сделка не открывается.                                          │
 * └────────────────────────────────────────────────────────────────────┘
 *
 * Защиты:
 *  1. DAILY LOSS LIMIT    — при -10% дня (configurable) останавливает
 *                            торги до начала следующих суток UTC.
 *  2. WEEKLY LOSS LIMIT   — при -20% за 7 дней останавливает до ручного
 *                            включения через RESUME_AFTER_WEEKLY_BLOCK env.
 *  3. DAILY TRADE LIMIT   — не более N сделок в сутки (default 3).
 *  4. BTC CRASH DETECTOR  — при движении BTC -3% за 30мин или -5% за 60мин
 *                            блокирует новые входы на 2 часа.
 *  5. CONCURRENT POSITIONS — не более N одновременно открытых позиций
 *                            (default 2). Защита от каскадного слива
 *                            на синхронных движениях рынка.
 *  6. COLD START          — первые 24ч после рестарта/после weekly-block
 *                            режим только Telegram-confirmation.
 *
 * Все цифры — env-переменные с дефолтами. Состояние держится в памяти,
 * сбрасывается при рестарте Railway. Это намеренно: после рестарта daily
 * counters обнуляются (Railway редко рестартит, обычно раз в день при
 * деплое — это совпадает с границей дня).
 *
 * Thread-safe: все методы могут вызываться из любого потока.
 *
 * Usage:
 *   RiskGuard guard = RiskGuard.getInstance();
 *   RiskGuard.Decision d = guard.canTrade(symbol, balanceUsd);
 *   if (!d.allowed) {
 *       LOG.info("Blocked: " + d.reason);
 *       return;
 *   }
 *   // ... open position ...
 *   guard.recordTradeOpened(symbol, size);
 *   // ... позже, при закрытии ...
 *   guard.recordTradeClosed(symbol, pnlUsd);
 */
public final class RiskGuard {

    private static final Logger LOG = Logger.getLogger("RiskGuard");
    private static final RiskGuard INSTANCE = new RiskGuard();
    public  static RiskGuard getInstance() { return INSTANCE; }

    // ─── Configuration (env-overridable) ──────────────────────────────
    private final double DAILY_LOSS_LIMIT_PCT;
    private final double WEEKLY_LOSS_LIMIT_PCT;
    private final int    DAILY_TRADE_LIMIT;
    private final int    MAX_CONCURRENT_POSITIONS;
    private final double BTC_CRASH_30M_PCT;
    private final double BTC_CRASH_60M_PCT;
    private final long   BTC_CRASH_BLOCK_MS;
    private final long   COLD_START_MS;

    private RiskGuard() {
        this.DAILY_LOSS_LIMIT_PCT      = envDouble("RG_DAILY_LOSS_LIMIT_PCT", 10.0);
        this.WEEKLY_LOSS_LIMIT_PCT     = envDouble("RG_WEEKLY_LOSS_LIMIT_PCT", 20.0);
        this.DAILY_TRADE_LIMIT         = envInt("RG_DAILY_TRADE_LIMIT", 3);
        this.MAX_CONCURRENT_POSITIONS  = envInt("RG_MAX_CONCURRENT_POSITIONS", 2);
        this.BTC_CRASH_30M_PCT         = envDouble("RG_BTC_CRASH_30M_PCT", 3.0);
        this.BTC_CRASH_60M_PCT         = envDouble("RG_BTC_CRASH_60M_PCT", 5.0);
        this.BTC_CRASH_BLOCK_MS        = envLong  ("RG_BTC_CRASH_BLOCK_MS",   2 * 60 * 60_000L);
        this.COLD_START_MS             = envLong  ("RG_COLD_START_MS",       24 * 60 * 60_000L);

        this.startupTime = System.currentTimeMillis();

        LOG.info(String.format(
                "[RiskGuard] init: dailyLoss=%.1f%% weeklyLoss=%.1f%% dailyTrades=%d "
                        + "maxConcurrent=%d btcCrash30m=%.1f%% btcCrash60m=%.1f%% coldStart=%dh",
                DAILY_LOSS_LIMIT_PCT, WEEKLY_LOSS_LIMIT_PCT, DAILY_TRADE_LIMIT,
                MAX_CONCURRENT_POSITIONS, BTC_CRASH_30M_PCT, BTC_CRASH_60M_PCT,
                COLD_START_MS / 3_600_000L));
    }

    // ─── State ────────────────────────────────────────────────────────
    private final long startupTime;

    /** Текущий "день" в UTC. Сравниваем чтобы понять когда сбросить счётчики. */
    private volatile LocalDate currentDayUtc = LocalDate.now(ZoneId.of("UTC"));
    private final AtomicLong dailyTradesOpened = new AtomicLong(0);
    /** PnL за текущие сутки UTC, в долларах. */
    private volatile double dailyPnlUsd = 0.0;
    /** PnL за последние 7 дней (rolling window). */
    private final Deque<DayPnl> weeklyPnlHistory = new ArrayDeque<>();

    /** Открытые позиции — symbol → notional USD. */
    private final ConcurrentHashMap<String, Double> openPositions = new ConcurrentHashMap<>();

    /** Manual lock — поднимается при weekly limit или ручной командой. */
    private volatile boolean manualLock = false;
    private volatile String manualLockReason = "";

    /** BTC price snapshots для crash detector. (timestamp, priceUsd). */
    private final Deque<PricePoint> btcPriceHistory = new ArrayDeque<>();
    private volatile long btcCrashBlockUntil = 0L;

    /** Опорный баланс для расчёта % убытка дня. Берётся в начале дня. */
    private volatile double dayStartBalance = 0.0;
    private volatile double weekStartBalance = 0.0;

    private static final class DayPnl {
        final LocalDate date;
        double pnlUsd;
        double startBalance;
        DayPnl(LocalDate date, double startBalance) {
            this.date = date; this.startBalance = startBalance;
        }
    }

    private static final class PricePoint {
        final long ts; final double price;
        PricePoint(long ts, double price) { this.ts = ts; this.price = price; }
    }

    // ─── Decision API ─────────────────────────────────────────────────
    public static final class Decision {
        public final boolean allowed;
        public final String  reason;
        public final String  hint;
        private Decision(boolean allowed, String reason, String hint) {
            this.allowed = allowed; this.reason = reason; this.hint = hint;
        }
        public static Decision allow() {
            return new Decision(true, "OK", "");
        }
        public static Decision block(String reason, String hint) {
            return new Decision(false, reason, hint);
        }
        @Override public String toString() {
            return allowed ? "ALLOW" : ("BLOCK: " + reason
                                        + (hint.isEmpty() ? "" : " (" + hint + ")"));
        }
    }

    /**
     * Главная проверка: можно ли открывать новую позицию по symbol сейчас.
     *
     * @param symbol      символ пары (например, "BTCUSDT")
     * @param balanceUsd  текущий баланс счёта в USD
     * @return Decision.allow() или Decision.block(reason)
     */
    public synchronized Decision canTrade(String symbol, double balanceUsd) {
        long now = System.currentTimeMillis();
        rolloverIfNewDay(balanceUsd);

        // 1. Manual lock (weekly limit или ручная команда)
        if (manualLock) {
            return Decision.block("manual lock", manualLockReason);
        }

        // 2. Daily loss limit
        if (dayStartBalance > 0) {
            double dayDrawdownPct = -100.0 * dailyPnlUsd / dayStartBalance;
            if (dayDrawdownPct >= DAILY_LOSS_LIMIT_PCT) {
                return Decision.block("daily loss limit",
                        String.format("dayPnL=%.2f USD (-%.1f%% of $%.2f start). "
                                        + "Resume at 00:00 UTC.",
                                dailyPnlUsd, dayDrawdownPct, dayStartBalance));
            }
        }

        // 3. Weekly loss limit
        double weeklyPnl = computeWeeklyPnl();
        if (weekStartBalance > 0) {
            double weekDrawdownPct = -100.0 * weeklyPnl / weekStartBalance;
            if (weekDrawdownPct >= WEEKLY_LOSS_LIMIT_PCT) {
                manualLock = true;
                manualLockReason = String.format("weekly DD %.1f%% — manual resume required",
                        weekDrawdownPct);
                return Decision.block("weekly loss limit", manualLockReason);
            }
        }

        // 4. Daily trade limit
        long openedToday = dailyTradesOpened.get();
        if (openedToday >= DAILY_TRADE_LIMIT) {
            return Decision.block("daily trade limit",
                    String.format("opened %d/%d today. Resume at 00:00 UTC.",
                            openedToday, DAILY_TRADE_LIMIT));
        }

        // 5. Max concurrent positions
        if (openPositions.containsKey(symbol)) {
            return Decision.block("position already open",
                    "already have a position on " + symbol);
        }
        if (openPositions.size() >= MAX_CONCURRENT_POSITIONS) {
            return Decision.block("max concurrent positions",
                    String.format("already %d open (limit %d): %s",
                            openPositions.size(), MAX_CONCURRENT_POSITIONS,
                            String.join(",", openPositions.keySet())));
        }

        // 6. BTC crash block
        if (now < btcCrashBlockUntil) {
            long minsLeft = (btcCrashBlockUntil - now) / 60_000L;
            return Decision.block("BTC crash block",
                    String.format("active for %d more minutes", minsLeft));
        }

        return Decision.allow();
    }

    // ─── State updates from outside ───────────────────────────────────

    /**
     * Notify guard that a position was opened. Increments daily counter
     * and records notional for concurrent-position check.
     */
    public synchronized void recordTradeOpened(String symbol, double notionalUsd) {
        rolloverIfNewDay(0);
        dailyTradesOpened.incrementAndGet();
        openPositions.put(symbol, notionalUsd);
        LOG.info(String.format("[RiskGuard] trade opened: %s notional=$%.2f "
                        + "todayCount=%d/%d concurrent=%d/%d",
                symbol, notionalUsd, dailyTradesOpened.get(), DAILY_TRADE_LIMIT,
                openPositions.size(), MAX_CONCURRENT_POSITIONS));
    }

    /**
     * Notify guard that a position was closed. Updates daily PnL.
     */
    public synchronized void recordTradeClosed(String symbol, double pnlUsd) {
        rolloverIfNewDay(0);
        openPositions.remove(symbol);
        dailyPnlUsd += pnlUsd;
        LOG.info(String.format("[RiskGuard] trade closed: %s pnl=$%+.2f dayPnL=$%+.2f",
                symbol, pnlUsd, dailyPnlUsd));
    }

    /**
     * Update BTC price for crash detector. Should be called every cycle
     * by SignalSender (it already tracks BTC price). Called frequently is
     * safe — we cap history at 30 entries.
     */
    public synchronized void updateBtcPrice(double priceUsd) {
        if (priceUsd <= 0) return;
        long now = System.currentTimeMillis();
        btcPriceHistory.addLast(new PricePoint(now, priceUsd));
        // Drop entries older than 70min (we need 60min lookback + buffer)
        while (!btcPriceHistory.isEmpty()
                && now - btcPriceHistory.peekFirst().ts > 70 * 60_000L) {
            btcPriceHistory.pollFirst();
        }
        // Cap absolute size
        while (btcPriceHistory.size() > 100) btcPriceHistory.pollFirst();

        // Check crash conditions
        Double price30mAgo = priceAtAge(30 * 60_000L);
        Double price60mAgo = priceAtAge(60 * 60_000L);
        boolean crash = false;
        String reason = "";
        if (price30mAgo != null) {
            double drop30 = -100.0 * (priceUsd - price30mAgo) / price30mAgo;
            if (drop30 >= BTC_CRASH_30M_PCT) {
                crash = true;
                reason = String.format("BTC -%.2f%% in 30min", drop30);
            }
        }
        if (!crash && price60mAgo != null) {
            double drop60 = -100.0 * (priceUsd - price60mAgo) / price60mAgo;
            if (drop60 >= BTC_CRASH_60M_PCT) {
                crash = true;
                reason = String.format("BTC -%.2f%% in 60min", drop60);
            }
        }
        if (crash && now >= btcCrashBlockUntil) {
            btcCrashBlockUntil = now + BTC_CRASH_BLOCK_MS;
            LOG.warning("[RiskGuard] BTC CRASH detected: " + reason
                    + " — blocking new entries for "
                    + (BTC_CRASH_BLOCK_MS / 60_000L) + " min");
        }
    }

    private Double priceAtAge(long ageMs) {
        long now = System.currentTimeMillis();
        long target = now - ageMs;
        // Find oldest entry that is at/before target
        PricePoint best = null;
        for (PricePoint p : btcPriceHistory) {
            if (p.ts <= target) best = p;
            else break;
        }
        return best == null ? null : best.price;
    }

    // ─── Manual controls ──────────────────────────────────────────────

    /** Manually unlock after weekly limit was hit. */
    public synchronized void manualResume() {
        manualLock = false;
        manualLockReason = "";
        LOG.info("[RiskGuard] manual resume — locks cleared");
    }

    /** Manually halt trading. Use as emergency stop. */
    public synchronized void manualHalt(String reason) {
        manualLock = true;
        manualLockReason = "manual halt: " + reason;
        LOG.warning("[RiskGuard] MANUAL HALT: " + reason);
    }

    /** True if the bot is in cold-start period (first 24h). Used by
     *  caller to optionally enforce Telegram-confirmation mode. */
    public boolean isColdStart() {
        return System.currentTimeMillis() - startupTime < COLD_START_MS;
    }

    // ─── Diagnostic ───────────────────────────────────────────────────

    public synchronized String statusLine() {
        long openedToday = dailyTradesOpened.get();
        double weeklyPnl = computeWeeklyPnl();
        long now = System.currentTimeMillis();
        String btcBlock = (now < btcCrashBlockUntil)
                ? String.format(" btcBlock=%dmin",
                (btcCrashBlockUntil - now) / 60_000L)
                : "";
        String lock = manualLock ? " LOCKED(" + manualLockReason + ")" : "";
        return String.format("[RG] day=%s trades=%d/%d dayPnL=$%+.2f weekPnL=$%+.2f "
                        + "open=%d/%d%s%s%s",
                currentDayUtc, openedToday, DAILY_TRADE_LIMIT,
                dailyPnlUsd, weeklyPnl,
                openPositions.size(), MAX_CONCURRENT_POSITIONS,
                btcBlock,
                isColdStart() ? " COLD" : "",
                lock);
    }

    // ─── Internal: rollover ───────────────────────────────────────────
    private synchronized void rolloverIfNewDay(double currentBalanceUsd) {
        LocalDate today = LocalDate.now(ZoneId.of("UTC"));
        if (!today.equals(currentDayUtc)) {
            // Archive yesterday's PnL into weekly history
            DayPnl yesterday = new DayPnl(currentDayUtc, dayStartBalance);
            yesterday.pnlUsd = dailyPnlUsd;
            weeklyPnlHistory.addLast(yesterday);
            // Drop entries older than 7 days
            while (!weeklyPnlHistory.isEmpty()
                    && ChronoUnit.DAYS.between(weeklyPnlHistory.peekFirst().date, today) > 7) {
                weeklyPnlHistory.pollFirst();
            }
            LOG.info(String.format("[RiskGuard] day rollover: %s → %s. "
                            + "Yesterday PnL=$%+.2f, opened=%d trades",
                    currentDayUtc, today, dailyPnlUsd, dailyTradesOpened.get()));
            // Reset daily state
            currentDayUtc = today;
            dailyTradesOpened.set(0);
            dailyPnlUsd = 0.0;
            if (currentBalanceUsd > 0) {
                dayStartBalance = currentBalanceUsd;
                if (weekStartBalance <= 0
                        || weeklyPnlHistory.size() == 1) { // first day of week-window
                    weekStartBalance = currentBalanceUsd;
                }
            }
        } else if (dayStartBalance <= 0 && currentBalanceUsd > 0) {
            // First call of the day with valid balance — initialize.
            dayStartBalance = currentBalanceUsd;
            if (weekStartBalance <= 0) weekStartBalance = currentBalanceUsd;
        }
    }

    private double computeWeeklyPnl() {
        double sum = dailyPnlUsd;
        for (DayPnl d : weeklyPnlHistory) sum += d.pnlUsd;
        return sum;
    }

    // ─── Env helpers ──────────────────────────────────────────────────
    private static int envInt(String k, int d) {
        try { return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(d))); }
        catch (Exception e) { return d; }
    }
    private static long envLong(String k, long d) {
        try { return Long.parseLong(System.getenv().getOrDefault(k, String.valueOf(d))); }
        catch (Exception e) { return d; }
    }
    private static double envDouble(String k, double d) {
        try { return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(d))); }
        catch (Exception e) { return d; }
    }
}