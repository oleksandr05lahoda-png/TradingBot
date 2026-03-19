package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.*;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║       BotMain v9.0 — ALL FIXES INTEGRATED                               ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  [FIX-THREAD-DEATH] SafeRunnable: catches ALL Throwable                 ║
 * ║  [FIX-WATCHDOG] Built-in subsystem monitoring + Telegram alerts          ║
 * ║  [FIX-TRADE-RESOLVER] REST-based trade monitoring when UDS ❌            ║
 * ║  [FIX-SIGNAL-DROUGHT] Auto-diagnosis when no signals > 30 min           ║
 * ║  [FIX-SELF-HEAL] Auto streak guard reset when bot self-strangulates     ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public final class BotMain {

    private static final Logger LOG = Logger.getLogger(BotMain.class.getName());

    private static final String TG_TOKEN = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID  = System.getenv().getOrDefault("CHAT_ID", "953233853");
    private static final ZoneId ZONE     = ZoneId.of("Europe/Warsaw");
    private static final int    INTERVAL = envInt("SIGNAL_INTERVAL_MIN", 1);
    private static final int    KLINES   = envInt("KLINES_LIMIT", 220);

    private static final int  QUIET_START_H = 2;
    private static final int  QUIET_END_H   = 5;
    private static final int  QUIET_END_M   = 30;

    private static final Map<String, String> SECTOR_LEADERS = new LinkedHashMap<>() {{
        put("DOGEUSDT","MEME"); put("SOLUSDT","L1"); put("UNIUSDT","DEFI");
        put("LINKUSDT","INFRA"); put("ETHUSDT","TOP"); put("XRPUSDT","PAYMENT");
    }};

    private static final AtomicLong totalCycles  = new AtomicLong(0);
    private static final AtomicLong totalSignals = new AtomicLong(0);
    private static final AtomicLong skippedQuiet = new AtomicLong(0);
    private static final AtomicLong errorCount   = new AtomicLong(0);
    private static long startTimeMs = 0;

    // Circuit breaker
    private static final int  CB_THRESHOLD = 5;
    private static final long CB_WINDOW_MS = 5 * 60_000L;
    private static final long CB_PAUSE_MS  = 2 * 60_000L;
    private static long lastErrorWindowStart = 0;
    private static int  errorsInWindow = 0;

    // ══════════════════════════════════════════════════════════════
    //  [v9.0] WATCHDOG STATE — built into BotMain
    // ══════════════════════════════════════════════════════════════
    private static volatile long lastSignalMs      = 0;
    private static volatile long lastCycleSuccessMs = 0;
    private static volatile long lastStatsSuccessMs = 0;
    private static volatile long lastWatchdogAlertMs = 0;
    private static final long SIGNAL_DROUGHT_MS   = 30 * 60_000L;
    private static final long WATCHDOG_COOLDOWN_MS = 10 * 60_000L;
    private static final AtomicLong watchdogAlerts = new AtomicLong(0);

    // ══════════════════════════════════════════════════════════════
    //  [v9.0] TRADE RESOLVER STATE — built into BotMain
    //  REST-based price check for active signals when UDS is down
    // ══════════════════════════════════════════════════════════════
    static final ConcurrentHashMap<String, TrackedSignal> trackedSignals = new ConcurrentHashMap<>();

    static final class TrackedSignal {
        final String symbol;
        final com.bot.TradingCore.Side side;
        final double entry, sl, tp1, tp2;
        final long createdAt;
        volatile boolean tp1Hit = false;

        TrackedSignal(String sym, com.bot.TradingCore.Side side,
                      double entry, double sl, double tp1, double tp2) {
            this.symbol = sym; this.side = side; this.entry = entry;
            this.sl = sl; this.tp1 = tp1; this.tp2 = tp2;
            this.createdAt = System.currentTimeMillis();
        }
        long ageMs() { return System.currentTimeMillis() - createdAt; }
    }

    // ══════════════════════════════════════════════════════════════
    //  [v9.0] SAFE RUNNABLE — prevents silent thread death
    // ══════════════════════════════════════════════════════════════
    private static Runnable safe(String name, Runnable task,
                                 com.bot.TelegramBotSender tg) {
        return () -> {
            try {
                task.run();
            } catch (Throwable t) {
                LOG.log(Level.SEVERE, "[SAFE] Task '" + name + "' FAILED", t);
                // [v10.0] НЕ спамим в Telegram техническими ошибками
                // Пользователь видит только сигналы и статистику
            }
        };
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN
    // ══════════════════════════════════════════════════════════════

    public static void main(String[] args) {
        configureLogger();

        if (TG_TOKEN == null || TG_TOKEN.isBlank()) {
            LOG.severe("TELEGRAM_TOKEN не задан — выход.");
            System.exit(1);
        }

        startTimeMs = System.currentTimeMillis();
        lastErrorWindowStart = startTimeMs;
        lastSignalMs = startTimeMs;
        lastCycleSuccessMs = startTimeMs;
        lastStatsSuccessMs = startTimeMs;

        final com.bot.TelegramBotSender telegram = new com.bot.TelegramBotSender(TG_TOKEN, CHAT_ID);
        final com.bot.GlobalImpulseController gic = new com.bot.GlobalImpulseController();
        final com.bot.InstitutionalSignalCore isc = new com.bot.InstitutionalSignalCore();
        final com.bot.SignalSender sender = new com.bot.SignalSender(telegram, gic, isc);

        isc.setTimeStopCallback((sym, msg) -> LOG.info(msg)); // [v10.0] log only, no TG spam
        gic.setPanicCallback(msg -> LOG.warning(msg));          // [v10.0] log only

        LOG.info(buildStartMessage().replace("\n", " | "));
        // [v10.0] Не шлём startup сообщение в Telegram — только сигналы и статистика
        LOG.info("═══ GodBot v9.0 FIXED стартовал " + nowWarsawStr() + " ═══");

        // ── Main scheduler with SAFE wrappers ────────────────────
        ScheduledExecutorService mainSched = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "GodBotMain");
            t.setDaemon(false);
            t.setUncaughtExceptionHandler((th, ex) ->
                    LOG.log(Level.SEVERE, "UNCAUGHT in " + th.getName(), ex));
            return t;
        });

        ScheduledExecutorService auxSched = Executors.newScheduledThreadPool(3, r -> {
            Thread t = new Thread(r, "GodBotAux"); t.setDaemon(true); return t;
        });

        // Main cycle — SAFE wrapped
        mainSched.scheduleAtFixedRate(
                safe("MainCycle", () -> runCycle(telegram, gic, isc, sender), telegram),
                0, INTERVAL, TimeUnit.MINUTES);

        // Stats every 15 min — SAFE wrapped
        auxSched.scheduleAtFixedRate(
                safe("Stats", () -> logStats(telegram, gic, isc, sender), telegram),
                15, 15, TimeUnit.MINUTES);

        // [v9.0] Watchdog every 60 sec — SAFE wrapped
        auxSched.scheduleAtFixedRate(
                safe("Watchdog", () -> runWatchdog(telegram, isc, sender), telegram),
                60, 60, TimeUnit.SECONDS);

        // [v9.0] TradeResolver: REST price check every 2 min — SAFE wrapped
        auxSched.scheduleAtFixedRate(
                safe("TradeResolver", () -> runTradeResolver(sender, isc, telegram), telegram),
                90, 120, TimeUnit.SECONDS);

        // Backtest every 2 hours — SAFE wrapped
        auxSched.scheduleAtFixedRate(
                safe("Backtest", () -> runPeriodicBacktest(sender, isc, telegram), telegram),
                30, 120, TimeUnit.MINUTES);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Завершение работы...");
            mainSched.shutdown(); auxSched.shutdown();
            telegram.sendMessageAsync("🛑 GodBot v9.0 остановлен. Циклов: " + totalCycles.get()
                    + " | Сигналов: " + totalSignals.get());
            telegram.shutdown();
            try { mainSched.awaitTermination(8, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); mainSched.shutdownNow(); }
        }, "ShutdownHook"));
    }

    // ══════════════════════════════════════════════════════════════
    //  [v9.0] WATCHDOG — checks health every 60s
    // ══════════════════════════════════════════════════════════════

    private static void runWatchdog(com.bot.TelegramBotSender telegram,
                                    com.bot.InstitutionalSignalCore isc,
                                    com.bot.SignalSender sender) {
        if (isQuietHours()) return;
        long now = System.currentTimeMillis();
        List<String> issues = new ArrayList<>();

        // 1. Main cycle dead?
        if (now - lastCycleSuccessMs > 3 * 60_000L) {
            issues.add("💀 MainCycle silent " + (now - lastCycleSuccessMs)/1000 + "s");
        }

        // 2. Stats dead?
        if (now - lastStatsSuccessMs > 20 * 60_000L) {
            issues.add("💀 Stats silent " + (now - lastStatsSuccessMs)/60_000 + "min");
        }

        // 3. Signal drought
        if (now - lastSignalMs > SIGNAL_DROUGHT_MS) {
            long droughtMin = (now - lastSignalMs) / 60_000;
            StringBuilder diag = new StringBuilder();
            diag.append("📭 No signals for ").append(droughtMin).append(" min\n");

            double effConf = isc.getEffectiveMinConfidence();
            if (effConf > 62) {
                diag.append("  ⚠️ ISC effConf=").append(String.format("%.0f", effConf)).append("%");
                if (effConf > 68) diag.append(" 🔴 HIGH");
                diag.append("\n");
            }
            diag.append("  WS=").append(sender.getActiveWsCount())
                    .append(" UDS=").append(sender.isUdsConnected() ? "✅" : "❌")
                    .append(" ").append(isc.getStats());
            issues.add(diag.toString());

            // [v9.0] Self-heal: reset streak if minConf too high
            if (effConf > 68 && isc.getCurrentLossStreak() >= 2) {
                LOG.info("[WATCHDOG] Self-heal: resetting streak guard (effConf=" + effConf + ")");
                isc.resetStreakGuard();
                issues.add("🔧 Auto-reset streak guard");
            }
        }

        // 4. WebSocket health
        if (sender.getActiveWsCount() < 3 && !isQuietHours()) {
            issues.add("⚠️ WebSockets low: " + sender.getActiveWsCount());
        }

        // [v10.0] Watchdog issues logged to console only — no Telegram spam
        if (!issues.isEmpty() && now - lastWatchdogAlertMs > WATCHDOG_COOLDOWN_MS) {
            lastWatchdogAlertMs = now;
            watchdogAlerts.incrementAndGet();
            String msg = "[WATCHDOG] " + String.join(" | ", issues);
            LOG.warning(msg);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  [v9.0] TRADE RESOLVER — REST fallback for UDS
    // ══════════════════════════════════════════════════════════════

    static void trackSignal(com.bot.DecisionEngineMerged.TradeIdea idea) {
        if (idea == null) return;
        String key = idea.symbol + "_" + idea.side;
        trackedSignals.put(key, new TrackedSignal(
                idea.symbol, idea.side, idea.price, idea.stop, idea.tp1, idea.tp2));
    }

    private static void runTradeResolver(com.bot.SignalSender sender,
                                         com.bot.InstitutionalSignalCore isc,
                                         com.bot.TelegramBotSender telegram) {
        if (trackedSignals.isEmpty()) return;

        long now = System.currentTimeMillis();

        for (Iterator<Map.Entry<String, TrackedSignal>> it =
             trackedSignals.entrySet().iterator(); it.hasNext(); ) {

            Map.Entry<String, TrackedSignal> entry = it.next();
            TrackedSignal ts = entry.getValue();

            // Expired (90 min) — remove as NEUTRAL (NOT loss!)
            if (ts.ageMs() > 90 * 60_000L) {
                it.remove();
                LOG.info("[TradeResolver] EXPIRED (neutral): " + ts.symbol + " " + ts.side);
                // НЕ вызываем isc.registerConfirmedResult(false) !!!
                continue;
            }

            // Get current price
            double price = 0;
            try {
                List<com.bot.TradingCore.Candle> c = sender.fetchKlines(ts.symbol, "1m", 1);
                if (c != null && !c.isEmpty()) price = c.get(c.size() - 1).close;
            } catch (Exception ignored) { continue; }

            if (price <= 0) continue;

            boolean isLong = ts.side == com.bot.TradingCore.Side.LONG;

            // SL hit
            boolean slHit = isLong ? price <= ts.sl : price >= ts.sl;
            if (slHit) {
                double pnl = isLong ? (ts.sl - ts.entry) / ts.entry * 100
                        : (ts.entry - ts.sl) / ts.entry * 100;
                it.remove();
                isc.registerConfirmedResult(false); // CONFIRMED loss
                isc.closeTrade(ts.symbol, ts.side, pnl);
                telegram.sendMessageAsync(String.format("❌ *SL HIT* %s %s PnL: %+.2f%%",
                        ts.symbol, ts.side, pnl));
                LOG.info("[TradeResolver] SL HIT: " + ts.symbol + " pnl=" + pnl);
                continue;
            }

            // TP1 hit
            boolean tp1Hit = isLong ? price >= ts.tp1 : price <= ts.tp1;
            if (tp1Hit && !ts.tp1Hit) {
                ts.tp1Hit = true;
                telegram.sendMessageAsync(String.format("🎯 *TP1 HIT* %s %s → SL→BE", ts.symbol, ts.side));
            }

            // After TP1: check TP2 or BE
            if (ts.tp1Hit) {
                boolean tp2Hit = isLong ? price >= ts.tp2 : price <= ts.tp2;
                if (tp2Hit) {
                    double pnl = isLong ? (price - ts.entry) / ts.entry * 100
                            : (ts.entry - price) / ts.entry * 100;
                    it.remove();
                    isc.registerConfirmedResult(true); // CONFIRMED win
                    isc.closeTrade(ts.symbol, ts.side, pnl);
                    telegram.sendMessageAsync(String.format("✅ *TP2 HIT* %s %s PnL: %+.2f%%",
                            ts.symbol, ts.side, pnl));
                    continue;
                }
                // BE exit after TP1
                boolean beHit = isLong ? price <= ts.entry * 1.001 : price >= ts.entry * 0.999;
                if (beHit) {
                    double tp1Pnl = isLong ? (ts.tp1 - ts.entry) / ts.entry * 100 * 0.5
                            : (ts.entry - ts.tp1) / ts.entry * 100 * 0.5;
                    it.remove();
                    isc.registerConfirmedResult(true); // Partial win
                    isc.closeTrade(ts.symbol, ts.side, tp1Pnl);
                    telegram.sendMessageAsync(String.format("➡️ *BE EXIT* %s %s (TP1 partial: %+.2f%%)",
                            ts.symbol, ts.side, tp1Pnl));
                }
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN CYCLE
    // ══════════════════════════════════════════════════════════════

    private static void runCycle(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        long cycleStart = System.currentTimeMillis();

        // Quiet hours
        ZonedDateTime utcNow = ZonedDateTime.now(ZoneId.of("UTC"));
        int utcH = utcNow.getHour(), utcM = utcNow.getMinute();
        boolean quiet = (utcH == QUIET_START_H) || (utcH == 3) || (utcH == 4)
                || (utcH == QUIET_END_H && utcM < QUIET_END_M);
        if (quiet) { skippedQuiet.incrementAndGet(); return; }

        // Circuit breaker
        long now = System.currentTimeMillis();
        if (now - lastErrorWindowStart > CB_WINDOW_MS) { lastErrorWindowStart = now; errorsInWindow = 0; }
        if (errorsInWindow >= CB_THRESHOLD) {
            LOG.warning("Circuit breaker — пауза " + CB_PAUSE_MS/1000 + "s");
            try { Thread.sleep(CB_PAUSE_MS); } catch (InterruptedException ignored) {}
            errorsInWindow = 0;
            return;
        }

        long cycle = totalCycles.incrementAndGet();
        LOG.info("══ ЦИКЛ #" + cycle + " ══ " + nowWarsawStr());

        updateBtcContext(sender, gic);
        updateSectors(sender, gic);

        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        LOG.info("BTC: " + ctx.regime + " str=" + String.format("%.2f", ctx.impulseStrength)
                + " vol=" + String.format("%.2f", ctx.volatilityExpansion) + " | " + isc.getStats());

        List<com.bot.DecisionEngineMerged.TradeIdea> signals = sender.generateSignals();

        lastCycleSuccessMs = System.currentTimeMillis(); // Watchdog heartbeat

        if (signals == null || signals.isEmpty()) {
            LOG.info("Нет сигналов. " + isc.getStats());
            return;
        }

        int sent = 0;
        for (com.bot.DecisionEngineMerged.TradeIdea s : signals) {
            telegram.sendMessageAsync(s.toTelegramString());
            LOG.info("► " + s.symbol + " " + s.side + " prob=" + String.format("%.0f%%", s.probability));
            totalSignals.incrementAndGet();
            sent++;
            trackSignal(s);      // [v9.0] TradeResolver tracking
            lastSignalMs = System.currentTimeMillis(); // [v9.0] Watchdog
        }

        LOG.info("══ ЦИКЛ #" + cycle + " END ══ sent=" + sent
                + " time=" + (System.currentTimeMillis() - cycleStart) + "ms");
    }

    // ══════════════════════════════════════════════════════════════
    //  BACKTEST
    // ══════════════════════════════════════════════════════════════

    private static void runPeriodicBacktest(com.bot.SignalSender sender,
                                            com.bot.InstitutionalSignalCore isc,
                                            com.bot.TelegramBotSender telegram) {
        com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();
        String[] syms = {"BTCUSDT","ETHUSDT"};
        double totalEV = 0; int count = 0;

        for (String sym : syms) {
            try {
                List<com.bot.TradingCore.Candle> m15 = sender.fetchKlines(sym,"15m",500);
                List<com.bot.TradingCore.Candle> h1  = sender.fetchKlines(sym,"1h",200);
                List<com.bot.TradingCore.Candle> m1  = sender.fetchKlines(sym,"1m",500);
                List<com.bot.TradingCore.Candle> m5  = sender.fetchKlines(sym,"5m",300);
                if (m15 == null || m15.size() < 250) continue;

                com.bot.SimpleBacktester.BacktestResult r = bt.run(sym, m1, m5, m15, h1,
                        com.bot.DecisionEngineMerged.CoinCategory.TOP);
                if (r.total >= 5) { totalEV += r.ev; count++; }
            } catch (Exception e) { LOG.warning("[BT] " + sym + ": " + e.getMessage()); }
        }
        if (count > 0) {
            double avgEV = totalEV / count;
            isc.setBacktestResult(avgEV, System.currentTimeMillis());
            LOG.info(String.format("[BT] avgEV=%.4f effConf=%.0f%%", avgEV, isc.getEffectiveMinConfidence()));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  HELPERS
    // ══════════════════════════════════════════════════════════════

    private static void updateBtcContext(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        try {
            List<com.bot.TradingCore.Candle> btc = sender.fetchKlines("BTCUSDT","15m",KLINES);
            if (btc != null && btc.size() > 30) gic.update(btc);
        } catch (Exception e) { LOG.warning("BTC ctx fail: " + e.getMessage()); }
    }

    private static void updateSectors(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        for (Map.Entry<String, String> e : SECTOR_LEADERS.entrySet()) {
            try {
                List<com.bot.TradingCore.Candle> sc = sender.fetchKlines(e.getKey(),"15m",80);
                if (sc != null && sc.size() > 25) gic.updateSector(e.getValue(), sc);
            } catch (Exception ignored) {}
        }
    }

    private static boolean isQuietHours() {
        ZonedDateTime utc = ZonedDateTime.now(ZoneId.of("UTC"));
        int h = utc.getHour(), m = utc.getMinute();
        return (h == QUIET_START_H) || (h == 3) || (h == 4) || (h == QUIET_END_H && m < QUIET_END_M);
    }

    private static String buildStartMessage() {
        return "🚀 *GodBot v9.0 FIXED*\n"
                + "15M | TOP-100 | UTC 02:00–05:30 quiet\n"
                + "🆕 Safe tasks (no silent death)\n"
                + "🆕 Watchdog (auto-alerts+self-heal)\n"
                + "🆕 TradeResolver (REST when UDS ❌)\n"
                + "🆕 ISC v9.0 (TIME_STOP≠loss, decay)\n"
                + "_" + nowWarsawStr() + "_";
    }

    private static void logStats(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        lastStatsSuccessMs = System.currentTimeMillis(); // Watchdog heartbeat
        long uptimeMin = (System.currentTimeMillis() - startTimeMs) / 60_000;
        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        String msg = String.format(
                "📊 *GodBot v9.0*\n"
                        + "Up: %dm | Cyc: %d | Sig: %d | Tracked: %d\n"
                        + "BTC: %s (str=%.2f vol=%.2f)\n"
                        + "WS: %d | UDS: %s | Bal: $%.2f\n"
                        + "Err: %d | WD_alerts: %d\n%s",
                uptimeMin, totalCycles.get(), totalSignals.get(), trackedSignals.size(),
                ctx.regime, ctx.impulseStrength, ctx.volatilityExpansion,
                sender.getActiveWsCount(), sender.isUdsConnected() ? "✅" : "❌",
                sender.getAccountBalance(), errorCount.get(), watchdogAlerts.get(),
                isc.getStats());
        telegram.sendMessageAsync(msg);
        LOG.info("[STATS] " + msg.replace("\n"," | "));
    }

    private static String nowWarsawStr() {
        return ZonedDateTime.now(ZONE).format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"));
    }

    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis).atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }

    private static int envInt(String k, int d) {
        try { return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(d))); }
        catch (Exception e) { return d; }
    }

    private static void configureLogger() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        for (Handler h : root.getHandlers()) {
            h.setFormatter(new SimpleFormatter() {
                @Override public String format(LogRecord r) {
                    return String.format("[%s][%s] %s%n",
                            ZonedDateTime.now(ZoneId.of("Europe/Warsaw"))
                                    .format(DateTimeFormatter.ofPattern("HH:mm:ss")),
                            r.getLevel(), r.getMessage());
                }
            });
        }
    }
}