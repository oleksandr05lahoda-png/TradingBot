package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * BotMain v79.0 — INTEGRITY HARDENED + TRANSPARENT VERIFICATION
 *
 * Mode: PURE SIGNAL SCANNER. No auto-trade, no order execution.
 *
 * v79.0 — INTEGRITY & VERIFICATION OVERHAUL (audit response 2026-04-28):
 *   Реакция на внешний аудит, который указал на "self-reporting system"
 *   проблему. Точечные правки убирают РЕАЛЬНЫЕ дыры (часть утверждений
 *   аудита была ложной — например, ProbabilityCalibrator действительно
 *   присутствует в DecisionEngineMerged.java, секреты живут в env vars).
 *
 *   ИСПРАВЛЕНО:
 *     [I1] [I2] [I3] УСТАРЕЛИ 2026-07-28 — project_state id=14. Все три пункта
 *          описывали запись исходов в калибратор. Записи НЕТ: recordOutcome*()
 *          не вызывается ниоткуда, BotMain.recordSignalOutcome — мёртвая точка
 *          входа, data/calibrator.csv весит 102 байта. Загрузка CSV на старте,
 *          автосохранение и показ счётчика исходов в Telegram удалены — врущая
 *          приборная панель хуже отсутствующей. Учёт исходов переезжает в
 *          paper-harness: предрегистрация + HMAC-цепочка (project_state id=17).
 *     [I4] maxAgeMs синхронизирован с ISC TIME_STOP_BARS = 90 мин (было 5h).
 *          Это устраняет несоответствие между live time-stop правилом и
 *          верификатором (ранее verifier ждал 5 часов после того, как ISC
 *          уже считал позицию закрытой по time-stop).
 *     [I5] Cross-exchange price validation (опционально через ENV
 *          CROSS_EXCHANGE_VALIDATION=1) — сравнивает Binance с Bybit/OKX.
 *     [I6] Public verification API: forecastIntegrityCheck() —
 *          даёт пользователю/третьим лицам способ проверить, что
 *          бот не "потерял" выпавшие исходы.
 *     [I7] Forecast records persistence: при рестарте старые
 *          unresolved сигналы НЕ теряются (раньше после рестарта
 *          бот забывал что отправил → outcome никогда не записывался).
 *
 * Карта оригинального аудита (правда vs ложь):
 *   ПРАВДА:
 *     ✔ AMBIGUOUS игнорировались — ИСПРАВЛЕНО [I1]
 *     ✔ TIME_STOP игнорировались — ИСПРАВЛЕНО [I2]
 *     ✔ Нет cryptographic proof — НЕ исправлено. HMAC-цепочка в калибраторе
 *       написана, но подписывать ей нечего: ни одного записанного исхода.
 *       Требование перенесено в paper-harness (project_state id=17).
 *     ✔ Backtester / verifier мismatch — ИСПРАВЛЕНО [I4]
 *     ✔ Нет cross-exchange validation — ИСПРАВЛЕНО [I5] (опц.)
 *   ЛОЖЬ (аудит ИИ ошибался):
 *     ✘ "ProbabilityCalibrator не в коде" — класс ЕСТЬ в DecisionEngineMerged
 *        (ссылка на строку 5048 устарела, файл теперь короче 2600 строк), но
 *        исходов в нём ноль — см. [I1]-[I3] выше.
 *     ✘ "API ключи в коде" — все через requireEnv/System.getenv()
 *     ✘ "OBSERVATION_MODE не гарантирует ничего" — он напрямую проверяется
 *        в Dispatcher.dispatch перед каждой отправкой в Telegram
 */
public final class BotMain {

    private static final Logger LOG = Logger.getLogger(BotMain.class.getName());

    private static final String TG_TOKEN = requireEnv("TELEGRAM_TOKEN");
    private static final String CHAT_ID  = requireEnv("TELEGRAM_CHAT_ID");

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank())
            throw new IllegalStateException("Required env var missing: " + name);
        return v;
    }

    private static volatile ZoneId ZONE = ZoneId.of("Europe/Warsaw");
    private static final int INTERVAL = envInt("SIGNAL_INTERVAL_MIN", 5);

    // [v90 1H-PRIMARY 2026-05-09] Primary timeframe — string used for fetchKlines
    // and HTF derivation. Default "1h" (was hardcoded "15m" pre-v90). To revert
    // set env PRIMARY_TF=15m. Must match SignalSender.PRIMARY_TF.
    public static final String PRIMARY_TF =
            System.getenv().getOrDefault("PRIMARY_TF", "1h").trim();
    // [v86.91] 4h-support helpers (duplicated per-file by design — no new classes).
    private static int    tfMin(String tf)      { return "15m".equals(tf)?15 : "30m".equals(tf)?30 : "4h".equals(tf)?240 : 60; }
    private static int    barsPerDay(String tf) { return "15m".equals(tf)?96 : "30m".equals(tf)?48 : "4h".equals(tf)?6  : 24; }
    private static String htfFast(String tf)    { return "15m".equals(tf)?"1h" : "30m".equals(tf)?"4h" : "4h".equals(tf)?"1d" : "4h"; }
    private static long   tfBarMs(String tf)     { return tfMin(tf)*60_000L; }
    // [v86.91] 4h → HTF_FAST=1d (was falling to the 15m "1h" arm).
    public static final String HTF_FAST =
            System.getenv().getOrDefault("HTF_FAST",
                    "1h".equals(PRIMARY_TF) ? "4h" : "30m".equals(PRIMARY_TF) ? "4h" : "4h".equals(PRIMARY_TF) ? "1d" : "1h").trim();
    public static final boolean PRIMARY_IS_15M = "15m".equals(PRIMARY_TF);
    // [v86.91] 4h → 14_400_000 ms (was falling to the 15m else-branch).
    public static final long PRIMARY_TF_MS = "1h".equals(PRIMARY_TF) ? 60 * 60_000L
            : "4h".equals(PRIMARY_TF) ? 4 * 60 * 60_000L
            : "30m".equals(PRIMARY_TF) ? 30 * 60_000L : 15 * 60_000L;

    // [v78.1] Paper/observation mode. When OBSERVATION_MODE=1, signals reach
    // Telegram tagged 🧪 [PAPER]. Calibrator still records outcomes, so the bot
    // learns without budget risk. Required for first 50+ outcomes before any
    // real-money consideration.
    public static final boolean OBSERVATION_MODE =
            "1".equals(System.getenv().getOrDefault("OBSERVATION_MODE", "0"));

    // [v83 PHASE-3] Live auto-trading flag. Default OFF (paper-only).
    // To enable: BOT_AUTO_TRADE=1 in Railway env. Requires OBSERVATION_MODE=0
    // (mutually exclusive). Even when ON, it goes through RiskGuard.canTrade()
    // first — daily loss limit, BTC crash, trade limits all block automatically.
    // Default starts on TESTNET (BINANCE_USE_TESTNET=1). To use real money you
    // must explicitly set BINANCE_USE_TESTNET=0 with valid real API keys.
    public static final boolean AUTO_TRADE_ENABLED =
            "1".equals(System.getenv().getOrDefault("BOT_AUTO_TRADE", "0"));
    // [v87.5] Legacy candle-strategy backtests (startup + periodic + walk-forward). The candle strategy
    // is settled-DEAD; OFF by default now to silence the −142% noise/HARD-FAIL spam and focus the bot on
    // the breakout tracker (#5). Live candle scan stays (paper, mostly silent) — only the backtests are
    // gated. Re-enable everything with LEGACY_BT=1. NOT a delete — fully reversible.
    public static final boolean LEGACY_BT =
            "1".equals(System.getenv().getOrDefault("LEGACY_BT", "0"));

    // [v86.35] HARD live-trading kill switch — default OFF (DISARMED). While we validate the edge
    // (honest backtest + live verifier), the bot analyzes, sends signals, and the verifier
    // accumulates Live WR — but NO real trades are executed. Sits ON TOP of BOT_AUTO_TRADE /
    // OBSERVATION_MODE. Arm only AFTER results are proven: set LIVE_TRADING_ARMED=1 in Railway.
    public static final boolean LIVE_TRADING_ARMED =
            "1".equals(System.getenv().getOrDefault("LIVE_TRADING_ARMED", "0"));

    // [v86.96] New-listings catcher (observation-only). Detects freshly listed
    // USDT-M futures symbols (exchangeInfo diff vs a persisted known-set) and
    // records their first-N-hours microstructure (1m klines + funding + L1 spread)
    // to ./data for edge research. NEVER trades — sits entirely outside the
    // signal/execution path. Default ON; disable with NEW_LISTING_CATCHER=0.
    // Implemented in SignalSender.checkNewListings().
    public static final boolean NEW_LISTING_CATCHER =
            !"0".equals(System.getenv().getOrDefault("NEW_LISTING_CATCHER", "1"));

    // [v86.99] Funding-extreme snapshot (hypothesis #2, observation-only). Each cycle snapshots
    // symbols with extreme funding to ./data + Supabase for edge research. NEVER trades. Default ON;
    // disable with FUNDING_SNAPSHOT=0. Implemented in SignalSender.snapshotFundingExtremes().
    public static final boolean FUNDING_SNAPSHOT =
            !"0".equals(System.getenv().getOrDefault("FUNDING_SNAPSHOT", "1"));

    // [v87.0] Liquidation capture (hypothesis #3, observation-only). Buffers liquidation events
    // from the existing WS stream and flushes a batch per cycle to ./data + Supabase. NEVER trades.
    // Default ON; disable with LIQ_CAPTURE=0. Implemented in SignalSender (processLiquidationEvent
    // buffers, flushLiquidations drains).
    public static final boolean LIQ_CAPTURE =
            !"0".equals(System.getenv().getOrDefault("LIQ_CAPTURE", "1"));
    // [v87.4] Breakout trend tracker (hypothesis #5, observation-only). The ONLY strategy that survived
    // offline validation (daily Donchian breakout + trailing = regime-dependent trend premium). Logs
    // fresh daily breakout signals for forward-testing; outcomes computed offline. Default ON; LIQ-style.
    // Disable with TREND_TRACK=0. Implemented in SignalSender.trackBreakoutSignals().
    public static final boolean TREND_TRACK =
            !"0".equals(System.getenv().getOrDefault("TREND_TRACK", "1"));

    // [v79 I5] Cross-exchange price validation. Compares Binance kline last close
    // with Bybit/OKX. If discrepancy >0.5% → log warning + dispatch blocked.
    // Default OFF — Binance is generally trustworthy, but available for paranoid setups.
    public static final boolean CROSS_EXCHANGE_VALIDATION =
            "1".equals(System.getenv().getOrDefault("CROSS_EXCHANGE_VALIDATION", "0"));

    // [v90] KLINES default scales with PRIMARY_TF.
    //   15m primary: 420 bars = 4.4 days
    //   1h primary:  168 bars = 7 days (more history available, fewer bars needed)
    private static final int KLINES = envIntAny(
            PRIMARY_IS_15M ? 420 : 168, "KLINES", "KLINES_LIMIT");
    // Max signals per scan cycle. ISC + RiskGuard.MAX_CONCURRENT_POSITIONS
    // remain authoritative limiters on actual trading.
    private static final int MAX_SIGNALS_PER_CYCLE = envInt("MAX_SIGNALS_PER_CYCLE", 5);

    // Time-stop window for the verifier — single source of truth across bot.
    // [v86.5] Scales with PRIMARY_TF to MIRROR the real hold. Was hardcoded
    // 90 min (6×15m). On 1h the real position (PositionTracker PT_TIME_STOP_MS,
    // v86.3) and the backtest both hold up to 480 min (8h); the old 90 min judged
    // every 1h signal after ~2h → false TIME_STOP losses → corrupted "Live WR"
    // AND fed premature losses to the calibrator (which can suppress future live
    // signals via the calibration gate). Now a 1h signal gets the same 480 min
    // the bot/backtest actually give it. Grace 30 min covers exact-close hits.
    // [v86.91] 4h → 1440 min (24h), the hard invariant; CRITICAL — was 90 min (else-branch),
    // which would force-resolve every 4h signal after ~2h as a false TIME_STOP.
    private static final long VERIFIER_TIME_STOP_MS =
            "1h".equals(PRIMARY_TF) ? 480L * 60_000L
            : "4h".equals(PRIMARY_TF) ? 1440L * 60_000L
            : "30m".equals(PRIMARY_TF) ? 300L * 60_000L : 90L * 60_000L;
    private static final long VERIFIER_GRACE_MS     = 30 * 60_000L;
    private static final long VERIFIER_MAX_AGE_MS   = VERIFIER_TIME_STOP_MS + VERIFIER_GRACE_MS;

    private static final Map<String, String> SECTOR_LEADERS = new LinkedHashMap<>() {{
        put("DOGEUSDT", "MEME"); put("SOLUSDT", "L1"); put("UNIUSDT", "DEFI");
        put("LINKUSDT", "INFRA"); put("ETHUSDT", "TOP"); put("XRPUSDT", "PAYMENT");
        put("AVAXUSDT", "L1"); put("BNBUSDT", "CEX");
    }};

    private static final AtomicLong totalCycles  = new AtomicLong(0);
    private static final AtomicLong totalSignals = new AtomicLong(0);
    private static final AtomicLong errorCount   = new AtomicLong(0);
    private static final AtomicLong forecastSeq  = new AtomicLong(0);
    private static long startTimeMs = 0;

    private static final int  CB_THRESHOLD = 10;
    private static final long CB_WINDOW_MS = 10 * 60_000L;
    private static final long CB_PAUSE_MS  = 60_000L;
    private static volatile long lastErrorWindowStart = 0;
    private static final AtomicInteger errorsInWindow = new AtomicInteger(0);
    private static volatile long cbPauseUntil = 0;

    private static volatile long lastSignalMs       = 0;
    private static volatile long lastCycleSuccessMs = 0;
    private static volatile long lastStatsSuccessMs = 0;
    // [v9.9 2026-05-29] SIGNAL_DROUGHT 3h → 12h — reduce spam, тихий рынок не новость
    private static final long SIGNAL_DROUGHT_MS = 12 * 60 * 60_000L;
    private static final AtomicBoolean droughtAnnounced = new AtomicBoolean(false);
    private static volatile long lastInfraAlertMs = 0;
    private static final long INFRA_ALERT_COOLDOWN_MS = 60 * 60_000L;
    private static final AtomicLong watchdogAlerts = new AtomicLong(0);

    private static final int MAX_FORECAST_RECORDS = 500;
    static final ConcurrentHashMap<String, ForecastRecord> forecastRecords = new ConcurrentHashMap<>();

    // [v79 I7] Forecast records persistence — survive restarts so unresolved
    // signals don't disappear from the audit trail.
    private static final String FORECAST_PERSIST_FILE = System.getenv()
            .getOrDefault("FORECAST_RECORDS_FILE", "./data/forecast_records.csv");

    // [v86.62] Версия бота для баннеров. Годами лгала как "v80.0-RESTORED+5%" в
    // boot-логе и заголовке сводки бектеста, ломая сравнение сводок между версиями
    // (сводка прямо говорит «цифра — для сравнения версий»). Поднимать при каждом
    // versioned-коммите. БЕЗ символа '%' — строка попадает в format-шаблон.
    static final String BOT_VERSION = "v88.2.2";   // package-private so SignalSender can show the live version in Telegram

    static final class ForecastRecord {
        final String symbol;
        final com.bot.TradingCore.Side side;
        final double entryPrice;
        final String forecastBias;
        final double forecastScore;
        final double signalProbability;
        final double robustAtrPctAtSignal;
        final double tp1Level;
        final double slLevel;
        final long   createdAt;
        // [v79 I3] Snapshot BTC regime at signal time — passed to calibrator
        // for regime-segmented learning.
        final String btcRegimeAtSignal;
        volatile boolean resolved = false;
        volatile String  actualOutcome = null;
        final AtomicBoolean counted = new AtomicBoolean(false);

        ForecastRecord(String sym, com.bot.TradingCore.Side side, double price,
                       String bias, double score, double signalProb, double robustAtrPct,
                       double tp1, double sl, String btcRegime) {
            this.symbol = sym; this.side = side; this.entryPrice = price;
            this.forecastBias = bias; this.forecastScore = score;
            this.signalProbability = signalProb;
            this.robustAtrPctAtSignal = robustAtrPct;
            this.tp1Level = tp1; this.slLevel = sl;
            this.btcRegimeAtSignal = btcRegime != null ? btcRegime : "UNKNOWN";
            this.createdAt = System.currentTimeMillis();
        }
        long ageMs() { return System.currentTimeMillis() - createdAt; }

        private static String escape(String s) {
            if (s == null) return "";
            return s.replace(";", ",").replace("\n", " ");
        }
    }

    private static final AtomicInteger forecastTotal     = new AtomicInteger(0);
    private static final AtomicInteger forecastCorrect   = new AtomicInteger(0);
    // [v86.59] Live-исходы в разрезе «режим BTC · сторона» (например NEUTRAL·S).
    // Отвечает на вопрос юзера «не сливают ли опять шорты»: все 5 первых live-лузов
    // были шортами в мёртвом флэте BTC — этот разрез покажет паттерн данными, а не
    // ощущением, и станет основой для решений (например флэт-вето), которые бектест
    // проверить НЕ может (он не воспроизводит исторический режим BTC).
    // key → int[2]{total, wins}; персистится в forecast-файле (#REGIME| строки).
    private static final Map<String, int[]> regimeOutcomes = new ConcurrentHashMap<>();
    // [v79 I1] AMBIGUOUS counted separately — visible in stats so user can
    // see "out of 100 signals: 60 wins, 30 losses, 10 ambiguous half-credit".
    private static final AtomicInteger forecastAmbiguous = new AtomicInteger(0);
    // [v79 I2] TIME_STOP / FLAT — counted as losses but tracked separately
    // so user can distinguish "real SL" from "didn't move".
    private static final AtomicInteger forecastTimeStop  = new AtomicInteger(0);

    private static final class SignalOutcome {
        final String symbol; final double confidence; final String category;
        final boolean hit; final long ts;
        SignalOutcome(String s, double c, String cat, boolean h) {
            symbol = s; confidence = c; category = cat; hit = h;
            ts = System.currentTimeMillis();
        }
    }

    private static final ConcurrentLinkedDeque<SignalOutcome> signalOutcomes = new ConcurrentLinkedDeque<>();
    private static final int SIGNAL_OUTCOME_WINDOW = 200;


    static final ConcurrentHashMap<String, TrackedSignal> trackedSignals = new ConcurrentHashMap<>();

    static final class TrackedSignal {
        final String symbol;
        final com.bot.TradingCore.Side side;
        final double entry, sl, tp1, tp2, tp3;
        final long createdAt;
        final String forecastBias;
        final double forecastScore;
        final boolean sentToUser;

        volatile boolean tp1Hit = false;
        volatile boolean tp2Hit = false;
        volatile boolean timeStopNotified = false;
        volatile double  trailingStop = 0;
        volatile boolean chandelierActive = false;

        private double extremeLow  = Double.MAX_VALUE;
        private double extremeHigh = Double.NEGATIVE_INFINITY;
        private final Object extremeLock = new Object();

        TrackedSignal(String sym, com.bot.TradingCore.Side side,
                      double entry, double sl, double tp1, double tp2, double tp3,
                      String forecastBias, double forecastScore, boolean sentToUser) {
            this.symbol = sym; this.side = side; this.entry = entry;
            this.sl = sl; this.tp1 = tp1; this.tp2 = tp2; this.tp3 = tp3;
            this.forecastBias = forecastBias; this.forecastScore = forecastScore;
            this.sentToUser = sentToUser;
            this.createdAt = System.currentTimeMillis();
        }
        long ageMs() { return System.currentTimeMillis() - createdAt; }
        double getExtremeLow()  { synchronized (extremeLock) { return extremeLow; } }
        double getExtremeHigh() { synchronized (extremeLock) { return extremeHigh; } }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  DISPATCHER — the only gateway for trade-signal → Telegram.
    // ═══════════════════════════════════════════════════════════════════

    // ═══════════════════════════════════════════════════════════════════
    // [v79 I5] Cross-exchange validation helper.
    // Best-effort: if Bybit/OKX unreachable, we DON'T block — failing safe
    // toward "trust Binance" rather than blocking real signals on infra issues.
    //
    // RETURN CONTRACT:
    //   null              = ok, signal can pass
    //   non-null string   = block reason for logging / Result.blocked()
    //
    // Никаких отдельных классов — простой контракт через nullable String.
    // ═══════════════════════════════════════════════════════════════════
    private static final ConcurrentHashMap<String, Long> xCheckLastErrorMs = new ConcurrentHashMap<>();


    private static Runnable safe(String name, Runnable task) {
        return () -> {
            try { task.run(); }
            catch (Throwable t) {
                errorCount.incrementAndGet();
                boolean isTransient = t instanceof java.io.IOException ||
                        t instanceof java.net.http.HttpTimeoutException ||
                        t.getClass().getSimpleName().toLowerCase().contains("json") ||
                        (t.getMessage() != null && (t.getMessage().contains("502") || t.getMessage().contains("timeout")));
                if (!isTransient) errorsInWindow.incrementAndGet();
                LOG.log(Level.SEVERE, "[SAFE] Task '" + name + "' FAILED: " + t.getMessage(), t);
            }
        };
    }

    // ═══════════════════════════════════════════════════════════════════
    //  MAIN
    // ═══════════════════════════════════════════════════════════════════
    public static void main(String[] args) {
        try {
            System.setOut(new java.io.PrintStream(System.out, true, "UTF-8"));
            System.setErr(new java.io.PrintStream(System.err, true, "UTF-8"));
        } catch (java.io.UnsupportedEncodingException ignored) {}

        configureLogger();
        resolveTimezoneAsync();

        startTimeMs          = System.currentTimeMillis();
        lastErrorWindowStart = startTimeMs;
        lastSignalMs         = startTimeMs;
        lastCycleSuccessMs   = startTimeMs;
        lastStatsSuccessMs   = startTimeMs;

        final com.bot.TelegramBotSender telegram = new com.bot.TelegramBotSender(TG_TOKEN, CHAT_ID);
        final com.bot.SignalSender sender         = new com.bot.SignalSender(telegram);

        // [project_state id=14] Calibrator dashboard removed. Nothing has ever called
        // recordOutcome*() — the write path was dead — so loading the CSV, auto-saving it
        // and reporting its outcome count only dressed an empty file up as evidence of
        // learning. Outcome accounting belongs to the paper-harness, which has
        // pre-registration and an HMAC chain by construction (project_state id=17); running
        // two accounting systems side by side would only let them drift apart.
        // CALIBRATOR_FILE and RESET_CALIBRATOR_ON_BOOT are no longer read.

        com.bot.DecisionEngineMerged.USER_ZONE = ZONE;

        ScheduledExecutorService mainSched = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "TradingBot-Main");
            t.setDaemon(false);
            t.setUncaughtExceptionHandler((th, ex) ->
                    LOG.log(Level.SEVERE, "UNCAUGHT in " + th.getName(), ex));
            return t;
        });
        ScheduledExecutorService auxSched = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "TradingBot-Aux");
            t.setDaemon(true);
            return t;
        });
        ExecutorService heavySched = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "TradingBot-Heavy");
            t.setDaemon(true);
            return t;
        });

        // [supabase-bridge] Our-logic executor path: drain the Supabase bot_orders queue.
        // Additive + DEFAULT-OFF (SUPABASE_BRIDGE_ENABLED=0) + testnet-locked → existing
        // candle behaviour is completely unchanged unless explicitly switched on.
        if (SupabaseSignalBridge.isEnabled()) {
            mainSched.scheduleAtFixedRate(
                    safe("SupabaseBridge", SupabaseSignalBridge.getInstance()::poll),
                    120, 30, TimeUnit.SECONDS);
            LOG.info("[BOOT] SupabaseSignalBridge ENABLED — draining bot_orders (testnet="
                    + System.getenv().getOrDefault("BINANCE_USE_TESTNET", "0") + ")");
        }

        // [data-catchers] Keep the Supabase data feeds alive: nl_micro (new listings),
        // funding_snaps (funding extremes), trend_signals (breakouts). These feed the
        // LIVE Supabase crons. The candle signal brain is gone, but the data pipeline stays.
        auxSched.scheduleAtFixedRate(
                safe("DataCatchers", () -> {
                    sender.checkNewListings();
                    sender.snapshotFundingExtremes();
                    sender.trackBreakoutSignals();
                }),
                60, INTERVAL * 60L, TimeUnit.SECONDS);

        auxSched.scheduleAtFixedRate(
                safe("TimeSync", sender::syncServerTime),
                5, 120, TimeUnit.MINUTES);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("═══ Shutdown. Cycles: " + totalCycles.get()
                    + " | Signals: " + totalSignals.get() + " ═══");
            mainSched.shutdown(); auxSched.shutdown(); heavySched.shutdown();
            try { mainSched.awaitTermination(4, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            mainSched.shutdownNow();
            telegram.flushAndShutdown(8000);
        }, "ShutdownHook"));

        LOG.info("═══ TradingBot " + BOT_VERSION + " started " + nowLocalStr()
                + " (first cycle in 90s, OBSERVATION_MODE="
                + (OBSERVATION_MODE ? "ON/PAPER" : "OFF/LIVE")
                + ", X-EXCHANGE_CHECK=" + (CROSS_EXCHANGE_VALIDATION ? "ON" : "OFF") + ") ═══");

        // [project_state id=22] RiskGuard is instantiated here and NOWHERE ELSE calls it.
        // canTrade / recordTradeOpened / recordTradeClosed / updateBtcPrice are unreachable
        // from every live root, so the daily and weekly loss limits, MAX_CONCURRENT_POSITIONS,
        // the BTC-crash block and the cooldowns constrain NOTHING today. The old comment here
        // said RiskGuard "only gets wired into BotMain.Dispatcher in PHASE-3" — that Dispatcher
        // was deleted in v88.2, so the wiring it was waiting for is never coming.
        // Wiring it to SupabaseSignalBridge.drainOpens() is project_state id=25, on its own
        // branch. Until then this line is a status print, not a guarantee.
        try {
            com.bot.RiskGuard rg = com.bot.RiskGuard.getInstance();
            LOG.info("[BOOT] RiskGuard (NOT ENFORCED — see project_state id=25): " + rg.statusLine());
        } catch (Throwable t) {
            LOG.warning("[BOOT] RiskGuard init failed: " + t.getMessage());
        }

        // Initialize the executor. It is a no-op until BOT_AUTO_TRADE=1 AND OBSERVATION_MODE=0.
        // The only limit that actually applies on the live path is BRIDGE_MAX_OPEN inside
        // SupabaseSignalBridge; RiskGuard.canTrade() is NOT consulted (project_state id=22).
        try {
            com.bot.BinanceTradeExecutor ex = com.bot.BinanceTradeExecutor.getInstance();
            // Wire emergency-close failure alerts to Telegram. If SL placement
            // fails AND emergencyClose can't close the position, operator gets
            // alerted immediately.
            com.bot.BinanceTradeExecutor.setEmergencyAlertSink(msg -> {
                try { telegram.sendMessageAsync(msg); } catch (Throwable ignored) {}
            });
            if (AUTO_TRADE_ENABLED && !OBSERVATION_MODE && ex.isReady() && !LIVE_TRADING_ARMED) {
                // [v88.2.2] LIVE_TRADING_ARMED gates only the (deleted) candle path — the
                // SupabaseSignalBridge has its own gates (BRIDGE_ALLOW_REAL et al). The old
                // "торговля ВЫКЛЮЧЕНА" TG banner was misleading while the bridge trades real.
                LOG.info("[BOOT] Legacy candle auto-trade path disarmed (LIVE_TRADING_ARMED=0) — "
                        + "irrelevant since v88.2: execution goes through SupabaseSignalBridge.");
                telegram.sendMessageAsync(
                        "🤖 *Бот v88.2.2 запущен* (исполнитель)\n" +
                                "Сделки открываются ТОЛЬКО из очереди ордеров (Supabase → мост → Binance"
                                + (SupabaseSignalBridge.isEnabled() ? ", мост ВКЛЮЧЁН" : ", мост выключен") + ").\n" +
                                "_Стратегии пишут в очередь после прохождения своих гейтов._");
            } else if (AUTO_TRADE_ENABLED && !OBSERVATION_MODE && ex.isReady()) {
                LOG.info("[BOOT] Auto-trade ENABLED. Mode: "
                        + (ex.isTestnet() ? "TESTNET" : "REAL/LIVE")
                        + " leverage=" + ex.getLeverage()
                        + "x risk=" + ex.getRiskPct() + "%");
                // [project_state id=22] The RiskGuard limits used to be printed here, which
                // advertised protection that is not applied to any trade — canTrade() is never
                // called. Stating them is worse than silence, so the banner now reports only
                // what is actually enforced.
                telegram.sendMessageAsync(String.format(
                        "🤖 *Auto-trade АКТИВИРОВАН*\n" +
                                "Режим: %s\n" +
                                "Плечо: %dx | Риск: %.1f%%/сделка\n" +
                                "⚠️ Риск-лимиты RiskGuard НЕ применяются (project_state id=25).\n" +
                                "Единственное ограничение на живом пути — не более %d открытых позиций из очереди.",
                        ex.isTestnet() ? "🧪 TESTNET" : "🔴 REAL/LIVE",
                        ex.getLeverage(), ex.getRiskPct(),
                        (int) SupabaseSignalBridge.maxOpen()));
            } else if (AUTO_TRADE_ENABLED && OBSERVATION_MODE) {
                LOG.warning("[BOOT] BOT_AUTO_TRADE=1 but OBSERVATION_MODE=1 — paper wins, no live trades.");
            } else if (AUTO_TRADE_ENABLED && !ex.isReady()) {
                LOG.warning("[BOOT] BOT_AUTO_TRADE=1 but Binance API keys missing — auto-trade DISABLED.");
                telegram.sendMessageAsync(
                        "⚠️ *Auto-trade НЕ запустился*\n" +
                                "Причина: API ключи Binance не настроены.\n" +
                                "Нужны env: `BINANCE_TESTNET_API_KEY` + `BINANCE_TESTNET_API_SECRET` (для testnet)\n" +
                                "или `BINANCE_API_KEY` + `BINANCE_API_SECRET` (для real).");
            } else {
                LOG.info("[BOOT] Auto-trade disabled (BOT_AUTO_TRADE="
                        + (AUTO_TRADE_ENABLED ? "1" : "0")
                        + " OBSERVATION_MODE=" + (OBSERVATION_MODE ? "1" : "0") + ").");
            }
        } catch (Throwable t) {
            LOG.warning("[BOOT] Executor/Tracker init failed: " + t.getMessage());
        }


        // [v87.6] NEW-strategy backtest summary (breakout): replaces the disabled candle one. Honest —
        // per-year + survivorship caveat in the message; live green-light still requires the forward test.
        // Disable with BREAKOUT_BT=0.
        try {
            if (!"0".equals(System.getenv().getOrDefault("BREAKOUT_BT", "1"))) {
                heavySched.submit(safe("BreakoutBT", () -> sender.runBreakoutBacktest(telegram)));
            }
        } catch (Throwable t) {
            LOG.warning("[BREAKOUT-BT] init failed: " + t.getMessage());
        }

    }





    // ═══════════════════════════════════════════════════════════════════
    // [v79] checkForecastAccuracy — REWRITTEN with full outcome accounting.
    //   Старая версия игнорировала AMBIGUOUS и FLAT/TIME_STOP outcomes →
    //   калибратор недополучал данные → шёл вечный cold-start.
    //   Новая версия:
    //     • TP1 hit → calibrator: hit=true, weight=1.0, tag=TP1
    //     • SL hit → calibrator: hit=false, weight=1.0, tag=SL
    //     • AMBIGUOUS (TP and SL in same bar) → hit=true, weight=0.5, tag=AMBIGUOUS
    //     • TIME_STOP / FLAT → hit=false, weight=1.0, tag=TIME_STOP
    //     • MOVED_UP/DOWN → hit=(matches bias), weight=1.0
    //   В audit log пишется КАЖДЫЙ outcome с full payload + HMAC.
    // ═══════════════════════════════════════════════════════════════════

    // ═══════════════════════════════════════════════════════════════════
    // [v79 I7] Forecast records persistence — survive Railway restarts.
    // ═══════════════════════════════════════════════════════════════════

    // ═══════════════════════════════════════════════════════════════════
    // [v79 I6] Daily integrity report — public-verifiable stats sent to Telegram.
    //   Если бот говорит "WR=65%", цифра должна совпадать с тем, что
    //   калибратор хранит в state. Эта функция показывает обе цифры
    //   рядом — если они расходятся, видно сразу.
    // ═══════════════════════════════════════════════════════════════════
    private static volatile long lastIntegrityReportMs = 0;

    /** [v79 I6] Public API for third-party verification. Returns JSON-ish string. */


    static volatile String lastBtcRegimeForAlert = null;

    private static volatile long lastHeartbeatMs = 0;
    // [v9.9 2026-05-29] HEARTBEAT 90 min → 6h. Раньше каждые 90мин в тихом
    // рынке шло "💓 Heartbeat" = spam без actionable info. Теперь только
    // каждые 6h, и только если 6h без signals. Реальная информация о трейдах
    // приходит через notifyTradeOutcome() при resolution каждого signal.
    private static final long HEARTBEAT_INTERVAL_MS = 6 * 60 * 60_000L;
    private static final long HEARTBEAT_QUIET_MS    = 6 * 60 * 60_000L;



    // ═══════════════════════════════════════════════════════════════════════
    // [STARTUP-BACKTEST 2026-05-02 / FIX-1 2026-05-02]
    // Full one-shot backtest at boot. Pre-trains the ProbabilityCalibrator
    // on historical data across the entire trading universe so that LIVE
    // signals start with calibrated probabilities, not Cal=0.
    //
    // [FIX-1] Separates technical failures from "weak strategy" verdict.
    // Previous version flagged 🔴 even when 0 pairs ran due to rate-limits.
    // Plus: 3-min boot delay, 15m-only fetch (3× less weight), 4-sec pacing.
    // ═══════════════════════════════════════════════════════════════════════


    /**
     * [PATCH B 2026-05-22] Fetch klines with backwards pagination.
     *
     * Binance Futures /fapi/v1/klines max limit = 1500 per request.
     * For backtest we may need 8640 5m bars (30 days) or 43200 1m bars.
     * This helper fetches in 1500-bar chunks, walking backwards using endTime.
     *
     * Returns oldest-first list, may be shorter than requested if pair is young.
     *
     * Pacing: 600ms between requests (well under Binance rate-limit).
     * Total time at 8640 bars = 6 requests × 600ms = ~3.6s per pair.
     */






    private static String nowLocalStr() {
        return ZonedDateTime.now(ZONE)
                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"));
    }


    private static void resolveTimezoneAsync() {
        String envTz = System.getenv("TIMEZONE");
        if (envTz != null && !envTz.isBlank()) {
            try {
                ZONE = ZoneId.of(envTz.trim());
                com.bot.DecisionEngineMerged.USER_ZONE = ZONE;
                return;
            } catch (Exception ignored) {}
        }
        Thread tzThread = new Thread(() -> {
            try {
                java.net.URI uri = java.net.URI.create("http://ip-api.com/json?fields=timezone,city");
                java.net.HttpURLConnection conn = (java.net.HttpURLConnection) uri.toURL().openConnection();
                conn.setConnectTimeout(3000);
                conn.setReadTimeout(3000);
                conn.setRequestMethod("GET");
                if (conn.getResponseCode() == 200) {
                    try (java.io.InputStream is = conn.getInputStream();
                         java.util.Scanner sc = new java.util.Scanner(is, "UTF-8")) {
                        String body = sc.useDelimiter("\\A").hasNext() ? sc.next() : "";
                        int idx = body.indexOf("\"timezone\"");
                        if (idx >= 0) {
                            int q1 = body.indexOf('"', idx + 10) + 1;
                            int q2 = body.indexOf('"', q1);
                            if (q1 > 0 && q2 > q1) {
                                ZoneId z = ZoneId.of(body.substring(q1, q2));
                                ZONE = z;
                                com.bot.DecisionEngineMerged.USER_ZONE = z;
                            }
                        }
                    }
                }
                conn.disconnect();
            } catch (Exception ignored) {}
        }, "tz-resolver");
        tzThread.setDaemon(true);
        tzThread.start();
    }

    private static int envInt(String k, int d) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) return d;
        // [v86.93] .trim(): a stray space in a Railway value (e.g. "6000 ") used to throw in
        // parseInt → catch → silent fallback to default. That hid STARTUP_BT_BARS_PRIMARY=6000.
        try { return Integer.parseInt(v.trim()); }
        catch (Exception e) { return d; }
    }


    private static int envIntAny(int defaultValue, String... keys) {
        for (String k : keys) {
            String v = System.getenv(k);
            if (v != null && !v.isBlank()) {
                try { return Integer.parseInt(v.trim()); }
                catch (NumberFormatException ignored) {}
            }
        }
        return defaultValue;
    }


    private static void configureLogger() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        for (Handler h : root.getHandlers()) {
            h.setFormatter(new SimpleFormatter() {
                private final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss");
                @Override
                public String format(LogRecord r) {
                    return String.format("[%s][%-7s] %s%n",
                            ZonedDateTime.now(ZONE).format(fmt),
                            r.getLevel(), r.getMessage());
                }
            });
        }
    }
}