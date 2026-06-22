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
 *     [I1] AMBIGUOUS outcomes теперь ЗАПИСЫВАЮТСЯ в калибратор с весом 0.5
 *          (раньше игнорировались → искусственно завышенный win-rate).
 *     [I2] TIME_STOP/FLAT outcomes теперь ЗАПИСЫВАЮТСЯ в калибратор как
 *          LOSS с весом 1.0 (раньше выпадали из статистики совсем).
 *     [I3] Все outcomes идут в append-only audit log с HMAC-SHA256.
 *          Запись через DecisionEngineMerged.getCalibrator().recordOutcome*().
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
 *     ✔ Нет cryptographic proof — ИСПРАВЛЕНО [I3] (HMAC в калибраторе)
 *     ✔ Backtester / verifier мismatch — ИСПРАВЛЕНО [I4]
 *     ✔ Нет cross-exchange validation — ИСПРАВЛЕНО [I5] (опц.)
 *   ЛОЖЬ (аудит ИИ ошибался):
 *     ✘ "ProbabilityCalibrator не в коде" — он ЕСТЬ, DecisionEngineMerged.java:5048
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
    private static final String BOT_VERSION = "v87.3";

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

        /** [v79 I7] CSV serialization for persistence. */
        String toCsvLine() {
            return String.join(";",
                    escape(symbol),
                    side == null ? "" : side.name(),
                    Double.toString(entryPrice),
                    escape(forecastBias),
                    Double.toString(forecastScore),
                    Double.toString(signalProbability),
                    Double.toString(robustAtrPctAtSignal),
                    Double.toString(tp1Level),
                    Double.toString(slLevel),
                    escape(btcRegimeAtSignal),
                    Long.toString(createdAt),
                    Boolean.toString(resolved),
                    actualOutcome == null ? "" : escape(actualOutcome),
                    Boolean.toString(counted.get()));
        }
        private static String escape(String s) {
            if (s == null) return "";
            return s.replace(";", ",").replace("\n", " ");
        }
        static ForecastRecord fromCsvLine(String line) {
            try {
                String[] p = line.split(";", -1);
                if (p.length < 14) return null;
                ForecastRecord fr = new ForecastRecord(
                        p[0], com.bot.TradingCore.Side.valueOf(p[1]),
                        Double.parseDouble(p[2]), p[3], Double.parseDouble(p[4]),
                        Double.parseDouble(p[5]), Double.parseDouble(p[6]),
                        Double.parseDouble(p[7]), Double.parseDouble(p[8]),
                        p[9]);
                java.lang.reflect.Field cf = ForecastRecord.class.getDeclaredField("createdAt");
                cf.setAccessible(true);
                cf.setLong(fr, Long.parseLong(p[10]));
                fr.resolved = Boolean.parseBoolean(p[11]);
                fr.actualOutcome = p[12].isEmpty() ? null : p[12];
                if (Boolean.parseBoolean(p[13])) fr.counted.set(true);
                return fr;
            } catch (Throwable ignored) { return null; }
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

    public static void recordSignalOutcome(String sym, double conf, String cat, boolean hit) {
        signalOutcomes.addLast(new SignalOutcome(sym, conf, cat, hit));
        while (signalOutcomes.size() > SIGNAL_OUTCOME_WINDOW) signalOutcomes.pollFirst();
    }

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
        void updateExtremes(double low, double high) {
            synchronized (extremeLock) {
                extremeLow  = Math.min(extremeLow,  low);
                extremeHigh = Math.max(extremeHigh, high);
            }
        }
        double getExtremeLow()  { synchronized (extremeLock) { return extremeLow; } }
        double getExtremeHigh() { synchronized (extremeLock) { return extremeHigh; } }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  DISPATCHER — the only gateway for trade-signal → Telegram.
    // ═══════════════════════════════════════════════════════════════════
    public static final class Dispatcher {

        private static final int    COLD_START_MIN_OUTCOMES = 20;
        private static final double MIN_RR          = 2.00;
        // [2026-05-25] SL min 0.70% → 0.50%. TrendPullback использует swing-based стопы
        // от 0.5%, старый порог резал тонкие но валидные сетапы.
        private static final double MIN_SL_PCT      = 0.0050;
        // [v80] Dedup 6 мин → 4 часа. Защита от повторных входов в ту же пару.
        private static final long   SYMBOL_DEDUP_MS = 4 * 60 * 60_000L;
        // [v80] Часовая квота 20 → 6. Цель: 1-3 качественных сигнала в день, не 20 средненьких.
        private static final int    MAX_PER_HOUR    = 6;
        private static final int    MAX_PER_5MIN    = 2;
        private static final long   HOUR_MS         = 60 * 60_000L;
        private static final long   FIVE_MIN_MS     = 5 * 60_000L;
        // [v80] QUALITY GATE thresholds — после warmup
        // [v85 FLAT-FIX] Static defaults preserved for back-compat & strict (TREND) regime.
        private static final double MIN_CONFIDENCE_AFTER_WARMUP = 65.0;
        private static final int    MIN_CLUSTERS_AFTER_WARMUP   = 3;

        // [v85 FLAT-FIX] Regime-aware thresholds.
        //
        // Проблема: единый порог 65/3 параличует бота в NEUTRAL-флэте, где
        // 4-кластерная модель физически не может собрать 3 согласных
        // кластера (Volume низкий, Momentum около нуля). За 1024 цикла —
        // 0 сигналов. На вчерашнем трендовом дне — 3 сигнала с prob 65-67%.
        //
        // Решение: пороги масштабируются по силе BTC trend.
        //   STRONG_TREND  (|str| >= 0.50)  → 65/3 строго (как было)
        //   WEAK_TREND    (|str| 0.30-0.50)→ 60/2 умеренно
        //   FLAT/NEUTRAL  (|str| < 0.30)   → 56/2 + extra checks
        //
        // Защита качества во флэте:
        //   - R:R на TP2 ≥ 2.2 (было 2.0) — компенсация WR risk premium
        //   - SL ≥ 0.85% (было 0.70%) — на флэте noise шире
        //   - max 2 signals/hour, 4/day (вместо 6/h, no daily cap)
        //
        // Это НЕ "ослабление" — это адаптация к режиму. Если бот не торгует
        // вообще, edge=0 by design. Если торгует на флэте плохо подобранными
        // сетапами — edge<0. Цель: торговать средне-качественно тогда, когда
        // высоко-качественных сетапов нет физически.
        private static final double FLAT_MARKET_MIN_CONF      = 55.0;   // [PATCH 2026-05-13] was 56.0 — match MIN_CONF env=55
        private static final int    FLAT_MARKET_MIN_CLUSTERS  = 2;
        private static final double FLAT_MARKET_MIN_RR        = 2.00;   // [PATCH 2026-05-13] was 2.20 — synced with CS_TP2_R=2.4 via env
        private static final double FLAT_MARKET_MIN_SL_PCT    = 0.0060;  // [2026-05-25] 0.85% → 0.60% sync с TrendPullback
        private static final int    FLAT_MARKET_MAX_PER_HOUR  = 3;      // [PATCH 2026-05-13] was 2 — more samples for calibrator
        private static final int    FLAT_MARKET_MAX_PER_DAY   = 5;      // [PATCH 2026-05-13] was 4 — more samples for calibrator

        private static final double WEAK_TREND_MIN_CONF       = 60.0;
        private static final int    WEAK_TREND_MIN_CLUSTERS   = 2;

        private static final double TREND_THRESHOLD_STRONG    = 0.50;
        private static final double TREND_THRESHOLD_WEAK      = 0.30;

        /**
         * [v85 FLAT-FIX] Read current BTC regime via GIC singleton.
         * Returns trend strength magnitude. 0 = no GIC available (defensive).
         */
        private static double currentTrendStrength() {
            try {
                com.bot.GlobalImpulseController gic = com.bot.GlobalImpulseController.getLatest();
                if (gic == null) return 0;
                com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
                if (ctx == null) return 0;
                return Math.abs(ctx.impulseStrength);
            } catch (Throwable t) { return 0; }
        }

        /** Daily counter for per-day cap (resets at UTC midnight). */
        private final ConcurrentLinkedDeque<Long> dailyDispatchTimestamps = new ConcurrentLinkedDeque<>();
        private void pruneDailyTimestamps(long now) {
            long cutoff = now - 24L * 60 * 60_000L;
            while (!dailyDispatchTimestamps.isEmpty()) {
                Long head = dailyDispatchTimestamps.peekFirst();
                if (head == null || head < cutoff) dailyDispatchTimestamps.pollFirst();
                else break;
            }
        }

        private final com.bot.TelegramBotSender tg;
        private final com.bot.InstitutionalSignalCore isc;
        // [v79 I5] SignalSender reference for cross-exchange checks — optional.
        private volatile com.bot.SignalSender sender;

        private final ConcurrentHashMap<String, Long> lastDispatchMs = new ConcurrentHashMap<>();
        private final ConcurrentLinkedDeque<Long> dispatchTimestamps = new ConcurrentLinkedDeque<>();
        private final AtomicLong totalDispatched = new AtomicLong(0);
        private final AtomicLong blockedByGate   = new AtomicLong(0);

        private final AtomicLong blockedBipolar    = new AtomicLong(0);
        private final AtomicLong blockedRR         = new AtomicLong(0);
        private final AtomicLong blockedSL         = new AtomicLong(0);
        private final AtomicLong blockedColdStart  = new AtomicLong(0);
        private final AtomicLong blockedDedup      = new AtomicLong(0);
        private final AtomicLong blockedHourly     = new AtomicLong(0);
        private final AtomicLong blockedXExchange  = new AtomicLong(0); // [v79 I5]
        // [v80] Новые счётчики
        private final AtomicLong blockedQuality    = new AtomicLong(0);

        private static volatile Dispatcher INSTANCE;

        private Dispatcher(com.bot.TelegramBotSender tg, com.bot.InstitutionalSignalCore isc) {
            this.tg = tg; this.isc = isc;
        }

        public static Dispatcher init(com.bot.TelegramBotSender tg, com.bot.InstitutionalSignalCore isc) {
            if (INSTANCE == null) INSTANCE = new Dispatcher(tg, isc);
            return INSTANCE;
        }

        public static Dispatcher getInstance() { return INSTANCE; }

        /** [v79 I5] Wire SignalSender after construction so dispatcher can do
         *  cross-exchange validation when CROSS_EXCHANGE_VALIDATION=1. */
        public void setSender(com.bot.SignalSender s) { this.sender = s; }

        public static final class Result {
            public final boolean dispatched;
            public final String  reason;
            Result(boolean d, String r) { dispatched = d; reason = r; }
            static Result ok() { return new Result(true, "OK"); }
            static Result blocked(String why) { return new Result(false, why); }
        }

        public Result dispatch(com.bot.DecisionEngineMerged.TradeIdea idea, String source) {
            if (idea == null) return Result.blocked("null idea");

            if (!isc.isSymbolAvailable(idea.symbol)) {
                blockedByGate.incrementAndGet();
                blockedBipolar.incrementAndGet();
                return Result.blocked("bipolar/cooldown");
            }

            double riskDist = Math.abs(idea.stop - idea.price);
            double tp2Dist  = Math.abs(idea.tp2 - idea.price);
            double actualRR = riskDist > 1e-9 ? tp2Dist / riskDist : 0;

            // [v85 FLAT-FIX] Regime-aware thresholds
            double trendStr = currentTrendStrength();
            boolean isFlat = trendStr < TREND_THRESHOLD_WEAK;
            boolean isWeakTrend = !isFlat && trendStr < TREND_THRESHOLD_STRONG;
            double effMinRR = isFlat ? FLAT_MARKET_MIN_RR : MIN_RR;
            double effMinSlPct = isFlat ? FLAT_MARKET_MIN_SL_PCT : MIN_SL_PCT;
            int effMaxPerHour = isFlat ? FLAT_MARKET_MAX_PER_HOUR : MAX_PER_HOUR;

            if (actualRR < effMinRR) {
                blockedByGate.incrementAndGet();
                blockedRR.incrementAndGet();
                return Result.blocked(String.format("R:R=%.2f<%.1f (%s)",
                        actualRR, effMinRR, isFlat ? "flat" : isWeakTrend ? "weak" : "trend"));
            }

            double slPct = riskDist / idea.price;
            if (slPct < effMinSlPct) {
                blockedByGate.incrementAndGet();
                blockedSL.incrementAndGet();
                return Result.blocked(String.format("SL=%.3f%%<%.2f%% (%s)",
                        slPct * 100, effMinSlPct * 100, isFlat ? "flat" : "trend"));
            }

            // [v80] QUALITY GATE — отсекаем Grade C/D и низкую confidence ПОСЛЕ warmup.
            // До warmup (n<20) пропускаем чтобы калибратор обучался.
            // [v85 FLAT-FIX] Regime-aware thresholds.
            int _calForGate = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            if (_calForGate >= COLD_START_MIN_OUTCOMES) {
                // [B1 2026-05-08] Use direction-correct supportingClusters from analyze()
                // when available; fall back to substring counting only for paths that
                // bypass DE.generate() (EARLY_TICK, LiveTradeProbe, manual ideas).
                int _clusters = idea.getAgreeingClusters() >= 0
                        ? idea.getAgreeingClusters()
                        : countClusterFlags(idea.flags);
                double effMinConf;
                int effMinClusters;
                String regimeLabel;
                if (isFlat) {
                    effMinConf = FLAT_MARKET_MIN_CONF;
                    effMinClusters = FLAT_MARKET_MIN_CLUSTERS;
                    regimeLabel = "flat";
                } else if (isWeakTrend) {
                    effMinConf = WEAK_TREND_MIN_CONF;
                    effMinClusters = WEAK_TREND_MIN_CLUSTERS;
                    regimeLabel = "weak";
                } else {
                    effMinConf = MIN_CONFIDENCE_AFTER_WARMUP;
                    effMinClusters = MIN_CLUSTERS_AFTER_WARMUP;
                    regimeLabel = "trend";
                }

                // [v9.8 2026-05-28] AUTO-THROTTLE на drawdown — foundation для big-capital safety.
                // Когда live WR за всю историю (≥30 trades) падает ниже healthy levels, бот
                // автоматически становится консервативнее: поднимает effMinConf, пропускает
                // только high-confidence setups. Это safety net для real money: если edge
                // исчезает (regime shift / strategy degradation), bot самозащищается без
                // user intervention. Когда WR восстанавливается — gate auto-resets.
                //
                // Thresholds calibrated:
                //   WR < 35% (catastrophic): +6pp gate. Только perfect setups.
                //   WR 35-40% (degraded):    +3pp gate. Conservative mode.
                //   WR ≥ 45% (healthy):      no throttle.
                //   WR 40-45%:               soft +1pp (border zone).
                int liveTradeCount = isc.getTotalTradeCount();
                if (liveTradeCount >= 30) {
                    double liveWr = isc.getOverallWinRate();
                    double throttleBoost = 0;
                    if (liveWr < 0.35) {
                        throttleBoost = 6.0;
                        regimeLabel += "/THROTTLE_HARD";
                    } else if (liveWr < 0.40) {
                        throttleBoost = 3.0;
                        regimeLabel += "/THROTTLE_MILD";
                    } else if (liveWr < 0.45) {
                        throttleBoost = 1.0;
                        regimeLabel += "/THROTTLE_SOFT";
                    }
                    effMinConf += throttleBoost;
                }

                if (idea.probability < effMinConf || _clusters < effMinClusters) {
                    blockedByGate.incrementAndGet();
                    blockedQuality.incrementAndGet();
                    return Result.blocked(String.format(
                            "quality[%s]: prob=%.0f<%.0f OR clusters=%d<%d (str=%.2f n=%d)",
                            regimeLabel, idea.probability, effMinConf,
                            _clusters, effMinClusters, trendStr, _calForGate));
                }
            }

            // [v79 I5] Cross-exchange validation. If enabled and price differs
            // from second exchange by >0.5%, block — likely Binance feed issue
            // or pump-and-dump on a single exchange.
            if (CROSS_EXCHANGE_VALIDATION && sender != null) {
                String xcBlockReason = doCrossExchangeCheck(idea.symbol, idea.price);
                if (xcBlockReason != null) {
                    blockedByGate.incrementAndGet();
                    blockedXExchange.incrementAndGet();
                    LOG.warning("[XCHECK] " + idea.symbol + " blocked: " + xcBlockReason);
                    return Result.blocked("xchange: " + xcBlockReason);
                }
            }

            int calSamples = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            // [B1 2026-05-08] Same direction-correct fallback for cold-start gate.
            int clusters   = idea.getAgreeingClusters() >= 0
                    ? idea.getAgreeingClusters()
                    : countClusterFlags(idea.flags);
            double fcConf  = idea.forecast != null ? idea.forecast.confidence : 0.0;

            double probFloor;
            double probShortcut;
            int    minClusters;
            double minFcConf;

            if (calSamples >= COLD_START_MIN_OUTCOMES) {
                probFloor = 0; probShortcut = 0; minClusters = 0; minFcConf = 0;
            } else if (calSamples >= 10) {
                probFloor = 50.0; probShortcut = 52.0; minClusters = 1; minFcConf = 0.22;
            } else {
                probFloor = 48.0; probShortcut = 0; minClusters = 0; minFcConf = 0;
            }

            if (probFloor > 0 && idea.probability < probFloor) {
                blockedByGate.incrementAndGet();
                blockedColdStart.incrementAndGet();
                return Result.blocked(String.format(
                        "cold-start: prob=%.0f<%.0f (n=%d)", idea.probability, probFloor, calSamples));
            }
            if (probShortcut > 0 && idea.probability < probShortcut) {
                boolean clustersOk = clusters >= minClusters;
                boolean fcOk       = fcConf   >= minFcConf;
                if (!clustersOk && !fcOk) {
                    blockedByGate.incrementAndGet();
                    blockedColdStart.incrementAndGet();
                    return Result.blocked(String.format(
                            "cold-start: prob=%.0f<shortcut=%.0f AND clusters=%d<%d AND fcConf=%.2f<%.2f (n=%d)",
                            idea.probability, probShortcut,
                            clusters, minClusters, fcConf, minFcConf, calSamples));
                }
            }

            String dedupKey = idea.symbol + "_" + idea.side.name();
            Long lastMs = lastDispatchMs.get(dedupKey);
            long now = System.currentTimeMillis();
            if (lastMs != null && now - lastMs < SYMBOL_DEDUP_MS) {
                blockedByGate.incrementAndGet();
                blockedDedup.incrementAndGet();
                return Result.blocked(String.format("dedup %ds", (now - lastMs) / 1000));
            }

            pruneDispatchTimestamps(now);
            pruneDailyTimestamps(now);

            // [v85 FLAT-FIX] Daily cap (only enforced in flat market)
            if (isFlat && dailyDispatchTimestamps.size() >= FLAT_MARKET_MAX_PER_DAY) {
                blockedByGate.incrementAndGet();
                blockedHourly.incrementAndGet();
                return Result.blocked("flat daily cap " + FLAT_MARKET_MAX_PER_DAY);
            }
            if (dispatchTimestamps.size() >= effMaxPerHour) {
                blockedByGate.incrementAndGet();
                blockedHourly.incrementAndGet();
                return Result.blocked("hourly cap " + effMaxPerHour
                        + (isFlat ? " (flat)" : ""));
            }
            int recentBurst = 0;
            for (Long t : dispatchTimestamps) {
                if (t != null && now - t <= FIVE_MIN_MS) recentBurst++;
            }
            if (recentBurst >= MAX_PER_5MIN) {
                blockedByGate.incrementAndGet();
                blockedHourly.incrementAndGet();
                return Result.blocked("burst cap " + MAX_PER_5MIN + "/5min");
            }

            try {
                String tgMessage = idea.toTelegramString();
                if (OBSERVATION_MODE) {
                    tgMessage = "🧪 *PAPER MODE* — НЕ ТОРГУЙ ЖИВЫМИ ДЕНЬГАМИ\n"
                            + "_Сигнал для валидации, не для входа в рынок_\n"
                            + "━━━━━━━━━━━━━━━━━━━━━\n"
                            + tgMessage;
                }
                tg.sendMessageAsync(tgMessage);
                totalDispatched.incrementAndGet();
                lastDispatchMs.put(dedupKey, now);
                dispatchTimestamps.addLast(now);
                dailyDispatchTimestamps.addLast(now);  // [v85 FLAT-FIX]
                totalSignals.incrementAndGet();
                lastSignalMs = now;
                droughtAnnounced.set(false);
                trackSignal(idea, true);

                // [v79 I3] Audit log entry — append-only, HMAC signed.
                try {
                    com.bot.DecisionEngineMerged.getCalibrator().writeDispatchAudit(
                            idea.symbol, idea.side.name(), idea.price,
                            idea.tp1, idea.stop, idea.probability,
                            lastBtcRegimeForAlert == null ? "UNKNOWN" : lastBtcRegimeForAlert,
                            source, OBSERVATION_MODE);
                } catch (Throwable ignored) {}

                // [v83 PHASE-3] AUTO-TRADE HOOK
                // Only when: BOT_AUTO_TRADE=1, OBSERVATION_MODE=0, executor ready.
                // Goes through RiskGuard.canTrade() — daily loss, BTC crash,
                // trade limits all enforced here. Failure → message in Telegram,
                // bot continues normally. Success → position opened on exchange,
                // SL placed, tracker watching.
                if (AUTO_TRADE_ENABLED && !OBSERVATION_MODE && LIVE_TRADING_ARMED) {
                    autoTradeHook(idea, tg);
                }
                // [v86.35] When DISARMED (LIVE_TRADING_ARMED=0) the signal is still dispatched +
                // tracked by the verifier (Live WR), but NO real trade is opened — experiment mode.

                isc.setSignalCooldown(idea.symbol, 3 * 60_000L);
                LOG.info(String.format("[DISPATCH/%s%s] %s %s prob=%.0f conf=%.2f rr=%.2f sl=%.2f%% n=%d",
                        source, OBSERVATION_MODE ? "/PAPER" : "",
                        idea.symbol, idea.side, idea.probability,
                        fcConf, actualRR, slPct * 100, calSamples));
                return Result.ok();
            } catch (Throwable t) {
                LOG.warning("[DISPATCH/" + source + "] " + idea.symbol + " failed: " + t.getMessage());
                return Result.blocked("exception");
            }
        }

        /**
         * [v83 PHASE-3] Auto-trade execution path.
         *
         * Flow:
         *   1. RiskGuard.canTrade()  — pre-trade safety checks. BLOCK → notify, return.
         *   2. fetch live balance   — needed for position sizing + RG day-baseline.
         *   3. BinanceTradeExecutor.openPositionWithSl() — atomic open+SL.
         *   4. PositionTracker.trackOpened() — start watching for close.
         *   5. Telegram notification with all execution details.
         *
         * Any failure mid-flow is logged, sent to Telegram, but doesn't crash the
         * bot. Worst case: signal was sent to Telegram, position was NOT opened.
         */
        private void autoTradeHook(com.bot.DecisionEngineMerged.TradeIdea idea,
                                   com.bot.TelegramBotSender tg) {
            try {
                com.bot.RiskGuard rg = com.bot.RiskGuard.getInstance();
                com.bot.BinanceTradeExecutor ex = com.bot.BinanceTradeExecutor.getInstance();
                com.bot.PositionTracker tracker = com.bot.PositionTracker.getInstance();

                if (!ex.isReady()) {
                    tg.sendMessageAsync("⚠️ Auto-trade " + idea.symbol
                            + ": API ключи не настроены, пропуск.");
                    return;
                }

                // Sanity-check idea isn't older than 90 sec. Stale idea →
                // price/SL may have drifted, risking invalid SL placement.
                long ideaAgeMs = System.currentTimeMillis() - idea.createdAtMs;
                if (ideaAgeMs > 90_000L) {
                    tg.sendMessageAsync(String.format(
                            "⏰ *Auto-trade SKIP* %s — сигнал старый (%.1f сек), "
                                    + "цена/SL могли уйти. Жду свежего setup.",
                            idea.symbol, ideaAgeMs / 1000.0));
                    LOG.info("[AUTO-TRADE/" + idea.symbol + "] stale idea, age="
                            + ideaAgeMs + "ms");
                    return;
                }

                // 1. Fetch live balance (this also feeds RiskGuard's day baseline)
                // [v86.18] The demo backend's -1109 ("Invalid account") is INTERMITTENT — a
                // single call can fail then succeed seconds later (proven: emergencyClose
                // attempt-1 -1109 -> attempt-2 OK; probe + LTC opened while DOT's balance
                // fetch -1109'd in the same minute). So RETRY before giving up, otherwise a
                // transient -1109 silently kills an otherwise-valid signal.
                double balance = ex.fetchAvailableBalance();
                for (int _bRetry = 0; balance <= 0 && _bRetry < 4; _bRetry++) {
                    try { Thread.sleep(400L); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                    balance = ex.fetchAvailableBalance();
                }
                if (balance <= 0) {
                    tg.sendMessageAsync("⚠️ Auto-trade " + idea.symbol
                            + ": не удалось получить баланс (−1109 после 5 попыток).");
                    return;
                }

                // 2. Pre-trade safety checks
                com.bot.RiskGuard.Decision d = rg.canTrade(idea.symbol, balance);
                if (!d.allowed) {
                    tg.sendMessageAsync(String.format(
                            "🛡️ *RiskGuard BLOCK* %s\n" +
                                    "Причина: %s\n%s",
                            idea.symbol, d.reason,
                            d.hint.isEmpty() ? "" : "_" + d.hint + "_"));
                    LOG.info("[AUTO-TRADE/" + idea.symbol + "] blocked: " + d);
                    return;
                }

                // 3. Open position
                com.bot.BinanceTradeExecutor.ExecutionResult r =
                        ex.openPositionWithSl(idea, balance);
                if (!r.success) {
                    tg.sendMessageAsync(String.format(
                            "❌ *Auto-trade FAIL* %s %s\n_%s_",
                            idea.symbol, idea.side.name(), r.reason));
                    LOG.warning("[AUTO-TRADE/" + idea.symbol + "] exec failed: " + r.reason);
                    return;
                }

                // 4. Hand off to tracker, register with RiskGuard
                rg.recordTradeOpened(idea.symbol, r.notionalUsd);
                tracker.trackOpened(idea.symbol,
                        idea.side == com.bot.TradingCore.Side.LONG,
                        r.entryPrice, r.qty, r.slPrice, r.notionalUsd,
                        r.orderId, r.slOrderId);

                // 5. Notify — теперь с РЕАЛЬНЫМ риском (margin = notional/leverage)
                // и явно указываем сколько fees/spread заберёт минимум.
                double marginUsed = r.notionalUsd / Math.max(1, ex.getLeverage());
                double riskUsd    = Math.abs(r.entryPrice - r.slPrice) * r.qty;
                tg.sendMessageAsync(String.format(
                        "🤖 *Auto-trade ОТКРЫТА* %s\n" +
                                "%s %s | qty=%s\n" +
                                "Entry: %.6f | SL: %.6f\n" +
                                "Notional: $%.2f | Margin: $%.2f (lev=%dx)\n" +
                                "Risk if SL: $%.2f (%.2f%% депо)\n" +
                                "Балaнс до: $%.2f | Mode: %s",
                        idea.symbol,
                        idea.side.name(),
                        ex.isTestnet() ? "🧪TESTNET" : "🔴LIVE",
                        formatNum(r.qty),
                        r.entryPrice, r.slPrice,
                        r.notionalUsd, marginUsed, ex.getLeverage(),
                        riskUsd, balance > 0 ? (riskUsd / balance * 100.0) : 0.0,
                        balance,
                        rg.statusLine()));
                LOG.info("[AUTO-TRADE/" + idea.symbol + "] OPENED " + r);
            } catch (Throwable t) {
                LOG.severe("[AUTO-TRADE] " + idea.symbol + " unexpected: " + t.getMessage());
                try {
                    tg.sendMessageAsync("🆘 Auto-trade exception on " + idea.symbol
                            + ": " + t.getClass().getSimpleName() + " " + t.getMessage());
                } catch (Throwable ignored) {}
            }
        }

        private static String formatNum(double v) {
            return java.math.BigDecimal.valueOf(v)
                    .setScale(8, java.math.RoundingMode.DOWN)
                    .stripTrailingZeros()
                    .toPlainString();
        }

        private void pruneDispatchTimestamps(long now) {
            while (!dispatchTimestamps.isEmpty()
                    && now - dispatchTimestamps.peekFirst() > HOUR_MS) {
                dispatchTimestamps.pollFirst();
            }
        }

        public long getTotalDispatched() { return totalDispatched.get(); }
        public long getBlockedByGate()   { return blockedByGate.get(); }
        public int  getHourlyCount() {
            pruneDispatchTimestamps(System.currentTimeMillis());
            return dispatchTimestamps.size();
        }

        public String getBlockBreakdown() {
            // [A2 2026-05-08] Added qg (quality gate) counter — was already incremented
            // at line ~473 (blockedQuality.incrementAndGet()) but invisible in heartbeat.
            // The "always 0" line was giving false comfort that pipeline was clean while
            // hundreds of signals were being killed by quality gate every cycle.
            return String.format("bi:%d rr:%d sl:%d cold:%d dd:%d hr:%d xch:%d qg:%d",
                    blockedBipolar.get(), blockedRR.get(), blockedSL.get(),
                    blockedColdStart.get(), blockedDedup.get(), blockedHourly.get(),
                    blockedXExchange.get(), blockedQuality.get());
        }
    }

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
    private static String doCrossExchangeCheck(String symbol, double binancePrice) {
        // Only run if at least 30s since last error for this symbol — avoid hammering.
        Long lastErr = xCheckLastErrorMs.get(symbol);
        if (lastErr != null && System.currentTimeMillis() - lastErr < 30_000L) {
            return null; // cooldown — fail open
        }
        try {
            // Bybit linear perpetual ticker
            String url = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=" + symbol;
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    java.net.URI.create(url).toURL().openConnection();
            conn.setConnectTimeout(2500);
            conn.setReadTimeout(2500);
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200) {
                xCheckLastErrorMs.put(symbol, System.currentTimeMillis());
                return null; // remote down — fail open
            }
            String body;
            try (java.io.InputStream is = conn.getInputStream();
                 java.util.Scanner sc = new java.util.Scanner(is, "UTF-8")) {
                body = sc.useDelimiter("\\A").hasNext() ? sc.next() : "";
            }
            int idx = body.indexOf("\"lastPrice\"");
            if (idx < 0) return null; // parse fail — fail open
            int q1 = body.indexOf('"', idx + 12) + 1;
            int q2 = body.indexOf('"', q1);
            if (q1 < 0 || q2 < q1) return null;
            double bybitPrice = Double.parseDouble(body.substring(q1, q2));
            if (bybitPrice <= 0) return null;
            double diff = Math.abs(binancePrice - bybitPrice) / bybitPrice;
            if (diff > 0.005) {
                return String.format("binance=%.6f bybit=%.6f diff=%.2f%%",
                        binancePrice, bybitPrice, diff * 100);
            }
            return null; // ok
        } catch (Throwable t) {
            xCheckLastErrorMs.put(symbol, System.currentTimeMillis());
            return null; // exception — fail open
        }
    }

    private static int countClusterFlags(List<String> flags) {
        if (flags == null) return 0;
        int c = 0;
        for (String f : flags) {
            if (f == null) continue;
            String u = f.toUpperCase();
            if (u.startsWith("CLUSTER") || u.contains("TREND")
                    || u.contains("BREAKOUT") || u.contains("VSA") || u.contains("PUMP")
                    || u.contains("EXH") || u.contains("OFV_STRONG") || u.contains("OBI")
                    || u.contains("HTF_") || u.contains("BOS") || u.contains("FVG")
                    || u.contains("LIQ_MAGNET") || u.contains("DIV")
                    || u.contains("REVERSAL") || u.contains("EXHAUST_REV")
                    || u.contains("LOCAL_REVERSAL")) c++;
        }
        return c;
    }

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
        final com.bot.GlobalImpulseController gic = new com.bot.GlobalImpulseController();
        final com.bot.InstitutionalSignalCore isc = new com.bot.InstitutionalSignalCore();
        final com.bot.SignalSender sender         = new com.bot.SignalSender(telegram, gic, isc);

        Dispatcher.init(telegram, isc).setSender(sender); // [v79 I5]

        // [v84.0 PROBE] One-shot live-trade probe — runs only if PROBE_RUN env var is set.
        // Purpose: verify automation end-to-end on Binance demo before trusting real signals.
        // Opens a tiny BTCUSDT position with 5x leverage + SL + TP1 + TP2,
        // holds for PROBE_HOLD_SECONDS, then closes and cleans up.
        // Set PROBE_RUN=BTCUSDT in Railway env to enable. REMOVE after success.
        try {
            com.bot.LiveTradeProbe.runIfRequested(telegram);
        } catch (Throwable t) {
            LOG.warning("[PROBE] launch error (non-fatal, bot continues): " + t.getMessage());
        }

        final String calibratorFile = System.getenv()
                .getOrDefault("CALIBRATOR_FILE", "./data/calibrator.csv");

        // [A1+ 2026-05-08] One-shot calibrator wipe via env. Use when:
        //   1. Startup backtest poisoned the file (WR=32% baseline → blocks live).
        //   2. Switching strategy and want to start clean.
        // Procedure on Railway:
        //   - Set RESET_CALIBRATOR_ON_BOOT=1 in env.
        //   - Redeploy. On boot the .csv is moved to .bak (kept for forensic
        //     review), in-memory state is cleared.
        //   - REMOVE the env var (or set =0) immediately after this deploy
        //     succeeds — otherwise EVERY restart wipes the calibrator and you
        //     never accumulate live data.
        if ("1".equals(System.getenv().getOrDefault("RESET_CALIBRATOR_ON_BOOT", "0"))) {
            try {
                java.io.File f = new java.io.File(calibratorFile);
                if (f.exists()) {
                    java.io.File bak = new java.io.File(calibratorFile + ".bak."
                            + System.currentTimeMillis());
                    if (f.renameTo(bak)) {
                        LOG.warning("[Calibrator] RESET_CALIBRATOR_ON_BOOT=1 — "
                                + "renamed " + calibratorFile + " → " + bak.getName());
                    } else {
                        // renameTo can fail across mount boundaries — try delete instead.
                        if (f.delete()) {
                            LOG.warning("[Calibrator] RESET_CALIBRATOR_ON_BOOT=1 — "
                                    + "deleted " + calibratorFile + " (rename failed)");
                        }
                    }
                }
                com.bot.DecisionEngineMerged.getCalibrator().resetAll();
                LOG.warning("[Calibrator] in-memory state cleared. "
                        + "REMEMBER to unset RESET_CALIBRATOR_ON_BOOT after this deploy.");
            } catch (Throwable t) {
                LOG.warning("[Calibrator] reset-on-boot failed: " + t.getMessage());
            }
        }

        try {
            com.bot.DecisionEngineMerged.getCalibrator().loadFromFile(calibratorFile);
        } catch (Throwable t) {
            LOG.warning("[Calibrator] load failed: " + t.getMessage());
        }

        // [v79 I7] Restore unresolved forecast records from previous run.
        loadForecastRecords();

        com.bot.DecisionEngineMerged.USER_ZONE = ZONE;

        isc.setTimeStopCallback((sym, msg) -> {
            for (TrackedSignal ts : trackedSignals.values()) {
                if (!ts.symbol.equals(sym)) continue;
                if (!ts.sentToUser) continue;
                if (ts.timeStopNotified) continue;
                ts.timeStopNotified = true;
                break;
            }
        });

        gic.setPanicCallback(msg -> LOG.warning("[GIC panic] " + msg));

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

        mainSched.scheduleAtFixedRate(
                safe("MainCycle", () -> runCycle(telegram, gic, isc, sender)),
                90, INTERVAL * 60L, TimeUnit.SECONDS);
        auxSched.scheduleAtFixedRate(
                safe("LogStats", () -> logStats(gic, isc, sender)),
                15, 30, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("Watchdog", () -> runWatchdog(telegram, sender)),
                5 * 60, 120, TimeUnit.SECONDS);
        auxSched.scheduleAtFixedRate(
                safe("Heartbeat", () -> maybeSendHeartbeat(telegram, sender, gic, isc)),
                15, 15, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("CalibratorSave", () -> {
                    com.bot.DecisionEngineMerged.getCalibrator().saveToFile(calibratorFile);
                    saveForecastRecords(); // [v79 I7] persist forecast records too
                    int cnt = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
                    LOG.info("[Calibrator] auto-saved, total outcomes: " + cnt);
                }),
                30, 30, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("TimeSync", sender::syncServerTime),
                5, 120, TimeUnit.MINUTES);
        // [v79 I4] Verifier runs every 5 min instead of 10 — finer granularity
        // catches TP1/SL hits closer to real time, less drift in calibrator.
        auxSched.scheduleAtFixedRate(
                safe("ForecastChecker", () -> checkForecastAccuracy(sender, telegram)),
                5, 5, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("WalkForward", () -> {
                    int hourUtc = ZonedDateTime.now(ZoneOffset.UTC).getHour();
                    if (hourUtc == 5) {   // [v86.80] daily (was every 3rd day) — real OOS verdict
                        runWalkForwardValidation(sender, telegram);
                    }
                }),
                60, 60, TimeUnit.MINUTES);
        auxSched.scheduleAtFixedRate(
                safe("BacktestSubmit", () -> {
                    int hourUtc = ZonedDateTime.now(ZoneOffset.UTC).getHour();
                    if (hourUtc == 3) {
                        heavySched.submit(safe("Backtest",
                                () -> runPeriodicBacktest(sender, isc)));
                    }
                }),
                60, 60, TimeUnit.MINUTES);

        // [v79 I6] Daily integrity report — once per day at UTC 06:00 send
        // Telegram summary with public-verifiable stats.
        auxSched.scheduleAtFixedRate(
                safe("IntegrityReport", () -> {
                    int hourUtc = ZonedDateTime.now(ZoneOffset.UTC).getHour();
                    int minute = ZonedDateTime.now(ZoneOffset.UTC).getMinute();
                    if (hourUtc == 6 && minute < 30) {
                        sendIntegrityReport(telegram);
                    }
                }),
                30, 30, TimeUnit.MINUTES);

        final String calibratorFileFinal = calibratorFile;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("═══ Shutdown. Cycles: " + totalCycles.get()
                    + " | Signals: " + totalSignals.get() + " ═══");
            mainSched.shutdown(); auxSched.shutdown(); heavySched.shutdown();
            try { mainSched.awaitTermination(4, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            mainSched.shutdownNow();
            try {
                com.bot.DecisionEngineMerged.getCalibrator().saveToFile(calibratorFileFinal);
                saveForecastRecords();
            } catch (Throwable ignored) {}
            telegram.flushAndShutdown(8000);
        }, "ShutdownHook"));

        LOG.info("═══ TradingBot " + BOT_VERSION + " started " + nowLocalStr()
                + " (first cycle in 90s, OBSERVATION_MODE="
                + (OBSERVATION_MODE ? "ON/PAPER" : "OFF/LIVE")
                + ", X-EXCHANGE_CHECK=" + (CROSS_EXCHANGE_VALIDATION ? "ON" : "OFF") + ") ═══");

        // [v83 PHASE-1] Force RiskGuard to load now so we see its init line in
        // logs even before any trading code calls it. RiskGuard is a no-op at
        // this stage — it only gets wired into BotMain.Dispatcher in PHASE-3.
        // This call is just "ping, are you there" and prints status.
        try {
            com.bot.RiskGuard rg = com.bot.RiskGuard.getInstance();
            LOG.info("[BOOT] " + rg.statusLine());
        } catch (Throwable t) {
            LOG.warning("[BOOT] RiskGuard init failed: " + t.getMessage());
        }

        // [v83 PHASE-3] Initialize Executor + Tracker. They are no-ops until
        // BOT_AUTO_TRADE=1 AND OBSERVATION_MODE=0. Even then, every trade
        // passes through RiskGuard.canTrade() first.
        try {
            com.bot.BinanceTradeExecutor ex = com.bot.BinanceTradeExecutor.getInstance();
            com.bot.PositionTracker tracker = com.bot.PositionTracker.getInstance();
            tracker.setTelegram(telegram); // for close notifications
            // Wire emergency-close failure alerts to Telegram. If SL placement
            // fails AND emergencyClose can't close the position, operator gets
            // alerted immediately.
            com.bot.BinanceTradeExecutor.setEmergencyAlertSink(msg -> {
                try { telegram.sendMessageAsync(msg); } catch (Throwable ignored) {}
            });
            // Only start polling if auto-trade is enabled — otherwise tracker
            // would just spin uselessly.
            if (AUTO_TRADE_ENABLED && !OBSERVATION_MODE && ex.isReady() && !LIVE_TRADING_ARMED) {
                // [v86.35] Auto-trade configured but DISARMED — experiment/validation mode.
                // Tracker still runs (manages any leftover position safely), signals + verifier
                // keep working, but NO real trades open.
                tracker.start();
                LOG.warning("[BOOT] LIVE TRADING DISARMED (LIVE_TRADING_ARMED=0) — experiment mode: "
                        + "signals + verifier run, NO real trades. Arm with LIVE_TRADING_ARMED=1 when proven.");
                telegram.sendMessageAsync(
                        "🛡 *Живая торговля ВЫКЛЮЧЕНА* (режим эксперимента)\n" +
                                "Бот анализирует рынок, шлёт сигналы и копит Live WR в верификаторе — "
                                + "но РЕАЛЬНЫХ сделок НЕ открывает. Деньги не рискуют.\n" +
                                "_Включим, когда докажем edge: `LIVE_TRADING_ARMED=1`._");
            } else if (AUTO_TRADE_ENABLED && !OBSERVATION_MODE && ex.isReady()) {
                tracker.start();
                LOG.info("[BOOT] Auto-trade ENABLED. Mode: "
                        + (ex.isTestnet() ? "TESTNET" : "REAL/LIVE")
                        + " leverage=" + ex.getLeverage()
                        + "x risk=" + ex.getRiskPct() + "%");
                // Pull actual limits from RiskGuard so Telegram boot banner
                // reflects any env overrides.
                com.bot.RiskGuard rg = com.bot.RiskGuard.getInstance();
                int    dailyLim     = rg.getDailyTradeLimit();
                int    concurrentLim= rg.getMaxConcurrentPositions();
                double dlPct        = rg.getDailyLossLimitPct();
                double wlPct        = rg.getWeeklyLossLimitPct();
                String dailyStr     = (dailyLim     >= 1000) ? "∞" : String.valueOf(dailyLim);
                String concurrentStr= (concurrentLim>= 1000) ? "∞" : String.valueOf(concurrentLim);
                telegram.sendMessageAsync(String.format(
                        "🤖 *Auto-trade АКТИВИРОВАН*\n" +
                                "Режим: %s\n" +
                                "Плечо: %dx | Риск: %.1f%%/сделка\n" +
                                "Защиты: dailyLoss -%.0f%%, weeklyLoss -%.0f%%, max %s сделок/день, max %s одновременно\n" +
                                "_Любая сделка → автоматическая остановка при срабатывании защит._",
                        ex.isTestnet() ? "🧪 TESTNET" : "🔴 REAL/LIVE",
                        ex.getLeverage(), ex.getRiskPct(),
                        dlPct, wlPct, dailyStr, concurrentStr));
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

        try {
            int _calN = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            if (!OBSERVATION_MODE && _calN < 50) {
                telegram.sendMessageAsync(String.format(
                        "🚨 *ВНИМАНИЕ: LIVE режим без калибровки*\n"
                                + "━━━━━━━━━━━━━━━━━━━━━\n"
                                + "Калибратор: %d/50 исходов\n"
                                + "Сигналы выходят с НЕкалиброванной вероятностью.\n"
                                + "━━━━━━━━━━━━━━━━━━━━━\n"
                                + "_Рекомендации:_\n"
                                + "• Установи `OBSERVATION_MODE=1` в Railway env\n"
                                + "• Либо торгуй размером ×0.25 от обычного\n"
                                + "• Не больше 2 позиций в одну сторону одновременно",
                        _calN));
            }
        } catch (Throwable ignored) {}

        try {
            com.bot.SimpleBacktester.SelfValidator.start(sender, telegram, 5 * 60_000L);
        } catch (Throwable t) {
            LOG.warning("[SELF-VALIDATOR] init failed: " + t.getMessage());
        }

        // ═══════════════════════════════════════════════════════════════════
        // [STARTUP-BACKTEST 2026-05-02] One-shot full backtest at boot.
        // Runs in heavySched (background) so main cycle is not blocked.
        // Skips automatically if calibrator already has ≥STARTUP_BT_MIN_SAMPLES
        // (default 200) outcomes — no double-training on restarts.
        // Disable explicitly with STARTUP_BACKTEST=0 in Railway env.
        // ═══════════════════════════════════════════════════════════════════
        try {
            // [v85.2] Вернул DEFAULT ON (юзер хочет обычный бэктест). На нормальной каденции
            // (без частых рестартов) startup-BT грузит ОК — 418 был от ~10 рестартов подряд
            // + тяжёлого pairs-BT. pairs-BT остаётся OFF. Откл: STARTUP_BACKTEST=0.
            boolean startupBacktestEnabled =
                    !"0".equals(System.getenv().getOrDefault("STARTUP_BACKTEST", "1"));
            int existingOutcomes =
                    com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            int minSamplesNeeded = Integer.parseInt(System.getenv()
                    .getOrDefault("STARTUP_BT_MIN_SAMPLES", "200"));

            if (!startupBacktestEnabled) {
                LOG.info("[STARTUP-BT] Disabled via STARTUP_BACKTEST=0");
            } else {
                // [v86.55] Бектест теперь запускается ВСЕГДА: с v86.53 он — измерительный
                // стенд (EXIT-SHADOW, walk-forward, сводка для сравнения версий). Выше
                // порога калибратора он идёт в MEASURE-ONLY: сим-исходы НЕ пишутся в
                // калибратор (порог защищает живые данные от затопления симуляцией),
                // но сводка и shadow-таблица приходят при каждом рестарте.
                final boolean measureOnly = existingOutcomes >= minSamplesNeeded;
                LOG.info(measureOnly
                        ? "[STARTUP-BT] Scheduled MEASURE-ONLY (calibrator full: "
                          + existingOutcomes + "≥" + minSamplesNeeded
                          + ") — сводка+EXIT-SHADOW, калибратор не трогаем"
                        : "[STARTUP-BT] Scheduled — calibrator has only "
                          + existingOutcomes + " outcomes, need ≥" + minSamplesNeeded);
                final String _calibFile = calibratorFile;
                heavySched.submit(safe("StartupBacktest",
                        () -> runStartupBacktest(sender, isc, telegram, _calibFile, measureOnly)));
            }
        } catch (Throwable t) {
            LOG.warning("[STARTUP-BT] init failed: " + t.getMessage());
        }

        // [v86.80] One-shot REAL OOS walk-forward after boot — queues on the SAME
        // single-thread heavySched BEHIND the startup-BT, so it runs AFTER the heavy BT
        // frees its memory (no -Xmx380m OOM overlap). Sends an honest out-of-sample verdict
        // (the startup-BT "X/4" line is in-sample bucketing, not OOS).
        try {
            heavySched.submit(safe("WalkForwardBoot",
                    () -> runWalkForwardValidation(sender, telegram)));
        } catch (Throwable t) {
            LOG.warning("[WF-BOOT] init failed: " + t.getMessage());
        }

    }

    private static void runCycle(com.bot.TelegramBotSender telegram,
                                 com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        long cycleStart = System.currentTimeMillis();
        long now = System.currentTimeMillis();

        if (sender.isRlBanned()) {
            LOG.info("[CYCLE] Skipped — Binance IP ban for " + sender.rlBanSecondsLeft() + "s");
            return;
        }

        if (now < cbPauseUntil) return;
        if (now - lastErrorWindowStart > CB_WINDOW_MS) {
            lastErrorWindowStart = now;
            errorsInWindow.set(0);
        }
        if (errorsInWindow.get() >= CB_THRESHOLD) {
            cbPauseUntil = now + CB_PAUSE_MS;
            errorsInWindow.set(0);
            LOG.warning("[CB] Too many errors, pause till " + formatLocalTime(cbPauseUntil));
            return;
        }

        long cycle = totalCycles.incrementAndGet();
        LOG.info("══ CYCLE #" + cycle + " ══ " + nowLocalStr());

        updateBtcContext(sender, gic);
        updateSectors(sender, gic);
        try { sender.getPumpHunter().periodicCleanup(); } catch (Throwable ignored) {}
        // [v86.96] New-listings catcher — observation-only, never trades.
        if (NEW_LISTING_CATCHER) { try { sender.checkNewListings(); } catch (Throwable ignored) {} }
        // [v86.99] Funding-extreme snapshot (#2) — observation-only.
        if (FUNDING_SNAPSHOT) { try { sender.snapshotFundingExtremes(); } catch (Throwable ignored) {} }
        // [v87.0] Liquidation capture flush (#3) — observation-only.
        if (LIQ_CAPTURE) { try { sender.flushLiquidations(); } catch (Throwable ignored) {} }

        double bal = sender.getAccountBalance();
        if (bal > 0) isc.updateBalance(bal);
        int totalTrades = isc.getTotalTradeCount();
        if (totalTrades >= 50) {
            sender.getDecisionEngine().updateBayesPrior(isc.getOverallWinRate(), totalTrades);
        }

        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        lastBtcRegimeForAlert = String.valueOf(ctx.regime);
        LOG.info("BTC: " + ctx.regime
                + " str=" + String.format("%.2f", ctx.impulseStrength)
                + " vol=" + String.format("%.2f", ctx.volatilityExpansion)
                + " | " + isc.getStats());

        List<com.bot.DecisionEngineMerged.TradeIdea> signals = sender.generateSignals();
        lastCycleSuccessMs = System.currentTimeMillis();
        if (signals == null || signals.isEmpty()) {
            LOG.info("No signals. " + isc.getStats());
            return;
        }

        signals.sort(Comparator
                .comparingDouble((com.bot.DecisionEngineMerged.TradeIdea i) -> i.probability).reversed()
                .thenComparingDouble(i -> i.forecast != null ? i.forecast.confidence : 0.0).reversed()
                .thenComparingDouble(i -> i.forecast != null ? Math.abs(i.forecast.directionScore) : 0.0).reversed());
        int limit = Math.min(signals.size(), MAX_SIGNALS_PER_CYCLE);
        List<com.bot.DecisionEngineMerged.TradeIdea> dispatchSignals =
                reorderSignalsForDispatch(new ArrayList<>(signals.subList(0, limit)));

        int sent = 0, blocked = 0;
        for (com.bot.DecisionEngineMerged.TradeIdea s : dispatchSignals) {
            Dispatcher.Result res = Dispatcher.getInstance().dispatch(s, "CYCLE");
            if (res.dispatched) {
                isc.registerSignal(s);
                sent++;
            } else {
                blocked++;
                isc.unregisterSignal(s);
                LOG.info("[CYCLE-BLOCK] " + s.symbol + " " + s.side + ": " + res.reason);
            }
        }
        LOG.info("══ CYCLE #" + cycle + " END ══ sent=" + sent + " blocked=" + blocked
                + " time=" + (System.currentTimeMillis() - cycleStart) + "ms");
    }

    private static List<com.bot.DecisionEngineMerged.TradeIdea> reorderSignalsForDispatch(
            List<com.bot.DecisionEngineMerged.TradeIdea> selected) {
        if (selected == null || selected.size() < 3) return selected;
        List<com.bot.DecisionEngineMerged.TradeIdea> longs = new ArrayList<>();
        List<com.bot.DecisionEngineMerged.TradeIdea> shorts = new ArrayList<>();
        for (com.bot.DecisionEngineMerged.TradeIdea idea : selected) {
            if (idea.side == com.bot.TradingCore.Side.LONG) longs.add(idea);
            else shorts.add(idea);
        }
        if (longs.isEmpty() || shorts.isEmpty()) return selected;
        List<com.bot.DecisionEngineMerged.TradeIdea> ordered = new ArrayList<>(selected.size());
        int li = 0, si = 0;
        boolean takeLong = selected.get(0).side == com.bot.TradingCore.Side.LONG;
        while (li < longs.size() || si < shorts.size()) {
            if (takeLong && li < longs.size()) ordered.add(longs.get(li++));
            else if (!takeLong && si < shorts.size()) ordered.add(shorts.get(si++));
            else if (li < longs.size()) ordered.add(longs.get(li++));
            else if (si < shorts.size()) ordered.add(shorts.get(si++));
            takeLong = !takeLong;
        }
        return ordered;
    }

    public static void trackSignal(com.bot.DecisionEngineMerged.TradeIdea idea) {
        trackSignal(idea, true);
    }

    static void trackSignal(com.bot.DecisionEngineMerged.TradeIdea idea, boolean sentToUser) {
        if (idea == null) return;
        String key = idea.symbol + "_" + idea.side;
        trackedSignals.remove(key);
        String forecastBias = "NEUTRAL";
        double forecastScore = 0.0;
        if (idea.forecast != null) {
            forecastBias = idea.forecast.bias.name();
            forecastScore = idea.forecast.directionScore;
        }
        double tp3 = idea.tp3 > 0 ? idea.tp3 : idea.price + (idea.tp2 - idea.price) * 1.5;
        trackedSignals.put(key, new TrackedSignal(
                idea.symbol, idea.side, idea.price, idea.stop,
                idea.tp1, idea.tp2, tp3,
                forecastBias, forecastScore, sentToUser));
        if (forecastRecords.size() >= MAX_FORECAST_RECORDS) {
            forecastRecords.entrySet().removeIf(e ->
                    e.getValue().resolved && e.getValue().ageMs() > 60 * 60_000L);
            if (forecastRecords.size() >= MAX_FORECAST_RECORDS) {
                forecastRecords.entrySet().stream()
                        .min(Comparator.comparingLong(e -> e.getValue().createdAt))
                        .ifPresent(e -> forecastRecords.remove(e.getKey()));
            }
        }
        forecastRecords.put(key + "_" + forecastSeq.incrementAndGet(),
                new ForecastRecord(idea.symbol, idea.side, idea.price,
                        forecastBias, forecastScore, idea.probability, idea.getRobustAtrPct(),
                        idea.tp1, idea.stop,
                        lastBtcRegimeForAlert == null ? "UNKNOWN" : lastBtcRegimeForAlert));
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
    private static void checkForecastAccuracy(com.bot.SignalSender sender,
                                              com.bot.TelegramBotSender telegram) {
        // [v86.91] min resolve age scales with PRIMARY_TF: a 4h signal can't be judged after
        // 30 min (< one primary bar). max(30min, PRIMARY_TF_MS) → 4h waits a full 4h bar.
        long minAgeMs = Math.max(30 * 60_000L, PRIMARY_TF_MS);
        long maxAgeMs = VERIFIER_MAX_AGE_MS; // [v79 I4] sync with ISC TIME_STOP_BARS
        for (Iterator<Map.Entry<String, ForecastRecord>> it =
             forecastRecords.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, ForecastRecord> e = it.next();
            ForecastRecord fr = e.getValue();
            if (fr.ageMs() < minAgeMs) continue;
            if (fr.resolved) {
                if (fr.ageMs() > minAgeMs + 30 * 60_000L) it.remove();
                continue;
            }
            // [v79 I4] At maxAgeMs, force-resolve as TIME_STOP rather than silently dropping.
            boolean forceTimeStop = fr.ageMs() > maxAgeMs;

            try {
                int barsNeeded = (int) Math.ceil(fr.ageMs() / (double) PRIMARY_TF_MS) + 2;
                barsNeeded = Math.max(5, Math.min(30, barsNeeded));
                List<com.bot.TradingCore.Candle> c = sender.fetchKlines(fr.symbol, PRIMARY_TF, barsNeeded);
                if (c == null || c.isEmpty()) {
                    if (forceTimeStop) it.remove();
                    continue;
                }
                double currentPrice = c.get(c.size() - 1).close;

                double atrAbs = fr.robustAtrPctAtSignal > 0
                        ? fr.robustAtrPctAtSignal * fr.entryPrice
                        : (c.size() >= 15 ? com.bot.TradingCore.atr(c, 14) : 0);

                // [HOLE-1 FIX 2026-05-15] Signal side is the authoritative opinion,
                // NOT the HTF bias snapshot. Old logic required forecastBias to contain
                // "BULL" or "BEAR" — but HTFBias.NONE is a perfectly valid state for
                // counter-trend / range-bound / mean-reversion setups. The signal still
                // made a directional bet via fr.side (LONG/SHORT), and that bet either
                // won (TP1) or lost (SL/TIME_STOP).
                //
                // Symptom of the old bug: forecastTimeStop=4 but forecastTotal=0 in the
                // daily integrity report — proof that 4 records hit TIME_STOP yet none
                // reached the calibrator because their bias was NONE.
                //
                // The old bullish/bearish flags are kept for the legacy bias-driven
                // synthetic level fallback (when fr.tp1Level / fr.slLevel are missing).
                boolean bullishBias = fr.forecastBias.contains("BULL");
                boolean bearishBias = fr.forecastBias.contains("BEAR");
                // Authoritative opinion = the side the bot bet on. Period.
                boolean hasOpinion = fr.side == com.bot.TradingCore.Side.LONG
                        || fr.side == com.bot.TradingCore.Side.SHORT;

                boolean hitTP1 = false, hitSL = false, ambiguous = false;
                double tp1Use = fr.tp1Level;
                double slUse  = fr.slLevel;
                boolean haveRealLevels = tp1Use > 0 && slUse > 0;
                if (!haveRealLevels && hasOpinion && atrAbs > 0) {
                    // [HOLE-1 FIX] Use signal side (not bias) to synthesize fallback levels.
                    // For NONE-bias signals this branch is now reachable and computes
                    // sensible ATR-based TP/SL based on what the signal actually bet on.
                    boolean longSide = fr.side == com.bot.TradingCore.Side.LONG;
                    tp1Use = longSide ? fr.entryPrice + atrAbs * 1.0 : fr.entryPrice - atrAbs * 1.0;
                    slUse  = longSide ? fr.entryPrice - atrAbs * 0.8 : fr.entryPrice + atrAbs * 0.8;
                    haveRealLevels = true;
                }

                if (haveRealLevels && hasOpinion) {
                    boolean longSide = fr.side == com.bot.TradingCore.Side.LONG;
                    for (com.bot.TradingCore.Candle bar : c) {
                        // [v86.91] leak-filter window = one primary bar (was hardcoded 15m).
                        if (bar.openTime + PRIMARY_TF_MS < fr.createdAt) continue;
                        boolean tpHere = longSide ? bar.high >= tp1Use : bar.low  <= tp1Use;
                        boolean slHere = longSide ? bar.low  <= slUse  : bar.high >= slUse;
                        if (tpHere && slHere) {
                            double openPx = bar.open;
                            double tpDist = Math.abs(tp1Use - openPx);
                            double slDist = Math.abs(slUse  - openPx);
                            if (longSide) {
                                boolean openInFavor = openPx > fr.entryPrice && tpDist < slDist;
                                boolean openAgainst = openPx < fr.entryPrice && slDist < tpDist;
                                if (openInFavor) { hitTP1 = true; break; }
                                if (openAgainst) { hitSL  = true; break; }
                                ambiguous = true; break;
                            } else {
                                boolean openInFavor = openPx < fr.entryPrice && tpDist < slDist;
                                boolean openAgainst = openPx > fr.entryPrice && slDist < tpDist;
                                if (openInFavor) { hitTP1 = true; break; }
                                if (openAgainst) { hitSL  = true; break; }
                                ambiguous = true; break;
                            }
                        }
                        if (tpHere) { hitTP1 = true; break; }
                        if (slHere) { hitSL  = true; break; }
                    }
                }

                // [v79 I4] If verifier ran out of time and price never decisively moved → TIME_STOP.
                boolean timeStopOutcome = false;

                String outcome;
                boolean correct;
                double calWeight = 1.0; // [v79 I1] 0.5 for AMBIGUOUS, 1.0 otherwise
                String outcomeTag;

                if (hitTP1) {
                    correct = true; outcome = "TP1"; outcomeTag = "TP1";
                } else if (hitSL) {
                    correct = false; outcome = "SL"; outcomeTag = "SL";
                } else if (ambiguous) {
                    // [v79 I1] AMBIGUOUS — раньше игнорировался. Теперь записываем как half-credit.
                    // hit=true (since price did reach TP), weight=0.5 (uncertainty penalty).
                    // Логически: signal directionally correct, но execution с reasonable
                    // entry мог попасть как в TP, так и в SL — поэтому credit half.
                    correct = true; outcome = "AMBIGUOUS"; outcomeTag = "AMBIGUOUS";
                    calWeight = 0.5;
                    forecastAmbiguous.incrementAndGet();
                } else if (forceTimeStop) {
                    // [v79 I2] TIME_STOP — раньше "FLAT" ignorировался для калибратора.
                    // Теперь записываем как LOSS (signal no-op = no edge proved).
                    correct = false; outcome = "TIME_STOP"; outcomeTag = "TIME_STOP";
                    timeStopOutcome = true;
                    forecastTimeStop.incrementAndGet();
                } else {
                    // Mid-flight: signal still has time. Don't resolve yet.
                    continue;
                }

                fr.resolved = true;
                fr.actualOutcome = outcome;

                // [v79] EVERY decisive outcome counts now (no skip on AMBIGUOUS/TIME_STOP).
                if (hasOpinion) {
                    if (fr.counted.compareAndSet(false, true)) {
                        forecastTotal.incrementAndGet();
                        if (correct) forecastCorrect.incrementAndGet();
                        // [v86.59] разрез «режим BTC · сторона» — где именно бот сливает
                        String rKey = fr.btcRegimeAtSignal + "·"
                                + (fr.side == com.bot.TradingCore.Side.LONG ? "L" : "S");
                        int[] rs = regimeOutcomes.computeIfAbsent(rKey, k -> new int[2]);
                        rs[0]++;
                        if (correct) rs[1]++;
                    }
                    try {
                        double sigProb01 = Math.max(0.01, Math.min(0.99, fr.signalProbability / 100.0));
                        double atrForFc = c.size() >= 15 ? com.bot.TradingCore.atr(c, 14) : 0;
                        double atrPctForBucket = fr.robustAtrPctAtSignal > 0
                                ? fr.robustAtrPctAtSignal * 100.0
                                : (atrForFc > 0 ? (atrForFc / currentPrice) * 100.0 : 1.0);
                        // [v79 I3] New extended recordOutcome — calibrator handles
                        // weight + tag + regime bucket internally.
                        com.bot.DecisionEngineMerged.getCalibrator().recordOutcomeExtended(
                                fr.symbol, sigProb01, correct, atrPctForBucket,
                                calWeight, outcomeTag, fr.btcRegimeAtSignal,
                                fr.entryPrice, currentPrice);
                        String cat = com.bot.DecisionEngineMerged.detectAssetType(fr.symbol).label;
                        recordSignalOutcome(fr.symbol, fr.signalProbability, cat, correct);
                    } catch (Throwable ignored) {}
                }

                int total = forecastTotal.get();
                int correct2 = forecastCorrect.get();
                double acc = total > 0 ? (double) correct2 / total * 100 : 0;
                LOG.info(String.format("[FC] %s bias=%s outcome=%s w=%.1f %s | Acc: %.0f%% (%d/%d) Amb:%d TS:%d",
                        fr.symbol, fr.forecastBias, outcome, calWeight, correct ? "✅" : "❌",
                        acc, correct2, total,
                        forecastAmbiguous.get(), forecastTimeStop.get()));

                // [v9.9 2026-05-29] TRADE OUTCOME NOTIFICATION — user-facing.
                // Раньше user видел signal в Telegram, потом ничего пока не
                // приходил Daily Integrity Report. Теперь сразу как verifier
                // resolved outcome — short Telegram с результатом, движением
                // цены, cumulative WR. Это replaces spam heartbeats (которые
                // теперь только каждые 6h).
                try {
                    boolean isLong = fr.side == com.bot.TradingCore.Side.LONG;
                    double pnlPct = isLong
                            ? (currentPrice - fr.entryPrice) / fr.entryPrice * 100.0
                            : (fr.entryPrice - currentPrice) / fr.entryPrice * 100.0;
                    String emoji;
                    String outcomeLabel;
                    switch (outcome) {
                        case "TP1":       emoji = "✅"; outcomeLabel = "TP1 HIT";        break;
                        case "SL":        emoji = "❌"; outcomeLabel = "SL HIT";         break;
                        case "AMBIGUOUS": emoji = "🟡"; outcomeLabel = "AMBIGUOUS (½)"; break;
                        case "TIME_STOP": emoji = "⏳"; outcomeLabel = "TIME-STOP";      break;
                        default:          emoji = "⚪"; outcomeLabel = outcome;
                    }
                    String durationMin = String.valueOf(fr.ageMs() / 60_000L);
                    String tradeNotif = String.format(
                            "%s *%s* — %s %s\n"
                                    + "Entry: `%.6f` → Exit: `%.6f`\n"
                                    + "Movement: `%+.2f%%` · Held: %s мин\n"
                                    + "━━━━━━━━━━━━━━━━━━━\n"
                                    + "📊 Live WR: *%d/%d (%.0f%%)* · Cal n=%d",
                            emoji, outcomeLabel, fr.symbol, isLong ? "LONG" : "SHORT",
                            fr.entryPrice, currentPrice, pnlPct, durationMin,
                            correct2, total, acc,
                            com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount());
                    telegram.sendMessageAsync(tradeNotif);
                } catch (Throwable notifEx) {
                    LOG.fine("[FC] notification failed: " + notifEx.getMessage());
                }
            } catch (Exception ex) {
                LOG.fine("[FC] Fetch fail: " + fr.symbol + " " + ex.getMessage());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // [v79 I7] Forecast records persistence — survive Railway restarts.
    // ═══════════════════════════════════════════════════════════════════
    private static synchronized void saveForecastRecords() {
        try {
            java.io.File f = new java.io.File(FORECAST_PERSIST_FILE);
            java.io.File parent = f.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            try (java.io.PrintWriter pw = new java.io.PrintWriter(
                    new java.io.BufferedWriter(new java.io.FileWriter(f)))) {
                pw.println("# forecast_records v1");
                pw.println("# format: sym;side;entry;bias;score;prob;atrPct;tp1;sl;regime;createdAt;resolved;outcome;counted");
                // [v86.45] Persist the RESOLVED-outcome counters too. The Daily Integrity
                // Report's "Verifier X/Y wins" is the Gate-2 GO/NO-GO evidence (need 30-50
                // signals WR>=55% before arming real money) — but the counters were in-memory
                // only and reset to 0/0 on every restart (deploy or watchdog self-heal), while
                // pending records survived. Losing the tally mid-phase = losing the forward
                // proof. Parsed by loadForecastRecords BEFORE the generic '#' comment skip.
                pw.println("#COUNTERS|" + forecastTotal.get() + "|" + forecastCorrect.get()
                        + "|" + forecastAmbiguous.get() + "|" + forecastTimeStop.get());
                // [v86.59] per-regime·side live tally — survives restarts like #COUNTERS
                for (Map.Entry<String, int[]> re : regimeOutcomes.entrySet()) {
                    pw.println("#REGIME|" + re.getKey() + "|"
                            + re.getValue()[0] + "|" + re.getValue()[1]);
                }
                long now = System.currentTimeMillis();
                for (Map.Entry<String, ForecastRecord> e : forecastRecords.entrySet()) {
                    ForecastRecord fr = e.getValue();
                    // Skip records older than 6h (no longer relevant on restart)
                    if (now - fr.createdAt > 6 * 60 * 60_000L) continue;
                    pw.print(e.getKey()); pw.print('|');
                    pw.println(fr.toCsvLine());
                }
            }
        } catch (Throwable t) { LOG.warning("[ForecastSave] " + t.getMessage()); }
    }
    private static synchronized void loadForecastRecords() {
        try {
            java.io.File f = new java.io.File(FORECAST_PERSIST_FILE);
            if (!f.exists()) return;
            int restored = 0;
            long maxSeq = 0;
            try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(f))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // [v86.59] Restore per-regime·side tally (before the generic '#' skip).
                    if (line.startsWith("#REGIME|")) {
                        try {
                            String[] p = line.split("\\|");
                            regimeOutcomes.put(p[1],
                                    new int[]{Integer.parseInt(p[2]), Integer.parseInt(p[3])});
                        } catch (Throwable ignored) {}
                        continue;
                    }
                    // [v86.45] Restore resolved-outcome counters (must run before the '#' skip).
                    if (line.startsWith("#COUNTERS|")) {
                        try {
                            String[] p = line.split("\\|");
                            forecastTotal.set(Integer.parseInt(p[1]));
                            forecastCorrect.set(Integer.parseInt(p[2]));
                            forecastAmbiguous.set(Integer.parseInt(p[3]));
                            forecastTimeStop.set(Integer.parseInt(p[4]));
                            LOG.info("[ForecastLoad] counters restored: " + p[1] + "/" + p[2]
                                    + " amb=" + p[3] + " ts=" + p[4]);
                        } catch (Throwable ignored) {}
                        continue;
                    }
                    if (line.startsWith("#") || line.isBlank()) continue;
                    int barIdx = line.indexOf('|');
                    if (barIdx < 0) continue;
                    String key = line.substring(0, barIdx);
                    String csv = line.substring(barIdx + 1);
                    ForecastRecord fr = ForecastRecord.fromCsvLine(csv);
                    if (fr != null) {
                        forecastRecords.put(key, fr);
                        restored++;
                        // [v79 FIX I7] Track highest sequence number across all loaded keys.
                        // Keys look like "BTCUSDT_LONG_42" — trailing number after last '_'.
                        // If forecastSeq isn't bumped past max(restored), the next signal
                        // for the same symbol+side will collide with a still-unresolved
                        // loaded record and silently overwrite it.
                        int us = key.lastIndexOf('_');
                        if (us > 0 && us < key.length() - 1) {
                            try {
                                long s = Long.parseLong(key.substring(us + 1));
                                if (s > maxSeq) maxSeq = s;
                            } catch (NumberFormatException ignored) {}
                        }
                    }
                }
            }
            // [v79 FIX I7] Set seq to max+1 — guarantees no collision with restored keys.
            // Use an atomic CAS-loop in case scheduler somehow already started signal flow.
            while (true) {
                long cur = forecastSeq.get();
                if (cur >= maxSeq) break;
                if (forecastSeq.compareAndSet(cur, maxSeq)) break;
            }
            LOG.info("[ForecastLoad] restored " + restored + " unresolved records, seq advanced to " + forecastSeq.get());
        } catch (Throwable t) { LOG.warning("[ForecastLoad] " + t.getMessage()); }
    }

    // ═══════════════════════════════════════════════════════════════════
    // [v79 I6] Daily integrity report — public-verifiable stats sent to Telegram.
    //   Если бот говорит "WR=65%", цифра должна совпадать с тем, что
    //   калибратор хранит в state. Эта функция показывает обе цифры
    //   рядом — если они расходятся, видно сразу.
    // ═══════════════════════════════════════════════════════════════════
    private static volatile long lastIntegrityReportMs = 0;
    private static void sendIntegrityReport(com.bot.TelegramBotSender telegram) {
        long now = System.currentTimeMillis();
        if (now - lastIntegrityReportMs < 23 * 60 * 60_000L) return; // ~daily
        lastIntegrityReportMs = now;
        try {
            int verifierTotal = forecastTotal.get();
            int verifierWins  = forecastCorrect.get();
            int verifierAmb   = forecastAmbiguous.get();
            int verifierTS    = forecastTimeStop.get();
            double verifierAcc = verifierTotal > 0 ? (double) verifierWins / verifierTotal * 100 : 0;

            int calN = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            String calIntegrity = com.bot.DecisionEngineMerged.getCalibrator().verifyAuditIntegrity();

            String regime = lastBtcRegimeForAlert == null ? "?" : lastBtcRegimeForAlert;
            String paperFlag = OBSERVATION_MODE ? "🧪 PAPER" : "🔴 LIVE";

            // [v86.59] разрез live-исходов «режим BTC · сторона» — видно, ГДЕ бот
            // сливает (например NEUTRAL·S 0/4 = шорты в флэте сквизит). База для
            // будущих решений (флэт-вето), которые бектест проверить не может.
            StringBuilder regimeLines = new StringBuilder();
            if (!regimeOutcomes.isEmpty()) {
                regimeLines.append("По режимам (режим·сторона W/N):\n");
                regimeOutcomes.entrySet().stream()
                        .sorted((a, b) -> b.getValue()[0] - a.getValue()[0])
                        .limit(8)
                        .forEach(e -> regimeLines.append(String.format("  %s: %d/%d\n",
                                e.getKey(), e.getValue()[1], e.getValue()[0])));
            }

            telegram.sendMessageAsync(String.format(
                    "📊 *Daily Integrity Report*\n"
                            + "━━━━━━━━━━━━━━━━━━━━━\n"
                            + "Verifier: *%d/%d wins* (%.0f%%)\n"
                            + "Ambiguous: %d (½ credit) · Time-stop: %d (loss)\n"
                            + "%s"
                            + "Calibrator: n=%d outcomes\n"
                            + "Audit log: %s\n"
                            + "━━━━━━━━━━━━━━━━━━━━━\n"
                            + "BTC: %s · %s\n"
                            + "_Числа выше — независимо проверяемые:_\n"
                            + "_calibrator.csv + audit.log с HMAC-SHA256_",
                    verifierWins, verifierTotal, verifierAcc,
                    verifierAmb, verifierTS, regimeLines.toString(), calN, calIntegrity,
                    regime, paperFlag));
        } catch (Throwable t) {
            LOG.warning("[IntegrityReport] " + t.getMessage());
        }
    }

    /** [v79 I6] Public API for third-party verification. Returns JSON-ish string. */
    public static String forecastIntegrityCheck() {
        int total = forecastTotal.get();
        int wins  = forecastCorrect.get();
        int amb   = forecastAmbiguous.get();
        int ts    = forecastTimeStop.get();
        int calN  = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
        return String.format("{\"verifier_total\":%d,\"verifier_wins\":%d," +
                        "\"ambiguous\":%d,\"time_stops\":%d,\"calibrator_n\":%d," +
                        "\"win_rate_pct\":%.2f,\"audit_status\":\"%s\"}",
                total, wins, amb, ts, calN,
                total > 0 ? (double) wins / total * 100 : 0,
                com.bot.DecisionEngineMerged.getCalibrator().verifyAuditIntegrity());
    }

    private static void runWatchdog(com.bot.TelegramBotSender telegram,
                                    com.bot.SignalSender sender) {
        long now = System.currentTimeMillis();
        if (sender.isRlBanned()) return;
        if (now - startTimeMs < 10 * 60_000L) return;

        // [v86.30] SELF-HEAL a genuinely HUNG main cycle. runCycle is scheduleAtFixedRate on a
        // single thread — if one execution blocks (e.g. a no-timeout HTTP call), every later cycle
        // is blocked forever and this watchdog otherwise only ALERTS, so the bot silently stops
        // trading (this is exactly what happened: cycles froze after #4). If the cycle has been
        // silent >15 min (3 missed 5-min cycles = real hang, not a pause), exit for a CLEAN Railway
        // auto-restart. Safe: fires ONLY on a real hang (stale lastCycleSuccessMs — if logs were
        // merely delayed but the bot cycled, this won't fire); clean restart = no double-trades;
        // the 10-min startup grace above covers the boot backtest.
        if (now - lastCycleSuccessMs > 15 * 60_000L) {
            LOG.severe("[Watchdog] MainCycle HUNG " + ((now - lastCycleSuccessMs) / 60_000L)
                    + "min — System.exit(1) for clean Railway auto-restart");
            try {
                if (telegram != null) telegram.sendMessageAsync("💀 Главный цикл завис >15 мин — перезапускаю бота (auto-recovery)");
                Thread.sleep(2000);
            } catch (Throwable ignored) {}
            System.exit(1);
        }

        List<String> infraIssues = new ArrayList<>();
        if (now - lastCycleSuccessMs > 3 * 60_000L)
            infraIssues.add("💀 MainCycle silent " + (now - lastCycleSuccessMs) / 1000 + "s");
        if (now - lastStatsSuccessMs > 40 * 60_000L)
            infraIssues.add("💀 Stats silent " + (now - lastStatsSuccessMs) / 60_000 + "min");
        if (sender.getActiveWsCount() < 10)
            infraIssues.add("⚠️ WebSockets low: " + sender.getActiveWsCount());

        boolean signalDrought = now - lastSignalMs > SIGNAL_DROUGHT_MS;

        if (sender.getActiveWsCount() < 10 || now - lastCycleSuccessMs > 3 * 60_000L) {
            sender.forceResubscribeTopPairs();
        }

        if (!infraIssues.isEmpty() && now - lastInfraAlertMs > INFRA_ALERT_COOLDOWN_MS) {
            lastInfraAlertMs = now;
            watchdogAlerts.incrementAndGet();
            LOG.warning("[Watchdog #" + watchdogAlerts.get() + "] "
                    + String.join(" | ", infraIssues));
        }

        if (signalDrought && droughtAnnounced.compareAndSet(false, true)) {
            try {
                Dispatcher disp = Dispatcher.getInstance();
                String breakdown = (disp != null) ? disp.getBlockBreakdown() : "n/a";
                long hoursSilent = (now - lastSignalMs) / (60 * 60_000L);
                String regime = ctxRegimeForAlert();
                int wsCount = sender.getActiveWsCount();
                int calN = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
                String paperFlag = OBSERVATION_MODE ? "🧪 PAPER" : "🔴 LIVE";
                telegram.sendMessageAsync(String.format(
                        "📭 *Тихо на рынке* %dh\n"
                                + "BTC: %s | WS: %d | Cal n=%d | %s\n"
                                + "Блокировки: %s\n"
                                + "_(`bi`=cooldown, `rr`=R:R<2.0, `sl`=SL<0.70%%, `cold`=cold-start, `dd`=dedup, `hr`=hourly cap, `xch`=cross-exch, `qg`=quality)_",
                        hoursSilent, regime, wsCount, calN, paperFlag, breakdown));
            } catch (Throwable ignored) {}
        }
    }

    private static String ctxRegimeForAlert() {
        return lastBtcRegimeForAlert != null ? lastBtcRegimeForAlert : "?";
    }
    static volatile String lastBtcRegimeForAlert = null;

    private static volatile long lastHeartbeatMs = 0;
    // [v9.9 2026-05-29] HEARTBEAT 90 min → 6h. Раньше каждые 90мин в тихом
    // рынке шло "💓 Heartbeat" = spam без actionable info. Теперь только
    // каждые 6h, и только если 6h без signals. Реальная информация о трейдах
    // приходит через notifyTradeOutcome() при resolution каждого signal.
    private static final long HEARTBEAT_INTERVAL_MS = 6 * 60 * 60_000L;
    private static final long HEARTBEAT_QUIET_MS    = 6 * 60 * 60_000L;

    private static void maybeSendHeartbeat(com.bot.TelegramBotSender telegram,
                                           com.bot.SignalSender sender,
                                           com.bot.GlobalImpulseController gic,
                                           com.bot.InstitutionalSignalCore isc) {
        long now = System.currentTimeMillis();
        if (now - startTimeMs < 30 * 60_000L) return;
        if (now - lastHeartbeatMs < HEARTBEAT_INTERVAL_MS) return;
        if (now - lastSignalMs   < HEARTBEAT_QUIET_MS) return;
        try {
            Dispatcher disp = Dispatcher.getInstance();
            String breakdown = (disp != null) ? disp.getBlockBreakdown() : "n/a";
            int wsCount = sender.getActiveWsCount();
            int calN    = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
            com.bot.GlobalImpulseController.GlobalContext gc = gic.getContext();
            lastBtcRegimeForAlert = String.valueOf(gc.regime);
            long minSilent = (now - lastSignalMs) / 60_000L;
            String paperFlag = OBSERVATION_MODE ? "🧪 PAPER" : "🔴 LIVE";

            // [A3 2026-05-08] DE-side rejection trace. peekRejectTrace() does NOT reset
            // counters — getAndResetRejectTrace() (called elsewhere by SignalSender DIAG
            // logs) still owns the reset semantics, so the same trace can be sent to
            // both heartbeat (humans) and diag log (Railway logs) without one wiping
            // the other. Top-8 keys is enough to reveal the dominant funnel-killer.
            String deRejects = "";
            try {
                deRejects = com.bot.DecisionEngineMerged.peekRejectTrace(8);
            } catch (Throwable ignored) {}

            // [A3 2026-05-08] processPair-side rejection breakdown (liq/corr/early/final/isc).
            // Same fail-soft contract: if SignalSender doesn't expose the getter we just skip.
            String ppRejects = "";
            try {
                ppRejects = sender.getProcessPairBreakdown();
            } catch (Throwable ignored) {}

            StringBuilder sb = new StringBuilder();
            sb.append(String.format(
                    "💓 *Heartbeat* (%dмин без сигнала)\n"
                            + "BTC: %s str=%.2f | WS: %d | Cal n=%d | %s\n"
                            + "Cycles: %d | Errors: %d\n"
                            + "Блок: %s",
                    minSilent, gc.regime, gc.impulseStrength, wsCount, calN, paperFlag,
                    totalCycles.get(), errorCount.get(), breakdown));
            if (ppRejects != null && !ppRejects.isEmpty()) {
                sb.append("\nprocessPair: ").append(ppRejects);
            }
            if (deRejects != null && !deRejects.isEmpty()) {
                sb.append("\nDE-rejects: ").append(deRejects);
            }
            // [v87.2] Диагностика захвата ликвидаций (#3) прямо в heartbeat — видно в Telegram,
            // доходит ли поток до бота (raw>0 = доходит). Fail-soft.
            String liqDiag = "";
            try { liqDiag = sender.getLiqCaptureDiag(); } catch (Throwable ignored) {}
            if (liqDiag != null && !liqDiag.isEmpty()) {
                sb.append("\nLIQ#3: ").append(liqDiag);
            }
            telegram.sendMessageAsync(sb.toString());
            lastHeartbeatMs = now;
        } catch (Throwable ignored) {}
    }

    private static void runPeriodicBacktest(com.bot.SignalSender sender,
                                            com.bot.InstitutionalSignalCore isc) {
        com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();
        LinkedHashSet<String> universe = new LinkedHashSet<>(List.of(
                "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "LINKUSDT", "XRPUSDT"));
        universe.addAll(sender.getScanUniverseSnapshot(6));
        double totalEV = 0; int count = 0;
        for (String sym : universe) {
            try {
                Thread.sleep(3_000L);
                List<com.bot.TradingCore.Candle> m15 = sender.fetchKlines(sym, PRIMARY_TF, 300);
                List<com.bot.TradingCore.Candle> h1  = sender.fetchKlines(sym, HTF_FAST,  100);
                List<com.bot.TradingCore.Candle> m1  = sender.getM1FromWs(sym);
                List<com.bot.TradingCore.Candle> m5  = sender.fetchKlines(sym, "5m",  200);
                int periodicMinBars = PRIMARY_IS_15M ? 200 : 150;
                if (m15 == null || m15.size() < periodicMinBars) continue;
                com.bot.DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(sym);
                com.bot.SimpleBacktester.BacktestResult r = bt.run(sym, m1, m5, m15, h1, cat);
                if (r.total >= 5) {
                    totalEV += r.ev; count++;
                    isc.setSymbolBacktestResult(sym, r.ev);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt(); return;
            } catch (Exception ignored) {}
        }
        if (count > 0) {
            double avgEV = totalEV / count;
            isc.setBacktestResult(avgEV, System.currentTimeMillis());
        }
    }

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
    private static void runStartupBacktest(com.bot.SignalSender sender,
                                           com.bot.InstitutionalSignalCore isc,
                                           com.bot.TelegramBotSender telegram,
                                           String calibratorFilePath,
                                           boolean measureOnly) {
        long t0 = System.currentTimeMillis();
        LOG.info("[STARTUP-BT] ▶ Starting full backtest pre-training…");
        try {
            telegram.sendMessageAsync(
                    "🔄 *Стартовый backtest запущен*\n"
                            + "━━━━━━━━━━━━━━━━━━━━━\n"
                            + "Фильтр ликвидности: TRADE_TIER="
                            + sender.getTradeTier()   // [v86.44] single source of truth (banner used to show its own default "TOP" while the filter ran TOPALT)
                            + " (TOP=только ликвид · TOPALT=без меме · ALL=всё)\n"
                            + "Бот собирает историю по парам и обучает\n"
                            + "калибратор на симулированных сделках.\n"
                            + "Длится 5–20 минут. Live-сигналы идут параллельно.\n"
                            + "Сводка придёт по завершении.");
        } catch (Throwable ignored) {}

        // [FIX-1] Wait 3 min before starting fetches — first main cycle starts
        // at t+90s, fetches BTC+ETH+top pairs and populates cachedPairs.
        // By t+180s the rate-limit headroom recovers from boot warmup.
        try { Thread.sleep(180_000L); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }

        // Universe size — default 50, controlled via STARTUP_BT_PAIRS env.
        // Each pair = ~7sec (paging + fetch), so 50 pairs ≈ 6 minutes.
        final int btPairsLimit = Math.max(10, envInt("STARTUP_BT_PAIRS", 35));  // [v86.39] 25→35: больше пар = больше сделок/период для честного walk-forward (env-override есть)

        // 1. Universe — wait for at least 80% target loaded (gives cachedPairs
        // time to actually fill, not just have 1 pair). Hard timeout still 60s
        // — if loader is slow, proceed with whatever we have.
        List<String> universe = new ArrayList<>();
        int targetMin = Math.max(10, (int) (btPairsLimit * 0.8));
        for (int waitS = 0; waitS < 60 && universe.size() < targetMin; waitS++) {
            try { Thread.sleep(2_000L); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            universe = sender.getScanUniverseSnapshot(btPairsLimit);
        }
        if (universe.isEmpty()) {
            LOG.warning("[STARTUP-BT] universe empty — aborted");
            try { telegram.sendMessageAsync(
                    "⚠️ *Стартовый backtest пропущен*\n"
                            + "Не удалось получить список пар (universe empty).\n"
                            + "Бот продолжает работу в обычном режиме.\n"
                            + "Калибратор будет обучаться на live-сделках."); }
            catch (Throwable ignored) {}
            return;
        }
        LOG.info("[STARTUP-BT] Universe: " + universe.size() + " pairs");

        // 2. Backtester instance.
        com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();

        // 3. Aggregate counters — separate failures from low-data skips.
        int totalTrades = 0, totalWins = 0, totalLosses = 0, totalTimeStops = 0;
        int totalBE = 0, totalProfitLock = 0, totalTrail = 0, totalStag = 0;
        double totalNetPnL = 0.0;
        double totalGrossPnL = 0.0, totalFeesAgg = 0.0, totalSlipAgg = 0.0, totalFundAgg = 0.0;  // [v86.67] cost-decomp
        java.util.List<Double> allPnls = new ArrayList<>();  // [v86.70] bootstrap-CI значимости edge + Monte-Carlo
        // [v86.53 EXIT-SHADOW] суммы по 5 вариантам геометрии выхода (см. SimpleBacktester)
        int shadowN = 0;
        double[] shadowNet = new double[5];
        int[] shadowWins = new int[5];
        // [v86.54] теневые суммы по WF-периодам: [период][вариант] — отвечает на
        // решающий вопрос «выживает ли D/B в чопе П2/П4».
        double[][] shadowHalfPnL = new double[4][5];
        // [v86.56 MR-SHADOW] спящая mean-rev (STRATEGY_MODE=MR) measure-only на тех же
        // данных. Вопрос портфеля: зелёная ли MR в чопе П2/П4, где TREND красный?
        // Если да — взаимодополнение = легальный путь к 4/4. Калибратор НЕ кормится.
        int mrTrades = 0, mrWins = 0;
        double mrNet = 0.0;
        double[] mrHalfPnL = new double[4];
        // [v86.83 FLOW-FALSIFY] bucket MR trades by entry flow-EXHAUSTION (the FLOW_FADE thesis:
        // aggressor-flow turning AGAINST the trade side into the extreme = absorption). Cheap
        // falsification on the 507-trade MR sample BEFORE writing a FLOW_FADE generator — if the
        // exhaustion bucket is NOT less-bad than the rest, the absorption discriminator is inert.
        int mrExhN = 0, mrExhW = 0, mrNoexhN = 0, mrNoexhW = 0;
        double mrExhNet = 0.0, mrNoexhNet = 0.0;
        // [v86.90 CVD-RESUME FALSIFY] tag TREND trades by LEAK-FREE flow-resumption at the SIGNAL
        // bar (ei-1, ei-2 — strictly BEFORE the fill bar ei, so no v86.83-style entry-bar peek) in
        // the trade direction. Tests if an order-flow gate would cut the dead-cat reclaims that
        // reverse (user's "вошёл на верху → развернулось"). Cheap: runs on the real 66 TREND trades.
        int cvdResN = 0, cvdResW = 0, cvdNoresN = 0, cvdNoresW = 0;
        double cvdResNet = 0.0, cvdNoresNet = 0.0;
        // [v86.60 PHASE-0] PnL по возрасту 4h-тренда (TREND-сделки): бакеты
        // {0-2,3-5,6-12,13-24,25+} + решающая свёртка young(≤12)/old(>12) × WF.
        // Отвечает данными: живёт ли edge в МОЛОДЫХ трендах (тезис TREND_EARLY)?
        int[] ageN = new int[5];
        int[] ageWins = new int[5];  // [v86.63] WR по возрастным бакетам
        double[] agePnL = new double[5];
        int[] ageYoN = new int[2];
        double[][] ageYoHalf = new double[2][4];
        // [v86.60 TE-SHADOW] TREND_EARLY: 2 арма (struct ON / NSC) measure-only
        final String[] TE_TOKENS = {"TREND_EARLY", "TREND_EARLY_NSC"};
        int[] teTrades = new int[2], teWins = new int[2];
        double[] teNet = new double[2];
        double[][] teHalf = new double[2][4];
        int[] teLongN = new int[2], teShortN = new int[2], teLoN = new int[2];
        double[] teLongNet = new double[2], teShortNet = new double[2], teLoNet = new double[2];
        java.util.List<java.util.List<Double>> teSlPcts =
                java.util.List.of(new ArrayList<>(), new ArrayList<>());
        // [v86.69 FLOW-SHADOW] flow-gated чоп-стратегия (value-area break + taker-flow), measure-only
        int fbTrades = 0, fbWins = 0, fbLongN = 0, fbShortN = 0;
        double fbNet = 0.0, fbLongNet = 0.0, fbShortNet = 0.0;
        double[] fbHalf = new double[4];
        java.util.List<Double> fbSlPcts = new ArrayList<>();
        // [v86.84 FLOW_FADE-SHADOW] фейд границы рейнджа + flow-истощение, measure-only. Чоп-нога
        // (htfSep<TA_HTF_SEP_MIN = дополнение TREND), где TREND молчит. Никогда не кормит калибратор/ISC.
        int ffTrades = 0, ffWins = 0;
        double ffNet = 0.0;
        double[] ffHalfPnL = new double[4];
        // [v86.89 ABSORB-SHADOW] поглощение→пробой + flow (структурный инверс MOMENTUM), measure-only.
        // На ТЕХ ЖЕ свечах. Никогда не кормит калибратор/ISC; только строка сравнения в сводке.
        int abTrades = 0, abWins = 0;
        double abNet = 0.0;
        double[] abHalfPnL = new double[4];
        // [v83.7] Разбивка по тирам уверенности (confidence/score, шкала 0-100).
        // Цель: увидеть, есть ли edge у "уверенных" сигналов (какой score-тир в
        // плюсе), чтобы оставить только их. Бины по 5: 50-55,55-60,...,75+.
        final int CONF_TIERS = 6;
        int[]    tierN   = new int[CONF_TIERS];
        int[]    tierWin = new int[CONF_TIERS];
        double[] tierPnL = new double[CONF_TIERS];
        // [v84.3] Раскол 30-дн окна на 4 ПЕРИОДА (≈7.5 дн каждый) = walk-forward
        // ВНУТРИ 30 дней (юзер: только 30д, не 90, ради Railway). Edge = плюс на
        // ВСЕХ 4 периодах. Плюс в 1-2 из 4 → везение окна, не edge.
        final int WF_PERIODS = 4;
        int[]    halfN   = new int[WF_PERIODS];
        int[]    halfWin = new int[WF_PERIODS];
        double[] halfPnL = new double[WF_PERIODS];
        int symbolsRun = 0;          // pairs that completed bt.run()
        int symbolsRateLimited = 0;  // fetchKlines returned null
        int symbolsLowData = 0;      // fetched but <200 bars
        int symbolsErrored = 0;      // exception during run()

        com.bot.DecisionEngineMerged.ProbabilityCalibrator cal =
                com.bot.DecisionEngineMerged.getCalibrator();

        // [v82.1 / v90 1H-PRIMARY] History depth + pacing.
        // ENV:
        //  STARTUP_BT_BARS_PRIMARY — кол-во primary-TF свечей (default 720 для 1h
        //                            ≈ 30 дней; 1000 для 15m ≈ 10 дней). Cap 1500.
        //  STARTUP_BT_BARS_HTF     — кол-во HTF свечей (default 250). Минимум 150
        //                            = MIN_BARS движка.
        //  STARTUP_BT_PACING_MS    — задержка между парами (default 5000ms).
        final String btPrimaryTfEnv = System.getenv().getOrDefault("PRIMARY_TF", "1h").trim();
        final boolean btIs15m = "15m".equals(btPrimaryTfEnv);
        final boolean btIs4h  = "4h".equals(btPrimaryTfEnv);
        final boolean btIs30m = "30m".equals(btPrimaryTfEnv);
        // [v86.91] 4h: primary "4h", HTF "1d" (headline bug — was running 1h candles labelled 4h);
        // default 1620 primary bars = 270 days × 6 bars/day.
        // [30m] primary "30m", HTF "4h" (reuse wired 4h HTF); default 2880 primary bars = 60 days × 48 bars/day.
        final String btPrimaryTf = btIs15m ? "15m" : btIs30m ? "30m" : btIs4h ? "4h" : "1h";
        final String btHtfTf     = btIs15m ? "1h"  : btIs30m ? "4h"  : btIs4h ? "1d" : "4h";
        final int defaultPrimaryBars = btIs15m ? 1000 : btIs30m ? 2880 : btIs4h ? 1620 : 720; // 1h: 30 days / 4h: 270 days / 30m: 60 days

        // Backward compat: read both legacy STARTUP_BT_BARS_15M and new STARTUP_BT_BARS_PRIMARY.
        int legacyBars = envInt("STARTUP_BT_BARS_15M", -1);
        int primaryBarsCfg = (legacyBars > 0 && btIs15m) ? legacyBars
                : envInt("STARTUP_BT_BARS_PRIMARY", defaultPrimaryBars);
        // [v80.3 2026-05-31] Принудительный floor 2880 баров (30 дней на 15m) для
        // статистической надёжности. ROOT: 1500 баров = 49 trades = малая выборка →
        // дикий разброс backtest (−8%..+4.88% на ИДЕНТИЧНОМ коде). 2880 баров =
        // ~100 trades = стабильное число, отражает РЕАЛЬНЫЙ edge, не lucky window.
        // Это улучшает ИЗМЕРЕНИЕ, не стратегию. Решает "как понять edge не ждать месяц".
        // [v83.6 2026-06-01] 15m statFloor 5760→2880 (60→30 дней). ПРИЧИНА:
        // экономия Railway (меньше данных = короче прогон = дешевле) + 30 дней
        // (≈2880 баров 15m ≈ 100+ сделок) достаточно для оценки edge. 60 дней
        // были нужны для разовой сверки VCB vs 1h — больше не нужны.
        int statFloor = btIs15m ? 2880 : btIs30m ? 2880 : btIs4h ? 1620 : 1440; // [v86.39] 1h: 30дн→60дн; [v86.91] 4h: 1620 баров = 270 дней; [30m] 2880 баров = 60 дней (env STARTUP_BT_BARS_PRIMARY override; cap 6000)
        final int barsPrimaryTarget = Math.min(6000, Math.max(statFloor, primaryBarsCfg));
        int legacyHtf = envInt("STARTUP_BT_BARS_1H", -1);
        // [v86.91] 4h: HTF=1d, default 300 1d-bars (≈ the 270-day window + warmup).
        int htfBarsCfg = (legacyHtf > 0 && btIs15m) ? legacyHtf
                : envInt("STARTUP_BT_BARS_HTF", btIs30m ? 900 : btIs4h ? 300 : 250);
        // [v83.6] HTF floor 1440→720: на 15m primary HTF=1h, 30 дней = 720 баров.
        // Синхронно с откатом окна 60→30 дней (экономия Railway).
        // [v86.91] 4h: HTF=1d → floor 270 bars (=270 days), not 720 (=720 days, impossible).
        final int barsHtfTarget = Math.min(2000, Math.max(btIs30m ? 360 : btIs4h ? 270 : 720, htfBarsCfg));
        final long pacingMs     = Math.max(3000L, envInt("STARTUP_BT_PACING_MS", 8000));  // [v85.3] 5000→8000: мягче темп запросов
        // [v86.93] WINDOW-DIAG: surface the raw env + actual fetch window so a malformed
        // STARTUP_BT_BARS_PRIMARY (whitespace → envInt fallback, fixed v86.93) is visible in logs.
        LOG.info("[STARTUP-BT] WINDOW-DIAG PRIMARY_TF=" + btPrimaryTf
                + " rawEnv[STARTUP_BT_BARS_PRIMARY]=[" + System.getenv("STARTUP_BT_BARS_PRIMARY") + "]"
                + " cfg=" + primaryBarsCfg + " target=" + barsPrimaryTarget
                + " (~" + (barsPrimaryTarget / (btIs15m ? 96 : btIs30m ? 48 : btIs4h ? 6 : 24)) + "d) htfTarget=" + barsHtfTarget);

        // Min bars guard for primary TF: 200 on 15m, 150 on 1h.
        final int primaryMinBars = btIs15m ? 200 : 150;

        // [STARTUP-BT EARLY-STOP 2026-05-20] Останавливаемся когда набрали ровно
        // STARTUP_BT_TARGET_VALID_PAIRS пар с достаточной историей. Раньше шли
        // по всему universe (50 пар) и считали "Мало данных" как сводный счётчик.
        // Теперь Universe = overfetch buffer (45), цель = 30 валидных. Эффект:
        // "Мало данных" в сводке падает в 0, потому что мы прекращаем перебор
        // ДО того как доберёмся до молодых пар без истории.
        final int targetValidPairs = Math.max(5, envInt("STARTUP_BT_TARGET_VALID_PAIRS", 24));  // [v86.39] 15→24: больше независимых рынков = значимая выборка на период (честный 4/4, не подгонка)
        int validPairs = 0;

        // [v86.73] FIX: btGic в бэктесте был ЗАМОРОЖЕН NEUTRAL (никогда не кормился BTC) →
        // BTC-режим-гейт no-op в BT, но ЖИВОЙ в проде → BT оптимистичен в BTC-панику + BTC-aware
        // чоп-стратегии невалидируемы. Грузим BTC 1h ОДИН раз (то же якорное окно, v86.71);
        // SimpleBacktester per-bar обновляет btGic барами, закрытыми ≤ decisionTime (без look-ahead).
        try {
            List<com.bot.TradingCore.Candle> btcBt = fetchKlinesPaged(sender, "BTCUSDT", btPrimaryTf, barsPrimaryTarget);
            bt.setBtcCandles(btcBt);
            LOG.info("[STARTUP-BT] BTC-context candles=" + (btcBt != null ? btcBt.size() : 0));
        } catch (Exception be) {
            LOG.warning("[STARTUP-BT] BTC-context load failed: " + be.getMessage());
        }

        for (String sym : universe) {
            // Cooperative cancellation if JVM is shutting down.
            if (Thread.currentThread().isInterrupted()) {
                LOG.info("[STARTUP-BT] Interrupted — stopping early");
                break;
            }
            try {
                Thread.sleep(pacingMs);

                // Primary TF candles — пагинация снимает кэп Бинанса в 1500 баров.
                // На 15m это даёт 30+ дней истории вместо 15.6 дней.
                List<com.bot.TradingCore.Candle> m15 = fetchKlinesPaged(sender, sym, btPrimaryTf, barsPrimaryTarget);
                if (m15 == null || m15.isEmpty()) {
                    symbolsRateLimited++;
                    LOG.warning("[STARTUP-BT] " + sym + " — primary " + btPrimaryTf
                            + " fetch returned null (rate limit?)");
                    Thread.sleep(8_000L);
                    continue;
                }
                if (m15.size() < primaryMinBars) {
                    symbolsLowData++;
                    LOG.info("[STARTUP-BT] " + sym + " — only " + m15.size()
                            + " " + btPrimaryTf + " bars, need ≥" + primaryMinBars);
                    continue;
                }

                // HTF candles — тоже через пагинацию для consistency.
                Thread.sleep(1_500L);
                List<com.bot.TradingCore.Candle> h1 = fetchKlinesPaged(sender, sym, btHtfTf, barsHtfTarget);
                if (h1 == null || h1.isEmpty()) {
                    symbolsRateLimited++;
                    LOG.warning("[STARTUP-BT] " + sym + " — HTF " + btHtfTf
                            + " fetch returned null (rate limit?)");
                    Thread.sleep(8_000L);
                    continue;
                }
                // [v86.91] 4h-primary HTF=1d: 150 1d-bars = 150 days is over-strict; the engine
                // HTF only needs ~EMA50 depth. Floor 60 1d-bars for 4h, 150 otherwise.
                int htfStartupFloor = btIs4h ? 60 : 150;
                if (h1.size() < htfStartupFloor) {
                    symbolsLowData++;
                    LOG.info("[STARTUP-BT] " + sym + " — only " + h1.size()
                            + " " + btHtfTf + " bars, need ≥" + htfStartupFloor + " (engine MIN_BARS guard)");
                    continue;
                }

                // [STARTUP-BT EARLY-STOP] Пара прошла обе проверки данных
                // (primary≥minBars, HTF≥150). Это валидная пара для backtest.
                validPairs++;
                if (validPairs > targetValidPairs) {
                    LOG.info("[STARTUP-BT] Reached " + targetValidPairs
                            + " valid pairs — stopping early");
                    break;
                }

                // [PATCH B 2026-05-22] Fetch 5m candles for Breakout strategy.
                // Раньше передавался empty list → Breakout/PumpHunter мгновенно
                // отвергались на bo_insufficient_5m / silent null. Теперь Breakout
                // тестируется впервые. PumpHunter всё ещё нужно 1m — оставлен на
                // следующую итерацию (требует +7 мин к startup).
                //
                // Объём: 30 дней × 288 5m-баров/день = 8640 баров. Binance отдаёт
                // макс 1500/запрос → ~6 запросов на пару. Pacing 1.5s → +9 sec/pair.
                Thread.sleep(1_500L);
                List<com.bot.TradingCore.Candle> m5 = fetchKlinesPaged(sender, sym, "5m", 8640);
                if (m5 == null) m5 = new ArrayList<>();   // безопасный fallback
                // Не блочим прогон если 5m не загрузились — Breakout просто не сработает
                // на этой паре, остальные стратегии продолжат работать.
                LOG.info("[STARTUP-BT] " + sym + " 5m bars=" + m5.size());

                List<com.bot.TradingCore.Candle> empty = new ArrayList<>();
                com.bot.DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(sym);

                // [PATCH C 2026-05-22] Inject historical funding rates so that
                // FundingMomentum strategy can evaluate against the rate active at
                // each historical decision bar. Раньше fundingHistory оставался
                // пустым → fundingAt() возвращал 0.0 → setSimulatedFunding не
                // вызывался → fundingCache.get(symbol)=null → fm_no_funding_data.
                //
                // Окно: from primary candles' time range. Стоимость: 1 запрос
                // на пару (~300 funding events за 30 дней умещаются в один
                // /fapi/v1/fundingRate ответ с limit=1000).
                try {
                    long fhStart = m15.get(0).openTime;
                    long fhEnd   = m15.get(m15.size() - 1).openTime;
                    java.util.TreeMap<Long, Double> fundingHist =
                            com.bot.SimpleBacktester.fetchFundingHistory(sym, fhStart, fhEnd);
                    bt.setFundingHistory(fundingHist);
                    LOG.info("[STARTUP-BT] " + sym + " funding events="
                            + (fundingHist != null ? fundingHist.size() : 0));
                } catch (Exception fe) {
                    LOG.warning("[STARTUP-BT] " + sym + " funding history failed: "
                            + fe.getMessage() + " — FM disabled for this pair");
                    bt.setFundingHistory(new java.util.TreeMap<>()); // reset to empty
                }

                // [PATCH B] Pass real m5 — Breakout впервые получает данные.
                // c1 (1m) пока оставлен empty — PumpHunter не сработает в backtest.
                com.bot.SimpleBacktester.BacktestResult r =
                        bt.run(sym, empty, m5, m15, h1, cat);
                if (r == null || r.trades == null || r.trades.isEmpty()) {
                    symbolsRun++;
                    LOG.info("[STARTUP-BT] " + sym + " — 0 trades on history (filters too tight)");
                    continue;
                }

                // 4. FEED CALIBRATOR — this is the entire point of the exercise.
                // [A1 2026-05-08] Optional skip: SKIP_STARTUP_CALIBRATION=1 prevents
                // back-tested outcomes from being written into the live calibrator.
                // Reason: a strategy with negative edge on history (WR=32%, Net=-63%)
                // poisons the PAV regression — every live raw-prob is mapped to the
                // empirical loser distribution and reject("calibrated_lt_minConf_*")
                // becomes the dominant rejection reason.
                //
                // [v87 2026-05-09] DEFAULT FLIPPED 0→1.
                // Empirically: every prior backtest run that fed the live calibrator with
                // negative-edge data caused permanent reject("calibrated_lt_minConf_*")
                // downstream — calibrator learned "every signal loses" and snapped raw
                // scores to ~0.40, while MIN_CONF gate is 0.58. Result: 4 signals/28h
                // when paper mode would expect 30+.
                //
                // SAFE default = "do not poison". To opt INTO the old behavior (feed
                // calibrator from backtest), explicitly set env SKIP_STARTUP_CALIBRATION=0.
                // Only do that AFTER you've validated the strategy has positive edge on
                // history (otherwise you're feeding the calibrator a loser distribution).
                // [v82.9 2026-06-01] CATCH-22 FIX: default flipped "1"→"0" = FEED calibrator.
                // ПРОБЛЕМА: с default skip=ON стартовый backtest НЕ кормил калибратор,
                // live-сделок 0 → калибратор навсегда застревал на n=0 (Cal:DISABLED
                // passthrough). Бот месяцами слал НЕкалиброванные (сырые) вероятности.
                // БЕЗОПАСНОСТЬ: BIAS-GUARD ниже всё равно блокирует запись если backtest
                // показывает negative edge (Net<0 ИЛИ WR<40%) — отравить PAV нельзя.
                // Текущий backtest +13% WR 54% → edge положительный → калибратор стартует
                // с ~80 outcomes вместо 0. Откат: SKIP_STARTUP_CALIBRATION=1 в env.
                // [v86.70] default 0→1: бэктест БОЛЬШЕ НЕ кормит live-калибратор (in-sample +
                // survivorship-исходы заражали live-гейт). Калибратор теперь учится ТОЛЬКО на
                // живых/paper-исходах верификатора. Откат к старому: SKIP_STARTUP_CALIBRATION=0.
                boolean skipCalRecord = measureOnly || "1".equals(System.getenv()
                        .getOrDefault("SKIP_STARTUP_CALIBRATION", "1"));

                // [v87 BIAS-GUARD 2026-05-09] Even with skipCalRecord=false, refuse to
                // record outcomes if the backtest itself shows the strategy is losing
                // (Net < 0% OR WR < 40%). Feeding a known-bad strategy's outcomes to PAV
                // teaches it to under-rate EVERY future signal — the opposite of what
                // calibration is for.
                boolean negativeEdge = (r.netPnL < 0.0) || (r.winRate < 0.40);
                if (!skipCalRecord && negativeEdge) {
                    LOG.warning("[STARTUP-BT] " + sym
                            + " SKIPPING calibrator record: backtest shows negative edge "
                            + String.format("(WR=%.1f%% NetPnL=%+.2f%%) — would poison PAV.",
                            r.winRate * 100, r.netPnL));
                    skipCalRecord = true;
                }

                if (!skipCalRecord) {
                    for (com.bot.SimpleBacktester.TradeRecord tr : r.trades) {
                        boolean hit = tr.pnlPct > 0;
                        double rawScore01 = Math.max(0.0, Math.min(1.0, tr.confidence / 100.0));
                        double atrPctApprox = Math.abs(tr.entry > 0
                                ? (tr.sl - tr.entry) / tr.entry * 100.0 : 1.5);
                        // [v87] Updated to recognize new partial-close exit reasons from
                        // SimpleBacktester v10.1: TP1_TP2 (full TP success), TP1_BE (partial
                        // win + breakeven on remainder), TP1_TS (partial win + time-stop),
                        // SL (full loss), TIME_STOP (no movement).
                        String tag;
                        if ("TIME_STOP".equals(tr.exitReason) || "TP1_TS".equals(tr.exitReason)) {
                            tag = "TIME_STOP";
                        } else if (hit) {
                            tag = (tr.exitReason != null ? tr.exitReason : "TP1");
                        } else {
                            tag = "SL";
                        }
                        cal.recordOutcomeExtended(sym, rawScore01, hit, atrPctApprox,
                                1.0, tag, "NEUTRAL", tr.entry, tr.exit);
                    }
                }

                totalTrades       += r.total;
                totalWins         += r.wins;
                totalLosses       += r.losses;
                totalTimeStops    += r.timeStops;
                totalBE           += r.breakEvens;
                totalProfitLock   += r.profitLocks;
                totalTrail        += r.trailExits;
                totalStag         += r.stagnationExits;
                totalNetPnL       += r.netPnL;
                totalGrossPnL     += r.grossPnL;       // [v86.67] cost-decomp
                totalFeesAgg      += r.totalFees;
                totalSlipAgg      += r.totalSlippage;
                totalFundAgg      += r.totalFunding;
                // [v86.53 EXIT-SHADOW] aggregate shadow exit-variant results
                shadowN += r.shadowN;
                for (int sv = 0; sv < 5; sv++) {
                    shadowNet[sv]  += r.shadowNet[sv];
                    shadowWins[sv] += r.shadowWins[sv];
                }
                // [v83.7] bucket по тиру уверенности (score).
                // [v83.9] + раскол по времени (1-я/2-я половина окна пары).
                if (r.trades != null && !r.trades.isEmpty()) {
                    long tMin = Long.MAX_VALUE, tMax = Long.MIN_VALUE;
                    for (com.bot.SimpleBacktester.TradeRecord t : r.trades) {
                        if (t.entryTime < tMin) tMin = t.entryTime;
                        if (t.entryTime > tMax) tMax = t.entryTime;
                    }
                    long tSpan = Math.max(1L, tMax - tMin);
                    java.util.Map<Long, Integer> cvdIdx = new java.util.HashMap<>();  // [v86.90] m15 openTime→idx
                    for (int mi = 0; mi < m15.size(); mi++) cvdIdx.put(m15.get(mi).openTime, mi);
                    for (com.bot.SimpleBacktester.TradeRecord t : r.trades) {
                        int bi = (int) ((t.confidence - 50.0) / 5.0);
                        if (bi < 0) bi = 0;
                        if (bi >= CONF_TIERS) bi = CONF_TIERS - 1;
                        tierN[bi]++;
                        if (t.pnlPct > 0.05) tierWin[bi]++;
                        tierPnL[bi] += t.pnlPct;
                        allPnls.add(t.pnlPct);  // [v86.70] для bootstrap-CI / Monte-Carlo
                        // [v86.90 CVD-RESUME] leak-free flow-resumption tag (bars cei-1, cei-2 < fill bar cei)
                        Integer cei = cvdIdx.get(t.entryTime);
                        if (cei != null && cei >= 2) {
                            double d1 = 2.0 * m15.get(cei - 1).takerBuyBaseVolume - m15.get(cei - 1).volume;
                            double d2 = 2.0 * m15.get(cei - 2).takerBuyBaseVolume - m15.get(cei - 2).volume;
                            boolean tLong = t.side == com.bot.TradingCore.Side.LONG;
                            boolean resume = tLong ? (d1 > 0 && d1 >= d2) : (d1 < 0 && d1 <= d2);
                            boolean tWin = t.pnlPct > 0.05;
                            if (resume) { cvdResN++; if (tWin) cvdResW++; cvdResNet += t.pnlPct; }
                            else        { cvdNoresN++; if (tWin) cvdNoresW++; cvdNoresNet += t.pnlPct; }
                        }

                        int hi = (int) (WF_PERIODS * (t.entryTime - tMin) / tSpan);
                        if (hi < 0) hi = 0;
                        if (hi >= WF_PERIODS) hi = WF_PERIODS - 1;
                        halfN[hi]++;
                        if (t.pnlPct > 0.05) halfWin[hi]++;
                        halfPnL[hi] += t.pnlPct;
                        // [v86.60 PHASE-0] PnL по возрасту тренда
                        if (t.trendAge >= 0) {
                            int ab = t.trendAge <= 2 ? 0 : t.trendAge <= 5 ? 1
                                    : t.trendAge <= 12 ? 2 : t.trendAge <= 24 ? 3 : 4;
                            ageN[ab]++;
                            if (t.pnlPct > 0.05) ageWins[ab]++;  // [v86.63]
                            agePnL[ab] += t.pnlPct;
                            int yo = t.trendAge <= 12 ? 0 : 1;
                            ageYoN[yo]++;
                            ageYoHalf[yo][hi] += t.pnlPct;
                        }
                    }
                    // [v86.54] теневые варианты — в те же WF-периоды (тот же tMin/tSpan)
                    for (double[] srow : r.shadowTradeRows) {
                        int shi = (int) (WF_PERIODS * ((long) srow[0] - tMin) / tSpan);
                        if (shi < 0) shi = 0;
                        if (shi >= WF_PERIODS) shi = WF_PERIODS - 1;
                        for (int sv = 0; sv < 5; sv++) shadowHalfPnL[shi][sv] += srow[sv + 1];
                    }
                }
                symbolsRun++;
                // Existing per-symbol EV signal for ISC.
                if (r.total >= 5) isc.setSymbolBacktestResult(sym, r.ev);

                // [v86.56 MR-SHADOW] второй measure-only прогон: спящая mean-rev на ТЕХ ЖЕ
                // свечах (данные уже скачаны — это секунды CPU). Никогда не кормит
                // калибратор/ISC; только аккумулирует для строки сравнения в сводке.
                try {
                    bt.setStrategyModeOverride("MR");
                    com.bot.SimpleBacktester.BacktestResult r2 =
                            bt.run(sym, empty, m5, m15, h1, cat);
                    if (r2 != null && r2.trades != null && !r2.trades.isEmpty()) {
                        mrTrades += r2.total;
                        mrWins   += r2.wins;
                        mrNet    += r2.netPnL;
                        long tMin2 = Long.MAX_VALUE, tMax2 = Long.MIN_VALUE;
                        for (com.bot.SimpleBacktester.TradeRecord t : r2.trades) {
                            if (t.entryTime < tMin2) tMin2 = t.entryTime;
                            if (t.entryTime > tMax2) tMax2 = t.entryTime;
                        }
                        long tSpan2 = Math.max(1L, tMax2 - tMin2);
                        // [v86.83] index m15 by openTime for entry-bar flow lookup
                        java.util.Map<Long, Integer> mIdx = new java.util.HashMap<>();
                        for (int mi = 0; mi < m15.size(); mi++) mIdx.put(m15.get(mi).openTime, mi);
                        for (com.bot.SimpleBacktester.TradeRecord t : r2.trades) {
                            int hi2 = (int) (WF_PERIODS * (t.entryTime - tMin2) / tSpan2);
                            if (hi2 < 0) hi2 = 0;
                            if (hi2 >= WF_PERIODS) hi2 = WF_PERIODS - 1;
                            mrHalfPnL[hi2] += t.pnlPct;
                            // [v86.83 FLOW-FALSIFY] tag entry flow-exhaustion (FLOW_FADE core thesis)
                            Integer ei = mIdx.get(t.entryTime);
                            if (ei != null && ei >= 2) {
                                double f0 = m15.get(ei).takerBuySellRatio();
                                double f2 = m15.get(ei - 2).takerBuySellRatio();
                                boolean isLong = t.side == com.bot.TradingCore.Side.LONG;
                                // exhaustion = aggressor flow turning AGAINST the trade side into entry:
                                //   SHORT faded at top    → buyers fading  → flow falling & <0.50
                                //   LONG  faded at bottom  → sellers fading → flow rising  & >0.50
                                boolean exh = isLong
                                        ? (f0 - f2 >= 0.06 && f0 > 0.50)
                                        : (f2 - f0 >= 0.06 && f0 < 0.50);
                                boolean win = t.pnlPct > 0.05;
                                if (exh) { mrExhN++; if (win) mrExhW++; mrExhNet += t.pnlPct; }
                                else     { mrNoexhN++; if (win) mrNoexhW++; mrNoexhNet += t.pnlPct; }
                            }
                        }
                    }
                } catch (Throwable mrEx) {
                    LOG.fine("[STARTUP-BT] MR-shadow " + sym + " failed: " + mrEx.getMessage());
                } finally {
                    bt.setStrategyModeOverride(null);
                }

                // [v86.60 TE-SHADOW] TREND_EARLY: 2 measure-only прохода (struct/nsc)
                // на ТЕХ ЖЕ свечах. Никогда не кормит калибратор/ISC.
                for (int ti = 0; ti < TE_TOKENS.length; ti++) {
                    try {
                        bt.setStrategyModeOverride(TE_TOKENS[ti]);
                        com.bot.SimpleBacktester.BacktestResult r3 =
                                bt.run(sym, empty, m5, m15, h1, cat);
                        if (r3 != null && r3.trades != null && !r3.trades.isEmpty()) {
                            long tMin3 = Long.MAX_VALUE, tMax3 = Long.MIN_VALUE;
                            for (com.bot.SimpleBacktester.TradeRecord t : r3.trades) {
                                if (t.entryTime < tMin3) tMin3 = t.entryTime;
                                if (t.entryTime > tMax3) tMax3 = t.entryTime;
                            }
                            long tSpan3 = Math.max(1L, tMax3 - tMin3);
                            for (com.bot.SimpleBacktester.TradeRecord t : r3.trades) {
                                teTrades[ti]++;
                                teNet[ti] += t.pnlPct;
                                if (t.pnlPct > 0.05) teWins[ti]++;
                                int hi3 = (int) (WF_PERIODS * (t.entryTime - tMin3) / tSpan3);
                                if (hi3 < 0) hi3 = 0;
                                if (hi3 >= WF_PERIODS) hi3 = WF_PERIODS - 1;
                                teHalf[ti][hi3] += t.pnlPct;
                                if (t.side == com.bot.TradingCore.Side.LONG) {
                                    teLongN[ti]++; teLongNet[ti] += t.pnlPct;
                                } else {
                                    teShortN[ti]++; teShortNet[ti] += t.pnlPct;
                                }
                                if (t.confidence < 65) { teLoN[ti]++; teLoNet[ti] += t.pnlPct; }
                                if (t.entry > 0) teSlPcts.get(ti)
                                        .add(Math.abs(t.entry - t.sl) / t.entry * 100.0);
                            }
                        }
                    } catch (Throwable teEx) {
                        LOG.fine("[STARTUP-BT] TE-shadow " + sym + " failed: " + teEx.getMessage());
                    } finally {
                        bt.setStrategyModeOverride(null);
                    }
                }

                // [v86.69 FLOW-SHADOW] FLOW_BREAK: чоп-стратегия measure-only на ТЕХ ЖЕ свечах.
                try {
                    bt.setStrategyModeOverride("FLOW_BREAK");
                    com.bot.SimpleBacktester.BacktestResult r4 =
                            bt.run(sym, empty, m5, m15, h1, cat);
                    if (r4 != null && r4.trades != null && !r4.trades.isEmpty()) {
                        long tMin4 = Long.MAX_VALUE, tMax4 = Long.MIN_VALUE;
                        for (com.bot.SimpleBacktester.TradeRecord t : r4.trades) {
                            if (t.entryTime < tMin4) tMin4 = t.entryTime;
                            if (t.entryTime > tMax4) tMax4 = t.entryTime;
                        }
                        long tSpan4 = Math.max(1L, tMax4 - tMin4);
                        for (com.bot.SimpleBacktester.TradeRecord t : r4.trades) {
                            fbTrades++;
                            fbNet += t.pnlPct;
                            if (t.pnlPct > 0.05) fbWins++;
                            int hi4 = (int) (WF_PERIODS * (t.entryTime - tMin4) / tSpan4);
                            if (hi4 < 0) hi4 = 0;
                            if (hi4 >= WF_PERIODS) hi4 = WF_PERIODS - 1;
                            fbHalf[hi4] += t.pnlPct;
                            if (t.side == com.bot.TradingCore.Side.LONG) { fbLongN++; fbLongNet += t.pnlPct; }
                            else { fbShortN++; fbShortNet += t.pnlPct; }
                            if (t.entry > 0) fbSlPcts.add(Math.abs(t.entry - t.sl) / t.entry * 100.0);
                        }
                    }
                } catch (Throwable fbEx) {
                    LOG.fine("[STARTUP-BT] FLOW-shadow " + sym + " failed: " + fbEx.getMessage());
                } finally {
                    bt.setStrategyModeOverride(null);
                }

                // [v86.84 FLOW_FADE-SHADOW] фейд границы рейнджа + flow-истощение measure-only на
                // ТЕХ ЖЕ свечах. Никогда не кормит калибратор/ISC; только строка сравнения в сводке.
                try {
                    bt.setStrategyModeOverride("FLOW_FADE");
                    com.bot.SimpleBacktester.BacktestResult r5 =
                            bt.run(sym, empty, m5, m15, h1, cat);
                    if (r5 != null && r5.trades != null && !r5.trades.isEmpty()) {
                        ffTrades += r5.total;
                        ffWins   += r5.wins;
                        ffNet    += r5.netPnL;
                        long tMin5 = Long.MAX_VALUE, tMax5 = Long.MIN_VALUE;
                        for (com.bot.SimpleBacktester.TradeRecord t : r5.trades) {
                            if (t.entryTime < tMin5) tMin5 = t.entryTime;
                            if (t.entryTime > tMax5) tMax5 = t.entryTime;
                        }
                        long tSpan5 = Math.max(1L, tMax5 - tMin5);
                        for (com.bot.SimpleBacktester.TradeRecord t : r5.trades) {
                            int hi5 = (int) (WF_PERIODS * (t.entryTime - tMin5) / tSpan5);
                            if (hi5 < 0) hi5 = 0;
                            if (hi5 >= WF_PERIODS) hi5 = WF_PERIODS - 1;
                            ffHalfPnL[hi5] += t.pnlPct;
                        }
                    }
                } catch (Throwable ffEx) {
                    LOG.fine("[STARTUP-BT] FLOW_FADE-shadow " + sym + " failed: " + ffEx.getMessage());
                } finally {
                    bt.setStrategyModeOverride(null);
                }

                // [v86.89 ABSORB-SHADOW] поглощение→пробой + flow measure-only на ТЕХ ЖЕ свечах.
                // Структурный инверс MOMENTUM (стена поглощает поток → пробой стойла). Никогда не
                // кормит калибратор/ISC; только строка сравнения в сводке. Mirror of FLOW_FADE block.
                try {
                    bt.setStrategyModeOverride("ABSORB_BREAK");
                    com.bot.SimpleBacktester.BacktestResult r6 =
                            bt.run(sym, empty, m5, m15, h1, cat);
                    if (r6 != null && r6.trades != null && !r6.trades.isEmpty()) {
                        abTrades += r6.total;
                        abWins   += r6.wins;
                        abNet    += r6.netPnL;
                        long tMin6 = Long.MAX_VALUE, tMax6 = Long.MIN_VALUE;
                        for (com.bot.SimpleBacktester.TradeRecord t : r6.trades) {
                            if (t.entryTime < tMin6) tMin6 = t.entryTime;
                            if (t.entryTime > tMax6) tMax6 = t.entryTime;
                        }
                        long tSpan6 = Math.max(1L, tMax6 - tMin6);
                        for (com.bot.SimpleBacktester.TradeRecord t : r6.trades) {
                            int hi6 = (int) (WF_PERIODS * (t.entryTime - tMin6) / tSpan6);
                            if (hi6 < 0) hi6 = 0;
                            if (hi6 >= WF_PERIODS) hi6 = WF_PERIODS - 1;
                            abHalfPnL[hi6] += t.pnlPct;
                        }
                    }
                } catch (Throwable abEx) {
                    LOG.fine("[STARTUP-BT] ABSORB-shadow " + sym + " failed: " + abEx.getMessage());
                } finally {
                    bt.setStrategyModeOverride(null);
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt(); break;
            } catch (Exception e) {
                symbolsErrored++;
                LOG.warning("[STARTUP-BT] " + sym + " errored: " + e.getMessage());
            }
        }

        // 6. Persist calibrator immediately so a crash before the next 30-min
        //    auto-save would not wipe out the training.
        try {
            cal.saveToFile(calibratorFilePath);
        } catch (Throwable t) {
            LOG.warning("[STARTUP-BT] calibrator save failed: " + t.getMessage());
        }

        long elapsedSec = (System.currentTimeMillis() - t0) / 1000L;
        double wr = totalTrades > 0 ? 100.0 * totalWins / totalTrades : 0.0;
        double wlRatio = totalLosses > 0 ? (double) totalWins / totalLosses : 0.0;
        int newCalCount = cal.totalOutcomeCount();

        // [v83.7] Сводка по тирам уверенности — показывает, какой score-тир в плюсе.
        // Решает, какой порог BACKTEST_MIN_CONF / MIN_CONF оставить, чтобы торговать
        // только "уверенные" сигналы (цель ~100-200 сделок/мес на лучшем тире).
        StringBuilder tb = new StringBuilder("🎚 *По уверенности (score):*\n");
        String[] tierLbl = {"50-55", "55-60", "60-65", "65-70", "70-75", "75+"};
        for (int k = 0; k < CONF_TIERS; k++) {
            if (tierN[k] == 0) continue;
            double tWr  = 100.0 * tierWin[k] / tierN[k];
            double tAvg = tierPnL[k] / tierN[k];
            String mark = tierPnL[k] > 0 ? "🟢" : "🔴";
            tb.append(String.format(
                    "  %s %s: %d сд · WR %.0f%% · PnL %+.1f%% · avg %+.3f%%\n",
                    mark, tierLbl[k], tierN[k], tWr, tierPnL[k], tAvg));
        }
        // [v86.60 PHASE-0] PnL по возрасту 4h-тренда — решает ДАННЫМИ, где живёт
        // edge: в молодых трендах (тезис TREND_EARLY) или зрелых. Selection-bias
        // оговорка печатается: гистограмма обусловлена прошедшими гейты сделками.
        int ageTot = ageN[0] + ageN[1] + ageN[2] + ageN[3] + ageN[4];
        if (ageTot > 0) {
            tb.append("📅 PnL по возрасту 4h-тренда (баров с пересечения EMA):\n");
            String[] ageLbl = {"0-2", "3-5", "6-12", "13-24", "25+"};
            for (int k = 0; k < 5; k++) {
                if (ageN[k] == 0) continue;
                tb.append(String.format("  %s: %d сд · WR %.0f%% · %+.1f%% · %+.3f%%/сд\n",
                        ageLbl[k], ageN[k], 100.0 * ageWins[k] / ageN[k],
                        agePnL[k], agePnL[k] / ageN[k]));
            }
            tb.append(String.format(
                    "  young≤12: %d сд П1..П4 %+.0f/%+.0f/%+.0f/%+.0f · old>12: %d сд %+.0f/%+.0f/%+.0f/%+.0f\n",
                    ageYoN[0], ageYoHalf[0][0], ageYoHalf[0][1], ageYoHalf[0][2], ageYoHalf[0][3],
                    ageYoN[1], ageYoHalf[1][0], ageYoHalf[1][1], ageYoHalf[1][2], ageYoHalf[1][3]));
        }
        String tierBreakdown = tb.toString();

        // [v84.3] Walk-forward ВНУТРИ 30д: 4 периода. Edge = плюс на ВСЕХ 4.
        StringBuilder hb = new StringBuilder("🔭 *In-sample стабильность (4 периода × ~7.5д, бакеты по entryTime — НЕ OOS):*\n");
        String[] halfLbl = {"П1 (старый)", "П2", "П3", "П4 (свежий)"};
        int wfGreens = 0;
        for (int k = 0; k < WF_PERIODS; k++) {
            if (halfN[k] == 0) { hb.append("  ").append(halfLbl[k]).append(": нет сделок\n"); continue; }
            double hWr  = 100.0 * halfWin[k] / halfN[k];
            double hAvg = halfPnL[k] / halfN[k];
            boolean g = halfPnL[k] > 0 && halfN[k] >= 10;
            if (g) wfGreens++;
            hb.append(String.format(
                    "  %s %s: %d сд · WR %.0f%% · %+.1f%% · avg %+.3f%%\n",
                    g ? "🟢" : "🔴", halfLbl[k], halfN[k], hWr, halfPnL[k], hAvg));
        }
        hb.append(String.format("  _%d/4 периодов в плюс (IN-SAMPLE по entryTime, НЕ OOS — настоящий OOS приходит отдельным walk-forward сообщением)_\n", wfGreens));
        // [v86.54] shadow-варианты по периодам: C(текущий)/B(1.5R)/D(100%→TP2).
        // Решающий тест: если D/B бьют C и в красных периодах (П2/П4-чоп), смена
        // выхода робастна; если выигрывают только в трендовых П1/П3 — это window-fit.
        if (shadowN > 0) {
            hb.append("  🧪 _shadow C/B/D:_ ");
            for (int k = 0; k < WF_PERIODS; k++) {
                hb.append(String.format("П%d %+.0f/%+.0f/%+.0f", k + 1,
                        shadowHalfPnL[k][0], shadowHalfPnL[k][2], shadowHalfPnL[k][3]));
                if (k < WF_PERIODS - 1) hb.append(" · ");
            }
            hb.append("\n");
        }
        String halfBreakdown = hb.toString();

        // [v86.78] CI-ВЕРДИКТ: «🟢 РЕАЛЬНЫЙ edge» теперь требует, чтобы нижняя граница
        // bootstrap-CI95 на avg/сделку была > 0 (ПОСЛЕ косто́в). Раньше вердикт ВРАЛ —
        // зелёный по порогу Net PnL>20, хотя CI накрывал ноль (= возможно везучее окно).
        // Тот же seed, что у sigBlock → значения совпадают. Защита от арма реала на шуме.
        boolean ciSignificant = false;
        if (allPnls.size() >= 20) {
            int vB = 1000, vm = allPnls.size();
            double[] vMeans = new double[vB];
            java.util.Random vRng = new java.util.Random(20260614L);
            for (int b = 0; b < vB; b++) {
                double s = 0.0;
                for (int i = 0; i < vm; i++) s += allPnls.get(vRng.nextInt(vm));
                vMeans[b] = s / vm;
            }
            java.util.Arrays.sort(vMeans);
            ciSignificant = vMeans[(int) (0.025 * vB)] > 0;  // нижняя граница CI95 > 0
        }

        // [FIX-1] Differentiate technical failure vs strategy verdict.
        String verdict;
        boolean techFailure = totalTrades == 0
                && (symbolsRateLimited + symbolsErrored) >= Math.max(1, symbolsRun);
        if (techFailure) {
            verdict = "⚠️ *Технический сбой* — пары не загрузились.\n"
                    + "Калибратор не обучен через backtest. Бот продолжает\n"
                    + "работу — калибровка пойдёт через live-сделки (медленнее).";
        } else if (totalTrades == 0) {
            verdict = "ℹ️ *0 сделок на истории*\n"
                    + "Фильтры стратегии слишком строгие для последних 16 дней\n"
                    + "(BTC в глубоком флэте). Это не плохо — стратегия осторожна.\n"
                    + "Калибровка пойдёт через live.";
        } else if (totalTrades < 50) {
            // [HOLE-3 FIX 2026-05-15] Small sample size still requires PnL/WR sanity check.
            // OLD BUG: 43 trades with -39% NetPnL was labeled "малая выборка" (neutral),
            // hiding a clear loss pattern. At 5× leverage that's a -197% margin loss =
            // liquidation on real money. Sample-size doctrine should not override
            // glaring negative-edge evidence.
            //
            // New rule: if NetPnL ≤ -10% AND/OR WR < 25% on any sample size, raise
            // the alarm — direction is too clear to call "не хватает данных".
            int leverage = 1;
            try {
                leverage = com.bot.BinanceTradeExecutor.getInstance().getLeverage();
            } catch (Throwable ignore) {}
            double leveragedLoss = totalNetPnL * leverage;
            boolean clearlyNegative = (totalNetPnL <= -10.0) || (wr < 25.0);
            if (clearlyNegative) {
                StringBuilder vb = new StringBuilder();
                vb.append("🔴 *Малая выборка + явный negative edge*\n");
                vb.append(String.format("%d сделок · WR=%.1f%% · NetPnL=%+.1f%%\n",
                        totalTrades, wr, totalNetPnL));
                if (leverage > 1) {
                    vb.append(String.format(
                            "⚠️ С плечом %dx реальный убыток ≈ %+.1f%% от маржи",
                            leverage, leveragedLoss));
                    if (leveragedLoss <= -100.0) {
                        vb.append(" → *ЛИКВИДАЦИЯ*");
                    }
                    vb.append("\n");
                }
                vb.append("Размер выборки мал, но направление однозначное — стратегия теряет.\n");
                vb.append("*Real money категорически нельзя.*");
                verdict = vb.toString();
            } else {
                verdict = "🟡 *Малая выборка* — " + totalTrades + " сделок.\n"
                        + "Слишком мало для статистических выводов. Продолжайте paper.";
            }
        } else {
            // [v81 FIX] Оценка по PnL и Expectancy, не по WR.
            // При RR 1:2 стратегия с WR=35% уже прибыльна. WR в отрыве от RR
            // — бесполезная метрика. Старая логика "WR<45% = слабо" ВРАЛА:
            // помечала прибыльную стратегию (+33% PnL) как провальную.
            double avgPnLPerTrade = totalNetPnL / totalTrades;
            double expectancyR = avgPnLPerTrade / 1.0; // assume avg risk = 1% (estimate)
            double monthlyPnL  = avgPnLPerTrade * 30; // grobering daily count

            if (totalNetPnL > 20.0 && wr >= 35.0 && ciSignificant) {
                verdict = "🟢 *Стратегия показывает РЕАЛЬНЫЙ edge.* (CI95 исключает ноль)\n"
                        + "WR=" + String.format("%.1f%%", wr)
                        + " · Net PnL=" + String.format("%+.1f%%", totalNetPnL)
                        + " на " + totalTrades + " сделках.\n"
                        + "Avg/trade=" + String.format("%+.2f%%", avgPnLPerTrade) + ".\n"
                        + "Низкий WR при positive PnL — это профессиональная норма для RR 1:2.\n"
                        + "Дальше paper для подтверждения 100+ сделок.";
            } else if (totalNetPnL > 0 && !ciSignificant) {
                verdict = "🟡 *Положительный, но НЕ ДОКАЗАН.* Net PnL="
                        + String.format("%+.1f%%", totalNetPnL) + " (avg "
                        + String.format("%+.2f%%", avgPnLPerTrade) + "/сд), но\n"
                        + "bootstrap-CI95 НАКРЫВАЕТ НОЛЬ → статистически неотличим от нуля\n"
                        + "(возможно везение окна, не доказанный edge).\n"
                        + "Реал НЕЛЬЗЯ — нужно больше сделок / живое подтверждение.";
            } else if (totalNetPnL > 0 && wr >= 30.0) {
                verdict = "🟡 *Положительный edge, но слабый.*\n"
                        + "WR=" + String.format("%.1f%%", wr)
                        + " · Net PnL=" + String.format("%+.1f%%", totalNetPnL) + ".\n"
                        + "Прибыльно, но margin тонкая. Нужно больше сделок для подтверждения.\n"
                        + "Реальные деньги — только мелкими размерами после 100+ paper.";
            } else if (totalNetPnL > -5.0) {
                verdict = "🟡 *Граничный результат* — Net PnL="
                        + String.format("%+.1f%%", totalNetPnL) + ", WR=" + String.format("%.1f%%", wr) + ".\n"
                        + "Нет явного edge. Реальные деньги категорически нельзя.";
            } else {
                // [v87 LEVERAGE-WARN 2026-05-09] Backtest PnL is at 1× equiv. With leverage
                // > 1 the real account loss is multiplied. Make this explicit in the verdict
                // so a casual reader doesn't underestimate the disaster scale.
                int leverage = 1;
                try {
                    leverage = com.bot.BinanceTradeExecutor.getInstance().getLeverage();
                } catch (Throwable ignore) {}

                StringBuilder vb = new StringBuilder();
                vb.append("🔴 *Стратегия убыточна на истории.*\n");
                vb.append("WR=").append(String.format("%.1f%%", wr));
                vb.append(" · Net PnL=").append(String.format("%+.1f%%", totalNetPnL)).append(".\n");
                if (leverage > 1) {
                    double leveragedLoss = totalNetPnL * leverage;
                    vb.append(String.format(
                            "⚠️ С плечом %dx реальный убыток ≈ %+.1f%% от маржи",
                            leverage, leveragedLoss));
                    if (leveragedLoss <= -100.0) {
                        vb.append(" → *ЛИКВИДАЦИЯ*");
                    }
                    vb.append(".\n");
                }
                vb.append("Структурная проблема логики входа.\n");
                vb.append("Реальные деньги категорически нельзя.");
                verdict = vb.toString();
            }
        }

        // [v86.53 EXIT-SHADOW] таблица сравнения геометрий выхода на тех же входах.
        // Сравнивать строго МЕЖДУ СОБОЙ: C = контроль (текущая геометрия) в том же
        // упрощённом теневом движке (чистые брекеты, пессимистичный двойной-тач),
        // поэтому C ≠ основным цифрам сводки. Решение о смене выхода — только если
        // вариант стабильно бьёт C несколько прогонов подряд.
        String shadowBlock = "";
        // [v86.61] было if(shadowN>0) вокруг ВСЕГО блока — при пустом основном прогоне
        // (0 TREND-сделок) строки MR/TE-SHADOW копились, но не печатались (минор из
        // ревью v86.60). Теперь печатаем всё, что есть; EXIT-таблица — только при shadowN>0.
        if (shadowN > 0 || mrTrades > 0 || teTrades[0] > 0 || teTrades[1] > 0 || fbTrades > 0 || ffTrades > 0 || abTrades > 0) {
            String[] svName = {
                    "C 50%@TP1+BE (текущий)",
                    "F D+актив (PL+stag, ~live)",
                    "B 50%@1.0R+BE (старая)",
                    "D 100%→TP2, BE@1R (тень)",
                    "E 100%→TP2, без BE"};
            StringBuilder sb2 = new StringBuilder();
            if (shadowN > 0) {
                sb2.append("🧪 EXIT-SHADOW (").append(shadowN).append(" входов, чистые брекеты):\n");
                for (int sv = 0; sv < 5; sv++) {
                    sb2.append(String.format("  %s: WR %.0f%% · %+.1f%% · %+.3f%%/сд\n",
                            svName[sv], 100.0 * shadowWins[sv] / shadowN,
                            shadowNet[sv], shadowNet[sv] / shadowN));
                }
                sb2.append("  _сравнивать варианты между собой; C=контроль, движок упрощён_\n");
            }
            // [v86.56 MR-SHADOW] строка про спящую mean-rev: дополняет ли она тренд в чопе.
            if (mrTrades > 0) {
                sb2.append(String.format(
                        "🧪 MR-SHADOW (спящий mean-rev, measure-only): %d сд · WR %.0f%% · %+.1f%%\n"
                        + "  _по периодам: П1 %+.0f · П2 %+.0f · П3 %+.0f · П4 %+.0f — ценно, если зелёный в П2/П4 (чоп), где TREND красный_\n",
                        mrTrades, 100.0 * mrWins / mrTrades, mrNet,
                        mrHalfPnL[0], mrHalfPnL[1], mrHalfPnL[2], mrHalfPnL[3]));
                if (mrExhN + mrNoexhN > 0) {
                    sb2.append(String.format(
                            "  🧪 _MR×flow-ИСТОЩЕНИЕ (тест ядра FLOW_FADE): истощ %d сд WR %.0f%% net %+.1f%% avg %+.3f · без %d сд WR %.0f%% net %+.1f%% avg %+.3f_\n"
                            + "  _истощ зеленее/менее-красного → flow-дискриминатор несёт инфо → строю FLOW_FADE; ≈ или хуже → absorption-гипотеза мертва_\n",
                            mrExhN, mrExhN > 0 ? 100.0 * mrExhW / mrExhN : 0.0, mrExhNet, mrExhN > 0 ? mrExhNet / mrExhN : 0.0,
                            mrNoexhN, mrNoexhN > 0 ? 100.0 * mrNoexhW / mrNoexhN : 0.0, mrNoexhNet, mrNoexhN > 0 ? mrNoexhNet / mrNoexhN : 0.0));
                }
            } else {
                sb2.append("🧪 MR-SHADOW: 0 сделок (mean-rev сетапов на истории нет)\n");
            }
            // [v86.60 TE-SHADOW] TREND_EARLY (ранний вход в свежий тренд): 2 арма.
            String[] teName = {"struct", "nsc"};
            for (int ti = 0; ti < 2; ti++) {
                if (teTrades[ti] == 0) {
                    sb2.append("🧪 TE-SHADOW(").append(teName[ti])
                       .append("): 0 сделок (свежих трендов с сетапом на окне нет)\n");
                    continue;
                }
                java.util.List<Double> sls = new ArrayList<>(teSlPcts.get(ti));
                java.util.Collections.sort(sls);
                double medSl = sls.isEmpty() ? 0 : sls.get(sls.size() / 2);
                sb2.append(String.format(
                        "🧪 TE-SHADOW(%s) ранний вход: %d сд · WR %.0f%% · %+.1f%% · %+.3f%%/сд\n"
                        + "  П1..П4 %+.0f/%+.0f/%+.0f/%+.0f · L %d/%+.1f%% S %d/%+.1f%% · <65конф %d/%+.1f%% · medSL %.2f%%\n",
                        teName[ti], teTrades[ti], 100.0 * teWins[ti] / teTrades[ti],
                        teNet[ti], teNet[ti] / teTrades[ti],
                        teHalf[ti][0], teHalf[ti][1], teHalf[ti][2], teHalf[ti][3],
                        teLongN[ti], teLongNet[ti], teShortN[ti], teShortNet[ti],
                        teLoN[ti], teLoNet[ti], medSl));
            }
            sb2.append("  _TE: KILL если medSL<1.0% (кост>0.25R) или объём-без-эджа или красный чоп; решение ≥3 прогонов_\n");
            // [v86.69 FLOW-SHADOW] чоп-комплемент к TREND: value-area break + taker-flow.
            // ЦЕЛЬ: зелёный в П2/П4 (где TREND красный) = кандидат в чоп-ногу к 4/4.
            if (fbTrades == 0) {
                sb2.append("🧪 MOMENTUM-SHADOW: 0 сделок (взрывных свечей с flow на окне нет)\n");
            } else {
                java.util.List<Double> fsls = new ArrayList<>(fbSlPcts);
                java.util.Collections.sort(fsls);
                double fbMedSl = fsls.isEmpty() ? 0 : fsls.get(fsls.size() / 2);
                sb2.append(String.format(
                        "🧪 MOMENTUM-SHADOW (ловля взрыва: range+vol+flow, цель 3.5R): %d сд · WR %.0f%% · %+.1f%% · %+.3f%%/сд\n"
                        + "  П1..П4 %+.0f/%+.0f/%+.0f/%+.0f · L %d/%+.1f%% S %d/%+.1f%% · medSL %.2f%% (низкий WR норма; зелёный net = ловит большие движения!)\n",
                        fbTrades, 100.0 * fbWins / fbTrades, fbNet, fbNet / fbTrades,
                        fbHalf[0], fbHalf[1], fbHalf[2], fbHalf[3],
                        fbLongN, fbLongNet, fbShortN, fbShortNet, fbMedSl));
            }
            // [v86.84 FLOW_FADE-SHADOW] фейд границы рейнджа + flow-истощение, measure-only.
            // ЦЕЛЬ: зелёный в П2/П4 (чоп), где TREND молчит = кандидат в чоп-ногу к 4/4.
            if (ffTrades > 0) {
                sb2.append(String.format(
                        "🧪 FLOW_FADE-SHADOW (фейд границы + flow-истощение, measure-only): %d сд · WR %.0f%% · %+.1f%%\n"
                        + "  _по периодам: П1 %+.0f · П2 %+.0f · П3 %+.0f · П4 %+.0f — нужен зелёный в П2/П4 (чоп), где TREND молчит_\n",
                        ffTrades, 100.0 * ffWins / ffTrades, ffNet,
                        ffHalfPnL[0], ffHalfPnL[1], ffHalfPnL[2], ffHalfPnL[3]));
            }
            // [v86.89 ABSORB-SHADOW] поглощение→пробой + flow (структурный инверс MOMENTUM), measure-only.
            // ЦЕЛЬ: зелёный net на ≥3 прогонах = order-flow ранний вход несёт edge.
            if (abTrades > 0) {
                sb2.append(String.format(
                        "🧪 ABSORB-SHADOW (поглощение→пробой + flow, measure-only): %d сд · WR %.0f%% · %+.1f%%\n"
                        + "  _по периодам: П1 %+.0f · П2 %+.0f · П3 %+.0f · П4 %+.0f — order-flow ранний вход; нужен зелёный net на >=3 прогонах_\n",
                        abTrades, 100.0 * abWins / abTrades, abNet,
                        abHalfPnL[0], abHalfPnL[1], abHalfPnL[2], abHalfPnL[3]));
            }
            // [v86.90 CVD-RESUME FALSIFY] does leak-free flow-resumption at the signal bar separate
            // TREND winners? on-target for the user's late-reversal pain. Decision: подтв clearly
            // greener → build the CVD_RESUME gate on TREND; ≈/worse → flow doesn't carry info here.
            if (cvdResN + cvdNoresN > 0) {
                sb2.append(String.format(
                        "🧪 _TREND×CVD-возобновление (тест гейта CVD_RESUME): flow-подтв %d сд WR %.0f%% net %+.1f%% avg %+.3f · без %d сд WR %.0f%% net %+.1f%% avg %+.3f_\n"
                        + "  _подтв зеленее → flow-гейт улучшит TREND (режет дохлые отскоки); ≈/хуже → не несёт инфо_\n",
                        cvdResN, cvdResN > 0 ? 100.0 * cvdResW / cvdResN : 0.0, cvdResNet, cvdResN > 0 ? cvdResNet / cvdResN : 0.0,
                        cvdNoresN, cvdNoresN > 0 ? 100.0 * cvdNoresW / cvdNoresN : 0.0, cvdNoresNet, cvdNoresN > 0 ? cvdNoresNet / cvdNoresN : 0.0));
            }
            shadowBlock = sb2.toString();
        }

        // [v86.67] COST-DECOMPOSITION — где живёт минус: в сигнале или в костах?
        // net = gross − fee − slip − funding. Если gross ПОЛОЖИТЕЛЕН, а net ~0 —
        // сигнал есть, его съедают косты → главный рычаг = maker-исполнение (limit-
        // вход: fee~0 + 0 slip на вход-ноге). Если gross ≤ 0 — сигнал мёртв, никакое
        // execution не спасёт. Maker-прикидка ОПТИМИСТИЧНА (без adverse-selection и
        // риска непрофилла лимитника). Решает, куда вообще копать после exit-тупика.
        String costBlock = "";
        if (totalTrades > 0) {
            double gAvg    = totalGrossPnL / totalTrades;
            double feeAvg  = totalFeesAgg  * 100.0 / totalTrades;
            double slipAvg = totalSlipAgg  * 100.0 / totalTrades;
            double fundAvg = totalFundAgg  * 100.0 / totalTrades;
            double netAvg  = gAvg - feeAvg - slipAvg - fundAvg;
            double makerAvg = gAvg - feeAvg * 0.5 - slipAvg * 0.5 - fundAvg; // экономит вход-ногу
            costBlock = String.format(
                    "💸 *Косты/сделку:* gross %+.3f%% − fee %.3f − slip %.3f − fund %.3f = net %+.3f%%\n"
                  + "  maker-вход (оптимистич., без adverse-sel): ~%+.3f%%/сд %s\n",
                    gAvg, feeAvg, slipAvg, fundAvg, netAvg, makerAvg,
                    gAvg > 0 ? (makerAvg > 0 ? "🟢 gross+ → косты-рычаг ЖИВ" : "🟡 gross+ но maker мало")
                             : "🔴 gross≤0 → сигнал мёртв, execution не спасёт");
        }

        // [v86.70] ЗНАЧИМОСТЬ edge — bootstrap CI (честный тест «edge или шум»). Заменяет
        // фикс-пороги: edge РЕАЛЕН только если нижняя граница CI95 на avg/сделку > 0 ПОСЛЕ
        // косто́в. + Monte-Carlo maxDD(95%) на перетасовке сделок — калибровка риск-лимитов
        // (раньше monteCarloDrawdown был мёртвым кодом). Seed фиксирован → CI воспроизводим.
        String sigBlock = "";
        if (allPnls.size() >= 20) {
            int B = 1000, m = allPnls.size();
            double[] means = new double[B];
            java.util.Random sigRng = new java.util.Random(20260614L);  // seeded → воспроизводимо
            for (int b = 0; b < B; b++) {
                double s = 0.0;
                for (int i = 0; i < m; i++) s += allPnls.get(sigRng.nextInt(m));
                means[b] = s / m;
            }
            java.util.Arrays.sort(means);
            double ciLo = means[(int) (0.025 * B)];
            double ciHi = means[(int) (0.975 * B)];
            double mean = 0.0; for (double p : allPnls) mean += p; mean /= m;
            double mc95 = com.bot.SimpleBacktester.monteCarloDrawdown(allPnls, 2000, 0.95);
            String sMark = ciLo > 0 ? "🟢 edge ОТЛИЧИМ от нуля"
                                    : "🔴 НЕотличим от нуля (= шум/кост, реальные деньги нельзя)";
            sigBlock = String.format(
                    "📊 *Значимость* (bootstrap %d сд): avg %+.3f%%/сд · CI95 [%+.3f, %+.3f] %s\n"
                  + "  MonteCarlo maxDD(95%%): %.1f%% (калибровка риск-лимитов)\n",
                    m, mean, ciLo, ciHi, sMark, mc95);
        }

        String summary = String.format(
                "✅ *Стартовый backtest завершён* `" + BOT_VERSION + "`\n"
                        + "━━━━━━━━━━━━━━━━━━━━━\n"
                        + "⏱ Время: %d сек\n"
                        + "📊 Пар обработано: %d\n"
                        + "  ⚠️ Rate-limited: %d\n"
                        + "  📉 Мало данных: %d\n"
                        + "  ❌ Ошибок: %d\n"
                        + "🎯 Сделок: %d\n"
                        + "  ✅ Wins: %d (%.1f%%)\n"
                        + "  ❌ Losses: %d\n"
                        + "  ⏳ Time-stops: %d\n"
                        + "  🟰 BE: %d  🔒 ProfitLock: %d\n"
                        + "  📉 Trail: %d  ⏸ Stagnation: %d\n"
                        + "💰 Net PnL (сумма %% по парам): %+.2f\n"
                        + "  ⚠️ _это НЕ доход депозита: суммирует %% всех пар как\n"
                        + "  отдельные счета. Реальный портфель (1 депо, maxConcurrent=5,\n"
                        + "  корреляция) даёт МЕНЬШЕ. Цифра — для сравнения версий, не profit._\n"
                        + "  Avg/сделку: %+.3f%% (вот это ближе к реальности на сделку)\n"
                        + "%s"
                        + "%s"
                        + "📈 W/L ratio: %.2f\n"
                        + "🧠 Калибратор: %d outcomes\n"
                        + "%s"
                        + "%s"
                        + "%s"
                        + "━━━━━━━━━━━━━━━━━━━━━\n"
                        + "%s",
                elapsedSec, symbolsRun, symbolsRateLimited, symbolsLowData,
                symbolsErrored, totalTrades,
                totalWins, wr, totalLosses, totalTimeStops,
                totalBE, totalProfitLock, totalTrail, totalStag, totalNetPnL,
                (totalTrades > 0 ? totalNetPnL / totalTrades : 0.0),  // [v82.11] avg/trade
                costBlock,  // [v86.67] cost-decomposition
                sigBlock,   // [v86.70] edge significance (bootstrap CI + Monte-Carlo)
                wlRatio, newCalCount, tierBreakdown, halfBreakdown,
                shadowBlock,  // [v86.53 EXIT-SHADOW]
                verdict);

        try { telegram.sendMessageAsync(summary); } catch (Throwable ignored) {}
        LOG.info("[STARTUP-BT] ✓ Done in " + elapsedSec + "s | trades=" + totalTrades
                + " wr=" + String.format("%.1f%%", wr) + " calN=" + newCalCount);
    }

    private static void logStats(com.bot.GlobalImpulseController gic,
                                 com.bot.InstitutionalSignalCore isc,
                                 com.bot.SignalSender sender) {
        lastStatsSuccessMs = System.currentTimeMillis();
        long uptimeMin = (System.currentTimeMillis() - startTimeMs) / 60_000;
        com.bot.GlobalImpulseController.GlobalContext ctx = gic.getContext();
        Dispatcher d = Dispatcher.getInstance();
        String corrSlots = sender.getCorrelationSlotsSnapshot();
        String saturatedMark = sender.isCorrelationSaturated() ? "🔒" : "";
        String msg = String.format(
                "[STATS] Up:%dm Cyc:%d Sig:%d Trk:%d FR:%d | BTC:%s str=%.2f | "
                        + "WS:%d Corr:%s%s Disp:%d/%d [%s] | FC:%.0f%%(%d/%d) Amb:%d TS:%d Err:%d WD:%d | %s",
                uptimeMin, totalCycles.get(), totalSignals.get(),
                trackedSignals.size(), forecastRecords.size(),
                ctx.regime, ctx.impulseStrength,
                sender.getActiveWsCount(),
                corrSlots, saturatedMark,
                d != null ? d.getTotalDispatched() : 0,
                d != null ? d.getBlockedByGate() : 0,
                d != null ? d.getBlockBreakdown() : "?",
                forecastTotal.get() > 0 ? (double) forecastCorrect.get() / forecastTotal.get() * 100 : 0.0,
                forecastCorrect.get(), forecastTotal.get(),
                forecastAmbiguous.get(), forecastTimeStop.get(),
                errorCount.get(), watchdogAlerts.get(),
                isc.getStats());
        LOG.info(msg);
        try {
            Set<String> autoBlocked = isc.getAutoBlacklist();
            for (String sym : autoBlocked) sender.addToGarbageBlocklist(sym);
        } catch (Exception ignored) {}
    }

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

    private static List<com.bot.TradingCore.Candle> fetchKlinesPaged(
            com.bot.SignalSender sender, String symbol, String interval, int totalBars) {
        java.util.TreeMap<Long, com.bot.TradingCore.Candle> byTime = new java.util.TreeMap<>();
        // [v86.71] ВОСПРОИЗВОДИМОСТЬ: раньше endTime = now каждый бут → 30-дн окно скользило
        // → ±9× прыжки PnL (правка казалась улучшением/регрессией по ЧИСТОЙ удаче, см. v86.70:
        // −2.5→−22 на measure-only коде). Якорим к последней закрытой UTC-границе суток: все
        // буты одного дня = ИДЕНТИЧНАЯ история → правка = СИГНАЛ, не монетка. Сдвиг раз в сутки
        // → не протухает. Env BT_END_TIME (мс) — явный фикс-окно для строгих A/B. Откат: =0.
        long btEndEnv = 0L;
        try { btEndEnv = Long.parseLong(System.getenv().getOrDefault("BT_END_TIME", "0").trim()); }
        catch (NumberFormatException ignore) {}
        final long DAY_MS = 24L * 60L * 60L * 1000L;
        long endTime = btEndEnv > 0 ? btEndEnv : (System.currentTimeMillis() / DAY_MS) * DAY_MS;
        int remaining = totalBars;
        int maxRequests = (totalBars / 1500) + 2;   // safety guard

        for (int req = 0; req < maxRequests && remaining > 0; req++) {
            int batchLimit = Math.min(1500, remaining);
            try {
                String url = String.format(
                        "https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&endTime=%d&limit=%d",
                        symbol, interval, endTime, batchLimit);
                java.net.http.HttpClient http = java.net.http.HttpClient.newBuilder()
                        .connectTimeout(java.time.Duration.ofSeconds(10)).build();
                java.net.http.HttpResponse<String> resp = http.send(
                        java.net.http.HttpRequest.newBuilder()
                                .uri(java.net.URI.create(url))
                                .timeout(java.time.Duration.ofSeconds(15))
                                .GET().build(),
                        java.net.http.HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() != 200) {
                    LOG.warning("[fetchKlinesPaged] " + symbol + " " + interval
                            + " HTTP " + resp.statusCode() + " — stopping pagination");
                    break;
                }
                org.json.JSONArray arr = new org.json.JSONArray(resp.body());
                if (arr.length() == 0) break;
                long minOpenT = Long.MAX_VALUE;
                for (int i = 0; i < arr.length(); i++) {
                    org.json.JSONArray k = arr.getJSONArray(i);
                    long openT = k.getLong(0);
                    minOpenT = Math.min(minOpenT, openT);
                    double o = Double.parseDouble(k.getString(1));
                    double h = Double.parseDouble(k.getString(2));
                    double l = Double.parseDouble(k.getString(3));
                    double c = Double.parseDouble(k.getString(4));
                    double v = Double.parseDouble(k.getString(5));
                    long closeT = k.getLong(6);
                    double qv = Double.parseDouble(k.getString(7));
                    // [v86.68] DATA-PIPELINE: парсим taker-buy объём (klines idx 8/9/10) в бэктест.
                    // Раньше зануляли через 8-арг конструктор → CVD/aggressor-flow = 0 в shadow →
                    // ЛЮБАЯ order-flow чоп-стратегия (для портфеля к 4/4) была НЕпроверяема. Данные
                    // уже приходят в klines, просто выбрасывались. TREND их не читает → его BT-edge
                    // не меняется (проверено: generateTrendAligned/DEM не ссылаются на takerBuy).
                    int    nTr     = k.length() > 8  ? k.getInt(8)                       : 0;
                    double tbBase  = k.length() > 9  ? Double.parseDouble(k.getString(9))  : 0.0;
                    double tbQuote = k.length() > 10 ? Double.parseDouble(k.getString(10)) : 0.0;
                    byTime.putIfAbsent(openT,
                            new com.bot.TradingCore.Candle(openT, o, h, l, c, v, qv, closeT, nTr, tbBase, tbQuote));
                }
                remaining -= arr.length();
                // Walk further back: next endTime = oldest bar's openTime - 1
                endTime = minOpenT - 1;
                if (arr.length() < batchLimit) break;   // pair history exhausted
                Thread.sleep(600L);
            } catch (Exception e) {
                LOG.warning("[fetchKlinesPaged] " + symbol + " " + interval
                        + " error: " + e.getMessage());
                break;
            }
        }
        return new ArrayList<>(byTime.values());   // sorted by openTime asc
    }

    private static void updateBtcContext(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        try {
            List<com.bot.TradingCore.Candle> btc = sender.fetchKlines("BTCUSDT", PRIMARY_TF, KLINES);
            if (btc != null && btc.size() > 30) {
                gic.update(btc);
                // [v86.72] FIX (audit): RiskGuard.updateBtcPrice НИКОГДА не вызывался →
                // btcCrashBlockUntil=0 → лимит «BTC-crash» (защита от каскада, где все альты
                // падают разом) был МЁРТВ. Кормим ценой BTC каждый цикл → детектор −3%/30мин /
                // −5%/60мин теперь живой и заблокирует новые входы во время обвала (когда armed).
                com.bot.RiskGuard.getInstance().updateBtcPrice(btc.get(btc.size() - 1).close);
            }
        } catch (Exception e) { LOG.warning("[BTC ctx] " + e.getMessage()); }
    }

    private static void updateSectors(com.bot.SignalSender sender, com.bot.GlobalImpulseController gic) {
        for (Map.Entry<String, String> e : SECTOR_LEADERS.entrySet()) {
            try {
                List<com.bot.TradingCore.Candle> sc = sender.fetchKlines(e.getKey(), PRIMARY_TF, 80);
                if (sc != null && sc.size() > 25) gic.updateSector(e.getValue(), sc);
            } catch (Exception ignored) {}
        }
    }

    private static String calibrationStatus() {
        int s = com.bot.DecisionEngineMerged.getCalibrator().totalOutcomeCount();
        if (s < 30)  return String.format("⚠️ *Cold-start: %d/30 исходов*\n"
                + "_Калибратор учится. Сигналы сейчас на сырых вероятностях._", s);
        if (s < 100) return "🔸 Калибровка: " + s + "/100";
        return "✅ Калибровка: " + s + " исходов";
    }

    private static String buildStartMessage() {
        return "⚡ *TradingBot SCANNER* `v79.0`\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + "`" + PRIMARY_TF.toUpperCase() + "` Futures · TOP-" + envInt("TOP_N", 20) + " · Scanner-only\n"
                + "R:R min `1:2` · SL min `0.30%`\n"
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + (OBSERVATION_MODE
                ? "🧪 *PAPER MODE АКТИВЕН* — сигналы только для валидации\n━━━━━━━━━━━━━━━━━━━━━\n"
                : "🔴 *LIVE MODE* — реальные сигналы в чат\n━━━━━━━━━━━━━━━━━━━━━\n")
                + "🛡 *INTEGRITY UPGRADES (v79):*\n"
                + "• AMBIGUOUS outcomes теперь учитываются (½ credit)\n"
                + "• TIME_STOP считается loss (раньше игнорировался)\n"
                + "• HMAC-SHA256 подпись на всех записях калибратора\n"
                + "• Append-only audit log\n"
                + "• Forecast records переживают рестарт\n"
                + (CROSS_EXCHANGE_VALIDATION ? "• Cross-exchange (Bybit) sanity check\n" : "")
                + "━━━━━━━━━━━━━━━━━━━━━\n"
                + calibrationStatus() + "\n"
                + "_Прогнозы Entry / TP1-3 / SL._";
    }

    private static String nowLocalStr() {
        return ZonedDateTime.now(ZONE)
                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"));
    }

    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis).atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
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

    private static double envDbl(String k, double d) {
        try { return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(d))); }
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

    private static void runWalkForwardValidation(com.bot.SignalSender sender,
                                                 com.bot.TelegramBotSender telegram) {
        try {
            com.bot.SimpleBacktester bt = new com.bot.SimpleBacktester();
            // [v86.81] Wider OOS universe — 8 SECTOR_LEADERS gave only ~8 held-out trades
            // (< min). Use the live scan universe (same source the startup-BT samples) so the
            // OOS sample is usable; fall back to leaders if the snapshot isn't ready yet. Runs
            // AFTER the startup-BT (memory freed) so the extra fetches are safe on -Xmx380m.
            List<String> symbols;
            try {
                List<String> snap = sender.getScanUniverseSnapshot(30);
                symbols = (snap != null && snap.size() >= 8)
                        ? new ArrayList<>(snap) : new ArrayList<>(SECTOR_LEADERS.keySet());
            } catch (Throwable t) {
                symbols = new ArrayList<>(SECTOR_LEADERS.keySet());
            }
            int alerts = 0, totalWindows = 0;
            double totalDelta = 0;
            java.util.List<Double> oosPnls = new ArrayList<>();  // [v86.80] real purged-OOS per-trade net
            for (String sym : symbols) {
                try {
                    // [v90] Walk-forward bars scale with TF. [v86.82] train shrunk / test grown:
                    // the train window is vestigial here (TREND is rule-based, NOT fit per-window),
                    // so a smaller train + bigger test extracts ~1.7x more held-out OOS trades from
                    // the SAME fetched data (old 336/72 gave only ~16 OOS trades across 30 coins).
                    //   15m: 2880 bars; train 672 (~7d), test 480 (~5d)
                    //   1h:   720 bars; train 168 (~7d), test 120 (~5d)
                    //   4h:  1620 bars (270d); HTF=1d 300 bars; train 42 (~7d), test 30 (~5d)  [v86.91]
                    //   30m: 2880 bars (60d); HTF=4h 900 bars; train 336 (~7d), test 240 (~5d)
                    boolean wfIs4h  = "4h".equals(PRIMARY_TF);
                    boolean wfIs30m = "30m".equals(PRIMARY_TF);
                    int wfTotalBars = PRIMARY_IS_15M ? 2880 : wfIs30m ? 2880 : wfIs4h ? 1620 : 720;
                    int wfHtfBars   = PRIMARY_IS_15M ? 720  : wfIs30m ? 900  : wfIs4h ? 300  : 180;
                    int wfMinBars   = PRIMARY_IS_15M ? 1500 : wfIs30m ? 1500 : wfIs4h ? 900  : 400;
                    int wfWindow    = PRIMARY_IS_15M ? 672  : wfIs30m ? 336  : wfIs4h ? 42   : 168;   // train bars (~7d)
                    int wfStep      = PRIMARY_IS_15M ? 480  : wfIs30m ? 240  : wfIs4h ? 30   : 120;   // test bars (~5d)
                    List<com.bot.TradingCore.Candle> m15 = sender.fetchKlines(sym, PRIMARY_TF, wfTotalBars);
                    List<com.bot.TradingCore.Candle> h1  = sender.fetchKlines(sym, HTF_FAST,  wfHtfBars);
                    if (m15 == null || m15.size() < wfMinBars) continue;
                    com.bot.DecisionEngineMerged.CoinCategory cat = sender.getCoinCategory(sym);
                    if (cat == null) cat = com.bot.DecisionEngineMerged.CoinCategory.ALT;
                    List<com.bot.SimpleBacktester.BacktestResult> oos =
                            bt.walkForward(sym, m15, h1, cat, wfWindow, wfStep);
                    for (com.bot.SimpleBacktester.BacktestResult r : oos) {
                        totalWindows++;
                        if (r.trades != null)
                            for (com.bot.SimpleBacktester.TradeRecord t : r.trades) oosPnls.add(t.pnlPct);
                    }
                    if (oos.size() >= 2) {
                        double firstHalf = 0, secondHalf = 0;
                        int half = oos.size() / 2;
                        for (int i = 0; i < half; i++) firstHalf += oos.get(i).winRate * 100.0;
                        for (int i = half; i < oos.size(); i++) secondHalf += oos.get(i).winRate * 100.0;
                        firstHalf  /= Math.max(1, half);
                        secondHalf /= Math.max(1, oos.size() - half);
                        double delta = firstHalf - secondHalf;
                        totalDelta += Math.abs(delta);
                        if (Math.abs(delta) > 8.0) alerts++;
                    }
                } catch (Throwable ignored) {}
            }
            // [v86.80] REAL purged-OOS edge verdict — bootstrap CI on held-out trades.
            // The startup-BT "X/4 периодов" line is IN-SAMPLE entryTime-bucketing (not OOS);
            // THIS is the honest out-of-sample (train→gap→test→embargo) significance.
            if (oosPnls.size() >= 20) {   // [v86.81] align with in-sample gate (allPnls>=20)
                int m = oosPnls.size();
                double net = 0.0; for (double p : oosPnls) net += p;
                double avg = net / m;
                int B = 1000;
                double[] means = new double[B];
                java.util.Random rng = new java.util.Random(20260614L);  // seeded → воспроизводимо
                for (int b = 0; b < B; b++) {
                    double s = 0.0; for (int i = 0; i < m; i++) s += oosPnls.get(rng.nextInt(m));
                    means[b] = s / m;
                }
                java.util.Arrays.sort(means);
                double ciLo = means[(int) (0.025 * B)], ciHi = means[(int) (0.975 * B)];
                String mark = ciLo > 0 ? "🟢 OOS edge ОТЛИЧИМ от нуля"
                                       : "🔴 OOS НЕотличим от нуля (CI покрывает шум/кост — реал нельзя)";
                String msg = String.format(
                        "🔭 *НАСТОЯЩИЙ OOS — purged walk-forward (embargo)*\n"
                      + "━━━━━━━━━━━━━━━━━━━━━\n"
                      + "%d OOS-сделок · %d окон · %d монет (scan-вселенная)\n"
                      + "Net %+.1f%% · avg %+.3f%%/сд · CI95 [%+.3f, %+.3f]\n"
                      + "%s\n"
                      + "_Out-of-sample: train→gap→test→embargo. In-sample «X/4» в стартовой сводке — НЕ это._",
                        m, totalWindows, symbols.size(), net, avg, ciLo, ciHi, mark);
                try { telegram.sendMessageAsync(msg); } catch (Throwable ignored) {}
                LOG.info(String.format("[WalkForward] OOS %d trades net=%.1f avg=%.3f CI[%.3f,%.3f]",
                        m, net, avg, ciLo, ciHi));
            } else {
                // [v86.82] Still send an HONEST message (don't go silent): a thin held-out
                // sample is itself informative — 30d of a selective strategy yields few OOS
                // trades. Report the count; the judge stays the in-sample CI + ≥100 live.
                String msg = String.format(
                        "🔭 *OOS walk-forward (purged) — выборка тонкая*\n"
                      + "━━━━━━━━━━━━━━━━━━━━━\n"
                      + "Только %d held-out сделок (%d окон · %d монет) — мало для CI-вердикта.\n"
                      + "30д истории + селективный TREND дают мало OOS-сделок. Судья остаётся:\n"
                      + "in-sample bootstrap-CI (в сводке) + ≥100 живых исходов.",
                        oosPnls.size(), totalWindows, symbols.size());
                try { telegram.sendMessageAsync(msg); } catch (Throwable ignored) {}
                LOG.info("[WalkForward] OOS thin sample (" + oosPnls.size() + " < 20) — informational msg sent");
            }
            if (alerts > 0) {
                LOG.info(String.format(
                        "[WalkForward] %d/%d symbols unstable, Avg Δ: %.1f%%",
                        alerts, symbols.size(),
                        totalWindows > 0 ? totalDelta / Math.max(1, totalWindows) : 0));
            }
        } catch (Throwable ignored) {}
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