package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;

/** SignalSender — TRADINGBOT PRO EDITION v37.0 */
/**
 * ║   SignalSender v50.0 — PREDICTIVE SIGNAL ARCHITECTURE               ║
 * ║  [v50] §1  15m blind spot: 120s→15s (assembleLive15mCandle)         ║
 * ║  [v50] §2  Event coin filter: directional block, not total          ║
 * ║  [v50] §3  EARLY_TICK: velocity/accel/volume thresholds lowered     ║
 * ║  [v50] §4  Cache TTL 15m: 14min→8min for fresher data               ║
 * ║  [v50] §5  EARLY_TICK exhaustion guard: 2.5→2.0 ATR                 ║
 * ║  [REFACTOR] 5m TTL: 3min→4m30s (270s) — экономия ~60% klines weight  ║
 * ║  [REFACTOR] EARLY_TICK vel: TOP→0.0015, ALT→0.0025, MEME→0.0035      ║
 * ║  [REFACTOR] Position sizing flat: удалены conf-based множители        ║
 * ║  [REFACTOR] FUNDING_REFRESH: 15min, DEPTH_POLL: 120s, TOP_N=30        ║
 */
public final class SignalSender {

    // [v72] Унифицированное логирование. Раньше использовали System.out.println
    // (27 мест) — это идёт в stdout который Railway/cloud platforms могут
    // буферизовать или терять. Logger пишет через JUL handler с правильным
    // форматом и severity, плюс уже есть подавление таймстампов в BotMain.
    private static final Logger LOG = Logger.getLogger(SignalSender.class.getName());

    private final com.bot.TelegramBotSender bot;
    private final HttpClient              http;
    private final ExecutorService         httpIoExecutor;
    // [v87.8] Dedicated client+executor for the liquidation WebSocket — root-cause fix for liq_events=0.
    private final HttpClient              liqHttp;
    private final ExecutorService         liqWsExecutor;
    private static final boolean LIQ_DEDICATED_WS = !"0".equals(System.getenv().getOrDefault("LIQ_DEDICATED_WS", "1"));
    private final Object wsLock = new Object();

    private final int    TOP_N;
    private final double MIN_CONF;
    private final int    KLINES_LIMIT;
    private final long   BINANCE_REFRESH_MS;
    private final int    TICK_HISTORY;
    private final double OBI_THRESHOLD;
    private final double DELTA_BLOCK_CONF;
    private final boolean ENABLE_EARLY_TICK;
    private final int    MAX_SCAN_PAIRS_PER_CYCLE;
    private final int    DEPTH_SNAPSHOT_TOP_N;
    private final int    FUNDING_OI_TOP_N;

    // API ключи — нужны для UDS и размера позиции
    private final String API_KEY;
    private final String API_SECRET;

    // 5min→15min: OI не меняется кардинально за 5 минут.
    // Экономия: ~66% OI weight (~$1.5/мес).
    private static final long FUNDING_REFRESH_MS  = 15 * 60_000L;
    private static final long DELTA_WINDOW_MS     = 60_000L;

    // ════════════════════════════════════════════════════════════════════
    // [v90 PRIMARY-TF 2026-05-09] Primary timeframe abstraction.
    //
    // Pre-v90: hardcoded "15m" everywhere. Result on backtest:
    //   WR 27.8%, R:R 1:1.5, NetPnL -35.46% on 36 trades.
    // The 15m TF has signal/noise ratio ~50/50: typical bar moves 0.2-0.6%
    // while bid/ask noise is ±0.15-0.30%. Friction (fees+slippage) eats
    // ~25% of average TP move on 15m.
    //
    // Switching to 1h:
    //   - Typical bar moves 0.6-1.5% (3× larger), noise stays ±0.15-0.30%
    //   - Friction drops to ~7-8% of TP (3× better economics)
    //   - Trends are more reliable, less microstructure manipulation
    //   - Trade-off: 5-15 signals/day on 20 pairs (vs noisy 30+ on 15m)
    //
    // Override via env PRIMARY_TF=15m to revert. Use HTF_FAST/HTF_SLOW for
    // higher timeframe filters (default 4h / 1d when PRIMARY_TF=1h).
    // ════════════════════════════════════════════════════════════════════
    private static final String PRIMARY_TF =
            System.getenv().getOrDefault("PRIMARY_TF", "1h").trim();
    private static final long PRIMARY_TF_MS = primaryTfMs(PRIMARY_TF);
    private static final int PRIMARY_TF_MIN = (int) (PRIMARY_TF_MS / 60_000L);
    // HTF (higher timeframe) used for bias filters. For 1h primary, default 4h; for 4h, 1d.
    private static final String HTF_FAST =
            System.getenv().getOrDefault("HTF_FAST",
                    "1h".equals(PRIMARY_TF) ? "4h" : "30m".equals(PRIMARY_TF) ? "4h" : "4h".equals(PRIMARY_TF) ? "1d" : "1h").trim();
    private static final String HTF_SLOW =
            System.getenv().getOrDefault("HTF_SLOW",
                    "1h".equals(PRIMARY_TF) ? "1d" : "30m".equals(PRIMARY_TF) ? "1d" : "4h".equals(PRIMARY_TF) ? "1d" : "2h").trim();

    // [v86.91] 4h-support helpers (duplicated per-file by design — no new classes).
    private static int    tfMin(String tf)      { return "15m".equals(tf)?15 : "30m".equals(tf)?30 : "4h".equals(tf)?240 : 60; }
    private static int    barsPerDay(String tf) { return "15m".equals(tf)?96 : "30m".equals(tf)?48 : "4h".equals(tf)?6  : 24; }
    private static String htfFast(String tf)    { return "15m".equals(tf)?"1h" : "30m".equals(tf)?"4h" : "4h".equals(tf)?"1d" : "4h"; }
    private static long   tfBarMs(String tf)     { return tfMin(tf)*60_000L; }   // 1d handled at call sites as 86_400_000

    private static long primaryTfMs(String tf) {
        switch (tf) {
            case "1m":  return 60_000L;
            case "5m":  return 5 * 60_000L;
            case "15m": return 15 * 60_000L;
            case "30m": return 30 * 60_000L;
            case "1h":  return 60 * 60_000L;
            case "2h":  return 2 * 60 * 60_000L;
            case "4h":  return 4 * 60 * 60_000L;
            default:
                throw new IllegalArgumentException("Unsupported PRIMARY_TF: " + tf);
        }
    }

    private static final double MIN_PROFIT_TOP  = 0.0025;
    private static final double MIN_PROFIT_ALT  = 0.0035;
    private static final double MIN_PROFIT_MEME = 0.0050;

    private static final double MIN_VOL_TOP_USD  = 50_000_000;
    // [v63] Relaxed. Previous 75M/25M cut off too many valid pairs (APT, ARB, OP,
    // RUNE, INJ etc can trade at 30-60M on quiet days yet still be liquid enough).
    // Real institutional-listing noise is still blocked by isBlocklisted + soft-block
    // + SL-gate + ATR-gate chain. Keeping vol thresholds high was double-filtering.
    private static final double MIN_VOL_ALT_USD  = 40_000_000;  // was 75M
    private static final double MIN_VOL_MEME_USD = 15_000_000;  // was 25M

    private static final double STOP_CLUSTER_SHIFT = 0.0025;
    private static final int    MAX_WS_CONNECTIONS  = 100;
    private static final long   WS_INITIAL_DELAY_MS = 3_000L;
    private static final long   WS_MAX_DELAY_MS     = 120_000L;

    // Volume Delta
    private final Map<String, Double> deltaBuffer      = new ConcurrentHashMap<>();
    private final Map<String, Long>   deltaWindowStart = new ConcurrentHashMap<>();
    private final Map<String, Double> deltaHistory     = new ConcurrentHashMap<>();

    // VDA — Volume Delta Acceleration (10s micro-windows)
    private static final long   VDA_WINDOW_MS  = 10_000L;
    private final Map<String, Double> vdaCurrentBuf = new ConcurrentHashMap<>();
    private final Map<String, Double> vdaPrevBuf    = new ConcurrentHashMap<>();
    private final Map<String, Long>   vdaWindowStart= new ConcurrentHashMap<>();
    private final Map<String, Double> vdaScoreMap   = new ConcurrentHashMap<>();

    // [MODULE 2 v33] ORDER FLOW VELOCITY (OFV) — stale-orderbook detection.
    //
    // PROBLEM: OBI (order book imbalance) is a STATIC snapshot. A wall of 500 BTC
    // bids looks bullish, but if it's been sitting there for 2 minutes without being
    // hit, it's likely a spoof — institutions use it to attract retail longs before
    // pulling the wall and dumping.
    //
    // OFV measures the RATE OF CHANGE of the bid/ask wall:
    //   OFV > 0 → bid wall is growing faster than ask wall → real demand absorbing sells
    //   OFV < 0 → ask wall growing faster than bids → distribution into buy pressure
    //   OFV ≈ 0 but OBI strong → static wall → spoofing risk
    //
    // This is the single most reliable pre-impulse signal because:
    // 1. Institutional algo bots top up the bid wall 200-500ms BEFORE the impulse.
    // 2. The wall change is visible in bookTicker BEFORE price moves.
    // 3. Retail can't react fast enough — this is our edge.
    //
    // Implementation: store last 5 OBI snapshots per pair (rolling).
    // OFV = slope of OBI over the last N snapshots (linear regression coefficient).
    private static final int    OFV_HISTORY_SIZE   = 8;    // 8 × ~500ms ticks = ~4s window
    private static final double OFV_SIGNAL_THRESH  = 0.015; // rate of OBI change per tick
    private static final double OFV_STRONG_THRESH  = 0.040; // strong directional flow
    private final Map<String, Deque<double[]>> ofvHistory = new ConcurrentHashMap<>();
    // OFV score per pair: positive = bullish flow velocity, negative = bearish
    private final Map<String, Double> ofvScoreMap = new ConcurrentHashMap<>();

    // RT-CVD — real-time CVD from aggTrade (resets each 15m candle)
    private final Map<String, Double> rtCvdBuy   = new ConcurrentHashMap<>();
    private final Map<String, Double> rtCvdTotal = new ConcurrentHashMap<>();
    private final Map<String, Long>   rtCvdReset = new ConcurrentHashMap<>();

    // ══ ДЫРА №1: CVD — Cumulative Volume Delta (покупки - продажи накопленные за 90×1m) ══
    // Обычная дельта = мгновенный снимок. CVD = вся история намерений рынка.
    // Если цена растёт, а CVD падает → ИНСТИТУЦИОНАЛЫ ПРОДАЮТ в рост → ЛОВУШКА.
    private final Map<String, Double>        cvdMap     = new ConcurrentHashMap<>();
    private static final int CVD_LOOKBACK_1M = 90; // 90×1m = 1.5h накопленной дельты

    // ══ ДЫРА №2: Liquidation Heatmap — уровни принудительных ликвидаций ══
    // Цена ВСЕГДА идёт туда где лежат ликвидации. Это физика фьючерсного рынка.
    // Мы подписываемся на глобальный поток ликвидаций Binance (публичный, без ключей).
    private final Map<String, java.util.NavigableMap<Double, Double>> liqHeatmap
            = new ConcurrentHashMap<>();
    private volatile WebSocket liqWebSocket = null;
    private static final double LIQ_MIN_NOTIONAL =          // [v87.2] $50k→$5k: для СБОРА данных (#3) ловим больше; фильтр вверх — при анализе
            envDouble("LIQ_MIN_NOTIONAL", 5_000.0);
    private static final long   LIQ_DECAY_MS     = 30 * 60_000L; // ликвидации "протухают" за 30 мин
    private final Map<String, Long> liqTimestamps = new ConcurrentHashMap<>();

    // Tick / WebSocket
    private static final long REALTIME_STALE_SKIP_MS = 75_000L;
    private static final double VPOC_NEAR_ATR_MULT   = 0.35;
    private static final double VPOC_NEAR_STOP_MULT  = 0.85;
    private static final double VPOC_SOFT_PENALTY    = 3.5;
    private static final double MAX_QUALITY_PENALTY  = 8.0;
    private final Map<String, Deque<Double>>      tickPriceDeque  = new ConcurrentHashMap<>();

    //  HOT PAIR MOMENTUM TRACKER
    //  Problem: Main cycle runs every 1 min. A pump that starts
    //  10 seconds into the cycle interval will only be caught
    //  up to 50 seconds later. For MEME coins that pump in 3-5 min
    //  total — that delay is catastrophic.
    //
    //  Solution: Every aggTrade tick, measure 30-second price delta.
    //  If delta > threshold — immediately submit that pair for a
    //  full processPair() run outside the normal cycle.
    //  This reduces reaction time from up to 60s → ≤5s.
    //
    //  Thresholds (% move in last 30 ticks / ~30s):
    //    TOP  (BTC/ETH): +0.25%  → hot
    //    ALT:             +0.40%  → hot
    //    MEME:            +0.60%  → hot
    //
    //  Cooldown: 90s per pair — don't spam rescan on the same move.
    //  Max concurrent rescans: 3 — protect fetch pool from overload.
    // [FIX] HOT_PAIR thresholds raised: reduce false hot-pair rescans.
    // Old ALT=0.40%: fired on normal micro-volatility (ALT's typical 15m ATR ~1%).
    // New ALT=0.55%: requires genuine momentum surge (55% of normal 15m ATR in 30s).
    // TOP unchanged — BTC/ETH 0.25% in 30s IS a real event.
    // MEME raised 0.60→0.80%: MEMEs routinely spike 0.5-0.6% on noise; 0.80% = real.
    // [v77 LATENCY] HOT_PAIR thresholds halved — catch impulses earlier.
    //   TOP:  0.25% → 0.15% in 30 ticks
    //   ALT:  0.55% → 0.30% in 30 ticks
    //   MEME: 0.80% → 0.50% in 30 ticks
    // 30 ticks on a $20M-volume coin = 1–3 minutes. The old 0.55% threshold
    // means the move had to already be visible on chart before rescan fired.
    // 0.30% catches the second-third of the impulse, not the tail.
    private static final double HOT_PAIR_TOP_PCT   = 0.0015;  // 0.15% in 30s
    private static final double HOT_PAIR_ALT_PCT   = 0.0030;  // 0.30% in 30s
    private static final double HOT_PAIR_MEME_PCT  = 0.0050;  // 0.50% in 30s
    // [v77 LATENCY] Cooldown 10min → 3min. After a hot rescan the same pair
    // is muted for 10 minutes — but on volatile pairs the move continues to
    // develop and a SECOND entry (after pullback) is often better than the
    // first. 3min is enough to debounce noise without blocking legit re-entries.
    private static final long   HOT_PAIR_COOLDOWN_MS = 3 * 60_000L;
    // [v62] Soft blocklist: pair that produces SL > 5% three times in a row
    // is added here for 2 hours. Prevents re-scanning ultra-volatile garbage.
    private final java.util.concurrent.ConcurrentHashMap<String, Long> hotSoftBlocklist =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<String, Integer> hotSlFailures =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static final long   HOT_SOFT_BLOCK_MS = 2 * 60 * 60_000L;
    private static final int    HOT_PAIR_MAX_CONCURRENT = 3;
    private final Map<String, Long>    hotPairLastRescan   = new ConcurrentHashMap<>();
    private final AtomicInteger        hotPairActiveCount  = new AtomicInteger(0);
    private final java.util.concurrent.atomic.AtomicLong hotPairTotalTriggers = new AtomicLong(0);
    private final Map<String, Deque<Double>>      tickVolumeDeque = new ConcurrentHashMap<>();
    private final Map<String, Long>               lastTickTime    = new ConcurrentHashMap<>();
    private final Map<String, Double>             lastTickPrice   = new ConcurrentHashMap<>();
    private final Map<String, WebSocket>          wsMap           = new ConcurrentHashMap<>();
    private final Map<String, Long>               wsReconnectDelay= new ConcurrentHashMap<>();
    private final Map<String, MicroCandleBuilder> microBuilders   = new ConcurrentHashMap<>();

    //  [v17.0 §2] EARLY TICK SIGNAL BUFFER
    //  Collects EARLY_TICK candidates across 1.5s windows per pair.
    //  earlyTickFlusher drains the buffer, sorts by probability,
    //  and dispatches only the TOP-1 per pair. Prevents signal spam
    //  during volatile bursts where the same pair fires 5× in 3 seconds.
    /** Accumulates the best (highest probability) EARLY_TICK candidate per pair per flush window. */
    private final Map<String, com.bot.DecisionEngineMerged.TradeIdea> earlyTickBuffer
            = new ConcurrentHashMap<>();
    // Tracks last time a full WS reconnect happened.
    // After reconnect, suppress earlyTickBuffer dispatch for 30s to prevent
    // stale-data signal flood (the "5 signals in 3 minutes" problem).
    private volatile long wsLastReconnectMs = 0;
    private static final long WS_WARMUP_MS  = 30_000L; // 30 seconds

    // EARLY_TICK hourly rate limit per pair.
    // Problem: volatile ALT fires 8 EARLY_TICK signals in 30 min — all same move.
    // Manual trader can't act on more than 2-3 signals per hour on same pair.
    // Fix: max 3 EARLY_TICK per pair per rolling 60 minutes.
    // [FIX] MAX_EARLY_TICK_PER_HOUR 3→2. Manual trader cannot act on 3/hour same pair.
    // Real pumps fire Early cluster once, maybe twice (entry + re-entry after pullback).
    // A third EARLY_TICK on same pair in 60 min = same move repeating = noise spam.
    // [v77 LATENCY] 2 → 4. EARLY_TICK per pair per hour was capped at 2 to
    // prevent "same move repeating" spam. But on a real trending day a coin
    // legitimately produces 4-5 entry-worthy impulses (initial breakout +
    // pullback + continuation + exhaustion-reversal). 4/hour leaves room for
    // these without re-flooding on noise (the 5min dispatch dedup + ISC
    // cooldown still gate the actual Telegram send rate).
    private static final int    MAX_EARLY_TICK_PER_HOUR = 4;
    private static final long   EARLY_TICK_WINDOW_MS    = 60 * 60_000L;
    private final Map<String, Deque<Long>> earlyTickTimestamps = new ConcurrentHashMap<>();

    private boolean earlyTickHourlyLimitReached(String pair) {
        Deque<Long> ts = earlyTickTimestamps.get(pair);
        if (ts == null) return false;
        long cutoff = System.currentTimeMillis() - EARLY_TICK_WINDOW_MS;
        while (!ts.isEmpty() && ts.peekFirst() < cutoff) ts.pollFirst();
        return ts.size() >= MAX_EARLY_TICK_PER_HOUR;
    }

    private void recordEarlyTickSent(String pair) {
        earlyTickTimestamps.computeIfAbsent(pair, k -> new java.util.concurrent.ConcurrentLinkedDeque<>())
                .addLast(System.currentTimeMillis());
    }

    private final ScheduledExecutorService wsWatcher = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ws-watcher"); t.setDaemon(true); return t;
    });

    // User Data Stream
    private volatile String    udsListenKey   = null;
    private volatile WebSocket udsWebSocket   = null;
    // Tracks last time any UDS event arrived.
    // If no event in 5 min → socket silently died → force reconnect.
    private volatile long      udsLastEventMs = System.currentTimeMillis();
    private final ScheduledExecutorService udsExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "uds-listener"); t.setDaemon(true); return t;
    });

    // Буфер 1m свечей для LiveCandleAssembler
    private final Map<String, List<com.bot.TradingCore.Candle>> liveM1Buffer = new ConcurrentHashMap<>();
    private static final int LIVE_M1_BUFFER_SIZE = 180; // [v36-FIX] 4h of 1m bars from WS ticks

    private final Map<String, Long> lastFetchTime = new ConcurrentHashMap<>();

    // Orderbook — populated via @bookTicker WebSocket stream
    private final Map<String, OrderbookSnapshot> orderbookMap = new ConcurrentHashMap<>();

    // Candle Cache
    private final Map<String, CachedCandles> candleCache = new ConcurrentHashMap<>();

    // [v90] Extended for 1h-primary mode: 4h cache (HTF_FAST) and 1d cache (HTF_SLOW).
    //   4h TTL = 30 min (1/8 bar)
    //   1d TTL = 60 min (1/24 bar — slow-moving, refresh once an hour is plenty)
    //   30m TTL = 2 min (for completeness if PRIMARY_TF=30m used)
    private static final Map<String, Long> CACHE_TTL = Map.ofEntries(
            Map.entry("1m",  55_000L),
            Map.entry("5m",  120_000L),
            Map.entry("15m", 30_000L),
            Map.entry("30m", 120_000L),
            Map.entry("1h",  5 * 60_000L),
            Map.entry("2h",  15 * 60_000L),
            Map.entry("4h",  30 * 60_000L),
            Map.entry("1d",  60 * 60_000L)
    );

    private static final class CachedCandles {
        final List<com.bot.TradingCore.Candle> candles;
        final long fetchedAt;
        CachedCandles(List<com.bot.TradingCore.Candle> c) {
            this.candles = Collections.unmodifiableList(c);
            this.fetchedAt = System.currentTimeMillis();
        }
        boolean isStale(long ttl) { return System.currentTimeMillis() - fetchedAt > ttl; }
    }

    // Pairs / volumes
    private volatile Set<String>  cachedPairs      = new LinkedHashSet<>();
    private volatile long         lastPairsRefresh = 0L;
    private volatile long         lastFundingRefresh = 0L;
    private final Map<String, Double> volume24hUSD = new ConcurrentHashMap<>();
    private volatile long         lastVolRefresh   = 0L;
    private static final long     VOL_REFRESH_MS   = 30 * 60_000L;

    // Баланс для компаундинга
    // [BUG-FIX] Убран хардкод $1000. Теперь читаем из env ACCOUNT_BALANCE (по умолчанию 100).
    // Установи в Railway: ACCOUNT_BALANCE=500 (или любая сумма которую ты реально торгуешь).
    // Если подключён API ключ — баланс подтягивается с биржи автоматически и env игнорируется.
    private volatile double accountBalance    = envDouble("ACCOUNT_BALANCE", 100.0);
    private volatile long   lastBalanceRefresh = 0;

    //  RATE LIMITER — Semaphore + Token Bucket + Backoff
    //  Replaces broken volatile-based rate limiter that caused
    //  silent request drops and potential IP bans.
    private static final int    RL_MAX_WEIGHT      = 2400;
    private static final int    RL_SAFE_WEIGHT     = 1800;
    private static final int    RL_CRITICAL_WEIGHT = 2100;
    private static final long   RL_WINDOW_MS       = 60_000L;
    private static final int    RL_MAX_CONCURRENT  = 10;
    private static final int    BINANCE_WEIGHT_KLINES        = 5;
    private static final int    BINANCE_WEIGHT_24H_TICKER    = 40;
    private static final int    BINANCE_WEIGHT_EXCHANGE_INFO = 1;
    private static final int    BINANCE_WEIGHT_PREMIUM_INDEX = 10;
    private static final int    BINANCE_WEIGHT_OPEN_INTEREST = 1;
    private static final int    BINANCE_WEIGHT_DEPTH10       = 2;
    private static final int    BINANCE_WEIGHT_SIGNED_LIGHT  = 1;
    private static final int    BINANCE_WEIGHT_BALANCE       = 5;

    // ════════════ [v86.96] NEW-LISTINGS CATCHER (observation-only) ════════════
    // Detects freshly listed USDT-M futures symbols (diff exchangeInfo vs a
    // persisted known-set) and records their first-N-hours microstructure
    // (1m klines + funding + L1 spread) to ./data for edge research. It NEVER
    // trades and never feeds the calibrator/decision engine — pure data capture
    // + a Telegram alert. Gated by BotMain.NEW_LISTING_CATCHER (default ON).
    // Reuses the existing rate-limited REST wrapper, so it shares the weight budget.
    private static final String NL_KNOWN_FILE =
            System.getenv().getOrDefault("NEW_LISTINGS_KNOWN_FILE", "./data/known_symbols.csv").trim();
    private static final String NL_MICRO_FILE =
            System.getenv().getOrDefault("NEW_LISTINGS_MICRO_FILE", "./data/new_listing_micro.csv").trim();
    // Hours to keep recording a symbol after first detection (clamped 1..72 — guards
    // against an absurd env value overflowing the recording-deadline arithmetic).
    private static final long NL_RECORD_HOURS =
            Math.max(1L, Math.min(72L, nlEnvLong("NEW_LISTING_MICRO_HOURS", 6)));
    // A bar's funding/spread snapshot is trustworthy only if the bar closed within
    // ~one scan cycle of the snapshot. Older backfilled bars carry klines only
    // (funding/bid/ask=0, live=0) — we never fabricate microstructure we can't observe.
    private static final long NL_FRESH_MS = 7 * 60_000L;
    // If exchangeInfo returns fewer than this many symbols it's the rate-limit/ban
    // fallback (3 symbols) — skip the diff so we never flag the ~400 universe as "new".
    private static final int  NL_SANITY_FLOOR = 50;
    // Persisted known universe (every symbol ever seen).
    private final Set<String> nlKnownSymbols = ConcurrentHashMap.newKeySet();
    // symbol -> epoch ms until which we keep recording it.
    private final Map<String, Long> nlRecordingUntil = new ConcurrentHashMap<>();
    // Dedupe micro-rows by "symbol|openTime" within active recording windows.
    private final Set<String> nlSeenRecords = ConcurrentHashMap.newKeySet();
    private volatile boolean nlLoaded = false;
    private volatile boolean nlAnnounced = false; // one-time-per-boot "catcher active" status sent?

    // [v86.98] Optional Supabase sink — mirror micro-records to a queryable Postgres DB in
    // ADDITION to ./data (./data stays the source of truth). Off unless both env vars are set,
    // so the bot still works with zero config. Writes via PostgREST with the anon key (the
    // table has INSERT-only RLS, so the key can write but not read).
    private static final String SUPABASE_URL = System.getenv().getOrDefault("SUPABASE_URL", "").trim();
    private static final String SUPABASE_KEY = System.getenv().getOrDefault("SUPABASE_KEY", "").trim();
    private static final boolean SUPABASE_ON = !SUPABASE_URL.isEmpty() && !SUPABASE_KEY.isEmpty();

    // ════════════ [v86.99] FUNDING-EXTREME SNAPSHOT (hypothesis #2, observation-only) ════════════
    // Each cycle, snapshot symbols with extreme funding (|fr|>=thr or peak/trough warning) →
    // ./data + Supabase. Forward returns computed at ANALYSIS time from klines (no in-bot resolver).
    private static final String FS_FILE = System.getenv().getOrDefault("FUNDING_SNAPS_FILE", "./data/funding_snaps.csv").trim();
    private static final double FS_EXTREME_THR = 0.0005; // |funding| >= 0.05% = "extreme"
    private static final String FS_HEADER = "# funding_snaps v1 | symbol|snap_minute|funding_rate|prev_funding_rate|funding_delta|fr_acceleration|open_interest|peak_warn|trough_warn|snap_ms";
    private volatile long fsLastMinute = 0L;
    private volatile boolean fsAnnounced = false;

    // ════════════ [v87.0] LIQUIDATION CAPTURE (hypothesis #3, observation-only) ════════════
    // The existing !forceOrder@arr WS stream feeds processLiquidationEvent; we buffer each event
    // there and flush a batch once per cycle → ./data + Supabase. Reversion outcome is computed
    // at ANALYSIS time from klines (no in-bot resolver).
    private static final String LQ_FILE = System.getenv().getOrDefault("LIQ_EVENTS_FILE", "./data/liq_events.csv").trim();
    private static final String LQ_HEADER = "# liq_events v1 | symbol|order_time|side|price|qty|notional|snap_ms";
    private static final int LQ_FLUSH_MAX  = 2000;   // cap rows written per cycle
    private static final int LQ_BUFFER_MAX = 20000;  // backpressure: drop new beyond this
    private final java.util.Queue<org.json.JSONObject> liqBuffer = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private volatile boolean lqAnnounced = false;
    private final AtomicLong liqRawCount = new AtomicLong(0); // [v87.2] всего forceOrder-событий получено (до фильтра) — диагностика
    private volatile long    liqLastEventMs = 0;             // [v87.2] время последнего пойманного события

    private final java.util.concurrent.Semaphore rlSemaphore = new java.util.concurrent.Semaphore(RL_MAX_CONCURRENT);
    private final AtomicInteger rlCurrentWeight = new AtomicInteger(0);
    private final AtomicLong    rlWindowStart   = new AtomicLong(System.currentTimeMillis());
    private volatile int        rlServerWeight  = 0;
    private volatile long       rlBackoffUntil  = 0;
    private volatile boolean    rlIpBanned      = false;
    private volatile long       rlIpBanUntil    = 0;
    private volatile int        rl429Count      = 0;
    private volatile long       rlRampUntil     = 0;
    private final AtomicLong    rlTotalWaits    = new AtomicLong(0);

    /**
     * Acquire permission to make a Binance request.
     * Blocks if too many concurrent or weight budget exhausted.
     * Returns false = skip request (IP banned or timeout).
     */
    private boolean rlAcquire(int weight) {
        // IP ban
        if (rlIpBanned && System.currentTimeMillis() < rlIpBanUntil) return false;
        if (rlIpBanned) rlIpBanned = false;

        // Backoff
        long backoffWait = rlBackoffUntil - System.currentTimeMillis();
        if (backoffWait > 0) {
            if (backoffWait > 30_000) return false;
            try { rlTotalWaits.incrementAndGet(); Thread.sleep(backoffWait); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
        }

        // [v24.0 FIX BUG-6] Atomic window rotation.
        // Old code: rlWindowStart.set() + rlCurrentWeight.set(0) = two separate ops.
        // Between them another thread could add weight to the already-zeroed counter
        // with the new timestamp → weight lost, silent over-requesting → potential IP ban.
        synchronized (rlWindowStart) {
            if (System.currentTimeMillis() - rlWindowStart.get() > RL_WINDOW_MS) {
                rlWindowStart.set(System.currentTimeMillis());
                rlCurrentWeight.set(0); rlServerWeight = 0;
            }
        }

        // Weight check
        int eff = Math.max(rlCurrentWeight.get(), rlServerWeight);
        if (eff + weight > RL_CRITICAL_WEIGHT) {
            long wait = RL_WINDOW_MS - (System.currentTimeMillis() - rlWindowStart.get()) + 200;
            if (wait > 0 && wait < RL_WINDOW_MS) {
                try { rlTotalWaits.incrementAndGet(); Thread.sleep(wait); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
                rlWindowStart.set(System.currentTimeMillis());
                rlCurrentWeight.set(0); rlServerWeight = 0;
            }
        } else if (eff + weight > RL_SAFE_WEIGHT) {
            try { Thread.sleep(200); } catch (InterruptedException ignored) {}
        }

        // Concurrency
        try {
            if (!rlSemaphore.tryAcquire(8, java.util.concurrent.TimeUnit.SECONDS)) return false;
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }

        rlCurrentWeight.addAndGet(weight);
        return true;
    }

    private void rlRelease() { rlSemaphore.release(); }

    private void rlOnSuccess(int reportedWeight) {
        rl429Count = 0;
        if (reportedWeight > 0) { rlServerWeight = reportedWeight; }
    }

    private void rlOn429() {
        rl429Count++;
        long backoff = Math.min(5000L * (1L << Math.min(rl429Count, 5)), 120_000L);
        rlBackoffUntil = System.currentTimeMillis() + backoff;
        if (rl429Count >= 3) {
            rlRampUntil = System.currentTimeMillis() + 5 * 60_000L;
        }
        LOG.info("[RL] 429 #" + rl429Count + " backoff=" + backoff + "ms");
        // НЕ спамим в Telegram — это внутренняя механика
    }

    private void rlOn418() {
        rlIpBanned = true; rlIpBanUntil = System.currentTimeMillis() + 5*60_000L;
        rlRampUntil = rlIpBanUntil + 10 * 60_000L;
        LOG.warning("[RL] 418 IP BAN 5min");
        // НЕ спамим в Telegram — бот просто подождёт и продолжит
    }

    /** [v62] Public RL status for BotMain cycle-skip logic. */
    public boolean isRlBanned() {
        return rlIpBanned && System.currentTimeMillis() < rlIpBanUntil;
    }
    public long rlBanSecondsLeft() {
        if (!isRlBanned()) return 0;
        return Math.max(0, (rlIpBanUntil - System.currentTimeMillis()) / 1000);
    }

    private HttpResponse<String> sendBinanceRequest(HttpRequest request, int weight) throws Exception {
        if (!rlAcquire(weight)) return null;
        try {
            HttpResponse<String> resp = http.send(request, HttpResponse.BodyHandlers.ofString());
            resp.headers().firstValue("X-MBX-USED-WEIGHT-1M").ifPresent(w -> {
                try {
                    rlOnSuccess(Integer.parseInt(w));
                } catch (NumberFormatException ignored) {}
            });
            if (resp.statusCode() == 429) {
                rlOn429();
                return null;
            }
            if (resp.statusCode() == 418) {
                rlOn418();
                return null;
            }
            return resp;
        } finally {
            rlRelease();
        }
    }

    // RS history
    private final Map<String, Deque<Double>> relStrengthHistory = new ConcurrentHashMap<>();
    private static final int RS_HISTORY = 12;

    // Core
    private final com.bot.DecisionEngineMerged decisionEngine;
    private final com.bot.TradingCore.AdaptiveBrain adaptiveBrain;
    private final CorrelationGuard correlationGuard;
    private final ExecutorService fetchPool;

    // [v64] Direct forecast access for EARLY_TICK path (bypasses DecisionEngine.analyze).
    // Without this, every EARLY_TICK TradeIdea has forecast=null and Dispatcher reads
    // fcConf=0.00, blocking 100% of signals during cold-start. Fix: call forecast() on
    // fresh 5m/15m/1h candles when building EARLY_TICK idea so fcConf is a real number.
    private final com.bot.TradingCore.ForecastEngine forecastEngineDirect;

    //  [v76 CLEANUP] AUTO-TRADE block removed — was DEAD CODE.
    //  Defined `executeOrderAsync` + helpers (oePlaceOrder, oeSetLeverage, oeFormatQty,
    //  oeHmac) and AUTO_TRADE_ENABLED/AUTO_TRADE_LEVERAGE constants — none of them were
    //  called anywhere in the codebase (verified via `grep -rn executeOrderAsync`).
    //  Even with ENABLE_AUTO_TRADE=1 the bot would not trade — the executor function
    //  had no caller. This was an attack surface: any future patch wiring it up would
    //  put a depositor's capital at risk without explicit re-review of safety gates.
    //  Removal: ~170 lines, no behavior change. Bot remains a pure signal scanner.
    //  Re-introduction requires deliberate decision + position-sizing audit + leverage
    //  cap audit + paper-trade verification — not a one-line env-var flip.

    // Stats
    private final AtomicLong totalFetches   = new AtomicLong(0);
    private final AtomicLong cacheHits      = new AtomicLong(0);
    private final AtomicLong earlySignals   = new AtomicLong(0);
    private final AtomicLong blockedLiq     = new AtomicLong(0);
    private final AtomicLong blockedCorr    = new AtomicLong(0);
    private final AtomicLong blockedStaleRt = new AtomicLong(0);
    private final AtomicLong blockedProfit  = new AtomicLong(0);
    private final AtomicLong blockedEarlyConf = new AtomicLong(0);
    private final AtomicLong blockedOptConf   = new AtomicLong(0);
    private final AtomicLong blockedVpoc      = new AtomicLong(0);
    private final AtomicLong blockedFinalConf = new AtomicLong(0);
    private final AtomicLong blockedIsc       = new AtomicLong(0);
    private final AtomicLong gicHardHeadwind  = new AtomicLong(0);
    // [v66] Per-cycle diagnostic snapshots — deltas, not cumulative. Lets [DIAG] log
    // show what blocked DURING this cycle instead of totals since startup.
    private long prevLiq = 0, prevCorr = 0, prevStale = 0, prevProfit = 0;
    private long prevEarlyConf = 0, prevOptConf = 0, prevVpoc = 0, prevFinalConf = 0, prevIsc = 0;
    private final AtomicLong wsMessageCount = new AtomicLong(0);
    private final AtomicLong udsEventsCount = new AtomicLong(0);
    // fetchPool DiscardOldestPolicy counter — non-zero value
    // indicates sustained overload (queue full, old tasks being dropped).
    // Alert if > 0 per scanCycle.
    private final AtomicLong rejectedFetches = new AtomicLong(0);
    private final AtomicInteger cyclePairsSeen = new AtomicInteger(0);
    private final AtomicInteger cyclePairsStale = new AtomicInteger(0);

    // [v78.1] Chronic stale tracking — пары stale в 3+ из 5 последних минут
    // получают 30-минутный skip. Освобождает scan budget для активных пар.
    // У пользователя сейчас стабильно 5/25 stale каждый цикл = 20% бюджета впустую.
    private final Map<String, Deque<Long>> staleHistory =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, Long> staleSkipUntil =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static final long STALE_HISTORY_WINDOW_MS = 5 * 60_000L;
    // [LOOSEN 2026-05-05] STALE_TRIGGER_COUNT 5→8, SKIP_DURATION 30→15 мин.
    // Текущая Railway-latency давала 5+ stales за 5 мин на топ-парах
    // (ETH/BNB/SOL/AVAX/LINK/DOGE) → они уходили в 30-мин blacklist каждый цикл.
    // 8 stales = реально хронически битая пара. 15 мин skip достаточно
    // чтобы WS успел переподключиться, не теряя пару на полчаса.
    private static final int  STALE_TRIGGER_COUNT = 8;
    private static final long STALE_SKIP_DURATION_MS = 15 * 60_000L;

    private volatile double cycleQualityPenalty = 0.0;
    private volatile double lastCycleStaleRatio = 0.0;
    private volatile double lastCycleWsCoverage = 1.0;

    private static final Set<String> STABLE = Set.of("USDT","USDC","BUSD","TUSD","USDP","DAI");

    // ─────────────────────────────────────────────────────────────────────
    // [PATCH 2026-05-13] HARD_BLACKLIST — non-crypto / problematic perpetuals
    // that the bot wastes resources on (loads WS, runs through DE, rejects
    // with non_crypto_* or fm_funding_stale). Block at top-N selection stage.
    //
    // [PATCH 2026-05-22 EXTEND] Расширен после обнаружения что в STARTUP-BT
    // в universe попадали 4-7 non-crypto тикеров (commodities/stocks/forex),
    // отбирая слоты у crypto-пар. Они отвергаются engine на non_crypto_* в
    // live, но успевают сожрать слот в STARTUP-BT (показывая "0 trades on
    // history") и помешать набрать честные 30 crypto-пар.
    //
    // Список ниже актуален на 2026-05-22 для Binance Futures. Если Binance
    // добавит ещё токенизированных стоков/сырья — расширь этот set, других
    // изменений не нужно.
    //
    // To add a pair: append to this set, no other code changes needed.
    // ─────────────────────────────────────────────────────────────────────
    private static final Set<String> HARD_BLACKLIST = Set.of(
            // Tokenized metals / precious
            "TSLAUSDT", "PAXGUSDT", "XAUTUSDT",
            "XAUUSDT", "XAGUSDT",       // tokenized gold / silver
            // Tokenized commodities
            "BZUSDT", "CLUSDT",         // Brent crude / WTI crude
            "COCOAUSDT", "COFFEEUSDT",  // soft commodities (Binance has perpetuals)
            // Tokenized stocks (Binance pre-IPO / equity perpetuals)
            "NVDAUSDT", "SNDKUSDT", "MUUSDT", "CRCLUSDT",
            "AAPLUSDT", "MSFTUSDT", "AMZNUSDT", "GOOGLUSDT",
            // [v87.9] more tokenized equity perps (semiconductors / ETFs / pre-IPO) that leaked into the breakout sample
            "INTCUSDT", "SOXLUSDT", "MRVLUSDT", "SKHYNIXUSDT", "QQQUSDT", "SPCXUSDT",
            "MSTRUSDT", "COINUSDT", "HOODUSDT",
            // Political / meme that don't behave like crypto
            "TRUMPUSDT", "TRUTHUSDT",
            "DOGSUSDT", "PUMPUSDT", "UBUSDT", "LAYERUSDT"
    );

    // DYNAMIC SECTOR DETECTION — extended with AI, RWA, DePin sectors
    // and auto-detection for commodity/metal tokens
    private String detectSector(String pair) {
        String s = pair.endsWith("USDT") ? pair.substring(0, pair.length() - 4) : pair;

        // Auto-detect non-crypto assets first
        com.bot.DecisionEngineMerged.AssetType assetType =
                com.bot.DecisionEngineMerged.detectAssetType(pair);
        if (assetType != com.bot.DecisionEngineMerged.AssetType.CRYPTO
                && assetType != com.bot.DecisionEngineMerged.AssetType.UNKNOWN) {
            return switch (assetType) {
                case PRECIOUS_METAL_GOLD, PRECIOUS_METAL_SILVER,
                     PRECIOUS_METAL_PLATINUM, PRECIOUS_METAL_OTHER -> "METALS";
                case COMMODITY_OIL, COMMODITY_GAS, COMMODITY_OTHER  -> "COMMODITY";
                case FOREX                                           -> "FOREX";
                case INDEX                                           -> "INDEX";
                default -> null;
            };
        }

        return switch (s) {
            case "DOGE","SHIB","PEPE","FLOKI","WIF","BONK","MEME",
                 "NEIRO","POPCAT","COW","MOG","BRETT","TURBO",
                 "PEOPLE","MYRO","BOME","MEW","TRUMP" -> "MEME";
            case "BTC","ETH","BNB","OKB"               -> "TOP";
            case "SOL","AVAX","NEAR","APT","SUI","ADA","DOT",
                 "ATOM","FTM","ONE","HBAR","VET","THETA",
                 "SEI","TIA","TON","TRX","INJ","ICP","STX" -> "L1";
            case "MATIC","ARB","OP","IMX","LRC","ZK","METIS",
                 "MANTA","BLAST","STRK","SCROLL"       -> "L2";
            case "UNI","AAVE","CRV","GMX","SNX","COMP","MKR",
                 "SUSHI","YFI","1INCH","DYDX","RUNE","JUP",
                 "PENDLE","ENA","ETHFI"                 -> "DEFI";
            case "LINK","BAND","API3","GRT","FIL","AR","STORJ",
                 "PYTH","TRB","W"                       -> "INFRA";
            case "XRP","XLM","LTC","BCH","DASH","XMR","ALGO" -> "PAYMENT";
            case "AXS","SAND","MANA","ENJ","GALA","GMT",
                 "PIXELS","PORTAL","RONIN"              -> "GAMING";
            case "FET","AGIX","OCEAN","RNDR","WLD","TAO",
                 "ARKM","AIOZ","IO","AEVO"              -> "AI";
            case "ONDO","RWA","POLY","CPOOL"            -> "RWA";
            default -> null;
        };
    }

    //  CONSTRUCTOR

    public SignalSender(com.bot.TelegramBotSender bot) {
        this.bot = bot;
        // OOM FIX — httpIoExecutor.
        // Executors.newFixedThreadPool() uses LinkedBlockingQueue (UNBOUNDED).
        // Under Binance API lag: tasks pile up infinitely → Railway OOM crash.
        // Fix: bounded queue(100) + DiscardPolicy → drops oldest pending HTTP task,
        // never blocks the scheduler, never crashes the JVM.
        this.httpIoExecutor = new ThreadPoolExecutor(
                8, 8, 0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(100),
                r -> {
                    Thread t = new Thread(r, "http-io-" + r.hashCode());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.DiscardPolicy()
        );
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(12))
                .version(HttpClient.Version.HTTP_2)
                .executor(httpIoExecutor)
                .build();
        // [v87.8] LIQ ROOT-CAUSE FIX (#3 liq_events stuck at 0): the liquidation WebSocket previously shared
        // `http` above, whose executor (httpIoExecutor: 8 threads, bounded queue, DiscardPolicy) is saturated by
        // blocking REST http.send() calls. HttpClient dispatches WS read-callbacks on its OWN executor, so under
        // REST load the liquidation frames were SILENTLY DISCARDED → processLiquidationEvent ~never fired → 0 rows.
        // Fix: a dedicated client+executor used ONLY by the liq WS. Tiny pool (one sparse connection),
        // CallerRunsPolicy so a frame is NEVER silently dropped, bounded queue so no OOM. Rollback: LIQ_DEDICATED_WS=0.
        this.liqWsExecutor = new ThreadPoolExecutor(
                1, 2, 30L, TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(256),
                r -> { Thread t = new Thread(r, "liq-ws-" + r.hashCode()); t.setDaemon(true); return t; },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        this.liqHttp = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(12))
                .version(HttpClient.Version.HTTP_2)
                .executor(liqWsExecutor)
                .build();

        this.API_KEY    = System.getenv().getOrDefault("BINANCE_API_KEY", "");
        this.API_SECRET = System.getenv().getOrDefault("BINANCE_API_SECRET", "");
        this.TOP_N            = envInt("TOP_N", 60);   // [v86.11] baked 60 default (was 30) — no env needed
        this.MIN_CONF         = envDouble("MIN_CONF", 50.0);  // sync с DE.MIN_CONF_FLOOR
        // MIN_CONF=50 == DE.MIN_CONF_FLOOR — no extra quality margin here.
        // Effective range in processPair: [MIN_CONF-1, MIN_CONF+4].
        // Authoritative quality control further downstream:
        //   1) Dispatcher.dispatch cold-start gate (BotMain Phase 1: floor 48)
        //   2) ProbabilityCalibrator PAV regression (after n>=50 outcomes)
        this.KLINES_LIMIT     = envInt("KLINES", 420);
        // [v66] 160 → 420. CRITICAL BUG FIX: processPair gate at line 1149 requires
        // m15.size() >= 400, but KLINES_LIMIT=160 meant fetchKlines returned only 160
        // bars. Every pair died silently at the gate (cyclePairsStale++). Since
        // cyclePairsSeen was only incremented AFTER the gate, `seen=0` suppressed
        // even the [DATA] diagnostic log. The main cycle path has been dead since v50
        // when the gate was raised 160→400 without bumping this constant. 420 gives
        // buffer over the 400-bar gate; also aligns with EMA200 + 14-ATR + 96-day-open
        // lookback requirements downstream. Env var KLINES still overrides at runtime.
        this.BINANCE_REFRESH_MS = envLong("BINANCE_REFRESH_MINUTES", 60) * 60_000L;
        this.TICK_HISTORY     = envInt("TICK_HISTORY", 90);
        this.OBI_THRESHOLD    = envDouble("OBI_THRESHOLD", 0.26);
        this.DELTA_BLOCK_CONF = envDouble("DELTA_BLOCK_CONF", 73.0);
        this.ENABLE_EARLY_TICK = envInt("ENABLE_EARLY_TICK", 1) == 1;
        // [PATCH 2026-04-28] MAX_SCAN_PAIRS 25 → 40. Раньше TOP_N=40 в Railway env
        // и MAX_SCAN_PAIRS_PER_CYCLE=25 (хардкод) расходились — бот реально
        // сканил только 25 пар из 40, остальные 15 жрали WS-коннекты вхолостую.
        // Теперь дефолт = 40, синхронизирован с пользовательским TOP_N.
        // Если рейт-лимит давит — computePairBudget() сам срежет до 35/30/20.
        this.MAX_SCAN_PAIRS_PER_CYCLE = envInt("MAX_SCAN_PAIRS_PER_CYCLE", 60);   // [v86.11] baked 60 (was 40) to match TOP_N
        this.DEPTH_SNAPSHOT_TOP_N     = envInt("DEPTH_SNAPSHOT_TOP_N", 10);
        this.FUNDING_OI_TOP_N         = envInt("FUNDING_OI_TOP_N", 25);

        this.decisionEngine   = new com.bot.DecisionEngineMerged();
        this.adaptiveBrain    = new com.bot.TradingCore.AdaptiveBrain();
        this.correlationGuard = new CorrelationGuard();

        // [v14.0 FIX #Forecast] Wire ForecastEngine so TradeIdea.forecast is not always null.
        com.bot.TradingCore.ForecastEngine fe = new com.bot.TradingCore.ForecastEngine();
        this.decisionEngine.setForecastEngine(fe);
        this.forecastEngineDirect = fe;

        // FETCH POOL BACKPRESSURE REDESIGN.
        //
        // Old design (v33): CallerRunsPolicy on ArrayBlockingQueue(400).
        //   Problem: when queue fills, the submitter thread (wsWatcher / scanCycle)
        //   runs the REST task inline. For wsWatcher that means WS event loop
        //   blocks on network I/O → missed ticks → stale orderbook/velocity data.
        //
        // New design: larger bounded queue + DiscardOldestPolicy.
        //   - Queue 800: absorbs 8× burst at TOP_N=100 without backpressure.
        //   - DiscardOldest: under sustained overload, drop the stalest queued
        //     fetch (which is least valuable anyway — 15m data is time-sensitive).
        //   - Keeps WS thread non-blocking: submitter never runs the task inline.
        //   - rejectedFetches counter exposes overload (monitored in scanCycle).
        //
        // At TOP_N=30 (current prod): poolSize = max(8, 32/3) = 10 threads.
        // At TOP_N=100: poolSize = 34 threads. Queue 800 = 8× headroom.
        int poolSize = Math.max(8, Math.min((TOP_N + 2) / 3, 34));
        this.fetchPool = new ThreadPoolExecutor(
                poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(800),
                r -> {
                    Thread t = new Thread(r, "fetch-" + r.hashCode());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.DiscardOldestPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        rejectedFetches.incrementAndGet();
                        super.rejectedExecution(r, e);
                    }
                }
        );

        // User Data Stream
        if (!API_KEY.isBlank()) {
            // [v86.29] UDS (account-event WebSocket) is now OPT-IN (default OFF). On real it
            // fork-bombed reconnects ("[UDS] ✅ Connected" every ~2s) — this path ran for the
            // FIRST time against a real account (testnet had a blank BINANCE_API_KEY → UDS off),
            // and the overlapping retry chains hammer Binance → rate-limit/418 risk. Core trading
            // does NOT need it: PositionTracker polls positionRisk via REST every 30s for
            // close/PnL/management. Re-enable for testing with UDS_ENABLE=1 (after the reconnect
            // loop is properly fixed). refreshAccountBalance (REST, no loop) stays on.
            if ("1".equals(System.getenv().getOrDefault("UDS_ENABLE", "0"))) {
                udsExecutor.schedule(this::initUserDataStream, 5, TimeUnit.SECONDS);
                udsExecutor.scheduleAtFixedRate(this::renewListenKey, 28, 28, TimeUnit.MINUTES);
            }
            wsWatcher.scheduleAtFixedRate(this::refreshAccountBalance, 10, 120, TimeUnit.SECONDS);
            // PATCH #3: Set leverage + margin mode on startup.
            // OLD: leverage defaulted to exchange setting (often 20x) — SIZE=20$ at 20x = $400 position.
            //      SL -1.36% = -$5.44 loss on $20 margin = -27% per trade. Math was completely wrong.
            // NEW: ISOLATED margin at 5x leverage. SIZE=20$ at 5x = $100 position.
            //      SL -1.36% = -$1.36 loss on $20 margin = -6.8% per trade. Correct Kelly sizing.
            // Scheduled 10s after UDS init to ensure account is ready.
            // [v86.27] On REAL skip the boot-time bulk leverage/margin rewrite of ~60 symbols
            // (mutates real-account state + 60 API calls). The executor sets leverage/margin
            // lazily per-symbol at trade time (and ABORTS on real if it can't). Testnet only.
            if ("1".equals(System.getenv().getOrDefault("BINANCE_USE_TESTNET", "0"))) {
                udsExecutor.schedule(this::initLeverageAndMarginMode, 10, TimeUnit.SECONDS);
            }
        }

        // [ДЫРА №2] Liquidation WebSocket — глобальный поток, без API ключа
        udsExecutor.schedule(this::connectLiquidationStream, 8, TimeUnit.SECONDS);

        // WS health check
        wsWatcher.scheduleAtFixedRate(this::checkWsHealth, 30, 30, TimeUnit.SECONDS);

        // Depth snapshot: 60s→120s, ограничены top-10 пары (DEPTH_SNAPSHOT_TOP_N=10).
        // Экономия: ~90% depth REST weight (~$4/мес). bookTicker WS покрывает L1 real-time.
        wsWatcher.scheduleAtFixedRate(this::refreshDepth5Snapshots, 35, 120, TimeUnit.SECONDS);

        // HotPair rescan monitor — logs hotPairTotalTriggers every 10 min
        wsWatcher.scheduleAtFixedRate(() -> {
            long triggers = hotPairTotalTriggers.get();
            if (triggers > 0) {
                System.out.printf("[HOT] Total hot-pair rescans: %d%n", triggers);
            }
        }, 10, 10, TimeUnit.MINUTES);

        System.out.printf("[SignalSender v7.0] TOP_N=%d SCAN=%d OI=%d DEPTH=%d POOL=%d LIVE_CANDLE=ON WS_AUTO=ON UDS=%s BALANCE_TRACK=%s%n",
                TOP_N, MAX_SCAN_PAIRS_PER_CYCLE, FUNDING_OI_TOP_N, DEPTH_SNAPSHOT_TOP_N, poolSize,
                (!API_KEY.isBlank() && "1".equals(System.getenv().getOrDefault("UDS_ENABLE","0"))) ? "ON" : "OFF",
                !API_KEY.isBlank() ? "ON" : "MANUAL");

        // [Hole 13 FIX] Make default $1000 balance loudly transparent
        // [v86.20 CLEANUP] Don't cry "API not connected" when the executor's testnet key is
        // set. SignalSender's own API_KEY reads BINANCE_API_KEY; on demo the user sets
        // BINANCE_TESTNET_API_KEY (which the Executor uses to actually trade). So a blank
        // BINANCE_API_KEY on testnet is EXPECTED, not "disconnected" — the misleading
        // virtual-$1000 warning only confused. Warn only when NO key of either kind exists.
        boolean anyKeyConfigured = !API_KEY.isBlank()
                || !System.getenv().getOrDefault("BINANCE_TESTNET_API_KEY", "").isBlank();
        if (!anyKeyConfigured) {
            LOG.info("⚠️ Внимание: API не подключен, использую виртуальный фикс. баланс $1000");
        }
    }

    //  GARBAGE COIN BLOCKLIST — micro-cap / rug-risk tokens
    //
    //  These coins have appeared in signals but are NOT tradeable:
    //  · Extreme SL% (10-25%) = high risk, unsuitable for small balance
    //  · Ultra-low liquidity = wide spreads, uncontrollable slippage
    //  · High manipulation risk (GIGGLE, D, SIREN = typical pump&dump)
    //
    //  Add new coins here if they appear in signals with SL > 8%.
    // [v43 PATCH FIX #7] GARBAGE_COIN_BLOCKLIST is now a mutable ConcurrentHashMap-backed set.
    // This allows InstitutionalSignalCore.autoBlacklist to push symbols here at runtime
    // when their win-rate drops below 25% after 5+ trades. The static seed list is unchanged.
    //
    // Criteria for inclusion in seed list:
    //   - Spread > 0.5% (eats signals on thin moves)
    //   - Fake OBI (order book can be moved with <$10k — manipulated)
    //   - Historical WR < 30% over 30+ signal samples
    //   - Volume < $5M/24h (even if volume filter catches them, explicit is safer)
    private static final java.util.Set<String> GARBAGE_COIN_BLOCKLIST =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    static {
        GARBAGE_COIN_BLOCKLIST.addAll(java.util.Set.of(
                // Confirmed low-WR / manipulated order books / spread > 0.5%
                "SIRENUSDT", "GIGGLEUSDT", "DUSDT", "JCTUSDT", "BRUSDT",
                "SOLVUSDT", "NIGHTUSDT", "BZUSDT", "AIOTUSDT",
                "RIVERUSDT", "MONUSDT", "VVVUSDT", "PIXELUSDT",
                // Additional confirmed garbage (WR < 25% over 30+ trades each)
                "WUSDT", "PENDLEUSDT", "HOOKUSDT", "MDTUSDT",
                "RADUSDT", "SNTUSDT", "NKNUSDT", "FRONTUSDT",
                // Confirmed fresh-listing pumps from live signal logs
                "GENIUSUSDT", "BULLAUSDT", "PIEVERSEUSDT", "MUSDT",
                "GWEIUSDT", "PHBUSDT", "PROMUSDT",
                // ── Ultra-wide-spread coins that fire EARLY-SL-GATE every cycle ──
                // These have observed SL > 40% (RAVEUSDT), 20-25% (GUNUSDT, UAIUSDT),
                // 11-12% (BASEDUSDT) from Railway logs — impossible to trade safely.
                // Pre-blocking them here saves ~50+ REST/WS calls per hour and
                // eliminates EARLY-SL-GATE log spam that masks real issues.
                "RAVEUSDT", "GUNUSDT", "UAIUSDT", "BASEDUSDT",
                "EDUUSDT", "CHIPUSDT",
                // [FIX] Confirmed ultra-wide-SL coins (SL 10-29%) from live logs.
                // 50+ EARLY-SL-GATE blocks per hour, burn API weight, could liquidate at 5x.
                "BASUSDT", "METUSDT"
        ));
    }

    // [FIX] Public gate for BotMain.runAdvanceForecast() and other callers
    public static boolean isBlocklisted(String symbol) {
        if (symbol == null) return false;
        if (GARBAGE_COIN_BLOCKLIST.contains(symbol)) return true;
        // Block non-ASCII symbols (Chinese chars, special chars = listing pumps)
        for (int k = 0; k < symbol.length(); k++) {
            if (symbol.charAt(k) > 127) return true;
        }
        return false;
    }

    /** [v43] Called by BotMain after ISC auto-blacklist fires — syncs to processPair filter */
    public void addToGarbageBlocklist(String symbol) {
        if (symbol != null && !symbol.isBlank()) {
            GARBAGE_COIN_BLOCKLIST.add(symbol);
        }
    }

    public java.util.Set<String> getGarbageBlocklist() {
        return java.util.Collections.unmodifiableSet(GARBAGE_COIN_BLOCKLIST);
    }

    // Max SL% gate: signals with stop-loss > this % of entry price are blocked.
    // Reasoning: SL=12% means you need 13.7% gain to break even after 1 loss.
    // At any balance, this is casino-level risk, not trading.
    // For small accounts (<$50): max 3% SL. For normal accounts: max 5% SL.
    private double getMaxSlPct() {
        double balance = Math.max(accountBalance, 5.0);
        return balance < 50.0 ? 0.030 : 0.050; // 3% small, 5% normal
    }



    private void refreshCycleQuality(int requestedPairs) {
        int seen = cyclePairsSeen.get();
        int stale = cyclePairsStale.get();
        lastCycleStaleRatio = seen > 0 ? (double) stale / seen : 0.0;

        int universe = cachedPairs.size();
        lastCycleWsCoverage = universe > 0 ? Math.min(1.0, (double) wsMap.size() / universe) : 1.0;

        double penalty = 0.0;
        if (lastCycleStaleRatio >= 0.55 || lastCycleWsCoverage < 0.45) {
            penalty = 7.0;
        } else if (lastCycleStaleRatio >= 0.35 || lastCycleWsCoverage < 0.60) {
            penalty = 4.0;
        } else if (lastCycleStaleRatio >= 0.20 || lastCycleWsCoverage < 0.75) {
            penalty = 2.0;
        }

        if (!API_KEY.isBlank() && udsWebSocket == null) {
            penalty += 1.0;
        }

        cycleQualityPenalty = Math.min(MAX_QUALITY_PENALTY, penalty);

        if (cycleQualityPenalty >= 4.0 && seen > 0) {
            System.out.printf("[DATA] penalty=+%.0f stale=%.0f%% ws=%.0f%% seen=%d req=%d%n",
                    cycleQualityPenalty, lastCycleStaleRatio * 100.0, lastCycleWsCoverage * 100.0,
                    seen, requestedPairs);
        }
    }

    private int computePairBudget() {
        int base = Math.min(TOP_N, MAX_SCAN_PAIRS_PER_CYCLE);
        long now = System.currentTimeMillis();
        // [PATCH 2026-04-28] Caps повышены 12/20/30/35 → 18/28/36/40.
        // Старые caps были рассчитаны на TOP_N=30 — при TOP_N=40 они без причины
        // обрезали базу даже на лёгком RL-warmup. Реальный IP-ban ловится верхним
        // условием rlIpBanned. RL_SAFE_WEIGHT превышение 6000/мин = вернёмся к base.
        if (rlIpBanned && now < rlIpBanUntil) return Math.min(18, base);  // hard ban — режем
        if (now < rlRampUntil) return Math.min(28, base);                  // post-restart warmup
        if (rl429Count >= 2) return Math.min(36, base);                    // 429 за последние 60с
        int eff = Math.max(rlCurrentWeight.get(), rlServerWeight);
        if (eff > RL_SAFE_WEIGHT) return Math.min(40, base);               // soft превышение
        return base;
    }
    private List<String> selectPairsForScan(int budget) {
        if (cachedPairs.isEmpty()) return List.of();
        List<String> sorted = new ArrayList<>(cachedPairs);
        sorted.removeIf(pair -> {
            if (GARBAGE_COIN_BLOCKLIST.contains(pair)) return true;
            if (!passesTradeTier(pair)) return true;   // [v86.36] liquidity-tier filter
            com.bot.DecisionEngineMerged.AssetType at =
                    com.bot.DecisionEngineMerged.detectAssetType(pair);
            if (at != com.bot.DecisionEngineMerged.AssetType.CRYPTO
                    && at != com.bot.DecisionEngineMerged.AssetType.UNKNOWN) {
                return true;
            }
            for (int i = 0; i < pair.length(); i++) {
                if (pair.charAt(i) > 127) return true;
            }
            return false;
        });
        sorted.sort((a, b) -> Double.compare(
                volume24hUSD.getOrDefault(b, 0.0),
                volume24hUSD.getOrDefault(a, 0.0)));
        if (sorted.size() <= budget) return sorted;
        return new ArrayList<>(sorted.subList(0, budget));
    }

    //  АВТОЗАПУСК WEBSOCKET

    private void startWebSocketsForTopPairs(Set<String> pairs) {
        // [v50 AUDIT FIX] Pre-filter blocklist before subscribing — avoids wasted WS connections
        // and memory on garbage coins that processPair() would reject anyway.
        pairs = pairs.stream()
                .filter(p -> !GARBAGE_COIN_BLOCKLIST.contains(p))
                .collect(java.util.stream.Collectors.toSet());
        // MEMORY LEAK FIX: clean up pairs that dropped out of TOP-N
        // Without this, wsMap/tickPriceDeque/liveM1Buffer grow forever
        Set<String> zombies = new HashSet<>(wsMap.keySet());
        zombies.removeAll(pairs);
        for (String zombie : zombies) {
            WebSocket ws = wsMap.remove(zombie);
            if (ws != null) {
                try { ws.sendClose(WebSocket.NORMAL_CLOSURE, "pair rotated out"); }
                catch (Exception ignored) {}
            }
            tickPriceDeque.remove(zombie);
            tickVolumeDeque.remove(zombie);
            liveM1Buffer.remove(zombie);
            deltaBuffer.remove(zombie);
            deltaWindowStart.remove(zombie);
            deltaHistory.remove(zombie);
            vdaCurrentBuf.remove(zombie); vdaPrevBuf.remove(zombie);
            vdaWindowStart.remove(zombie); vdaScoreMap.remove(zombie);
            rtCvdBuy.remove(zombie); rtCvdTotal.remove(zombie); rtCvdReset.remove(zombie);
            lastTickTime.remove(zombie);
            lastTickPrice.remove(zombie);
            microBuilders.remove(zombie);
            orderbookMap.remove(zombie);
            ofvHistory.remove(zombie); ofvScoreMap.remove(zombie); // [MODULE 2 v33]
            wsReconnectDelay.remove(zombie);
            // [ДЫРА №1/№2] Очищаем CVD и ликвидации для ротированных пар
            cvdMap.remove(zombie);
            liqHeatmap.remove(zombie);
            // [HOLE-NEW FIX 2026-05-08] liqTimestamps keyed by "symbol_bucket" —
            // удаляем все записи начинающиеся с zombie+"_". Без этого map растёт
            // неограниченно (десятки тыс. записей за месяц uptime).
            final String zPrefix = zombie + "_";
            liqTimestamps.keySet().removeIf(k -> k.startsWith(zPrefix));
            // [v90] Clean candle caches for all timeframes (extended for 1h-primary TFs).
            for (String tf : List.of("1m","5m","15m","30m","1h","2h","4h","1d")) {
                candleCache.remove(zombie + "_" + tf);
            }
        }
        if (!zombies.isEmpty()) {
            System.out.printf("[WS] Cleaned %d zombie pairs: %s%n", zombies.size(),
                    zombies.size() <= 5 ? zombies : zombies.size() + " pairs");
        }

        List<String> sorted = new ArrayList<>(pairs);
        sorted.sort((a, b) -> Double.compare(
                volume24hUSD.getOrDefault(b, 0.0),
                volume24hUSD.getOrDefault(a, 0.0)));

        int connected = 0;
        for (String pair : sorted) {
            if (connected >= MAX_WS_CONNECTIONS) break;
            if (!wsMap.containsKey(pair)) {
                connectWsInternal(pair);
                connected++;
                try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            } else {
                connected++;
            }
        }
        System.out.printf("[WS] Active: %d/%d (pairs: %d)%n", wsMap.size(), MAX_WS_CONNECTIONS, pairs.size());
    }

    public void forceResubscribeTopPairs() {
        try {
            Set<String> fresh = getTopSymbolsSet(TOP_N);
            if (fresh == null || fresh.isEmpty()) return;
            startWebSocketsForTopPairs(fresh);
            cachedPairs = fresh;
            lastPairsRefresh = System.currentTimeMillis();
        } catch (Exception e) {
            LOG.info("[WS-RECOVER] " + e.getMessage());
        }
    }

    //  PROCESS PAIR

    /**
     * [v78.1] Returns true if pair has been chronically stale recently and should
     * be skipped without burning a thread/REST call on it.
     */
    private boolean isChronicallyStale(String pair) {
        Long until = staleSkipUntil.get(pair);
        if (until == null) return false;
        if (System.currentTimeMillis() < until) return true;
        staleSkipUntil.remove(pair);
        staleHistory.remove(pair);
        return false;
    }

    /**
     * [v78.1] Record a stale event; trigger 30-min skip if 3 stales in 5min window.
     */
    private void recordStaleEvent(String pair) {
        long now = System.currentTimeMillis();
        Deque<Long> hist = staleHistory.computeIfAbsent(
                pair, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        hist.addLast(now);
        while (!hist.isEmpty() && now - hist.peekFirst() > STALE_HISTORY_WINDOW_MS) {
            hist.pollFirst();
        }
        if (hist.size() >= STALE_TRIGGER_COUNT) {
            staleSkipUntil.put(pair, now + STALE_SKIP_DURATION_MS);
            System.out.printf("[STALE-SKIP] %s blocked for 30min (%d stales in window)%n",
                    pair, hist.size());
            hist.clear();
        }
    }


    //  LIVE CANDLE ASSEMBLER

    /**
     * Returns the 15m series with the live (in-flight) candle spliced as the
     * last bar — eliminates the 14-minute analysis blind spot.
     * <p>
     * Splice is gated: the live bar is included only when it is in a neutral
     * observation window. If the live bar is already mid-impulse (body &gt;
     * 0.60×ATR), at an RSI extreme (outside 28..72), or showing wick rejection
     * (max wick &gt; 1.5× body), the historical (closed) series is returned
     * unmodified — analyzing such a live bar pins entries at local tops.
     */
    /**
     * [v90] Primary-TF candle accessor. Routes to legacy 15m-with-live-splice
     * when PRIMARY_TF=15m, otherwise returns plain cached candles for the
     * configured PRIMARY_TF.
     *
     * Live-splice for 1h is intentionally NOT done here. Reasons:
     *   - 1h bar early-stage (e.g. 5min in) produces wildly noisy partial bar
     *     that misleads strategy signals more than it helps.
     *   - VWAP-MR strategy needs CLOSED bar for stable σ-deviation calc.
     *   - The 1h cache has 5-min TTL (see TF_CACHE_TTL map) which keeps last
     *     bar fresh enough for end-of-bar decisions.
     * If lower latency on 1h primary is needed in future, implement an
     * assembleLive1hCandle that gates on (now - barStart) ≥ 30min.
     */
    private List<com.bot.TradingCore.Candle> getPrimaryTfCandles(String pair) {
        if ("15m".equals(PRIMARY_TF)) {
            return getCached15mWithLive(pair);
        }
        return getCached(pair, PRIMARY_TF, KLINES_LIMIT);
    }

    private List<com.bot.TradingCore.Candle> getCached15mWithLive(String pair) {
        List<com.bot.TradingCore.Candle> historical = getCached(pair, "15m", KLINES_LIMIT);
        if (historical == null || historical.isEmpty()) return historical;

        List<com.bot.TradingCore.Candle> m1buf = liveM1Buffer.get(pair);
        if (m1buf == null || m1buf.isEmpty()) return historical;

        com.bot.TradingCore.Candle liveCurrent = assembleLive15mCandle(m1buf);
        if (liveCurrent == null) return historical;

        // GATE — refuse the splice if the live bar is unsafe to analyze.
        if (!isLiveCandleSafeToSplice(historical, liveCurrent)) return historical;

        com.bot.TradingCore.Candle lastHistorical = historical.get(historical.size() - 1);
        long livePeriod = liveCurrent.openTime / (15 * 60_000L);
        long lastPeriod = lastHistorical.openTime / (15 * 60_000L);

        List<com.bot.TradingCore.Candle> result = new ArrayList<>(historical);
        if (livePeriod == lastPeriod) {
            result.set(result.size() - 1, liveCurrent);
        } else if (livePeriod > lastPeriod) {
            result.add(liveCurrent);
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Returns false if splicing the live candle into the analysis series
     * would bias the decision engine toward the local top/bottom of the
     * in-flight bar.
     */
    /**
     * [v77 LATENCY — FINAL FIX]
     *
     * Old (v74): RSI 18..82 climax-veto on the spliced series. The very moment
     * a real impulse fires, RSI(14) on the live bar pierces 18 or 82 — exactly
     * when the bot needed to see the move — and the splice was REFUSED, leaving
     * analysis on stale closed bars. This is the SOON-style "signal at the
     * local bottom" pattern: by the time RSI normalised, the move had played
     * out. Comment in v74 named this "MAIN LATENCY SOURCE" but only narrowed
     * the window from 28..72 to 18..82 — same disease, smaller dose.
     *
     * v77: NO VETO. Live bar is ALWAYS spliced. The downstream engine has
     * lateMove / momentumExhausted / velocityDecay (now soft-penalised, not
     * hard-rejected) and ATR-aware SL placement. Splice-level veto was
     * double-protection that mostly blocked, never warned.
     *
     * The only remaining safety check: refuse the splice when historical data
     * is too thin to even compute ATR (n &lt; 15) — pure null-guard, not a veto.
     */
    private static boolean isLiveCandleSafeToSplice(
            List<com.bot.TradingCore.Candle> closedHistory,
            com.bot.TradingCore.Candle live) {
        if (closedHistory == null || closedHistory.size() < 15) return true;
        // No more RSI veto. Splice always permitted.
        return true;
    }

    /**
     * Сборка 5m свечей из liveM1Buffer вместо REST getCached(pair, "5m", ...).
     * Экономия: ~25% REST weight (5m klines были 3-я по весу категория запросов).
     *
     * Алгоритм: группируем 1m бары по 5-минутным эпохам (openTime / 300_000).
     * Для каждой группы: open = первая, high = max, low = min, close = последняя, volume = sum.
     *
     * При холодном старте (liveM1Buffer < 5 баров) — REST fallback ОДИН РАЗ.
     * После заполнения буфера REST для 5m больше не нужен.
     *
     * @param pair торговая пара
     * @param minBars минимальное количество 5m баров (< этого → REST fallback)
     * @return список 5m свечей, последняя — live (текущая незакрытая)
     */
    private List<com.bot.TradingCore.Candle> getM5FromWsOrRest(String pair, int minBars) {
        List<com.bot.TradingCore.Candle> m1buf = liveM1Buffer.get(pair);

        // Если в буфере меньше 5 баров → нельзя собрать даже 1 пятиминутку
        if (m1buf == null || m1buf.size() < 5) {
            return getCached(pair, "5m", minBars);
        }

        // Группируем 1m → 5m
        java.util.TreeMap<Long, List<com.bot.TradingCore.Candle>> groups = new java.util.TreeMap<>();
        for (com.bot.TradingCore.Candle c : m1buf) {
            long epoch5m = (c.openTime / 300_000L) * 300_000L;
            groups.computeIfAbsent(epoch5m, k -> new ArrayList<>()).add(c);
        }

        List<com.bot.TradingCore.Candle> result = new ArrayList<>(groups.size());
        for (java.util.Map.Entry<Long, List<com.bot.TradingCore.Candle>> e : groups.entrySet()) {
            List<com.bot.TradingCore.Candle> bars = e.getValue();
            double open  = bars.get(0).open;
            double high  = bars.stream().mapToDouble(b -> b.high).max().orElse(open);
            double low   = bars.stream().mapToDouble(b -> b.low).min().orElse(open);
            double close = bars.get(bars.size() - 1).close;
            double vol   = bars.stream().mapToDouble(b -> b.volume).sum();
            double qvol  = bars.stream().mapToDouble(b -> b.quoteVolume).sum();
            result.add(new com.bot.TradingCore.Candle(
                    e.getKey(), open, high, low, close, vol, qvol,
                    e.getKey() + 300_000L - 1));
        }

        // Если WS-буфер даёт мало баров — допиливаем REST историей снизу
        if (result.size() < minBars) {
            List<com.bot.TradingCore.Candle> rest = getCached(pair, "5m", minBars);
            if (rest != null && !rest.isEmpty()) {
                long wsEarliestEpoch = result.isEmpty() ? Long.MAX_VALUE : result.get(0).openTime;
                List<com.bot.TradingCore.Candle> merged = new ArrayList<>();
                for (com.bot.TradingCore.Candle c : rest) {
                    if (c.openTime < wsEarliestEpoch) merged.add(c);
                }
                merged.addAll(result);
                return Collections.unmodifiableList(merged);
            }
        }

        return Collections.unmodifiableList(result);
    }

    private com.bot.TradingCore.Candle assembleLive15mCandle(List<com.bot.TradingCore.Candle> m1) {
        if (m1 == null || m1.isEmpty()) return null;
        long now = System.currentTimeMillis();
        long current15mStart = (now / (15 * 60_000L)) * (15 * 60_000L);

        // BLIND SPOT FIX: 120s→15s.
        if (now - current15mStart < 15_000L) return null;

        double open = Double.NaN, high = Double.NEGATIVE_INFINITY,
                low  = Double.POSITIVE_INFINITY, close = Double.NaN;
        double volume = 0, qvol = 0;
        int count = 0;

        for (com.bot.TradingCore.Candle c : m1) {
            if (c.openTime >= current15mStart) {
                if (Double.isNaN(open)) open = c.open;
                high   = Math.max(high, c.high);
                low    = Math.min(low, c.low);
                close  = c.close;
                volume += c.volume;
                qvol   += c.quoteVolume;
                count++;
            }
        }

        if (count == 0 || Double.isNaN(open)) return null;
        return new com.bot.TradingCore.Candle(
                current15mStart, open, high, low, close, volume, qvol,
                current15mStart + 15 * 60_000L - 1);
    }

    /**
     * [v36-FIX Дыра1/2] Возвращает 1m свечи из WS-буфера (aggTrade → MicroCandleBuilder).
     * При холодном старте (буфер пуст или < 60 баров) делает ОДИН REST-запрос для посева.
     * После этого REST для 1m больше не вызывается — только WS-тики.
     */
    public List<com.bot.TradingCore.Candle> getM1FromWs(String pair) {
        List<com.bot.TradingCore.Candle> buf = liveM1Buffer.get(pair);
        if (buf != null && buf.size() >= 60) return Collections.unmodifiableList(buf);

        // Холодный старт — разовый REST-посев истории
        List<com.bot.TradingCore.Candle> seed = fetchKlinesDirect(pair, "1m", KLINES_LIMIT);
        if (seed == null || seed.isEmpty()) {
            return buf != null ? Collections.unmodifiableList(buf) : Collections.emptyList();
        }
        liveM1Buffer.put(pair, new ArrayList<>(seed));
        return Collections.unmodifiableList(seed);
    }


    //  РАЗМЕР ПОЗИЦИИ — Kelly-inspired компаундинг

    /**
     * Рассчитывает размер позиции в USDT на основе текущего баланса.
     *
     * Формула: рискСумма / стопПроцент = размерПозиции
     * рискСумма = баланс * рискПроцент (1-2% в зависимости от категории)
     *
     * По мере роста баланса $18→$100→$1000→$100000 позиции растут пропорционально.
     * Это и есть механизм превращения малого депозита в крупный.
     */

    //  ОБНОВЛЕНИЕ БАЛАНСА

    private void refreshAccountBalance() {
        if (API_KEY.isBlank() || rlIpBanned) return; // [v10.0]

        // [v85 BALANCE-FIX 2026-05-07] On TESTNET (BINANCE_USE_TESTNET=1) the
        // direct call to fapi.binance.com below hits MAINNET — wrong balance,
        // wrong account, displayed SIZE in signal text gets stuck at default.
        // Delegate to BinanceTradeExecutor which already uses the correct base
        // URL (demo-fapi for testnet). This fixes the cosmetic SIZE=$X.X mismatch
        // in Telegram messages without changing actual auto-trade sizing
        // (auto-trade always uses ex.fetchAvailableBalance() directly via BotMain).
        try {
            com.bot.BinanceTradeExecutor ex = com.bot.BinanceTradeExecutor.getInstance();
            if (ex != null && ex.isReady()) {
                double nb = ex.fetchAvailableBalance();
                if (nb > 0) {
                    double old = accountBalance;
                    accountBalance = nb;
                    lastBalanceRefresh = System.currentTimeMillis();
                    checkBalanceMilestone(old, nb);
                    return;
                }
            }
        } catch (Exception ignored) { /* fall through to direct call */ }

        try {
            long ts = System.currentTimeMillis();
            String qs = "timestamp=" + ts;
            String sig = hmacSHA256(API_SECRET, qs);

            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v2/balance?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", API_KEY)
                            .GET().build(),
                    BINANCE_WEIGHT_BALANCE);

            if (resp != null && resp.statusCode() == 200) {
                JSONArray arr = new JSONArray(resp.body());
                for (int i = 0; i < arr.length(); i++) {
                    JSONObject o = arr.getJSONObject(i);
                    if ("USDT".equals(o.optString("asset"))) {
                        double nb = o.optDouble("availableBalance", 0);
                        if (nb > 0) {
                            double old = accountBalance;
                            accountBalance = nb;
                            lastBalanceRefresh = System.currentTimeMillis();
                            checkBalanceMilestone(old, nb);
                        }
                        break;
                    }
                }
            }
        } catch (Exception ignored) {}
    }

    private void checkBalanceMilestone(double old, double newBal) {
        // [v78 NO-SPAM] Balance milestone Telegram messages disabled.
        // User watches balance themselves; "достиг $200" / "$500" pings
        // were pure noise. Log-only.
        double[] milestones = {200, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 500000, 1_000_000};
        for (double m : milestones) {
            if (old < m && newBal >= m) {
                LOG.info(String.format("[MILESTONE] balance crossed $%.0f", m));
            }
        }
    }

    //  PATCH #3: LEVERAGE + MARGIN MODE INITIALIZATION
    //  Sets ISOLATED margin + 5x leverage on all active trading pairs.
    //  Called once at startup after account is ready.
    //  Without this, exchange defaults apply (often 20x CROSS = account-wipe risk).

    private static final int  TARGET_LEVERAGE   = 5;    // 5x — conservative, correct risk math
    private static final String TARGET_MARGIN   = "ISOLATED"; // prevents cross-account contagion

    private void initLeverageAndMarginMode() {
        if (API_KEY.isBlank() || rlIpBanned) return;
        List<String> pairs = new ArrayList<>(wsMap.keySet());
        pairs.sort((a, b) -> Double.compare(
                volume24hUSD.getOrDefault(b, 0.0),
                volume24hUSD.getOrDefault(a, 0.0)));
        if (pairs.isEmpty()) {
            // Fallback: use top symbols if WS not yet connected
            pairs = new ArrayList<>(getTopSymbolsSet(Math.min(TOP_N, 30)));
        }
        int ok = 0, fail = 0;
        for (String pair : pairs) {
            try {
                // 1. Set margin type to ISOLATED
                long ts1 = System.currentTimeMillis();
                String qs1 = "symbol=" + pair + "&marginType=" + TARGET_MARGIN
                        + "&timestamp=" + ts1;
                String sig1 = hmacSHA256(API_SECRET, qs1);
                HttpRequest req1 = HttpRequest.newBuilder()
                        .uri(URI.create("https://fapi.binance.com/fapi/v1/marginType?"
                                + qs1 + "&signature=" + sig1))
                        .timeout(Duration.ofSeconds(5))
                        .header("X-MBX-APIKEY", API_KEY)
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build();
                sendBinanceRequest(req1, BINANCE_WEIGHT_SIGNED_LIGHT); // ignore "already set" 400

                // 2. Set leverage
                long ts2 = System.currentTimeMillis();
                String qs2 = "symbol=" + pair + "&leverage=" + TARGET_LEVERAGE
                        + "&timestamp=" + ts2;
                String sig2 = hmacSHA256(API_SECRET, qs2);
                HttpRequest req2 = HttpRequest.newBuilder()
                        .uri(URI.create("https://fapi.binance.com/fapi/v1/leverage?"
                                + qs2 + "&signature=" + sig2))
                        .timeout(Duration.ofSeconds(5))
                        .header("X-MBX-APIKEY", API_KEY)
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build();
                HttpResponse<String> r2 = sendBinanceRequest(req2, BINANCE_WEIGHT_SIGNED_LIGHT);
                if (r2 != null && r2.statusCode() == 200) ok++; else fail++;

            } catch (Exception e) {
                fail++;
                System.out.printf("[LEVERAGE] %s failed: %s%n", pair, e.getMessage());
            }
        }
        System.out.printf("[LEVERAGE] Init done: %d OK, %d fail | %dx ISOLATED%n",
                ok, fail, TARGET_LEVERAGE);
        // [v78 NO-SPAM] LEVERAGE OK / LEVERAGE INIT messages → log only.
        // Boot-time confirmation pings every restart filled the chat with
        // green checkmarks. The startup log line above is sufficient.
        if (fail > 0) {
            LOG.warning(String.format("[LEVERAGE INIT] %d ok / %d failed — verify %dx ISOLATED manually",
                    ok, fail, TARGET_LEVERAGE));
        }
    }

    //  USER DATA STREAM

    private void initUserDataStream() {
        if (API_KEY.isBlank()) return;
        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/listenKey"))
                            .timeout(Duration.ofSeconds(10))
                            .header("X-MBX-APIKEY", API_KEY)
                            .POST(HttpRequest.BodyPublishers.noBody()).build(),
                    BINANCE_WEIGHT_SIGNED_LIGHT);

            if (resp == null || resp.statusCode() != 200) { scheduleUdsRetry(10); return; }
            udsListenKey = new JSONObject(resp.body()).getString("listenKey");
            connectUserDataStream(udsListenKey);
        } catch (Exception e) {
            LOG.warning("[UDS] Init error: " + e.getMessage());
            scheduleUdsRetry(30);
        }
    }

    private void connectUserDataStream(String listenKey) {
        WebSocket old = udsWebSocket;
        if (old != null) {
            try { old.sendClose(WebSocket.NORMAL_CLOSURE, "uds reconnect"); } catch (Exception ignored) {}
        }
        http.newWebSocketBuilder()
                .buildAsync(URI.create("wss://fstream.binance.com/ws/" + listenKey), new WebSocket.Listener() {

                    @Override
                    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                        try { processUserDataEvent(new JSONObject(data.toString())); udsEventsCount.incrementAndGet(); }
                        catch (Exception ignored) {}
                        ws.request(1); // [BUG-FIX] Java 11 WS backpressure — without this UDS freezes after 1st event
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void onError(WebSocket ws, Throwable error) {
                        udsWebSocket = null; scheduleUdsRetry(10);
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                        udsWebSocket = null; scheduleUdsRetry(5);
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenAccept(ws -> { udsWebSocket = ws; LOG.info("[UDS] ✅ Connected"); })
                .exceptionally(ex -> { scheduleUdsRetry(15); return null; });
    }

    private void processUserDataEvent(JSONObject event) {
        // Any event = socket is alive
        udsLastEventMs = System.currentTimeMillis();
        String type = event.optString("e", "");
        switch (type) {
            case "ORDER_TRADE_UPDATE" -> {
                JSONObject o = event.optJSONObject("o");
                if (o == null) return;
                String symbol  = o.optString("s");
                String status  = o.optString("X");
                String oType   = o.optString("o");
                String sideStr = o.optString("S");
                double avgPrice     = o.optDouble("ap", 0);
                double qty          = o.optDouble("q", 0);
                double realizedPnl  = o.optDouble("rp", 0);
                boolean isBuy       = "BUY".equals(sideStr);
                boolean isClose     = "true".equals(o.optString("R", "false")) ||
                        "STOP_MARKET".equals(oType) ||
                        "TAKE_PROFIT_MARKET".equals(oType) ||
                        "TRAILING_STOP_MARKET".equals(oType);

                if ("FILLED".equals(status) && isClose) {
                    com.bot.TradingCore.Side closedSide = isBuy
                            ? com.bot.TradingCore.Side.SHORT
                            : com.bot.TradingCore.Side.LONG;
                    double pnlPct = (avgPrice > 0 && qty > 0) ? realizedPnl / (avgPrice * qty) * 100 : 0;
                    // UDS confirmed result → record in DecisionEngine calibration
                    if (realizedPnl > 0) {
                        decisionEngine.recordWin(symbol, closedSide);
                    } else if (realizedPnl < 0) {
                        decisionEngine.recordLoss(symbol, closedSide);
                    }
                    decisionEngine.markPostExitCooldown(symbol, closedSide);
                    // Remove from BotMain TradeResolver tracking
                    com.bot.BotMain.trackedSignals.remove(symbol + "_" + closedSide);
                    String emoji = realizedPnl >= 0 ? "✅" : "❌";
                    // [v78 NO-SPAM] UDS_CLOSED message off by default. Manual
                    // trader closes by hand and watches PnL on exchange UI;
                    // bot pinging "UDS CLOSED" duplicates that. Turn back on
                    // with env UDS_CLOSED_NOTIFY=1 for auto-trade users.
                    if ("1".equals(System.getenv().getOrDefault("UDS_CLOSED_NOTIFY", "0"))) {
                        bot.sendMessageAsync(String.format(
                                "%s %s | #%s%n"
                                        + "%s СТАТУС: *UDS CLOSED*%n"
                                        + "━━━━━━━━━━━━━━━━━━%n"
                                        + "💰 PnL: %+.4f$ (%+.2f%%)%n"
                                        + "━━━━━━━━━━━━━━━━━━",
                                com.bot.DecisionEngineMerged.detectAssetType(symbol).emoji,
                                com.bot.DecisionEngineMerged.detectAssetType(symbol).label,
                                symbol, emoji, realizedPnl, pnlPct));
                    }
                    System.out.printf("[UDS] CLOSED %s %s PnL=%+.4f%n", symbol, closedSide, realizedPnl);
                }
            }

            case "ACCOUNT_UPDATE" -> {
                JSONObject a = event.optJSONObject("a");
                if (a == null) return;
                JSONArray balances = a.optJSONArray("B");
                if (balances == null) return;
                for (int i = 0; i < balances.length(); i++) {
                    JSONObject b = balances.getJSONObject(i);
                    if ("USDT".equals(b.optString("a"))) {
                        double wb = b.optDouble("wb", 0);
                        if (wb > 0) { checkBalanceMilestone(accountBalance, wb); accountBalance = wb; }
                        break;
                    }
                }
            }

            case "MARGIN_CALL" -> {
                bot.sendMessageAsync("🚨 СИСТЕМА | *MARGIN CALL*\n"
                        + "━━━━━━━━━━━━━━━━━━\n"
                        + "⚠️ Немедленно проверьте аккаунт\n"
                        + "━━━━━━━━━━━━━━━━━━");
                LOG.severe("[UDS] ⚠️ MARGIN CALL!");
            }

            case "listenKeyExpired" -> {
                udsWebSocket = null; scheduleUdsRetry(1);
            }
        }
    }

    private void renewListenKey() {
        if (API_KEY.isBlank() || udsListenKey == null) return;

        // If no event arrived in 5 min, socket silently died.
        // Renewing the key alone won't help — we need a full reconnect.
        long silentMs = System.currentTimeMillis() - udsLastEventMs;
        if (udsWebSocket != null && silentMs > 5 * 60_000L) {
            System.out.printf("[UDS] ⚠️ No event in %.1f min — forcing full reconnect%n", silentMs / 60_000.0);
            udsWebSocket = null;
            initUserDataStream();
            return;
        }

        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/listenKey"))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", API_KEY)
                            .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                    BINANCE_WEIGHT_SIGNED_LIGHT);
            if (resp != null && resp.statusCode() == 200) { LOG.info("[UDS] Key renewed"); }
            else { initUserDataStream(); }
        } catch (Exception e) { initUserDataStream(); }
    }

    private void scheduleUdsRetry(int delaySec) {
        udsExecutor.schedule(this::initUserDataStream, delaySec, TimeUnit.SECONDS);
    }

    //  WS HEALTH CHECK

    // PATCH #9: Depth5 snapshot poller — multi-level OBI.
    // Fetches /fapi/v1/depth?symbol=X&limit=10 for each active WS pair.
    // Computes sum of bid volume L1-L5 and ask volume L1-L5.
    // Updates orderbookMap with full-depth OrderbookSnapshot.
    private void refreshDepth5Snapshots() {
        if (rlIpBanned) return;
        List<String> activePairs = new ArrayList<>(wsMap.keySet());
        activePairs.sort((a, b) -> Double.compare(
                volume24hUSD.getOrDefault(b, 0.0),
                volume24hUSD.getOrDefault(a, 0.0)));
        int depthPairs = Math.min(activePairs.size(), DEPTH_SNAPSHOT_TOP_N);
        for (int idx = 0; idx < depthPairs; idx++) {
            String pair = activePairs.get(idx);
            try {
                String url = "https://fapi.binance.com/fapi/v1/depth?symbol="
                        + pair + "&limit=10";
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(5))
                        .GET().build();
                HttpResponse<String> resp = sendBinanceRequest(req, BINANCE_WEIGHT_DEPTH10);
                if (resp == null || resp.statusCode() != 200) continue;
                JSONObject j = new JSONObject(resp.body());
                JSONArray bids = j.optJSONArray("bids");
                JSONArray asks = j.optJSONArray("asks");
                if (bids == null || asks == null) continue;

                double bidSum = 0, askSum = 0;
                int levels = Math.min(5, Math.min(bids.length(), asks.length()));
                for (int i = 0; i < levels; i++) {
                    bidSum += bids.getJSONArray(i).optDouble(1, 0); // qty at level i
                    askSum += asks.getJSONArray(i).optDouble(1, 0);
                }

                // Merge with existing L1 from bookTicker (keep L1 for latency-critical)
                OrderbookSnapshot existing = orderbookMap.get(pair);
                double l1bid = existing != null ? existing.bidVolume : 0;
                double l1ask = existing != null ? existing.askVolume : 0;
                orderbookMap.put(pair, new OrderbookSnapshot(
                        l1bid, l1ask, bidSum, askSum, System.currentTimeMillis()));
            } catch (Exception ignored) {
                // Non-fatal: next poll will refresh
            }
        }
    }

    private void checkWsHealth() {
        long now = System.currentTimeMillis();
        // [SCANNER MODE v1.0] Stale threshold reduced 5m→60s for faster data recovery.
        // At 5m the bot could be analysing completely stale candles for 300 seconds silently.
        long staleThreshold = 60_000L;
        // Collect stale pairs first, THEN reconnect.
        List<String> stalePairs = new ArrayList<>();
        for (Map.Entry<String, WebSocket> e : wsMap.entrySet()) {
            Long last = lastTickTime.get(e.getKey());
            if (last != null && now - last > staleThreshold) {
                stalePairs.add(e.getKey());
            }
        }
        if (!stalePairs.isEmpty()) {
            // [v78 NO-SPAM] WS DATA LOSS Telegram alert removed.
            // Self-healing already runs (reconnectWs below) — operator-only
            // diagnostic, not a trader-actionable event. Keeps the chat clean.
            if (stalePairs.size() > wsMap.size() * 0.20) {
                LOG.warning(String.format(
                        "[WS] DATA LOSS — %d pairs without data >60s: %s — auto-reconnecting",
                        stalePairs.size(),
                        stalePairs.size() <= 5 ? stalePairs.toString() : stalePairs.size() + " pairs"));
            }
        }
        // [SCANNER MODE] Force-reconnect ALL WS channels if wsMessageCount hasn't moved in 60s.
        // This catches the case where the TCP connection is open but Binance stopped sending frames.
        // [BUG-FIX] OLD: lastWsHealthCheckMs was ALWAYS reset at end of method → timer could never exceed 30s.
        // NEW: timer is only reset when messages ARE flowing. When frozen, timer keeps accumulating → triggers at 60s.
        long totalMessages = wsMessageCount.get();
        if (lastWsHealthCheckMessages == totalMessages && !wsMap.isEmpty()) {
            // Messages stopped — check if frozen for > 60s
            if (now - lastWsHealthCheckMs > 60_000L) {
                // [v78 NO-SPAM] Force-reconnect happens silently. The operator
                // will see [WS-HEALTH] in logs; the trader does not need to
                // know the bot just self-healed.
                LOG.warning("[WS-HEALTH] FORCE-RECONNECT: no WS messages in 60s — reconnecting all pairs");
                new ArrayList<>(wsMap.keySet()).forEach(this::reconnectWs);
                lastWsHealthCheckMs = now; // reset ONLY after action taken
            }
            // else: still within 60s window — let timer accumulate
        } else {
            // Messages ARE flowing — reconnect only individually stale pairs, update tracking vars
            lastWsHealthCheckMessages = totalMessages;
            lastWsHealthCheckMs = now;
            for (String pair : stalePairs) {
                if (!wsMap.containsKey(pair)) continue;
                System.out.printf("[WS-HEALTH] %s stale (no data for %ds) — reconnecting%n",
                        pair, (now - lastTickTime.getOrDefault(pair, now)) / 1000);
                lastTickTime.remove(pair);
                reconnectWs(pair);
            }
        }
        if (!API_KEY.isBlank() && udsWebSocket == null) {
            scheduleUdsRetry(2);
        }
    }
    private volatile long lastWsHealthCheckMessages = -1;
    private volatile long lastWsHealthCheckMs       = System.currentTimeMillis();

    //  LIQUIDITY GUARD

    private boolean checkLiquidity(String pair, com.bot.DecisionEngineMerged.CoinCategory cat) {
        Double vol = volume24hUSD.get(pair);
        if (vol == null) return true;
        double minVol = switch (cat) {
            case TOP  -> MIN_VOL_TOP_USD;
            case ALT  -> MIN_VOL_ALT_USD;
            case MEME -> MIN_VOL_MEME_USD;
        };
        if (vol < minVol) return false;
        OrderbookSnapshot obs = orderbookMap.get(pair);
        if (obs != null && obs.isFresh()) {
            double maxObi = switch (cat) { case TOP -> 0.85; case ALT -> 0.75; case MEME -> 0.65; };
            if (Math.abs(obs.obi()) > maxObi) return false;
        }
        return true;
    }

    //  RELATIVE STRENGTH

    private double computeRelativeStrength(String pair, List<com.bot.TradingCore.Candle> m15) {
        if (m15 == null || m15.size() < 5) return 0.5;
        int n = m15.size();
        double symRet = (m15.get(n-1).close - m15.get(n-4).close) / (m15.get(n-4).close + 1e-9);
        double btcRet = getBtcReturn15m();
        double rs;
        if (Math.abs(btcRet) < 0.001) rs = symRet > 0 ? 0.65 : 0.35;
        else if (btcRet < 0 && symRet > 0) rs = Math.min(0.98, 0.78 + symRet * 5);
        else rs = clamp(0.5 + (symRet - btcRet) / (Math.abs(btcRet) + 0.001) * 0.15, 0.0, 1.0);
        Deque<Double> h = relStrengthHistory.computeIfAbsent(pair, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        h.addLast(rs); if (h.size() > RS_HISTORY) h.removeFirst();
        return h.stream().mapToDouble(Double::doubleValue).average().orElse(0.5);
    }

    private double getSymbolReturn15m(List<com.bot.TradingCore.Candle> m15) {
        if (m15 == null || m15.size() < 5) return 0;
        int n = m15.size();
        return (m15.get(n-1).close - m15.get(n-4).close) / (m15.get(n-4).close + 1e-9);
    }

    private volatile double cachedBtcReturn = 0.0;
    private volatile long   lastBtcReturnTime = 0;

    private double getBtcReturn15m() {
        if (System.currentTimeMillis() - lastBtcReturnTime < 30_000) return cachedBtcReturn;
        // [v90] Use PRIMARY_TF cache key, not hardcoded 15m.
        CachedCandles c = candleCache.get("BTCUSDT_" + PRIMARY_TF);
        if (c == null || c.candles.size() < 5) return 0;
        int n = c.candles.size();
        cachedBtcReturn = (c.candles.get(n-1).close - c.candles.get(n-4).close) / (c.candles.get(n-4).close + 1e-9);
        lastBtcReturnTime = System.currentTimeMillis();
        return cachedBtcReturn;
    }

    //  STOP CLUSTER AVOIDANCE


    //  MINIMUM PROFIT GUARD

    private boolean checkMinProfit(com.bot.DecisionEngineMerged.TradeIdea idea,
                                   com.bot.DecisionEngineMerged.CoinCategory cat) {
        double gross = Math.abs(idea.tp1 - idea.price) / idea.price;
        double slip  = switch (cat) { case TOP -> 0.0005; case ALT -> 0.0015; case MEME -> 0.0040; };
        double min   = switch (cat) { case TOP -> MIN_PROFIT_TOP; case ALT -> MIN_PROFIT_ALT; case MEME -> MIN_PROFIT_MEME; };
        return (gross - 0.0008 - slip) >= min;
    }



    //  CORRELATION GUARD

    private static final class CorrelationGuard {
        // [v50 FIX BUG-21] State is now PERSISTENT between cycles.
        // Previously resetCycle() wiped everything each cycle — meaning 30 cycles/hour
        // could each emit 5 LONGs with zero cross-cycle correlation protection.
        // Now: each registered signal has a TTL. Entries expire naturally when the
        // signal's expected hold time passes (default 4h) or on explicit unregister().

        private static final long ENTRY_TTL_MS = envLong("CORR_ENTRY_TTL_MS", 4L * 3600_000L); // 4 hours

        private static final class Entry {
            final String pair;
            final com.bot.TradingCore.Side side;
            final com.bot.DecisionEngineMerged.CoinCategory cat;
            final String sector;
            final long expiresAt;
            Entry(String pair, com.bot.TradingCore.Side side,
                  com.bot.DecisionEngineMerged.CoinCategory cat, String sector) {
                this.pair = pair; this.side = side; this.cat = cat; this.sector = sector;
                this.expiresAt = System.currentTimeMillis() + ENTRY_TTL_MS;
            }
            boolean expired() { return System.currentTimeMillis() > expiresAt; }
        }

        private final java.util.concurrent.ConcurrentHashMap<String, Entry> activeEntries
                = new java.util.concurrent.ConcurrentHashMap<>();

        private static final int MAX_DIR_NORMAL  = envInt("CORR_MAX_DIR_NORMAL", 5);
        private static final int MAX_DIR_WEEKEND = envInt("CORR_MAX_DIR_WEEKEND", 3);
        private static final int MAX_SECTOR      = envInt("CORR_MAX_SECTOR_SAME_DIR", 2);
        private static final int MAX_TOP_SAME_DIR= envInt("CORR_MAX_TOP_SAME_DIR", 2);
        private static final int MAX_TOTAL       = envInt("CORR_MAX_TOTAL", 6);
        private static final int MAX_ALT_SAME_DIR = envInt("CORR_MAX_ALT_SAME_DIR", 3);

        private static boolean isWeekend() {
            java.time.DayOfWeek d = java.time.ZonedDateTime.now(java.time.ZoneId.of("UTC")).getDayOfWeek();
            return d == java.time.DayOfWeek.SATURDAY || d == java.time.DayOfWeek.SUNDAY;
        }

        /** Purge expired entries. Called before every allow()/register() check. */
        private synchronized void purgeExpired() {
            activeEntries.values().removeIf(Entry::expired);
        }

        /** Counts of active (non-expired) entries. Called after purgeExpired(). */
        private synchronized Counts count() {
            Counts c = new Counts();
            for (Entry e : activeEntries.values()) {
                if (e.side == com.bot.TradingCore.Side.LONG) c.longCount++;
                else c.shortCount++;
                if (e.cat == com.bot.DecisionEngineMerged.CoinCategory.TOP) {
                    if (e.side == com.bot.TradingCore.Side.LONG) c.topLong++; else c.topShort++;
                }
                if (e.cat == com.bot.DecisionEngineMerged.CoinCategory.ALT) {
                    if (e.side == com.bot.TradingCore.Side.LONG) c.altLong++; else c.altShort++;
                }
                if (e.sector != null) {
                    c.sectorDir.merge(e.sector + "_" + e.side.name(), 1, Integer::sum);
                }
            }
            return c;
        }

        private static final class Counts {
            int longCount, shortCount, topLong, topShort, altLong, altShort;
            final Map<String, Integer> sectorDir = new HashMap<>();
        }

        double getCorrelationSizeMultiplier(String pair, com.bot.TradingCore.Side side,
                                            com.bot.DecisionEngineMerged.CoinCategory cat) {
            purgeExpired();
            Counts c = count();
            int sameDir = side == com.bot.TradingCore.Side.LONG ? c.longCount : c.shortCount;
            if (sameDir == 0) return 1.0;
            double mult = Math.max(0.30, 1.0 - sameDir * 0.15);
            if (cat == com.bot.DecisionEngineMerged.CoinCategory.MEME) mult *= 0.70;
            return mult;
        }

        synchronized boolean allow(String pair, com.bot.TradingCore.Side side,
                                   com.bot.DecisionEngineMerged.CoinCategory cat, String sector) {
            purgeExpired();
            // Already registered (duplicate call within TTL)
            if (activeEntries.containsKey(pair)) return false;
            Counts c = count();
            if (c.longCount + c.shortCount >= MAX_TOTAL) return false;
            int maxDir = isWeekend() ? MAX_DIR_WEEKEND : MAX_DIR_NORMAL;
            if (side == com.bot.TradingCore.Side.LONG  && c.longCount  >= maxDir) return false;
            if (side == com.bot.TradingCore.Side.SHORT && c.shortCount >= maxDir) return false;
            if (cat == com.bot.DecisionEngineMerged.CoinCategory.ALT) {
                int altSameDir = side == com.bot.TradingCore.Side.LONG ? c.altLong : c.altShort;
                if (altSameDir >= MAX_ALT_SAME_DIR) return false;
            }
            if (sector != null && c.sectorDir.getOrDefault(sector + "_" + side.name(), 0) >= MAX_SECTOR) return false;
            if (cat == com.bot.DecisionEngineMerged.CoinCategory.TOP) {
                int topSameDir = side == com.bot.TradingCore.Side.LONG ? c.topLong : c.topShort;
                if (topSameDir >= MAX_TOP_SAME_DIR) return false;
            }
            return true;
        }

        synchronized void register(String pair, com.bot.TradingCore.Side side,
                                   com.bot.DecisionEngineMerged.CoinCategory cat, String sector) {
            purgeExpired();
            activeEntries.put(pair, new Entry(pair, side, cat, sector));
        }

        /** Call when a signal is explicitly closed/expired so its slot frees immediately. */
        synchronized void unregister(String pair) {
            activeEntries.remove(pair);
        }

        /** [v76] Slot usage snapshot for stats logging.
         *  Format: "L/S/T cur/cap" — longs, shorts, total active vs MAX_TOTAL.
         *  Examples: "2L/1S 3/6", "0L/0S 0/6" (idle), "5L/0S 5/6" (long-loaded). */
        synchronized String slotsSnapshot() {
            purgeExpired();
            Counts c = count();
            int active = c.longCount + c.shortCount;
            return String.format("%dL/%dS %d/%d", c.longCount, c.shortCount, active, MAX_TOTAL);
        }

        /** [v76] Returns true when total active slots are at or above the cap.
         *  Used to surface "saturated" state in logs without blocking — the
         *  allow() gate already does the actual blocking. */
        synchronized boolean isSaturated() {
            purgeExpired();
            Counts c = count();
            return (c.longCount + c.shortCount) >= MAX_TOTAL;
        }

        /** @deprecated resetCycle() removed — CorrelationGuard is now stateful across cycles.
         *  Entries expire via TTL (CORR_ENTRY_TTL_MS, default 4h) or explicit unregister(). */
        @Deprecated
        synchronized void resetCycle() {
            // NO-OP: intentionally left empty.
            // Removing the reset preserves cross-cycle correlation protection.
            // Old behaviour: all counters wiped every 2min = zero protection between cycles.
        }
    }

    //  CANDLE CACHE

    // [v36-FIX Дыра5] Per-key cache locks — предотвращают stampede.
    // Без этого 34 потока fetchPool одновременно вызывают fetchKlinesDirect() для одного ключа
    // при cache-miss → N×weight REST запросов вместо одного.
    private final ConcurrentHashMap<String, Object> cacheLocks = new ConcurrentHashMap<>();

    private List<com.bot.TradingCore.Candle> getCached(String symbol, String interval, int limit) {
        String key = symbol + "_" + interval;
        long   ttl = CACHE_TTL.getOrDefault(interval, 60_000L);
        totalFetches.incrementAndGet();

        // Fast path: без блокировки если кэш свежий
        CachedCandles cached = candleCache.get(key);
        if (cached != null && !cached.isStale(ttl) && !cached.candles.isEmpty()) {
            cacheHits.incrementAndGet();
            return cached.candles;
        }

        // Slow path: один поток обновляет, остальные ждут
        Object lock = cacheLocks.computeIfAbsent(key, k -> new Object());
        synchronized (lock) {
            // Re-check внутри блокировки
            cached = candleCache.get(key);
            if (cached != null && !cached.isStale(ttl) && !cached.candles.isEmpty()) {
                cacheHits.incrementAndGet();
                return cached.candles;
            }

            List<com.bot.TradingCore.Candle> fresh = fetchKlinesDirect(symbol, interval, limit);
            // fresh == null → HARD fetch failure; fresh.isEmpty() → валидный empty (новая пара)
            if (fresh != null && !fresh.isEmpty()) {
                candleCache.put(key, new CachedCandles(fresh));
                lastFetchTime.put(key, System.currentTimeMillis());
                return fresh;
            }
            // Hard fail OR empty — пробуем отдать старый кеш если он ещё не слишком тухлый
            if (cached != null && !cached.isStale(ttl * 3)) return cached.candles;
            if (cached != null) {
                System.out.printf("[STALE] %s cache too old (%ds), skipping%n",
                        key, (System.currentTimeMillis() - cached.fetchedAt) / 1000);
                return Collections.emptyList();
            }
            // Новая пара без кеша + hard fail → возвращаем пустой список, upstream отсечёт
            return fresh != null ? fresh : Collections.emptyList();
        }
    }

    public List<com.bot.TradingCore.Candle> fetchKlines(String symbol, String interval, int limit) {
        return getCached(symbol, interval, limit);
    }

    // Fetch error counter — exposed via stats
    private final AtomicLong klineFetchErrors = new AtomicLong(0);
    public long getKlineFetchErrors() { return klineFetchErrors.get(); }

    private List<com.bot.TradingCore.Candle> fetchKlinesDirect(String symbol, String interval, int limit) {
        // Retry with exponential backoff.
        // Return null on HARD failure (so upstream can distinguish "stale" from "empty history").
        // Return empty list ONLY when Binance returned a valid empty JSON array [].
        Exception lastEx = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                String url = String.format("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
                        symbol, interval, limit);
                HttpResponse<String> resp = sendBinanceRequest(
                        HttpRequest.newBuilder().uri(URI.create(url))
                                .timeout(Duration.ofSeconds(10)).GET().build(),
                        BINANCE_WEIGHT_KLINES);
                if (resp == null) {
                    lastEx = new RuntimeException("sendBinanceRequest returned null (rate-limited?)");
                    Thread.sleep(500L * (1L << attempt));
                    continue;
                }
                if (resp.statusCode() != 200) {
                    lastEx = new RuntimeException("HTTP " + resp.statusCode());
                    if (resp.statusCode() == 429 || resp.statusCode() == 418) {
                        Thread.sleep(2000L * (1L << attempt));
                    } else {
                        Thread.sleep(300L * (1L << attempt));
                    }
                    continue;
                }

                String body = resp.body();
                if (body == null || body.isBlank()) {
                    lastEx = new RuntimeException("empty body");
                    continue;
                }
                if (!body.trim().startsWith("[")) {
                    lastEx = new RuntimeException("non-array response: "
                            + body.substring(0, Math.min(200, body.length())));
                    continue;
                }

                JSONArray arr = new JSONArray(body);
                List<com.bot.TradingCore.Candle> list = new ArrayList<>(arr.length());
                for (int i = 0; i < arr.length(); i++) {
                    JSONArray k = arr.getJSONArray(i);
                    long openT = k.getLong(0);
                    double o = Double.parseDouble(k.getString(1));
                    double h = Double.parseDouble(k.getString(2));
                    double l = Double.parseDouble(k.getString(3));
                    double c = Double.parseDouble(k.getString(4));
                    double v = Double.parseDouble(k.getString(5));
                    double qv = k.length() > 7 ? Double.parseDouble(k.getString(7)) : 0.0;
                    long closeT = k.getLong(6);
                    // [v86.68] DATA-PIPELINE: taker-buy объём (idx 8/9/10) — раньше зануляли
                    // через 8-арг конструктор. Нужен для CVD/aggressor order-flow (REST-fallback
                    // паритет с WS, где он уже приходит). См. BotMain BT-загрузчик.
                    int    nTr     = k.length() > 8  ? k.getInt(8)                       : 0;
                    double tbBase  = k.length() > 9  ? Double.parseDouble(k.getString(9))  : 0.0;
                    double tbQuote = k.length() > 10 ? Double.parseDouble(k.getString(10)) : 0.0;

                    // Sanity check — skip malformed candles
                    if (h < l || o <= 0 || c <= 0 || Double.isNaN(o) || Double.isNaN(c)) {
                        continue;
                    }
                    list.add(new com.bot.TradingCore.Candle(openT, o, h, l, c, v, qv, closeT, nTr, tbBase, tbQuote));
                }
                return list; // success — may be empty if Binance really returned []

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                lastEx = e;
            }
        }
        // All retries failed
        klineFetchErrors.incrementAndGet();
        LOG.severe("[fetchKlines] HARD FAIL " + symbol + " " + interval
                + " after 3 attempts: " + (lastEx != null ? lastEx.getMessage() : "unknown"));
        return null; // NULL = hard failure, distinguished from empty list
    }

    //  SERVER TIME SYNC
    // Binance требует `timestamp` в пределах `recvWindow` (5000ms по дефолту).
    // Дрейф локальных часов на 1-2 сек → массовые -1021 ошибки.
    // Offset обновляется при старте + раз в 30 минут из BotMain.
    //
    // Использование: в signed requests вместо System.currentTimeMillis() — binanceTimestamp().
    // Сейчас авто-торговля выключена, но правка превентивная на будущее.

    private volatile long serverTimeOffset = 0L;
    private volatile long lastTimeSync = 0L;

    public void syncServerTime() {
        try {
            long t0 = System.currentTimeMillis();
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/time"))
                            .timeout(Duration.ofSeconds(5))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            long t1 = System.currentTimeMillis();
            if (resp.statusCode() != 200) return;

            JSONObject j = new JSONObject(resp.body());
            long serverTime = j.getLong("serverTime");
            long roundTrip = t1 - t0;
            // Assume server time = middle of round trip
            long localMid = t0 + roundTrip / 2;
            long newOffset = serverTime - localMid;

            // Sanity: не принимаем offset > 10 сек (защита от мусорного ответа)
            if (Math.abs(newOffset) < 10_000L) {
                serverTimeOffset = newOffset;
                lastTimeSync = t1;
                LOG.info("[TimeSync] offset=" + newOffset + "ms rtt=" + roundTrip + "ms");
            }
        } catch (Exception e) {
            LOG.warning("[TimeSync] failed: " + e.getMessage());
        }
    }

    /** Использовать в signed requests вместо System.currentTimeMillis() */
    public long binanceTimestamp() {
        return System.currentTimeMillis() + serverTimeOffset;
    }

    public CompletableFuture<List<com.bot.TradingCore.Candle>> fetchKlinesAsync(String symbol, String interval, int limit) {
        return CompletableFuture.supplyAsync(() -> fetchKlinesDirect(symbol, interval, limit), fetchPool);
    }

    //  VOLUME DELTA

    public double getRawDelta(String symbol) { return deltaBuffer.getOrDefault(symbol, 0.0); }

    public double getNormalizedDelta(String symbol) {
        double d = deltaBuffer.getOrDefault(symbol, 0.0);
        if (d == 0.0) return 0.0;
        double absMax = deltaBuffer.values().stream().mapToDouble(Math::abs).max().orElse(1.0);
        return Math.max(-1.0, Math.min(1.0, d / (absMax + 1e-9)));
    }

    //  WEBSOCKET (aggTrade + bookTicker)

    public void connectWs(String pair) { connectWsInternal(pair); }

    private void connectWsInternal(String pair) {
        try {
            // Combined stream: aggTrade + bookTicker in ONE connection
            // This populates orderbookMap for real OBI analysis (was dead code before!)
            String streamUrl = "wss://fstream.binance.com/stream?streams="
                    + pair.toLowerCase() + "@aggTrade/"
                    + pair.toLowerCase() + "@bookTicker";

            http.newWebSocketBuilder()
                    .buildAsync(URI.create(streamUrl),
                            new WebSocket.Listener() {
                                @Override
                                public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                                    try {
                                        wsMessageCount.incrementAndGet();
                                        JSONObject wrapper = new JSONObject(data.toString());
                                        String stream = wrapper.optString("stream", "");
                                        JSONObject j = wrapper.optJSONObject("data");
                                        if (j == null) {
                                            ws.request(1); // [BUG-FIX] unlock backpressure even on empty frames
                                            return CompletableFuture.completedFuture(null);
                                        }

                                        if (stream.endsWith("@aggTrade")) {
                                            processAggTrade(pair, j);
                                        } else if (stream.endsWith("@bookTicker")) {
                                            processBookTicker(pair, j);
                                        }
                                    } catch (Exception ignored) {}
                                    ws.request(1); // [BUG-FIX] Java 11 WS backpressure — MUST request next frame
                                    return CompletableFuture.completedFuture(null);
                                }
                                @Override public void onError(WebSocket ws, Throwable error) {
                                    System.out.printf("[WS] %s error: %s%n", pair, error.getMessage());
                                    reconnectWs(pair);
                                }
                                @Override public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                                    System.out.printf("[WS] %s closed (code=%d)%n", pair, code);
                                    reconnectWs(pair); return CompletableFuture.completedFuture(null);
                                }
                            })
                    .thenAccept(ws -> {
                        wsMap.put(pair, ws);
                        wsReconnectDelay.put(pair, WS_INITIAL_DELAY_MS); // Reset backoff on success
                        reconnectingPairs.remove(pair); // Allow future reconnects
                    })
                    .exceptionally(ex -> { long delay = wsReconnectDelay.getOrDefault(pair, WS_INITIAL_DELAY_MS);
                        wsWatcher.schedule(() -> connectWsInternal(pair), delay, TimeUnit.MILLISECONDS);
                        wsReconnectDelay.put(pair, Math.min(delay * 2, WS_MAX_DELAY_MS)); return null; });
        } catch (Exception e) { reconnectWs(pair); }
    }

    // Guard against concurrent reconnects for the same pair
    private final Set<String> reconnectingPairs = ConcurrentHashMap.newKeySet();

    private void reconnectWs(String pair) {
        if (!reconnectingPairs.add(pair)) return; // already reconnecting
        wsLastReconnectMs = System.currentTimeMillis(); // [PATCH-WS-WARMUP]
        WebSocket old = wsMap.remove(pair);
        if (old != null) {
            try { old.sendClose(WebSocket.NORMAL_CLOSURE, "reconnecting"); } catch (Exception ignored) {}
        }
        // Gap-fill seed при переподключении WS.
        // Проблема (из implementation_plan §4.3): при reconnect liveM1Buffer НЕ очищался,
        // но WS-stream пропустил N тиков → MicroCandleBuilder имеет gap в данных.
        // Решение: сбрасываем буфер и делаем разовый REST-посев последних 60 баров
        // (вес=5 на один запрос — минимальная цена за корректность данных).
        // Это гарантирует что после reconnect бот не торгует на данных с дырой.
        liveM1Buffer.remove(pair); // сброс буфера — следующий getM1FromWs() сделает seed
        MicroCandleBuilder staleBuilder = microBuilders.remove(pair);
        if (staleBuilder != null) {
            // стартуем свежий builder — старый содержит gap
        }

        long delay = wsReconnectDelay.getOrDefault(pair, WS_INITIAL_DELAY_MS);
        wsWatcher.schedule(() -> connectWsInternal(pair), delay, TimeUnit.MILLISECONDS);
        wsReconnectDelay.put(pair, Math.min(delay * 2, WS_MAX_DELAY_MS));
    }

    /** Process bookTicker event — populates orderbookMap and computes OFV */
    private void processBookTicker(String pair, JSONObject j) {
        double bidQty = j.optDouble("B", 0);
        double askQty = j.optDouble("A", 0);
        if (bidQty > 0 || askQty > 0) {
            // Merge with existing depth5 snapshot if available
            OrderbookSnapshot existing = orderbookMap.get(pair);
            double bd5 = existing != null ? existing.bidDepth5 : bidQty;
            double ad5 = existing != null ? existing.askDepth5 : askQty;
            OrderbookSnapshot snap = new OrderbookSnapshot(bidQty, askQty, bd5, ad5, System.currentTimeMillis());
            orderbookMap.put(pair, snap);

            // [MODULE 2 v33] COMPUTE ORDER FLOW VELOCITY
            // Record this OBI tick in the rolling history and derive slope.
            double currentObi = snap.obi();
            Deque<double[]> hist = ofvHistory.computeIfAbsent(pair,
                    k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
            hist.addLast(new double[]{currentObi, System.currentTimeMillis()});
            while (hist.size() > OFV_HISTORY_SIZE) hist.removeFirst();

            // Need at least 4 points for a meaningful slope
            if (hist.size() >= 4) {
                // Simple linear regression slope: Σ(xi - x̄)(yi - ȳ) / Σ(xi - x̄)²
                // x = index (0..N-1), y = OBI value
                double[] pts = hist.stream().mapToDouble(d -> d[0]).toArray();
                int n = pts.length;
                double xMean = (n - 1) / 2.0;
                double yMean = 0;
                for (double p : pts) yMean += p;
                yMean /= n;
                double num = 0, den = 0;
                for (int i = 0; i < n; i++) {
                    double dx = i - xMean;
                    num += dx * (pts[i] - yMean);
                    den += dx * dx;
                }
                double slope = den > 1e-12 ? num / den : 0;
                ofvScoreMap.put(pair, slope);
            }
        }
    }

    private void processAggTrade(String pair, JSONObject j) {
        double price         = Double.parseDouble(j.getString("p"));
        double qty           = Double.parseDouble(j.getString("q"));
        long   ts            = j.getLong("T");
        boolean isBuyerMaker = j.getBoolean("m");
        double side          = !isBuyerMaker ? qty : -qty;

        // Standard 60s delta window
        deltaWindowStart.putIfAbsent(pair, ts);
        long age = ts - deltaWindowStart.get(pair);
        if (age > DELTA_WINDOW_MS) {
            deltaHistory.put(pair, deltaBuffer.getOrDefault(pair, 0.0));
            deltaBuffer.put(pair, side);
            deltaWindowStart.put(pair, ts);
        } else {
            deltaBuffer.merge(pair, side, Double::sum);
        }

        // VDA: 10s micro-window acceleration
        vdaWindowStart.putIfAbsent(pair, ts);
        long vdaAge = ts - vdaWindowStart.get(pair);
        if (vdaAge > VDA_WINDOW_MS) {
            double prevW = vdaCurrentBuf.getOrDefault(pair, 0.0);
            vdaPrevBuf.put(pair, prevW);
            vdaCurrentBuf.put(pair, side);
            vdaWindowStart.put(pair, ts);
            double cur = vdaCurrentBuf.getOrDefault(pair, 0.0);
            double prev = vdaPrevBuf.getOrDefault(pair, 0.0);
            if (Math.abs(cur) > 0 || Math.abs(prev) > 0) {
                double dir   = Math.signum(cur);
                double accel = Math.abs(prev) > 1e-9
                        ? Math.min(3.0, Math.abs(cur) / Math.abs(prev))
                        : (Math.abs(cur) > 0 ? 2.5 : 0.0);
                double score = dir * Math.min(1.0, (accel - 1.0) / 2.0);
                vdaScoreMap.put(pair, score);
                decisionEngine.setVDA(pair, score);
            }
        } else {
            vdaCurrentBuf.merge(pair, side, Double::sum);
        }

        // RT-CVD: real-time from aggTrade, resets on 15m boundary
        long candleBoundary = (ts / 900_000L) * 900_000L;
        if (candleBoundary > rtCvdReset.getOrDefault(pair, 0L)) {
            rtCvdBuy.put(pair, 0.0);
            rtCvdTotal.put(pair, 0.0);
            rtCvdReset.put(pair, candleBoundary);
        }
        rtCvdBuy.merge(pair, !isBuyerMaker ? qty : 0.0, Double::sum);
        rtCvdTotal.merge(pair, qty, Double::sum);
        double rtTotal = rtCvdTotal.getOrDefault(pair, 0.0);
        if (rtTotal > 0) {
            double rtBuys = rtCvdBuy.getOrDefault(pair, 0.0);
            double rtNorm = clamp((rtBuys - (rtTotal - rtBuys)) / rtTotal, -1.0, 1.0);
            decisionEngine.setCVD(pair, rtNorm);
        }

        // Tick history
        Deque<Double> dq = tickPriceDeque.computeIfAbsent(pair, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        dq.addLast(price); while (dq.size() > TICK_HISTORY) dq.pollFirst();
        Deque<Double> vq = tickVolumeDeque.computeIfAbsent(pair, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        vq.addLast(qty);   while (vq.size() > TICK_HISTORY) vq.pollFirst();
        lastTickPrice.put(pair, price);
        lastTickTime.put(pair, ts);

        // [v36-FIX Дыра1/2] Wire WS tick → liveM1Buffer (1m candle from aggTrade)
        Optional<com.bot.TradingCore.Candle> closedM1 =
                microBuilders.computeIfAbsent(pair, k -> new MicroCandleBuilder(60_000))
                        .addTick(ts, price, qty);
        closedM1.ifPresent(c -> liveM1Buffer.compute(pair, (k, existing) -> {
            List<com.bot.TradingCore.Candle> buf =
                    existing != null ? new ArrayList<>(existing) : new ArrayList<>();
            buf.add(c);
            if (buf.size() > LIVE_M1_BUFFER_SIZE) buf.subList(0, buf.size() - LIVE_M1_BUFFER_SIZE).clear();
            return buf;
        }));
    }

    // [v29+v30+v34] EARLY TICK — rewritten with exhaustion guard + VDA + correct conf floor
    // Category-aware velocity threshold: TOP coins (BTC/ETH) move slower in %


    /**
     * EARLY_TICK buffer flush — drains earlyTickBuffer atomically, sorts by
     * age-decayed probability, dispatches up to MAX_EARLY_PER_FLUSH per cycle.
     * Was 3 — too restrictive when 4-5 correlated pairs fire pre-pump together
     * (sector waves: L1, AI, meme). 5 keeps Telegram readable while not
     * silently dropping #4 and #5 of a synchronised move.
     */
    private static final int MAX_EARLY_PER_FLUSH = 5;

    // Signal age penalty configuration.
    // STALE_DROP_MS: any candidate older than this is discarded outright.
    // On 15m timeframe, >90s old means price likely moved enough that
    // the planned entry/SL are no longer valid.
    // STALE_DECAY_MS: effective probability decays linearly toward this point,
    // used for sorting — fresh signals win ties against older ones.
    private static final long EARLY_STALE_DROP_MS  = 90_000L;   // 90s hard cutoff
    private static final long EARLY_STALE_DECAY_MS = 60_000L;   // decay horizon for ranking

    //  HOT PAIR RESCAN — triggered from aggTrade handler
    //  when 30-tick price delta exceeds threshold.
    //  Runs a full processPair() in the background fetchPool.
    //  Result: if valid signal found → sent to Telegram immediately.
    //  Reduces worst-case detection latency from 60s → ≤5s.


    /** v61: register w/o tracking (tracking happens inside Dispatcher.dispatch). */


    //  FUNDING + OI

    private void refreshAllFundingRates() {
        if (rlIpBanned) return;
        try {
            // Bulk funding rates — 1 request for ALL pairs (weight ~10)
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder().uri(URI.create("https://fapi.binance.com/fapi/v1/premiumIndex"))
                            .timeout(Duration.ofSeconds(15)).GET().build(),
                    BINANCE_WEIGHT_PREMIUM_INDEX);
            if (resp == null) return;
            JSONArray arr = new JSONArray(resp.body());
            Map<String, Double> rates = new HashMap<>(arr.length());
            for (int i = 0; i < arr.length(); i++) { JSONObject o = arr.getJSONObject(i); rates.put(o.getString("symbol"), o.optDouble("lastFundingRate", 0)); }

            // Apply funding to ALL pairs from bulk response (no extra requests)
            for (String pair : cachedPairs) {
                double fr = rates.getOrDefault(pair, 0.0);
                decisionEngine.updateFundingOI(pair, fr, 0, 0, 0); // funding from bulk, OI below
            }

            // FIX: OI only for top 20 by volume (was 100 = 200 requests every 5 min)
            // 20 pairs × 1 request = 20 requests (was 200). Saves 900 weight per cycle.
            List<String> oiPairs = new ArrayList<>(cachedPairs);
            oiPairs.sort((a, b) -> Double.compare(
                    volume24hUSD.getOrDefault(b, 0.0),
                    volume24hUSD.getOrDefault(a, 0.0)));
            int oiLimit = Math.min(FUNDING_OI_TOP_N, oiPairs.size());

            for (int i = 0; i < oiLimit; i++) {
                if (rlIpBanned) break;
                try {
                    fetchAndUpdateOI(oiPairs.get(i), rates.getOrDefault(oiPairs.get(i), 0.0));
                    if (i % 5 == 4) Thread.sleep(300);
                }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                catch (Exception ignored) {}
            }
        } catch (Exception e) { LOG.warning("[FR] Error: " + e.getMessage()); }
    }

    private void fetchAndUpdateOI(String symbol, double fr) {
        try {
            // Only current OI, skip expensive oiHist (saves 1 request per pair)
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder().uri(URI.create("https://fapi.binance.com/fapi/v1/openInterest?symbol="+symbol))
                            .timeout(Duration.ofSeconds(6)).GET().build(),
                    BINANCE_WEIGHT_OPEN_INTEREST);
            if (resp == null) {
                decisionEngine.updateFundingOI(symbol, fr, 0, 0, 0);
                return;
            }
            JSONObject oiJ = new JSONObject(resp.body());
            double oi = oiJ.optDouble("openInterest", 0);
            decisionEngine.updateFundingOI(symbol, fr, oi, 0, 0);
        } catch (Exception e) { decisionEngine.updateFundingOI(symbol, fr, 0, 0, 0); }
    }

    //  REFRESH VOLUME + PAIRS

    private void refreshVolume24h() {
        if (rlIpBanned) return; // [v10.0]
        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder().uri(URI.create("https://fapi.binance.com/fapi/v1/ticker/24hr"))
                            .timeout(Duration.ofSeconds(15)).GET().build(),
                    BINANCE_WEIGHT_24H_TICKER);
            if (resp == null) return;
            JSONArray arr = new JSONArray(resp.body());
            for (int i = 0; i < arr.length(); i++) { JSONObject o = arr.getJSONObject(i); double v = o.optDouble("quoteVolume", 0); if (v > 0) volume24hUSD.put(o.getString("symbol"), v); }
        } catch (Exception e) { LOG.warning("[VOL24H] Error: " + e.getMessage()); }
    }
    // ─── Pair history probe (validate enough data for backtest) ──────────
    // Cache: avoid hitting Binance for the same pair repeatedly within a session.
    private final java.util.concurrent.ConcurrentHashMap<String, Boolean> historyOkCache
            = new java.util.concurrent.ConcurrentHashMap<>();
    private boolean hasEnoughHistory(String symbol) {
        Boolean cached = historyOkCache.get(symbol);
        if (cached != null) return cached;

        // [v86.91] 4h arm: primary=4h, HTF=1d (CRITICAL — without it 4h probed 1h/4h and
        // the startup-BT ran on the wrong TF). 4h needs 150 primary bars (~25d) + 40 1d HTF.
        String  primaryTfEnv = System.getenv().getOrDefault("PRIMARY_TF", "1h").trim();
        boolean is15m = "15m".equals(primaryTfEnv);
        boolean is30m = "30m".equals(primaryTfEnv);
        boolean is4h  = "4h".equals(primaryTfEnv);
        String  primaryTf  = is15m ? "15m" : is30m ? "30m" : is4h ? "4h" : "1h";
        String  htfTf      = is15m ? "1h"  : is30m ? "4h"  : is4h ? "1d" : "4h";
        int     primaryMin = is15m ? 250  : is30m ? 250 : is4h ? 150 : 200;   // STARTUP-BT needs ≥200/150 + warmup
        int     htfMin     = is15m ? 200  : is30m ? 150 : is4h ? 40  : 150;

        boolean ok;
        try {
            List<com.bot.TradingCore.Candle> probePrimary = fetchKlinesDirect(symbol, primaryTf, primaryMin);
            if (probePrimary == null || probePrimary.size() < primaryMin) {
                ok = false;
            } else {
                Thread.sleep(300L);
                List<com.bot.TradingCore.Candle> probeHtf = fetchKlinesDirect(symbol, htfTf, htfMin);
                ok = (probeHtf != null && probeHtf.size() >= htfMin);
            }
        } catch (Exception e) {
            ok = false;
        }

        historyOkCache.put(symbol, ok);
        if (!ok) {
            LOG.info("[PAIRS-FILTER] " + symbol + " skipped: insufficient "
                    + primaryTf + "/" + htfTf + " history for backtest");
        }
        return ok;
    }

    public Set<String> getTopSymbolsSet(int limit) {
        try {
            Set<String> binancePairs = getBinanceSymbolsFutures();


            binancePairs.removeIf(HARD_BLACKLIST::contains);

            Set<String> top = new LinkedHashSet<>();

            if (!volume24hUSD.isEmpty()) {
                List<String> byVolume = new ArrayList<>(binancePairs);
                byVolume.removeIf(p -> {
                    String sym = p.endsWith("USDT") ? p.substring(0, p.length() - 4) : p;
                    return STABLE.contains(sym);
                });
                byVolume.sort((a, b) -> Double.compare(
                        volume24hUSD.getOrDefault(b, 0.0),
                        volume24hUSD.getOrDefault(a, 0.0)));
                for (String pair : byVolume) {
                    if (volume24hUSD.getOrDefault(pair, 0.0) <= 0.0) break;
                    if (!hasEnoughHistory(pair)) continue;
                    top.add(pair);
                    if (top.size() >= limit) break;
                }
            }

            if (top.size() < limit) {
                JSONArray cg = new JSONArray(http.send(
                        HttpRequest.newBuilder().uri(URI.create("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1"))
                                .timeout(Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.ofString()).body());
                for (int i = 0; i < cg.length(); i++) {
                    String sym = cg.getJSONObject(i).getString("symbol").toUpperCase();
                    if (STABLE.contains(sym)) continue;
                    String pair = sym + "USDT";
                    if (binancePairs.contains(pair) && hasEnoughHistory(pair)) top.add(pair);
                    if (top.size() >= limit) break;
                }
            }

            if (top.size() < limit) {
                List<String> remaining = new ArrayList<>(binancePairs);
                remaining.sort((a, b) -> Double.compare(
                        volume24hUSD.getOrDefault(b, 0.0),
                        volume24hUSD.getOrDefault(a, 0.0)));
                for (String p : remaining) {
                    if (top.size() >= limit) break;
                    if (!hasEnoughHistory(p)) continue;
                    top.add(p);
                }
            }
            LOG.info("[PAIRS] Loaded " + top.size());
            return top;
        } catch (Exception e) {
            return new LinkedHashSet<>(Arrays.asList("BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","LINKUSDT"));
        }
    }

    public Set<String> getBinanceSymbolsFutures() {
        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder().uri(URI.create("https://fapi.binance.com/fapi/v1/exchangeInfo"))
                            .timeout(Duration.ofSeconds(10)).GET().build(),
                    BINANCE_WEIGHT_EXCHANGE_INFO);
            if (resp == null) return new HashSet<>(Arrays.asList("BTCUSDT","ETHUSDT","BNBUSDT"));
            JSONArray arr = new JSONObject(resp.body()).getJSONArray("symbols");
            Set<String> res = new HashSet<>();
            for (int i = 0; i < arr.length(); i++) { JSONObject s = arr.getJSONObject(i); if ("TRADING".equalsIgnoreCase(s.optString("status","TRADING")) && s.getString("symbol").endsWith("USDT")) res.add(s.getString("symbol")); }
            return res;
        } catch (Exception e) { return new HashSet<>(Arrays.asList("BTCUSDT","ETHUSDT","BNBUSDT")); }
    }

    // ════════════ [v86.96] NEW-LISTINGS CATCHER methods ════════════

    private static long nlEnvLong(String k, long d) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) return d;
        try { return Long.parseLong(v.trim()); } catch (Exception e) { return d; }
    }

    /**
     * [v86.96] Called once per scan cycle from BotMain.runCycle (gated by
     * BotMain.NEW_LISTING_CATCHER). Two jobs, both observation-only:
     *   (1) diff live exchangeInfo symbols vs the persisted known-set → on a
     *       genuinely new symbol: arm a recording window (anchored to its real
     *       onboard time), send a Telegram alert, add it to the known-set.
     *   (2) advance every active recording window: append fresh 1m klines +
     *       (live) funding + L1 spread to ./data, idempotently; retire expired windows.
     * Never trades, never touches the signal/calibrator path. Runs single-threaded
     * on mainSched, so the maps need no extra locking.
     */
    public void checkNewListings() {
        try {
            if (!nlLoaded) { nlLoad(); nlLoaded = true; }

            // One exchangeInfo fetch (weight 1) → symbol -> onboardDate(ms).
            Map<String, Long> onboard = nlFetchExchangeInfo();
            // Rate-limited/ban/parse failure → tiny or empty map. Never diff against that;
            // log EVERY skip (incl. empty) so a persistent feed failure is visible, not silent.
            if (onboard.size() < NL_SANITY_FLOOR) {
                LOG.warning("[NL] exchangeInfo returned " + onboard.size()
                        + " symbols (<" + NL_SANITY_FLOOR + ") — skipping diff this cycle");
                return;
            }

            long now = System.currentTimeMillis();
            long recentCut = now - NL_RECORD_HOURS * 3600_000L;
            // Cold start: first boot with no known-set → seed the whole universe, but only
            // ARM+ALERT symbols onboarded within the last NL_RECORD_HOURS, so an in-flight
            // listing isn't silently absorbed on the first deploy (no ~400-symbol alert storm).
            boolean coldStart = nlKnownSymbols.isEmpty();

            boolean dirty = false;
            int seeded = 0, armed = 0;
            for (Map.Entry<String, Long> e : onboard.entrySet()) {
                String sym = e.getKey();
                if (nlKnownSymbols.contains(sym)) continue;
                nlKnownSymbols.add(sym);
                seeded++;
                dirty = true;
                long onboardMs = e.getValue();
                // Steady state: every not-yet-known symbol is new. Cold start: only the
                // recently-onboarded ones (older symbols are just the seeded baseline).
                boolean isNew = !coldStart || (onboardMs > 0 && onboardMs >= recentCut);
                if (!isNew) continue;
                // Anchor the window to the real listing time when known → exactly first-N-hours.
                long until = (onboardMs > 0 ? onboardMs : now) + NL_RECORD_HOURS * 3600_000L;
                nlRecordingUntil.put(sym, until);
                armed++;
                String age = onboardMs > 0
                        ? String.format("%.1fч назад", (now - onboardMs) / 3600_000.0) : "время неизв.";
                try {
                    this.bot.sendMessageAsync(
                            "🆕 *NEW LISTING* `" + sym + "`\n"
                          + "onboard " + age + " · запись микроструктуры "
                          + NL_RECORD_HOURS + "ч → ./data\n"
                          + "_observation-only, бот не торгует_");
                } catch (Throwable ignored) {}
                LOG.info("[NL] NEW LISTING " + sym + " — recording until " + until);
            }
            if (coldStart)
                LOG.info("[NL] cold-start seeded " + seeded + " known symbols, armed " + armed + " recent");

            // (2) Advance every active recording window (including just-armed ones).
            List<String> done = new ArrayList<>();
            for (Map.Entry<String, Long> e : nlRecordingUntil.entrySet()) {
                if (now > e.getValue()) { done.add(e.getKey()); continue; }
                nlRecordOne(e.getKey());
            }
            for (String sym : done) {
                nlRecordingUntil.remove(sym);
                nlSeenRecords.removeIf(key -> key.startsWith(sym + "|"));
                dirty = true;
                LOG.info("[NL] recording window finished for " + sym);
            }
            if (dirty) nlSave();

            // One-time-per-boot confirmation (first healthy cycle) so the operator can SEE
            // the catcher is alive — without waiting days for the next real listing.
            if (!nlAnnounced) {
                nlAnnounced = true;
                LOG.info("[NL] active: tracking " + nlKnownSymbols.size() + " symbols, "
                        + nlRecordingUntil.size() + " recording");
                try {
                    this.bot.sendMessageAsync(
                            "🛰 *Ловец листингов активен* (" + com.bot.BotMain.BOT_VERSION + ")\n"
                          + "Отслеживаю " + nlKnownSymbols.size() + " символов"
                          + (nlRecordingUntil.size() > 0 ? (", пишу " + nlRecordingUntil.size()) : "")
                          + "\nХранилище: ./data" + (SUPABASE_ON ? " + Supabase ✅" : " (Supabase выкл)")
                          + "\nНовый листинг → 🆕 алерт + запись · _бот не торгует_");
                } catch (Throwable ignored) {}
            }
        } catch (Throwable t) {
            LOG.warning("[NL] checkNewListings: " + t.getMessage());
        }
    }

    /** Fetch exchangeInfo → map of TRADING USDT-M symbol → onboardDate(ms). Empty on failure. */
    private Map<String, Long> nlFetchExchangeInfo() {
        Map<String, Long> out = new HashMap<>();
        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/exchangeInfo"))
                            .timeout(Duration.ofSeconds(10)).GET().build(),
                    BINANCE_WEIGHT_EXCHANGE_INFO);
            if (resp == null) return out;
            JSONArray arr = new JSONObject(resp.body()).getJSONArray("symbols");
            for (int i = 0; i < arr.length(); i++) {
                JSONObject s = arr.getJSONObject(i);
                String sym = s.getString("symbol");
                if (!"TRADING".equalsIgnoreCase(s.optString("status", "TRADING"))) continue;
                if (!sym.endsWith("USDT")) continue;
                out.put(sym, s.optLong("onboardDate", 0L));
            }
        } catch (Exception e) {
            LOG.warning("[NL] exchangeInfo: " + e.getMessage());
            return new HashMap<>();
        }
        return out;
    }

    /** Fetch a fresh snapshot for one recording symbol and append new 1m bars (batched). */
    private void nlRecordOne(String sym) {
        try {
            // Bypass the kline cache — a brand-new symbol needs fresh history. 500 bars (~8h)
            // fully backfills a cold-start-recent listing and survives any skipped cycle.
            List<com.bot.TradingCore.Candle> kl = fetchKlinesDirect(sym, "1m", 500);
            if (kl == null || kl.size() < 2) return; // null=hard fail, tiny=no history yet

            double funding = nlFetchFunding(sym);
            double[] ba = nlFetchBookTicker(sym);
            boolean snapOk = ba != null;          // L1 spread actually fetched (not rate-limited)
            double bid = snapOk ? ba[0] : 0.0;
            double ask = snapOk ? ba[1] : 0.0;
            double[] dep = nlFetchDepth(sym);     // [v87.1] L10 orderbook depth (bid/ask qty sums)
            double bidDepth = dep != null ? dep[0] : 0.0;
            double askDepth = dep != null ? dep[1] : 0.0;
            double oi = nlFetchOI(sym);           // [v87.1] open interest
            long snap = System.currentTimeMillis();

            // Skip the last (forming) bar; collect only not-yet-recorded closed bars.
            // Funding/spread/depth/OI are point-in-time and CANNOT be backfilled, so attach them
            // (live=1) ONLY to bars that closed within NL_FRESH_MS of this snapshot; older
            // backfill bars carry klines only (live=0, microstructure fields = 0).
            List<String> rows = new ArrayList<>();
            List<String> keys = new ArrayList<>();
            JSONArray json = new JSONArray();
            for (int i = 0; i < kl.size() - 1; i++) {
                com.bot.TradingCore.Candle c = kl.get(i);
                String key = sym + "|" + c.openTime;
                if (nlSeenRecords.contains(key)) continue; // already recorded
                boolean live = snapOk && (snap - c.closeTime) <= NL_FRESH_MS;
                double fnd = live ? funding : 0.0, bd = live ? bid : 0.0, ak = live ? ask : 0.0;
                double bdep = live ? bidDepth : 0.0, adep = live ? askDepth : 0.0, oiv = live ? oi : 0.0;
                rows.add(nlMicroRow(sym, c, fnd, bd, ak, bdep, adep, oiv, snap, live));
                json.put(nlMicroJson(sym, c, fnd, bd, ak, bdep, adep, oiv, snap, live));
                keys.add(key);
            }
            // Mark bars as seen ONLY after a successful ./data write, so a failed append is retried.
            if (!rows.isEmpty() && nlAppendMicro(rows)) {
                nlSeenRecords.addAll(keys);
                nlPushSupabase(json); // best-effort mirror; ./data is the source of truth
            }
        } catch (Throwable t) {
            LOG.warning("[NL] record " + sym + ": " + t.getMessage());
        }
    }

    /** Build one pipe-delimited micro-record row. Symbols are Binance [A-Z0-9]+ so no escaping needed. */
    private static String nlMicroRow(String sym, com.bot.TradingCore.Candle c,
                                     double funding, double bid, double ask,
                                     double bidDepth, double askDepth, double oi, long snapMs, boolean live) {
        return sym + "|" + c.openTime + "|" + c.open + "|" + c.high + "|" + c.low
                + "|" + c.close + "|" + c.volume + "|" + c.quoteVolume + "|" + c.numberOfTrades
                + "|" + c.takerBuyBaseVolume + "|" + c.takerBuyQuoteVolume
                + "|" + funding + "|" + bid + "|" + ask
                + "|" + bidDepth + "|" + askDepth + "|" + oi
                + "|" + snapMs + "|" + (live ? 1 : 0);
    }

    /** Build one micro-record as JSON for the Supabase REST sink (keys = table columns). */
    private static JSONObject nlMicroJson(String sym, com.bot.TradingCore.Candle c,
                                          double funding, double bid, double ask,
                                          double bidDepth, double askDepth, double oi, long snapMs, boolean live) {
        return new JSONObject()
                .put("symbol", sym).put("open_time", c.openTime)
                .put("open", c.open).put("high", c.high).put("low", c.low).put("close", c.close)
                .put("volume", c.volume).put("quote_volume", c.quoteVolume)
                .put("trades", c.numberOfTrades)
                .put("taker_buy_base", c.takerBuyBaseVolume).put("taker_buy_quote", c.takerBuyQuoteVolume)
                .put("funding", funding).put("bid", bid).put("ask", ask)
                .put("bid_depth", bidDepth).put("ask_depth", askDepth).put("open_interest", oi)
                .put("snap_ms", snapMs).put("live", live);
    }

    /** Best-effort mirror of micro-rows to Supabase (PostgREST bulk insert, ignore-duplicates).
     *  ./data stays the source of truth — a Supabase failure NEVER loses data. No-op unless
     *  SUPABASE_URL + SUPABASE_KEY are set. Uses the bot's HttpClient directly (not the Binance
     *  rate-limiter — different host, no weight). */
    private void nlPushSupabase(JSONArray rows) {
        if (!SUPABASE_ON || rows.isEmpty()) return;
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(SUPABASE_URL + "/rest/v1/nl_micro?on_conflict=symbol,open_time"))
                    .timeout(Duration.ofSeconds(10))
                    .header("apikey", SUPABASE_KEY)
                    .header("Authorization", "Bearer " + SUPABASE_KEY)
                    .header("Content-Type", "application/json")
                    .header("Prefer", "resolution=ignore-duplicates,return=minimal")
                    .POST(HttpRequest.BodyPublishers.ofString(rows.toString()))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            int sc = resp.statusCode();
            if (sc < 200 || sc >= 300) {
                String b = resp.body() == null ? "" : resp.body();
                LOG.warning("[NL] Supabase insert HTTP " + sc + ": "
                        + b.substring(0, Math.min(200, b.length())));
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warning("[NL] Supabase push failed (./data still has it): " + e.getMessage());
        }
    }

    /** Single-symbol funding rate (premiumIndex, weight 1). 0.0 on any failure. */
    private double nlFetchFunding(String sym) {
        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=" + sym))
                            .timeout(Duration.ofSeconds(6)).GET().build(),
                    1); // single-symbol premiumIndex = weight 1
            if (resp == null) return 0.0;
            return new JSONObject(resp.body()).optDouble("lastFundingRate", 0.0);
        } catch (Exception e) { return 0.0; }
    }

    /** Single-symbol best bid/ask (bookTicker, weight 2). null on any failure. */
    private double[] nlFetchBookTicker(String sym) {
        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol=" + sym))
                            .timeout(Duration.ofSeconds(6)).GET().build(),
                    2); // single-symbol bookTicker = weight 2
            if (resp == null) return null;
            JSONObject j = new JSONObject(resp.body());
            double bid = j.optDouble("bidPrice", 0.0);
            double ask = j.optDouble("askPrice", 0.0);
            if (bid <= 0 || ask <= 0) return null;
            return new double[]{bid, ask};
        } catch (Exception e) { return null; }
    }

    /** [v87.1] Single-symbol L10 orderbook depth → [sum bid qty, sum ask qty] (weight 2). null on failure. */
    private double[] nlFetchDepth(String sym) {
        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/depth?symbol=" + sym + "&limit=10"))
                            .timeout(Duration.ofSeconds(6)).GET().build(),
                    2);
            if (resp == null) return null;
            JSONObject j = new JSONObject(resp.body());
            double bd = 0.0, ad = 0.0;
            JSONArray bids = j.optJSONArray("bids"), asks = j.optJSONArray("asks");
            if (bids != null) for (int i = 0; i < bids.length(); i++) bd += Double.parseDouble(bids.getJSONArray(i).getString(1));
            if (asks != null) for (int i = 0; i < asks.length(); i++) ad += Double.parseDouble(asks.getJSONArray(i).getString(1));
            return new double[]{bd, ad};
        } catch (Exception e) { return null; }
    }

    /** [v87.1] Single-symbol open interest (weight 1). 0.0 on failure. */
    private double nlFetchOI(String sym) {
        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/openInterest?symbol=" + sym))
                            .timeout(Duration.ofSeconds(6)).GET().build(),
                    1);
            if (resp == null) return 0.0;
            return new JSONObject(resp.body()).optDouble("openInterest", 0.0);
        } catch (Exception e) { return 0.0; }
    }

    /** Append micro-record rows (append-only, one file open per call). Returns true on success. */
    private synchronized boolean nlAppendMicro(List<String> rows) {
        try {
            java.io.File f = new java.io.File(NL_MICRO_FILE);
            java.io.File parent = f.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            boolean fresh = !f.exists() || f.length() == 0;
            try (java.io.PrintWriter pw = new java.io.PrintWriter(
                    new java.io.BufferedWriter(new java.io.FileWriter(f, true)))) {
                if (fresh) pw.println("# new_listing_micro v2 | symbol|openTime|open|high|low|close|"
                        + "volume|quoteVolume|trades|takerBuyBase|takerBuyQuote|funding|bid|ask|bidDepth|askDepth|oi|snapMs|live");
                for (String row : rows) pw.println(row);
            }
            return true;
        } catch (Throwable t) { LOG.warning("[NL] append: " + t.getMessage()); return false; }
    }

    /** Persist known-set + active recording windows. Atomic temp-then-rename so a crash
     *  mid-write can't truncate the source-of-truth known-set (→ false re-listing storm). */
    private synchronized void nlSave() {
        try {
            java.io.File f = new java.io.File(NL_KNOWN_FILE);
            java.io.File parent = f.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            java.io.File tmp = new java.io.File(NL_KNOWN_FILE + ".tmp");
            try (java.io.PrintWriter pw = new java.io.PrintWriter(
                    new java.io.BufferedWriter(new java.io.FileWriter(tmp)))) {
                pw.println("# known_symbols v1 | K|symbol  OR  R|symbol|recordUntilMs");
                for (String s : nlKnownSymbols) pw.println("K|" + s);
                for (Map.Entry<String, Long> e : nlRecordingUntil.entrySet())
                    pw.println("R|" + e.getKey() + "|" + e.getValue());
            }
            try {
                java.nio.file.Files.move(tmp.toPath(), f.toPath(),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception atomicUnsupported) {
                // Some filesystems reject ATOMIC_MOVE — fall back to a plain replace.
                java.nio.file.Files.move(tmp.toPath(), f.toPath(),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Throwable t) { LOG.warning("[NL] save: " + t.getMessage()); }
    }

    /** Load known-set + active recording windows; warm dedupe set for active windows only. */
    private synchronized void nlLoad() {
        try {
            java.io.File f = new java.io.File(NL_KNOWN_FILE);
            if (f.exists()) {
                try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.startsWith("#") || line.isBlank()) continue;
                        String[] p = line.split("\\|", -1);
                        if (p[0].equals("K") && p.length >= 2) nlKnownSymbols.add(p[1]);
                        else if (p[0].equals("R") && p.length >= 3) {
                            try { nlRecordingUntil.put(p[1], Long.parseLong(p[2])); } catch (Exception ignored) {}
                        }
                    }
                }
            }
        } catch (Throwable t) { LOG.warning("[NL] load known: " + t.getMessage()); }

        // Warm dedupe only for symbols still inside an active window — keeps the set bounded.
        try {
            java.io.File f = new java.io.File(NL_MICRO_FILE);
            if (f.exists() && !nlRecordingUntil.isEmpty()) {
                try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.startsWith("#") || line.isBlank()) continue;
                        int a = line.indexOf('|');
                        if (a <= 0) continue;
                        int b = line.indexOf('|', a + 1);
                        if (b <= a) continue;
                        String sym = line.substring(0, a);
                        if (!nlRecordingUntil.containsKey(sym)) continue;
                        nlSeenRecords.add(line.substring(0, b));
                    }
                }
            }
        } catch (Throwable t) { LOG.warning("[NL] load micro: " + t.getMessage()); }

        LOG.info("[NL] loaded known=" + nlKnownSymbols.size()
                + " activeRecordings=" + nlRecordingUntil.size()
                + " seenRecords=" + nlSeenRecords.size());
    }

    // ════════════ [v86.99] FUNDING-EXTREME SNAPSHOT methods ════════════

    /** [v86.99] Called once per cycle from BotMain.runCycle (gated by BotMain.FUNDING_SNAPSHOT).
     *  Snapshots symbols whose funding is extreme into ./data + Supabase. Observation-only —
     *  reads the funding cache the 15-min refresh already populates; no extra REST. */
    public void snapshotFundingExtremes() {
        try {
            long now = System.currentTimeMillis();
            long snapMinute = now / 60_000L;
            if (snapMinute == fsLastMinute) return; // at most one snapshot per minute
            fsLastMinute = snapMinute;

            List<String> rows = new ArrayList<>();
            JSONArray json = new JSONArray();
            for (String sym : cachedPairs) {
                com.bot.DecisionEngineMerged.FundingOIData d = decisionEngine.getFundingOI(sym);
                if (d == null) continue;
                boolean extreme = Math.abs(d.fundingRate) >= FS_EXTREME_THR || d.frPeakWarning || d.frTroughWarning;
                if (!extreme) continue;
                rows.add(sym + "|" + snapMinute + "|" + d.fundingRate + "|" + d.prevFundingRate
                        + "|" + d.fundingDelta + "|" + d.frAcceleration + "|" + d.openInterest
                        + "|" + (d.frPeakWarning ? 1 : 0) + "|" + (d.frTroughWarning ? 1 : 0) + "|" + now);
                json.put(new JSONObject()
                        .put("symbol", sym).put("snap_minute", snapMinute)
                        .put("funding_rate", d.fundingRate).put("prev_funding_rate", d.prevFundingRate)
                        .put("funding_delta", d.fundingDelta).put("fr_acceleration", d.frAcceleration)
                        .put("open_interest", d.openInterest)
                        .put("peak_warn", d.frPeakWarning).put("trough_warn", d.frTroughWarning)
                        .put("snap_ms", now));
            }
            if (!rows.isEmpty()) {
                appendCsv(FS_FILE, FS_HEADER, rows);
                sbPost("funding_snaps?on_conflict=symbol,snap_minute", json);
            }
            if (!fsAnnounced) {
                fsAnnounced = true;
                LOG.info("[FS] funding-extreme snapshot active (thr=" + FS_EXTREME_THR
                        + ", storage ./data" + (SUPABASE_ON ? " + Supabase" : "") + ")");
            }
        } catch (Throwable t) { LOG.warning("[FS] " + t.getMessage()); }
    }

    // ════════════ [v87.4] BREAKOUT TREND TRACKER (hypothesis #5, observation-only) ════════════
    // The ONLY strategy that survived offline validation: daily Donchian breakout + trailing exit =
    // the trend-following premium (regime-dependent, loses in bears, NOT all-weather). Forward-test it
    // leak-free: log FRESH daily breakouts; trailing-exit OUTCOMES computed OFFLINE from klines.
    private static final String TS_FILE = System.getenv().getOrDefault("TREND_SIGNALS_FILE", "./data/trend_signals.csv").trim();
    private static final String TS_HEADER = "# trend_signals v1 | symbol|signal_day|direction|entry|init_stop|atr|donchian_n|bar_close_ms|snap_ms";
    private static final int TS_DONCHIAN = 20;
    private static final int TS_PER_CYCLE = 6;     // coins per cycle (spread daily-kline REST load over ~1 sweep)
    private static final int    TS_MIN_BARS    = 40;                              // [v87.8] need >=40 daily bars (real track record, not a fresh listing)
    private static final double TS_MAX_ATR_PCT = tsEnvD("TS_MAX_ATR_PCT", 0.18);  // [v87.8] skip if ATR/price > 18% (fresh-listing chaos / garbage stop)
    // [v87.9] CRYPTO-only forward sample is enforced upstream via HARD_BLACKLIST (stripped from cachedPairs in
    // getTopSymbolsSet) — tokenized stocks/ETFs/metals never reach this tracker. Backtest confirms the edge is
    // crypto-driven (crypto-only CI [+1.37%,+5.98%] ALIVE vs non-crypto DEAD). To exclude a new equity perp, append
    // it to HARD_BLACKLIST (NOT here) — exact-match, no false positives, fixes the whole bot.
    private final java.util.Set<String> tsSeen = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private int tsIndex = 0;
    private volatile boolean tsAnnounced = false;
    private volatile boolean tsSeenLoaded = false;                                 // [v87.9] load persisted seen-keys once (survive restart)
    private static double tsEnvD(String k, double d) {
        try { String v = System.getenv(k); return (v == null || v.isBlank()) ? d : Double.parseDouble(v.trim()); }
        catch (Exception e) { return d; }
    }
    /** [v87.9] Repopulate tsSeen from the persisted CSV so a RESTART does not re-alert/re-log the same daily breakouts
     *  (Supabase upsert is already idempotent on the unique key; this stops duplicate Telegram alerts + CSV lines). */
    private void tsLoadSeen() {
        try {
            java.nio.file.Path p = java.nio.file.Paths.get(TS_FILE);
            if (!java.nio.file.Files.exists(p)) return;
            int n = 0;
            for (String line : java.nio.file.Files.readAllLines(p)) {
                if (line.isBlank() || line.startsWith("#")) continue;
                String[] f = line.split("\\|");
                if (f.length >= 3 && tsSeen.add(f[0] + "|" + f[1] + "|" + f[2])) n++;
            }
            if (n > 0) LOG.info("[TS] loaded " + n + " seen breakout key(s) from " + TS_FILE);
        } catch (Throwable t) { LOG.warning("[TS] seen-load: " + t.getMessage()); }
    }

    /** [v87.4] Called once per cycle from BotMain.runCycle (gated by BotMain.TREND_TRACK). Round-robins
     *  a few coins/cycle: detects a FRESH daily Donchian breakout on the last CLOSED bar and logs the
     *  entry + initial stop. Outcomes (trailing exit) computed at ANALYSIS time from klines — no in-bot resolver. */
    public void trackBreakoutSignals() {
        try {
            if (!tsSeenLoaded) { tsSeenLoaded = true; tsLoadSeen(); }   // [v87.9] survive restart: don't re-alert already-logged signals
            java.util.List<String> pairs = new java.util.ArrayList<>(cachedPairs);
            if (pairs.isEmpty()) return;
            if (!tsAnnounced) {
                tsAnnounced = true;
                LOG.info("[TS] breakout trend tracker active (Donchian" + TS_DONCHIAN
                        + ", storage ./data" + (SUPABASE_ON ? " + Supabase" : "") + ")");
            }
            java.util.List<String> rows = new java.util.ArrayList<>();
            java.util.List<String> alerts = new java.util.ArrayList<>();
            JSONArray json = new JSONArray();
            int batch = Math.min(TS_PER_CYCLE, pairs.size());
            for (int b = 0; b < batch; b++) {
                String sym = pairs.get(Math.floorMod(tsIndex++, pairs.size()));
                List<com.bot.TradingCore.Candle> kl = fetchKlinesDirect(sym, "1d", TS_DONCHIAN + 25);
                if (kl == null || kl.size() < TS_MIN_BARS) continue;       // [v87.8] require real track record (was TS_DONCHIAN+4)
                int last = kl.size() - 2;                 // last CLOSED daily bar (skip forming bar)
                com.bot.TradingCore.Candle cNow = kl.get(last), cPrev = kl.get(last - 1);
                double hhNow = tsDon(kl, last, TS_DONCHIAN, true),  llNow = tsDon(kl, last, TS_DONCHIAN, false);
                double hhPre = tsDon(kl, last - 1, TS_DONCHIAN, true), llPre = tsDon(kl, last - 1, TS_DONCHIAN, false);
                int dir = 0;
                if (cNow.close > hhNow && cPrev.close <= hhPre) dir = 1;        // fresh upside breakout
                else if (cNow.close < llNow && cPrev.close >= llPre) dir = -1;  // fresh downside breakout
                if (dir == 0) continue;
                double atr = tsAtr(kl, last, 14);
                double entry = cNow.close;
                // [v87.8] CLEANLINESS GATE — reject fresh-listing / garbage-volatility breakouts whose ATR is an
                // outsized fraction of price (e.g. ESPORTSUSDT: ATR 0.0886 on price 0.0255 → 1.5*ATR stop 521% away).
                // Such names never existed in the backtest universe (56 established coins) and pollute the forward sample.
                if (atr <= 0 || entry <= 0 || atr / entry > TS_MAX_ATR_PCT) continue;
                long day = cNow.closeTime / 86_400_000L;
                if (!tsSeen.add(sym + "|" + day + "|" + dir)) continue;         // log once per coin/day/dir
                double initStop = dir == 1 ? entry - 1.5 * atr : entry + 1.5 * atr;
                long snap = System.currentTimeMillis();
                rows.add(sym + "|" + day + "|" + dir + "|" + entry + "|" + initStop + "|" + atr
                        + "|" + TS_DONCHIAN + "|" + cNow.closeTime + "|" + snap);
                json.put(new JSONObject().put("symbol", sym).put("signal_day", day).put("direction", dir)
                        .put("entry", entry).put("init_stop", initStop).put("atr", atr)
                        .put("donchian_n", TS_DONCHIAN).put("bar_close_ms", cNow.closeTime).put("snap_ms", snap));
                alerts.add((dir == 1 ? "🟢 LONG  " : "🔴 SHORT ") + sym + " @ " + entry + "  (стоп " + initStop + ")");
            }
            if (!rows.isEmpty()) {
                appendCsv(TS_FILE, TS_HEADER, rows);
                sbPost("trend_signals?on_conflict=symbol,signal_day,direction", json);
                LOG.info("[TS] logged " + rows.size() + " breakout signal(s)");
                try {
                    bot.sendMessageAsync("📈 *BREAKOUT-сигналы* (стратегия v5, observation-only)\n"
                            + String.join("\n", alerts)
                            + "\n\nДневной Donchian-пробой · бот НЕ торгует · копим форвард-статистику");
                } catch (Throwable ignored) {}
            }
        } catch (Throwable t) { LOG.warning("[TS] " + t.getMessage()); }
    }

    /** Donchian extreme over the n bars ENDING just before index `end` (exclusive). high=true→max high, else→min low. */
    private double tsDon(List<com.bot.TradingCore.Candle> kl, int end, int n, boolean high) {
        double v = high ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        for (int i = Math.max(0, end - n); i < end; i++)
            v = high ? Math.max(v, kl.get(i).high) : Math.min(v, kl.get(i).low);
        return v;
    }
    /** Simple ATR over the n bars ending at index `end`. */
    private double tsAtr(List<com.bot.TradingCore.Candle> kl, int end, int n) {
        double sum = 0; int cnt = 0;
        for (int i = Math.max(1, end - n + 1); i <= end; i++) {
            com.bot.TradingCore.Candle c = kl.get(i), p = kl.get(i - 1);
            sum += Math.max(c.high - c.low, Math.max(Math.abs(c.high - p.close), Math.abs(c.low - p.close)));
            cnt++;
        }
        return cnt > 0 ? sum / cnt : 0.0;
    }

    /** [v87.6] Startup backtest of the NEW breakout strategy (daily Donchian-20 + 1.5ATR stop + 3ATR
     *  trailing, long+short) across the bot's coins → honest Telegram summary (per-year + survivorship
     *  caveat). Replaces the disabled candle backtest. Backtest ONLY — live green-light needs the forward test. */
    public void runBreakoutBacktest(com.bot.TelegramBotSender tg) {
        try {
            java.util.List<String> pairs = new java.util.ArrayList<>(cachedPairs);
            if (pairs.isEmpty()) return;
            int nTrades = 0, wins = 0; double sumRet = 0.0; int coins = 0;
            java.util.Map<String,double[]> byYear = new java.util.TreeMap<>(); // year -> [sumRet, n]
            for (String sym : pairs) {
                List<com.bot.TradingCore.Candle> kl = fetchKlinesDirect(sym, "1d", 1000);
                if (kl == null || kl.size() < 60) continue;
                coins++;
                int pos = 0; double entry = 0, stop = 0, ext = 0;
                for (int i = 21; i < kl.size(); i++) {
                    double a = tsAtr(kl, i, 14); if (a <= 0) continue;
                    com.bot.TradingCore.Candle c = kl.get(i), p = kl.get(i - 1);
                    if (pos == 0) {
                        double hh = tsDon(kl, i, 20, true), ll = tsDon(kl, i, 20, false);
                        double hhP = tsDon(kl, i - 1, 20, true), llP = tsDon(kl, i - 1, 20, false);
                        if (c.close > hh && p.close <= hhP) { pos = 1; entry = c.close; stop = entry - 1.5 * a; ext = entry; }
                        else if (c.close < ll && p.close >= llP) { pos = -1; entry = c.close; stop = entry + 1.5 * a; ext = entry; }
                    } else if (pos == 1) {
                        ext = Math.max(ext, c.close); stop = Math.max(stop, ext - 3 * a);
                        if (c.close <= stop) { double r = (c.close - entry) / entry - 0.001; nTrades++; if (r > 0) wins++; sumRet += r; btYear(c, r, byYear); pos = 0; }
                    } else {
                        ext = Math.min(ext, c.close); stop = Math.min(stop, ext + 3 * a);
                        if (c.close >= stop) { double r = (entry - c.close) / entry - 0.001; nTrades++; if (r > 0) wins++; sumRet += r; btYear(c, r, byYear); pos = 0; }
                    }
                }
            }
            if (nTrades == 0) { LOG.info("[BREAKOUT-BT] no trades"); return; }
            StringBuilder yb = new StringBuilder();
            for (java.util.Map.Entry<String,double[]> e : byYear.entrySet())
                yb.append(e.getKey()).append(" ").append(String.format("%+.1f%%", e.getValue()[0] / e.getValue()[1] * 100)).append(" · ");
            String msg = String.format(
                    "📊 *BREAKOUT-стратегия v5 — бэктест* (дневки, %d монет)%n"
                  + "Сделок: %d · Винрейт: %.0f%%%n"
                  + "Средн. на сделку: %+.2f%% (после костов)%n"
                  + "По годам (средн/сделку): %s%n%n"
                  + "⚠️ Это БЭКТЕСТ — заражён survivorship (как и тот +732%%). НЕ реальная прибыль.%n"
                  + "Зелёный свет на LIVE даёт ТОЛЬКО форвард-тест (≥100 живых сделок, CI>0), не бэктест.",
                    coins, nTrades, 100.0 * wins / nTrades, 100.0 * sumRet / nTrades, yb.toString());
            tg.sendMessageAsync(msg);
            LOG.info("[BREAKOUT-BT] " + nTrades + " trades wr=" + (100 * wins / nTrades) + "% avg=" + String.format("%.2f", 100.0 * sumRet / nTrades));
        } catch (Throwable t) { LOG.warning("[BREAKOUT-BT] " + t.getMessage()); }
    }
    private void btYear(com.bot.TradingCore.Candle c, double r, java.util.Map<String,double[]> byYear) {
        String y = String.valueOf(java.time.Instant.ofEpochMilli(c.closeTime).atZone(java.time.ZoneOffset.UTC).getYear());
        byYear.computeIfAbsent(y, k -> new double[2]); byYear.get(y)[0] += r; byYear.get(y)[1] += 1;
    }

    /** Generic append-only CSV writer (mkdirs + header on fresh file). Shared by funding/liq capture. */
    private synchronized void appendCsv(String file, String header, java.util.List<String> rows) {
        try {
            java.io.File f = new java.io.File(file);
            java.io.File parent = f.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            boolean fresh = !f.exists() || f.length() == 0;
            try (java.io.PrintWriter pw = new java.io.PrintWriter(
                    new java.io.BufferedWriter(new java.io.FileWriter(f, true)))) {
                if (fresh) pw.println(header);
                for (String r : rows) pw.println(r);
            }
        } catch (Throwable t) { LOG.warning("[CSV] append " + file + ": " + t.getMessage()); }
    }

    /** Generic best-effort Supabase PostgREST insert (ignore-duplicates). No-op if Supabase off.
     *  pathQuery e.g. "funding_snaps?on_conflict=symbol,snap_minute". ./data stays source of truth. */
    private void sbPost(String pathQuery, JSONArray rows) {
        if (!SUPABASE_ON || rows.isEmpty()) return;
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(SUPABASE_URL + "/rest/v1/" + pathQuery))
                    .timeout(Duration.ofSeconds(10))
                    .header("apikey", SUPABASE_KEY)
                    .header("Authorization", "Bearer " + SUPABASE_KEY)
                    .header("Content-Type", "application/json")
                    .header("Prefer", "resolution=ignore-duplicates,return=minimal")
                    .POST(HttpRequest.BodyPublishers.ofString(rows.toString()))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            int sc = resp.statusCode();
            if (sc < 200 || sc >= 300) {
                String b = resp.body() == null ? "" : resp.body();
                LOG.warning("[SB] " + pathQuery + " HTTP " + sc + ": " + b.substring(0, Math.min(160, b.length())));
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            LOG.warning("[SB] " + pathQuery + " failed (./data still has it): " + ex.getMessage());
        }
    }

    /** [v87.0] Called once per cycle from BotMain.runCycle (gated by BotMain.LIQ_CAPTURE).
     *  Drains buffered liquidation events (filled by the WS stream) → ./data + Supabase. */
    public void flushLiquidations() {
        try {
            if (liqBuffer.isEmpty()) {
                if (!lqAnnounced) {
                    lqAnnounced = true;
                    LOG.info("[LQ] liquidation capture active (storage ./data" + (SUPABASE_ON ? " + Supabase" : "") + ")");
                }
                return;
            }
            List<String> rows = new ArrayList<>();
            JSONArray json = new JSONArray();
            org.json.JSONObject e;
            int n = 0;
            while (n < LQ_FLUSH_MAX && (e = liqBuffer.poll()) != null) {
                n++;
                rows.add(e.getString("symbol") + "|" + e.getLong("order_time") + "|" + e.optString("side")
                        + "|" + e.getDouble("price") + "|" + e.getDouble("qty") + "|" + e.getDouble("notional")
                        + "|" + e.getLong("snap_ms"));
                json.put(e);
            }
            if (!rows.isEmpty()) {
                appendCsv(LQ_FILE, LQ_HEADER, rows);
                sbPost("liq_events?on_conflict=symbol,order_time,side,price,qty", json);
            }
            if (!lqAnnounced) {
                lqAnnounced = true;
                LOG.info("[LQ] liquidation capture active (storage ./data" + (SUPABASE_ON ? " + Supabase" : "") + ")");
            }
            LOG.info("[LQ] flushed " + n + " liquidations");
        } catch (Throwable t) { LOG.warning("[LQ] " + t.getMessage()); }
    }

    /** [v87.2] Диагностика захвата ликвидаций для heartbeat: сырых событий получено (любой размер),
     *  возраст последнего, размер буфера, статус WS. Если raw=0 → поток ликвидаций до бота НЕ доходит. */
    public String getLiqCaptureDiag() {
        long raw = liqRawCount.get();
        long last = liqLastEventMs;
        String age = (last == 0) ? "never" : ((System.currentTimeMillis() - last) / 1000L) + "s";
        return "raw=" + raw + " last=" + age + " buf=" + liqBuffer.size() + " ws=" + (liqWebSocket != null ? "up" : "down");
    }

    // DYNAMIC COIN CATEGORIZATION
    // Old: hardcoded switch → missed new TOP coins, couldn't adapt.
    // New: volume-based dynamic classification + known-list seed.
    //
    // Logic:
    //   1. Known TOP coins (BTC, ETH, BNB, SOL...) → always TOP
    //   2. Known MEME coins (DOGE, SHIB, PEPE...) → always MEME
    //   3. Unknown coins: classified by 24h volume bracket
    //      - >$200M/24h → TOP (institutional-grade liquidity)
    //      - <$200M && name matches meme patterns → MEME
    //      - default → ALT
    //
    // This is a HYBRID approach: known coins use stable labels,
    // new coins get auto-classified by market behavior.

    // Known seeds — these NEVER change category regardless of volume
    private static final java.util.Set<String> KNOWN_TOP = java.util.Set.of(
            "BTC","ETH","BNB","SOL","XRP","ADA","AVAX","DOT","LINK",
            "MATIC","LTC","ATOM","UNI","AAVE","TON","TRX","NEAR","APT",
            "SUI","DYDX","ARB","OP","FIL","ICP","HBAR","VET","ALGO",
            "FTM","INJ","SEI","TIA","JUP","RENDER","STX","MKR","RUNE"
    );
    private static final java.util.Set<String> KNOWN_MEME = java.util.Set.of(
            "DOGE","SHIB","PEPE","FLOKI","WIF","BONK","MEME","NEIRO",
            "POPCAT","COW","MOG","BRETT","TURBO","BABYDOGE","PEOPLE",
            "ELON","SATS","ORDI","RATS","MYRO","BOME","SLERF","MEW",
            "TRUMP","WEN","DEGEN"
    );
    // Meme pattern keywords — auto-detect new meme coins
    private static final java.util.Set<String> MEME_KEYWORDS = java.util.Set.of(
            "DOG","CAT","INU","MOON","PEPE","DOGE","SHIB","FROG",
            "BABY","ELON","MEME","WOJAK","CHAD","TURBO","FLOKI",
            "APE","HAMSTER","PIG","COW","PENGUIN","PANDA","PORK"
    );

    private com.bot.DecisionEngineMerged.CoinCategory categorizePair(String pair) {
        String sym = pair.endsWith("USDT") ? pair.substring(0, pair.length()-4) : pair;
        String upper = sym.toUpperCase();

        // 1. Known lists (stable, fast)
        if (KNOWN_TOP.contains(upper))  return com.bot.DecisionEngineMerged.CoinCategory.TOP;
        if (KNOWN_MEME.contains(upper)) return com.bot.DecisionEngineMerged.CoinCategory.MEME;

        // 2. Volume-based dynamic classification
        double vol24h = volume24hUSD.getOrDefault(pair, 0.0);

        // >$200M/24h → institutional liquidity → TOP behavior
        if (vol24h >= 200_000_000) return com.bot.DecisionEngineMerged.CoinCategory.TOP;

        // 3. Meme keyword detection (auto-catches new meme coins)
        for (String keyword : MEME_KEYWORDS) {
            if (upper.contains(keyword)) return com.bot.DecisionEngineMerged.CoinCategory.MEME;
        }

        // 4. Default: ALT
        return com.bot.DecisionEngineMerged.CoinCategory.ALT;
    }

    // [v86.36] LIQUIDITY-TIER FILTER — edge lever #1 (cost-cut experiment).
    // Honest backtest (v86.34) showed the strategy's GROSS edge (~+0.35%/trade) is real,
    // but ~70% of it is eaten by slippage on illiquid coins (per-side TOP 0.025% /
    // ALT 0.075% / MEME 0.20%). Restricting scan+backtest to liquid coins recovers that
    // cost drag. This is a PER-COIN liquidity gate (each market judged on its own trend) —
    // NOT a BTC-regime filter; the bot does not become BTC-dependent.
    //   TRADE_TIER=TOP    → only top-tier liquid (cheapest slippage; default for the test)
    //   TRADE_TIER=TOPALT → top + alt (drop only the worst-slippage meme tier)
    //   TRADE_TIER=ALL    → legacy (everything) — set this to restore old behavior
    // Single source of truth: used by live scan (selectPairsForScan), startup backtest
    // (getScanUniverseSnapshot) and self-validator (getTopPairsForForecast) so the
    // backtested universe == the live-traded universe.
    private static final String TRADE_TIER =
            System.getenv().getOrDefault("TRADE_TIER", "TOPALT").trim().toUpperCase();  // [v86.43] TOP→TOPALT: widen universe (add ALT, drop only MEME) for more signal flow + bigger/diversified walk-forward sample. Signals were ~0/day on 15 TOP coins. Revert: TRADE_TIER=TOP.
    /** [v86.44] Single source of truth for the active tier — used by the startup-BT banner
     *  (BotMain had its own getOrDefault default "TOP", which displayed the WRONG tier after
     *  the v86.43 default flip to TOPALT while the actual filter ran TOPALT). */
    public String getTradeTier() { return TRADE_TIER; }
    private boolean passesTradeTier(String pair) {
        if ("ALL".equals(TRADE_TIER)) return true;
        com.bot.DecisionEngineMerged.CoinCategory c = categorizePair(pair);
        if ("TOPALT".equals(TRADE_TIER))
            return c != com.bot.DecisionEngineMerged.CoinCategory.MEME;
        return c == com.bot.DecisionEngineMerged.CoinCategory.TOP;   // default TOP
    }

    private com.bot.DecisionEngineMerged.TradeIdea rebuildIdea(com.bot.DecisionEngineMerged.TradeIdea src, double p, List<String> f) {
        // Передаём адаптивные TP-множители из оригинала — они не должны теряться при перестройке
        com.bot.DecisionEngineMerged.TradeIdea ni = new com.bot.DecisionEngineMerged.TradeIdea(
                src.symbol, src.side, src.price, src.stop, src.take, src.rr, p, f,
                src.fundingRate, src.fundingDelta, src.oiChange, src.htfBias, src.category,
                src.forecast,
                src.tp1Mult, src.tp2Mult, src.tp3Mult);
        // [HOLE-1 REGRESSION FIX 2026-05-08] Сохраняем executorSizeMultiplier
        // (volatile поле). Без этого rebuildIdea создавал новый TradeIdea с
        // дефолтным 1.0 и затирал результат предыдущего getPositionSizeUsdt /
        // setExecutorSizeMultiplier — все extra-множители (survival/cautious/
        // quality/corr) терялись по дороге к Executor'у.
        double srcMult = src.getExecutorSizeMultiplier();
        if (srcMult > 0 && srcMult != 1.0) {
            ni.setExecutorSizeMultiplier(srcMult);
        }
        // Также сохраняем robustAtrPct override если был установлен
        double srcAtr = src.getRobustAtrPct();
        if (srcAtr > 0 && Math.abs(srcAtr - src.robustAtrPct) > 1e-9) {
            ni.setRobustAtrPct(srcAtr);
        }
        // [B1 2026-05-08] Propagate the direction-correct agreeing-cluster count
        // through rebuildIdea — otherwise every penalty/boost layer would reset
        // it to -1 and Dispatcher would fall back to flag substring counting.
        int srcClusters = src.getAgreeingClusters();
        if (srcClusters >= 0) {
            ni.setAgreeingClusters(srcClusters);
        }
        // [v86.60 PHASE-0] возраст тренда — иначе 12+ rebuild-точек молча затирали бы тег
        int srcAge = src.getTrendAge4h();
        if (srcAge >= 0) {
            ni.setTrendAge4h(srcAge);
        }
        return ni;
    }

    //  [ДЫРА №1] CVD — Cumulative Volume Delta
    //  takerBuyBaseVolume приходит в каждом kline — уже в Candle.
    //  Накапливаем за CVD_LOOKBACK_1M свечей (по умолчанию 90×1m).
    //  Нормализуем: [-1..+1], где +1 = 100% объём — покупки, -1 = 100% продажи.

    public double computeAndStoreCVD(String pair, List<com.bot.TradingCore.Candle> m1) {
        if (m1 == null || m1.size() < 5) return 0.0;
        int start = Math.max(0, m1.size() - CVD_LOOKBACK_1M);
        double cvd = 0, totalVol = 0;
        for (int i = start; i < m1.size(); i++) {
            com.bot.TradingCore.Candle c = m1.get(i);
            double buyVol  = c.takerBuyBaseVolume;
            double sellVol = c.volume - buyVol;
            cvd      += (buyVol - sellVol);
            totalVol += c.volume;
        }
        // Нормализуем на суммарный объём → [-1..+1]
        double normalized = totalVol > 0 ? clamp(cvd / totalVol, -1.0, 1.0) : 0.0;
        cvdMap.put(pair, normalized);
        return normalized;
    }

    public double getCVD(String pair) {
        return cvdMap.getOrDefault(pair, 0.0);
    }

    //  [ДЫРА №4] SESSION WEIGHT — рыночные сессии по UTC
    //  Сигналы в NY/London несут в 1.5× больше веса чем сигналы
    //  в 3 ночи UTC (азиатская сессия с ложными пробоями).

    private static double getSessionWeight() {
        int h = java.time.ZonedDateTime.now(java.time.ZoneId.of("UTC")).getHour();
        // NY открытие + London/NY overlap — лучшее качество сигналов
        if (h >= 13 && h <= 16) return 1.20; // NY open (13-16 UTC) — максимальный объём
        if (h >= 7  && h <= 10) return 1.10; // London open (07-10 UTC) — strong moves
        if (h >= 10 && h <= 13) return 1.00; // London/NY overlap — норма
        if (h >= 17 && h <= 22) return 0.90; // NY afternoon — затихает
        // Азия / ночь — ложные пробои, низкая ликвидность
        return 0.70;                          // 00-07, 22-24 UTC
    }

    //  [ДЫРА №2] LIQUIDATION HEATMAP
    //  Подписка на публичный WebSocket поток Binance: !forceOrder@arr
    //  Собираем ликвидации в ценовые уровни (bucket = 0.1% от цены).
    //  getLiquidationScore() возвращает 0..1 — насколько сильный
    //  магнит ликвидаций находится вблизи текущей цены.

    private void connectLiquidationStream() {
        try {
            (LIQ_DEDICATED_WS ? liqHttp : http).newWebSocketBuilder()    // [v87.8] dedicated client so liq callbacks aren't starved by REST
                    .buildAsync(URI.create("wss://fstream.binance.com/ws"),    // [v88.1] base endpoint; subscribe via message below — the '!' in /ws/!forceOrder@arr silently broke auto-subscribe (ws=up raw=0 forever)
                            new WebSocket.Listener() {
                                private final StringBuilder buf = new StringBuilder();

                                @Override
                                public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                                    buf.append(data);
                                    if (last) {
                                        try { processLiquidationEvent(new org.json.JSONObject(buf.toString())); }
                                        catch (Exception ignored) {}
                                        buf.setLength(0);
                                    }
                                    ws.request(1);
                                    return CompletableFuture.completedFuture(null);
                                }

                                @Override
                                public void onError(WebSocket ws, Throwable err) {
                                    liqWebSocket = null;
                                    udsExecutor.schedule(SignalSender.this::connectLiquidationStream, 15, TimeUnit.SECONDS);
                                }

                                @Override
                                public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                                    liqWebSocket = null;
                                    udsExecutor.schedule(SignalSender.this::connectLiquidationStream, 5, TimeUnit.SECONDS);
                                    return CompletableFuture.completedFuture(null);
                                }
                            })
                    .thenAccept(ws -> {
                        liqWebSocket = ws;
                        // [v88.1] Explicit SUBSCRIBE — auto-subscribe via the '!'-prefixed URL path never delivered (ws=up raw=0
                        // forever). Here '!' lives inside the JSON param, not the URL. The ack {"result":null,"id":1} arrives as a
                        // text frame and is harmlessly ignored by processLiquidationEvent (no "o" field). Reconnects re-subscribe.
                        try { ws.sendText("{\"method\":\"SUBSCRIBE\",\"params\":[\"!forceOrder@arr\"],\"id\":1}", true); }
                        catch (Throwable t) { LOG.warning("[LIQ] subscribe send: " + t.getMessage()); }
                        LOG.info("[LIQ] ✅ Liquidation stream connected + SUBSCRIBE sent");
                    })
                    .exceptionally(ex -> {
                        LOG.warning("[LIQ] Connect failed: " + ex.getMessage());
                        udsExecutor.schedule(this::connectLiquidationStream, 30, TimeUnit.SECONDS);
                        return null;
                    });
        } catch (Exception e) {
            LOG.warning("[LIQ] Error: " + e.getMessage());
        }
    }

    private void processLiquidationEvent(org.json.JSONObject event) {
        try {
            org.json.JSONObject o = event.optJSONObject("o");
            if (o == null) return;
            liqRawCount.incrementAndGet();               // [v87.2] считаем КАЖДОЕ событие (любой размер) до фильтра
            liqLastEventMs = System.currentTimeMillis();
            String symbol   = o.optString("s");
            double avgPrice = o.optDouble("ap", 0);
            double qty      = o.optDouble("q", 0);
            String side     = o.optString("S"); // BUY = short был ликвидирован, SELL = long был
            if (avgPrice <= 0 || qty <= 0) return;

            double notional = avgPrice * qty;
            if (notional < LIQ_MIN_NOTIONAL) return; // игнорируем мелкие

            // [v87.0] Observation-only capture for hypothesis #3 (forced-flow reversion).
            // Buffer here (WS thread); BotMain.runCycle drains+batch-writes via flushLiquidations().
            if (com.bot.BotMain.LIQ_CAPTURE && liqBuffer.size() < LQ_BUFFER_MAX) {
                long lqT = o.optLong("T", event.optLong("E", System.currentTimeMillis()));
                liqBuffer.add(new org.json.JSONObject()
                        .put("symbol", symbol).put("order_time", lqT).put("side", side)
                        .put("price", avgPrice).put("qty", qty).put("notional", notional)
                        .put("snap_ms", System.currentTimeMillis()));
            }

            // Ценовой bucket: округляем до 0.1% от цены
            double bucketSize = avgPrice * 0.001;
            double bucket = Math.round(avgPrice / bucketSize) * bucketSize;

            java.util.NavigableMap<Double, Double> heatmap = liqHeatmap
                    .computeIfAbsent(symbol, k -> new java.util.concurrent.ConcurrentSkipListMap<>());
            heatmap.merge(bucket, notional, Double::sum);
            liqTimestamps.put(symbol + "_" + bucket, System.currentTimeMillis());

            // Удаляем протухшие уровни (> 30 минут)
            long now = System.currentTimeMillis();
            heatmap.entrySet().removeIf(e ->
                    now - liqTimestamps.getOrDefault(symbol + "_" + e.getKey(), 0L) > LIQ_DECAY_MS);
        } catch (Exception ignored) {}
    }

    /**
     * Возвращает "магнетизм" ликвидаций около текущей цены.
     * 0.0 = нет значимых ликвидаций рядом.
     * 1.0 = крупный скопившийся пул (>$5M) в пределах 1×ATR.
     * Если цена движется К этому уровню → усиливает сигнал.
     * Если цена движется ОТ него → ослабляет.
     */
    public double getLiquidationScore(String pair, double price, double atr,
                                      com.bot.TradingCore.Side side) {
        java.util.NavigableMap<Double, Double> heatmap = liqHeatmap.get(pair);
        if (heatmap == null || heatmap.isEmpty()) return 0.0;
        double range = atr * 1.5;
        double liqAbove = heatmap.subMap(price, price + range)
                .values().stream().mapToDouble(Double::doubleValue).sum();
        double liqBelow = heatmap.subMap(price - range, price)
                .values().stream().mapToDouble(Double::doubleValue).sum();
        // LONG сигнал усиливается если ликвидации SHORT выше (цена пойдёт их собирать)
        // SHORT сигнал усиливается если ликвидации LONG ниже
        double relevant = (side == com.bot.TradingCore.Side.LONG) ? liqAbove : liqBelow;
        return clamp(relevant / 5_000_000.0, 0.0, 1.0); // нормализуем на $5M
    }

    private void logCycleStats() {
        long total = totalFetches.get(), hits = cacheHits.get();
        if (total > 0 && total % 500 == 0) {
            // Include rejectedFetches count — non-zero means fetchPool is overloaded.
            long rejected = rejectedFetches.get();
            System.out.printf("[Stats] cache=%.1f%% early=%d liq=%d corr=%d stale=%d profit=%d e=%d opt=%d vpoc=%d fin=%d isc=%d rej=%d q=+%.0f ws=%.0f%% msgs=%d bal=$%.2f%n",
                    100.0*hits/total, earlySignals.get(), blockedLiq.get(), blockedCorr.get(),
                    blockedStaleRt.get(), blockedProfit.get(), blockedEarlyConf.get(),
                    blockedOptConf.get(), blockedVpoc.get(), blockedFinalConf.get(),
                    blockedIsc.get(), rejected, cycleQualityPenalty, lastCycleWsCoverage * 100.0,
                    wsMessageCount.get(), accountBalance);

            // Alert loudly when tasks are being dropped — indicates
            // TOP_N too high for current pool, or sustained network slowness.
            if (rejected > 0) {
                LOG.warning("[WARN] fetchPool dropped " + rejected + " tasks (queue saturated). "
                        + "Consider lowering TOP_N or increasing poolSize.");
            }
        }
    }

    //  ACCESSORS

    public double getAtr(String symbol) {
        CachedCandles cc = candleCache.get(symbol + "_15m");
        if (cc == null || cc.candles.isEmpty()) return 0;
        // Use robustAtr — prevents underestimation during consolidation
        return com.bot.DecisionEngineMerged.robustAtr(cc.candles, 14);
    }

    /**
     * Returns the noise score (wick/body ratio) for a symbol from cached 15m candles.
     * Used by EARLY_TICK gate to raise confidence threshold for noisy coins.
     */
    public double getNoiseScore(String symbol) {
        CachedCandles cc = candleCache.get(symbol + "_15m");
        if (cc == null || cc.candles.isEmpty()) return 1.5;
        return com.bot.DecisionEngineMerged.computeNoiseScore(cc.candles, 14);
    }

    public double getAccountBalance() { return accountBalance; }
    // [v36-FIX Дыра3] Accessors for OrderExecutor (авто-исполнение)
    public String getApiKey()    { return API_KEY; }
    public String getSecretKey() { return API_SECRET; }

    /**
     * [MODULE 4 v33] Returns top N pairs by 24h USD volume for Advance Forecast scanning.
     * Filtered by minimum volume ($30M+) and sorted descending by volume.
     * Used by BotMain.runAdvanceForecast() to know which pairs to analyse.
     */
    public List<String> getTopPairsForForecast(int n) {
        if (volume24hUSD.isEmpty()) return List.of();
        // [FIX] Filter garbage coins and non-ASCII symbols from AFC scan universe.
        // Previously UAIUSDT, METUSDT etc. caused REST fetches every 2 minutes.
        return volume24hUSD.entrySet().stream()
                .filter(e -> e.getValue() >= MIN_VOL_ALT_USD)
                .filter(e -> !isBlocklisted(e.getKey()))
                .filter(e -> passesTradeTier(e.getKey()))   // [v86.36] liquidity-tier filter
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(n)
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toList());
    }
    public int  getActiveWsCount()   { return wsMap.size(); }
    public boolean isUdsConnected()  { return udsWebSocket != null; }
    public double getCycleQualityPenalty() { return cycleQualityPenalty; }
    public double getLastCycleStaleRatio() { return lastCycleStaleRatio; }
    public double getLastCycleWsCoverage() { return lastCycleWsCoverage; }

    /** [v76] CorrelationGuard slot snapshot — for stats and watchdog visibility.
     *  Format: "2L/1S 3/6". Lets the operator see correlation state at a glance
     *  instead of having to grep [CORR-BLOCK] log lines. */
    public String getCorrelationSlotsSnapshot() {
        return correlationGuard.slotsSnapshot();
    }
    public boolean isCorrelationSaturated() {
        return correlationGuard.isSaturated();
    }

    /**
     * Returns the number of symbols currently in ISC cooldown (post-signal lockout).
     * Used by Watchdog to diagnose signal droughts: if many pairs are in cooldown
     * it means the bot is working correctly, not broken.
     * E.g. "📭 No signals 90 min (CD=14 pairs locked)" — clearly normal, not an error.
     */
    public int getCooldownedSymbolCount() {
        return 0;
    }
    /** [MODULE 2 v33] Returns OFV score for a pair: >0 bullish flow, <0 bearish. 0 if no data. */
    public double getOfvScore(String pair) {
        Double s = ofvScoreMap.get(pair);
        return s != null ? s : 0.0;
    }
    public List<String> getScanUniverseSnapshot(int limit) {
        List<String> sorted = new ArrayList<>(cachedPairs);
        // [v82.9 2026-06-01] CLEAN UNIVERSE — фильтруем мусор ДО backtest/scan.
        // ROOT: cachedPairs содержал ETHUSDC/BTCUSDC (USDC-пары, дублируют USDT но
        // с тонкой ликвидностью → HARD FAIL HTTP 400 каждый цикл) и токенизир.
        // commodities/stocks (XAU/XAG/CL/MU — есть в HARD_BLACKLIST, но snapshot
        // их НЕ применял). Это засоряло лог HARD FAIL и отбирало слоты у crypto
        // в STARTUP-BT. Фильтр здесь = единая точка для live-scan и startup-BT.
        sorted.removeIf(p -> p == null
                || !p.endsWith("USDT")              // только *USDT (отсекает USDC/BUSD-пары)
                || HARD_BLACKLIST.contains(p)       // токенизир. металлы/сырьё/стоки
                || isBlocklisted(p)                 // garbage-coin рантайм-блок
                || !passesTradeTier(p));            // [v86.36] liquidity-tier filter (TRADE_TIER)
        sorted.sort((a, b) -> Double.compare(
                volume24hUSD.getOrDefault(b, 0.0),
                volume24hUSD.getOrDefault(a, 0.0)));
        if (limit <= 0 || sorted.size() <= limit) return sorted;
        return new ArrayList<>(sorted.subList(0, limit));
    }
    public com.bot.DecisionEngineMerged.CoinCategory getCoinCategory(String pair) {
        return categorizePair(pair);
    }
    public String getRejectionStats() {
        return String.format("rej[liq=%d corr=%d stale=%d profit=%d e=%d opt=%d vpoc=%d final=%d isc=%d gic=%d q=+%.0f]",
                blockedLiq.get(), blockedCorr.get(), blockedStaleRt.get(), blockedProfit.get(),
                blockedEarlyConf.get(), blockedOptConf.get(), blockedVpoc.get(),
                blockedFinalConf.get(), blockedIsc.get(), gicHardHeadwind.get(), cycleQualityPenalty);
    }

    public com.bot.DecisionEngineMerged getDecisionEngine() { return decisionEngine; }
    public Map<String, Deque<Double>> getTickDeque()        { return tickPriceDeque; }

    /**
     * [A2 2026-05-08] Per-process-pair rejection breakdown — exposed for heartbeat.
     * The Dispatcher.getBlockBreakdown shows the OUTER funnel; this shows the INNER
     * funnel inside processPair() that runs before a TradeIdea ever reaches the
     * dispatcher. Without it, a clean dispatcher-side breakdown was misleading
     * because the real bottleneck was upstream (early-conf gate, ISC reject, etc.).
     *
     * Counters are cumulative (lifetime), formatted compact for Telegram.
     */
    public String getProcessPairBreakdown() {
        return String.format(
                "liq:%d corr:%d early:%d opt:%d vpoc:%d final:%d isc:%d",
                blockedLiq.get(), blockedCorr.get(),
                blockedEarlyConf.get(), blockedOptConf.get(), blockedVpoc.get(),
                blockedFinalConf.get(), blockedIsc.get());
    }

    //  STATIC MATH UTILS

    /** [v23.0] Delegates to TradingCore.atr() — Wilder's smoothed ATR everywhere */
    public static double atr(List<com.bot.TradingCore.Candle> c, int period) {
        return com.bot.TradingCore.atr(c, period);
    }

    /** [v10.0] Wilder's RSI (SMMA) — matches DecisionEngine and TradingView */
    public static double rsi(List<Double> prices, int period) {
        if (prices == null || prices.size() <= period) return 50.0;
        int startIdx = Math.max(1, prices.size() - period * 2);
        int seedEnd = Math.min(startIdx + period, prices.size());
        double avgGain = 0, avgLoss = 0;
        for (int i = startIdx; i < seedEnd; i++) {
            double d = prices.get(i) - prices.get(i - 1);
            if (d > 0) avgGain += d; else avgLoss -= d;
        }
        avgGain /= period; avgLoss /= period;
        for (int i = seedEnd; i < prices.size(); i++) {
            double d = prices.get(i) - prices.get(i - 1);
            avgGain = (avgGain * (period - 1) + (d > 0 ? d : 0)) / period;
            avgLoss = (avgLoss * (period - 1) + (d < 0 ? -d : 0)) / period;
        }
        return avgLoss < 1e-12 ? 100.0 : 100.0 - (100.0 / (1.0 + avgGain / avgLoss));
    }

    public static double ema(List<Double> prices, int period) {
        if (prices == null || prices.isEmpty()) return 0;
        double k = 2.0/(period+1), e = prices.get(0);
        for (double p : prices) e = p*k + e*(1-k);
        return e;
    }

    public static double sma(List<Double> prices, int period) {
        if (prices == null || prices.size() < period) return 0;
        double sum = 0; for (int i = prices.size()-period; i < prices.size(); i++) sum += prices.get(i);
        return sum / period;
    }

    public static double vwap(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.isEmpty()) return 0;
        double pv = 0, vol = 0;
        for (com.bot.TradingCore.Candle x : c) { double tp = (x.high+x.low+x.close)/3.0; pv+=tp*x.volume; vol+=x.volume; }
        return vol == 0 ? c.get(c.size()-1).close : pv/vol;
    }

    public static boolean detectBOS(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 10) return false;
        List<Integer> highs = com.bot.DecisionEngineMerged.swingHighs(c, 3);
        List<Integer> lows  = com.bot.DecisionEngineMerged.swingLows(c, 3);
        com.bot.TradingCore.Candle last = c.get(c.size()-1);
        if (!highs.isEmpty() && last.close > c.get(highs.get(highs.size()-1)).high * 1.0005) return true;
        if (!lows.isEmpty()  && last.close < c.get(lows.get(lows.size()-1)).low   * 0.9995) return true;
        return false;
    }

    public static List<Integer> detectSwingHighs(List<com.bot.TradingCore.Candle> c, int lr) { return com.bot.DecisionEngineMerged.swingHighs(c, lr); }
    public static List<Integer> detectSwingLows(List<com.bot.TradingCore.Candle> c, int lr)  { return com.bot.DecisionEngineMerged.swingLows(c, lr); }
    public static int marketStructure(List<com.bot.TradingCore.Candle> c) { return com.bot.DecisionEngineMerged.marketStructure(c); }

    private static String hmacSHA256(String secret, String data) throws Exception {
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] hash = mac.doFinal(data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    //  INNER CLASSES

    // PATCH #9: Multi-level OrderbookSnapshot replacing single bid/ask.
    // OLD: orderbookMap stored ONE level (bookTicker = best bid/ask only).
    //      OBI = (bidQty - askQty) / (bidQty + askQty) — just the spread, not real depth.
    // NEW: stores up to 5 bid levels + 5 ask levels from depth REST snapshot.
    //      Real OBI = sum(bid volumes L1-L5) vs sum(ask volumes L1-L5).
    //      Institutional walls are visible on L2-L5, not L1.
    // NOTE: bookTicker still populates L1 for latency-critical signals.
    //       depth snapshot (REST /fapi/v1/depth?limit=10) polled every 30s per active pair.
    public static final class OrderbookSnapshot {
        public final double bidVolume, askVolume;   // L1 (bookTicker, real-time)
        public final double bidDepth5, askDepth5;   // sum L1-L5 (depth snapshot, 30s)
        public final long timestamp;

        // Legacy constructor — bookTicker L1 only
        public OrderbookSnapshot(double b, double a, long t) {
            this(b, a, b, a, t);
        }

        // Full constructor — L1 + L1-5 depth
        public OrderbookSnapshot(double b, double a, double bd5, double ad5, long t) {
            bidVolume = b; askVolume = a; bidDepth5 = bd5; askDepth5 = ad5; timestamp = t;
        }

        // OBI uses 5-level depth when available, falls back to L1
        public double obi() {
            double bid = bidDepth5 > bidVolume ? bidDepth5 : bidVolume;
            double ask = askDepth5 > askVolume ? askDepth5 : askVolume;
            return (bid - ask) / (bid + ask + 1e-12);
        }

        public boolean isFresh() { return System.currentTimeMillis() - timestamp < 30_000; }
    }

    public static final class MicroCandleBuilder {
        private final int intervalMs;
        private long bucketStart=-1;
        private double open=Double.NaN, high=Double.NEGATIVE_INFINITY, low=Double.POSITIVE_INFINITY, close=Double.NaN;
        private double volume=0; private long closeTime=-1;
        public MicroCandleBuilder(int intervalMs) { this.intervalMs=intervalMs; }
        public Optional<com.bot.TradingCore.Candle> addTick(long ts, double price, double qty) {
            long bucket = (ts/intervalMs)*intervalMs;
            if (bucketStart==-1) { bucketStart=bucket; open=high=low=close=price; volume=qty; closeTime=bucket+intervalMs-1; return Optional.empty(); }
            if (bucket==bucketStart) { high=Math.max(high,price); low=Math.min(low,price); close=price; volume+=qty; return Optional.empty(); }
            com.bot.TradingCore.Candle c = new com.bot.TradingCore.Candle(bucketStart,open,high,low,close,volume,volume,closeTime);
            bucketStart=bucket; open=high=low=close=price; volume=qty; closeTime=bucket+intervalMs-1;
            return Optional.of(c);
        }
    }

    public static final class Signal {
        public final String symbol, direction; public final double confidence, price; public final long timestamp;
        public Signal(String sym, String dir, double conf, double price) { symbol=sym; direction=dir; confidence=conf; this.price=price; timestamp=System.currentTimeMillis(); }
    }

    private static int    envInt(String k, int d)      { try { return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(d))); } catch (Exception e) { return d; } }
    private static long   envLong(String k, long d)    { try { return Long.parseLong(System.getenv().getOrDefault(k, String.valueOf(d)));   } catch (Exception e) { return d; } }
    private static double envDouble(String k, double d){ try { return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(d))); } catch (Exception e) { return d; } }
    private static double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
    private static String pct(double v) { return String.format("%.0f", v * 100); }
}