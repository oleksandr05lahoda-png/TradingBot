package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  SignalSender — GODBOT PRO EDITION v36.0                                ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  [v34.0] Dynamic CoinCategory: volume-based TOP, keyword MEME          ║
 * ║  [v34.0] Extended detectSector: AI, RWA, DePin, Commodities, Metals    ║
 * ║  [v34.0] Category-aware EARLY_TICK velocity thresholds                  ║
 * ║  [v34.0] Category-aware event coin filter (TOP=5%, ALT=8%, MEME=12%)   ║
 * ║  [v34.0] Asset type display in Telegram signals + tradability hints     ║
 * ║  [v34.0] PumpHunter integration with category-aware thresholds          ║
 * ║  [FIX-BLIND]  14-минутная слепота устранена — LiveCandleAssembler      ║
 * ║  [FIX-WS]     WebSocket автозапускается для топ-30 пар по объёму       ║
 * ║  [FIX-UDS]    User Data Stream — реальное закрытие ордеров с биржи     ║
 * ║  [FIX-COMP]   Размер позиции масштабируется с балансом $100→$1M        ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public final class SignalSender {

    private final com.bot.TelegramBotSender bot;
    private final HttpClient              http;
    private final ExecutorService         httpIoExecutor;
    private final com.bot.GlobalImpulseController gic;
    private final com.bot.InstitutionalSignalCore isc;
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

    private static final long FUNDING_REFRESH_MS  = 5 * 60_000L;
    private static final long DELTA_WINDOW_MS     = 60_000L;

    private static final double MIN_PROFIT_TOP  = 0.0025;
    private static final double MIN_PROFIT_ALT  = 0.0035;
    private static final double MIN_PROFIT_MEME = 0.0050;

    private static final double MIN_VOL_TOP_USD  = 50_000_000;
    // [FIX v32+] Minimum volume raised dramatically.
    // OLD: $5M ALT / $1M MEME — this allowed RIVERUSDT, MONUSDT, VVVUSDT into analysis.
    // These low-cap coins have: wide spreads, noisy CVD, unreliable OBI, thin orderbook.
    // Volume delta on a $3M/24h coin is 95% noise. Signals are not tradeable.
    // NEW: $30M ALT / $10M MEME — forces analysis on liquid, institutional coins only.
    // Net effect: TOP_N=100 → ~35-45 actual candidates. Deep analysis, not shallow scanning.
    private static final double MIN_VOL_ALT_USD  = 30_000_000;  // was 5_000_000
    private static final double MIN_VOL_MEME_USD = 10_000_000;  // was 1_000_000

    private static final double STOP_CLUSTER_SHIFT = 0.0025;
    private static final int    MAX_WS_CONNECTIONS  = 100;
    private static final long   WS_INITIAL_DELAY_MS = 3_000L;
    private static final long   WS_MAX_DELAY_MS     = 120_000L;

    // Volume Delta
    private final Map<String, Double> deltaBuffer      = new ConcurrentHashMap<>();
    private final Map<String, Long>   deltaWindowStart = new ConcurrentHashMap<>();
    private final Map<String, Double> deltaHistory     = new ConcurrentHashMap<>();

    // [v29] VDA — Volume Delta Acceleration (10s micro-windows)
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

    // [v29] RT-CVD — real-time CVD from aggTrade (resets each 15m candle)
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
    private static final double LIQ_MIN_NOTIONAL = 50_000.0;  // игнорируем < $50k
    private static final long   LIQ_DECAY_MS     = 30 * 60_000L; // ликвидации "протухают" за 30 мин
    private final Map<String, Long> liqTimestamps = new ConcurrentHashMap<>();

    // Tick / WebSocket
    private static final long REALTIME_STALE_SKIP_MS = 75_000L;
    private static final double VPOC_NEAR_ATR_MULT   = 0.35;
    private static final double VPOC_NEAR_STOP_MULT  = 0.85;
    private static final double VPOC_SOFT_PENALTY    = 3.5;
    private static final double MAX_QUALITY_PENALTY  = 8.0;
    private final Map<String, Deque<Double>>      tickPriceDeque  = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>      tickVolumeDeque = new ConcurrentHashMap<>();
    private final Map<String, Long>               lastTickTime    = new ConcurrentHashMap<>();
    private final Map<String, Double>             lastTickPrice   = new ConcurrentHashMap<>();
    private final Map<String, WebSocket>          wsMap           = new ConcurrentHashMap<>();
    private final Map<String, Long>               wsReconnectDelay= new ConcurrentHashMap<>();
    private final Map<String, MicroCandleBuilder> microBuilders   = new ConcurrentHashMap<>();

    // ══════════════════════════════════════════════════════════════
    //  [v17.0 §2] EARLY TICK SIGNAL BUFFER
    //  Collects EARLY_TICK candidates across 1.5s windows per pair.
    //  earlyTickFlusher drains the buffer, sorts by probability,
    //  and dispatches only the TOP-1 per pair. Prevents signal spam
    //  during volatile bursts where the same pair fires 5× in 3 seconds.
    // ══════════════════════════════════════════════════════════════
    /** Accumulates the best (highest probability) EARLY_TICK candidate per pair per flush window. */
    private final Map<String, com.bot.DecisionEngineMerged.TradeIdea> earlyTickBuffer
            = new ConcurrentHashMap<>();

    // [v34.0] EARLY_TICK hourly rate limit per pair.
    // Problem: volatile ALT fires 8 EARLY_TICK signals in 30 min — all same move.
    // Manual trader can't act on more than 2-3 signals per hour on same pair.
    // Fix: max 3 EARLY_TICK per pair per rolling 60 minutes.
    private static final int    MAX_EARLY_TICK_PER_HOUR = 3;
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

    // [FIX-UDS] User Data Stream
    private volatile String    udsListenKey   = null;
    private volatile WebSocket udsWebSocket   = null;
    private final ScheduledExecutorService udsExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "uds-listener"); t.setDaemon(true); return t;
    });

    // [FIX-BLIND] Буфер 1m свечей для LiveCandleAssembler
    private final Map<String, List<com.bot.TradingCore.Candle>> liveM1Buffer = new ConcurrentHashMap<>();
    private static final int LIVE_M1_BUFFER_SIZE = 240; // [v36-FIX] 4h of 1m bars from WS ticks

    private final Map<String, Long> lastFetchTime = new ConcurrentHashMap<>();

    // [v12.0] Orderbook — populated via @bookTicker WebSocket stream
    private final Map<String, OrderbookSnapshot> orderbookMap = new ConcurrentHashMap<>();

    // Candle Cache
    private final Map<String, CachedCandles> candleCache = new ConcurrentHashMap<>();

    private static final Map<String, Long> CACHE_TTL = Map.of(
            "1m",  55_000L,
            "5m",  4  * 60_000L,
            "15m", 14 * 60_000L,   // исторические свечи ок, последняя пересобирается live
            "1h",  59 * 60_000L,
            "2h",  119 * 60_000L
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

    // [FIX-COMP] Баланс для компаундинга
    // [BUG-FIX] Убран хардкод $1000. Теперь читаем из env ACCOUNT_BALANCE (по умолчанию 100).
    // Установи в Railway: ACCOUNT_BALANCE=500 (или любая сумма которую ты реально торгуешь).
    // Если подключён API ключ — баланс подтягивается с биржи автоматически и env игнорируется.
    private volatile double accountBalance    = envDouble("ACCOUNT_BALANCE", 100.0);
    private volatile long   lastBalanceRefresh = 0;

    // ══════════════════════════════════════════════════════════════
    //  [v9.0] RATE LIMITER — Semaphore + Token Bucket + Backoff
    //  Replaces broken volatile-based rate limiter that caused
    //  silent request drops and potential IP bans.
    // ══════════════════════════════════════════════════════════════
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
        System.out.println("[RL] 429 #" + rl429Count + " backoff=" + backoff + "ms");
        // [v10.0] НЕ спамим в Telegram — это внутренняя механика
    }

    private void rlOn418() {
        rlIpBanned = true; rlIpBanUntil = System.currentTimeMillis() + 5*60_000L;
        rlRampUntil = rlIpBanUntil + 10 * 60_000L;
        System.out.println("[RL] 418 IP BAN 5min");
        // [v10.0] НЕ спамим в Telegram — бот просто подождёт и продолжит
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
    private final com.bot.SignalOptimizer optimizer;
    private final com.bot.PumpHunter pumpHunter;
    private final CorrelationGuard correlationGuard;
    private final ExecutorService fetchPool;

    // ══════════════════════════════════════════════════════════════
    //  [v36-FIX Дыра3] ORDER EXECUTOR — встроен в SignalSender
    //  Включается через env ENABLE_AUTO_TRADE=1.
    //  При 0 — работает в режиме сигналов (текущее поведение).
    //  Исполняет: MARKET entry + STOP_MARKET SL + TAKE_PROFIT TP1.
    //  API-ключи берутся из уже существующих полей API_KEY / API_SECRET.
    // ══════════════════════════════════════════════════════════════
    private static final boolean AUTO_TRADE_ENABLED =
            "1".equals(System.getenv("ENABLE_AUTO_TRADE"));
    private static final int AUTO_TRADE_LEVERAGE =
            Integer.parseInt(System.getenv().getOrDefault("AUTO_TRADE_LEVERAGE", "5"));

    /**
     * Выставляет MARKET ордер + STOP_MARKET SL + TAKE_PROFIT_MARKET TP1.
     * Вызывается после dispatch сигнала если ENABLE_AUTO_TRADE=1.
     * Полностью асинхронный — не блокирует цикл.
     */
    public void executeOrderAsync(com.bot.DecisionEngineMerged.TradeIdea idea, double sizeUsdt) {
        if (!AUTO_TRADE_ENABLED) return;
        if (API_KEY.isBlank() || API_SECRET.isBlank()) {
            System.out.println("[OE] ENABLE_AUTO_TRADE=1 но API ключи не заданы — пропуск");
            return;
        }

        CompletableFuture.runAsync(() -> {
            try {
                String side      = idea.side == com.bot.TradingCore.Side.LONG ? "BUY"  : "SELL";
                String closeSide = idea.side == com.bot.TradingCore.Side.LONG ? "SELL" : "BUY";

                // 1. Установить плечо
                oeSetLeverage(idea.symbol, AUTO_TRADE_LEVERAGE);
                Thread.sleep(200);

                // 2. Рассчитать quantity
                double qty = (sizeUsdt * AUTO_TRADE_LEVERAGE) / idea.price;
                String qtyStr = oeFormatQty(idea.symbol, qty);

                // 3. MARKET entry
                long orderId = oePlaceOrder(idea.symbol, side, "MARKET", qtyStr, 0, false);
                if (orderId < 0) {
                    bot.sendMessageAsync("❌ [AUTO] MARKET failed: " + idea.symbol);
                    return;
                }
                Thread.sleep(300);

                // 4. STOP_MARKET SL — reduceOnly=true
                oePlaceOrder(idea.symbol, closeSide, "STOP_MARKET", qtyStr, idea.stop, true);
                Thread.sleep(200);

                // 5. TAKE_PROFIT_MARKET TP1 — reduceOnly=true
                oePlaceOrder(idea.symbol, closeSide, "TAKE_PROFIT_MARKET", qtyStr, idea.tp1, true);

                bot.sendMessageAsync(String.format(
                        "✅ [AUTO EXEC] %s %s\n"
                                + "📌 Entry MARKET | SL: `%.4f` | TP1: `%.4f`\n"
                                + "💰 Размер: $%.1f × %dx лечо",
                        idea.symbol, idea.side, idea.stop, idea.tp1,
                        sizeUsdt, AUTO_TRADE_LEVERAGE));

            } catch (Exception e) {
                System.out.println("[OE] Error " + idea.symbol + ": " + e.getMessage());
                bot.sendMessageAsync("⚠️ [AUTO] Error " + idea.symbol + ": " + e.getMessage());
            }
        }, fetchPool);
    }

    /** Выставляет ордер на Binance Futures. Возвращает orderId или -1 при ошибке. */
    private long oePlaceOrder(String symbol, String side, String type,
                              String qty, double stopPrice, boolean reduceOnly) {
        try {
            StringBuilder body = new StringBuilder();
            body.append("symbol=").append(symbol);
            body.append("&side=").append(side);
            body.append("&type=").append(type);
            body.append("&quantity=").append(qty);
            if (stopPrice > 0)
                body.append("&stopPrice=").append(String.format("%.4f", stopPrice));
            if (reduceOnly)
                body.append("&reduceOnly=true");
            body.append("&timestamp=").append(System.currentTimeMillis());

            String sig = oeHmac(body.toString());
            body.append("&signature=").append(sig);

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://fapi.binance.com/fapi/v1/order"))
                    .timeout(Duration.ofSeconds(5))
                    .header("X-MBX-APIKEY", API_KEY)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                System.out.println("[OE] Order error " + resp.statusCode() + ": " + resp.body());
                return -1;
            }
            String r = resp.body();
            int idx = r.indexOf("\"orderId\":");
            if (idx < 0) return -1;
            int start = idx + 10, end = r.indexOf(',', start);
            if (end < 0) end = r.indexOf('}', start);
            return Long.parseLong(r.substring(start, end).trim());
        } catch (Exception e) {
            System.out.println("[OE] placeOrder exception: " + e.getMessage());
            return -1;
        }
    }

    /** Устанавливает плечо на паре через Binance API. */
    private void oeSetLeverage(String symbol, int leverage) {
        try {
            String body = "symbol=" + symbol + "&leverage=" + leverage
                    + "&timestamp=" + System.currentTimeMillis();
            String sig = oeHmac(body);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://fapi.binance.com/fapi/v1/leverage"))
                    .timeout(Duration.ofSeconds(5))
                    .header("X-MBX-APIKEY", API_KEY)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body + "&signature=" + sig))
                    .build();
            http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (Exception ignored) {}
    }

    /** Форматирует qty с правильной точностью по типу монеты. */
    private static String oeFormatQty(String symbol, double qty) {
        // BTC/ETH — 3 знака; большинство ALT — 1 знак; мелкие монеты — целые
        if (symbol.startsWith("BTC") || symbol.startsWith("ETH")) return String.format("%.3f", qty);
        if (qty >= 10) return String.format("%.1f", qty);
        return String.format("%.0f", qty);
    }

    /** HMAC-SHA256 подпись для Binance API. */
    private String oeHmac(String data) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(
                    API_SECRET.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] raw = mac.doFinal(
                    data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("HMAC failed", e);
        }
    }

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
    private final AtomicLong wsMessageCount = new AtomicLong(0);
    private final AtomicLong udsEventsCount = new AtomicLong(0);
    private final AtomicInteger cyclePairsSeen = new AtomicInteger(0);
    private final AtomicInteger cyclePairsStale = new AtomicInteger(0);
    private volatile double cycleQualityPenalty = 0.0;
    private volatile double lastCycleStaleRatio = 0.0;
    private volatile double lastCycleWsCoverage = 1.0;

    private static final Set<String> STABLE = Set.of("USDT","USDC","BUSD","TUSD","USDP","DAI");

    // [v34.0] DYNAMIC SECTOR DETECTION — extended with AI, RWA, DePin sectors
    // and auto-detection for commodity/metal tokens
    private String detectSector(String pair) {
        String s = pair.endsWith("USDT") ? pair.substring(0, pair.length() - 4) : pair;

        // [v34.0] Auto-detect non-crypto assets first
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

    // ══════════════════════════════════════════════════════════════
    //  CONSTRUCTOR
    // ══════════════════════════════════════════════════════════════

    public SignalSender(com.bot.TelegramBotSender bot,
                        com.bot.GlobalImpulseController sharedGIC,
                        com.bot.InstitutionalSignalCore sharedISC) {
        this.bot = bot;
        this.gic = sharedGIC;
        this.isc = sharedISC;
        // [PATCH B1 v33] OOM FIX — httpIoExecutor.
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

        this.API_KEY    = System.getenv().getOrDefault("BINANCE_API_KEY", "");
        this.API_SECRET = System.getenv().getOrDefault("BINANCE_API_SECRET", "");

        // TOP_N=100 — сканируем все 100 ликвидных пар.
        // Реальный пропуск через volume filter ($30M ALT / $10M MEME) ограничивает
        // кандидатов до ~35-50 активных пар. CallerRunsPolicy на fetchPool гарантирует
        // что перегрузка Binance API вызовет backpressure, не OOM.
        this.TOP_N            = envInt("TOP_N", 100);
        // MIN_CONF 62.0 — выровнен с BASE_CONF в DecisionEngineMerged
        this.MIN_CONF         = envDouble("MIN_CONF", 62.0);
        this.KLINES_LIMIT     = envInt("KLINES", 220);
        this.BINANCE_REFRESH_MS = envLong("BINANCE_REFRESH_MINUTES", 60) * 60_000L;
        this.TICK_HISTORY     = envInt("TICK_HISTORY", 120);
        this.OBI_THRESHOLD    = envDouble("OBI_THRESHOLD", 0.26);
        this.DELTA_BLOCK_CONF = envDouble("DELTA_BLOCK_CONF", 73.0);
        this.ENABLE_EARLY_TICK = envInt("ENABLE_EARLY_TICK", 1) == 1;
        this.MAX_SCAN_PAIRS_PER_CYCLE = envInt("MAX_SCAN_PAIRS_PER_CYCLE", 100);
        this.DEPTH_SNAPSHOT_TOP_N     = envInt("DEPTH_SNAPSHOT_TOP_N", 10); // [v36-FIX Дыра10] was 100 → 10 (bookTicker WS covers L1)
        this.FUNDING_OI_TOP_N         = envInt("FUNDING_OI_TOP_N", 40);

        this.decisionEngine   = new com.bot.DecisionEngineMerged();
        this.adaptiveBrain    = new com.bot.TradingCore.AdaptiveBrain();
        this.optimizer        = new com.bot.SignalOptimizer(this.tickPriceDeque);
        this.pumpHunter       = new com.bot.PumpHunter();
        this.correlationGuard = new CorrelationGuard();

        this.decisionEngine.setPumpHunter(this.pumpHunter);
        this.decisionEngine.setGIC(this.gic);
        // [v14.0 FIX #Forecast] Wire ForecastEngine so TradeIdea.forecast is not always null.
        this.decisionEngine.setForecastEngine(new com.bot.TradingCore.ForecastEngine());
        this.optimizer.setPumpHunter(this.pumpHunter);

        // [PATCH B2 v33] OOM FIX — fetchPool.
        // Unbounded LinkedBlockingQueue → OOM under TOP_N=100 load.
        // CallerRunsPolicy: if queue full, submitter thread runs the task itself
        // → natural backpressure, zero silent drops.
        // At TOP_N=100: poolSize = min((100+2)/3, 34) = 34 threads.
        // Queue=400 accommodates 4× burst without blocking the main scheduler.
        int poolSize = Math.max(8, Math.min((TOP_N + 2) / 3, 34));
        this.fetchPool = new ThreadPoolExecutor(
                poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(400),
                r -> {
                    Thread t = new Thread(r, "fetch-" + r.hashCode());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // [FIX-UDS] User Data Stream
        if (!API_KEY.isBlank()) {
            udsExecutor.schedule(this::initUserDataStream, 5, TimeUnit.SECONDS);
            udsExecutor.scheduleAtFixedRate(this::renewListenKey, 28, 28, TimeUnit.MINUTES);
            wsWatcher.scheduleAtFixedRate(this::refreshAccountBalance, 10, 120, TimeUnit.SECONDS);
            // [v28.0] PATCH #3: Set leverage + margin mode on startup.
            // OLD: leverage defaulted to exchange setting (often 20x) — SIZE=20$ at 20x = $400 position.
            //      SL -1.36% = -$5.44 loss on $20 margin = -27% per trade. Math was completely wrong.
            // NEW: ISOLATED margin at 5x leverage. SIZE=20$ at 5x = $100 position.
            //      SL -1.36% = -$1.36 loss on $20 margin = -6.8% per trade. Correct Kelly sizing.
            // Scheduled 10s after UDS init to ensure account is ready.
            udsExecutor.schedule(this::initLeverageAndMarginMode, 10, TimeUnit.SECONDS);
        }

        // [ДЫРА №2] Liquidation WebSocket — глобальный поток, без API ключа
        udsExecutor.schedule(this::connectLiquidationStream, 8, TimeUnit.SECONDS);

        // WS health check
        wsWatcher.scheduleAtFixedRate(this::checkWsHealth, 30, 30, TimeUnit.SECONDS);

        // [v28.0] PATCH #9: Poll depth5 snapshot every 30s for multi-level OBI.
        wsWatcher.scheduleAtFixedRate(this::refreshDepth5Snapshots, 35, 60, TimeUnit.SECONDS); // [v36-FIX Дыра10] was 30s → 60s

        // [v17.0 §2] EARLY TICK SIGNAL BUFFER FLUSHER — runs every 1500ms.
        // Drains earlyTickBuffer, sorts candidates by probability (highest first),
        // dispatches TOP-1 per pair. Prevents burst spam of 5+ signals on the same pair.
        wsWatcher.scheduleAtFixedRate(this::flushEarlyTickBuffer, 2, 2, TimeUnit.SECONDS);

        System.out.printf("[SignalSender v7.0] TOP_N=%d SCAN=%d OI=%d DEPTH=%d POOL=%d LIVE_CANDLE=ON WS_AUTO=ON UDS=%s BALANCE_TRACK=%s%n",
                TOP_N, MAX_SCAN_PAIRS_PER_CYCLE, FUNDING_OI_TOP_N, DEPTH_SNAPSHOT_TOP_N, poolSize,
                !API_KEY.isBlank() ? "ON" : "OFF",
                !API_KEY.isBlank() ? "ON" : "MANUAL");

        // [Hole 13 FIX] Make default $1000 balance loudly transparent
        if (API_KEY.isBlank()) {
            System.out.println("⚠️ Внимание: API не подключен, использую виртуальный фикс. баланс $1000");
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  GENERATE SIGNALS
    // ══════════════════════════════════════════════════════════════

    public List<com.bot.DecisionEngineMerged.TradeIdea> generateSignals() {

        // [v10.0] If IP banned — skip entire cycle silently, wait for ban to expire
        if (rlIpBanned && System.currentTimeMillis() < rlIpBanUntil) {
            return Collections.emptyList();
        }
        if (rlIpBanned) rlIpBanned = false;

        if (volume24hUSD.isEmpty() || System.currentTimeMillis() - lastVolRefresh > VOL_REFRESH_MS) {
            refreshVolume24h();
            lastVolRefresh = System.currentTimeMillis();
        }

        if (cachedPairs.isEmpty() ||
                System.currentTimeMillis() - lastPairsRefresh > BINANCE_REFRESH_MS) {
            Set<String> fresh = getTopSymbolsSet(TOP_N);
            if (!fresh.isEmpty()) {
                startWebSocketsForTopPairs(fresh);
                cachedPairs = fresh;
                lastPairsRefresh = System.currentTimeMillis();
            }
            // [v10.0] Пауза после тяжёлой загрузки пар (exchangeInfo+CoinGecko = ~80 weight)
            try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
        }

        if (System.currentTimeMillis() - lastFundingRefresh > FUNDING_REFRESH_MS) {
            refreshAllFundingRates();
            lastFundingRefresh = System.currentTimeMillis();
            // [v10.0] Пауза после funding refresh (premiumIndex + 100× OI = ~700 weight)
            try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
        }

        correlationGuard.resetCycle();
        cyclePairsSeen.set(0);
        cyclePairsStale.set(0);

        try {
            List<com.bot.TradingCore.Candle> btc5m = getCached("BTCUSDT", "5m", 30);
            if (btc5m != null && btc5m.size() >= 10) gic.updateFast(btc5m);
        } catch (Exception ignored) {}

        int pairBudget = computePairBudget();
        List<String> scanPairs = selectPairsForScan(pairBudget);
        List<CompletableFuture<com.bot.DecisionEngineMerged.TradeIdea>> futures = new ArrayList<>();
        for (String pair : scanPairs) {
            futures.add(CompletableFuture.supplyAsync(() -> processPair(pair), fetchPool));
        }

        List<com.bot.DecisionEngineMerged.TradeIdea> result = new ArrayList<>();
        for (CompletableFuture<com.bot.DecisionEngineMerged.TradeIdea> f : futures) {
            try {
                com.bot.DecisionEngineMerged.TradeIdea idea = f.get(18, TimeUnit.SECONDS);
                if (idea != null) result.add(idea);
            } catch (Exception ignored) {}
        }

        result.sort(Comparator.comparingDouble(
                (com.bot.DecisionEngineMerged.TradeIdea i) -> i.probability).reversed());

        refreshCycleQuality(scanPairs.size());
        logCycleStats();
        return result;
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
        if (rlIpBanned && now < rlIpBanUntil) return Math.min(12, base);
        if (now < rlRampUntil) return Math.min(20, base);
        if (rl429Count >= 2) return Math.min(30, base);
        int eff = Math.max(rlCurrentWeight.get(), rlServerWeight);
        if (eff > RL_SAFE_WEIGHT) return Math.min(35, base);
        return base;
    }

    private List<String> selectPairsForScan(int budget) {
        if (cachedPairs.isEmpty()) return List.of();
        List<String> sorted = new ArrayList<>(cachedPairs);
        sorted.sort((a, b) -> Double.compare(
                volume24hUSD.getOrDefault(b, 0.0),
                volume24hUSD.getOrDefault(a, 0.0)));
        if (sorted.size() <= budget) return sorted;
        return new ArrayList<>(sorted.subList(0, budget));
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-WS] АВТОЗАПУСК WEBSOCKET
    // ══════════════════════════════════════════════════════════════

    private void startWebSocketsForTopPairs(Set<String> pairs) {
        // [v10.0] MEMORY LEAK FIX: clean up pairs that dropped out of TOP-N
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
            // Clean candle caches for all timeframes
            for (String tf : List.of("1m","5m","15m","1h","2h")) {
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
            System.out.println("[WS-RECOVER] " + e.getMessage());
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  PROCESS PAIR
    // ══════════════════════════════════════════════════════════════

    private com.bot.DecisionEngineMerged.TradeIdea processPair(String pair) {
        try {
            // [v36-FIX Дыра1/2] m1 берётся из WS-буфера (aggTrade → MicroCandleBuilder).
            // REST для 1m больше не вызывается в основном цикле — только холодный старт.
            // Экономия: ~50 REST запросов/мин × weight=5 = 250 weight/мин.
            List<com.bot.TradingCore.Candle> m1  = getM1FromWs(pair);
            List<com.bot.TradingCore.Candle> m5  = getCached(pair, "5m",  KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> m15 = getCached15mWithLive(pair); // [FIX-BLIND]
            List<com.bot.TradingCore.Candle> h1  = getCached(pair, "1h",  KLINES_LIMIT);
            List<com.bot.TradingCore.Candle> h2  = getCached(pair, "2h",  120);
            // updateLiveM1Buffer больше не нужен — буфер заполняется из processAggTrade()

            // [ДЫРА №1] CVD — считаем накопленную дельту из 1m свечей
            double cvdNormalized = computeAndStoreCVD(pair, m1);
            decisionEngine.setCVD(pair, cvdNormalized);

            if (m15 == null || m15.size() < 160 || h1 == null || h1.size() < 160) return null;

            // [v34.0] Categorize early — needed for event filter and all downstream logic
            com.bot.DecisionEngineMerged.CoinCategory cat = categorizePair(pair);
            String sector = detectSector(pair);

            // ═══════════════════════════════════════════════════════
            // [v13.0+v34.0] EVENT COIN FILTER — category-aware
            // TOP coins: 5% daily move = massive event (BTC rarely does 8%)
            // ALT: 8% = event
            // MEME: 12% = event (they routinely move 5-8%)
            // ═══════════════════════════════════════════════════════
            {
                int n15 = m15.size();
                int dayBarsAgo = Math.min(96, n15 - 1);
                double dayOpen = m15.get(n15 - 1 - dayBarsAgo).close;
                double dayCurrent = m15.get(n15 - 1).close;
                double dailyChangePct = Math.abs(dayCurrent - dayOpen) / (dayOpen + 1e-9);

                // [v34.0] Category-aware event threshold
                double eventThreshold = switch (cat) {
                    case TOP  -> 0.05;   // 5% for BTC/ETH = massive event
                    case ALT  -> 0.08;   // 8% for ALT = event
                    case MEME -> 0.12;   // 12% for MEME = event (they're volatile by nature)
                };
                if (dailyChangePct > eventThreshold) {
                    return null;
                }

                // Volume anomaly: compare last 4 bars avg volume to 96-bar avg
                // [v34.0] Category-aware: TOP 4x spike = event, MEME needs 8x
                if (n15 > 100) {
                    double recentVol = 0;
                    for (int vi = n15 - 4; vi < n15; vi++) recentVol += m15.get(vi).volume;
                    recentVol /= 4;

                    double histVol = 0;
                    for (int vi = n15 - 100; vi < n15 - 4; vi++) histVol += m15.get(vi).volume;
                    histVol /= 96;

                    double volEventMult = switch (cat) {
                        case TOP  -> 4.0;  // BTC 4x volume = serious event
                        case ALT  -> 5.0;  // ALT 5x = event
                        case MEME -> 8.0;  // MEME 8x = event (they spike routinely)
                    };
                    if (histVol > 0 && recentVol > histVol * volEventMult) {
                        return null;
                    }
                }
            }

            if (!checkLiquidity(pair, cat)) { blockedLiq.incrementAndGet(); return null; }
            cyclePairsSeen.incrementAndGet();

            Long lastRealtimeTick = lastTickTime.get(pair);
            // [v16.0 FIX] Stale WS tick no longer hard-blocks the signal.
            // OLD: stale → return null → ALL pairs without recent WS tick dropped (UDS=❌ caused
            //      the bot to discard most pairs when WebSocket connectivity was degraded).
            // NEW: stale → zero out delta/VDA (conservative assumption: no momentum) and CONTINUE.
            // The historical candle data (15m/1h/2h) is still valid and sufficient for analysis.
            if (lastRealtimeTick == null || System.currentTimeMillis() - lastRealtimeTick > REALTIME_STALE_SKIP_MS) {
                decisionEngine.setVolumeDelta(pair, 0.0);
                decisionEngine.setVDA(pair, 0.0);
                cyclePairsStale.incrementAndGet();
                // Note: NOT returning null here anymore — analysis continues with zeroed delta
            }

            optimizer.updateFromCandles(pair, m15);

            double normDelta = getNormalizedDelta(pair);
            decisionEngine.setVolumeDelta(pair, normDelta);

            double relStrength = computeRelativeStrength(pair, m15);
            decisionEngine.updateRelativeStrength(pair, getSymbolReturn15m(m15), getBtcReturn15m());

            com.bot.DecisionEngineMerged.TradeIdea idea =
                    decisionEngine.analyze(pair, m1, m5, m15, h1, h2, cat);

            if (idea == null) return null;

            // [v16.0 FIX] qualityPenalty removed from confidence gate — it now affects SIZE ONLY.
            // OLD: earlyMinConf = effConf + symbolBoost + qualityPenalty (could reach 80%+)
            // NEW: earlyMinConf = effConf + symbolBoost (max ~68+4=72%, actually achievable)
            // qualityPenalty still reduces position size in getPositionSizeUsdt() below.
            double symbolConfBoost = isc.getSymbolMinConfBoost(pair);
            double qualityPenalty = cycleQualityPenalty;
            double earlyMinConf = Math.max(MIN_CONF, isc.getEffectiveMinConfidence() + symbolConfBoost);
            if (idea.probability < earlyMinConf) {
                blockedEarlyConf.incrementAndGet();
                return null;
            }

            boolean isLong   = idea.side == com.bot.TradingCore.Side.LONG;
            double gicWeight = gic.getFilterWeight(pair, isLong, relStrength, sector);
            if (gicWeight <= 0.05) gicHardHeadwind.incrementAndGet();

            // [v23.0] GIC weight → probability penalty, NOT hard veto
            // Old: gicWeight <= 0 → return null (killed ALL longs during BTC dip)
            // New: gicWeight → scaled penalty. Panic = -25, Danger = -15, Watch = -8
            if (gicWeight <= 0.05) {
                // Effectively blocked — apply massive penalty but let probability gate decide
                List<String> gicFlags = new ArrayList<>(idea.flags);
                gicFlags.add("GIC_BLOCK");
                idea = rebuildIdea(idea, Math.max(40, idea.probability - 25), gicFlags);
            } else if (gicWeight < 0.50) {
                List<String> gicFlags = new ArrayList<>(idea.flags);
                gicFlags.add("GIC_WEAK" + String.format("%.0f", gicWeight * 100));
                double penalty = (0.50 - gicWeight) * 30; // 0.05 → -13.5, 0.30 → -6, 0.49 → -0.3
                idea = rebuildIdea(idea, Math.max(50, idea.probability - penalty), gicFlags);
            } else if (gicWeight > 1.10) {
                // GIC boost — slightly increase probability
                double boost = Math.min(5, (gicWeight - 1.0) * 8);
                List<String> gicFlags = new ArrayList<>(idea.flags);
                gicFlags.add("GIC_BOOST" + String.format("%.0f", gicWeight * 100));
                idea = rebuildIdea(idea, Math.min(85, idea.probability + boost), gicFlags);
            }

            if (!correlationGuard.allow(pair, idea.side, cat, sector)) {
                blockedCorr.incrementAndGet(); return null;
            }

            // [v18.0 REFACTOR] PumpHunter: flag only, no arbitrary confidence scaling
            // [v34.0] Pass category for category-aware thresholds
            com.bot.PumpHunter.PumpEvent pump = pumpHunter.detectPump(pair, m1, m5, m15, cat);
            if (pump != null && pump.strength > 0.40) {
                boolean aligned = (idea.side == com.bot.TradingCore.Side.LONG && pump.isBullish()) ||
                        (idea.side == com.bot.TradingCore.Side.SHORT && pump.isBearish());
                if (aligned) {
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("PH_" + pump.type.name());
                    idea = rebuildIdea(idea, idea.probability, nf);
                }
            }

            idea = optimizer.withAdjustedConfidence(idea);
            if (idea.probability < MIN_CONF) {
                blockedOptConf.incrementAndGet();
                return null;
            }

            // [v18.0 REFACTOR] OBI: flag only, no probability scaling, no blocking
            // [FIX v32+] ANTI-SPOOFING: cross-validate OBI with realised taker flow.
            // A limit wall (OBI) WITHOUT confirmed taker activity = likely spoofing.
            // Spoofer pattern: large bid wall appears → bot flags bullish → wall yanked → dump.
            // Validation: if OBI says bullish (bid > ask) but normDelta says sell flow → SKIP OBI.
            // This does NOT block the signal — it just removes the OBI confidence flag.
            OrderbookSnapshot obs = orderbookMap.get(pair);
            if (obs != null && obs.isFresh()) {
                double obi = obs.obi();
                boolean obiAligned = (isLong && obi > OBI_THRESHOLD) || (!isLong && obi < -OBI_THRESHOLD);
                if (obiAligned) {
                    // [FIX v32+] Anti-spoofing: OBI bullish but taker flow bearish = spoof suspect
                    boolean obiAndFlowAgree = isLong
                            ? normDelta > -0.10   // bid wall + some sell flow OK, but not heavy sell
                            : normDelta < 0.10;   // ask wall + some buy flow OK, but not heavy buy
                    if (obiAndFlowAgree) {
                        List<String> nf = new ArrayList<>(idea.flags);
                        nf.add("OBI" + String.format("%+.0f", obi * 100));
                        idea = rebuildIdea(idea, idea.probability, nf);
                    } else {
                        // Spoof suspected — tag it but don't boost
                        List<String> nf = new ArrayList<>(idea.flags);
                        nf.add("OBI_SPOOF?");
                        idea = rebuildIdea(idea, idea.probability, nf);
                    }
                }
            }

            // [MODULE 2 v33] ORDER FLOW VELOCITY — applied after OBI check.
            // OFV measures whether the bid/ask wall is ACTIVELY GROWING (real demand)
            // or static (probable spoof). Key distinction from plain OBI:
            //
            //   OBI strong + OFV positive → real institutional accumulation → BOOST signal
            //   OBI strong + OFV ≈ 0     → static wall, no active buyers → no boost (spoof risk)
            //   OFV strong + no OBI flag  → flow building before wall appears → EARLY signal boost
            //   OFV negative vs direction → active distribution into price → PENALTY
            //
            // Score [-1..+1]: positive = bullish flow velocity, negative = bearish
            Double ofvScore = ofvScoreMap.get(pair);
            if (ofvScore != null && Math.abs(ofvScore) >= OFV_SIGNAL_THRESH) {
                boolean ofvBullish = ofvScore > 0;
                boolean ofvBearish = ofvScore < 0;
                boolean aligned    = (isLong && ofvBullish) || (!isLong && ofvBearish);
                boolean opposed    = (isLong && ofvBearish) || (!isLong && ofvBullish);

                List<String> nf = new ArrayList<>(idea.flags);
                if (aligned) {
                    // OFV confirms direction — confidence boost
                    double boost = Math.abs(ofvScore) >= OFV_STRONG_THRESH ? 4.5 : 2.5;
                    String tag = Math.abs(ofvScore) >= OFV_STRONG_THRESH ? "OFV_STRONG" : "OFV_ALIGN";
                    nf.add(tag + (isLong ? "↑" : "↓"));
                    idea = rebuildIdea(idea, Math.min(85.0, idea.probability + boost), nf);
                } else if (opposed) {
                    // Active flow against the signal direction — reduce confidence
                    double penalty = Math.abs(ofvScore) >= OFV_STRONG_THRESH ? 5.0 : 2.5;
                    nf.add("OFV_OPPOSE" + (isLong ? "↓" : "↑"));
                    idea = rebuildIdea(idea, idea.probability - penalty, nf);
                }
            }
            if (Math.abs(normDelta) > 0.28) {
                List<String> nf = new ArrayList<>(idea.flags);
                nf.add("Δ" + (normDelta > 0 ? "BUY" : "SELL") + pct(Math.abs(normDelta)));
                idea = rebuildIdea(idea, idea.probability, nf);
            }

            // [v18.0 REFACTOR] Sector Weakness: tag only, no confidence fading
            if (sector != null && isLong) {
                double weakness = gic.getSectorWeakness(sector);
                if (weakness > 0.7) {
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("WEAK_SECTOR");
                    idea = rebuildIdea(idea, idea.probability, nf);
                }
            }

            idea = adjustStopForClusters(idea, m15);
            idea = applyVpocBarrierGuard(idea, m15);
            if (idea == null) { blockedVpoc.incrementAndGet(); return null; }

            if (!checkMinProfit(idea, cat)) { blockedProfit.incrementAndGet(); return null; }

            // [FIX-COMP] Добавляем рекомендованный размер позиции
            double posSize = getPositionSizeUsdt(idea, cat);
            // [v16.0] SURVIVAL MODE (day <= -6%): position size ×0.25.
            // [v16.0] CAUTIOUS MODE (day <= -3%): position size ×0.5.
            // This replaces the old hard signal block with meaningful size reduction.
            if (isc.isSurvivalMode()) {
                posSize = posSize * 0.25;
            } else if (isc.isCautiousMode()) {
                posSize = posSize * 0.5;
            }
            if (qualityPenalty > 0.0) {
                posSize *= Math.max(0.55, 1.0 - qualityPenalty * 0.06);
            }
            List<String> nf = new ArrayList<>(idea.flags);
            String sizeMode = isc.isSurvivalMode() ? " 🆘SURVIVAL"
                    : isc.isCautiousMode() ? " ⚠️CAUTIOUS" : "";
            // [v17.0 §4] Append REDUCED_RISK flag from DrawdownManager if active
            String rrFlag = isc.getReducedRiskFlag();
            if (!rrFlag.isEmpty()) sizeMode += " " + rrFlag;
            if (qualityPenalty > 0.0) sizeMode += String.format(" Q+%.0f", qualityPenalty);
            nf.add(String.format("SIZE=%.1f$%s", posSize, sizeMode));
            idea = rebuildIdea(idea, idea.probability, nf);

            // [v25.0] SESSION WEIGHT → FLAG + SIZE REDUCTION ONLY.
            // Session context predicts LIQUIDITY, not signal direction.
            // Low liquidity = smaller position. Signal validity is unchanged.
            // Probability modification removed — it was incorrectly killing valid setups at 03:00 UTC.
            double sessionW = getSessionWeight();
            List<String> sf = new ArrayList<>(idea.flags);
            if (sessionW < 0.85) {
                sf.add("SESS_LOW");    // carried to SignalSender for size reduction
            } else if (sessionW >= 1.20) {
                sf.add("SESS_NY");     // London/NY overlap — max liquidity
            } else if (sessionW >= 1.10) {
                sf.add("SESS_LONDON"); // London open
            }
            // NO probability modification. Size is adjusted in getPositionSizeUsdt() via sessionW.
            idea = rebuildIdea(idea, idea.probability, sf);

            // [ДЫРА №2] LIQUIDATION SCORE — усиливаем сигнал если рядом крупные ликвидации
            double atrForLiq = com.bot.TradingCore.atr(m15, 14);
            double liqScore  = getLiquidationScore(pair, idea.price, atrForLiq, idea.side);
            if (liqScore > 0.25) {
                List<String> lf = new ArrayList<>(idea.flags);
                lf.add(String.format("LIQ_MAGNET+%.0f%%", liqScore * 100));
                double liqBoost = liqScore * 8; // max +8 к вероятности
                idea = rebuildIdea(idea, Math.min(85, idea.probability + liqBoost), lf);
            }

            // [v16.0 FIX] qualityPenalty excluded from final confidence gate (size only).
            double finalMinConf = Math.max(MIN_CONF, isc.getEffectiveMinConfidence() + symbolConfBoost);
            if (idea.probability < finalMinConf) {
                blockedFinalConf.incrementAndGet();
                return null;
            }
            if (!isc.allowSignal(idea)) {
                blockedIsc.incrementAndGet();
                return null;
            }

            // [v28.0] PATCH #12: Adaptive TP calibration based on real historical RR.
            // Problem: TP_TREND_EARLY sets tp3Mult=4.2, but if symbol historically only
            //          reaches avgRR=1.3, those wide TPs are wishful thinking.
            //          Bot waited for TP3 that never came, missing TP1 exits.
            // Fix: if real avgRR < 70% of assumed tp3Mult → compress all TP multipliers.
            double realRR = isc.getAvgRealizedRR(pair);
            if (realRR > 0 && idea.tp3Mult > 0 && realRR < idea.tp3Mult * 0.70) {
                // Symbol's real edge is weaker than the TP targets assume.
                // Scale all TP levels down proportionally. TP1 always stays >= 0.6×risk.
                double scale = Math.max(0.60, realRR / idea.tp3Mult);
                double newTp1 = Math.max(0.60, idea.tp1Mult * scale);
                double newTp2 = Math.max(1.00, idea.tp2Mult * scale);
                double newTp3 = Math.max(1.50, idea.tp3Mult * scale);
                List<String> tf = new ArrayList<>(idea.flags);
                tf.add(String.format("TP_CAL×%.1f", scale));
                idea = new com.bot.DecisionEngineMerged.TradeIdea(
                        idea.symbol, idea.side, idea.price, idea.stop, idea.take,
                        idea.rr, idea.probability, tf,
                        idea.fundingRate, idea.fundingDelta, idea.oiChange,
                        idea.htfBias, idea.category, idea.forecast,
                        newTp1, newTp2, newTp3);
            }

            isc.registerSignal(idea);
            // [PATCH B4 v33] HARD R:R GATE — enforced AFTER adaptive TP calibration.
            // tp2Mult floor is Math.max(1.00, ...) → worst case TP2=entry±1×risk → R:R=1:1.
            // This violates the system's minimum 1:2 requirement.
            // Gate: actualRR measured from TP2 (realistic partial exit target), not TP3.
            // Tolerance: 1.80 instead of 2.00 — 10% buffer for slippage on entry fill.
            double _riskDist = Math.abs(idea.stop - idea.price);
            double _tp2Dist  = Math.abs(idea.tp2 - idea.price);
            double actualRR  = _riskDist > 1e-9 ? _tp2Dist / _riskDist : 0;
            if (actualRR < 1.80) {
                // Undo the registration — R:R is insufficient
                isc.unregisterSignal(idea);
                System.out.printf("[RR-GATE] %s %s BLOCKED: actualRR=%.2f < 1.80 (TP2=%.6f entry=%.6f SL=%.6f)%n",
                        pair, idea.side, actualRR, idea.tp2, idea.price, idea.stop);
                return null;
            }
            // [v23.0] Confirm signal → sets cooldown + lastSigPrice in DecisionEngine
            decisionEngine.confirmSignal(idea.symbol, idea.side, idea.price, System.currentTimeMillis());
            correlationGuard.register(pair, idea.side, cat, sector);

            return idea;
        } catch (Exception e) {
            System.out.println("[processPair] " + pair + ": " + e.getMessage());
            return null;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-BLIND] LIVE CANDLE ASSEMBLER
    // ══════════════════════════════════════════════════════════════

    /**
     * Возвращает список 15m свечей, где ПОСЛЕДНЯЯ СВЕЧА — живая,
     * собранная из 1m данных прямо сейчас.
     *
     * Это устраняет 14-минутную слепоту: RSI, EMA, ATR пересчитываются
     * каждую минуту вместо раза в 14 минут.
     */
    private List<com.bot.TradingCore.Candle> getCached15mWithLive(String pair) {
        List<com.bot.TradingCore.Candle> historical = getCached(pair, "15m", KLINES_LIMIT);
        if (historical == null || historical.isEmpty()) return historical;

        List<com.bot.TradingCore.Candle> m1buf = liveM1Buffer.get(pair);
        if (m1buf == null || m1buf.isEmpty()) return historical;

        com.bot.TradingCore.Candle liveCurrent = assembleLive15mCandle(m1buf);
        if (liveCurrent == null) return historical;

        com.bot.TradingCore.Candle lastHistorical = historical.get(historical.size() - 1);
        long livePeriod = liveCurrent.openTime / (15 * 60_000L);
        long lastPeriod = lastHistorical.openTime / (15 * 60_000L);

        List<com.bot.TradingCore.Candle> result = new ArrayList<>(historical);
        if (livePeriod == lastPeriod) {
            result.set(result.size() - 1, liveCurrent); // заменяем текущую
        } else if (livePeriod > lastPeriod) {
            result.add(liveCurrent); // новая свеча началась
        }
        return Collections.unmodifiableList(result);
    }

    private com.bot.TradingCore.Candle assembleLive15mCandle(List<com.bot.TradingCore.Candle> m1) {
        if (m1 == null || m1.isEmpty()) return null;
        long now = System.currentTimeMillis();
        long current15mStart = (now / (15 * 60_000L)) * (15 * 60_000L);

        // [v36-FIX Дыра2] Если текущая 15m свеча только началась (<2 мин WS-данных),
        // не отдаём "урезанную" свечу в индикаторы — она исказит ATR и RSI.
        if (now - current15mStart < 120_000L) return null;

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
        // Начиная с этого момента WS-тики будут дополнять буфер через processAggTrade()
        liveM1Buffer.put(pair, new ArrayList<>(seed));
        return Collections.unmodifiableList(seed);
    }

    /** @deprecated Заменён на getM1FromWs(). Оставлен для совместимости с TradeResolver. */
    @Deprecated
    private void updateLiveM1Buffer(String pair, List<com.bot.TradingCore.Candle> m1) {
        // no-op: liveM1Buffer теперь заполняется исключительно из processAggTrade()
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-COMP] РАЗМЕР ПОЗИЦИИ — Kelly-inspired компаундинг
    // ══════════════════════════════════════════════════════════════

    /**
     * Рассчитывает размер позиции в USDT на основе текущего баланса.
     *
     * Формула: рискСумма / стопПроцент = размерПозиции
     * рискСумма = баланс * рискПроцент (1-2% в зависимости от категории)
     *
     * По мере роста баланса $100→$1000→$100000 позиции растут пропорционально.
     * Это и есть механизм превращения $100 в $1,000,000.
     */
    public double getPositionSizeUsdt(com.bot.DecisionEngineMerged.TradeIdea idea,
                                      com.bot.DecisionEngineMerged.CoinCategory cat) {
        double balance = Math.max(accountBalance, 100.0);

        double riskPct = switch (cat) {
            // [v10.0] Conservative risk until 100+ real trades prove the edge
            case TOP  -> 0.010; // 1.0% (was 1.5%)
            case ALT  -> 0.010; // 1.0% (was 2.0% — dangerous for unproven system)
            case MEME -> 0.006; // 0.6% (was 1.0%)
        };

        // Высокая уверенность → чуть больше позиция
        if (idea.probability >= 80)      riskPct *= 1.30;
        else if (idea.probability >= 70) riskPct *= 1.15;
        else if (idea.probability < 58)  riskPct *= 0.75;

        double riskUsdt  = balance * riskPct;
        riskUsdt *= isc.getRiskSizeMultiplier();

        // [v25.0] SESSION LIQUIDITY → size multiplier (was probability penalty — wrong approach).
        // Low liquidity session = smaller position to compensate wider spreads & slippage.
        // Probability is NOT modified — the SIGNAL is valid; only execution risk changes.
        double sessionW = getSessionWeight();
        if (sessionW < 0.85) {
            riskUsdt *= Math.max(0.50, sessionW); // Asia/night: max 50% of normal size
        } else if (sessionW >= 1.20) {
            riskUsdt *= 1.10; // NY open: slight size boost — highest liquidity
        }

        // [v25.0] LATE_ENTRY flag → reduce size 20% (signal is valid but half the move is done)
        if (idea.flags.contains("LATE_ENTRY_SIZE_CUT")) {
            riskUsdt *= 0.80;
        }

        double stopPct   = Math.max(0.005, Math.abs(idea.price - idea.stop) / idea.price);
        double posSize   = riskUsdt / stopPct;

        // Лимиты: не более 20% баланса, не менее $6.5
        posSize = Math.min(posSize, balance * 0.20);
        posSize = Math.max(posSize, 6.5);

        return Math.round(posSize * 100.0) / 100.0;
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-COMP] ОБНОВЛЕНИЕ БАЛАНСА
    // ══════════════════════════════════════════════════════════════

    private void refreshAccountBalance() {
        if (API_KEY.isBlank() || rlIpBanned) return; // [v10.0]
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
        double[] milestones = {200, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 500000, 1_000_000};
        for (double m : milestones) {
            if (old < m && newBal >= m) {
                bot.sendMessageAsync(String.format("🎯 *MILESTONE* Баланс достиг $%.0f! 🚀🚀🚀", m));
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  [v28.0] PATCH #3: LEVERAGE + MARGIN MODE INITIALIZATION
    //  Sets ISOLATED margin + 5x leverage on all active trading pairs.
    //  Called once at startup after account is ready.
    //  Without this, exchange defaults apply (often 20x CROSS = account-wipe risk).
    // ══════════════════════════════════════════════════════════════

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
        if (fail > 0) {
            bot.sendMessageAsync(String.format(
                    "⚠️ *LEVERAGE INIT* %d пар настроено, %d ошибок\n" +
                            "Проверьте %dx ISOLATED вручную перед торговлей!", ok, fail, TARGET_LEVERAGE));
        } else {
            bot.sendMessageAsync(String.format(
                    "✅ *LEVERAGE OK* Все %d пар: %dx ISOLATED", ok, TARGET_LEVERAGE));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-UDS] USER DATA STREAM
    // ══════════════════════════════════════════════════════════════

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
            System.out.println("[UDS] Init error: " + e.getMessage());
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
                .thenAccept(ws -> { udsWebSocket = ws; System.out.println("[UDS] ✅ Connected"); })
                .exceptionally(ex -> { scheduleUdsRetry(15); return null; });
    }

    private void processUserDataEvent(JSONObject event) {
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
                    // [v9.0] UDS confirmed result → register in ISC properly
                    if (realizedPnl > 0) {
                        isc.registerConfirmedResult(true, closedSide);
                        decisionEngine.recordWin(symbol, closedSide);
                    } else if (realizedPnl < 0) {
                        isc.registerConfirmedResult(false, closedSide);
                        decisionEngine.recordLoss(symbol, closedSide);
                    }
                    decisionEngine.markPostExitCooldown(symbol, closedSide);
                    String closeReason = realizedPnl > 0 ? "UDS_TP"
                            : realizedPnl < 0 ? "UDS_SL" : "UDS_CLOSE";
                    isc.closeTrade(symbol, closedSide, pnlPct, closeReason);
                    // [v9.0] Remove from BotMain TradeResolver tracking
                    com.bot.BotMain.trackedSignals.remove(symbol + "_" + closedSide);
                    String emoji = realizedPnl >= 0 ? "✅" : "❌";
                    bot.sendMessageAsync(String.format("%s *UDS CLOSED* %s %s\nPnL: %+.4f$ (%+.2f%%)",
                            emoji, symbol, closedSide, realizedPnl, pnlPct));
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
                bot.sendMessageAsync("🚨 *MARGIN CALL* — немедленно проверьте аккаунт!");
                System.out.println("[UDS] ⚠️ MARGIN CALL!");
            }

            case "listenKeyExpired" -> {
                udsWebSocket = null; scheduleUdsRetry(1);
            }
        }
    }

    private void renewListenKey() {
        if (API_KEY.isBlank() || udsListenKey == null) return;
        try {
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder()
                            .uri(URI.create("https://fapi.binance.com/fapi/v1/listenKey"))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", API_KEY)
                            .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                    BINANCE_WEIGHT_SIGNED_LIGHT);
            if (resp != null && resp.statusCode() == 200) { System.out.println("[UDS] Key renewed"); }
            else { initUserDataStream(); }
        } catch (Exception e) { initUserDataStream(); }
    }

    private void scheduleUdsRetry(int delaySec) {
        udsExecutor.schedule(this::initUserDataStream, delaySec, TimeUnit.SECONDS);
    }

    // ══════════════════════════════════════════════════════════════
    //  WS HEALTH CHECK
    // ══════════════════════════════════════════════════════════════

    // [v28.0] PATCH #9: Depth5 snapshot poller — multi-level OBI.
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
        // [FIX-WS-STORM] Collect stale pairs first, THEN reconnect.
        List<String> stalePairs = new ArrayList<>();
        for (Map.Entry<String, WebSocket> e : wsMap.entrySet()) {
            Long last = lastTickTime.get(e.getKey());
            if (last != null && now - last > staleThreshold) {
                stalePairs.add(e.getKey());
            }
        }
        if (!stalePairs.isEmpty()) {
            // [v28.0] PATCH #19 + [SCANNER] alert if >20% of pairs are stale (not just any pair)
            if (stalePairs.size() > wsMap.size() * 0.20) {
                bot.sendMessageAsync(String.format(
                        "⚠️ *WS DATA LOSS* — %d пар без данных >60s: %s\n🔄 Переподключение...",
                        stalePairs.size(),
                        stalePairs.size() <= 5 ? stalePairs.toString() : stalePairs.size() + " пар"));
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
                System.out.println("[WS-HEALTH] FORCE-RECONNECT: no WS messages in 60s — reconnecting all pairs");
                bot.sendMessageAsync("⚠️ *WS SILENT 60s* — форс-переподключение всех каналов...");
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

    // ══════════════════════════════════════════════════════════════
    //  LIQUIDITY GUARD
    // ══════════════════════════════════════════════════════════════

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

    // ══════════════════════════════════════════════════════════════
    //  RELATIVE STRENGTH
    // ══════════════════════════════════════════════════════════════

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
        CachedCandles c = candleCache.get("BTCUSDT_15m");
        if (c == null || c.candles.size() < 5) return 0;
        int n = c.candles.size();
        cachedBtcReturn = (c.candles.get(n-1).close - c.candles.get(n-4).close) / (c.candles.get(n-4).close + 1e-9);
        lastBtcReturnTime = System.currentTimeMillis();
        return cachedBtcReturn;
    }

    // ══════════════════════════════════════════════════════════════
    //  STOP CLUSTER AVOIDANCE
    // ══════════════════════════════════════════════════════════════

    private com.bot.DecisionEngineMerged.TradeIdea adjustStopForClusters(
            com.bot.DecisionEngineMerged.TradeIdea idea,
            List<com.bot.TradingCore.Candle> m15) {
        if (m15 == null || m15.size() < 20) return idea;
        double price = idea.price, oldStop = idea.stop, newStop = oldStop;
        int n = m15.size(), lookback = Math.min(20, n - 1);

        if (idea.side == com.bot.TradingCore.Side.LONG) {
            double swingLow = Double.MAX_VALUE;
            for (int i = n - lookback; i < n - 1; i++) {
                double low = m15.get(i).low;
                if (low >= oldStop * 0.98 && low <= oldStop * 1.005) swingLow = Math.min(swingLow, low);
            }
            if (swingLow != Double.MAX_VALUE) {
                newStop = Math.max(swingLow * (1 - STOP_CLUSTER_SHIFT), price - atr(m15, 14) * 2.2);
            }
        } else {
            double swingHigh = Double.NEGATIVE_INFINITY;
            for (int i = n - lookback; i < n - 1; i++) {
                double high = m15.get(i).high;
                if (high >= oldStop * 0.995 && high <= oldStop * 1.02) swingHigh = Math.max(swingHigh, high);
            }
            if (swingHigh != Double.NEGATIVE_INFINITY) {
                newStop = Math.min(swingHigh * (1 + STOP_CLUSTER_SHIFT), price + atr(m15, 14) * 2.2);
            }
        }

        if (newStop == oldStop) return idea;
        List<String> nf = new ArrayList<>(idea.flags);
        nf.add("SL_ADJ");
        return new com.bot.DecisionEngineMerged.TradeIdea(
                idea.symbol, idea.side, idea.price, newStop, idea.take, idea.rr,
                idea.probability, nf, idea.fundingRate, idea.fundingDelta,
                idea.oiChange, idea.htfBias, idea.category,
                idea.forecast,
                idea.tp1Mult, idea.tp2Mult, idea.tp3Mult);
    }

    // ══════════════════════════════════════════════════════════════
    //  MINIMUM PROFIT GUARD
    // ══════════════════════════════════════════════════════════════

    private boolean checkMinProfit(com.bot.DecisionEngineMerged.TradeIdea idea,
                                   com.bot.DecisionEngineMerged.CoinCategory cat) {
        double gross = Math.abs(idea.tp1 - idea.price) / idea.price;
        double slip  = switch (cat) { case TOP -> 0.0005; case ALT -> 0.0015; case MEME -> 0.0040; };
        double min   = switch (cat) { case TOP -> MIN_PROFIT_TOP; case ALT -> MIN_PROFIT_ALT; case MEME -> MIN_PROFIT_MEME; };
        return (gross - 0.0008 - slip) >= min;
    }

    private com.bot.DecisionEngineMerged.TradeIdea applyVpocBarrierGuard(
            com.bot.DecisionEngineMerged.TradeIdea idea,
            List<com.bot.TradingCore.Candle> m15) {
        if (idea == null || idea.forecast == null || m15 == null || m15.size() < 20) return idea;

        double vpoc = idea.forecast.magnetLevel;
        if (!(vpoc > 0.0) || !Double.isFinite(vpoc)) return idea;

        double price = idea.price;
        double tp1 = idea.tp1;
        double stopDist = Math.abs(price - idea.stop);
        double tp1Dist = Math.abs(tp1 - price);
        if (stopDist <= 0.0 || tp1Dist <= 0.0) return idea;

        boolean inPath = idea.side == com.bot.TradingCore.Side.LONG
                ? (vpoc > price && vpoc < tp1)
                : (vpoc < price && vpoc > tp1);
        if (!inPath) return idea;

        double barrierDist = Math.abs(vpoc - price);
        double atr14 = atr(m15, 14);
        boolean leadBreakout = hasLeadBreakout(idea);
        boolean strongForecast = idea.forecast.confidence >= 0.64
                && ((idea.side == com.bot.TradingCore.Side.LONG && idea.forecast.directionScore > 0.20)
                || (idea.side == com.bot.TradingCore.Side.SHORT && idea.forecast.directionScore < -0.20));
        boolean tightBarrier = barrierDist <= Math.max(stopDist * VPOC_NEAR_STOP_MULT, atr14 * VPOC_NEAR_ATR_MULT);

        if (tightBarrier && !(leadBreakout || strongForecast)) {
            return null;
        }

        if (barrierDist <= tp1Dist * 0.55) {
            List<String> nf = new ArrayList<>(idea.flags);
            nf.add(leadBreakout ? "VPOC_BREAKTRY" : "VPOC_NEAR");
            double penalty = leadBreakout ? 1.5 : VPOC_SOFT_PENALTY;
            return rebuildIdea(idea, Math.max(50, idea.probability - penalty), nf);
        }

        return idea;
    }

    private boolean hasLeadBreakout(com.bot.DecisionEngineMerged.TradeIdea idea) {
        if (idea == null || idea.flags == null || idea.flags.isEmpty()) return false;
        if (idea.side == com.bot.TradingCore.Side.LONG) {
            return idea.flags.contains("EARLY_SOLO")
                    || idea.flags.contains("BOS_UP_5M")
                    || idea.flags.contains("COMP_BREAK_UP")
                    || idea.flags.contains("ANTI_LAG_UP")
                    || idea.flags.contains("PUMP_HUNT_B")
                    || idea.flags.stream().anyMatch(f -> f.startsWith("VDA+"));
        }
        return idea.flags.contains("EARLY_SOLO")
                || idea.flags.contains("BOS_DN_5M")
                || idea.flags.contains("COMP_BREAK_DN")
                || idea.flags.contains("ANTI_LAG_DN")
                || idea.flags.contains("PUMP_HUNT_S")
                || idea.flags.stream().anyMatch(f -> f.startsWith("VDA-"));
    }

    // ══════════════════════════════════════════════════════════════
    //  CORRELATION GUARD
    // ══════════════════════════════════════════════════════════════

    private static final class CorrelationGuard {
        private int longCount = 0, shortCount = 0;
        private int topLongCount = 0, topShortCount = 0;
        private final Map<String, Integer> sectorDirCount = new HashMap<>();
        private final Set<String> registered = new HashSet<>();
        // Limits stay directional, but are configurable so a 100-pair scanner
        // is not silently stuck at the old "6 per side" profile.
        private static final int MAX_DIR_NORMAL  = envInt("CORR_MAX_DIR_NORMAL", 10);
        private static final int MAX_DIR_WEEKEND = envInt("CORR_MAX_DIR_WEEKEND", 6);
        private static final int MAX_SECTOR      = envInt("CORR_MAX_SECTOR_SAME_DIR", 4);
        private static final int MAX_TOP_SAME_DIR= envInt("CORR_MAX_TOP_SAME_DIR", 4);

        private static boolean isWeekend() {
            java.time.DayOfWeek d = java.time.ZonedDateTime.now(java.time.ZoneId.of("UTC")).getDayOfWeek();
            return d == java.time.DayOfWeek.SATURDAY || d == java.time.DayOfWeek.SUNDAY;
        }

        synchronized void resetCycle() {
            longCount = 0;
            shortCount = 0;
            topLongCount = 0;
            topShortCount = 0;
            sectorDirCount.clear();
            registered.clear();
        }

        synchronized boolean allow(String pair, com.bot.TradingCore.Side side,
                                   com.bot.DecisionEngineMerged.CoinCategory cat, String sector) {
            int maxDir = isWeekend() ? MAX_DIR_WEEKEND : MAX_DIR_NORMAL;
            if (side == com.bot.TradingCore.Side.LONG  && longCount  >= maxDir) return false;
            if (side == com.bot.TradingCore.Side.SHORT && shortCount >= maxDir) return false;
            if (sector != null && sectorDirCount.getOrDefault(sector + "_" + side.name(), 0) >= MAX_SECTOR) return false;
            if (cat == com.bot.DecisionEngineMerged.CoinCategory.TOP) {
                int topSameDir = side == com.bot.TradingCore.Side.LONG ? topLongCount : topShortCount;
                if (topSameDir >= MAX_TOP_SAME_DIR) return false;
            }
            return true;
        }

        synchronized void register(String pair, com.bot.TradingCore.Side side,
                                   com.bot.DecisionEngineMerged.CoinCategory cat, String sector) {
            if (registered.contains(pair)) return;
            registered.add(pair);
            if (side == com.bot.TradingCore.Side.LONG) longCount++; else shortCount++;
            if (cat == com.bot.DecisionEngineMerged.CoinCategory.TOP) {
                if (side == com.bot.TradingCore.Side.LONG) topLongCount++; else topShortCount++;
            }
            if (sector != null) sectorDirCount.merge(sector + "_" + side.name(), 1, Integer::sum);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  CANDLE CACHE
    // ══════════════════════════════════════════════════════════════

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
            if (!fresh.isEmpty()) {
                candleCache.put(key, new CachedCandles(fresh));
                lastFetchTime.put(key, System.currentTimeMillis());
                return fresh;
            }
            // [v24.0 FIX WEAK-5] Staleness guard
            if (cached != null && !cached.isStale(ttl * 3)) return cached.candles;
            if (cached != null) {
                System.out.printf("[STALE] %s cache too old (%ds), skipping%n",
                        key, (System.currentTimeMillis() - cached.fetchedAt) / 1000);
                return Collections.emptyList();
            }
            return fresh;
        }
    }

    public List<com.bot.TradingCore.Candle> fetchKlines(String symbol, String interval, int limit) {
        return getCached(symbol, interval, limit);
    }

    private List<com.bot.TradingCore.Candle> fetchKlinesDirect(String symbol, String interval, int limit) {
        try {
            String url = String.format("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
                    symbol, interval, limit);
            HttpResponse<String> resp = sendBinanceRequest(
                    HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
                    BINANCE_WEIGHT_KLINES);
            if (resp == null) return Collections.emptyList();

            String body = resp.body();
            if (!body.trim().startsWith("[")) return Collections.emptyList();
            JSONArray arr = new JSONArray(body);
            List<com.bot.TradingCore.Candle> list = new ArrayList<>(arr.length());
            for (int i = 0; i < arr.length(); i++) {
                JSONArray k = arr.getJSONArray(i);
                list.add(new com.bot.TradingCore.Candle(
                        k.getLong(0), Double.parseDouble(k.getString(1)),
                        Double.parseDouble(k.getString(2)), Double.parseDouble(k.getString(3)),
                        Double.parseDouble(k.getString(4)), Double.parseDouble(k.getString(5)),
                        k.length() > 7 ? Double.parseDouble(k.getString(7)) : 0.0, k.getLong(6)));
            }
            return list;
        } catch (Exception e) { return Collections.emptyList(); }
    }

    public CompletableFuture<List<com.bot.TradingCore.Candle>> fetchKlinesAsync(String symbol, String interval, int limit) {
        return CompletableFuture.supplyAsync(() -> fetchKlinesDirect(symbol, interval, limit), fetchPool);
    }

    // ══════════════════════════════════════════════════════════════
    //  VOLUME DELTA
    // ══════════════════════════════════════════════════════════════

    public double getRawDelta(String symbol) { return deltaBuffer.getOrDefault(symbol, 0.0); }

    public double getNormalizedDelta(String symbol) {
        double d = deltaBuffer.getOrDefault(symbol, 0.0);
        if (d == 0.0) return 0.0;
        double absMax = deltaBuffer.values().stream().mapToDouble(Math::abs).max().orElse(1.0);
        return Math.max(-1.0, Math.min(1.0, d / (absMax + 1e-9)));
    }

    // ══════════════════════════════════════════════════════════════
    //  WEBSOCKET (aggTrade + bookTicker)
    // ══════════════════════════════════════════════════════════════

    public void connectWs(String pair) { connectWsInternal(pair); }

    private void connectWsInternal(String pair) {
        try {
            // [v12.0] Combined stream: aggTrade + bookTicker in ONE connection
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

    // [v30] Guard against concurrent reconnects for the same pair
    private final Set<String> reconnectingPairs = ConcurrentHashMap.newKeySet();

    private void reconnectWs(String pair) {
        if (!reconnectingPairs.add(pair)) return; // already reconnecting
        WebSocket old = wsMap.remove(pair);
        if (old != null) {
            try { old.sendClose(WebSocket.NORMAL_CLOSURE, "reconnecting"); } catch (Exception ignored) {}
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

        // [v29] VDA: 10s micro-window acceleration
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

        // [v29] RT-CVD: real-time from aggTrade, resets on 15m boundary
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

        if (ENABLE_EARLY_TICK) {
            com.bot.DecisionEngineMerged.TradeIdea et = generateEarlyTickSignal(pair, price, ts);
            if (et != null && filterEarlySignal(et)) {
                // [v17.0 §3] STRICT FORECAST GATE for EARLY_TICK signals.
                // A mid-candle signal has no full 15m bar yet — ForecastEngine is the
                // primary macro context. If it disagrees or is neutral → DROP.
                boolean fcPasses = com.bot.DecisionEngineMerged.forecastPassesEarlyTickGate(
                        et.forecast, et.side == com.bot.TradingCore.Side.LONG);
                if (!fcPasses) {
                    // Forecast neutral/opposite — do not send this EARLY_TICK
                    return;
                }

                // [v17.0 §2] SIGNAL BUFFER — collect into per-pair buffer, flush best on timer.
                // Instead of sending immediately (which caused signal spam during vol spikes),
                // we store the candidate and let earlyTickFlush() pick the top signal per pair
                // within a 1.5s collection window.
                earlyTickBuffer.merge(pair, et, (existing, candidate) ->
                        candidate.probability > existing.probability ? candidate : existing);
                // Flush is handled by the earlyTickFlusher scheduled task (see constructor).
            }
        }
    }

    // [v29+v30+v34] EARLY TICK — rewritten with exhaustion guard + VDA + correct conf floor
    // [v34.0] Category-aware velocity threshold: TOP coins (BTC/ETH) move slower in %
    private com.bot.DecisionEngineMerged.TradeIdea generateEarlyTickSignal(String symbol, double price, long ts) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        Deque<Double> vq = tickVolumeDeque.get(symbol);
        if (dq == null || dq.size() < 30 || vq == null || vq.size() < 30) return null;
        List<Double> buf    = new ArrayList<>(dq);
        List<Double> volBuf = new ArrayList<>(vq);
        int n = buf.size();

        double move = buf.get(n - 1) - buf.get(n - 22);
        double avg  = buf.stream().mapToDouble(Double::doubleValue).average().orElse(price);
        double vel  = Math.abs(move) / (avg + 1e-9);

        // [v34.0] Category-aware velocity threshold
        // TOP coins (BTC/ETH) have ~3-5x lower % moves than ALTs
        // Old: flat 0.0018 → BTC almost never triggered
        // New: TOP=0.0008, ALT=0.0015, MEME=0.0020
        com.bot.DecisionEngineMerged.CoinCategory etCat = categorizePair(symbol);
        double velThreshold = switch (etCat) {
            case TOP  -> 0.0008;  // BTC 0.08% move = significant
            case ALT  -> 0.0015;  // ALT 0.15% = meaningful
            case MEME -> 0.0020;  // MEME needs stronger signal (noisy)
        };
        if (vel < velThreshold) return null;
        boolean up = move > 0;

        double atrV = getAtr(symbol);
        if (atrV <= 0) atrV = price * 0.005;

        // EXHAUSTION GUARD: if already > 2.5×ATR from base → tail, not start
        double recentBase = up
                ? buf.subList(Math.max(0, n - 40), n - 1).stream().mapToDouble(Double::doubleValue).min().orElse(price)
                : buf.subList(Math.max(0, n - 40), n - 1).stream().mapToDouble(Double::doubleValue).max().orElse(price);
        double moveFromBase  = Math.abs(price - recentBase);
        double tickRangeHigh = buf.subList(Math.max(0, n - 40), n).stream().mapToDouble(Double::doubleValue).max().orElse(price);
        double tickRangeLow  = buf.subList(Math.max(0, n - 40), n).stream().mapToDouble(Double::doubleValue).min().orElse(price);
        double exhaustThresh = Math.max(atrV, (tickRangeHigh - tickRangeLow) * 0.60);
        if (moveFromBase > exhaustThresh * 2.5) return null;

        // Acceleration check
        // [v34.0] TOP coins have smoother moves — lower acceleration threshold
        double accelThreshold = etCat == com.bot.DecisionEngineMerged.CoinCategory.TOP ? 1.15 : 1.35;
        double m1 = buf.get(n / 2 - 1) - buf.get(0);
        double m2 = buf.get(n - 1) - buf.get(n / 2);
        if (!(Math.abs(m2) > Math.abs(m1) * accelThreshold)) return null;

        // VDA: must not actively disagree
        double vda = vdaScoreMap.getOrDefault(symbol, 0.0);
        if (Math.abs(vda) > 0.30 && ((up && vda < 0) || (!up && vda > 0))) return null;

        // Volume spike
        // [v34.0] TOP coins: lower volume spike threshold (institutional flow is steadier)
        double volSpikeThresh = etCat == com.bot.DecisionEngineMerged.CoinCategory.TOP ? 1.15 : 1.35;
        int vw = Math.min(30, volBuf.size());
        double avgVol = volBuf.subList(0, vw - 5).stream().mapToDouble(Double::doubleValue).average().orElse(0.001);
        double recVol = volBuf.subList(vw - 5, vw).stream().mapToDouble(Double::doubleValue).average().orElse(0);
        if (recVol < avgVol * volSpikeThresh) return null;

        // Tick streak
        int streak = 0;
        for (int i = n - 1; i >= Math.max(1, n - 5); i--) {
            if ((buf.get(i) >= buf.get(i - 1)) == up) streak++; else break;
        }
        if (streak < 2) return null;

        // Confidence: starts at 66 (raised from 62 for manual trading quality)
        // [v34.0] EARLY_TICK confidence floor raised: 62 → 66.
        // At 62%, signal is barely above noise. Manual trader needs 3-5 seconds to
        // read Telegram, open exchange, place order. By then, micro-impulse is over.
        // At 66%+ the move has enough structure to survive the 5s execution delay.
        // [v34.0] Velocity multiplier scaled by category (TOP moves are smaller but significant)
        double velMultiplier = switch (etCat) {
            case TOP  -> 6000;  // BTC 0.1% vel → +6 conf
            case ALT  -> 3500;  // ALT 0.2% vel → +7 conf
            case MEME -> 2500;  // MEME 0.3% vel → +7.5 conf
        };
        double conf = 66.0
                + Math.min(8.0,  vel * velMultiplier)
                + Math.min(5.0,  Math.abs(vda) * 15)
                + (((up && vda > 0.15) || (!up && vda < -0.15)) ? 3.0 : 0.0);
        conf = Math.min(84.0, conf);

        return new com.bot.DecisionEngineMerged.TradeIdea(
                symbol,
                up ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT,
                price,
                up ? price - atrV * 1.4 : price + atrV * 1.4,
                up ? price + atrV * 3.0 : price - atrV * 3.0,
                conf,
                new java.util.ArrayList<>(java.util.List.of(
                        "EARLY_TICK", up ? "UP" : "DN",
                        String.format("vel=%.2e", vel),
                        String.format("vda=%+.2f", vda),
                        "stk=" + streak)));
    }

    private boolean filterEarlySignal(com.bot.DecisionEngineMerged.TradeIdea sig) {
        boolean isLong = sig.side == com.bot.TradingCore.Side.LONG;
        double rs = relStrengthHistory.getOrDefault(sig.symbol, new java.util.concurrent.ConcurrentLinkedDeque<>())
                .stream().mapToDouble(Double::doubleValue).average().orElse(0.5);
        double gicWeight = gic.getFilterWeight(sig.symbol, isLong, rs, detectSector(sig.symbol));
        double minEarlyGicWeight = sig.probability >= 76.0 ? 0.45 : 0.55;
        if (gicWeight < minEarlyGicWeight) return false;
        if (!isc.allowSignal(sig)) return false;
        // [v16.0 FIX] Use plain effectiveMinConfidence (no symbolBoost stacking in early path)
        return sig.probability >= isc.getEffectiveMinConfidence();
    }

    /**
     * [v17.0 §2] EARLY TICK BUFFER FLUSH — called every 2 seconds by wsWatcher.
     *
     * Drains earlyTickBuffer atomically. Sorts remaining candidates by probability (desc),
     * then dispatches each one: sends to Telegram, registers with ISC, tracks in BotMain.
     *
     * [v35.0] MAX 3 signals per flush — prevents 9 signals in 2 minutes.
     * When BTC moves, all ALTs correlate → 30 pairs fire simultaneously.
     * Sending all 30 to Telegram = information overload. Top 3 is plenty.
     */
    private static final int MAX_EARLY_PER_FLUSH = 3;

    private void flushEarlyTickBuffer() {
        if (earlyTickBuffer.isEmpty()) return;

        // Drain atomically — swap out the entire map contents
        List<com.bot.DecisionEngineMerged.TradeIdea> candidates = new ArrayList<>();
        for (String pair : new ArrayList<>(earlyTickBuffer.keySet())) {
            com.bot.DecisionEngineMerged.TradeIdea idea = earlyTickBuffer.remove(pair);
            if (idea != null) candidates.add(idea);
        }
        if (candidates.isEmpty()) return;

        // Sort by probability descending — highest conviction goes first
        candidates.sort(Comparator.comparingDouble(
                (com.bot.DecisionEngineMerged.TradeIdea i) -> i.probability).reversed());

        // [v35.0] Cap to top-N best signals per flush
        if (candidates.size() > MAX_EARLY_PER_FLUSH) {
            candidates = new ArrayList<>(candidates.subList(0, MAX_EARLY_PER_FLUSH));
        }

        // [v17.0 §4] Apply REDUCED_RISK flag from DrawdownManager
        String rrFlag = isc.getReducedRiskFlag();
        double rrMult = isc.getReducedRiskMultiplier();

        for (com.bot.DecisionEngineMerged.TradeIdea et : candidates) {
            // Re-check ISC (state may have changed since buffering)
            if (!isc.isSymbolAvailable(et.symbol)) continue;

            // [v34.0] EARLY_TICK hourly rate limit — max 3 per pair per hour
            if (earlyTickHourlyLimitReached(et.symbol)) continue;

            // [v35.0] VOLUME FILTER for EARLY_TICK — blocks trash coins.
            // Problem: RIVERUSDT, SIRENUSDT, VVVUSDT pass EARLY_TICK without
            // any liquidity check. Their $2M daily volume = untradeable.
            // Fix: same MIN_VOL check as processPair.
            Double etVol = volume24hUSD.get(et.symbol);
            if (etVol != null) {
                com.bot.DecisionEngineMerged.CoinCategory etCatCheck = categorizePair(et.symbol);
                double minVol = switch (etCatCheck) {
                    case TOP  -> MIN_VOL_TOP_USD;
                    case ALT  -> MIN_VOL_ALT_USD;
                    case MEME -> MIN_VOL_MEME_USD;
                };
                if (etVol < minVol) continue; // Skip illiquid pair
            }

            // [v34.0 FIX] Categorize BEFORE using cat
            com.bot.DecisionEngineMerged.CoinCategory cat =
                    et.category != null ? et.category : categorizePair(et.symbol);
            String sector = detectSector(et.symbol);

            // Apply REDUCED_RISK flag to signal if in drawdown mode
            com.bot.DecisionEngineMerged.TradeIdea finalEt = et;
            if (!rrFlag.isEmpty()) {
                List<String> nf = new ArrayList<>(et.flags);
                nf.removeIf(f -> f.startsWith("SIZE="));
                double baseSize = 20.0;
                for (String f : et.flags) {
                    if (f.startsWith("SIZE=")) {
                        try { baseSize = Double.parseDouble(f.replace("SIZE=", "").replace("$", "").split(" ")[0]); }
                        catch (Exception ignored) {}
                    }
                }
                nf.add(String.format("SIZE=%.1f$ %s", baseSize * rrMult, rrFlag));
                finalEt = rebuildIdea(et, et.probability, nf);
            }

            // [v35.0] CLEAN DISPATCH — toTelegramString() is now self-contained.
            // No external header needed. Just send the signal directly.
            bot.sendMessageAsync(finalEt.toTelegramString());
            earlySignals.incrementAndGet();
            recordEarlyTickSent(finalEt.symbol);
            registerApprovedSignal(finalEt, finalEt.symbol, cat, sector,
                    System.currentTimeMillis(), true);
        }
    }

    private void registerApprovedSignal(com.bot.DecisionEngineMerged.TradeIdea idea,
                                        String pair,
                                        com.bot.DecisionEngineMerged.CoinCategory cat,
                                        String sector,
                                        long approvedAtMs,
                                        boolean trackLifecycle) {
        isc.registerSignal(idea);
        decisionEngine.confirmSignal(idea.symbol, idea.side, idea.price, approvedAtMs);
        correlationGuard.register(pair, idea.side, cat, sector);
        if (trackLifecycle) {
            com.bot.BotMain.trackSignal(idea);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  FUNDING + OI
    // ══════════════════════════════════════════════════════════════

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

            // [v10.0] Apply funding to ALL pairs from bulk response (no extra requests)
            for (String pair : cachedPairs) {
                double fr = rates.getOrDefault(pair, 0.0);
                decisionEngine.updateFundingOI(pair, fr, 0, 0, 0); // funding from bulk, OI below
            }

            // [v10.0] FIX: OI only for top 20 by volume (was 100 = 200 requests every 5 min)
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
        } catch (Exception e) { System.out.println("[FR] Error: " + e.getMessage()); }
    }

    private void fetchAndUpdateOI(String symbol, double fr) {
        try {
            // [v10.0] Only current OI, skip expensive oiHist (saves 1 request per pair)
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

    // ══════════════════════════════════════════════════════════════
    //  REFRESH VOLUME + PAIRS
    // ══════════════════════════════════════════════════════════════

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
        } catch (Exception e) { System.out.println("[VOL24H] Error: " + e.getMessage()); }
    }

    public Set<String> getTopSymbolsSet(int limit) {
        try {
            Set<String> binancePairs = getBinanceSymbolsFutures();
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
                    if (binancePairs.contains(pair)) top.add(pair);
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
                    top.add(p);
                }
            }
            System.out.println("[PAIRS] Loaded " + top.size());
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

    // ══════════════════════════════════════════════════════════════
    // [v34.0] DYNAMIC COIN CATEGORIZATION
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
    // ══════════════════════════════════════════════════════════════

    // [v34.0] Known seeds — these NEVER change category regardless of volume
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
    // [v34.0] Meme pattern keywords — auto-detect new meme coins
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

    private com.bot.DecisionEngineMerged.TradeIdea rebuildIdea(com.bot.DecisionEngineMerged.TradeIdea src, double p, List<String> f) {
        // Передаём адаптивные TP-множители из оригинала — они не должны теряться при перестройке
        return new com.bot.DecisionEngineMerged.TradeIdea(
                src.symbol, src.side, src.price, src.stop, src.take, src.rr, p, f,
                src.fundingRate, src.fundingDelta, src.oiChange, src.htfBias, src.category,
                src.forecast,
                src.tp1Mult, src.tp2Mult, src.tp3Mult);
    }

    // ══════════════════════════════════════════════════════════════
    //  [ДЫРА №1] CVD — Cumulative Volume Delta
    //  takerBuyBaseVolume приходит в каждом kline — уже в Candle.
    //  Накапливаем за CVD_LOOKBACK_1M свечей (по умолчанию 90×1m).
    //  Нормализуем: [-1..+1], где +1 = 100% объём — покупки, -1 = 100% продажи.
    // ══════════════════════════════════════════════════════════════

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

    // ══════════════════════════════════════════════════════════════
    //  [ДЫРА №4] SESSION WEIGHT — рыночные сессии по UTC
    //  Сигналы в NY/London несут в 1.5× больше веса чем сигналы
    //  в 3 ночи UTC (азиатская сессия с ложными пробоями).
    // ══════════════════════════════════════════════════════════════

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

    // ══════════════════════════════════════════════════════════════
    //  [ДЫРА №2] LIQUIDATION HEATMAP
    //  Подписка на публичный WebSocket поток Binance: !forceOrder@arr
    //  Собираем ликвидации в ценовые уровни (bucket = 0.1% от цены).
    //  getLiquidationScore() возвращает 0..1 — насколько сильный
    //  магнит ликвидаций находится вблизи текущей цены.
    // ══════════════════════════════════════════════════════════════

    private void connectLiquidationStream() {
        try {
            http.newWebSocketBuilder()
                    .buildAsync(URI.create("wss://fstream.binance.com/ws/!forceOrder@arr"),
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
                        System.out.println("[LIQ] ✅ Liquidation stream connected");
                    })
                    .exceptionally(ex -> {
                        System.out.println("[LIQ] Connect failed: " + ex.getMessage());
                        udsExecutor.schedule(this::connectLiquidationStream, 30, TimeUnit.SECONDS);
                        return null;
                    });
        } catch (Exception e) {
            System.out.println("[LIQ] Error: " + e.getMessage());
        }
    }

    private void processLiquidationEvent(org.json.JSONObject event) {
        try {
            org.json.JSONObject o = event.optJSONObject("o");
            if (o == null) return;
            String symbol   = o.optString("s");
            double avgPrice = o.optDouble("ap", 0);
            double qty      = o.optDouble("q", 0);
            String side     = o.optString("S"); // BUY = short был ликвидирован, SELL = long был
            if (avgPrice <= 0 || qty <= 0) return;

            double notional = avgPrice * qty;
            if (notional < LIQ_MIN_NOTIONAL) return; // игнорируем мелкие

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
            System.out.printf("[Stats] cache=%.1f%% early=%d liq=%d corr=%d stale=%d profit=%d e=%d opt=%d vpoc=%d fin=%d isc=%d q=+%.0f ws=%.0f%% msgs=%d bal=$%.2f%n",
                    100.0*hits/total, earlySignals.get(), blockedLiq.get(), blockedCorr.get(),
                    blockedStaleRt.get(), blockedProfit.get(), blockedEarlyConf.get(),
                    blockedOptConf.get(), blockedVpoc.get(), blockedFinalConf.get(),
                    blockedIsc.get(), cycleQualityPenalty, lastCycleWsCoverage * 100.0,
                    wsMessageCount.get(), accountBalance);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  ACCESSORS
    // ══════════════════════════════════════════════════════════════

    public double getAtr(String symbol) {
        CachedCandles cc = candleCache.get(symbol + "_15m");
        if (cc == null || cc.candles.isEmpty()) return 0;
        return atr(cc.candles, 14);
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
        return volume24hUSD.entrySet().stream()
                .filter(e -> e.getValue() >= MIN_VOL_ALT_USD)
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
    /** [MODULE 2 v33] Returns OFV score for a pair: >0 bullish flow, <0 bearish. 0 if no data. */
    public double getOfvScore(String pair) {
        Double s = ofvScoreMap.get(pair);
        return s != null ? s : 0.0;
    }
    public List<String> getScanUniverseSnapshot(int limit) {
        List<String> sorted = new ArrayList<>(cachedPairs);
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
    public com.bot.SignalOptimizer getOptimizer()           { return optimizer; }
    public com.bot.InstitutionalSignalCore getSignalCore()  { return isc; }
    public com.bot.PumpHunter getPumpHunter()               { return pumpHunter; }
    public com.bot.GlobalImpulseController getGIC()         { return gic; }
    public Map<String, Deque<Double>> getTickDeque()        { return tickPriceDeque; }

    // ══════════════════════════════════════════════════════════════
    //  STATIC MATH UTILS
    // ══════════════════════════════════════════════════════════════

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
    public static boolean detectLiquiditySweep(List<com.bot.TradingCore.Candle> c) { return com.bot.DecisionEngineMerged.detectLiquiditySweep(c); }

    private static String hmacSHA256(String secret, String data) throws Exception {
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] hash = mac.doFinal(data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    // ══════════════════════════════════════════════════════════════
    //  INNER CLASSES
    // ══════════════════════════════════════════════════════════════

    // [v28.0] PATCH #9: Multi-level OrderbookSnapshot replacing single bid/ask.
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

        // [v28.0] OBI uses 5-level depth when available, falls back to L1
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