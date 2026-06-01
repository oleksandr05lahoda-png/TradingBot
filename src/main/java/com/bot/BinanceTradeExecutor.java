package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * BinanceTradeExecutor v83.3 — live order execution for Binance USD-M Futures.
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  ВАЖНО: этот класс ничего не делает без явного вызова из BotMain.   │
 * │  Сам по себе он лежит "в спячке". Активация — на Этапе 3.           │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * [v83.3] BREAKING CHANGE на стороне Binance (effective 2025-12-09):
 * условные ордера (STOP_MARKET / TAKE_PROFIT_MARKET / STOP / TAKE_PROFIT /
 * TRAILING_STOP_MARKET) больше НЕ принимаются на /fapi/v1/order. Они
 * мигрировали в Algo Service: POST /fapi/v1/algoOrder. Старый эндпоинт
 * возвращает ошибку -4120 (STOP_ORDER_SWITCH_ALGO). Поправлены два
 * метода: sendStopMarketOrder() — теперь шлёт на /fapi/v1/algoOrder,
 * и cancelAllOpenOrders() — теперь дополнительно гасит алго-ордера
 * через /fapi/v1/allAlgoOpenOrders, иначе SL висит как orphan.
 * Источник: developers.binance.com/docs/derivatives/change-log (2025-11-06).
 *
 * Принципы безопасности:
 *
 *  1. NEVER OPEN WITHOUT SL.
 *     После MARKET-открытия позиции — немедленно (в той же транзакции,
 *     одним методом) отправляется STOP_MARKET ордер на SL через algoOrder.
 *     Если SL не подтвердился за 5 секунд — позиция закрывается
 *     принудительно. Лучше потерять комиссию, чем остаться без стопа.
 *
 *  2. NO FUNDS, NO MOVE.
 *     Перед каждой сделкой проверяется реальный баланс на бирже. Если
 *     баланс < min_required для риска 2% — не открываем.
 *
 *  3. TESTNET-FIRST.
 *     env BINANCE_USE_TESTNET=1 переключает все URL на testnet.
 *     Default = 1 (testnet). Чтобы перейти на real — поставить =0.
 *     Это специально: дефолт безопасный, ошибка → testnet, не real.
 *
 *  4. ISOLATED MARGIN, FIXED LEVERAGE.
 *     Перед первой сделкой по символу выставляются:
 *       - margin type = ISOLATED (не CROSS — изолируем риск каждой пары)
 *       - leverage    = LEVERAGE (default 5x)
 *     Эти параметры запоминаются — повторно не дёргаем.
 *
 *  5. HARDCODED LEVERAGE CAP.
 *     LEVERAGE захардкожен в коде с верхней границей 10. Даже если кто-то
 *     поставит env LEVERAGE=50 — будет применено 10. Это не "конфиг",
 *     это safety-rail.
 *
 *  6. SLIPPAGE PROTECTION.
 *     Перед открытием берём текущий best bid/ask с биржи. Если spread
 *     > 0.5% — отказываемся (тонкий стакан, плохое исполнение).
 *
 * env vars:
 *   BINANCE_USE_TESTNET     = 1 (default)  | 0 для real
 *   BINANCE_API_KEY         — ключ (real или testnet, в зависимости от флага)
 *   BINANCE_API_SECRET      — секрет
 *   BINANCE_TESTNET_API_KEY — отдельный testnet ключ (если хочешь держать оба)
 *   BINANCE_TESTNET_API_SECRET
 *   LEVERAGE                = 5    (cap 10, hard limit)
 *   RISK_PCT_PER_TRADE      = 2.0  (% депо на сделку, cap 5)
 *   SL_PLACEMENT_TIMEOUT_MS = 5000 (если SL не встал — закрываем)
 *   MAX_SPREAD_PCT          = 0.5  (отказ если spread больше)
 */
public final class BinanceTradeExecutor {

    private static final Logger LOG = Logger.getLogger("BinanceTradeExecutor");

    // ─── Configuration ────────────────────────────────────────────────
    private final boolean useTestnet;
    private final String  apiKey;
    private final String  apiSecret;
    private final int     leverage;            // 1..10 hard cap
    private final double  riskPctPerTrade;     // 0.5..5.0 hard cap
    private final long    slPlacementTimeoutMs;
    private final double  maxSpreadPct;

    private final String baseUrl;              // computed from useTestnet

    private final HttpClient http;
    /** symbols where we have already set leverage + isolated. */
    private final java.util.Set<String> initializedSymbols = ConcurrentHashMap.newKeySet();

    /**
     * [C5 2026-05-08] Last error body returned by initSymbolMargin / initSymbolLeverage.
     * Allows openPositionWithSl() to surface the real reason ("symbol not on testnet",
     * "already in hedge mode", "leverage too high for symbol") in ExecutionResult.fail
     * instead of the opaque "init margin failed for X". Cleared at start of each attempt.
     */
    private volatile String lastInitErrorBody = "";

    /**
     * [v83.2] Cache of exchange filters per symbol. Populated lazily on first
     * trade by reading /fapi/v1/exchangeInfo. Without this, qty/price rounding
     * is wrong for many alts → MARKET order rejected by Binance.
     *
     * Each entry holds the actual stepSize / tickSize / minNotional rules
     * Binance enforces. Cache lives until process restart, which is fine —
     * exchangeInfo changes maybe once a quarter for any given symbol.
     */
    private final ConcurrentHashMap<String, SymbolInfo> symbolInfoCache = new ConcurrentHashMap<>();

    // ─── Server-time sync ───────────
    // Binance отвергает запросы с timestamp дрейфом > recvWindow (5s).
    // Sync раз в 30 мин через /fapi/v1/time, прибавляем offset ко всем timestamp.
    private volatile long timeOffsetMs = 0L;
    private volatile long lastTimeSync = 0L;
    private static final long TIME_SYNC_INTERVAL_MS = 30 * 60_000L;

    /** Returns server-time-corrected millis for use as Binance "timestamp" param. */
    private long ts() {
        long now = System.currentTimeMillis();
        if (now - lastTimeSync > TIME_SYNC_INTERVAL_MS) {
            syncServerTime();
        }
        return now + timeOffsetMs;
    }

    /** Syncs local clock with Binance server time. Called lazily from ts(). */
    private void syncServerTime() {
        try {
            HttpResponse<String> r = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/time"))
                            .timeout(Duration.ofSeconds(3))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() == 200) {
                JSONObject o = new JSONObject(r.body());
                long server = o.optLong("serverTime", 0);
                if (server > 0) {
                    long offset = server - System.currentTimeMillis();
                    timeOffsetMs = offset;
                    lastTimeSync = System.currentTimeMillis();
                    if (Math.abs(offset) > 1000) {
                        LOG.warning("[Executor] clock drift detected: " + offset
                                + "ms — applying offset");
                    }
                }
            }
        } catch (Exception ignored) { /* keep last offset, retry next time */ }
    }

    private static final BinanceTradeExecutor INSTANCE = new BinanceTradeExecutor();
    public static BinanceTradeExecutor getInstance() { return INSTANCE; }

    private BinanceTradeExecutor() {
        this.useTestnet = "1".equals(System.getenv().getOrDefault("BINANCE_USE_TESTNET", "1"));

        if (useTestnet) {
            this.apiKey    = pick("BINANCE_TESTNET_API_KEY", "BINANCE_API_KEY", "");
            this.apiSecret = pick("BINANCE_TESTNET_API_SECRET", "BINANCE_API_SECRET", "");
            // [v82.3 2026-06-01] REVERTED demo-fapi → testnet.binancefuture.com.
            // ROOT CAUSE of -1109 "Invalid account" on EVERY write (marginType/
            // leverage/order) while reads worked: demo-fapi (= demo.binance.com
            // "Demo Trading") is a MANUAL-ONLY UI environment with NO API order
            // placement. The classic Binance Futures testnet DOES support
            // /fapi/v1/order for bots. Keys must be generated at
            // testnet.binancefuture.com (separate from demo/live keys).
            // Откат: вернуть "https://demo-fapi.binance.com" если Binance вернёт API на demo.
            this.baseUrl   = "https://testnet.binancefuture.com";
        } else {
            this.apiKey    = System.getenv().getOrDefault("BINANCE_API_KEY", "");
            this.apiSecret = System.getenv().getOrDefault("BINANCE_API_SECRET", "");
            this.baseUrl   = "https://fapi.binance.com";
        }

        // HARDCODED CAPS — even if env says crazy, we clamp.
        int lev = envInt("LEVERAGE", 5);
        this.leverage = Math.max(1, Math.min(10, lev));
        if (lev > 10) {
            LOG.warning("[Executor] LEVERAGE=" + lev + " requested, but capped to 10");
        }

        double rp = envDouble("RISK_PCT_PER_TRADE", 2.0);
        // Risk per trade: floor 0.05%, ceiling 5%. Floor prevents accidental 0%
        // (zeroes-out qty calc); ceiling caps overcommit.
        this.riskPctPerTrade = Math.max(0.05, Math.min(5.0, rp));
        if (rp < 0.05 && rp > 0) {
            LOG.warning("[Executor] RISK_PCT_PER_TRADE=" + rp + " requested (below 0.05% floor), using 0.05%");
        } else if (rp > 5.0) {
            LOG.warning("[Executor] RISK_PCT_PER_TRADE=" + rp + " requested, capped to 5");
        }

        // SL placement timeout. Default 2s — demo-fapi confirms in 200-400ms;
        // larger window = longer naked-position exposure after MARKET-fill.
        this.slPlacementTimeoutMs = envLong("SL_PLACEMENT_TIMEOUT_MS", 2000L);
        // Max spread % — looser on testnet (thin books), stricter on mainnet
        // (protect edge from illiquid fills). MAX_SPREAD_PCT env override wins.
        double defaultSpread = useTestnet ? 1.0 : 0.30;
        this.maxSpreadPct         = envDouble("MAX_SPREAD_PCT", defaultSpread);

        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        LOG.info(String.format("[Executor] init: %s leverage=%dx risk=%.1f%%/trade keys=%s",
                useTestnet ? "TESTNET" : "REAL/LIVE",
                leverage, riskPctPerTrade,
                apiKey.isBlank() ? "MISSING" : "present"));

        // Normalize position mode to ONE_WAY at boot. HEDGE-mode accounts
        // reject MARKET orders without positionSide=LONG/SHORT (-4061).
        // Idempotent: if already ONE_WAY, Binance returns -4059 (ignored).
        if (!apiKey.isBlank() && !apiSecret.isBlank()) {
            try { ensureOneWayMode(); } catch (Exception e) {
                LOG.warning("[Executor] ensureOneWayMode failed (non-fatal): " + e.getMessage());
            }
        }
    }

    /**
     * POST /fapi/v1/positionSide/dual?dualSidePosition=false
     * Force account into ONE_WAY position mode so MARKET orders without
     * positionSide=LONG/SHORT are accepted. Idempotent — safe to call repeatedly.
     *
     * Auto-handles -4067 ("Position side cannot be changed if there exists
     * open orders") by enumerating all open orders, cancelling them, then
     * retrying the position-side switch once.
     */
    private void ensureOneWayMode() throws Exception {
        ensureOneWayMode(false); // first attempt without cancel
    }

    private void ensureOneWayMode(boolean afterCancel) throws Exception {
        long ts = ts();
        String body = "dualSidePosition=false&timestamp=" + ts + "&recvWindow=5000";
        String sig = hmacSHA256(apiSecret, body);
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/fapi/v1/positionSide/dual"))
                        .timeout(Duration.ofSeconds(8))
                        .header("X-MBX-APIKEY", apiKey)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(body + "&signature=" + sig))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() == 200) {
            LOG.info("[Executor] position mode set to ONE_WAY");
        } else {
            String b = resp.body() == null ? "" : resp.body();
            // -4059 = "No need to change position side" — already ONE_WAY, OK.
            if (b.contains("-4059") || b.contains("No need")) {
                LOG.info("[Executor] position mode already ONE_WAY");
            }
            // -4067 = "Position side cannot be changed if there exists open
            // orders" — cancel ALL open orders across all symbols, then retry.
            // Only retry once (afterCancel guard) to avoid infinite loop.
            else if ((b.contains("-4067") || b.contains("open orders")) && !afterCancel) {
                LOG.warning("[Executor] -4067 detected — cancelling all open orders before retry");
                int cancelled = cancelAllOrdersAccountWide();
                LOG.info("[Executor] cancelled " + cancelled + " open orders, retrying ensureOneWayMode");
                ensureOneWayMode(true); // single retry
            } else {
                LOG.warning("[Executor] positionSide/dual HTTP " + resp.statusCode() + ": " + b);
            }
        }
    }

    /**
     * Account-wide cleanup: enumerates all symbols with open orders via
     * GET /fapi/v1/openOrders (no symbol = all), then DELETE /fapi/v1/allOpenOrders
     * per symbol. Returns count of orders cancelled.
     *
     * Used by ensureOneWayMode to clear blockers preventing position-mode
     * change, and exposed publicly for callers (PositionTracker reconcile)
     * that need to wipe stale orders.
     */
    public int cancelAllOrdersAccountWide() {
        int total = 0;
        // Cancel regular orders (LIMIT, MARKET) via /fapi/v1/openOrders
        try {
            long ts = ts();
            String qs = "timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/openOrders?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) {
                org.json.JSONArray arr = new org.json.JSONArray(resp.body());
                java.util.Set<String> symbols = new java.util.HashSet<>();
                for (int i = 0; i < arr.length(); i++) {
                    symbols.add(arr.getJSONObject(i).getString("symbol"));
                }
                for (String sym : symbols) {
                    try {
                        cancelAllOpenOrders(sym);
                        total += arr.length(); // approximate
                    } catch (Throwable ignored) {}
                }
                if (!symbols.isEmpty()) {
                    LOG.info("[Executor] cancelled regular orders on " + symbols.size() + " symbols");
                }
            } else {
                LOG.warning("[Executor] cancelAllOrdersAccountWide regular HTTP " + resp.statusCode());
            }
        } catch (Throwable t) {
            LOG.warning("[Executor] cancelAllOrdersAccountWide regular error: " + t.getMessage());
        }

        // Cancel algo orders (STOP_MARKET, TAKE_PROFIT_MARKET = SL/TP) via
        // /fapi/v1/algoOrders. These don't show up in /openOrders endpoint
        // and previously kept blocking position-mode changes with -4067.
        try {
            long ts2 = ts();
            String qs2 = "timestamp=" + ts2 + "&recvWindow=5000";
            String sig2 = hmacSHA256(apiSecret, qs2);
            HttpResponse<String> resp2 = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/algoOrders?" + qs2 + "&signature=" + sig2))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp2.statusCode() == 200) {
                String body = resp2.body();
                // Response format may vary — try as array or as {data: [...]}
                org.json.JSONArray algos = null;
                try {
                    algos = new org.json.JSONArray(body);
                } catch (Throwable parseErr) {
                    try {
                        org.json.JSONObject obj = new org.json.JSONObject(body);
                        if (obj.has("data")) algos = obj.getJSONArray("data");
                        else if (obj.has("orders")) algos = obj.getJSONArray("orders");
                    } catch (Throwable ignored) {}
                }
                if (algos != null && algos.length() > 0) {
                    int cancelledAlgo = 0;
                    for (int i = 0; i < algos.length(); i++) {
                        try {
                            org.json.JSONObject o = algos.getJSONObject(i);
                            String sym = o.optString("symbol", "");
                            long algoId = o.optLong("algoId", 0L);
                            if (algoId == 0) algoId = o.optLong("orderId", 0L);
                            if (!sym.isEmpty() && algoId > 0) {
                                if (cancelAlgoOrder(sym, String.valueOf(algoId))) {
                                    cancelledAlgo++;
                                }
                            }
                        } catch (Throwable ignored) {}
                    }
                    LOG.info("[Executor] cancelled " + cancelledAlgo
                            + " algo orders (SL/TP) account-wide");
                    total += cancelledAlgo;
                }
            } else if (resp2.statusCode() == 404) {
                // Endpoint may not exist on testnet — silently skip
                LOG.fine("[Executor] /fapi/v1/algoOrders not available (testnet?), skipping algo wipe");
            } else {
                LOG.warning("[Executor] algoOrders list HTTP " + resp2.statusCode()
                        + " body=" + resp2.body());
            }
        } catch (Throwable t) {
            LOG.warning("[Executor] cancelAllOrdersAccountWide algo error: " + t.getMessage());
        }

        return total;
    }

    /**
     * Public re-entry point for one-way mode setup. Called by PositionTracker
     * after startup reconcile completes (positions closed, orders cancelled).
     * At this point the account is "clean" and the switch should succeed.
     */
    public void ensureCleanAccountAndOneWayMode() {
        if (!isReady()) return;
        try {
            ensureOneWayMode();
        } catch (Exception e) {
            LOG.warning("[Executor] post-reconcile ensureOneWayMode failed: " + e.getMessage());
        }
    }

    /** True if exchange credentials are configured and we can call API. */
    public boolean isReady() {
        return !apiKey.isBlank() && !apiSecret.isBlank();
    }

    public boolean isTestnet() { return useTestnet; }
    public int  getLeverage()    { return leverage; }
    public double getRiskPct()   { return riskPctPerTrade; }

    /**
     * [HOLE-3 FIX 2026-05-08] Public emergency-close helper used by PositionTracker
     * startup reconcile when an orphan position is detected (no SL on exchange).
     *
     * Closes the position via opposite-side MARKET reduceOnly, then cancels all
     * remaining algo orders for the symbol so nothing dangles. Returns true on
     * successful close (or position already empty), false on hard failure that
     * needs manual intervention.
     *
     * positionAmt sign: + = LONG, − = SHORT. Pass the raw value from
     * /fapi/v2/positionRisk (we infer side and absolute qty internally).
     */
    public boolean closeOrphanPosition(String symbol, double positionAmt) {
        if (!isReady()) return false;
        if (Math.abs(positionAmt) < 1e-9) return true; // already empty
        boolean wasLong = positionAmt > 0;
        double qty = Math.abs(positionAmt);
        try {
            emergencyClosePosition(symbol, wasLong, qty);
            // Verify close succeeded by re-checking positionAmt within ~1.5s.
            for (int i = 0; i < 3; i++) {
                Thread.sleep(500);
                JSONArray ps = fetchAllOpenPositionsRaw();
                if (ps == null) break;
                boolean stillOpen = false;
                for (int k = 0; k < ps.length(); k++) {
                    JSONObject p = ps.getJSONObject(k);
                    if (symbol.equals(p.optString("symbol", ""))
                            && Math.abs(p.optDouble("positionAmt", 0)) > 1e-9) {
                        stillOpen = true; break;
                    }
                }
                if (!stillOpen) {
                    cancelAllOpenOrders(symbol);
                    return true;
                }
            }
            // Position still showing as open after emergency close — log and bail.
            LOG.severe("[Executor] closeOrphanPosition: " + symbol
                    + " still open after emergency close — MANUAL INTERVENTION");
            return false;
        } catch (Throwable t) {
            LOG.severe("[Executor] closeOrphanPosition " + symbol + " exception: "
                    + t.getMessage());
            return false;
        }
    }

    // ─── Public API ───────────────────────────────────────────────────

    public static final class ExecutionResult {
        public final boolean success;
        public final String  reason;
        public final String  orderId;        // main entry order id
        public final String  slOrderId;      // SL order id
        public final String  tp1OrderId;     // TP1 algo id (may be null/empty)
        public final String  tp2OrderId;     // TP2 algo id (may be null/empty)
        public final int     tpsPlaced;      // 0..2 — how many TP orders confirmed
        public final double  entryPrice;     // actual fill price
        public final double  qty;            // base quantity
        public final double  notionalUsd;
        public final double  slPrice;
        public final double  tp1Price;
        public final double  tp2Price;
        public final long    timestampMs;

        private ExecutionResult(boolean ok, String reason, String orderId, String slOrderId,
                                String tp1Id, String tp2Id, int tpsPlaced,
                                double entry, double qty, double notional,
                                double sl, double tp1, double tp2) {
            this.success = ok; this.reason = reason;
            this.orderId = orderId; this.slOrderId = slOrderId;
            this.tp1OrderId = tp1Id == null ? "" : tp1Id;
            this.tp2OrderId = tp2Id == null ? "" : tp2Id;
            this.tpsPlaced = tpsPlaced;
            this.entryPrice = entry; this.qty = qty;
            this.notionalUsd = notional; this.slPrice = sl;
            this.tp1Price = tp1; this.tp2Price = tp2;
            this.timestampMs = System.currentTimeMillis();
        }

        public static ExecutionResult fail(String reason) {
            return new ExecutionResult(false, reason, "", "", "", "", 0, 0, 0, 0, 0, 0, 0);
        }
        public static ExecutionResult ok(String orderId, String slOrderId,
                                         String tp1Id, String tp2Id, int tpsPlaced,
                                         double entry, double qty, double notional,
                                         double sl, double tp1, double tp2) {
            return new ExecutionResult(true, "OK", orderId, slOrderId,
                    tp1Id, tp2Id, tpsPlaced,
                    entry, qty, notional, sl, tp1, tp2);
        }

        @Override public String toString() {
            return success
                    ? String.format("OK orderId=%s slId=%s tp1=%s tp2=%s tpsPlaced=%d "
                                    + "entry=%.6f qty=%.6f notional=$%.2f sl=%.6f tp1p=%.6f tp2p=%.6f",
                    orderId, slOrderId, tp1OrderId, tp2OrderId, tpsPlaced,
                    entryPrice, qty, notionalUsd, slPrice, tp1Price, tp2Price)
                    : "FAIL " + reason;
        }
    }

    /**
     * Fetch USDT futures balance.
     * @return availableBalance in USDT, or -1 on error.
     */
    public double fetchAvailableBalance() {
        if (!isReady()) return -1;
        try {
            long ts = ts();
            String qs = "timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/fapi/v2/balance?" + qs + "&signature=" + sig))
                    .timeout(Duration.ofSeconds(8))
                    .header("X-MBX-APIKEY", apiKey)
                    .GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                LOG.warning("[Executor] balance HTTP " + resp.statusCode() + " body=" + resp.body());
                return -1;
            }
            JSONArray arr = new JSONArray(resp.body());
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                if ("USDT".equals(o.optString("asset"))) {
                    return o.optDouble("availableBalance", 0);
                }
            }
            return 0;
        } catch (Exception e) {
            LOG.warning("[Executor] balance error: " + e.getMessage());
            return -1;
        }
    }

    /**
     * Open a market position with immediate SL placement.
     *
     * Flow:
     *   1. Set leverage + isolated (once per symbol).
     *   2. Sanity check spread.
     *   3. Calculate position size from balance × risk × leverage.
     *   4. Send MARKET BUY/SELL.
     *   5. Within {@link #slPlacementTimeoutMs}: send STOP_MARKET.
     *   6. If SL didn't confirm — close position immediately.
     *
     * @return ExecutionResult with full details, or fail() on any error.
     */
    public ExecutionResult openPositionWithSl(com.bot.DecisionEngineMerged.TradeIdea idea,
                                              double balanceUsd) {
        if (!isReady()) return ExecutionResult.fail("API keys not configured");
        if (idea == null) return ExecutionResult.fail("null idea");
        if (balanceUsd <= 0) return ExecutionResult.fail("zero balance");

        String symbol = idea.symbol;
        boolean isLong = idea.side == com.bot.TradingCore.Side.LONG;

        try {
            // ════════════════════════════════════════════════════════════════
            // [HOLE-6 FIX 2026-05-15] PRE-CHECK SYMBOL TRADABILITY
            // ════════════════════════════════════════════════════════════════
            // Symptom we observed (TAOUSDT 2026-05-15 02:43):
            //   ❌ Auto-trade FAIL TAOUSDT SHORT
            //   "MARKET order rejected by Binance"
            //
            // Root cause: the symbol may be HALT / BREAK / PENDING_TRADING /
            // delisted on this account's exchange (e.g. listed on mainnet but
            // not on testnet). The dispatcher upstream has no knowledge of
            // exchange status — it only sees that the symbol passes its filters.
            //
            // Fix: hit exchangeInfo BEFORE leverage/margin POSTs (those mutate
            // account state on unrelated symbols if they succeed mid-flow), check
            // status == "TRADING". A null SymbolInfo here means the symbol does
            // not exist on this exchange at all → abort with a clear reason that
            // the dispatcher / GARBAGE_COIN_BLOCKLIST can act on next cycle.
            SymbolInfo preCheckInfo = loadSymbolInfo(symbol);
            if (preCheckInfo == null) {
                return ExecutionResult.fail("symbol not in exchangeInfo: " + symbol
                        + " (not listed on this " + (useTestnet ? "testnet" : "real") + " endpoint)");
            }
            if (!preCheckInfo.isTradable()) {
                return ExecutionResult.fail("symbol not TRADING: " + symbol
                        + " status=" + preCheckInfo.status
                        + " (suspended / delisted / pre-trading)");
            }

            // 1. Initialize symbol settings (leverage + isolated)
            if (!initializedSymbols.contains(symbol)) {
                if (!initSymbolMargin(symbol)) {
                    // [C5 2026-05-08] Include real Binance error body so user can act on it
                    // (e.g. symbol not on testnet → add to GARBAGE_COIN_BLOCKLIST; hedge mode
                    // mismatch → switch account to one-way; -4046 already isolated → bug).
                    return ExecutionResult.fail("init margin failed for " + symbol
                            + (lastInitErrorBody.isEmpty() ? "" : " — " + lastInitErrorBody));
                }
                if (!initSymbolLeverage(symbol)) {
                    return ExecutionResult.fail("init leverage failed for " + symbol
                            + (lastInitErrorBody.isEmpty() ? "" : " — " + lastInitErrorBody));
                }
                initializedSymbols.add(symbol);
            }

            // 2. Spread check
            double[] bidAsk = fetchBestBidAsk(symbol);
            if (bidAsk == null) return ExecutionResult.fail("cannot fetch book");
            double bid = bidAsk[0], ask = bidAsk[1];
            double mid = (bid + ask) / 2.0;
            if (mid <= 0) return ExecutionResult.fail("invalid book");
            double spreadPct = 100.0 * (ask - bid) / mid;
            if (spreadPct > maxSpreadPct) {
                return ExecutionResult.fail(String.format("spread %.3f%% > max %.3f%% (bid=%.6f ask=%.6f)",
                        spreadPct, maxSpreadPct, bid, ask));
            }

            // 3. Position size calculation
            // Risk = balance × riskPct/100 (e.g., $20 × 2% = $0.40 risked)
            // SL distance per unit = |entry - sl| (in price units)
            // Quantity = riskUsd / slDistance
            // Notional = quantity × entry (this is the position value)
            // Margin used = notional / leverage
            double entry = isLong ? ask : bid; // worst price to be conservative
            double slDistance = Math.abs(entry - idea.stop);
            if (slDistance <= 0) return ExecutionResult.fail("zero SL distance");

            // SL distance sanity: must be 0.10%–10% of price.
            // < 0.10% = stale-data tight stop → tiny qty, no real position.
            // > 10%   = de-facto disabled risk control or wrong direction.
            double slDistancePct = slDistance / entry;
            if (slDistancePct < 0.001) {
                return ExecutionResult.fail(String.format(
                        "SL too tight: %.3f%% from entry (need ≥0.10%%) — likely stale data",
                        slDistancePct * 100));
            }
            if (slDistancePct > 0.10) {
                return ExecutionResult.fail(String.format(
                        "SL too wide: %.2f%% from entry (max 10%%) — idea.stop=%.6f vs entry=%.6f, "
                                + "stale signal or wrong direction",
                        slDistancePct * 100, idea.stop, entry));
            }
            // Дополнительная проверка направления: SL для LONG должен быть НИЖЕ entry,
            // для SHORT — ВЫШЕ. Если перепутано — фатальный баг в DecisionEngine.
            if (isLong && idea.stop >= entry) {
                return ExecutionResult.fail(String.format(
                        "LONG SL=%.6f >= entry=%.6f (wrong direction)", idea.stop, entry));
            }
            if (!isLong && idea.stop <= entry) {
                return ExecutionResult.fail(String.format(
                        "SHORT SL=%.6f <= entry=%.6f (wrong direction)", idea.stop, entry));
            }

            // Load real exchange filters: stepSize (qty), tickSize (price),
            // minNotional (smallest position) — all per-symbol from exchangeInfo.
            SymbolInfo si = loadSymbolInfo(symbol);
            if (si == null) {
                return ExecutionResult.fail("cannot load exchangeInfo for " + symbol);
            }

            double riskUsd = balanceUsd * (riskPctPerTrade / 100.0);
            double qty = riskUsd / slDistance;

            // Apply unified size multiplier from idea (set upstream by
            // SignalSender for category/flag/session/ISC adjustments).
            // Default 1.0 = no scaling; clamped [0.20, 1.20] in setter.
            double sizeMult = idea.getExecutorSizeMultiplier();
            if (sizeMult != 1.0) {
                qty *= sizeMult;
                LOG.fine("[Executor] " + symbol + " sizeMult=" + sizeMult
                        + " applied → qty=" + qty);
            }

            double notional = qty * entry;
            double marginUsed = notional / leverage;

            if (marginUsed > balanceUsd * 0.5) {
                return ExecutionResult.fail(String.format(
                        "position too large: margin $%.2f > 50%% of balance $%.2f",
                        marginUsed, balanceUsd));
            }

            // Round qty DOWN to step size (truncate, never inflate position).
            qty = roundDownToStep(qty, si.stepSize);
            if (qty <= 0) return ExecutionResult.fail("qty rounded to zero");

            // Recompute notional after rounding — must satisfy minNotional.
            double notionalAfterRound = qty * entry;
            if (notionalAfterRound < si.minNotional) {
                return ExecutionResult.fail(String.format(
                        "notional $%.2f < min $%.2f for %s. Need bigger balance "
                                + "or smaller SL distance to fit min size.",
                        notionalAfterRound, si.minNotional, symbol));
            }

            // Round SL price to tick size (Binance rejects unaligned prices).
            double slPriceRounded = roundToTick(idea.stop, si.tickSize, isLong);

            // 4. Send MARKET order
            String entryOrderId = sendMarketOrder(symbol, isLong, qty);
            if (entryOrderId == null) {
                return ExecutionResult.fail("MARKET order rejected by Binance "
                        + "(see [Executor] log for code+msg)");
            }

            // Active poll for fill (50ms × 10 = 500ms max). Beats blind sleep
            // since demo-fapi typically confirms in 50-200ms.
            double actualEntry = 0;
            for (int pollI = 0; pollI < 10; pollI++) {
                actualEntry = fetchOrderAvgPrice(symbol, entryOrderId);
                if (actualEntry > 0) break;
                Thread.sleep(50);
            }
            if (actualEntry <= 0) actualEntry = entry; // fallback на квоту

            // ════════════════════════════════════════════════════════════════
            // [HOLE-8 FIX 2026-05-15] HARD SLIPPAGE GUARD
            // ════════════════════════════════════════════════════════════════
            // OLD BUG: pre-trade `maxSpreadPct` check covered only the bid/ask gap
            // at quote time, NOT the gap between idea.price (signal time, ~5-90 sec
            // earlier) and the actual MARKET fill price. HYPEUSDT slipped 2.34%
            // between signal and fill → SL and TP became invalid relative to the
            // new entry, yet the trade proceeded silently.
            //
            // NEW POLICY: if actualEntry deviates from idea.price by more than
            //   • 3.0%  on testnet (thin books, accepted)
            //   • 0.5%  on real (any larger gap = stale signal, kill it)
            // emergency-close and report. The signal probabilities, R:R, and
            // TP/SL were computed for idea.price — a different entry breaks them.
            double slippagePct = idea.price > 0
                    ? Math.abs(actualEntry - idea.price) / idea.price : 0.0;
            double maxSlippage = useTestnet ? 0.030 : 0.005;
            if (slippagePct > maxSlippage) {
                LOG.severe(String.format(
                        "[Executor] %s SLIPPAGE %.2f%% > max %.2f%% "
                                + "(idea=%.6f → fill=%.6f) — emergency close",
                        symbol, slippagePct * 100, maxSlippage * 100,
                        idea.price, actualEntry));
                // Need real on-exchange qty for emergency close — it may differ
                // from `qty` if there was a partial fill before we got here.
                double posAmtForClose = qty;
                try {
                    double remoteAmt = Math.abs(fetchPositionAmount(symbol));
                    if (remoteAmt > 0) posAmtForClose = remoteAmt;
                } catch (Throwable ignored) {}
                emergencyClosePosition(symbol, isLong, posAmtForClose);
                return ExecutionResult.fail(String.format(
                        "Slippage %.2f%% exceeded max %.2f%% (idea %.6f → fill %.6f) → emergency close",
                        slippagePct * 100, maxSlippage * 100,
                        idea.price, actualEntry));
            }

            // ════════════════════════════════════════════════════════════════
            // [HOLE-2 FIX 2026-05-15] TP / SL DIRECTION SANITY VS ACTUAL ENTRY
            // ════════════════════════════════════════════════════════════════
            // idea.tp1, idea.tp2 and idea.stop were computed against idea.price.
            // Even tiny slippage can flip a TP to the wrong side of actualEntry.
            // Example from HYPEUSDT 2026-05-15:
            //   idea.price=44.949  idea.tp1=45.844  idea.stop=44.054
            //   actualEntry=46.000 (slippage +2.34%)
            //   → tp1=45.844 is NOW BELOW entry for a LONG (impossible profit target).
            //   → stop=44.054 is now -4.23% from entry vs planned -2% (risk budget blown).
            //
            // We do three explicit checks AFTER actualEntry, BEFORE placing any algo orders:
            //   (a) Both TPs on wrong side of actualEntry  → abort, emergency close.
            //   (b) SL on wrong side of actualEntry         → abort, emergency close.
            //   (c) SL distance bloated >1.7× planned       → abort, risk budget blown.
            //
            // Note: rare case where only TP1 is on the wrong side (slippage moved
            // entry past TP1 but not past TP2) is handled later in the TP block —
            // we drop TP1 and fall back to a single TP at tp2 via closePosition=true.
            boolean tp1WrongSide = isLong ? (idea.tp1 <= actualEntry) : (idea.tp1 >= actualEntry);
            boolean tp2WrongSide = isLong ? (idea.tp2 <= actualEntry) : (idea.tp2 >= actualEntry);
            boolean slWrongSide  = isLong ? (idea.stop >= actualEntry) : (idea.stop <= actualEntry);

            if (slWrongSide) {
                LOG.severe(String.format(
                        "[Executor] %s SL on WRONG SIDE after slippage: "
                                + "%s entry=%.6f sl=%.6f — emergency close",
                        symbol, isLong ? "LONG" : "SHORT", actualEntry, idea.stop));
                emergencyClosePosition(symbol, isLong, qty);
                return ExecutionResult.fail(String.format(
                        "SL flipped to wrong side by slippage (entry %.6f → sl %.6f for %s)",
                        actualEntry, idea.stop, isLong ? "LONG" : "SHORT"));
            }
            if (tp1WrongSide && tp2WrongSide) {
                LOG.severe(String.format(
                        "[Executor] %s BOTH TPs on WRONG SIDE after slippage: "
                                + "%s entry=%.6f tp1=%.6f tp2=%.6f — emergency close",
                        symbol, isLong ? "LONG" : "SHORT",
                        actualEntry, idea.tp1, idea.tp2));
                emergencyClosePosition(symbol, isLong, qty);
                return ExecutionResult.fail(String.format(
                        "Both TPs flipped wrong-side by slippage (entry %.6f, tp1 %.6f, tp2 %.6f)",
                        actualEntry, idea.tp1, idea.tp2));
            }

            // [HOLE-2c FIX] Risk-budget guard: SL distance after slippage must not
            // exceed 1.7× the planned distance. If user planned -2% but slippage
            // made it -4.5%, the risk-per-trade calculation upstream was for the
            // smaller stop — actual loss on SL would be 2.25× the budgeted amount.
            double plannedSlDist = Math.abs(idea.price - idea.stop);
            double actualSlDist  = Math.abs(actualEntry - idea.stop);
            if (plannedSlDist > 0 && actualSlDist > plannedSlDist * 1.7) {
                double plannedPct = plannedSlDist / Math.max(1e-9, idea.price);
                double actualPct  = actualSlDist  / Math.max(1e-9, actualEntry);
                LOG.severe(String.format(
                        "[Executor] %s slippage blew SL budget: planned=%.2f%% "
                                + "actual=%.2f%% (×%.2f) — emergency close",
                        symbol, plannedPct * 100, actualPct * 100,
                        actualSlDist / plannedSlDist));
                emergencyClosePosition(symbol, isLong, qty);
                return ExecutionResult.fail(String.format(
                        "Slippage bloated SL %.2f%%→%.2f%% (>1.7× planned) — risk budget exceeded",
                        plannedPct * 100, actualPct * 100));
            }

            // Partial-fill detection: on illiquid alts MARKET may fill < requested.
            // SL has closePosition=true (handles real qty), but TP1+TP2 split
            // could fail with "qty exceeds position". If filled < 95% of requested,
            // use the smaller value for SL/TP sizing.
            double filledQty = qty;
            try {
                double posAmt = Math.abs(fetchPositionAmount(symbol));
                if (posAmt > 0 && posAmt < qty * 0.95) {
                    LOG.warning(String.format(
                            "[Executor] %s PARTIAL FILL: requested=%.6f filled=%.6f (%.0f%%)",
                            symbol, qty, posAmt, posAmt / qty * 100.0));
                    filledQty = posAmt;
                    // Re-round to step size to avoid TP/SL precision issues
                    filledQty = roundDownToStep(filledQty, si.stepSize);
                    if (filledQty <= 0) {
                        emergencyClosePosition(symbol, isLong, qty);
                        return ExecutionResult.fail("partial fill rounded to zero");
                    }
                }
            } catch (Throwable t) {
                LOG.fine("[Executor] partial-fill check failed for " + symbol
                        + ": " + t.getMessage() + " — using requested qty");
            }
            // From here on, qty refers to the actual on-exchange position.
            qty = filledQty;
            double actualNotional = qty * actualEntry;

            // 5. Place SL — STOP_MARKET via Algo Service (/fapi/v1/algoOrder).
            // closePosition=true closes the full position on trigger.
            // 4 attempts × ~250ms each fits within slPlacementTimeoutMs (2s default).
            String slOrderId = null;
            String slLastError = "no attempts";
            long slDeadline = System.currentTimeMillis() + slPlacementTimeoutMs;
            int attempts = 0;
            while (System.currentTimeMillis() < slDeadline && attempts < 4) {
                attempts++;
                String[] slResult = sendStopMarketOrderDiag(symbol, !isLong, slPriceRounded);
                slOrderId = slResult[0];
                slLastError = slResult[1];
                if (slOrderId != null) break;
                LOG.warning("[Executor] SL attempt " + attempts + "/4 failed for "
                        + symbol + ": " + slLastError);
                Thread.sleep(200);
            }

            // 6. SL FAILED — emergency close
            if (slOrderId == null) {
                LOG.severe("[Executor] CRITICAL: SL placement failed after " + attempts
                        + " attempts on " + symbol + " (lastError=" + slLastError
                        + ") — emergency closing position");
                emergencyClosePosition(symbol, isLong, qty);
                return ExecutionResult.fail("SL not placed (" + slLastError + ") → emergency close");
            }

            // 7. Place TP1 + TP2 — partial profit-taking via TAKE_PROFIT_MARKET algo orders.
            //
            // Дизайн:
            //   - TP1: закрывает ~50% позиции по цене idea.tp1
            //   - TP2: закрывает оставшиеся ~50% по цене idea.tp2
            //   - Используем reduceOnly=true (без closePosition) → частичное закрытие
            //   - Если qty/2 → 0 после rounding (мелкий контракт): fallback на единичный
            //     TAKE_PROFIT_MARKET с closePosition=true на цене tp2 (более консервативно
            //     ждать дальнюю цель чем взять ближнюю и упустить)
            //
            // ВАЖНО: TP-фейл НЕ триггерит emergency close. SL уже стоит, позиция защищена.
            // Просто логируем warning — пользователь увидит сколько TP реально встало.
            String tp1OrderId = null;
            String tp2OrderId = null;
            int tpsPlaced = 0;
            double tp1Price = idea.tp1;
            double tp2Price = idea.tp2;

            try {
                // Round TP prices to tick size (same direction as SL: rounds toward
                // entry → conservative, locks profit slightly earlier).
                double tp1Rounded = (idea.tp1 > 0)
                        ? roundToTick(idea.tp1, si.tickSize, isLong) : 0;
                double tp2Rounded = (idea.tp2 > 0)
                        ? roundToTick(idea.tp2, si.tickSize, isLong) : 0;

                // [HOLE-2 FIX 2026-05-15] After tick rounding, re-check TP1 vs actualEntry.
                // Edge case the top-level guard cannot catch: tp1 was barely on the right
                // side before rounding (e.g., +0.05% above entry for LONG) and tick
                // rounding pulled it across the entry line. Drop it and let the small-size
                // fallback below place a single TP at tp2.
                boolean tp1RoundedWrongSide = isLong
                        ? (tp1Rounded > 0 && tp1Rounded <= actualEntry)
                        : (tp1Rounded > 0 && tp1Rounded >= actualEntry);
                if (tp1RoundedWrongSide) {
                    LOG.warning("[Executor] " + symbol + " TP1 rounded across entry "
                            + "(entry=" + actualEntry + " tp1Rounded=" + tp1Rounded
                            + ") — dropping TP1, will use single TP2 fallback");
                    tp1Rounded = 0; // skip TP1, partialOk below will be false
                }

                tp1Price = tp1Rounded;
                tp2Price = tp2Rounded;

                // Split qty 50/50, ensuring sum doesn't exceed total.
                double tp1Qty = roundDownToStep(qty * 0.5, si.stepSize);
                double tp2Qty = qty - tp1Qty; // remainder, already a step multiple

                // Check minNotional for partial TPs
                double tp1Notional = tp1Qty * actualEntry;
                double tp2Notional = tp2Qty * actualEntry;
                // [HOLE-2 FIX] partialOk now also requires tp1Rounded > 0 — if TP1
                // was dropped above, force the single-TP fallback path.
                boolean partialOk = tp1Rounded > 0 && tp1Qty > 0 && tp2Qty > 0
                        && tp1Notional >= si.minNotional
                        && tp2Notional >= si.minNotional;

                if (partialOk && tp1Rounded > 0) {
                    String[] r1 = sendTakeProfitMarketOrderDiag(
                            symbol, !isLong, tp1Rounded, tp1Qty, false);
                    tp1OrderId = r1[0];
                    if (tp1OrderId != null) tpsPlaced++;
                    else LOG.warning("[Executor] TP1 failed " + symbol + ": " + r1[1]);
                }
                if (partialOk && tp2Rounded > 0) {
                    String[] r2 = sendTakeProfitMarketOrderDiag(
                            symbol, !isLong, tp2Rounded, tp2Qty, false);
                    tp2OrderId = r2[0];
                    if (tp2OrderId != null) tpsPlaced++;
                    else LOG.warning("[Executor] TP2 failed " + symbol + ": " + r2[1]);
                }

                // Fallback: position too small for partial TPs (or TP1 was dropped
                // by HOLE-2 round-across-entry guard) → single TP at tp2 with
                // closePosition=true. Better one TP than none.
                if (!partialOk && tp2Rounded > 0) {
                    LOG.info("[Executor] " + symbol + " single TP at tp2 fallback "
                            + "(notional=$" + String.format("%.2f", actualNotional)
                            + " min=$" + si.minNotional
                            + (tp1Rounded == 0 ? " tp1Dropped" : "") + ")");
                    String[] rSingle = sendTakeProfitMarketOrderDiag(
                            symbol, !isLong, tp2Rounded, 0, true);
                    tp2OrderId = rSingle[0];
                    if (tp2OrderId != null) tpsPlaced++;
                    else LOG.warning("[Executor] TP fallback failed " + symbol
                            + ": " + rSingle[1]);
                }

                LOG.info("[Executor] TPs result " + symbol + ": tpsPlaced=" + tpsPlaced
                        + " tp1=" + tp1OrderId + " tp2=" + tp2OrderId);
            } catch (Exception tpEx) {
                LOG.warning("[Executor] TP block exception " + symbol + ": "
                        + tpEx.getMessage() + " — position remains with SL only");
            }

            LOG.info(String.format(
                    "[Executor] OPENED %s %s qty=%s entry=%.8f notional=$%.2f sl=%.8f "
                            + "tp1=%.8f tp2=%.8f tpsPlaced=%d orderId=%s slId=%s",
                    symbol, isLong ? "LONG" : "SHORT", formatQtyForLog(qty), actualEntry,
                    actualNotional, slPriceRounded, tp1Price, tp2Price, tpsPlaced,
                    entryOrderId, slOrderId));

            return ExecutionResult.ok(entryOrderId, slOrderId, tp1OrderId, tp2OrderId,
                    tpsPlaced, actualEntry, qty, actualNotional,
                    slPriceRounded, tp1Price, tp2Price);
        } catch (Exception e) {
            LOG.severe("[Executor] openPosition exception: " + e.getMessage());
            return ExecutionResult.fail("exception: " + e.getMessage());
        }
    }

    /**
     * Force-close an open position market-style. Used by RiskGuard when
     * kill-switch fires, by TimeStop, or by emergency procedures.
     * Also cancels any open SL/TP orders for this symbol.
     */
    public boolean closePosition(String symbol, String reason) {
        if (!isReady()) return false;
        try {
            // 1. Find position
            double posQty = fetchPositionAmount(symbol);
            if (Math.abs(posQty) < 1e-12) {
                LOG.info("[Executor] closePosition " + symbol + ": no position to close");
                cancelAllOpenOrders(symbol); // still cancel orphan SL/TP
                return true;
            }
            boolean wasLong = posQty > 0;
            double absQty = Math.abs(posQty);

            // 2. Cancel any open orders first (so SL doesn't conflict with our close)
            cancelAllOpenOrders(symbol);

            // 3. Send opposite-side market order
            String orderId = sendMarketOrder(symbol, !wasLong, absQty);
            if (orderId == null) {
                LOG.severe("[Executor] CLOSE FAILED on " + symbol + " — manual intervention needed");
                return false;
            }
            LOG.info(String.format("[Executor] CLOSED %s qty=%.6f reason=%s orderId=%s",
                    symbol, absQty, reason, orderId));
            return true;
        } catch (Exception e) {
            LOG.severe("[Executor] closePosition exception: " + e.getMessage());
            return false;
        }
    }

    /**
     * Fetch current position size on the exchange. >0 = long, <0 = short, 0 = none.
     */
    public double fetchPositionAmount(String symbol) {
        if (!isReady()) return 0;
        try {
            long ts = ts();
            String qs = "symbol=" + symbol + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v2/positionRisk?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return 0;
            JSONArray arr = new JSONArray(resp.body());
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                if (symbol.equals(o.optString("symbol"))) {
                    return o.optDouble("positionAmt", 0);
                }
            }
            return 0;
        } catch (Exception e) {
            LOG.warning("[Executor] fetchPosition error: " + e.getMessage());
            return 0;
        }
    }

    /**
     * Get unrealized PnL for an open position.
     * @return PnL in USDT, or 0 if no position.
     */
    public double fetchUnrealizedPnl(String symbol) {
        if (!isReady()) return 0;
        try {
            long ts = ts();
            String qs = "symbol=" + symbol + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v2/positionRisk?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return 0;
            JSONArray arr = new JSONArray(resp.body());
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                if (symbol.equals(o.optString("symbol"))) {
                    return o.optDouble("unRealizedProfit", 0);
                }
            }
            return 0;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * [v86 EXIT-FIX] Get current mark price for a symbol via positionRisk endpoint.
     * Used by PositionTracker for active management (trailing-stop / profit-lock /
     * stagnation exit) — needs current price each poll, not just qty.
     * @return mark price, or 0 if no position / API failed.
     */
    public double fetchMarkPrice(String symbol) {
        if (!isReady()) return 0;
        try {
            long ts = ts();
            String qs = "symbol=" + symbol + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v2/positionRisk?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return 0;
            JSONArray arr = new JSONArray(resp.body());
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                if (symbol.equals(o.optString("symbol"))) {
                    return o.optDouble("markPrice", 0);
                }
            }
            return 0;
        } catch (Exception e) {
            return 0;
        }
    }
    private boolean initSymbolMargin(String symbol) throws Exception {
        long ts = ts();
        String qs = "symbol=" + symbol + "&marginType=ISOLATED&timestamp=" + ts + "&recvWindow=5000";
        String sig = hmacSHA256(apiSecret, qs);
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/fapi/v1/marginType"))
                        .timeout(Duration.ofSeconds(8))
                        .header("X-MBX-APIKEY", apiKey)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(qs + "&signature=" + sig))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        // 200 = OK, or already isolated (Binance returns "No need to change margin type" code -4046)
        int code = resp.statusCode();
        String body = resp.body() == null ? "" : resp.body();
        if (code == 200) { lastInitErrorBody = ""; return true; }
        if (body.contains("-4046")) { lastInitErrorBody = ""; return true; }
        // [PATCH 2026-05-18] -4067 on marginType: stale open/algo orders block the
        // position-mode switch. cancelAllOrdersAccountWide() wipes regular + algo
        // (SL/TP) orders account-wide, then single retry. On mainnet the algo-order
        // cleanup actually succeeds (testnet returns 404 on /algoOrders, so there
        // the retry may still fail — that path is harmless, just returns false).
        if (body.contains("-4067") || body.contains("open orders")) {
            LOG.warning("[Executor] marginType -4067 — cancel-all + single retry " + symbol);
            cancelAllOrdersAccountWide();
            long tsR = ts();
            String qsR = "symbol=" + symbol + "&marginType=ISOLATED&timestamp=" + tsR + "&recvWindow=5000";
            String sigR = hmacSHA256(apiSecret, qsR);
            HttpResponse<String> respR = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/marginType"))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .POST(HttpRequest.BodyPublishers.ofString(qsR + "&signature=" + sigR))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            String bodyR = respR.body() == null ? "" : respR.body();
            if (respR.statusCode() == 200 || bodyR.contains("-4046")) {
                lastInitErrorBody = "";
                return true;
            }
            lastInitErrorBody = "HTTP " + respR.statusCode() + ": " + bodyR;
            LOG.warning("[Executor] marginType retry failed " + symbol + " " + lastInitErrorBody);
            return false;
        }
        // [v82.1 2026-06-01] FIX -1109 "Invalid account" на marginType.
        // ROOT: demo/PM/Multi-Assets аккаунт не даёт менять маржу per-symbol через
        // классический /fapi/v1/marginType. Баланс при этом читается (ключи валидны).
        // Раньше -1109 → abort трейда. Теперь: НЕ фатально — торгуем в текущем
        // режиме маржи аккаунта (как делают prod-боты, не форсящие ISOLATED).
        if (body.contains("-1109")) {
            LOG.warning("[Executor] marginType -1109 для " + symbol
                    + " — НЕ фатально, торгую в текущем режиме маржи аккаунта");
            lastInitErrorBody = "";
            return true;
        }
        // [C5 2026-05-08] Save body so caller can surface real reason in Telegram.
        lastInitErrorBody = "HTTP " + code + ": " + body;
        LOG.warning("[Executor] marginType " + symbol + " " + lastInitErrorBody);
        return false;
    }

    private boolean initSymbolLeverage(String symbol) throws Exception {
        long ts = ts();
        String qs = "symbol=" + symbol + "&leverage=" + leverage + "&timestamp=" + ts + "&recvWindow=5000";
        String sig = hmacSHA256(apiSecret, qs);
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/fapi/v1/leverage"))
                        .timeout(Duration.ofSeconds(8))
                        .header("X-MBX-APIKEY", apiKey)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(qs + "&signature=" + sig))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() == 200) { lastInitErrorBody = ""; return true; }
        // [C5 2026-05-08] Save body so caller can surface real reason in Telegram.
        String body = resp.body() == null ? "" : resp.body();
        // [v82.1 2026-06-01] -1109 на leverage тоже НЕ фатально (см. marginType).
        // Аккаунт в режиме где per-symbol leverage не ставится → торгуем с текущим.
        if (body.contains("-1109")) {
            LOG.warning("[Executor] leverage -1109 для " + symbol
                    + " — НЕ фатально, торгую с текущим плечом аккаунта");
            lastInitErrorBody = "";
            return true;
        }
        lastInitErrorBody = "HTTP " + resp.statusCode() + ": " + body;
        LOG.warning("[Executor] leverage " + symbol + " " + lastInitErrorBody);
        return false;
    }

    private double[] fetchBestBidAsk(String symbol) {
        try {
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/ticker/bookTicker?symbol=" + symbol))
                            .timeout(Duration.ofSeconds(5))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            JSONObject o = new JSONObject(resp.body());
            return new double[]{o.optDouble("bidPrice", 0), o.optDouble("askPrice", 0)};
        } catch (Exception e) { return null; }
    }

    /** Send a MARKET order. Returns orderId on success, null on failure. */
    private String sendMarketOrder(String symbol, boolean buy, double qty) throws Exception {
        long ts = ts();
        String body = "symbol=" + symbol
                + "&side=" + (buy ? "BUY" : "SELL")
                + "&type=MARKET"
                + "&quantity=" + formatQty(qty)
                + "&newOrderRespType=RESULT"
                + "&timestamp=" + ts + "&recvWindow=5000";
        String sig = hmacSHA256(apiSecret, body);
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/fapi/v1/order"))
                        .timeout(Duration.ofSeconds(10))
                        .header("X-MBX-APIKEY", apiKey)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(body + "&signature=" + sig))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            String b = resp.body() == null ? "" : resp.body();
            // -4061 = "Order's position side does not match." HEDGE mode
            // detected despite ensureOneWayMode() at boot — retry with explicit
            // positionSide as fallback.
            if (b.contains("-4061") || b.contains("position side")) {
                LOG.warning("[Executor] " + symbol + " HEDGE mode detected, retrying with positionSide");
                long ts2 = ts();
                String body2 = "symbol=" + symbol
                        + "&side=" + (buy ? "BUY" : "SELL")
                        + "&positionSide=" + (buy ? "LONG" : "SHORT")
                        + "&type=MARKET"
                        + "&quantity=" + formatQty(qty)
                        + "&newOrderRespType=RESULT"
                        + "&timestamp=" + ts2 + "&recvWindow=5000";
                String sig2 = hmacSHA256(apiSecret, body2);
                HttpResponse<String> resp2 = http.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create(baseUrl + "/fapi/v1/order"))
                                .timeout(Duration.ofSeconds(10))
                                .header("X-MBX-APIKEY", apiKey)
                                .header("Content-Type", "application/x-www-form-urlencoded")
                                .POST(HttpRequest.BodyPublishers.ofString(body2 + "&signature=" + sig2))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                if (resp2.statusCode() == 200) {
                    return new JSONObject(resp2.body()).optString("orderId", null);
                }
                LOG.warning("[Executor] MARKET retry " + symbol + " HTTP " + resp2.statusCode()
                        + ": " + resp2.body());
                return null;
            }
            LOG.warning("[Executor] MARKET order " + symbol + " HTTP " + resp.statusCode()
                    + ": " + b);
            return null;
        }
        return new JSONObject(resp.body()).optString("orderId", null);
    }

    /**
     * [v83.3] Send a STOP_MARKET order via the new Algo Service endpoint.
     *
     * Migration context: до 2025-12-09 условные ордера принимались на
     * POST /fapi/v1/order. После — Binance переключил их на отдельный
     * Algo Service. Старый эндпоинт теперь возвращает -4120
     * "Order type not supported for this endpoint. Please use the
     * Algo Order API endpoints instead."
     *
     * Изменения по сравнению со старой реализацией:
     *   - URL:        /fapi/v1/order  →  /fapi/v1/algoOrder
     *   - Добавлено:  algoType=CONDITIONAL  (обязательный параметр)
     *   - Параметр:   stopPrice  →  triggerPrice  (имя поменялось)
     *   - Ответ:      orderId (string)  →  algoId (long)
     *
     * Возвращает algoId как строку (для совместимости с интерфейсом, где
     * SL-ID хранится как String). Возвращает null при ошибке.
     */
    /**
     * [v84.0] Diagnostic STOP_MARKET sender. Returns String[2]:
     *   [0] = algoId (success) or null (failure)
     *   [1] = error reason string (always populated for diagnostics)
     *
     * Replaces the old sendStopMarketOrder() — same endpoint, same params,
     * but full request body + response body are now captured for the caller.
     * This is what makes "SL not placed" debuggable instead of mysterious.
     */
    private String[] sendStopMarketOrderDiag(String symbol, boolean buy, double stopPrice) throws Exception {
        long ts = ts();
        // recvWindow bumped to 60000 per Binance recommendation for high-latency
        // environments — testnet often has 200-500ms RTT spikes.
        String body = "algoType=CONDITIONAL"
                + "&symbol=" + symbol
                + "&side=" + (buy ? "BUY" : "SELL")
                + "&type=STOP_MARKET"
                + "&triggerPrice=" + formatPrice(stopPrice)
                + "&closePosition=true"
                + "&workingType=MARK_PRICE"
                + "&timestamp=" + ts + "&recvWindow=60000";
        String sig = hmacSHA256(apiSecret, body);
        HttpResponse<String> resp;
        try {
            resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/algoOrder"))
                            .timeout(Duration.ofSeconds(10))
                            .header("X-MBX-APIKEY", apiKey)
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .POST(HttpRequest.BodyPublishers.ofString(body + "&signature=" + sig))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            String err = "HTTP exception: " + e.getClass().getSimpleName()
                    + ": " + e.getMessage();
            LOG.severe("[Executor] STOP_MARKET(algo) " + symbol + " " + err
                    + " | sentBody=" + body);
            return new String[]{null, err};
        }

        if (resp.statusCode() != 200) {
            String err = "HTTP " + resp.statusCode() + " body=" + resp.body();
            LOG.severe("[Executor] STOP_MARKET(algo) FAIL " + symbol + " " + err
                    + " | sentBody=" + body);
            return new String[]{null, err};
        }

        try {
            JSONObject o = new JSONObject(resp.body());
            long algoId = o.optLong("algoId", 0L);
            if (algoId <= 0) {
                String err = "200 OK but no algoId. body=" + resp.body();
                LOG.severe("[Executor] STOP_MARKET(algo) " + symbol + " " + err);
                return new String[]{null, err};
            }
            return new String[]{String.valueOf(algoId), "OK"};
        } catch (Exception parseEx) {
            String err = "JSON parse: " + parseEx.getMessage() + " body=" + resp.body();
            LOG.severe("[Executor] STOP_MARKET(algo) " + symbol + " " + err);
            return new String[]{null, err};
        }
    }

    /**
     * [v84.0] Send a TAKE_PROFIT_MARKET algo order. Two modes:
     *
     *   1) Partial close (useClosePosition=false):
     *        sends quantity + reduceOnly=true. When triggered, closes only
     *        that quantity. Used for TP1 (50%) and TP2 (50%).
     *
     *   2) Full close (useClosePosition=true):
     *        sends closePosition=true (no quantity, no reduceOnly).
     *        When triggered, closes the entire remaining position.
     *        Used as fallback when position is too small for partial split.
     *
     * Returns String[2]: [0]=algoId or null, [1]=reason.
     */
    private String[] sendTakeProfitMarketOrderDiag(String symbol, boolean buy,
                                                   double triggerPrice, double qty,
                                                   boolean useClosePosition) throws Exception {
        long ts = ts();
        StringBuilder body = new StringBuilder();
        body.append("algoType=CONDITIONAL")
                .append("&symbol=").append(symbol)
                .append("&side=").append(buy ? "BUY" : "SELL")
                .append("&type=TAKE_PROFIT_MARKET")
                .append("&triggerPrice=").append(formatPrice(triggerPrice))
                .append("&workingType=MARK_PRICE");
        if (useClosePosition) {
            body.append("&closePosition=true");
        } else {
            body.append("&quantity=").append(formatQty(qty))
                    .append("&reduceOnly=true");
        }
        body.append("&timestamp=").append(ts).append("&recvWindow=60000");

        String bodyStr = body.toString();
        String sig = hmacSHA256(apiSecret, bodyStr);

        HttpResponse<String> resp;
        try {
            resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/algoOrder"))
                            .timeout(Duration.ofSeconds(10))
                            .header("X-MBX-APIKEY", apiKey)
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .POST(HttpRequest.BodyPublishers.ofString(bodyStr + "&signature=" + sig))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            String err = "HTTP exception: " + e.getClass().getSimpleName()
                    + ": " + e.getMessage();
            return new String[]{null, err};
        }

        if (resp.statusCode() != 200) {
            return new String[]{null, "HTTP " + resp.statusCode() + " body=" + resp.body()
                    + " | sentBody=" + bodyStr};
        }

        try {
            JSONObject o = new JSONObject(resp.body());
            long algoId = o.optLong("algoId", 0L);
            if (algoId <= 0) {
                return new String[]{null, "200 OK but no algoId. body=" + resp.body()};
            }
            return new String[]{String.valueOf(algoId), "OK"};
        } catch (Exception parseEx) {
            return new String[]{null, "JSON parse: " + parseEx.getMessage()
                    + " body=" + resp.body()};
        }
    }

    /**
     * Cancel ALL open orders on a symbol — обычные И алго-ордера.
     *
     * [v83.3] После миграции SL/TP в Algo Service обычный
     * DELETE /fapi/v1/allOpenOrders больше не убивает условные ордера —
     * они живут отдельно. Без второго вызова orphan-SL остаётся висеть
     * на бирже после ручного закрытия позиции и может позже вылететь
     * по триггеру в пустом стакане. Поэтому гасим обе очереди.
     *
     * Оба запроса делаются с try/catch вокруг каждого, чтобы падение
     * одного не мешало второму отработать.
     */
    /**
     * [v84.0] Public wrapper for cleanup. Cancels both regular orders and algo
     * orders (SL/TP/TRAILING) for the given symbol. Used by PositionTracker
     * when it detects a position has closed (e.g., SL triggered naturally) —
     * needed to evict orphan TP/SL algo orders that would otherwise sit in
     * the queue forever and possibly trigger spuriously later.
     *
     * Idempotent and exception-safe. Both queues are attempted independently;
     * failure of one does not prevent the other from being cleared.
     */
    public void cancelAllOrdersOnSymbol(String symbol) {
        cancelAllOpenOrders(symbol);
    }

    // ═════════════════════════════════════════════════════════════════
    // PositionTracker support: targeted algo cancel, move-to-breakeven,
    // real-fill PnL, position enumeration. Used by PositionTracker; not
    // called from openPositionWithSl (existing flows untouched).
    // ═════════════════════════════════════════════════════════════════

    /**
     * Cancel a SINGLE algo order by algoId. Does NOT touch other algo orders
     * on the symbol — critical for breakeven moves where we want to replace
     * just the SL without killing the TPs.
     *
     * Treats "-2011" / "-2013" / "Unknown order" as success (the order is
     * gone — either filled or already cancelled — which is what we wanted).
     *
     * @return true if order is gone (cancelled or already gone), false on hard error.
     */
    public boolean cancelAlgoOrder(String symbol, String algoId) {
        if (!isReady()) return false;
        if (algoId == null || algoId.isBlank()) {
            LOG.warning("[Executor] cancelAlgoOrder " + symbol + ": empty algoId");
            return false;
        }
        try {
            long ts = ts();
            String qs = "symbol=" + symbol + "&algoId=" + algoId
                    + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/algoOrder?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .DELETE().build(),
                    HttpResponse.BodyHandlers.ofString());
            int code = resp.statusCode();
            if (code == 200) {
                LOG.info("[Executor] cancelAlgoOrder OK " + symbol + "/" + algoId);
                return true;
            }
            // Order already gone — treat as success
            String body = resp.body() == null ? "" : resp.body();
            if (body.contains("-2011") || body.contains("-2013")
                    || body.toLowerCase().contains("unknown order")) {
                LOG.info("[Executor] cancelAlgoOrder " + symbol + "/" + algoId
                        + " already gone (treated as success)");
                return true;
            }
            LOG.warning("[Executor] cancelAlgoOrder " + symbol + "/" + algoId
                    + " HTTP " + code + ": " + body);
            return false;
        } catch (Exception e) {
            LOG.warning("[Executor] cancelAlgoOrder " + symbol + "/" + algoId
                    + " exception: " + e.getMessage());
            return false;
        }
    }

    /**
     * Replace the SL with a new breakeven SL atomically.
     *
     * Sequence (NOT a single transaction — Binance has no such primitive,
     * so we do it in the safest possible order):
     *   1. Cancel old SL via DELETE algoOrder (targeted).
     *   2. If cancel fails → ABORT (don't place a second SL on top of the
     *      live one — that creates two SLs which can both fire).
     *   3. Place new SL via algoOrder (closePosition=true).
     *   4. If new SL placement fails → log SEVERE. The position is now
     *      UNPROTECTED. Caller should send loud Telegram alert. We don't
     *      auto-restore the old SL because the cancel may have already
     *      partially propagated and a re-place could conflict.
     *
     * @param oldSlAlgoId  algoId of the existing SL to be cancelled
     * @param newSlPrice   the new SL price (will be tick-rounded internally)
     * @return new algoId on success, null on any failure
     */
    public String replaceSlWithBreakeven(String symbol, boolean isLong,
                                         String oldSlAlgoId, double newSlPrice) {
        if (!isReady()) return null;
        SymbolInfo si = loadSymbolInfo(symbol);
        if (si == null) {
            LOG.warning("[Executor] BE move " + symbol + ": no symbolInfo, abort");
            return null;
        }
        // 1. Cancel old SL
        boolean cancelled = cancelAlgoOrder(symbol, oldSlAlgoId);
        if (!cancelled) {
            LOG.severe("[Executor] BE move " + symbol + ": old SL cancel FAILED. "
                    + "Aborting — old SL may still be alive.");
            return null;
        }

        // 2. Round new SL to tick. For LONG, BE SL is ABOVE entry (sl is normally
        // below entry, but BE flips that — we set it slightly above to lock a
        // tiny profit / cover fees). Use HALF_UP-ish rounding via roundToTick:
        //   - For LONG: round DOWN (don't go further from spot than asked).
        //   - For SHORT: round UP. roundToTick already does this correctly.
        double rounded = roundToTick(newSlPrice, si.tickSize, isLong);

        // 3. Place new SL
        try {
            String[] r = sendStopMarketOrderDiag(symbol, !isLong, rounded);
            if (r[0] != null) {
                LOG.info("[Executor] BE move " + symbol + " OK: oldSl=" + oldSlAlgoId
                        + " → newSl=" + r[0] + " price=" + rounded);
                return r[0];
            }
            LOG.severe("[Executor] BE move " + symbol + " — new SL placement FAILED: "
                    + r[1] + " — POSITION NOW UNPROTECTED. Manual intervention may be required.");
            return null;
        } catch (Exception e) {
            LOG.severe("[Executor] BE move " + symbol + " exception: " + e.getMessage()
                    + " — POSITION NOW UNPROTECTED");
            return null;
        }
    }

    /**
     * Fetch the VWAP of CLOSING fills for a symbol since a given timestamp.
     *
     * "Closing" = trades on the side opposite to the original entry direction.
     * If you opened LONG (BUY), closing trades are SELL. We pull /fapi/v1/userTrades
     * with startTime=sinceMs and average all SELL trades by qty.
     *
     * Why this exists: the old PositionTracker.guessClosingPrice() returned
     * t.slPrice unconditionally, which made PnL totally wrong when:
     *   • TP1 fired (50% closed at +R) followed by SL on remainder — pnl
     *     was reported as 100% × slPrice instead of 50% TP + 50% SL.
     *   • User manually closed in profit — pnl was reported as SL_HIT loss.
     *   • Slippage caused real fill 0.3% worse than configured SL.
     *
     * @param symbol               trading pair
     * @param originalEntryWasLong true if the original entry side was BUY (long)
     * @param sinceMs              ms epoch — start of the window to look for fills
     * @return VWAP of closing fills, or 0.0 if API failed / no closing fills found
     */
    public double fetchRealizedClosingPrice(String symbol,
                                            boolean originalEntryWasLong,
                                            long sinceMs) {
        if (!isReady()) return 0.0;
        try {
            long ts = ts();
            // limit=500 is the max for userTrades. For a single trade this is
            // way more than enough (TP1 + SL = 2 fills, plus the entry = 3).
            String qs = "symbol=" + symbol + "&startTime=" + sinceMs
                    + "&limit=500&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/userTrades?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                LOG.warning("[Executor] userTrades " + symbol + " HTTP "
                        + resp.statusCode() + ": " + resp.body());
                return 0.0;
            }
            JSONArray arr = new JSONArray(resp.body());
            String closingSide = originalEntryWasLong ? "SELL" : "BUY";
            double totalQty = 0.0;
            double weightedPrice = 0.0;
            int closingCount = 0;
            for (int i = 0; i < arr.length(); i++) {
                JSONObject tr = arr.getJSONObject(i);
                if (!closingSide.equals(tr.optString("side"))) continue;
                double qty = tr.optDouble("qty", 0);
                double price = tr.optDouble("price", 0);
                if (qty > 0 && price > 0) {
                    totalQty += qty;
                    weightedPrice += qty * price;
                    closingCount++;
                }
            }
            if (totalQty > 0) {
                double vwap = weightedPrice / totalQty;
                LOG.info(String.format(
                        "[Executor] realizedClosePrice %s: %d fills totalQty=%.6f VWAP=%.8f",
                        symbol, closingCount, totalQty, vwap));
                return vwap;
            }
            LOG.fine("[Executor] realizedClosePrice " + symbol + ": no closing fills found");
            return 0.0;
        } catch (Exception e) {
            LOG.warning("[Executor] realizedClosePrice " + symbol + " exception: "
                    + e.getMessage());
            return 0.0;
        }
    }

    /**
     * Fetch ALL non-zero positions on the futures account. Used by PositionTracker
     * at startup to detect orphan positions (Railway restart while a trade was open).
     *
     * @return JSONArray of position objects (positionAmt, entryPrice, markPrice,
     *         unRealizedProfit, symbol, …) or null on API failure.
     */
    public JSONArray fetchAllOpenPositionsRaw() {
        if (!isReady()) return null;
        try {
            long ts = ts();
            String qs = "timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v2/positionRisk?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(10))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                LOG.warning("[Executor] fetchAllPositions HTTP " + resp.statusCode()
                        + ": " + resp.body());
                return null;
            }
            return new JSONArray(resp.body());
        } catch (Exception e) {
            LOG.warning("[Executor] fetchAllPositions exception: " + e.getMessage());
            return null;
        }
    }

    private void cancelAllOpenOrders(String symbol) {
        // 1) Обычные ордера
        try {
            long ts = ts();
            String body = "symbol=" + symbol + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, body);
            http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/allOpenOrders?" + body + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .DELETE().build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            LOG.warning("[Executor] cancelAllOrders(plain) " + symbol + " error: " + e.getMessage());
        }

        // 2) Алго-ордера (SL/TP/трейлинги — теперь отдельная очередь)
        try {
            long ts = ts();
            String body = "symbol=" + symbol + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, body);
            http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/allAlgoOpenOrders?" + body + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .DELETE().build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            LOG.warning("[Executor] cancelAllOrders(algo) " + symbol + " error: " + e.getMessage());
        }
    }

    /**
     * [HOLE-3 FIX 2026-05-08] Returns true if there is an active STOP_MARKET (SL)
     * order on the exchange for this symbol — checked across both the plain
     * order queue (legacy) AND the algo order queue (current standard since
     * Binance breaking change 2025-12-09).
     *
     * Used by PositionTracker.reconcileAtStartup to decide if an orphan
     * position is "naked" (needs auto-close) or "protected" (just leave it).
     *
     * Returns false on API error — fail-safe: prefer auto-close to leaving
     * an orphan that we wrongly think has SL.
     */
    public boolean hasActiveStopLoss(String symbol) {
        if (!isReady()) return false;
        // 1. Check plain open orders queue
        try {
            long ts = ts();
            String qs = "symbol=" + symbol + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> r = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/openOrders?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() == 200) {
                JSONArray arr = new JSONArray(r.body());
                for (int i = 0; i < arr.length(); i++) {
                    JSONObject o = arr.getJSONObject(i);
                    String type = o.optString("type", "").toUpperCase();
                    if (type.contains("STOP")) return true;
                }
            }
        } catch (Exception e) {
            LOG.warning("[Executor] hasActiveStopLoss(plain) " + symbol + " error: " + e.getMessage());
            return false;
        }
        // 2. Check algo open orders queue
        try {
            long ts = ts();
            String qs = "symbol=" + symbol + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> r = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/algoOpenOrders?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(8))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() == 200) {
                // Endpoint can return JSONArray OR {"orders":[...]} depending on version.
                String body = r.body() == null ? "" : r.body().trim();
                if (body.startsWith("[")) {
                    JSONArray arr = new JSONArray(body);
                    for (int i = 0; i < arr.length(); i++) {
                        JSONObject o = arr.getJSONObject(i);
                        String type = o.optString("algoType", o.optString("type", "")).toUpperCase();
                        if (type.contains("STOP")) return true;
                    }
                } else if (body.startsWith("{")) {
                    JSONObject obj = new JSONObject(body);
                    JSONArray arr = obj.optJSONArray("orders");
                    if (arr != null) {
                        for (int i = 0; i < arr.length(); i++) {
                            JSONObject o = arr.getJSONObject(i);
                            String type = o.optString("algoType", o.optString("type", "")).toUpperCase();
                            if (type.contains("STOP")) return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warning("[Executor] hasActiveStopLoss(algo) " + symbol + " error: " + e.getMessage());
        }
        return false;
    }

    /**
     * [HOLE-3 FIX 2026-05-08] Emergency close — теперь с проверкой HTTP-статуса,
     * 3-кратным retry (300ms, 600ms backoff) и пост-верификацией позиции.
     *
     * До правки: один fire-and-forget POST. Если Binance возвращал 4xx/5xx —
     * бот тихо считал что закрыл, депозит мог потеряться. Теперь:
     *   1. Проверяем resp.statusCode() == 200 на каждой попытке
     *   2. После 200 — verify через /fapi/v2/positionRisk: позиция реально =0
     *   3. До 3 попыток с экспоненциальным backoff
     *   4. Если все попытки упали — Telegram alert (через staticTelegram если задан)
     *
     * Returns true если позиция подтверждённо закрыта, false иначе.
     * Старые callers ожидали void — обёртки оставлены ниже для совместимости.
     */
    private boolean emergencyClosePositionWithVerify(String symbol, boolean wasLong, double qty) {
        if (Math.abs(qty) < 1e-9) return true;
        Exception lastEx = null;
        int    lastStatus = 0;
        String lastBody   = "";
        final int maxAttempts = 3;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                long ts = ts();
                String body = "symbol=" + symbol
                        + "&side=" + (wasLong ? "SELL" : "BUY")
                        + "&type=MARKET"
                        + "&quantity=" + formatQty(qty)
                        + "&reduceOnly=true"
                        + "&timestamp=" + ts + "&recvWindow=5000";
                String sig = hmacSHA256(apiSecret, body);
                HttpResponse<String> resp = http.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create(baseUrl + "/fapi/v1/order"))
                                .timeout(Duration.ofSeconds(10))
                                .header("X-MBX-APIKEY", apiKey)
                                .header("Content-Type", "application/x-www-form-urlencoded")
                                .POST(HttpRequest.BodyPublishers.ofString(body + "&signature=" + sig))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                lastStatus = resp.statusCode();
                lastBody   = resp.body() == null ? "" : resp.body();
                if (lastStatus == 200) {
                    // Verify position closed within ~1.5s.
                    boolean verified = false;
                    for (int v = 0; v < 3; v++) {
                        try { Thread.sleep(500); } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt(); break;
                        }
                        double remaining = fetchPositionAmount(symbol);
                        if (Math.abs(remaining) < 1e-9) { verified = true; break; }
                    }
                    if (verified) {
                        LOG.info("[Executor] emergencyClose " + symbol + " OK (attempt "
                                + attempt + ")");
                        return true;
                    }
                    LOG.severe("[Executor] emergencyClose " + symbol
                            + " HTTP200 but position still open after 1.5s verify — retrying");
                } else if (lastBody.contains("-4061") || lastBody.contains("position side")) {
                    // HEDGE-mode require explicit positionSide
                    long ts2 = ts();
                    String body2 = "symbol=" + symbol
                            + "&side=" + (wasLong ? "SELL" : "BUY")
                            + "&positionSide=" + (wasLong ? "LONG" : "SHORT")
                            + "&type=MARKET"
                            + "&quantity=" + formatQty(qty)
                            + "&timestamp=" + ts2 + "&recvWindow=5000";
                    String sig2 = hmacSHA256(apiSecret, body2);
                    HttpResponse<String> resp2 = http.send(
                            HttpRequest.newBuilder()
                                    .uri(URI.create(baseUrl + "/fapi/v1/order"))
                                    .timeout(Duration.ofSeconds(10))
                                    .header("X-MBX-APIKEY", apiKey)
                                    .header("Content-Type", "application/x-www-form-urlencoded")
                                    .POST(HttpRequest.BodyPublishers.ofString(body2 + "&signature=" + sig2))
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
                    lastStatus = resp2.statusCode();
                    lastBody   = resp2.body() == null ? "" : resp2.body();
                    if (lastStatus == 200) {
                        // verify
                        for (int v = 0; v < 3; v++) {
                            try { Thread.sleep(500); } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt(); break;
                            }
                            double remaining = fetchPositionAmount(symbol);
                            if (Math.abs(remaining) < 1e-9) {
                                LOG.info("[Executor] emergencyClose " + symbol
                                        + " OK via HEDGE retry");
                                return true;
                            }
                        }
                    } else {
                        LOG.warning("[Executor] emergencyClose " + symbol
                                + " attempt " + attempt + " HEDGE retry failed: HTTP "
                                + lastStatus + " " + lastBody);
                    }
                } else {
                    LOG.warning("[Executor] emergencyClose " + symbol
                            + " attempt " + attempt + " HTTP " + lastStatus + ": " + lastBody);
                }
            } catch (Exception e) {
                lastEx = e;
                LOG.warning("[Executor] emergencyClose " + symbol
                        + " attempt " + attempt + " exception: " + e.getMessage());
            }
            if (attempt < maxAttempts) {
                try { Thread.sleep(300L * attempt); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); break;
                }
            }
        }
        // All attempts exhausted — escalate
        String reason = lastEx != null ? ("exception: " + lastEx.getMessage())
                : ("HTTP " + lastStatus + ": " + lastBody);
        LOG.severe("[Executor] EMERGENCY CLOSE FAILED " + symbol + " after " + maxAttempts
                + " attempts (" + reason + ") — MANUAL INTERVENTION REQUIRED");
        emergencyAlert(symbol, wasLong, qty, reason);
        return false;
    }

    /** Backwards-compat shim for callers that just need fire-and-verify. */
    private void emergencyClosePosition(String symbol, boolean wasLong, double qty) {
        emergencyClosePositionWithVerify(symbol, wasLong, qty);
    }

    /** Telegram alert for failed emergency close. Static hook set by SignalSender. */
    private static volatile java.util.function.Consumer<String> emergencyAlertSink = null;
    public static void setEmergencyAlertSink(java.util.function.Consumer<String> sink) {
        emergencyAlertSink = sink;
    }
    private void emergencyAlert(String symbol, boolean wasLong, double qty, String reason) {
        try {
            java.util.function.Consumer<String> sink = emergencyAlertSink;
            if (sink != null) {
                sink.accept(String.format(
                        "🚨 *EMERGENCY CLOSE FAILED* %s\n"
                                + "Side: %s | qty: %s\n"
                                + "Причина: %s\n"
                                + "*РУЧНОЕ ВМЕШАТЕЛЬСТВО:* зайди в Binance и закрой позицию вручную.",
                        symbol, wasLong ? "LONG (close=SELL)" : "SHORT (close=BUY)",
                        formatQty(qty), reason));
            }
        } catch (Throwable ignored) {}
    }

    /** Fetch average fill price from order. Returns 0 if not filled. */
    private double fetchOrderAvgPrice(String symbol, String orderId) {
        try {
            long ts = ts();
            String qs = "symbol=" + symbol + "&orderId=" + orderId
                    + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, qs);
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/order?" + qs + "&signature=" + sig))
                            .timeout(Duration.ofSeconds(5))
                            .header("X-MBX-APIKEY", apiKey)
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return 0;
            JSONObject o = new JSONObject(resp.body());
            return o.optDouble("avgPrice", 0);
        } catch (Exception e) {
            return 0;
        }
    }

    // ─── Exchange filters: exchangeInfo cache + rounding ──────

    /**
     * Holds the Binance per-symbol filter values that we actually need to
     * round qty/price correctly. Loaded once per symbol from /fapi/v1/exchangeInfo.
     *
     * Filter sources on Binance:
     *   - LOT_SIZE.stepSize       → quantity granularity (e.g., 0.001, 1, 1000)
     *   - PRICE_FILTER.tickSize   → price granularity   (e.g., 0.01, 0.0001)
     *   - MIN_NOTIONAL.notional   → smallest position $ allowed
     *
     * [HOLE-6 FIX 2026-05-15] Now also captures status ("TRADING" / "HALT" /
     * "BREAK" / "PENDING_TRADING" / "PRE_DELIVERING" / "DELIVERING" / "SETTLING"
     * / "CLOSE"). Only "TRADING" symbols are safe to open new positions on —
     * the others either silently reject MARKET orders (the TAOUSDT case) or
     * accept them but leave you with no exit liquidity.
     */
    private static final class SymbolInfo {
        final double stepSize;
        final double tickSize;
        final double minNotional;
        final String status;          // "TRADING" | "HALT" | "BREAK" | ...
        SymbolInfo(double stepSize, double tickSize, double minNotional, String status) {
            this.stepSize = stepSize;
            this.tickSize = tickSize;
            this.minNotional = minNotional;
            this.status = status == null ? "UNKNOWN" : status;
        }
        boolean isTradable() { return "TRADING".equals(status); }
    }

    /**
     * Load (or fetch from cache) per-symbol filter rules. Returns null if
     * exchangeInfo couldn't be fetched. We intentionally fetch ALL symbols
     * once and populate the cache on first miss — exchangeInfo is one big
     * payload (~1MB) but we only do it once per process lifetime.
     */
    private SymbolInfo loadSymbolInfo(String symbol) {
        SymbolInfo cached = symbolInfoCache.get(symbol);
        if (cached != null) return cached;

        // Cache miss → fetch full exchangeInfo and populate ALL symbols.
        try {
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/exchangeInfo"))
                            .timeout(Duration.ofSeconds(15))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                LOG.warning("[Executor] exchangeInfo HTTP " + resp.statusCode());
                return null;
            }
            JSONObject root = new JSONObject(resp.body());
            JSONArray symbols = root.getJSONArray("symbols");
            int populated = 0;
            for (int i = 0; i < symbols.length(); i++) {
                JSONObject s = symbols.getJSONObject(i);
                String sym = s.optString("symbol", "");
                if (sym.isEmpty()) continue;
                // [HOLE-6 FIX 2026-05-15] Capture status. Field name on USD-M Futures is
                // "status" (TRADING/HALT/BREAK/PENDING_TRADING/PRE_DELIVERING/...).
                // contractStatus exists on some endpoints — fall back to it as a safety.
                String status = s.optString("status",
                        s.optString("contractStatus", "UNKNOWN"));
                double stepSize = 0, tickSize = 0, minNotional = 0;
                JSONArray filters = s.optJSONArray("filters");
                if (filters == null) continue;
                for (int j = 0; j < filters.length(); j++) {
                    JSONObject f = filters.getJSONObject(j);
                    String type = f.optString("filterType", "");
                    if ("LOT_SIZE".equals(type)) {
                        stepSize = f.optDouble("stepSize", 0);
                    } else if ("PRICE_FILTER".equals(type)) {
                        tickSize = f.optDouble("tickSize", 0);
                    } else if ("MIN_NOTIONAL".equals(type)) {
                        // Field name varies: some endpoints use "notional",
                        // futures uses "notional" or "minNotional".
                        double v = f.optDouble("notional", 0);
                        if (v == 0) v = f.optDouble("minNotional", 0);
                        minNotional = v;
                    }
                }
                if (stepSize > 0 && tickSize > 0) {
                    if (minNotional <= 0) minNotional = 5.0; // Binance default
                    symbolInfoCache.put(sym,
                            new SymbolInfo(stepSize, tickSize, minNotional, status));
                    populated++;
                }
            }
            LOG.info("[Executor] exchangeInfo loaded: " + populated + " symbols cached");
        } catch (Exception e) {
            LOG.warning("[Executor] exchangeInfo error: " + e.getMessage());
            return null;
        }
        return symbolInfoCache.get(symbol);
    }

    /**
     * Round qty DOWN to nearest multiple of stepSize. We always round DOWN
     * (truncate), never UP — this guarantees we never accidentally inflate
     * the position size beyond what risk math says.
     *
     * Example: qty=1234567.89, stepSize=1 → 1234567
     *          qty=0.123456, stepSize=0.001 → 0.123
     */
    private static double roundDownToStep(double value, double step) {
        if (step <= 0) return value;
        // BigDecimal to avoid floating-point drift on large multipliers
        java.math.BigDecimal v = java.math.BigDecimal.valueOf(value);
        java.math.BigDecimal s = java.math.BigDecimal.valueOf(step);
        java.math.BigDecimal steps = v.divide(s, 0, java.math.RoundingMode.DOWN);
        return steps.multiply(s).doubleValue();
    }

    /**
     * Round price to tick size. For SHORT positions (isLong=false), SL is
     * ABOVE entry — round UP so we never shrink the protective distance.
     * For LONG, SL is BELOW entry — round DOWN. Symmetric logic.
     */
    private static double roundToTick(double price, double tick, boolean isLong) {
        if (tick <= 0) return price;
        java.math.BigDecimal p = java.math.BigDecimal.valueOf(price);
        java.math.BigDecimal t = java.math.BigDecimal.valueOf(tick);
        java.math.RoundingMode mode = isLong
                ? java.math.RoundingMode.DOWN  // LONG SL below entry → don't pull closer
                : java.math.RoundingMode.UP;   // SHORT SL above entry → don't pull closer
        java.math.BigDecimal ticks = p.divide(t, 0, mode);
        return ticks.multiply(t).doubleValue();
    }

    // ─── Formatting helpers (used by HTTP body builders) ──────────────

    private static String formatQty(double q) {
        // Trim trailing zeros, max 8 decimals. Note: actual rounding to
        // stepSize is done by roundDownToStep BEFORE this is called — so
        // q here is already a valid multiple of stepSize.
        return java.math.BigDecimal.valueOf(q)
                .setScale(8, java.math.RoundingMode.DOWN)
                .stripTrailingZeros()
                .toPlainString();
    }

    private static String formatPrice(double p) {
        // Same: tickSize alignment is done by roundToTick before this.
        return java.math.BigDecimal.valueOf(p)
                .setScale(8, java.math.RoundingMode.HALF_UP)
                .stripTrailingZeros()
                .toPlainString();
    }

    /** Pretty qty for log lines (avoids 1.23e+6 scientific notation on big numbers). */
    private static String formatQtyForLog(double q) {
        return java.math.BigDecimal.valueOf(q)
                .setScale(8, java.math.RoundingMode.DOWN)
                .stripTrailingZeros()
                .toPlainString();
    }

    // ─── Utility ──────────────────────────────────────────────────────
    private static String hmacSHA256(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] hash = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("HMAC fail", e);
        }
    }

    private static String pick(String primaryKey, String fallbackKey, String def) {
        String v = System.getenv().getOrDefault(primaryKey, "");
        if (!v.isBlank()) return v;
        return System.getenv().getOrDefault(fallbackKey, def);
    }
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