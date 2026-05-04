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
 * BinanceTradeExecutor v1.0 — live order execution for Binance USD-M Futures.
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  ВАЖНО: этот класс ничего не делает без явного вызова из BotMain.   │
 * │  Сам по себе он лежит "в спячке". Активация — на Этапе 3.           │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * Принципы безопасности:
 *
 *  1. NEVER OPEN WITHOUT SL.
 *     После MARKET-открытия позиции — немедленно (в той же транзакции,
 *     одним методом) отправляется STOP_MARKET ордер на SL. Если SL
 *     не подтвердился за 5 секунд — позиция закрывается принудительно.
 *     Лучше потерять комиссию, чем остаться без стопа.
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
     * [v83.2] Cache of exchange filters per symbol. Populated lazily on first
     * trade by reading /fapi/v1/exchangeInfo. Without this, qty/price rounding
     * is wrong for many alts → MARKET order rejected by Binance.
     *
     * Each entry holds the actual stepSize / tickSize / minNotional rules
     * Binance enforces. Cache lives until process restart, which is fine —
     * exchangeInfo changes maybe once a quarter for any given symbol.
     */
    private final ConcurrentHashMap<String, SymbolInfo> symbolInfoCache = new ConcurrentHashMap<>();

    private static final BinanceTradeExecutor INSTANCE = new BinanceTradeExecutor();
    public static BinanceTradeExecutor getInstance() { return INSTANCE; }

    private BinanceTradeExecutor() {
        this.useTestnet = "1".equals(System.getenv().getOrDefault("BINANCE_USE_TESTNET", "1"));

        if (useTestnet) {
            this.apiKey    = pick("BINANCE_TESTNET_API_KEY", "BINANCE_API_KEY", "");
            this.apiSecret = pick("BINANCE_TESTNET_API_SECRET", "BINANCE_API_SECRET", "");
            // [v83.1] Binance переименовал testnet с testnet.binancefuture.com
            // на demo-fapi.binance.com. UI открыт на demo.binance.com, REST API
            // на demo-fapi.binance.com. Источник: developers.binance.com docs.
            this.baseUrl   = "https://demo-fapi.binance.com";
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
        this.riskPctPerTrade = Math.max(0.5, Math.min(5.0, rp));
        if (rp > 5.0) {
            LOG.warning("[Executor] RISK_PCT_PER_TRADE=" + rp + " requested, capped to 5");
        }

        this.slPlacementTimeoutMs = envLong("SL_PLACEMENT_TIMEOUT_MS", 5000L);
        this.maxSpreadPct         = envDouble("MAX_SPREAD_PCT", 0.5);

        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        LOG.info(String.format("[Executor] init: %s leverage=%dx risk=%.1f%%/trade keys=%s",
                useTestnet ? "TESTNET" : "REAL/LIVE",
                leverage, riskPctPerTrade,
                apiKey.isBlank() ? "MISSING" : "present"));
    }

    /** True if exchange credentials are configured and we can call API. */
    public boolean isReady() {
        return !apiKey.isBlank() && !apiSecret.isBlank();
    }

    public boolean isTestnet() { return useTestnet; }
    public int  getLeverage()    { return leverage; }
    public double getRiskPct()   { return riskPctPerTrade; }

    // ─── Public API ───────────────────────────────────────────────────

    public static final class ExecutionResult {
        public final boolean success;
        public final String  reason;
        public final String  orderId;        // main entry order id
        public final String  slOrderId;      // SL order id
        public final double  entryPrice;     // actual fill price
        public final double  qty;            // base quantity
        public final double  notionalUsd;
        public final double  slPrice;
        public final long    timestampMs;

        private ExecutionResult(boolean ok, String reason, String orderId, String slOrderId,
                                double entry, double qty, double notional, double sl) {
            this.success = ok; this.reason = reason;
            this.orderId = orderId; this.slOrderId = slOrderId;
            this.entryPrice = entry; this.qty = qty;
            this.notionalUsd = notional; this.slPrice = sl;
            this.timestampMs = System.currentTimeMillis();
        }

        public static ExecutionResult fail(String reason) {
            return new ExecutionResult(false, reason, "", "", 0, 0, 0, 0);
        }
        public static ExecutionResult ok(String orderId, String slOrderId,
                                         double entry, double qty, double notional, double sl) {
            return new ExecutionResult(true, "OK", orderId, slOrderId, entry, qty, notional, sl);
        }

        @Override public String toString() {
            return success
                    ? String.format("OK orderId=%s slId=%s entry=%.6f qty=%.6f notional=$%.2f sl=%.6f",
                    orderId, slOrderId, entryPrice, qty, notionalUsd, slPrice)
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
            long ts = System.currentTimeMillis();
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
            // 1. Initialize symbol settings (leverage + isolated)
            if (!initializedSymbols.contains(symbol)) {
                if (!initSymbolMargin(symbol)) {
                    return ExecutionResult.fail("init margin failed for " + symbol);
                }
                if (!initSymbolLeverage(symbol)) {
                    return ExecutionResult.fail("init leverage failed for " + symbol);
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
                return ExecutionResult.fail(String.format("spread %.2f%% > max %.2f%%",
                        spreadPct, maxSpreadPct));
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

            // [v83.2] Load real exchange filters for this symbol. Critical:
            // every pair has its own stepSize (qty granularity), tickSize
            // (price granularity), minNotional (smallest position). Hardcoded
            // 3-decimal rounding of v1 worked for BTC/ETH/SOL but rejected
            // exotic alts like BABYUSDT (need integer qty in thousands).
            SymbolInfo si = loadSymbolInfo(symbol);
            if (si == null) {
                return ExecutionResult.fail("cannot load exchangeInfo for " + symbol);
            }

            double riskUsd = balanceUsd * (riskPctPerTrade / 100.0);
            double qty = riskUsd / slDistance;
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

            // Wait briefly for fill, fetch actual fill price
            Thread.sleep(800);
            double actualEntry = fetchOrderAvgPrice(symbol, entryOrderId);
            if (actualEntry <= 0) actualEntry = entry; // fallback

            double actualNotional = qty * actualEntry;

            // 5. Place SL — STOP_MARKET, reduceOnly, closePosition.
            // Use slPriceRounded (snapped to tickSize) — raw idea.stop would
            // get rejected by Binance for tick alignment.
            String slOrderId = null;
            long slDeadline = System.currentTimeMillis() + slPlacementTimeoutMs;
            int attempts = 0;
            while (System.currentTimeMillis() < slDeadline && attempts < 3) {
                attempts++;
                slOrderId = sendStopMarketOrder(symbol, !isLong, slPriceRounded);
                if (slOrderId != null) break;
                Thread.sleep(500);
            }

            // 6. SL FAILED — emergency close
            if (slOrderId == null) {
                LOG.severe("[Executor] CRITICAL: SL placement failed after " + attempts
                        + " attempts on " + symbol + " — emergency closing position");
                emergencyClosePosition(symbol, isLong, qty);
                return ExecutionResult.fail("SL not placed → emergency close");
            }

            LOG.info(String.format(
                    "[Executor] OPENED %s %s qty=%s entry=%.8f notional=$%.2f sl=%.8f orderId=%s slId=%s",
                    symbol, isLong ? "LONG" : "SHORT", formatQtyForLog(qty), actualEntry,
                    actualNotional, slPriceRounded, entryOrderId, slOrderId));

            return ExecutionResult.ok(entryOrderId, slOrderId, actualEntry, qty,
                    actualNotional, slPriceRounded);
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
            long ts = System.currentTimeMillis();
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
            long ts = System.currentTimeMillis();
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

    // ─── Internal: HTTP signed calls ──────────────────────────────────

    private boolean initSymbolMargin(String symbol) throws Exception {
        long ts = System.currentTimeMillis();
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
        if (code == 200) return true;
        if (resp.body() != null && resp.body().contains("-4046")) return true;
        LOG.warning("[Executor] marginType " + symbol + " HTTP " + code + ": " + resp.body());
        return false;
    }

    private boolean initSymbolLeverage(String symbol) throws Exception {
        long ts = System.currentTimeMillis();
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
        if (resp.statusCode() == 200) return true;
        LOG.warning("[Executor] leverage " + symbol + " HTTP " + resp.statusCode() + ": " + resp.body());
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
        long ts = System.currentTimeMillis();
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
            LOG.warning("[Executor] MARKET order " + symbol + " HTTP " + resp.statusCode()
                    + ": " + resp.body());
            return null;
        }
        return new JSONObject(resp.body()).optString("orderId", null);
    }

    /** Send a STOP_MARKET order. Returns orderId on success, null otherwise. */
    private String sendStopMarketOrder(String symbol, boolean buy, double stopPrice) throws Exception {
        long ts = System.currentTimeMillis();
        String body = "symbol=" + symbol
                + "&side=" + (buy ? "BUY" : "SELL")
                + "&type=STOP_MARKET"
                + "&stopPrice=" + formatPrice(stopPrice)
                + "&closePosition=true"
                + "&workingType=MARK_PRICE"
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
            LOG.warning("[Executor] STOP_MARKET " + symbol + " HTTP " + resp.statusCode()
                    + ": " + resp.body());
            return null;
        }
        return new JSONObject(resp.body()).optString("orderId", null);
    }

    /** Cancel ALL open orders on a symbol. */
    private void cancelAllOpenOrders(String symbol) {
        try {
            long ts = System.currentTimeMillis();
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
            LOG.warning("[Executor] cancelAllOrders " + symbol + " error: " + e.getMessage());
        }
    }

    /** Emergency: close just-opened position by sending opposite market order. */
    private void emergencyClosePosition(String symbol, boolean wasLong, double qty) {
        try {
            // Opposite side, reduceOnly to ensure we don't accidentally flip
            long ts = System.currentTimeMillis();
            String body = "symbol=" + symbol
                    + "&side=" + (wasLong ? "SELL" : "BUY")
                    + "&type=MARKET"
                    + "&quantity=" + formatQty(qty)
                    + "&reduceOnly=true"
                    + "&timestamp=" + ts + "&recvWindow=5000";
            String sig = hmacSHA256(apiSecret, body);
            http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/fapi/v1/order"))
                            .timeout(Duration.ofSeconds(10))
                            .header("X-MBX-APIKEY", apiKey)
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .POST(HttpRequest.BodyPublishers.ofString(body + "&signature=" + sig))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            LOG.severe("[Executor] EMERGENCY CLOSE FAILED " + symbol + ": " + e.getMessage()
                    + " — MANUAL INTERVENTION REQUIRED");
        }
    }

    /** Fetch average fill price from order. Returns 0 if not filled. */
    private double fetchOrderAvgPrice(String symbol, String orderId) {
        try {
            long ts = System.currentTimeMillis();
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

    // ─── [v83.2] Exchange filters: exchangeInfo cache + rounding ──────

    /**
     * Holds the Binance per-symbol filter values that we actually need to
     * round qty/price correctly. Loaded once per symbol from /fapi/v1/exchangeInfo.
     *
     * Filter sources on Binance:
     *   - LOT_SIZE.stepSize       → quantity granularity (e.g., 0.001, 1, 1000)
     *   - PRICE_FILTER.tickSize   → price granularity   (e.g., 0.01, 0.0001)
     *   - MIN_NOTIONAL.notional   → smallest position $ allowed
     */
    private static final class SymbolInfo {
        final double stepSize;
        final double tickSize;
        final double minNotional;
        SymbolInfo(double stepSize, double tickSize, double minNotional) {
            this.stepSize = stepSize;
            this.tickSize = tickSize;
            this.minNotional = minNotional;
        }
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
                    symbolInfoCache.put(sym, new SymbolInfo(stepSize, tickSize, minNotional));
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