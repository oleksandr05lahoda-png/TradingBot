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

            // 5. Place SL — STOP_MARKET через Algo Service.
            // [v83.3] С 2025-12-09 условные ордера живут на /fapi/v1/algoOrder.
            // Параметр closePosition=true оставляем — закрывает всю позицию
            // при срабатывании. reduceOnly с closePosition несовместим, не шлём.
            // slPriceRounded уже снап на tickSize — иначе Binance отклонит.
            //
            // [v84.0 DIAGNOSTIC] При фейле логируем тело ответа ПОЛНОСТЬЮ
            // и SEVERE-уровнем. До этого ошибка терялась в WARNING.
            String slOrderId = null;
            String slLastError = "no attempts";
            long slDeadline = System.currentTimeMillis() + slPlacementTimeoutMs;
            int attempts = 0;
            while (System.currentTimeMillis() < slDeadline && attempts < 3) {
                attempts++;
                String[] slResult = sendStopMarketOrderDiag(symbol, !isLong, slPriceRounded);
                slOrderId = slResult[0];
                slLastError = slResult[1];
                if (slOrderId != null) break;
                LOG.warning("[Executor] SL attempt " + attempts + "/3 failed for "
                        + symbol + ": " + slLastError);
                Thread.sleep(500);
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
                tp1Price = tp1Rounded;
                tp2Price = tp2Rounded;

                // Split qty 50/50, ensuring sum doesn't exceed total.
                double tp1Qty = roundDownToStep(qty * 0.5, si.stepSize);
                double tp2Qty = qty - tp1Qty; // remainder, already a step multiple

                // Check minNotional for partial TPs
                double tp1Notional = tp1Qty * actualEntry;
                double tp2Notional = tp2Qty * actualEntry;
                boolean partialOk = tp1Qty > 0 && tp2Qty > 0
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

                // Fallback: position too small for partial TPs → single TP at tp2
                // with closePosition=true. Better one TP than none.
                if (!partialOk && tp2Rounded > 0) {
                    LOG.info("[Executor] " + symbol + " too small for partial TPs "
                            + "(notional=$" + String.format("%.2f", actualNotional)
                            + " min=$" + si.minNotional + ") — single TP at tp2 fallback");
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
        long ts = System.currentTimeMillis();
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
        long ts = System.currentTimeMillis();
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

    private void cancelAllOpenOrders(String symbol) {
        // 1) Обычные ордера
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
            LOG.warning("[Executor] cancelAllOrders(plain) " + symbol + " error: " + e.getMessage());
        }

        // 2) Алго-ордера (SL/TP/трейлинги — теперь отдельная очередь)
        try {
            long ts = System.currentTimeMillis();
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