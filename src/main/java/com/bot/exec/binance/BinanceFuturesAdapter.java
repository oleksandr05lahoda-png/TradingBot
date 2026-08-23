package com.bot.exec.binance;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.exec.ExchangeException;
import com.bot.exec.ExchangePort;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.exec.ClientOrderIdFactory;
import com.bot.exec.OrderRequest;
import com.bot.exec.OrderTypes.OrderPurpose;
import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.exec.RateLimiter;
import com.bot.risk.MarginTier;
import com.bot.risk.MarginTierTable;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * {@link ExchangePort} over Binance USDⓈ-M futures REST, demo or real per its {@link BinanceVenue};
 * every request URL passes {@link BinanceVenue#require}, so a request aimed at the wrong venue dies
 * inside this process. A timeout or a 5xx stays {@link ExchangeException#ambiguous()} and never
 * becomes a refusal — {@code IdempotentOrderPlacer} handles the two differently.
 */
public final class BinanceFuturesAdapter implements ExchangePort {

    private static final Logger LOG = Logger.getLogger(BinanceFuturesAdapter.class.getName());

    private static final long FILTER_CACHE_TTL_MS = 6 * 60 * 60 * 1000L;

    private record CachedFilters(InstrumentFilters filters, long fetchedAtMs) {}

    private final BinanceVenue venue;
    private final HttpClient http;
    private final BinanceSigner signer;
    private final RateLimiter rateLimiter;
    private final long recvWindowMs;
    private final ConcurrentHashMap<String, CachedFilters> filterCache = new ConcurrentHashMap<>();

    private volatile long clockOffsetMs = 0;
    private volatile boolean conditionalListingAvailable = true;

    public static BinanceFuturesAdapter fromEnvironment(BinanceVenue venue) {
        return new BinanceFuturesAdapter(
                venue,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build(),
                BinanceSigner.fromEnvironment(venue),
                RateLimiter.binanceDefaults(),
                BinanceSigner.DEFAULT_RECV_WINDOW_MS);
    }

    public BinanceFuturesAdapter(BinanceVenue venue, HttpClient http, BinanceSigner signer,
                                 RateLimiter rateLimiter, long recvWindowMs) {
        this.venue = Preconditions.notNull(venue, "venue");
        this.http = Preconditions.notNull(http, "http");
        this.signer = Preconditions.notNull(signer, "signer");
        this.rateLimiter = Preconditions.notNull(rateLimiter, "rateLimiter");
        this.recvWindowMs = recvWindowMs;
        synchronizeClock();
    }

    @Override public String endpointHost() {
        return venue.restHost();
    }

    /** Records the offset every signed request is stamped with. */
    public void synchronizeClock() {
        try {
            JSONObject time = new JSONObject(publicGet("/fapi/v1/time", Map.of(), 1));
            long serverTime = time.getLong("serverTime");
            clockOffsetMs = serverTime - System.currentTimeMillis();
            if (Math.abs(clockOffsetMs) > 1000) {
                LOG.warning("[Binance] host clock is " + clockOffsetMs
                        + "ms from the exchange; signing against the exchange's clock");
            }
        } catch (RuntimeException e) {
            LOG.warning("[Binance] clock sync failed, using the local clock: " + e.getMessage());
        }
    }

    @Override public long serverTimeMillis() {
        return System.currentTimeMillis() + clockOffsetMs;
    }

    // ─── Account and instrument data ─────────────────────────────────────────────────────────

    @Override public AccountSnapshot fetchAccount() {
        JSONObject account = new JSONObject(signedGet("/fapi/v2/account", Map.of(), 5));
        return new AccountSnapshot(
                decimal(account, "totalWalletBalance"),
                decimal(account, "availableBalance"),
                decimal(account, "totalUnrealizedProfit"),
                serverTimeMillis());
    }

    @Override public InstrumentFilters fetchFilters(String symbol) {
        Preconditions.notBlank(symbol, "symbol");
        CachedFilters cached = filterCache.get(symbol);
        if (cached != null && System.currentTimeMillis() - cached.fetchedAtMs() < FILTER_CACHE_TTL_MS) {
            return cached.filters();
        }
        JSONObject info = new JSONObject(publicGet("/fapi/v1/exchangeInfo", Map.of("symbol", symbol), 1));
        JSONArray symbols = info.getJSONArray("symbols");
        for (int i = 0; i < symbols.length(); i++) {
            JSONObject s = symbols.getJSONObject(i);
            if (!symbol.equals(s.getString("symbol"))) continue;
            InstrumentFilters filters = parseFilters(s);
            filterCache.put(symbol, new CachedFilters(filters, System.currentTimeMillis()));
            return filters;
        }
        throw ExchangeException.refused(
                symbol + " is not listed on " + endpointHost()
                        + (venue.isReal() ? ""
                                : " — the testnet lists a different, smaller universe than the live exchange"),
                200, 0);
    }

    private static InstrumentFilters parseFilters(JSONObject symbolInfo) {
        BigDecimal tickSize = null, minPrice = null, maxPrice = null;
        BigDecimal stepSize = null, minQty = null, maxQty = null, marketMaxQty = null;
        BigDecimal minNotional = null;

        JSONArray filters = symbolInfo.getJSONArray("filters");
        for (int i = 0; i < filters.length(); i++) {
            JSONObject f = filters.getJSONObject(i);
            switch (f.getString("filterType")) {
                case "PRICE_FILTER" -> {
                    tickSize = decimal(f, "tickSize");
                    minPrice = decimal(f, "minPrice");
                    maxPrice = decimal(f, "maxPrice");
                }
                case "LOT_SIZE" -> {
                    stepSize = decimal(f, "stepSize");
                    minQty = decimal(f, "minQty");
                    maxQty = decimal(f, "maxQty");
                }
                case "MARKET_LOT_SIZE" -> marketMaxQty = decimal(f, "maxQty");
                case "MIN_NOTIONAL" -> minNotional = decimal(f, "notional");
                default -> { /* PERCENT_PRICE, MAX_NUM_ORDERS and friends are not used here */ }
            }
        }
        Preconditions.require(tickSize != null && stepSize != null && minQty != null,
                "exchangeInfo for " + symbolInfo.getString("symbol")
                        + " is missing PRICE_FILTER or LOT_SIZE — refusing to guess them");
        if (marketMaxQty == null) marketMaxQty = maxQty;
        // Absent MIN_NOTIONAL falls back to the 5 USDT floor, not zero: fail toward rejecting (19.08).
        if (minNotional == null) minNotional = new BigDecimal("5");

        return new InstrumentFilters(
                symbolInfo.getString("symbol"),
                tickSize, minPrice, maxPrice,
                stepSize, minQty, maxQty, marketMaxQty, minNotional,
                symbolInfo.getInt("pricePrecision"),
                symbolInfo.getInt("quantityPrecision"));
    }

    @Override public MarginTierTable fetchMarginTiers(String symbol) {
        JSONArray response = new JSONArray(
                signedGet("/fapi/v1/leverageBracket", Map.of("symbol", symbol), 1));
        for (int i = 0; i < response.length(); i++) {
            JSONObject entry = response.getJSONObject(i);
            if (!symbol.equals(entry.optString("symbol"))) continue;
            JSONArray brackets = entry.getJSONArray("brackets");

            // Widen the highest notionalFloor to infinity, not the last index: the response may descend.
            double highestFloor = Double.NEGATIVE_INFINITY;
            for (int b = 0; b < brackets.length(); b++) {
                highestFloor = Math.max(highestFloor, brackets.getJSONObject(b).getDouble("notionalFloor"));
            }

            List<MarginTier> tiers = new ArrayList<>();
            for (int b = 0; b < brackets.length(); b++) {
                JSONObject br = brackets.getJSONObject(b);
                double floor = br.getDouble("notionalFloor");
                double cap = floor == highestFloor ? Double.POSITIVE_INFINITY : br.getDouble("notionalCap");
                tiers.add(new MarginTier(floor, cap,
                        br.getDouble("maintMarginRatio"), br.getDouble("cum"),
                        br.getInt("initialLeverage")));
            }
            return new MarginTierTable(tiers);
        }
        throw ExchangeException.refused("no leverage brackets returned for " + symbol, 200, 0);
    }

    @Override public double fetchRealizedPnlSince(long sinceEpochMs) {
        // Commissions and funding count too. Income ascends from startTime, so pages must be walked.
        final int pageSize = 1000;
        final int maxPages = 20;

        double total = 0;
        long cursor = sinceEpochMs;
        for (int page = 0; page < maxPages; page++) {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("startTime", Long.toString(cursor));
            params.put("limit", Integer.toString(pageSize));
            JSONArray rows = new JSONArray(signedGet("/fapi/v1/income", params, 30));
            if (rows.isEmpty()) return total;

            long newestSeen = cursor;
            for (int i = 0; i < rows.length(); i++) {
                JSONObject row = rows.getJSONObject(i);
                String type = row.optString("incomeType", "");
                if (type.equals("REALIZED_PNL") || type.equals("COMMISSION") || type.equals("FUNDING_FEE")) {
                    total += row.optDouble("income", 0);
                }
                newestSeen = Math.max(newestSeen, row.optLong("time", cursor));
            }
            if (rows.length() < pageSize) return total;
            if (newestSeen <= cursor) {
                // Timestamps did not advance on a full page: paging cannot progress, looping would double-count.
                LOG.warning("[Binance] income paging stalled at " + cursor
                        + "; the realised-PnL total may be incomplete");
                return total;
            }
            // +1ms so the last row of this page is not counted again on the next one.
            cursor = newestSeen + 1;
        }
        LOG.warning("[Binance] income paging hit " + maxPages + " pages; the realised-PnL total may be incomplete");
        return total;
    }

    // ─── Position and margin settings ────────────────────────────────────────────────────────

    @Override public void ensureIsolatedMargin(String symbol) {
        // Ask before telling: Binance refuses a marginType POST while the symbol carries working
        // orders, and that refusal must stay non-benign — cross margin under isolated sizing is damage.
        if (isAlreadyIsolated(symbol)) {
            LOG.fine("[Binance] " + symbol + " is already isolated; no marginType call needed");
            return;
        }
        try {
            signedPost("/fapi/v1/marginType",
                    new LinkedHashMap<>(Map.of("symbol", symbol, "marginType", "ISOLATED")), 1, false);
        } catch (ExchangeException e) {
            if (!BinanceErrorCodes.isBenignAlreadyInDesiredState(e.exchangeCode())) throw e;
            LOG.fine("[Binance] " + symbol + " is already isolated");
        }
    }

    /** Any doubt returns false, so the caller still attempts the change and a real refusal surfaces. */
    private boolean isAlreadyIsolated(String symbol) {
        try {
            JSONArray rows = new JSONArray(
                    signedGet("/fapi/v2/positionRisk", Map.of("symbol", symbol), 5));
            for (int i = 0; i < rows.length(); i++) {
                JSONObject row = rows.getJSONObject(i);
                if (symbol.equals(row.optString("symbol", ""))) {
                    return "isolated".equalsIgnoreCase(row.optString("marginType", ""));
                }
            }
        } catch (RuntimeException e) {
            LOG.fine("[Binance] could not read margin type for " + symbol + ": " + e.getMessage());
        }
        return false;
    }

    /** Any refusal propagates, {@link BinanceErrorCodes#INVALID_LEVERAGE} included. */
    @Override public void setLeverage(String symbol, int leverage) {
        Preconditions.positive(leverage, "leverage");
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("leverage", Integer.toString(leverage));
        signedPost("/fapi/v1/leverage", params, 1, false);
    }

    @Override public List<PositionSnapshot> openPositions() {
        // positionRisk rather than the account endpoint: only this one carries liquidationPrice.
        JSONArray rows = new JSONArray(signedGet("/fapi/v2/positionRisk", Map.of(), 5));
        List<PositionSnapshot> out = new ArrayList<>();
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.getJSONObject(i);
            BigDecimal amount = decimal(row, "positionAmt");
            if (amount.signum() == 0) continue;
            out.add(new PositionSnapshot(
                    row.getString("symbol"),
                    amount,
                    decimal(row, "entryPrice"),
                    row.optInt("leverage", 1),
                    "isolated".equalsIgnoreCase(row.optString("marginType", "")),
                    decimal(row, "unRealizedProfit"),
                    decimal(row, "liquidationPrice")));
        }
        return out;
    }

    // ─── Orders ──────────────────────────────────────────────────────────────────────────────

    /**
     * Conditional orders go to {@code /fapi/v1/algoOrder}, the rest to {@code /fapi/v1/order}: Binance
     * split them in December 2025, and the plain endpoint answers {@code -4120} for a trigger type.
     */
    @Override public OrderStatus placeOrder(OrderRequest request) {
        return request.type().isConditional() ? placeAlgoOrder(request) : placePlainOrder(request);
    }

    private OrderStatus placePlainOrder(OrderRequest request) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", request.symbol());
        params.put("side", request.side().name());
        params.put("type", request.type().name());
        params.put("newClientOrderId", request.clientOrderId());
        params.put("newOrderRespType", "RESULT");   // ACK would not carry executedQty or avgPrice

        if (request.quantity() != null) params.put("quantity", request.quantity().toPlainString());
        if (request.price() != null) params.put("price", request.price().toPlainString());
        if (request.timeInForce() != null) params.put("timeInForce", request.timeInForce().name());
        // Sent only when true: Binance rejects reduceOnly and closePosition together, even as "false".
        if (request.reduceOnly()) params.put("reduceOnly", "true");

        OrderStatus placed = parseOrder(new JSONObject(signedPost("/fapi/v1/order", params, 1, true)));

        // RESULT should carry the average fill price; a reduce-only market close came back with zero.
        if (placed.hasFill() && placed.averagePrice().signum() == 0) {
            Optional<OrderStatus> settled = queryOrder(request.symbol(), request.clientOrderId());
            if (settled.isPresent() && settled.get().averagePrice().signum() > 0) return settled.get();
            LOG.warning("[Binance] " + request.clientOrderId() + " filled "
                    + placed.executedQuantity().toPlainString() + " but reported no average price");
        }
        return placed;
    }

    private OrderStatus placeAlgoOrder(OrderRequest request) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", request.symbol());
        params.put("side", request.side().name());
        // The endpoint's own names: `type` as on the plain one, but `clientAlgoId` and `triggerPrice`.
        params.put("algoType", "CONDITIONAL");
        params.put("type", request.type().name());
        params.put("clientAlgoId", request.clientOrderId());
        params.put("triggerPrice", request.stopPrice().toPlainString());

        if (request.quantity() != null) params.put("quantity", request.quantity().toPlainString());
        if (request.workingType() != null) params.put("workingType", request.workingType().name());
        if (request.reduceOnly()) params.put("reduceOnly", "true");
        if (request.closePosition()) params.put("closePosition", "true");

        return parseAlgoOrder(new JSONObject(signedPost("/fapi/v1/algoOrder", params, 1, true)),
                request.symbol());
    }

    @Override public Optional<OrderStatus> queryOrder(String symbol, String clientOrderId) {
        boolean algo = isAlgoId(clientOrderId);
        Map<String, String> params = new LinkedHashMap<>();
        // The algo endpoint identifies an order by id alone; the plain one requires the symbol.
        if (!algo) params.put("symbol", symbol);
        params.put(algo ? "clientAlgoId" : "origClientOrderId", clientOrderId);
        try {
            String body = signedGet(algo ? "/fapi/v1/algoOrder" : "/fapi/v1/order", params, 1);
            JSONObject json = new JSONObject(body);
            return Optional.of(algo ? parseAlgoOrder(json, symbol) : parseOrder(json));
        } catch (ExchangeException e) {
            if (e.exchangeCode() == BinanceErrorCodes.NO_SUCH_ORDER) {
                // Documented caveat: cancelled orders with no fills stop being queryable after three days.
                return Optional.empty();
            }
            throw e;
        }
    }

    /** Both endpoints: the reconciler reads this list, so omitting the algo half reports stops missing. */
    @Override public List<OrderStatus> openOrders(String symbol) {
        List<OrderStatus> out = new ArrayList<>();
        JSONArray plain = new JSONArray(signedGet("/fapi/v1/openOrders", Map.of("symbol", symbol), 1));
        for (int i = 0; i < plain.length(); i++) out.add(parseOrder(plain.getJSONObject(i)));

        // The path is openAlgoOrders, NOT algoOpenOrders: the transposed spelling 404s, listing then
        // returned nothing, and every closed position left its stop resting — 64 dead orders by 15.08,
        // against the conditional cap. A lost listing must not read as "no stops": canListConditionalOrders.
        try {
            JSONArray algo = new JSONArray(signedGet("/fapi/v1/openAlgoOrders", Map.of("symbol", symbol), 1));
            for (int i = 0; i < algo.length(); i++) out.add(parseAlgoOrder(algo.getJSONObject(i), symbol));
            conditionalListingAvailable = true;
        } catch (ExchangeException e) {
            if (e.httpStatus() != 404) throw e;
            if (conditionalListingAvailable) {
                conditionalListingAvailable = false;
                LOG.warning("[Binance] " + endpointHost() + " does not implement /fapi/v1/openAlgoOrders. "
                        + "Conditional orders cannot be enumerated, so reconciliation cannot verify "
                        + "that a position still has its stop, and closed positions will leave their "
                        + "stops resting until something cancels them by name.");
            }
        }
        return out;
    }

    @Override public boolean canListConditionalOrders() { return conditionalListingAvailable; }

    @Override public long heldByExchangeForMillis() { return rateLimiter.heldForMillis(); }

    @Override public void onExchangeHold(java.util.function.LongConsumer listener) { rateLimiter.onHold(listener); }

    @Override public void cancelOrder(String symbol, String clientOrderId) {
        boolean algo = isAlgoId(clientOrderId);
        Map<String, String> params = new LinkedHashMap<>();
        if (!algo) params.put("symbol", symbol);
        params.put(algo ? "clientAlgoId" : "origClientOrderId", clientOrderId);
        try {
            signedDelete(algo ? "/fapi/v1/algoOrder" : "/fapi/v1/order", params, 1);
        } catch (ExchangeException e) {
            if (!BinanceErrorCodes.isOrderAbsent(e.exchangeCode())) throw e;
        }
    }

    /** Cancels both kinds: {@code allOpenOrders} does not reach the conditional ones. */
    @Override public void cancelAllOpenOrders(String symbol) {
        signedDelete("/fapi/v1/allOpenOrders", new LinkedHashMap<>(Map.of("symbol", symbol)), 1);
        for (OrderStatus order : openOrders(symbol)) {
            if (order.isWorking()) cancelOrder(symbol, order.clientOrderId());
        }
    }

    /** Read from the purpose letter in the id; a foreign id reads as plain, costing one "no such order". */
    private static boolean isAlgoId(String clientOrderId) {
        return ClientOrderIdFactory.purposeOf(clientOrderId)
                .map(p -> p == OrderPurpose.STOP_LOSS || p == OrderPurpose.TAKE_PROFIT)
                .orElse(false);
    }

    @Override public void armDeadMansSwitch(String symbol, long countdownMillis) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("countdownTime", Long.toString(Math.max(0, countdownMillis)));
        signedPost("/fapi/v1/countdownCancelAll", params, 10, false);
    }

    @Override public void close() {
        // HttpClient holds nothing to release; present so callers can use try-with-resources.
    }

    // ─── Parsing ─────────────────────────────────────────────────────────────────────────────

    private static OrderStatus parseOrder(JSONObject o) {
        return new OrderStatus(
                o.getString("clientOrderId"),
                o.optLong("orderId", 0),
                o.getString("symbol"),
                parseState(o.optString("status", "")),
                parseType(o.optString("type", "")),
                decimal(o, "origQty"),
                decimal(o, "executedQty"),
                decimal(o, "avgPrice"),
                decimal(o, "stopPrice"),
                o.optBoolean("reduceOnly", false),
                o.optBoolean("closePosition", false),
                o.optLong("updateTime", 0));
    }

    /**
     * The payload renames everything ({@code algoId}/{@code clientAlgoId}/{@code algoStatus}/
     * {@code triggerPrice}); a conditional order has no fill, so quantity filled and price stay zero.
     */
    private static OrderStatus parseAlgoOrder(JSONObject o, String fallbackSymbol) {
        BigDecimal quantity = decimal(o, "quantity");
        return new OrderStatus(
                o.optString("clientAlgoId", ""),
                o.optLong("algoId", 0),
                o.optString("symbol", fallbackSymbol),
                parseAlgoState(o.optString("algoStatus", "")),
                parseType(o.optString("orderType", "")),
                quantity,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                decimal(o, "triggerPrice"),
                o.optBoolean("reduceOnly", false),
                o.optBoolean("closePosition", false),
                o.optLong("updateTime", o.optLong("createTime", 0)));
    }

    /** Working versus not is what the reconciler reads; an unrecognised value is UNKNOWN, not a guess. */
    private static OrderState parseAlgoState(String raw) {
        return switch (raw.toUpperCase(Locale.ROOT)) {
            case "NEW", "WORKING", "ACTIVE" -> OrderState.NEW;
            case "TRIGGERED", "FINISHED", "FILLED" -> OrderState.FILLED;
            case "CANCELLED", "CANCELED", "USER_CANCELLED" -> OrderState.CANCELED;
            case "EXPIRED" -> OrderState.EXPIRED;
            case "REJECTED" -> OrderState.REJECTED;
            default -> {
                LOG.warning("[Binance] unrecognised algo status \"" + raw + "\"");
                yield OrderState.UNKNOWN;
            }
        };
    }

    private static OrderState parseState(String raw) {
        try {
            return OrderState.valueOf(raw);
        } catch (IllegalArgumentException e) {
            // NEW_INSURANCE / NEW_ADL and future additions: never guess FILLED — it decides whether
            // a position is believed to exist.
            LOG.warning("[Binance] unrecognised order status \"" + raw + "\"");
            return OrderState.UNKNOWN;
        }
    }

    private static OrderType parseType(String raw) {
        try {
            return OrderType.valueOf(raw);
        } catch (IllegalArgumentException e) {
            return OrderType.MARKET;
        }
    }

    private static BigDecimal decimal(JSONObject o, String field) {
        if (!o.has(field) || o.isNull(field)) return BigDecimal.ZERO;
        String raw = o.get(field).toString();
        if (raw.isBlank()) return BigDecimal.ZERO;
        return new BigDecimal(raw);
    }

    // ─── Transport ───────────────────────────────────────────────────────────────────────────

    private String publicGet(String path, Map<String, String> params, int weight) {
        String query = BinanceSigner.encode(new LinkedHashMap<>(params));
        return send(() -> HttpRequest.newBuilder()
                .uri(venue.require(
                        venue.restBaseUrl() + path + (query.isEmpty() ? "" : "?" + query)))
                .GET(), weight, false);
    }

    private String signedGet(String path, Map<String, String> params, int weight) {
        return send(() -> HttpRequest.newBuilder()
                .uri(venue.require(venue.restBaseUrl() + path + "?"
                        + signer.signedQuery(new LinkedHashMap<>(params), serverTimeMillis(), recvWindowMs)))
                .header("X-MBX-APIKEY", signer.apiKey())
                .GET(), weight, false);
    }

    private String signedPost(String path, Map<String, String> params, int weight, boolean isOrder) {
        return send(() -> HttpRequest.newBuilder()
                .uri(venue.require(venue.restBaseUrl() + path + "?"
                        + signer.signedQuery(new LinkedHashMap<>(params), serverTimeMillis(), recvWindowMs)))
                .header("X-MBX-APIKEY", signer.apiKey())
                .POST(HttpRequest.BodyPublishers.noBody()), weight, isOrder);
    }

    private String signedDelete(String path, Map<String, String> params, int weight) {
        return send(() -> HttpRequest.newBuilder()
                .uri(venue.require(venue.restBaseUrl() + path + "?"
                        + signer.signedQuery(new LinkedHashMap<>(params), serverTimeMillis(), recvWindowMs)))
                .header("X-MBX-APIKEY", signer.apiKey())
                .DELETE(), weight, false);
    }

    private String send(java.util.function.Supplier<HttpRequest.Builder> fresh, int weight, boolean isOrder) {
        boolean resynced = false;
        while (true) {
            HttpRequest request;
            try {
                // Acquire BEFORE building the request: the limiter may sleep seconds on a 429
                // back-off or hours on a 418 ban, and a signature timestamped before that sleep
                // lands outside recvWindow - the -1021 that followed the 429 at 02:18 on 23.08.
                rateLimiter.acquire(weight, isOrder);
                request = fresh.get().timeout(Duration.ofSeconds(20)).build();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ExchangeException.ambiguous("interrupted while waiting to send", e);
            }
            try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            observeLimitHeaders(response);

            int status = response.statusCode();
            if (status == 429 || status == 418) {
                // Record the ban whatever Retry-After holds (RFC 7231 allows an HTTP-date): retrying
                // while banned escalates a 429 into a 418.
                long retryAfterMs = 60_000L;
                try {
                    retryAfterMs = response.headers().firstValue("Retry-After")
                            .map(v -> Long.parseLong(v.trim()) * 1000L).orElse(60_000L);
                } catch (NumberFormatException ignored) {
                    // Unparseable header: the default stands.
                }
                rateLimiter.observeBan(retryAfterMs);
                throw ExchangeException.refused("rate limited (HTTP " + status + "), backing off "
                        + (retryAfterMs / 1000) + "s", status, 0);
            }
            if (status >= 500) {
                // Binance calls EVERY 5xx "execution status unknown": a filled-but-502 entry must not
                // become an untracked position, so the placer probes instead of retrying blind.
                throw ExchangeException.ambiguous(
                        "HTTP " + status + " from the exchange — execution status is unknown", null);
            }
            if (status / 100 != 2) {
                int code = 0;
                String message = response.body();
                try {
                    JSONObject error = new JSONObject(response.body());
                    code = error.optInt("code", 0);
                    message = error.optString("msg", message);
                } catch (RuntimeException ignored) {
                    // Not JSON — keep the raw body.
                }
                if (code == BinanceErrorCodes.TIMESTAMP_OUT_OF_RECV_WINDOW && !resynced) {
                    // -1021 is refused before anything executes, so re-signing is safe for every
                    // method. Once: a second refusal means the clock, not the timing.
                    LOG.warning("[Binance] signature rejected on clock skew — resynchronising and retrying once");
                    synchronizeClock();
                    resynced = true;
                    continue;
                }
                throw ExchangeException.refused(
                        request.method() + " " + request.uri().getPath() + " -> " + message, status, code);
            }
            return response.body();

        } catch (HttpTimeoutException e) {
            throw ExchangeException.ambiguous(
                    "timeout on " + request.method() + " " + request.uri().getPath()
                            + " — the request may have been executed", e);
        } catch (IOException e) {
            throw ExchangeException.ambiguous(
                    "transport failure on " + request.method() + " " + request.uri().getPath()
                            + " — the request may have been executed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExchangeException.ambiguous("interrupted while sending " + request.uri().getPath(), e);
        }
        }
    }

    private void observeLimitHeaders(HttpResponse<String> response) {
        response.headers().firstValue("x-mbx-used-weight-1m")
                .or(() -> response.headers().firstValue("X-MBX-USED-WEIGHT-1M"))
                .ifPresent(value -> {
                    try {
                        rateLimiter.observeUsedWeight(Integer.parseInt(value.trim()));
                    } catch (NumberFormatException ignored) {
                        // Malformed header: the local tally stands.
                    }
                });
    }

    @Override public String toString() {
        return "BinanceFuturesAdapter[" + venue.name() + " " + endpointHost().toLowerCase(Locale.ROOT) + "]";
    }
}
