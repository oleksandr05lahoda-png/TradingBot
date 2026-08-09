package com.bot.exec.binance;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.exec.ExchangeException;
import com.bot.exec.ExchangePort;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.exec.OrderRequest;
import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.exec.RateLimiter;
import com.bot.risk.MarginTier;
import com.bot.risk.MarginTierTable;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
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
 * {@link ExchangePort} over Binance USDⓈ-M futures <b>testnet</b> REST.
 *
 * <p>Everything exchange-specific lives here: signing, the query-string layout, JSON field names,
 * numeric error codes, rate-limit headers and the base URL. Nothing above this class knows that
 * Binance exists, which is why the risk engine and the whole execution protocol are testable against
 * a fake without a socket.
 *
 * <p>Three details worth pointing at:
 *
 * <p><b>Every URL goes through {@link BinanceTestnetEndpoint#requireTestnet}.</b> Not just the base:
 * each assembled request is re-checked, so no future edit can route a single call somewhere else.
 *
 * <p><b>Clock drift is corrected against the exchange, not trusted from the host.</b> Binance
 * validates {@code serverTime - timestamp <= recvWindow}, so a host clock a few seconds off makes
 * every signed request fail with {@code -1021} — including, at the worst possible moment, the one
 * placing a stop.
 *
 * <p><b>Ambiguity is preserved rather than flattened.</b> A timeout or a 503 becomes
 * {@link ExchangeException#ambiguous()}, an error code becomes a refusal. The caller
 * ({@code IdempotentOrderPlacer}) makes a genuinely different decision in each case.
 */
public final class BinanceFuturesTestnetAdapter implements ExchangePort {

    private static final Logger LOG = Logger.getLogger(BinanceFuturesTestnetAdapter.class.getName());

    private static final long FILTER_CACHE_TTL_MS = 6 * 60 * 60 * 1000L;

    private record CachedFilters(InstrumentFilters filters, long fetchedAtMs) {}

    private final HttpClient http;
    private final BinanceSigner signer;
    private final RateLimiter rateLimiter;
    private final long recvWindowMs;
    private final ConcurrentHashMap<String, CachedFilters> filterCache = new ConcurrentHashMap<>();

    private volatile long clockOffsetMs = 0;

    public static BinanceFuturesTestnetAdapter fromEnvironment() {
        return new BinanceFuturesTestnetAdapter(
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build(),
                BinanceSigner.fromEnvironment(),
                RateLimiter.binanceDefaults(),
                BinanceSigner.DEFAULT_RECV_WINDOW_MS);
    }

    public BinanceFuturesTestnetAdapter(HttpClient http, BinanceSigner signer,
                                        RateLimiter rateLimiter, long recvWindowMs) {
        this.http = Preconditions.notNull(http, "http");
        this.signer = Preconditions.notNull(signer, "signer");
        this.rateLimiter = Preconditions.notNull(rateLimiter, "rateLimiter");
        this.recvWindowMs = recvWindowMs;
        synchronizeClock();
    }

    @Override public String endpointHost() {
        return BinanceTestnetEndpoint.restHost();
    }

    /** Fetches server time and records the offset every signed request will be stamped with. */
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
                        + " — the testnet lists a different, smaller universe than the live exchange", 200, 0);
    }

    private static InstrumentFilters parseFilters(JSONObject symbolInfo) {
        BigDecimal tickSize = null, minPrice = null, maxPrice = null;
        BigDecimal stepSize = null, minQty = null, maxQty = null, marketMaxQty = null;
        BigDecimal minNotional = BigDecimal.ZERO;

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
            List<MarginTier> tiers = new ArrayList<>();
            for (int b = 0; b < brackets.length(); b++) {
                JSONObject br = brackets.getJSONObject(b);
                double floor = br.getDouble("notionalFloor");
                double cap = br.getDouble("notionalCap");
                // The topmost bracket carries a large finite cap; it is the "and everything above"
                // bracket, so it is widened to infinity to satisfy the table's coverage invariant.
                boolean isLast = b == brackets.length() - 1;
                tiers.add(new MarginTier(floor, isLast ? Double.POSITIVE_INFINITY : cap,
                        br.getDouble("maintMarginRatio"), br.getDouble("cum"),
                        br.getInt("initialLeverage")));
            }
            return new MarginTierTable(tiers);
        }
        throw ExchangeException.refused("no leverage brackets returned for " + symbol, 200, 0);
    }

    @Override public double fetchRealizedPnlSince(long sinceEpochMs) {
        // Realised PnL alone understates the day: commissions and funding are money that left the
        // account just as surely, and the daily loss limit is about the account, not about a
        // bookkeeping category.
        Map<String, String> params = new LinkedHashMap<>();
        params.put("startTime", Long.toString(sinceEpochMs));
        params.put("limit", "1000");
        JSONArray rows = new JSONArray(signedGet("/fapi/v1/income", params, 30));
        double total = 0;
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.getJSONObject(i);
            String type = row.optString("incomeType", "");
            if (type.equals("REALIZED_PNL") || type.equals("COMMISSION") || type.equals("FUNDING_FEE")) {
                total += row.optDouble("income", 0);
            }
        }
        return total;
    }

    // ─── Position and margin settings ────────────────────────────────────────────────────────

    @Override public void ensureIsolatedMargin(String symbol) {
        try {
            signedPost("/fapi/v1/marginType",
                    new LinkedHashMap<>(Map.of("symbol", symbol, "marginType", "ISOLATED")), 1, false);
        } catch (ExchangeException e) {
            if (!BinanceErrorCodes.isBenignAlreadyInDesiredState(e.exchangeCode())) throw e;
            LOG.fine("[Binance] " + symbol + " is already isolated");
        }
    }

    @Override public void setLeverage(String symbol, int leverage) {
        Preconditions.positive(leverage, "leverage");
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("symbol", symbol);
            params.put("leverage", Integer.toString(leverage));
            signedPost("/fapi/v1/leverage", params, 1, false);
        } catch (ExchangeException e) {
            if (!BinanceErrorCodes.isBenignAlreadyInDesiredState(e.exchangeCode())) throw e;
        }
    }

    @Override public List<PositionSnapshot> openPositions() {
        // positionRisk rather than the account endpoint: it is the one that carries liquidationPrice,
        // and a liquidation price read from the exchange is the check on the one this system computes.
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

    @Override public OrderStatus placeOrder(OrderRequest request) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", request.symbol());
        params.put("side", request.side().name());
        params.put("type", request.type().name());
        params.put("newClientOrderId", request.clientOrderId());
        params.put("newOrderRespType", "RESULT");   // ACK would not carry executedQty or avgPrice

        if (request.quantity() != null) params.put("quantity", request.quantity().toPlainString());
        if (request.price() != null) params.put("price", request.price().toPlainString());
        if (request.stopPrice() != null) params.put("stopPrice", request.stopPrice().toPlainString());
        if (request.timeInForce() != null) params.put("timeInForce", request.timeInForce().name());
        if (request.workingType() != null) params.put("workingType", request.workingType().name());
        // Sent only when true: Binance rejects reduceOnly and closePosition together, and an explicit
        // "false" for one of them alongside the other is exactly that combination.
        if (request.reduceOnly()) params.put("reduceOnly", "true");
        if (request.closePosition()) params.put("closePosition", "true");

        return parseOrder(new JSONObject(signedPost("/fapi/v1/order", params, 1, true)));
    }

    @Override public Optional<OrderStatus> queryOrder(String symbol, String clientOrderId) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("origClientOrderId", clientOrderId);
        try {
            return Optional.of(parseOrder(new JSONObject(signedGet("/fapi/v1/order", params, 1))));
        } catch (ExchangeException e) {
            if (e.exchangeCode() == BinanceErrorCodes.NO_SUCH_ORDER) {
                // Genuinely unknown to the exchange. Note the documented caveat: cancelled orders with
                // no fills stop being queryable after three days, far longer than any order here lives.
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override public List<OrderStatus> openOrders(String symbol) {
        JSONArray rows = new JSONArray(signedGet("/fapi/v1/openOrders", Map.of("symbol", symbol), 1));
        List<OrderStatus> out = new ArrayList<>();
        for (int i = 0; i < rows.length(); i++) out.add(parseOrder(rows.getJSONObject(i)));
        return out;
    }

    @Override public void cancelOrder(String symbol, String clientOrderId) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("origClientOrderId", clientOrderId);
        try {
            signedDelete("/fapi/v1/order", params, 1);
        } catch (ExchangeException e) {
            if (!BinanceErrorCodes.isOrderAbsent(e.exchangeCode())) throw e;
        }
    }

    @Override public void cancelAllOpenOrders(String symbol) {
        signedDelete("/fapi/v1/allOpenOrders", new LinkedHashMap<>(Map.of("symbol", symbol)), 1);
    }

    @Override public void armDeadMansSwitch(String symbol, long countdownMillis) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("countdownTime", Long.toString(Math.max(0, countdownMillis)));
        signedPost("/fapi/v1/countdownCancelAll", params, 10, false);
    }

    @Override public void close() {
        // java.net.http.HttpClient holds no resource that needs releasing here; the method exists so
        // callers can use try-with-resources uniformly across ExchangePort implementations.
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

    private static OrderState parseState(String raw) {
        try {
            return OrderState.valueOf(raw);
        } catch (IllegalArgumentException e) {
            // NEW_INSURANCE / NEW_ADL and any future addition: unknown is unknown, never guessed as
            // FILLED, because a wrong guess here decides whether a position is believed to exist.
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
        return send(HttpRequest.newBuilder()
                .uri(BinanceTestnetEndpoint.requireTestnet(
                        BinanceTestnetEndpoint.REST_BASE_URL + path + (query.isEmpty() ? "" : "?" + query)))
                .GET(), weight, false);
    }

    private String signedGet(String path, Map<String, String> params, int weight) {
        String query = signer.signedQuery(new LinkedHashMap<>(params), serverTimeMillis(), recvWindowMs);
        return send(HttpRequest.newBuilder()
                .uri(BinanceTestnetEndpoint.requireTestnet(BinanceTestnetEndpoint.REST_BASE_URL + path + "?" + query))
                .header("X-MBX-APIKEY", signer.apiKey())
                .GET(), weight, false);
    }

    private String signedPost(String path, Map<String, String> params, int weight, boolean isOrder) {
        String query = signer.signedQuery(new LinkedHashMap<>(params), serverTimeMillis(), recvWindowMs);
        return send(HttpRequest.newBuilder()
                .uri(BinanceTestnetEndpoint.requireTestnet(BinanceTestnetEndpoint.REST_BASE_URL + path + "?" + query))
                .header("X-MBX-APIKEY", signer.apiKey())
                .POST(HttpRequest.BodyPublishers.noBody()), weight, isOrder);
    }

    private String signedDelete(String path, Map<String, String> params, int weight) {
        String query = signer.signedQuery(new LinkedHashMap<>(params), serverTimeMillis(), recvWindowMs);
        return send(HttpRequest.newBuilder()
                .uri(BinanceTestnetEndpoint.requireTestnet(BinanceTestnetEndpoint.REST_BASE_URL + path + "?" + query))
                .header("X-MBX-APIKEY", signer.apiKey())
                .DELETE(), weight, false);
    }

    private String send(HttpRequest.Builder builder, int weight, boolean isOrder) {
        HttpRequest request = builder.timeout(Duration.ofSeconds(20)).build();
        try {
            rateLimiter.acquire(weight, isOrder);
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            observeLimitHeaders(response);

            int status = response.statusCode();
            if (status == 429 || status == 418) {
                long retryAfterMs = response.headers().firstValue("Retry-After")
                        .map(v -> Long.parseLong(v.trim()) * 1000L).orElse(60_000L);
                rateLimiter.observeBan(retryAfterMs);
                throw ExchangeException.refused("rate limited (HTTP " + status + "), backing off "
                        + (retryAfterMs / 1000) + "s", status, 0);
            }
            if (status == 503) {
                // Binance documents 503 as "unknown execution status": the request may have been
                // executed. That is the definition of ambiguous, and it must not be retried blindly.
                throw ExchangeException.ambiguous(
                        "HTTP 503 from the exchange — execution status is unknown", null);
            }
            if (status / 100 != 2) {
                int code = 0;
                String message = response.body();
                try {
                    JSONObject error = new JSONObject(response.body());
                    code = error.optInt("code", 0);
                    message = error.optString("msg", message);
                } catch (RuntimeException ignored) {
                    // Not JSON — keep the raw body, which is the only information there is.
                }
                if (code == BinanceErrorCodes.TIMESTAMP_OUT_OF_RECV_WINDOW) {
                    LOG.warning("[Binance] signature rejected on clock skew — resynchronising");
                    synchronizeClock();
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

    private void observeLimitHeaders(HttpResponse<String> response) {
        response.headers().firstValue("x-mbx-used-weight-1m")
                .or(() -> response.headers().firstValue("X-MBX-USED-WEIGHT-1M"))
                .ifPresent(value -> {
                    try {
                        rateLimiter.observeUsedWeight(Integer.parseInt(value.trim()));
                    } catch (NumberFormatException ignored) {
                        // A malformed header is not worth failing a request over; the local tally stands.
                    }
                });
    }

    /** Base URI, exposed for the boot banner. Always a testnet host. */
    public static URI baseUri() {
        return BinanceTestnetEndpoint.requireTestnet(BinanceTestnetEndpoint.REST_BASE_URL);
    }

    @Override public String toString() {
        return "BinanceFuturesTestnetAdapter[" + endpointHost().toLowerCase(Locale.ROOT) + "]";
    }
}
