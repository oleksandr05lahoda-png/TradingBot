package com.bot.signal;

import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.risk.RiskConstants;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.logging.Logger;

/**
 * Reads trade requests from an external PostgREST queue (table {@code public.bot_orders}), outside
 * this system's trust boundary — every row still goes through {@link com.bot.risk.RiskEngine}
 * exactly like a typed line. Row contract: {@code id, symbol, side, entry, sl, atr, leverage,
 * status, testnet, created_at}; one of {@code sl}/{@code atr} required; {@code testnet=false} rows
 * are never fetched and sit pending forever (venue comes from {@code REAL_TRADING}, not this
 * column). Status {@code pending -> sent -> rejected}, closes
 * {@code close_requested -> close_sent -> closed}.
 * <p>Claim before act: the PATCH carries the previous status as a predicate, so two pollers cannot
 * both win a row. Credentials from {@code SUPABASE_URL} plus {@code SUPABASE_QUEUE_KEY} (falling
 * back to {@code SUPABASE_KEY}, then {@code SUPABASE_SERVICE_KEY}); never logged.
 */
public final class SupabaseQueueSource implements SignalSource {

    private static final Logger LOG = Logger.getLogger(SupabaseQueueSource.class.getName());

    private static final String STATUS_PENDING = "pending";
    private static final String STATUS_CLAIMED = "sent";
    private static final String STATUS_REJECTED = "rejected";
    private static final String STATUS_CLOSE_REQUESTED = "close_requested";
    private static final String STATUS_CLOSE_CLAIMED = "close_sent";
    private static final String STATUS_CLOSED = "closed";

    /** Prefixes that let a signal or close id be mapped back to its row. */
    private static final String OPEN_ID_PREFIX = "sbq-";
    private static final String CLOSE_ID_PREFIX = "sbqc-";

    private final String baseUrl;
    private final String apiKey;
    private final int batchSize;
    private final int defaultLeverage;
    private final Clock clock;
    private final HttpClient http;

    public SupabaseQueueSource(String baseUrl, String apiKey, int batchSize, int defaultLeverage, Clock clock) {
        this.baseUrl = Preconditions.notBlank(baseUrl, "SUPABASE_URL").replaceAll("/+$", "");
        this.apiKey = Preconditions.notBlank(apiKey, "SUPABASE_QUEUE_KEY");
        this.batchSize = Preconditions.positive(batchSize, "batchSize");
        this.defaultLeverage = Preconditions.positive(defaultLeverage, "defaultLeverage");
        Preconditions.require(defaultLeverage <= RiskConstants.MAX_LEVERAGE,
                "defaultLeverage exceeds the hard cap " + RiskConstants.MAX_LEVERAGE);
        this.clock = Preconditions.notNull(clock, "clock");
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    /** Returns {@code null} when not configured; absence is not an error. */
    public static SupabaseQueueSource fromEnvironmentOrNull(int defaultLeverage) {
        String url = System.getenv("SUPABASE_URL");
        // bot_orders has RLS on with no policies: a publishable key can do nothing, must be a secret key.
        String key = firstPresent(System.getenv("SUPABASE_QUEUE_KEY"),
                System.getenv("SUPABASE_KEY"),
                System.getenv("SUPABASE_SERVICE_KEY"));
        if (url == null || url.isBlank() || key == null) {
            return null;
        }
        return new SupabaseQueueSource(url, key, 20, defaultLeverage, Clock.systemUTC());
    }

    private static String firstPresent(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) return candidate;
        }
        return null;
    }

    @Override public String name() { return "supabase-queue"; }

    @Override public List<Signal> poll() throws IOException, InterruptedException {
        String path = "/rest/v1/bot_orders"
                + "?status=eq." + STATUS_PENDING
                + "&testnet=is.true"
                + "&order=id.asc"
                + "&limit=" + batchSize
                + "&select=id,symbol,side,entry,sl,atr,leverage,testnet,created_at";

        HttpResponse<String> response = send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(15))
                .GET());

        if (response.statusCode() / 100 != 2) {
            throw new IOException("queue read failed with HTTP " + response.statusCode()
                    + " — refusing to treat this as 'no signals'");
        }

        JSONArray rows = new JSONArray(response.body());
        List<Signal> out = new ArrayList<>();
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.getJSONObject(i);
            long id = row.optLong("id", -1);
            try {
                if (!row.optBoolean("testnet", false)) {
                    // Belt and braces behind the query filter.
                    reject(id, "row is not marked testnet");
                    continue;
                }
                Signal signal = toSignal(row);
                if (claim(id, STATUS_PENDING, STATUS_CLAIMED)) {
                    out.add(signal);
                } else {
                    LOG.fine("[SupabaseQueue] row " + id + " was claimed by someone else");
                }
            } catch (RuntimeException e) {
                LOG.warning("[SupabaseQueue] row " + id + " is unusable: " + e.getMessage());
                reject(id, e.getMessage());
            }
        }
        return out;
    }

    /** Writes the fill back; the gap between {@code entry} and {@code filled_price} is the execution cost. */
    @Override public void onAccepted(Signal signal, ExecutionFeedback feedback)
            throws IOException, InterruptedException {
        long id = rowIdOf(signal);
        if (id < 0) return;

        JSONObject body = new JSONObject();
        body.put("client_order_id", feedback.clientOrderId());
        body.put("filled_qty", feedback.filledQuantity().doubleValue());
        body.put("filled_price", feedback.averageFillPrice().doubleValue());
        body.put("executed_at", clock.instant().toString());
        body.put("exec_note", feedback.note());

        HttpResponse<String> response = send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/rest/v1/bot_orders?id=eq." + id))
                .timeout(Duration.ofSeconds(15))
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body.toString())));

        if (response.statusCode() / 100 != 2) {
            // Not fatal: the position exists and is protected; only the measurement is lost.
            LOG.warning("[SupabaseQueue] row " + id + " executed but the fill could not be written "
                    + "back (HTTP " + response.statusCode() + ") — this trade is missing from the "
                    + "execution-cost sample");
        }
    }

    @Override public void onRejected(Signal signal, String reason) throws IOException, InterruptedException {
        long id = rowIdOf(signal);
        if (id >= 0) reject(id, reason);
    }

    /** Package-private so the row-to-signal contract can be tested without a queue or a socket. */
    Signal toSignal(JSONObject row) {
        long id = row.getLong("id");
        String symbol = row.getString("symbol").toUpperCase(Locale.ROOT);
        Side side = Side.valueOf(row.getString("side").toUpperCase(Locale.ROOT));
        double entry = row.getDouble("entry");

        OptionalDouble stop = row.has("sl") && !row.isNull("sl")
                ? OptionalDouble.of(row.getDouble("sl")) : OptionalDouble.empty();
        OptionalDouble atr = row.has("atr") && !row.isNull("atr")
                ? OptionalDouble.of(row.getDouble("atr")) : OptionalDouble.empty();
        int leverage = row.has("leverage") && !row.isNull("leverage")
                ? row.getInt("leverage") : defaultLeverage;

        // Refused, not clamped: silently lowering it would let a queue publish 20x rows unnoticed.
        Preconditions.require(leverage >= 1 && leverage <= RiskConstants.MAX_LEVERAGE,
                "row " + id + " asks for " + leverage + "x, outside [1, " + RiskConstants.MAX_LEVERAGE + "]");

        return new Signal(OPEN_ID_PREFIX + id, symbol, side, entry, stop, atr, leverage, clock.instant());
    }

    @Override public List<CloseRequest> pollCloses() throws IOException, InterruptedException {
        String path = "/rest/v1/bot_orders"
                + "?status=eq." + STATUS_CLOSE_REQUESTED
                + "&testnet=is.true"
                + "&order=id.asc&limit=" + batchSize
                + "&select=id,symbol,exec_note";

        HttpResponse<String> response = send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(15))
                .GET());
        if (response.statusCode() / 100 != 2) {
            throw new IOException("close-queue read failed with HTTP " + response.statusCode()
                    + " — refusing to treat this as 'nothing to close'");
        }

        JSONArray rows = new JSONArray(response.body());
        List<CloseRequest> out = new ArrayList<>();
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.getJSONObject(i);
            long id = row.optLong("id", -1);
            String symbol = row.optString("symbol", "");
            if (id < 0 || symbol.isBlank()) continue;
            if (claim(id, STATUS_CLOSE_REQUESTED, STATUS_CLOSE_CLAIMED)) {
                String reason = row.optString("exec_note", "");
                out.add(new CloseRequest(CLOSE_ID_PREFIX + id, symbol.toUpperCase(Locale.ROOT),
                        reason.isBlank() ? "queue" : reason, clock.instant()));
            }
        }
        return out;
    }

    @Override public void onClosed(CloseRequest request, ExecutionFeedback feedback)
            throws IOException, InterruptedException {
        long id = rowIdOf(request.id(), CLOSE_ID_PREFIX);
        if (id < 0) return;

        JSONObject body = new JSONObject();
        body.put("status", STATUS_CLOSED);
        body.put("closed_at", clock.instant().toString());
        // close_*, not filled_*: overwriting filled_price (the ENTRY fill) destroys the slippage sample.
        body.put("close_qty", feedback.filledQuantity().doubleValue());
        body.put("close_price", feedback.averageFillPrice().doubleValue());
        body.put("exec_note", feedback.note());

        HttpResponse<String> response = send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/rest/v1/bot_orders?id=eq." + id))
                .timeout(Duration.ofSeconds(15))
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body.toString())));
        if (response.statusCode() / 100 != 2) {
            LOG.warning("[SupabaseQueue] row " + id + " was closed on the exchange but the row could "
                    + "not be updated (HTTP " + response.statusCode() + ")");
        }
    }

    /** Conditional status transition, {@code from} travelling as a predicate; false = another poller won. */
    private boolean claim(long id, String from, String to) throws IOException, InterruptedException {
        HttpResponse<String> response = send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/rest/v1/bot_orders?id=eq." + id + "&status=eq." + from))
                .timeout(Duration.ofSeconds(15))
                .header("Prefer", "return=representation")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(
                        new JSONObject(Map.of("status", to)).toString())));

        if (response.statusCode() / 100 != 2) {
            throw new IOException("claim of row " + id + " (" + from + " -> " + to
                    + ") failed with HTTP " + response.statusCode());
        }
        return new JSONArray(response.body()).length() == 1;
    }

    private void reject(long id, String reason) throws IOException, InterruptedException {
        if (id < 0) return;
        JSONObject body = new JSONObject();
        body.put("status", STATUS_REJECTED);
        // Record the reason: a gate refusing everything for one reason is a defect, invisible otherwise.
        body.put("exec_note", reason == null ? "" : reason);
        HttpResponse<String> response = send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/rest/v1/bot_orders?id=eq." + id))
                .timeout(Duration.ofSeconds(15))
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body.toString())));
        if (response.statusCode() / 100 != 2) {
            LOG.warning("[SupabaseQueue] could not mark row " + id + " rejected (HTTP "
                    + response.statusCode() + "). The queue's status column must accept '"
                    + STATUS_REJECTED + "' or the row will be polled again. Reason was: " + reason);
        }
    }

    private static long rowIdOf(Signal signal) {
        return rowIdOf(signal.id(), OPEN_ID_PREFIX);
    }

    /** Maps an id this class minted back to its row, or -1 when it came from somewhere else. */
    private static long rowIdOf(String id, String prefix) {
        if (id == null || !id.startsWith(prefix)) return -1;
        try {
            return Long.parseLong(id.substring(prefix.length()));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Supabase {@code sb_*} keys are not JWTs: PostgREST fails to decode one sent as a bearer token
     * and 401s the whole request (reading as "queue unreachable"), so they go in {@code apikey}
     * alone. Legacy JWT keys want both headers.
     */
    private HttpResponse<String> send(HttpRequest.Builder builder) throws IOException, InterruptedException {
        builder.header("apikey", apiKey);
        if (!isNonJwtKey(apiKey)) builder.header("Authorization", "Bearer " + apiKey);
        HttpRequest request = builder
                .header("Content-Type", "application/json")
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /** True for the {@code sb_}-prefixed key formats, which are opaque rather than JWTs. */
    static boolean isNonJwtKey(String key) {
        return key != null && key.startsWith("sb_");
    }
}
