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
 * Reads trade requests from an external PostgREST queue (Supabase table {@code public.bot_orders}).
 *
 * <p>This source transports decisions; it does not make them. Whatever fills that table — a person,
 * a cron job, a separate research stack — is outside this system's trust boundary, which is why
 * every row still goes through {@link com.bot.risk.RiskEngine} exactly like a typed line does.
 *
 * <h2>Queue contract</h2>
 * <pre>
 *   id          bigint   — row identity; the signal id, and therefore the client order id, derives from it
 *   symbol      text
 *   side        text     — LONG | SHORT
 *   entry       double   — intended entry price
 *   sl          double   — structural stop; optional if atr is present
 *   atr         double   — optional volatility for the fallback stop
 *   leverage    int      — optional, defaults to the configured value, hard-capped at 5
 *   status      text     — pending -> sent (claimed) -> rejected, driven by this class
 *   testnet     bool     — must be true; a row that is not testnet is refused, not routed
 *   created_at  timestamptz
 * </pre>
 *
 * <h2>Two safety properties worth stating</h2>
 * <b>Claim before act.</b> A row moves {@code pending -> sent} with a conditional PATCH that carries
 * {@code status=eq.pending} as a predicate. Two processes polling the same queue cannot both win that
 * write, so a row becomes at most one order even with a duplicate deployment running.
 *
 * <p><b>A failed read throws.</b> It does not return an empty list. "The queue looked empty" and
 * "the queue was unreachable" have to be distinguishable, because only one of them means there is
 * nothing to do.
 *
 * <p>Credentials come from the environment and are never logged. Configure {@code SUPABASE_URL} and
 * {@code SUPABASE_QUEUE_KEY} (falling back to {@code SUPABASE_KEY}).
 */
public final class SupabaseQueueSource implements SignalSource {

    private static final Logger LOG = Logger.getLogger(SupabaseQueueSource.class.getName());

    private static final String STATUS_PENDING = "pending";
    private static final String STATUS_CLAIMED = "sent";
    private static final String STATUS_REJECTED = "rejected";

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

    /**
     * Builds a source from the environment, or returns {@code null} when it is not configured.
     * Absence is not an error — the smoke run uses {@link ManualTestnetInput} and never needs this.
     */
    public static SupabaseQueueSource fromEnvironmentOrNull(int defaultLeverage) {
        String url = System.getenv("SUPABASE_URL");
        String key = System.getenv("SUPABASE_QUEUE_KEY");
        if (key == null || key.isBlank()) key = System.getenv("SUPABASE_KEY");
        if (url == null || url.isBlank() || key == null || key.isBlank()) {
            return null;
        }
        return new SupabaseQueueSource(url, key, 20, defaultLeverage, Clock.systemUTC());
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
            // Fail-closed: an unreachable queue is not an empty queue.
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
                    // Belt and braces behind the query filter: a row that is not marked testnet is
                    // refused outright rather than routed anywhere.
                    reject(id, "row is not marked testnet");
                    continue;
                }
                Signal signal = toSignal(row);
                if (claim(id)) {
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

    @Override public void onRejected(Signal signal, String reason) throws IOException, InterruptedException {
        long id = rowIdOf(signal);
        if (id >= 0) reject(id, reason);
    }

    private Signal toSignal(JSONObject row) {
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

        return new Signal("sbq-" + id, symbol, side, entry, stop, atr,
                Math.min(leverage, RiskConstants.MAX_LEVERAGE), clock.instant());
    }

    /** Conditional {@code pending -> sent}. Returns false when another poller won the row. */
    private boolean claim(long id) throws IOException, InterruptedException {
        HttpResponse<String> response = send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/rest/v1/bot_orders?id=eq." + id + "&status=eq." + STATUS_PENDING))
                .timeout(Duration.ofSeconds(15))
                .header("Prefer", "return=representation")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(
                        new JSONObject(Map.of("status", STATUS_CLAIMED)).toString())));

        if (response.statusCode() / 100 != 2) {
            throw new IOException("claim of row " + id + " failed with HTTP " + response.statusCode());
        }
        return new JSONArray(response.body()).length() == 1;
    }

    private void reject(long id, String reason) throws IOException, InterruptedException {
        if (id < 0) return;
        JSONObject body = new JSONObject();
        body.put("status", STATUS_REJECTED);
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
        String id = signal.id();
        if (!id.startsWith("sbq-")) return -1;
        try {
            return Long.parseLong(id.substring(4));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private HttpResponse<String> send(HttpRequest.Builder builder) throws IOException, InterruptedException {
        HttpRequest request = builder
                .header("apikey", apiKey)
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
