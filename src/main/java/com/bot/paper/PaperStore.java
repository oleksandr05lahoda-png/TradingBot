package com.bot.paper;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Supabase access for the harness. Deliberately self-contained: no static env reads, no reference
 * to BotMain or any class whose initialiser demands a Telegram token or Binance keys, so a backtest
 * runs from an IDE with an empty environment. Credentials arrive as constructor arguments; who
 * found them is the caller's problem.
 *
 * Reads are plain PostgREST GETs. Writes are two separate operations by design — see
 * {@link #insertPrediction} and {@link #updateOutcome}; the database now refuses to accept them as
 * one, which is what makes "the prediction preceded the fact" a fact rather than a claim.
 */
public final class PaperStore {

    private final String baseUrl;
    private final String apiKey;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15)).build();

    /** Page size for kline reads. PostgREST caps rows per request; the readers paginate. */
    private static final int PAGE = 1000;

    public PaperStore(String baseUrl, String apiKey) {
        if (baseUrl == null || baseUrl.isBlank()) throw new IllegalArgumentException("Supabase URL required");
        if (apiKey == null || apiKey.isBlank())   throw new IllegalArgumentException("Supabase key required");
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.apiKey  = apiKey;
    }

    // ─── reads ────────────────────────────────────────────────────────

    /**
     * Bars for one symbol in [fromMs, toMs), ascending.
     *
     * @param table klines_4h or klines_1h
     */
    public List<Bar> loadBars(String table, String symbol, long intervalMs, long fromMs, long toMs)
            throws Exception {
        List<Bar> out = new ArrayList<>();
        long cursor = fromMs;
        while (true) {
            String q = "/rest/v1/" + table
                    + "?symbol=eq." + enc(symbol)
                    + "&ts_ms=gte." + cursor
                    + "&ts_ms=lt." + toMs
                    + "&order=ts_ms.asc&limit=" + PAGE;
            JSONArray a = get(q);
            if (a.isEmpty()) break;
            for (int i = 0; i < a.length(); i++) {
                JSONObject o = a.getJSONObject(i);
                out.add(new Bar(symbol, o.getLong("ts_ms"), intervalMs,
                        o.getDouble("open"), o.getDouble("high"),
                        o.getDouble("low"), o.getDouble("close"),
                        o.optDouble("volume", 0)));
            }
            if (a.length() < PAGE) break;
            cursor = out.get(out.size() - 1).openMs + 1;
        }
        return out;
    }

    /** Funding settlements for one symbol in [fromMs, toMs], ascending. */
    public List<PaperExecutor.FundingPoint> loadFunding(String symbol, long fromMs, long toMs)
            throws Exception {
        List<PaperExecutor.FundingPoint> out = new ArrayList<>();
        long cursor = fromMs;
        while (true) {
            String q = "/rest/v1/funding_history"
                    + "?symbol=eq." + enc(symbol)
                    + "&funding_time_ms=gte." + cursor
                    + "&funding_time_ms=lte." + toMs
                    + "&order=funding_time_ms.asc&limit=" + PAGE;
            JSONArray a = get(q);
            if (a.isEmpty()) break;
            for (int i = 0; i < a.length(); i++) {
                JSONObject o = a.getJSONObject(i);
                out.add(new PaperExecutor.FundingPoint(
                        o.getLong("funding_time_ms"), o.getDouble("funding_rate")));
            }
            if (a.length() < PAGE) break;
            cursor = out.get(out.size() - 1).timeMs + 1;
        }
        return out;
    }

    // ─── writes: two operations, never one ────────────────────────────

    /**
     * Write the prediction, with NO outcome. Returns the new row id.
     *
     * The outcome columns are not merely omitted here — the database rejects an INSERT that carries
     * any of them (trigger paper_signals_no_outcome_on_insert). This method could not write a
     * result even if a later edit tried to.
     */
    public long insertPrediction(String hypothesisName, String hypothesisVersion, String mode,
                                 String universeId, Signal s, long entryBarOpenMs, double entryPx,
                                 boolean isForward, String prevHash) throws Exception {
        JSONObject body = new JSONObject()
                .put("kind", "directional")
                .put("hypothesis_name", hypothesisName)
                .put("hypothesis_version", hypothesisVersion)
                .put("mode", mode)
                .put("universe_id", universeId)
                .put("symbol", s.symbol)
                .put("side", s.side.name())
                .put("signal_bar_close_ms", s.signalBarCloseMs)
                .put("entry_bar_open_ms", entryBarOpenMs)
                .put("entry_px", entryPx)
                .put("stop_px", s.stopPrice)
                .put("is_forward", isForward);
        if (s.targetPrice > 0) body.put("target_px", s.targetPrice);
        if (prevHash != null)  body.put("prev_hash", prevHash);
        body.put("row_hash", chainHash(prevHash, body));

        HttpResponse<String> r = send("POST", "/rest/v1/paper_signals", body.toString(), true);
        if (r.statusCode() / 100 != 2) {
            throw new IllegalStateException("insertPrediction HTTP " + r.statusCode() + ": " + r.body());
        }
        JSONArray a = new JSONArray(r.body());
        if (a.isEmpty()) throw new IllegalStateException("insertPrediction returned no row");
        return a.getJSONObject(0).getLong("id");
    }

    /**
     * Write a delta-neutral carry prediction, with NO outcome.
     *
     * side is SHORT because the perp leg defines the position; stop_px is left NULL, which the
     * schema now permits for kind='carry' — a carry has no stop, and inventing one to satisfy a
     * constraint would journal a protective level that never existed. entry_px carries the perp
     * fill so the directional columns stay meaningful, and both legs go in their own columns:
     * paper_signals_carry_legs_ck rejects the row otherwise.
     */
    public long insertCarryPrediction(String hypothesisName, String hypothesisVersion, String mode,
                                      String universeId, CarryPosition pos,
                                      CarryPaperExecutor.Fill f, boolean isForward, String prevHash)
            throws Exception {
        JSONObject body = new JSONObject()
                .put("kind", "carry")
                .put("hypothesis_name", hypothesisName)
                .put("hypothesis_version", hypothesisVersion)
                .put("mode", mode)
                .put("universe_id", universeId)
                .put("symbol", pos.symbol)
                .put("side", "SHORT")
                .put("signal_bar_close_ms", pos.signalBarCloseMs)
                .put("entry_bar_open_ms", f.entryBarOpenMs)
                .put("entry_px", f.perpEntryPx)
                .put("perp_entry_px", f.perpEntryPx)
                .put("spot_entry_px", f.spotEntryPx)
                .put("basis_entry_bp", f.basisEntryBp)
                .put("is_forward", isForward);
        if (prevHash != null) body.put("prev_hash", prevHash);
        body.put("row_hash", chainHash(prevHash, body));

        HttpResponse<String> r = send("POST", "/rest/v1/paper_signals", body.toString(), true);
        if (r.statusCode() / 100 != 2) {
            throw new IllegalStateException("insertCarryPrediction HTTP " + r.statusCode() + ": " + r.body());
        }
        JSONArray a = new JSONArray(r.body());
        if (a.isEmpty()) throw new IllegalStateException("insertCarryPrediction returned no row");
        return a.getJSONObject(0).getLong("id");
    }

    /**
     * Write a carry outcome. Separate operation, always.
     *
     * Both exit legs are required by paper_signals_carry_exit_legs_ck: without them the two-legged
     * P&amp;L could not be reconstructed from the row, and ret_net would have to be taken on trust.
     * ret_gross, fees and funding go with it for the same reason — the atomic check now demands
     * every term, so ret_net can always be re-derived rather than believed.
     */
    public void updateCarryOutcome(long id, CarryPaperExecutor.Fill f) throws Exception {
        JSONObject body = new JSONObject()
                .put("exit_bar_ms", f.exitBarMs)
                .put("exit_px", f.perpExitPx)
                .put("perp_exit_px", f.perpExitPx)
                .put("spot_exit_px", f.spotExitPx)
                .put("basis_exit_bp", f.basisExitBp)
                .put("exit_reason", f.exitReason.name())
                .put("ret_gross", f.retGross)
                .put("fees", f.fees)
                .put("funding", f.funding)
                .put("ret_net", f.retNet)
                .put("resolved_at", java.time.Instant.now().toString());
        HttpResponse<String> r = send("PATCH", "/rest/v1/paper_signals?id=eq." + id
                + "&resolved_at=is.null", body.toString(), false);
        if (r.statusCode() / 100 != 2) {
            throw new IllegalStateException("updateCarryOutcome HTTP " + r.statusCode() + ": " + r.body());
        }
    }

    /** Write the outcome onto an existing prediction. Separate operation, always. */
    public void updateOutcome(long id, PaperExecutor.Fill f) throws Exception {
        JSONObject body = new JSONObject()
                .put("exit_bar_ms", f.exitBarMs)
                .put("exit_px", f.exitPx)
                .put("exit_reason", f.exitReason.name())
                .put("ret_gross", f.retGross)
                .put("fees", f.fees)
                .put("funding", f.funding)
                .put("ret_net", f.retNet)
                .put("resolved_at", java.time.Instant.now().toString());
        HttpResponse<String> r = send("PATCH", "/rest/v1/paper_signals?id=eq." + id
                + "&resolved_at=is.null", body.toString(), false);
        if (r.statusCode() / 100 != 2) {
            throw new IllegalStateException("updateOutcome HTTP " + r.statusCode() + ": " + r.body());
        }
    }

    /** The most recent row_hash for this sleeve+version+mode, or null when the chain is empty. */
    public String lastHash(String name, String version, String mode) throws Exception {
        JSONArray a = get("/rest/v1/paper_signals?hypothesis_name=eq." + enc(name)
                + "&hypothesis_version=eq." + enc(version)
                + "&mode=eq." + enc(mode)
                + "&select=row_hash&order=id.desc&limit=1");
        return a.isEmpty() ? null : a.getJSONObject(0).optString("row_hash", null);
    }

    // ─── plumbing ─────────────────────────────────────────────────────

    /**
     * SHA-256 over prev_hash plus this row's fields — a tamper-EVIDENT chain, not a tamper-proof
     * one. Anyone who can write rows can also recompute it. Making it tamper-proof needs a key the
     * writer does not hold, which is project_state id=17 and is not claimed here.
     */
    static String chainHash(String prevHash, JSONObject row) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update((prevHash == null ? "" : prevHash).getBytes(StandardCharsets.UTF_8));
            for (String k : new java.util.TreeSet<>(row.keySet())) {
                md.update((k + "=" + row.get(k) + ";").getBytes(StandardCharsets.UTF_8));
            }
            StringBuilder sb = new StringBuilder();
            for (byte b : md.digest()) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("hash failed: " + e.getMessage(), e);
        }
    }

    private JSONArray get(String pathQuery) throws Exception {
        HttpResponse<String> r = send("GET", pathQuery, null, false);
        if (r.statusCode() / 100 != 2) {
            throw new IllegalStateException("GET " + pathQuery + " HTTP " + r.statusCode() + ": " + r.body());
        }
        return new JSONArray(r.body());
    }

    private HttpResponse<String> send(String method, String pathQuery, String body, boolean wantRow)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + pathQuery))
                .timeout(Duration.ofSeconds(60))
                .header("apikey", apiKey)
                .header("Authorization", "Bearer " + apiKey);
        if (body != null) {
            b.header("Content-Type", "application/json");
            b.header("Prefer", wantRow ? "return=representation" : "return=minimal");
        }
        b.method(method, body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body));
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }
}
