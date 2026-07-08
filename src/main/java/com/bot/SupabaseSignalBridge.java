package com.bot;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * SupabaseSignalBridge — thin, additive executor bridge for the current ("our") logic.
 *
 * The DECISION lives entirely in Supabase (regime basket + vetted new-coin lottery).
 * This bridge ONLY executes: it drains the public.bot_orders queue and drives the
 * existing {@link BinanceTradeExecutor}. It never decides what to trade and never shorts.
 * The dead candle brain (DecisionEngineMerged.analyze) is simply not invoked on this path.
 *
 * Hard safety (defence in depth):
 *   - LONG-ONLY: rejects any row whose side != LONG (DB also enforces side='LONG').
 *   - TESTNET-LOCKED IN CODE: refuses to place ANY order unless BINANCE_USE_TESTNET=1.
 *     Real money is NOT reachable via env flags — arming real requires a deliberate code
 *     change + the queue-security hardening (service-role key, tight RLS) + sign-off.
 *   - DEFAULT-OFF: no-op unless SUPABASE_BRIDGE_ENABLED=1 (existing bot behaviour unchanged).
 *   - IDEMPOTENT: pending->sent is claimed by a conditional PostgREST PATCH (CAS) before
 *     execution; close_requested->close_sent likewise; a reconcile sweep adopts/fails stale
 *     'sent' orphans against the exchange truth so a crash can never leave a silent live position.
 *   - BUDGETED: refuses to open beyond BRIDGE_MAX_OPEN concurrent (aggregate exposure cap).
 *
 * NOTE: the per-row `testnet` filter is a ROUTING hint, not isolation — real isolation comes
 * from the code-level testnet lock above and (before real money) a service-role key + tight RLS.
 */
public final class SupabaseSignalBridge {
    private static final Logger LOG = Logger.getLogger(SupabaseSignalBridge.class.getName());

    private static final String  SUPABASE_URL = System.getenv().getOrDefault("SUPABASE_URL", "").trim();
    // Prefer a dedicated bridge key (service-role, set before real money); fall back to the bot's anon key on testnet.
    private static final String  SUPABASE_KEY = firstNonBlank(
            System.getenv().get("BRIDGE_SUPABASE_KEY"), System.getenv().get("SUPABASE_KEY"));
    private static final boolean ENABLED      = "1".equals(System.getenv().getOrDefault("SUPABASE_BRIDGE_ENABLED", "0"));
    private static final boolean USE_TESTNET  = "1".equals(System.getenv().getOrDefault("BINANCE_USE_TESTNET", "0"));
    private static final boolean ALLOW_REAL   = "1".equals(System.getenv().getOrDefault("BRIDGE_ALLOW_REAL", "0"));
    private static final double  BAL_PER_LEG  = envDouble("BRIDGE_BALANCE_PER_LEG", 0.0);
    private static final int     MAX_OPEN     = (int) envDouble("BRIDGE_MAX_OPEN", 20);
    private static final int     MAX_PENDING_AGE_MIN = (int) envDouble("BRIDGE_MAX_PENDING_AGE_MIN", 15);

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final BinanceTradeExecutor executor = BinanceTradeExecutor.getInstance();
    private volatile boolean bannerLogged = false;

    private static SupabaseSignalBridge instance;
    public static synchronized SupabaseSignalBridge getInstance() {
        if (instance == null) instance = new SupabaseSignalBridge();
        return instance;
    }
    private SupabaseSignalBridge() {}

    /** Enabled only when explicitly switched on AND Supabase is configured. */
    public static boolean isEnabled() {
        return ENABLED && !SUPABASE_URL.isEmpty() && !SUPABASE_KEY.isEmpty();
    }

    /** One poll cycle: reconcile orphans, place pending opens, process requested closes. */
    public void poll() {
        if (!isEnabled()) return;
        if (!executionAllowed()) return;
        if (!bannerLogged) {
            bannerLogged = true;
            LOG.warning("[Bridge] ARMED endpoint=" + (USE_TESTNET ? "TESTNET(demo)" : "*** REAL ***")
                    + " allowReal=" + ALLOW_REAL + " balPerLeg=$" + BAL_PER_LEG
                    + " maxOpen=" + MAX_OPEN + " maxPendingAgeMin=" + MAX_PENDING_AGE_MIN
                    + " key=" + (SUPABASE_KEY.isEmpty() ? "none" : "set"));
        }
        try { reconcileSent(); } catch (Exception e) { LOG.warning("[Bridge] reconcile: " + e.getMessage()); }
        try { drainOpens();   } catch (Exception e) { LOG.warning("[Bridge] drainOpens: " + e.getMessage()); }
        try { drainCloses();  } catch (Exception e) { LOG.warning("[Bridge] drainCloses: " + e.getMessage()); }
    }

    /**
     * Endpoint gate. Testnet is always allowed. The REAL endpoint requires an explicit, deliberate
     * opt-in: BRIDGE_ALLOW_REAL=1 (AND, not OR). Unset/null env => testnet=false + allowReal=false =>
     * refused. Real money therefore needs THREE deliberate flags together:
     *   BINANCE_USE_TESTNET=0  +  BRIDGE_ALLOW_REAL=1  +  SUPABASE_BRIDGE_ENABLED=1.
     */
    private boolean executionAllowed() {
        if (USE_TESTNET) return true;
        if (ALLOW_REAL)  return true;
        LOG.severe("[Bridge] BLOCKED: real endpoint (BINANCE_USE_TESTNET!=1) without BRIDGE_ALLOW_REAL=1 — refusing every order.");
        return false;
    }

    /**
     * Reconcile 'sent' orphans (claimed but no terminal patch — e.g. crash between claim and result).
     * Resolve against the EXCHANGE truth, never guess: live position -> adopt 'open', flat -> 'failed',
     * read failure (NaN) -> leave for the next sweep.
     */
    private void reconcileSent() throws Exception {
        String cutoff = Instant.now().minusSeconds(120).toString();
        JSONArray stuck = sbGet("/rest/v1/bot_orders?status=eq.sent&testnet=eq." + USE_TESTNET
                + "&sent_at=lt." + enc(cutoff) + "&order=sent_at.asc&limit=20");
        for (int i = 0; i < stuck.length(); i++) {
            JSONObject o = stuck.getJSONObject(i);
            long id = o.getLong("id");
            String symbol = o.optString("symbol", "");
            double amt = executor.fetchPositionAmountChecked(symbol);  // NaN = read failed
            if (Double.isNaN(amt)) continue;                           // do NOT guess; retry next sweep
            if (Math.abs(amt) > 1e-9) patch(id, "open",   "reconciled: adopted live position qty=" + amt);
            else                      patch(id, "failed", "reconciled: no position on exchange");
        }
    }

    private void drainOpens() throws Exception {
        int active = countActive();
        JSONArray pend = sbGet("/rest/v1/bot_orders?status=eq.pending&testnet=eq." + USE_TESTNET
                + "&order=created_at.asc&limit=20");
        for (int i = 0; i < pend.length(); i++) {
            JSONObject o = pend.getJSONObject(i);
            long id = o.getLong("id");
            String side = o.optString("side", "");
            if (!"LONG".equalsIgnoreCase(side)) {                 // LONG-ONLY guard at the wire
                patch(id, "failed", "rejected: side=" + side + " (long-only bridge)");
                continue;
            }
            if (isStale(o.optString("created_at", ""))) {         // don't open at a stale enqueue price
                patch(id, "failed", "stale pending (>" + MAX_PENDING_AGE_MIN + "min)");
                continue;
            }
            if (active >= MAX_OPEN) {
                LOG.warning("[Bridge] aggregate cap reached (BRIDGE_MAX_OPEN=" + MAX_OPEN + ") — deferring remaining pendings");
                break;
            }
            if (!claimPending(id)) continue;                     // someone/something already took it
            active++;
            try {
                double reqMult = o.optDouble("size_mult", 1.0);
                DecisionEngineMerged.TradeIdea idea = buildLongIdea(o);
                if (BAL_PER_LEG <= 0) { patch(id, "failed", "BRIDGE_BALANCE_PER_LEG not set"); continue; }
                BinanceTradeExecutor.ExecutionResult r = executor.openPositionWithSl(idea, BAL_PER_LEG);
                if (r != null && r.success) {
                    patch(id, "open", String.format("opened notional=$%.2f qty=%.6f tps=%d mult req=%.2f applied=%.2f",
                            r.notionalUsd, r.qty, r.tpsPlaced, reqMult, idea.getExecutorSizeMultiplier()));
                } else {
                    String reason = (r == null ? "null" : r.reason);
                    if (reason != null && reason.toUpperCase().contains("NAKED")) {
                        // SL failed AND emergency close failed => a LIVE unhedged position. Never bury this in 'failed'.
                        patch(id, "failed_naked", "*** NAKED — verify exchange MANUALLY: " + reason);
                        LOG.severe("[Bridge] *** NAKED POSITION " + o.optString("symbol", "") + " id=" + id
                                + " — manual intervention required: " + reason);
                    } else {
                        patch(id, "failed", "exec: " + reason);
                    }
                }
            } catch (Throwable t) {
                patch(id, "failed", "ex: " + t.getMessage());
            }
        }
    }

    private void drainCloses() throws Exception {
        JSONArray cl = sbGet("/rest/v1/bot_orders?status=eq.close_requested&testnet=eq." + USE_TESTNET
                + "&order=created_at.asc&limit=20");
        for (int i = 0; i < cl.length(); i++) {
            JSONObject o = cl.getJSONObject(i);
            long id = o.getLong("id");
            String symbol = o.optString("symbol", "");
            if (!symbol.matches("^[A-Z0-9]{2,20}USDT$")) { patch(id, "failed", "bad symbol: " + symbol); continue; }
            if (!claimClose(id)) continue;                        // CAS close_requested -> close_sent
            boolean closed = executor.closePosition(symbol, "supabase-bridge");  // reduce-only, fail-closed
            patch(id, closed ? "closed" : "failed", closed ? "closed reduce-only" : "close failed (verify exchange)");
        }
    }

    private int countActive() throws Exception {
        JSONArray a = sbGet("/rest/v1/bot_orders?status=in.(sent,open)&testnet=eq." + USE_TESTNET + "&select=id&limit=200");
        return a.length();
    }

    /** Build a LONG TradeIdea carrying EXACT Supabase TP prices (reflection override + read-back verify). */
    private DecisionEngineMerged.TradeIdea buildLongIdea(JSONObject o) throws Exception {
        String symbol = o.getString("symbol");
        double entry  = o.getDouble("entry");
        double sl     = o.getDouble("sl");
        double tp1    = o.optDouble("tp1", 0);
        double tp2    = o.optDouble("tp2", 0);
        double mult   = o.optDouble("size_mult", 1.0);

        java.util.List<String> flags = new java.util.ArrayList<>();
        flags.add("SUPABASE");
        flags.add(o.optString("kind", "basket").toUpperCase());

        // Public 10-arg ctor (symbol, side, price, stop, take, probability, flags, fundingRate, oiChange, htfBias).
        DecisionEngineMerged.TradeIdea idea = new DecisionEngineMerged.TradeIdea(
                symbol, com.bot.TradingCore.Side.LONG,
                entry, sl, tp2 > 0 ? tp2 : entry,
                0.95, flags, 0.0, 0.0, "SUPABASE");

        // ctor derives tp1/tp2/tp3 from stop distance — override to the exact Supabase levels.
        if (tp1 > 0) setFinalDouble(idea, "tp1", tp1);
        if (tp2 > 0) { setFinalDouble(idea, "tp2", tp2); setFinalDouble(idea, "tp3", tp2); }
        idea.setExecutorSizeMultiplier(mult);

        // VERIFY the override actually took (tp1/tp2 are public final — read them back; fail loud if not).
        if (tp1 > 0 && Math.abs(idea.tp1 - tp1) > Math.abs(tp1) * 1e-6)
            throw new IllegalStateException("tp1 override failed: got " + idea.tp1 + " want " + tp1);
        if (tp2 > 0 && Math.abs(idea.tp2 - tp2) > Math.abs(tp2) * 1e-6)
            throw new IllegalStateException("tp2 override failed: got " + idea.tp2 + " want " + tp2);
        return idea;
    }

    private static void setFinalDouble(Object obj, String name, double value) throws Exception {
        java.lang.reflect.Field f = obj.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.setDouble(obj, value);
    }

    // ── Supabase PostgREST helpers ────────────────────────────────────────────────────────

    private JSONArray sbGet(String pathQuery) throws Exception {
        HttpResponse<String> resp = http.send(HttpRequest.newBuilder()
                .uri(URI.create(SUPABASE_URL + pathQuery))
                .header("apikey", SUPABASE_KEY)
                .header("Authorization", "Bearer " + SUPABASE_KEY)
                .timeout(Duration.ofSeconds(15)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            LOG.warning("[Bridge] GET " + pathQuery + " HTTP " + resp.statusCode());
            return new JSONArray();
        }
        return new JSONArray(resp.body());
    }

    /**
     * Conditional claim pending->sent. Ownership is decided from the RETURNED ROW, not the 2xx status —
     * this REQUIRES the Prefer: return=representation header (set in sbPatch). A future change to 204
     * No Content here would silently break the CAS, so the check is exact (one row, our id, status sent).
     */
    private boolean claimPending(long id) throws Exception {
        JSONObject body = new JSONObject().put("status", "sent").put("sent_at", Instant.now().toString());
        HttpResponse<String> resp = sbPatch("/rest/v1/bot_orders?id=eq." + id + "&status=eq.pending", body.toString());
        if (resp.statusCode() / 100 != 2) return false;
        try {
            JSONArray a = new JSONArray(resp.body());
            return a.length() == 1 && a.getJSONObject(0).optLong("id") == id
                    && "sent".equals(a.getJSONObject(0).optString("status"));
        } catch (Exception e) { return false; }
    }

    /** Conditional claim close_requested->close_sent (same CAS discipline as claimPending). */
    private boolean claimClose(long id) throws Exception {
        JSONObject body = new JSONObject().put("status", "close_sent");
        HttpResponse<String> resp = sbPatch("/rest/v1/bot_orders?id=eq." + id + "&status=eq.close_requested", body.toString());
        if (resp.statusCode() / 100 != 2) return false;
        try {
            JSONArray a = new JSONArray(resp.body());
            return a.length() == 1 && a.getJSONObject(0).optLong("id") == id
                    && "close_sent".equals(a.getJSONObject(0).optString("status"));
        } catch (Exception e) { return false; }
    }

    private void patch(long id, String status, String note) throws Exception {
        JSONObject body = new JSONObject().put("status", status).put("exec_note", trunc(note, 300));
        if ("closed".equals(status)) body.put("closed_at", Instant.now().toString());
        sbPatch("/rest/v1/bot_orders?id=eq." + id, body.toString());
    }

    private HttpResponse<String> sbPatch(String pathQuery, String json) throws Exception {
        return http.send(HttpRequest.newBuilder()
                .uri(URI.create(SUPABASE_URL + pathQuery))
                .header("apikey", SUPABASE_KEY)
                .header("Authorization", "Bearer " + SUPABASE_KEY)
                .header("Content-Type", "application/json")
                .header("Prefer", "return=representation")   // REQUIRED for the claim CAS above
                .timeout(Duration.ofSeconds(15))
                .method("PATCH", HttpRequest.BodyPublishers.ofString(json)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** A pending row older than MAX_PENDING_AGE_MIN must not open — its entry/sl/tp are stale. */
    private static boolean isStale(String createdAtIso) {
        if (createdAtIso == null || createdAtIso.isEmpty()) return false;
        try {
            return java.time.OffsetDateTime.parse(createdAtIso).toInstant()
                    .isBefore(Instant.now().minusSeconds((long) MAX_PENDING_AGE_MIN * 60));
        } catch (Exception e) { return false; }   // unparseable -> do not expire
    }

    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }
    private static String trunc(String s, int n) { return s == null ? "" : (s.length() <= n ? s : s.substring(0, n)); }
    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) return a.trim();
        if (b != null && !b.isBlank()) return b.trim();
        return "";
    }
    private static double envDouble(String k, double d) {
        try { String v = System.getenv(k); return (v == null || v.isBlank()) ? d : Double.parseDouble(v.trim()); }
        catch (Exception e) { return d; }
    }
}
