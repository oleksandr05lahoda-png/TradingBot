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
 * Every claim below was re-verified against the code and the live schema on 2026-07-28
 * (project_state id=4); the DB-side checks cited are real constraints on public.bot_orders.
 *
 * Hard safety (defence in depth):
 *   - LONG-ONLY: rejects any row whose side != LONG (DB also enforces it —
 *     constraint bot_orders_side_long CHECK (side = 'LONG')).
 *   - TESTNET-LOCKED IN CODE: refuses to OPEN any position unless the executor reports the
 *     testnet/demo endpoint AND BINANCE_USE_TESTNET was set explicitly (absence is a refusal,
 *     never a default). CLOSES are deliberately NOT gated — see poll() — because a lock that
 *     seals positions in is not a safety feature. Real money is NOT reachable via env flags for
 *     opening new exposure: BRIDGE_ALLOW_REAL is still
 *     read but grants nothing, and the endpoint is taken from BinanceTradeExecutor.isTestnet()
 *     rather than re-read here, so the two can never disagree. Unlocking is a deliberate code
 *     change gated on {@link #REAL_UNLOCK_REQUIREMENTS}, plus queue hardening (service-role key,
 *     tight RLS) + sign-off.
 *   - DEFAULT-OFF: no-op unless SUPABASE_BRIDGE_ENABLED=1 (existing bot behaviour unchanged).
 *   - IDEMPOTENT: pending->sent is claimed by a conditional PostgREST PATCH (CAS) before
 *     execution; close_requested->close_sent likewise. TWO reconcile sweeps resolve rows against
 *     the exchange truth rather than guessing: reconcileSent() adopts/fails stale 'sent' orphans
 *     so a crash can never leave a silent live position, and reconcileOpen() closes 'open' rows
 *     the exchange reports flat so a stop/take-profit hit outside the bridge cannot permanently
 *     consume a slot. Both skip on a failed read and both PATCH conditionally on the expected
 *     current status, so neither can clobber a concurrent transition.
 *   - BUDGETED: defers any pending beyond BRIDGE_MAX_OPEN concurrent (aggregate exposure cap);
 *     deferred rows stay 'pending' and are retried, or expire via MAX_PENDING_AGE_MIN.
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
    /**
     * Raw BINANCE_USE_TESTNET, deliberately left UNPARSED so that "absent" stays distinguishable
     * from "explicitly 0". A missing variable must not select an endpoint by default — that is
     * exactly how an unset Railway variable became live orders (project_state id=2).
     */
    private static final String  TESTNET_ENV  = System.getenv("BINANCE_USE_TESTNET");
    private static final boolean TESTNET_SET  = TESTNET_ENV != null && !TESTNET_ENV.isBlank();
    /** Read, but no longer grants anything — see {@link #REAL_UNLOCK_REQUIREMENTS}. Kept so the log can say it was ignored. */
    private static final boolean ALLOW_REAL   = "1".equals(System.getenv().getOrDefault("BRIDGE_ALLOW_REAL", "0"));
    private static final double  BAL_PER_LEG  = envDouble("BRIDGE_BALANCE_PER_LEG", 0.0);
    private static final int     MAX_OPEN     = (int) envDouble("BRIDGE_MAX_OPEN", 20);
    private static final int     MAX_PENDING_AGE_MIN = (int) envDouble("BRIDGE_MAX_PENDING_AGE_MIN", 15);

    /**
     * GREPPABLE UNLOCK CONTRACT — the real endpoint is locked in code, not behind an env flag.
     * All of the following must hold, and this lock be lifted in the same deliberate commit,
     * before real capital is reachable again:
     *   1. paper-harness merged — Hypothesis / MarketSnapshot / PaperExecutor, entry at the OPEN of
     *      the first bar strictly after the signal bar, 0.10% round-trip taker + funding in PnL;
     *   2. isSleeveApproved() enforced fail-closed in this class against strategy_trials.passed_v2;
     *   3. at least one sleeve with strategy_trials.passed_v2 = true AND lab_forward_status.forward_ok = true
     *      (as of 2026-07-28: 63 trials with 0 passed, 23 forward rows with 0 ok — zero sleeves qualify);
     *   4. operator sign-off recorded in public.project_state.
     */
    static final String REAL_UNLOCK_REQUIREMENTS =
            "real endpoint is locked in code — see SupabaseSignalBridge.REAL_UNLOCK_REQUIREMENTS "
          + "(paper-harness + isSleeveApproved + a sleeve with passed_v2 & forward_ok + operator sign-off)";

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final BinanceTradeExecutor executor = BinanceTradeExecutor.getInstance();
    /**
     * SINGLE SOURCE OF TRUTH for which endpoint we are on: taken from the executor that actually
     * builds the URLs, never re-read from env here. Two independent reads of BINANCE_USE_TESTNET
     * are precisely how this bridge could log "TESTNET(demo)" while the executor traded fapi.binance.com.
     */
    private final boolean useTestnet = executor.isTestnet();
    private volatile boolean bannerLogged = false;
    /** Last gate refusal already logged — poll() runs on a timer, so log each distinct reason once. */
    private volatile String lastGateLog = null;

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

    /**
     * One poll cycle, deliberately ASYMMETRIC about the endpoint lock.
     *
     * Everything that can only SHRINK exposure runs unconditionally — reconciling 'sent' orphans
     * and 'open' ghosts (both DB-only, they place no orders) and draining close_requested. Only
     * drainOpens(), the single path that can create exposure, sits behind executionAllowed().
     *
     * The reason is that a lock which seals positions IN is not a safety feature: while the gate
     * covered the whole cycle, a live position could not be closed by the bridge at all and the
     * live_time_closer cron's close_requested rows piled up unprocessed, so time-stops silently
     * stopped working (project_state id=11).
     *
     * reconcileOpen() still runs before drainOpens(), so capacity freed by a position that died
     * on the exchange is usable in the same cycle.
     */
    public void poll() {
        if (!isEnabled()) return;
        boolean opensAllowed = executionAllowed();
        if (!bannerLogged) {
            bannerLogged = true;
            LOG.warning("[Bridge] polling endpoint=" + (useTestnet ? "TESTNET(demo)" : "*** REAL ***")
                    + " opens=" + (opensAllowed ? "ALLOWED" : "LOCKED") + " closes=ALWAYS"
                    + " allowReal=" + ALLOW_REAL + " balPerLeg=$" + BAL_PER_LEG
                    + " maxOpen=" + MAX_OPEN + " maxPendingAgeMin=" + MAX_PENDING_AGE_MIN
                    + " key=" + (SUPABASE_KEY.isEmpty() ? "none" : "set"));
        }

        // --- exposure-reducing, never gated ---
        try { reconcileSent(); } catch (Exception e) { LOG.warning("[Bridge] reconcile: " + e.getMessage()); }
        try { reconcileOpen(); } catch (Exception e) { LOG.warning("[Bridge] reconcileOpen: " + e.getMessage()); }
        try { drainCloses();  } catch (Exception e) { LOG.warning("[Bridge] drainCloses: " + e.getMessage()); }

        // --- the ONLY exposure-increasing path ---
        if (!opensAllowed) return;
        try { drainOpens();   } catch (Exception e) { LOG.warning("[Bridge] drainOpens: " + e.getMessage()); }
    }

    /**
     * Endpoint gate — FAIL-CLOSED, and no longer an env decision. Three checks, in order:
     *   1. BINANCE_USE_TESTNET absent          -> refuse (absence must never pick an endpoint silently);
     *   2. executor is on the REAL endpoint    -> refuse UNCONDITIONALLY (BRIDGE_ALLOW_REAL cannot override);
     *   3. explicit testnet/demo               -> allow.
     *
     * There is no combination of environment variables that reaches real capital from here; lifting
     * the lock is a code change gated on {@link #REAL_UNLOCK_REQUIREMENTS}.
     */
    private boolean executionAllowed() {
        if (ALLOW_REAL) {
            gateLog("[Bridge] BRIDGE_ALLOW_REAL=1 IGNORED: " + REAL_UNLOCK_REQUIREMENTS);
        }
        if (!TESTNET_SET) {
            gateLog("[Bridge] BLOCKED: BINANCE_USE_TESTNET is not set — refusing to trade rather than "
                    + "silently choosing an endpoint.");
            return false;
        }
        if (!useTestnet) {
            gateLog("[Bridge] BLOCKED: executor is on the REAL endpoint (BINANCE_USE_TESTNET="
                    + TESTNET_ENV.trim() + ") — " + REAL_UNLOCK_REQUIREMENTS);
            return false;
        }
        return true;
    }

    /** poll() runs on a timer; log each distinct gate reason once so severe keeps its meaning. */
    private void gateLog(String msg) {
        if (msg.equals(lastGateLog)) return;
        lastGateLog = msg;
        LOG.severe(msg);
    }

    /**
     * Reconcile 'sent' orphans (claimed but no terminal patch — e.g. crash between claim and result).
     * Resolve against the EXCHANGE truth, never guess: live position -> adopt 'open', flat -> 'failed',
     * read failure (NaN) -> leave for the next sweep.
     */
    private void reconcileSent() throws Exception {
        String cutoff = Instant.now().minusSeconds(120).toString();
        JSONArray stuck = sbGet("/rest/v1/bot_orders?status=eq.sent&testnet=eq." + useTestnet
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

    /**
     * Reconcile 'open' rows against the exchange. A position closed ON THE EXCHANGE — stop, take-profit,
     * manual intervention, liquidation — leaves its row 'open' forever, because nothing else transitions
     * it: drainCloses() only ever looks at close_requested. Those ghosts inflate countActive() until it
     * reaches BRIDGE_MAX_OPEN and the bridge silently stops opening anything (project_state id=3;
     * bot_orders id=1 sat 'open' for 20 days after the operator closed it by hand).
     *
     * Conservative by construction: only a SUCCESSFUL read showing flat closes the row; a failed read
     * (NaN) is left for the next sweep. The PATCH is conditional on the row still being 'open', so it
     * cannot clobber a close_requested raced in by the live_time_closer cron between GET and PATCH.
     */
    private void reconcileOpen() throws Exception {
        String cutoff = Instant.now().minusSeconds(120).toString();
        JSONArray live = sbGet("/rest/v1/bot_orders?status=eq.open&testnet=eq." + useTestnet
                + "&sent_at=lt." + enc(cutoff) + "&order=sent_at.asc&limit=20");
        for (int i = 0; i < live.length(); i++) {
            JSONObject o = live.getJSONObject(i);
            long id = o.getLong("id");
            String symbol = o.optString("symbol", "");
            if (symbol.isEmpty()) continue;
            double amt = executor.fetchPositionAmountChecked(symbol);  // NaN = read failed
            if (Double.isNaN(amt)) continue;                           // do NOT guess; retry next sweep
            if (Math.abs(amt) > 1e-9) continue;                        // still live on the exchange
            JSONObject body = new JSONObject()
                    .put("status", "closed")
                    .put("closed_at", Instant.now().toString())
                    .put("exec_note", trunc("reconciled: flat on exchange (closed outside the bridge)", 300));
            sbPatch("/rest/v1/bot_orders?id=eq." + id + "&status=eq.open", body.toString());
            LOG.info("[Bridge] reconciled stale open id=" + id + " " + symbol + " -> closed (flat on exchange)");
        }
    }

    private void drainOpens() throws Exception {
        int active = countActive();
        JSONArray pend = sbGet("/rest/v1/bot_orders?status=eq.pending&testnet=eq." + useTestnet
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
                // Sizing base = LIVE account balance from Binance (the account is the truth,
                // not an env constant). BRIDGE_BALANCE_PER_LEG, if set >0, acts as a CAP on
                // the sizing base (useful to fence off part of the account), never a substitute.
                double bal = executor.fetchAvailableBalance();
                if (bal <= 0) { patch(id, "failed", "balance fetch failed (" + bal + ") — order not sized"); continue; }
                if (BAL_PER_LEG > 0 && bal > BAL_PER_LEG) bal = BAL_PER_LEG;
                BinanceTradeExecutor.ExecutionResult r = executor.openPositionWithSl(idea, bal);
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

    /**
     * Drain close_requested. Runs UNGATED (see poll()) because closing only shrinks exposure.
     *
     * CAVEAT, verified 2026-07-28 and filed as project_state id=18: closePosition() is reduce-only
     * BY CONSTRUCTION but NOT at the API level — it reads the position, sizes the market order to
     * exactly |posQty| and skips entirely when flat, yet it does not send reduceOnly=true. If the
     * exchange-side stop fills inside the read->send window, the order lands as a NEW opposite-side
     * position instead of a close. Narrow, but on the real endpoint it is the one way this ungated
     * path could create exposure. The fix is one parameter in BinanceTradeExecutor.sendMarketOrder;
     * not applied here because rewriting the execution layer is out of scope for this refactor.
     */
    private void drainCloses() throws Exception {
        JSONArray cl = sbGet("/rest/v1/bot_orders?status=eq.close_requested&testnet=eq." + useTestnet
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
        JSONArray a = sbGet("/rest/v1/bot_orders?status=in.(sent,open)&testnet=eq." + useTestnet + "&select=id&limit=200");
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

    /**
     * New-format Supabase keys (sb_secret_/sb_publishable_) are NOT JWTs — sending them as
     * "Authorization: Bearer" makes PostgREST reject the request. They go in "apikey" only.
     * Legacy JWT keys (eyJ...) keep the historical double-header form.
     */
    private static HttpRequest.Builder sbAuth(HttpRequest.Builder b) {
        b.header("apikey", SUPABASE_KEY);
        if (!SUPABASE_KEY.startsWith("sb_")) b.header("Authorization", "Bearer " + SUPABASE_KEY);
        return b;
    }

    private JSONArray sbGet(String pathQuery) throws Exception {
        HttpResponse<String> resp = http.send(sbAuth(HttpRequest.newBuilder()
                .uri(URI.create(SUPABASE_URL + pathQuery)))
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
        return http.send(sbAuth(HttpRequest.newBuilder()
                .uri(URI.create(SUPABASE_URL + pathQuery)))
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
