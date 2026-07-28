package com.bot.paper;


import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Replays stored klines through a hypothesis and records every signal in paper_signals.
 *
 * Runs standalone: {@code main} needs no BotMain, no Telegram token and no Binance keys, so a
 * backtest starts from the IDE with an empty environment. That is the normal way to use it now that
 * there is no hosting — and it is asserted by a test rather than believed.
 *
 * Usage:
 *   --hypothesis com.example.MyHypothesis   fully qualified class implementing {@link Hypothesis}
 *   --mode       dev | holdout
 *   --symbols    BTCUSDT,ETHUSDT
 *   --from       2023-01-01        inclusive
 *   --to         2025-06-30        inclusive
 *   --tf         4h | 1h
 *   --hold       6                 maxHoldBars (default 6)
 *   --url        https://xxx.supabase.co   (or env SUPABASE_URL)
 *   --key        <read-only key>            (or env SUPABASE_READONLY_KEY / SUPABASE_KEY)
 */
public final class HistoricalDriver {

    /**
     * THE SPLIT. Constants, never parameters — a split that can be passed in is a split that gets
     * widened the day a result is disappointing.
     */
    public static final LocalDate DEV_END       = LocalDate.of(2025, 6, 30);
    public static final LocalDate HOLDOUT_START = LocalDate.of(2025, 7, 1);

    public enum Mode {
        dev("backtest_dev"), holdout("backtest_holdout");
        public final String dbValue;
        Mode(String v) { this.dbValue = v; }
    }

    /** Parsed run configuration. Package-private so a test can build it without touching main(). */
    static final class Config {
        String hypothesisClass;
        Mode   mode;
        List<String> symbols;
        LocalDate from, to;
        String timeframe = "4h";
        int    maxHoldBars = 6;
        String url, key;
        String universeId;
    }

    // ─── entry point ──────────────────────────────────────────────────

    public static void main(String[] args) {
        Config cfg;
        try {
            cfg = parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println();
            System.err.println(usage());
            System.exit(2);
            return;
        }
        try {
            int n = run(cfg, new PaperStore(cfg.url, cfg.key));
            System.out.println("done: " + n + " signals recorded for "
                    + cfg.hypothesisClass + " mode=" + cfg.mode + " universe=" + cfg.universeId);
        } catch (Exception e) {
            System.err.println("run failed: " + e);
            System.exit(1);
        }
    }

    static String usage() {
        return "usage: HistoricalDriver --hypothesis <class> --mode dev|holdout "
                + "--symbols A,B --from YYYY-MM-DD --to YYYY-MM-DD [--tf 4h|1h] [--hold N] "
                + "[--url <supabase url>] [--key <read-only key>]\n"
                + "  url/key may come from SUPABASE_URL and SUPABASE_READONLY_KEY (or SUPABASE_KEY).\n"
                + "  dev covers data up to " + DEV_END + "; holdout starts at " + HOLDOUT_START + ".";
    }

    // ─── configuration ────────────────────────────────────────────────

    /**
     * Parse and VALIDATE. Every failure is an IllegalArgumentException carrying what was wrong and
     * what to do — never a NullPointerException from a missing variable, and never a silent default.
     */
    static Config parse(String[] args) {
        Map<String, String> a = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) throw new IllegalArgumentException("unexpected token: " + args[i]);
            if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + args[i]);
            a.put(args[i].substring(2), args[++i]);
        }

        Config c = new Config();
        c.hypothesisClass = require(a, "hypothesis");
        String mode = require(a, "mode");
        try { c.mode = Mode.valueOf(mode); }
        catch (Exception e) { throw new IllegalArgumentException("mode must be dev or holdout, got: " + mode); }

        c.symbols = new ArrayList<>();
        for (String s : require(a, "symbols").split(",")) {
            String t = s.trim().toUpperCase();
            if (!t.isEmpty()) c.symbols.add(t);
        }
        if (c.symbols.isEmpty()) throw new IllegalArgumentException("--symbols is empty");
        Collections.sort(c.symbols);   // universe_id must not depend on argument order

        c.from = parseDate(require(a, "from"), "from");
        c.to   = parseDate(require(a, "to"), "to");
        if (c.to.isBefore(c.from)) throw new IllegalArgumentException("--to is before --from");

        if (a.containsKey("tf")) c.timeframe = a.get("tf").trim();
        if (!c.timeframe.equals("4h") && !c.timeframe.equals("1h")) {
            throw new IllegalArgumentException("--tf must be 4h or 1h, got: " + c.timeframe);
        }
        if (a.containsKey("hold")) {
            try { c.maxHoldBars = Integer.parseInt(a.get("hold").trim()); }
            catch (Exception e) { throw new IllegalArgumentException("--hold must be an integer"); }
            if (c.maxHoldBars <= 0) throw new IllegalArgumentException("--hold must be positive");
        }

        // THE SPLIT, enforced here so no run can quietly cross it.
        if (c.mode == Mode.holdout && c.from.isBefore(HOLDOUT_START)) {
            throw new IllegalArgumentException("holdout starts at " + HOLDOUT_START
                    + "; --from " + c.from + " would read development data. Refusing.");
        }
        if (c.mode == Mode.dev && c.to.isAfter(DEV_END)) {
            throw new IllegalArgumentException("dev ends at " + DEV_END
                    + "; --to " + c.to + " would read holdout data. Refusing.");
        }

        c.url = firstNonBlank(a.get("url"), System.getenv("SUPABASE_URL"));
        c.key = firstNonBlank(a.get("key"),
                System.getenv("SUPABASE_READONLY_KEY"), System.getenv("SUPABASE_KEY"));
        if (isBlank(c.url)) throw new IllegalArgumentException(
                "Supabase URL missing. Pass --url, or set SUPABASE_URL.");
        if (isBlank(c.key)) throw new IllegalArgumentException(
                "Supabase key missing. Pass --key, or set SUPABASE_READONLY_KEY (or SUPABASE_KEY). "
                        + "A read-only key is enough — this driver only reads klines and funding.");

        // Fixed BEFORE the run and written to every row, so the universe cannot be chosen later to
        // suit the result. Deterministic in the symbol set and timeframe, not in their order.
        c.universeId = universeId(c.symbols, c.timeframe);
        return c;
    }

    /** Stable identifier of the symbol set + timeframe. Same set, same id, regardless of order. */
    static String universeId(List<String> sortedSymbols, String timeframe) {
        String joined = String.join(",", sortedSymbols) + "|" + timeframe;
        return "u_" + PaperStore.chainHash(null,
                new org.json.JSONObject().put("u", joined)).substring(0, 16)
                + "_n" + sortedSymbols.size();
    }

    // ─── replay ───────────────────────────────────────────────────────

    /**
     * Load, replay, record. Returns the number of signals written.
     *
     * Each signal is written independently and unconditionally: no risk limit, no exposure cap, no
     * position count. Filtering here would keep only the signals that happened to arrive when the
     * book was empty, and that selection correlates with the very conditions being measured.
     */
    static int run(Config c, PaperStore store) throws Exception {
        Hypothesis h = loadHypothesis(c.hypothesisClass);
        String table = c.timeframe.equals("4h") ? "klines_4h" : "klines_1h";
        long intervalMs = c.timeframe.equals("4h") ? 4 * 3_600_000L : 3_600_000L;

        long fromMs = c.from.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long toMs   = c.to.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

        // Second guard on the split: even if a Config were built by hand, holdout cannot read
        // anything before HOLDOUT_START.
        if (c.mode == Mode.holdout) {
            long holdoutMs = HOLDOUT_START.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            if (fromMs < holdoutMs) {
                throw new IllegalStateException("holdout run tried to read bars before "
                        + HOLDOUT_START + " — refusing");
            }
        }

        Map<String, List<Bar>> series = new LinkedHashMap<>();
        Map<String, List<PaperExecutor.FundingPoint>> funding = new LinkedHashMap<>();
        TreeSet<Long> closes = new TreeSet<>();
        for (String sym : c.symbols) {
            List<Bar> bars = store.loadBars(table, sym, intervalMs, fromMs, toMs);
            series.put(sym, Collections.unmodifiableList(bars));   // snapshots take views of these
            funding.put(sym, store.loadFunding(sym, fromMs, toMs));
            for (Bar b : bars) closes.add(b.closeMs);
            System.out.println("loaded " + sym + ": " + bars.size() + " bars, "
                    + funding.get(sym).size() + " funding points");
        }
        if (closes.isEmpty()) {
            System.out.println("no bars in range — nothing to replay");
            return 0;
        }

        PaperExecutor exec = new PaperExecutor();
        String prevHash = store.lastHash(h.name(), h.version(), c.mode.dbValue);
        int written = 0;

        for (long t : closes) {
            MarketSnapshot snap = MarketSnapshot.asOf(t, series);
            List<Signal> signals = h.evaluate(snap);
            if (signals == null || signals.isEmpty()) continue;

            for (Signal s : signals) {
                List<Bar> full = series.get(s.symbol);
                if (full == null) continue;                       // symbol outside the fixed universe
                PaperExecutor.Fill f = exec.simulate(s, full, funding.get(s.symbol), c.maxHoldBars);
                if (f == null) continue;                          // window not finished — not a trade

                // TWO operations. The prediction first, on its own; the outcome afterwards. The
                // database now refuses to take them together, so this cannot regress silently.
                long id = store.insertPrediction(h.name(), h.version(), c.mode.dbValue,
                        c.universeId, s, f.entryBarOpenMs, f.entryPx, false, prevHash);
                store.updateOutcome(id, f);
                prevHash = store.lastHash(h.name(), h.version(), c.mode.dbValue);
                written++;
            }
        }
        return written;
    }

    static Hypothesis loadHypothesis(String className) {
        try {
            Class<?> k = Class.forName(className);
            Object o = k.getDeclaredConstructor().newInstance();
            if (!(o instanceof Hypothesis)) {
                throw new IllegalArgumentException(className + " does not implement Hypothesis");
            }
            return (Hypothesis) o;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("hypothesis class not found: " + className);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("cannot instantiate " + className
                    + " (needs a public no-arg constructor): " + e.getMessage());
        }
    }

    // ─── small helpers ────────────────────────────────────────────────

    private static String require(Map<String, String> a, String k) {
        String v = a.get(k);
        if (isBlank(v)) throw new IllegalArgumentException("--" + k + " is required");
        return v.trim();
    }

    private static LocalDate parseDate(String s, String what) {
        try { return LocalDate.parse(s); }
        catch (Exception e) { throw new IllegalArgumentException("--" + what + " must be YYYY-MM-DD, got: " + s); }
    }

    private static boolean isBlank(String s) { return s == null || s.isBlank(); }

    private static String firstNonBlank(String... v) {
        for (String s : v) if (!isBlank(s)) return s.trim();
        return null;
    }

    private HistoricalDriver() { }
}
