package com.bot.paper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HistoricalDriverTest {

    private static final String[] BASE = {
            "--hypothesis", "com.bot.paper.HistoricalDriverTest$Noop",
            "--mode", "dev",
            "--universe", "perp49_2026_07_28",
            "--from", "2023-01-01",
            "--to", "2025-06-30",
            "--url", "https://example.supabase.co",
            "--key", "test-key"
    };

    /** A hypothesis that predicts nothing — the harness ships empty, and this is not a strategy. */
    public static final class Noop implements Hypothesis {
        @Override public String name() { return "noop"; }
        @Override public String version() { return "v0"; }
        @Override public List<Signal> evaluate(MarketSnapshot snap) { return List.of(); }
    }

    // ─── the requirement: no mandatory environment ────────────────────

    @Test
    @DisplayName("runs as a real process with every bot env var removed")
    void runsWithNoBotEnvironment() throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                System.getProperty("java.home") + "/bin/java",
                "-cp", System.getProperty("java.class.path"),
                "com.bot.paper.HistoricalDriver");     // no args -> must print usage, not crash

        // Strip exactly what the rest of this codebase demands at class-init. If HistoricalDriver
        // ever pulls in BotMain, requireEnv("TELEGRAM_TOKEN") throws and this test fails — which is
        // the whole point. PATH and SystemRoot stay so the JVM can start at all.
        Map<String, String> env = pb.environment();
        env.keySet().removeIf(k -> {
            String u = k.toUpperCase();
            return u.startsWith("TELEGRAM") || u.startsWith("BINANCE") || u.startsWith("SUPABASE")
                    || u.startsWith("BRIDGE_") || u.startsWith("RG_") || u.startsWith("BOT_");
        });
        pb.redirectErrorStream(true);

        Process p = pb.start();
        String out;
        try (InputStream in = p.getInputStream(); ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            in.transferTo(bos);
            out = bos.toString(java.nio.charset.StandardCharsets.UTF_8);
        }
        int code = p.waitFor();

        assertEquals(2, code, "no args should exit 2 with usage, got output:\n" + out);
        assertTrue(out.contains("usage: HistoricalDriver"),
                "should print usage; got:\n" + out);
        assertTrue(out.contains("--hypothesis is required"),
                "should say what is missing; got:\n" + out);
        assertTrue(!out.contains("TELEGRAM_TOKEN") && !out.contains("Required env var missing"),
                "must not drag BotMain's env requirements in; got:\n" + out);
    }

    @Test
    @DisplayName("explicit --url/--key need no environment at all")
    void explicitCredentialsParse() {
        HistoricalDriver.Config c = HistoricalDriver.parse(BASE);
        assertEquals("https://example.supabase.co", c.url);
        assertEquals("test-key", c.key);
        assertEquals(HistoricalDriver.Mode.dev, c.mode);
        assertEquals("perp49_2026_07_28", c.universeId);
        assertNull(c.symbols, "membership is resolved from the frozen universe in run(), not parsed");
    }

    @Test
    @DisplayName("missing key fails with an actionable message, not a NullPointerException")
    void missingKeyIsActionable() {
        String[] noKey = {
                "--hypothesis", "com.bot.paper.HistoricalDriverTest$Noop",
                "--mode", "dev", "--universe", "perp49_2026_07_28",
                "--from", "2023-01-01", "--to", "2025-06-30",
                "--url", "https://example.supabase.co"
        };
        // Only meaningful when the ambient environment has no key either; when it does, parse()
        // legitimately succeeds by picking it up.
        if (System.getenv("SUPABASE_READONLY_KEY") != null || System.getenv("SUPABASE_KEY") != null) return;

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> HistoricalDriver.parse(noKey));
        assertTrue(e.getMessage().contains("SUPABASE_READONLY_KEY"),
                "must name the variable to set, got: " + e.getMessage());
    }

    // ─── the split is a constant, not a suggestion ────────────────────

    @Test
    @DisplayName("holdout refuses to read anything before HOLDOUT_START")
    void holdoutRefusesDevData() {
        String[] a = BASE.clone();
        a[3] = "holdout";
        a[7] = "2024-01-01";        // --from, well inside development data
        a[9] = "2025-12-31";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> HistoricalDriver.parse(a));
        assertTrue(e.getMessage().contains("holdout starts at 2025-07-01"),
                "got: " + e.getMessage());
    }

    @Test
    @DisplayName("dev refuses to read past DEV_END")
    void devRefusesHoldoutData() {
        String[] a = BASE.clone();
        a[9] = "2025-08-01";        // --to, past the split
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> HistoricalDriver.parse(a));
        assertTrue(e.getMessage().contains("dev ends at 2025-06-30"), "got: " + e.getMessage());
    }

    @Test
    @DisplayName("run() refuses a hand-built holdout config that reaches before the split")
    void runGuardsTheSplitToo() {
        HistoricalDriver.Config c = new HistoricalDriver.Config();
        c.hypothesisClass = "com.bot.paper.HistoricalDriverTest$Noop";
        c.mode = HistoricalDriver.Mode.holdout;
        c.symbols = List.of("BTCUSDT");
        c.from = java.time.LocalDate.of(2024, 1, 1);     // bypasses parse() entirely
        c.to = java.time.LocalDate.of(2025, 12, 31);
        c.universeId = "u_test";
        assertThrows(IllegalStateException.class,
                () -> HistoricalDriver.run(c, new PaperStore("https://example.supabase.co", "k")),
                "the guard in parse() must not be the only one");
    }

    // ─── universe is fixed before the run ─────────────────────────────

    @Test
    @DisplayName("--universe is required; there is no default symbol set")
    void universeIsRequired() {
        String[] a = {
                "--hypothesis", "com.bot.paper.HistoricalDriverTest$Noop",
                "--mode", "dev", "--from", "2023-01-01", "--to", "2025-06-30",
                "--url", "https://example.supabase.co", "--key", "k"
        };
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> HistoricalDriver.parse(a));
        assertTrue(e.getMessage().contains("--universe"),
                "a missing universe must be named, not silently defaulted: " + e.getMessage());
    }

    @Test
    @DisplayName("--symbols is gone: a symbol list on the command line is not a pre-registration")
    void symbolsArgumentIsRejected() {
        String[] a = BASE.clone();
        a[4] = "--symbols";
        a[5] = "BTCUSDT,ETHUSDT";
        // parse() treats unknown flags as absent required ones rather than accepting them, so the
        // universe requirement fires. The point is that a caller cannot slip a hand-picked set in.
        assertThrows(IllegalArgumentException.class, () -> HistoricalDriver.parse(a));
    }
}
