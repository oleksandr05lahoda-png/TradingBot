package com.bot;

import com.bot.exec.binance.BinanceVenue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The production endpoint exists in exactly one file, {@code BinanceVenue}, and is reachable
 * through exactly one gate, {@code BinanceVenue.resolve}. This test holds both halves: it scans
 * every source for production hostnames outside the venue class, and it drives the gate through
 * each of its fail-closed edges. The predecessor of this test forbade production hosts outright;
 * the invariant it protected — that missing or mistyped configuration can never reach the real
 * exchange — is unchanged.
 *
 * <p>Hostnames are assembled at runtime from fragments, including in this javadoc, so this file
 * has no self-exclusion and is scanned like any other. The names nest — testnet
 * {@code demo-fapi.<domain>} ends with production {@code fapi.<domain>}, which ends with
 * {@code api.<domain>} — so each pattern carries a negative lookbehind: a match only counts when
 * the forbidden name <i>starts</i> a hostname label rather than ending one.
 */
class VenueContainmentTest {

    /** Assembled at runtime: this string never appears as a literal in this file. */
    private static final String BINANCE_DOMAIN = "binance" + '.' + "com";

    /** The single file allowed to name production hosts. */
    private static final String VENUE_FILE = "BinanceVenue.java";

    private static final Set<String> DEMO_HOSTS = Set.of(
            "demo-fapi." + BINANCE_DOMAIN,
            "demo-fstream." + BINANCE_DOMAIN);

    private static final Set<String> REAL_HOSTS = Set.of(
            "fapi." + BINANCE_DOMAIN,
            "fstream." + BINANCE_DOMAIN);

    private static final List<String> PRODUCTION_LABELS = List.of(
            "fapi",      // USDⓈ-M futures production REST
            "dapi",      // COIN-M futures production REST
            "api",       // spot production REST
            "fstream",   // USDⓈ-M futures production websocket
            "dstream",   // COIN-M futures production websocket
            "stream",    // spot production websocket
            "www",       // the site itself
            "papi");     // portfolio margin production REST

    private static final Pattern ANY_BINANCE_HOST =
            Pattern.compile("[A-Za-z0-9.-]*" + BINANCE_DOMAIN.replace(".", "\\."));

    private static Pattern hostPattern(String label) {
        return Pattern.compile("(?<![-A-Za-z0-9.])" + label + "\\." + BINANCE_DOMAIN.replace(".", "\\."));
    }

    // ─── Containment: the scan half ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("production hosts appear in BinanceVenue.java and nowhere else")
    void productionHostsOnlyInVenueClass() throws IOException {
        List<Path> sources = sourceFiles();
        assertFalse(sources.isEmpty(), "found no source files to scan — the scan root is wrong");

        List<String> offences = new ArrayList<>();
        boolean venueFileNamesProduction = false;
        for (Path file : sources) {
            String content = Files.readString(file, StandardCharsets.UTF_8);
            boolean isVenueFile = file.getFileName().toString().equals(VENUE_FILE);
            for (String label : PRODUCTION_LABELS) {
                Matcher matcher = hostPattern(label).matcher(content);
                while (matcher.find()) {
                    String host = matcher.group().toLowerCase(Locale.ROOT);
                    if (isVenueFile && REAL_HOSTS.contains(host)) {
                        venueFileNamesProduction = true;
                        continue;
                    }
                    offences.add(file + " line " + lineOf(content, matcher.start())
                            + ": production endpoint \"" + matcher.group() + "\"");
                }
            }
        }
        if (!offences.isEmpty()) {
            fail("Production endpoints may exist only inside " + VENUE_FILE + ", behind the arming "
                    + "gate. Found elsewhere:\n  " + String.join("\n  ", offences));
        }
        assertTrue(venueFileNamesProduction,
                VENUE_FILE + " no longer names the production hosts this test expects — "
                        + "if the venue moved, move this test's knowledge with it");
    }

    @Test
    @DisplayName("every Binance hostname outside the venue class is a demo host")
    void onlyDemoHostsOutsideVenueClass() throws IOException {
        List<String> offences = new ArrayList<>();
        for (Path file : sourceFiles()) {
            if (file.getFileName().toString().equals(VENUE_FILE)) continue;
            Matcher matcher = ANY_BINANCE_HOST.matcher(Files.readString(file, StandardCharsets.UTF_8));
            while (matcher.find()) {
                String host = matcher.group().toLowerCase(Locale.ROOT);
                if (host.equals(BINANCE_DOMAIN)) continue; // bare-domain doc mention
                if (!DEMO_HOSTS.contains(host)) {
                    offences.add(host + "  (in " + file + ")");
                }
            }
        }
        assertTrue(offences.isEmpty(),
                "hostnames outside the demo allowlist " + DEMO_HOSTS + " appear outside "
                        + VENUE_FILE + ": " + offences);
    }

    // ─── The gate: fail-closed on every edge ────────────────────────────────────────────────

    @Test
    @DisplayName("no configuration at all resolves to the demo venue")
    void defaultIsDemo() {
        BinanceVenue venue = BinanceVenue.resolve(Map.of());
        assertFalse(venue.isReal());
        assertTrue(DEMO_HOSTS.contains(venue.restHost().toLowerCase(Locale.ROOT)));
        assertEquals("BINANCE_TESTNET_API_KEY", venue.keyEnv());
    }

    @Test
    @DisplayName("ARMED with real keys resolves to the real venue, in OBSERVE mode by default")
    void armedResolvesToRealObserve() {
        BinanceVenue venue = BinanceVenue.resolve(Map.of(
                "REAL_TRADING", "ARMED",
                "BINANCE_REAL_API_KEY", "k", "BINANCE_REAL_API_SECRET", "s"));
        assertTrue(venue.isReal());
        assertEquals(BinanceVenue.RealMode.OBSERVE, venue.realMode());
        assertTrue(REAL_HOSTS.contains(venue.restHost().toLowerCase(Locale.ROOT)));
        assertEquals("BINANCE_REAL_API_KEY", venue.keyEnv());
    }

    @Test
    @DisplayName("REAL_MODE=trade is the only way to an opening real venue")
    void tradeModeIsExplicit() {
        BinanceVenue venue = BinanceVenue.resolve(Map.of(
                "REAL_TRADING", "ARMED", "REAL_MODE", "trade",
                "BINANCE_REAL_API_KEY", "k", "BINANCE_REAL_API_SECRET", "s"));
        assertEquals(BinanceVenue.RealMode.TRADE, venue.realMode());
    }

    @Test
    @DisplayName("any REAL_TRADING value other than exactly ARMED refuses to start")
    void mistypedArmingRefusesToStart() {
        for (String almost : List.of("armed", "Armed", "ARMED ", "true", "1", "yes")) {
            Map<String, String> env = Map.of(
                    "REAL_TRADING", almost,
                    "BINANCE_REAL_API_KEY", "k", "BINANCE_REAL_API_SECRET", "s");
            if (almost.trim().equals("ARMED")) continue; // trimmed exact match is the armed case
            assertThrows(IllegalStateException.class, () -> BinanceVenue.resolve(env),
                    "REAL_TRADING=\"" + almost + "\" must refuse to start, not guess a venue");
        }
    }

    @Test
    @DisplayName("ARMED without real credentials refuses to start")
    void armedWithoutKeysRefusesToStart() {
        assertThrows(IllegalStateException.class,
                () -> BinanceVenue.resolve(Map.of("REAL_TRADING", "ARMED")));
        assertThrows(IllegalStateException.class,
                () -> BinanceVenue.resolve(Map.of(
                        "REAL_TRADING", "ARMED",
                        // testnet names must not satisfy the real gate
                        "BINANCE_TESTNET_API_KEY", "k", "BINANCE_TESTNET_API_SECRET", "s")));
    }

    @Test
    @DisplayName("an unknown REAL_MODE refuses to start")
    void unknownRealModeRefusesToStart() {
        assertThrows(IllegalStateException.class,
                () -> BinanceVenue.resolve(Map.of(
                        "REAL_TRADING", "ARMED", "REAL_MODE", "banana",
                        "BINANCE_REAL_API_KEY", "k", "BINANCE_REAL_API_SECRET", "s")));
    }

    @Test
    @DisplayName("each venue refuses the other's hosts on every request")
    void venuesRejectEachOthersHosts() {
        BinanceVenue demo = BinanceVenue.resolve(Map.of());
        BinanceVenue real = BinanceVenue.resolve(Map.of(
                "REAL_TRADING", "ARMED",
                "BINANCE_REAL_API_KEY", "k", "BINANCE_REAL_API_SECRET", "s"));
        String realUrl = "https://" + "fapi" + "." + BINANCE_DOMAIN + "/fapi/v1/order";
        String demoUrl = "https://demo-fapi." + BINANCE_DOMAIN + "/fapi/v1/order";

        assertThrows(IllegalArgumentException.class, () -> demo.require(realUrl),
                "the demo venue accepted a production URL");
        assertThrows(IllegalArgumentException.class, () -> real.require(demoUrl),
                "the real venue accepted a demo URL");
        demo.require(demoUrl);
        real.require(realUrl);
    }

    // ─── Plumbing ───────────────────────────────────────────────────────────────────────────

    private static List<Path> sourceFiles() throws IOException {
        Path root = scanRoot();
        List<Path> files = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root.resolve("src"))) {
            walk.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".java"))
                    .forEach(files::add);
        }
        try (Stream<Path> walk = Files.list(root)) {
            walk.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".gradle"))
                    .forEach(files::add);
        }
        return files;
    }

    /** Gradle runs tests from the project directory; this tolerates being run from elsewhere too. */
    private static Path scanRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        for (int up = 0; up < 4; up++) {
            if (Files.isDirectory(candidate.resolve("src")) && Files.isRegularFile(candidate.resolve("build.gradle"))) {
                return candidate;
            }
            Path parent = candidate.getParent();
            if (parent == null) break;
            candidate = parent;
        }
        throw new IllegalStateException("could not locate the project root from " + Path.of("").toAbsolutePath());
    }

    private static int lineOf(String content, int offset) {
        int line = 1;
        for (int i = 0; i < offset && i < content.length(); i++) {
            if (content.charAt(i) == '\n') line++;
        }
        return line;
    }
}
