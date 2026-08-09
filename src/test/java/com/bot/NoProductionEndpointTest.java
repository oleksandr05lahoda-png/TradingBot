package com.bot;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fails the build if a production Binance endpoint appears anywhere in the sources.
 *
 * <p>This is the load-bearing test of the whole repository. Every other safety property assumes the
 * orders are going to a testnet; if that assumption breaks, none of the rest matters. It is also the
 * property most likely to be broken by accident — by a copied snippet, a commented-out URL, a
 * "temporary" switch — which is exactly why it is asserted mechanically rather than reviewed.
 *
 * <h2>How it avoids matching itself</h2>
 * The forbidden hostnames are assembled at runtime from fragments, so no production host appears as
 * a literal anywhere in this file either. That means this test scans its own source like any other
 * and has no self-exclusion to hide behind.
 *
 * <h2>How it avoids matching the testnet</h2>
 * The hostnames nest: the testnet host {@code demo-fapi.<domain>} ends with the production host
 * {@code fapi.<domain>}, which in turn ends with the spot host {@code api.<domain>}. A plain
 * substring search would therefore flag the legitimate testnet host as a production endpoint. Each
 * pattern is anchored with a negative lookbehind for hostname characters, so a match only counts
 * when the forbidden name <i>starts</i> a hostname label rather than ending one.
 *
 * <p>(This is also why the documentation above writes {@code <domain>} instead of spelling the real
 * one out: the first draft of this file did spell it out, and the test failed on its own javadoc.
 * That failure was correct, and keeping the rule honest is worth a slightly awkward comment.)
 */
class NoProductionEndpointTest {

    /** Assembled at runtime: this string never appears as a literal in this file. */
    private static final String BINANCE_DOMAIN = "binance" + '.' + "com";

    /** Hostnames the build is allowed to contain, mirroring BinanceTestnetEndpoint.ALLOWED_HOSTS. */
    private static final Set<String> ALLOWED_HOSTS = Set.of(
            "demo-fapi." + BINANCE_DOMAIN,
            "demo-fstream." + BINANCE_DOMAIN);

    /** A forbidden name only counts when it begins a hostname, never mid-label. */
    private static Pattern hostPattern(String label) {
        return Pattern.compile("(?<![-A-Za-z0-9.])" + label + "\\." + BINANCE_DOMAIN.replace(".", "\\."));
    }

    private static final List<String> FORBIDDEN_LABELS = List.of(
            "fapi",      // USDⓈ-M futures production REST
            "dapi",      // COIN-M futures production REST
            "api",       // spot production REST
            "fstream",   // USDⓈ-M futures production websocket
            "dstream",   // COIN-M futures production websocket
            "stream",    // spot production websocket
            "www",       // the site itself
            "papi");     // portfolio margin production REST

    /** Every hostname on the exchange's domain mentioned anywhere in the sources. */
    private static final Pattern ANY_BINANCE_HOST =
            Pattern.compile("[A-Za-z0-9.-]*" + BINANCE_DOMAIN.replace(".", "\\."));

    @Test
    @DisplayName("no production Binance endpoint appears in any source file")
    void noProductionEndpointInSources() throws IOException {
        List<Path> sources = sourceFiles();
        assertFalse(sources.isEmpty(), "found no source files to scan — the scan root is wrong");

        List<String> offences = new ArrayList<>();
        for (Path file : sources) {
            String content = Files.readString(file, StandardCharsets.UTF_8);
            for (String label : FORBIDDEN_LABELS) {
                Matcher matcher = hostPattern(label).matcher(content);
                while (matcher.find()) {
                    offences.add(file + " line " + lineOf(content, matcher.start())
                            + ": production endpoint \"" + matcher.group() + "\"");
                }
            }
        }

        if (!offences.isEmpty()) {
            fail("This build talks to the Binance futures TESTNET and nothing else. "
                    + "A production endpoint was found in the sources:\n  " + String.join("\n  ", offences));
        }
    }

    @Test
    @DisplayName("every Binance hostname in the sources is on the testnet allowlist")
    void onlyAllowlistedBinanceHostsAppear() throws IOException {
        Set<String> found = new LinkedHashSet<>();
        for (Path file : sourceFiles()) {
            Matcher matcher = ANY_BINANCE_HOST.matcher(Files.readString(file, StandardCharsets.UTF_8));
            while (matcher.find()) {
                String host = matcher.group().toLowerCase(Locale.ROOT);
                // The bare domain with no label is a documentation mention, not an endpoint.
                if (host.equals(BINANCE_DOMAIN)) continue;
                if (!ALLOWED_HOSTS.contains(host)) {
                    found.add(host + "  (in " + file + ")");
                }
            }
        }
        assertTrue(found.isEmpty(),
                "hostnames outside the testnet allowlist " + ALLOWED_HOSTS + " appear in the sources: " + found);
    }

    @Test
    @DisplayName("the endpoint constant itself resolves to a testnet host")
    void endpointConstantIsTestnet() {
        String host = com.bot.exec.binance.BinanceTestnetEndpoint.restHost().toLowerCase(Locale.ROOT);
        assertTrue(ALLOWED_HOSTS.contains(host), "REST base resolves to \"" + host + "\", not a testnet host");
        assertTrue(com.bot.exec.binance.BinanceTestnetEndpoint.ALLOWED_HOSTS.stream()
                        .allMatch(h -> ALLOWED_HOSTS.contains(h.toLowerCase(Locale.ROOT))),
                "BinanceTestnetEndpoint.ALLOWED_HOSTS has grown beyond the testnet hosts this test knows about");
    }

    @Test
    @DisplayName("requireTestnet refuses a non-testnet host")
    void requireTestnetRefusesOtherHosts() {
        // Built from fragments for the same reason as everything else in this file.
        String productionish = "https://" + "fapi" + "." + BINANCE_DOMAIN + "/fapi/v1/order";
        IllegalArgumentException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> com.bot.exec.binance.BinanceTestnetEndpoint.requireTestnet(productionish));
        assertTrue(thrown.getMessage().contains("testnet"), thrown.getMessage());
    }

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
