package com.bot.signal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Enforces the rule that there are exactly two ways a trade idea can enter this system: an operator
 * typing, and an external queue.
 *
 * <p>The rule is structural rather than stylistic. Everything downstream of a
 * {@link SignalSource} — sizing, stops, liquidation, exposure, reconciliation — is safety machinery
 * that applies to any signal regardless of origin. A strategy added as a third implementation would
 * inherit all of it and would be indistinguishable, from every other layer's point of view, from a
 * person typing a line. That is the disguise this project cannot afford: no strategy in it has ever
 * passed its own pre-registered validation gates, and 92 candidates have failed them.
 *
 * <p>The declaration forms matter. An earlier version of this test matched only the keyword
 * {@code class}, so {@code record X(...) implements SignalSource} — the shape this codebase reaches
 * for by default, used already by {@code RiskDecision.Approved} and {@code StopLoss.Structural} —
 * was invisible to it, and a third implementation could have landed with the suite green. Every
 * declaration form that can implement an interface is matched now, and the strategy scan follows
 * whatever the first check finds rather than a hardcoded list of filenames.
 */
class SignalSourceImplementationsTest {

    private static final Set<String> EXPECTED = Set.of("ManualTestnetInput", "SupabaseQueueSource");

    /** class, record, enum and interface — every form that can carry {@code implements}. */
    private static final Pattern IMPLEMENTS_SIGNAL_SOURCE = Pattern.compile(
            "\\b(?:class|record|enum|interface)\\s+(\\w+)[^{;]*\\bimplements\\b[^{;]*\\bSignalSource\\b");

    /** Anonymous implementations, which carry no name to report. */
    private static final Pattern ANONYMOUS_SIGNAL_SOURCE = Pattern.compile("new\\s+SignalSource\\s*\\(");

    /**
     * Whole words that only appear when something is computing a trading decision. Matched on word
     * boundaries, not as substrings, so "representation" is not mistaken for an EMA.
     *
     * <p>{@code atr} is deliberately absent: a source may <i>carry</i> a volatility figure supplied
     * from outside — that is transport. It may not compute one.
     */
    private static final Pattern STRATEGY_SHAPED = Pattern.compile(
            "\\b(ema|sma|rsi|macd|bollinger|keltner|adx|stochastic|indicator|indicators|candle|candles|"
                    + "ohlc|kline|klines|backtest|orderbook|divergence)\\b",
            Pattern.CASE_INSENSITIVE);

    @Test
    @DisplayName("exactly two SignalSource implementations exist, in any declaration form")
    void exactlyTwoImplementations() throws IOException {
        Map<String, Path> found = findImplementations();
        assertEquals(EXPECTED, new TreeSet<>(found.keySet()),
                "SignalSource must have exactly the two permitted implementations. A third would be a "
                        + "strategy wearing the interface as a costume: it would inherit every safety "
                        + "property and look, to the rest of the system, like an operator typing.");
    }

    @Test
    @DisplayName("no anonymous SignalSource is created anywhere in the main sources")
    void noAnonymousImplementations() throws IOException {
        for (Path file : mainSources()) {
            Matcher matcher = ANONYMOUS_SIGNAL_SOURCE.matcher(Files.readString(file, StandardCharsets.UTF_8));
            if (matcher.find()) {
                fail("anonymous SignalSource in " + file + " — an implementation with no name is one "
                        + "the implementation count cannot see");
            }
        }
    }

    @Test
    @DisplayName("no signal source computes anything that resembles a trading decision")
    void sourcesAreTransportsNotStrategies() throws IOException {
        // Driven by whatever the first check found, so a new implementation is scanned automatically
        // instead of being skipped by a filename list that nobody remembered to update.
        for (Map.Entry<String, Path> implementation : findImplementations().entrySet()) {
            String body = Files.readString(implementation.getValue(), StandardCharsets.UTF_8);
            Matcher matcher = STRATEGY_SHAPED.matcher(body);
            if (matcher.find()) {
                fail(implementation.getKey() + " mentions \"" + matcher.group()
                        + "\" at offset " + matcher.start()
                        + ". A source transports a decision made elsewhere; it does not compute one.");
            }
        }
    }

    @Test
    @DisplayName("the detector itself sees every declaration form")
    void detectorCoversEveryDeclarationForm() {
        // Guards the guard: an earlier version silently missed the record form, which is the one
        // this codebase writes by default.
        List<String> shouldMatch = List.of(
                "public final class Foo implements SignalSource {",
                "public class Foo implements SignalSource {",
                "class Foo implements SignalSource {",
                "public record Foo(Clock clock) implements SignalSource {",
                "record Foo() implements SignalSource {",
                "public enum Foo implements SignalSource {",
                "public final class Foo extends Bar implements AutoCloseable, SignalSource {");

        for (String declaration : shouldMatch) {
            Matcher matcher = IMPLEMENTS_SIGNAL_SOURCE.matcher(declaration);
            assertTrue(matcher.find(), "the detector does not see: " + declaration);
            assertEquals("Foo", matcher.group(1), "wrong name extracted from: " + declaration);
        }

        assertTrue(ANONYMOUS_SIGNAL_SOURCE.matcher("SignalSource s = new SignalSource() {").find(),
                "the detector does not see an anonymous implementation");
    }

    /** Type name to the file that declares it, for every SignalSource implementation in main. */
    private static Map<String, Path> findImplementations() throws IOException {
        Map<String, Path> found = new LinkedHashMap<>();
        for (Path file : mainSources()) {
            Matcher matcher = IMPLEMENTS_SIGNAL_SOURCE.matcher(Files.readString(file, StandardCharsets.UTF_8));
            while (matcher.find()) {
                found.put(matcher.group(1), file);
            }
        }
        return found;
    }

    private static List<Path> mainSources() throws IOException {
        Path root = Path.of("").toAbsolutePath();
        for (int up = 0; up < 4 && root != null && !Files.isDirectory(root.resolve("src/main/java")); up++) {
            root = root.getParent();
        }
        if (root == null) throw new IllegalStateException("could not locate src/main/java");
        List<Path> files = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root.resolve("src/main/java"))) {
            walk.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".java"))
                    .forEach(files::add);
        }
        return files;
    }
}
