package com.bot.signal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * <p>So the count is asserted, and a new implementation has to delete this test to land — which is a
 * conversation rather than an accident.
 */
class SignalSourceImplementationsTest {

    private static final Set<String> EXPECTED = Set.of("ManualTestnetInput", "SupabaseQueueSource");

    private static final Pattern IMPLEMENTS_SIGNAL_SOURCE =
            Pattern.compile("(?:final\\s+)?class\\s+(\\w+)[^{]*implements[^{]*\\bSignalSource\\b");

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
    @DisplayName("exactly two SignalSource implementations exist in the main sources")
    void exactlyTwoImplementations() throws IOException {
        Set<String> found = new TreeSet<>();
        for (Path file : mainSources()) {
            Matcher matcher = IMPLEMENTS_SIGNAL_SOURCE.matcher(Files.readString(file, StandardCharsets.UTF_8));
            while (matcher.find()) found.add(matcher.group(1));
        }
        assertEquals(EXPECTED, found,
                "SignalSource must have exactly the two permitted implementations. A third would be a "
                        + "strategy wearing the interface as a costume: it would inherit every safety "
                        + "property and look, to the rest of the system, like an operator typing.");
    }

    @Test
    @DisplayName("no signal source computes anything that resembles a trading decision")
    void sourcesAreTransportsNotStrategies() throws IOException {
        for (Path file : mainSources()) {
            String name = file.getFileName().toString();
            if (!name.equals("ManualTestnetInput.java") && !name.equals("SupabaseQueueSource.java")) continue;

            Matcher matcher = STRATEGY_SHAPED.matcher(Files.readString(file, StandardCharsets.UTF_8));
            assertTrue(!matcher.find(),
                    name + " mentions \"" + (matcher.hitEnd() ? "" : matcher.group())
                            + "\". A source transports a decision made elsewhere; it does not compute one.");
        }
    }

    private static List<Path> mainSources() throws IOException {
        Path root = Path.of("").toAbsolutePath();
        for (int up = 0; up < 4 && root != null && !Files.isDirectory(root.resolve("src/main/java")); up++) {
            root = root.getParent();
        }
        if (root == null) throw new IllegalStateException("could not locate src/main/java");
        try (Stream<Path> walk = Files.walk(root.resolve("src/main/java"))) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".java"))
                    .toList();
        }
    }
}
