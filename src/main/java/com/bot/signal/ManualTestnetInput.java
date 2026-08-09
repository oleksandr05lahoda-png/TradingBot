package com.bot.signal;

import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.risk.RiskConstants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * An operator typing trades, one line at a time. This is the source the smoke run uses, and the
 * reason the system can be exercised end to end without anything that resembles a strategy.
 *
 * <h2>Line format</h2>
 * <pre>
 *   SYMBOL SIDE entry=&lt;price&gt; [stop=&lt;price&gt;] [atr=&lt;value&gt;] [lev=&lt;1..5&gt;] [id=&lt;text&gt;]
 *   CLOSE SYMBOL [id=&lt;text&gt;] [reason=&lt;text&gt;]
 *
 *   BTCUSDT LONG  entry=64000 stop=62800 lev=3
 *   ETHUSDT SHORT entry=3120  atr=45     lev=2
 *   CLOSE BTCUSDT reason=done
 * </pre>
 * Blank lines and lines starting with {@code #} are ignored. At least one of {@code stop} or
 * {@code atr} must be present — a line with neither is refused at the point of typing rather than
 * silently turned into an unprotected position.
 *
 * <p>Ids are stable per line: an explicit {@code id=} is used as given, otherwise one is derived
 * from the content and a monotonic counter. Because the client order id derives from the signal id,
 * two identical lines typed deliberately are two trades, while the <i>same</i> line replayed from a
 * script after a crash is one.
 */
public final class ManualTestnetInput implements SignalSource {

    private static final Logger LOG = Logger.getLogger(ManualTestnetInput.class.getName());

    private final BufferedReader reader;
    private final Clock clock;
    private final int defaultLeverage;
    private final AtomicLong sequence = new AtomicLong();
    private final boolean blocking;
    private final Deque<Signal> pendingSignals = new ArrayDeque<>();
    private final Deque<CloseRequest> pendingCloses = new ArrayDeque<>();

    /** Reads from the console. Used by the smoke run. */
    public static ManualTestnetInput fromConsole(int defaultLeverage) {
        return new ManualTestnetInput(
                new InputStreamReader(System.in, StandardCharsets.UTF_8), Clock.systemUTC(), defaultLeverage, true);
    }

    /** Reads from any reader — a script file in a smoke run, a {@code StringReader} in a test. */
    public static ManualTestnetInput fromReader(Reader reader, Clock clock, int defaultLeverage) {
        return new ManualTestnetInput(reader, clock, defaultLeverage, false);
    }

    private ManualTestnetInput(Reader reader, Clock clock, int defaultLeverage, boolean blocking) {
        this.reader = new BufferedReader(Preconditions.notNull(reader, "reader"));
        this.clock = Preconditions.notNull(clock, "clock");
        this.defaultLeverage = Preconditions.positive(defaultLeverage, "defaultLeverage");
        Preconditions.require(defaultLeverage <= RiskConstants.MAX_LEVERAGE,
                "defaultLeverage " + defaultLeverage + " exceeds the hard cap " + RiskConstants.MAX_LEVERAGE);
        this.blocking = blocking;
    }

    @Override public String name() { return "manual-testnet-input"; }

    /**
     * Consumes whatever lines are available. On the console this blocks for one line at a time, which
     * is what an operator expects; from a reader it drains what is buffered and returns.
     *
     * <p>A malformed line is logged and skipped, not thrown: a typo must not take down a loop that is
     * currently holding positions.
     */
    @Override public List<Signal> poll() throws IOException {
        readAvailableLines();
        return takeAll(pendingSignals);
    }

    @Override public List<CloseRequest> pollCloses() throws IOException {
        readAvailableLines();
        return takeAll(pendingCloses);
    }

    /** Reads and routes: a {@code CLOSE} line becomes a close request, anything else a signal. */
    private void readAvailableLines() throws IOException {
        while (blocking || reader.ready()) {
            String line = reader.readLine();
            if (line == null) break;
            if (line.isBlank() || line.trim().startsWith("#")) {
                if (blocking) break;
                continue;
            }
            try {
                long seq = sequence.incrementAndGet();
                if (line.trim().toUpperCase(Locale.ROOT).startsWith("CLOSE")) {
                    pendingCloses.add(parseClose(line, clock, seq));
                } else {
                    pendingSignals.add(parse(line, clock, defaultLeverage, seq));
                }
            } catch (RuntimeException e) {
                LOG.warning("[ManualInput] ignoring line \"" + line.trim() + "\": " + e.getMessage());
            }
            if (blocking) break;
        }
    }

    private static <T> List<T> takeAll(Deque<T> queue) {
        if (queue.isEmpty()) return List.of();
        List<T> out = new ArrayList<>(queue);
        queue.clear();
        return out;
    }

    /**
     * Parses {@code CLOSE SYMBOL [id=<text>] [reason=<text>]}.
     *
     * @throws IllegalArgumentException on anything it cannot turn into a close request
     */
    public static CloseRequest parseClose(String line, Clock clock, long sequence) {
        Preconditions.notBlank(line, "line");
        String[] parts = line.trim().split("\\s+");
        Preconditions.require(parts.length >= 2, "expected: CLOSE SYMBOL");

        String symbol = parts[1].toUpperCase(Locale.ROOT);
        String id = null;
        String reason = "operator";
        for (int i = 2; i < parts.length; i++) {
            int eq = parts[i].indexOf('=');
            Preconditions.require(eq > 0, "expected key=value, got \"" + parts[i] + "\"");
            String key = parts[i].substring(0, eq).toLowerCase(Locale.ROOT);
            String value = parts[i].substring(eq + 1);
            switch (key) {
                case "id" -> id = value;
                case "reason" -> reason = value;
                default -> throw new IllegalArgumentException("unknown key \"" + key + "\"");
            }
        }
        return new CloseRequest(
                id != null && !id.isBlank() ? id : "manual-close-" + sequence + "-" + symbol,
                symbol, reason, clock.instant());
    }

    @Override public void onClosed(CloseRequest request, ExecutionFeedback feedback) {
        LOG.info("[ManualInput] " + request.symbol() + " closed " + feedback.filledQuantity().toPlainString()
                + " @ " + feedback.averageFillPrice().toPlainString() + " — " + feedback.note());
    }

    @Override public void onAccepted(Signal signal, ExecutionFeedback feedback) {
        LOG.info(String.format("[ManualInput] %s filled %s @ %s (%.1f bp vs the %s asked for) — %s",
                signal.id(), feedback.filledQuantity().toPlainString(),
                feedback.averageFillPrice().toPlainString(),
                feedback.slippageBpAgainst(signal.entryPrice(), signal.side() == com.bot.core.Side.LONG),
                signal.entryPrice(), feedback.note()));
    }

    @Override public void onRejected(Signal signal, String reason) {
        LOG.warning("[ManualInput] REFUSED " + signal.id() + ": " + reason);
    }

    @Override public void close() throws IOException {
        reader.close();
    }

    /**
     * Parses one line into a {@link Signal}. Public and static so the format is testable without a
     * stream, a clock or a thread.
     *
     * @throws IllegalArgumentException on anything it cannot turn into a valid signal
     */
    public static Signal parse(String line, Clock clock, int defaultLeverage, long sequence) {
        Preconditions.notBlank(line, "line");
        String[] parts = line.trim().split("\\s+");
        Preconditions.require(parts.length >= 3,
                "expected at least: SYMBOL SIDE entry=<price>");

        String symbol = parts[0].toUpperCase(Locale.ROOT);
        Side side;
        try {
            side = Side.valueOf(parts[1].toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("side must be LONG or SHORT, got \"" + parts[1] + "\"");
        }

        Double entry = null;
        Double stop = null;
        Double atr = null;
        Integer leverage = null;
        String id = null;

        for (int i = 2; i < parts.length; i++) {
            String token = parts[i];
            int eq = token.indexOf('=');
            Preconditions.require(eq > 0, "expected key=value, got \"" + token + "\"");
            String key = token.substring(0, eq).toLowerCase(Locale.ROOT);
            String value = token.substring(eq + 1);
            switch (key) {
                case "entry" -> entry = parseDouble(key, value);
                case "stop", "sl" -> stop = parseDouble(key, value);
                case "atr" -> atr = parseDouble(key, value);
                case "lev", "leverage" -> leverage = parseInt(key, value);
                case "id" -> id = value;
                default -> throw new IllegalArgumentException("unknown key \"" + key + "\"");
            }
        }

        Preconditions.require(entry != null, "entry= is required");
        Preconditions.require(stop != null || atr != null,
                "one of stop= or atr= is required — a signal with neither cannot be sized and "
                        + "cannot be protected");

        String signalId = id != null && !id.isBlank()
                ? id
                : String.format("manual-%d-%s-%s", sequence, symbol, side);

        return new Signal(
                signalId,
                symbol,
                side,
                entry,
                stop == null ? OptionalDouble.empty() : OptionalDouble.of(stop),
                atr == null ? OptionalDouble.empty() : OptionalDouble.of(atr),
                leverage == null ? defaultLeverage : leverage,
                clock.instant());
    }

    private static double parseDouble(String key, String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + "= is not a number: \"" + value + "\"");
        }
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + "= is not an integer: \"" + value + "\"");
        }
    }
}
