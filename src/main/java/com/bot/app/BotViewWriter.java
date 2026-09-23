package com.bot.app;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * {@code <DATA_DIR>/bot_view.json}: the operator snapshot as a file, for the lab bot's 📊 Панель on
 * the owner's laptop (BOT VIEW CONTRACT v1, 24.09). The owner wanted one place with everything -
 * wallet, the day's result, the daily limit, every position - instead of the same numbers spread
 * over three bots; the panel reads this file over SSH, so the trading bot answers no new command.
 *
 * <p>It is a file and nothing else: it controls nothing, and it carries no key, token or account
 * id - only the fields below. The loop only renders a string and hands it over; the write runs on
 * a thread of its own behind a one-slot queue, so a slow or full disk can neither throw into the
 * loop nor hold it. At most one write per pass ({@link #MIN_GAP} apart); each is a temp file in the same
 * directory moved over the old one atomically, so a reader sees the old file or the new, never
 * half of one. A failing write only logs - once, then at most every {@link #FAILURE_LOG_EVERY}.
 */
final class BotViewWriter {

    private static final Logger LOG = Logger.getLogger(BotViewWriter.class.getName());

    static final String FILE_NAME = "bot_view.json";
    static final int VERSION = 1;
    /** The reconcile cadence: one write per pass, never more. */
    static final Duration MIN_INTERVAL = Duration.ofSeconds(30);
    /**
     * The throttle's actual gap, five seconds under the cadence (24.09 review). The loop starts a pass
     * 30 s after the previous START, but offers at the pass END: a pass that runs faster than the one
     * before (the P&amp;L probe's income call makes one slow) lands its offer 29.3 s after the last, and
     * a strict 30 s dropped every other pass. Still far above anything that could hammer the disk.
     */
    static final Duration MIN_GAP = MIN_INTERVAL.minusSeconds(5);
    /**
     * A hold shorter than this does not stall the file enough to matter - the loop publishes again
     * within one pass. Longer, and the hold watchdog writes the parked view itself (24.09 review).
     */
    static final Duration HOLD_WRITE_AFTER = Duration.ofMinutes(1);
    static final Duration FAILURE_LOG_EVERY = Duration.ofMinutes(10);
    /** A halt reason may quote an exchange's HTML error page; the panel needs the gist, not the page. */
    static final int MAX_REASON_CHARS = 300;

    /** Replaces the file's content; the production one is atomic. Swappable so tests can fail or stall it. */
    interface Sink {
        void replace(Path file, String body) throws IOException;
    }

    private final Path file;
    private final Clock clock;
    private final Executor executor;
    private final Sink sink;
    /**
     * Guards {@link #lastOfferAt}: the loop and, during an exchange hold, the hold watchdog both
     * offer. A lock of its own, so an offer never waits on the failure log's monitor.
     */
    private final Object offerLock = new Object();
    private Instant lastOfferAt;
    // Failure bookkeeping, touched from the writer thread and (for a render failure) the loop.
    private int failures;
    private Instant lastFailureLogAt;

    BotViewWriter(Path file, Clock clock, Executor executor, Sink sink) {
        this.file = file;
        this.clock = clock;
        this.executor = executor;
        this.sink = sink;
    }

    /**
     * The production writer: one daemon thread and a queue of one. A newer view pushes out one still
     * waiting - only the latest matters - so a stalled disk costs a stale file, never a queue that
     * grows or a loop that waits. Null if even that could not be set up; the bot runs without it.
     */
    static BotViewWriter startOrNull(Path file) {
        try {
            return start(file, Clock.systemUTC(), BotViewWriter::atomicReplace);
        } catch (RuntimeException e) {
            LOG.warning("[BotView] panel file disabled: " + e.getMessage());
            return null;
        }
    }

    /** The writer thread and its one-slot queue around any sink; the tests stall the sink through it. */
    static BotViewWriter start(Path file, Clock clock, Sink sink) {
        ThreadPoolExecutor ex = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1), r -> {
                    Thread t = new Thread(r, "bot-view-writer");
                    t.setDaemon(true);
                    return t;
                }, new ThreadPoolExecutor.DiscardOldestPolicy());
        return new BotViewWriter(file, clock, ex, sink);
    }

    /**
     * Called by the loop after it publishes a snapshot. Renders here (pure, microseconds) and hands
     * the write to the writer thread. Never throws, never waits on the disk.
     *
     * @return true when a write was handed over; false when throttled or when rendering failed
     */
    boolean offer(OperatorSnapshot s, Optional<String> haltReason) {
        try {
            if (s == null) return false;
            synchronized (offerLock) {
                Instant now = clock.instant();
                if (lastOfferAt != null && Duration.between(lastOfferAt, now).compareTo(MIN_GAP) < 0) return false;
                String body = render(s, haltReason == null ? Optional.empty() : haltReason, now);
                lastOfferAt = now;
                executor.execute(() -> write(body));
                return true;
            }
        } catch (RuntimeException e) {
            failed("not rendered", e);
            return false;
        }
    }

    /**
     * Called by the hold watchdog, never by the loop. During an exchange hold the loop parks inside
     * the rate limiter and offers nothing - for 17 hours on 27.08 - so the file kept saying
     * "trading" and the panel could only call it old. The channel's last snapshot with the hold read
     * now (local state, no request) says what /status says: the exchange is refusing, N min left.
     * Suppliers so a throwing reader costs this one write, never the watchdog thread.
     *
     * @return true when a write was handed over
     */
    boolean offerDuringHold(long heldMs, java.util.function.Supplier<OperatorSnapshot> latestWithHold,
                            java.util.function.Supplier<Optional<String>> haltReason) {
        try {
            if (heldMs < HOLD_WRITE_AFTER.toMillis()) return false;
            OperatorSnapshot s = latestWithHold.get();
            if (s == null || s.exchangeHoldMs() <= 0) return false;
            return offer(s, haltReason.get());
        } catch (RuntimeException e) {
            failed("not rendered during the exchange hold", e);
            return false;
        }
    }

    private void write(String body) {
        try {
            sink.replace(file, body);
            recovered();
        } catch (IOException | RuntimeException e) {
            failed("not written to " + file, e);
        }
    }

    private synchronized void failed(String what, Throwable e) {
        failures++;
        Instant now = clock.instant();
        if (failures == 1 || lastFailureLogAt == null
                || Duration.between(lastFailureLogAt, now).compareTo(FAILURE_LOG_EVERY) >= 0) {
            LOG.warning("[BotView] panel file " + what + " (" + failures + " failure(s) in a row): "
                    + e.getMessage() + " - trading is unaffected, the panel shows the file as old");
            lastFailureLogAt = now;
        }
    }

    private synchronized void recovered() {
        if (failures > 0) {
            LOG.info("[BotView] panel file is being written again after " + failures + " failure(s)");
            failures = 0;
            lastFailureLogAt = null;
        }
    }

    /** Temp file beside the target, then one atomic rename over it: a reader never sees half a file. */
    static void atomicReplace(Path file, String body) throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            Files.writeString(tmp, body, StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException | RuntimeException ignored) {
                // The next write overwrites it anyway.
            }
            throw e;
        }
    }

    // ─── The contract ────────────────────────────────────────────────────────────────────────

    /**
     * The file's text: exactly the contract's fields, in the contract's order. Numbers the loop did
     * not know are {@code null}, never 0 - a panel must not show "$0.00" for "no idea". The state is
     * the one /status leads with, from the same code.
     */
    static String render(OperatorSnapshot s, Optional<String> haltReason, Instant now) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("v", VERSION);
        root.put("ts", ts(s.at()));
        root.put("build", s.buildStamp());
        root.put("started_at", ts(s.startedAt()));
        root.put("state", OperatorViews.stateCode(s, haltReason, now));
        // Plain text - the reason's own "<" stays a "<", and a reader escapes before putting it in a
        // page. One line and clipped like halt_reason (24.09 review): a halt's headline embeds the
        // whole reason, and a quoted error page must not become a multi-line headline.
        root.put("state_line", clip(TgFormat.plain(OperatorViews.stateLine(s, haltReason, now))));
        root.put("halt_reason", haltReason.map(BotViewWriter::clip).orElse(null));

        OperatorSnapshot.Account a = s.account();
        if (a == null) {
            root.put("account", null);
        } else {
            Map<String, Object> acc = new LinkedHashMap<>();
            acc.put("wallet", usd(a.wallet()));
            acc.put("margin_balance", usd(a.marginBalance()));
            acc.put("available", usd(a.available()));
            acc.put("unrealized", usd(a.unrealized()));
            acc.put("at", ts(a.at()));
            root.put("account", acc);
        }

        OperatorSnapshot.Pnl p = s.pnl();
        if (p == null || !p.known()) {
            root.put("pnl", null);
        } else {
            Map<String, Object> pnl = new LinkedHashMap<>();
            pnl.put("today", usd(p.today()));
            pnl.put("d7", usd(p.week()));
            pnl.put("d30", usd(p.month()));
            pnl.put("net", true);   // the income ledger: trades + commissions + funding, no split
            pnl.put("at", ts(p.computedAt()));
            pnl.put("stale", p.failedAt() != null);
            root.put("pnl", pnl);
        }

        OperatorSnapshot.KillSwitch k = s.killSwitch();
        if (k == null) {
            root.put("kill_switch", null);
        } else {
            Map<String, Object> ks = new LinkedHashMap<>();
            ks.put("tripped", k.tripped());
            ks.put("limit_frac", round(k.limitFrac(), 4));
            ks.put("day_start_balance", usd(k.dayStartBalance()));
            ks.put("day_loss_frac", round(k.dayLossFrac(), 4));
            ks.put("resumes_at", ts(k.resumesAt()));
            root.put("kill_switch", ks);
        }

        List<Object> positions = new ArrayList<>();
        for (OperatorSnapshot.Position x : s.positions()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("symbol", x.symbol());
            row.put("side", x.side().name());
            row.put("qty", price(x.quantity()));
            row.put("entry", price(x.entry()));
            row.put("mark", price(x.mark()));
            row.put("pnl_usd", usd(x.unrealizedUsd()));
            row.put("pnl_pct", round(x.pnlPct(), 2));
            row.put("stop", price(x.stop()));
            row.put("take", price(x.take()));
            row.put("to_stop_pct", round(x.toStopPct(), 2));
            row.put("to_take_pct", round(x.toTakePct(), 2));
            row.put("opened_at", ts(x.openedAt()));
            row.put("risk_usd", usd(riskUsd(x)));
            positions.add(row);
        }
        root.put("positions", positions);
        root.put("pending_closes", s.pendingCloses());
        root.put("last_reconcile_ok", ts(s.lastReconcileOkAt()));
        StringBuilder sb = new StringBuilder(512 + 256 * positions.size());
        json(sb, root);
        return sb.append('\n').toString();
    }

    /** What the stop takes if it fills at its trigger, from entry: {@code qty * |entry - stop|}, as the entry line says it. */
    private static double riskUsd(OperatorSnapshot.Position x) {
        if (!(x.quantity() > 0) || !(x.entry() > 0) || !(x.stop() > 0)) return Double.NaN;
        return x.quantity() * Math.abs(x.entry() - x.stop());
    }

    /**
     * One line, at most {@link #MAX_REASON_CHARS}. The cut never splits an emoji (24.09 review): half
     * a surrogate pair cannot be encoded as UTF-8, so every write failed for as long as the halt stood.
     */
    static String clip(String text) {
        String oneLine = text.replace('\n', ' ').replace('\r', ' ').trim();
        if (oneLine.length() <= MAX_REASON_CHARS) return oneLine;
        int end = MAX_REASON_CHARS - 1;
        if (Character.isHighSurrogate(oneLine.charAt(end - 1))) end--;
        return oneLine.substring(0, end) + "…";
    }

    /** {@code 2026-09-24T08:15:02Z}; null stays null. */
    private static String ts(Instant at) {
        return at == null ? null : at.truncatedTo(ChronoUnit.SECONDS).toString();
    }

    private static BigDecimal usd(double v) {
        return round(v, 2);
    }

    private static BigDecimal round(double v, int decimals) {
        if (!Double.isFinite(v)) return null;
        return BigDecimal.valueOf(v).setScale(decimals, java.math.RoundingMode.HALF_UP).stripTrailingZeros();
    }

    /** Prices and sizes: eight significant digits - a mark derived from unrealised P&amp;L carries float dust. */
    private static BigDecimal price(double v) {
        if (!Double.isFinite(v)) return null;
        return new BigDecimal(v).round(new MathContext(8)).stripTrailingZeros();
    }

    /**
     * A JSON string with only what JSON requires escaped. org.json's quote() also escapes the whole
     * U+2000 block, so "—" and "−" in the state line reached the file as backslash-u codes: valid,
     * but unreadable when the owner opens the file by eye.
     */
    private static void quote(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    // Control characters, the two line separators a JavaScript reader chokes on, and a
                    // lone surrogate (an exception message may carry one): coded, it is valid JSON;
                    // raw, the UTF-8 write throws and the file stops updating (24.09 review).
                    boolean pair = Character.isHighSurrogate(c) && i + 1 < s.length()
                            && Character.isLowSurrogate(s.charAt(i + 1));
                    if (pair) {
                        sb.append(c).append(s.charAt(++i));
                    } else if (c < 0x20 || c == 0x2028 || c == 0x2029 || Character.isSurrogate(c)) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    /** A small ordered writer: org.json's objects are hash maps, and a file read by eye should keep its order. */
    private static void json(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof String str) {
            quote(sb, str);
        } else if (v instanceof BigDecimal d) {
            sb.append(d.signum() == 0 ? "0" : d.toPlainString());
        } else if (v instanceof Number || v instanceof Boolean) {
            sb.append(v);
        } else if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                quote(sb, String.valueOf(e.getKey()));
                sb.append(':');
                json(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof List<?> list) {
            sb.append('[');
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(',');
                json(sb, list.get(i));
            }
            sb.append(']');
        } else {
            throw new IllegalArgumentException("not a JSON value: " + v.getClass().getSimpleName());
        }
    }
}
