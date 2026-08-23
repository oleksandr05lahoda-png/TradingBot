package com.bot.app;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.logging.Logger;

/**
 * One JSONL row per trading event, on the persistent volume. This is the raw material for honest
 * learning: after a hundred live rows the lab can say which entries were mistakes with the same
 * arithmetic it uses on history — instead of the operator remembering three winners and no losers.
 * Best-effort by design: a full disk must never stop a close from happening.
 */
final class TradeJournal {

    private static final Logger LOG = Logger.getLogger(TradeJournal.class.getName());

    private final Path path;
    private boolean writable = true;

    TradeJournal(Path path) {
        this.path = path;
    }

    /** From {@code TRADE_JOURNAL_PATH}; null (no journal) when the variable is absent or blank. */
    static TradeJournal fromEnvironmentOrNull() {
        String p = System.getenv("TRADE_JOURNAL_PATH");
        if (p == null || p.isBlank()) return null;
        return new TradeJournal(Path.of(p.trim()));
    }

    void entryOpened(String signalId, String symbol, String side, String askedPrice,
                     String fillPrice, String quantity, String stopId, String note) {
        write(row("entry", symbol)
                .put("signalId", signalId).put("side", side)
                .put("asked", askedPrice).put("fill", fillPrice).put("qty", quantity)
                .put("stopId", stopId == null ? "" : stopId).put("note", note));
    }

    void entryRejected(String signalId, String symbol, String reason) {
        write(row("rejected", symbol).put("signalId", signalId).put("reason", reason));
    }

    void closed(String requestId, String symbol, String reason, String quantity, String price, String note) {
        write(row("close", symbol)
                .put("requestId", requestId).put("reason", reason)
                .put("qty", quantity).put("price", price).put("note", note));
    }

    /** A stop or take that fired on the venue, seen by the reconciler rather than commanded. */
    void exchangeExit(String symbol, String detail) {
        write(row("exchange-exit", symbol).put("detail", detail));
    }

    private static JSONObject row(String kind, String symbol) {
        return new JSONObject().put("ts", Instant.now().toString()).put("kind", kind).put("symbol", symbol);
    }

    private void write(JSONObject row) {
        try {
            Files.writeString(path, row + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            if (!writable) {
                writable = true;
                LOG.info("[Journal] writable again: " + path);
            }
        } catch (IOException e) {
            // Say it once per outage; rows lost while unwritable are simply lost.
            if (writable) {
                writable = false;
                LOG.warning("[Journal] cannot append to " + path + ": " + e.getMessage());
            }
        }
    }
}
