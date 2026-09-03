package com.bot.app;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;

/**
 * One JSONL row per trading event, on the persistent volume. This is the raw material for honest
 * learning: after a hundred live rows the lab can say which entries were mistakes with the same
 * arithmetic it uses on history — instead of the operator remembering three winners and no losers.
 * Best-effort by design: a full disk must never stop a close from happening.
 *
 * <p>Row kinds: {@code entry} (asked vs fill, stop price, take-profit prices, risked dollars,
 * leverage, notional — enough for an R-denominated verdict), {@code rejected}, {@code aborted}
 * (filled and unwound at once: a real round trip with two taker fees), {@code close} (a close this
 * process commanded, with price and quantity) and {@code exchange-exit} (a stop or take that filled
 * on the venue, with the order's price and quantity when they could be read — including exits
 * that happened while the process was down). Fees and funding are NOT here: the lab joins them
 * from {@code /fapi/v1/income} by symbol and time, which is the exchange's own ledger.
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
        write(entryRow(signalId, symbol, side, askedPrice, fillPrice, quantity, stopId, note));
    }

    /** The full plan behind the fill, so a verdict can be computed in R and net of the stop distance. */
    void entryOpened(String signalId, String symbol, String side, String askedPrice, String fillPrice,
                     String quantity, String stopId, String stopPrice, List<String> takeProfitPrices,
                     double riskUsd, int leverage, double notionalUsd, String note) {
        JSONObject row = entryRow(signalId, symbol, side, askedPrice, fillPrice, quantity, stopId, note)
                .put("stopPrice", stopPrice == null ? "" : stopPrice)
                .put("tp", new JSONArray(takeProfitPrices == null ? List.of() : takeProfitPrices))
                .put("riskUsd", round(riskUsd))
                .put("lev", leverage)
                .put("notionalUsd", round(notionalUsd));
        write(row);
    }

    private static JSONObject entryRow(String signalId, String symbol, String side, String askedPrice,
                                       String fillPrice, String quantity, String stopId, String note) {
        return row("entry", symbol)
                .put("signalId", signalId).put("side", side)
                .put("asked", askedPrice).put("fill", fillPrice).put("qty", quantity)
                .put("stopId", stopId == null ? "" : stopId).put("note", note);
    }

    void entryRejected(String signalId, String symbol, String reason) {
        write(row("rejected", symbol).put("signalId", signalId).put("reason", reason));
    }

    /**
     * Filled and unwound in the same breath — a slippage abort or a stop that could not be placed.
     * Two taker fees and whatever the unwind cost; invisible to the record until this row existed.
     */
    void entryAborted(String signalId, String symbol, String side, String fillPrice, String quantity,
                      String outcome, String note) {
        write(row("aborted", symbol)
                .put("signalId", signalId).put("side", side)
                .put("fill", fillPrice).put("qty", quantity)
                .put("outcome", outcome).put("note", note));
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

    /** Same, with what the order itself reported — the price and quantity the verdict needs. */
    void exchangeExit(String symbol, String detail, String orderId, String state, String price,
                      String quantity, boolean whileDown) {
        write(row("exchange-exit", symbol)
                .put("detail", detail)
                .put("orderId", orderId == null ? "" : orderId)
                .put("state", state == null ? "" : state)
                .put("price", price == null ? "" : price)
                .put("qty", quantity == null ? "" : quantity)
                .put("whileDown", whileDown));
    }

    private static double round(double value) {
        return Math.round(value * 10_000.0) / 10_000.0;
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
