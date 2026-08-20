package com.bot.app;

import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.risk.ExposureBook;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Persists the exposure book across restarts, because the venue cannot: {@code demo-fapi} does not
 * list conditional orders, so a protective stop's id lives only in the book of the process that
 * placed it, and without this snapshot every restart with open positions ended in a halt and a
 * forced flatten. Seeding takes side/quantity/entry from the exchange and borrows from the file only
 * what the exchange cannot know (stop id, risked dollars); a symbol on the exchange but absent from
 * the file still surfaces as UNKNOWN_POSITION and halts opening.
 */
final class BookLedger {

    private static final Logger LOG = Logger.getLogger(BookLedger.class.getName());

    private BookLedger() {}

    /** Writes atomically; skips the write when nothing changed since the last call. */
    static void save(ExposureBook book, Path path, String[] lastWritten) {
        JSONArray positions = new JSONArray();
        for (ExposureBook.OpenPosition p : book.all()) {
            JSONObject row = new JSONObject();
            row.put("symbol", p.symbol());
            row.put("side", p.side().name());
            row.put("riskUsd", p.riskUsd());
            p.protectiveStopId().ifPresent(id -> row.put("stopId", id));
            positions.put(row);
        }
        String body = new JSONObject().put("positions", positions).toString();
        if (body.equals(lastWritten[0])) return;
        try {
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, body, StandardCharsets.UTF_8);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            lastWritten[0] = body;
        } catch (IOException e) {
            // A failed snapshot must not disturb trading; the cost is one more flatten-on-restart.
            LOG.warning("[Ledger] could not persist the book to " + path + ": " + e.getMessage());
        }
    }

    /** Seeds an empty book from the ledger file plus live positions; returns how many got a stop id. */
    static int seed(ExposureBook book, List<PositionSnapshot> exchange, Path path) {
        if (!Files.exists(path)) return 0;
        JSONObject root;
        try {
            root = new JSONObject(Files.readString(path, StandardCharsets.UTF_8));
        } catch (Exception e) {
            LOG.warning("[Ledger] " + path + " is unreadable (" + e.getMessage()
                    + ") — starting with an empty book, as before this class existed");
            return 0;
        }
        JSONArray rows = root.optJSONArray("positions");
        if (rows == null) return 0;

        int seeded = 0;
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.getJSONObject(i);
            String symbol = row.optString("symbol", "");
            String stopId = row.optString("stopId", "");
            double riskUsd = row.optDouble("riskUsd", 0.0);
            if (symbol.isBlank() || stopId.isBlank()) continue;

            PositionSnapshot live = exchange.stream()
                    .filter(p -> p.symbol().equals(symbol) && p.signedQuantity().signum() != 0)
                    .findFirst().orElse(null);
            if (live == null) continue;    // closed while we were away; the stop died with it

            BigDecimal quantity = live.signedQuantity().abs();
            double entry = live.entryPrice().doubleValue();
            Side side = live.signedQuantity().signum() > 0 ? Side.LONG : Side.SHORT;
            book.open(new ExposureBook.OpenPosition(symbol, side, quantity, entry,
                    quantity.doubleValue() * entry, Math.max(0.0, riskUsd), Optional.of(stopId)));
            seeded++;
        }
        return seeded;
    }
}
