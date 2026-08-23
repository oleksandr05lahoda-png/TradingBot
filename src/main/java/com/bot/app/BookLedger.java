package com.bot.app;

import com.bot.core.Side;
import com.bot.exec.ExchangePort;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.exec.OrderTypes.OrderType;
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
 * what the exchange cannot know (stop id, risked dollars). Where the venue does list them — the real
 * one does — {@link #adopt} reads the stop back off the exchange, and the file is a convenience
 * rather than the only record. A position neither seeded nor adopted still surfaces as
 * UNKNOWN_POSITION and halts opening.
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

    /**
     * Restores what the file could not. Where the venue lists conditional orders the resting stop
     * names itself, so a lost ledger — a fresh container, an ephemeral disk — no longer costs a halt.
     * Risk comes from the stop's own distance, which is what was actually risked. A position with no
     * working stop is left alone on purpose: UNKNOWN_POSITION is the correct answer for it.
     */
    static int adopt(ExposureBook book, ExchangePort port, List<PositionSnapshot> exchange) {
        if (!port.canListConditionalOrders()) return 0;
        int adopted = 0;
        for (PositionSnapshot position : exchange) {
            if (position.isFlat()) continue;
            ExposureBook.OpenPosition existing = book.get(position.symbol()).orElse(null);
            if (existing != null && existing.protectiveStopId().isPresent()) continue;
            Optional<OrderStatus> stop;
            try {
                stop = port.openOrders(position.symbol()).stream()
                        .filter(OrderStatus::isWorking)
                        .filter(o -> o.type() == OrderType.STOP_MARKET && (o.reduceOnly() || o.closePosition()))
                        .filter(o -> o.stopPrice() != null && o.stopPrice().signum() > 0)
                        .findFirst();
            } catch (RuntimeException e) {
                LOG.warning("[Ledger] could not read resting orders for " + position.symbol()
                        + ": " + e.getMessage());
                continue;
            }
            if (stop.isEmpty()) continue;

            BigDecimal quantity = position.absoluteQuantity();
            double entry = position.entryPrice().doubleValue();
            // closePosition stops carry quantity 0, so the size comes from the position itself.
            double risk = stop.get().stopPrice().subtract(position.entryPrice()).abs().doubleValue()
                    * quantity.doubleValue();
            // A realigned-but-nameless entry (a reconcile pass after a failed boot read) is replaced.
            if (existing != null) book.close(position.symbol());
            book.open(new ExposureBook.OpenPosition(position.symbol(), position.direction().orElseThrow(),
                    quantity, entry, quantity.doubleValue() * entry, risk,
                    Optional.of(stop.get().clientOrderId())));
            adopted++;
        }
        return adopted;
    }
}
