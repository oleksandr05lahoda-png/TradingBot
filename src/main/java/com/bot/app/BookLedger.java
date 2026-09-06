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
            // riskUsd is a dollar figure for THIS size: a position trimmed while the process is
            // down (a hand close, a take leg) would otherwise inherit the full figure and the
            // missing-stop repair would place its stop at an inflated distance (audit 06.09).
            row.put("quantity", p.quantity().toPlainString());
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
            // A null or string element is a hand edit or a half-repaired file, not a reason to
            // fail the boot with "could not read the exchange" (audit 06.09).
            JSONObject row = rows.optJSONObject(i);
            if (row == null) {
                LOG.warning("[Ledger] " + path + " positions[" + i + "] is not an object - skipped");
                continue;
            }
            String symbol = row.optString("symbol", "");
            String stopId = row.optString("stopId", "");
            double riskUsd = row.optDouble("riskUsd", 0.0);
            if (symbol.isBlank() || stopId.isBlank()) continue;
            // Seeding must be idempotent: the boot retry loop runs it again after a transient
            // failure further down, and ExposureBook.open refuses a symbol already booked.
            if (book.hasPosition(symbol)) continue;

            PositionSnapshot live = exchange.stream()
                    .filter(p -> p.symbol().equals(symbol) && p.signedQuantity().signum() != 0)
                    .findFirst().orElse(null);
            if (live == null) continue;    // closed while we were away; the stop died with it

            BigDecimal quantity = live.signedQuantity().abs();
            double entry = live.entryPrice().doubleValue();
            Side side = live.signedQuantity().signum() > 0 ? Side.LONG : Side.SHORT;
            String savedSide = row.optString("side", "");
            if (!savedSide.isBlank() && !savedSide.equals(side.name())) {
                // The recorded position is gone and a hand-opened one in the other direction
                // took its symbol: its stop id and risk figure belong to a trade that no longer
                // exists. Left for adopt() or UNKNOWN_POSITION rather than booked as ours.
                LOG.warning("[Ledger] " + symbol + " is " + side + " on the exchange but " + savedSide
                        + " in the ledger - the recorded stop belongs to a position that no longer "
                        + "exists; not seeded");
                continue;
            }
            // Risk is a dollar figure for the size that was persisted; scale it to the size that
            // is live so a partial exit while down does not inflate the implied stop distance.
            BigDecimal savedQty = row.has("quantity") ? new BigDecimal(row.getString("quantity")) : null;
            double risk = savedQty != null && savedQty.signum() > 0
                    ? riskUsd * quantity.doubleValue() / savedQty.doubleValue()
                    : riskUsd;
            book.open(new ExposureBook.OpenPosition(symbol, side, quantity, entry,
                    quantity.doubleValue() * entry, Math.max(0.0, risk), Optional.of(stopId)));
            seeded++;
        }
        return seeded;
    }

    /** A ledger row whose symbol the exchange no longer holds: its exit happened while we were away. */
    record ClosedWhileAway(String symbol, String side, String stopId) {}

    /**
     * Rows the last process persisted for positions that are flat now. {@link #seed} skips them
     * silently, so a stop or take that filled while the process was down produced no journal row
     * at all and the forward record lost the exit (audit 03.09). Read-only; the caller journals.
     */
    static List<ClosedWhileAway> closedWhileAway(List<PositionSnapshot> exchange, Path path) {
        List<ClosedWhileAway> out = new java.util.ArrayList<>();
        if (!Files.exists(path)) return out;
        JSONArray rows;
        try {
            rows = new JSONObject(Files.readString(path, StandardCharsets.UTF_8)).optJSONArray("positions");
        } catch (Exception e) {
            return out;
        }
        if (rows == null) return out;
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.optJSONObject(i);
            if (row == null) continue;    // seed() already warned about it
            String symbol = row.optString("symbol", "");
            if (symbol.isBlank()) continue;
            String side = row.optString("side", "");
            // A live position on the OTHER side is not the recorded one still open: the recorded
            // one exited (its stop-out must be journaled) and a hand trade took the symbol.
            boolean stillOpen = exchange.stream()
                    .anyMatch(p -> p.symbol().equals(symbol) && p.signedQuantity().signum() != 0
                            && (side.isBlank() || side.equals(
                                    p.signedQuantity().signum() > 0 ? Side.LONG.name() : Side.SHORT.name())));
            if (stillOpen) continue;
            out.add(new ClosedWhileAway(symbol, row.optString("side", ""), row.optString("stopId", "")));
        }
        return out;
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
