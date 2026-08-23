package com.bot.app;

import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.risk.ExposureBook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ledger's whole job: a stop id placed by one process must be readable by the next one,
 * because the venue will not repeat it. Everything else here defends that one property.
 */
class BookLedgerTest {

    @TempDir
    Path dir;

    private static ExposureBook.OpenPosition pos(String symbol, String stopId) {
        return new ExposureBook.OpenPosition(symbol, Side.LONG, new BigDecimal("100"), 2.0,
                200.0, 25.0, Optional.of(stopId));
    }

    private static PositionSnapshot snap(String symbol, String amt, String entry) {
        return new PositionSnapshot(symbol, new BigDecimal(amt), new BigDecimal(entry),
                2, true, BigDecimal.ZERO, new BigDecimal("1.0"));
    }

    @Test
    void stopIdSurvivesARestart() {
        Path file = dir.resolve("ledger.json");
        ExposureBook before = new ExposureBook();
        before.open(pos("AAAUSDT", "bt-s0-alpha"));
        BookLedger.save(before, file, new String[]{""});

        ExposureBook after = new ExposureBook();
        int seeded = BookLedger.seed(after, List.of(snap("AAAUSDT", "100", "2.1")), file);

        assertEquals(1, seeded);
        ExposureBook.OpenPosition p = after.get("AAAUSDT").orElseThrow();
        assertEquals(Optional.of("bt-s0-alpha"), p.protectiveStopId());
        // side, quantity and entry come from the exchange, not the file
        assertEquals(new BigDecimal("100"), p.quantity());
        assertEquals(2.1, p.entryPrice(), 1e-9);
        // risked dollars are the one number the exchange cannot know
        assertEquals(25.0, p.riskUsd(), 1e-9);
    }

    @Test
    void positionClosedWhileAwayIsNotResurrected() {
        Path file = dir.resolve("ledger.json");
        ExposureBook before = new ExposureBook();
        before.open(pos("AAAUSDT", "bt-s0-alpha"));
        before.open(pos("BBBUSDT", "bt-s0-beta"));
        BookLedger.save(before, file, new String[]{""});

        ExposureBook after = new ExposureBook();
        // BBB is gone from the exchange: its stop or take fired while the process was down
        int seeded = BookLedger.seed(after, List.of(snap("AAAUSDT", "100", "2.0")), file);

        assertEquals(1, seeded);
        assertTrue(after.get("BBBUSDT").isEmpty(), "a position the exchange no longer holds must not be seeded");
    }

    @Test
    void exchangePositionUnknownToTheLedgerStaysUnknown() {
        Path file = dir.resolve("ledger.json");
        BookLedger.save(new ExposureBook(), file, new String[]{""});

        ExposureBook after = new ExposureBook();
        int seeded = BookLedger.seed(after, List.of(snap("CCCUSDT", "5", "10.0")), file);

        assertEquals(0, seeded, "a position with no recorded stop id must be left for the reconciler to flag");
        assertTrue(after.all().isEmpty());
    }

    @Test
    void shortSideIsReconstructedFromTheSignOfTheAmount() {
        Path file = dir.resolve("ledger.json");
        ExposureBook before = new ExposureBook();
        before.open(new ExposureBook.OpenPosition("DDDUSDT", Side.SHORT, new BigDecimal("7"), 3.0,
                21.0, 10.0, Optional.of("bt-s0-delta")));
        BookLedger.save(before, file, new String[]{""});

        ExposureBook after = new ExposureBook();
        BookLedger.seed(after, List.of(snap("DDDUSDT", "-7", "3.0")), file);
        assertEquals(Side.SHORT, after.get("DDDUSDT").orElseThrow().side());
    }

    @Test
    void unreadableFileMeansEmptyBookNotACrash() throws Exception {
        Path file = dir.resolve("ledger.json");
        Files.writeString(file, "{this is not json");
        ExposureBook after = new ExposureBook();
        assertEquals(0, BookLedger.seed(after, List.of(snap("AAAUSDT", "1", "1.0")), file));
        assertTrue(after.all().isEmpty());
    }

    // ─── Adoption from the exchange ──────────────────────────────────────────────────────────

    /** Only the two calls {@link BookLedger#adopt} makes; the rest must never be reached. */
    private static class RestingOrders implements com.bot.exec.ExchangePort {
        private final Map<String, List<OrderStatus>> bySymbol;
        private final boolean lists;
        int openOrdersCalls = 0;

        RestingOrders(Map<String, List<OrderStatus>> bySymbol, boolean lists) {
            this.bySymbol = bySymbol;
            this.lists = lists;
        }

        @Override public List<OrderStatus> openOrders(String symbol) {
            openOrdersCalls++;
            return bySymbol.getOrDefault(symbol, List.of());
        }

        @Override public boolean canListConditionalOrders() { return lists; }

        private static UnsupportedOperationException no() {
            return new UnsupportedOperationException("adoption must not reach this");
        }

        @Override public String endpointHost() { throw no(); }
        @Override public long serverTimeMillis() { throw no(); }
        @Override public com.bot.exec.ExchangeSnapshots.AccountSnapshot fetchAccount() { throw no(); }
        @Override public com.bot.core.InstrumentFilters fetchFilters(String s) { throw no(); }
        @Override public com.bot.risk.MarginTierTable fetchMarginTiers(String s) { throw no(); }
        @Override public double fetchRealizedPnlSince(long since) { throw no(); }
        @Override public void ensureIsolatedMargin(String s) { throw no(); }
        @Override public void setLeverage(String s, int l) { throw no(); }
        @Override public OrderStatus placeOrder(com.bot.exec.OrderRequest r) { throw no(); }
        @Override public Optional<OrderStatus> queryOrder(String s, String id) { throw no(); }
        @Override public List<PositionSnapshot> openPositions() { throw no(); }
        @Override public void cancelOrder(String s, String id) { throw no(); }
        @Override public void cancelAllOpenOrders(String s) { throw no(); }
        @Override public void armDeadMansSwitch(String s, long ms) { throw no(); }
        @Override public void close() { }
    }

    private static OrderStatus stop(String symbol, String id, String trigger) {
        // closePosition stops carry quantity 0, exactly as Binance returns them
        return new OrderStatus(id, 1L, symbol, OrderState.NEW, OrderType.STOP_MARKET,
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, new BigDecimal(trigger),
                true, true, 1L);
    }

    private static OrderStatus take(String symbol, String id, String trigger) {
        return new OrderStatus(id, 2L, symbol, OrderState.NEW, OrderType.TAKE_PROFIT_MARKET,
                new BigDecimal("37"), BigDecimal.ZERO, BigDecimal.ZERO, new BigDecimal(trigger),
                true, false, 1L);
    }

    @Test
    void aLostLedgerIsRebuiltFromTheStopsRestingOnTheExchange() {
        // The Railway incident of 21.08: a fresh container, no file, four protected positions,
        // and every one of them reported UNKNOWN_POSITION.
        RestingOrders exchange = new RestingOrders(Map.of(
                "ADAUSDT", List.of(take("ADAUSDT", "bt-t0-take", "0.2369"),
                        stop("ADAUSDT", "bt-s0-real", "0.188"))), true);

        ExposureBook book = new ExposureBook();
        int adopted = BookLedger.adopt(book, exchange, List.of(snap("ADAUSDT", "37", "0.2058")));

        assertEquals(1, adopted);
        ExposureBook.OpenPosition p = book.get("ADAUSDT").orElseThrow();
        assertEquals(Optional.of("bt-s0-real"), p.protectiveStopId(), "the take must not be mistaken for the stop");
        assertEquals(Side.LONG, p.side());
        // risk is the stop's own distance: |0.2058 - 0.188| * 37
        assertEquals(0.6586, p.riskUsd(), 1e-4);
    }

    @Test
    void aPositionWithNoRestingStopIsLeftForTheReconcilerToHalt() {
        RestingOrders exchange = new RestingOrders(Map.of(
                "ADAUSDT", List.of(take("ADAUSDT", "bt-t0-take", "0.2369"))), true);

        ExposureBook book = new ExposureBook();
        assertEquals(0, BookLedger.adopt(book, exchange, List.of(snap("ADAUSDT", "37", "0.2058"))));
        assertTrue(book.all().isEmpty(), "adoption must never invent protection that is not there");
    }

    @Test
    void aVenueThatCannotListConditionalOrdersIsNotAsked() {
        RestingOrders exchange = new RestingOrders(Map.of(), false);
        assertEquals(0, BookLedger.adopt(new ExposureBook(), exchange,
                List.of(snap("ADAUSDT", "37", "0.2058"))));
        assertEquals(0, exchange.openOrdersCalls, "demo-fapi answers such a listing with a 404");
    }

    @Test
    void adoptionNeverOverwritesWhatTheLedgerAlreadySeeded() {
        RestingOrders exchange = new RestingOrders(Map.of(
                "AAAUSDT", List.of(stop("AAAUSDT", "bt-s0-stale", "1.5"))), true);

        ExposureBook book = new ExposureBook();
        book.open(pos("AAAUSDT", "bt-s0-alpha"));
        assertEquals(0, BookLedger.adopt(book, exchange, List.of(snap("AAAUSDT", "100", "2.0"))));
        assertEquals(Optional.of("bt-s0-alpha"), book.get("AAAUSDT").orElseThrow().protectiveStopId());
    }

    @Test
    void aNamelessEntryIsGivenItsStopId() {
        // A reconcile pass after a failed boot read leaves the position in the book with no stop id;
        // late adoption must fill it in rather than skip the symbol as "already known".
        RestingOrders exchange = new RestingOrders(Map.of(
                "ADAUSDT", List.of(stop("ADAUSDT", "bt-s0-late", "0.188"))), true);
        ExposureBook book = new ExposureBook();
        book.open(new ExposureBook.OpenPosition("ADAUSDT", Side.LONG, new BigDecimal("37"), 0.2058,
                7.6, 0.0, Optional.empty()));

        assertEquals(1, BookLedger.adopt(book, exchange, List.of(snap("ADAUSDT", "37", "0.2058"))));
        assertEquals(Optional.of("bt-s0-late"), book.get("ADAUSDT").orElseThrow().protectiveStopId());
        assertEquals(1, book.openCount());
    }

    @Test
    void aFlatPositionIsNotAdopted() {
        RestingOrders exchange = new RestingOrders(Map.of(
                "AAAUSDT", List.of(stop("AAAUSDT", "bt-s0-orphan", "1.5"))), true);
        assertEquals(0, BookLedger.adopt(new ExposureBook(), exchange,
                List.of(snap("AAAUSDT", "0", "2.0"))));
    }

    @Test
    void anUnreadableSymbolDoesNotStopTheRest() {
        RestingOrders exchange = new RestingOrders(Map.of(
                "BBBUSDT", List.of(stop("BBBUSDT", "bt-s0-beta", "1.8"))), true) {
            @Override public List<OrderStatus> openOrders(String symbol) {
                if (symbol.equals("AAAUSDT")) throw new IllegalStateException("HTTP 503");
                return super.openOrders(symbol);
            }
        };

        ExposureBook book = new ExposureBook();
        int adopted = BookLedger.adopt(book, exchange,
                List.of(snap("AAAUSDT", "10", "2.0"), snap("BBBUSDT", "10", "2.0")));

        assertEquals(1, adopted);
        assertTrue(book.get("AAAUSDT").isEmpty());
        assertTrue(book.get("BBBUSDT").isPresent());
    }

    @Test
    void shortsRiskTheDistanceUpToTheirStop() {
        RestingOrders exchange = new RestingOrders(Map.of(
                "DDDUSDT", List.of(stop("DDDUSDT", "bt-s0-delta", "3.5"))), true);

        ExposureBook book = new ExposureBook();
        BookLedger.adopt(book, exchange, List.of(snap("DDDUSDT", "-7", "3.0")));
        ExposureBook.OpenPosition p = book.get("DDDUSDT").orElseThrow();
        assertEquals(Side.SHORT, p.side());
        assertEquals(3.5, p.riskUsd(), 1e-9);   // |3.0 - 3.5| * 7
    }

    @Test
    void unchangedBookIsNotRewritten() throws Exception {
        Path file = dir.resolve("ledger.json");
        ExposureBook book = new ExposureBook();
        book.open(pos("AAAUSDT", "bt-s0-alpha"));
        String[] last = {""};
        BookLedger.save(book, file, last);
        var firstWrite = Files.getLastModifiedTime(file);
        assertFalse(last[0].isEmpty());

        BookLedger.save(book, file, last);   // same content: must not touch the file
        assertEquals(firstWrite, Files.getLastModifiedTime(file));
    }
}
