package com.bot.app;

import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.risk.ExposureBook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
