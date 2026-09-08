package com.bot.app;

import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.risk.ExposureBook;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The exact ledger the live VPS held on 08.09, copied verbatim, so no later change to the
 * exit-naming path can quietly cost the machine its stop ids at boot. A ledger the bot cannot read
 * is a production incident: every open position reconciles as unknown and it boots halted with real
 * money out. The synthetic fixtures elsewhere in BookLedgerTest do not prove this - only the real
 * file's own key set does.
 */
class LiveLedgerShapeTest {

    @TempDir
    Path dir;

    /** /opt/tradingbot-data/book-ledger-real.json, 08.09, eight positions open (two kept here). */
    private static final String LIVE = "{\"positions\":["
            + "{\"symbol\":\"SYRUPUSDT\",\"side\":\"LONG\",\"quantity\":\"22\","
            + "\"stopId\":\"bt-s0-niXIcw8QrWEdk7CqbQIxfA\",\"riskUsd\":0.6809000000000001},"
            + "{\"symbol\":\"ICPUSDT\",\"side\":\"LONG\",\"quantity\":\"2\","
            + "\"stopId\":\"bt-s0-416RmbvdrL7V_SaEKfU1D8\",\"riskUsd\":0.7240000000000002}"
            + "]}";

    private static PositionSnapshot snap(String symbol, String amt, String entry) {
        return new PositionSnapshot(symbol, new BigDecimal(amt), new BigDecimal(entry),
                2, true, BigDecimal.ZERO, new BigDecimal("1.0"));
    }

    @Test
    @DisplayName("the ledger the live machine holds today still seeds its stop ids and risk figures")
    void liveLedgerStillLoads() throws Exception {
        Path file = dir.resolve("book-ledger-real.json");
        Files.write(file, LIVE.getBytes(StandardCharsets.UTF_8));

        ExposureBook book = new ExposureBook();
        int seeded = BookLedger.seed(book, List.of(
                snap("SYRUPUSDT", "22", "0.2276"), snap("ICPUSDT", "2", "3.03")), file);

        assertEquals(2, seeded, "both rows must come back");
        ExposureBook.OpenPosition syrup = book.get("SYRUPUSDT").orElseThrow();
        assertEquals(Side.LONG, syrup.side());
        assertEquals(Optional.of("bt-s0-niXIcw8QrWEdk7CqbQIxfA"), syrup.protectiveStopId(),
                "without the stop id the reconciler cannot tell its own stop from a stranger's");
        assertTrue(syrup.riskUsd() > 0, "a zero risk figure would let the kill switch under-count");
        assertTrue(book.get("ICPUSDT").isPresent());
    }
}
