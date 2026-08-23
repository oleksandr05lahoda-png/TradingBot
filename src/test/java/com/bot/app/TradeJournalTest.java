package com.bot.app;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The journal's contract: every event is one parseable JSONL row, and a bad path never throws. */
class TradeJournalTest {

    @TempDir
    Path dir;

    @Test
    void everyEventKindIsOneParseableRow() throws Exception {
        Path file = dir.resolve("trades.jsonl");
        TradeJournal journal = new TradeJournal(file);

        journal.entryOpened("auto-trend-X-1", "XUSDT", "LONG", "1.00", "1.001", "7", "bt-s0-x", "FILLED");
        journal.entryRejected("auto-trend-Y-1", "YUSDT", "BELOW_MIN_NOTIONAL: $3 < $5");
        journal.closed("auto-close-X-1", "XUSDT", "trend-exited", "7", "1.05", "flat");
        journal.exchangeExit("ZUSDT", "its protective stop bt-s0-z filled");

        List<String> lines = Files.readAllLines(file);
        assertEquals(4, lines.size());
        assertEquals("entry", new JSONObject(lines.get(0)).getString("kind"));
        assertEquals("rejected", new JSONObject(lines.get(1)).getString("kind"));
        assertEquals("close", new JSONObject(lines.get(2)).getString("kind"));
        JSONObject exit = new JSONObject(lines.get(3));
        assertEquals("exchange-exit", exit.getString("kind"));
        assertTrue(exit.getString("ts").startsWith("20"), "rows carry a timestamp");
    }

    @Test
    void anUnwritablePathIsALogLineNotACrash() {
        TradeJournal journal = new TradeJournal(dir);   // a directory cannot be appended to
        journal.exchangeExit("XUSDT", "anything");      // must not throw
        journal.exchangeExit("XUSDT", "again");         // and must not spam either
    }
}
