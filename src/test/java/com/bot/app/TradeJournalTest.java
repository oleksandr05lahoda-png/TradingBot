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
    void openMarksFollowEntriesAndExitsAndSurviveARestart() throws Exception {
        Path file = dir.resolve("trades.jsonl");
        TradeJournal journal = new TradeJournal(file);
        journal.entryOpened("s1", "ADAUSDT", "LONG", "1.00", "1.001", "7", "bt-s0-a", "0.95",
                List.of("", "1.10"), 0.35, 3, 7.0, "FILLED");
        journal.entryOpened("s2", "XRPUSDT", "SHORT", "0.50", "0.50", "10", "bt-s0-x", "0.52",
                List.of("0.46"), 0.20, 3, 5.0, "FILLED");
        Files.writeString(file, "{\"kind\":\"entry\",\"symbol\":\"TORN", java.nio.file.StandardOpenOption.APPEND);
        Files.writeString(file, System.lineSeparator(), java.nio.file.StandardOpenOption.APPEND);

        java.util.Map<String, TradeJournal.OpenMark> marks = journal.openMarks();
        assertEquals(2, marks.size(), "a torn line is skipped, not fatal");
        assertEquals("LONG", marks.get("ADAUSDT").side());
        assertEquals(0.95, marks.get("ADAUSDT").stopPrice(), 1e-12);
        assertEquals(1.10, marks.get("ADAUSDT").takePrice(), 1e-12, "the first take that names a price");
        assertTrue(marks.get("ADAUSDT").openedAt() != null);

        journal.exchangeExit("XRPUSDT", "partial-exit", "a take leg", "", "", "");
        assertTrue(journal.openMarks().containsKey("XRPUSDT"), "a shrink leaves the trade open");
        journal.exchangeExit("XRPUSDT", "stop-loss", "its stop filled", "o1", "0.52", "10");
        journal.closed("tg-close-ADAUSDT-1", "ADAUSDT", "operator via Telegram", "7", "1.05", "flat");
        assertTrue(journal.openMarks().isEmpty(), "live writes keep the view current");

        journal.entryOpened("s3", "SOLUSDT", "LONG", "150", "150", "1", "bt-s0-s", "140",
                List.of("170"), 0.5, 3, 150.0, "FILLED");
        assertEquals(java.util.Set.of("SOLUSDT"), new TradeJournal(file).openMarks().keySet(),
                "a new process rebuilds the same view from the file");
    }

    @Test
    void anUnwritablePathIsALogLineNotACrash() {
        TradeJournal journal = new TradeJournal(dir);   // a directory cannot be appended to
        journal.exchangeExit("XUSDT", "anything");      // must not throw
        journal.exchangeExit("XUSDT", "again");         // and must not spam either
    }
}
