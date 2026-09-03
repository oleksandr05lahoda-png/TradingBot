package com.bot.exec;

import com.bot.risk.RiskEngine;
import com.bot.risk.TradePlan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The plan on disk before the send: recorded, spent, reloaded by the next process, expired. */
class EntryIntentsTest {

    @TempDir Path dir;

    private final FakeExchange exchange = new FakeExchange();
    private final RiskEngine engine = ExecFixtures.engine();

    private TradePlan plan() {
        return ExecFixtures.approvedPlan(engine, exchange.fetchFilters("BTCUSDT"));
    }

    @Test
    @DisplayName("recorded before the send, gone once the stop rests")
    void recordThenClear() {
        EntryIntents intents = EntryIntents.inMemory();
        long now = ExecFixtures.NOON.toEpochMilli();
        intents.record(plan(), now);
        EntryIntents.Intent intent = intents.get("BTCUSDT", now).orElseThrow();
        assertEquals(plan().stopPrice(), intent.stopPrice());
        assertEquals(plan().quantity(), intent.quantity());

        intents.clear("BTCUSDT");
        assertTrue(intents.get("BTCUSDT", now).isEmpty());
    }

    @Test
    @DisplayName("a file-backed intent survives into the next process")
    void persistsAcrossProcesses() {
        Path file = dir.resolve("entry-intents.json");
        long now = ExecFixtures.NOON.toEpochMilli();
        EntryIntents first = EntryIntents.at(file, now);
        first.record(plan(), now);
        assertTrue(Files.exists(file));

        EntryIntents next = EntryIntents.at(file, now + 60_000L);
        assertTrue(next.get("BTCUSDT", now + 60_000L).isPresent(), "the next process sees the intent");

        next.clear("BTCUSDT");
        EntryIntents third = EntryIntents.at(file, now + 120_000L);
        assertTrue(third.get("BTCUSDT", now + 120_000L).isEmpty(), "and its clearing");
    }

    @Test
    @DisplayName("an intent older than a day is no evidence about anything still open")
    void expiresAfterADay() {
        Path file = dir.resolve("entry-intents.json");
        long now = ExecFixtures.NOON.toEpochMilli();
        EntryIntents first = EntryIntents.at(file, now);
        first.record(plan(), now);

        long muchLater = now + EntryIntents.MAX_AGE_MS + 1;
        assertTrue(first.get("BTCUSDT", muchLater).isEmpty(), "expired in memory");
        EntryIntents reloaded = EntryIntents.at(file, muchLater);
        assertTrue(reloaded.all().isEmpty(), "and pruned on load");
    }

    @Test
    @DisplayName("an unreadable file starts empty instead of killing the boot")
    void unreadableFileStartsEmpty() throws Exception {
        Path file = dir.resolve("entry-intents.json");
        Files.writeString(file, "{not json");
        EntryIntents intents = EntryIntents.at(file, ExecFixtures.NOON.toEpochMilli());
        assertTrue(intents.all().isEmpty());
        assertFalse(intents.get("BTCUSDT", ExecFixtures.NOON.toEpochMilli()).isPresent());
    }
}
