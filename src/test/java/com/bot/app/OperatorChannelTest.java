package com.bot.app;

import com.bot.exec.TradingHalt;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The operator's three commands, driven without a network. The properties that matter: only the
 * configured chat is obeyed, /resume is the one thing that clears a halt, and nothing here can
 * open a position.
 */
class OperatorChannelTest {

    private static final Instant NOON = Instant.parse("2026-08-22T12:00:00Z");

    private static final class FakeTelegram implements OperatorChannel.Transport {
        final List<String> sent = new ArrayList<>();
        @Override public String poll(long offset) { return "{\"ok\":true,\"result\":[]}"; }
        @Override public void send(String text) { sent.add(text); }
    }

    private static String update(long id, String chat, String text) {
        return "{\"ok\":true,\"result\":[{\"update_id\":" + id + ",\"message\":{\"chat\":{\"id\":"
                + chat + "},\"text\":\"" + text + "\"}}]}";
    }

    @Test
    @DisplayName("/resume is the only way out of a halt, and it names what it cleared")
    void resumeClearsAHalt() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("reconciliation drift: UNKNOWN_POSITION on XUSDT", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = new OperatorChannel(tg, "42", halt);

        ops.handleUpdates(update(1, "42", "/resume"));

        assertFalse(halt.isHalted());
        assertEquals(1, tg.sent.size());
        assertTrue(tg.sent.get(0).contains("UNKNOWN_POSITION on XUSDT"), tg.sent.get(0));
    }

    @Test
    @DisplayName("a stranger's /resume is ignored and not even answered")
    void strangersAreIgnored() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("something", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = new OperatorChannel(tg, "42", halt);

        ops.handleUpdates(update(1, "999", "/resume"));

        assertTrue(halt.isHalted(), "only the operator's chat may clear a halt");
        assertTrue(tg.sent.isEmpty(), "no reply: a reply would confirm the bot exists");
    }

    @Test
    @DisplayName("/halt latches and /status reports what the loop last published")
    void haltAndStatus() throws Exception {
        TradingHalt halt = new TradingHalt();
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = new OperatorChannel(tg, "42", halt);
        ops.publishStatus("REAL TRADE | 3 positions | halt: none", NOON);

        ops.handleUpdates(update(1, "42", "/halt"));
        assertTrue(halt.isHalted());
        assertEquals("operator via Telegram", halt.reason().orElseThrow());

        ops.handleUpdates(update(2, "42", "/status@SomeBot"));
        assertTrue(tg.sent.get(1).startsWith("REAL TRADE | 3 positions"), tg.sent.get(1));

        ops.handleUpdates(update(3, "42", "/halt"));
        assertTrue(tg.sent.get(2).startsWith("already halted"), tg.sent.get(2));
    }

    @Test
    @DisplayName("non-commands and unknown commands change nothing")
    void chatterIsHarmless() throws Exception {
        TradingHalt halt = new TradingHalt();
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = new OperatorChannel(tg, "42", halt);

        ops.handleUpdates(update(1, "42", "hello bot"));
        ops.handleUpdates(update(2, "42", "/open BTCUSDT"));

        assertFalse(halt.isHalted());
        assertEquals(1, tg.sent.size());
        assertTrue(tg.sent.get(0).startsWith("unknown command"));
    }

    @Test
    @DisplayName("/resume does not lift REAL_MODE=observe: a mode is not an incident")
    void observeIsNotClearable() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("REAL_MODE=observe - reading the account, opening nothing", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = new OperatorChannel(tg, "42", halt);

        ops.handleUpdates(update(1, "42", "/resume"));

        assertTrue(halt.isHalted(), "observe must survive /resume; only the environment ends it");
        assertTrue(tg.sent.get(0).contains("REAL_MODE=trade"), tg.sent.get(0));
    }

    @Test
    @DisplayName("a command Telegram replays from before this process started is ignored")
    void staleCommandsAreIgnored() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("boot drift", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = new OperatorChannel(tg, "42", halt);
        ops.markStarted(NOON);

        long beforeBoot = NOON.getEpochSecond() - 120;
        ops.handleUpdates("{\"ok\":true,\"result\":[{\"update_id\":7,\"message\":{\"date\":" + beforeBoot
                + ",\"chat\":{\"id\":42},\"text\":\"/resume\"}}]}");

        assertTrue(halt.isHalted(), "a /resume typed while the bot was down must not clear the new boot's halt");
        assertTrue(tg.sent.isEmpty());

        long afterBoot = NOON.getEpochSecond() + 10;
        ops.handleUpdates("{\"ok\":true,\"result\":[{\"update_id\":8,\"message\":{\"date\":" + afterBoot
                + ",\"chat\":{\"id\":42},\"text\":\"/resume\"}}]}");
        assertFalse(halt.isHalted(), "a live command still works");
    }

    @Test
    @DisplayName("replies name the venue, because demo and real share one chat")
    void repliesCarryTheVenue() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = new OperatorChannel(tg, "42", new TradingHalt()).withVenueTag("REAL TRADE");
        ops.handleUpdates(update(1, "42", "/help"));
        assertTrue(tg.sent.get(0).startsWith("[REAL TRADE] "), tg.sent.get(0));
    }

    @Test
    @DisplayName("/close queues a reduce-only close for the loop; garbage symbols are refused")
    void closeQueuesForTheLoop() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = new OperatorChannel(tg, "42", new TradingHalt());

        ops.handleUpdates(update(1, "42", "/close adausdt"));
        var queued = ops.drainCloses();
        assertEquals(1, queued.size());
        assertEquals("ADAUSDT", queued.get(0).symbol());
        assertEquals("operator via Telegram", queued.get(0).reason());
        assertTrue(tg.sent.get(0).startsWith("queued: closing ADAUSDT"), tg.sent.get(0));
        assertTrue(ops.drainCloses().isEmpty(), "drained once, gone");

        ops.handleUpdates(update(2, "42", "/close"));
        ops.handleUpdates(update(3, "42", "/close $$$"));
        assertTrue(ops.drainCloses().isEmpty(), "garbage must queue nothing");
        assertTrue(tg.sent.get(1).startsWith("usage:"), tg.sent.get(1));
    }

    @Test
    @DisplayName("the channel only exists when Telegram is configured")
    void absentWithoutCredentials() {
        assertEquals(null, OperatorChannel.fromEnvironmentOrNull(k -> null, new TradingHalt()));
        assertTrue(OperatorChannel.fromEnvironmentOrNull(
                k -> k.equals("TELEGRAM_BOT_TOKEN") ? "t" : "42", new TradingHalt()) != null);
    }
}
