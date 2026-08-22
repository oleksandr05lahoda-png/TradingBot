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
    @DisplayName("the channel only exists when Telegram is configured")
    void absentWithoutCredentials() {
        assertEquals(null, OperatorChannel.fromEnvironmentOrNull(k -> null, new TradingHalt()));
        assertTrue(OperatorChannel.fromEnvironmentOrNull(
                k -> k.equals("TELEGRAM_BOT_TOKEN") ? "t" : "42", new TradingHalt()) != null);
    }
}
