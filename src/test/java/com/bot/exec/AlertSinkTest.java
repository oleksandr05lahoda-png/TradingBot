package com.bot.exec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The alert path, when nobody configured it. An unset TELEGRAM_BOT_TOKEN used to disable pushes in
 * silence, which is indistinguishable from a working channel that simply has nothing to say — so a
 * halt would sit in a log file on one machine and the operator would never learn of it. The point of
 * these tests is that the absence announces itself, exactly once, and still does not stop the bot.
 */
class AlertSinkTest {

    private final List<LogRecord> logged = new ArrayList<>();
    private final Logger telegramLog = Logger.getLogger(AlertSink.Telegram.LOGGER_NAME);
    private final Handler capture = new Handler() {
        @Override public void publish(LogRecord record) { logged.add(record); }
        @Override public void flush() { }
        @Override public void close() { }
    };

    private boolean parentHandlersWere;

    @BeforeEach
    void listenToTheAlertLogger() {
        parentHandlersWere = telegramLog.getUseParentHandlers();
        // Captured, not echoed: the expected warning is not a test failure to read on the console.
        telegramLog.setUseParentHandlers(false);
        telegramLog.addHandler(capture);
    }

    @AfterEach
    void stopListening() {
        telegramLog.removeHandler(capture);
        telegramLog.setUseParentHandlers(parentHandlersWere);
    }

    /** An environment holding exactly the given variables and nothing else. */
    private static AlertSink.Telegram build(Map<String, String> environment, AtomicBoolean warned) {
        return AlertSink.Telegram.fromEnvironmentOrNull(environment::get, warned);
    }

    private String onlyWarning() {
        assertEquals(1, logged.size(), "expected exactly one log record, got " + logged);
        assertEquals(Level.WARNING, logged.get(0).getLevel());
        return logged.get(0).getMessage();
    }

    @Test
    @DisplayName("an unconfigured Telegram says so by name, and says what it costs")
    void unconfiguredIsAnnounced() {
        assertNull(build(Map.of(), new AtomicBoolean()));

        String warning = onlyWarning();
        assertTrue(warning.contains(AlertSink.Telegram.TOKEN_VAR), warning);
        assertTrue(warning.contains(AlertSink.Telegram.CHAT_ID_VAR), warning);
        // The consequence, not just the fact: "OFF" alone reads as a setting, not as a blind spot.
        assertTrue(warning.contains("CRITICAL"), warning);
    }

    @Test
    @DisplayName("the warning is logged once, not on every call")
    void theWarningIsNotRepeated() {
        AtomicBoolean warned = new AtomicBoolean();

        // Today one caller asks once at startup. The latch is what keeps that true if the call ever
        // moves into the poll loop, where a line per pass would bury the log it is trying to protect.
        for (int i = 0; i < 5; i++) {
            assertNull(build(Map.of(), warned));
        }

        assertEquals(1, logged.size(), "expected one warning across five calls, got " + logged);
    }

    @Test
    @DisplayName("a half-configured Telegram names only the variable that is actually missing")
    void halfConfiguredNamesTheMissingOne() {
        assertNull(build(Map.of(AlertSink.Telegram.TOKEN_VAR, "not-a-real-token"), new AtomicBoolean()));

        String warning = onlyWarning();
        assertTrue(warning.contains(AlertSink.Telegram.CHAT_ID_VAR), warning);
        assertFalse(warning.contains(AlertSink.Telegram.TOKEN_VAR), warning);
    }

    @Test
    @DisplayName("a blank value counts as unset, not as a configured empty chat")
    void blankCountsAsUnset() {
        assertNull(build(Map.of(
                AlertSink.Telegram.TOKEN_VAR, "not-a-real-token",
                AlertSink.Telegram.CHAT_ID_VAR, "   "), new AtomicBoolean()));

        assertTrue(onlyWarning().contains(AlertSink.Telegram.CHAT_ID_VAR));
    }

    @Test
    @DisplayName("a configured environment builds the sink and stays quiet")
    void configuredSaysNothing() {
        AlertSink.Telegram telegram = build(Map.of(
                AlertSink.Telegram.TOKEN_VAR, "not-a-real-token",
                AlertSink.Telegram.CHAT_ID_VAR, "12345"), new AtomicBoolean());

        assertNotNull(telegram);
        assertTrue(logged.isEmpty(), "a configured channel has nothing to warn about, got " + logged);
    }

    @Test
    @DisplayName("a missing push channel never stops the bot: the log floor still takes the alert")
    void missingChannelStillLeavesAWorkingSink() {
        // Assembly with no channel configured must not throw on the way up. Assembled through the
        // seam rather than through fromEnvironment(): that one reads the real environment, so on a
        // machine that does set the two variables this test would build a sink around a live token,
        // and it would also spend the process-wide warning latch for every test after it.
        assertNotNull(AlertSink.Composite.assemble(Map.<String, String>of()::get, new AtomicBoolean()));
        logged.clear();

        // And the floor that is left when Telegram is absent still carries a CRITICAL. Exercised on
        // a hand-built sink rather than the assembled one, so this test can never reach the network.
        Logger floorLog = Logger.getLogger(AlertSink.Logging.LOGGER_NAME);
        boolean floorParentHandlers = floorLog.getUseParentHandlers();
        floorLog.setUseParentHandlers(false);
        floorLog.addHandler(capture);
        try {
            new AlertSink.Composite(List.of(new AlertSink.Logging()))
                    .critical("Position without stop", "test-only, no exchange and no network involved");
        } finally {
            floorLog.removeHandler(capture);
            floorLog.setUseParentHandlers(floorParentHandlers);
        }

        assertEquals(1, logged.size(), "the log floor dropped the alert: " + logged);
        assertEquals(Level.SEVERE, logged.get(0).getLevel());
    }

    // --- Delivery, driven through the Sender seam ---------------------------------------------

    /** Records every send; answers with the scripted statuses in order, then 200. */
    private static final class ScriptedSender implements AlertSink.Telegram.Sender {
        final List<String> texts = new java.util.concurrent.CopyOnWriteArrayList<>();
        final java.util.ArrayDeque<AlertSink.Telegram.SendResult> script = new java.util.ArrayDeque<>();
        long blockMs = 0;

        @Override public AlertSink.Telegram.SendResult send(String chatId, String text) throws InterruptedException {
            if (blockMs > 0) Thread.sleep(blockMs);
            texts.add(text);
            AlertSink.Telegram.SendResult next = script.poll();
            return next == null ? new AlertSink.Telegram.SendResult(200, "{\"ok\":true}") : next;
        }
    }

    @Test
    @DisplayName("flush waits for an alert that is in flight, not only for one still queued")
    void flushWaitsForAnInFlightDelivery() {
        ScriptedSender sender = new ScriptedSender();
        sender.blockMs = 300;
        AlertSink.Telegram telegram = new AlertSink.Telegram("not-a-real-token", "42", sender);

        telegram.alert(AlertSink.Severity.CRITICAL, "Reconciliation drift", "why the bot halted");
        telegram.flush(5_000L);

        assertEquals(1, sender.texts.size(),
                "the shutdown hook returned while the POST was still running, and the JVM killed it");
    }

    @Test
    @DisplayName("flood control (429) is waited out and retried, not blamed on the credentials")
    void floodControlIsRetried() {
        ScriptedSender sender = new ScriptedSender();
        sender.script.add(new AlertSink.Telegram.SendResult(429,
                "{\"ok\":false,\"error_code\":429,\"parameters\":{\"retry_after\":1}}"));
        AlertSink.Telegram telegram = new AlertSink.Telegram("not-a-real-token", "42", sender);

        telegram.alert(AlertSink.Severity.INFO, "Bot is up", "proof of life");
        telegram.flush(10_000L);

        assertEquals(2, sender.texts.size(), "one refused by flood control, one delivered");
        assertTrue(logged.stream().noneMatch(r -> r.getMessage().contains("check TELEGRAM")),
                "flood control is not a credentials problem: " + logged);
    }

    @Test
    @DisplayName("a message over Telegram's 4096 chars is cut, not refused with a 400")
    void oversizedMessageIsTruncated() {
        ScriptedSender sender = new ScriptedSender();
        AlertSink.Telegram telegram = new AlertSink.Telegram("not-a-real-token", "42", sender);

        telegram.alert(AlertSink.Severity.WARNING, "Reconciliation drift", "x".repeat(6_000));
        telegram.flush(5_000L);

        assertEquals(1, sender.texts.size());
        assertTrue(sender.texts.get(0).length() <= AlertSink.Telegram.MAX_TEXT_CHARS, "" + sender.texts.get(0).length());
        assertTrue(sender.texts.get(0).startsWith("⚠ Reconciliation drift"), "the title must survive the cut");
        assertEquals(1_000L, AlertSink.Telegram.retryAfterMillis("no such field"));
        assertEquals(3_000L, AlertSink.Telegram.retryAfterMillis("{\"parameters\":{\"retry_after\": 3}}"));
    }
}
