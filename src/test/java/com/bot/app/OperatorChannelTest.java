package com.bot.app;

import com.bot.core.Side;
import com.bot.exec.TradingHalt;
import com.bot.signal.CloseRequest;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The operator's commands and buttons, driven without a network. The properties that matter: only
 * the configured chat is obeyed, /resume is the one thing that clears a halt, nothing here can open
 * a position, every close goes into the queue the main loop drains, and a reply is never lost to
 * its own formatting.
 */
class OperatorChannelTest {

    private static final Instant NOON = Instant.parse("2026-08-22T12:00:00Z");

    /** Records every Bot API call; answers 200 with a fresh message id unless told otherwise. */
    static final class FakeTelegram implements OperatorChannel.Transport {
        record Call(String method, Map<String, String> params) {}

        final List<Call> calls = new ArrayList<>();
        Function<Call, OperatorChannel.Response> responder =
                c -> new OperatorChannel.Response(200, "{\"ok\":true,\"result\":{\"message_id\":" + (100 + calls.size()) + "}}");

        @Override public String poll(long offset) { return "{\"ok\":true,\"result\":[]}"; }

        @Override public OperatorChannel.Response call(String method, Map<String, String> params) {
            Call c = new Call(method, new LinkedHashMap<>(params));
            calls.add(c);
            return responder.apply(c);
        }

        List<Call> of(String method) {
            return calls.stream().filter(c -> c.method().equals(method)).toList();
        }

        /** Texts of messages sent or edited, in order. */
        List<String> sent() {
            return calls.stream().filter(c -> c.method().equals("sendMessage") || c.method().equals("editMessageText"))
                    .map(c -> c.params().get("text")).toList();
        }
    }

    private static String update(long id, String chat, String text) {
        return "{\"ok\":true,\"result\":[{\"update_id\":" + id + ",\"message\":{\"chat\":{\"id\":"
                + chat + "},\"text\":\"" + text + "\"}}]}";
    }

    private static String press(long id, String chat, long messageDate, String data) {
        return new JSONObject().put("ok", true).put("result", new JSONArray().put(new JSONObject()
                .put("update_id", id)
                .put("callback_query", new JSONObject()
                        .put("id", "cb" + id)
                        .put("from", new JSONObject().put("id", Long.parseLong(chat)))
                        .put("data", data)
                        .put("message", new JSONObject()
                                .put("message_id", 77)
                                .put("date", messageDate)
                                .put("chat", new JSONObject().put("id", Long.parseLong(chat))))))).toString();
    }

    static OperatorSnapshot.Position position(String symbol, Side side, double entry, double mark) {
        double qty = 2;
        return new OperatorSnapshot.Position(symbol, side, qty, entry, mark, (mark - entry) * qty * side.sign(),
                side == Side.LONG ? entry * 0.95 : entry * 1.05, side == Side.LONG ? entry * 1.10 : entry * 0.90,
                NOON.minusSeconds(14 * 3600), true);
    }

    static OperatorSnapshot snapshot(Instant at, List<OperatorSnapshot.Position> positions) {
        return new OperatorSnapshot(at, at.minusSeconds(7200), "20260923-053822Z", false, false, 0, false, false,
                0, 0, at, new OperatorSnapshot.Account(143.10, 120.70, 1.23), positions, true, null, List.of());
    }

    /** A clock the test moves by hand. */
    private static final class MovableClock extends Clock {
        final AtomicReference<Instant> now = new AtomicReference<>(NOON);
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now.get(); }
    }

    /** The operator's chat is 42 and the clock stands at NOON, where every test snapshot is dated. */
    private static OperatorChannel channel(FakeTelegram tg, TradingHalt halt) {
        return new OperatorChannel(tg, "42", halt).withClock(Clock.fixed(NOON, ZoneOffset.UTC));
    }

    private static String closeAllToken(FakeTelegram tg) {
        FakeTelegram.Call prompt = tg.calls.stream()
                .filter(c -> c.params().getOrDefault("reply_markup", "").contains("ca:y:"))
                .reduce((a, b) -> b).orElseThrow();
        JSONArray row = new JSONObject(prompt.params().get("reply_markup")).getJSONArray("inline_keyboard").getJSONArray(0);
        return row.getJSONObject(0).getString("callback_data").substring("ca:y:".length());
    }

    // ─── The commands ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("/resume is the only way out of a halt, and it names what it cleared")
    void resumeClearsAHalt() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("reconciliation drift: UNKNOWN_POSITION on XUSDT", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);

        ops.handleUpdates(update(1, "42", "/resume"));

        assertFalse(halt.isHalted());
        assertEquals(1, tg.sent().size());
        assertTrue(tg.sent().get(0).startsWith("🟢 <b>Халт снят</b>"), tg.sent().get(0));
        assertTrue(tg.sent().get(0).contains("UNKNOWN_POSITION on XUSDT"), tg.sent().get(0));
    }

    @Test
    @DisplayName("a stranger's /resume is ignored and not even answered")
    void strangersAreIgnored() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("something", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);

        ops.handleUpdates(update(1, "999", "/resume"));

        assertTrue(halt.isHalted(), "only the operator's chat may clear a halt");
        assertTrue(tg.calls.isEmpty(), "no reply: a reply would confirm the bot exists");
    }

    @Test
    @DisplayName("/halt latches and /status leads with the halt, from what the loop last published")
    void haltAndStatus() throws Exception {
        TradingHalt halt = new TradingHalt();
        FakeTelegram tg = new FakeTelegram();
        MovableClock clock = new MovableClock();
        OperatorChannel ops = channel(tg, halt).withClock(clock);
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));

        ops.handleUpdates(update(1, "42", "/halt"));
        assertTrue(halt.isHalted());
        assertEquals("operator via Telegram", halt.reason().orElseThrow());
        assertTrue(tg.sent().get(0).startsWith("⏸ <b>Халт включён</b>"), tg.sent().get(0));

        clock.now.set(NOON.plusSeconds(12));
        ops.handleUpdates(update(2, "42", "/status@SomeBot"));
        String status = tg.sent().get(1);
        assertTrue(status.startsWith("⏸ <b>Халт:</b> operator via Telegram"), status);
        assertTrue(status.contains("$143.10"), status);
        assertTrue(status.contains("20260923-053822Z"), status);
        assertTrue(status.contains("12 с назад"), status);

        ops.handleUpdates(update(3, "42", "/halt"));
        assertTrue(tg.sent().get(2).startsWith("⏸ <b>Уже на халте</b>"), tg.sent().get(2));
    }

    @Test
    @DisplayName("a loop parked by an exchange hold: /status names the hold, read live, above the silence")
    void statusExplainsAParkedLoop() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt()).withExchangeHold(() -> 720_000L);
        ops.publish(snapshot(NOON.minusSeconds(600), List.of()));   // the loop last spoke 10 min ago

        ops.handleUpdates(update(1, "42", "/status"));

        String[] lines = tg.sent().get(0).split("\n");
        assertEquals("🔴 <b>Биржа не отвечает</b> — пауза ещё 12 мин", lines[0]);
        assertEquals("🔴 Цикл молчит 10 мин", lines[1]);
        assertTrue(tg.sent().get(0).contains("Стопы стоят на бирже"), tg.sent().get(0));

        OperatorChannel broken = channel(tg, new TradingHalt()).withExchangeHold(() -> { throw new IllegalStateException("x"); });
        broken.publish(snapshot(NOON, List.of()));
        broken.handleUpdates(update(2, "42", "/status"));
        assertTrue(tg.sent().get(1).startsWith("🟢 <b>Торгует</b>"), "a failing hold reader costs only the live figure");
    }

    @Test
    @DisplayName("non-commands and unknown commands change nothing")
    void chatterIsHarmless() throws Exception {
        TradingHalt halt = new TradingHalt();
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);

        ops.handleUpdates(update(1, "42", "hello bot"));
        ops.handleUpdates(update(2, "42", "/open BTCUSDT"));

        assertFalse(halt.isHalted());
        assertEquals(1, tg.sent().size());
        assertTrue(tg.sent().get(0).startsWith("🟠 Не знаю такой команды"), tg.sent().get(0));
        assertTrue(ops.drainCloses().isEmpty());
    }

    @Test
    @DisplayName("/resume does not lift REAL_MODE=observe: a mode is not an incident")
    void observeIsNotClearable() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("REAL_MODE=observe - reading the account, opening nothing", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);

        ops.handleUpdates(update(1, "42", "/resume"));

        assertTrue(halt.isHalted(), "observe must survive /resume; only the environment ends it");
        assertTrue(tg.sent().get(0).startsWith("👁"), tg.sent().get(0));
        assertTrue(tg.sent().get(0).contains("REAL_MODE=trade"), tg.sent().get(0));
    }

    @Test
    @DisplayName("a command Telegram replays from before this process started is ignored")
    void staleCommandsAreIgnored() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("boot drift", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);
        ops.markStarted(NOON);

        long beforeBoot = NOON.getEpochSecond() - 120;
        ops.handleUpdates("{\"ok\":true,\"result\":[{\"update_id\":7,\"message\":{\"date\":" + beforeBoot
                + ",\"chat\":{\"id\":42},\"text\":\"/resume\"}}]}");

        assertTrue(halt.isHalted(), "a /resume typed while the bot was down must not clear the new boot's halt");
        assertTrue(tg.calls.isEmpty());

        long afterBoot = NOON.getEpochSecond() + 10;
        ops.handleUpdates("{\"ok\":true,\"result\":[{\"update_id\":8,\"message\":{\"date\":" + afterBoot
                + ",\"chat\":{\"id\":42},\"text\":\"/resume\"}}]}");
        assertFalse(halt.isHalted(), "a live command still works");
    }

    @Test
    @DisplayName("replies name the venue, because demo and real share one chat")
    void repliesCarryTheVenue() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt()).withVenueTag("REAL TRADE");
        ops.handleUpdates(update(1, "42", "/help"));
        assertTrue(tg.sent().get(0).startsWith("[REAL TRADE] 📊 <b>Команды</b>"), tg.sent().get(0));
        assertEquals("HTML", tg.of("sendMessage").get(0).params().get("parse_mode"));
    }

    @Test
    @DisplayName("/close queues a reduce-only close for the loop; garbage symbols are refused")
    void closeQueuesForTheLoop() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());

        ops.handleUpdates(update(1, "42", "/close adausdt"));
        var queued = ops.drainCloses();
        assertEquals(1, queued.size());
        assertEquals("ADAUSDT", queued.get(0).symbol());
        assertEquals("operator via Telegram", queued.get(0).reason());
        assertTrue(tg.sent().get(0).startsWith("⏳ <b>ADAUSDT — в очереди на закрытие</b>"), tg.sent().get(0));
        assertTrue(ops.drainCloses().isEmpty(), "drained once, gone");

        ops.handleUpdates(update(2, "42", "/close"));
        ops.handleUpdates(update(3, "42", "/close $$$"));
        ops.handleUpdates(update(4, "42", "/close ada"));   // no book published: too short to trust
        assertTrue(ops.drainCloses().isEmpty(), "garbage must queue nothing");
        assertTrue(tg.sent().get(1).startsWith("🟠 <b>Какую закрыть?</b>"), tg.sent().get(1));
    }

    @Test
    @DisplayName("/close ada finds ADAUSDT in the published book - a phone keyboard is lowercase and short")
    void closeResolvesTheShortName() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));

        ops.handleUpdates(update(1, "42", "/close ada"));

        List<CloseRequest> queued = ops.drainCloses();
        assertEquals(1, queued.size());
        assertEquals("ADAUSDT", queued.get(0).symbol());
    }

    @Test
    @DisplayName("/resume on an observe venue clears the drift latch but does not promise entries")
    void resumeInObserveModeDoesNotPromiseEntries() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("reconciliation drift: UNKNOWN_POSITION on XUSDT", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt).withObserveMode(true);

        ops.handleUpdates(update(1, "42", "/resume"));

        assertFalse(halt.isHalted(), "the drift latch itself is the operator's to clear");
        assertFalse(tg.sent().get(0).contains("Входы — со следующего сигнала"), tg.sent().get(0));
        assertTrue(tg.sent().get(0).contains("REAL_MODE=observe"), tg.sent().get(0));
        // ...and the loop puts the observe latch back on its next tick.
        assertTrue(TestnetBot.relatchObserve(halt, true, NOON));
        assertTrue(halt.reason().orElse("").startsWith(OperatorChannel.OBSERVE_REASON_PREFIX));
        assertFalse(TestnetBot.relatchObserve(halt, true, NOON), "already latched: nothing to do");
    }

    @Test
    @DisplayName("the observe re-latch never overwrites a genuine halt and never fires on a trade venue")
    void relatchObserveRespectsOtherHalts() {
        TradingHalt drift = new TradingHalt();
        drift.halt("reconciliation drift: x", NOON);
        assertFalse(TestnetBot.relatchObserve(drift, true, NOON));
        assertEquals("reconciliation drift: x", drift.reason().orElseThrow());

        TradingHalt clear = new TradingHalt();
        assertFalse(TestnetBot.relatchObserve(clear, false, NOON));
        assertFalse(clear.isHalted());
    }

    @Test
    @DisplayName("the channel only exists when Telegram is configured")
    void absentWithoutCredentials() {
        assertEquals(null, OperatorChannel.fromEnvironmentOrNull(k -> null, new TradingHalt()));
        assertTrue(OperatorChannel.fromEnvironmentOrNull(
                k -> k.equals("TELEGRAM_BOT_TOKEN") ? "t" : "42", new TradingHalt()) != null);
    }

    @Test
    @DisplayName("the '/' menu is registered once, in Russian, with every command the channel answers")
    void commandsAreRegistered() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());

        ops.registerCommands();

        assertEquals(1, tg.of("setMyCommands").size());
        JSONArray commands = new JSONArray(tg.of("setMyCommands").get(0).params().get("commands"));
        List<String> names = new ArrayList<>();
        for (int i = 0; i < commands.length(); i++) {
            names.add(commands.getJSONObject(i).getString("command"));
            String description = commands.getJSONObject(i).getString("description");
            assertTrue(description.matches(".*[А-Яа-я].*"), "Russian description: " + description);
        }
        // Control and the one fallback screen (24.09); the numbers live in the lab bot's panel.
        assertEquals(List.of("status", "halt", "resume", "close", "menu", "help"), names);
        // The unadvertised read-only screens still answer when typed.
        List<String> typed = new ArrayList<>(names);
        typed.addAll(List.of("book", "pnl", "queue", "why"));
        for (String name : typed) {
            ops.handleUpdates(update(typed.indexOf(name) + 1, "42", "/" + name));
        }
        assertFalse(tg.sent().stream().anyMatch(t -> t.contains("Не знаю такой команды")), tg.sent().toString());
    }

    // ─── Formatting never costs a message ────────────────────────────────────────────────────

    @Test
    @DisplayName("a halt reason quoting '<html> & co' is escaped, never sent as markup")
    void dynamicTextIsEscaped() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("exchange said <html><body>502 & bad gateway</body>", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt).withVenueTag("<demo>");
        ops.publish(snapshot(NOON, List.of()));

        ops.handleUpdates(update(1, "42", "/status"));

        String text = tg.sent().get(0);
        assertTrue(text.startsWith("[&lt;demo&gt;] ⏸ <b>Халт:</b> exchange said &lt;html&gt;&lt;body&gt;502 &amp; bad"), text);
        assertFalse(text.contains("<html>"), text);
    }

    @Test
    @DisplayName("when Telegram refuses the HTML (400), the same reply is resent once as plain text")
    void plainTextFallbackOn400() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        tg.responder = c -> c.params().containsKey("parse_mode")
                ? new OperatorChannel.Response(400, "{\"ok\":false,\"description\":\"Bad Request: can't parse entities\"}")
                : new OperatorChannel.Response(200, "{\"ok\":true,\"result\":{\"message_id\":5}}");
        TradingHalt halt = new TradingHalt();
        halt.halt("a & b", NOON);
        OperatorChannel ops = channel(tg, halt);

        ops.handleUpdates(update(1, "42", "/halt"));

        List<FakeTelegram.Call> sends = tg.of("sendMessage");
        assertEquals(2, sends.size(), "one HTML attempt, one plain resend, no more");
        assertFalse(sends.get(1).params().containsKey("parse_mode"));
        assertEquals("⏸ Уже на халте\nПричина: a & b", sends.get(1).params().get("text"));
    }

    @Test
    @DisplayName("a reply past 4096 characters arrives in parts, each valid, the table split cleanly")
    void longRepliesAreSplit() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());
        List<OperatorSnapshot.Position> many = new ArrayList<>();
        for (int i = 0; i < 300; i++) many.add(position("C" + (1000 + i) + "USDT", Side.LONG, 1.0, 1.02));
        ops.publish(snapshot(NOON, many));

        ops.handleUpdates(update(1, "42", "/book"));

        List<String> parts = tg.sent();
        assertTrue(parts.size() >= 3, "300 rows do not fit one message: " + parts.size());
        int rows = 0;
        for (String p : parts) {
            assertTrue(p.length() <= TgFormat.MAX_MESSAGE_CHARS, "part of " + p.length());
            assertEquals(count(p, "<pre>"), count(p, "</pre>"), "every part closes the table it opens");
            rows += count(p, "  +2.0% ");
        }
        assertEquals(300, rows, "no row lost at a seam");
    }

    private static int count(String s, String needle) {
        int n = 0;
        for (int i = s.indexOf(needle); i >= 0; i = s.indexOf(needle, i + needle.length())) n++;
        return n;
    }

    // ─── Buttons ────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("/menu shows the state first and control only (24.09); every callback_data fits 64 bytes")
    void menuHasTheButtons() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));

        ops.handleUpdates(update(1, "42", "/start"));

        FakeTelegram.Call menu = tg.of("sendMessage").get(0);
        assertTrue(menu.params().get("text").startsWith("🟢 <b>Торгует</b>"), menu.params().get("text"));
        JSONArray rows = new JSONObject(menu.params().get("reply_markup")).getJSONArray("inline_keyboard");
        List<String> labels = new ArrayList<>();
        for (int r = 0; r < rows.length(); r++) {
            for (int b = 0; b < rows.getJSONArray(r).length(); b++) {
                JSONObject button = rows.getJSONArray(r).getJSONObject(b);
                labels.add(button.getString("text"));
                assertTrue(button.getString("callback_data").getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 64);
            }
        }
        assertEquals(List.of("📊 Статус", "⏸ Халт", "▶️ Снять халт", "🧯 Закрыть всё"), labels);
        assertEquals(3, rows.length(), "status / halt + resume / close all");
        assertThrows(IllegalArgumentException.class, () -> new OperatorViews.Button("x", "d".repeat(65)));
    }

    @Test
    @DisplayName("a button runs its command's code, edits the pressed message, and is always answered")
    void buttonsRunTheCommands() throws Exception {
        TradingHalt halt = new TradingHalt();
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));
        long now = NOON.getEpochSecond();

        ops.handleUpdates(press(1, "42", now, OperatorViews.CB_HALT));
        assertTrue(halt.isHalted(), "⏸ is /halt");
        ops.handleUpdates(press(2, "42", now, OperatorViews.CB_RESUME));
        assertTrue(halt.isHalted(), "▶️ asks first");
        ops.handleUpdates(press(3, "42", now, OperatorViews.CB_RESUME_YES + resumeToken(tg)));
        assertFalse(halt.isHalted(), "▶️ then ✅ is /resume");
        ops.handleUpdates(press(4, "42", now, OperatorViews.CB_BOOK));
        ops.handleUpdates(press(5, "42", now, OperatorViews.CB_QUEUE));
        ops.handleUpdates(press(6, "42", now, "m:nonsense"));

        assertEquals(6, tg.of("answerCallbackQuery").size(), "every press answered, or the button spins");
        List<FakeTelegram.Call> edits = tg.of("editMessageText");
        assertEquals(5, edits.size(), "an unknown button changes no screen");
        assertEquals("77", edits.get(0).params().get("message_id"));
        assertTrue(edits.get(3).params().get("text").startsWith("📒 <b>Книга: 1 позиция</b>"), edits.get(3).params().get("text"));
        assertTrue(edits.get(4).params().get("text").contains("Нет данных сканера"));
        assertTrue(edits.get(4).params().get("reply_markup").contains("m:status"), "screens keep the menu under them");
    }

    @Test
    @DisplayName("pressing the same screen twice ('message is not modified') is not an error and sends nothing new")
    void notModifiedIsSuccess() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        tg.responder = c -> c.method().equals("editMessageText")
                ? new OperatorChannel.Response(400, "{\"ok\":false,\"description\":\"Bad Request: message is not modified\"}")
                : new OperatorChannel.Response(200, "{\"ok\":true,\"result\":true}");
        OperatorChannel ops = channel(tg, new TradingHalt());

        ops.handleUpdates(press(1, "42", NOON.getEpochSecond(), OperatorViews.CB_STATUS));

        assertEquals(1, tg.of("editMessageText").size());
        assertTrue(tg.of("sendMessage").isEmpty());
    }

    @Test
    @DisplayName("a button pressed in any other chat is ignored silently - not answered, not obeyed")
    void strangerButtonsAreIgnored() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("drift", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);

        ops.handleUpdates(press(1, "999", NOON.getEpochSecond(), OperatorViews.CB_RESUME));
        ops.handleUpdates(press(2, "999", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL));

        assertTrue(halt.isHalted());
        assertTrue(tg.calls.isEmpty(), "no answer: an answer would confirm the bot exists");
        assertEquals(0, ops.liveCloseAllTokens());
    }

    @Test
    @DisplayName("a button on a message from before this boot is ignored: ▶️ replayed at start must not lift the new halt")
    void staleButtonsAreIgnored() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("boot drift", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);
        ops.markStarted(NOON);

        ops.handleUpdates(press(1, "42", NOON.getEpochSecond() - 600, OperatorViews.CB_RESUME));

        assertTrue(halt.isHalted(), "the old menu's ▶️ did nothing");
        assertEquals(1, tg.of("answerCallbackQuery").size(), "the operator is told why");
        assertTrue(tg.of("editMessageText").isEmpty());
        assertEquals(1, tg.of("sendMessage").size(), "a fresh menu instead");
        assertTrue(tg.of("sendMessage").get(0).params().get("reply_markup").contains("m:resume"));

        ops.handleUpdates(press(2, "42", NOON.getEpochSecond() + 5, OperatorViews.CB_RESUME));
        ops.handleUpdates(press(3, "42", NOON.getEpochSecond() + 5, OperatorViews.CB_RESUME_YES + resumeToken(tg)));
        assertFalse(halt.isHalted(), "a button on this boot's message works");
    }

    // ─── Close all ──────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("close-all confirms exactly the published book, queues one close per position, once")
    void closeAllQueuesThePublishedBookOnce() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        MovableClock clock = new MovableClock();
        OperatorChannel ops = channel(tg, new TradingHalt()).withClock(clock);
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01),
                position("XRPUSDT", Side.SHORT, 0.5, 0.49))));

        ops.handleUpdates(update(1, "42", "/close all"));
        assertTrue(ops.drainCloses().isEmpty(), "the prompt alone closes nothing");
        String prompt = tg.sent().get(0);
        assertTrue(prompt.startsWith("🧯 <b>Закрыть всё: 2 позиции?</b>"), prompt);
        assertTrue(prompt.contains("ADA") && prompt.contains("XRP"), prompt);
        assertTrue(tg.of("sendMessage").get(0).params().get("reply_markup").contains("✅ Да, закрыть 2"));
        String token = closeAllToken(tg);

        clock.now.set(NOON.plusSeconds(20));
        ops.handleUpdates(press(2, "42", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL_YES + token));

        List<CloseRequest> queued = ops.drainCloses();
        assertEquals(List.of("ADAUSDT", "XRPUSDT"), queued.stream().map(CloseRequest::symbol).toList());
        long epoch = NOON.plusSeconds(20).getEpochSecond();
        assertEquals("tg-closeall-ADAUSDT-" + epoch, queued.get(0).id());
        assertEquals("tg-closeall-XRPUSDT-" + epoch, queued.get(1).id());
        assertTrue(queued.stream().allMatch(c -> c.reason().equals("operator: close all via Telegram")));
        String result = tg.of("editMessageText").get(0).params().get("text");
        assertTrue(result.startsWith("🧯 <b>В очереди на закрытие: 2</b>"), result);

        ops.handleUpdates(press(3, "42", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL_YES + token));
        assertTrue(ops.drainCloses().isEmpty(), "a second press queues nothing");
        assertEquals(1, tg.of("editMessageText").size(), "and does not repaint the result");
        assertEquals("Уже обработано", tg.of("answerCallbackQuery").get(1).params().get("text"));
    }

    @Test
    @DisplayName("a close-all confirmation dies after 60 s: pressed at 61 s it closes nothing")
    void closeAllExpires() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        MovableClock clock = new MovableClock();
        OperatorChannel ops = channel(tg, new TradingHalt()).withClock(clock);
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));
        ops.handleUpdates(press(1, "42", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL));
        String token = closeAllToken(tg);

        clock.now.set(NOON.plusSeconds(61));
        ops.handleUpdates(press(2, "42", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL_YES + token));

        assertTrue(ops.drainCloses().isEmpty());
        String last = tg.sent().get(tg.sent().size() - 1);
        assertTrue(last.contains("Подтверждение истекло"), last);
        assertEquals(0, ops.liveCloseAllTokens(), "an expired token is gone, not reusable");
    }

    @Test
    @DisplayName("cancel spends the token: a later ✅ on the same prompt queues nothing")
    void closeAllCancelSpendsTheToken() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));
        ops.handleUpdates(update(1, "42", "/close ALL"));
        String token = closeAllToken(tg);

        ops.handleUpdates(press(2, "42", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL_NO + token));
        ops.handleUpdates(press(3, "42", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL_YES + token));

        assertTrue(ops.drainCloses().isEmpty());
        assertTrue(tg.of("editMessageText").get(0).params().get("text").startsWith("✖️ <b>Отменено</b>"));
    }

    @Test
    @DisplayName("a valid close-all token pressed from another chat does nothing and stays unspent")
    void closeAllTokenIsTheOperatorsOnly() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));
        ops.handleUpdates(update(1, "42", "/close all"));
        String token = closeAllToken(tg);
        int before = tg.calls.size();

        ops.handleUpdates(press(2, "999", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL_YES + token));

        assertTrue(ops.drainCloses().isEmpty());
        assertEquals(before, tg.calls.size());
        assertEquals(1, ops.liveCloseAllTokens());
    }

    @Test
    @DisplayName("close-all on an empty or unpublished book issues no token")
    void closeAllWithNothingToClose() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());

        ops.handleUpdates(update(1, "42", "/close all"));
        ops.publish(snapshot(NOON, List.of()));
        ops.handleUpdates(update(2, "42", "/close all"));

        assertEquals(0, ops.liveCloseAllTokens());
        assertTrue(tg.sent().get(0).contains("Книги ещё нет"), tg.sent().get(0));
        assertTrue(tg.sent().get(1).startsWith("📒 <b>Книга пуста</b>"), tg.sent().get(1));
        assertFalse(tg.calls.stream().anyMatch(c -> c.params().getOrDefault("reply_markup", "").contains("ca:y:")),
                "no confirm button offered for nothing");
    }

    // ─── The scanner's view: /queue, /why and the coin buttons ─────────────────────────────

    @TempDir
    Path dataDir;

    /** A fresh scanner file ten minutes before NOON, with a "why" for ADAUSDT. */
    private Path scannerFile() throws Exception {
        JSONObject doc = ScannerViewTest.doc().put("ts", NOON.minusSeconds(600).toString());
        doc.getJSONObject("why").put("ADAUSDT", doc.getJSONObject("why").getJSONObject("TIAUSDT"));
        Path f = dataDir.resolve(ScannerView.FILE_NAME);
        Files.writeString(f, doc.toString(), StandardCharsets.UTF_8);
        return f;
    }

    @Test
    @DisplayName("/why ADA reads the scanner's file; no argument asks which coin, without coin buttons (24.09)")
    void whyCommand() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt()).withScannerView(scannerFile());
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));

        ops.handleUpdates(update(1, "42", "/why ada"));
        ops.handleUpdates(update(2, "42", "/why"));

        assertTrue(tg.sent().get(0).startsWith("📒 <b>ADA · LONG · почему вошли</b>"), tg.sent().get(0));
        assertTrue(tg.sent().get(0).contains("Сейчас: +1.0%"), tg.sent().get(0));
        assertTrue(tg.sent().get(1).startsWith("🟠 <b>Какую монету?</b>"), tg.sent().get(1));
        assertNull(tg.of("sendMessage").get(1).params().get("reply_markup"));
    }

    @Test
    @DisplayName("24.09: /book has no coin buttons; an old message's coin button still draws /why, read-only")
    void oldCoinButtonsStillAnswer() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt()).withScannerView(scannerFile());
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));
        long now = NOON.getEpochSecond();

        ops.handleUpdates(update(1, "42", "/book"));
        ops.handleUpdates(press(2, "42", now, OperatorViews.CB_BOOK));
        ops.handleUpdates(press(3, "42", now, OperatorViews.CB_WHY + "ADAUSDT"));
        ops.handleUpdates(press(4, "42", now, OperatorViews.CB_WHY + "ada<script>"));

        assertNull(tg.of("sendMessage").get(0).params().get("reply_markup"), "typed /book: no keyboard");
        List<FakeTelegram.Call> edits = tg.of("editMessageText");
        assertEquals(2, edits.size(), "a garbage coin draws nothing");
        assertFalse(edits.get(0).params().get("reply_markup").contains("w:"), "no coin buttons under the book");
        assertTrue(edits.get(0).params().get("reply_markup").contains("m:status"), "the menu stays under the book");
        assertTrue(edits.get(1).params().get("text").startsWith("📒 <b>ADA · LONG · почему вошли</b>"));
        assertTrue(edits.get(1).params().get("reply_markup").contains("m:status"), "the menu under /why");
        assertFalse(edits.get(1).params().get("reply_markup").contains("\"m:book\""), "no « Книга any more");
        assertEquals(3, tg.of("answerCallbackQuery").size(), "every press answered, the bad one too");
        assertEquals("Неизвестная монета", tg.of("answerCallbackQuery").get(2).params().get("text"));
        assertTrue(ops.drainCloses().isEmpty(), "a coin button never closes anything");
    }

    @Test
    @DisplayName("a stranger's /why button gets nothing at all")
    void strangerWhyIgnored() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt()).withScannerView(scannerFile());

        ops.handleUpdates(press(1, "999", NOON.getEpochSecond(), OperatorViews.CB_WHY + "ADAUSDT"));

        assertTrue(tg.calls.isEmpty(), tg.calls.toString());
    }

    @Test
    @DisplayName("/queue and ⏳ read the scanner's file; without one they say so")
    void queueReadsTheScanner() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());
        ops.publish(snapshot(NOON, List.of()));

        ops.handleUpdates(update(1, "42", "/queue"));
        ops.withScannerView(scannerFile());
        ops.handleUpdates(update(2, "42", "/queue"));
        ops.handleUpdates(press(3, "42", NOON.getEpochSecond(), OperatorViews.CB_QUEUE));

        assertTrue(tg.sent().get(0).startsWith("🟠 <b>Нет данных сканера</b>"), tg.sent().get(0));
        assertTrue(tg.sent().get(1).startsWith("⏳ <b>Ждут входа: 2</b>"), tg.sent().get(1));
        assertTrue(tg.sent().get(2).startsWith("⏳ <b>Ждут входа: 2</b>"), tg.sent().get(2));
    }

    @Test
    @DisplayName("a trade line is stamped with the venue, resent once as plain text on a 400, and reports failure")
    void deliverHtml() {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt()).withVenueTag("REAL TRADE");
        tg.responder = c -> c.params().containsKey("parse_mode")
                ? new OperatorChannel.Response(400, "{\"ok\":false,\"description\":\"can't parse entities\"}")
                : new OperatorChannel.Response(200, "{\"ok\":true,\"result\":{\"message_id\":5}}");

        assertTrue(ops.deliverHtml("✅ <b>Тейк TAO ≈+$0.51</b>").delivered());
        List<FakeTelegram.Call> sends = tg.of("sendMessage");
        assertEquals(2, sends.size());
        assertEquals("[REAL TRADE] ✅ <b>Тейк TAO ≈+$0.51</b>", sends.get(0).params().get("text"));
        assertEquals("[REAL TRADE] ✅ Тейк TAO ≈+$0.51", sends.get(1).params().get("text"));

        tg.responder = c -> new OperatorChannel.Response(502, "bad gateway");
        TgOutbox.Attempt failed = ops.deliverHtml("🛑 <b>Стоп ADA</b>");
        assertFalse(failed.delivered(), "the outbox must know to retry");
        assertEquals(502, failed.status());

        // A 429 goes back to the outbox untouched: it waits on its own thread, not here.
        int before = tg.calls.size();
        tg.responder = c -> new OperatorChannel.Response(429,
                "{\"ok\":false,\"error_code\":429,\"parameters\":{\"retry_after\":7}}");
        TgOutbox.Attempt flood = ops.deliverHtml("🛑 <b>Стоп ADA</b>");
        assertEquals(429, flood.status());
        assertEquals(7_000L, OperatorChannel.retryAfterMillis(flood.body()));
        assertEquals(before + 1, tg.calls.size(), "no wait and no second call on the outbox's path");
    }

    // ─── Fix round: flood control, ▶️ confirmation, stale bursts, 🧯 at execution time ────────

    @Test
    @DisplayName("a reply hit by flood control is sent once more after Telegram's own retry_after")
    void replyWaitsOutFloodControl() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        List<Long> sleeps = new ArrayList<>();
        OperatorChannel ops = channel(tg, new TradingHalt()).withSleeper(sleeps::add);
        int[] n = {0};
        tg.responder = c -> ++n[0] == 1
                ? new OperatorChannel.Response(429, "{\"ok\":false,\"parameters\":{\"retry_after\":3}}")
                : new OperatorChannel.Response(200, "{\"ok\":true,\"result\":{\"message_id\":9}}");

        ops.handleUpdates(update(1, "42", "/help"));

        assertEquals(List.of(3_000L), sleeps);
        assertEquals(2, tg.of("sendMessage").size(), "the /help reply arrives after the wait");

        // A wait longer than the poll thread may sit out is not waited: one call, a log line.
        tg.responder = c -> new OperatorChannel.Response(429, "{\"ok\":false,\"parameters\":{\"retry_after\":25}}");
        ops.handleUpdates(update(2, "42", "/help"));
        assertEquals(List.of(3_000L), sleeps);
        assertEquals(3, tg.of("sendMessage").size());
    }

    private static String resumeToken(FakeTelegram tg) {
        FakeTelegram.Call prompt = tg.calls.stream()
                .filter(c -> c.params().getOrDefault("reply_markup", "").contains("rs:y:"))
                .reduce((a, b) -> b).orElseThrow();
        JSONArray row = new JSONObject(prompt.params().get("reply_markup")).getJSONArray("inline_keyboard").getJSONArray(0);
        return row.getJSONObject(0).getString("callback_data").substring("rs:y:".length());
    }

    @Test
    @DisplayName("▶️ asks first: the halt stays until ✅, the token is single-use, ✖️ keeps the halt")
    void resumeButtonAsksFirst() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("close abandoned on ADAUSDT after 8 attempts", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);
        long now = NOON.getEpochSecond();

        ops.handleUpdates(press(1, "42", now, OperatorViews.CB_RESUME));
        assertTrue(halt.isHalted(), "one tap on ▶️ lifts nothing");
        String prompt = tg.of("editMessageText").get(0).params().get("text");
        assertTrue(prompt.startsWith("⏸ <b>Снять халт?</b>\nПричина: close abandoned on ADAUSDT"), prompt);
        String token = resumeToken(tg);

        ops.handleUpdates(press(2, "42", now, OperatorViews.CB_RESUME_NO + token));
        ops.handleUpdates(press(3, "42", now, OperatorViews.CB_RESUME_YES + token));
        assertTrue(halt.isHalted(), "cancel spent the token");
        assertEquals("Уже обработано", tg.of("answerCallbackQuery").get(2).params().get("text"));

        ops.handleUpdates(press(4, "42", now, OperatorViews.CB_RESUME));
        ops.handleUpdates(press(5, "42", now, OperatorViews.CB_RESUME_YES + resumeToken(tg)));
        assertFalse(halt.isHalted());
        String done = tg.of("editMessageText").get(tg.of("editMessageText").size() - 1).params().get("text");
        assertTrue(done.startsWith("🟢 <b>Халт снят</b>"), done);

        // With nothing to lift, ▶️ says so at once and issues no token.
        ops.handleUpdates(press(6, "42", now, OperatorViews.CB_RESUME));
        String last = tg.of("editMessageText").get(tg.of("editMessageText").size() - 1).params().get("text");
        assertTrue(last.startsWith("🟢 <b>Халта нет</b>"), last);
    }

    @Test
    @DisplayName("▶️'s ✅ does not lift a different halt than the one it showed, nor after 60 s")
    void resumeConfirmationIsBoundToItsReason() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("operator via Telegram", NOON);
        FakeTelegram tg = new FakeTelegram();
        MovableClock clock = new MovableClock();
        OperatorChannel ops = channel(tg, halt).withClock(clock);
        long now = NOON.getEpochSecond();

        ops.handleUpdates(press(1, "42", now, OperatorViews.CB_RESUME));
        String token = resumeToken(tg);
        halt.clear();
        halt.halt("close abandoned on XRPUSDT after 8 attempts", NOON);
        ops.handleUpdates(press(2, "42", now, OperatorViews.CB_RESUME_YES + token));
        assertEquals("close abandoned on XRPUSDT after 8 attempts", halt.reason().orElseThrow());
        String changed = tg.of("editMessageText").get(1).params().get("text");
        assertTrue(changed.startsWith("🟠 <b>Халт уже другой</b>"), changed);

        ops.handleUpdates(press(3, "42", now, OperatorViews.CB_RESUME));
        String late = resumeToken(tg);
        clock.now.set(NOON.plusSeconds(61));
        ops.handleUpdates(press(4, "42", now, OperatorViews.CB_RESUME_YES + late));
        assertTrue(halt.isHalted());
        assertTrue(tg.sent().get(tg.sent().size() - 1).contains("Подтверждение истекло"));
    }

    @Test
    @DisplayName("▶️ on an observe latch answers at once and never offers to lift the mode")
    void resumeButtonOnObserve() throws Exception {
        TradingHalt halt = new TradingHalt();
        halt.halt("REAL_MODE=observe - reading the account, opening nothing", NOON);
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, halt);

        ops.handleUpdates(press(1, "42", NOON.getEpochSecond(), OperatorViews.CB_RESUME));

        assertTrue(halt.isHalted());
        assertTrue(tg.of("editMessageText").get(0).params().get("text").startsWith("👁"));
        assertFalse(tg.calls.stream().anyMatch(c -> c.params().getOrDefault("reply_markup", "").contains("rs:y:")));
    }

    @Test
    @DisplayName("five stale presses at boot: five answers, one fresh menu")
    void staleBurstGetsOneMenu() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        MovableClock clock = new MovableClock();
        OperatorChannel ops = channel(tg, new TradingHalt()).withClock(clock);
        ops.markStarted(NOON);

        for (int i = 1; i <= 5; i++) {
            ops.handleUpdates(press(i, "42", NOON.getEpochSecond() - 600, OperatorViews.CB_HALT));
        }
        assertEquals(5, tg.of("answerCallbackQuery").size());
        assertEquals(1, tg.of("sendMessage").size());

        clock.now.set(NOON.plusSeconds(31));
        ops.handleUpdates(press(6, "42", NOON.getEpochSecond() - 600, OperatorViews.CB_HALT));
        assertEquals(2, tg.of("sendMessage").size(), "a later stale press is shown the menu again");
    }

    @Test
    @DisplayName("a confirmed 🧯 also closes what the loop holds at execution: an entry filled after the prompt")
    void closeAllCoversWhatIsHeldAtExecution() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));
        ops.handleUpdates(update(1, "42", "/close all"));
        ops.handleUpdates(press(2, "42", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL_YES + closeAllToken(tg)));

        int[] reads = {0};
        List<CloseRequest> queued = ops.drainCloses(() -> {
            reads[0]++;
            return List.of("TIAUSDT", "ADAUSDT");   // TIA filled after the last reconcile pass
        });

        assertEquals(List.of("ADAUSDT", "TIAUSDT"), queued.stream().map(CloseRequest::symbol).toList());
        assertEquals("tg-closeall-TIAUSDT-" + NOON.getEpochSecond(), queued.get(1).id());
        assertEquals(OperatorChannel.CLOSE_ALL_REASON, queued.get(1).reason());
        assertEquals(1, reads[0]);
        assertTrue(ops.drainCloses(() -> { reads[0]++; return List.of("X"); }).isEmpty(), "drained once");
        assertEquals(1, reads[0], "what is held is read only while a 🧯 is pending");
        assertTrue(tg.sent().get(0).contains("Открытое после этого списка"), tg.sent().get(0));
        assertTrue(tg.sent().get(1).contains("Плюс открытое до исполнения"), tg.sent().get(1));
    }

    /** An ExchangePort that answers openPositions() only; anything else is a test failure. */
    private static com.bot.exec.ExchangePort positionsOnly(java.util.function.Supplier<List<com.bot.exec.ExchangeSnapshots.PositionSnapshot>> answer) {
        return (com.bot.exec.ExchangePort) java.lang.reflect.Proxy.newProxyInstance(
                OperatorChannelTest.class.getClassLoader(), new Class<?>[] {com.bot.exec.ExchangePort.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("openPositions")) return answer.get();
                    throw new AssertionError("close-all must read nothing but positions: " + method.getName());
                });
    }

    private static com.bot.exec.ExchangeSnapshots.PositionSnapshot exchangePosition(String symbol, String qty) {
        return new com.bot.exec.ExchangeSnapshots.PositionSnapshot(symbol, new java.math.BigDecimal(qty),
                java.math.BigDecimal.ONE, 3, true, java.math.BigDecimal.ZERO, java.math.BigDecimal.ZERO);
    }

    @Test
    @DisplayName("what a 🧯 covers at execution: the loop's book plus the exchange's non-flat listing; a failed read keeps the book")
    void heldSymbolsJoinsBookAndExchange() {
        com.bot.risk.RiskEngine engine = new com.bot.risk.RiskEngine(com.bot.risk.RiskConfig.defaults(),
                new com.bot.risk.ExposureBook(), new com.bot.risk.DailyLossKillSwitch(0.03));
        engine.book().open(new com.bot.risk.ExposureBook.OpenPosition("TIAUSDT", Side.LONG, java.math.BigDecimal.TEN,
                0.5, 5.0, 0.7, java.util.Optional.of("stop-1")));

        java.util.Set<String> held = TestnetBot.heldSymbols(engine, positionsOnly(() -> List.of(
                exchangePosition("ADAUSDT", "2"), exchangePosition("DOGEUSDT", "0"), exchangePosition("TIAUSDT", "10"))));
        assertEquals(List.of("TIAUSDT", "ADAUSDT"), List.copyOf(held), "flat rows are not positions");

        java.util.Set<String> blind = TestnetBot.heldSymbols(engine, positionsOnly(() -> {
            throw new IllegalStateException("HTTP 418");
        }));
        assertEquals(List.of("TIAUSDT"), List.copyOf(blind));
    }

    @Test
    @DisplayName("a failing read of what is held still closes every symbol the prompt listed")
    void closeAllSurvivesAFailedHeldRead() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt());
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01),
                position("XRPUSDT", Side.SHORT, 0.5, 0.49))));
        ops.handleUpdates(update(1, "42", "/close all"));
        ops.handleUpdates(press(2, "42", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL_YES + closeAllToken(tg)));

        List<CloseRequest> queued = ops.drainCloses(() -> { throw new IllegalStateException("HTTP 503"); });

        assertEquals(List.of("ADAUSDT", "XRPUSDT"), queued.stream().map(CloseRequest::symbol).toList());
    }

    @Test
    @DisplayName("during an exchange hold 🧯 does not promise 'seconds': it names the pause and the app")
    void closeAllNamesTheHold() throws Exception {
        FakeTelegram tg = new FakeTelegram();
        OperatorChannel ops = channel(tg, new TradingHalt()).withExchangeHold(() -> 6 * 3_600_000L);
        ops.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));

        ops.handleUpdates(update(1, "42", "/close all"));
        ops.handleUpdates(press(2, "42", NOON.getEpochSecond(), OperatorViews.CB_CLOSE_ALL_YES + closeAllToken(tg)));

        String prompt = tg.sent().get(0);
        String result = tg.sent().get(1);
        assertTrue(prompt.contains("🔴 Биржа на паузе ещё 6 ч"), prompt);
        assertTrue(result.contains("закрытия пройдут после паузы"), result);
        assertTrue(result.contains("вручную в приложении"), result);
        assertFalse(result.contains("за секунды"), result);

        OperatorChannel calm = channel(tg, new TradingHalt());
        calm.publish(snapshot(NOON, List.of(position("ADAUSDT", Side.LONG, 1.0, 1.01))));
        calm.handleUpdates(update(3, "42", "/close all"));
        assertFalse(tg.sent().get(2).contains("на паузе"), tg.sent().get(2));
    }
}
