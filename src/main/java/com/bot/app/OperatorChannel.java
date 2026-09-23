package com.bot.app;

import com.bot.app.OperatorViews.Button;
import com.bot.app.OperatorViews.Reply;
import com.bot.core.Preconditions;
import com.bot.exec.TradingHalt;
import com.bot.signal.CloseRequest;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;

import static com.bot.app.TgFormat.esc;

/**
 * The operator's hands, over Telegram: {@code /status}, {@code /halt}, {@code /resume},
 * {@code /close}, and the same behind inline buttons ({@code /menu}); since 24.09 the numbers live
 * in the lab bot's panel and /status is the one fallback screen here. Until 21.08
 * a halt cleared only with a redeploy and "what is the bot doing" meant opening the hosting
 * console. Commands are accepted from the configured chat only, and nothing here can open a
 * position or call the exchange: every screen renders a snapshot the main loop publishes, halt
 * sets the same latch the reconciler uses, resume clears it - the one place that ever does - and
 * every close goes into the queue the main loop drains and executes.
 */
public final class OperatorChannel implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(OperatorChannel.class.getName());

    /** Telegram behind two calls, so tests drive the channel without a network. */
    public interface Transport {
        /** Raw {@code getUpdates} body for updates after {@code offset}; long-polls up to ~25s. */
        String poll(long offset) throws IOException, InterruptedException;

        /**
         * One Bot API method, form-encoded. The status and body come back as Telegram sent them:
         * the caller decides what a 400 means (bad markup, or a harmless "message is not modified").
         */
        Response call(String method, Map<String, String> params) throws IOException, InterruptedException;

        /** Strips whatever secret the transport carries out of a message bound for a log. */
        default String redact(String text) { return text; }
    }

    /** What one Bot API call came back with. */
    public record Response(int status, String body) {
        boolean ok() { return status / 100 == 2; }
    }

    /** A halt whose reason starts with this is a venue mode, not an incident; /resume must not lift it. */
    static final String OBSERVE_REASON_PREFIX = "REAL_MODE=observe";
    /** Consecutive failed polls (5s apart) before the operator is told the channel is dead. */
    static final int POLL_FAILURES_BEFORE_ALERT = 12;
    /** How long a close-all confirmation button stays live. Long enough to read, short enough to be now. */
    static final Duration CLOSE_ALL_TTL = Duration.ofSeconds(60);
    static final String CLOSE_ALL_REASON = "operator: close all via Telegram";

    /**
     * The "/" menu in the Telegram client. Russian: it is read by the owner, typed in latin. Control
     * and the one fallback screen only (24.09): /book /pnl /queue /why still answer when typed, but
     * the lab bot's 📊 Панель is where those numbers live, and three places showing them was noise.
     */
    static final List<String[]> COMMANDS = List.of(
            new String[] {"status", "Состояние бота"},
            new String[] {"halt", "Стоп новых входов"},
            new String[] {"resume", "Снять халт"},
            new String[] {"close", "Закрыть: /close ADAUSDT или all"},
            new String[] {"menu", "Кнопки управления"},
            new String[] {"help", "Все команды"});

    private final Transport transport;
    private final String chatId;
    private final TradingHalt halt;
    private final AtomicReference<OperatorSnapshot> snapshot = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile long offset;
    private volatile long startedAtEpochSec;
    private volatile String venueTag = "";
    private volatile com.bot.exec.AlertSink alerts;
    /** REAL_MODE=observe: /resume may clear a drift latch, but entries stay off and the loop re-latches. */
    private volatile boolean observeMode;
    private final java.util.concurrent.ConcurrentLinkedQueue<CloseRequest> closes =
            new java.util.concurrent.ConcurrentLinkedQueue<>();
    /** Live close-all confirmations by token; removed on first use, so a second press finds nothing. */
    private final Map<String, PendingCloseAll> closeAllTokens = new ConcurrentHashMap<>();
    /**
     * Confirmed close-alls, expanded into closes by the main loop when it drains - not here. The
     * prompt lists the last published snapshot, which trails a fill by up to one reconcile pass
     * (30 s, minutes while passes fail); a coin bought in that gap was on no list and stayed open.
     */
    private final java.util.concurrent.ConcurrentLinkedQueue<CloseAllOrder> closeAllOrders =
            new java.util.concurrent.ConcurrentLinkedQueue<>();
    /** Live ▶️ confirmations by token, same single-use rule as close-all. */
    private final Map<String, PendingResume> resumeTokens = new ConcurrentHashMap<>();
    /** One fresh menu per burst of stale presses: five taps on a dead bot's menu used to buy five menus. */
    private Instant lastStaleMenuAt;
    private final SecureRandom random = new SecureRandom();
    /** The scanner's scanner_view.json; null = not configured, and /queue says so. */
    private volatile java.nio.file.Path scannerViewFile;
    private volatile Clock clock = Clock.systemUTC();
    private volatile TgOutbox.Sleeper sleeper = Thread::sleep;
    private volatile java.util.function.LongSupplier exchangeHold;
    private int pollFailures;
    private Thread thread;

    /** What the operator agreed to close: the symbols listed on the confirmation, nothing else. */
    private record PendingCloseAll(List<String> symbols, Instant expiresAt) {}

    /** A confirmed close-all: what the prompt showed, and when the operator said yes. */
    private record CloseAllOrder(List<String> shown, Instant confirmedAt) {}

    /** What the operator agreed to lift: the reason shown on the prompt, nothing else. */
    private record PendingResume(String reason, Instant expiresAt) {}

    /** How long ▶️'s confirmation lives; the same "read it, then decide now" as close-all. */
    static final Duration RESUME_TTL = Duration.ofSeconds(60);
    /** At most one fresh menu this often in answer to presses on an old boot's messages. */
    static final Duration STALE_MENU_EVERY = Duration.ofSeconds(30);
    /** The longest flood-control wait the poll thread sits out for one reply; longer, and it gives up. */
    static final long REPLY_FLOOD_WAIT_MAX_MS = 10_000L;

    OperatorChannel(Transport transport, String chatId, TradingHalt halt) {
        this.transport = Preconditions.notNull(transport, "transport");
        this.chatId = Preconditions.notBlank(chatId, "chatId");
        this.halt = Preconditions.notNull(halt, "halt");
    }

    /** Replies carry the venue, because demo and real share one chat and may share one token. */
    public OperatorChannel withVenueTag(String tag) {
        this.venueTag = tag == null ? "" : tag;
        return this;
    }

    /** Tells /resume not to promise entries on a venue whose mode forbids them. */
    public OperatorChannel withObserveMode(boolean on) {
        this.observeMode = on;
        return this;
    }

    /** Lets the channel say so when it cannot hear commands (409 from a second poller, 401, ...). */
    public OperatorChannel withAlerts(com.bot.exec.AlertSink sink) {
        this.alerts = sink;
        return this;
    }

    /**
     * Where the scanner writes what it saw ({@code <DATA_DIR>/scanner_view.json}). /queue and /why
     * read it on THIS thread - a local file, size-guarded, never the exchange - at each press.
     */
    public OperatorChannel withScannerView(java.nio.file.Path file) {
        this.scannerViewFile = file;
        return this;
    }

    private ScannerView.Read scannerView(Instant now) {
        return ScannerView.read(scannerViewFile, now);
    }

    /** Tests do not sit out a flood-control wait for real. */
    OperatorChannel withSleeper(TgOutbox.Sleeper sleeper) {
        this.sleeper = Preconditions.notNull(sleeper, "sleeper");
        return this;
    }

    /** Tests move time; production reads the wall clock. */
    OperatorChannel withClock(Clock clock) {
        this.clock = Preconditions.notNull(clock, "clock");
        return this;
    }

    /** Commands older than this process are replayed by Telegram for up to 24h; they are ignored. */
    void markStarted(Instant at) {
        this.startedAtEpochSec = at.getEpochSecond();
    }

    /** Null when Telegram is not configured — alerts are off then too, so nobody is listening. */
    public static OperatorChannel fromEnvironmentOrNull(TradingHalt halt) {
        return fromEnvironmentOrNull(System::getenv, halt);
    }

    static OperatorChannel fromEnvironmentOrNull(UnaryOperator<String> env, TradingHalt halt) {
        String token = env.apply("TELEGRAM_BOT_TOKEN");
        String chat = env.apply("TELEGRAM_CHAT_ID");
        if (token == null || token.isBlank() || chat == null || chat.isBlank()) return null;
        return new OperatorChannel(new HttpTransport(token.trim()), chat.trim(), halt);
    }

    /** Closes queued by /close and 🧯, with a confirmed 🧯 covering only what its prompt listed. */
    public java.util.List<CloseRequest> drainCloses() {
        return drainCloses(null);
    }

    /**
     * Closes queued by /close and 🧯; the main loop drains and executes them — never this thread.
     * A confirmed 🧯 is expanded HERE, on the loop, into the symbols its prompt showed plus every
     * symbol {@code heldNow} names - the loop's book and the exchange's listing at this moment. The
     * prompt is drawn from the last reconcile pass, and an entry filled since then (the very one
     * the owner may be reacting to) used to stay open behind a "the book is closed" reply.
     *
     * @param heldNow read only when a 🧯 is pending; null or a throwing supplier costs the extras
     *                only - the listed symbols are always queued
     */
    public java.util.List<CloseRequest> drainCloses(
            java.util.function.Supplier<? extends java.util.Collection<String>> heldNow) {
        if (closes.isEmpty() && closeAllOrders.isEmpty()) return java.util.List.of();
        java.util.List<CloseRequest> out = new java.util.ArrayList<>();
        CloseRequest c;
        while ((c = closes.poll()) != null) out.add(c);
        java.util.Collection<String> held = null;
        CloseAllOrder order;
        while ((order = closeAllOrders.poll()) != null) {
            if (held == null) held = heldNow(heldNow);
            java.util.LinkedHashSet<String> symbols = new java.util.LinkedHashSet<>(order.shown());
            symbols.addAll(held);
            if (symbols.size() > order.shown().size()) {
                List<String> shown = order.shown();
                List<String> extra = symbols.stream().filter(s -> !shown.contains(s)).toList();
                LOG.warning("[Operator] close-all also covers " + extra + ": held now, not on the prompt");
            }
            long epoch = order.confirmedAt().getEpochSecond();
            for (String symbol : symbols) {
                out.add(new CloseRequest("tg-closeall-" + symbol + "-" + epoch, symbol, CLOSE_ALL_REASON,
                        order.confirmedAt()));
            }
        }
        return out;
    }

    private static java.util.Collection<String> heldNow(
            java.util.function.Supplier<? extends java.util.Collection<String>> heldNow) {
        if (heldNow == null) return List.of();
        try {
            java.util.Collection<String> held = heldNow.get();
            return held == null ? List.of() : held;
        } catch (RuntimeException e) {
            LOG.warning("[Operator] close-all: what is held now could not be read (" + e.getMessage()
                    + ") - closing the symbols the prompt listed");
            return List.of();
        }
    }

    /** The main loop calls this; the command thread never touches the book or the exchange itself. */
    void publish(OperatorSnapshot s) {
        snapshot.set(s);
    }

    /**
     * The last snapshot the loop published, with the exchange hold read now; null before the first.
     * A hold reader that throws only costs the live figure - the published one stands.
     */
    OperatorSnapshot snapshot() {
        OperatorSnapshot s = snapshot.get();
        java.util.function.LongSupplier hold = exchangeHold;
        if (s == null || hold == null) return s;
        try {
            return s.withExchangeHold(Math.max(0L, hold.getAsLong()));
        } catch (RuntimeException e) {
            return s;
        }
    }

    /** The port's own hold clock (local state, no request), so /status can explain a parked loop. */
    public OperatorChannel withExchangeHold(java.util.function.LongSupplier holdMs) {
        this.exchangeHold = holdMs;
        return this;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        markStarted(Instant.now());
        thread = new Thread(this::loop, "operator-channel");
        thread.setDaemon(true);
        thread.start();
        LOG.info("[Operator] Telegram commands armed: /status /halt /resume /close /menu /help");
    }

    @Override public void close() {
        running.set(false);
        if (thread != null) thread.interrupt();
    }

    private void loop() {
        // On this thread, not the loop's: a slow Telegram must never delay a close.
        registerCommands();
        while (running.get()) {
            try {
                String body = transport.poll(offset);
                if (body != null) handleUpdates(body);
                if (pollFailures > 0) {
                    LOG.info("[Operator] Telegram polling is back after " + pollFailures + " failure(s)");
                    pollFailures = 0;
                }
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                // Telegram being down must never matter to trading - but a dead channel must not be
                // silent either: /halt typed at 3am into a 409 (another poller on this token) or a
                // 401 (rotated token) would simply vanish. Loud on the first failure and every
                // minute after, and once to the alert chat (sendMessage works even when getUpdates
                // does not).
                pollFailures++;
                if (pollFailures == 1 || pollFailures % POLL_FAILURES_BEFORE_ALERT == 0) {
                    LOG.warning("[Operator] poll failed (" + pollFailures + " in a row): "
                            + transport.redact(String.valueOf(e.getMessage()))
                            + " - /status /halt /resume are NOT being heard");
                }
                com.bot.exec.AlertSink sink = alerts;
                if (pollFailures == POLL_FAILURES_BEFORE_ALERT && sink != null) {
                    sink.warning("Operator commands unavailable",
                            "Telegram getUpdates has failed " + pollFailures + " times: "
                                    + transport.redact(String.valueOf(e.getMessage()))
                                    + ". A second bot polling this token (409) or a rotated token (401) "
                                    + "are the usual causes. Alerts still arrive; commands do not.");
                }
                try { Thread.sleep(5_000L); } catch (InterruptedException ie) { return; }
            }
        }
    }

    /** setMyCommands, once per boot. A failure costs the "/" menu only, never a command. */
    void registerCommands() {
        JSONArray list = new JSONArray();
        for (String[] c : COMMANDS) list.put(new JSONObject().put("command", c[0]).put("description", c[1]));
        Map<String, String> params = new LinkedHashMap<>();
        params.put("commands", list.toString());
        Response r = call("setMyCommands", params);
        if (r != null && !r.ok()) {
            LOG.warning("[Operator] setMyCommands refused, HTTP " + r.status() + ": " + transport.redact(r.body()));
        }
    }

    /** Package-private so a test can feed one body without a thread. */
    void handleUpdates(String body) throws IOException, InterruptedException {
        JSONObject root = new JSONObject(body);
        JSONArray updates = root.optJSONArray("result");
        if (updates == null) return;
        for (int i = 0; i < updates.length(); i++) {
            JSONObject u = updates.getJSONObject(i);
            offset = Math.max(offset, u.optLong("update_id", 0) + 1);
            try {
                JSONObject callback = u.optJSONObject("callback_query");
                if (callback != null) {
                    handleCallback(callback);
                    continue;
                }
                JSONObject msg = u.optJSONObject("message");
                if (msg != null) handleMessage(msg);
            } catch (RuntimeException e) {
                // One malformed update must not take the rest of the batch with it; the offset
                // above already moved past it, so it is not redelivered either.
                LOG.warning("[Operator] update " + u.optLong("update_id", 0) + " skipped: "
                        + transport.redact(String.valueOf(e.getMessage())));
            }
        }
    }

    private void handleMessage(JSONObject msg) {
        JSONObject chat = msg.optJSONObject("chat");
        String from = chat == null ? "" : String.valueOf(chat.opt("id"));
        String text = msg.optString("text", "").trim();
        long sentAt = msg.optLong("date", 0L);
        if (startedAtEpochSec > 0 && sentAt > 0 && sentAt < startedAtEpochSec - 5) {
            // Telegram redelivers unconfirmed updates for 24h. A /resume typed while the bot was
            // down must not clear the halt the NEW boot just latched. Offset advanced above.
            LOG.warning("[Operator] ignored a command sent " + (startedAtEpochSec - sentAt)
                    + "s before this process started: " + text);
            return;
        }
        if (!from.equals(chatId)) {
            // Not the operator. Say nothing back — a reply would confirm the bot exists.
            LOG.warning("[Operator] ignored a command from chat " + from + ": " + text);
            return;
        }
        if (text.startsWith("/")) {
            Reply reply = execute(text, clock.instant());
            sendHtml(tagged(reply.html()), reply.keyboard());
        }
    }

    /**
     * A button press. Same rules as a typed command: the operator's chat only (anyone else is not
     * answered at all), and a button on a message from before this process started does nothing -
     * Telegram queues presses made while the bot was down, and a ▶️ replayed at boot must not clear
     * the halt the new boot latched. Every press from the operator is answered, or the button spins.
     */
    private void handleCallback(JSONObject cb) {
        String callbackId = cb.optString("id", "");
        JSONObject message = cb.optJSONObject("message");
        JSONObject chat = message == null ? null : message.optJSONObject("chat");
        String from = chat == null ? "" : String.valueOf(chat.opt("id"));
        String data = cb.optString("data", "");
        if (!from.equals(chatId)) {
            LOG.warning("[Operator] ignored a button press from chat " + from + ": " + data);
            return;
        }
        long messageId = message.optLong("message_id", 0L);
        long messageDate = message.optLong("date", 0L);
        if (startedAtEpochSec > 0 && messageDate < startedAtEpochSec - 5) {
            // An inaccessible message reports date 0 - equally unknowable, equally ignored.
            LOG.warning("[Operator] ignored a button on a message from before this process started: " + data);
            answerCallback(callbackId, "Кнопка от прошлого запуска — вот свежее меню");
            // Taps made while the bot restarted arrive together at boot; each is answered, but one
            // fresh menu serves the whole burst (and keeps it out of the boot's flood of alerts).
            Instant now = clock.instant();
            if (lastStaleMenuAt == null || !now.isBefore(lastStaleMenuAt.plus(STALE_MENU_EVERY))) {
                lastStaleMenuAt = now;
                sendHtml(tagged(OperatorViews.menu(snapshot(), halt.reason(), now)), OperatorViews.MENU);
            }
            return;
        }
        Instant now = clock.instant();
        CallbackOutcome outcome = onButton(data, now);
        answerCallback(callbackId, outcome.toast());
        if (outcome.screen() != null) {
            editHtml(messageId, tagged(outcome.screen().html()), outcome.screen().keyboard());
        }
    }

    /** What a press does: a short toast, and the screen the pressed message turns into (null = unchanged). */
    record CallbackOutcome(String toast, Reply screen) {}

    /** The whole button language. Every button runs the code its command runs. */
    CallbackOutcome onButton(String data, Instant now) {
        OperatorSnapshot s = snapshot();
        switch (data) {
            case OperatorViews.CB_MENU:
                return new CallbackOutcome("", new Reply(OperatorViews.menu(s, halt.reason(), now), OperatorViews.MENU));
            case OperatorViews.CB_STATUS:
                return new CallbackOutcome("", withMenu(OperatorViews.status(s, halt.reason(), now)));
            // 📒 💰 ⏳ and the coin buttons left the menu on 24.09; old messages still carry them,
            // so they keep drawing their read-only screens rather than spinning.
            case OperatorViews.CB_BOOK:
                return new CallbackOutcome("", withMenu(OperatorViews.book(s, now)));
            case OperatorViews.CB_PNL:
                return new CallbackOutcome("", withMenu(OperatorViews.pnl(s, now)));
            case OperatorViews.CB_QUEUE:
                return new CallbackOutcome("", withMenu(OperatorViews.queue(s, scannerView(now), now)));
            case OperatorViews.CB_HALT:
                return new CallbackOutcome("⏸ Халт", withMenu(haltCommand(now)));
            case OperatorViews.CB_RESUME:
                return new CallbackOutcome("", resumePrompt(now));
            case OperatorViews.CB_CLOSE_ALL:
                return new CallbackOutcome("", closeAllPrompt(now));
            default:
                break;
        }
        if (data.startsWith(OperatorViews.CB_WHY)) {
            // Read-only: the symbol only picks which screen to draw, and garbage draws "no data".
            String symbol = data.substring(OperatorViews.CB_WHY.length());
            if (!symbol.matches("[A-Z0-9]{2,20}")) return new CallbackOutcome("Неизвестная монета", null);
            return new CallbackOutcome("", withMenu(OperatorViews.why(symbol, s, scannerView(now), now)));
        }
        if (data.startsWith(OperatorViews.CB_CLOSE_ALL_YES)) {
            return confirmCloseAll(data.substring(OperatorViews.CB_CLOSE_ALL_YES.length()), now);
        }
        if (data.startsWith(OperatorViews.CB_RESUME_YES)) {
            return confirmResume(data.substring(OperatorViews.CB_RESUME_YES.length()), now);
        }
        if (data.startsWith(OperatorViews.CB_RESUME_NO)) {
            PendingResume dropped = resumeTokens.remove(data.substring(OperatorViews.CB_RESUME_NO.length()));
            if (dropped == null) return new CallbackOutcome("Уже обработано", null);
            return new CallbackOutcome("Отменено", withMenu("✖️ <b>Отменено</b> — халт остаётся."));
        }
        if (data.startsWith(OperatorViews.CB_CLOSE_ALL_NO)) {
            PendingCloseAll dropped = closeAllTokens.remove(data.substring(OperatorViews.CB_CLOSE_ALL_NO.length()));
            if (dropped == null) return new CallbackOutcome("Уже обработано", null);
            return new CallbackOutcome("Отменено", withMenu("✖️ <b>Отменено</b> — ничего не закрыто."));
        }
        LOG.warning("[Operator] unknown button: " + data);
        return new CallbackOutcome("Неизвестная кнопка — /menu", null);
    }

    private static Reply withMenu(String html) {
        return new Reply(html, OperatorViews.MENU);
    }

    /** The whole command language. Returns the reply. */
    Reply execute(String text, Instant now) {
        String[] words = text.split("\\s+", 3);
        String cmd = words[0].toLowerCase(Locale.ROOT);
        int at = cmd.indexOf('@');
        if (at > 0) cmd = cmd.substring(0, at);          // "/status@MyBot" in group chats
        OperatorSnapshot s = snapshot();
        switch (cmd) {
            case "/start":
            case "/menu":
                return new Reply(OperatorViews.menu(s, halt.reason(), now), OperatorViews.MENU);
            case "/status":
                return Reply.of(OperatorViews.status(s, halt.reason(), now));
            // Not advertised since 24.09 (the lab bot's panel shows these), still answered when
            // typed: read-only, and a remembered command should not meet "unknown command".
            case "/book":
                return Reply.of(OperatorViews.book(s, now));
            case "/pnl":
                return Reply.of(OperatorViews.pnl(s, now));
            case "/queue":
                return Reply.of(OperatorViews.queue(s, scannerView(now), now));
            case "/why": {
                String arg = words.length > 1 ? words[1].toUpperCase(Locale.ROOT) : "";
                String symbol = resolveSymbol(arg, s);
                if (symbol == null) {
                    // No coin buttons since 24.09: they were the /why buttons the owner removed.
                    return Reply.of("🟠 <b>Какую монету?</b>\n/why ADA — почему бот вошёл");
                }
                return Reply.of(OperatorViews.why(symbol, s, scannerView(now), now));
            }
            case "/halt":
                return Reply.of(haltCommand(now));
            case "/resume":
                return Reply.of(resumeCommand());
            case "/close": {
                String arg = words.length > 1 ? words[1].toUpperCase(Locale.ROOT) : "";
                if (arg.equals("ALL")) return closeAllPrompt(now);
                String symbol = resolveSymbol(arg, s);
                if (symbol == null) {
                    return Reply.of("🟠 <b>Какую закрыть?</b>\n/close ADAUSDT — одну позицию\n/close all — всё, с подтверждением");
                }
                closes.add(new CloseRequest("tg-close-" + symbol + "-" + now.getEpochSecond(),
                        symbol, "operator via Telegram", now));
                return Reply.of("⏳ <b>" + esc(symbol) + " — в очереди на закрытие</b>\n"
                        + "По рынку, reduce-only; стоп и тейк\nснимутся вместе с ней.\n"
                        + "Обычно — за секунду. При паузе биржи\n(см. /status) — после паузы.\n"
                        + "<i>Неудача повторяется ~10 мин;\nрестарт бота очищает очередь.</i>");
            }
            case "/help":
                return Reply.of(OperatorViews.help());
            default:
                return Reply.of("🟠 Не знаю такой команды. Список: /help");
        }
    }

    /**
     * "ADAUSDT" as typed, or "ada" when the published book holds ADAUSDT - the phone keyboard is
     * lowercase and short. Anything that is not a plausible symbol is null: garbage queues nothing.
     */
    static String resolveSymbol(String arg, OperatorSnapshot s) {
        if (arg == null || !arg.matches("[A-Z0-9]{2,20}")) return null;
        if (s != null && !arg.endsWith("USDT")) {
            String withQuote = arg + "USDT";
            for (OperatorSnapshot.Position p : s.positions()) {
                if (p.symbol().equals(withQuote)) return withQuote;
            }
        }
        return arg.length() >= 5 ? arg : null;
    }

    private String haltCommand(Instant now) {
        if (halt.isHalted()) {
            return "⏸ <b>Уже на халте</b>\nПричина: " + esc(halt.reason().orElse("?"));
        }
        halt.halt("operator via Telegram", now);
        return "⏸ <b>Халт включён</b>\nНовые входы остановлены.\n"
                + "Выходы, стопы и тейки работают.\nСнять: /resume";
    }

    private String resumeCommand() {
        if (!halt.isHalted()) return "🟢 <b>Халта нет</b> — снимать нечего.";
        String was = halt.reason().orElse("?");
        if (was.startsWith(OBSERVE_REASON_PREFIX)) {
            // Observe is a venue mode set by the operator's environment, not an incident a
            // command may wave away: clearing it here would arm real entries from a phone.
            return "👁 <b>Это режим наблюдения, не халт</b>\n"
                    + "Входы включает только REAL_MODE=trade\nв окружении и перезапуск.";
        }
        halt.clear();
        if (observeMode) {
            // The loop re-latches observe on its next tick; promising entries here was a lie.
            return "🟢 <b>Халт снят</b>\nБыл: " + esc(was) + "\n"
                    + "👁 Площадка в REAL_MODE=observe:\nвходы остаются выключены,\n"
                    + "наблюдение вернётся на следующем такте.";
        }
        return "🟢 <b>Халт снят</b>\nБыл: " + esc(was) + "\n"
                + "Входы — со следующего сигнала;\nсканер предложит их на ближайшем\nчасовом проходе.";
    }

    /**
     * ▶️ asks first. The button sits under every screen, one row below ⏸ and 📒, and some latches
     * never lift on their own ("close abandoned after 8 attempts", the owner's own overnight halt):
     * a slipped thumb re-armed entries next to a position the bot could not close. Typed /resume is
     * deliberate and stays one step. Nothing to lift, or observe: answered at once, no token.
     */
    private Reply resumePrompt(Instant now) {
        java.util.Optional<String> reason = halt.reason();
        if (!halt.isHalted() || reason.isEmpty() || reason.get().startsWith(OBSERVE_REASON_PREFIX)) {
            return withMenu(resumeCommand());
        }
        resumeTokens.values().removeIf(p -> now.isAfter(p.expiresAt()));
        String token = newToken();
        resumeTokens.put(token, new PendingResume(reason.get(), now.plus(RESUME_TTL)));
        List<List<Button>> keyboard = List.of(List.of(
                new Button("✅ Снять халт", OperatorViews.CB_RESUME_YES + token),
                new Button("✖️ Отмена", OperatorViews.CB_RESUME_NO + token)));
        return new Reply(OperatorViews.resumePrompt(reason.get(), observeMode, RESUME_TTL.toSeconds()), keyboard);
    }

    /** ✅ lifts exactly the halt the prompt showed; a different latch since then is not waved away unseen. */
    private CallbackOutcome confirmResume(String token, Instant now) {
        PendingResume p = resumeTokens.remove(token);
        if (p == null) return new CallbackOutcome("Уже обработано", null);
        if (now.isAfter(p.expiresAt())) {
            return new CallbackOutcome("Время вышло",
                    withMenu("🟠 <b>Подтверждение истекло</b> — халт остаётся.\nЗаново: ▶️ или /resume"));
        }
        String current = halt.reason().orElse(null);
        if (current != null && !current.equals(p.reason())) {
            return new CallbackOutcome("Причина сменилась",
                    withMenu("🟠 <b>Халт уже другой</b> — не снят.\nСейчас: " + esc(current)
                            + "\nПроверь /status и снова ▶️"));
        }
        return new CallbackOutcome("", withMenu(resumeCommand()));
    }

    private String newToken() {
        byte[] raw = new byte[8];
        random.nextBytes(raw);
        return HexFormat.of().formatHex(raw);
    }

    /**
     * The 🧯 confirmation: lists what the published book holds and issues a single-use token that
     * lives {@link #CLOSE_ALL_TTL}. The list is what the owner reads; the loop closes that AND
     * whatever it holds when it executes (see {@link #drainCloses(java.util.function.Supplier)}).
     */
    private Reply closeAllPrompt(Instant now) {
        OperatorSnapshot s = snapshot();
        if (s == null) {
            return withMenu("🟠 <b>Книги ещё нет</b> — цикл не прислал данных.\nЗакрывать пока нечего показать.");
        }
        if (s.positions().isEmpty()) {
            return withMenu("📒 <b>Книга пуста</b> — закрывать нечего.\n<i>По сверке " + TgFormat.ago(s.at(), now)
                    + ". Только что открытую —\n/close ADA.</i>");
        }
        closeAllTokens.values().removeIf(p -> now.isAfter(p.expiresAt()));
        String token = newToken();
        List<String> symbols = s.positions().stream().map(OperatorSnapshot.Position::symbol).toList();
        closeAllTokens.put(token, new PendingCloseAll(symbols, now.plus(CLOSE_ALL_TTL)));
        List<List<Button>> keyboard = List.of(List.of(
                new Button("✅ Да, закрыть " + symbols.size(), OperatorViews.CB_CLOSE_ALL_YES + token),
                new Button("✖️ Отмена", OperatorViews.CB_CLOSE_ALL_NO + token)));
        return new Reply(OperatorViews.closeAllPrompt(s.positions(), now, CLOSE_ALL_TTL.toSeconds())
                + OperatorViews.closeDelayNote(s, now), keyboard);
    }

    /**
     * Confirm: one close-all order into the queue the main loop drains; the loop turns it into one
     * reduce-only CloseRequest per symbol - the listed ones and anything it holds by then - and
     * executes them with its retries. The token is removed before anything is queued, so a double
     * tap - or Telegram delivering the press twice - queues the book once.
     */
    private CallbackOutcome confirmCloseAll(String token, Instant now) {
        PendingCloseAll p = closeAllTokens.remove(token);
        if (p == null) return new CallbackOutcome("Уже обработано", null);
        if (now.isAfter(p.expiresAt())) {
            return new CallbackOutcome("Время вышло",
                    withMenu("🟠 <b>Подтверждение истекло</b> — ничего не закрыто.\nЗаново: /close all"));
        }
        closeAllOrders.add(new CloseAllOrder(p.symbols(), now));
        LOG.warning("[Operator] close-all confirmed via Telegram: " + p.symbols() + " plus anything held at execution");
        String list = String.join(", ", p.symbols().stream().map(TgFormat::shortSymbol).map(TgFormat::esc).toList());
        String delay = OperatorViews.closeDelayNote(snapshot(), now);
        return new CallbackOutcome("Принято: " + p.symbols().size() + " в очереди",
                withMenu("🧯 <b>В очереди на закрытие: " + p.symbols().size() + "</b>\n" + list
                        + "\nПлюс открытое до исполнения."
                        + (delay.isEmpty() ? "\nИсполнит основной цикл за секунды." : delay)
                        + "\nИтог каждой придёт в чат; проверить: /book"));
    }

    // ─── Sending ────────────────────────────────────────────────────────────────────────────

    private String tagged(String html) {
        return venueTag.isEmpty() ? html : "[" + esc(venueTag) + "] " + html;
    }

    /**
     * sendMessage with parse_mode=HTML, split at 4096 with the keyboard on the last part. A 400 is
     * almost always markup Telegram would not parse - the message is then sent once more as plain
     * text, because an operator who hears nothing is worse than one who sees a stray tag.
     *
     * @return the message id of the last part, or 0 when Telegram did not say
     */
    long sendHtml(String html, List<List<Button>> keyboard) {
        return send(html, keyboard, REPLY_FLOOD_WAIT_MAX_MS).lastMessageId();
    }

    /**
     * A trade line from the outbox's thread, stamped with the venue like every reply. Delivered
     * only when Telegram took every part; otherwise the last refusal comes back as it was, so the
     * outbox can sit out a 429's own retry_after and stop at once on a 401/403. Safe off the poll
     * thread: it touches nothing but the transport, which is one thread-safe HTTP client.
     */
    TgOutbox.Attempt deliverHtml(String html) {
        Sent sent = send(tagged(html), null, 0L);
        if (sent.allDelivered()) return TgOutbox.Attempt.DELIVERED;
        Response f = sent.lastFailure();
        return f == null ? TgOutbox.Attempt.NO_ANSWER : new TgOutbox.Attempt(false, f.status(), f.body());
    }

    /** {@code lastFailure} is the last part Telegram refused, null when every refusal was a network failure. */
    private record Sent(long lastMessageId, boolean allDelivered, Response lastFailure) {}

    /**
     * @param floodWaitMaxMs how long this caller may sit out a 429's retry_after for one more try;
     *                       0 = hand the 429 back (the outbox waits on its own thread)
     */
    private Sent send(String html, List<List<Button>> keyboard, long floodWaitMaxMs) {
        List<String> parts = TgFormat.split(html, TgFormat.MAX_MESSAGE_CHARS);
        long lastId = 0;
        boolean all = true;
        Response lastFailure = null;
        for (int i = 0; i < parts.size(); i++) {
            List<List<Button>> kb = i == parts.size() - 1 ? keyboard : null;
            Map<String, String> params = messageParams(parts.get(i), kb);
            Response r = callPatiently("sendMessage", params, floodWaitMaxMs);
            if (r != null && r.status() == 400) {
                LOG.warning("[Operator] HTML refused (" + transport.redact(r.body()) + ") - resending as plain text");
                params.remove("parse_mode");
                params.put("text", TgFormat.plain(parts.get(i)));
                r = callPatiently("sendMessage", params, floodWaitMaxMs);
            }
            if (r != null && r.ok()) {
                lastId = messageId(r.body());
            } else {
                all = false;
                if (r != null) {
                    lastFailure = r;
                    LOG.warning("[Operator] reply not delivered, HTTP " + r.status());
                }
            }
        }
        return new Sent(lastId, all, lastFailure);
    }

    /**
     * One call, and on Telegram's flood control (429) one more after the wait it names - when that
     * wait fits {@code floodWaitMaxMs}. A burst (a closed book's exit lines next to the kill
     * switch's alerts, all in one chat) used to cost the operator's reply outright: /status
     * pressed then got nothing but a log line. AlertSink learned the same on 06.09.
     */
    private Response callPatiently(String method, Map<String, String> params, long floodWaitMaxMs) {
        Response r = call(method, params);
        if (r == null || r.status() != 429 || floodWaitMaxMs <= 0) return r;
        long wait = retryAfterMillis(r.body());
        if (wait > floodWaitMaxMs) {
            LOG.warning("[Operator] Telegram flood control asks for " + wait + " ms - " + method + " not retried");
            return r;
        }
        LOG.warning("[Operator] Telegram flood control, waiting " + wait + " ms before one more " + method);
        try {
            sleeper.sleep(wait);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return r;
        }
        return call(method, params);
    }

    /**
     * Telegram's flood control names its own wait: {@code "parameters":{"retry_after":N}}. Same
     * reading as AlertSink's (a nested class of another package, so a copy): 1 s floor, 30 s cap.
     */
    static long retryAfterMillis(String body) {
        if (body == null) return 1_000L;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\"retry_after\"\\s*:\\s*(\\d+)").matcher(body);
        if (!m.find()) return 1_000L;
        try {
            return Math.max(1_000L, Math.min(30_000L, Long.parseLong(m.group(1)) * 1_000L));
        } catch (NumberFormatException e) {
            return 1_000L;
        }
    }

    /**
     * Turns the pressed message into the next screen. "message is not modified" (the same screen
     * pressed twice) is success. Any other refusal gets one plain-text edit, and if the message
     * cannot be edited at all (too old, deleted) the screen arrives as a new message instead.
     */
    void editHtml(long messageId, String html, List<List<Button>> keyboard) {
        List<String> parts = TgFormat.split(html, TgFormat.MAX_MESSAGE_CHARS);
        boolean single = parts.size() == 1;
        Map<String, String> params = messageParams(parts.get(0), single ? keyboard : null);
        params.put("message_id", Long.toString(messageId));
        Response r = callPatiently("editMessageText", params, REPLY_FLOOD_WAIT_MAX_MS);
        if (r != null && !r.ok() && !notModified(r)) {
            params.remove("parse_mode");
            params.put("text", TgFormat.plain(parts.get(0)));
            r = callPatiently("editMessageText", params, REPLY_FLOOD_WAIT_MAX_MS);
            if (r != null && !r.ok() && !notModified(r)) {
                LOG.warning("[Operator] could not edit message " + messageId + " (HTTP " + r.status()
                        + ") - sending the screen as a new message");
                sendHtml(html, keyboard);
                return;
            }
        }
        if (!single) sendHtml(String.join("\n", parts.subList(1, parts.size())), keyboard);
    }

    private static boolean notModified(Response r) {
        return r.status() == 400 && r.body() != null && r.body().contains("message is not modified");
    }

    /** Every press is answered, or the button spins on the phone; the toast is at most 200 characters. */
    void answerCallback(String callbackId, String toast) {
        if (callbackId == null || callbackId.isEmpty()) return;
        Map<String, String> params = new LinkedHashMap<>();
        params.put("callback_query_id", callbackId);
        if (toast != null && !toast.isEmpty()) params.put("text", toast.length() > 200 ? toast.substring(0, 200) : toast);
        call("answerCallbackQuery", params);
    }

    private Map<String, String> messageParams(String text, List<List<Button>> keyboard) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("chat_id", chatId);
        params.put("text", text);
        params.put("parse_mode", "HTML");
        params.put("disable_web_page_preview", "true");
        if (keyboard != null) params.put("reply_markup", keyboardJson(keyboard));
        return params;
    }

    static String keyboardJson(List<List<Button>> keyboard) {
        JSONArray rows = new JSONArray();
        for (List<Button> row : keyboard) {
            JSONArray r = new JSONArray();
            for (Button b : row) r.put(new JSONObject().put("text", b.text()).put("callback_data", b.data()));
            rows.put(r);
        }
        return new JSONObject().put("inline_keyboard", rows).toString();
    }

    private static long messageId(String body) {
        try {
            JSONObject result = new JSONObject(body).optJSONObject("result");
            return result == null ? 0 : result.optLong("message_id", 0);
        } catch (RuntimeException e) {
            return 0;
        }
    }

    /**
     * One Bot API call that never throws: this thread's failures stay here. Null when the call did
     * not complete (network, or interrupted - the flag is restored so the poll loop exits).
     */
    private Response call(String method, Map<String, String> params) {
        try {
            return transport.call(method, params);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (IOException | RuntimeException e) {
            LOG.warning("[Operator] " + method + " failed: " + transport.redact(String.valueOf(e.getMessage())));
            return null;
        }
    }

    /** Real Telegram. */
    static final class HttpTransport implements Transport {
        private final String token;
        private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        /** The token is a URL path segment; an exception that quotes the URL quotes the token. */
        @Override public String redact(String text) {
            if (text == null) return "null";
            return token.isEmpty() ? text : text.replace(token, "<token>");
        }

        HttpTransport(String token) {
            this.token = token;
        }

        @Override public String poll(long offset) throws IOException, InterruptedException {
            // Buttons arrive as callback_query; without it in allowed_updates a press never reaches us.
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.telegram.org/bot" + token
                            + "/getUpdates?timeout=25&allowed_updates="
                            + URLEncoder.encode("[\"message\",\"callback_query\"]", StandardCharsets.UTF_8)
                            + "&offset=" + offset))
                    .timeout(Duration.ofSeconds(40))
                    .GET().build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("getUpdates HTTP " + response.statusCode());
            }
            return response.body();
        }

        @Override public Response call(String method, Map<String, String> params)
                throws IOException, InterruptedException {
            List<String> pairs = new ArrayList<>();
            for (Map.Entry<String, String> e : params.entrySet()) {
                pairs.add(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "="
                        + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
            }
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.telegram.org/bot" + token + "/" + method))
                    .timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(String.join("&", pairs))).build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    /** For a test: the live close-all tokens. */
    int liveCloseAllTokens() {
        return closeAllTokens.size();
    }
}
