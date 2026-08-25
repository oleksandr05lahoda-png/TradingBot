package com.bot.app;

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
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;

/**
 * The operator's hands, over Telegram: {@code /status}, {@code /halt}, {@code /resume}. Until now a
 * halt cleared only with a redeploy and "what is the bot doing" meant opening the hosting console;
 * three restarts on 21.08 were for exactly that. Commands are accepted from the configured chat
 * only, and nothing here can open a position: status is a snapshot the main loop publishes, halt
 * sets the same latch the reconciler uses, resume clears it — the one place that ever does.
 */
public final class OperatorChannel implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(OperatorChannel.class.getName());

    /** Telegram behind two calls, so tests drive the channel without a network. */
    public interface Transport {
        /** Raw {@code getUpdates} body for updates after {@code offset}; long-polls up to ~25s. */
        String poll(long offset) throws IOException, InterruptedException;

        void send(String text) throws IOException, InterruptedException;
    }

    /** A halt whose reason starts with this is a venue mode, not an incident; /resume must not lift it. */
    static final String OBSERVE_REASON_PREFIX = "REAL_MODE=observe";
    /** Consecutive failed polls (5s apart) before the operator is told the channel is dead. */
    static final int POLL_FAILURES_BEFORE_ALERT = 12;

    private final Transport transport;
    private final String chatId;
    private final TradingHalt halt;
    private final AtomicReference<String> status = new AtomicReference<>("status not published yet");
    private final AtomicReference<Instant> statusAt = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile long offset;
    private volatile long startedAtEpochSec;
    private volatile String venueTag = "";
    private volatile com.bot.exec.AlertSink alerts;
    private final java.util.concurrent.ConcurrentLinkedQueue<CloseRequest> closes =
            new java.util.concurrent.ConcurrentLinkedQueue<>();
    private int pollFailures;
    private Thread thread;

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

    /** Lets the channel say so when it cannot hear commands (409 from a second poller, 401, ...). */
    public OperatorChannel withAlerts(com.bot.exec.AlertSink sink) {
        this.alerts = sink;
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
        return new OperatorChannel(new HttpTransport(token.trim(), chat.trim()), chat.trim(), halt);
    }

    /** Closes queued by /close; the main loop drains and executes them — never this thread. */
    public java.util.List<CloseRequest> drainCloses() {
        if (closes.isEmpty()) return java.util.List.of();
        java.util.List<CloseRequest> out = new java.util.ArrayList<>();
        CloseRequest c;
        while ((c = closes.poll()) != null) out.add(c);
        return out;
    }

    /** The main loop calls this; the command thread never touches the book itself. */
    public void publishStatus(String text, Instant at) {
        status.set(text);
        statusAt.set(at);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        markStarted(Instant.now());
        thread = new Thread(this::loop, "operator-channel");
        thread.setDaemon(true);
        thread.start();
        LOG.info("[Operator] Telegram commands armed: /status /halt /resume /close /help");
    }

    @Override public void close() {
        running.set(false);
        if (thread != null) thread.interrupt();
    }

    private void loop() {
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
                    LOG.warning("[Operator] poll failed (" + pollFailures + " in a row): " + e.getMessage()
                            + " - /status /halt /resume are NOT being heard");
                }
                com.bot.exec.AlertSink sink = alerts;
                if (pollFailures == POLL_FAILURES_BEFORE_ALERT && sink != null) {
                    sink.warning("Operator commands unavailable",
                            "Telegram getUpdates has failed " + pollFailures + " times: " + e.getMessage()
                                    + ". A second bot polling this token (409) or a rotated token (401) "
                                    + "are the usual causes. Alerts still arrive; commands do not.");
                }
                try { Thread.sleep(5_000L); } catch (InterruptedException ie) { return; }
            }
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
            JSONObject msg = u.optJSONObject("message");
            if (msg == null) continue;
            JSONObject chat = msg.optJSONObject("chat");
            String from = chat == null ? "" : String.valueOf(chat.opt("id"));
            String text = msg.optString("text", "").trim();
            long sentAt = msg.optLong("date", 0L);
            if (startedAtEpochSec > 0 && sentAt > 0 && sentAt < startedAtEpochSec - 5) {
                // Telegram redelivers unconfirmed updates for 24h. A /resume typed while the bot was
                // down must not clear the halt the NEW boot just latched. Offset advanced above.
                LOG.warning("[Operator] ignored a command sent " + (startedAtEpochSec - sentAt)
                        + "s before this process started: " + text);
                continue;
            }
            if (!from.equals(chatId)) {
                // Not the operator. Say nothing back — a reply would confirm the bot exists.
                LOG.warning("[Operator] ignored a command from chat " + from + ": " + text);
                continue;
            }
            if (text.startsWith("/")) {
                String reply = execute(text, Instant.now());
                transport.send(venueTag.isEmpty() ? reply : "[" + venueTag + "] " + reply);
            }
        }
    }

    /** The whole command language. Returns the reply. */
    String execute(String text, Instant now) {
        String cmd = text.split("\\s+", 2)[0].toLowerCase(Locale.ROOT);
        int at = cmd.indexOf('@');
        if (at > 0) cmd = cmd.substring(0, at);          // "/status@MyBot" in group chats
        switch (cmd) {
            case "/status": {
                Instant t = statusAt.get();
                String age = t == null ? "" : " (as of " + Duration.between(t, now).toSeconds() + "s ago)";
                return status.get() + age;
            }
            case "/halt": {
                if (halt.isHalted()) {
                    return "already halted: " + halt.reason().orElse("?");
                }
                halt.halt("operator via Telegram", now);
                return "HALTED - no new positions will be opened. Closes, stops and takes keep working. /resume to lift.";
            }
            case "/resume": {
                if (!halt.isHalted()) return "not halted - nothing to clear.";
                String was = halt.reason().orElse("?");
                if (was.startsWith(OBSERVE_REASON_PREFIX)) {
                    // Observe is a venue mode set by the operator's environment, not an incident a
                    // command may wave away: clearing it here would arm real entries from a phone.
                    return "this is REAL_MODE=observe, not a halt. Set REAL_MODE=trade in the "
                            + "environment and restart to enable entries.";
                }
                halt.clear();
                return "halt cleared. It was: " + was + "\nEntries resume at the next signal; the scanner re-checks within 5 min.";
            }
            case "/close": {
                String symbol = text.split("\\s+", 3).length > 1
                        ? text.split("\\s+", 3)[1].toUpperCase(Locale.ROOT) : "";
                if (!symbol.matches("[A-Z0-9]{5,20}")) {
                    return "usage: /close SYMBOL   (e.g. /close ADAUSDT)";
                }
                closes.add(new CloseRequest("tg-close-" + symbol + "-" + now.getEpochSecond(),
                        symbol, "operator via Telegram", now));
                return "queued: closing " + symbol + " reduce-only. The loop executes it within a "
                        + "second and cancels its stop and take; if there is no such position you "
                        + "will see 'already flat' in the log.";
            }
            case "/help":
                return "/status - what the bot holds and whether it is halted\n"
                        + "/halt - stop opening new positions (closes keep working)\n"
                        + "/resume - lift a halt after you have looked at the reason\n"
                        + "/close SYMBOL - flatten one position through the bot (better than the app: "
                        + "the stop and take are cancelled with it)";
            default:
                return "unknown command. /help";
        }
    }

    /** Real Telegram. */
    static final class HttpTransport implements Transport {
        private final String token;
        private final String chatId;
        private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        HttpTransport(String token, String chatId) {
            this.token = token;
            this.chatId = chatId;
        }

        @Override public String poll(long offset) throws IOException, InterruptedException {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.telegram.org/bot" + token
                            + "/getUpdates?timeout=25&allowed_updates=%5B%22message%22%5D&offset=" + offset))
                    .timeout(Duration.ofSeconds(40))
                    .GET().build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("getUpdates HTTP " + response.statusCode());
            }
            return response.body();
        }

        @Override public void send(String text) throws IOException, InterruptedException {
            String form = "chat_id=" + URLEncoder.encode(chatId, StandardCharsets.UTF_8)
                    + "&text=" + URLEncoder.encode(text, StandardCharsets.UTF_8);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.telegram.org/bot" + token + "/sendMessage"))
                    .timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(form)).build();
            http.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
