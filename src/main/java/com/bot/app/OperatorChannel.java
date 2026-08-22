package com.bot.app;

import com.bot.core.Preconditions;
import com.bot.exec.TradingHalt;
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

    private final Transport transport;
    private final String chatId;
    private final TradingHalt halt;
    private final AtomicReference<String> status = new AtomicReference<>("status not published yet");
    private final AtomicReference<Instant> statusAt = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile long offset;
    private Thread thread;

    OperatorChannel(Transport transport, String chatId, TradingHalt halt) {
        this.transport = Preconditions.notNull(transport, "transport");
        this.chatId = Preconditions.notBlank(chatId, "chatId");
        this.halt = Preconditions.notNull(halt, "halt");
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

    /** The main loop calls this; the command thread never touches the book itself. */
    public void publishStatus(String text, Instant at) {
        status.set(text);
        statusAt.set(at);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        thread = new Thread(this::loop, "operator-channel");
        thread.setDaemon(true);
        thread.start();
        LOG.info("[Operator] Telegram commands armed: /status /halt /resume /help");
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
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                // Telegram being down must never matter to trading; try again after a pause.
                LOG.fine("[Operator] poll failed: " + e.getMessage());
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
            if (!from.equals(chatId)) {
                // Not the operator. Say nothing back — a reply would confirm the bot exists.
                LOG.warning("[Operator] ignored a command from chat " + from + ": " + text);
                continue;
            }
            if (text.startsWith("/")) transport.send(execute(text, Instant.now()));
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
                halt.clear();
                return "halt cleared. It was: " + was + "\nEntries resume at the next signal; the scanner re-checks within 5 min.";
            }
            case "/help":
                return "/status - what the bot holds and whether it is halted\n"
                        + "/halt - stop opening new positions (closes keep working)\n"
                        + "/resume - lift a halt after you have looked at the reason";
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
