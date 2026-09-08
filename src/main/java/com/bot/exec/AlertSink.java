package com.bot.exec;

import com.bot.core.Preconditions;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Destination for operator alerts — reconciliation drift, a fired dead-man's switch. The default
 * writes to the log; {@link Telegram} adds a push when the environment supplies credentials.
 */
public interface AlertSink {

    enum Severity { INFO, WARNING, CRITICAL }

    void alert(Severity severity, String title, String message);

    default void critical(String title, String message) { alert(Severity.CRITICAL, title, message); }

    default void warning(String title, String message) { alert(Severity.WARNING, title, message); }

    default void info(String title, String message) { alert(Severity.INFO, title, message); }

    /** The always-available floor. */
    final class Logging implements AlertSink {

        static final String LOGGER_NAME = "Alert";

        private static final Logger LOG = Logger.getLogger(LOGGER_NAME);

        @Override public void alert(Severity severity, String title, String message) {
            Level level = switch (severity) {
                case CRITICAL -> Level.SEVERE;
                case WARNING -> Level.WARNING;
                case INFO -> Level.INFO;
            };
            LOG.log(level, "[" + severity + "] " + title + " — " + message);
        }
    }

    /**
     * Telegram push, from {@code TELEGRAM_BOT_TOKEN} and {@code TELEGRAM_CHAT_ID}. Delivery failures
     * are swallowed on purpose: the alert path must never throw into the trading path it reports on.
     */
    final class Telegram implements AlertSink {

        static final String LOGGER_NAME = "Alert.Telegram";

        static final String TOKEN_VAR = "TELEGRAM_BOT_TOKEN";
        static final String CHAT_ID_VAR = "TELEGRAM_CHAT_ID";

        private static final Logger LOG = Logger.getLogger(LOGGER_NAME);

        /** One-shot: "pushes are off" is a fact about the process, not about one sink. */
        private static final AtomicBoolean DISABLED_WARNING_LOGGED = new AtomicBoolean();

        private final String token;
        private final String chatId;
        /** WARNING and CRITICAL get two more tries; the alert path is what replaces the log nobody reads. */
        private static final int RETRIES_FOR_SERIOUS = 3;

        /** Telegram refuses a text longer than this with HTTP 400; a summary over 12 symbols reached it. */
        static final int MAX_TEXT_CHARS = 4096;

        /** What one HTTP send came back with; the body carries {@code retry_after} on a 429. */
        record SendResult(int status, String body) {}

        /** The transport, so a test can drive delivery without a network. */
        interface Sender {
            SendResult send(String chatId, String text) throws IOException, InterruptedException;
        }

        private final Sender sender;
        /**
         * Alerts accepted and not yet delivered, in flight included. The outbox alone lied to
         * {@link #flush}: the worker takes an item BEFORE delivering it, so a CRITICAL in the middle
         * of its HTTP send (or a retry sleep) left the queue empty and the shutdown hook returned at
         * once, and the JVM killed the daemon mid-POST (audit 06.09).
         */
        private final java.util.concurrent.atomic.AtomicInteger pending =
                new java.util.concurrent.atomic.AtomicInteger();
        private volatile boolean shuttingDown;

        /**
         * Delivery runs on its own thread. Every caller is the loop thread — the one that executes
         * closes, the heartbeat and reconciliation — and three attempts with 8 s timeouts used to
         * park it for up to ~35 s per serious alert whenever Telegram was slow (audit 03.09).
         * Bounded: a Telegram outage must not grow the heap; the log still carries every line.
         */
        private final java.util.concurrent.BlockingQueue<Runnable> outbox =
                new java.util.concurrent.LinkedBlockingQueue<>(256);

        private Telegram(String token, String chatId) {
            this(token, chatId, new HttpSender(token));
        }

        /** Package-private: the seam a test uses to stand in for Telegram itself. */
        Telegram(String token, String chatId, Sender sender) {
            this.token = token;
            this.chatId = chatId;
            this.sender = Preconditions.notNull(sender, "sender");
            Thread worker = new Thread(this::drainForever, "telegram-alerts");
            worker.setDaemon(true);
            worker.start();
            // A shutdown must not lose the last CRITICAL: give the outbox a bounded moment. The
            // flag makes deliver() skip its retry sleeps, so the wait is spent sending, not sleeping.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                shuttingDown = true;
                flush(10_000L);
            }, "telegram-alerts-flush"));
        }

        /** The real transport: one POST to the Bot API. */
        private static final class HttpSender implements Sender {
            private final String token;
            private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

            HttpSender(String token) { this.token = token; }

            @Override public SendResult send(String chatId, String text) throws IOException, InterruptedException {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("https://api.telegram.org/bot" + token + "/sendMessage"))
                        .timeout(Duration.ofSeconds(8))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "chat_id=" + URLEncoder.encode(chatId, StandardCharsets.UTF_8)
                                        + "&text=" + URLEncoder.encode(text, StandardCharsets.UTF_8)))
                        .build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                return new SendResult(response.statusCode(), response.body());
            }
        }

        private void drainForever() {
            while (true) {
                try {
                    outbox.take().run();
                } catch (InterruptedException e) {
                    return;
                } catch (RuntimeException e) {
                    LOG.warning("Telegram delivery failed: " + redact(String.valueOf(e.getMessage())));
                }
            }
        }

        /** Waits until every accepted alert is delivered (or given up) or the deadline passes. */
        void flush(long maxWaitMs) {
            long deadline = System.currentTimeMillis() + maxWaitMs;
            while (pending.get() > 0 && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /** Returns null when the environment does not configure Telegram. */
        public static Telegram fromEnvironmentOrNull() {
            return fromEnvironmentOrNull(System::getenv, DISABLED_WARNING_LOGGED);
        }

        /** Same, with the environment and the one-shot latch injected — the seams the tests use. */
        static Telegram fromEnvironmentOrNull(UnaryOperator<String> environment, AtomicBoolean warned) {
            String token = environment.apply(TOKEN_VAR);
            String chat = environment.apply(CHAT_ID_VAR);
            if (isBlank(token) || isBlank(chat)) {
                // Unconfigured used to look exactly like configured-and-quiet, the worst way for an
                // alert path to fail. The latch keeps it to one line if this moves into a loop.
                if (warned.compareAndSet(false, true)) {
                    LOG.warning(disabledWarning(token, chat));
                }
                // Null, not an exception: a missing push channel must not stop the bot.
                return null;
            }
            return new Telegram(token.trim(), chat.trim());
        }

        private static String disabledWarning(String token, String chatId) {
            List<String> missing = new ArrayList<>();
            if (isBlank(token)) missing.add(TOKEN_VAR);
            if (isBlank(chatId)) missing.add(CHAT_ID_VAR);
            return "Telegram alerts are OFF: " + String.join(" and ", missing)
                    + (missing.size() == 1 ? " is not set" : " are not set")
                    + " — CRITICAL events and trading halts will reach this log file and nothing else. "
                    + "Set both variables (see example.env) to be told when the bot stops itself.";
        }

        /**
         * The bot token is a path segment of the Telegram URL, so any message that quotes the URI
         * carries it. Log lines end up in the hosting console, which is a different trust boundary
         * from the process; a leaked token lets a stranger post as this bot and read the chat.
         * Binance's adapter has the same discipline for the opposite reason — it logs
         * {@code uri.getPath()} only, because the signature rides in the query.
         */
        private String redact(String text) {
            if (text == null) return "null";
            return token.isEmpty() ? text : text.replace(token, "<token>");
        }

        private static boolean isBlank(String value) {
            return value == null || value.isBlank();
        }

        @Override public void alert(Severity severity, String title, String message) {
            // Counted before the offer so flush() can never observe "nothing pending" between the
            // offer and the increment; a refused offer takes the count back.
            pending.incrementAndGet();
            Runnable job = () -> {
                try {
                    deliver(severity, title, message);
                } finally {
                    pending.decrementAndGet();
                }
            };
            if (!outbox.offer(job)) {
                pending.decrementAndGet();
                LOG.warning("Telegram outbox is full — alert dropped (still in this log): " + title);
            }
        }

        /** Telegram's flood control names its own wait: {@code "parameters":{"retry_after":N}}. */
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

        /** Keeps the head of an oversized message: the title and the first lines say what happened. */
        static String truncate(String text) {
            if (text.length() <= MAX_TEXT_CHARS) return text;
            String marker = "\n[... truncated; the full text is in the log]";
            return text.substring(0, MAX_TEXT_CHARS - marker.length()) + marker;
        }

        private void deliver(Severity severity, String title, String message) {
            String text = switch (severity) {
                case CRITICAL -> "❌ CRITICAL: ";
                case WARNING -> "⚠ ";
                case INFO -> "";
            } + title + "\n" + message;
            // One dropped packet used to lose the message outright, and the only record of a lost
            // CRITICAL was the log the alert exists to replace. Serious alerts get three tries.
            text = truncate(text);
            // INFO used to get one try. Since 08.09 the good news travels as INFO - a
            // take-profit that fired, a position the owner closed in the app - and the
            // message this bot exists to send became the least reliably delivered one it
            // has. Retries cost nothing on success, so every severity now gets three.
            int attempts = RETRIES_FOR_SERIOUS;
            boolean floodRetried = false;
            for (int attempt = 1; attempt <= attempts; attempt++) {
                try {
                    SendResult response = sender.send(chatId, text);
                    int status = response.status();
                    if (status / 100 == 2) return;
                    // Flood control is a 4xx too, but it names its own wait and is not the
                    // credentials: a burst at boot used to drop the CRITICAL among six alerts with
                    // a line blaming the token (audit 06.09). One extra try even for INFO.
                    if (status == 429) {
                        if (shuttingDown) return;
                        long wait = retryAfterMillis(response.body());
                        if (!floodRetried) {
                            floodRetried = true;
                            attempts = Math.max(attempts, attempt + 1);
                        }
                        LOG.warning("Telegram flood control, waiting " + wait + " ms (attempt "
                                + attempt + "/" + attempts + ")");
                        Thread.sleep(wait);
                        continue;
                    }
                    // 401/403/404 are the chat id or the token - retrying cannot fix it, and a
                    // rotated token must not cost three timeouts for every alert. Any other 4xx
                    // names its own reason in the body.
                    if (status / 100 == 4) {
                        LOG.warning("Telegram alert REFUSED, HTTP " + status
                                + (status == 401 || status == 403 || status == 404
                                        ? " — check TELEGRAM_CHAT_ID and TELEGRAM_BOT_TOKEN"
                                        : " — " + redact(String.valueOf(response.body()))));
                        return;
                    }
                    LOG.warning("Telegram alert not delivered, HTTP " + status
                            + " (attempt " + attempt + "/" + attempts + ")");
                } catch (IOException e) {
                    LOG.warning("Telegram alert not delivered: " + redact(String.valueOf(e.getMessage()))
                            + " (attempt " + attempt + "/" + attempts + ")");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    // URI.create quotes the whole URL — token included — in its exception message.
                    LOG.warning("Telegram alert not built: " + redact(String.valueOf(e.getMessage())));
                    return;
                }
                if (attempt < attempts) {
                    // The shutdown hook is waiting on this thread: send again at once or give up.
                    if (shuttingDown) continue;
                    try {
                        Thread.sleep(2000L * attempt);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    /** Stamps the sending machine: demo and real share one chat, where "halted" reads identically. */
    final class Tagged implements AlertSink {
        private final String tag;
        private final AlertSink delegate;

        public Tagged(String tag, AlertSink delegate) {
            this.tag = Preconditions.notBlank(tag, "tag");
            this.delegate = Preconditions.notNull(delegate, "delegate");
        }

        @Override public void alert(Severity severity, String title, String message) {
            delegate.alert(severity, "[" + tag + "] " + title, message);
        }
    }

    /** Sends to several sinks; one failing sink never prevents the others from being tried. */
    final class Composite implements AlertSink {
        private static final Logger LOG = Logger.getLogger("Alert.Composite");
        private final List<AlertSink> sinks;

        public Composite(List<AlertSink> sinks) {
            this.sinks = List.copyOf(Preconditions.notNull(sinks, "sinks"));
        }

        @Override public void alert(Severity severity, String title, String message) {
            for (AlertSink sink : sinks) {
                try {
                    sink.alert(severity, title, message);
                } catch (RuntimeException e) {
                    LOG.warning("alert sink " + sink.getClass().getSimpleName() + " failed: " + e.getMessage());
                }
            }
        }

        /** Package-private so a test can assemble without touching the real environment. */
        static AlertSink assemble(UnaryOperator<String> environment, AtomicBoolean warned) {
            List<AlertSink> sinks = new ArrayList<>();
            sinks.add(new Logging());
            Telegram telegram = Telegram.fromEnvironmentOrNull(environment, warned);
            if (telegram != null) sinks.add(telegram);
            return new Composite(sinks);
        }
    }

    /** Log always, plus Telegram when the environment configures it — and a warning when it does not. */
    static AlertSink fromEnvironment() {
        return Composite.assemble(System::getenv, Telegram.DISABLED_WARNING_LOGGED);
    }

    /** Same, with every alert stamped {@code [tag]} so the sender is never in doubt. */
    static AlertSink fromEnvironment(String tag) {
        return new Tagged(tag, fromEnvironment());
    }
}
