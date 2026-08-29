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

        private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        private Telegram(String token, String chatId) {
            this.token = token;
            this.chatId = chatId;
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
            String text = switch (severity) {
                case CRITICAL -> "❌ CRITICAL: ";
                case WARNING -> "⚠ ";
                case INFO -> "";
            } + title + "\n" + message;
            // One dropped packet used to lose the message outright, and the only record of a lost
            // CRITICAL was the log the alert exists to replace. Serious alerts get three tries.
            int attempts = severity == Severity.INFO ? 1 : RETRIES_FOR_SERIOUS;
            for (int attempt = 1; attempt <= attempts; attempt++) {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("https://api.telegram.org/bot" + token + "/sendMessage"))
                            .timeout(Duration.ofSeconds(8))
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .POST(HttpRequest.BodyPublishers.ofString(
                                    "chat_id=" + URLEncoder.encode(chatId, StandardCharsets.UTF_8)
                                            + "&text=" + URLEncoder.encode(text, StandardCharsets.UTF_8)))
                            .build();
                    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() / 100 == 2) return;
                    // 4xx is the chat id or the token — retrying cannot fix it, and a rotated token
                    // must not cost three timeouts on the loop thread for every alert.
                    if (response.statusCode() / 100 == 4) {
                        LOG.warning("Telegram alert REFUSED, HTTP " + response.statusCode()
                                + " — check TELEGRAM_CHAT_ID and TELEGRAM_BOT_TOKEN");
                        return;
                    }
                    LOG.warning("Telegram alert not delivered, HTTP " + response.statusCode()
                            + " (attempt " + attempt + "/" + attempts + ")");
                } catch (IOException e) {
                    LOG.warning("Telegram alert not delivered: " + redact(String.valueOf(e.getMessage()))
                            + " (attempt " + attempt + "/" + attempts + ")");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (attempt < attempts) {
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
