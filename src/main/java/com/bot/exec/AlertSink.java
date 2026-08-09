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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Destination for operator alerts such as reconciliation drift or a fired dead-man's switch. The
 * default writes to the log; {@link Telegram} adds a push when the environment supplies credentials.
 */
public interface AlertSink {

    enum Severity { INFO, WARNING, CRITICAL }

    void alert(Severity severity, String title, String message);

    default void critical(String title, String message) { alert(Severity.CRITICAL, title, message); }

    default void warning(String title, String message) { alert(Severity.WARNING, title, message); }

    default void info(String title, String message) { alert(Severity.INFO, title, message); }

    /** The always-available floor. */
    final class Logging implements AlertSink {
        private static final Logger LOG = Logger.getLogger("Alert");

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
     * Telegram push, configured from {@code TELEGRAM_BOT_TOKEN} and {@code TELEGRAM_CHAT_ID}.
     * Delivery failures are logged and swallowed on purpose: the alert path must never throw into
     * the trading path it is reporting on.
     */
    final class Telegram implements AlertSink {
        private static final Logger LOG = Logger.getLogger("Alert.Telegram");

        private final String token;
        private final String chatId;
        private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        private Telegram(String token, String chatId) {
            this.token = token;
            this.chatId = chatId;
        }

        /** Returns null when the environment does not configure Telegram. */
        public static Telegram fromEnvironmentOrNull() {
            String token = System.getenv("TELEGRAM_BOT_TOKEN");
            String chat = System.getenv("TELEGRAM_CHAT_ID");
            if (token == null || token.isBlank() || chat == null || chat.isBlank()) return null;
            return new Telegram(token.trim(), chat.trim());
        }

        @Override public void alert(Severity severity, String title, String message) {
            String text = switch (severity) {
                case CRITICAL -> "❌ CRITICAL: ";
                case WARNING -> "⚠ ";
                case INFO -> "";
            } + title + "\n" + message;
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
                if (response.statusCode() / 100 != 2) {
                    LOG.warning("Telegram alert not delivered, HTTP " + response.statusCode());
                }
            } catch (IOException e) {
                LOG.warning("Telegram alert not delivered: " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
    }

    /** Log always, plus Telegram when the environment configures it. */
    static AlertSink fromEnvironment() {
        List<AlertSink> sinks = new ArrayList<>();
        sinks.add(new Logging());
        Telegram telegram = Telegram.fromEnvironmentOrNull();
        if (telegram != null) sinks.add(telegram);
        return new Composite(sinks);
    }
}
