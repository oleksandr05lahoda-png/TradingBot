package com.bot.exec.binance;

import com.bot.core.Preconditions;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * HMAC-SHA256 request signing, and the credentials it needs.
 *
 * <p>Two rules are enforced by construction rather than by convention:
 * <ul>
 *   <li><b>Keys come from the environment only.</b> {@link #fromEnvironment()} is the only way to
 *       build one outside of a test, and there is no constant, config file or default in this
 *       repository that could hold a key.</li>
 *   <li><b>Nothing here is loggable.</b> {@link #toString()} is overridden to reveal nothing, and the
 *       signature is never returned as part of any message. A secret that reaches a log file is a
 *       leaked secret, and log files travel.</li>
 * </ul>
 *
 * <p>Binance validates {@code serverTime - timestamp <= recvWindow && timestamp < serverTime + 1000},
 * so the timestamp is taken from an exchange-synchronised clock rather than the local one — see
 * {@link BinanceFuturesTestnetAdapter}'s drift correction.
 */
public final class BinanceSigner {

    /** Default {@code recvWindow}: how much clock skew the exchange will tolerate, in milliseconds. */
    public static final long DEFAULT_RECV_WINDOW_MS = 5_000L;

    private final String apiKey;
    private final byte[] apiSecret;

    public BinanceSigner(String apiKey, String apiSecret) {
        this.apiKey = Preconditions.notBlank(apiKey, "apiKey");
        this.apiSecret = Preconditions.notBlank(apiSecret, "apiSecret").getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Reads {@code BINANCE_TESTNET_API_KEY} and {@code BINANCE_TESTNET_API_SECRET}.
     *
     * <p>The variables are named for the testnet on purpose. A key variable that is not
     * testnet-specific invites a real key to be pasted into it, and this build has nowhere to send a
     * real key anyway — but the naming should not be the thing that makes someone find that out.
     *
     * @throws IllegalStateException when either variable is missing, naming what to set
     */
    public static BinanceSigner fromEnvironment() {
        String key = System.getenv("BINANCE_TESTNET_API_KEY");
        String secret = System.getenv("BINANCE_TESTNET_API_SECRET");
        if (key == null || key.isBlank() || secret == null || secret.isBlank()) {
            throw new IllegalStateException(
                    "testnet credentials are not set. Export BINANCE_TESTNET_API_KEY and "
                            + "BINANCE_TESTNET_API_SECRET, from a key created with withdrawals DISABLED. "
                            + "See the README section \"Testnet keys\".");
        }
        return new BinanceSigner(key.trim(), secret.trim());
    }

    public String apiKey() { return apiKey; }

    /**
     * Builds the signed query string: parameters in insertion order, then {@code timestamp} and
     * {@code recvWindow}, then {@code signature} computed over everything preceding it.
     */
    public String signedQuery(Map<String, String> parameters, long timestampMs, long recvWindowMs) {
        Preconditions.notNull(parameters, "parameters");
        Map<String, String> all = new LinkedHashMap<>(parameters);
        all.put("recvWindow", Long.toString(recvWindowMs));
        all.put("timestamp", Long.toString(timestampMs));

        String query = encode(all);
        return query + "&signature=" + hmacSha256Hex(query);
    }

    /** Query string with no signature, for public endpoints. */
    public static String encode(Map<String, String> parameters) {
        StringJoiner joiner = new StringJoiner("&");
        for (Map.Entry<String, String> e : parameters.entrySet()) {
            if (e.getValue() == null) continue;
            // Binance signs the literal query string it receives, so the value that is signed and the
            // value that is sent must be byte-identical. Every parameter this system sends is a
            // symbol, an enum or a decimal number, none of which contain characters that need
            // percent-encoding; anything else would have to be encoded on both sides consistently.
            String value = e.getValue();
            Preconditions.require(value.chars().noneMatch(c -> c == '&' || c == '=' || c == '?' || c == ' '),
                    "parameter " + e.getKey() + " contains a character that would corrupt the signed "
                            + "query string: \"" + value + "\"");
            joiner.add(e.getKey() + "=" + value);
        }
        return joiner.toString();
    }

    private String hmacSha256Hex(String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(apiSecret, "HmacSHA256"));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(raw.length * 2);
            for (byte b : raw) hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            return hex.toString();
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", e);
        }
    }

    /** Deliberately reveals nothing: this object ends up inside exception messages and log lines. */
    @Override public String toString() {
        return "BinanceSigner[credentials redacted]";
    }
}
