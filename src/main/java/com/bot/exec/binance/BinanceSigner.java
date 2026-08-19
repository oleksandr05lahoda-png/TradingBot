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
 * HMAC-SHA256 request signing, and the credentials it needs. Keys come from the environment only.
 *
 * <p>Binance validates {@code serverTime - timestamp <= recvWindow && timestamp < serverTime + 1000},
 * so timestamps must come from an exchange-synchronised clock — see
 * {@link BinanceFuturesAdapter}'s drift correction.
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
     * Reads the venue's own key names ({@code BINANCE_TESTNET_API_*} for demo,
     * {@code BINANCE_REAL_API_*} for real). Venue-specific names on purpose: a generic name
     * invites the wrong key to be pasted in, and the two venues must never share credentials.
     */
    public static BinanceSigner fromEnvironment(BinanceVenue venue) {
        Preconditions.notNull(venue, "venue");
        String key = System.getenv(venue.keyEnv());
        String secret = System.getenv(venue.secretEnv());
        if (key == null || key.isBlank() || secret == null || secret.isBlank()) {
            throw new IllegalStateException(
                    venue.name() + " credentials are not set.\n"
                            + "  Put them in local.env (git-ignored) as two lines:\n"
                            + "      " + venue.keyEnv() + "=...\n"
                            + "      " + venue.secretEnv() + "=...\n"
                            + "  Copy example.env if the file does not exist yet. The key MUST be\n"
                            + "  created with WITHDRAWALS DISABLED"
                            + (venue.isReal() ? " and an IP whitelist" : "") + ".\n"
                            + "  Real environment variables work too and take precedence.");
        }
        return new BinanceSigner(key.trim(), secret.trim());
    }

    public String apiKey() { return apiKey; }

    /** Parameters in insertion order, then recvWindow and timestamp, then a signature over all of it. */
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
            // Binance signs the literal query string, so signed and sent must be byte-identical.
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

    /** Redacted on purpose: this object ends up inside exception messages and log lines. */
    @Override public String toString() {
        return "BinanceSigner[credentials redacted]";
    }
}
