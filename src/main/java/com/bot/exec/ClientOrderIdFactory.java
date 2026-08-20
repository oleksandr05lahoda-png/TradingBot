package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.exec.OrderTypes.OrderPurpose;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Optional;

/**
 * Client order ids derived purely from {@code (signalId, purpose, index)} — nothing that varies
 * between attempts may enter the hash — so a retry reproduces the same id and the exchange rejects
 * the duplicate ({@code -4116}) instead of opening a second position. Format
 * {@code bt-<letter><index>-<22 base64url chars>}, inside {@code ^[\.A-Z\:/a-z0-9_-]{1,36}$}.
 */
public final class ClientOrderIdFactory {

    private static final String PREFIX = "bt-";
    private static final int DIGEST_CHARS = 22;

    private ClientOrderIdFactory() {}

    /**
     * 22 base64url characters is 132 bits, so distinct signals do not collide inside Binance's
     * 36-character limit; {@code index} separates orders of one purpose (take-profit legs).
     */
    public static String create(String signalId, OrderPurpose purpose, int index) {
        Preconditions.notBlank(signalId, "signalId");
        Preconditions.notNull(purpose, "purpose");
        Preconditions.require(index >= 0 && index <= 99, "index must be in [0, 99], got " + index);

        String material = signalId + '|' + purpose.name() + '|' + index;
        String digest = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(sha256(material))
                .substring(0, DIGEST_CHARS);

        String id = PREFIX + letterOf(purpose) + index + '-' + digest;
        Preconditions.require(OrderRequest.CLIENT_ORDER_ID.matcher(id).matches(),
                "generated client order id \"" + id + "\" does not match the exchange pattern");
        return id;
    }

    /**
     * The purpose an id was minted for, read back from its letter; the adapter routes queries and
     * cancels on it, since conditional orders have a separate id space that answers "no such order"
     * rather than an error for the rest. Empty for a foreign id.
     */
    public static Optional<OrderPurpose> purposeOf(String clientOrderId) {
        if (clientOrderId == null || !clientOrderId.startsWith(PREFIX) || clientOrderId.length() <= PREFIX.length()) {
            return Optional.empty();
        }
        char letter = clientOrderId.charAt(PREFIX.length());
        for (OrderPurpose purpose : OrderPurpose.values()) {
            if (letterOf(purpose) == letter) return Optional.of(purpose);
        }
        return Optional.empty();
    }

    private static char letterOf(OrderPurpose purpose) {
        return switch (purpose) {
            case ENTRY -> 'e';
            case STOP_LOSS -> 's';
            case TAKE_PROFIT -> 't';
            case EMERGENCY_CLOSE -> 'x';
        };
    }

    private static byte[] sha256(String material) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(material.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java platform and is missing", e);
        }
    }
}
