package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.exec.OrderTypes.OrderPurpose;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Client order ids that are a pure function of what the order <i>is</i>.
 *
 * <p>This is the whole of the idempotency story. A retry that invents a fresh id turns an ambiguous
 * timeout into two positions; an id derived from {@code (signalId, purpose, index)} is the same on
 * the retry, so the exchange refuses the duplicate ({@code -4116}) and
 * {@link IdempotentOrderPlacer} can ask what happened instead of guessing.
 *
 * <p>Nothing that varies between attempts may enter the hash — no timestamp, no random suffix, no
 * attempt counter — because a well-meaning "make ids unique" change is how this property gets lost.
 * Pinned by {@code IdempotentResubmitTest.clientOrderIdsAreDeterministic} and
 * {@code reExecutingAPlanIsSafe}.
 *
 * <p>Format {@code bt-<purpose letter><index>-<22 chars of base64url(sha-256)>}, inside Binance's
 * {@code ^[\.A-Z\:/a-z0-9_-]{1,36}$}. 22 base64url characters is 132 bits of digest.
 */
public final class ClientOrderIdFactory {

    private static final String PREFIX = "bt-";
    private static final int DIGEST_CHARS = 22;

    private ClientOrderIdFactory() {}

    /**
     * @param signalId stable identity of the originating signal
     * @param purpose  what the order is for; an entry and its stop must not share an id
     * @param index    distinguishes several orders of the same purpose, e.g. take-profit legs
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
