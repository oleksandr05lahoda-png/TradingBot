package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.binance.BinanceErrorCodes;

import java.util.Optional;
import java.util.logging.Logger;

/**
 * Sends orders so a lost response cannot become a second position: query first by the deterministic
 * client order id, treat {@code -4116 DUPLICATED_CLIENT_ORDER_ID} as success and adopt the existing
 * order, probe by id before resending after an ambiguous failure — always with the same id, which
 * keeps the duplicate rejection as the backstop. That backstop only exists for orders that REST:
 * a filled MARKET entry is no longer open, so its id is accepted again and a resend is a second
 * fill. Such an entry is never resent; its fate is left to the caller's position probe. A definite
 * refusal propagates, never retried.
 */
public final class IdempotentOrderPlacer {

    private static final Logger LOG = Logger.getLogger(IdempotentOrderPlacer.class.getName());

    private final ExchangePort port;
    private final int maxSendAttempts;
    private final int confirmationProbes;
    private final long probeIntervalMs;
    private final RateLimiter.Sleeper sleeper;

    public IdempotentOrderPlacer(ExchangePort port) {
        this(port, 3, 3, 400, Thread::sleep);
    }

    public IdempotentOrderPlacer(ExchangePort port, int maxSendAttempts, int confirmationProbes,
                                 long probeIntervalMs, RateLimiter.Sleeper sleeper) {
        this.port = Preconditions.notNull(port, "port");
        this.maxSendAttempts = Preconditions.positive(maxSendAttempts, "maxSendAttempts");
        this.confirmationProbes = Preconditions.positive(confirmationProbes, "confirmationProbes");
        this.probeIntervalMs = probeIntervalMs;
        this.sleeper = Preconditions.notNull(sleeper, "sleeper");
    }

    /**
     * Places {@code request}, or returns what the exchange already holds under the same id; safe to
     * call repeatedly. Throws on a definite refusal, and on an outcome still unknown after every
     * probe and resend — which the caller must treat as "halt and reconcile".
     */
    public OrderStatus place(OrderRequest request) throws InterruptedException {
        Preconditions.notNull(request, "request");

        // The pre-send look-up is a READ: if it cannot be had, nothing has been sent, and the caller
        // must hear exactly that. Propagated raw, a 20 s timeout on this GET used to read as an
        // ambiguous SEND — a halt for an entry, a market close of the fresh position for its stop —
        // over a request that never left this host (audit 03.09). It is also the only guard against
        // re-sending a MARKET order whose earlier copy already filled: Binance keeps a client id
        // unique among OPEN orders only, so a filled entry's id is accepted again. Hence no send
        // without a successful look-up.
        Optional<OrderStatus> preexisting = queryBeforeSend(request);
        if (preexisting.isPresent()) {
            OrderStatus found = preexisting.get();
            if (!found.isWorking() && isStale(found)) {
                // A terminal order from an earlier life under this id (a book replayed after a
                // restart, a reused signal id). Adopting it would report a fill that happened hours
                // ago as today's; sending again would fill twice. Neither: refuse, definitely.
                throw ExchangeException.refused("client order id " + request.clientOrderId()
                        + " already ran to " + found.state() + " at " + found.updateTimeMs()
                        + " — this signal was executed before; not sending it again", 409, 0);
            }
            LOG.info("[Placer] " + request.clientOrderId() + " already exists on the exchange ("
                    + found.state() + ") — not sending again");
            return found;
        }

        ExchangeException lastAmbiguous = null;
        for (int attempt = 1; attempt <= maxSendAttempts; attempt++) {
            try {
                return port.placeOrder(request);
            } catch (ExchangeException e) {
                if (e.exchangeCode() == BinanceErrorCodes.DUPLICATED_CLIENT_ORDER_ID) {
                    // The exchange just said the order EXISTS. A failed read of it now is ignorance
                    // about a live order, never "refused": reported as a refusal, a 429 on this
                    // query turned a landed entry into "nothing happened" — no stop, no book record.
                    LOG.info("[Placer] exchange reports " + request.clientOrderId()
                            + " is a duplicate — adopting the existing order");
                    Optional<OrderStatus> existing = probeForOrder(request);
                    if (existing.isPresent()) return existing.get();
                    throw ExchangeException.ambiguous(
                            "exchange rejected " + request.clientOrderId() + " as a duplicate but its "
                                    + "state could not be read afterwards — it may be live and filled", e);
                }
                if (!e.ambiguous()) {
                    throw e;
                }
                lastAmbiguous = e;
                LOG.warning("[Placer] attempt " + attempt + " for " + request.clientOrderId()
                        + " ended ambiguously (" + e.getMessage() + ") — asking the exchange before resending");

                Optional<OrderStatus> landed = probeForOrder(request);
                if (landed.isPresent()) {
                    LOG.info("[Placer] " + request.clientOrderId() + " did land despite the failure — adopting it");
                    return landed.get();
                }
                if (!resendIsBackstopped(request)) {
                    // A MARKET entry that filled is no longer OPEN, so its id is accepted again and a
                    // resend is a second fill, not a duplicate rejection. Three probes that found
                    // nothing (or could not read) are not proof it never landed: the matching engine
                    // is slowest under exactly the load that produces -1007. The caller's own
                    // position probe decides; nothing is sent again.
                    throw ExchangeException.ambiguous("the fate of " + request.clientOrderId()
                            + " is unknown after an ambiguous send and a resend could fill twice — "
                            + "not resending; the position itself must be checked", e);
                }
            }
        }
        throw ExchangeException.ambiguous(
                "could not establish the fate of " + request.clientOrderId() + " after " + maxSendAttempts
                        + " attempts — refusing to send again", lastAmbiguous);
    }

    /**
     * @return {@code true} when the order is known not to be working (cancelled, or no such order);
     *         {@code false} when the cancel failed and it may still be live
     */
    public boolean cancelQuietly(String symbol, String clientOrderId) {
        try {
            port.cancelOrder(symbol, clientOrderId);
            return true;
        } catch (ExchangeException e) {
            if (BinanceErrorCodes.isOrderAbsent(e.exchangeCode())) {
                LOG.fine("[Placer] nothing to cancel for " + clientOrderId);
                return true;
            }
            LOG.warning("[Placer] cancel of " + clientOrderId + " failed, the order may still be "
                    + "working: " + e.getMessage());
            return false;
        }
    }

    /**
     * The look-up that must succeed before anything is sent. Retried a few times on any failure,
     * then given up as {@link ExchangeException#neverSent}: the caller may retry the whole placement
     * (nothing happened) or refuse, but must never treat it as a lost send.
     */
    private Optional<OrderStatus> queryBeforeSend(OrderRequest request) throws InterruptedException {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= confirmationProbes; attempt++) {
            try {
                return port.queryOrder(request.symbol(), request.clientOrderId());
            } catch (RuntimeException e) {
                last = e;
                LOG.warning("[Placer] pre-send look-up of " + request.clientOrderId() + " failed ("
                        + attempt + "/" + confirmationProbes + "): " + e.getMessage());
                if (attempt < confirmationProbes) sleeper.sleepMillis(probeIntervalMs);
            }
        }
        throw ExchangeException.neverSent("could not verify " + request.clientOrderId()
                + " before sending — nothing was sent (" + last.getMessage() + ")", last);
    }

    /**
     * Whether {@code -4116} really guards a resend of this request. A resting order (LIMIT, the
     * conditional types) keeps its id among the OPEN orders, so a second send is rejected as a
     * duplicate. A reduce-only market close cannot over-close: a flat symbol answers -2022. Only a
     * non-reducing MARKET entry has no backstop at all once it filled.
     */
    static boolean resendIsBackstopped(OrderRequest request) {
        return request.type() != OrderTypes.OrderType.MARKET || request.isStrictlyReducing();
    }

    /** A terminal order older than this is a relic of an earlier process, not this attempt's fill. */
    static final long STALE_TERMINAL_MS = 10 * 60_000L;

    private boolean isStale(OrderStatus terminal) {
        long stamped = terminal.updateTimeMs();
        if (stamped <= 0) return false;                      // no timestamp: cannot call it old
        return port.serverTimeMillis() - stamped > STALE_TERMINAL_MS;
    }

    private Optional<OrderStatus> probeForOrder(OrderRequest request) throws InterruptedException {
        for (int probe = 0; probe < confirmationProbes; probe++) {
            sleeper.sleepMillis(probeIntervalMs);
            try {
                Optional<OrderStatus> found = port.queryOrder(request.symbol(), request.clientOrderId());
                if (found.isPresent()) return found;
            } catch (ExchangeException e) {
                LOG.warning("[Placer] probe " + (probe + 1) + " for " + request.clientOrderId()
                        + " failed: " + e.getMessage());
            }
        }
        return Optional.empty();
    }
}
