package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.binance.BinanceErrorCodes;

import java.util.Optional;
import java.util.logging.Logger;

/**
 * Sends orders in a way that a lost response cannot turn into a second position.
 *
 * <p>The dangerous case is not an order that fails. It is an order whose <i>outcome is unknown</i>:
 * the request left, the response did not come back, and the exchange may be holding a live order the
 * bot has no record of. Retrying that blindly is how one intended position becomes two, at double
 * the risk, with one of them completely unmanaged.
 *
 * <p>The protocol, in order:
 * <ol>
 *   <li><b>Ask before sending.</b> The client order id is deterministic
 *       ({@link ClientOrderIdFactory}), so if the exchange already knows it — from a previous
 *       attempt, or from before a restart — that order <i>is</i> the answer, and nothing is sent.</li>
 *   <li><b>Send.</b></li>
 *   <li><b>A definite refusal is final.</b> An error code means the exchange evaluated and declined:
 *       it is propagated, never retried, because retrying a refusal just collects another one.</li>
 *   <li><b>{@code -4116 DUPLICATED_CLIENT_ORDER_ID} is a success in disguise.</b> The exchange is
 *       saying it already has this order. Fetch it and return it.</li>
 *   <li><b>An ambiguous failure means ask, not resend.</b> Query by client order id a few times —
 *       an order can take a moment to become visible — and only if the exchange still has never
 *       heard of it, send again <i>with the same id</i>, which keeps step 4 as the backstop.</li>
 * </ol>
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
     * Places {@code request}, or returns the order the exchange already holds under the same client
     * order id. Calling this repeatedly with the same request is safe by construction.
     *
     * @throws ExchangeException on a definite refusal, or when the outcome remains unknown after
     *         every probe and resend — a state the caller must treat as "halt and reconcile",
     *         never as "try something else"
     */
    public OrderStatus place(OrderRequest request) throws InterruptedException {
        Preconditions.notNull(request, "request");

        Optional<OrderStatus> preexisting = port.queryOrder(request.symbol(), request.clientOrderId());
        if (preexisting.isPresent()) {
            LOG.info("[Placer] " + request.clientOrderId() + " already exists on the exchange ("
                    + preexisting.get().state() + ") — not sending again");
            return preexisting.get();
        }

        ExchangeException lastAmbiguous = null;
        for (int attempt = 1; attempt <= maxSendAttempts; attempt++) {
            try {
                return port.placeOrder(request);
            } catch (ExchangeException e) {
                if (e.exchangeCode() == BinanceErrorCodes.DUPLICATED_CLIENT_ORDER_ID) {
                    LOG.info("[Placer] exchange reports " + request.clientOrderId()
                            + " is a duplicate — adopting the existing order");
                    return port.queryOrder(request.symbol(), request.clientOrderId())
                            .orElseThrow(() -> ExchangeException.ambiguous(
                                    "exchange rejected " + request.clientOrderId() + " as a duplicate but "
                                            + "then reported no such order — state is inconsistent", e));
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
            }
        }
        throw ExchangeException.ambiguous(
                "could not establish the fate of " + request.clientOrderId() + " after " + maxSendAttempts
                        + " attempts — refusing to send again", lastAmbiguous);
    }

    /**
     * Cancels by client order id.
     *
     * @return {@code true} when the order is known not to be working any more — either the cancel
     *         succeeded, or the exchange says no such order exists. {@code false} when the cancel
     *         failed for any other reason, in which case the order may well still be live and the
     *         caller must not assume it is gone. "Quietly" refers to not throwing, not to
     *         pretending every outcome is the same one.
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
