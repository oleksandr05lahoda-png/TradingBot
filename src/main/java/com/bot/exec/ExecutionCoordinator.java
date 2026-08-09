package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.OrderTypes.OrderPurpose;
import com.bot.exec.OrderTypes.OrderSide;
import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.exec.OrderTypes.TimeInForce;
import com.bot.risk.RiskEngine;
import com.bot.risk.TakeProfitPolicy;
import com.bot.risk.TradePlan;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Turns an approved {@link TradePlan} into orders, and never leaves a filled position unprotected in
 * between. Three rules carry that:
 *
 * <ol>
 *   <li><b>The ordering.</b> The stop goes on immediately after the fill — before slippage is
 *       assessed, before exits are computed, before the book is updated. If it cannot be placed, the
 *       position is closed reduce-only and trading halts.</li>
 *   <li><b>The filled quantity is the only quantity.</b> A partial fill is a different position from
 *       the approved one; a stop sized for the intended quantity leaves the difference exposed while
 *       every log line reads as covered.</li>
 *   <li><b>Slippage can void a trade after it opens.</b> If the real distance to the stop risks more
 *       than the budget, the position is closed rather than kept.</li>
 * </ol>
 */
public final class ExecutionCoordinator {

    private static final Logger LOG = Logger.getLogger(ExecutionCoordinator.class.getName());

    public enum Outcome {
        /** Entry filled completely; stop and exits are on the exchange. */
        FILLED,
        /** Entry filled partially; stop and exits cover the filled quantity. */
        PARTIALLY_FILLED,
        /** Nothing filled. No stop, no exits, no position, nothing to clean up. */
        NOT_FILLED,
        /** Filled, then closed again because the fill price broke the risk budget. */
        ABORTED_ON_SLIPPAGE,
        /** Filled, but the protective stop could not be placed. Trading is halted. */
        ABORTED_UNPROTECTED,
        /**
         * The entry's fate is unknown: the send failed ambiguously and the exchange could not be
         * asked. A position may exist, unprotected and unrecorded. Trading is halted.
         */
        UNKNOWN_AFTER_SEND,
        /** Refused before anything was sent — halted, or the plan failed a pre-trade check. */
        REFUSED
    }

    public record Report(
            Outcome outcome,
            TradePlan plan,
            BigDecimal filledQuantity,
            BigDecimal averageFillPrice,
            Optional<OrderStatus> entryOrder,
            Optional<OrderStatus> protectiveStop,
            List<OrderStatus> takeProfitOrders,
            String note) {

        public Report {
            takeProfitOrders = List.copyOf(takeProfitOrders);
        }

        /** True when a position is known to exist as a result of this execution. */
        public boolean opened() {
            return outcome == Outcome.FILLED || outcome == Outcome.PARTIALLY_FILLED;
        }

        /**
         * True when a position may exist that this process cannot account for. Distinct from
         * {@link #opened()}: the answer here is "unknown", and treating unknown as "no" is what
         * lets a naked position sit while the loop opens the next one.
         */
        public boolean mayHaveOpenedUnknownRisk() {
            return outcome == Outcome.UNKNOWN_AFTER_SEND || outcome == Outcome.ABORTED_UNPROTECTED;
        }
    }

    /**
     * @param entryType                 MARKET is the default: on a thin testnet book a resting limit
     *                                  order mostly measures patience, and the entry price is
     *                                  reconciled against reality afterwards either way
     * @param fillPollAttempts          how many times to ask the exchange what the entry did
     * @param maxAdverseRiskOverrun     how far the realised risk may exceed the planned risk before
     *                                  the position is closed again; 0.20 = 20%
     */
    public record Settings(
            OrderType entryType,
            TimeInForce entryTimeInForce,
            int fillPollAttempts,
            long fillPollIntervalMs,
            double maxAdverseRiskOverrun) {

        public static Settings defaults() {
            return new Settings(OrderType.MARKET, TimeInForce.IOC, 10, 500, 0.20);
        }
    }

    private final ExchangePort port;
    private final RiskEngine engine;
    private final IdempotentOrderPlacer placer;
    private final TradingHalt halt;
    private final AlertSink alerts;
    private final Settings settings;
    private final Clock clock;
    private final RateLimiter.Sleeper sleeper;

    public ExecutionCoordinator(ExchangePort port, RiskEngine engine, IdempotentOrderPlacer placer,
                                TradingHalt halt, AlertSink alerts, Settings settings,
                                Clock clock, RateLimiter.Sleeper sleeper) {
        this.port = Preconditions.notNull(port, "port");
        this.engine = Preconditions.notNull(engine, "engine");
        this.placer = Preconditions.notNull(placer, "placer");
        this.halt = Preconditions.notNull(halt, "halt");
        this.alerts = Preconditions.notNull(alerts, "alerts");
        this.settings = Preconditions.notNull(settings, "settings");
        this.clock = Preconditions.notNull(clock, "clock");
        this.sleeper = Preconditions.notNull(sleeper, "sleeper");
    }

    /** Executes an approved plan. Never throws for an ordinary refusal — that comes back as a report. */
    public Report execute(TradePlan plan) throws InterruptedException {
        Preconditions.notNull(plan, "plan");

        if (halt.isHalted()) {
            return refused(plan, "trading is halted: " + halt.reason().orElse("unknown"));
        }

        InstrumentFilters filters = plan.filters();
        OrderSide openSide = OrderSide.toOpen(plan.side());
        OrderSide closeSide = OrderSide.toClose(plan.side());

        OrderRequest entryRequest = settings.entryType() == OrderType.LIMIT
                ? OrderRequest.limitEntry(plan.symbol(), openSide, plan.quantity(), plan.entryPrice(),
                        settings.entryTimeInForce(), ClientOrderIdFactory.create(plan.signalId(), OrderPurpose.ENTRY, 0))
                : OrderRequest.marketEntry(plan.symbol(), openSide, plan.quantity(),
                        ClientOrderIdFactory.create(plan.signalId(), OrderPurpose.ENTRY, 0));

        PreTradeValidator.Result entryCheck = PreTradeValidator.validate(entryRequest, filters, plan.entryPrice());
        if (!entryCheck.ok()) {
            return refused(plan, "entry order would be rejected: " + entryCheck.describe());
        }
        OrderRequest stopRequest = OrderRequest.protectiveStop(plan.symbol(), closeSide, plan.stopPrice(),
                ClientOrderIdFactory.create(plan.signalId(), OrderPurpose.STOP_LOSS, 0));
        PreTradeValidator.Result stopCheck = PreTradeValidator.validate(stopRequest, filters, plan.entryPrice());
        if (!stopCheck.ok()) {
            // Refusing here, before the entry, is the point: a stop that cannot be placed must stop
            // the trade rather than be discovered after the position exists.
            return refused(plan, "protective stop would be rejected, so the entry is not sent: "
                    + stopCheck.describe());
        }

        // Margin mode and leverage before anything is sent. Both are idempotent.
        port.ensureIsolatedMargin(plan.symbol());
        port.setLeverage(plan.symbol(), plan.leverage());

        OrderStatus entry;
        try {
            entry = placer.place(entryRequest);
            entry = awaitEntryResolution(entry, entryRequest);
        } catch (ExchangeException e) {
            if (!e.ambiguous()) {
                // A definite refusal: the exchange evaluated the order and declined it. Nothing
                // landed, so there is nothing to clean up and no reason to stop trading.
                return refused(plan, "entry refused by the exchange: " + e.getMessage());
            }
            // The placer never established what happened, so the order may be live. Not swallowable
            // as "the signal failed": the loop would open the next position on top of one it does
            // not know exists, sized against a book that omits it.
            return unknownAfterSend(plan, e);
        }

        BigDecimal filled = entry.executedQuantity();
        if (filled.signum() <= 0) {
            LOG.info("[Coordinator] " + plan.symbol() + " entry did not fill (" + entry.state()
                    + ") — nothing to protect, nothing to clean up");
            return new Report(Outcome.NOT_FILLED, plan, BigDecimal.ZERO, BigDecimal.ZERO,
                    Optional.of(entry), Optional.empty(), List.of(), "entry " + entry.state());
        }

        BigDecimal avgPrice = entry.averagePrice().signum() > 0 ? entry.averagePrice() : plan.entryPrice();

        // ── The stop goes on now. Nothing between the fill and this. ────────────────────────────
        OrderStatus stop;
        try {
            stop = placer.place(stopRequest);
        } catch (RuntimeException e) {
            return abandonUnprotectedPosition(plan, entry, filled, avgPrice, e);
        }

        engine.registerFill(plan, filled, avgPrice.doubleValue());

        // ── Only now is it safe to do arithmetic. ───────────────────────────────────────────────
        double realisedRisk = filled.doubleValue()
                * Math.abs(avgPrice.doubleValue() - plan.stopPrice().doubleValue());
        boolean stopCrossed = !plan.side().isValidStopGeometry(avgPrice.doubleValue(), plan.stopPrice().doubleValue());
        double allowedRisk = plan.riskUsd() * (1 + settings.maxAdverseRiskOverrun());

        if (stopCrossed || realisedRisk > allowedRisk) {
            String why = stopCrossed
                    ? String.format("filled at %s, already through the stop at %s",
                            avgPrice.toPlainString(), plan.stopPrice().toPlainString())
                    : String.format("filled at %s: risk $%.4f against a planned $%.4f (+%.0f%% allowed)",
                            avgPrice.toPlainString(), realisedRisk, plan.riskUsd(),
                            settings.maxAdverseRiskOverrun() * 100);
            return closeOnSlippage(plan, entry, stop, filled, avgPrice, why);
        }

        // ── Reduce-only exits, re-projected from the price that actually filled. ────────────────
        List<OrderStatus> takeProfits = placeTakeProfits(plan, closeSide, filled, avgPrice, filters);

        boolean partial = filled.compareTo(plan.quantity()) < 0;
        String note = partial
                ? String.format("partial fill %s of %s — stop and exits sized to what filled",
                        filled.toPlainString(), plan.quantity().toPlainString())
                : "filled in full";
        LOG.info("[Coordinator] " + plan.symbol() + " " + note + " @ " + avgPrice.toPlainString()
                + "; stop " + stop.clientOrderId() + "; " + takeProfits.size() + " exit(s)");

        return new Report(partial ? Outcome.PARTIALLY_FILLED : Outcome.FILLED, plan, filled, avgPrice,
                Optional.of(entry), Optional.of(stop), takeProfits, note);
    }

    /**
     * Closes a position reduce-only. Used by the slippage abort, by the kill switch and by the
     * operator. Deliberately does not consult {@link TradingHalt}: a halt stops opening, never
     * closing.
     */
    public OrderStatus flatten(String symbol, Side direction, BigDecimal quantity, String signalId)
            throws InterruptedException {
        OrderRequest close = OrderRequest.emergencyClose(symbol, OrderSide.toClose(direction), quantity,
                ClientOrderIdFactory.create(signalId, OrderPurpose.EMERGENCY_CLOSE, 0));
        return placer.place(close);
    }

    private List<OrderStatus> placeTakeProfits(TradePlan plan, OrderSide closeSide, BigDecimal filled,
                                               BigDecimal avgPrice, InstrumentFilters filters)
            throws InterruptedException {
        TakeProfitPolicy policy = engine.config().takeProfitPolicy();
        List<TakeProfitPolicy.ProjectedLeg> legs = policy.project(
                plan.side(), avgPrice.doubleValue(), plan.stopPrice().doubleValue(), filled, filters);

        List<OrderStatus> placed = new ArrayList<>();
        for (int i = 0; i < legs.size(); i++) {
            TakeProfitPolicy.ProjectedLeg leg = legs.get(i);
            OrderRequest request = OrderRequest.takeProfit(plan.symbol(), closeSide, leg.quantity(),
                    leg.price(), ClientOrderIdFactory.create(plan.signalId(), OrderPurpose.TAKE_PROFIT, i));
            PreTradeValidator.Result check = PreTradeValidator.validate(request, filters, avgPrice);
            if (!check.ok()) {
                LOG.warning("[Coordinator] skipping take-profit leg " + i + " at " + leg.rMultiple()
                        + "R: " + check.describe());
                continue;
            }
            try {
                placed.add(placer.place(request));
            } catch (RuntimeException e) {
                // A missing exit is a lost opportunity; a missing stop would be a lost account. This
                // one is logged and alerted, not escalated to closing the position.
                alerts.warning("Take-profit leg not placed",
                        plan.symbol() + " leg " + i + " at " + leg.rMultiple() + "R: " + e.getMessage());
            }
        }
        return placed;
    }

    /** Polls until the entry reaches a state worth acting on, cancelling a stale remainder. */
    private OrderStatus awaitEntryResolution(OrderStatus initial, OrderRequest request) throws InterruptedException {
        OrderStatus current = initial;
        for (int attempt = 0; attempt < settings.fillPollAttempts(); attempt++) {
            if (current.state().isTerminal()) return current;
            sleeper.sleepMillis(settings.fillPollIntervalMs());
            current = port.queryOrder(request.symbol(), request.clientOrderId()).orElse(current);
        }

        if (current.isWorking()) {
            // Still resting after the poll budget. Cancel the remainder so the intended size stops
            // growing behind the bot's back, then take the final truth from the exchange.
            LOG.info("[Coordinator] entry " + request.clientOrderId() + " still " + current.state()
                    + " — cancelling the remainder");
            placer.cancelQuietly(request.symbol(), request.clientOrderId());
            current = port.queryOrder(request.symbol(), request.clientOrderId()).orElse(current);
        }
        return current;
    }

    /**
     * The entry was sent and its outcome could not be established. A position may exist, and this
     * process cannot say. It halts and alerts rather than returning a refusal, because "unknown" and
     * "did not happen" are different answers and only one of them is safe to keep trading on.
     */
    private Report unknownAfterSend(TradePlan plan, ExchangeException cause) {
        String note = "the fate of the entry order is unknown (" + cause.getMessage()
                + "). A position may be open and unprotected. Trading is halted until an operator "
                + "reconciles the account.";
        alerts.critical("Entry outcome unknown", plan.symbol() + ": " + note);
        halt.halt("entry outcome unknown on " + plan.symbol(), clock.instant());
        return new Report(Outcome.UNKNOWN_AFTER_SEND, plan, BigDecimal.ZERO, BigDecimal.ZERO,
                Optional.empty(), Optional.empty(), List.of(), note);
    }

    private Report abandonUnprotectedPosition(TradePlan plan, OrderStatus entry, BigDecimal filled,
                                              BigDecimal avgPrice, RuntimeException cause)
            throws InterruptedException {
        alerts.critical("Protective stop could not be placed",
                plan.symbol() + " filled " + filled.toPlainString() + " @ " + avgPrice.toPlainString()
                        + " but the stop was refused (" + cause.getMessage()
                        + "). Closing the position and halting.");
        halt.halt("protective stop could not be placed on " + plan.symbol(), clock.instant());

        String note;
        try {
            OrderStatus close = flatten(plan.symbol(), plan.side(), filled, plan.signalId());
            BigDecimal residual = filled.subtract(close.executedQuantity());
            if (residual.signum() > 0) {
                note = "stop refused; the reduce-only close only filled " + close.executedQuantity()
                        .toPlainString() + " of " + filled.toPlainString()
                        + " — " + residual.toPlainString() + " REMAINS OPEN AND UNPROTECTED";
                alerts.critical("Naked position", plan.symbol() + ": " + note);
            } else {
                note = "stop refused; position closed reduce-only in full";
            }
        } catch (RuntimeException e) {
            note = "stop refused AND the reduce-only close also failed (" + e.getMessage()
                    + ") — MANUAL INTERVENTION REQUIRED";
            alerts.critical("Naked position", plan.symbol() + ": " + note);
        }
        return new Report(Outcome.ABORTED_UNPROTECTED, plan, filled, avgPrice,
                Optional.of(entry), Optional.empty(), List.of(), note);
    }

    private Report closeOnSlippage(TradePlan plan, OrderStatus entry, OrderStatus stop,
                                   BigDecimal filled, BigDecimal avgPrice, String why)
            throws InterruptedException {
        alerts.critical("Fill broke the risk budget", plan.symbol() + ": " + why
                + " — closing the position reduce-only");

        String note;
        boolean closedCompletely = false;
        try {
            OrderStatus close = flatten(plan.symbol(), plan.side(), filled, plan.signalId());
            BigDecimal residual = filled.subtract(close.executedQuantity());
            if (residual.signum() > 0) {
                // Accepted but incomplete. The exchange returned no error, so nothing was thrown —
                // and the number that proves the position is still there is the one in the response
                // that an earlier version of this method discarded. A thin book is enough to cause it.
                note = why + "; the reduce-only close filled only " + close.executedQuantity().toPlainString()
                        + " of " + filled.toPlainString() + " — " + residual.toPlainString()
                        + " is still open. Keeping the protective stop and halting.";
                alerts.critical("Slippage abort left a residual position", plan.symbol() + ": " + note);
                halt.halt("partial reduce-only close on " + plan.symbol(), clock.instant());
            } else {
                closedCompletely = true;
                note = why + "; closed reduce-only";
                // The book entry goes; the realised PnL does not get invented here. It arrives from
                // the exchange's own income ledger on the next reconciliation pass, which is the only
                // source that knows what the close actually cost in slippage and fees.
                engine.registerClose(plan.symbol());
            }
        } catch (RuntimeException e) {
            note = why + "; the reduce-only close FAILED (" + e.getMessage()
                    + ") — keeping the protective stop and halting; check this position by hand";
            alerts.critical("Slippage abort could not close", plan.symbol() + ": " + note);
            halt.halt("reduce-only close failed on " + plan.symbol(), clock.instant());
        }

        // The stop is cancelled ONLY when the position is confirmed flat. Cancelling it on the
        // failure path would strip the protection off a position that is demonstrably still open —
        // and would do so immediately after telling the operator the stop was still working.
        if (closedCompletely) {
            placer.cancelQuietly(plan.symbol(), stop.clientOrderId());
        }
        return new Report(Outcome.ABORTED_ON_SLIPPAGE, plan, filled, avgPrice,
                Optional.of(entry), closedCompletely ? Optional.empty() : Optional.of(stop), List.of(), note);
    }

    private Report refused(TradePlan plan, String note) {
        LOG.warning("[Coordinator] refusing to execute " + plan.symbol() + ": " + note);
        return new Report(Outcome.REFUSED, plan, BigDecimal.ZERO, BigDecimal.ZERO,
                Optional.empty(), Optional.empty(), List.of(), note);
    }
}
