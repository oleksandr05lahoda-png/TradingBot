package com.bot.exec;

import com.bot.core.InstrumentFilters;
import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
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
 * Turns an approved {@link TradePlan} into orders, never leaving a filled position unprotected: the
 * stop goes on right after the fill sized to what <i>filled</i>, or the position is closed again.
 */
public final class ExecutionCoordinator {

    private static final Logger LOG = Logger.getLogger(ExecutionCoordinator.class.getName());

    public enum Outcome {
        FILLED,
        /** Stop and exits cover the filled quantity, not the intended one. */
        PARTIALLY_FILLED,
        NOT_FILLED,
        /** Filled, then closed again because the fill price broke the risk budget. */
        ABORTED_ON_SLIPPAGE,
        /** Filled, but the protective stop could not be placed. Trading is halted. */
        ABORTED_UNPROTECTED,
        /** Ambiguous send: a position may exist, unprotected and unrecorded. Trading is halted. */
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

        public boolean opened() {
            return outcome == Outcome.FILLED || outcome == Outcome.PARTIALLY_FILLED;
        }

        /** True when a position may exist that this process cannot account for — unknown, not "no". */
        public boolean mayHaveOpenedUnknownRisk() {
            return outcome == Outcome.UNKNOWN_AFTER_SEND || outcome == Outcome.ABORTED_UNPROTECTED;
        }
    }

    /** @param maxAdverseRiskOverrun realised-risk overshoot tolerated before closing again; 0.20 = 20% */
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
            // Checked before the entry: an unplaceable stop must not be discovered after the fill.
            return refused(plan, "protective stop would be rejected, so the entry is not sent: "
                    + stopCheck.describe());
        }

        // Margin mode and leverage before anything is sent. Both are idempotent.
        port.ensureIsolatedMargin(plan.symbol());
        port.setLeverage(plan.symbol(), plan.leverage());

        OrderStatus entry;
        try {
            entry = placer.place(entryRequest);
        } catch (ExchangeException e) {
            if (!e.ambiguous()) {
                // A definite refusal never landed, so there is nothing to clean up.
                return refused(plan, "entry refused by the exchange: " + e.getMessage());
            }
            // Outcome never established, so the order may be live; not swallowable as "signal failed".
            return unknownAfterSend(plan, e);
        } catch (RuntimeException e) {
            // A parse error mid-response is not a refusal: the request may well have executed.
            return unknownAfterSend(plan, e);
        }
        try {
            entry = awaitEntryResolution(entry, entryRequest);
        } catch (RuntimeException e) {
            // The order EXISTS by now. Any failure to READ it — a 429 on the poll, a body the
            // parser chokes on — is ignorance about a live order, never "the entry never landed".
            // Reported as refused, this was a filled position living outside the book (28.08 audit).
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

        // The stop goes on now. Nothing between the fill and this.
        OrderStatus stop;
        try {
            stop = placer.place(stopRequest);
        } catch (RuntimeException e) {
            return abandonUnprotectedPosition(plan, entry, filled, avgPrice, e);
        }

        engine.registerFill(plan, filled, avgPrice.doubleValue(), stop.clientOrderId());

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

    /** @param flat whether the symbol is confirmed to hold nothing afterwards */
    public record CloseReport(
            String symbol,
            boolean flat,
            BigDecimal closedQuantity,
            BigDecimal averagePrice,
            String note) {}

    /**
     * Flattens reduce-only, confirms, then cancels the protective orders — in that order, or the
     * position rides naked while the close runs. A partial close keeps its stop and halts instead.
     */
    public CloseReport closeOut(String symbol, String requestId) throws InterruptedException {
        Preconditions.notBlank(symbol, "symbol");
        Preconditions.notBlank(requestId, "requestId");

        PositionSnapshot position = port.openPositions().stream()
                .filter(p -> p.symbol().equals(symbol) && !p.isFlat())
                .findFirst()
                .orElse(null);

        if (position == null) {
            // Already flat. Any protective order still resting is an orphan by definition, so it goes.
            port.cancelAllOpenOrders(symbol);
            engine.registerClose(symbol);
            LOG.info("[Coordinator] " + symbol + " was already flat — cancelled leftover orders");
            return new CloseReport(symbol, true, BigDecimal.ZERO, BigDecimal.ZERO, "already flat");
        }

        Side direction = position.direction().orElseThrow();
        BigDecimal held = position.absoluteQuantity();

        OrderStatus close;
        try {
            close = flatten(symbol, direction, held, requestId);
        } catch (RuntimeException e) {
            String note = "reduce-only close was refused (" + e.getMessage()
                    + ") — the position and its stop are still in place";
            alerts.critical("Close failed", symbol + ": " + note);
            halt.halt("close failed on " + symbol, clock.instant());
            return new CloseReport(symbol, false, BigDecimal.ZERO, BigDecimal.ZERO, note);
        }

        BigDecimal residual = held.subtract(close.executedQuantity());
        if (residual.signum() != 0) {
            // Negative means the order under this id reports MORE filled than the position holds:
            // the placer adopted a terminal order from an earlier close under a repeated id, so it
            // describes a different close than this one. Reading that as "flat" would cancel the
            // protective orders off a position that is still open (28.08 review).
            String note = residual.signum() > 0
                    ? "closed " + close.executedQuantity().toPlainString() + " of "
                            + held.toPlainString() + "; " + residual.toPlainString()
                            + " still open, keeping the protective orders"
                    : "the order under this id reports " + close.executedQuantity().toPlainString()
                            + " filled against " + held.toPlainString() + " held — it describes an "
                            + "earlier close, not this one; keeping the protective orders";
            alerts.critical("Partial close", symbol + ": " + note);
            halt.halt("partial close on " + symbol, clock.instant());
            return new CloseReport(symbol, false, close.executedQuantity(), close.averagePrice(), note);
        }

        port.cancelAllOpenOrders(symbol);
        engine.registerClose(symbol);
        LOG.info("[Coordinator] " + symbol + " closed " + held.toPlainString()
                + " @ " + close.averagePrice().toPlainString() + "; protective orders cancelled");
        return new CloseReport(symbol, true, close.executedQuantity(), close.averagePrice(),
                "closed reduce-only in full");
    }

    /** Closes reduce-only. Deliberately ignores {@link TradingHalt}: a halt stops opening, never closing. */
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
                // A missing exit is a lost opportunity, not a lost account: alert, do not escalate.
                alerts.warning("Take-profit leg not placed",
                        plan.symbol() + " leg " + i + " at " + leg.rMultiple() + "R: " + e.getMessage());
            }
        }
        // Losing every leg is not a thinner exit but no exit at all, so it must not hide inside a
        // per-leg warning — seen live 14.08, the venue's conditional-order cap swallowed both legs.
        if (placed.isEmpty() && !legs.isEmpty()) {
            alerts.warning("Position has NO take-profit",
                    plan.symbol() + ": every take-profit leg was refused. The stop still protects it, "
                            + "but nothing will bank a win automatically — only the stop or a "
                            + "signal-driven close will end this position.");
        }
        return placed;
    }

    private OrderStatus awaitEntryResolution(OrderStatus initial, OrderRequest request) throws InterruptedException {
        OrderStatus current = initial;
        for (int attempt = 0; attempt < settings.fillPollAttempts(); attempt++) {
            if (current.state().isTerminal()) return current;
            sleeper.sleepMillis(settings.fillPollIntervalMs());
            current = port.queryOrder(request.symbol(), request.clientOrderId()).orElse(current);
        }

        if (current.isWorking()) {
            // Cancel the remainder so the size stops growing, then re-read the truth from the exchange.
            LOG.info("[Coordinator] entry " + request.clientOrderId() + " still " + current.state()
                    + " — cancelling the remainder");
            placer.cancelQuietly(request.symbol(), request.clientOrderId());
            current = port.queryOrder(request.symbol(), request.clientOrderId()).orElse(current);
        }
        return current;
    }

    /** Halts and alerts rather than refusing: "unknown" is not "did not happen". */
    private Report unknownAfterSend(TradePlan plan, RuntimeException cause) {
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
                        + "). Closing the position.");

        // Invariant: no position lives without a stop; the unwind below enforces it. Only a
        // confirmed-flat close avoids the halt — one symbol with unplaceable stops (live 14.08: stale
        // conditional orders on a venue that cannot list them) must not stop every other symbol.
        String note;
        try {
            OrderStatus close = flatten(plan.symbol(), plan.side(), filled, plan.signalId());
            BigDecimal residual = filled.subtract(close.executedQuantity());
            if (residual.signum() > 0) {
                halt.halt("protective stop could not be placed on " + plan.symbol()
                        + " and the unwind left a remainder", clock.instant());
                note = "stop refused; the reduce-only close only filled " + close.executedQuantity()
                        .toPlainString() + " of " + filled.toPlainString()
                        + " — " + residual.toPlainString() + " REMAINS OPEN AND UNPROTECTED";
                alerts.critical("Naked position", plan.symbol() + ": " + note);
            } else {
                note = "stop refused; position closed reduce-only in full — trading continues, "
                        + "but this symbol should be left alone until its conditional orders are cleaned up";
                LOG.warning("[Coordinator] " + plan.symbol() + ": " + note);
            }
        } catch (RuntimeException e) {
            halt.halt("protective stop could not be placed on " + plan.symbol()
                    + " and the unwind failed", clock.instant());
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
                // Accepted but incomplete: no error is thrown, only the response quantity shows it.
                note = why + "; the reduce-only close filled only " + close.executedQuantity().toPlainString()
                        + " of " + filled.toPlainString() + " — " + residual.toPlainString()
                        + " is still open. Keeping the protective stop and halting.";
                alerts.critical("Slippage abort left a residual position", plan.symbol() + ": " + note);
                halt.halt("partial reduce-only close on " + plan.symbol(), clock.instant());
            } else {
                closedCompletely = true;
                note = why + "; closed reduce-only";
                // Realised PnL is not invented here; it arrives from the exchange ledger next pass.
                engine.registerClose(plan.symbol());
            }
        } catch (RuntimeException e) {
            note = why + "; the reduce-only close FAILED (" + e.getMessage()
                    + ") — keeping the protective stop and halting; check this position by hand";
            alerts.critical("Slippage abort could not close", plan.symbol() + ": " + note);
            halt.halt("reduce-only close failed on " + plan.symbol(), clock.instant());
        }

        // Only when confirmed flat: otherwise this strips protection off a still-open position.
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
