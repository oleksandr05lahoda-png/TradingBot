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
    /** Where the plan is written down before the entry goes out; see {@link EntryIntents}. */
    private volatile EntryIntents intents = EntryIntents.inMemory();
    /** Entries between "sent" and "protected or unwound": a shutdown must wait for zero. */
    private final java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger();

    /** Attempts at the protective stop before the position is unwound; only risk-free failures retry. */
    static final int STOP_PLACEMENT_ATTEMPTS = 3;
    static final long STOP_RETRY_DELAY_MS = 1_000L;
    /** How long an ambiguous entry is probed for a position before the halt is latched. */
    static final int UNKNOWN_ENTRY_PROBES = 5;
    static final long UNKNOWN_ENTRY_PROBE_DELAY_MS = 1_000L;

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

    /** Persistent intents (a file beside the ledger) instead of the in-memory default. */
    public ExecutionCoordinator withEntryIntents(EntryIntents intents) {
        this.intents = Preconditions.notNull(intents, "intents");
        return this;
    }

    public EntryIntents intents() { return intents; }

    /** Entries currently between fill and stop. A shutdown hook waits for this to reach zero. */
    public int inFlight() { return inFlight.get(); }

    /** Executes an approved plan. Never throws for an ordinary refusal — that comes back as a report. */
    public Report execute(TradePlan plan) throws InterruptedException {
        Preconditions.notNull(plan, "plan");
        inFlight.incrementAndGet();
        try {
            return executeGuarded(plan);
        } finally {
            inFlight.decrementAndGet();
        }
    }

    private Report executeGuarded(TradePlan plan) throws InterruptedException {
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

        // An intent still on record means an earlier entry on this symbol is unresolved: a limit
        // that could not be cancelled, a remainder resting on a held symbol, a send whose fate is
        // unknown. Recording a new one would replace it, and two entries could fill (audit 06.09).
        if (intents.get(plan.symbol(), clock.millis()).isPresent()) {
            return refused(plan, "an earlier entry on this symbol is still unresolved (intent on record); "
                    + "the reconciler clears it once the order is gone or the fill is protected");
        }

        // Margin mode and leverage before anything is sent. Both are idempotent.
        port.ensureIsolatedMargin(plan.symbol());
        port.setLeverage(plan.symbol(), plan.leverage());

        // The plan goes on record BEFORE the send. Between a market fill and its stop the book
        // knows nothing; if this process dies there, or the response is lost and the fill happened
        // anyway, the reconciler reads the intent and places the stop that was meant.
        intents.record(plan, clock.millis());

        OrderStatus entry;
        try {
            entry = placer.place(entryRequest);
        } catch (ExchangeException e) {
            if (!e.ambiguous()) {
                // A definite refusal never landed, so there is nothing to clean up.
                intents.clear(plan.symbol());
                return refused(plan, "entry refused by the exchange: " + e.getMessage());
            }
            // Outcome never established, so the order may be live; not swallowable as "signal failed".
            return unknownAfterSend(plan, stopRequest, e);
        } catch (RuntimeException e) {
            // A parse error mid-response is not a refusal: the request may well have executed.
            return unknownAfterSend(plan, stopRequest, e);
        }
        try {
            entry = awaitEntryResolution(entry, entryRequest);
        } catch (RuntimeException e) {
            // The order EXISTS by now. Any failure to READ it — a 429 on the poll, a body the
            // parser chokes on — is ignorance about a live order, never "the entry never landed".
            // Reported as refused, this was a filled position living outside the book (28.08 audit).
            return unknownAfterSend(plan, stopRequest, e);
        }

        BigDecimal filled = entry.executedQuantity();
        if (filled.signum() <= 0) {
            // UNKNOWN is a status string this build cannot read, not an absent order: the reconciler
            // already treats it as ignorance for stops, and the coordinator treated it as "did not
            // fill" for entries, wiping the intent of an order that may be live (audit 06.09).
            if (entry.isWorking() || entry.state() == OrderState.UNKNOWN) {
                // The window closed, the cancel did not go through, and the re-read still says the
                // order rests. A GTC limit that fills after this return would be a position the book
                // knows nothing about, so the intent is NOT spent: the reconciler inspects intent
                // symbols every pass, cancels the leftover entry once it is out of grace, adopts a
                // late fill with the planned stop, and clears the intent only once the symbol is
                // flat with nothing working (the limit-orphan gap of the 30.08 backlog).
                LOG.warning("[Coordinator] " + plan.symbol() + " entry " + entry.clientOrderId() + " is still "
                        + entry.state() + " after the fill window" + (entry.isWorking()
                                ? " and its cancel failed" : " (a state this build cannot read)") + " — intent kept");
                alerts.warning("Entry still resting after its window",
                        plan.symbol() + ": the limit entry could not be cancelled and may still fill. "
                                + "Its planned stop stays on record; the reconciler will cancel the order "
                                + "or protect the fill, whichever comes first.");
                return new Report(Outcome.NOT_FILLED, plan, BigDecimal.ZERO, BigDecimal.ZERO,
                        Optional.of(entry), Optional.empty(), List.of(),
                        "entry " + entry.state() + " — cancel failed, intent kept for the reconciler");
            }
            intents.clear(plan.symbol());
            LOG.info("[Coordinator] " + plan.symbol() + " entry did not fill (" + entry.state()
                    + ") — nothing to protect, nothing to clean up");
            return new Report(Outcome.NOT_FILLED, plan, BigDecimal.ZERO, BigDecimal.ZERO,
                    Optional.of(entry), Optional.empty(), List.of(), "entry " + entry.state());
        }

        BigDecimal avgPrice = entry.averagePrice().signum() > 0 ? entry.averagePrice() : plan.entryPrice();
        // Filled in part with the rest still resting (the cancel failed): the fill is protected
        // below, but the remainder can still fill onto a held symbol. The intent stays for it.
        boolean remainderResting = entry.isWorking();
        return protect(plan, stopRequest, Optional.of(entry), filled, avgPrice, remainderResting);
    }

    /**
     * Everything after a confirmed fill: the stop first, then the book, then the exits. Reached from
     * a normal fill and from an ambiguous send whose position turned up on the exchange anyway.
     */
    private Report protect(TradePlan plan, OrderRequest stopRequest, Optional<OrderStatus> entry,
                           BigDecimal filled, BigDecimal avgPrice, boolean remainderResting)
            throws InterruptedException {
        // The stop goes on now. Nothing between the fill and this.
        OrderStatus stop;
        try {
            stop = placeStopWithRetries(stopRequest);
        } catch (RuntimeException e) {
            return abandonUnprotectedPosition(plan, entry, filled, avgPrice, e);
        }

        engine.registerFill(plan, filled, avgPrice.doubleValue(), stop.clientOrderId());
        if (remainderResting) {
            // Protected and on the book, but the unfilled remainder still rests and could grow the
            // position past its stop's sizing. The intent keeps the symbol under the reconciler's
            // eye: it cancels the remainder once out of grace and clears the intent only then.
            BigDecimal rest = plan.quantity().subtract(filled);
            LOG.warning("[Coordinator] " + plan.symbol() + ": " + filled.toPlainString() + " filled and "
                    + "protected, " + rest.toPlainString() + " still rests and could not be cancelled — intent kept");
            alerts.warning("Entry remainder still resting",
                    plan.symbol() + ": " + filled.toPlainString() + " filled and protected, "
                            + rest.toPlainString() + " still rests and could not be cancelled; the "
                            + "reconciler cancels it once it is out of grace.");
        } else {
            // Protected and on the book: the intent has done its job.
            intents.clear(plan.symbol());
        }

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

        List<OrderStatus> takeProfits = placeTakeProfits(plan, OrderSide.toClose(plan.side()), filled,
                avgPrice, plan.filters());

        boolean partial = filled.compareTo(plan.quantity()) < 0;
        String note = partial
                ? String.format("partial fill %s of %s — stop and exits sized to what filled",
                        filled.toPlainString(), plan.quantity().toPlainString())
                : "filled in full";
        LOG.info("[Coordinator] " + plan.symbol() + " " + note + " @ " + avgPrice.toPlainString()
                + "; stop " + stop.clientOrderId() + "; " + takeProfits.size() + " exit(s)");

        return new Report(partial ? Outcome.PARTIALLY_FILLED : Outcome.FILLED, plan, filled, avgPrice,
                entry, Optional.of(stop), takeProfits, note);
    }

    /**
     * The stop is retried only on failures that provably sent nothing — a connection that never
     * opened, a rate limit the limiter sleeps out before the next send, a pre-send look-up that
     * failed. A coded refusal or an ambiguous send goes straight to the unwind: retrying those
     * could double an order. Three attempts a second apart cover the transient case that used to
     * market-close a fresh position over one 20 s timeout (audit 03.09).
     */
    private OrderStatus placeStopWithRetries(OrderRequest stopRequest) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                return placer.place(stopRequest);
            } catch (ExchangeException e) {
                if (!e.retryableWithoutRisk() || attempt >= STOP_PLACEMENT_ATTEMPTS) throw e;
                LOG.warning("[Coordinator] stop " + stopRequest.clientOrderId() + " was not sent ("
                        + e.getMessage() + ") — retrying " + (attempt + 1) + "/" + STOP_PLACEMENT_ATTEMPTS);
                sleeper.sleepMillis(STOP_RETRY_DELAY_MS);
            }
        }
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
            // The book is NOT touched: if it still holds the symbol, the exit happened on the
            // exchange (a stop, a take, a close whose response was lost) and the reconciler's
            // ghost path is what journals it. Closing the book here erased that exit from the
            // record the forward verdict is built on (audit 06.09).
            cancelLeftovers(symbol);
            intents.clear(symbol);
            LOG.info("[Coordinator] " + symbol + " was already flat — cancelled leftover orders"
                    + (engine.book().hasPosition(symbol) ? "; the book keeps it until the reconciler "
                            + "records the exchange-side exit" : ""));
            return new CloseReport(symbol, true, BigDecimal.ZERO, BigDecimal.ZERO, "already flat");
        }

        Side direction = position.direction().orElseThrow();
        BigDecimal held = position.absoluteQuantity();

        OrderStatus close;
        try {
            close = flatten(symbol, direction, held, requestId);
        } catch (RuntimeException e) {
            // Not a latch. The caller retries with backoff and escalates when the retries are spent;
            // halting here on one 429 stood the scanner down for the life of the process while the
            // second attempt closed the position seconds later (audit 03.09).
            boolean ambiguous = e instanceof ExchangeException x && x.ambiguous();
            if (ambiguous && Boolean.TRUE.equals(readFlat(symbol))) {
                // The send's outcome was never read, but the position is gone: the close executed.
                // Reported as "refused ... still in place" this told the operator something the code
                // did not know, and the retry then journaled nothing for a close of ours (audit 06.09).
                boolean cleaned = cancelLeftovers(symbol);
                engine.registerClose(symbol);
                intents.clear(symbol);
                BigDecimal price = closePriceIfReadable(symbol, requestId);
                LOG.info("[Coordinator] " + symbol + " close " + requestId + ": the response was lost but "
                        + "the position is flat — treating it as closed" + (cleaned ? "" : "; leftover orders "
                        + "await the orphan sweep"));
                return new CloseReport(symbol, true, held, price,
                        "closed reduce-only (the response was lost; the position read flat afterwards)"
                                + (cleaned ? "" : "; leftover orders await the orphan sweep"));
            }
            String note = ambiguous
                    ? "reduce-only close outcome UNKNOWN (" + e.getMessage() + ") — re-read on the retry; "
                            + "the stop stays until the position is confirmed flat"
                    : "reduce-only close was refused (" + e.getMessage()
                            + ") — the position and its stop are still in place";
            LOG.warning("[Coordinator] " + symbol + ": " + note);
            return new CloseReport(symbol, false, BigDecimal.ZERO, BigDecimal.ZERO, note);
        }

        BigDecimal residual = held.subtract(close.executedQuantity());
        if (residual.signum() != 0 && Boolean.TRUE.equals(readFlat(symbol))) {
            // The ORDER says less than `held` filled, the POSITION says nothing is left: a take or
            // the stop filled between the read at the top and this send, and the venue trimmed
            // the reduce-only quantity to what remained. Believing the order here latched the
            // halt on a "partial close" of a symbol that was flat, and left the book holding it.
            boolean cleaned = cancelLeftovers(symbol);
            engine.registerClose(symbol);
            intents.clear(symbol);
            LOG.info("[Coordinator] " + symbol + " closed " + close.executedQuantity().toPlainString()
                    + " of " + held.toPlainString() + " @ " + close.averagePrice().toPlainString()
                    + "; the remainder had already left through a resting exit"
                    + (cleaned ? "; protective orders cancelled" : "; protective orders NOT yet cancelled"));
            return new CloseReport(symbol, true, close.executedQuantity(), close.averagePrice(),
                    "closed reduce-only; the remainder had already left through a resting exit"
                            + (cleaned ? "" : "; leftover orders await the orphan sweep"));
        }
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

        // The close is confirmed: it must be booked and reported whatever the cleanup does. A 429 on
        // the cancel used to throw out of here BEFORE registerClose, so a real exit was retried as
        // a failed close and never reached the journal (audit 03.09). Leftovers are the orphan
        // sweep's job.
        boolean cleaned = cancelLeftovers(symbol);
        engine.registerClose(symbol);
        intents.clear(symbol);
        LOG.info("[Coordinator] " + symbol + " closed " + held.toPlainString()
                + " @ " + close.averagePrice().toPlainString()
                + (cleaned ? "; protective orders cancelled" : "; protective orders NOT yet cancelled"));
        return new CloseReport(symbol, true, close.executedQuantity(), close.averagePrice(),
                cleaned ? "closed reduce-only in full"
                        : "closed reduce-only in full; leftover orders await the orphan sweep");
    }

    /** The average price of the close under {@code requestId}, or ZERO when it cannot be read. */
    private BigDecimal closePriceIfReadable(String symbol, String requestId) {
        try {
            return port.queryOrder(symbol, ClientOrderIdFactory.create(requestId, OrderPurpose.EMERGENCY_CLOSE, 0))
                    .map(OrderStatus::averagePrice).orElse(BigDecimal.ZERO);
        } catch (RuntimeException e) {
            return BigDecimal.ZERO;
        }
    }

    /** @return {@code TRUE} flat, {@code FALSE} still held, {@code null} when the read failed */
    private Boolean readFlat(String symbol) {
        try {
            return port.openPositions().stream().noneMatch(p -> p.symbol().equals(symbol) && !p.isFlat());
        } catch (RuntimeException e) {
            LOG.warning("[Coordinator] " + symbol + ": could not re-read the position after the close ("
                    + e.getMessage() + ")");
            return null;
        }
    }

    /** @return {@code true} when the symbol's working orders are gone; {@code false} leaves them to the sweep */
    private boolean cancelLeftovers(String symbol) {
        try {
            // NOT cancelAllOpenOrders: that wipes every working order on the symbol, the owner's
            // included. The reconciler learned this on 06.09 - a resting entry he placed by hand is
            // his business, and a close of ours arriving on a symbol he already flattened himself
            // would silently take it with it. A leg that REDUCES is an orphan whoever placed it,
            // because the position it guarded is gone; anything else stays.
            boolean all = true;
            for (OrderStatus order : port.openOrders(symbol)) {
                if (!order.isWorking()) continue;
                boolean reduces = order.reduceOnly() || order.closePosition();
                if (!reduces && !ClientOrderIdFactory.isOurs(order.clientOrderId())) {
                    LOG.info("[Coordinator] " + symbol + ": leaving " + order.clientOrderId()
                            + " alone - it adds exposure and is not this machine's to cancel");
                    all = false;
                    continue;
                }
                port.cancelOrder(symbol, order.clientOrderId());
            }
            return all;
        } catch (RuntimeException e) {
            LOG.warning("[Coordinator] " + symbol + ": could not cancel leftover orders after the close ("
                    + e.getMessage() + ") - the reconciler sweeps them as orphans");
            return false;
        }
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
        if (legs.isEmpty()) {
            // The filled size is under the lot minimum for any exit leg: nothing to place, and a
            // silent log line used to be the only trace. The owner reads Telegram, not the log.
            alerts.warning("Position has NO take-profit",
                    plan.symbol() + ": the filled quantity " + filled.toPlainString()
                            + " is below the lot minimum for any take-profit leg. The stop still protects "
                            + "it; only the stop or a signal-driven close will end this position.");
            return placed;
        }
        for (int i = 0; i < legs.size(); i++) {
            TakeProfitPolicy.ProjectedLeg leg = legs.get(i);
            OrderRequest request = OrderRequest.takeProfit(plan.symbol(), closeSide, leg.quantity(),
                    leg.price(), ClientOrderIdFactory.create(plan.signalId(), OrderPurpose.TAKE_PROFIT, i));
            PreTradeValidator.Result check = PreTradeValidator.validate(request, filters, avgPrice);
            if (!check.ok()) {
                // Same weight as a leg the venue refused: with two legs one can be missing while the
                // other rests, and the "NO take-profit" alert below would stay quiet.
                LOG.warning("[Coordinator] skipping take-profit leg " + i + " at " + leg.rMultiple()
                        + "R: " + check.describe());
                alerts.warning("Take-profit leg not placed",
                        plan.symbol() + " leg " + i + " at " + leg.rMultiple() + "R would be rejected: "
                                + check.describe());
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

    /**
     * "Unknown" is not "did not happen". Before the latch, the exchange is asked whether the
     * position exists: an entry whose response was lost but that filled is the common case, and it
     * used to reach the next pass as a foreign position with no risk on record that the stop repair
     * refused to touch. Found, it is protected exactly like a confirmed fill. Not found (or
     * unseeable), the halt latches and the intent stays on record for the reconciler.
     */
    private Report unknownAfterSend(TradePlan plan, OrderRequest stopRequest, RuntimeException cause)
            throws InterruptedException {
        try {
            for (int probe = 1; probe <= UNKNOWN_ENTRY_PROBES; probe++) {
                sleeper.sleepMillis(UNKNOWN_ENTRY_PROBE_DELAY_MS);
                Optional<PositionSnapshot> found = port.openPositions().stream()
                        .filter(p -> p.symbol().equals(plan.symbol()) && !p.isFlat()
                                && p.direction().orElse(null) == plan.side())
                        .findFirst();
                if (found.isPresent()) {
                    PositionSnapshot position = found.get();
                    BigDecimal filled = position.absoluteQuantity();
                    BigDecimal avgPrice = position.entryPrice().signum() > 0
                            ? position.entryPrice() : plan.entryPrice();
                    LOG.warning("[Coordinator] " + plan.symbol() + ": the entry's response was lost ("
                            + cause.getMessage() + ") but the exchange holds " + filled.toPlainString()
                            + " @ " + avgPrice.toPlainString() + " — protecting it now");
                    alerts.warning("Entry landed despite a lost response",
                            plan.symbol() + ": " + filled.toPlainString() + " @ " + avgPrice.toPlainString()
                                    + " found on the exchange after " + probe + " probe(s); placing its stop.");
                    return protect(plan, stopRequest, Optional.empty(), filled, avgPrice, false);
                }
            }
        } catch (RuntimeException probeFailure) {
            LOG.warning("[Coordinator] " + plan.symbol() + ": could not probe for the position after the "
                    + "ambiguous send (" + probeFailure.getMessage() + ")");
        }
        String note = "the fate of the entry order is unknown (" + cause.getMessage()
                + ") and no position was seen in " + UNKNOWN_ENTRY_PROBES + " probes. A position may "
                + "still appear; its intended stop is on record and the reconciler will place it. "
                + "Trading is halted until an operator reconciles the account.";
        alerts.critical("Entry outcome unknown", plan.symbol() + ": " + note);
        halt.halt("entry outcome unknown on " + plan.symbol(), clock.instant());
        return new Report(Outcome.UNKNOWN_AFTER_SEND, plan, BigDecimal.ZERO, BigDecimal.ZERO,
                Optional.empty(), Optional.empty(), List.of(), note);
    }

    private Report abandonUnprotectedPosition(TradePlan plan, Optional<OrderStatus> entry, BigDecimal filled,
                                              BigDecimal avgPrice, RuntimeException cause)
            throws InterruptedException {
        LOG.severe("[Coordinator] " + plan.symbol() + " filled " + filled.toPlainString() + " @ "
                + avgPrice.toPlainString() + " but the stop was refused (" + cause.getMessage()
                + ") — closing the position");

        // Invariant: no position lives without a stop; the unwind below enforces it. Only a
        // confirmed-flat close avoids the halt — one symbol with unplaceable stops (live 14.08: stale
        // conditional orders on a venue that cannot list them) must not stop every other symbol.
        // Act first, then tell: a Telegram retry cycle must not sit between a naked fill and its unwind.
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
                intents.clear(plan.symbol());
                note = "stop refused (" + cause.getMessage() + "); position closed reduce-only in full — "
                        + "trading continues, but this symbol should be left alone until its conditional "
                        + "orders are cleaned up";
                alerts.critical("Protective stop could not be placed — position closed",
                        plan.symbol() + ": " + note);
            }
        } catch (RuntimeException e) {
            halt.halt("protective stop could not be placed on " + plan.symbol()
                    + " and the unwind failed", clock.instant());
            note = "stop refused (" + cause.getMessage() + ") AND the reduce-only close also failed ("
                    + e.getMessage() + ") — MANUAL INTERVENTION REQUIRED";
            alerts.critical("Naked position", plan.symbol() + ": " + note);
        }
        return new Report(Outcome.ABORTED_UNPROTECTED, plan, filled, avgPrice,
                entry, Optional.empty(), List.of(), note);
    }

    private Report closeOnSlippage(TradePlan plan, Optional<OrderStatus> entry, OrderStatus stop,
                                   BigDecimal filled, BigDecimal avgPrice, String why)
            throws InterruptedException {
        LOG.warning("[Coordinator] " + plan.symbol() + ": " + why + " — closing the position reduce-only");

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
                halt.halt("partial reduce-only close on " + plan.symbol(), clock.instant());
                alerts.critical("Slippage abort left a residual position", plan.symbol() + ": " + note);
            } else {
                closedCompletely = true;
                note = why + "; closed reduce-only";
                // Realised PnL is not invented here; it arrives from the exchange ledger next pass.
                engine.registerClose(plan.symbol());
                alerts.critical("Fill broke the risk budget — position closed", plan.symbol() + ": " + note);
            }
        } catch (RuntimeException e) {
            note = why + "; the reduce-only close FAILED (" + e.getMessage()
                    + ") — keeping the protective stop and halting; check this position by hand";
            halt.halt("reduce-only close failed on " + plan.symbol(), clock.instant());
            alerts.critical("Slippage abort could not close", plan.symbol() + ": " + note);
        }

        // Only when confirmed flat: otherwise this strips protection off a still-open position.
        if (closedCompletely) {
            placer.cancelQuietly(plan.symbol(), stop.clientOrderId());
        }
        return new Report(Outcome.ABORTED_ON_SLIPPAGE, plan, filled, avgPrice,
                entry, closedCompletely ? Optional.empty() : Optional.of(stop), List.of(), note);
    }

    private Report refused(TradePlan plan, String note) {
        LOG.warning("[Coordinator] refusing to execute " + plan.symbol() + ": " + note);
        return new Report(Outcome.REFUSED, plan, BigDecimal.ZERO, BigDecimal.ZERO,
                Optional.empty(), Optional.empty(), List.of(), note);
    }
}
