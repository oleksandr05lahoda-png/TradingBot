package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.exec.OrderTypes.OrderState;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.risk.ExposureBook;
import com.bot.risk.LiquidationSafety;
import com.bot.risk.RiskEngine;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Brings local belief back in line with the exchange and stops trading when the two disagree: the
 * book is realigned <i>and</i> {@link TradingHalt} is tripped, since drift is a defect rather than a
 * number to correct and forget.
 */
public final class Reconciler {

    private static final Logger LOG = Logger.getLogger(Reconciler.class.getName());

    public record Drift(Kind kind, String symbol, String detail) {

        public enum Kind {
            /** The exchange holds a position the book does not know about. */
            UNKNOWN_POSITION,
            /** The book holds a position the exchange says is flat — how a stop-out or TP fill looks. */
            GHOST_POSITION,
            /** The position shrank — a partial exit filled on the exchange; the stop still covers the rest. */
            QUANTITY_MISMATCH,
            /** The position GREW without this bot's doing — something else is trading this symbol. */
            POSITION_GREW,
            SIDE_MISMATCH,
            /** An open position with no working stop — what a crash between fill and stop leaves. */
            POSITION_WITHOUT_STOP,
            /** A working order on a symbol with no position, old enough not to be a race. */
            ORPHAN_ORDER,
            /** The resting stop no longer sits far enough inside the exchange's liquidation price. */
            LIQUIDATION_BUFFER_BREACHED,
            /** A missing stop was re-placed from the book's own record — repaired, not just reported. */
            STOP_REPLACED,
            /** A missing stop could not be re-placed, so the position was closed reduce-only. */
            UNPROTECTED_CLOSED
        }

        /**
         * Critical means the account holds risk this process cannot vouch for; only that halts.
         * A ghost, a shrink or an orphan is the normal wake of an exchange-side exit — realigned and
         * announced, never halted on: the first live day proved that freezing after every take-profit
         * turns a 24/7 machine into one that trades from boot to first winner.
         */
        public boolean isCritical() {
            return kind == Kind.POSITION_WITHOUT_STOP || kind == Kind.SIDE_MISMATCH
                    || kind == Kind.UNKNOWN_POSITION || kind == Kind.POSITION_GREW
                    || kind == Kind.LIQUIDATION_BUFFER_BREACHED;
        }
    }

    public record Report(Instant at, List<Drift> drifts, int exchangePositions, int bookPositionsBefore) {

        public Report {
            drifts = List.copyOf(drifts);
        }

        public boolean converged() { return drifts.isEmpty(); }

        /** True when nothing here calls for a halt — clean, or only exchange-side exits to absorb. */
        public boolean healthy() { return drifts.stream().noneMatch(Drift::isCritical); }

        public String describe() {
            if (drifts.isEmpty()) return "converged";
            StringBuilder sb = new StringBuilder();
            for (Drift d : drifts) {
                sb.append("\n  - ").append(d.kind()).append(' ').append(d.symbol()).append(": ").append(d.detail());
            }
            return sb.toString();
        }
    }

    private final ExchangePort port;
    private final RiskEngine engine;
    private final TradingHalt halt;
    private final AlertSink alerts;
    private final IdempotentOrderPlacer placer;
    private final BigDecimal quantityTolerance;
    private final long orphanGraceMillis;

    /** Hears benign exchange-side exits (ghost/shrink), for the trade journal. Never trading logic. */
    private volatile java.util.function.BiConsumer<String, String> exchangeExitListener;

    /** Symbols whose working orders were still inside the grace window on the previous pass. */
    private volatile Set<String> carriedOverSymbols = Set.of();

    /** Symbols whose stop read "triggered" last pass, with the position still open. */
    private volatile Set<String> triggeredStops = Set.of();

    /** Flattens reduce-only through the same path as an operator {@code /close}. */
    @FunctionalInterface
    public interface PositionCloser {
        ExecutionCoordinator.CloseReport close(String symbol, String requestId) throws InterruptedException;
    }

    /** When set, a booked position found without a stop is REPAIRED, not just halted on. */
    private volatile PositionCloser closer;

    /** Consecutive passes a symbol's stop could neither be confirmed nor denied. */
    private final Map<String, Integer> unconfirmablePasses = new java.util.concurrent.ConcurrentHashMap<>();
    /** Passes an unconfirmable stop is tolerated before it is treated as missing. */
    static final int UNCONFIRMABLE_PASSES_LIMIT = 3;

    /** Consecutive passes the realised-PnL seed failed; at the threshold the operator hears about it. */
    private int pnlSeedFailures = 0;
    private boolean pnlSeedAlerted = false;
    static final int PNL_SEED_FAILURES_BEFORE_ALERT = 10;

    /** Hears a repair close, for the trade journal — the one real-money exit no operator typed. */
    private volatile CloseObserver closeObserver;

    /** Arms the missing-stop repair: re-place from the book's record, or close reduce-only. */
    public void withStopRepair(PositionCloser closer) {
        this.closer = Preconditions.notNull(closer, "closer");
    }

    /** Records repair closes in the trade journal; optional, and never allowed to throw. */
    public void onRepairClose(CloseObserver observer) {
        this.closeObserver = observer;
    }

    public Reconciler(ExchangePort port, RiskEngine engine, TradingHalt halt, AlertSink alerts,
                      IdempotentOrderPlacer placer) {
        this(port, engine, halt, alerts, placer, new BigDecimal("0.0000001"), 60_000L);
    }

    /** (symbol, detail) for each ghost or shrink the pass absorbed. */
    public void onExchangeExit(java.util.function.BiConsumer<String, String> listener) {
        this.exchangeExitListener = listener;
    }

    public Reconciler(ExchangePort port, RiskEngine engine, TradingHalt halt, AlertSink alerts,
                      IdempotentOrderPlacer placer, BigDecimal quantityTolerance, long orphanGraceMillis) {
        this.port = Preconditions.notNull(port, "port");
        this.engine = Preconditions.notNull(engine, "engine");
        this.halt = Preconditions.notNull(halt, "halt");
        this.alerts = Preconditions.notNull(alerts, "alerts");
        this.placer = Preconditions.notNull(placer, "placer");
        this.quantityTolerance = Preconditions.notNull(quantityTolerance, "quantityTolerance");
        this.orphanGraceMillis = orphanGraceMillis;
    }

    /** One pass: realigns local state and cancels provably unmanaged orders. Never opens anything. */
    public Report reconcile(Instant now) {
        Preconditions.notNull(now, "now");

        AccountSnapshot account = port.fetchAccount();
        List<PositionSnapshot> exchangePositions = port.openPositions().stream()
                .filter(p -> !p.isFlat())
                .toList();
        ExposureBook book = engine.book();
        List<ExposureBook.OpenPosition> before = book.all();
        Map<String, ExposureBook.OpenPosition> localBySymbol = new LinkedHashMap<>();
        for (ExposureBook.OpenPosition p : before) localBySymbol.put(p.symbol(), p);

        List<Drift> drifts = new ArrayList<>();
        List<ExposureBook.OpenPosition> truth = new ArrayList<>();
        Set<String> symbolsToInspect = new HashSet<>(localBySymbol.keySet());

        for (PositionSnapshot position : exchangePositions) {
            symbolsToInspect.add(position.symbol());
            Side direction = position.direction().orElseThrow();
            BigDecimal quantity = position.absoluteQuantity();
            double entryPrice = position.entryPrice().doubleValue();
            ExposureBook.OpenPosition local = localBySymbol.get(position.symbol());

            if (local == null) {
                drifts.add(new Drift(Drift.Kind.UNKNOWN_POSITION, position.symbol(),
                        "exchange holds " + direction + " " + quantity.toPlainString()
                                + " @ " + position.entryPrice().toPlainString() + ", the book holds nothing"));
            } else {
                if (local.side() != direction) {
                    drifts.add(new Drift(Drift.Kind.SIDE_MISMATCH, position.symbol(),
                            "book says " + local.side() + ", exchange says " + direction));
                }
                BigDecimal delta = quantity.subtract(local.quantity());
                if (delta.abs().compareTo(quantityTolerance) > 0) {
                    // Direction decides severity: shrinking is a partial exit the closePosition stop
                    // still covers; growing means another actor holds risk under this bot's name.
                    drifts.add(new Drift(
                            delta.signum() > 0 ? Drift.Kind.POSITION_GREW : Drift.Kind.QUANTITY_MISMATCH,
                            position.symbol(),
                            "book says " + local.quantity().toPlainString()
                                    + ", exchange says " + quantity.toPlainString()));
                }
            }

            // The exchange stores no intended stop, so the distance comes from the local record; with
            // none, risk is 0 — an understatement never sized against, since UNKNOWN_POSITION halts.
            double stopDistance = local != null && local.quantity().signum() > 0
                    ? local.riskUsd() / local.quantity().doubleValue()
                    : 0.0;
            // The stop id must be carried across passes; the exchange cannot supply it again.
            truth.add(new ExposureBook.OpenPosition(position.symbol(), direction, quantity, entryPrice,
                    quantity.doubleValue() * entryPrice, quantity.doubleValue() * stopDistance,
                    local == null ? Optional.empty() : local.protectiveStopId()));
        }

        Set<String> exchangeSymbols = new HashSet<>();
        for (PositionSnapshot p : exchangePositions) exchangeSymbols.add(p.symbol());
        for (ExposureBook.OpenPosition local : before) {
            if (!exchangeSymbols.contains(local.symbol())) {
                drifts.add(new Drift(Drift.Kind.GHOST_POSITION, local.symbol(),
                        "the book holds " + local.side() + " " + local.quantity().toPlainString()
                                + ", the exchange is flat — " + describeExit(local)));
            }
        }

        // Exchange wins, always and immediately, before any of the checks below act on the book.
        book.replaceAll(truth);

        // Carried over, or an order inside the grace window at that one moment is never re-inspected.
        symbolsToInspect.addAll(carriedOverSymbols);
        Set<String> stillInteresting = new HashSet<>();
        Set<String> stillTriggered = new HashSet<>();

        for (String symbol : symbolsToInspect) {
            List<OrderStatus> working = port.openOrders(symbol).stream().filter(OrderStatus::isWorking).toList();
            boolean hasPosition = exchangeSymbols.contains(symbol);

            if (hasPosition) {
                Optional<OrderStatus> protectiveStop = working.stream()
                        .filter(o -> o.type() == OrderType.STOP_MARKET && (o.reduceOnly() || o.closePosition()))
                        .findFirst();
                if (protectiveStop.isEmpty()) {
                    ExposureBook.OpenPosition local = localBySymbol.get(symbol);
                    boolean recorded = local != null && local.protectiveStopId().isPresent();
                    if (!port.canListConditionalOrders() || recorded) {
                        // The listing is the normal proof. When it shows nothing but an id is on
                        // record, one look by name comes before the alarm: positions are read at
                        // the top of the pass and orders seconds later, so a stop that fires in
                        // between looks exactly like a naked position - on the most volatile
                        // minute of the day, when a false emergency costs the most trust.
                        protectiveStop = confirmStopByName(symbol, local, drifts, stillTriggered, now);
                    } else {
                        handleMissingStop(symbol,
                                "an open position has no working reduce-only stop on the exchange",
                                drifts, now, StopOrigin.CONFIRMED_MISSING);
                    }
                }
                protectiveStop.ifPresent(stop -> {
                    unconfirmablePasses.remove(symbol);
                    checkLiquidationBuffer(symbol, exchangePositions, stop, drifts);
                });
            } else {
                for (OrderStatus order : working) {
                    boolean oldEnough = order.updateTimeMs() > 0
                            && now.toEpochMilli() - order.updateTimeMs() > orphanGraceMillis;
                    if (!oldEnough) {
                        // May belong to an entry still being worked; let the next pass decide.
                        stillInteresting.add(symbol);
                        continue;
                    }
                    drifts.add(new Drift(Drift.Kind.ORPHAN_ORDER, symbol,
                            order.type() + " " + order.clientOrderId()
                                    + " is working with no position behind it — cancelling"));
                    placer.cancelQuietly(symbol, order.clientOrderId());
                }
            }
        }
        carriedOverSymbols = stillInteresting;
        triggeredStops = stillTriggered;
        // A symbol no longer held has nothing to confirm; a stale count must not ambush a re-entry.
        unconfirmablePasses.keySet().retainAll(exchangeSymbols);

        // From the exchange's ledger, not a local tally: a restart must not clear the day's loss.
        long utcMidnight = LocalDate.ofInstant(now, ZoneOffset.UTC)
                .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        try {
            engine.killSwitch().seedRealizedPnl(port.fetchRealizedPnlSince(utcMidnight), now);
            if (pnlSeedAlerted) {
                alerts.info("Daily-loss feed is back",
                        "realised PnL is being read from the exchange again after "
                                + pnlSeedFailures + " failed pass(es)");
            }
            pnlSeedFailures = 0;
            pnlSeedAlerted = false;
        } catch (RuntimeException e) {
            // A stale feed is a protection quietly off: with realised PnL stuck at its last value
            // (zero after a UTC rollover), six stop-outs in a row cannot trip the daily limit.
            // A log line was the only trace of that; now the operator hears when it persists.
            pnlSeedFailures++;
            LOG.warning("[Reconciler] realised PnL could not be re-seeded from the exchange ("
                    + pnlSeedFailures + " in a row): " + e.getMessage());
            if (pnlSeedFailures == PNL_SEED_FAILURES_BEFORE_ALERT && !pnlSeedAlerted) {
                pnlSeedAlerted = true;
                alerts.warning("Daily-loss limit is part-blind",
                        "realised PnL could not be read for " + pnlSeedFailures + " passes ("
                                + e.getMessage() + "). Closed losses are NOT reaching the daily "
                                + "kill switch; only open unrealised loss still counts. Per-position "
                                + "stops are unaffected.");
            }
        }
        engine.killSwitch().observeOpenUnrealizedPnl(account.totalUnrealizedPnl().doubleValue(), now);
        engine.killSwitch().observeBalance(Math.max(1e-9, account.equityUsd()), now);

        Report report = new Report(now, drifts, exchangePositions.size(), before.size());
        if (!report.healthy()) {
            Drift first = drifts.stream().filter(Drift::isCritical).findFirst().orElseThrow();
            String summary = "reconciliation found " + drifts.size() + " disagreement(s):" + report.describe();
            LOG.severe("[Reconciler] " + summary);
            alerts.critical("Reconciliation drift", summary);
            halt.halt("reconciliation drift: " + first.kind() + " on " + first.symbol(), now);
        } else if (!report.converged()) {
            // The exchange finished trades on its own — stops and takes doing their job. The book is
            // realigned above; the operator hears about it, the machine keeps trading. A repaired
            // stop (STOP_REPLACED / UNPROTECTED_CLOSED) already sent its own critical alert and must
            // not be re-announced as an exchange-side exit it is not.
            boolean exchangeSide = drifts.stream().anyMatch(d ->
                    d.kind() == Drift.Kind.GHOST_POSITION || d.kind() == Drift.Kind.QUANTITY_MISMATCH
                            || d.kind() == Drift.Kind.ORPHAN_ORDER);
            String summary = "the exchange closed or trimmed position(s) while this process watched:"
                    + report.describe();
            if (exchangeSide) {
                LOG.info("[Reconciler] " + summary);
                alerts.warning("Exchange-side exit", summary);
            } else {
                LOG.info("[Reconciler] pass repaired protection without drift to announce:"
                        + report.describe());
            }
            java.util.function.BiConsumer<String, String> listener = exchangeExitListener;
            if (listener != null) {
                for (Drift d : drifts) {
                    if (d.kind() == Drift.Kind.GHOST_POSITION || d.kind() == Drift.Kind.QUANTITY_MISMATCH) {
                        try {
                            listener.accept(d.symbol(), d.detail());
                        } catch (RuntimeException e) {
                            LOG.fine("[Reconciler] exit listener failed: " + e.getMessage());
                        }
                    }
                }
            }
        } else {
            LOG.fine("[Reconciler] converged: " + exchangePositions.size() + " position(s)");
        }
        return report;
    }

    /**
     * Names what took the position out, so the exit alert reads as a fact instead of a mystery.
     * Best-effort: the answer changes the wording, never the verdict.
     */
    private String describeExit(ExposureBook.OpenPosition local) {
        Optional<String> stopId = local.protectiveStopId();
        if (stopId.isEmpty()) return "it closed without this process noticing";
        try {
            Optional<OrderStatus> stop = port.queryOrder(local.symbol(), stopId.get());
            if (stop.isPresent() && stop.get().state() == OrderState.FILLED) {
                return "its protective stop " + stopId.get() + " filled";
            }
            if (stop.isPresent() && stop.get().isWorking()) {
                return "closed past its still-resting stop (a take-profit fill or a manual close); "
                        + "leftover orders will be swept as orphans";
            }
        } catch (RuntimeException e) {
            LOG.fine("[Reconciler] could not name the exit on " + local.symbol() + ": " + e.getMessage());
        }
        return "it closed without this process noticing";
    }

    /**
     * Confirms the stop by name, for a venue that answers a query for a conditional order but has no
     * endpoint enumerating them ({@code demo-fapi}). A missing id or a failed lookup leaves the
     * question open rather than declaring the position naked; other states add their own drift.
     */
    private Optional<OrderStatus> confirmStopByName(String symbol, ExposureBook.OpenPosition local,
                                                    List<Drift> drifts, Set<String> stillTriggered,
                                                    Instant now) {
        Optional<String> recorded = local == null ? Optional.empty() : local.protectiveStopId();
        if (recorded.isEmpty()) {
            LOG.warning("[Reconciler] " + symbol + " holds a position, this venue does not list "
                    + "conditional orders and no stop id is on record — cannot confirm its stop is in place");
            countUnconfirmable(symbol, "no stop id on record and the venue lists nothing", drifts, now);
            return Optional.empty();
        }

        String stopId = recorded.get();
        Optional<OrderStatus> stop;
        try {
            stop = port.queryOrder(symbol, stopId);
        } catch (RuntimeException e) {
            // Unreadable is not absent: halting on one failed lookup is halting on a network blip.
            LOG.warning("[Reconciler] " + symbol + ": stop " + stopId + " could not be read this pass ("
                    + e.getMessage() + ")");
            countUnconfirmable(symbol, "stop " + stopId + " unreadable: " + e.getMessage(), drifts, now);
            return Optional.empty();
        }
        if (stop.isPresent() && stop.get().isWorking()) {
            unconfirmablePasses.remove(symbol);
            return stop;
        }

        if (stop.isPresent() && stop.get().state() == OrderState.UNKNOWN) {
            // A state this build cannot read is ignorance, not an absent stop — but ignorance three
            // passes long stops being caution: one renamed venue enum must not hide a naked
            // position forever (the silent-forever hole the 28.08 audit confirmed).
            LOG.warning("[Reconciler] " + symbol + ": stop " + stopId + " came back in a state this "
                    + "build cannot read — not confirming it either way");
            countUnconfirmable(symbol, "stop " + stopId + " is in a state this build cannot read",
                    drifts, now);
            return Optional.empty();
        }
        unconfirmablePasses.remove(symbol);

        // FILLED: the stop itself fired. EXPIRED: the venue retired it because the position is on
        // its way out - which is what Binance does the instant a take-profit triggers, seconds
        // before the closing market order lands. Both mean "this position is leaving", and both
        // looked identical to a naked position: STXUSDT halted the bot for nine hours on 25.08,
        // sixteen seconds before its take filled for +$1.19. One pass of grace for each - not two,
        // because a trigger whose market order never landed reads the same from here.
        OrderState state = stop.map(OrderStatus::state).orElse(OrderState.UNKNOWN);
        if (state == OrderState.FILLED || state == OrderState.EXPIRED) {
            String what = state == OrderState.FILLED ? "triggered" : "was retired by the venue";
            if (!triggeredStops.contains(symbol)) {
                stillTriggered.add(symbol);
                LOG.warning("[Reconciler] " + symbol + ": stop " + stopId + " " + what
                        + "; expecting the position to be gone by the next pass");
                return Optional.empty();
            }
            handleMissingStop(symbol,
                    "stop " + stopId + " " + what + ", yet the position is still open a pass later",
                    drifts, now, StopOrigin.CONFIRMED_MISSING);
            return Optional.empty();
        }

        handleMissingStop(symbol,
                "the stop on record (" + stopId + ") is "
                        + stop.map(s -> s.state().toString()).orElse("unknown to the exchange")
                        + " while the position is still open",
                drifts, now, StopOrigin.CONFIRMED_MISSING);
        return Optional.empty();
    }

    /** One more pass of ignorance; at the limit it stops being caution and becomes a missing stop. */
    private void countUnconfirmable(String symbol, String why, List<Drift> drifts, Instant now) {
        int passes = unconfirmablePasses.merge(symbol, 1, Integer::sum);
        if (passes >= UNCONFIRMABLE_PASSES_LIMIT) {
            unconfirmablePasses.remove(symbol);
            handleMissingStop(symbol,
                    "the stop could not be confirmed for " + passes + " passes in a row (" + why + ")",
                    drifts, now, StopOrigin.UNCONFIRMABLE);
        }
    }

    /**
     * Whether the stop is <b>known</b> gone (the venue named it cancelled, filled or absent) or
     * merely unreadable. The difference decides what a failed repair may do: a stop never proven
     * gone must not cost the position a market close on read failures alone.
     */
    private enum StopOrigin { CONFIRMED_MISSING, UNCONFIRMABLE }

    /**
     * A stop-shaped invariant gets a stop-shaped repair, not just a latch: re-place the stop from
     * the book's own record (entry ∓ riskUsd/quantity reconstructs the recorded distance), and if
     * the venue refuses, close the position reduce-only through the same path as an operator
     * {@code /close}. Only a position this bot sized is touched — {@code riskUsd > 0} — because a
     * price cannot be reconstructed for an adopted one, and flattening what the owner may be
     * holding by hand is not this process's call. Repair failing on both legs is the old emergency:
     * the critical drift below latches the halt.
     */
    private void handleMissingStop(String symbol, String detail, List<Drift> drifts, Instant now,
                                   StopOrigin origin) {
        unconfirmablePasses.remove(symbol);
        PositionCloser close = this.closer;
        ExposureBook.OpenPosition local = engine.book().get(symbol).orElse(null);
        boolean repairable = close != null && local != null
                && local.riskUsd() > 0 && local.quantity().signum() > 0;
        if (!repairable) {
            drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol, detail));
            return;
        }

        double distance = local.riskUsd() / local.quantity().doubleValue();
        double raw = local.side() == Side.LONG
                ? local.entryPrice() - distance
                : local.entryPrice() + distance;
        if (raw > 0 && Double.isFinite(raw)) {
            try {
                com.bot.core.InstrumentFilters filters = port.fetchFilters(symbol);
                BigDecimal stopPrice = filters.quantizeStopPrice(local.side(), raw);
                OrderRequest request = OrderRequest.protectiveStop(symbol,
                        OrderTypes.OrderSide.toClose(local.side()), stopPrice,
                        ClientOrderIdFactory.create(
                                "requard-" + symbol + "-" + now.getEpochSecond(),
                                OrderTypes.OrderPurpose.STOP_LOSS, 0));
                PreTradeValidator.Result check = PreTradeValidator.validate(request, filters,
                        BigDecimal.valueOf(local.entryPrice()));
                if (check.ok()) {
                    OrderStatus placed = placer.place(request);
                    engine.book().recordStop(symbol, placed.clientOrderId());
                    // Retire the id on record only AFTER the replacement rests, never before: on the
                    // unconfirmable path the old stop may well be alive, and cancelling first would
                    // open a window with no protection at all. Cancelling second can only leave a
                    // duplicate for a moment. A failed cancel is tolerated — it degrades to the
                    // duplicate this ordering already accepts, not to a naked position.
                    local.protectiveStopId()
                            .filter(oldId -> !oldId.equals(placed.clientOrderId()))
                            .ifPresent(oldId -> placer.cancelQuietly(symbol, oldId));
                    drifts.add(new Drift(Drift.Kind.STOP_REPLACED, symbol,
                            detail + " — re-placed at " + stopPrice.toPlainString()
                                    + " as " + placed.clientOrderId()));
                    alerts.critical("Stop was missing — re-placed", symbol + ": " + detail
                            + ". A new reduce-only stop now rests at " + stopPrice.toPlainString()
                            + " (" + placed.clientOrderId() + "); trading continues.");
                    return;
                }
                LOG.warning("[Reconciler] " + symbol + ": replacement stop would be rejected ("
                        + check.describe() + ") — closing instead");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol,
                        detail + " (repair interrupted)"));
                return;
            } catch (RuntimeException e) {
                LOG.warning("[Reconciler] " + symbol + ": replacement stop was refused ("
                        + e.getMessage() + ") — closing instead");
            }
        }

        if (origin == StopOrigin.UNCONFIRMABLE) {
            // The stop was never proven gone — three passes of unreadable answers is a venue
            // problem, and the same outage is why the replacement was refused. Market-closing a
            // position that may well still be protected would turn a read failure into a realised
            // loss. Halt and let a human look, which is what the latch is for.
            drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol, detail
                    + " — a replacement stop was also refused; the position is NOT closed because "
                    + "the original stop was never proven gone"));
            return;
        }

        try {
            ExecutionCoordinator.CloseReport report = close.close(symbol,
                    "requard-close-" + symbol + "-" + now.getEpochSecond());
            if (report.flat()) {
                boolean actuallyClosed = report.closedQuantity().signum() > 0;
                drifts.add(new Drift(Drift.Kind.UNPROTECTED_CLOSED, symbol,
                        detail + " — a replacement stop could not be placed; closed reduce-only ("
                                + report.note() + ")"));
                if (actuallyClosed) {
                    alerts.critical("Unprotected position closed", symbol + ": " + detail
                            + ". A replacement stop was refused, so the position was closed reduce-only.");
                    CloseObserver observer = this.closeObserver;
                    if (observer != null) {
                        try {
                            observer.closed("requard-close-" + symbol + "-" + now.getEpochSecond(),
                                    symbol, report);
                        } catch (RuntimeException e) {
                            LOG.fine("[Reconciler] close observer failed: " + e.getMessage());
                        }
                    }
                } else {
                    // Nothing was closed because nothing was there: the position left on its own
                    // between the read and the repair. Not an emergency, and not a close.
                    alerts.warning("Stop repair found the position already gone", symbol + ": "
                            + detail + ". It had already exited before the repair ran; nothing was closed.");
                }
                return;
            }
            LOG.severe("[Reconciler] " + symbol + ": the reduce-only close did not complete: "
                    + report.note());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            LOG.severe("[Reconciler] " + symbol + ": the reduce-only close failed: " + e.getMessage());
        }
        drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol,
                detail + " — re-placing the stop AND closing reduce-only both failed"));
    }

    /**
     * Runtime counterpart to the pre-trade check, against the liquidation price the <b>exchange</b>
     * reports: it moves after approval (margin changes, funding, a bracket change), so a stop can
     * drift outside it with no order changing. Zero means "not reachable" and is skipped.
     */
    private void checkLiquidationBuffer(String symbol, List<PositionSnapshot> positions,
                                        OrderStatus stop, List<Drift> drifts) {
        PositionSnapshot position = positions.stream()
                .filter(p -> p.symbol().equals(symbol))
                .findFirst()
                .orElse(null);
        if (position == null) return;

        double liquidation = position.liquidationPrice().doubleValue();
        double entry = position.entryPrice().doubleValue();
        double stopPrice = stop.stopPrice().doubleValue();
        if (liquidation <= 0 || entry <= 0 || stopPrice <= 0) return;

        Side direction = position.direction().orElse(null);
        if (direction == null) return;

        LiquidationSafety.Buffer buffer = LiquidationSafety.evaluate(direction, entry, stopPrice, liquidation);
        double floor = engine.config().minLiquidationBufferFraction();
        if (!buffer.satisfies(floor)) {
            drifts.add(new Drift(Drift.Kind.LIQUIDATION_BUFFER_BREACHED, symbol, String.format(
                    "the exchange reports liquidation at %.8g; the resting stop at %s now leaves %.1f%% "
                            + "of the entry-to-liquidation distance, below the %.0f%% floor",
                    liquidation, stop.stopPrice().toPlainString(), buffer.fraction() * 100, floor * 100)));
        }
    }

    /**
     * Convenience for the boot path: reconcile and return whether it is safe to start trading.
     * Benign leftovers — a stop that fired while the process was down, orphan legs awaiting their
     * sweep — do not fail a boot; only risk this process cannot vouch for does.
     */
    public boolean bootstrap(Instant now) {
        Report report = reconcile(now);
        if (report.healthy()) {
            LOG.info("[Reconciler] start-up state adopted from the exchange: "
                    + engine.book());
            return !halt.isHalted();
        }
        return false;
    }
}
