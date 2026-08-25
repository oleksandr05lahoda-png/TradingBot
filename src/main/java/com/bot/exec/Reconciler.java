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
            LIQUIDATION_BUFFER_BREACHED
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
                        protectiveStop = confirmStopByName(symbol, local, drifts, stillTriggered);
                    } else {
                        drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol,
                                "an open position has no working reduce-only stop on the exchange"));
                    }
                }
                protectiveStop.ifPresent(stop -> checkLiquidationBuffer(symbol, exchangePositions, stop, drifts));
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

        // From the exchange's ledger, not a local tally: a restart must not clear the day's loss.
        long utcMidnight = LocalDate.ofInstant(now, ZoneOffset.UTC)
                .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        try {
            engine.killSwitch().seedRealizedPnl(port.fetchRealizedPnlSince(utcMidnight), now);
        } catch (RuntimeException e) {
            LOG.warning("[Reconciler] realised PnL could not be re-seeded from the exchange: " + e.getMessage());
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
            // realigned above; the operator hears about it, the machine keeps trading.
            String summary = "the exchange closed or trimmed position(s) while this process watched:"
                    + report.describe();
            LOG.info("[Reconciler] " + summary);
            alerts.warning("Exchange-side exit", summary);
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
                                                    List<Drift> drifts, Set<String> stillTriggered) {
        Optional<String> recorded = local == null ? Optional.empty() : local.protectiveStopId();
        if (recorded.isEmpty()) {
            LOG.warning("[Reconciler] " + symbol + " holds a position, this venue does not list "
                    + "conditional orders and no stop id is on record — cannot confirm its stop is in place");
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
            return Optional.empty();
        }
        if (stop.isPresent() && stop.get().isWorking()) return stop;

        if (stop.isPresent() && stop.get().state() == OrderState.UNKNOWN) {
            // A state this build cannot read is ignorance, not an absent stop — no false emergency.
            LOG.warning("[Reconciler] " + symbol + ": stop " + stopId + " came back in a state this "
                    + "build cannot read — not confirming it either way");
            return Optional.empty();
        }

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
            drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol,
                    "stop " + stopId + " " + what + ", yet the position is still open a pass later"));
            return Optional.empty();
        }

        drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol,
                "the stop on record (" + stopId + ") is "
                        + stop.map(s -> s.state().toString()).orElse("unknown to the exchange")
                        + " while the position is still open"));
        return Optional.empty();
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
