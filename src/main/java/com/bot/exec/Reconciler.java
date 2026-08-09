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
 * Brings local belief back in line with the exchange, and stops trading when the two disagree: the
 * book is realigned <i>and</i> {@link TradingHalt} is tripped, since drift is a defect rather than a
 * number to correct and forget. {@link Drift.Kind#POSITION_WITHOUT_STOP} is what a crash between
 * "entry filled" and "stop placed" leaves behind.
 */
public final class Reconciler {

    private static final Logger LOG = Logger.getLogger(Reconciler.class.getName());

    public record Drift(Kind kind, String symbol, String detail) {

        public enum Kind {
            /** The exchange holds a position the book does not know about. */
            UNKNOWN_POSITION,
            /** The book holds a position the exchange says is flat. */
            GHOST_POSITION,
            /** Same symbol, different size. */
            QUANTITY_MISMATCH,
            /** Same symbol, opposite direction. */
            SIDE_MISMATCH,
            /** An open position with no working stop on the exchange. */
            POSITION_WITHOUT_STOP,
            /** A working order on a symbol with no position, old enough not to be a race. */
            ORPHAN_ORDER,
            /**
             * The resting stop no longer sits far enough inside the liquidation price the exchange
             * itself reports.
             */
            LIQUIDATION_BUFFER_BREACHED
        }

        public boolean isCritical() {
            return kind == Kind.POSITION_WITHOUT_STOP || kind == Kind.SIDE_MISMATCH
                    || kind == Kind.UNKNOWN_POSITION || kind == Kind.LIQUIDATION_BUFFER_BREACHED;
        }
    }

    public record Report(Instant at, List<Drift> drifts, int exchangePositions, int bookPositionsBefore) {

        public Report {
            drifts = List.copyOf(drifts);
        }

        public boolean converged() { return drifts.isEmpty(); }

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

    /** Symbols whose working orders were still inside the grace window on the previous pass. */
    private volatile Set<String> carriedOverSymbols = Set.of();

    /** Symbols whose stop already read "triggered" on the previous pass, with the position still open. */
    private volatile Set<String> triggeredStops = Set.of();

    public Reconciler(ExchangePort port, RiskEngine engine, TradingHalt halt, AlertSink alerts,
                      IdempotentOrderPlacer placer) {
        this(port, engine, halt, alerts, placer, new BigDecimal("0.0000001"), 60_000L);
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

    /**
     * One reconciliation pass: reads the exchange, realigns local state and cancels provably
     * unmanaged orders. Idempotent, and never opens anything.
     *
     * @param now used for the UTC-day boundary of the realised-PnL reseed and the orphan grace window
     */
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
                if (local.quantity().subtract(quantity).abs().compareTo(quantityTolerance) > 0) {
                    drifts.add(new Drift(Drift.Kind.QUANTITY_MISMATCH, position.symbol(),
                            "book says " + local.quantity().toPlainString()
                                    + ", exchange says " + quantity.toPlainString()));
                }
            }

            // The exchange does not store an intended stop, so the distance comes from the local
            // record; with none, risk is 0 — a known understatement never sized against, because
            // the UNKNOWN_POSITION drift above halts trading.
            double stopDistance = local != null && local.quantity().signum() > 0
                    ? local.riskUsd() / local.quantity().doubleValue()
                    : 0.0;
            // The stop id is carried across passes: the exchange cannot supply it, so losing it here
            // would mean the named check below works once and never again.
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
                                + ", the exchange is flat — it closed without this process noticing"));
            }
        }

        // Exchange wins, always and immediately, before any of the checks below act on the book.
        book.replaceAll(truth);

        // Carried over, otherwise a closed symbol is inspected once and an order still inside the
        // grace window at that single moment is never looked at again.
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
                if (protectiveStop.isEmpty() && !port.canListConditionalOrders()) {
                    protectiveStop = confirmStopByName(symbol, localBySymbol.get(symbol), drifts, stillTriggered);
                } else if (protectiveStop.isEmpty()) {
                    drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol,
                            "an open position has no working reduce-only stop on the exchange"));
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
        if (!report.converged()) {
            String summary = "reconciliation found " + drifts.size() + " disagreement(s):" + report.describe();
            LOG.severe("[Reconciler] " + summary);
            alerts.critical("Reconciliation drift", summary);
            halt.halt("reconciliation drift: "
                    + drifts.get(0).kind() + " on " + drifts.get(0).symbol(), now);
        } else {
            LOG.fine("[Reconciler] converged: " + exchangePositions.size() + " position(s)");
        }
        return report;
    }

    /**
     * Confirms the protective stop by name, for a venue that accepts conditional orders and answers a
     * query for one but has no endpoint that enumerates them — {@code demo-fapi} is exactly that. The
     * id comes from the book, where it was recorded the moment the stop was placed.
     *
     * <p>No id on record, or a lookup that fails, leaves the question open rather than declaring the
     * position naked: an alert that fires on healthy positions is one the operator learns to ignore.
     *
     * @return the stop when it is confirmed working; the states that are not add their own drift here
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
            // A vocabulary this build does not recognise is ignorance, not an absent stop. Reporting
            // it as a naked position would turn a payload change into a false emergency.
            LOG.warning("[Reconciler] " + symbol + ": stop " + stopId + " came back in a state this "
                    + "build cannot read — not confirming it either way");
            return Optional.empty();
        }

        if (stop.isPresent() && stop.get().state() == OrderState.FILLED) {
            // A stop that fired takes the position with it within seconds, so one pass of grace. Not
            // two: a trigger whose market order never landed looks exactly like this from here, and
            // that is a naked position wearing the costume of a normal exit.
            if (!triggeredStops.contains(symbol)) {
                stillTriggered.add(symbol);
                LOG.warning("[Reconciler] " + symbol + ": stop " + stopId
                        + " has triggered; expecting the position to be gone by the next pass");
                return Optional.empty();
            }
            drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol,
                    "stop " + stopId + " triggered, yet the position is still open a pass later"));
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
     * reports: that number moves after approval — manual margin changes, funding, a
     * maintenance-bracket change — for reasons this system does not model, so a stop can drift
     * outside liquidation with no order changing. A reported liquidation price of zero means "not
     * reachable" and is skipped.
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

    /** Convenience for the boot path: reconcile and return whether it is safe to start trading. */
    public boolean bootstrap(Instant now) {
        Report report = reconcile(now);
        if (report.converged()) {
            LOG.info("[Reconciler] start-up state adopted from the exchange: "
                    + engine.book());
            return !halt.isHalted();
        }
        return false;
    }

    /** Risk currently open according to the book, for status lines. */
    public Optional<Double> openRiskUsd() {
        double risk = engine.book().totalRiskUsd();
        return risk > 0 ? Optional.of(risk) : Optional.empty();
    }
}
