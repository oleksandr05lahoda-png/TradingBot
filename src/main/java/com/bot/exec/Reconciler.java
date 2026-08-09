package com.bot.exec;

import com.bot.core.Preconditions;
import com.bot.core.Side;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.OrderStatus;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.exec.OrderTypes.OrderType;
import com.bot.risk.ExposureBook;
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
 * Brings local belief back in line with the exchange, and stops trading when the two disagree.
 *
 * <p>Local state drifts for reasons that have nothing to do with bugs: a fill delivered while the
 * process was restarting, a position closed by hand from the same account, a stop that triggered
 * during a network partition. So the question this class answers is never "is the local state
 * right?" — it is "what does the exchange say?", and whatever it says wins.
 *
 * <p>Drift is treated as a defect, not as a number to correct and forget. The book is realigned
 * <i>and</i> {@link TradingHalt} is tripped, because a disagreement means some assumption in this
 * system was wrong, and continuing to open positions on top of a wrong assumption is how a small
 * bug becomes an expensive one. Closing stays available — a halt never seals a position in.
 *
 * <p>The check with the sharpest teeth is {@link Drift.Kind#POSITION_WITHOUT_STOP}: an open position
 * with no working protective order on the exchange. That is the state this whole system exists to
 * prevent, and it is the one that a crash between "entry filled" and "stop placed" would produce.
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
            /** Same symbol, opposite direction. The worst kind of quiet. */
            SIDE_MISMATCH,
            /** An open position with no working stop on the exchange. */
            POSITION_WITHOUT_STOP,
            /** A working order on a symbol with no position, old enough not to be a race. */
            ORPHAN_ORDER
        }

        public boolean isCritical() {
            return kind == Kind.POSITION_WITHOUT_STOP || kind == Kind.SIDE_MISMATCH
                    || kind == Kind.UNKNOWN_POSITION;
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
     * One reconciliation pass. Idempotent by construction: it reads the exchange, realigns local
     * state and cancels orders that are provably unmanaged. It never opens anything.
     *
     * @param now used for the UTC-day boundary of the realised-PnL reseed and for the orphan grace window
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

            // The exchange's own numbers rebuild the entry. The stop distance is taken from whatever
            // the local record believed, because the exchange does not store an intended stop — and
            // when there is no local record, the risk is recorded as unknown rather than guessed.
            double stopDistance = local != null && local.quantity().signum() > 0
                    ? local.riskUsd() / local.quantity().doubleValue()
                    : 0.0;
            truth.add(new ExposureBook.OpenPosition(position.symbol(), direction, quantity, entryPrice,
                    quantity.doubleValue() * entryPrice, quantity.doubleValue() * stopDistance));
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

        for (String symbol : symbolsToInspect) {
            List<OrderStatus> working = port.openOrders(symbol).stream().filter(OrderStatus::isWorking).toList();
            boolean hasPosition = exchangeSymbols.contains(symbol);

            if (hasPosition) {
                boolean protectedByStop = working.stream()
                        .anyMatch(o -> o.type() == OrderType.STOP_MARKET && (o.reduceOnly() || o.closePosition()));
                if (!protectedByStop) {
                    drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol,
                            "an open position has no working reduce-only stop on the exchange"));
                }
            } else {
                for (OrderStatus order : working) {
                    boolean oldEnough = order.updateTimeMs() > 0
                            && now.toEpochMilli() - order.updateTimeMs() > orphanGraceMillis;
                    if (!oldEnough) continue;   // may belong to an entry that is still being worked
                    drifts.add(new Drift(Drift.Kind.ORPHAN_ORDER, symbol,
                            order.type() + " " + order.clientOrderId()
                                    + " is working with no position behind it — cancelling"));
                    placer.cancelQuietly(symbol, order.clientOrderId());
                }
            }
        }

        // Realised PnL for the daily limit comes from the exchange's ledger, not from a local tally:
        // a restart must not be able to clear the day's loss, and only the exchange knows what fees
        // and funding did to it.
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
