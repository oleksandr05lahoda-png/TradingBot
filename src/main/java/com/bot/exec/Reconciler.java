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
import java.util.Locale;
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
            UNPROTECTED_CLOSED,
            /**
             * A position the book did not know matched an entry this process had written down
             * before sending (crash between fill and stop, or a lost response): booked with its
             * planned risk so the missing-stop repair can place the stop that was meant.
             */
            ADOPTED_BY_INTENT
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

    /**
     * What took a booked position out, as far as the exchange is willing to say. The wording of the
     * exit alert and the {@code cause} field of its journal row both come from here, so that a
     * later analysis can tell a take-profit from a close the owner made in the app without parsing
     * prose.
     */
    public enum ExitCause {
        /** A take-profit leg of ours filled: the position reached its target. */
        TAKE_PROFIT("take-profit"),
        /** The protective stop filled. */
        STOP_LOSS("stop-loss"),
        /** This process closed it - an operator {@code /close}, the kill switch, a stop repair. */
        BOT_CLOSE("bot-close"),
        /** A reduce-only fill that is not ours: the owner closed it in the Binance app. */
        HAND_CLOSE("hand-close"),
        /** Gone while its own stop still rests - a take fill or a hand close, and the venue will not say which. */
        CLOSED_EARLY("closed-early"),
        /** The venue closed it itself: liquidation, auto-deleveraging or a settlement. */
        LIQUIDATION("liquidation"),
        /** Nothing the exchange returned accounts for it. */
        UNEXPLAINED("unexplained"),
        /** The position shrank rather than closed; this pass does not name the leg behind it. */
        PARTIAL_EXIT("partial-exit");

        private final String token;

        ExitCause(String token) { this.token = token; }

        /** Lowercase, stable, machine-readable - the journal's {@code cause} field. */
        public String token() { return token; }

        /**
         * An exit the owner or this machine meant to happen, and the only kind reported calmly.
         * A liquidation is deliberately NOT expected: dressing one up as a normal exit would tell
         * the owner his account is fine on the day it was not.
         */
        public boolean expected() {
            // CLOSED_EARLY is deliberately NOT here. It is reached when the order history did not
            // answer and the stop merely still rests - which is exactly what a liquidation looks
            // like before the venue retires its legs. "Probably fine" is not an explanation, and a
            // calm line about one would be the same lie as a calm line about a liquidation.
            return this == TAKE_PROFIT || this == STOP_LOSS || this == BOT_CLOSE
                    || this == HAND_CLOSE;
        }
    }

    /**
     * Hears an exchange-side exit the pass absorbed, for the trade journal. Never trading logic;
     * {@code price} and {@code quantity} are blank when the order that closed it could not be read.
     */
    @FunctionalInterface
    public interface ExitListener {
        void exit(String symbol, String cause, String detail, String orderId, String price, String quantity);
    }

    /** A named exit plus the order that proves it. */
    private record Exit(ExitCause cause, String detail, String orderId, String price, String quantity) {

        static Exit unexplained() {
            return new Exit(ExitCause.UNEXPLAINED, "it closed without this process noticing", "", "", "");
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
    private volatile ExitListener exchangeExitListener;

    /**
     * When the trade notifier announces every exit in its own words, the calm "Position closed"
     * push says the same thing twice in two languages (23.09). It then goes to the log only.
     * Warnings - an exit nothing explains, a liquidation - are pushed exactly as before.
     */
    private volatile boolean calmExitsAnnouncedElsewhere;

    /**
     * How far back an order is allowed to be and still count as the one that closed a position.
     * A ghost is at most one pass old, so this is slack for a paused host, not a real window; a
     * closing fill older than the position's own entry is rejected outright below.
     */
    static final long EXIT_EVIDENCE_WINDOW_MS = 86_400_000L;
    /** How fresh a closing fill must be to explain a position we never saw opened. */
    static final long ADOPTED_EXIT_RECENCY_MS = 300_000L;

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

    /**
     * When each (kind, symbol) drift was last pushed to the operator. A drift the machine cannot
     * repair — a naked position it may not touch, a book that will not converge — re-raises on
     * every pass, and at a 30s interval that is 120 identical CRITICAL messages an hour. The one
     * that matters is the first; the rest bury it. Mirrors the hold watchdog's hourly reminder.
     */
    private final Map<String, Instant> driftAnnouncedAt = new java.util.concurrent.ConcurrentHashMap<>();
    static final long DRIFT_REANNOUNCE_MS = 3_600_000L;

    /** Consecutive passes the realised-PnL seed failed; at the threshold the operator hears about it. */
    private int pnlSeedFailures = 0;
    private boolean pnlSeedAlerted = false;
    static final int PNL_SEED_FAILURES_BEFORE_ALERT = 10;

    /** Hears a repair close, for the trade journal — the one real-money exit no operator typed. */
    private volatile CloseObserver closeObserver;

    /** Entries the coordinator wrote down before sending; a stopless position that matches one is ours. */
    private volatile EntryIntents intents = EntryIntents.inMemory();

    /**
     * Consecutive passes on which the exchange listed no positions while the book held some. One
     * such read is not believed: a truncated positionRisk answer used to erase the book, the ledger
     * and every stop id in one pass, and the next pass halted on the same positions as unknown.
     */
    private int emptyReadsInARow = 0;
    private int passes = 0;

    /** The account and non-flat positions a pass read and believed. A view for humans, never an input to trading. */
    public record LastRead(Instant at, AccountSnapshot account, List<PositionSnapshot> positions) {
        public LastRead {
            Preconditions.notNull(at, "at");
            Preconditions.notNull(account, "account");
            positions = List.copyOf(Preconditions.notNull(positions, "positions"));
        }
    }

    private volatile LastRead lastRead;

    /** Empty until a pass has read the exchange; the operator channel renders it, nothing trades on it. */
    public Optional<LastRead> lastRead() {
        return Optional.ofNullable(lastRead);
    }
    /** Account-wide orphan sweep cadence in passes (~10 min at 30 s); the plain listing weighs 40. */
    static final int ORPHAN_SWEEP_EVERY = 20;

    /** Persistent intents shared with the coordinator instead of this instance's empty default. */
    public void withEntryIntents(EntryIntents intents) {
        this.intents = Preconditions.notNull(intents, "intents");
    }

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

    /** One call per ghost or shrink the pass absorbed, with the cause it could prove. */
    public void onExchangeExit(ExitListener listener) {
        this.exchangeExitListener = listener;
    }

    /** True when per-trade notifications carry the calm exits; see {@link #calmExitsAnnouncedElsewhere}. */
    public void announceCalmExitsElsewhere(boolean on) {
        this.calmExitsAnnouncedElsewhere = on;
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
        // Order update times are the exchange's clock. The adapter corrects the host's skew for
        // its signatures; the orphan grace compared the raw host clock with them, so a host 60 s
        // behind never swept an orphan and one 60 s ahead swept a just-placed leg (audit 06.09).
        long exchangeNow = now.toEpochMilli() + port.clockSkewMillis();

        AccountSnapshot account = port.fetchAccount();
        List<PositionSnapshot> exchangePositions = port.openPositions().stream()
                .filter(p -> !p.isFlat())
                .toList();
        ExposureBook book = engine.book();
        List<ExposureBook.OpenPosition> before = book.all();
        Map<String, ExposureBook.OpenPosition> localBySymbol = new LinkedHashMap<>();
        for (ExposureBook.OpenPosition p : before) localBySymbol.put(p.symbol(), p);

        // A listing with NO positions against a book that holds some is not believed on sight: the
        // account's own unrealised PnL says whether anything is open, and a truncated read must not
        // erase the book, the ledger and every stop id in one pass (audit 03.09). One pass of grace
        // even when the PnL is silent; a second empty read in a row is accepted as the truth.
        if (exchangePositions.isEmpty() && !before.isEmpty()) {
            boolean pnlSaysOpen = account.totalUnrealizedPnl().signum() != 0;
            emptyReadsInARow++;
            // One position stopping out is the everyday case and is believed at once (when the PnL
            // agrees); three or more vanishing in the same 30 s is far rarer than a truncated read.
            boolean wholeBookGone = before.size() >= 3;
            if (pnlSaysOpen || (wholeBookGone && emptyReadsInARow < 2)) {
                throw ExchangeException.ambiguous("positionRisk listed no positions while the book holds "
                        + before.size() + (pnlSaysOpen ? " and the account reports unrealised PnL "
                        + account.totalUnrealizedPnl().toPlainString() : "")
                        + " — treating this read as unconfirmed, not as an exit (pass "
                        + emptyReadsInARow + ")", null);
            }
        } else {
            emptyReadsInARow = 0;
        }
        // Kept for the operator's /status and /book, which must not call the exchange from the
        // Telegram thread: these two reads already happened, so showing them costs no weight.
        // Only a read this pass BELIEVED is kept - a disbelieved empty listing threw above.
        lastRead = new LastRead(now, account, exchangePositions);

        List<Drift> drifts = new ArrayList<>();
        List<ExposureBook.OpenPosition> truth = new ArrayList<>();
        Set<String> symbolsToInspect = new HashSet<>(localBySymbol.keySet());
        // Symbol -> the exit this pass could prove, for the alert's severity and the journal's cause.
        Map<String, Exit> exits = new LinkedHashMap<>();

        for (PositionSnapshot position : exchangePositions) {
            symbolsToInspect.add(position.symbol());
            Side direction = position.direction().orElseThrow();
            BigDecimal quantity = position.absoluteQuantity();
            double entryPrice = position.entryPrice().doubleValue();
            ExposureBook.OpenPosition local = localBySymbol.get(position.symbol());

            // The exchange stores no intended stop, so the distance comes from the local record; with
            // none, risk is 0 — an understatement never sized against, since UNKNOWN_POSITION halts.
            double stopDistance = local != null && local.quantity().signum() > 0
                    ? local.riskUsd() / local.quantity().doubleValue()
                    : 0.0;
            // The stop id must be carried across passes; the exchange cannot supply it again.
            Optional<String> stopId = local == null ? Optional.empty() : local.protectiveStopId();

            if (local == null) {
                Optional<EntryIntents.Intent> intent = intents.get(position.symbol(), now.toEpochMilli());
                if (intent.isPresent() && intent.get().side() == direction) {
                    // Not foreign: this process meant to open exactly this, and died (or lost the
                    // response) between the fill and the stop. Booked with the planned stop distance,
                    // so the missing-stop repair below can place the stop that was intended instead
                    // of refusing a position with no risk on record.
                    stopDistance = intent.get().stopPrice().subtract(position.entryPrice()).abs().doubleValue();
                    drifts.add(new Drift(Drift.Kind.ADOPTED_BY_INTENT, position.symbol(),
                            "exchange holds " + direction + " " + quantity.toPlainString() + " @ "
                                    + position.entryPrice().toPlainString() + " matching the entry this "
                                    + "process intended (signal " + intent.get().signalId()
                                    + ") — booked with its planned stop at "
                                    + intent.get().stopPrice().toPlainString()));
                } else {
                    drifts.add(new Drift(Drift.Kind.UNKNOWN_POSITION, position.symbol(),
                            "exchange holds " + direction + " " + quantity.toPlainString()
                                    + " @ " + position.entryPrice().toPlainString() + ", the book holds nothing"));
                }
            } else {
                if (local.side() != direction) {
                    drifts.add(new Drift(Drift.Kind.SIDE_MISMATCH, position.symbol(),
                            "book says " + local.side() + ", exchange says " + direction));
                }
                BigDecimal delta = quantity.subtract(local.quantity());
                if (delta.abs().compareTo(quantityTolerance) > 0) {
                    // Direction decides severity: shrinking is a partial exit the closePosition stop
                    // still covers; growing means another actor holds risk under this bot's name.
                    String detail = "book says " + local.quantity().toPlainString()
                            + ", exchange says " + quantity.toPlainString();
                    drifts.add(new Drift(
                            delta.signum() > 0 ? Drift.Kind.POSITION_GREW : Drift.Kind.QUANTITY_MISMATCH,
                            position.symbol(), detail));
                    if (delta.signum() < 0) {
                        // A trim, not an exit: the position is still open and still protected, so no
                        // leg is named for it. It keeps its warning on purpose - a book that shrank
                        // for a reason this pass did not establish is not a calm event.
                        exits.put(position.symbol(),
                                new Exit(ExitCause.PARTIAL_EXIT, detail, "", "", ""));
                    }
                }
            }

            truth.add(new ExposureBook.OpenPosition(position.symbol(), direction, quantity, entryPrice,
                    quantity.doubleValue() * entryPrice, quantity.doubleValue() * stopDistance, stopId));
        }

        Set<String> exchangeSymbols = new HashSet<>();
        for (PositionSnapshot p : exchangePositions) exchangeSymbols.add(p.symbol());
        // Positions this pass watched leave: their protective legs are orphans NOW, not after grace.
        Set<String> justClosed = new HashSet<>();
        // Only those whose exit the exchange actually NAMED. "One positionRisk read did not list
        // this symbol" is not proof a position is gone, and cancelling a protective leg on that
        // alone would strip a live position of its stop on a bad read. An unexplained or merely
        // guessed exit keeps the old behaviour: the 60s grace, then the account-wide sweep.
        Set<String> provenClosed = new HashSet<>();
        for (ExposureBook.OpenPosition local : before) {
            if (!exchangeSymbols.contains(local.symbol())) {
                Exit exit = resolveExit(local, exchangeNow);
                exits.put(local.symbol(), exit);
                justClosed.add(local.symbol());
                if (exit.cause() != ExitCause.UNEXPLAINED && exit.cause() != ExitCause.CLOSED_EARLY) {
                    provenClosed.add(local.symbol());
                }
                drifts.add(new Drift(Drift.Kind.GHOST_POSITION, local.symbol(),
                        "the book holds " + local.side() + " " + local.quantity().toPlainString()
                                + ", the exchange is flat - " + exit.detail()));
                intents.clear(local.symbol());
                // Where the venue lists conditional orders the flat branch below cancels the
                // leftovers on this very pass; demo-fapi lists none, so nothing can SEE the stop
                // and only its recorded name reaches it. Waiting for the account-wide sweep left a
                // live stop on a flat symbol for up to ten minutes. A stop that itself fired needs
                // no cancel, and a foreign one is the owner's to manage.
                // Same proof requirement as the flat branch: on a venue that lists nothing, a
                // cancel by recorded name is fired blind, so it may only follow an exit the
                // exchange named. A guess would cancel the stop of a position still open.
                if (!port.canListConditionalOrders() && exit.cause() != ExitCause.STOP_LOSS
                        && exit.cause() != ExitCause.UNEXPLAINED
                        && exit.cause() != ExitCause.CLOSED_EARLY) {
                    local.protectiveStopId()
                            .filter(ClientOrderIdFactory::isOurs)
                            .ifPresent(id -> placer.cancelQuietly(local.symbol(), id));
                }
            }
        }

        // Exchange wins, always and immediately, before any of the checks below act on the book.
        book.replaceAll(truth);

        // Carried over, or an order inside the grace window at that one moment is never re-inspected.
        symbolsToInspect.addAll(carriedOverSymbols);
        // An entry this process meant to open and could not account for: a limit that rested past
        // its window with a failed cancel, or a send this process died around. Flat and unbooked,
        // the symbol would otherwise never be looked at again, and its GTC entry could fill days
        // later as a foreign position. Inspected until the order is gone or the fill is adopted.
        for (EntryIntents.Intent intent : intents.all()) {
            if (!intent.expired(now.toEpochMilli())) symbolsToInspect.add(intent.symbol());
        }
        Set<String> stillInteresting = new HashSet<>();
        Set<String> stillTriggered = new HashSet<>();

        for (String symbol : symbolsToInspect) {
            boolean hasPosition = exchangeSymbols.contains(symbol);
            List<OrderStatus> working;
            try {
                working = port.openOrders(symbol).stream().filter(OrderStatus::isWorking).toList();
            } catch (RuntimeException e) {
                // The book is already realigned above. A throw here used to abort the pass with the
                // ghost/shrink drifts unreported and their journal rows unwritten, while the loop
                // believed nothing had been touched. One symbol's unreadable orders are one symbol's
                // ignorance: counted as unconfirmable if it holds a position, revisited if it does not.
                LOG.warning("[Reconciler] " + symbol + ": working orders could not be read this pass ("
                        + e.getMessage() + ")");
                if (hasPosition) {
                    countUnconfirmable(symbol, "orders unreadable: " + e.getMessage(), drifts, now);
                } else {
                    stillInteresting.add(symbol);
                }
                continue;
            }

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
                // An entry remainder of OURS resting on a held symbol: a limit that filled in part
                // and whose cancel failed. Left alone it fills later, the position outgrows its
                // stop's sizing and the next pass halts on POSITION_GREW (audit 06.09). Cancelled
                // once out of grace; the intent that kept this symbol in view is spent only when
                // no such order rests any more.
                boolean entryRemainderRests = false;
                for (OrderStatus order : working) {
                    boolean reducing = order.reduceOnly() || order.closePosition();
                    boolean ourEntry = ClientOrderIdFactory.isOurs(order.clientOrderId())
                            && ClientOrderIdFactory.purposeOf(order.clientOrderId())
                                    .filter(p -> p == OrderTypes.OrderPurpose.ENTRY).isPresent();
                    if (reducing || !ourEntry) continue;
                    entryRemainderRests = true;
                    boolean oldEnough = order.updateTimeMs() > 0
                            && exchangeNow - order.updateTimeMs() > orphanGraceMillis;
                    if (!oldEnough) {
                        stillInteresting.add(symbol);
                        continue;
                    }
                    drifts.add(new Drift(Drift.Kind.ORPHAN_ORDER, symbol,
                            order.type() + " " + order.clientOrderId()
                                    + " is an entry remainder resting on a held symbol — cancelling"));
                    placer.cancelQuietly(symbol, order.clientOrderId());
                    stillInteresting.add(symbol);   // the cancel may have raced a fill: one more look
                }
                if (!entryRemainderRests && book.hasPosition(symbol)
                        && intents.get(symbol, now.toEpochMilli()).isPresent()) {
                    LOG.info("[Reconciler] " + symbol + ": booked and held with no entry remainder "
                            + "resting — the recorded entry intent has done its job; clearing it");
                    intents.clear(symbol);
                }
            } else {
                // The same rule as the account-wide sweep: a non-reducing order this machine did not
                // mint is the owner's resting entry, not an orphan. This branch cancelled EVERYTHING
                // on a flat inspected symbol, and the ledger books any stopped hand trade, so the
                // owner's next limit buy on it was swept the pass after his stop fired (audit 06.09).
                List<OrderStatus> ours = new ArrayList<>();
                for (OrderStatus order : working) {
                    boolean reducing = order.reduceOnly() || order.closePosition();
                    if (!reducing && !ClientOrderIdFactory.isOurs(order.clientOrderId())) {
                        LOG.info("[Reconciler] " + symbol + ": foreign " + order.type() + " "
                                + order.clientOrderId() + " on a flat symbol is the owner's — left alone");
                        continue;
                    }
                    ours.add(order);
                }
                for (OrderStatus order : ours) {
                    boolean oldEnough = order.updateTimeMs() > 0
                            && exchangeNow - order.updateTimeMs() > orphanGraceMillis;
                    // The grace exists because a young order may belong to an entry still being
                    // worked. It does not apply to a protective leg of OURS on a position this very
                    // pass watched leave: that one is provably guarding nothing, and leaving it to
                    // the next pass (or the ~10-minute account sweep) is a live stop on a flat
                    // symbol, eating the conditional-order cap and able to open a short of its own.
                    boolean strandedByThisPass = provenClosed.contains(symbol)
                            && (order.reduceOnly() || order.closePosition())
                            && ClientOrderIdFactory.isOurs(order.clientOrderId());
                    if (!oldEnough && !strandedByThisPass) {
                        // May belong to an entry still being worked; let the next pass decide.
                        stillInteresting.add(symbol);
                        continue;
                    }
                    drifts.add(new Drift(Drift.Kind.ORPHAN_ORDER, symbol,
                            order.type() + " " + order.clientOrderId()
                                    + (strandedByThisPass
                                            ? " is a protective leg of the position that just closed - cancelling"
                                            : " is working with no position behind it - cancelling")));
                    placer.cancelQuietly(symbol, order.clientOrderId());
                }
                // The intent is spent only on a pass that read the symbol flat with NOTHING of ours
                // working, and only once the intent itself is out of grace: an order cancelled just
                // above may have filled between the read and the cancel, and a market send whose
                // response was lost may still be settling. One pass of lag, never a naked position.
                Optional<EntryIntents.Intent> intent = intents.get(symbol, now.toEpochMilli());
                boolean intentOutOfGrace = intent.isPresent()
                        && now.toEpochMilli() - intent.get().recordedAtMs() > orphanGraceMillis;
                if (ours.isEmpty() && intentOutOfGrace) {
                    LOG.info("[Reconciler] " + symbol + ": flat with no working orders — the recorded "
                            + "entry intent never became a position; clearing it");
                    intents.clear(symbol);
                }
            }
        }
        carriedOverSymbols = stillInteresting;
        triggeredStops = stillTriggered;
        // Orphans on symbols neither booked nor held are invisible to the per-symbol loop above:
        // a closePosition stop left behind by a close whose cancel failed rests there until it
        // expires, eating the conditional-order cap (~33 measured). Swept account-wide, rarely.
        passes++;
        if (passes % ORPHAN_SWEEP_EVERY == 0) {
            sweepAccountWideOrphans(exchangeSymbols, book, symbolsToInspect, drifts, now);
        }
        // Announcement stamps expire on the clock, not on absence: an intermittent drift that
        // vanished for one pass and came back used to re-announce every ~90 s.
        driftAnnouncedAt.entrySet().removeIf(e ->
                now.toEpochMilli() - e.getValue().toEpochMilli() > DRIFT_REANNOUNCE_MS);
        // A symbol no longer held has nothing to confirm; a stale count must not ambush a re-entry.
        unconfirmablePasses.keySet().retainAll(exchangeSymbols);

        // From the exchange's ledger, not a local tally: a restart must not clear the day's loss.
        long utcMidnight = LocalDate.ofInstant(now, ZoneOffset.UTC)
                .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        try {
            engine.killSwitch().seedRealizedPnl(port.fetchRealizedPnlSince(utcMidnight), now);
            engine.killSwitch().markRealizedStale(false);
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
            // While the realised half is frozen, a stop-out would IMPROVE the measured loss (its open
            // loss vanishes, its realised loss never arrives); the switch ratchets instead.
            engine.killSwitch().markRealizedStale(true);
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
        // Per symbol, so a loser carried in from yesterday is not counted twice - once excluded by
        // the open baseline, then in full through the realised feed the moment it closes.
        Map<String, Double> openPnlBySymbol = new LinkedHashMap<>();
        for (PositionSnapshot p : exchangePositions) {
            openPnlBySymbol.merge(p.symbol(), p.unrealizedPnl().doubleValue(), Double::sum);
        }
        engine.killSwitch().observeOpenUnrealizedPnl(openPnlBySymbol, now);
        engine.killSwitch().observeBalance(Math.max(1e-9, account.equityUsd()), now);

        Report report = new Report(now, drifts, exchangePositions.size(), before.size());

        // Exchange-side exits reach the journal whatever else the pass found: a stop-out on one
        // symbol next to a drift on another used to be lost because only the healthy branch told
        // the listener.
        ExitListener listener = exchangeExitListener;
        if (listener != null) {
            for (Drift d : drifts) {
                if (d.kind() == Drift.Kind.GHOST_POSITION || d.kind() == Drift.Kind.QUANTITY_MISMATCH) {
                    Exit exit = exits.getOrDefault(d.symbol(), Exit.unexplained());
                    try {
                        listener.exit(d.symbol(), exit.cause().token(), d.detail(),
                                exit.orderId(), exit.price(), exit.quantity());
                    } catch (RuntimeException e) {
                        LOG.fine("[Reconciler] exit listener failed: " + e.getMessage());
                    }
                }
            }
        }

        if (!report.healthy()) {
            Drift first = drifts.stream().filter(Drift::isCritical).findFirst().orElseThrow();
            String summary = "reconciliation found " + drifts.size() + " disagreement(s):" + report.describe();
            LOG.severe("[Reconciler] " + summary);
            // The halt latches every pass as before; only the PUSH is throttled per drift, so a
            // condition that cannot clear itself does not bury the next real alert under itself.
            // Keyed on EVERY critical signature, not the first one: a new naked position must
            // not sit silent for an hour because an older, unrelated drift is inside its
            // re-announce window (audit 30.08). Announce when ANY current critical drift is new
            // or stale; stamp them all so the hourly cadence still holds per condition.
            // And ALWAYS when this pass is the one latching the halt: after /resume the same drift
            // used to re-latch inside the throttle window, and the operator who had just cleared
            // it heard nothing for up to an hour (audit 03.09).
            boolean newlyHalting = !halt.isHalted();
            boolean announce = newlyHalting;
            Instant oldestAnnounced = null;
            for (Drift d : drifts) {
                if (!d.isCritical()) continue;
                Instant seen = driftAnnouncedAt.get(d.kind() + "|" + d.symbol());
                if (seen == null || now.toEpochMilli() - seen.toEpochMilli() >= DRIFT_REANNOUNCE_MS) {
                    announce = true;
                }
                if (seen != null && (oldestAnnounced == null || seen.isBefore(oldestAnnounced))) {
                    oldestAnnounced = seen;
                }
            }
            if (announce) {
                for (Drift d : drifts) {
                    if (d.isCritical()) driftAnnouncedAt.put(d.kind() + "|" + d.symbol(), now);
                }
                alerts.critical("Reconciliation drift", summary
                        + (oldestAnnounced == null || newlyHalting ? "" : "\n(unresolved since "
                                + oldestAnnounced + "; repeated hourly)")
                        + (newlyHalting && oldestAnnounced != null
                                ? "\n(the halt was cleared and the same condition is back)" : ""));
            }
            halt.halt("reconciliation drift: " + first.kind() + " on " + first.symbol(), now);
        } else if (!report.converged()) {
            // The exchange finished trades on its own — stops and takes doing their job. The book is
            // realigned above; the operator hears about it, the machine keeps trading. A repaired
            // stop (STOP_REPLACED / UNPROTECTED_CLOSED) already sent its own critical alert and must
            // not be re-announced as an exchange-side exit it is not.
            boolean exchangeSide = drifts.stream().anyMatch(d ->
                    d.kind() == Drift.Kind.GHOST_POSITION || d.kind() == Drift.Kind.QUANTITY_MISMATCH
                            || d.kind() == Drift.Kind.ORPHAN_ORDER);
            if (exchangeSide) {
                // A take-profit that fired, a stop that did its job, a close the owner made in the
                // app: the machine working. Announcing those with a warning sign and the words
                // "without this process noticing" made the first take-profit this account ever
                // scored (VVVUSDT, +$1.31 on 08.09) read like a fault. Only an exit nothing
                // explains still warns - and a liquidation, which ExitCause.expected() excludes on
                // purpose, because a calm line about one would be a lie on the worst day.
                // A cancelled orphan is not an exit at all: it is this pass tidying up after one.
                boolean anyExit = drifts.stream().anyMatch(d ->
                        d.kind() == Drift.Kind.GHOST_POSITION || d.kind() == Drift.Kind.QUANTITY_MISMATCH);
                boolean allExpected = drifts.stream()
                        .filter(d -> d.kind() == Drift.Kind.GHOST_POSITION
                                || d.kind() == Drift.Kind.QUANTITY_MISMATCH)
                        .allMatch(d -> exits.getOrDefault(d.symbol(), Exit.unexplained())
                                .cause().expected());
                // allMatch over an empty stream is vacuously true: on a pass whose only drift is a
                // cancelled ORPHAN there is no ghost to judge, and the 20-pass account-wide sweep -
                // the alarm that once surfaced 64 dead conditional orders against a ~33 cap - went
                // out as a calm "Leftover orders cancelled". A calm line needs a named exit behind it.
                allExpected = allExpected && anyExit;
                String summary = (allExpected
                        ? "position(s) left the book the way they were meant to:"
                        : "the exchange closed or trimmed position(s) while this process watched:")
                        + report.describe();
                LOG.info("[Reconciler] " + summary);
                if (!allExpected) {
                    alerts.warning("Exchange-side exit", summary);
                } else if (!(anyExit && calmExitsAnnouncedElsewhere)) {
                    alerts.info(anyExit ? "Position closed" : "Leftover orders cancelled", summary);
                }
            } else {
                LOG.info("[Reconciler] pass repaired protection without drift to announce:"
                        + report.describe());
            }
        } else {
            LOG.fine("[Reconciler] converged: " + exchangePositions.size() + " position(s)");
        }
        return report;
    }

    /**
     * Cancels protective orders resting on symbols that are flat and unbooked — the leftovers of a
     * close whose cleanup failed, or of a crash after a flatten. Reduce-only and closePosition
     * orders only: a hand-placed entry order on a flat symbol is the owner's business. Best effort;
     * a failed listing costs nothing but the next sweep.
     */
    private void sweepAccountWideOrphans(Set<String> exchangeSymbols, ExposureBook book,
                                         Set<String> alreadyInspected, List<Drift> drifts, Instant now) {
        long exchangeNow = now.toEpochMilli() + port.clockSkewMillis();
        List<OrderStatus> all;
        try {
            all = port.openOrdersAll();
        } catch (RuntimeException e) {
            LOG.fine("[Reconciler] account-wide order listing failed: " + e.getMessage());
            return;
        }
        for (OrderStatus order : all) {
            if (!order.isWorking() || !(order.reduceOnly() || order.closePosition())) continue;
            String symbol = order.symbol();
            if (exchangeSymbols.contains(symbol) || book.hasPosition(symbol) || alreadyInspected.contains(symbol)) {
                continue;
            }
            boolean oldEnough = order.updateTimeMs() > 0
                    && exchangeNow - order.updateTimeMs() > orphanGraceMillis;
            if (!oldEnough) continue;
            drifts.add(new Drift(Drift.Kind.ORPHAN_ORDER, symbol,
                    order.type() + " " + order.clientOrderId()
                            + " rests on a flat, unbooked symbol — cancelling (account-wide sweep)"));
            placer.cancelQuietly(symbol, order.clientOrderId());
        }
    }

    /**
     * Names what took the position out, so the exit reads as a fact instead of a mystery. Asking the
     * stop by name was the whole answer until 08.09 and it is not enough: Binance retires a
     * {@code closePosition} conditional order the moment the position goes, so after a take-profit
     * fills the stop is neither FILLED nor working and every winner was announced as "it closed
     * without this process noticing". The order history knows - a triggered conditional appears
     * there under its own id - so that is asked first, and only on this rare flat path.
     *
     * <p>Best-effort throughout: what the exchange cannot prove stays UNEXPLAINED and keeps its
     * warning. The answer changes the wording and the severity, never the verdict on the book.
     */
    private Exit resolveExit(ExposureBook.OpenPosition local, long exchangeNow) {
        Exit fromHistory = exitFromOrderHistory(local, exchangeNow);
        if (fromHistory != null) return fromHistory;

        Optional<String> stopId = local.protectiveStopId();
        if (stopId.isEmpty()) return Exit.unexplained();
        try {
            Optional<OrderStatus> stop = port.queryOrder(local.symbol(), stopId.get());
            if (stop.isPresent() && stop.get().state() == OrderState.FILLED) {
                return new Exit(ExitCause.STOP_LOSS, "its protective stop " + stopId.get() + " filled",
                        stopId.get(), plain(stop.get().averagePrice()), plain(stop.get().executedQuantity()));
            }
            if (stop.isPresent() && stop.get().isWorking()) {
                // The stop is intact, so it was not the stop: a take-profit fill or a close by hand.
                // Which of the two only the order history can say, and it did not answer here.
                return new Exit(ExitCause.CLOSED_EARLY, "it closed ahead of its still-resting stop "
                        + stopId.get() + " - a take-profit fill or a close made by hand",
                        stopId.get(), "", "");
            }
        } catch (RuntimeException e) {
            LOG.fine("[Reconciler] could not name the exit on " + local.symbol() + ": " + e.getMessage());
        }
        return Exit.unexplained();
    }

    /**
     * The closing fill the venue recorded most recently on this symbol, or null when the venue does
     * not answer, nothing there closes anything, or the newest closing fill is OLDER than our own
     * newest entry fill - which means it belongs to a previous round trip on the same symbol and
     * proves nothing about this one.
     */
    private Exit exitFromOrderHistory(ExposureBook.OpenPosition local, long exchangeNow) {
        List<OrderStatus> history;
        try {
            history = port.recentOrders(local.symbol(), exchangeNow - EXIT_EVIDENCE_WINDOW_MS);
        } catch (RuntimeException e) {
            LOG.fine("[Reconciler] order history for " + local.symbol() + " is unreadable: " + e.getMessage());
            return null;
        }

        // A port that answers null rather than throwing used to take the NPE out of this method and
        // through the whole reconcile pass - the one pass that has to survive a bad answer, and one
        // that counts toward the blind-pass limit.
        if (history == null) return null;
        OrderStatus closer = null;
        ExitCause cause = null;
        long ourNewestEntryFill = Long.MIN_VALUE;
        for (OrderStatus order : history) {
            if (order == null) continue;
            if (!order.hasFill() && order.state() != OrderState.FILLED) continue;
            ExitCause candidate = causeOf(order);
            if (candidate == null) {
                if (isOurEntry(order.clientOrderId())) {
                    ourNewestEntryFill = Math.max(ourNewestEntryFill, order.updateTimeMs());
                }
                continue;
            }
            if (closer == null || order.updateTimeMs() >= closer.updateTimeMs()) {
                closer = order;
                cause = candidate;
            }
        }
        if (closer == null || closer.updateTimeMs() < ourNewestEntryFill) return null;
        // A position adopted from the exchange has no entry of ours to date the round trip from, so
        // the 24h evidence window would accept a fill from yesterday as the cause of a close that
        // happened seconds ago - and write yesterday's price into the journal. Without that anchor
        // the closing fill has to be as young as a ghost can actually be: the loop runs every ~30s.
        if (ourNewestEntryFill == Long.MIN_VALUE
                && exchangeNow - closer.updateTimeMs() > ADOPTED_EXIT_RECENCY_MS) {
            return null;
        }

        String id = closer.clientOrderId();
        String price = plain(closer.averagePrice());
        String at = price.isEmpty() ? "" : " at " + price;
        String detail = switch (cause) {
            case TAKE_PROFIT -> "its take-profit " + id + " filled" + at;
            case STOP_LOSS -> "its protective stop " + id + " filled" + at;
            case BOT_CLOSE -> "this process closed it (" + id + ")" + at;
            case HAND_CLOSE -> "it was closed by hand on the exchange (" + id + ")" + at;
            case LIQUIDATION -> "the exchange closed it itself (" + id
                    + ") - a liquidation, auto-deleverage or settlement" + at;
            default -> "it closed without this process noticing";
        };
        return new Exit(cause, detail, id, price, plain(closer.executedQuantity()));
    }

    /**
     * Which exit a filled order is, from its client id alone - the ids are deterministic and carry
     * their purpose. Null when the order closed nothing: our own entry, or a foreign order that
     * adds rather than reduces.
     */
    private static ExitCause causeOf(OrderStatus order) {
        String id = order.clientOrderId();
        if (isVenueAutoClose(id)) return ExitCause.LIQUIDATION;
        if (ClientOrderIdFactory.isOurs(id)) {
            Optional<OrderTypes.OrderPurpose> purpose = ClientOrderIdFactory.purposeOf(id);
            if (purpose.isEmpty()) return null;
            return switch (purpose.get()) {
                case TAKE_PROFIT -> ExitCause.TAKE_PROFIT;
                case STOP_LOSS -> ExitCause.STOP_LOSS;
                case EMERGENCY_CLOSE -> ExitCause.BOT_CLOSE;
                case ENTRY -> null;
            };
        }
        // Not ours, and it reduces. "By hand" is claimed only for a plain reduce - the Close
        // button in the app. A foreign STOP_MARKET or TAKE_PROFIT_MARKET filling is the owner's
        // own conditional order firing, which is not a decision he made just now and may well be a
        // loss he wants to see; it is named for what it is rather than dressed as a hand close.
        if (!(order.reduceOnly() || order.closePosition())) return null;
        OrderTypes.OrderType type = order.type();
        if (type == OrderTypes.OrderType.STOP_MARKET) return ExitCause.STOP_LOSS;
        if (type == OrderTypes.OrderType.TAKE_PROFIT_MARKET) return ExitCause.TAKE_PROFIT;
        return ExitCause.HAND_CLOSE;
    }

    private static boolean isOurEntry(String clientOrderId) {
        return ClientOrderIdFactory.isOurs(clientOrderId)
                && ClientOrderIdFactory.purposeOf(clientOrderId)
                        .filter(p -> p == OrderTypes.OrderPurpose.ENTRY).isPresent();
    }

    /**
     * Binance mints the ids of the orders it places for itself: {@code autoclose-} for a
     * liquidation, {@code adl_autoclose} for auto-deleveraging, {@code settlement_autoclose} for a
     * delisting. None of these may ever be reported as a normal exit.
     */
    private static boolean isVenueAutoClose(String id) {
        // Substring and case-insensitive on purpose: an id one character off the exact prefix
        // ("autoclose_" instead of "autoclose-") used to fall through and be announced calmly as a
        // close made by hand. A venue-minted close must never be able to read as a normal exit
        // because its id was punctuated differently.
        return id != null && id.toLowerCase(Locale.ROOT).contains("autoclose");
    }

    /** Blank rather than "0" for a figure the venue did not fill in, so the journal row stays honest. */
    private static String plain(BigDecimal value) {
        return value == null || value.signum() <= 0 ? "" : value.toPlainString();
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
        if (stop.isEmpty() && !ClientOrderIdFactory.isOurs(stopId)) {
            // A foreign stop id (adopted from the exchange) that no endpoint answers for: this
            // process cannot repair such a position, so "absent" here is a CRITICAL halt. One
            // read of nothing is not proof - the stop may have fired between the position read
            // and this lookup - so it is ignorance first, and missing only at the limit.
            LOG.warning("[Reconciler] " + symbol + ": foreign stop " + stopId
                    + " is unknown to the exchange this pass — not confirming it either way");
            countUnconfirmable(symbol, "foreign stop " + stopId + " unknown to the exchange", drifts, now);
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
        // Ours = sized here and, when a stop id is on record, minted here. A hand trade whose app
        // stop was adopted carries a risk figure too, and must not be repaired or closed by this
        // process; a position adopted from an entry intent carries risk and no id yet, and is ours.
        boolean ours = local != null && local.riskUsd() > 0
                && local.protectiveStopId().map(ClientOrderIdFactory::isOurs).orElse(true);
        boolean repairable = close != null && ours && local.quantity().signum() > 0;
        if (!repairable) {
            drifts.add(new Drift(Drift.Kind.POSITION_WITHOUT_STOP, symbol, detail
                    + (local != null && local.riskUsd() > 0 && !ours
                            ? " (adopted from the exchange with a foreign stop id — not this process's to repair)"
                            : "")));
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
                    intents.clear(symbol);
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
