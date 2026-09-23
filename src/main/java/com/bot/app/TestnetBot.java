package com.bot.app;

import com.bot.core.InstrumentFilters;
import com.bot.exec.AlertSink;
import com.bot.exec.DeadMansSwitch;
import com.bot.exec.EntryIntents;
import com.bot.exec.ExchangePort;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExchangeSnapshots.PositionSnapshot;
import com.bot.exec.ExecutionCoordinator;
import com.bot.exec.IdempotentOrderPlacer;
import com.bot.exec.KillSwitchEnforcer;
import com.bot.exec.Reconciler;
import com.bot.exec.OrderTypes;
import com.bot.exec.TradingHalt;
import com.bot.exec.binance.BinanceFuturesAdapter;
import com.bot.exec.binance.BinanceVenue;
import com.bot.risk.DailyLossKillSwitch;
import com.bot.risk.ExposureBook;
import com.bot.risk.MarginTierTable;
import com.bot.risk.PositionSizer;
import com.bot.risk.RiskConfig;
import com.bot.risk.RiskConstants;
import com.bot.risk.RiskDecision;
import com.bot.risk.RiskEngine;
import com.bot.risk.TakeProfitPolicy;
import com.bot.risk.TradePlan;
import com.bot.risk.TradeRequest;
import com.bot.signal.CloseRequest;
import com.bot.signal.ExecutionFeedback;
import com.bot.signal.ManualInput;
import com.bot.signal.Signal;
import com.bot.signal.SignalSource;
import com.bot.signal.SupabaseQueueSource;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;

/** Entry point: assembly and the main loop only — no risk arithmetic, no exchange knowledge. */
public final class TestnetBot {

    private static final Logger LOG = Logger.getLogger(TestnetBot.class.getName());

    private static final long RECONCILE_INTERVAL_MS = 30_000L;
    /** Consecutive failed reconcile passes before the operator hears about it (~90s). */
    private static final int RECONCILE_WARN_AFTER = 3;
    /** ...and before entries are refused until a pass succeeds (~5 min). Closes keep working. */
    private static final int RECONCILE_BLIND_AFTER = 10;
    private static final long HEARTBEAT_INTERVAL_MS = 30_000L;
    /**
     * 300s of silence before entries pause, matching {@link #RECONCILE_BLIND_AFTER}'s ~5 min: the
     * old 90s latched a permanent halt on a two-minute egress blip (28.08 audit). The exchange-side
     * countdown must outlast the silence tolerance, so it moves with it.
     */
    private static final long DEAD_MANS_COUNTDOWN_MS = 600_000L;
    private static final long DEAD_MANS_MAX_SILENCE_MS = 300_000L;
    /** A failed close retries with backoff this many times before the operator is told to act. */
    private static final int CLOSE_MAX_ATTEMPTS = 8;
    private static final long CLOSE_RETRY_BASE_MS = 5_000L;
    private static final long CLOSE_RETRY_CAP_MS = 300_000L;
    /** Repeat the exchange-hold alert this often while the hold lasts; one alert was silence for 17h. */
    private static final long HOLD_REMINDER_INTERVAL_MS = 3_600_000L;
    /** A plain "alive" line this often: the only periodic INFO left once the PnL seed line went quiet. */
    private static final long ALIVE_LOG_INTERVAL_MS = 300_000L;
    /** A daily-loss breach must persist this long before the book is flattened: one pass, not one wick. */
    private static final long KILL_SWITCH_CONFIRM_MS = 20_000L;
    /** How long a shutdown waits for an entry caught between fill and stop; docker stop grants 45 s. */
    private static final long SHUTDOWN_GRACE_MS = 40_000L;
    /** The one halt reason the loop may lift by itself, once a converged pass contradicts it. */
    private static final String BOOT_READ_HALT_PREFIX = "boot could not read the exchange: ";
    /** The observe latch's reason; the scanner recognises the prefix and stands down on closes too. */
    static final String OBSERVE_HALT_REASON = OperatorChannel.OBSERVE_REASON_PREFIX
            + " - reading the account, accepting closes, opening nothing. Set REAL_MODE=trade "
            + "and restart to enable entries.";

    /** A close that did not confirm flat, waiting for its next attempt. */
    private record PendingClose(CloseRequest close, int attempts, long notBeforeMs) {}

    private TestnetBot() {}

    public static void main(String[] args) throws Exception {
        if (List.of(args).contains("--help")) {
            System.out.println(usage());
            return;
        }
        Instant processStartedAt = Instant.now();
        String buildStamp = buildStamp(Path.of("/app/BUILD_STAMP"), System.getenv("BUILD_STAMP"));

        int defaultLeverage = intProperty("DEFAULT_LEVERAGE", 3);
        String sourceName = argValue(args, "--source", "manual");
        String scriptPath = argValue(args, "--script", null);

        // Venue first so every alert names it: demo and real share one Telegram chat, and a
        // mis-set arming flag must be refused before anything else runs.
        BinanceVenue venue;
        try {
            venue = BinanceVenue.resolveFromEnvironment();
        } catch (IllegalStateException e) {
            // An operator mistake: print the fix, not a stack trace.
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }

        AlertSink alerts = AlertSink.fromEnvironment(
                venue.isReal() ? "REAL " + venue.realMode() : "DEMO");
        RiskConfig config = RiskConfig.defaults();
        // Book width is operational, not a risk limit: the per-position ceilings and the daily kill
        // switch are what bound a correlated book. Tight default unless the operator says otherwise.
        int maxPositions = intProperty("MAX_POSITIONS", 0);
        if (maxPositions > 0) config = config.withMaxConcurrentPositions(maxPositions);
        // RISK_PER_TRADE as a fraction (0.01 = 1%). The scanner already read this for its
        // feasibility check while the bot ignored it (audit 03.09, finding 49) — the two must agree.
        // Clamped to the 1% hard cap with a loud line rather than refusing to boot over a typo.
        double riskPerTrade = doubleProperty("RISK_PER_TRADE", 0.0);
        if (riskPerTrade > 0) {
            if (riskPerTrade > RiskConstants.MAX_RISK_FRACTION_PER_TRADE) {
                LOG.warning("[Boot] RISK_PER_TRADE=" + riskPerTrade + " exceeds the hard cap "
                        + RiskConstants.MAX_RISK_FRACTION_PER_TRADE + " - using the cap");
                riskPerTrade = RiskConstants.MAX_RISK_FRACTION_PER_TRADE;
            }
            config = config.withRiskFractionPerTrade(riskPerTrade);
        }
        // One take frees a conditional-order slot. Binance caps conditional orders per account (cap
        // arrived near 33, measured live 14.08): stop+2 takes protects ~10 positions, stop+1 ~15.
        double tpR = doubleProperty("TP_R_MULTIPLE", 0.0);
        if (tpR > 0) config = config.withTakeProfitPolicy(TakeProfitPolicy.single(tpR));
        // The ledger's directory is where every persisted safety record lives: the stop-id ledger,
        // the entry intents and the kill-switch latch. One path, one volume.
        Path ledgerPath = Path.of(System.getenv().getOrDefault("BOOK_LEDGER_PATH", "book-ledger.json"));
        Path dataDir = ledgerPath.toAbsolutePath().getParent();
        DailyLossKillSwitch killSwitch = new DailyLossKillSwitch(config.dailyLossFractionLimit())
                .withConfirmationWindowMs(KILL_SWITCH_CONFIRM_MS)
                .withLatchFile(dataDir.resolve("killswitch-latch.json"), Instant.now());
        RiskEngine engine = new RiskEngine(config, new ExposureBook(), killSwitch);
        // LOT_ROUND_UP=on: one lot step up when the floored size is refused by the $5 minimum, inside
        // a 20% risk tolerance (the fill tolerance the coordinator already accepts). Owner's switch:
        // at $137 it turns "too wide to size" refusals into entries at up to 0.6% risk instead of 0.5%.
        double lotRoundUp = lotRoundUpTolerance();
        if (lotRoundUp > 0) engine.withLotRoundUpTolerance(lotRoundUp);
        TradingHalt halt = new TradingHalt();

        ExchangePort port;
        try {
            port = BinanceFuturesAdapter.fromEnvironment(venue);
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }

        // A long hold is an incident the operator must hear about: positions stay under their
        // exchange-side stops, but the bot is blind and opens nothing until it lifts. Railway's
        // egress IPs are shared, so another tenant's traffic can earn this bot the ban (22.08).
        port.onExchangeHold(ms -> alerts.critical("Exchange is refusing this IP",
                "Binance answered 429/418 - all requests held for " + (ms / 60_000) + " min. "
                        + "Open positions stay protected by their resting stops; nothing will be "
                        + "opened or closed by the bot until the hold ends, then it resumes by itself."));
        long heldAtBoot = port.heldByExchangeForMillis();
        if (heldAtBoot > 0) {
            alerts.critical("Exchange is refusing this IP",
                    "the first request after boot was refused - held for " + (heldAtBoot / 60_000)
                            + " min. The bot waits it out and then boots normally.");
        }

        // The operator's hands: /status, /halt, /resume over the alert chat. Null when Telegram
        // is not configured, and nothing here can open a position.
        OperatorChannel operator = OperatorChannel.fromEnvironmentOrNull(halt);
        if (operator != null) {
            operator.withVenueTag(venue.isReal() ? "REAL " + venue.realMode() : "DEMO").withAlerts(alerts)
                    .withExchangeHold(port::heldByExchangeForMillis);
        }
        // Observe is enforced here, on every signal, independent of any latch: the latch below
        // exists so the scanner stands down, but a latch can be cleared and a mode cannot.
        final boolean observeOnly = venue.isReal() && venue.realMode() == BinanceVenue.RealMode.OBSERVE;
        if (operator != null) operator.withObserveMode(observeOnly);

        // The raw material for honest learning: one JSONL row per event, on the volume.
        TradeJournal journal = TradeJournal.fromEnvironmentOrNull();

        // One Telegram line per entry and exit, beside the journal row and never instead of it.
        // Sent from its own thread: the loop only drops the line into a bounded queue.
        final TradeNotifier notes = operator == null ? null : tradeNotifier(operator);
        if (operator != null) operator.withScannerView(dataDir.resolve(ScannerView.FILE_NAME));

        // `closedOnExit` only releases the port on the way out; components are wired to `port`.
        try (ExchangePort closedOnExit = port;
             SignalSource signals = openSource(sourceName, scriptPath, defaultLeverage);
             AutoCloseable operatorChannel = operator == null ? () -> { } : operator) {
            if (operator != null) operator.start();

            IdempotentOrderPlacer placer = new IdempotentOrderPlacer(port);
            // The plan is on disk before the entry goes out: a crash between fill and stop, or an
            // entry whose response was lost, no longer leaves a position the repair refuses to touch.
            EntryIntents intents = EntryIntents.at(dataDir.resolve("entry-intents.json"), System.currentTimeMillis());
            ExecutionCoordinator coordinator = new ExecutionCoordinator(port, engine, placer, halt, alerts,
                    entrySettings(), Clock.systemUTC(), Thread::sleep).withEntryIntents(intents);
            Reconciler reconciler = new Reconciler(port, engine, halt, alerts, placer);
            reconciler.withEntryIntents(intents);
            // A booked position found without a stop is repaired — stop re-placed from the book's
            // record, or closed reduce-only — instead of only halted on (28.08 audit, finding #1).
            reconciler.withStopRepair(coordinator::closeOut);
            if (journal != null || notes != null) {
                // Journal first: the record is the thing that must not be lost; the line is a courtesy.
                reconciler.onExchangeExit((symbol, cause, detail, orderId, price, quantity) -> {
                    if (journal != null) journal.exchangeExit(symbol, cause, detail, orderId, price, quantity);
                    notifyExchangeExit(notes, symbol, cause, price, quantity);
                });
                reconciler.onRepairClose((requestId, symbol, closeReport) -> {
                    if (journal != null) {
                        journal.closed(requestId, symbol, "stop-repair",
                                closeReport.closedQuantity().toPlainString(),
                                closeReport.averagePrice().toPlainString(), closeReport.note());
                    }
                    notifyClose(notes, symbol, "stop-repair", closeReport);
                });
            }
            // The calm English "Position closed" push would repeat the line the notifier just sent.
            if (notes != null) reconciler.announceCalmExitsElsewhere(true);
            DeadMansSwitch deadMansSwitch = new DeadMansSwitch(port, engine, alerts,
                    DEAD_MANS_COUNTDOWN_MS, DEAD_MANS_MAX_SILENCE_MS);
            // The daily loss limit closes the book on the trip (KILL_SWITCH_ACTION=halt-only opts
            // out): -3% must mean -3%, not "-3% plus whatever the open losers still give back".
            TradeJournal journalRef = journal;
            KillSwitchEnforcer killSwitchEnforcer = new KillSwitchEnforcer(engine,
                    coordinator::closeOut, alerts, killSwitchAction(),
                    journalRef == null && notes == null ? null : (requestId, symbol, closeReport) -> {
                        if (journalRef != null) {
                            journalRef.closed(requestId, symbol, "daily-loss-kill-switch",
                                    closeReport.closedQuantity().toPlainString(),
                                    closeReport.averagePrice().toPlainString(), closeReport.note());
                        }
                        notifyClose(notes, symbol, "daily-loss-kill-switch", closeReport);
                    });

            banner(venue, port, signals, config, defaultLeverage);

            // Re-arm the book from the last snapshot BEFORE reconciling: the venue cannot name
            // resting stops, so otherwise a restart with open positions halts and forces a flatten.
            String[] lastLedgerBody = {""};

            // A start-up disagreement stops OPENING only — exiting would strand a position with no
            // way to unwind it through the bot. Covers the first exchange read too: dying there was
            // measured with an invalid key on 20.08.
            boolean bootstrapped;
            int bootAttempts = 0;
            // The journal appends unconditionally: a retry of this loop after a failure further
            // down must not write the same exchange-exit rows a second time (audit 06.09).
            boolean exitsJournaled = false;
            while (true) {
                try {
                    // One 5xx or timeout here used to leave the book empty for the life of the process
                    // and every later pass flagging UNKNOWN_POSITION. Three tries, then the loop retries
                    // adoption itself after its first successful reconcile.
                    List<PositionSnapshot> live = readPositionsWithRetry(port, 3);
                    if (!exitsJournaled) {
                        journalExitsWhileDown(journal, notes, port, live, ledgerPath);
                        exitsJournaled = true;
                    }
                    int seeded = BookLedger.seed(engine.book(), live, ledgerPath);
                    if (seeded > 0) {
                        LOG.info("[Boot] re-armed " + seeded + " position(s) with recorded stop ids from " + ledgerPath);
                    }
                    // The file is not the only record on a venue that lists conditional orders: without
                    // this a fresh container halted on positions whose stops the exchange could name.
                    int adopted = BookLedger.adopt(engine.book(), port, live);
                    if (adopted > 0) {
                        LOG.info("[Boot] adopted " + adopted + " position(s) with stops read from the exchange");
                    }
                    bootstrapped = reconciler.bootstrap(Instant.now());
                } catch (RuntimeException e) {
                    // An exchange-side hold (a 429/418 IP ban) is a wait, not a disagreement. On
                    // 27.08 the overnight ban expired at 16:51, the very first request earned a
                    // fresh 1028-minute one, and this catch latched a halt only a restart could
                    // clear - a day of dead bot over an IP that was never ours to fix. Sleep the
                    // hold out and read the account again; the halt latch is for books that do
                    // not match, never for a venue that is not answering.
                    long heldMs = port.heldByExchangeForMillis();
                    if (heldMs > 0) {
                        LOG.warning("[Boot] exchange hold in effect (" + (heldMs / 60_000)
                                + " min left) - waiting it out, then reading the account again");
                        Thread.sleep(heldMs + 15_000L);
                        continue;
                    }
                    // fetchAccount and the per-symbol order reads inside bootstrap had no retry at
                    // all: one 5xx latched this halt for the life of the process (audit 03.09).
                    if (++bootAttempts < 3) {
                        LOG.warning("[Boot] start-up read failed (" + bootAttempts + "/3): " + e.getMessage()
                                + " — retrying in 5 s");
                        Thread.sleep(5_000L);
                        continue;
                    }
                    bootstrapped = false;
                    halt.halt(BOOT_READ_HALT_PREFIX + e.getMessage(), Instant.now());
                }
                break;
            }
            if (!bootstrapped) {
                LOG.severe("[Boot] start-up reconciliation did not converge — trading is halted. "
                        + "Closes are still accepted; inspect the account, then restart to resume.");
                alerts.critical("Start-up reconciliation failed",
                        "no new positions will be opened; the bot stays up so positions can still be closed");
            }
            // Readiness is not health. The scanner waits for this line before it writes anything,
            // and it used to wait for one only a HEALTHY bootstrap logged - so a boot that halted
            // on drift left the scanner permanently not-ready and its EXITS never ran, on exactly
            // the book that had just gone wrong (29.08 audit). Logged on both paths, once.
            LOG.info("[Boot] account read complete — the loop is accepting closes");

            // Observe is a pre-latched halt, not a separate mechanism: same entry gate, closes and
            // reconciliation keep working, only an operator restart with REAL_MODE=trade clears it.
            // Latched AFTER bootstrap (a clean observe boot is not a failed reconcile) and only if
            // no genuine halt holds the latch — a drift reason must not be overwritten by it.
            // When a genuine halt does hold it, the loop re-latches observe the moment that halt
            // is cleared: without that the scanner read the cleared halt as a trading bot and drove
            // closes into a read-only account (audit 06.09).
            if (relatchObserve(halt, observeOnly, Instant.now())) {
                alerts.warning("Real venue in OBSERVE mode",
                        "the bot reads the account and accepts closes; no position will be opened");
            }

            if (venue.isReal() && venue.realMode() == BinanceVenue.RealMode.TRADE) {
                // Proof of life for the alert channel itself: a rotated token or a chat id gone
                // stale fails silently, and the first message the operator would miss is an
                // incident. Every TRADE boot sends this line; its absence IS the alarm.
                alerts.info("Bot is up", "REAL TRADE armed; " + engine.book().openCount()
                        + " position(s) on the book. Every boot sends this line — a deploy with no "
                        + "such message means the alert channel is broken.");
            }

            int tpLegs = config.takeProfitPolicy().legs().size();
            int conditionalBudget = config.maxConcurrentPositions() * (1 + tpLegs);
            if (conditionalBudget > 30) {
                String text = "MAX_POSITIONS=" + config.maxConcurrentPositions() + " with " + tpLegs
                        + " take-profit leg(s) needs up to " + conditionalBudget + " conditional "
                        + "orders; the venue cap measured live on 14.08 was ~33. Positions past the "
                        + "cap will have stops or takes refused. Set TP_R_MULTIPLE for a single "
                        + "take, or lower MAX_POSITIONS.";
                LOG.warning("[Boot] " + text);
                alerts.warning("Conditional-order budget above the venue cap", text);
            }

            // The same snapshot as a file for the lab bot's panel (24.09). Writes on its own thread;
            // offer() never throws and never waits on the disk. Null = the panel reads no file.
            // Created before the hold watchdog, which writes it while the loop is parked.
            BotViewWriter botView = operator == null ? null
                    : BotViewWriter.startOrNull(dataDir.resolve(BotViewWriter.FILE_NAME));

            // The loop thread parks inside the rate limiter for the length of an exchange hold, so
            // it cannot repeat its own alert: 17 hours of sleep used to be one Telegram line at the
            // start and then silence. This thread exists only to keep telling the truth meanwhile.
            Thread holdWatchdog = new Thread(() -> {
                long lastReminderMs = 0;
                while (true) {
                    try {
                        Thread.sleep(60_000L);
                    } catch (InterruptedException e) {
                        return;
                    }
                    long held = port.heldByExchangeForMillis();
                    // The panel file too (24.09 review): the parked loop offers nothing, so the file
                    // said "trading" through a whole ban. The channel's last view with the hold read
                    // now; offerDuringHold never throws and is a no-op for a short hold.
                    if (botView != null) {
                        botView.offerDuringHold(held, operator::snapshot, halt::reason);
                    }
                    if (held < 300_000L) {
                        lastReminderMs = 0;
                        continue;
                    }
                    long nowWatchMs = System.currentTimeMillis();
                    if (lastReminderMs == 0) {
                        // The onExchangeHold alert already announced the hold; remind hourly after.
                        lastReminderMs = nowWatchMs;
                        continue;
                    }
                    if (nowWatchMs - lastReminderMs >= HOLD_REMINDER_INTERVAL_MS) {
                        alerts.critical("Exchange hold continues", (held / 60_000) + " min left. The "
                                + "bot sleeps the hold out and resumes by itself; resting stops keep "
                                + "guarding every position. A /close executes after the hold ends.");
                        lastReminderMs = nowWatchMs;
                    }
                }
            }, "hold-watchdog");
            holdWatchdog.setDaemon(true);
            holdWatchdog.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // An entry caught between its fill and its stop must finish before the JVM goes:
                // docker stop grants 45 s, and the intent file covers the case where it does not.
                long deadline = System.currentTimeMillis() + SHUTDOWN_GRACE_MS;
                while (coordinator.inFlight() > 0 && System.currentTimeMillis() < deadline) {
                    LOG.warning("[Shutdown] waiting for " + coordinator.inFlight() + " entr(ies) in flight");
                    try {
                        Thread.sleep(500L);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("[Shutdown] disarming the dead-man's switch");
                deadMansSwitch.disarmAll();
            }));

            long lastReconcileMs = System.currentTimeMillis();
            long lastHeartbeatMs = System.currentTimeMillis();
            long lastAliveLogMs = System.currentTimeMillis();
            int sourceFailures = 0;
            boolean bookAgreesWithExchange = bootstrapped;
            int reconcileFailures = 0;
            boolean blind = false;
            boolean outageAlerted = false;
            boolean adoptionDone = bootstrapped;
            // Closes that did not confirm flat, waiting for their next attempt with backoff. A
            // close used to be single-shot: one 429 destroyed the request forever (28.08 audit).
            java.util.ArrayDeque<PendingClose> pendingCloses = new java.util.ArrayDeque<>();
            long signalMaxAgeMs = Math.max(0, intProperty("SIGNAL_MAX_AGE_MIN", 90)) * 60_000L;
            // The operator's screens: built here from what the loop already knows, rendered on the
            // Telegram thread. /status answers from the first second, not 30 s after boot.
            Instant lastReconcileOkAt = bootstrapped ? Instant.now() : null;
            PnlProbe pnlProbe = new PnlProbe(port);
            OperatorSnapshot.Pnl pnl = null;
            if (operator != null) {
                try {
                    OperatorSnapshot first = operatorSnapshot(processStartedAt, buildStamp, observeOnly, engine, port,
                            deadMansSwitch, reconciler, journal, false, 0, pendingCloses, lastReconcileOkAt, null);
                    operator.publish(first);
                    if (botView != null) botView.offer(first, halt.reason());
                    if (notes != null) notes.observe(first.positions());
                } catch (RuntimeException e) {
                    LOG.warning("[Boot] operator snapshot not published: " + e.getMessage());
                }
            }

            while (!Thread.currentThread().isInterrupted()) {
                Instant now = Instant.now();

                // An unreachable signal source must not kill the process: a dead bot cannot close
                // the position it holds. Backing off keeps a hard-down queue from spinning the loop.
                try {
                    // Closes first, always: a halt must never be able to stop an unwind. Retries
                    // before fresh requests, so a backlog cannot starve a close already in flight.
                    int duePending = pendingCloses.size();
                    for (int i = 0; i < duePending; i++) {
                        PendingClose pending = pendingCloses.poll();
                        if (pending == null) break;
                        if (pending.notBeforeMs() > System.currentTimeMillis()) {
                            pendingCloses.add(pending);   // not due yet; rotate to the back
                            continue;
                        }
                        attemptClose(pending.close(), pending.attempts() + 1,
                                coordinator, signals, alerts, journal, notes, halt, pendingCloses);
                    }
                    if (operator != null) {
                        // A confirmed 🧯 is expanded here, against what is held NOW: its prompt was
                        // drawn from the last reconcile pass and could miss this hour's fills.
                        for (CloseRequest close : operator.drainCloses(() -> heldSymbols(engine, port))) {
                            attemptClose(close, 1, coordinator, signals, alerts, journal, notes, halt, pendingCloses);
                        }
                    }
                    for (CloseRequest close : signals.pollCloses()) {
                        attemptClose(close, 1, coordinator, signals, alerts, journal, notes, halt, pendingCloses);
                    }
                    for (Signal signal : signals.poll()) {
                        if (observeOnly) {
                            LOG.info("[Loop] REAL_MODE=observe - refusing " + signal.symbol());
                            signals.onRejected(signal, "REAL_MODE=observe: entries are disabled by the environment");
                            continue;
                        }
                        if (blind) {
                            // The book may be stale; the scanner re-nominates next hour anyway.
                            LOG.warning("[Loop] refusing " + signal.symbol() + ": reconciliation has failed "
                                    + reconcileFailures + " passes in a row, the book cannot be trusted");
                            signals.onRejected(signal, "RECONCILE_BLIND: exchange state unverified for "
                                    + reconcileFailures + " passes");
                            continue;
                        }
                        if (deadMansSwitch.isDegraded()) {
                            // Soft and self-clearing, exactly like blind: contact returning lifts it.
                            LOG.warning("[Loop] refusing " + signal.symbol()
                                    + ": no exchange contact (dead-man's switch degraded)");
                            signals.onRejected(signal, "EXCHANGE_CONTACT_LOST: entries pause until "
                                    + "the heartbeat succeeds again");
                            continue;
                        }
                        // Read the clock HERE, not at the top of the tick: the closes above can park
                        // inside the rate limiter for the length of an exchange hold, and an age
                        // measured against a `now` from before that reads as fresh no matter how
                        // long the wait was — which is the exact backlog this gate exists to stop.
                        Instant atSignal = Instant.now();
                        long ageMs = atSignal.toEpochMilli() - signal.createdAt().toEpochMilli();
                        if (signalMaxAgeMs > 0 && ageMs > signalMaxAgeMs) {
                            // A backlog written during a stall must not execute at market on prices
                            // from another market. Closes are exempt above: an exit is never stale.
                            LOG.warning("[Loop] refusing " + signal.symbol() + ": the signal is "
                                    + (ageMs / 60_000) + " min old (limit SIGNAL_MAX_AGE_MIN="
                                    + signalMaxAgeMs / 60_000 + ")");
                            signals.onRejected(signal, "STALE_SIGNAL: " + (ageMs / 60_000)
                                    + " min old, priced on a market that no longer exists");
                            continue;
                        }
                        if (ageMs < -60_000L) {
                            // Dated in the future: a writer's clock is wrong, and an age that reads
                            // negative would sail through the gate above for as long as the skew.
                            LOG.warning("[Loop] refusing " + signal.symbol() + ": dated "
                                    + (-ageMs / 60_000) + " min in the future");
                            signals.onRejected(signal, "STALE_SIGNAL: dated in the future — the "
                                    + "writer's clock disagrees with this process's");
                            continue;
                        }
                        handle(signal, engine, coordinator, port, signals, atSignal, journal, notes);
                    }
                    if (sourceFailures > 0) {
                        LOG.info("[Loop] signal source is answering again after " + sourceFailures + " failure(s)");
                        sourceFailures = 0;
                    }
                } catch (IOException e) {
                    sourceFailures++;
                    // Loud once, then quiet: an outage lasting hours must not bury the log.
                    if (sourceFailures == 1 || sourceFailures % 240 == 0) {
                        LOG.severe("[Loop] signal source unreachable (" + sourceFailures + " in a row): "
                                + e.getMessage() + " — the loop stays up so open positions can still be closed");
                    }
                    if (sourceFailures == 1) {
                        alerts.warning("Signal source unreachable", e.getMessage());
                    }
                    Thread.sleep(Math.min(30_000L, 1_000L * sourceFailures));
                }

                // The snapshot is what lets the NEXT process confirm stops by name — but only a book
                // the exchange has agreed with may overwrite it. A boot that failed to adopt holds an
                // empty book, and writing that erased the very record that could clear the halt.
                if (bookAgreesWithExchange) {
                    BookLedger.save(engine.book(), ledgerPath, lastLedgerBody);
                }

                long nowMs = System.currentTimeMillis();
                if (nowMs - lastAliveLogMs >= ALIVE_LOG_INTERVAL_MS) {
                    // The launcher's "already running" guard and the operator's eye both want a
                    // periodic line; the PnL seed used to supply one every 30 s by accident.
                    LOG.info("[Loop] alive: " + engine.book().openCount() + " position(s), halt="
                            + (halt.isHalted() ? "YES" : "none") + ", reconcile failures=" + reconcileFailures
                            + ", pending closes=" + pendingCloses.size()
                            + ", entries in flight=" + coordinator.inFlight());
                    lastAliveLogMs = nowMs;
                }
                // Housekeeping never kills the loop: a dead bot cannot close the position it holds.
                // The halt latch is how a real problem stops trading, not process death.
                if (nowMs - lastHeartbeatMs >= HEARTBEAT_INTERVAL_MS) {
                    try {
                        deadMansSwitch.heartbeat(Instant.now());
                    } catch (RuntimeException e) {
                        LOG.warning("[Loop] heartbeat failed: " + e.getMessage());
                    }
                    lastHeartbeatMs = nowMs;
                }
                if (nowMs - lastReconcileMs >= RECONCILE_INTERVAL_MS) {
                    try {
                        // Any completed pass earns the right to persist — reconcile realigns the book
                        // to the exchange before returning, so what follows is truthful even when the
                        // pass also found drift. Only a pass that THREW leaves the flag untouched.
                        Reconciler.Report passReport = reconciler.reconcile(Instant.now());
                        bookAgreesWithExchange = true;
                        lastReconcileOkAt = Instant.now();
                        if (passReport.converged()) {
                            // The only self-clearing latch: a boot that could not READ the exchange,
                            // now contradicted by a converged pass. Drift latches stay for /resume.
                            halt.clearIfReasonStartsWith(BOOT_READ_HALT_PREFIX,
                                    "the exchange answered and the book converged");
                        }
                        // Observe is an invariant, not a one-shot: whatever cleared the halt (the
                        // line above, an operator /resume), the venue mode is still read-only.
                        if (relatchObserve(halt, observeOnly, Instant.now())) {
                            alerts.warning("Real venue in OBSERVE mode",
                                    "re-latched after another halt was cleared; entries stay off");
                        }
                        if (!adoptionDone) {
                            // The boot read failed; the book was realigned from the exchange but
                            // carries no stop ids. Read them back now that the exchange answers.
                            try {
                                int adopted = BookLedger.adopt(engine.book(), port, port.openPositions());
                                LOG.info("[Loop] late adoption after a failed boot read: " + adopted
                                        + " position(s) now carry their stop ids");
                                adoptionDone = true;
                            } catch (RuntimeException e) {
                                LOG.warning("[Loop] late adoption failed, will retry: " + e.getMessage());
                            }
                        }
                        if (reconcileFailures > 0) {
                            LOG.info("[Loop] reconciliation is back after " + reconcileFailures + " failed pass(es)");
                            if (outageAlerted) {
                                alerts.info("Reconciliation is back", "after " + reconcileFailures
                                        + " failed pass(es)" + (blind ? "; entries resume" : ""));
                            }
                            reconcileFailures = 0;
                            blind = false;
                            outageAlerted = false;
                        }
                        // The pass above fed the switch fresh numbers; a trip now closes the book
                        // reduce-only so the daily limit is a ceiling, not a commentary.
                        killSwitchEnforcer.enforce(Instant.now());
                    } catch (RuntimeException e) {
                        // A pass that could not run is blindness, not drift: the book was not touched,
                        // the resting stops still guard every position, and the next pass is 30s away.
                        // One 429 with "retry in 3s" used to halt the bot until an operator typed
                        // /resume (23.08 02:18). Now: quiet, then a warning, then - only if it
                        // persists - entries are refused until sight returns. Never the latch.
                        reconcileFailures++;
                        LOG.warning("[Loop] reconciliation failed (" + reconcileFailures + " in a row): "
                                + e.getMessage());
                        if (reconcileFailures == RECONCILE_WARN_AFTER) {
                            alerts.warning("Reconciliation failing",
                                    reconcileFailures + " passes in a row: " + e.getMessage()
                                            + ". Positions stay under their exchange stops.");
                            outageAlerted = true;
                        }
                        if (reconcileFailures >= RECONCILE_BLIND_AFTER && !blind) {
                            blind = true;
                            alerts.critical("Reconciliation blind",
                                    "no successful pass for " + reconcileFailures + " attempts: "
                                            + e.getMessage() + ". New entries are refused until a pass "
                                            + "succeeds; closes, stops and takes keep working.");
                        }
                    }
                    if (operator != null) {
                        // After the pass, so /book shows what the exchange just said. The P&L read
                        // waits for a healthy pass: while reconciliation fails, one more call to the
                        // same exchange would only fail too. Nothing here may disturb the loop.
                        try {
                            if (reconcileFailures == 0) pnl = pnlProbe.refreshIfDue(Instant.now());
                            OperatorSnapshot snap = operatorSnapshot(processStartedAt, buildStamp, observeOnly,
                                    engine, port, deadMansSwitch, reconciler, journal, blind, reconcileFailures,
                                    pendingCloses, lastReconcileOkAt, pnl);
                            operator.publish(snap);
                            if (botView != null) botView.offer(snap, halt.reason());
                            // The notifier learns entries, sizes and open times from the same view,
                            // so an exit can say what it made and how long it was held.
                            if (notes != null) notes.observe(snap.positions());
                        } catch (RuntimeException e) {
                            LOG.warning("[Loop] operator snapshot not published: " + e.getMessage());
                        }
                    }
                    lastReconcileMs = nowMs;
                }
                Thread.sleep(250);
            }
        }
    }

    /**
     * What a tripped daily loss limit DOES. The default closes the whole book reduce-only;
     * {@code KILL_SWITCH_ACTION=halt-only} restores the old block-entries-only behaviour.
     */
    /**
     * {@code LOT_ROUND_UP}: on = 0.20, a fraction = that fraction (capped at 1.0, where the 1% hard cap
     * is the only limit left), unset/off = 0. The fraction is how far above the risk budget a
     * floor-constrained coin may be sized so that its lot clears the exchange minimum; coins that
     * size normally are never touched. Measured 05.09 at $137: 32 too-wide candidates, 6 clear at
     * 0.20, 20 at 0.50, 31 at 1.00.
     */
    private static double lotRoundUpTolerance() {
        String raw = System.getenv().getOrDefault("LOT_ROUND_UP", "").trim().toLowerCase(java.util.Locale.ROOT);
        if (raw.isEmpty() || raw.equals("off") || raw.equals("0") || raw.equals("false")) return 0.0;
        if (raw.equals("on") || raw.equals("1") || raw.equals("true") || raw.equals("yes")) return 0.20;
        try {
            return Math.min(1.0, Math.max(0.0, Double.parseDouble(raw)));
        } catch (NumberFormatException e) {
            LOG.warning("[Boot] LOT_ROUND_UP=\"" + raw + "\" is neither on/off nor a fraction; leaving it off");
            return 0.0;
        }
    }

    private static KillSwitchEnforcer.Action killSwitchAction() {
        String raw = System.getenv().getOrDefault("KILL_SWITCH_ACTION", "flatten")
                .trim().toLowerCase(java.util.Locale.ROOT);
        if (raw.equals("halt-only") || raw.equals("halt_only")) return KillSwitchEnforcer.Action.HALT_ONLY;
        if (!raw.equals("flatten") && !raw.isEmpty()) {
            LOG.warning("[Boot] unknown KILL_SWITCH_ACTION '" + raw + "' - using flatten");
        }
        return KillSwitchEnforcer.Action.FLATTEN;
    }

    /** Poll cadence while a limit entry rests; the window below is expressed in these ticks. */
    private static final long ENTRY_FILL_POLL_MS = 500L;
    private static final int ENTRY_FILL_WINDOW_DEFAULT_SEC = 60;
    private static final int ENTRY_FILL_WINDOW_MIN_SEC = 5;
    private static final int ENTRY_FILL_WINDOW_MAX_SEC = 600;

    /**
     * Entry execution from the environment. ENTRY_TYPE=limit places the entry at the signal price
     * (caps slippage; taker fee only if it crosses) with ENTRY_TIF (GTC default) and an
     * ENTRY_FILL_WINDOW_SEC window (default 60 s, measured 05.09: 16 of 17 signal-price limits fill
     * inside one minute) before the unfilled remainder is cancelled - a missed fill costs nothing,
     * the scanner re-nominates within the hour. A cancel that fails leaves the intent on record for
     * the reconciler. Default stays MARKET: switching execution is the owner's call, not this code's.
     */
    private static ExecutionCoordinator.Settings entrySettings() {
        String type = System.getenv().getOrDefault("ENTRY_TYPE", "market").trim().toLowerCase(java.util.Locale.ROOT);
        if (!type.equals("limit")) return ExecutionCoordinator.Settings.defaults();
        String tif = System.getenv().getOrDefault("ENTRY_TIF", "GTC").trim().toUpperCase(java.util.Locale.ROOT);
        OrderTypes.TimeInForce inForce;
        try {
            inForce = OrderTypes.TimeInForce.valueOf(tif);
        } catch (IllegalArgumentException e) {
            LOG.warning("[Boot] unknown ENTRY_TIF '" + tif + "' - using GTC");
            inForce = OrderTypes.TimeInForce.GTC;
        }
        int windowSec = entryFillWindowSeconds(System.getenv().getOrDefault("ENTRY_FILL_WINDOW_SEC", ""));
        int polls = (int) Math.max(1, Math.round(windowSec * 1000.0 / ENTRY_FILL_POLL_MS));
        LOG.info("[Boot] limit entries enabled (tif " + inForce + ", " + windowSec + "s fill window)");
        return new ExecutionCoordinator.Settings(OrderTypes.OrderType.LIMIT, inForce, polls, ENTRY_FILL_POLL_MS, 0.20);
    }

    /** {@code ENTRY_FILL_WINDOW_SEC}, clamped to [5, 600]; blank or unparseable falls back to 60. */
    static int entryFillWindowSeconds(String raw) {
        raw = raw == null ? "" : raw.trim();
        if (raw.isEmpty()) return ENTRY_FILL_WINDOW_DEFAULT_SEC;
        try {
            int parsed = Integer.parseInt(raw);
            int clamped = Math.min(ENTRY_FILL_WINDOW_MAX_SEC, Math.max(ENTRY_FILL_WINDOW_MIN_SEC, parsed));
            if (clamped != parsed) {
                LOG.warning("[Boot] ENTRY_FILL_WINDOW_SEC=" + parsed + " clamped to " + clamped);
            }
            return clamped;
        } catch (NumberFormatException e) {
            LOG.warning("[Boot] ENTRY_FILL_WINDOW_SEC=\"" + raw + "\" is not a number; using "
                    + ENTRY_FILL_WINDOW_DEFAULT_SEC + "s");
            return ENTRY_FILL_WINDOW_DEFAULT_SEC;
        }
    }

    /**
     * Exits that happened while the process was down: the ledger names the positions, the stop
     * order names the price when the venue still answers for it. Without this row the forward
     * record simply lost every stop-out and take-profit that fired between two runs.
     */
    private static void journalExitsWhileDown(TradeJournal journal, TradeNotifier notes, ExchangePort port,
                                              List<PositionSnapshot> live, Path ledgerPath) {
        if (journal == null && notes == null) return;
        for (BookLedger.ClosedWhileAway gone : BookLedger.closedWhileAway(live, ledgerPath)) {
            String state = "", price = "", qty = "";
            if (!gone.stopId().isBlank()) {
                try {
                    java.util.Optional<com.bot.exec.ExchangeSnapshots.OrderStatus> stop =
                            port.queryOrder(gone.symbol(), gone.stopId());
                    if (stop.isPresent()) {
                        state = stop.get().state().name();
                        if (stop.get().averagePrice().signum() > 0) price = stop.get().averagePrice().toPlainString();
                        if (stop.get().executedQuantity().signum() > 0) qty = stop.get().executedQuantity().toPlainString();
                    }
                } catch (RuntimeException e) {
                    LOG.fine("[Boot] could not read the stop that may have closed " + gone.symbol()
                            + ": " + e.getMessage());
                }
            }
            LOG.info("[Boot] " + gone.symbol() + " left the book while this process was down (stop "
                    + gone.stopId() + (state.isEmpty() ? "" : " " + state) + ")");
            if (journal != null) {
                journal.exchangeExit(gone.symbol(), "closed while the process was down; ledger side "
                        + gone.side(), gone.stopId(), state, price, qty, true);
            }
            if (notes != null) {
                // Only a stop that says FILLED is named; anything else is honestly "gone".
                boolean stopFilled = "FILLED".equals(state);
                notes.exit(gone.symbol(), stopFilled ? TradeNotifier.ExitKind.STOP : TradeNotifier.ExitKind.UNEXPLAINED,
                        number(price), number(qty), Instant.now(), "пока бот стоял");
            }
        }
    }

    /**
     * Puts the observe latch back whenever the venue is observe-only and nothing else holds the
     * halt. A drift or boot-read reason is never overwritten; the latch returns once it is gone.
     *
     * @return {@code true} when this call latched it (the caller alerts once per latch)
     */
    static boolean relatchObserve(TradingHalt halt, boolean observeOnly, Instant now) {
        if (!observeOnly || halt.isHalted()) return false;
        halt.halt(OBSERVE_HALT_REASON, now);
        return true;
    }

    private static List<PositionSnapshot> readPositionsWithRetry(ExchangePort port, int attempts)
            throws InterruptedException {
        RuntimeException last = null;
        for (int i = 1; i <= attempts; i++) {
            try {
                return port.openPositions();
            } catch (RuntimeException e) {
                last = e;
                LOG.warning("[Boot] position read failed (" + i + "/" + attempts + "): " + e.getMessage());
                if (i < attempts) Thread.sleep(5_000L);
            }
        }
        throw last;
    }

    /**
     * Everything the operator's screens show, built on the loop thread from the loop's own state and
     * the reconciler's last read - no exchange call of its own. Every state that pauses entries is
     * in it, or /status would say "green" while the bot quietly refuses every signal.
     */
    private static OperatorSnapshot operatorSnapshot(Instant startedAt, String buildStamp, boolean observeOnly,
                                                     RiskEngine engine, ExchangePort port,
                                                     DeadMansSwitch deadMansSwitch, Reconciler reconciler,
                                                     TradeJournal journal, boolean blind, int reconcileFailures,
                                                     java.util.Collection<PendingClose> pendingCloses,
                                                     Instant lastReconcileOkAt, OperatorSnapshot.Pnl pnl) {
        Instant now = Instant.now();
        java.util.Optional<Reconciler.LastRead> read = reconciler.lastRead();
        java.util.Map<String, TradeJournal.OpenMark> marks = journal == null ? java.util.Map.of() : journal.openMarks();
        List<OperatorSnapshot.Position> positions = OperatorSnapshot.positions(
                read.map(Reconciler.LastRead::positions).orElse(null), engine.book().all(), marks);
        // One evaluate() - exactly what isTripped(now) ran before - so the panel's figures and the
        // tripped flag come from the same reading of the switch (24.09).
        com.bot.risk.DailyLossKillSwitch.Status ks = engine.killSwitch().evaluate(now);
        return new OperatorSnapshot(now, startedAt, buildStamp, observeOnly,
                ks.tripped(), port.heldByExchangeForMillis(),
                deadMansSwitch.isDegraded(), blind, reconcileFailures, pendingCloses.size(), lastReconcileOkAt,
                read.map(r -> OperatorSnapshot.Account.of(r.account(), r.at())).orElse(null),
                positions, read.isPresent(), pnl, closeQueue(pendingCloses),
                OperatorSnapshot.KillSwitch.of(ks, engine.killSwitch().dailyLossFractionLimit()));
    }

    /**
     * Everything a confirmed 🧯 must cover at the moment the loop executes it: the book this process
     * keeps (a fill lands there before the next reconcile pass does) and the exchange's own listing
     * (a position the book does not know). Read only when a 🧯 is pending. An exchange read that
     * fails costs its extras only - the book, and the symbols the prompt listed, are still closed.
     */
    static java.util.Set<String> heldSymbols(RiskEngine engine, ExchangePort port) {
        java.util.Set<String> held = new java.util.LinkedHashSet<>();
        for (com.bot.risk.ExposureBook.OpenPosition p : engine.book().all()) held.add(p.symbol());
        try {
            for (PositionSnapshot p : port.openPositions()) {
                if (!p.isFlat()) held.add(p.symbol());
            }
        } catch (RuntimeException e) {
            LOG.warning("[Loop] close-all: exchange positions unreadable (" + e.getMessage()
                    + ") - closing the book and the listed symbols");
        }
        return held;
    }

    /** The retry queue as /queue shows it: attempts spent of the cap, and when the next one is due. */
    private static List<OperatorSnapshot.QueuedClose> closeQueue(java.util.Collection<PendingClose> pending) {
        List<OperatorSnapshot.QueuedClose> out = new java.util.ArrayList<>();
        for (PendingClose p : pending) {
            out.add(new OperatorSnapshot.QueuedClose(p.close().symbol(), p.close().reason(), p.attempts(),
                    CLOSE_MAX_ATTEMPTS, Instant.ofEpochMilli(p.notBeforeMs())));
        }
        return out;
    }

    /**
     * The notifier behind {@code TRADE_NOTIFY} / {@code QUIET_HOURS}, delivering through the
     * operator channel from a thread of its own. Null when notifications are off. Quiet hours get a
     * ticker, so the night's summary goes out when the window ends even if the book is idle.
     */
    private static TradeNotifier tradeNotifier(OperatorChannel operator) {
        TgOutbox outbox = new TgOutbox(operator::deliverHtml, Thread::sleep);
        TradeNotifier notes = TradeNotifier.fromEnvironmentOrNull(System::getenv, outbox::offer);
        if (notes == null) return null;
        outbox.start();
        if (notes.quietHours() != null) {
            Thread ticker = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(30_000L);
                    } catch (InterruptedException e) {
                        return;
                    }
                    notes.tick(Instant.now());
                }
            }, "trade-notes-ticker");
            ticker.setDaemon(true);
            ticker.start();
        }
        return notes;
    }

    /** An exit the reconciler proved, told in the owner's words. A failure here costs the line only. */
    static void notifyExchangeExit(TradeNotifier notes, String symbol, String cause, String price, String quantity) {
        if (notes == null) return;
        try {
            if ("partial-exit".equals(cause)) {
                notes.partial(symbol, Instant.now());
                return;
            }
            notes.exit(symbol, TradeNotifier.ExitKind.ofCause(cause), number(price), number(quantity),
                    Instant.now(), "");
        } catch (RuntimeException e) {
            LOG.warning("[Notify] exit line for " + symbol + " not sent: " + e.getMessage());
        }
    }

    /** An entry that filled and is protected: fill, planned stop and first take, dollars at risk. */
    static void notifyEntry(TradeNotifier notes, Signal signal, ExecutionCoordinator.Report report) {
        if (notes == null) return;
        try {
            double fill = report.averageFillPrice().doubleValue();
            double qty = report.filledQuantity().doubleValue();
            double stop = report.plan().stopPrice().doubleValue();
            double take = report.takeProfitOrders().stream()
                    .filter(o -> o.stopPrice() != null && o.stopPrice().signum() > 0)
                    .mapToDouble(o -> o.stopPrice().doubleValue()).findFirst().orElse(Double.NaN);
            notes.entry(signal.symbol(), signal.side(), fill, qty, stop, take,
                    PositionSizer.riskUsd(qty, fill, stop), Instant.now());
        } catch (RuntimeException e) {
            LOG.warning("[Notify] entry line for " + signal.symbol() + " not sent: " + e.getMessage());
        }
    }

    /** A close this process made and confirmed with a fill. A failure here costs the line only. */
    static void notifyClose(TradeNotifier notes, String symbol, String reason,
                            ExecutionCoordinator.CloseReport report) {
        if (notes == null || report == null || report.closedQuantity().signum() <= 0) return;
        try {
            TradeNotifier.ExitKind kind = TradeNotifier.ExitKind.ofCloseReason(reason);
            notes.exit(symbol, kind, report.averagePrice().doubleValue(), report.closedQuantity().doubleValue(),
                    Instant.now(), kind == TradeNotifier.ExitKind.OTHER ? reason : "");
        } catch (RuntimeException e) {
            LOG.warning("[Notify] close line for " + symbol + " not sent: " + e.getMessage());
        }
    }

    private static double number(String raw) {
        if (raw == null || raw.isBlank()) return Double.NaN;
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }

    /**
     * Which build is running, for /status: the Dockerfile writes {@code /app/BUILD_STAMP}; a run
     * outside the container may set {@code BUILD_STAMP}; anything else is "unknown", said plainly.
     */
    static String buildStamp(Path file, String env) {
        try {
            if (file != null && java.nio.file.Files.isRegularFile(file)) {
                String s = java.nio.file.Files.readString(file, java.nio.charset.StandardCharsets.UTF_8).trim();
                if (!s.isEmpty()) return s;
            }
        } catch (IOException | RuntimeException e) {
            LOG.fine("[Boot] build stamp unreadable: " + e.getMessage());
        }
        return env == null || env.isBlank() ? "unknown" : env.trim();
    }

    /**
     * One attempt at a close. Anything short of a confirmed flat re-queues with exponential backoff
     * — a close used to be single-shot, and one 429 on the position read destroyed the request
     * forever while a halt stood the scanner down behind it (28.08 audit, finding #3). Retries get
     * a fresh id suffix so a partially-filled earlier attempt is not adopted as "already done";
     * reduce-only makes any duplicate harmless.
     */
    private static void attemptClose(CloseRequest close, int attempt, ExecutionCoordinator coordinator,
                                     SignalSource signals, AlertSink alerts, TradeJournal journal,
                                     TradeNotifier notes, TradingHalt halt,
                                     java.util.ArrayDeque<PendingClose> pendingCloses)
            throws Exception {
        LOG.info("[Loop] " + close
                + (attempt > 1 ? " (attempt " + attempt + "/" + CLOSE_MAX_ATTEMPTS + ")" : ""));
        String requestId = attempt == 1 ? close.id() : close.id() + "-r" + attempt;
        String failure;
        try {
            ExecutionCoordinator.CloseReport report = coordinator.closeOut(close.symbol(), requestId);
            if (report.flat()) {
                // "Already flat" is a confirmed outcome but not a fill. Journalling it would put an
                // exit at price 0 into the record the lab judges live trades by.
                if (journal != null && report.closedQuantity().signum() > 0) {
                    journal.closed(close.id(), close.symbol(), close.reason(),
                            report.closedQuantity().toPlainString(),
                            report.averagePrice().toPlainString(), report.note());
                }
                notifyClose(notes, close.symbol(), close.reason(), report);
                signals.onClosed(close, new ExecutionFeedback(close.id(), report.closedQuantity(),
                        report.averagePrice(), report.note()));
                if (attempt > 1) {
                    alerts.info("Close succeeded on retry", close.symbol() + " closed on attempt " + attempt);
                }
                return;
            }
            failure = report.note();
        } catch (RuntimeException e) {
            failure = e.getMessage();
        }
        if (attempt >= CLOSE_MAX_ATTEMPTS) {
            LOG.severe("[Loop] close of " + close.symbol() + " abandoned after " + attempt
                    + " attempts: " + failure);
            alerts.critical("Close abandoned after retries",
                    close.symbol() + ": " + failure + " — " + CLOSE_MAX_ATTEMPTS + " attempts "
                            + "failed. The position keeps its resting stop; close it by hand "
                            + "or re-issue /close. Entries are halted until /resume.");
            // The latch moved here from the first failure: a close that needs eight tries is an
            // incident, one that needs two is a 429 (audit 03.09).
            halt.halt("close abandoned on " + close.symbol() + " after " + attempt + " attempts", Instant.now());
            return;
        }
        long delay = Math.min(CLOSE_RETRY_CAP_MS, CLOSE_RETRY_BASE_MS << (attempt - 1));
        LOG.warning("[Loop] close of " + close.symbol() + " did not complete (" + failure
                + ") — retrying in " + (delay / 1000) + "s");
        if (attempt == 1) {
            // The queue is in memory: a restart before the retries land drops the request, so the
            // operator must hear about the FIRST failure, not only about abandonment ten minutes on.
            alerts.warning("Close failed — retrying",
                    close.symbol() + ": " + failure + ". Up to " + CLOSE_MAX_ATTEMPTS
                            + " attempts over ~10 min. The retry queue does not survive a restart — "
                            + "re-issue /close if the bot restarts meanwhile.");
        }
        pendingCloses.add(new PendingClose(close, attempt, System.currentTimeMillis() + delay));
    }

    private static void handle(Signal signal, RiskEngine engine, ExecutionCoordinator coordinator,
                               ExchangePort port, SignalSource signals, Instant now, TradeJournal journal,
                               TradeNotifier notes)
            throws Exception {
        LOG.info("[Loop] " + signal);
        try {
            InstrumentFilters filters = port.fetchFilters(signal.symbol());
            MarginTierTable tiers = port.fetchMarginTiers(signal.symbol());
            AccountSnapshot account = port.fetchAccount();

            // Leverage is bounded in Signal's constructor and again by RiskEngine; nothing here widens it.
            TradeRequest request = new TradeRequest(signal.id(), signal.symbol(), signal.side(),
                    signal.entryPrice(), signal.structuralStopPrice(), signal.atr(),
                    signal.leverage(), filters, tiers);

            RiskDecision decision = engine.evaluate(request, account.equityUsd(), now);
            switch (decision) {
                case RiskDecision.Rejected rejected -> {
                    LOG.warning("[Loop] " + rejected);
                    if (journal != null) {
                        journal.entryRejected(signal.id(), signal.symbol(),
                                rejected.reason() + ": " + rejected.detail());
                    }
                    signals.onRejected(signal, rejected.reason() + ": " + rejected.detail());
                }
                case RiskDecision.Approved approved -> {
                    ExecutionCoordinator.Report report = coordinator.execute(approved.plan());
                    if (report.mayHaveOpenedUnknownRisk()) {
                        // Already alerted and halted; SEVERE so it is not read as an ordinary miss.
                        LOG.severe("[Loop] " + report.outcome() + " on " + signal.symbol()
                                + " — " + report.note());
                    } else {
                        LOG.info("[Loop] " + report.outcome() + " — " + report.note());
                    }
                    if (report.opened()) {
                        if (journal != null) {
                            TradePlan plan = report.plan();
                            double fill = report.averageFillPrice().doubleValue();
                            double qty = report.filledQuantity().doubleValue();
                            journal.entryOpened(signal.id(), signal.symbol(), signal.side().name(),
                                    String.valueOf(signal.entryPrice()),
                                    report.averageFillPrice().toPlainString(),
                                    report.filledQuantity().toPlainString(),
                                    report.protectiveStop().map(o -> o.clientOrderId()).orElse(null),
                                    plan.stopPrice().toPlainString(),
                                    report.takeProfitOrders().stream()
                                            .map(o -> o.stopPrice() == null ? "" : o.stopPrice().toPlainString())
                                            .toList(),
                                    PositionSizer.riskUsd(qty, fill, plan.stopPrice().doubleValue()),
                                    plan.leverage(), qty * fill,
                                    report.outcome() + ": " + report.note());
                        }
                        notifyEntry(notes, signal, report);
                        signals.onAccepted(signal, new ExecutionFeedback(
                                report.entryOrder().map(o -> o.clientOrderId()).orElse("unknown"),
                                report.filledQuantity(),
                                report.averageFillPrice(),
                                report.outcome() + ": " + report.note()));
                    } else {
                        if (journal != null && report.filledQuantity().signum() > 0) {
                            // Filled and unwound at once: a real round trip the record must see.
                            journal.entryAborted(signal.id(), signal.symbol(), signal.side().name(),
                                    report.averageFillPrice().toPlainString(),
                                    report.filledQuantity().toPlainString(),
                                    report.outcome().name(), report.note());
                        } else if (journal != null
                                && report.outcome() == ExecutionCoordinator.Outcome.REFUSED) {
                            // Refused by the EXCHANGE (a -4164 at mark, a validator refusal): the
                            // scanner rests a coin only on a journaled refusal, so without this row
                            // the same unplaceable line was re-proposed and refused every hour.
                            journal.entryRejected(signal.id(), signal.symbol(),
                                    "EXCHANGE_REFUSED: " + report.note());
                        }
                        signals.onRejected(signal, report.outcome() + ": " + report.note());
                    }
                }
            }
        } catch (RuntimeException e) {
            LOG.warning("[Loop] " + signal.id() + " failed: " + e.getMessage());
            signals.onRejected(signal, e.getMessage());
        }
    }

    private static SignalSource openSource(String name, String scriptPath, int defaultLeverage) throws Exception {
        if ("supabase".equalsIgnoreCase(name)) {
            SupabaseQueueSource source = SupabaseQueueSource.fromEnvironmentOrNull(defaultLeverage);
            if (source == null) {
                throw new IllegalStateException(
                        "--source supabase needs SUPABASE_URL and SUPABASE_QUEUE_KEY (or SUPABASE_KEY)");
            }
            return source;
        }
        if (scriptPath != null) {
            return ManualInput.fromReader(
                    new FileReader(Path.of(scriptPath).toFile(), java.nio.charset.StandardCharsets.UTF_8),
                    Clock.systemUTC(), defaultLeverage);
        }
        return ManualInput.fromConsole(defaultLeverage);
    }

    private static void banner(BinanceVenue venue, ExchangePort port, SignalSource signals,
                               RiskConfig config, int defaultLeverage) {
        String headline = venue.isReal()
                ? (venue.realMode() == BinanceVenue.RealMode.OBSERVE
                        ? "  │  REAL EXCHANGE — REAL MONEY. Mode: OBSERVE (no entries).             │"
                        : "  │  REAL EXCHANGE — REAL MONEY. Mode: TRADE. Armed by the operator.     │")
                : "  │  DEMO venue. The real exchange needs REAL_TRADING=ARMED + real keys. │";
        LOG.info(String.join("\n",
                "",
                "  ┌──────────────────────────────────────────────────────────────────────┐",
                headline,
                "  └──────────────────────────────────────────────────────────────────────┘",
                "  endpoint        : " + port.endpointHost() + "  (venue: " + venue.name() + ")",
                "  signal source   : " + signals.name(),
                "  risk per trade  : " + pct(config.riskFractionPerTrade())
                        + "  (hard cap " + pct(RiskConstants.MAX_RISK_FRACTION_PER_TRADE) + ")",
                "  leverage        : default " + defaultLeverage + "x, config cap "
                        + config.maxLeverage() + "x, hard cap " + RiskConstants.MAX_LEVERAGE + "x",
                "  liq buffer min  : " + pct(config.minLiquidationBufferFraction()),
                "  daily loss stop : " + pct(config.dailyLossFractionLimit()) + " of the day's opening balance",
                "  exposure caps   : long " + pct(config.maxLongExposureFraction())
                        + ", short " + pct(config.maxShortExposureFraction()) + " of balance",
                "  max positions   : " + config.maxConcurrentPositions(),
                "  lot round-up    : " + (lotRoundUpTolerance() > 0
                        ? "ON, up to the exchange minimum inside +" + pct(lotRoundUpTolerance()) + " of the risk budget"
                        : "off (floored size or refusal)"),
                ""));
    }

    private static String pct(double fraction) {
        return String.format("%.2f%%", fraction * 100);
    }

    private static double doubleProperty(String env, double fallback) {
        String raw = System.getenv(env);
        if (raw == null || raw.isBlank()) return fallback;
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warning("[Boot] " + env + "=\"" + raw + "\" is not a number; using " + fallback);
            return fallback;
        }
    }

    private static int intProperty(String env, int fallback) {
        String raw = System.getenv(env);
        if (raw == null || raw.isBlank()) return fallback;
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warning("[Boot] " + env + "=\"" + raw + "\" is not an integer; using " + fallback);
            return fallback;
        }
    }

    private static String argValue(String[] args, String flag, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) return args[i + 1];
        }
        return fallback;
    }

    private static String usage() {
        return String.join("\n",
                "Binance USDⓈ-M futures risk and execution harness. Demo venue by default.",
                "",
                "  --source manual|supabase   where signals come from (default: manual)",
                "  --script <file>            feed manual signals from a file instead of the console",
                "  --help                     this text",
                "",
                "Environment:",
                "  BINANCE_TESTNET_API_KEY     demo key, created with withdrawals DISABLED",
                "  BINANCE_TESTNET_API_SECRET  demo secret",
                "  REAL_TRADING                exactly ARMED selects the real exchange; anything",
                "                              else set here refuses to start. Unset = demo.",
                "  REAL_MODE                   observe (default) reads and closes only; trade opens.",
                "  BINANCE_REAL_API_KEY        real key: WITHDRAWALS DISABLED + IP whitelist",
                "  BINANCE_REAL_API_SECRET     real secret",
                "  DEFAULT_LEVERAGE            default leverage when a signal does not specify (max "
                        + RiskConstants.MAX_LEVERAGE + ")",
                "  SUPABASE_URL / SUPABASE_QUEUE_KEY   only for --source supabase",
                "  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID   optional, for push alerts",
                "  TRADE_NOTIFY                on (default) | off: one Telegram line per entry and exit",
                "  QUIET_HOURS                 e.g. 23-08 (Warsaw): trade lines held, one summary after",
                "",
                "Manual signal format:",
                "  SYMBOL SIDE entry=<price> [stop=<price>] [atr=<value>] [lev=<1.."
                        + RiskConstants.MAX_LEVERAGE + ">] [id=<text>]",
                "  BTCUSDT LONG entry=64000 stop=62800 lev=3");
    }
}
