package com.bot.app;

import com.bot.core.InstrumentFilters;
import com.bot.exec.AlertSink;
import com.bot.exec.DeadMansSwitch;
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
import com.bot.risk.RiskConfig;
import com.bot.risk.RiskConstants;
import com.bot.risk.RiskDecision;
import com.bot.risk.RiskEngine;
import com.bot.risk.TakeProfitPolicy;
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

    /** A close that did not confirm flat, waiting for its next attempt. */
    private record PendingClose(CloseRequest close, int attempts, long notBeforeMs) {}

    private TestnetBot() {}

    public static void main(String[] args) throws Exception {
        if (List.of(args).contains("--help")) {
            System.out.println(usage());
            return;
        }

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
        // One take frees a conditional-order slot. Binance caps conditional orders per account (cap
        // arrived near 33, measured live 14.08): stop+2 takes protects ~10 positions, stop+1 ~15.
        double tpR = doubleProperty("TP_R_MULTIPLE", 0.0);
        if (tpR > 0) config = config.withTakeProfitPolicy(TakeProfitPolicy.single(tpR));
        RiskEngine engine = new RiskEngine(config, new ExposureBook(),
                new DailyLossKillSwitch(config.dailyLossFractionLimit()));
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
            operator.withVenueTag(venue.isReal() ? "REAL " + venue.realMode() : "DEMO").withAlerts(alerts);
        }
        // Observe is enforced here, on every signal, independent of any latch: the latch below
        // exists so the scanner stands down, but a latch can be cleared and a mode cannot.
        final boolean observeOnly = venue.isReal() && venue.realMode() == BinanceVenue.RealMode.OBSERVE;

        // The raw material for honest learning: one JSONL row per event, on the volume.
        TradeJournal journal = TradeJournal.fromEnvironmentOrNull();

        // `closedOnExit` only releases the port on the way out; components are wired to `port`.
        try (ExchangePort closedOnExit = port;
             SignalSource signals = openSource(sourceName, scriptPath, defaultLeverage);
             AutoCloseable operatorChannel = operator == null ? () -> { } : operator) {
            if (operator != null) operator.start();

            IdempotentOrderPlacer placer = new IdempotentOrderPlacer(port);
            ExecutionCoordinator coordinator = new ExecutionCoordinator(port, engine, placer, halt, alerts,
                    entrySettings(), Clock.systemUTC(), Thread::sleep);
            Reconciler reconciler = new Reconciler(port, engine, halt, alerts, placer);
            // A booked position found without a stop is repaired — stop re-placed from the book's
            // record, or closed reduce-only — instead of only halted on (28.08 audit, finding #1).
            reconciler.withStopRepair(coordinator::closeOut);
            if (journal != null) {
                reconciler.onExchangeExit(journal::exchangeExit);
                reconciler.onRepairClose((requestId, symbol, closeReport) ->
                        journal.closed(requestId, symbol, "stop-repair",
                                closeReport.closedQuantity().toPlainString(),
                                closeReport.averagePrice().toPlainString(), closeReport.note()));
            }
            DeadMansSwitch deadMansSwitch = new DeadMansSwitch(port, engine, alerts,
                    DEAD_MANS_COUNTDOWN_MS, DEAD_MANS_MAX_SILENCE_MS);
            // The daily loss limit closes the book on the trip (KILL_SWITCH_ACTION=halt-only opts
            // out): -3% must mean -3%, not "-3% plus whatever the open losers still give back".
            TradeJournal journalRef = journal;
            KillSwitchEnforcer killSwitchEnforcer = new KillSwitchEnforcer(engine,
                    coordinator::closeOut, alerts, killSwitchAction(),
                    journalRef == null ? null : (requestId, symbol, closeReport) ->
                            journalRef.closed(requestId, symbol, "daily-loss-kill-switch",
                                    closeReport.closedQuantity().toPlainString(),
                                    closeReport.averagePrice().toPlainString(), closeReport.note()));

            banner(venue, port, signals, config, defaultLeverage);

            // Re-arm the book from the last snapshot BEFORE reconciling: the venue cannot name
            // resting stops, so otherwise a restart with open positions halts and forces a flatten.
            Path ledgerPath = Path.of(System.getenv().getOrDefault("BOOK_LEDGER_PATH", "book-ledger.json"));
            String[] lastLedgerBody = {""};

            // A start-up disagreement stops OPENING only — exiting would strand a position with no
            // way to unwind it through the bot. Covers the first exchange read too: dying there was
            // measured with an invalid key on 20.08.
            boolean bootstrapped;
            while (true) {
                try {
                    // One 5xx or timeout here used to leave the book empty for the life of the process
                    // and every later pass flagging UNKNOWN_POSITION. Three tries, then the loop retries
                    // adoption itself after its first successful reconcile.
                    List<PositionSnapshot> live = readPositionsWithRetry(port, 3);
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
                    bootstrapped = false;
                    halt.halt("boot could not read the exchange: " + e.getMessage(), Instant.now());
                }
                break;
            }
            if (!bootstrapped) {
                LOG.severe("[Boot] start-up reconciliation did not converge — trading is halted. "
                        + "Closes are still accepted; inspect the account, then restart to resume.");
                alerts.critical("Start-up reconciliation failed",
                        "no new positions will be opened; the bot stays up so positions can still be closed");
            }

            // Observe is a pre-latched halt, not a separate mechanism: same entry gate, closes and
            // reconciliation keep working, only an operator restart with REAL_MODE=trade clears it.
            // Latched AFTER bootstrap (a clean observe boot is not a failed reconcile) and only if
            // no genuine halt holds the latch — a drift reason must not be overwritten by it.
            if (venue.isReal() && venue.realMode() == BinanceVenue.RealMode.OBSERVE
                    && !halt.isHalted()) {
                halt.halt("REAL_MODE=observe — reading the account, accepting closes, opening "
                        + "nothing. Set REAL_MODE=trade and restart to enable entries.", Instant.now());
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
                LOG.info("[Shutdown] disarming the dead-man's switch");
                deadMansSwitch.disarmAll();
            }));

            long lastReconcileMs = System.currentTimeMillis();
            long lastHeartbeatMs = System.currentTimeMillis();
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
                                coordinator, signals, alerts, journal, pendingCloses);
                    }
                    if (operator != null) {
                        for (CloseRequest close : operator.drainCloses()) {
                            attemptClose(close, 1, coordinator, signals, alerts, journal, pendingCloses);
                        }
                    }
                    for (CloseRequest close : signals.pollCloses()) {
                        attemptClose(close, 1, coordinator, signals, alerts, journal, pendingCloses);
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
                        handle(signal, engine, coordinator, port, signals, atSignal, journal);
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
                    if (operator != null) {
                        operator.publishStatus(
                                statusLine(venue, engine, halt, port, deadMansSwitch, pendingCloses.size()),
                                Instant.now());
                    }
                    try {
                        // Any completed pass earns the right to persist — reconcile realigns the book
                        // to the exchange before returning, so what follows is truthful even when the
                        // pass also found drift. Only a pass that THREW leaves the flag untouched.
                        reconciler.reconcile(Instant.now());
                        bookAgreesWithExchange = true;
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
                    lastReconcileMs = nowMs;
                }
                Thread.sleep(250);
            }
        }
    }

    /**
     * Entry execution from the environment. ENTRY_TYPE=limit places the entry at the signal price
     * (caps slippage; taker fee only if it crosses) with ENTRY_TIF (GTC default) and a ~10s window
     * before the unfilled remainder is cancelled - a missed fill costs nothing, the scanner
     * re-nominates within the hour. Default stays MARKET: switching execution is measured with the
     * cost probe, not assumed.
     */
    /**
     * What a tripped daily loss limit DOES. The default closes the whole book reduce-only;
     * {@code KILL_SWITCH_ACTION=halt-only} restores the old block-entries-only behaviour.
     */
    private static KillSwitchEnforcer.Action killSwitchAction() {
        String raw = System.getenv().getOrDefault("KILL_SWITCH_ACTION", "flatten")
                .trim().toLowerCase(java.util.Locale.ROOT);
        if (raw.equals("halt-only") || raw.equals("halt_only")) return KillSwitchEnforcer.Action.HALT_ONLY;
        if (!raw.equals("flatten") && !raw.isEmpty()) {
            LOG.warning("[Boot] unknown KILL_SWITCH_ACTION '" + raw + "' - using flatten");
        }
        return KillSwitchEnforcer.Action.FLATTEN;
    }

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
        LOG.info("[Boot] limit entries enabled (tif " + inForce + ", ~10s fill window)");
        return new ExecutionCoordinator.Settings(OrderTypes.OrderType.LIMIT, inForce, 20, 500, 0.20);
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

    /** One line the operator can read on a phone; built on the loop thread from the loop's own state. */
    private static String statusLine(BinanceVenue venue, RiskEngine engine, TradingHalt halt,
                                     ExchangePort port, DeadMansSwitch deadMansSwitch, int pendingCloses) {
        StringBuilder sb = new StringBuilder();
        sb.append(venue.isReal() ? "REAL " + venue.realMode() : "DEMO").append(" | ");
        List<ExposureBook.OpenPosition> open = engine.book().all();
        sb.append(open.size()).append(" position(s)");
        for (ExposureBook.OpenPosition p : open) {
            sb.append("\n  ").append(p.symbol()).append(' ').append(p.side())
                    .append(" qty ").append(p.quantity().stripTrailingZeros().toPlainString())
                    .append(" risk $").append(String.format(java.util.Locale.ROOT, "%.2f", p.riskUsd()))
                    .append(p.protectiveStopId().isPresent() ? " stop ok" : " NO STOP ID");
        }
        sb.append("\nhalt: ").append(halt.isHalted() ? halt.reason().orElse("yes") : "none");
        long held = port.heldByExchangeForMillis();
        if (held > 0) sb.append("\nexchange hold: ").append(held / 60_000).append(" min left");
        if (deadMansSwitch.isDegraded()) sb.append("\nentries: PAUSED (exchange contact lost)");
        if (pendingCloses > 0) sb.append("\ncloses retrying: ").append(pendingCloses);
        sb.append("\nkill switch: ").append(engine.killSwitch().isTripped(Instant.now()) ? "TRIPPED" : "armed");
        return sb.toString();
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
                                     java.util.ArrayDeque<PendingClose> pendingCloses)
            throws Exception {
        LOG.info("[Loop] " + close
                + (attempt > 1 ? " (attempt " + attempt + "/" + CLOSE_MAX_ATTEMPTS + ")" : ""));
        String requestId = attempt == 1 ? close.id() : close.id() + "-r" + attempt;
        String failure;
        try {
            ExecutionCoordinator.CloseReport report = coordinator.closeOut(close.symbol(), requestId);
            if (report.flat()) {
                if (journal != null) {
                    journal.closed(close.id(), close.symbol(), close.reason(),
                            report.closedQuantity().toPlainString(),
                            report.averagePrice().toPlainString(), report.note());
                }
                signals.onClosed(close, new ExecutionFeedback(close.id(), report.closedQuantity(),
                        report.averagePrice(), report.note()));
                if (attempt > 1) {
                    alerts.info("Close succeeded on retry", close.symbol() + " closed on attempt "
                            + attempt + "; a halt latched by the earlier failure still needs /resume");
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
                            + "or re-issue /close.");
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
                               ExchangePort port, SignalSource signals, Instant now, TradeJournal journal)
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
                            journal.entryOpened(signal.id(), signal.symbol(), signal.side().name(),
                                    String.valueOf(signal.entryPrice()),
                                    report.averageFillPrice().toPlainString(),
                                    report.filledQuantity().toPlainString(),
                                    report.protectiveStop()
                                            .map(o -> o.clientOrderId()).orElse(null),
                                    report.outcome() + ": " + report.note());
                        }
                        signals.onAccepted(signal, new ExecutionFeedback(
                                report.entryOrder().map(o -> o.clientOrderId()).orElse("unknown"),
                                report.filledQuantity(),
                                report.averageFillPrice(),
                                report.outcome() + ": " + report.note()));
                    } else {
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
                "",
                "Manual signal format:",
                "  SYMBOL SIDE entry=<price> [stop=<price>] [atr=<value>] [lev=<1.."
                        + RiskConstants.MAX_LEVERAGE + ">] [id=<text>]",
                "  BTCUSDT LONG entry=64000 stop=62800 lev=3");
    }
}
