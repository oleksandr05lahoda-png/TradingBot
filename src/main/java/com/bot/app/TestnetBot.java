package com.bot.app;

import com.bot.core.InstrumentFilters;
import com.bot.exec.AlertSink;
import com.bot.exec.DeadMansSwitch;
import com.bot.exec.ExchangePort;
import com.bot.exec.ExchangeSnapshots.AccountSnapshot;
import com.bot.exec.ExecutionCoordinator;
import com.bot.exec.IdempotentOrderPlacer;
import com.bot.exec.Reconciler;
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
import com.bot.signal.ManualTestnetInput;
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

/**
 * Entry point: wires the pieces together and runs the loop. Deliberately thin — assembly only, no
 * risk arithmetic and no exchange knowledge. Run with {@code --help} for options.
 */
public final class TestnetBot {

    private static final Logger LOG = Logger.getLogger(TestnetBot.class.getName());

    private static final long RECONCILE_INTERVAL_MS = 30_000L;
    private static final long HEARTBEAT_INTERVAL_MS = 30_000L;
    private static final long DEAD_MANS_COUNTDOWN_MS = 120_000L;
    private static final long DEAD_MANS_MAX_SILENCE_MS = 90_000L;

    private TestnetBot() {}

    public static void main(String[] args) throws Exception {
        if (List.of(args).contains("--help")) {
            System.out.println(usage());
            return;
        }

        int defaultLeverage = intProperty("DEFAULT_LEVERAGE", 3);
        String sourceName = argValue(args, "--source", "manual");
        String scriptPath = argValue(args, "--script", null);

        AlertSink alerts = AlertSink.fromEnvironment();
        RiskConfig config = RiskConfig.defaults();
        // Book width is an operational choice, not a risk limit — every hard ceiling (risk per
        // trade, leverage, daily loss, liquidation buffer) still binds per position, and the
        // daily kill switch is what actually bounds a correlated book. Left at the tight default
        // unless the operator says otherwise.
        int maxPositions = intProperty("MAX_POSITIONS", 0);
        if (maxPositions > 0) config = config.withMaxConcurrentPositions(maxPositions);
        // One take instead of two frees a conditional-order slot per position. The exchange caps
        // conditional orders per account (measured live 14.08: the cap arrived near 33), so with
        // stop + 2 takes the book tops out around ten protected positions; stop + 1 take buys
        // roughly fifteen. The R-multiple is the operator's, the split never was measured edge.
        double tpR = doubleProperty("TP_R_MULTIPLE", 0.0);
        if (tpR > 0) config = config.withTakeProfitPolicy(TakeProfitPolicy.single(tpR));
        RiskEngine engine = new RiskEngine(config, new ExposureBook(),
                new DailyLossKillSwitch(config.dailyLossFractionLimit()));
        TradingHalt halt = new TradingHalt();

        BinanceVenue venue;
        ExchangePort port;
        try {
            venue = BinanceVenue.resolveFromEnvironment();
            port = BinanceFuturesAdapter.fromEnvironment(venue);
        } catch (IllegalStateException e) {
            // A missing credential or a mis-set arming flag is an operator mistake:
            // print the fix, not a stack trace.
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }

        // `closedOnExit` only releases the port on the way out; components are wired to `port`.
        try (ExchangePort closedOnExit = port;
             SignalSource signals = openSource(sourceName, scriptPath, defaultLeverage)) {

            IdempotentOrderPlacer placer = new IdempotentOrderPlacer(port);
            ExecutionCoordinator coordinator = new ExecutionCoordinator(port, engine, placer, halt, alerts,
                    ExecutionCoordinator.Settings.defaults(), Clock.systemUTC(), Thread::sleep);
            Reconciler reconciler = new Reconciler(port, engine, halt, alerts, placer);
            DeadMansSwitch deadMansSwitch = new DeadMansSwitch(port, engine, halt, alerts,
                    DEAD_MANS_COUNTDOWN_MS, DEAD_MANS_MAX_SILENCE_MS);

            banner(venue, port, signals, config, defaultLeverage);

            // Observation is a pre-latched halt, not a separate mechanism: entries are refused
            // through the same gate every other halt uses, closes and reconciliation keep working,
            // and the only way out is the operator restarting with REAL_MODE=trade — the same
            // "operator clears it" contract as any other halt.
            if (venue.isReal() && venue.realMode() == BinanceVenue.RealMode.OBSERVE) {
                halt.halt("REAL_MODE=observe — reading the account, accepting closes, opening "
                        + "nothing. Set REAL_MODE=trade and restart to enable entries.", Instant.now());
                alerts.warning("Real venue in OBSERVE mode",
                        "the bot reads the account and accepts closes; no position will be opened");
            }

            // Re-arm the book from the last run's snapshot BEFORE reconciling: the venue cannot
            // name resting stops, so without this every restart with open positions ended in a
            // halt and a forced flatten. Positions the ledger does not know stay unknown and
            // still halt opening — that is the honest outcome for a genuinely unaccounted position.
            Path ledgerPath = Path.of(System.getenv().getOrDefault("BOOK_LEDGER_PATH", "book-ledger.json"));
            String[] lastLedgerBody = {""};
            int seeded = BookLedger.seed(engine.book(), port.openPositions(), ledgerPath);
            if (seeded > 0) {
                LOG.info("[Boot] re-armed " + seeded + " position(s) with recorded stop ids from " + ledgerPath);
            }

            // A start-up disagreement stops OPENING and nothing else. Exiting here would have been
            // the same mistake in a third place: the operator would be left with a position on the
            // exchange and no way to unwind it through the bot, which is precisely the state a
            // trading halt must never create. The loop stays up so closes are still processed.
            if (!reconciler.bootstrap(Instant.now())) {
                LOG.severe("[Boot] start-up reconciliation did not converge — trading is halted. "
                        + "Closes are still accepted; inspect the account, then restart to resume.");
                alerts.critical("Start-up reconciliation failed",
                        "no new positions will be opened; the bot stays up so positions can still be closed");
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("[Shutdown] disarming the dead-man's switch");
                deadMansSwitch.disarmAll();
            }));

            long lastReconcileMs = System.currentTimeMillis();
            long lastHeartbeatMs = System.currentTimeMillis();
            int sourceFailures = 0;

            while (!Thread.currentThread().isInterrupted()) {
                Instant now = Instant.now();

                // An unreachable signal source must not kill the process. The queue refusing to
                // answer says nothing about the exchange, and a dead bot cannot close the position
                // it is holding — the same reasoning that already protects the housekeeping below.
                // Backing off keeps a hard-down queue from spinning the loop at full speed.
                try {
                    // Closes first, always: a halt must never be able to stop an unwind.
                    for (CloseRequest close : signals.pollCloses()) {
                        handleClose(close, coordinator, signals);
                    }
                    for (Signal signal : signals.poll()) {
                        handle(signal, engine, coordinator, port, signals, now);
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

                // The snapshot is what lets the NEXT process confirm stops by name; a no-change
                // pass costs a string compare and nothing else.
                BookLedger.save(engine.book(), ledgerPath, lastLedgerBody);

                long nowMs = System.currentTimeMillis();
                // Housekeeping never kills the loop. A dead bot cannot close the position it is
                // holding, so a transient exchange error during a reconcile or a heartbeat must
                // leave the process alive and supervising — the halt latch is how a real problem
                // stops trading, not process death.
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
                        reconciler.reconcile(Instant.now());
                    } catch (RuntimeException e) {
                        LOG.severe("[Loop] reconciliation failed: " + e.getMessage()
                                + " — halting; local state can no longer be trusted");
                        alerts.critical("Reconciliation could not run", e.getMessage());
                        halt.halt("reconciliation failed: " + e.getMessage(), Instant.now());
                    }
                    lastReconcileMs = nowMs;
                }
                Thread.sleep(250);
            }
        }
    }

    private static void handleClose(CloseRequest close, ExecutionCoordinator coordinator,
                                    SignalSource signals) throws Exception {
        LOG.info("[Loop] " + close);
        try {
            ExecutionCoordinator.CloseReport report = coordinator.closeOut(close.symbol(), close.id());
            if (report.flat()) {
                signals.onClosed(close, new ExecutionFeedback(close.id(), report.closedQuantity(),
                        report.averagePrice(), report.note()));
            } else {
                // Already alerted and halted; the row stays claimed rather than retried into a loop.
                LOG.severe("[Loop] close of " + close.symbol() + " did not complete: " + report.note());
            }
        } catch (RuntimeException e) {
            LOG.severe("[Loop] close of " + close.symbol() + " failed: " + e.getMessage());
        }
    }

    private static void handle(Signal signal, RiskEngine engine, ExecutionCoordinator coordinator,
                               ExchangePort port, SignalSource signals, Instant now)
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
            return ManualTestnetInput.fromReader(
                    new FileReader(Path.of(scriptPath).toFile(), java.nio.charset.StandardCharsets.UTF_8),
                    Clock.systemUTC(), defaultLeverage);
        }
        return ManualTestnetInput.fromConsole(defaultLeverage);
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
