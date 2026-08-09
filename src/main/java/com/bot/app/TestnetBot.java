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
import com.bot.exec.binance.BinanceFuturesTestnetAdapter;
import com.bot.exec.binance.BinanceTestnetEndpoint;
import com.bot.risk.DailyLossKillSwitch;
import com.bot.risk.ExposureBook;
import com.bot.risk.MarginTierTable;
import com.bot.risk.RiskConfig;
import com.bot.risk.RiskConstants;
import com.bot.risk.RiskDecision;
import com.bot.risk.RiskEngine;
import com.bot.risk.TradeRequest;
import com.bot.signal.ManualTestnetInput;
import com.bot.signal.Signal;
import com.bot.signal.SignalSource;
import com.bot.signal.SupabaseQueueSource;

import java.io.FileReader;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;

/**
 * Entry point: wires the pieces together and runs the loop. Deliberately thin — it contains no risk
 * arithmetic and no exchange knowledge, only assembly, so that reading it tells you what talks to
 * what and nothing else.
 *
 * <pre>
 *   SignalSource ─▶ RiskEngine.evaluate ─▶ ExecutionCoordinator.execute ─▶ ExchangePort
 *                        │                          │                          ▲
 *                        └── ExposureBook ◀── Reconciler ───────────────────────┘
 *                                                   │
 *                                            DeadMansSwitch
 * </pre>
 *
 * <h2>Running it</h2>
 * <pre>
 *   BINANCE_TESTNET_API_KEY=...  BINANCE_TESTNET_API_SECRET=...  ./gradlew run
 *   ./gradlew run --args="--script signals.txt"    # non-interactive smoke run
 *   ./gradlew run --args="--source supabase"       # drain the external queue instead
 * </pre>
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
        RiskEngine engine = new RiskEngine(config, new ExposureBook(),
                new DailyLossKillSwitch(config.dailyLossFractionLimit()));
        TradingHalt halt = new TradingHalt();

        ExchangePort port;
        try {
            port = BinanceFuturesTestnetAdapter.fromEnvironment();
        } catch (IllegalStateException e) {
            // A missing credential is an operator mistake, not a bug. It deserves the sentence that
            // says how to fix it, not a stack trace that buries it.
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }

        // `closedOnExit` exists only so the port is released on the way out; `port` is what the
        // components below are wired to.
        try (ExchangePort closedOnExit = port;
             SignalSource signals = openSource(sourceName, scriptPath, defaultLeverage)) {

            IdempotentOrderPlacer placer = new IdempotentOrderPlacer(port);
            ExecutionCoordinator coordinator = new ExecutionCoordinator(port, engine, placer, halt, alerts,
                    ExecutionCoordinator.Settings.defaults(), Clock.systemUTC(), Thread::sleep);
            Reconciler reconciler = new Reconciler(port, engine, halt, alerts, placer);
            DeadMansSwitch deadMansSwitch = new DeadMansSwitch(port, engine, halt, alerts,
                    DEAD_MANS_COUNTDOWN_MS, DEAD_MANS_MAX_SILENCE_MS);

            banner(port, signals, config, defaultLeverage);

            if (!reconciler.bootstrap(Instant.now())) {
                LOG.severe("[Boot] start-up reconciliation did not converge — refusing to trade. "
                        + "Inspect the account, then restart.");
                alerts.critical("Start-up reconciliation failed",
                        "the bot refused to start trading; local state and the exchange disagree");
                return;
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("[Shutdown] disarming the dead-man's switch");
                deadMansSwitch.disarmAll();
            }));

            long lastReconcileMs = System.currentTimeMillis();
            long lastHeartbeatMs = System.currentTimeMillis();

            while (!Thread.currentThread().isInterrupted()) {
                Instant now = Instant.now();

                for (Signal signal : signals.poll()) {
                    handle(signal, engine, coordinator, port, signals, now);
                }

                long nowMs = System.currentTimeMillis();
                if (nowMs - lastHeartbeatMs >= HEARTBEAT_INTERVAL_MS) {
                    deadMansSwitch.heartbeat(Instant.now());
                    lastHeartbeatMs = nowMs;
                }
                if (nowMs - lastReconcileMs >= RECONCILE_INTERVAL_MS) {
                    reconciler.reconcile(Instant.now());
                    lastReconcileMs = nowMs;
                }
                Thread.sleep(250);
            }
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

            // The signal's leverage is already bounded by RiskConstants.MAX_LEVERAGE in Signal's
            // constructor; RiskEngine bounds it again against the config. Nothing here widens it.
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
                        // The coordinator has already alerted and halted. Log at SEVERE so the
                        // outcome is not mistaken for the ordinary "this signal did not work out".
                        LOG.severe("[Loop] " + report.outcome() + " on " + signal.symbol()
                                + " — " + report.note());
                    } else {
                        LOG.info("[Loop] " + report.outcome() + " — " + report.note());
                    }
                    if (report.opened()) {
                        signals.onAccepted(signal,
                                report.entryOrder().map(o -> o.clientOrderId()).orElse("unknown"));
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

    private static void banner(ExchangePort port, SignalSource signals, RiskConfig config, int defaultLeverage) {
        LOG.info(String.join("\n",
                "",
                "  ┌──────────────────────────────────────────────────────────────────────┐",
                "  │  TESTNET ONLY. No production endpoint exists in this build.          │",
                "  └──────────────────────────────────────────────────────────────────────┘",
                "  endpoint        : " + port.endpointHost() + "  (allowed: "
                        + BinanceTestnetEndpoint.ALLOWED_HOSTS + ")",
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
                "Binance USDⓈ-M futures testnet risk and execution harness.",
                "",
                "  --source manual|supabase   where signals come from (default: manual)",
                "  --script <file>            feed manual signals from a file instead of the console",
                "  --help                     this text",
                "",
                "Environment:",
                "  BINANCE_TESTNET_API_KEY     testnet key, created with withdrawals DISABLED",
                "  BINANCE_TESTNET_API_SECRET  testnet secret",
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
