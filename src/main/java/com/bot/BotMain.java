package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BotMain {

    private static final Logger LOGGER = Logger.getLogger(BotMain.class.getName());
    private static final String TG_TOKEN = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID = "953233853";
    private static final ZoneId ZONE = ZoneId.of("Europe/Warsaw");
    private static final int SIGNAL_INTERVAL_MIN = 15;
    private static final int KLINES_LIMIT = 200;

    public static void main(String[] args) {

        if (TG_TOKEN == null || TG_TOKEN.isBlank()) {
            LOGGER.severe("TELEGRAM_TOKEN not set!");
            return;
        }

        final TelegramBotSender telegram = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        final SignalSender signalSender = new SignalSender(telegram);
        final GlobalImpulseController globalImpulse = new GlobalImpulseController();
        final InstitutionalSignalCore institutionalCore = new InstitutionalSignalCore();

        telegram.sendMessageAsync("🚀 Bot запущен | 15M | Anti-Lag + ReverseDetect ACTIVE");
        LOGGER.info("Bot started at " + LocalDateTime.now());

        var scheduler = java.util.concurrent.Executors.newScheduledThreadPool(1, new BotThreadFactory());

        Runnable signalTask = () -> {
            try {
                LOGGER.info("=== SIGNAL SCAN START === " + LocalDateTime.now());

                List<DecisionEngineMerged.TradeIdea> rawSignals = signalSender.generateSignals();
                if (rawSignals == null || rawSignals.isEmpty()) {
                    LOGGER.info("No valid signals this cycle.");
                    return;
                }

                // === ГЛОБАЛЬНЫЙ ИМПУЛЬС (BTC контекст) ===
                List<TradingCore.Candle> btcCandles = signalSender.fetchKlines("BTCUSDT", "15m", KLINES_LIMIT);
                if (btcCandles != null && btcCandles.size() > 20) {
                    globalImpulse.update(btcCandles);
                }

                GlobalImpulseController.GlobalContext ctx = globalImpulse.getContext();
                LOGGER.info("BTC: " + ctx.regime
                        + " | strength=" + String.format("%.2f", ctx.impulseStrength)
                        + " | vol=" + String.format("%.2f", ctx.volatilityExpansion));

                // === ФИЛЬТР 1: GlobalImpulse ===
                List<DecisionEngineMerged.TradeIdea> impulseFiltered = rawSignals.stream()
                        .filter(s -> {
                            double coeff = globalImpulse.filterSignal(s);
                            if (coeff <= 0.0) {
                                LOGGER.fine("GlobalImpulse BLOCKED: " + s.symbol);
                                return false;
                            }
                            return true;
                        })
                        .toList();

                if (impulseFiltered.isEmpty()) {
                    LOGGER.info("No signals after GlobalImpulse filter.");
                    return;
                }

                // === ФИЛЬТР 2: Institutional (Portfolio Risk) ===
                List<DecisionEngineMerged.TradeIdea> finalSignals = impulseFiltered.stream()
                        .filter(s -> {
                            if (!institutionalCore.allowSignal(s)) {
                                LOGGER.fine("ISC REJECTED: " + s.symbol);
                                return false;
                            }
                            institutionalCore.registerSignal(s);
                            return true;
                        })
                        .toList();

                if (finalSignals.isEmpty()) {
                    LOGGER.info("No signals after ISC filter.");
                    return;
                }

                // === ОТПРАВКА ===
                for (DecisionEngineMerged.TradeIdea s : finalSignals) {
                    telegram.sendMessageAsync(s.toString());
                    LOGGER.info("SIGNAL SENT: " + s.symbol + " | " + s.side + " | Prob:" + s.probability);
                }

                LOGGER.info("=== Signals sent: " + finalSignals.size() + " ===");

            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "CRITICAL ERROR", t);
                telegram.sendMessageAsync("⚠️ ERROR: " + t.getMessage());
            }
        };

        scheduler.scheduleAtFixedRate(signalTask, 0, SIGNAL_INTERVAL_MIN, java.util.concurrent.TimeUnit.MINUTES);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down...");
            scheduler.shutdown();
            telegram.shutdown();
            try {
                if (!scheduler.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }));
    }

    static class BotThreadFactory implements java.util.concurrent.ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("SignalSchedulerThread");
            t.setDaemon(false);
            t.setUncaughtExceptionHandler((thread, ex) ->
                    Logger.getLogger(BotMain.class.getName()).log(Level.SEVERE, "UNCAUGHT", ex));
            return t;
        }
    }

    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis)
                .atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }
}