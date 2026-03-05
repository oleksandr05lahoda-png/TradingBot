package com.bot;

import java.text.DecimalFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class BotMain {

    private static final Logger LOGGER = Logger.getLogger(BotMain.class.getName());

    // ===== CONFIG =====
    private static final String TG_TOKEN = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID = "953233853";
    private static final ZoneId ZONE = ZoneId.systemDefault();
    private static final int SIGNAL_INTERVAL_MIN = 15;

    private BotMain() { }

    public static void main(String[] args) {

        if (TG_TOKEN == null || TG_TOKEN.isBlank()) {
            LOGGER.severe("TELEGRAM_TOKEN not set!");
            return;
        }

        TelegramBotSender telegram = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        TradingCore tradingCore = new TradingCore();
        DecisionEngineMerged decisionEngine = new DecisionEngineMerged();
        GlobalImpulseController globalImpulse = new GlobalImpulseController();

        telegram.sendMessageAsync("🚀 Trading Bot запущен");
        LOGGER.info("Bot started at " + LocalDateTime.now());

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new BotThreadFactory());

        Runnable signalTask = () -> {
            try {
                LOGGER.info("=== SIGNAL SCAN START === " + LocalDateTime.now());

                // === Получаем свечи с твоего источника (TradingCore) ===
                // Твой код должен здесь подставить реальные списки свечей
                List<TradingCore.Candle> candlesBTC = tradingCoreCandles("BTCUSDT"); // метод реализуй у себя
                List<TradingCore.Candle> candlesSymbol = tradingCoreCandles("BTCUSDT"); // пример для анализа

                if (candlesBTC.isEmpty() || candlesSymbol.isEmpty()) {
                    LOGGER.warning("Candles not available. Skipping cycle.");
                    return;
                }

                // === Обновляем глобальный импульс BTC ===
                globalImpulse.update(candlesBTC);
                GlobalImpulseController.GlobalContext context = globalImpulse.getContext();

                // === Генерируем сигнал через DecisionEngineMerged ===
                DecisionEngineMerged.TradeIdea signal = decisionEngine.analyze(
                        "BTCUSDT",
                        candlesSymbol, candlesSymbol, candlesSymbol, candlesBTC, // все свечи через твои списки
                        DecisionEngineMerged.CoinCategory.TOP,
                        context
                );

                if (signal != null) {
                    telegram.sendMessageAsync(formatSignal(signal));
                } else {
                    LOGGER.info("No valid signals this cycle.");
                }

            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "CRITICAL ERROR in signal cycle", t);
                telegram.sendMessageAsync("⚠ Ошибка цикла: " + t.getMessage());
            }
        };

        scheduler.scheduleAtFixedRate(signalTask, 0, SIGNAL_INTERVAL_MIN, TimeUnit.MINUTES);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down bot...");
            scheduler.shutdown();
            telegram.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }));
    }

    private static String formatSignal(DecisionEngineMerged.TradeIdea signal) {
        String flags = Optional.ofNullable(signal.flags)
                .filter(f -> !f.isEmpty())
                .map(f -> String.join(", ", f))
                .orElse("—");

        DecimalFormat df = new DecimalFormat("0.######");

        return String.format(
                "*%s* → *%s*\n" +
                        "Price: %s\n" +
                        "Probability: %.0f%%\n" +
                        "Stop-Take: %s - %s\n" +
                        "Flags: %s\n" +
                        "_time: %s_",
                signal.symbol,
                signal.side,
                df.format(signal.price),
                signal.probability,
                df.format(signal.stop),
                df.format(signal.take),
                flags,
                LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))
        );
    }

    private static final class BotThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "SignalSchedulerThread");
            t.setDaemon(false);
            t.setUncaughtExceptionHandler((thread, ex) ->
                    LOGGER.log(Level.SEVERE, "UNCAUGHT ERROR in " + thread.getName(), ex));
            return t;
        }
    }

    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis)
                .atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }

    // === ПРИМЕР МЕТОДА ДЛЯ ПОЛУЧЕНИЯ СВЕЧЕЙ ===
    // Реализуй по своему источнику, здесь просто заглушка
    private static List<TradingCore.Candle> tradingCoreCandles(String symbol) {
        return new ArrayList<>(); // сюда твой реальный источник свечей
    }
}