package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BotMain {

    // ===== CONFIG =====
    private static final Logger LOGGER = Logger.getLogger(BotMain.class.getName());
    private static final String TG_TOKEN = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID = "953233853";
    private static final ZoneId ZONE = ZoneId.systemDefault();
    private static final int SIGNAL_INTERVAL_MIN = 15;
    private static final int KLINES_LIMIT = 200; // например, 100 свечей
    public static void main(String[] args) {

        if (TG_TOKEN == null || TG_TOKEN.isBlank()) {
            LOGGER.severe("TELEGRAM_TOKEN not set!");
            return;
        }

        TelegramBotSender telegram = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        SignalSender signalSender = new SignalSender(telegram);
        GlobalImpulseController globalImpulse = new GlobalImpulseController();

        telegram.sendMessageAsync("🚀 Trading Bot запущен");
        LOGGER.info("Bot started at " + LocalDateTime.now());

        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(2, new BotThreadFactory());

        Runnable signalTask = () -> {
            try {
                LOGGER.info("=== SIGNAL SCAN START === " + LocalDateTime.now());

                // 1️⃣ Получаем все сигналы
                List<DecisionEngineMerged.TradeIdea> rawSignals = signalSender.generateSignals();

                if (rawSignals == null || rawSignals.isEmpty()) {
                    LOGGER.info("No valid signals this cycle.");
                    return;
                }

                // 2️⃣ Обновляем глобальный импульс для BTC
                List<TradingCore.Candle> btcCandles = signalSender.fetchKlines("BTCUSDT", "15m", KLINES_LIMIT);
                if (btcCandles != null && btcCandles.size() > 20) {
                    globalImpulse.update(btcCandles);
                }
                GlobalImpulseController.GlobalContext ctx = globalImpulse.getContext();

                // 3️⃣ Фильтруем сигналы через глобальный импульс
                List<DecisionEngineMerged.TradeIdea> filteredSignals = rawSignals.stream()
                        .filter(s -> {
                            if (ctx.onlyLong && !s.symbol.equals("BTCUSDT"))
                                return s.side.equals("LONG");

                            if (ctx.onlyShort && !s.symbol.equals("BTCUSDT"))
                                return s.side.equals("SHORT");
                            return true; // NEUTRAL – все сигналы
                        })
                        .toList();

                if (filteredSignals.isEmpty()) {
                    LOGGER.info("No signals after applying global impulse filter.");
                    return;
                }

                // 4️⃣ Отправляем в Telegram
                for (DecisionEngineMerged.TradeIdea s : filteredSignals) {
                    telegram.sendMessageAsync(formatSignal(s));
                }

                LOGGER.info("Signals sent: " + filteredSignals.size());

            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "CRITICAL ERROR in signal cycle", t);
                telegram.sendMessageAsync("⚠ Ошибка цикла: " + t.getMessage());
            }
        };

        // Запуск сразу + каждые 15 минут
        scheduler.scheduleAtFixedRate(signalTask, 0, SIGNAL_INTERVAL_MIN, TimeUnit.MINUTES);

        // Graceful shutdown
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

    /**
     * Формат сигнала для Telegram
     */
    private static String formatSignal(DecisionEngineMerged.TradeIdea s) {
        String flags = s.flags != null && !s.flags.isEmpty()
                ? String.join(", ", s.flags)
                : "—";

        double probabilityPercent = s.probability;

        return String.format(
                "*%s* → *%s*\n" +
                        "Price: %.6f\n" +
                        "Probability: %.0f%%\n" +
                        "Stop-Take: %.6f - %.6f\n" +
                        "Flags: %s\n" +
                        "_time: %s_",
                s.symbol,
                s.side,
                s.price,
                probabilityPercent,
                s.stop,
                s.take,
                flags,
                LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))
        );
    }

    /**
     * Кастомный ThreadFactory с защитой от смерти потока
     */
    static class BotThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("SignalSchedulerThread");
            t.setDaemon(false);
            t.setUncaughtExceptionHandler((thread, ex) -> {
                LOGGER.log(Level.SEVERE, "UNCAUGHT ERROR in " + thread.getName(), ex);
            });
            return t;
        }
    }

    /**
     * UTC → локальное
     */
    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis)
                .atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }
}