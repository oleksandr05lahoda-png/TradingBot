package com.bot;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BotMain {

    // ===== CONFIG =====
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

        TelegramBotSender telegram = new TelegramBotSender(TG_TOKEN, CHAT_ID);
        SignalSender signalSender = new SignalSender(telegram);
        GlobalImpulseController globalImpulse = new GlobalImpulseController();

        telegram.sendMessageAsync("🚀 Trading Bot запущен");
        LOGGER.info("Bot started at " + LocalDateTime.now());

        var scheduler = java.util.concurrent.Executors.newScheduledThreadPool(1, new BotThreadFactory());

        Runnable signalTask = () -> {
            try {
                LOGGER.info("=== SIGNAL SCAN START === " + LocalDateTime.now());

                // Получаем все сигналы
                List<DecisionEngineMerged.TradeIdea> rawSignals = signalSender.generateSignals();
                if (rawSignals == null || rawSignals.isEmpty()) {
                    LOGGER.info("No valid signals this cycle.");
                    return;
                }

                // Обновляем глобальный импульс для BTC
                List<TradingCore.Candle> btcCandles = signalSender.fetchKlines("BTCUSDT", "15m", KLINES_LIMIT);
                if (btcCandles != null && btcCandles.size() > 20) {
                    globalImpulse.update(btcCandles);
                }
                GlobalImpulseController.GlobalContext ctx = globalImpulse.getContext();
                LOGGER.info("BTC regime: " + ctx.regime +
                        " | strength=" + ctx.impulseStrength +
                        " | volExp=" + ctx.volatilityExpansion);
                // Мягкий фильтр глобального импульса (не блокирует сигналы полностью)
                List<DecisionEngineMerged.TradeIdea> filteredSignals = rawSignals.stream()
                        .filter(s -> {

                            // если сильный импульс BTC — немного ограничиваем противоположные сигналы
                            if (ctx.strongPressure) {

                                if (ctx.onlyLong && s.side == com.bot.TradingCore.Side.SHORT) {
                                    return s.probability >= 65; // разрешаем только сильные контртрендовые
                                }

                                if (ctx.onlyShort && s.side == com.bot.TradingCore.Side.LONG) {
                                    return s.probability >= 65;
                                }

                            }

                            return true;
                        })
                        .toList();

                if (filteredSignals.isEmpty()) {
                    LOGGER.info("No signals after applying global impulse filter.");
                    return;
                }

                for (DecisionEngineMerged.TradeIdea s : filteredSignals) {

                    telegram.sendMessageAsync(s.toString());
                }
                LOGGER.info("Signals sent: " + filteredSignals.size());

            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "CRITICAL ERROR in signal cycle", t);
                telegram.sendMessageAsync("⚠ Ошибка цикла: " + t);
            }
        };

        // Запуск сразу и каждые 15 минут
        scheduler.scheduleAtFixedRate(signalTask, 0, SIGNAL_INTERVAL_MIN, java.util.concurrent.TimeUnit.MINUTES);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down bot...");
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
            t.setUncaughtExceptionHandler((thread, ex) -> {
                LOGGER.log(Level.SEVERE, "UNCAUGHT ERROR in " + thread.getName(), ex);
            });
            return t;
        }
    }

    // ======================= UTC → локальное время =======================
    public static String formatLocalTime(long utcMillis) {
        return Instant.ofEpochMilli(utcMillis)
                .atZone(ZONE)
                .format(DateTimeFormatter.ofPattern("HH:mm"));
    }
}