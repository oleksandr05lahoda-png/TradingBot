package com.bot;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.DecimalFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;

public final class BotMain {
    private static final Logger LOGGER = Logger.getLogger(BotMain.class.getName());

    private static final String TG_TOKEN = System.getenv("TELEGRAM_TOKEN");
    private static final String CHAT_ID = "953233853";
    private static final ZoneId ZONE = ZoneId.systemDefault();
    private static final int SIGNAL_INTERVAL_MIN = 15;

    // Список монет для сканирования
    private static final List<String> SYMBOLS = List.of("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT");

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

                // ===== Получаем свечи BTC =====
                List<TradingCore.Candle> candlesBTC = tradingCoreCandles("BTCUSDT");
                if (candlesBTC.isEmpty()) {
                    LOGGER.warning("BTC candles not available. Skipping cycle.");
                    return;
                }

                // ===== Обновляем глобальный импульс BTC =====
                globalImpulse.update(candlesBTC);
                GlobalImpulseController.GlobalContext context = globalImpulse.getContext();

                // ===== Проходимся по всем монетам =====
                for (String symbol : SYMBOLS) {
                    List<TradingCore.Candle> candles = tradingCoreCandles(symbol);
                    if (candles.isEmpty()) continue;

                    DecisionEngineMerged.CoinCategory cat = symbol.equals("BTCUSDT") ?
                            DecisionEngineMerged.CoinCategory.TOP :
                            DecisionEngineMerged.CoinCategory.ALT;

                    DecisionEngineMerged.TradeIdea signal = decisionEngine.analyze(
                            symbol,
                            candles,
                            candles,
                            candles,
                            candlesBTC, // Глобальный 1H для всех монет — BTC
                            cat,
                            context
                    );

                    if (signal != null) {
                        telegram.sendMessageAsync(formatSignal(signal));
                        LOGGER.info("Signal sent for " + symbol);
                    }
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
                "*%s* → *%s*\nPrice: %s\nProbability: %.0f%%\nStop-Take: %s - %s\nFlags: %s\n_time: %s_",
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

    private static List<TradingCore.Candle> tradingCoreCandles(String symbol) {
        List<TradingCore.Candle> candles = new ArrayList<>();
        try {
            HttpClient client = HttpClient.newHttpClient();
            String url = String.format(
                    "https://api.binance.com/api/v3/klines?symbol=%s&interval=15m&limit=200",
                    symbol
            );
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JSONArray arr = new JSONArray(response.body());
                for (int i = 0; i < arr.length(); i++) {
                    JSONArray c = arr.getJSONArray(i);
                    long openTime = c.getLong(0);
                    double open = c.getDouble(1);
                    double high = c.getDouble(2);
                    double low = c.getDouble(3);
                    double close = c.getDouble(4);
                    double volume = c.getDouble(5);
                    double quoteVolume = c.getDouble(7);
                    long closeTime = c.getLong(6);
                    candles.add(new TradingCore.Candle(openTime, open, high, low, close, volume, quoteVolume, closeTime));
                }
            } else {
                LOGGER.warning("Binance API returned " + response.statusCode());
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error fetching candles: " + e.getMessage(), e);
        }
        return candles;
    }
}