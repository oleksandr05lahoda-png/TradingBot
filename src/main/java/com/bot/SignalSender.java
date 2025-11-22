package com.bot;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import org.json.*;
import org.telegram.telegrambots.bots.DefaultAbsSender;
import org.telegram.telegrambots.bots.DefaultBotOptions;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

public class SignalSender {

    private final String telegramToken;
    private final String chatId;
    private final int topNCoins = 100; // анализируем только топ-100 по объему

    public SignalSender() {
        telegramToken = System.getenv("TELEGRAM_TOKEN");
        chatId = System.getenv("CHAT_ID");

        if (telegramToken == null || chatId == null) {
            System.out.println("Переменные TELEGRAM_TOKEN и CHAT_ID не заданы!");
        }
    }

    // Получаем топ-N монет по объему
    public List<String> getTopUSDTCoins(int limit) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://api.binance.com/api/v3/ticker/24hr"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JSONArray jsonArray = new JSONArray(response.body());
        List<JSONObject> usdtCoins = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            if (obj.getString("symbol").endsWith("USDT")) {
                usdtCoins.add(obj);
            }
        }

        // Сортируем по объему (volume)
        usdtCoins.sort((a, b) -> Double.compare(b.getDouble("quoteVolume"), a.getDouble("quoteVolume")));

        List<String> topCoins = new ArrayList<>();
        for (int i = 0; i < Math.min(limit, usdtCoins.size()); i++) {
            topCoins.add(usdtCoins.get(i).getString("symbol"));
        }
        return topCoins;
    }

    // Получаем последние закрытые цены
    public List<Double> getPrices(String coin, String interval, int limit) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String url = String.format(
                "https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d",
                coin, interval, limit
        );
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JSONArray klines = new JSONArray(response.body());
        List<Double> prices = new ArrayList<>();
        for (int i = 0; i < klines.length(); i++) {
            JSONArray candle = klines.getJSONArray(i);
            prices.add(candle.getDouble(4)); // close price
        }
        return prices;
    }

    // Анализируем сигнал для монеты
    public String analyzeCoin(String coin, List<Double> prices) {
        double first = prices.get(0);
        double last = prices.get(prices.size() - 1);
        String signal = last > first ? "LONG" : "SHORT";
        double confidence = Math.abs(last - first) / first;

        return String.format("%s: %s, confidence: %.2f%%", coin, signal, confidence * 100);
    }

    // Отправка текста в Telegram
    public void sendToTelegram(String messageText) {
        if (telegramToken == null || chatId == null) {
            System.out.println("[LOG] " + messageText);
            return;
        }

        try {
            DefaultAbsSender bot = new DefaultAbsSender(new DefaultBotOptions()) {
                @Override
                public String getBotToken() {
                    return telegramToken;
                }
            };

            SendMessage message = new SendMessage();
            message.setChatId(chatId);
            message.setText(messageText);
            bot.execute(message);

        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }

    // Основной метод запуска анализа с фильтром порога и небольшой паузой
    public void start() {
        try {
            double threshold = Double.parseDouble(
                    System.getenv().getOrDefault("SIGNAL_THRESHOLD", "0.05")
            ); // порог уверенности 5% по умолчанию

            List<String> coins = getTopUSDTCoins(topNCoins);

            for (String coin : coins) {
                List<Double> prices = getPrices(coin, "1m", 20);
                double first = prices.get(0);
                double last = prices.get(prices.size() - 1);
                double confidence = Math.abs(last - first) / first;

                if (confidence >= threshold) {
                    String signal = analyzeCoin(coin, prices);
                    System.out.println(signal);
                    sendToTelegram(signal);
                }

                // Пауза между проверкой монет (например 5 секунд)
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
