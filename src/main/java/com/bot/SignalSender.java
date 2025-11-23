package com.bot;

import java.net.URI;
import java.net.http.*;
import java.util.*;
import org.json.*;

public class SignalSender {

    private final String telegramToken;
    private final String chatId;
    private final int topNCoins = 50; // количество монет для анализа

    public SignalSender() {
        telegramToken = System.getenv("TELEGRAM_TOKEN");
        chatId = System.getenv("CHAT_ID");

        if (telegramToken == null || chatId == null) {
            System.out.println("Переменные TELEGRAM_TOKEN и CHAT_ID не заданы!");
        }
    }

    // Получаем топ-N монет по объему
    public List<String> getTopUSDTCoins() throws Exception {
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

        usdtCoins.sort((a, b) -> Double.compare(b.getDouble("quoteVolume"), a.getDouble("quoteVolume")));

        List<String> topCoins = new ArrayList<>();
        for (int i = 0; i < Math.min(topNCoins, usdtCoins.size()); i++) {
            topCoins.add(usdtCoins.get(i).getString("symbol"));
        }
        return topCoins;
    }

    // Получаем цены закрытия
    public List<Double> getPrices(String coin, String interval, int limit) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String url = String.format(
                "https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d",
                coin, interval, limit
        );
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JSONArray klines = new JSONArray(response.body());
        List<Double> prices = new ArrayList<>();
        for (int i = 0; i < klines.length(); i++) {
            JSONArray candle = klines.getJSONArray(i);
            prices.add(candle.getDouble(4)); // close price
        }
        return prices;
    }

    // Отправка сигнала в Telegram
    public void sendToTelegram(String messageText) {
        if (telegramToken == null || chatId == null) {
            System.out.println("[LOG] " + messageText);
            return;
        }

        try {
            String url = String.format(
                    "https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s",
                    telegramToken, chatId, java.net.URLEncoder.encode(messageText, "UTF-8")
            );
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .build();
            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Анализ монеты и генерация сигнала
    public String analyzeCoin(String coin, List<Double> prices) {
        double first = prices.get(0);
        double last = prices.get(prices.size() - 1);
        String signal = last > first ? "LONG" : "SHORT";
        double confidence = Math.abs(last - first) / first;
        return String.format("%s|%s|%.2f", coin, signal, confidence);
    }

    public void start() {
        try {
            List<String> coins = getTopUSDTCoins();
            for (String coin : coins) {
                List<Double> prices = getPrices(coin, "5m", 20); // 5-минутные свечи
                String signal = analyzeCoin(coin, prices);
                sendToTelegram(signal);
                System.out.println("[LOG] " + signal);
                Thread.sleep(3000); // пауза между монетами
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
