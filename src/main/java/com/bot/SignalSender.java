package com.bot;

import java.io.File;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import org.json.*;
import org.telegram.telegrambots.bots.DefaultAbsSender;
import org.telegram.telegrambots.bots.DefaultBotOptions;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import org.knowm.xchart.*;
import org.knowm.xchart.BitmapEncoder.BitmapFormat;

public class SignalSender {

    private final String telegramToken;
    private final String chatId;
    private final int topNCoins = 100; // топ-N монет для анализа

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

        usdtCoins.sort((a, b) -> Double.compare(b.getDouble("quoteVolume"), a.getDouble("quoteVolume")));

        List<String> topCoins = new ArrayList<>();
        for (int i = 0; i < Math.min(limit, usdtCoins.size()); i++) {
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

    // Генерация графика через XChart
    public String generateChart(String coin, List<Double> prices) {
        try {
            XYChart chart = new XYChartBuilder()
                    .width(600)
                    .height(400)
                    .title(coin.replace("USDT", ""))
                    .xAxisTitle("Candles")
                    .yAxisTitle("Price")
                    .build();

            List<Integer> xData = new ArrayList<>();
            for (int i = 0; i < prices.size(); i++) xData.add(i);

            chart.addSeries(coin.replace("USDT", ""), xData, prices);

            String fileName = coin + "_chart.png";
            BitmapEncoder.saveBitmap(chart, fileName, BitmapFormat.PNG);
            return fileName;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Отправка графика и текста в Telegram
    public void sendToTelegram(String messageText, String chartFile) {
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

            SendPhoto photo = new SendPhoto();
            photo.setChatId(chatId);
            photo.setPhoto(new org.telegram.telegrambots.meta.api.objects.InputFile(new File(chartFile)));
            photo.setCaption(messageText);
            bot.execute(photo);

        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }

    // Анализ монеты и генерация сигнала
    public String analyzeCoin(String coin, List<Double> prices) {
        double first = prices.get(0);
        double last = prices.get(prices.size() - 1);
        String signal = last > first ? "LONG" : "SHORT";
        double confidence = Math.abs(last - first) / first;
        return String.format("%s: %s, confidence: %.2f%%", coin.replace("USDT", ""), signal, confidence * 100);
    }

    public void start() {
        try {
            double threshold = Double.parseDouble(
                    System.getenv().getOrDefault("SIGNAL_THRESHOLD", "0.05")
            );

            List<String> coins = getTopUSDTCoins(topNCoins);

            for (String coin : coins) {
                List<Double> prices = getPrices(coin, "5m", 20); // 5 минутный таймфрейм
                double first = prices.get(0);
                double last = prices.get(prices.size() - 1);
                double confidence = Math.abs(last - first) / first;

                if (confidence >= threshold) {
                    String signal = analyzeCoin(coin, prices);
                    String chartFile = generateChart(coin, prices);
                    System.out.println(signal);
                    sendToTelegram(signal, chartFile);
                }

                Thread.sleep(5000); // пауза между проверкой монет
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
