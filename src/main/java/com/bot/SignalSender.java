package com.bot;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import org.json.*;

import org.telegram.telegrambots.meta.api.objects.InputFile;
import org.telegram.telegrambots.bots.DefaultAbsSender;
import org.telegram.telegrambots.bots.DefaultBotOptions;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

public class SignalSender {

    private String telegramToken;
    private String chatId;

    public SignalSender() {
        telegramToken = System.getenv("TELEGRAM_TOKEN");
        chatId = System.getenv("CHAT_ID");

        if (telegramToken == null || chatId == null) {
            System.out.println("Переменные TELEGRAM_TOKEN и CHAT_ID не заданы!");
        }
    }

    // Получаем список всех монет с USDT
    public List<String> getAllUSDTCoins() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://api.binance.com/api/v3/ticker/price"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JSONArray jsonArray = new JSONArray(response.body());
        List<String> coins = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            String symbol = jsonArray.getJSONObject(i).getString("symbol");
            if (symbol.endsWith("USDT")) {
                coins.add(symbol);
            }
        }
        return coins;
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

    // Генерация графика через Python
    public void generateChart(String coin) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "python3",
                    "src/python-core/analysis.py",
                    coin
            );
            pb.redirectErrorStream(true);
            Process process = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Отправка текста и картинки в Telegram
    public void sendToTelegram(String messageText, String chartFile) {
        try {
            DefaultAbsSender bot = new DefaultAbsSender(new DefaultBotOptions()) {
                @Override
                public String getBotToken() {
                    return telegramToken;
                }
            };

            // Отправляем картинку
            if(chartFile != null) {
                SendPhoto photo = new SendPhoto();
                photo.setChatId(chatId);
                photo.setPhoto(new InputFile(new File(chartFile))); // <-- вот исправлено
                photo.setCaption(messageText);
                bot.execute(photo);
            } else {
                SendMessage message = new SendMessage();
                message.setChatId(chatId);
                message.setText(messageText);
                bot.execute(message);
            }
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }

    // Основной метод запуска анализа с фильтром порога и паузой
    public void start() {
        try {
            List<String> coins = getAllUSDTCoins();
            double threshold = Double.parseDouble(
                    System.getenv().getOrDefault("SIGNAL_THRESHOLD", "0.05")
            ); // порог уверенности 5% по умолчанию

            for (String coin : coins) {
                List<Double> prices = getPrices(coin, "1m", 20);
                double first = prices.get(0);
                double last = prices.get(prices.size() - 1);
                double confidence = Math.abs(last - first) / first;

                if (confidence >= threshold) {
                    String signal = analyzeCoin(coin, prices);
                    System.out.println(signal);

                    // Генерация графика через Python
                    generateChart(coin);
                    sendToTelegram(signal, coin + "_chart.png");
                }

                // Пауза между монетами (например 30 секунд)
                Thread.sleep(30_000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
