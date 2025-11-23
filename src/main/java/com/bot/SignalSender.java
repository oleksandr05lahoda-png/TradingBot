package com.bot;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import org.json.*;

/**
 * Улучшенный анализатор сигналов.
 * - топN = 100
 * - windowCandles = 200
 * - исключение стейблкоинов
 * - расчет сигнала: percentChange и normalizedMomentum = percentChange / volatility
 * - отправка только если abs(normalizedMomentum) > SIGNAL_THRESHOLD
 */
public class SignalSender {

    private final String telegramToken;
    private final String chatId;
    private final int topNCoins = 100;          // анализируем топ-100 по объему
    private final int windowCandles = 200;      // сколько свечей брать для анализа
    private final double SIGNAL_THRESHOLD = 0.8; // порог нормализованного импульса (tune this)
    private final int REQUEST_DELAY_MS = 350;   // задержка между запросами к Binance (без фанатизма)
    private final HttpClient client;

    // список префиксов/символов стейблкоинов или тех, что не нужны
    private final Set<String> STABLECOIN_KEYWORDS = new HashSet<>(Arrays.asList(
            "USDT","USDC","BUSD","TUSD","DAI","USDP","FRAX","UST","GUSD","USDD","FDUSD","USDE"
    ));

    public SignalSender() {
        telegramToken = System.getenv("TELEGRAM_TOKEN");
        chatId = System.getenv("CHAT_ID");
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        if (telegramToken == null || chatId == null) {
            System.out.println("Переменные TELEGRAM_TOKEN и CHAT_ID не заданы! Бот будет только логировать в консоль.");
        }
    }

    // Получаем топ-N монет по объему (24h quoteVolume)
    public List<String> getTopUSDTCoins() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://api.binance.com/api/v3/ticker/24hr"))
                .timeout(Duration.ofSeconds(20))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONArray jsonArray = new JSONArray(response.body());

        List<JSONObject> usdtCoins = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            String symbol = obj.getString("symbol");
            if (!symbol.endsWith("USDT")) continue;
            // фильтр стейблкоинов по вхождению токена в начало (USDCUSDT и т.п.)
            boolean isStable = STABLECOIN_KEYWORDS.stream().anyMatch(symbol::startsWith);
            if (isStable) continue;

            usdtCoins.add(obj);
        }

        usdtCoins.sort((a, b) -> {
            double va = a.optDouble("quoteVolume", 0.0);
            double vb = b.optDouble("quoteVolume", 0.0);
            return Double.compare(vb, va);
        });

        List<String> topCoins = new ArrayList<>();
        for (int i = 0; i < Math.min(topNCoins, usdtCoins.size()); i++) {
            topCoins.add(usdtCoins.get(i).getString("symbol"));
        }
        return topCoins;
    }

    // Получаем цены закрытия
    public List<Double> getPrices(String coin, String interval, int limit) throws Exception {
        String url = String.format(
                "https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d",
                coin, interval, limit
        );
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .GET()
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

    // Простая статистика: среднее и стандартное отклонение
    private static double mean(List<Double> arr) {
        double s = 0;
        for (double v : arr) s += v;
        return s / arr.size();
    }
    private static double std(List<Double> arr, double mean) {
        double s = 0;
        for (double v : arr) {
            double d = v - mean;
            s += d * d;
        }
        return Math.sqrt(s / arr.size());
    }

    // Анализ: возвращает null если сигнал слабый / не отправлять.
    // Формула: percentChange = (last-first)/first
    // volatility = std of returns (close[i]/close[i-1]-1)
    // normalizedMomentum = percentChange / volatility
    public String analyzeCoinAndGetSignal(String coin, List<Double> prices) {
        if (prices == null || prices.size() < 10) return null;

        double first = prices.get(0);
        double last = prices.get(prices.size() - 1);
        double percentChange = (last - first) / first; // e.g. 0.02 -> 2%

        // вычисляем волатильность по доходностям
        List<Double> returns = new ArrayList<>();
        for (int i = 1; i < prices.size(); i++) {
            double r = (prices.get(i) / prices.get(i-1)) - 1.0;
            returns.add(r);
        }
        double mu = mean(returns);
        double sigma = std(returns, mu);
        if (sigma == 0) sigma = 1e-9; // защита от деления на ноль

        double normalizedMomentum = percentChange / sigma;

        String direction = percentChange > 0 ? "LONG" : "SHORT";
        // Отправляем только сильные сигналы
        if (Math.abs(normalizedMomentum) < SIGNAL_THRESHOLD) {
            // логируем, почему не отправили
            System.out.printf("[SKIP] %s %s change=%.4f norm=%.3f vol=%.6f%n", coin, direction, percentChange, normalizedMomentum, sigma);
            return null;
        }

        String formatted = String.format("%s|%s|change=%.4f|norm=%.3f|vol=%.6f",
                coin, direction, percentChange, normalizedMomentum, sigma);
        return formatted;
    }

    // Отправка в Telegram (или лог)
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
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Запуск анализа всех монет
    public void start() {
        try {
            List<String> coins = getTopUSDTCoins();
            System.out.println("Analyzing top " + coins.size() + " coins...");
            for (String coin : coins) {
                try {
                    // не стучим слишком часто
                    Thread.sleep(REQUEST_DELAY_MS);

                    List<Double> prices = getPrices(coin, "5m", windowCandles);
                    String signal = analyzeCoinAndGetSignal(coin, prices);
                    if (signal != null) {
                        sendToTelegram(signal);
                        System.out.println("[SEND] " + signal);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.out.println("Error analyzing " + coin + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
