package com.bot;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.json.*;

public class SignalSender {

    private final TelegramBotSender bot;
    private final int topNCoins = 100;
    private final int windowCandles = 20; // последние 20 свечей
    private final int REQUEST_DELAY_MS = 350;
    private final HttpClient client;

    private final Set<String> STABLECOIN_KEYWORDS = new HashSet<>(Arrays.asList(
            "USDT","USDC","BUSD","TUSD","DAI","USDP","FRAX","UST","GUSD","USDD","FDUSD","USDE"
    ));

    public SignalSender(TelegramBotSender bot) {
        this.bot = bot;
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    // Получаем топ-N монет по объему, без стейблкоинов
    public List<String> getTopUSDTCoins() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.binance.com/api/v3/ticker/24hr"))
                    .timeout(Duration.ofSeconds(20))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            String body = response.body().trim();

            if (!body.startsWith("[")) {
                System.out.println("Unexpected response from Binance API: " + body);
                return Collections.emptyList();
            }

            JSONArray jsonArray = new JSONArray(body);
            List<JSONObject> usdtCoins = new ArrayList<>();

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                String symbol = obj.getString("symbol");
                if (!symbol.endsWith("USDT")) continue;
                boolean isStable = STABLECOIN_KEYWORDS.stream().anyMatch(symbol::startsWith);
                if (isStable) continue;
                usdtCoins.add(obj);
            }

            usdtCoins.sort((a, b) -> Double.compare(b.optDouble("quoteVolume", 0.0), a.optDouble("quoteVolume", 0.0)));

            List<String> topCoins = new ArrayList<>();
            for (int i = 0; i < Math.min(topNCoins, usdtCoins.size()); i++) {
                topCoins.add(usdtCoins.get(i).getString("symbol"));
            }

            return topCoins;

        } catch (Exception e) {
            System.out.println("Error fetching top coins: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    // Получаем цены закрытия
    public List<Double> getPrices(String coin, String interval, int limit) throws Exception {
        String url = String.format("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d", coin, interval, limit);
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(20)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONArray klines = new JSONArray(response.body());
        List<Double> prices = new ArrayList<>();
        for (int i = 0; i < klines.length(); i++) {
            JSONArray candle = klines.getJSONArray(i);
            prices.add(candle.getDouble(4)); // close price
        }
        return prices;
    }

    // RSI
    private double calculateRSI(List<Double> prices) {
        if (prices.size() < 2) return 50;
        double gain = 0, loss = 0;
        for (int i = 1; i < prices.size(); i++) {
            double diff = prices.get(i) - prices.get(i - 1);
            if (diff > 0) gain += diff;
            else loss -= diff;
        }
        if (gain + loss == 0) return 50;
        return 100 * gain / (gain + loss);
    }

    // EMA
    private double calculateEMA(List<Double> prices, int period) {
        if (prices.isEmpty()) return 0;
        double k = 2.0 / (period + 1);
        double ema = prices.get(0);
        for (int i = 1; i < prices.size(); i++) {
            ema = prices.get(i) * k + ema * (1 - k);
        }
        return ema;
    }

    // MACD (fast=12, slow=26)
    private double[] calculateMACD(List<Double> prices) {
        int size = prices.size();
        List<Double> last12 = prices.subList(Math.max(0, size - 12), size);
        List<Double> last26 = prices.subList(Math.max(0, size - 26), size);
        double ema12 = calculateEMA(last12, 12);
        double ema26 = calculateEMA(last26, 26);
        double macd = ema12 - ema26;
        double signal = macd; // упрощённо
        return new double[]{macd, signal};
    }

    // Генерация сигнала
    private Optional<String> generateSignal(String coin, List<Double> prices) {
        if (prices.size() < 2) return Optional.empty();

        double rsi = calculateRSI(prices);
        int size = prices.size();
        double emaShort = calculateEMA(prices.subList(Math.max(0, size - 10), size), 10);
        double emaLong = calculateEMA(prices.subList(Math.max(0, size - 20), size), 20);
        double[] macdArr = calculateMACD(prices);
        double macd = macdArr[0], signal = macdArr[1];

        double confidence = 0;

        if (rsi < 30) confidence += 0.3;
        if (rsi > 70) confidence += 0.3;
        if (emaShort > emaLong) confidence += 0.2;
        if (emaShort < emaLong) confidence += 0.2;
        if (macd > signal) confidence += 0.2;
        if (macd < signal) confidence += 0.2;

        String direction = null;
        if (rsi < 30 || (emaShort > emaLong && macd > signal)) direction = "LONG";
        else if (rsi > 70 || (emaShort < emaLong && macd < signal)) direction = "SHORT";

        if (direction != null && confidence >= 0.5) {
            return Optional.of(String.format("%s|%s|confidence=%.2f|RSI=%.1f|EMAshort=%.2f|EMAlong=%.2f|MACD=%.4f",
                    coin, direction, confidence, rsi, emaShort, emaLong, macd));
        }

        return Optional.empty();
    }

    // Запуск анализа
    public void start() {
        try {
            List<String> coins = getTopUSDTCoins();
            System.out.println("Analyzing top " + coins.size() + " coins asynchronously...");

            ExecutorService executor = Executors.newFixedThreadPool(10);
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (String coin : coins) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(REQUEST_DELAY_MS);
                        List<Double> prices = getPrices(coin, "5m", windowCandles);
                        Optional<String> signal = generateSignal(coin, prices);
                        signal.ifPresent(bot::sendSignal);
                        signal.ifPresent(s -> System.out.println("[SEND] " + s));
                    } catch (Exception e) {
                        System.out.println("Error analyzing " + coin + ": " + e.getMessage());
                    }
                }, executor);
                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            executor.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
