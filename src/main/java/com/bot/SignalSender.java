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
    private final int windowCandles = 200;
    private final int REQUEST_DELAY_MS = 350;
    private final HttpClient client;

    private final Set<String> STABLECOIN_KEYWORDS = new HashSet<>(Arrays.asList(
            "USDT","USDC","BUSD","TUSD","DAI","USDP","FRAX","UST","GUSD","USDD","FDUSD","USDE"
    ));

    // новые фильтры для сильных сигналов
    private final double MIN_PERCENT_CHANGE = 0.05;      // минимум 5% движения
    private final double MIN_NORMALIZED_MOMENTUM = 5.0;  // только уверенные сигналы

    public SignalSender(TelegramBotSender bot) {
        this.bot = bot;
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    public List<String> getTopUSDTCoins() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.binance.com/api/v3/ticker/24hr"))
                    .timeout(Duration.ofSeconds(20))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            String body = response.body().trim();

            // Проверяем, что ответ начинается с '[' → JSON-массив
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

            System.out.println("After filtering: " + topCoins.size() + " coins");
            return topCoins;

        } catch (Exception e) {
            System.out.println("Error fetching top coins: " + e.getMessage());
            return Collections.emptyList();
        }
    }


    public List<Double> getPrices(String coin, String interval, int limit) throws Exception {
        String url = String.format("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d", coin, interval, limit);
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(20)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONArray klines = new JSONArray(response.body());
        List<Double> prices = new ArrayList<>();
        for (int i = 0; i < klines.length(); i++) {
            JSONArray candle = klines.getJSONArray(i);
            prices.add(candle.getDouble(4));
        }
        return prices;
    }

    private static double mean(List<Double> arr) {
        return arr.stream().mapToDouble(d -> d).average().orElse(0.0);
    }

    private static double std(List<Double> arr, double mean) {
        double sum = 0;
        for (double v : arr) {
            double d = v - mean;
            sum += d * d;
        }
        return Math.sqrt(sum / arr.size());
    }

    public String analyzeCoinAndGetSignal(String coin, List<Double> prices) {
        if (prices == null || prices.size() < 10) return null;

        double first = prices.get(0);
        double last = prices.get(prices.size() - 1);
        double percentChange = (last - first) / first;

        List<Double> returns = new ArrayList<>();
        for (int i = 1; i < prices.size(); i++) {
            returns.add((prices.get(i) / prices.get(i - 1)) - 1.0);
        }

        double mu = mean(returns);
        double sigma = std(returns, mu);
        if (sigma == 0) sigma = 1e-9;

        double normalizedMomentum = percentChange / sigma;
        String direction = percentChange > 0 ? "LONG" : "SHORT";

        // фильтр сильных сигналов
        if (Math.abs(percentChange) < MIN_PERCENT_CHANGE || Math.abs(normalizedMomentum) < MIN_NORMALIZED_MOMENTUM) {
            System.out.printf("[SKIP] %s %s change=%.4f norm=%.3f vol=%.6f%n", coin, direction, percentChange, normalizedMomentum, sigma);
            return null;
        }

        return String.format("%s|%s|change=%.4f|norm=%.3f|vol=%.6f", coin, direction, percentChange, normalizedMomentum, sigma);
    }

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
                        String signal = analyzeCoinAndGetSignal(coin, prices);
                        if (signal != null) {
                            bot.sendSignal(signal);
                            System.out.println("[SEND] " + signal);
                        }
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
