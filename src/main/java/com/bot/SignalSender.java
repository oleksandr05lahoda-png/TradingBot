package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class SignalSender {

    private final TelegramBotSender bot;
    private final HttpClient http;
    private final Set<String> STABLE = Set.of("USDT");

    private final int TOP_N;
    private final double MIN_CONF;
    private final int INTERVAL_MIN;
    private final int KLINES_LIMIT;
    private final long REQUEST_DELAY_MS;

    private Set<String> BINANCE_PAIRS = new HashSet<>();
    private long lastBinancePairsRefresh = 0L; // millis
    private final long BINANCE_REFRESH_INTERVAL_MS = 60 * 60 * 1000L; // 60 min

    // track last candle time per symbol
    private final Map<String, Long> lastOpenTimeMap = new ConcurrentHashMap<>();

    public SignalSender(TelegramBotSender bot) {
        this.bot = bot;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        this.TOP_N = Integer.parseInt(System.getenv().getOrDefault("TOP_N", "100"));
        this.MIN_CONF = Double.parseDouble(System.getenv().getOrDefault("MIN_CONFIDENCE", "0.5"));
        this.INTERVAL_MIN = Integer.parseInt(System.getenv().getOrDefault("INTERVAL_MINUTES", "15"));
        this.KLINES_LIMIT = Integer.parseInt(System.getenv().getOrDefault("KLINES", "40"));
        this.REQUEST_DELAY_MS = Long.parseLong(System.getenv().getOrDefault("REQUEST_DELAY_MS", "200"));
    }

    // --- helper network methods unchanged (getBinanceSymbols, getTopSymbols, fetchClosesWithMeta) ---
    public Set<String> getBinanceSymbols() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.binance.com/api/v3/exchangeInfo"))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

            JSONObject json = new JSONObject(resp.body());
            JSONArray arr = json.getJSONArray("symbols");

            Set<String> result = new HashSet<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject s = arr.getJSONObject(i);
                String symbol = s.getString("symbol");
                String status = s.optString("status", "TRADING");
                if ("TRADING".equalsIgnoreCase(status) && symbol.endsWith("USDT")) {
                    result.add(symbol);
                }
            }
            System.out.println("[Binance] Loaded " + result.size() + " spot USDT pairs");
            return result;

        } catch (Exception e) {
            System.out.println("[Binance] Could NOT load pairs: " + e.getMessage());
            return Set.of("BTCUSDT","ETHUSDT","BNBUSDT");
        }
    }

    private void ensureBinancePairsFresh() {
        long now = System.currentTimeMillis();
        if (BINANCE_PAIRS.isEmpty() || (now - lastBinancePairsRefresh) > BINANCE_REFRESH_INTERVAL_MS) {
            BINANCE_PAIRS = getBinanceSymbols();
            lastBinancePairsRefresh = now;
        }
    }

    public List<String> getTopSymbols(int limit) {
        try {
            String url = String.format(
                    "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=%d&page=1",
                    limit
            );

            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            JSONArray arr = new JSONArray(resp.body());

            List<String> list = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject c = arr.getJSONObject(i);
                String sym = c.getString("symbol").toUpperCase();
                if (STABLE.contains(sym)) continue;
                list.add(sym + "USDT");
            }
            return list;

        } catch (Exception e) {
            System.out.println("[CoinGecko] Error: " + e.getMessage());
            return List.of("BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","ADAUSDT");
        }
    }

    public static class KlinesResult {
        public final List<Double> closes;
        public final long lastOpenTime;

        public KlinesResult(List<Double> closes, long lastOpenTime) {
            this.closes = closes;
            this.lastOpenTime = lastOpenTime;
        }
    }

    public KlinesResult fetchClosesWithMeta(String symbol) {
        try {
            Thread.sleep(REQUEST_DELAY_MS);
            String url = String.format(
                    "https://api.binance.com/api/v3/klines?symbol=%s&interval=5m&limit=%d",
                    symbol, KLINES_LIMIT
            );

            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            String body = resp.body();

            if (body == null || body.isEmpty() || !body.startsWith("[")) {
                System.out.println("[Binance] Invalid response for " + symbol + ": " + body);
                return new KlinesResult(Collections.emptyList(), 0L);
            }

            JSONArray arr = new JSONArray(body);
            List<Double> closes = new ArrayList<>();
            long lastOpen = 0L;
            for (int i = 0; i < arr.length(); i++) {
                JSONArray k = arr.getJSONArray(i);
                long openTime = k.getLong(0);
                double close = k.getDouble(4);
                closes.add(close);
                lastOpen = openTime;
            }

            return new KlinesResult(closes, lastOpen);

        } catch (Exception e) {
            System.out.println("[Binance] Error for " + symbol + ": " + e.getMessage());
            return new KlinesResult(Collections.emptyList(), 0L);
        }
    }

    // --- indicators (unchanged logic, but we'll normalize their outputs) ---
    public static double ema(List<Double> prices, int period) {
        double k = 2.0 / (period + 1);
        double ema = prices.get(0);
        for (double p : prices) ema = p*k + ema*(1-k);
        return ema;
    }

    public static double rsi(List<Double> prices, int period) {
        if (prices.size() <= period) return 50.0;
        double gain = 0, loss = 0;
        for (int i = prices.size() - period; i < prices.size(); i++) {
            double diff = prices.get(i) - prices.get(i-1);
            if (diff > 0) gain += diff; else loss -= diff;
        }
        if (gain + loss == 0) return 50.0;
        double rs = gain / (loss + 1e-12);
        return 100.0 - (100.0 / (1.0 + rs));
    }

    public static double macdHist(List<Double> prices) {
        return ema(prices, 12) - ema(prices, 26);
    }

    public static double momentum(List<Double> prices, int n) {
        if (prices.size() <= n) return 0.0;
        return (prices.get(prices.size()-1) - prices.get(prices.size()-1-n)) /
                (prices.get(prices.size()-1-n) + 1e-12);
    }

    // --- NEW: normalize helpers ---
    // map EMA crossover output (+0.6/-0.6) -> [-1, +1]
    private double normalizeEmaScore(double rawEma) {
        // rawEma expected ±0.6 from old function; divide by 0.6
        return Math.max(-1.0, Math.min(1.0, rawEma / 0.6));
    }

    // map RSI mean reversion (+0.7 / -0.7) -> [-1, +1] using distance from neutral (50)
    private double normalizeRsiScore(double rsiVal) {
        // rsiVal in [0,100]. We treat extremes as strong signals:
        // if rsi <= 10 -> +1 (oversold), if rsi >= 90 -> -1 (overbought)
        double oversold = Math.max(0, 50 - rsiVal);  // positive when rsi < 50
        double overbought = Math.max(0, rsiVal - 50); // positive when rsi > 50
        // normalize to [-1,1] with a sensible scale (50 points -> 1.0)
        double score = 0.0;
        if (rsiVal < 50) score = Math.min(1.0, oversold / 40.0);   // 40 -> ~1.0 sensitivity
        else score = -Math.min(1.0, overbought / 40.0);
        return score;
    }

    // map MACD histogram to [-1,1] by dividing by recent price scale
    private double normalizeMacdScore(double rawMacd, List<Double> closes) {
        // rawMacd is difference of EMAs; normalize by recent price (close)
        double last = closes.get(closes.size()-1);
        if (last <= 0) return 0.0;
        double rel = rawMacd / last; // relative MACD
        // choose a sensible scaling: rel of 0.01 -> fairly strong (1%)
        return Math.max(-1.0, Math.min(1.0, rel / 0.01));
    }

    // normalize momentum (raw small) to [-1,1]
    private double normalizeMomentumScore(double rawMomentum) {
        // rawMomentum roughly (p_now - p_n)/p_n
        // take 1% movement as strong (0.01 -> 1.0)
        return Math.max(-1.0, Math.min(1.0, rawMomentum / 0.01));
    }

    // --- strategy building blocks updated to return normalized scores in [-1,1] ---
    public double strategyEMACrossoverNorm(List<Double> closes) {
        int look = Math.min(closes.size(), 30);
        List<Double> slice = closes.subList(closes.size()-look, closes.size());
        double e9 = ema(slice,9);
        double e21 = ema(slice,21);
        double raw = e9 > e21 ? +0.6 : -0.6; // same as before
        return normalizeEmaScore(raw);
    }

    public double strategyRSINorm(List<Double> closes) {
        double r = rsi(closes,14);
        return normalizeRsiScore(r);
    }

    public double strategyMACDNorm(List<Double> closes) {
        double raw = macdHist(closes);
        return normalizeMacdScore(raw, closes);
    }

    public double strategyMomentumNorm(List<Double> closes) {
        double raw = momentum(closes,3);
        return normalizeMomentumScore(raw);
    }

    // --- NEW evaluate: combines normalized scores with weights that sum to 1.0 ---
    public Optional<Signal> evaluate(String pair, List<Double> closes) {
        if (closes == null || closes.size() < 22) return Optional.empty(); // нужно больше данных

        // --- получаем сигналы индикаторов ---
        double emaScore = strategyEMACrossoverNorm(closes);   // -1..1
        double rsiScore = strategyRSINorm(closes);            // -1..1
        double macdScore = strategyMACDNorm(closes);          // -1..1
        double momScore = strategyMomentumNorm(closes);       // -1..1

        // --- веса индикаторов ---
        double wEma = 0.25, wRsi = 0.25, wMacd = 0.25, wMom = 0.25;

        // --- итоговый rawScore в диапазоне [-1,1] ---
        double rawScore = emaScore * wEma + rsiScore * wRsi + macdScore * wMacd + momScore * wMom;

        // --- сила сигнала (0..1) ---
        double confidence = Math.min(1.0, Math.abs(rawScore));

        // --- направление (ЗДЕСЬ ДОБАВЛЕН ШОРТ) ---
        String direction;
        if (rawScore >= MIN_CONF) {
            direction = "LONG";
        } else if (rawScore <= -MIN_CONF) {
            direction = "SHORT";
        } else {
            return Optional.empty(); // слишком слабый сигнал
        }

        // логирование
        System.out.println(String.format(
                "[Eval] %s -> raw=%.3f conf=%.2f DIR=%s (EMA=%.2f RSI=%.2f MACD=%.2f MOM=%.2f)",
                pair, rawScore, confidence, direction, emaScore, rsiScore, macdScore, momScore
        ));

        String symbolOnly = pair.replace("USDT", "");
        double lastPrice = closes.get(closes.size()-1);
        double rsiVal = rsi(closes, 14);

        return Optional.of(new Signal(symbolOnly, direction, confidence, lastPrice, rsiVal, rawScore));
    }


    public static class Signal {
        public final String symbol;
        public final String direction;
        public final double confidence;
        public final double price;
        public final double rsi;
        public final double rawScore;

        public Signal(String symbol, String direction, double confidence, double price, double rsi, double rawScore){
            this.symbol=symbol; this.direction=direction; this.confidence=confidence;
            this.price=price; this.rsi=rsi; this.rawScore=rawScore;
        }

        public String toTelegramMessage(){
            return String.format("*%s* → *%s*\nConfidence: *%.2f*\nPrice: %.8f\nRSI(14): %.2f\n_time: %s_",
                    symbol,direction,confidence,price,rsi,Instant.now().toString());
        }
    }

    public void start() {

        System.out.println("[SignalSender] Starting TOP_N="+TOP_N+" MIN_CONF="+MIN_CONF+" INTERVAL_MIN="+INTERVAL_MIN);

        ensureBinancePairsFresh();

        try { bot.sendSignal("✅ SignalSender запущен и работает!"); }
        catch (Exception e){ System.out.println("[Telegram Test Message Error] " + e.getMessage()); }

        List<String> coins = getTopSymbols(TOP_N)
                .stream()
                .filter(BINANCE_PAIRS::contains)
                .toList();

        for(String pair : coins){
            KlinesResult kr = fetchClosesWithMeta(pair);
            if (kr.closes.isEmpty()) continue;

            Long lastOpen = lastOpenTimeMap.getOrDefault(pair, 0L);
            if (kr.lastOpenTime <= lastOpen) {
                System.out.println("[SKIP] No new candle for " + pair);
                continue;
            }
            lastOpenTimeMap.put(pair, kr.lastOpenTime);

            evaluate(pair,kr.closes).ifPresent(s -> {
                System.out.println("[Debug] First: " + s.symbol + " score=" + s.rawScore + " conf=" + s.confidence);
                bot.sendSignal(s.toTelegramMessage());
            });
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                ensureBinancePairsFresh();

                List<String> filtered = getTopSymbols(TOP_N)
                        .stream()
                        .filter(BINANCE_PAIRS::contains)
                        .toList();

                for(String pair : filtered){
                    KlinesResult kr = fetchClosesWithMeta(pair);
                    if (kr.closes.isEmpty()) continue;

                    Long lastOpen = lastOpenTimeMap.getOrDefault(pair, 0L);
                    if (kr.lastOpenTime <= lastOpen) {
                        System.out.println("[SKIP] No new candle for " + pair);
                        continue;
                    }
                    lastOpenTimeMap.put(pair, kr.lastOpenTime);

                    evaluate(pair,kr.closes).ifPresent(s -> {
                        System.out.println("[Debug] Scheduled: " + s.symbol + " score=" + s.rawScore + " conf=" + s.confidence);
                        bot.sendSignal(s.toTelegramMessage());
                    });
                }

            } catch(Exception e){
                System.out.println("[Job error] " + e.getMessage());
            }
        }, INTERVAL_MIN, INTERVAL_MIN, TimeUnit.MINUTES);
    }
}
