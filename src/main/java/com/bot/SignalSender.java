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

    // track last sent signal to avoid duplicates: symbol -> key(direction|price|rsi)
    private final Map<String, String> lastSentSignal = new ConcurrentHashMap<>();

    public SignalSender(TelegramBotSender bot) {
        this.bot = bot;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        this.TOP_N = Integer.parseInt(System.getenv().getOrDefault("TOP_N", "50"));
        this.MIN_CONF = Double.parseDouble(System.getenv().getOrDefault("MIN_CONFIDENCE", "0.5"));
        this.INTERVAL_MIN = Integer.parseInt(System.getenv().getOrDefault("INTERVAL_MINUTES", "15"));
        this.KLINES_LIMIT = Integer.parseInt(System.getenv().getOrDefault("KLINES", "40"));
        this.REQUEST_DELAY_MS = Long.parseLong(System.getenv().getOrDefault("REQUEST_DELAY_MS", "200"));
    }


    /* ----------------------  NEW: fetch Binance symbols ---------------------- */

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
                // only include TRADING/spot symbols (optionally you can filter by status)
                String symbol = s.getString("symbol");
                String status = s.optString("status", "TRADING");
                String quoteAsset = s.optString("quoteAsset", "");
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

    // helper to refresh BINANCE_PAIRS periodically
    private void ensureBinancePairsFresh() {
        long now = System.currentTimeMillis();
        if (BINANCE_PAIRS.isEmpty() || (now - lastBinancePairsRefresh) > BINANCE_REFRESH_INTERVAL_MS) {
            BINANCE_PAIRS = getBinanceSymbols();
            lastBinancePairsRefresh = now;
        }
    }


    /* ----------------------- Top CoinGecko symbols --------------------------- */

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
                if (STABLE.contains(sym)) continue; // skip direct stable symbols like USDT
                list.add(sym + "USDT");
            }
            return list;

        } catch (Exception e) {
            System.out.println("[CoinGecko] Error: " + e.getMessage());
            return List.of("BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","ADAUSDT");
        }
    }


    /* ------------------------------ fetch KLINES ----------------------------- */

    // wrapper to return closes + last open time (ms)
    public static class KlinesResult {
        public final List<Double> closes;
        public final long lastOpenTime; // ms since epoch

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

            // NEW: защита от ошибок Binance API
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

            System.out.println(String.format("[Binance] %s: fetched %d candles, lastOpen=%s",
                    symbol, closes.size(), Instant.ofEpochMilli(lastOpen).toString()));

            return new KlinesResult(closes, lastOpen);

        } catch (Exception e) {
            System.out.println("[Binance] Error for " + symbol + ": " + e.getMessage());
            return new KlinesResult(Collections.emptyList(), 0L);
        }
    }


    /* -------------------- Indicators & strategies (unchanged) -------------------- */
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
        return (prices.get(prices.size()-1) - prices.get(prices.size()-1-n)) /
                (prices.get(prices.size()-1-n) + 1e-12);
    }

    public double strategyEMACrossover(List<Double> closes) {
        int look = Math.min(closes.size(), 30);
        List<Double> slice = closes.subList(closes.size()-look, closes.size());
        double e9 = ema(slice,9);
        double e21 = ema(slice,21);
        return e9>e21?+0.6:-0.6;
    }

    public double strategyRSIMeanReversion(List<Double> closes) {
        double r = rsi(closes,14);
        if (r < 30) return +0.7;
        if (r > 70) return -0.7;
        return 0.0;
    }

    public double strategyMACDMomentum(List<Double> closes) {
        double h = macdHist(closes);
        return h>0?+0.4:-0.4;
    }

    public double strategyVolMomentum(List<Double> closes) {
        double m = momentum(closes,3);
        if(m>0.003) return +0.2;
        if(m<-0.003) return -0.2;
        return 0.0;
    }


    /* -------------------------------- evaluate -------------------------------- */

    public Optional<Signal> evaluate(String pair, List<Double> closes) {
        if (closes == null || closes.size() < 20) return Optional.empty();
        List<Double> last = closes.subList(Math.max(0,closes.size()-20), closes.size());
        double s1 = strategyEMACrossover(last);
        double s2 = strategyRSIMeanReversion(last);
        double s3 = strategyMACDMomentum(last);
        double s4 = strategyVolMomentum(last);

        double score = s1*0.3 + s2*0.35 + s3*0.25 + s4*0.1;
        double conf = Math.min(1.0, Math.abs(score));
        if(conf < MIN_CONF) return Optional.empty();

        String direction = score>0?"LONG":"SHORT";
        String symbolOnly = pair.replace("USDT","");
        double lastPrice = last.get(last.size()-1);
        double rsiVal = rsi(last,14);

        return Optional.of(new Signal(symbolOnly,direction,conf,lastPrice,rsiVal,score));
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


    /* ---------------------------------- START ---------------------------------- */

    public void start() {

        System.out.println("[SignalSender] Starting TOP_N="+TOP_N+" MIN_CONF="+MIN_CONF+" INTERVAL_MIN="+INTERVAL_MIN);

        // Load Binance pairs (initial)
        ensureBinancePairsFresh();

        // test telegram
        try { bot.sendSignal("✅ SignalSender запущен и работает!"); }
        catch (Exception e){ System.out.println("[Telegram Test Message Error] " + e.getMessage()); }


        /* First run immediately */
        List<String> coins = getTopSymbols(TOP_N)
                .stream()
                .filter(BINANCE_PAIRS::contains)
                .toList();

        for(String pair : coins){
            // fetch closes + meta
            KlinesResult kr = fetchClosesWithMeta(pair);
            if (kr.closes.isEmpty()) {
                System.out.println("[SKIP] No candles for " + pair);
                continue;
            }
            evaluate(pair,kr.closes).ifPresent(s -> {
                String key = signalKey(s);
                // duplicate suppression
                String prev = lastSentSignal.get(s.symbol);
                if (key.equals(prev)) {
                    System.out.println("[SKIP] Duplicate first signal for " + s.symbol + " -> " + key);
                } else {
                    lastSentSignal.put(s.symbol, key);
                    System.out.println("[Debug] First: " + s.symbol + " score=" + s.rawScore + " conf=" + s.confidence);
                    bot.sendSignal(s.toTelegramMessage());
                }
            });
        }


        /* Scheduled every INTERVAL_MIN */
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // refresh pairs if needed
                ensureBinancePairsFresh();

                List<String> filtered = getTopSymbols(TOP_N)
                        .stream()
                        .filter(BINANCE_PAIRS::contains)
                        .toList();

                for(String pair : filtered){
                    KlinesResult kr = fetchClosesWithMeta(pair);
                    if (kr.closes.isEmpty()) {
                        System.out.println("[SKIP] No candles for " + pair);
                        continue;
                    }
                    // optionally: check that lastOpenTime is new compared to previous run - we don't store per-symbol lastOpen here,
                    // duplicate suppression by key(direction|price|rsi) is used instead.
                    evaluate(pair,kr.closes).ifPresent(s -> {
                        String key = signalKey(s);
                        String prev = lastSentSignal.get(s.symbol);
                        if (key.equals(prev)) {
                            System.out.println("[SKIP] Duplicate scheduled signal for " + s.symbol + " -> " + key);
                        } else {
                            lastSentSignal.put(s.symbol, key);
                            System.out.println("[Debug] Scheduled: " + s.symbol + " score=" + s.rawScore + " conf=" + s.confidence);
                            bot.sendSignal(s.toTelegramMessage());
                        }
                    });
                }

            } catch(Exception e){
                System.out.println("[Job error] " + e.getMessage());
            }
        }, INTERVAL_MIN, INTERVAL_MIN, TimeUnit.MINUTES);
    }

    private static String signalKey(Signal s) {
        // key: direction|price_rounded|rsi_rounded - reduces tiny float diffs
        String priceFmt = String.format(Locale.ROOT,"%.5f", s.price); // 5 decimals enough for duplicate detection
        String rsiFmt = String.format(Locale.ROOT,"%.2f", s.rsi);
        return s.direction + "|" + priceFmt + "|" + rsiFmt;
    }
}
