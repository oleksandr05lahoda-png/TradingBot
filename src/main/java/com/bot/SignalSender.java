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
    private final Set<String> STABLE = Set.of("USDT","USDC","BUSD","DAI","FDUSD","TUSD","USDP","FRAX","USDD");

    private final int TOP_N;
    private final double MIN_CONF;
    private final int INTERVAL_MIN;
    private final int THREADS;
    private final int KLINES_LIMIT;
    private final long REQUEST_DELAY_MS;

    public SignalSender(TelegramBotSender bot) {
        this.bot = bot;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        this.TOP_N = Integer.parseInt(System.getenv().getOrDefault("TOP_N", "100"));
        this.MIN_CONF = Double.parseDouble(System.getenv().getOrDefault("MIN_CONFIDENCE", "0.70"));
        this.INTERVAL_MIN = Integer.parseInt(System.getenv().getOrDefault("INTERVAL_MINUTES", "5"));
        this.THREADS = Integer.parseInt(System.getenv().getOrDefault("THREADS", "8"));
        this.KLINES_LIMIT = Integer.parseInt(System.getenv().getOrDefault("KLINES", "40"));
        this.REQUEST_DELAY_MS = Long.parseLong(System.getenv().getOrDefault("REQUEST_DELAY_MS", "200"));
    }

    // Получаем топ N монет с CoinGecko
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
                list.add(sym + "USDT"); // Binance USDT pair
            }
            return list;
        } catch (Exception e) {
            System.out.println("[CoinGecko] error: " + e.getMessage());
            return List.of("BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","ADAUSDT");
        }
    }

    // Получаем цены закрытия с Binance
    public List<Double> fetchCloses(String symbol) {
        try {
            Thread.sleep(REQUEST_DELAY_MS); // чтобы не перегружать Binance
            String url = String.format("https://api.binance.com/api/v3/klines?symbol=%s&interval=5m&limit=%d",
                    symbol, KLINES_LIMIT);
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            JSONArray arr = new JSONArray(resp.body());
            List<Double> closes = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                closes.add(arr.getJSONArray(i).getDouble(4));
            }
            return closes;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    // Индикаторы
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
        double macd = ema(prices, 12) - ema(prices, 26);
        return macd;
    }

    public static double momentum(List<Double> prices, int n) {
        return (prices.get(prices.size()-1) - prices.get(prices.size()-1-n)) /
                (prices.get(prices.size()-1-n) + 1e-12);
    }

    public static double vol(List<Double> prices, int window) {
        List<Double> rets = new ArrayList<>();
        for (int i = prices.size()-window+1; i < prices.size(); i++) {
            double r = (prices.get(i) - prices.get(i-1)) / (prices.get(i-1) + 1e-12);
            rets.add(r);
        }
        double mean = rets.stream().mapToDouble(d -> d).average().orElse(0.0);
        double sumsq = 0; for(double r: rets) sumsq += (r-mean)*(r-mean);
        return Math.sqrt(sumsq/Math.max(1,rets.size()));
    }

    // Стратегии
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

    // Оценка сигнала
    public Optional<Signal> evaluate(String pair, List<Double> closes) {
        if (closes.size()<20) return Optional.empty();
        List<Double> last = closes.subList(Math.max(0,closes.size()-20),closes.size());
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
            return String.format("*%s* → *%s*\nConfidence: *%.2f*\nPrice: `%.8f`\nRSI(14): %.2f\n_time: %s_",
                    symbol,direction,confidence,price,rsi,Instant.now().toString());
        }
    }

    // Запуск scheduler на 24/7
    public void start(){
        System.out.println("[SignalSender] Starting TOP_N="+TOP_N+" MIN_CONF="+MIN_CONF+" INTERVAL_MIN="+INTERVAL_MIN);

        // <<< ВСТАВЬ СЮДА >>>
        try {
            bot.sendSignal("✅ SignalSender запущен и работает!");
        } catch (Exception e) {
            System.out.println("[Telegram Test Message Error] " + e.getMessage());
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Runnable job = () -> {
            try {
                List<String> pairs = getTopSymbols(TOP_N);
                for(String pair : pairs){
                    try{
                        List<Double> closes = fetchCloses(pair);
                        evaluate(pair,closes).ifPresent(s -> bot.sendSignal(s.toTelegramMessage()));
                    }catch(Exception e){ System.out.println("[Error] "+pair+" "+e.getMessage()); }
                }
            }catch(Exception e){ System.out.println("[Job error] "+e.getMessage()); }
        };
        scheduler.scheduleAtFixedRate(job,0,INTERVAL_MIN, TimeUnit.MINUTES);
    }
}
