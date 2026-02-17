package com.bot;

import java.util.*;
import java.util.concurrent.*;

public final class Elite5MinAnalyzer {

    private final DecisionEngineMerged engine;
    private final AdaptiveBrain brain = new AdaptiveBrain();

    private final Map<String, Long> lastSignal = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    /* ================= CONFIG ================= */

    private static final int MAX_COINS = 70;

    private static final int MIN_M15 = 60;
    private static final int MIN_H1  = 60;

    private static final double MIN_CONF = 0.56;

    private static final double MIN_ATR_PERCENT = 0.0023;
    private static final double MIN_ADX = 14.0;

    /* ================= CONSTRUCTOR ================= */

    public Elite5MinAnalyzer(DecisionEngineMerged engine) {
        this.engine = engine;

        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            lastSignal.entrySet()
                    .removeIf(e -> now - e.getValue() > 30 * 60_000);
        }, 5, 5, TimeUnit.MINUTES);
    }

    /* ================= OUTPUT ================= */

    public static final class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final String reason;
        public final String coinType;

        public TradeSignal(String symbol,
                           TradingCore.Side side,
                           double entry,
                           double stop,
                           double take,
                           double confidence,
                           String reason,
                           String coinType) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.reason = reason;
            this.coinType = coinType;
        }
    }

    /* ================= MAIN ================= */

    public List<TradeSignal> analyze(List<String> symbols,
                                     Map<String, List<TradingCore.Candle>> m15,
                                     Map<String, List<TradingCore.Candle>> h1,
                                     Map<String, String> coinTypes) {

        List<TradeSignal> result = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String symbol : symbols) {
            if (scanned++ >= MAX_COINS) break;

            List<TradingCore.Candle> tf15 = m15.get(symbol);
            List<TradingCore.Candle> tf1h = h1.get(symbol);

            if (!valid(tf15, MIN_M15) || !valid(tf1h, MIN_H1))
                continue;

            if (!hasVolatility(tf15)) continue;
            if (!hasTrendStrength(tf15)) continue;

            TradingCore.Side htfBias = detectHTFBias(tf1h);
            if (htfBias == null) continue;

            if (!pullbackReady(tf15, htfBias)) continue;

            String type = coinTypes.getOrDefault(symbol, "ALT");

            List<DecisionEngineMerged.TradeIdea> ideas =
                    engine.evaluate(
                            List.of(symbol),
                            Map.of(symbol, tf15),
                            Map.of(symbol, tf1h),
                            Map.of(symbol, type)
                    );

            for (DecisionEngineMerged.TradeIdea idea : ideas) {

                if (idea.side != htfBias)
                    continue;

                String key = symbol + "_" + idea.side;
                long cd = cooldown(type, idea.grade);

                if (lastSignal.containsKey(key)
                        && now - lastSignal.get(key) < cd)
                    continue;

                double conf = buildProfessionalConfidence(tf15, tf1h,
                        idea.confidence,
                        htfBias,
                        type);

                conf = brain.adjust(symbol, conf, type);

                if (conf < MIN_CONF) continue;

                result.add(new TradeSignal(
                        symbol,
                        idea.side,
                        idea.entry,
                        idea.stop,
                        idea.take,
                        clamp(conf, 0.0, 0.99),
                        idea.reason + " | HTF aligned",
                        type
                ));

                lastSignal.put(key, now);
            }
        }

        result.sort(
                Comparator.comparingDouble((TradeSignal t) -> t.confidence)
                        .reversed()
        );

        return result;
    }

    /* ================= PROFESSIONAL LOGIC ================= */

    private TradingCore.Side detectHTFBias(List<TradingCore.Candle> c) {
        double ema50 = ema(c, 50);
        double ema200 = ema(c, 200);
        double price = c.get(c.size() - 1).close;

        if (price > ema50 && ema50 > ema200)
            return TradingCore.Side.LONG;

        if (price < ema50 && ema50 < ema200)
            return TradingCore.Side.SHORT;

        return null;
    }

    private boolean pullbackReady(List<TradingCore.Candle> c,
                                  TradingCore.Side bias) {

        double ema20 = ema(c, 20);
        double lastClose = c.get(c.size() - 1).close;

        if (bias == TradingCore.Side.LONG)
            return lastClose > ema20;

        return lastClose < ema20;
    }

    private double buildProfessionalConfidence(List<TradingCore.Candle> m15,
                                               List<TradingCore.Candle> h1,
                                               double base,
                                               TradingCore.Side side,
                                               String type) {

        double trendScore = trendScore(h1);
        double momentumScore = momentumScore(m15);
        double volatilityScore = volatilityScore(m15);
        double structureScore = structureScore(m15);

        double conf =
                base * 0.35 +
                        trendScore * 0.25 +
                        momentumScore * 0.20 +
                        volatilityScore * 0.10 +
                        structureScore * 0.10;

        if ("TOP".equals(type)) conf += 0.02;
        if ("MEME".equals(type)) conf -= 0.03;

        return clamp(conf, 0.45, 0.93);
    }

    /* ================= SCORES ================= */

    private double trendScore(List<TradingCore.Candle> c) {
        double ema21 = ema(c, 21);
        double ema50 = ema(c, 50);
        return clamp(Math.abs(ema21 - ema50) / ema50 * 8, 0, 1);
    }

    private double momentumScore(List<TradingCore.Candle> c) {
        int n = c.size();
        double move = 0;
        for (int i = n - 6; i < n - 1; i++)
            move += Math.abs(c.get(i + 1).close - c.get(i).close);
        return clamp(move / 6.0 / atr(c, 14), 0, 1);
    }

    private double volatilityScore(List<TradingCore.Candle> c) {
        double atr = atr(c, 14);
        double price = c.get(c.size() - 1).close;
        return clamp((atr / price) * 40, 0, 1);
    }

    private double structureScore(List<TradingCore.Candle> c) {
        int n = c.size();
        int up = 0;
        for (int i = n - 5; i < n - 1; i++)
            if (c.get(i + 1).close > c.get(i).close)
                up++;
        return up / 4.0;
    }

    /* ================= INDICATORS ================= */

    private double atr(List<TradingCore.Candle> c, int period) {
        int n = c.size();
        double sum = 0;
        for (int i = n - period; i < n; i++)
            sum += c.get(i).high - c.get(i).low;
        return sum / period;
    }

    private boolean hasVolatility(List<TradingCore.Candle> c) {
        double atr = atr(c, 14);
        double price = c.get(c.size() - 1).close;
        return (atr / price) > MIN_ATR_PERCENT;
    }

    private boolean hasTrendStrength(List<TradingCore.Candle> c) {
        return adx(c, 14) > MIN_ADX;
    }

    private double adx(List<TradingCore.Candle> c, int period) {
        int n = c.size();
        double move = 0;
        for (int i = n - period; i < n - 1; i++)
            move += Math.abs(c.get(i + 1).close - c.get(i).close);
        return (move / period) / atr(c, period) * 25.0;
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private static boolean valid(List<?> l, int min) {
        return l != null && l.size() >= min;
    }

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    /* ================= ADAPTIVE ================= */

    static final class AdaptiveBrain {
        private final Map<String, Integer> streak =
                new ConcurrentHashMap<>();

        double adjust(String symbol, double base, String type) {
            int k = streak.getOrDefault(symbol, 0);
            double conf = base;

            if (k >= 2) conf += 0.02;
            if (k <= -2) conf -= 0.02;

            return conf;
        }
    }

    private static long cooldown(String type,
                                 DecisionEngineMerged.SignalGrade grade) {

        long base = 8 * 60_000;

        if (grade == DecisionEngineMerged.SignalGrade.A)
            base *= 0.7;

        if ("MEME".equals(type))
            base *= 0.6;

        return base;
    }
}
