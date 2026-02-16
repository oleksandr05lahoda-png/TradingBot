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

    private static final int MAX_COINS = 70; // топ 70 монет

    private static final int MIN_M15 = 50;
    private static final int MIN_H1  = 40;

    private static final double MIN_CONF = 0.55; // чуть мягче

    // Market filters (смягчены для большего количества сигналов)
    private static final double MIN_BODY_RATIO = 0.50;
    private static final double MIN_ATR_PERCENT = 0.0025; // 0.25%
    private static final double MIN_ADX = 12.0;

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

            if (isFlat(tf15)) continue;
            if (!hasVolatility(tf15)) continue;
            if (!hasTrendStrength(tf15)) continue;
            if (!hasVolumeExpansion(tf15)) continue;
            if (!structureConfirmed(tf15)) continue;

            String type = coinTypes.getOrDefault(symbol, "ALT");

            List<DecisionEngineMerged.TradeIdea> ideas =
                    engine.evaluate(
                            List.of(symbol),
                            Map.of(symbol, tf15),
                            Map.of(symbol, tf1h),
                            Map.of(symbol, type)
                    );

            for (DecisionEngineMerged.TradeIdea idea : ideas) {

                // Убрали жесткий фильтр против H1
                // if (counterTrendHTF(tf1h, idea.side)) continue;

                String key = symbol + "_" + idea.side;
                long cd = cooldown(type, idea.grade);

                if (lastSignal.containsKey(key)
                        && now - lastSignal.get(key) < cd)
                    continue;

                double conf = brain.adjust(symbol, idea.confidence, type);
                if (conf < MIN_CONF) continue;

                double atr = atr(tf15, 14);
                double riskPercent = Math.abs(idea.entry - idea.stop) / idea.entry;

                if (riskPercent > atr * 2.5) // смягчение проверки риска
                    continue;

                result.add(new TradeSignal(
                        symbol,
                        idea.side,
                        idea.entry,
                        idea.stop,
                        idea.take,
                        clamp(conf, 0.0, 0.99),
                        idea.reason,
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

    /* ================= MARKET FILTERS ================= */

    private boolean isFlat(List<TradingCore.Candle> c) {
        int n = c.size();
        double bodySum = 0;

        for (int i = n - 6; i < n - 1; i++) {
            TradingCore.Candle k = c.get(i);
            double body = Math.abs(k.close - k.open);
            double range = k.high - k.low + 1e-6;
            bodySum += body / range;
        }

        return (bodySum / 5.0) < MIN_BODY_RATIO;
    }

    private boolean hasVolatility(List<TradingCore.Candle> c) {
        double atr = atr(c, 14);
        double price = c.get(c.size() - 1).close;
        return (atr / price) > MIN_ATR_PERCENT;
    }

    private boolean hasTrendStrength(List<TradingCore.Candle> c) {
        return adx(c, 14) > MIN_ADX;
    }

    private boolean hasVolumeExpansion(List<TradingCore.Candle> c) {
        int n = c.size();
        double avg = 0;

        for (int i = n - 15; i < n - 1; i++)
            avg += c.get(i).volume;

        avg /= 14.0;

        return c.get(n - 1).volume > avg * 1.2;
    }

    private boolean structureConfirmed(List<TradingCore.Candle> c) {
        int n = c.size();
        int dir = 0;

        for (int i = n - 4; i < n - 1; i++) {
            int d = Double.compare(c.get(i + 1).close, c.get(i).close);
            if (d == 0) return false;
            if (dir == 0) dir = d;
            else if (d != dir) return false;
        }

        return true;
    }

    /* ================= INDICATORS ================= */

    private double atr(List<TradingCore.Candle> c, int period) {
        int n = c.size();
        double sum = 0;

        for (int i = n - period; i < n; i++) {
            TradingCore.Candle k = c.get(i);
            double tr = k.high - k.low;
            sum += tr;
        }

        return sum / period;
    }

    private double adx(List<TradingCore.Candle> c, int period) {
        int n = c.size();
        double move = 0;

        for (int i = n - period; i < n - 1; i++)
            move += Math.abs(c.get(i + 1).close - c.get(i).close);

        double atr = atr(c, period);
        return (move / period) / atr * 25.0;
    }

    /* ================= BRAIN ================= */

    static final class AdaptiveBrain {
        private final Map<String, Integer> streak =
                new ConcurrentHashMap<>();

        double adjust(String symbol,
                      double base,
                      String type) {

            int k = streak.getOrDefault(symbol, 0);
            double conf = base;

            if (k >= 2) conf += 0.03;
            if (k <= -2) conf -= 0.02;

            if ("TOP".equals(type)) conf += 0.03;

            return conf;
        }
    }

    /* ================= COOLDOWN ================= */

    private static long cooldown(String type,
                                 DecisionEngineMerged.SignalGrade grade) {

        long base = 8 * 60_000; // смягчено

        if (grade == DecisionEngineMerged.SignalGrade.A)
            base *= 0.7;

        if ("MEME".equals(type))
            base *= 0.6;

        return base;
    }

    /* ================= UTILS ================= */

    private static boolean valid(List<?> l, int min) {
        return l != null && l.size() >= min;
    }

    private static double clamp(double v,
                                double min,
                                double max) {
        return Math.max(min, Math.min(max, v));
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
