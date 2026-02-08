package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public final class Elite5MinAnalyzer {

    private final DecisionEngineMerged engine;
    private final AdaptiveBrain brain = new AdaptiveBrain();

    private final Map<String, Long> lastSignal = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private static final int MAX_COINS = 120;

    // ФИЛЬТРЫ ДЛЯ ФЬЮЧЕЙ
    private static final int MIN_M15 = 40;
    private static final int MIN_H1  = 30;

    private static final double MIN_BODY_RATIO = 0.55; // защита от 1–2 свечей
    private static final int MIN_TREND_CANDLES = 3;    // структура

    private final double MIN_CONF;

    /* ======================= CONSTRUCTORS ======================= */

    public Elite5MinAnalyzer(DecisionEngineMerged engine) {
        this(engine, 0.55); // ❗ ниже для фьючей нельзя
    }

    public Elite5MinAnalyzer(DecisionEngineMerged engine, double minConf) {
        this.engine = engine;
        this.MIN_CONF = minConf;

        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            lastSignal.entrySet()
                    .removeIf(e -> now - e.getValue() > 30 * 60_000);
        }, 5, 5, TimeUnit.MINUTES);
    }

    /* ======================= OUTPUT MODEL ======================= */

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

    /* ======================= MAIN ANALYSIS ======================= */

    public List<TradeSignal> analyze(List<String> symbols,
                                     Map<String, List<TradingCore.Candle>> c15,
                                     Map<String, List<TradingCore.Candle>> c1h,
                                     Map<String, String> coinTypes) {

        List<TradeSignal> result = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String symbol : symbols) {
            if (scanned++ >= MAX_COINS) break;

            List<TradingCore.Candle> m15 = c15.get(symbol);
            List<TradingCore.Candle> h1  = c1h.get(symbol);

            if (!valid(m15, MIN_M15) || !valid(h1, MIN_H1)) continue;

            // 🔥 КЛЮЧЕВО: отсекаем флэт сразу
            if (isFlatMarket(m15)) continue;

            String type = coinTypes.getOrDefault(symbol, "ALT");

            List<DecisionEngineMerged.TradeIdea> ideas =
                    engine.evaluate(
                            List.of(symbol),
                            Map.of(symbol, m15),
                            Map.of(symbol, h1),
                            Map.of(symbol, type)
                    );

            for (DecisionEngineMerged.TradeIdea idea : ideas) {

                // ❗ защита от ранних входов
                if (!trendStructureConfirmed(m15)) continue;

                // ❗ защита от ложных разворотов
                if (isFakeReversal(m15, h1, idea.side)) continue;

                String key = symbol + "_" + idea.side;
                long cd = cooldown(type, idea.grade);

                if (lastSignal.containsKey(key)
                        && now - lastSignal.get(key) < cd) continue;

                double conf = brain.adjust(symbol, idea.confidence, type);
                if (conf < MIN_CONF) continue;

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

    /* ======================= MARKET FILTERS ======================= */

    // ФЛЭТ = нет нормальных тел свечей
    private boolean isFlatMarket(List<TradingCore.Candle> c) {
        int n = c.size();
        double sum = 0;

        for (int i = n - 6; i < n - 1; i++) {
            TradingCore.Candle k = c.get(i);
            double body = Math.abs(k.close - k.open);
            double range = k.high - k.low + 1e-6;
            sum += body / range;
        }
        return (sum / 5.0) < MIN_BODY_RATIO;
    }

    // СТРУКТУРА: минимум 3 свечи в одну сторону
    private boolean trendStructureConfirmed(List<TradingCore.Candle> c) {
        int n = c.size();
        int dir = 0;

        for (int i = n - 4; i < n - 1; i++) {
            double a = c.get(i).close;
            double b = c.get(i + 1).close;
            int d = Double.compare(b, a);

            if (d == 0) return false;
            if (dir == 0) dir = d;
            else if (d != dir) return false;
        }
        return true;
    }

    // ЛОЖНЫЙ РАЗВОРОТ
    private boolean isFakeReversal(List<TradingCore.Candle> m15,
                                   List<TradingCore.Candle> h1,
                                   TradingCore.Side side) {

        TradingCore.Candle last15 = m15.get(m15.size() - 1);
        TradingCore.Candle prev15 = m15.get(m15.size() - 2);
        TradingCore.Candle lastH1 = h1.get(h1.size() - 1);

        boolean htfUp = lastH1.close > lastH1.open;
        boolean impulse =
                Math.abs(last15.close - prev15.close) >
                        Math.abs(prev15.close - m15.get(m15.size() - 3).close);

        if (side == TradingCore.Side.LONG && !htfUp && !impulse) return true;
        if (side == TradingCore.Side.SHORT && htfUp && !impulse) return true;

        return false;
    }

    /* ======================= ADAPTIVE BRAIN ======================= */

    static final class AdaptiveBrain {
        private final Map<String, Integer> streak = new ConcurrentHashMap<>();

        double adjust(String symbol, double baseConf, String type) {
            int k = streak.getOrDefault(symbol, 0);
            double conf = baseConf;

            if (k >= 2) conf += 0.04;
            if (k <= -2) conf -= 0.04;

            if ("TOP".equals(type)) conf += 0.02;

            return conf;
        }
    }

    /* ======================= COOLDOWN ======================= */

    private static long cooldown(String type,
                                 DecisionEngineMerged.SignalGrade grade) {

        long base = 12 * 60_000; // ⬆️ чтобы не переобувался

        if (grade == DecisionEngineMerged.SignalGrade.A) base *= 0.7;
        if ("MEME".equals(type)) base *= 0.6;

        return base;
    }

    /* ======================= UTILS ======================= */

    private static boolean valid(List<?> list, int min) {
        return list != null && list.size() >= min;
    }

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
