package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public final class Elite5MinAnalyzer {

    private final DecisionEngineMerged engine;
    private final AdaptiveBrain brain = new AdaptiveBrain();
    private final Map<String, Long> lastSignal = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static final int MAX_COINS = 80;
    private final double MIN_CONF; // минимальная уверенность сигнала

    /* ======================= CONSTRUCTORS ======================= */
    public Elite5MinAnalyzer(DecisionEngineMerged engine) {
        this(engine, 0.48);
    }

    public Elite5MinAnalyzer(DecisionEngineMerged engine, double minConf) {
        this.engine = engine;
        this.MIN_CONF = minConf;

        // Очистка старых сигналов каждые 10 минут
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            lastSignal.entrySet().removeIf(e -> now - e.getValue() > 20 * 60_000);
        }, 10, 10, TimeUnit.MINUTES);
    }

    /* ======================= OUTPUT MODEL ======================= */
    public static final class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final String reason;
        public final String coinType;

        public TradeSignal(String s, TradingCore.Side side, double entry,
                           double stop, double take, double conf,
                           String reason, String coinType) {
            this.symbol = s;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = conf;
            this.reason = reason;
            this.coinType = coinType;
        }

        @Override
        public String toString() {
            return String.format("TradeSignal[%s | %s | %s | Entry: %.4f Stop: %.4f Take: %.4f Conf: %.2f Reason: %s]",
                    symbol, coinType, side, entry, stop, take, confidence, reason);
        }
    }

    /* ======================= MAIN ANALYSIS ======================= */
    public List<TradeSignal> analyze(List<String> symbols,
                                     Map<String, List<TradingCore.Candle>> c15,
                                     Map<String, List<TradingCore.Candle>> c1h,
                                     Map<String, String> coinTypes) {

        List<TradeSignal> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String s : symbols) {
            if (scanned++ >= MAX_COINS) break;

            var m15 = c15.get(s);
            var h1 = c1h.get(s);
            if (!valid(m15, 50) || !valid(h1, 30)) continue;

            String type = coinTypes.getOrDefault(s, "ALT");

            List<DecisionEngineMerged.TradeIdea> ideas =
                    engine.evaluate(List.of(s), Map.of(s, m15), Map.of(s, h1), Map.of(s, type));

            for (var idea : ideas) {
                String key = s + "_" + idea.side;
                long cd = cooldown(type, idea.grade);

                if (lastSignal.containsKey(key) && now - lastSignal.get(key) < cd) continue;

                double conf = brain.adjust(s, idea.confidence, type);

                // усиление/ослабление по grade
                conf += idea.grade == DecisionEngineMerged.SignalGrade.A ? 0.03 : -0.02;
                conf = clamp(conf, 0.45, 0.95);

                if (conf < MIN_CONF) continue;

                TradeSignal signal = new TradeSignal(
                        s,
                        idea.side,
                        idea.entry,
                        idea.stop,
                        idea.take,
                        conf,
                        idea.reason,
                        type
                );

                out.add(signal);
                lastSignal.put(key, now);
                System.out.println("[SIGNAL GENERATED] " + signal);
            }
        }

        out.sort(Comparator.comparingDouble((TradeSignal t) -> t.confidence).reversed());
        return out;
    }

    /* ======================= ADAPTIVE BRAIN ======================= */
    static final class AdaptiveBrain {
        private final Map<String, Integer> streak = new ConcurrentHashMap<>();

        double adjust(String symbol, double baseConf, String type) {
            int k = streak.getOrDefault(symbol, 0);
            double conf = baseConf;

            if (k >= 2) conf += 0.03;
            if (k <= -2) conf -= 0.03;

            if ("TOP".equals(type)) conf += 0.02;
            if ("MEME".equals(type)) conf -= 0.01;

            int hour = LocalTime.now(ZoneOffset.UTC).getHour();
            if (hour >= 6 && hour <= 22) conf += 0.02;

            return clamp(conf, 0.0, 0.99);
        }

        public void registerResult(String symbol, boolean win) {
            streak.merge(symbol, win ? 1 : -1, Integer::sum);
        }
    }

    /* ======================= COOLDOWN ======================= */
    private static long cooldown(String type, DecisionEngineMerged.SignalGrade grade) {
        long base = 5 * 60_000; // 5 минут
        if (grade == DecisionEngineMerged.SignalGrade.A) base *= 0.5;  // 2.5 мин
        if (grade == DecisionEngineMerged.SignalGrade.B) base *= 0.8;  // 4 мин
        if ("MEME".equals(type)) base *= 0.6;
        if ("TOP".equals(type)) base *= 1.2;
        return base;
    }

    /* ======================= UTILS ======================= */
    private static boolean valid(List<?> list, int minSize) {
        return list != null && list.size() >= minSize;
    }

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
