package com.bot;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public final class Elite5MinAnalyzer {

    private final DecisionEngineMerged engine;
    private final AdaptiveBrain brain = new AdaptiveBrain();

    private final Map<String, Long> lastSignal = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();

    private static final int MAX_COINS = 80;

    public Elite5MinAnalyzer(DecisionEngineMerged engine) {
        this.engine = engine;

        cleaner.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            lastSignal.entrySet().removeIf(e -> now - e.getValue() > 30 * 60_000);
        }, 10, 10, TimeUnit.MINUTES);
    }
    public Elite5MinAnalyzer(DecisionEngineMerged engine, double someThreshold) {
        this(engine); // вызов существующего конструктора
        // можно использовать someThreshold где-то в классе
    }
    /* ======================= OUTPUT MODEL ======================= */
    public static final class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final String reason;
        public final String coinType;

        public TradeSignal(String s,
                           TradingCore.Side side,
                           double e,
                           double sl,
                           double tp,
                           double conf,
                           String reason,
                           String coinType) {
            this.symbol = s;
            this.side = side;
            this.entry = e;
            this.stop = sl;
            this.take = tp;
            this.confidence = conf;
            this.reason = reason;
            this.coinType = coinType;
        }
    }

    /* ======================= MAIN ======================= */
    public List<TradeSignal> analyze(
            List<String> symbols,
            Map<String, List<TradingCore.Candle>> c15,
            Map<String, List<TradingCore.Candle>> c1h,
            Map<String, String> coinTypes
    ) {
        List<TradeSignal> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String s : symbols) {
            if (scanned++ >= MAX_COINS) break;

            var m15 = c15.get(s);
            var h1  = c1h.get(s);
            if (!valid(m15, 60) || !valid(h1, 40)) continue;

            String type = coinTypes.getOrDefault(s, "ALT");

            List<DecisionEngineMerged.TradeIdea> ideas =
                    engine.evaluate(
                            List.of(s),
                            Map.of(s, m15),
                            Map.of(s, h1),
                            Map.of(s, type)
                    );

            for (var i : ideas) {

                long cd = cooldown(type, i.grade);
                if (lastSignal.containsKey(s) && now - lastSignal.get(s) < cd)
                    continue;

                double conf = brain.adjust(s, i.probability, type);
                if (conf < 0.52) continue; // 🔥 единый мягкий порог

                out.add(new TradeSignal(
                        s,
                        i.side,
                        i.entry,
                        i.stop,
                        i.take,
                        conf,
                        i.reason,
                        type
                ));

                lastSignal.put(s, now);
                System.out.println("[SIGNAL] " + s + " " + i.side + " conf=" + conf);
            }
        }

        out.sort((a, b) -> Double.compare(b.confidence, a.confidence));
        return out;
    }

    /* ======================= BRAIN ======================= */
    static final class AdaptiveBrain {
        private final Map<String, Integer> streak = new ConcurrentHashMap<>();

        double adjust(String s, double base, String type) {
            int k = streak.getOrDefault(s, 0);
            double c = base;

            if (k >= 2) c += 0.04;
            if (k <= -2) c -= 0.04;

            if ("TOP".equals(type)) c += 0.02;
            if ("MEME".equals(type)) c -= 0.02;

            int h = LocalTime.now(ZoneOffset.UTC).getHour();
            if (h >= 7 && h <= 18) c += 0.02;

            return clamp(c, 0.45, 0.90);
        }
    }

    /* ======================= COOLDOWN ======================= */
    private static long cooldown(String type, DecisionEngineMerged.SignalGrade g) {
        long base = 90_000;
        if (g == DecisionEngineMerged.SignalGrade.A) base *= 0.6;
        if ("MEME".equals(type)) base *= 0.8;
        if ("TOP".equals(type)) base *= 1.2;
        return base;
    }

    /* ======================= UTILS ======================= */
    private static boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    public void shutdown() {
        cleaner.shutdown();
    }
}
