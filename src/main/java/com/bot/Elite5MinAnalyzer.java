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
    private final double MIN_CONF; // минимальная уверенность сигналов

    /* ======================= CONSTRUCTORS ======================= */
    public Elite5MinAnalyzer(DecisionEngineMerged engine) {
        this(engine, 0.48); // default значение
    }

    public Elite5MinAnalyzer(DecisionEngineMerged engine, double minConf) {
        this.engine = engine;
        this.MIN_CONF = minConf;

        // Очистка старых сигналов каждые 10 минут
        cleaner.scheduleAtFixedRate(() -> {
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
            var h1 = c1h.get(s);
            if (!valid(m15, 50) || !valid(h1, 30)) continue;

            String type = coinTypes.getOrDefault(s, "ALT");

            // Получаем идеи от DecisionEngineMerged
            List<DecisionEngineMerged.TradeIdea> ideas =
                    engine.evaluate(
                            List.of(s),
                            Map.of(s, m15),
                            Map.of(s, h1),
                            Map.of(s, type)
                    );

            for (var i : ideas) {
                // cooldown по монета+направление
                String key = s + "_" + i.side;
                long cd = cooldown(type, i.grade);

                if (lastSignal.containsKey(key) && now - lastSignal.get(key) < cd)
                    continue;

                // адаптивная корректировка вероятности
                double conf = brain.adjust(s, i.confidence, type);

                // немного усиливаем или ослабляем сигнал по grade
                if (i.grade == DecisionEngineMerged.SignalGrade.A) conf += 0.03;
                if (i.grade == DecisionEngineMerged.SignalGrade.B) conf -= 0.02;

                conf = clamp(conf, 0.45, 0.95);
                if (conf < MIN_CONF) continue;

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

                lastSignal.put(key, now);
            }
        }

        // сортировка по уверенности, сильнейшие сигналы впереди
        out.sort((a, b) -> Double.compare(b.confidence, a.confidence));
        return out;
    }

    /* ======================= ADAPTIVE BRAIN ======================= */
    static final class AdaptiveBrain {
        private final Map<String, Integer> streak = new ConcurrentHashMap<>();

        double adjust(String s, double base, String type) {
            int k = streak.getOrDefault(s, 0);
            double c = base;

            if (k >= 2) c += 0.03;
            if (k <= -2) c -= 0.03;

            if ("TOP".equals(type)) c += 0.02;
            if ("MEME".equals(type)) c -= 0.01;

            int h = LocalTime.now(ZoneOffset.UTC).getHour();
            if (h >= 6 && h <= 22) c += 0.02;

            return c;
        }
    }

    /* ======================= COOLDOWN ======================= */
    private static long cooldown(String type, DecisionEngineMerged.SignalGrade g) {
        long base = 5 * 60_000; // 5 минут
        if (g == DecisionEngineMerged.SignalGrade.A) base *= 0.5;  // 2.5 минуты для сильного сигнала
        if (g == DecisionEngineMerged.SignalGrade.B) base *= 0.8;  // 4 минуты для слабого
        if ("MEME".equals(type)) base *= 0.6;
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
