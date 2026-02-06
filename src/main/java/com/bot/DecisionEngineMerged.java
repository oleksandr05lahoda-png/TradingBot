package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    public enum SignalGrade { A, B }

    public static final class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final double atr;
        public final SignalGrade grade;
        public final String reason;
        public final String coinType;

        public TradeIdea(String symbol, TradingCore.Side side, double entry,
                         double stop, double take, double confidence, double atr,
                         SignalGrade grade, String reason, String coinType) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.atr = atr;
            this.grade = grade;
            this.reason = reason;
            this.coinType = coinType;
        }

        @Override
        public String toString() {
            return String.format("[SIGNAL] %s | %s | %s | Entry: %.4f Stop: %.4f Take: %.4f Conf: %.2f ATR: %.4f Reason: %s",
                    symbol, coinType, side, entry, stop, take, confidence, atr, reason);
        }
    }

    private static final Set<String> FUTURES = ConcurrentHashMap.newKeySet();
    public static void loadFuturesSymbols(Collection<String> symbols) {
        FUTURES.clear();
        FUTURES.addAll(symbols);
        System.out.println("[INIT] Futures loaded: " + FUTURES.size());
    }
    private static boolean isFutures(String s) {
        return FUTURES.isEmpty() || FUTURES.contains(s);
    }

    private static final int MAX_COINS = 120;      // максимум монет для сканирования
    private static final double STOP_ATR = 0.45;   // чуть меньше, чтобы чаще были сигналы
    private static final double RR_A = 2.0;
    private static final double RR_B = 1.4;

    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> m15,
                                    Map<String, List<TradingCore.Candle>> h1,
                                    Map<String, String> coinTypes) {

        List<TradeIdea> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String s : symbols) {
            if (scanned++ >= MAX_COINS) break;
            if (!isFutures(s)) continue;

            var c15 = m15.get(s);
            var c1h = h1.get(s);

            if (!valid(c15, 15) || !valid(c1h, 15)) continue; // минимальный объем данных

            String type = coinTypes.getOrDefault(s, "ALT");
            double price = last(c15).close;
            double atr = atr(c15, 14);

            EMA ltf = emaContext(c15);
            EMA htf = emaContext(c1h);

            TradeIdea idea = scan(s, c15, atr, ltf, htf, type, now);
            if (idea != null) out.add(idea);
        }

        // сортировка по уверенности, выше confidence → раньше в списке
        out.sort(Comparator.comparingDouble((TradeIdea t) -> t.confidence)
                .reversed());

        return out;
    }

    private TradeIdea scan(String s, List<TradingCore.Candle> c, double atr,
                           EMA ltf, EMA htf, String type, long now) {

        double score = 0.50;

        // Трендовые оценки
        if (ltf.bullish) score += 0.08;
        if (ltf.bearish) score += 0.08;
        if (htf.bullish) score += 0.06;
        if (htf.bearish) score += 0.06;

        // Импульсная свеча и объем
        if (impulse(c)) score += 0.07;
        if (last(c).volume > avgVol(c, 20)) score += 0.05;

        // Вес MEME/TOP/ALT
        switch (type) {
            case "TOP" -> score += 0.03;
            case "MEME" -> score -= 0.01;
        }

        score = clamp(score, 0.45, 0.90); // максимум 0.90

        // Генерация стороны LONG/SHORT
        TradingCore.Side side;
        if (ltf.bullish && htf.bullish) side = TradingCore.Side.LONG;
        else if (ltf.bearish && htf.bearish) side = TradingCore.Side.SHORT;
        else return null; // пропускаем непонятные сигналы

        String key = s + side;
        long cd = 15 * 60_000; // cooldown 15 минут
        if (cooldown.containsKey(key) && now - cooldown.get(key) < cd) return null;
        cooldown.put(key, now);

        return build(s, c, side, atr, score, type);
    }

    private TradeIdea build(String s, List<TradingCore.Candle> c, TradingCore.Side side,
                            double atr, double score, String type) {

        SignalGrade grade = score >= 0.65 ? SignalGrade.A : SignalGrade.B;

        double entry = last(c).close;
        double risk = atr * STOP_ATR;

        double stop = side == TradingCore.Side.LONG ? entry - risk : entry + risk;
        double take = side == TradingCore.Side.LONG
                ? entry + risk * (grade == SignalGrade.A ? RR_A : RR_B)
                : entry - risk * (grade == SignalGrade.A ? RR_A : RR_B);

        return new TradeIdea(s, side, entry, stop, take,
                score, atr, grade,
                "PROF FUTURES " + grade, type);
    }

    private boolean impulse(List<TradingCore.Candle> c) {
        var k = last(c);
        double body = Math.abs(k.close - k.open);
        double range = k.high - k.low;
        return range > 0 && body / range > 0.5;
    }

    private EMA emaContext(List<TradingCore.Candle> c) {
        double e9 = ema(c, 9);
        double e21 = ema(c, 21);
        double e50 = ema(c, 50);
        return new EMA(e9, e21, e50, e9 >= e21 && e21 >= e50, e9 <= e21 && e21 <= e50);
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c != null && !c.isEmpty() ? c.get(c.size() - 1) : null;
    }

    private boolean valid(List<?> l, int n) {
        return l != null && l.size() >= n;
    }

    private double atr(List<TradingCore.Candle> c, int n) {
        double s = 0;
        for (int i = c.size() - n; i < c.size(); i++) s += c.get(i).high - c.get(i).low;
        return s / n;
    }

    private double avgVol(List<TradingCore.Candle> c, int n) {
        double s = 0;
        for (int i = c.size() - n; i < c.size(); i++) s += c.get(i).volume;
        return s / n;
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++) e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    private static final class EMA {
        final double e9, e21, e50;
        final boolean bullish, bearish;
        EMA(double e9, double e21, double e50, boolean bullish, boolean bearish) {
            this.e9 = e9; this.e21 = e21; this.e50 = e50; this.bullish = bullish; this.bearish = bearish;
        }
    }
}
