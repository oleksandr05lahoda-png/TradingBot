package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    /* ======================= MODEL ======================= */

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
            return String.format("TradeIdea[%s | %s | %s | Entry: %.4f Stop: %.4f Take: %.4f Conf: %.2f ATR: %.4f Reason: %s]",
                    symbol, coinType, side, entry, stop, take, confidence, atr, reason);
        }
    }

    /* ======================= FUTURES FILTER ======================= */

    private static final Set<String> FUTURES = ConcurrentHashMap.newKeySet();

    public static void loadFuturesSymbols(Collection<String> symbols) {
        FUTURES.clear();
        FUTURES.addAll(symbols);
        System.out.println("[INIT] Futures loaded: " + FUTURES.size());
    }

    private static boolean isFutures(String s) {
        return FUTURES.isEmpty() || FUTURES.contains(s);
    }

    /* ======================= CONFIG ======================= */

    private static final int MAX_COINS = 70;

    private static final double MIN_ATR_TOP  = 0.0012;
    private static final double MIN_ATR_ALT  = 0.0010;
    private static final double MIN_ATR_MEME = 0.0008;

    private static final double STOP_ATR = 0.6;
    private static final double RR_A = 2.0;
    private static final double RR_B = 1.5;

    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    /* ======================= MAIN ======================= */

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

            if (!valid(c15, 60) || !valid(c1h, 40)) continue;

            String type = coinTypes.getOrDefault(s, "ALT");
            double price = last(c15).close;
            double atr = atr(c15, 14);

            double minAtr = switch (type) {
                case "TOP"  -> MIN_ATR_TOP;
                case "MEME" -> MIN_ATR_MEME;
                default     -> MIN_ATR_ALT;
            };

            if (atr / price < minAtr) continue;

            EMA ltf = emaContext(c15);
            EMA htf = emaContext(c1h);

            TradeIdea idea = scan(s, c15, atr, ltf, htf, type, now);
            if (idea != null) out.add(idea);
        }

        // Сортировка по уверенности и ATR
        out.sort(Comparator.comparingDouble((TradeIdea t) -> t.confidence)
                .thenComparingDouble(t -> t.atr)
                .reversed());

        return out;
    }

    /* ======================= CORE LOGIC ======================= */

    private TradeIdea scan(String s, List<TradingCore.Candle> c, double atr,
                           EMA ltf, EMA htf, String type, long now) {

        double score = 0.50;

        // Тренд
        if (ltf.bullish && htf.bullish) score += 0.10;
        if (ltf.bearish && htf.bearish) score += 0.10;

        // Импульсная свеча
        if (impulse(c)) score += 0.06;

        // Объем
        if (last(c).volume > avgVol(c, 20)) score += 0.05;

        if (score < 0.55) return null;

        TradingCore.Side side = ltf.bullish ? TradingCore.Side.LONG :
                ltf.bearish ? TradingCore.Side.SHORT : null;

        if (side == null) return null;

        String key = s + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < 60_000) return null;

        cooldown.put(key, now);

        TradeIdea idea = build(s, c, side, atr, score, type);
        System.out.println("[SIGNAL] " + idea);
        return idea;
    }

    /* ======================= BUILD ======================= */

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
                clamp(score, 0.55, 0.85), atr, grade,
                "15M FUTURES CONF " + grade, type);
    }

    /* ======================= INDICATORS ======================= */

    private boolean impulse(List<TradingCore.Candle> c) {
        var k = last(c);
        double body = Math.abs(k.close - k.open);
        double range = k.high - k.low;
        return range > 0 && body / range > 0.6;
    }

    private EMA emaContext(List<TradingCore.Candle> c) {
        double e9 = ema(c, 9);
        double e21 = ema(c, 21);
        double e50 = ema(c, 50);
        return new EMA(e9, e21, e50, e9 > e21 && e21 > e50, e9 < e21 && e21 < e50);
    }

    /* ======================= UTILS ======================= */

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
            this.e9 = e9;
            this.e21 = e21;
            this.e50 = e50;
            this.bullish = bullish;
            this.bearish = bearish;
        }

        @Override
        public String toString() {
            return String.format("EMA[e9=%.4f, e21=%.4f, e50=%.4f, bullish=%b, bearish=%b]",
                    e9, e21, e50, bullish, bearish);
        }
    }
}
