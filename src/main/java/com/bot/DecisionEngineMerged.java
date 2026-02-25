package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Professional Decision Engine
 * Стабильная версия для бесконечного 15m цикла.
 */
public final class DecisionEngineMerged {

    /* ================= ENUMS ================= */

    public enum CoinCategory { TOP, ALT, MEME }
    public enum SignalGrade { A, B, C }
    public enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, CLIMAX, VOLATILE }
    public enum HTFBias { BULL, BEAR, NONE }

    /* ================= CONFIG ================= */

    private static final int MIN_BARS = 200;

    private static final long COOLDOWN_TOP = 5 * 60_000;
    private static final long COOLDOWN_ALT = 6 * 60_000;
    private static final long COOLDOWN_MEME = 8 * 60_000;

    private static final double MIN_SCORE_THRESHOLD = 2.4;
    private static final double MIN_CONFIDENCE = 0.52;

    /* ================= STATE ================= */

    private final Map<String, Long> cooldownMap = new ConcurrentHashMap<>();
    private final Map<String, Double> adaptiveTrendWeight = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>> recentDirections = new ConcurrentHashMap<>();

    /* ================= TRADE IDEA ================= */

    /* ================= TRADE IDEA ================= */
    public static final class TradeIdea {
        public final String symbol;          // Символ монеты
        public final TradingCore.Side side;  // LONG или SHORT
        public final double price;           // Цена закрытия последней свечи
        public final double stop;            // Стоп-лосс
        public final double take;            // Тейк-профит
        public final double probability;     // Вероятность успешного прогноза
        public final String reason;          // Причины сигнала (2-3 главных фактора)

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double price,
                         double stop,
                         double take,
                         double probability,
                         String reason) {
            this.symbol = symbol;
            this.side = side;
            this.price = price;
            this.stop = stop;
            this.take = take;
            this.probability = probability;
            this.reason = reason;
        }
    }

    /* ================= CORE ================= */
    private TradeIdea generate(String symbol,
                               List<TradingCore.Candle> c1,
                               List<TradingCore.Candle> c5,
                               List<TradingCore.Candle> c15,
                               List<TradingCore.Candle> c1h,
                               CoinCategory cat,
                               long now) {

        if (!valid(c15) || !valid(c1h)) return null;

        double price = last(c15).close;
        double atr = Math.max(atr(c15, 14), price * 0.0012);

        MarketState state = detectState(c15);
        HTFBias bias = detectBias(c1h);

        double scoreLong = 0;
        double scoreShort = 0;


        List<String> reasons = new ArrayList<>();

        // ===== Trend =====
        if (bias == HTFBias.BULL) {
            scoreLong += 2.0;
            reasons.add("BULL trend");
        }
        if (bias == HTFBias.BEAR) {
            scoreShort += 2.0;
            reasons.add("BEAR trend");
        }

        // ===== Pullback =====
        if (pullback(c15, true)) {
            scoreLong += 1.4;
            reasons.add("Pullback bullish");
        }
        if (pullback(c15, false)) {
            scoreShort += 1.4;
            reasons.add("Pullback bearish");
        }

        // ===== Micro impulse =====
        if (impulse(c1)) {
            scoreLong += 0.8;
            scoreShort += 0.8;
            reasons.add("Impulse detected");
        }

        // ===== Divergence =====
        if (bullDiv(c15)) {
            scoreLong += 1.2;
            reasons.add("Bullish divergence");
        }
        if (bearDiv(c15)) {
            scoreShort += 1.2;
            reasons.add("Bearish divergence");
        }

        // ===== Volume =====
        if (volumeSpike(c15, cat)) {
            scoreLong += 0.5;
            scoreShort += 0.5;
            reasons.add("Volume spike");
        }

        if (scoreLong < MIN_SCORE_THRESHOLD && scoreShort < MIN_SCORE_THRESHOLD)
            return null;

        TradingCore.Side side = scoreLong > scoreShort ? TradingCore.Side.LONG : TradingCore.Side.SHORT;
        double rawScore = Math.max(scoreLong, scoreShort);

        if (!cooldownAllowed(symbol, side, cat, now))
            return null;

        if (!flipAllowed(symbol, side))
            return null;

        double probability = computeConfidence(rawScore, state, cat);

        if (probability < MIN_CONFIDENCE)
            return null;

// ===== Дополнительные флаги и RSI для Reason =====
        List<String> flags = new ArrayList<>();
        if (atr(c15, 14) > price * 0.001) flags.add("ATR↑");
        if (volumeSpike(c15, cat)) flags.add("vol:true");
        if (impulse(c1)) flags.add("impulse:true");

        double rsi14 = rsi(c15, 14); // метод RSI добавляем ниже

        String reasonStr = String.join(", ", reasons.subList(0, Math.min(3, reasons.size())))
                + " | _flags_: " + String.join(", ", flags)
                + " | _raw_: " + String.format("%.3f mtf:0 vol:%b atr:%b", Math.max(scoreLong, scoreShort), volumeSpike(c15, cat), atr(c15, 14) > price * 0.001)
                + " | RSI(14): " + String.format("%.2f", rsi14);

        double riskMult =
                cat == CoinCategory.MEME ? 1.3 :
                        cat == CoinCategory.ALT ? 1.0 : 0.85;

        double rr = probability > 0.75 ? 2.8 : 2.2;

        double stop = side == TradingCore.Side.LONG ?
                price - atr * riskMult :
                price + atr * riskMult;

        double take = side == TradingCore.Side.LONG ?
                price + atr * riskMult * rr :
                price - atr * riskMult * rr;

        registerSignal(symbol, side, now);

        return new TradeIdea(
                symbol,
                side,
                price,
                stop,
                take,
                probability,
                reasonStr
        );
    }

    /* ================= COOLDOWN ================= */

    private boolean cooldownAllowed(String symbol,
                                    TradingCore.Side side,
                                    CoinCategory cat,
                                    long now) {

        String key = symbol + "_" + side;
        long base =
                cat == CoinCategory.TOP ? COOLDOWN_TOP :
                        cat == CoinCategory.ALT ? COOLDOWN_ALT :
                                COOLDOWN_MEME;

        Long last = cooldownMap.get(key);

        if (last != null && now - last < base)
            return false;

        cooldownMap.put(key, now);
        return true;
    }

    /* ================= FLIP ================= */

    private boolean flipAllowed(String symbol,
                                TradingCore.Side newSide) {

        Deque<String> history =
                recentDirections.computeIfAbsent(
                        symbol,
                        k -> new ArrayDeque<>());

        if (!history.isEmpty()) {
            String last = history.peekLast();
            if (!last.equals(newSide.name()))
                return false;
        }

        return true;
    }

    private void registerSignal(String symbol,
                                TradingCore.Side side,
                                long now) {

        String key = symbol + "_" + side;
        cooldownMap.put(key, now);

        Deque<String> history =
                recentDirections.computeIfAbsent(
                        symbol,
                        k -> new ArrayDeque<>());

        history.addLast(side.name());

        if (history.size() > 3)
            history.removeFirst();
    }

    /* ================= CONFIDENCE ================= */

    private double computeConfidence(double raw,
                                     MarketState state,
                                     CoinCategory cat) {

        double base = raw / 6.0;

        double stateBoost =
                state == MarketState.STRONG_TREND ? 0.12 :
                        state == MarketState.WEAK_TREND ? 0.08 :
                                state == MarketState.RANGE ? 0.05 :
                                        0.04;

        double catBoost =
                cat == CoinCategory.TOP ? 0.02 :
                        cat == CoinCategory.ALT ? 0.04 :
                                0.06;

        return clamp(0.52 + base + stateBoost + catBoost,
                0.52,
                0.95);
    }

    /* ================= MARKET ================= */

    private MarketState detectState(List<TradingCore.Candle> c) {
        double adx = adx(c, 14);
        if (adx > 25) return MarketState.STRONG_TREND;
        if (adx > 18) return MarketState.WEAK_TREND;
        return MarketState.RANGE;
    }

    private HTFBias detectBias(List<TradingCore.Candle> c) {
        if (!valid(c)) return HTFBias.NONE;
        double ema50 = ema(c, 50);
        double ema200 = ema(c, 200);
        if (ema50 > ema200) return HTFBias.BULL;
        if (ema50 < ema200) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    /* ================= INDICATORS ================= */

    public double atr(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++) {
            TradingCore.Candle cur = c.get(i);
            TradingCore.Candle prev = c.get(i - 1);
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));
            sum += tr;
        }
        return sum / n;
    }

    private double adx(List<TradingCore.Candle> c, int n) {
        double move = 0;
        for (int i = c.size() - n; i < c.size() - 1; i++)
            move += Math.abs(c.get(i + 1).close - c.get(i).close);
        return move / n;
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private boolean bullDiv(List<TradingCore.Candle> c) {
        if (c.size() < 20) return false;
        return c.get(c.size() - 1).low <
                c.get(c.size() - 4).low;
    }

    private boolean bearDiv(List<TradingCore.Candle> c) {
        if (c.size() < 20) return false;
        return c.get(c.size() - 1).high >
                c.get(c.size() - 4).high;
    }

    public boolean impulse(List<TradingCore.Candle> c) {
        if (c == null || c.size() < 5) return false;
        double delta =
                last(c).close -
                        c.get(c.size() - 5).close;
        return Math.abs(delta) > 0.0002;
    }

    public boolean volumeSpike(List<TradingCore.Candle> c,
                                CoinCategory cat) {

        if (c.size() < 10) return false;

        double avg =
                c.subList(c.size() - 10,
                                c.size() - 1)
                        .stream()
                        .mapToDouble(cd -> cd.volume)
                        .average()
                        .orElse(1);

        double lastVol =
                last(c).volume;

        double threshold =
                cat == CoinCategory.MEME ? 1.4 :
                        cat == CoinCategory.ALT ? 1.25 :
                                1.15;

        return lastVol / avg > threshold;
    }

    private boolean pullback(List<TradingCore.Candle> c,
                             boolean bull) {

        double ema21 = ema(c, 21);
        double price = last(c).close;

        return bull ?
                price <= ema21 * 1.01 :
                price >= ema21 * 0.99;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> c) {
        return c != null && c.size() >= MIN_BARS;
    }

    private double clamp(double v,
                         double min,
                         double max) {
        return Math.max(min, Math.min(max, v));
    }
    public double rsi(List<TradingCore.Candle> c, int period) {
        if (c.size() < period + 1) return 50.0;
        double gain = 0, loss = 0;
        for (int i = c.size() - period; i < c.size(); i++) {
            double change = c.get(i).close - c.get(i - 1).close;
            if (change > 0) gain += change;
            else loss -= change;
        }
        double rs = loss == 0 ? 100 : gain / loss;
        return 100 - (100 / (1 + rs));
    }
    /**
     * Публичный метод для внешнего вызова из SignalSender
     * Возвращает TradeIdea с нормальным reason
     */
    public TradeIdea analyze(String symbol,
                             List<TradingCore.Candle> c1,
                             List<TradingCore.Candle> c5,
                             List<TradingCore.Candle> c15,
                             List<TradingCore.Candle> c1h,
                             CoinCategory cat) {

        long now = System.currentTimeMillis();

        // Используем существующий private generate метод
        TradeIdea idea = generate(symbol, c1, c5, c15, c1h, cat, now);

        // Если generate вернул null, сигнал не подходит
        if (idea == null) return null;

        // Уже внутри generate reason собирается по тренду, импульсу, объему и RSI
        // Поэтому здесь просто возвращаем TradeIdea с нормальным reason
        return idea;
    }
}