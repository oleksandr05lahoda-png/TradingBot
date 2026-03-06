package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    public enum CoinCategory { TOP, ALT, MEME }
    public enum SignalGrade { A, B, C }
    public enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, VOLATILE }
    public enum HTFBias { BULL, BEAR, NONE }

    private static final int MIN_BARS = 150;

    private static final long COOLDOWN_TOP = 15 * 60_000;
    private static final long COOLDOWN_ALT = 15 * 60_000;
    private static final long COOLDOWN_MEME = 15 * 60_000;

    private static final double MIN_CONFIDENCE = 58.0;

    private final Map<String, Long> cooldownMap = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>> recentDirections = new ConcurrentHashMap<>();

    public static final class TradeIdea {

        public final String symbol;
        public final com.bot.TradingCore.Side side;
        public final double price;
        public final double stop;
        public final double take;
        public final double probability;
        public final List<String> flags;

        public TradeIdea(String symbol,
                         com.bot.TradingCore.Side side,
                         double price,
                         double stop,
                         double take,
                         double probability,
                         List<String> flags) {

            this.symbol = symbol;
            this.side = side;
            this.price = price;
            this.stop = stop;
            this.take = take;
            this.probability = probability;
            this.flags = flags != null ? flags : List.of();
        }
    }

    private TradeIdea generate(String symbol,
                               List<com.bot.TradingCore.Candle> c1,
                               List<com.bot.TradingCore.Candle> c5,
                               List<com.bot.TradingCore.Candle> c15,
                               List<com.bot.TradingCore.Candle> c1h,
                               CoinCategory cat,
                               long now) {

        if (!valid(c15) || !valid(c1h)) return null;

        double price = last(c15).close;
        double atr = Math.max(atr(c15, 14), price * 0.0012);

        MarketState state = detectState(c15);
        HTFBias bias = detectBias(c1h);

        double scoreLong = 0;
        double scoreShort = 0;

        Map<String, Double> reasonWeightsLong = new LinkedHashMap<>();
        Map<String, Double> reasonWeightsShort = new LinkedHashMap<>();

        /* ===== HTF Bias (balanced) ===== */

        if (bias == HTFBias.BULL) {
            scoreLong += 0.45;
            scoreShort -= 0.20;
        }
        else if (bias == HTFBias.BEAR) {
            scoreShort += 0.45;
            scoreLong -= 0.20;
        }
        if (pullback(c15, true)) {
            scoreLong += 0.9;
            reasonWeightsLong.put("Pullback bullish", 0.9);
        }

        if (pullback(c15, false)) {
            scoreShort += 0.9;
            reasonWeightsShort.put("Pullback bearish", 0.9);
        }
        if (impulse(c1)) {

            double atr1 = atr(c1, 14);
            double delta = last(c1).close - c1.get(c1.size() - 5).close;

            if (delta > atr1 * 0.32) {
                scoreLong += state == MarketState.STRONG_TREND ? 0.35 : 0.25;
            }

            if (delta < -atr1 * 0.32) {
                scoreShort += state == MarketState.STRONG_TREND ? 0.35 : 0.25;
            }
        }

        if (bullDiv(c15)) {
            scoreLong += 0.55;
            reasonWeightsLong.put("Bullish divergence", 0.55);
        }

        if (bearDiv(c15)) {
            scoreShort += 0.55;
            reasonWeightsShort.put("Bearish divergence", 0.55);
        }

        /* ===== RSI soft filter ===== */

        double rsi14 = rsi(c15, 14);

        if (state != MarketState.STRONG_TREND) {

            if (rsi14 > 84) scoreLong -= 0.12;
            if (rsi14 < 16) scoreShort -= 0.12;
        }

        /* ===== ADX trend protection ===== */

        double adxValue = adx(c15, 14);

        if (adxValue > 28) {

            if (bias == HTFBias.BULL && scoreShort > scoreLong)
                scoreShort *= 0.65;

            if (bias == HTFBias.BEAR && scoreLong > scoreShort)
                scoreLong *= 0.65;
        }

        /* ===== Dynamic threshold ===== */

        double dynamicThreshold =
                state == MarketState.STRONG_TREND ? 1.10 : 1.0;

        if (scoreLong < dynamicThreshold && scoreShort < dynamicThreshold)
            return null;

        /* ===== Momentum protection ===== */

        double move4 = (last(c15).close - c15.get(c15.size() - 4).close) / price;

        boolean strongMomentumUp = move4 > 0.018;
        boolean strongMomentumDown = move4 < -0.018;

        if (strongMomentumUp && scoreShort > scoreLong)
            scoreShort *= 0.8;

        if (strongMomentumDown && scoreLong > scoreShort)
            scoreLong *= 0.8;

        /* ===== Decide side ===== */

        double scoreDiff = Math.abs(scoreLong - scoreShort);

        if (scoreDiff < 0.06) return null;

        com.bot.TradingCore.Side side =
                scoreLong > scoreShort ?
                        com.bot.TradingCore.Side.LONG :
                        com.bot.TradingCore.Side.SHORT;

        /* ===== Cooldown ===== */

        if (!cooldownAllowed(symbol, side, cat, now))
            return null;

        if (!flipAllowed(symbol, side))
            return null;

        /* ===== Probability ===== */

        double probability = computeConfidence(
                Math.max(scoreLong, scoreShort),
                state,
                cat,
                atr,
                price
        );

        if (probability < MIN_CONFIDENCE)
            return null;

        /* ===== Flags ===== */

        List<String> flags = new ArrayList<>();

        if (atr / price > 0.0015) flags.add("ATR↑"); // чуть больше порог
        if (volumeSpike(c15, cat) && atr / price > 0.0015) flags.add("vol:true"); // более строгий фильтр
        if (impulse(c1)) flags.add("impulse:true");

        /* ===== Risk ===== */

        double riskMult =
                cat == CoinCategory.MEME ? 1.3 :
                        cat == CoinCategory.ALT ? 1.0 : 0.85;
        double rr;
        if (scoreDiff > 0.9) rr = 3.0;
        else if (scoreDiff > 0.6) rr = 2.6;
        else rr = 2.2;

        double stop =
                side == com.bot.TradingCore.Side.LONG ?
                        price - atr * riskMult :
                        price + atr * riskMult;

        double take =
                side == com.bot.TradingCore.Side.LONG ?
                        price + atr * riskMult * rr :
                        price - atr * riskMult * rr;

        registerSignal(symbol, side, now);

        return new TradeIdea(symbol, side, price, stop, take, probability, flags);
    }

    /* ===== COOLDOWN ===== */

    private boolean cooldownAllowed(String symbol, com.bot.TradingCore.Side side, CoinCategory cat, long now) {

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

    /* ===== FLIP ===== */

    private boolean flipAllowed(String symbol, com.bot.TradingCore.Side newSide) {

        Deque<String> history =
                recentDirections.computeIfAbsent(symbol, k -> new ArrayDeque<>());

        if (!history.isEmpty()) {

            String last = history.peekLast();

            if (!last.equals(newSide.name())) {

                if (history.size() >= 2) {

                    String prev = history.peekFirst();

                    if (prev.equals(newSide.name()))
                        return false;
                }
            }
        }

        return true;
    }

    private void registerSignal(String symbol, com.bot.TradingCore.Side side, long now) {

        String key = symbol + "_" + side;
        cooldownMap.put(key, now);

        Deque<String> history =
                recentDirections.computeIfAbsent(symbol, k -> new ArrayDeque<>());

        history.addLast(side.name());

        if (history.size() > 3)
            history.removeFirst();
    }

    private double computeConfidence(double rawScore,
                                     MarketState state,
                                     CoinCategory cat,
                                     double atr,
                                     double price) {

        // Подкорректированное смещение edge
        double edge = rawScore - 3.0; // раньше было 2.25
        double sigmoid = 1.0 / (1.0 + Math.exp(-1.2 * edge)); // раньше -1.4

        double regimeBoost = state == MarketState.STRONG_TREND ? 0.05 : 0;

        double categoryPenalty =
                cat == CoinCategory.MEME ? -0.04 :
                        cat == CoinCategory.ALT ? -0.015 :
                                0;

        double atrFactor =
                atr / price < 0.003 ? 0.05 :
                        atr / price > 0.01 ? -0.03 :
                                0;

        double rawProb = clamp(sigmoid + regimeBoost + categoryPenalty + atrFactor, 0, 1);

        // Привязка Probability к реальной шкале, но ограничиваем max 80-82
        return clamp(50 + rawProb * 30, 50, 82);
    }

    /* ===== STATE ===== */

    private MarketState detectState(List<com.bot.TradingCore.Candle> c) {

        double adx = adx(c, 14);

        if (adx > 26) return MarketState.STRONG_TREND;
        if (adx > 18) return MarketState.WEAK_TREND;

        return MarketState.RANGE;
    }

    private HTFBias detectBias(List<com.bot.TradingCore.Candle> c) {

        if (!valid(c)) return HTFBias.NONE;

        double ema50 = ema(c, 50);
        double ema200 = ema(c, 200);

        if (ema50 > ema200) return HTFBias.BULL;
        if (ema50 < ema200) return HTFBias.BEAR;

        return HTFBias.NONE;
    }

    /* ===== INDICATORS ===== */

    public double atr(List<com.bot.TradingCore.Candle> c, int n) {

        if (c.size() < n + 1) return 0;

        double sum = 0;

        for (int i = c.size() - n; i < c.size(); i++) {

            var cur = c.get(i);
            var prev = c.get(i - 1);

            double tr = Math.max(
                    cur.high - cur.low,
                    Math.max(
                            Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)
                    )
            );

            sum += tr;
        }

        return sum / n;
    }

    private double adx(List<com.bot.TradingCore.Candle> c, int n) {

        if (c.size() < n + 1) return 15;

        double trSum = 0;
        double plusDM = 0;
        double minusDM = 0;

        for (int i = c.size() - n; i < c.size(); i++) {

            var cur = c.get(i);
            var prev = c.get(i - 1);

            double highDiff = cur.high - prev.high;
            double lowDiff = prev.low - cur.low;

            double tr = Math.max(
                    cur.high - cur.low,
                    Math.max(
                            Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)
                    )
            );

            trSum += tr;

            if (highDiff > lowDiff && highDiff > 0)
                plusDM += highDiff;

            if (lowDiff > highDiff && lowDiff > 0)
                minusDM += lowDiff;
        }

        double atr = trSum / n;

        double plusDI = 100 * (plusDM / n) / atr;
        double minusDI = 100 * (minusDM / n) / atr;

        return 100 * Math.abs(plusDI - minusDI) /
                Math.max(plusDI + minusDI, 1);
    }

    private double ema(List<com.bot.TradingCore.Candle> c, int p) {

        if (c.size() < p) return last(c).close;

        double k = 2.0 / (p + 1);

        double e = c.get(c.size() - p).close;

        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);

        return e;
    }

    private boolean bullDiv(List<com.bot.TradingCore.Candle> c) {

        if (c.size() < 25) return false;

        int i1 = c.size() - 5;
        int i2 = c.size() - 1;

        double low1 = c.get(i1).low;
        double low2 = c.get(i2).low;

        double rsi1 = rsi(c.subList(0, i1 + 1), 14);
        double rsi2 = rsi(c, 14);

        return low2 < low1 && rsi2 > rsi1;
    }

    private boolean bearDiv(List<com.bot.TradingCore.Candle> c) {

        if (c.size() < 25) return false;

        int i1 = c.size() - 5;
        int i2 = c.size() - 1;

        double high1 = c.get(i1).high;
        double high2 = c.get(i2).high;

        double rsi1 = rsi(c.subList(0, i1 + 1), 14);
        double rsi2 = rsi(c, 14);

        return high2 > high1 && rsi2 < rsi1;
    }

    public boolean impulse(List<com.bot.TradingCore.Candle> c) {

        if (c == null || c.size() < 5) return false;

        double atrVal = atr(c, 14);

        return Math.abs(last(c).close - c.get(c.size() - 5).close)
                > atrVal * 0.12;
    }

    public boolean volumeSpike(List<com.bot.TradingCore.Candle> c, CoinCategory cat) {

        if (c.size() < 10) return false;

        double avg =
                c.subList(c.size() - 10, c.size() - 1)
                        .stream()
                        .mapToDouble(cd -> cd.volume)
                        .average()
                        .orElse(1);

        double lastVol = last(c).volume;

        double threshold =
                cat == CoinCategory.MEME ? 1.25 :
                        cat == CoinCategory.ALT ? 1.18 :
                                1.10;

        return lastVol / avg > threshold;
    }

    private boolean pullback(List<com.bot.TradingCore.Candle> c, boolean bull) {

        double ema21 = ema(c, 21);

        double price = last(c).close;

        return bull
                ? price <= ema21 * 0.996
                : price >= ema21 * 1.004;
    }

    private com.bot.TradingCore.Candle last(List<com.bot.TradingCore.Candle> c) {

        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> c) {

        return c != null && c.size() >= MIN_BARS;
    }

    private double clamp(double v, double min, double max) {

        return Math.max(min, Math.min(max, v));
    }

    public double rsi(List<com.bot.TradingCore.Candle> c, int period) {

        if (c.size() < period + 1) return 50.0;

        double gain = 0;
        double loss = 0;

        for (int i = c.size() - period; i < c.size(); i++) {

            double change = c.get(i).close - c.get(i - 1).close;

            if (change > 0)
                gain += change;
            else
                loss -= change;
        }

        double rs = loss == 0 ? 100 : gain / loss;

        return 100 - (100 / (1 + rs));
    }

    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             CoinCategory cat) {

        long now = System.currentTimeMillis();

        return generate(symbol, c1, c5, c15, c1h, cat, now);
    }
}