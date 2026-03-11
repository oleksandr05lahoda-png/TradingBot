package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    public enum CoinCategory { TOP, ALT, MEME }
    public enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, VOLATILE }
    public enum HTFBias { BULL, BEAR, NONE }

    private static final int MIN_BARS = 150;

    private static final long COOLDOWN_TOP = 15 * 60_000;
    private static final long COOLDOWN_ALT = 15 * 60_000;
    private static final long COOLDOWN_MEME = 15 * 60_000;
    private final Map<String, Double> lastSignalPrice = new ConcurrentHashMap<>();
    private static final double MIN_CONFIDENCE = 54.0;

    private final Map<String, Long> cooldownMap = new ConcurrentHashMap<>();

    private final Map<String, Deque<String>> recentDirections = new ConcurrentHashMap<>();
    public DecisionEngineMerged() {
    }
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

        @Override
        public String toString() {

            String flagStr =
                    flags == null || flags.isEmpty()
                            ? "-"
                            : String.join(", ", flags);

            String time =
                    java.time.ZonedDateTime
                            .now(java.time.ZoneId.of("Europe/Warsaw"))
                            .toLocalTime()
                            .withNano(0)
                            .toString();

            return String.format(
                    "*%s* → *%s*\n" +
                            "Price: %.6f\n" +
                            "Probability: %.0f%%\n" +
                            "Stop-Take: %.6f - %.6f\n" +
                            "Flags: %s\n" +
                            "_time: %s_",
                    symbol,
                    side,
                    price,
                    probability,
                    stop,
                    take,
                    flagStr,
                    time
            );
        }
    }

    private TradeIdea generate(String symbol,
                               List<com.bot.TradingCore.Candle> c1,
                               List<com.bot.TradingCore.Candle> c5,
                               List<com.bot.TradingCore.Candle> c15,
                               List<com.bot.TradingCore.Candle> c1h,
                               CoinCategory cat,
                               long now) {

        if (!valid(c15) || !valid(c1h)) {
            System.out.println("[DEBUG-DE] Not enough bars for " + symbol +
                    " | c15=" + (c15 != null ? c15.size() : "null") +
                    " | c1h=" + (c1h != null ? c1h.size() : "null"));
            return null;
        }

        double price = last(c15).close;
        double atr = atr(c15, 14);

        if (atr <= 0)
            return null;

        atr = Math.max(atr, price * 0.0012);

        MarketState state = detectState(c15);
        HTFBias bias = detectBias(c15);
        double scoreLong = 0;
        double scoreShort = 0;

        Map<String, Double> reasonWeightsLong = new LinkedHashMap<>();
        Map<String, Double> reasonWeightsShort = new LinkedHashMap<>();

        System.out.println("[DEBUG-DE] Start generating for symbol=" + symbol +
                " | state=" + state +
                " | bias=" + bias +
                " | last price=" + price);

        if (bias == HTFBias.BULL) {
            scoreLong += 0.35;
            scoreShort -= 0.10;
        } else if (bias == HTFBias.BEAR) {
            scoreShort += 0.35;
            scoreLong -= 0.10;
        }

        boolean pullbackUpFlag = pullback(c15, true);
        boolean pullbackDownFlag = pullback(c15, false);
        boolean impulseFlag = impulse(c1);
        boolean compressionFlag = volatilityCompression(c15);
        boolean pumpFlag = pumpBreakout(c1, cat);
        boolean bullDivFlag = bullDiv(c15);
        boolean bearDivFlag = bearDiv(c15);

        if (pullbackUpFlag) {
            scoreLong += 0.9;
            reasonWeightsLong.put("Pullback bullish", 0.9);
        }

        if (pullbackDownFlag) {
            scoreShort += 0.9;
            reasonWeightsShort.put("Pullback bearish", 0.9);
        }

        if (impulseFlag) {

            double atr1 = atr(c1, 14);
            double delta = last(c1).close - c1.get(c1.size() - 5).close;

            double impulseStrength = Math.abs(delta) / atr1;

            if (delta > 0 && impulseStrength > 0.28)
                scoreLong += state == MarketState.STRONG_TREND ? 0.45 : 0.35;

            if (delta < 0 && impulseStrength > 0.28)
                scoreShort += state == MarketState.STRONG_TREND ? 0.45 : 0.35;
        }
        /* ===== Compression bonus ===== */

        if (compressionFlag) {

            if (scoreLong > scoreShort)
                scoreLong += 0.20;
            else
                scoreShort += 0.20;
        }

        /* ===== Pump breakout ===== */

        if (pumpFlag) {

            if (scoreLong > scoreShort)
                scoreLong += 0.35;
            else
                scoreShort += 0.35;
        }
        if (bullDivFlag) {
            scoreLong += 0.55;
            reasonWeightsLong.put("Bullish divergence", 0.55);
        }

        if (bearDivFlag) {
            scoreShort += 0.55;
            reasonWeightsShort.put("Bearish divergence", 0.55);
        }

        /* ===== RSI soft filter ===== */
        double rsi14 = rsi(c15, 14);
        if (state != MarketState.STRONG_TREND) {

            if (rsi14 > 75)
                scoreLong -= 0.18;

            if (rsi14 < 25)
                scoreShort -= 0.18;
        }
        double adxValue = adx(c15, 14);
        if (adxValue > 30) {
            if (bias == HTFBias.BULL && scoreShort > scoreLong) scoreShort *= 0.78;
            if (bias == HTFBias.BEAR && scoreLong > scoreShort) scoreLong *= 0.78;
        }
        double dynamicThreshold = state == MarketState.STRONG_TREND ? 0.80 : 0.68;

        if (scoreLong < dynamicThreshold && scoreShort < dynamicThreshold) {
            // позволяем сигналу пройти если есть дивергенция
            if (!bullDivFlag && !bearDivFlag)
                return null;
        }

        double move4 = (last(c15).close - c15.get(c15.size() - 4).close) / price;

        if (move4 > 0.015 && scoreShort > scoreLong) scoreShort *= 0.88;
        if (move4 < -0.015 && scoreLong > scoreShort) scoreLong *= 0.88;

        double scoreDiff = Math.abs(scoreLong - scoreShort);
        if (scoreDiff < 0.22) return null;

        com.bot.TradingCore.Side side = scoreLong > scoreShort ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT;

        /* ===== Cooldown & flip ===== */
        if (!cooldownAllowed(symbol, side, cat, now)) return null;
        if (!flipAllowed(symbol, side)) return null;

        /* ===== Probability (честная, учитывает все сигналы) ===== */
        double probability = computeConfidence(
                scoreLong, scoreShort,
                state, cat,
                atr, price,
                bullDivFlag, bearDivFlag,
                pullbackUpFlag, pullbackDownFlag,
                impulseFlag
        );

        if (probability < MIN_CONFIDENCE) {
            System.out.println("[DEBUG-DE] probability < MIN_CONFIDENCE → rejected");
            return null;
        }

        System.out.println("[DEBUG-DE] symbol=" + symbol +
                " | scoreLong=" + scoreLong +
                " | scoreShort=" + scoreShort +
                " | probability=" + probability +
                " | atr=" + atr +
                " | price=" + price);

        List<String> flags = new ArrayList<>();

        if (atr / price > 0.0015) flags.add("ATR↑");
        if (volumeSpike(c15, cat) && atr / price > 0.0015) flags.add("vol:true");
        if (impulseFlag) flags.add("impulse:true");
        if (compressionFlag) flags.add("compression:true");
        if (pumpFlag) flags.add("pump:true");

        /* ===== Risk & Stop/Take ===== */
        double riskMult = cat == CoinCategory.MEME ? 1.3 : cat == CoinCategory.ALT ? 1.0 : 0.85;
        double rr = scoreDiff > 0.9 ? 3.0 : scoreDiff > 0.6 ? 2.6 : 2.2;

        double stop = side == com.bot.TradingCore.Side.LONG ? price - atr * riskMult : price + atr * riskMult;
        double take = side == com.bot.TradingCore.Side.LONG ? price + atr * riskMult * rr : price - atr * riskMult * rr;

        if (!priceMovedEnough(symbol, price)) return null;
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

    private boolean flipAllowed(String symbol, com.bot.TradingCore.Side newSide) {

        Deque<String> history =
                recentDirections.computeIfAbsent(symbol, k -> new ArrayDeque<>());

        if (history.size() < 2)
            return true;

        Iterator<String> it = history.descendingIterator();

        String last = it.next();
        String prev = it.next();

        if (!last.equals(newSide.name()) && prev.equals(newSide.name()))
            return false;

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
    private double computeConfidence(double scoreLong, double scoreShort,
                                     MarketState state, CoinCategory cat,
                                     double atr, double price,
                                     boolean bullDiv, boolean bearDiv,
                                     boolean pullbackUp, boolean pullbackDown,
                                     boolean impulse) {

        // 1️⃣ Разница между LONG и SHORT
        double scoreDiff = Math.abs(scoreLong - scoreShort);

        // 2️⃣ Нормируем по реальному максимуму сигналов
        double maxScore = 2.45; // HTF + pullback + дивергенции + импульс
        double normScore = scoreDiff / maxScore;

        // 3️⃣ Дополнительные бонусы от индикаторов
        if (bullDiv || bearDiv) normScore += 0.05;
        if (pullbackUp || pullbackDown) normScore += 0.05;
        if (impulse) normScore += 0.03;

        // 4️⃣ Ограничиваем 0..1
        normScore = Math.min(1.0, normScore);

        double probability = 50 + normScore * 45;

        // 6️⃣ Корректировка по тренду и категории (необязательно)
        if (state == MarketState.STRONG_TREND) probability += 3;  // максимум ~83%
        else if (state == MarketState.WEAK_TREND) probability += 1; // максимум ~81%

        if (cat == CoinCategory.MEME) probability -= 5; // MEME чуть снижаем
        else if (cat == CoinCategory.ALT) probability -= 2;

        // 7️⃣ Ограничиваем окончательно
        probability = clamp(probability, 50, 85);

        // 8️⃣ Округляем до целого %
        return Math.round(probability);
    }
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
                > atrVal * 0.25;
    }
    /* ===== PUMP BREAKOUT ===== */

    private boolean pumpBreakout(List<com.bot.TradingCore.Candle> c, CoinCategory cat) {

        if (c.size() < 12) return false;

        double atrVal = atr(c, 14);

        double move =
                Math.abs(last(c).close - c.get(c.size() - 6).close);

        double strength = move / atrVal;

        double threshold =
                cat == CoinCategory.MEME ? 0.75 :
                        cat == CoinCategory.ALT ? 0.85 :
                                1.0;

        return strength > threshold;
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
    /* ===== VOLATILITY COMPRESSION ===== */

    private boolean volatilityCompression(List<com.bot.TradingCore.Candle> c) {

        if (c.size() < 30) return false;

        double atrNow = atr(c, 14);

        double atrPast = 0;

        for (int i = c.size() - 30; i < c.size() - 14; i++) {

            var cur = c.get(i);
            var prev = c.get(i - 1);

            double tr = Math.max(
                    cur.high - cur.low,
                    Math.max(
                            Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)
                    )
            );

            atrPast += tr;
        }

        atrPast /= 16;

        return atrNow < atrPast * 0.65;
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
    private boolean priceMovedEnough(String symbol, double price) {

        Double last = lastSignalPrice.get(symbol);

        if (last == null) {
            lastSignalPrice.put(symbol, price);
            return true;
        }

        double diff = Math.abs(price - last) / last;

        if (diff < 0.0022)
            return false;

        lastSignalPrice.put(symbol, price);

        return true;
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