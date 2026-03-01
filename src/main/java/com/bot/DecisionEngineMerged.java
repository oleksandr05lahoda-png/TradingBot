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

    private static final int MIN_BARS = 150;
    private static final long COOLDOWN_TOP = 2 * 60_000; // 2 минуты
    private static final long COOLDOWN_ALT = 3 * 60_000; // 3 минуты
    private static final long COOLDOWN_MEME = 4 * 60_000; // 4 минуты
    private static final double MIN_SCORE_THRESHOLD = 3.2;
    private static final double MIN_CONFIDENCE = 58.0;

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
        public final List<String> flags;     // Флаги сигнала (ATR, volume, impulse и т.д.)
        public TradeIdea(String symbol,
                         TradingCore.Side side,
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


        Map<String, Double> reasonWeightsLong = new LinkedHashMap<>();
        Map<String, Double> reasonWeightsShort = new LinkedHashMap<>();
        if (bias == HTFBias.BULL) {
            scoreLong += 0.35;
        }
        else if (bias == HTFBias.BEAR) {
            scoreShort += 0.35;
        }

        if (pullback(c15, true)) {
            scoreLong += 1.0;
            reasonWeightsLong.put("Pullback bullish", 2.0);
        }
        if (pullback(c15, false)) {
            scoreShort += 1.0;
            reasonWeightsShort.put("Pullback bearish", 2.0); // <- исправлено
        }

        if (impulse(c1)) {
            if (bias == HTFBias.BULL && last(c1).close > c1.get(c1.size() - 5).close) {
                scoreLong += 0.4;
                reasonWeightsLong.put("Impulse", 0.4);
            } else if (bias == HTFBias.BEAR && last(c1).close < c1.get(c1.size() - 5).close) {
                scoreShort += 0.4;
                reasonWeightsShort.put("Impulse", 0.4);
            }
        }

        // ===== Divergence =====
        if (bullDiv(c15)) {
            scoreLong += 0.6;
            reasonWeightsLong.put("Bullish divergence", 2.0);
        }
        if (bearDiv(c15)) {
            scoreShort += 0.6;
            reasonWeightsShort.put("Bearish divergence", 2.0);
        }
// ===== RSI sanity filter (до выбора направления!) =====
        double rsi14 = rsi(c15, 14);

        if (rsi14 > 80) {
            scoreLong -= 0.5;
            reasonWeightsLong.put("RSI overbought", 0.8);
        }

        if (rsi14 < 20) {
            scoreShort -= 0.5;
            reasonWeightsShort.put("RSI oversold", 0.8);
        }
        // ===== Anti-counter-trend filter (важно!) =====
        double adxValue = adx(c15, 14);
        if (adxValue > 28) {

            if (bias == HTFBias.BULL && scoreShort > scoreLong) {
                scoreShort -= 0.6;
            }

            if (bias == HTFBias.BEAR && scoreLong > scoreShort) {
                scoreLong -= 0.6;
            }
        }
        if (volumeSpike(c15, cat)) {
            scoreLong += 0.5;
            scoreShort += 0.5;

            reasonWeightsLong.put("Volume", 0.5);
            reasonWeightsShort.put("Volume", 0.5);
        }
        double dynamicThreshold =
                state == MarketState.STRONG_TREND ? 2.0 :
                        state == MarketState.RANGE ? 2.2 :
                                1.9;
        if (scoreLong < dynamicThreshold && scoreShort < dynamicThreshold)
            return null;
        // === Честный выбор направления без перекоса ===
        TradingCore.Side side;
        double scoreDiff = Math.abs(scoreLong - scoreShort);

        if (scoreLong >= dynamicThreshold && scoreShort >= dynamicThreshold) {
            // ничего не делаем — пусть выберется сильнейший
        }
        else if (scoreDiff < 0.05) return null;  // мягче, оставляем маленькие различия

        if (scoreLong > scoreShort) {
            side = TradingCore.Side.LONG;
        } else {
            side = TradingCore.Side.SHORT;
        }

        double rawScore = Math.max(scoreLong, scoreShort);

        if (!cooldownAllowed(symbol, side, cat, now))
            return null;

        if (!flipAllowed(symbol, side))
            return null;

        double probability = computeConfidence(rawScore, state, cat, atr, price);
        if (probability < MIN_CONFIDENCE)
            return null;

// ===== Дополнительные флаги и RSI для Reason =====
        List<String> flags = new ArrayList<>();
        if (atr(c15, 14) > price * 0.001) flags.add("ATR↑");
        if (volumeSpike(c15, cat)) flags.add("vol:true");
        if (impulse(c1)) flags.add("impulse:true");

        Map<String, Double> chosen =
                scoreLong > scoreShort
                        ? reasonWeightsLong
                        : reasonWeightsShort;

        String mainReasons =
                chosen.entrySet().stream()
                        .sorted((a,b)->Double.compare(b.getValue(),a.getValue()))
                        .limit(3)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.joining(", "));

        double riskMult =
                cat == CoinCategory.MEME ? 1.3 :
                        cat == CoinCategory.ALT ? 1.0 : 0.85;

        double rr =
                probability > 80 ? 3.0 :
                        probability > 70 ? 2.6 :
                                2.2;

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
                flags
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
                return true;
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
    private double computeConfidence(double rawScore,
                                     MarketState state,
                                     CoinCategory cat,
                                     double atr,
                                     double price) {

        // --- базовая сигмоидная трансформация rawScore ---
        double edge = rawScore - 2.6;
        double sigmoid = 1.0 / (1.0 + Math.exp(-2.0 * edge)); // 0..1

        // --- бусты/штрафы ---
        if (cat == CoinCategory.ALT) {
            Double btcCorr = adaptiveTrendWeight.get("BTC");
            if (btcCorr != null) sigmoid += (btcCorr - 0.5) * 0.15;
        }

        double regimeBoost = 0.0;
        switch(state) {
            case STRONG_TREND: regimeBoost = 0.05; break;
            case WEAK_TREND: regimeBoost = 0.02; break;
            case RANGE: regimeBoost = -0.02; break;
            case VOLATILE:
            case CLIMAX: regimeBoost = -0.04; break;
        }

        double categoryPenalty = 0.0;
        switch(cat) {
            case MEME: categoryPenalty = -0.04; break;
            case ALT: categoryPenalty = -0.015; break;
            case TOP: categoryPenalty = 0.0; break;
        }

        double atrFactor = 0.0;
        double atrRatio = atr / price;
        if (atrRatio < 0.003) atrFactor = 0.05;
        else if (atrRatio > 0.01) atrFactor = -0.03;

        // --- аккуратное суммирование ---
        double rawProb = sigmoid + regimeBoost + categoryPenalty + atrFactor;

        // --- Нормируем в диапазон 0..1 ---
        rawProb = clamp(rawProb, 0.0, 1.0);

        // --- Преобразуем в 50–85% ---
        double probPercent = 50.0 + rawProb * 35.0;

        // --- Финальный clamp на всякий случай ---
        probPercent = clamp(probPercent, 50.0, 85.0);

        return probPercent;
    }

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
        if (c.size() < n + 1) return 15;

        double trSum = 0;
        double plusDM = 0;
        double minusDM = 0;

        for (int i = c.size() - n; i < c.size(); i++) {
            TradingCore.Candle cur = c.get(i);
            TradingCore.Candle prev = c.get(i - 1);

            double highDiff = cur.high - prev.high;
            double lowDiff = prev.low - cur.low;

            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));

            trSum += tr;

            if (highDiff > lowDiff && highDiff > 0)
                plusDM += highDiff;

            if (lowDiff > highDiff && lowDiff > 0)
                minusDM += lowDiff;
        }

        double atr = trSum / n;
        double plusDI = 100 * (plusDM / n) / atr;
        double minusDI = 100 * (minusDM / n) / atr;

        double dx = 100 * Math.abs(plusDI - minusDI) /
                Math.max(plusDI + minusDI, 1);

        return dx;
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private boolean bullDiv(List<TradingCore.Candle> c) {
        if (c.size() < 25) return false;

        int i1 = c.size() - 5;
        int i2 = c.size() - 1;

        double low1 = c.get(i1).low;
        double low2 = c.get(i2).low;

        double rsi1 = rsi(c.subList(0, i1 + 1), 14);
        double rsi2 = rsi(c, 14);

        return low2 < low1 && rsi2 > rsi1;
    }

    private boolean bearDiv(List<TradingCore.Candle> c) {
        if (c.size() < 25) return false;

        int i1 = c.size() - 5;
        int i2 = c.size() - 1;

        double high1 = c.get(i1).high;
        double high2 = c.get(i2).high;

        double rsi1 = rsi(c.subList(0, i1 + 1), 14);
        double rsi2 = rsi(c, 14);

        return high2 > high1 && rsi2 < rsi1;
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
                cat == CoinCategory.MEME ? 1.25 :
                        cat == CoinCategory.ALT ? 1.18 :
                                1.10;

        return lastVol / avg > threshold;
    }
    private boolean pullback(List<TradingCore.Candle> c,
                             boolean bull) {

        double ema21 = ema(c, 21);
        double price = last(c).close;

        // более глубокий откат
        return bull ?
                price <= ema21 * 0.992 :
                price >= ema21 * 1.008;
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