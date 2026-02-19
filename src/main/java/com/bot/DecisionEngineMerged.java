package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Профессиональная версия DecisionEngineMerged.
 * Улучшена надежность прогнозов, более точный расчёт confidence, фильтры трендов и паттернов.
 */
public final class DecisionEngineMerged {

    public enum SignalGrade { A, B }

    private enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, CLIMAX }

    private enum HTFBias { BULL, BEAR, NONE }

    public static final class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final SignalGrade grade;
        public final String reason;

        public TradeIdea(String symbol,
                         TradingCore.Side side,
                         double entry,
                         double stop,
                         double take,
                         double confidence,
                         SignalGrade grade,
                         String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.grade = grade;
            this.reason = reason;
        }
    }

    private static final int MIN_BARS = 200;
    private static final long COOLDOWN_MS = 45 * 60_000;
    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    /**
     * Основной метод: оценивает все символы и генерирует торговые идеи.
     */
    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> m15,
                                    Map<String, List<TradingCore.Candle>> h1) {

        List<TradeIdea> ideas = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (String symbol : symbols) {
            List<TradingCore.Candle> c15 = m15.get(symbol);
            List<TradingCore.Candle> c1h = h1.get(symbol);

            if (!isValid(c15) || !isValid(c1h)) continue;

            MarketState state = detectMarketState(c15);
            HTFBias bias = detectHTFBias(c1h);

            TradeIdea idea = generateTradeIdea(symbol, c15, state, bias, now);
            if (idea != null) ideas.add(idea);
        }

        ideas.sort(Comparator.comparingDouble((TradeIdea t) -> t.confidence).reversed());
        return ideas;
    }

    /**
     * Генерация одной торговой идеи на основе анализа свечей, тренда и паттернов.
     */
    private TradeIdea generateTradeIdea(String symbol,
                                        List<TradingCore.Candle> c,
                                        MarketState state,
                                        HTFBias bias,
                                        long now) {

        double price = last(c).close;
        double atr = atr(c, 14);
        double adx = adx(c, 14);
        double rsi = rsi(c, 14);

        TradingCore.Side side = null;
        String reason = null;

        switch (state) {
            case STRONG_TREND -> {
                if (isBullTrend(c) && bias == HTFBias.BULL) {
                    side = TradingCore.Side.LONG;
                    reason = "Strong trend continuation (bullish)";
                }
                if (isBearTrend(c) && bias == HTFBias.BEAR) {
                    side = TradingCore.Side.SHORT;
                    reason = "Strong trend continuation (bearish)";
                }
            }

            case WEAK_TREND -> {
                if (pullbackLong(c, rsi) && bias == HTFBias.BULL) {
                    side = TradingCore.Side.LONG;
                    reason = "Pullback entry (bullish)";
                }
                if (pullbackShort(c, rsi) && bias == HTFBias.BEAR) {
                    side = TradingCore.Side.SHORT;
                    reason = "Pullback entry (bearish)";
                }
            }

            case CLIMAX -> {
                if (reversalSignal(c, rsi, TradingCore.Side.LONG)) {
                    side = TradingCore.Side.LONG;
                    reason = "Exhaustion reversal (bullish)";
                }
                if (reversalSignal(c, rsi, TradingCore.Side.SHORT)) {
                    side = TradingCore.Side.SHORT;
                    reason = "Exhaustion reversal (bearish)";
                }
            }

            default -> {}
        }

        if (side == null) return null;

        // Проверка cooldown для одной стороны
        String key = symbol + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < COOLDOWN_MS) return null;

        // Надежность прогноза
        double confidence = computeConfidence(c, state, adx, rsi);

        // Расчет стопа и тейка
        double risk = atr * 1.2;
        double rr = confidence > 0.75 ? 2.8 : 2.0;

        double stop = side == TradingCore.Side.LONG ? price - risk : price + risk;
        double take = side == TradingCore.Side.LONG ? price + risk * rr : price - risk * rr;

        cooldown.put(key, now);

        return new TradeIdea(symbol, side, price, stop, take, confidence,
                confidence > 0.75 ? SignalGrade.A : SignalGrade.B, reason);
    }

    /* ===================== ЛОГИКА РЫНКА ===================== */

    private MarketState detectMarketState(List<TradingCore.Candle> c) {
        double adx = adx(c, 14);
        double vol = relativeVolume(c);

        if (adx > 28) return MarketState.STRONG_TREND;
        if (adx > 18) return MarketState.WEAK_TREND;
        if (vol > 1.8) return MarketState.CLIMAX;
        return MarketState.RANGE;
    }

    private boolean isBullTrend(List<TradingCore.Candle> c) {
        return ema(c, 21) > ema(c, 50) && last(c).close > ema(c, 21) && higherHighs(c);
    }

    private boolean isBearTrend(List<TradingCore.Candle> c) {
        return ema(c, 21) < ema(c, 50) && last(c).close < ema(c, 21) && lowerLows(c);
    }

    private boolean pullbackLong(List<TradingCore.Candle> c, double rsi) {
        return ema(c, 21) > ema(c, 50) && rsi < 45 && last(c).close > ema(c, 50);
    }

    private boolean pullbackShort(List<TradingCore.Candle> c, double rsi) {
        return ema(c, 21) < ema(c, 50) && rsi > 55 && last(c).close < ema(c, 50);
    }

    private boolean reversalSignal(List<TradingCore.Candle> c, double rsi, TradingCore.Side side) {
        TradingCore.Candle last = last(c);
        double body = Math.abs(last.close - last.open);
        double range = last.high - last.low;
        if (range == 0) return false;

        boolean exhaustion = body / range < 0.3;
        return (side == TradingCore.Side.LONG && rsi < 30 && exhaustion) ||
                (side == TradingCore.Side.SHORT && rsi > 70 && exhaustion);
    }

    /* ===================== CONFIDENCE ===================== */

    private double computeConfidence(List<TradingCore.Candle> c, MarketState state, double adx, double rsi) {
        double structure = Math.abs(ema(c, 21) - ema(c, 50)) / ema(c, 50) * 10;
        double vol = relativeVolume(c);

        double base = switch (state) {
            case STRONG_TREND -> 0.74;
            case WEAK_TREND -> 0.66;
            case CLIMAX -> 0.60;
            default -> 0.55;
        };

        double conf = base + structure * 0.05 + (adx / 50.0) * 0.1 + (vol - 1) * 0.05;

        // Более строгий диапазон для достоверности
        return clamp(conf, 0.52, 0.90);
    }

    /* ===================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===================== */

    private HTFBias detectHTFBias(List<TradingCore.Candle> c) {
        if (ema(c, 50) > ema(c, 200)) return HTFBias.BULL;
        if (ema(c, 50) < ema(c, 200)) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    private boolean higherHighs(List<TradingCore.Candle> c) {
        int n = c.size();
        return c.get(n - 1).high > c.get(n - 2).high && c.get(n - 2).high > c.get(n - 3).high;
    }

    private boolean lowerLows(List<TradingCore.Candle> c) {
        int n = c.size();
        return c.get(n - 1).low < c.get(n - 2).low && c.get(n - 2).low < c.get(n - 3).low;
    }

    private double relativeVolume(List<TradingCore.Candle> c) {
        int n = c.size();
        double avg = c.subList(n - 20, n - 1).stream().mapToDouble(cd -> cd.volume).average().orElse(0);
        return last(c).volume / avg;
    }

    private double atr(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++) {
            TradingCore.Candle cur = c.get(i);
            TradingCore.Candle prev = c.get(i - 1);
            double tr = Math.max(cur.high - cur.low, Math.max(Math.abs(cur.high - prev.close), Math.abs(cur.low - prev.close)));
            sum += tr;
        }
        return sum / n;
    }

    private double adx(List<TradingCore.Candle> c, int n) {
        double move = 0;
        for (int i = c.size() - n; i < c.size() - 1; i++) move += Math.abs(c.get(i + 1).close - c.get(i).close);
        return move / n / atr(c, n) * 25;
    }

    private double rsi(List<TradingCore.Candle> c, int n) {
        double gain = 0, loss = 0;
        for (int i = c.size() - n; i < c.size() - 1; i++) {
            double diff = c.get(i + 1).close - c.get(i).close;
            if (diff > 0) gain += diff;
            else loss -= diff;
        }
        if (loss == 0) return 100;
        double rs = gain / loss;
        return 100 - (100 / (1 + rs));
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++) e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean isValid(List<?> c) {
        return c != null && c.size() >= MIN_BARS;
    }

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}
