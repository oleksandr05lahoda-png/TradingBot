package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class Elite5MinAnalyzer {

    public enum SignalGrade { A, B, C }
    public enum CoinType { TOP, ALT, MEME }
    private enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, CLIMAX, VOLATILE }
    private enum HTFBias { BULL, BEAR, NONE }

    private static final int MIN_M15 = 150;
    private static final int MIN_H1 = 150;
    private static final double MIN_CONFIDENCE = 0.52;
    private static final long BASE_COOLDOWN = 7 * 60_000;

    private static final double W_HTF = 2.0;
    private static final double W_MICRO_IMPULSE = 2.2;
    private static final double W_MICRO_BREAK = 1.8;
    private static final double W_PULLBACK = 1.5;
    private static final double W_RANGE = 1.2;
    private static final double W_VOLUME = 1.5;
    private static final double W_DIVERGENCE = 1.4;
    private static final double W_REVERSAL = 3.0; // вес для раннего разворота

    private final Map<String, Deque<Long>> cooldownHistory = new ConcurrentHashMap<>();
    private final Map<String, Double> adaptiveTrendWeights = new ConcurrentHashMap<>();

    public static final class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final SignalGrade grade;
        public final String reason;
        public final CoinType type;

        public TradeSignal(String symbol, TradingCore.Side side, double entry, double stop, double take,
                           double confidence, SignalGrade grade, String reason, CoinType type) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.grade = grade;
            this.reason = reason;
            this.type = type;
        }
    }

    public List<TradeSignal> analyze(List<String> symbols,
                                     Map<String, List<TradingCore.Candle>> m1,
                                     Map<String, List<TradingCore.Candle>> m5,
                                     Map<String, List<TradingCore.Candle>> m15,
                                     Map<String, List<TradingCore.Candle>> h1,
                                     Map<String, CoinType> types) {

        long now = System.currentTimeMillis();
        List<TradeSignal> candidates = new ArrayList<>();

        for (String symbol : symbols) {
            List<TradingCore.Candle> c1 = m1.get(symbol);
            List<TradingCore.Candle> c5 = m5.get(symbol);
            List<TradingCore.Candle> c15 = m15.get(symbol);
            List<TradingCore.Candle> c1h = h1.get(symbol);

            if (!valid(c15, MIN_M15) || !valid(c1h, MIN_H1)) continue;

            CoinType type = types.getOrDefault(symbol, CoinType.ALT);
            if (!volatilityOk(c15, type)) continue;

            TradeSignal signal = generate(symbol, c1, c5, c15, c1h, type, now);
            if (signal != null) candidates.add(signal);
        }

        // Сортируем все сигналы по уверенности без ограничения Top-N
        return candidates.stream()
                .sorted(Comparator.comparingDouble(s -> -s.confidence))
                .collect(Collectors.toList());
    }

    private TradeSignal generate(String symbol,
                                 List<TradingCore.Candle> c1,
                                 List<TradingCore.Candle> c5,
                                 List<TradingCore.Candle> c15,
                                 List<TradingCore.Candle> c1h,
                                 CoinType type,
                                 long now) {

        double price = last(c15).close;
        double atr = Math.max(atr(c15, 14), price * 0.002);

        HTFBias bias = detectHTFBias(c1h);
        MarketState state = detectMarketState(c15);
        double microTrend = computeMicroTrend(c1);

        double scoreLong = 0, scoreShort = 0;

        // ===== HTF TREND =====
        double trendWeight = adaptiveTrendWeights.getOrDefault(symbol, W_HTF);
        if (bias == HTFBias.BULL) scoreLong += trendWeight;
        if (bias == HTFBias.BEAR) scoreShort += trendWeight;

        // ===== MICRO IMPULSE =====
        boolean impulse = detectMicroImpulse(c1);
        if (impulse) {
            if (last(c1).close > c1.get(c1.size() - 5).close) scoreLong += W_MICRO_IMPULSE * microTrend;
            else scoreShort += W_MICRO_IMPULSE * microTrend;
        }

        // ===== MICRO BREAKOUT =====
        if (detectMicroBreakout(c5, true)) scoreLong += W_MICRO_BREAK;
        if (detectMicroBreakout(c5, false)) scoreShort += W_MICRO_BREAK;

        // ===== PULLBACK ENTRY =====
        if (pricePullback(c15, true)) scoreLong += W_PULLBACK;
        if (pricePullback(c15, false)) scoreShort += W_PULLBACK;

        // ===== RANGE REACTION =====
        if (state == MarketState.RANGE) {
            double high = highest(c15, 15), low = lowest(c15, 15);
            if (price <= low * 1.004) scoreLong += W_RANGE;
            if (price >= high * 0.996) scoreShort += W_RANGE;
        }

        // ===== VOLUME CONFIRM =====
        double relVol = relativeVolume(c1);
        if (relVol > 1.1) {
            scoreLong += W_VOLUME * 0.5;
            scoreShort += W_VOLUME * 0.5;
        }

        // ===== DIVERGENCE =====
        if (bullishDivergence(c15)) scoreLong += W_DIVERGENCE;
        if (bearishDivergence(c15)) scoreShort += W_DIVERGENCE;

        // ===== EARLY REVERSAL LOGIC =====
        if (earlyReversalLong(c15)) scoreLong += W_REVERSAL;
        if (earlyReversalShort(c15)) scoreShort += W_REVERSAL;

        if (scoreLong < 2.5 && scoreShort < 2.5) return null;

        TradingCore.Side side = scoreLong > scoreShort ? TradingCore.Side.LONG : TradingCore.Side.SHORT;

        // ===== COOL DOWN HISTORY (по каждому сигналу отдельно) =====
        String key = symbol + "_" + side;
        Deque<Long> history = cooldownHistory.computeIfAbsent(key, k -> new ArrayDeque<>());
        long cutoff = now - BASE_COOLDOWN;
        history.removeIf(t -> t < cutoff);
        history.addLast(now);

        double raw = Math.max(scoreLong, scoreShort);
        double confidence = computeConfidence(raw, state, type, atr, price, microTrend);
        if (confidence < MIN_CONFIDENCE) return null;

        SignalGrade grade = confidence > 0.80 ? SignalGrade.A :
                confidence > 0.65 ? SignalGrade.B : SignalGrade.C;

        double riskMult = type == CoinType.MEME ? 1.3 : type == CoinType.ALT ? 1.0 : 0.85;
        double rr = confidence > 0.75 ? 2.8 : 2.2;

        double stop = side == TradingCore.Side.LONG ? price - atr * riskMult : price + atr * riskMult;
        double take = side == TradingCore.Side.LONG ? price + atr * riskMult * rr : price - atr * riskMult * rr;

        updateAdaptiveWeight(symbol, side, confidence);

        return new TradeSignal(symbol, side, price, stop, take, confidence, grade,
                "Score=" + String.format("%.2f", raw) + " State=" + state + " Vol=" + String.format("%.2f", relVol), type);
    }

    // ===================== EARLY REVERSALS =====================
    private boolean earlyReversalLong(List<TradingCore.Candle> c) {
        if (c.size() < 10) return false;
        return last(c).low < lowest(c, 5) && rsi(c, 14) < 30 && rsi(c.subList(c.size() - 5, c.size()), 5) > 35;
    }

    private boolean earlyReversalShort(List<TradingCore.Candle> c) {
        if (c.size() < 10) return false;
        return last(c).high > highest(c, 5) && rsi(c, 14) > 70 && rsi(c.subList(c.size() - 5, c.size()), 5) < 65;
    }

    // ===================== HELPERS =====================
    private void updateAdaptiveWeight(String symbol, TradingCore.Side side, double confidence) {
        double current = adaptiveTrendWeights.getOrDefault(symbol, W_HTF);
        if (confidence > 0.75) current += 0.05;
        else if (confidence < 0.55) current -= 0.05;
        adaptiveTrendWeights.put(symbol, clamp(current, 1.2, 3.5));
    }

    private double computeConfidence(double raw, MarketState state, CoinType type, double atr, double price, double microTrend) {
        double base = raw / 6.5;
        double stateBoost = switch (state) {
            case STRONG_TREND -> 0.12;
            case WEAK_TREND -> 0.08;
            case RANGE -> 0.05;
            case CLIMAX -> 0.07;
            case VOLATILE -> 0.04;
        };
        double volatilityFactor = Math.min(atr / price * 40, 0.1);
        double typeBoost = type == CoinType.MEME ? 0.06 : type == CoinType.ALT ? 0.04 : 0.02;
        double microBoost = clamp(microTrend, 0.8, 1.2);
        return clamp(0.50 + base + stateBoost + volatilityFactor + typeBoost * microBoost, 0.50, 0.95);
    }

    private boolean detectMicroBreakout(List<TradingCore.Candle> c, boolean bullish) {
        if (c == null || c.size() < 6) return false;
        double high = highest(c, 5), low = lowest(c, 5), price = last(c).close;
        return bullish ? price > high * 1.001 : price < low * 0.999;
    }

    private boolean detectMicroImpulse(List<TradingCore.Candle> c) {
        if (c == null || c.size() < 5) return false;
        double delta = last(c).close - c.get(c.size() - 5).close;
        return Math.abs(delta) > 0.0002 && relativeVolume(c) > 1.05;
    }

    private boolean pricePullback(List<TradingCore.Candle> c, boolean bull) {
        double ema21 = ema(c, 21), price = last(c).close;
        return bull ? price <= ema21 * 1.01 : price >= ema21 * 0.99;
    }

    private double computeMicroTrend(List<TradingCore.Candle> c) {
        if (c == null || c.size() < 5) return 1.0;
        double delta = last(c).close - c.get(c.size() - 5).close;
        return 1.0 + Math.tanh(delta * 10);
    }

    private MarketState detectMarketState(List<TradingCore.Candle> c) {
        double adx = adx(c, 14), vol = relativeVolume(c);
        if (adx > 25) return MarketState.STRONG_TREND;
        if (adx > 18) return MarketState.WEAK_TREND;
        if (vol > 1.8) return MarketState.CLIMAX;
        if (vol > 1.3) return MarketState.VOLATILE;
        return MarketState.RANGE;
    }

    private HTFBias detectHTFBias(List<TradingCore.Candle> c) {
        if (c.size() < 200) return HTFBias.NONE;
        double ema50 = ema(c, 50), ema200 = ema(c, 200);
        if (ema50 > ema200 * 1.002) return HTFBias.BULL;
        if (ema50 < ema200 * 0.998) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    private double atr(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = Math.max(1, c.size() - n); i < c.size(); i++) {
            TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            double tr = Math.max(cur.high - cur.low, Math.max(Math.abs(cur.high - prev.close), Math.abs(cur.low - prev.close)));
            sum += tr;
        }
        return sum / n;
    }

    private double adx(List<TradingCore.Candle> c, int n) {
        double move = 0;
        for (int i = Math.max(0, c.size() - n); i < c.size() - 1; i++)
            move += Math.abs(c.get(i + 1).close - c.get(i).close);
        return move / n / atr(c, n) * 25;
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(Math.max(0, c.size() - p)).close;
        for (int i = Math.max(0, c.size() - p) + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private boolean bullishDivergence(List<TradingCore.Candle> c) {
        if (c.size() < 20) return false;
        return c.get(c.size() - 1).low < c.get(c.size() - 4).low &&
                rsi(c, 14) > rsi(c.subList(0, c.size() - 2), 14);
    }

    private boolean bearishDivergence(List<TradingCore.Candle> c) {
        if (c.size() < 20) return false;
        return c.get(c.size() - 1).high > c.get(c.size() - 4).high &&
                rsi(c, 14) < rsi(c.subList(0, c.size() - 2), 14);
    }

    private double rsi(List<TradingCore.Candle> c, int n) {
        double g = 0, l = 0;
        for (int i = Math.max(0, c.size() - n); i < c.size() - 1; i++) {
            double d = c.get(i + 1).close - c.get(i).close;
            if (d > 0) g += d; else l += Math.abs(d);
        }
        if (l == 0) return 100;
        return 100 - (100 / (1 + g / l));
    }

    private double highest(List<TradingCore.Candle> c, int n) {
        return c.subList(Math.max(0, c.size() - n), c.size()).stream().mapToDouble(cd -> cd.high).max().orElse(0);
    }

    private double lowest(List<TradingCore.Candle> c, int n) {
        return c.subList(Math.max(0, c.size() - n), c.size()).stream().mapToDouble(cd -> cd.low).min().orElse(0);
    }

    private double relativeVolume(List<TradingCore.Candle> c) {
        int n = c.size();
        double avg = c.subList(Math.max(0, n - 20), n - 1).stream().mapToDouble(cd -> cd.volume).average().orElse(1);
        return last(c).volume / avg;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c) { return c.get(c.size() - 1); }
    private boolean valid(List<?> c, int min) { return c != null && c.size() >= min; }
    private double clamp(double v, double min, double max) { return Math.max(min, Math.min(max, v)); }

    private boolean volatilityOk(List<TradingCore.Candle> c, CoinType type) {
        double a = atr(c, 14), price = last(c).close;
        double multiplier = type == CoinType.MEME ? 0.0008 : type == CoinType.ALT ? 0.0015 : 0.001;
        return a / price > multiplier;
    }
}