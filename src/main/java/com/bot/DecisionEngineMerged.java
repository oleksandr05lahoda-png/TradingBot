package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    /* ================= ENUMS ================= */
    public enum SignalGrade { A, B }
    private enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, CLIMAX }
    private enum HTFBias { BULL, BEAR, NONE }

    /* ================= CONFIG ================= */
    private static final int MIN_BARS = 200;
    private static final long COOLDOWN_MS = 12 * 60_000; // не душим частоту
    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    /* ================= OUTPUT ================= */
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

    /* ================= MAIN EVALUATION ================= */
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

    /* ================= SIGNAL LOGIC ================= */
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

            // 🔥 Early Trend Impulse
            case STRONG_TREND -> {
                if (bias == HTFBias.BULL && ema(c, 21) > ema(c, 50) && rsi > 38 && rsi < 65) {
                    side = TradingCore.Side.LONG;
                    reason = "Early Trend Impulse (Bullish)";
                }
                if (bias == HTFBias.BEAR && ema(c, 21) < ema(c, 50) && rsi < 62 && rsi > 35) {
                    side = TradingCore.Side.SHORT;
                    reason = "Early Trend Impulse (Bearish)";
                }
            }

            // ⚡ Weak Trend Continuation
            case WEAK_TREND -> {
                if (bias == HTFBias.BULL && ema(c, 21) > ema(c, 50) && rsi < 55) {
                    side = TradingCore.Side.LONG;
                    reason = "Weak Trend Early Continuation";
                }
                if (bias == HTFBias.BEAR && ema(c, 21) < ema(c, 50) && rsi > 45) {
                    side = TradingCore.Side.SHORT;
                    reason = "Weak Trend Early Continuation";
                }
            }

            // 📦 Range Bounce
            case RANGE -> {
                double high = highest(c, 15);
                double low = lowest(c, 15);

                if (price <= low * 1.002) {
                    side = TradingCore.Side.LONG;
                    reason = "Range Early Support";
                }
                if (price >= high * 0.998) {
                    side = TradingCore.Side.SHORT;
                    reason = "Range Early Resistance";
                }
            }

            // 💥 Climax Reversal
            case CLIMAX -> {
                if (rsi < 30) {
                    side = TradingCore.Side.LONG;
                    reason = "Early Exhaustion Reversal";
                }
                if (rsi > 70) {
                    side = TradingCore.Side.SHORT;
                    reason = "Early Exhaustion Reversal";
                }
            }
        }

        if (side == null) return null;

        // cooldown
        String key = symbol + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < COOLDOWN_MS)
            return null;

        // confidence
        double confidence = computeConfidence(c, state, adx);

        double risk = atr * 1.0;
        double rr = confidence > 0.72 ? 2.8 : 2.2;

        double stop = side == TradingCore.Side.LONG ? price - risk : price + risk;
        double take = side == TradingCore.Side.LONG ? price + risk * rr : price - risk * rr;

        cooldown.put(key, now);

        return new TradeIdea(symbol, side, price, stop, take,
                confidence,
                confidence > 0.72 ? SignalGrade.A : SignalGrade.B,
                reason);
    }

    /* ================= MARKET STATE ================= */
    private MarketState detectMarketState(List<TradingCore.Candle> c) {
        double adx = adx(c, 14);
        if (adx > 22) return MarketState.STRONG_TREND;
        if (adx > 15) return MarketState.WEAK_TREND;
        if (relativeVolume(c) > 1.8) return MarketState.CLIMAX;
        return MarketState.RANGE;
    }

    private HTFBias detectHTFBias(List<TradingCore.Candle> c) {
        if (ema(c, 50) > ema(c, 200)) return HTFBias.BULL;
        if (ema(c, 50) < ema(c, 200)) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    /* ================= CONFIDENCE ================= */
    private double computeConfidence(List<TradingCore.Candle> c,
                                     MarketState state,
                                     double adx) {
        double structure = Math.abs(ema(c, 21) - ema(c, 50)) / ema(c, 50) * 10;
        double base = switch (state) {
            case STRONG_TREND -> 0.72;
            case WEAK_TREND -> 0.66;
            case RANGE -> 0.60;
            case CLIMAX -> 0.63;
        };
        double momentumBoost = (adx / 50.0) * 0.1;
        return clamp(base + structure * 0.05 + momentumBoost, 0.55, 0.90);
    }

    /* ================= INDICATORS ================= */
    private double atr(List<TradingCore.Candle> c, int n) {
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
        return move / n / atr(c, n) * 25;
    }

    private double rsi(List<TradingCore.Candle> c, int n) {
        double gain = 0, loss = 0;
        for (int i = c.size() - n; i < c.size() - 1; i++) {
            double diff = c.get(i + 1).close - c.get(i).close;
            if (diff > 0) gain += diff; else loss -= diff;
        }
        if (loss == 0) return 100;
        return 100 - (100 / (1 + gain / loss));
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private double highest(List<TradingCore.Candle> c, int n) {
        return c.subList(c.size() - n, c.size()).stream().mapToDouble(cd -> cd.high).max().orElse(0);
    }

    private double lowest(List<TradingCore.Candle> c, int n) {
        return c.subList(c.size() - n, c.size()).stream().mapToDouble(cd -> cd.low).min().orElse(0);
    }

    private double relativeVolume(List<TradingCore.Candle> c) {
        int n = c.size();
        double avg = c.subList(n - 20, n - 1).stream().mapToDouble(cd -> cd.volume).average().orElse(1);
        return last(c).volume / avg;
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