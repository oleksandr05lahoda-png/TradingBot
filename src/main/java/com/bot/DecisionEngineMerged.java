package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class DecisionEngineMerged {

    /* ================= ENUMS ================= */
    public enum CoinCategory { TOP, ALT, MEME }
    public enum SignalGrade { A, B, C }
    private enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, CLIMAX, VOLATILE }
    private enum HTFBias { BULL, BEAR, NONE }

    /* ================= CONFIG ================= */
    private static final int MIN_BARS = 200;
    private static final long COOLDOWN_MS = 10 * 60_000; // 10 минут
    private static final int MAX_TOP_COINS = 70;

    private final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    /* ================= OUTPUT ================= */
    public static final class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final SignalGrade grade;
        public final String reason;

        public TradeIdea(String symbol, TradingCore.Side side, double entry, double stop, double take,
                         double confidence, SignalGrade grade, String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.grade = grade;
            this.reason = reason;
        }

        @Override
        public String toString() {
            return String.format(
                    "TradeIdea[%s | %s | Entry: %.4f Stop: %.4f Take: %.4f Conf: %.2f Grade: %s Reason: %s]",
                    symbol, side, entry, stop, take, confidence, grade, reason
            );
        }
    }

    /* ================= MAIN EVALUATION ================= */
    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> m1,
                                    Map<String, List<TradingCore.Candle>> m5,
                                    Map<String, List<TradingCore.Candle>> m15,
                                    Map<String, List<TradingCore.Candle>> h1,
                                    Map<String, CoinCategory> categories) {

        List<TradeIdea> ideas = new ArrayList<>();
        long now = System.currentTimeMillis();

        // Валидные монеты
        List<String> validSymbols = symbols.stream()
                .filter(s -> isValid(m15.get(s)) && isValid(h1.get(s)))
                .limit(MAX_TOP_COINS)
                .collect(Collectors.toList());

        for (String symbol : validSymbols) {
            List<TradingCore.Candle> c1 = m1.get(symbol);
            List<TradingCore.Candle> c5 = m5.get(symbol);
            List<TradingCore.Candle> c15 = m15.get(symbol);
            List<TradingCore.Candle> c1h = h1.get(symbol);
            CoinCategory cat = categories.getOrDefault(symbol, CoinCategory.TOP);

            HTFBias bias = detectHTFBias(c1h);
            MarketState state = detectMarketState(c15);
            boolean microImpulse = detectMicroImpulse(c1);
            boolean volumeSpike = detectVolumeSpike(c1, cat);

            TradeIdea idea = generateTradeIdea(symbol, c1, c5, c15, c1h, state, bias, microImpulse, volumeSpike, cat, now);
            if (idea != null) ideas.add(idea);
        }

        // Убираем дубликаты по монете и стороне
        Map<String, TradeIdea> uniqueMap = new LinkedHashMap<>();
        for (TradeIdea t : ideas) {
            String key = t.symbol + "_" + t.side;
            if (!uniqueMap.containsKey(key) || uniqueMap.get(key).confidence < t.confidence) {
                uniqueMap.put(key, t);
            }
        }

        List<TradeIdea> finalIdeas = new ArrayList<>(uniqueMap.values());
        finalIdeas.sort(Comparator.comparingDouble(t -> -t.confidence));
        return finalIdeas;
    }

    /* ================= SIGNAL GENERATION ================= */
    private TradeIdea generateTradeIdea(String symbol,
                                        List<TradingCore.Candle> c1,
                                        List<TradingCore.Candle> c5,
                                        List<TradingCore.Candle> c15,
                                        List<TradingCore.Candle> c1h,
                                        MarketState state,
                                        HTFBias bias,
                                        boolean microImpulse,
                                        boolean volumeSpike,
                                        CoinCategory cat,
                                        long now) {

        double price = last(c15).close;
        double atrValue = atr(c15, 14);
        if (atrValue < 0.0001) atrValue = price * 0.001; // защита от нуля
        double adxValue = adx(c15, 14);
        double rsiValue = rsi(c15, 14);
        double vol = relativeVolume(c15);
        double microVol = relativeVolume(c1);

        TradingCore.Side side = null;
        String reason = null;

        // ================= TREND =================
        if ((state == MarketState.STRONG_TREND || state == MarketState.WEAK_TREND) && bias != HTFBias.NONE) {
            double emaShort = ema(c15, 21);
            double emaLong = ema(c15, 50);
            boolean trendBull = bias == HTFBias.BULL && emaShort > emaLong;
            boolean trendBear = bias == HTFBias.BEAR && emaShort < emaLong;

            if (trendBull && (microImpulse || pricePullback(c15, true))) {
                side = TradingCore.Side.LONG;
                reason = "Trend Bull + MicroImpulse/Pullback";
            } else if (trendBear && (microImpulse || pricePullback(c15, false))) {
                side = TradingCore.Side.SHORT;
                reason = "Trend Bear + MicroImpulse/Pullback";
            }
        }

        // ================= DIVERGENCE =================
        if (side == null) {
            if (bullishDivergence(c15)) { side = TradingCore.Side.LONG; reason = "Bullish Divergence"; }
            else if (bearishDivergence(c15)) { side = TradingCore.Side.SHORT; reason = "Bearish Divergence"; }
        }

        // ================= RANGE/CLIMAX =================
        if (side == null) {
            double high = highest(c15, 15);
            double low = lowest(c15, 15);
            if (price <= low * 1.005) { side = TradingCore.Side.LONG; reason = "Range Support"; }
            else if (price >= high * 0.995) { side = TradingCore.Side.SHORT; reason = "Range Resistance"; }

            if (state == MarketState.CLIMAX) {
                if (rsiValue < 45) { side = TradingCore.Side.LONG; reason = "Exhaustion Reversal"; }
                else if (rsiValue > 55) { side = TradingCore.Side.SHORT; reason = "Exhaustion Reversal"; }
            }
        }

        if (side == null) return null;

        // ================= COOLDOWN =================
        String key = symbol + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < COOLDOWN_MS) return null;

        // ================= CONFIDENCE =================
        double confidence = computeConfidence(c15, state, adxValue, vol, microVol, microImpulse, volumeSpike, cat);
        SignalGrade grade = confidence > 0.75 ? SignalGrade.A :
                confidence > 0.62 ? SignalGrade.B : SignalGrade.C;
        if (grade == SignalGrade.C && confidence < 0.52) return null;

        // ================= SL/TP =================
        double riskMultiplier = cat == CoinCategory.MEME ? 1.2 : cat == CoinCategory.ALT ? 1.0 : 0.8;
        double rr = confidence > 0.72 ? 2.8 : 2.2;
        double stop = side == TradingCore.Side.LONG ? price - atrValue * riskMultiplier : price + atrValue * riskMultiplier;
        double take = side == TradingCore.Side.LONG ? price + atrValue * riskMultiplier * rr : price - atrValue * riskMultiplier * rr;

        cooldown.put(key, now);
        return new TradeIdea(symbol, side, price, stop, take, confidence, grade, reason);
    }

    /* ================= MARKET STATE ================= */
    private MarketState detectMarketState(List<TradingCore.Candle> c) {
        double adxValue = adx(c, 14);
        double vol = relativeVolume(c);
        if (adxValue > 22) return MarketState.STRONG_TREND;
        if (adxValue > 15) return MarketState.WEAK_TREND;
        if (vol > 1.8) return MarketState.CLIMAX;
        if (vol > 1.2) return MarketState.VOLATILE;
        return MarketState.RANGE;
    }

    private HTFBias detectHTFBias(List<TradingCore.Candle> c) {
        if (c.size() < 200) return HTFBias.NONE;
        double ema50 = ema(c, 50);
        double ema200 = ema(c, 200);
        if (ema50 > ema200) return HTFBias.BULL;
        if (ema50 < ema200) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    /* ================= MICRO TREND ================= */
    private boolean detectMicroImpulse(List<TradingCore.Candle> c) {
        if (c == null || c.size() < 5) return false;
        double delta = last(c).close - c.get(c.size()-5).close;
        return Math.abs(delta) > 0.0003 && relativeVolume(c) > 1.02;
    }

    private boolean detectVolumeSpike(List<TradingCore.Candle> c, CoinCategory cat) {
        if (c == null || c.size() < 10) return false;
        double avgVol = c.subList(Math.max(0, c.size()-10), c.size()-1).stream().mapToDouble(cd -> cd.volume).average().orElse(1);
        double lastVol = last(c).volume;
        double threshold = cat == CoinCategory.MEME ? 1.3 : cat == CoinCategory.ALT ? 1.2 : 1.1;
        return lastVol / avgVol > threshold;
    }

    private boolean pricePullback(List<TradingCore.Candle> c, boolean bullish) {
        double ema21 = ema(c, 21);
        double price = last(c).close;
        return bullish ? price <= ema21 * 1.012 : price >= ema21 * 0.988;
    }

    /* ================= CONFIDENCE ================= */
    private double computeConfidence(List<TradingCore.Candle> c, MarketState state, double adx, double vol, double microVol,
                                     boolean microImpulse, boolean volSpike, CoinCategory cat) {
        double structure = Math.abs(ema(c,21) - ema(c,50))/ema(c,50) * 10;
        double base = switch(state) {
            case STRONG_TREND -> 0.72;
            case WEAK_TREND -> 0.66;
            case RANGE -> 0.60;
            case CLIMAX -> 0.63;
            case VOLATILE -> 0.58;
        };
        double momentumBoost = (adx / 50.0) * 0.1;
        double volBoost = Math.min((vol - 1) * 0.05, 0.08);
        double microBoost = microImpulse ? Math.min((microVol - 1) * 0.08, 0.1) : 0;
        double spikeBoost = volSpike ? 0.05 : 0;
        double catBoost = cat == CoinCategory.MEME ? 0.05 : cat == CoinCategory.ALT ? 0.03 : 0;
        return clamp(base + structure*0.05 + momentumBoost + volBoost + microBoost + spikeBoost + catBoost, 0.50, 0.95);
    }

    /* ================= DIVERGENCE ================= */
    private boolean bullishDivergence(List<TradingCore.Candle> c) {
        if (c.size() < 20) return false;
        double low1 = c.get(c.size()-3).low, low2 = c.get(c.size()-1).low;
        double rsi1 = rsi(c.subList(0, c.size()-2), 14), rsi2 = rsi(c, 14);
        return low2 < low1 && rsi2 > rsi1;
    }

    private boolean bearishDivergence(List<TradingCore.Candle> c) {
        if (c.size() < 20) return false;
        double high1 = c.get(c.size()-3).high, high2 = c.get(c.size()-1).high;
        double rsi1 = rsi(c.subList(0, c.size()-2), 14), rsi2 = rsi(c, 14);
        return high2 > high1 && rsi2 < rsi1;
    }

    /* ================= INDICATORS ================= */
    private double atr(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i = Math.max(1, c.size()-n); i < c.size(); i++) {
            TradingCore.Candle cur = c.get(i), prev = c.get(i-1);
            double tr = Math.max(cur.high - cur.low, Math.max(Math.abs(cur.high - prev.close), Math.abs(cur.low - prev.close)));
            sum += tr;
        }
        return sum / n;
    }

    private double adx(List<TradingCore.Candle> c, int n) {
        double move = 0;
        for (int i = Math.max(0, c.size()-n); i < c.size()-1; i++) move += Math.abs(c.get(i+1).close - c.get(i).close);
        return move / n / atr(c,n) * 25;
    }

    private double rsi(List<TradingCore.Candle> c, int n) {
        double gain = 0, loss = 0;
        for (int i = Math.max(0, c.size()-n); i < c.size()-1; i++) {
            double diff = c.get(i+1).close - c.get(i).close;
            if (diff > 0) gain += diff;
            else loss += Math.abs(diff);
        }
        if (loss == 0) return 100;
        return 100 - (100 / (1 + gain / loss));
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        if (c.size() < p) return last(c).close;
        double k = 2.0 / (p + 1);
        double e = c.get(c.size()-p).close;
        for (int i = c.size()-p+1; i < c.size(); i++) e = c.get(i).close*k + e*(1-k);
        return e;
    }

    private double highest(List<TradingCore.Candle> c,int n){ return c.subList(Math.max(0,c.size()-n),c.size()).stream().mapToDouble(cd->cd.high).max().orElse(0); }
    private double lowest(List<TradingCore.Candle> c,int n){ return c.subList(Math.max(0,c.size()-n),c.size()).stream().mapToDouble(cd->cd.low).min().orElse(0); }
    private double relativeVolume(List<TradingCore.Candle> c){ int n=c.size(); double avg=c.subList(Math.max(0,n-20),n-1).stream().mapToDouble(cd->cd.volume).average().orElse(1); return last(c).volume/avg; }
    private TradingCore.Candle last(List<TradingCore.Candle> c){ return c.get(c.size()-1); }
    private boolean isValid(List<?> c){ return c!=null && c.size()>=MIN_BARS; }
    private double clamp(double v,double min,double max){ return Math.max(min,Math.min(max,v)); }

}