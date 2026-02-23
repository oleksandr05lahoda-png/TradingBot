package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * World-class Decision Engine
 * Обрабатывает MarketState, SignalModel, Risk и Cooldown.
 */
public final class DecisionEngineMerged {

    /* ================= ENUMS ================= */
    public enum CoinCategory { TOP, ALT, MEME }
    public enum SignalGrade { A, B, C }
    public enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, CLIMAX, VOLATILE }
    public enum HTFBias { BULL, BEAR, NONE }

    /* ================= CONFIG ================= */
    private static final int MIN_BARS = 200;
    private static final long BASE_COOLDOWN_MS = 6 * 60_000; // 6 минут
    private static final int MAX_TOP_COINS = 70;

    private static final double DEFAULT_TREND_WEIGHT = 2.0;
    private static final double WEIGHT_PULLBACK = 1.5;
    private static final double WEIGHT_IMPULSE = 1.8;
    private static final double WEIGHT_DIVERGENCE = 1.4;
    private static final double WEIGHT_RANGE = 1.2;
    private static final double WEIGHT_VOLUME = 1.3;

    /* ================= STATE ================= */
    private final Map<String, Deque<Long>> cooldownHistory = new ConcurrentHashMap<>();
    private final Map<String, Double> adaptiveTrendWeights = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>> recentSignals = new ConcurrentHashMap<>();

    /* ================= TRADE IDEA ================= */
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
    }

    /* ================= MAIN ================= */
    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> m1,
                                    Map<String, List<TradingCore.Candle>> m5,
                                    Map<String, List<TradingCore.Candle>> m15,
                                    Map<String, List<TradingCore.Candle>> h1,
                                    Map<String, CoinCategory> categories) {

        long now = System.currentTimeMillis();

        List<TradeIdea> candidates = symbols.stream()
                .filter(s -> isValid(m15.get(s)) && isValid(h1.get(s)))
                .map(symbol -> generateTradeIdea(symbol,
                        m1.get(symbol), m5.get(symbol), m15.get(symbol), h1.get(symbol),
                        categories.getOrDefault(symbol, CoinCategory.TOP), now))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // Ограничение по категориям
        List<TradeIdea> top = filterAndLimit(candidates, CoinCategory.TOP, MAX_TOP_COINS / 2);
        List<TradeIdea> alt = filterAndLimit(candidates, CoinCategory.ALT, MAX_TOP_COINS / 3);
        List<TradeIdea> meme = filterAndLimit(candidates, CoinCategory.MEME, MAX_TOP_COINS / 6);

        List<TradeIdea> merged = new ArrayList<>();
        merged.addAll(top);
        merged.addAll(alt);
        merged.addAll(meme);

        // Сортировка по убыванию confidence
        return merged.stream()
                .sorted(Comparator.comparingDouble(t -> -t.confidence))
                .collect(Collectors.toList());
    }

    private List<TradeIdea> filterAndLimit(List<TradeIdea> ideas, CoinCategory cat, int limit) {
        return ideas.stream()
                .filter(t -> t != null)
                .sorted(Comparator.comparingDouble(t -> -t.confidence))
                .limit(limit)
                .collect(Collectors.toList());
    }

    /* ================= CORE ================= */
    private TradeIdea generateTradeIdea(String symbol,
                                        List<TradingCore.Candle> c1,
                                        List<TradingCore.Candle> c5,
                                        List<TradingCore.Candle> c15,
                                        List<TradingCore.Candle> c1h,
                                        CoinCategory cat,
                                        long now) {

        if (c15 == null || c1h == null || c15.isEmpty() || c1h.isEmpty()) return null;

        double price = last(c15).close;
        double atr = Math.max(atr(c15, 14), price * 0.0015);
        double trendWeight = adaptiveTrendWeights.getOrDefault(symbol, DEFAULT_TREND_WEIGHT);

        MarketState state = detectMarketState(c15);
        HTFBias bias = detectHTFBias(c1h);

        // ===== Multi-TF bias =====
        HTFBias bias4h = detectHTFBias(fetchCandles(symbol, "4h", 200));
        HTFBias bias1d = detectHTFBias(fetchCandles(symbol, "1d", 200));

        int bullCount = 0, bearCount = 0;
        if (bias == HTFBias.BULL) bullCount++; if (bias4h==HTFBias.BULL) bullCount++; if (bias1d==HTFBias.BULL) bullCount++;
        if (bias == HTFBias.BEAR) bearCount++; if (bias4h==HTFBias.BEAR) bearCount++; if (bias1d==HTFBias.BEAR) bearCount++;

        double microTrend = computeMicroTrend(c1);
        boolean impulse = detectMicroImpulse(c1);
        boolean volumeSpike = detectVolumeSpike(c1, cat);
        boolean pullbackBull = pricePullback(c15, true);
        boolean pullbackBear = pricePullback(c15, false);

        double scoreLong = 0, scoreShort = 0;

        // ===== SCORING =====
        if (bullCount>=2) scoreLong += trendWeight*1.2;
        if (bearCount>=2) scoreShort += trendWeight*1.2;

        if (pullbackBull) scoreLong += WEIGHT_PULLBACK;
        if (pullbackBear) scoreShort += WEIGHT_PULLBACK;

        if (impulse) { scoreLong += WEIGHT_IMPULSE*microTrend; scoreShort += WEIGHT_IMPULSE*microTrend; }

        if (bullishDivergence(c15)) scoreLong += WEIGHT_DIVERGENCE;
        if (bearishDivergence(c15)) scoreShort += WEIGHT_DIVERGENCE;

        if (state == MarketState.RANGE) {
            double high = highest(c15, 15);
            double low = lowest(c15, 15);
            if (price <= low*1.004) scoreLong += WEIGHT_RANGE;
            if (price >= high*0.996) scoreShort += WEIGHT_RANGE;
        }

        double volatility = relativeVolume(c15);
        double rangeFactor = state == MarketState.RANGE ? 0.7 : 1.0;
        scoreLong *= rangeFactor*(0.8 + 0.4*volatility);
        scoreShort *= rangeFactor*(0.8 + 0.4*volatility);

        if (volumeSpike) { scoreLong += WEIGHT_VOLUME*0.5; scoreShort += WEIGHT_VOLUME*0.5; }

        if (scoreLong < 2.5 && scoreShort < 2.5) return null;

        TradingCore.Side side = scoreLong > scoreShort ? TradingCore.Side.LONG : TradingCore.Side.SHORT;

        // ===== Flip protection =====
        if (!allowFlip(symbol, side.name(), 2, Math.max(scoreLong, scoreShort))) return null;

        if (!checkCooldown(symbol, side, now)) return null;

        double raw = Math.max(scoreLong, scoreShort);
        double confidence = computeConfidence(raw, state, cat, atr, price, microTrend);
        if (confidence < 0.52) return null;

        SignalGrade grade = confidence > 0.78 ? SignalGrade.A :
                confidence > 0.64 ? SignalGrade.B : SignalGrade.C;

        double riskMult = cat == CoinCategory.MEME ? 1.3 : cat == CoinCategory.ALT ? 1.0 : 0.85;
        double rr = confidence > 0.75 ? 2.8 : 2.2;

        double stop = side == TradingCore.Side.LONG ? price - atr*riskMult : price + atr*riskMult;
        double take = side == TradingCore.Side.LONG ? price + atr*riskMult*rr : price - atr*riskMult*rr;

        updateAdaptiveWeight(symbol, side, confidence);
        saveRecentSignal(symbol, side);

        return new TradeIdea(symbol, side, price, stop, take, confidence, grade,
                String.format("Score=%.3f State=%s MicroTrend=%.3f", raw, state, microTrend));
    }

    /* ================= ADAPTIVE TREND ================= */
    private void updateAdaptiveWeight(String symbol, TradingCore.Side side, double confidence) {
        double current = adaptiveTrendWeights.getOrDefault(symbol, DEFAULT_TREND_WEIGHT);
        current += confidence > 0.75 ? 0.05 : confidence < 0.55 ? -0.05 : 0.0;
        adaptiveTrendWeights.put(symbol, clamp(current, 1.2, 3.5));
    }

    private boolean checkCooldown(String symbol, TradingCore.Side side, long now) {
        String key = symbol + "_" + side;
        Deque<Long> history = cooldownHistory.computeIfAbsent(key, k -> new ArrayDeque<>());
        long cutoff = now - BASE_COOLDOWN_MS;
        history.removeIf(t -> t < cutoff);
        if (!history.isEmpty()) return false;
        history.addLast(now);
        return true;
    }

    /* ================= FLIP PROTECTION ================= */
    private boolean allowFlip(String symbol, String newDir, int lastNCycles, double score) {
        if (isRecentOppositeSignal(symbol, newDir, lastNCycles) && score < 0.78) return false;
        return true;
    }

    private void saveRecentSignal(String symbol, TradingCore.Side side) {
        String key = symbol;
        Deque<String> history = recentSignals.computeIfAbsent(key, k -> new ArrayDeque<>());
        history.addLast(side.name());
        while (history.size() > 5) history.removeFirst();
    }

    private boolean isRecentOppositeSignal(String symbol, String newDir, int lastNCycles) {
        Deque<String> history = recentSignals.getOrDefault(symbol, new ArrayDeque<>());
        int n = 0;
        Iterator<String> it = history.descendingIterator();
        while(it.hasNext() && n < lastNCycles) {
            String s = it.next();
            if(!s.equals(newDir)) return true;
            n++;
        }
        return false;
    }

    /* ================= CONFIDENCE ================= */
    private double computeConfidence(double raw, MarketState state, CoinCategory cat, double atr, double price, double microTrend) {
        double base = raw / 6.0;
        double stateBoost = switch (state) {
            case STRONG_TREND -> 0.12;
            case WEAK_TREND -> 0.08;
            case RANGE -> 0.05;
            case CLIMAX -> 0.07;
            case VOLATILE -> 0.04;
        };
        double volatilityFactor = Math.min(atr / price * 50, 0.1);
        double catBoost = switch (cat) {
            case TOP -> 0.02;
            case ALT -> 0.04;
            case MEME -> 0.06;
        };
        double microBoost = clamp(microTrend, 0.8, 1.2);
        return clamp(0.52 + base + stateBoost + volatilityFactor + catBoost*microBoost, 0.52, 0.95);
    }

    /* ================= MARKET ================= */
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
        if (ema50 > ema200*1.002) return HTFBias.BULL;
        if (ema50 < ema200*0.998) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    private List<TradingCore.Candle> fetchCandles(String symbol, String timeframe, int minBars) {
        // Тот же метод, который подгружает MTF данные
        // Тут заглушка – надо реализовать через TradingCore
        return new ArrayList<>();
    }

    /* ================= MICRO TREND ================= */
    private double computeMicroTrend(List<TradingCore.Candle> c) {
        if (c == null || c.size() < 5) return 1.0;
        double delta = last(c).close - c.get(c.size()-5).close;
        double microEMA = ema(c, 9) - ema(c, 21);
        return 1.0 + Math.tanh(delta*10)*0.7 + Math.tanh(microEMA*10)*0.3;
    }

    /* ================= INDICATORS ================= */
    private double atr(List<TradingCore.Candle> c, int n) {
        double sum = 0;
        for (int i=Math.max(1, c.size()-n); i<c.size(); i++) {
            TradingCore.Candle cur = c.get(i), prev = c.get(i-1);
            double tr = Math.max(cur.high-cur.low,
                    Math.max(Math.abs(cur.high-prev.close), Math.abs(cur.low-prev.close)));
            sum += tr;
        }
        return sum/n;
    }

    private double adx(List<TradingCore.Candle> c, int n) {
        double move = 0;
        for (int i=Math.max(0, c.size()-n); i<c.size()-1; i++)
            move += Math.abs(c.get(i+1).close - c.get(i).close);
        return move/n/atr(c, n)*25;
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        double k = 2.0/(p+1);
        double e = c.get(Math.max(0, c.size()-p)).close;
        for (int i=Math.max(0, c.size()-p)+1; i<c.size(); i++)
            e = c.get(i).close*k + e*(1-k);
        return e;
    }

    private boolean bullishDivergence(List<TradingCore.Candle> c) {
        if (c.size()<20) return false;
        return c.get(c.size()-1).low < c.get(c.size()-4).low &&
                rsi(c, 14) > rsi(c.subList(0, c.size()-2), 14);
    }

    private boolean bearishDivergence(List<TradingCore.Candle> c) {
        if (c.size()<20) return false;
        return c.get(c.size()-1).high > c.get(c.size()-4).high &&
                rsi(c, 14) < rsi(c.subList(0, c.size()-2), 14);
    }

    private double rsi(List<TradingCore.Candle> c, int n) {
        double g=0, l=0;
        for(int i=Math.max(0,c.size()-n); i<c.size()-1; i++){
            double d=c.get(i+1).close - c.get(i).close;
            if(d>0) g+=d; else l+=Math.abs(d);
        }
        return l==0?100:100-(100/(1+g/l));
    }

    private boolean detectMicroImpulse(List<TradingCore.Candle> c) {
        if(c==null||c.size()<5) return false;
        double delta = last(c).close - c.get(c.size()-5).close;
        return Math.abs(delta)>0.0002 && relativeVolume(c)>1.05;
    }

    private boolean detectVolumeSpike(List<TradingCore.Candle> c, CoinCategory cat) {
        if(c==null||c.size()<10) return false;
        double avg = c.subList(c.size()-10, c.size()-1).stream().mapToDouble(cd->cd.volume).average().orElse(1);
        double last = c.get(c.size()-1).volume;
        double th = switch(cat){ case MEME->1.4; case ALT->1.25; case TOP->1.15; };
        return last/avg>th;
    }

    private boolean pricePullback(List<TradingCore.Candle> c, boolean bull){
        double ema21=ema(c,21), price=last(c).close;
        return bull ? price <= ema21*1.01 : price >= ema21*0.99;
    }

    private double highest(List<TradingCore.Candle> c, int n){
        return c.subList(Math.max(0,c.size()-n),c.size()).stream().mapToDouble(cd->cd.high).max().orElse(0);
    }

    private double lowest(List<TradingCore.Candle> c, int n){
        return c.subList(Math.max(0,c.size()-n),c.size()).stream().mapToDouble(cd->cd.low).min().orElse(0);
    }

    private double relativeVolume(List<TradingCore.Candle> c){
        int n=c.size();
        double avg=c.subList(Math.max(0,n-20),n-1).stream().mapToDouble(cd->cd.volume).average().orElse(1);
        return last(c).volume/avg;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c){ return c.get(c.size()-1); }
    private boolean isValid(List<?> c){ return c!=null && c.size()>=MIN_BARS; }
    private double clamp(double v,double min,double max){ return Math.max(min, Math.min(max, v)); }

}
