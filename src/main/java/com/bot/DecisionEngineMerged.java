package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Professional Decision Engine (Stable & Optimized)
 * Полная рабочая версия с безопасными проверками, улучшенной вероятностью и фильтрами.
 */
public final class DecisionEngineMerged {

    /* ================= ENUMS ================= */
    public enum CoinCategory { TOP, ALT, MEME }
    public enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, VOLATILE }
    public enum HTFBias { BULL, BEAR, NONE }

    /* ================= CONFIG ================= */
    private static final int MIN_BARS = 70;
    private static final long COOLDOWN_TOP = 2 * 60_000;
    private static final long COOLDOWN_ALT = 3 * 60_000;
    private static final long COOLDOWN_MEME = 4 * 60_000;
    private static final double MIN_CONFIDENCE = 55.0;
    private static final double UNIQUE_PRICE_DIST = 0.006; // 0.6%
    /* ================= STATE ================= */
    private final Map<String, Long> cooldownMap = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>> recentDirections = new ConcurrentHashMap<>();
    private final Map<String, Double> lastSignalPrice = new ConcurrentHashMap<>();
    private final Set<String> currentCycleSignals = new HashSet<>();
    /* ================= MICRO TREND ================= */
    private static final class MicroTrendResult {
        public final double speed;       // положительное = вверх, отрицательное = вниз
        public final double impulse;     // сила движения
        public final TradingCore.Side direction; // LONG / SHORT / null

        public MicroTrendResult(double speed, double impulse, TradingCore.Side direction) {
            this.speed = speed;
            this.impulse = impulse;
            this.direction = direction;
        }
    }

    // вычисление микро-тренда
    private MicroTrendResult computeMicroTrend(List<TradingCore.Candle> candles) {
        if(candles == null || candles.size() < 10) return new MicroTrendResult(0,0,null);
        double delta = last(candles).close - candles.get(candles.size()-5).close;
        double atrVal = atr(candles, 14);
        TradingCore.Side dir = delta > 0 ? TradingCore.Side.LONG : delta < 0 ? TradingCore.Side.SHORT : null;
        double factor = Math.min(Math.abs(delta)/atrVal, 1.0); // нормализация скорости
        return new MicroTrendResult(delta/atrVal, factor, dir);
    }
    /* ================= TRADE IDEA ================= */
    public static final class TradeIdea {
        public final String symbol;
        public final TradingCore.Side side;
        public final double price;
        public final double stop;
        public final double take;
        public double probability;
        public final List<String> flags;

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

    /* ================= PUBLIC ANALYZE ================= */
    public List<TradeIdea> analyzeAll(Map<String, List<TradingCore.Candle>> candles1m,
                                      Map<String, List<TradingCore.Candle>> candles5m,
                                      Map<String, List<TradingCore.Candle>> candles15m,
                                      Map<String, List<TradingCore.Candle>> candles1h,
                                      Map<String, CoinCategory> coinCategories,
                                      com.bot.GlobalImpulseController.GlobalContext globalContext) {

        currentCycleSignals.clear();
        List<TradeIdea> results = new ArrayList<>();

        // BTC тренд для корреляции
        TradingCore.Side btcSide = null;
        if (candles15m.containsKey("BTCUSDT") && valid(candles15m.get("BTCUSDT"))) {
            TradeIdea btcIdea = generate("BTCUSDT",
                    candles1m.getOrDefault("BTCUSDT", Collections.emptyList()),
                    candles5m.getOrDefault("BTCUSDT", Collections.emptyList()),
                    candles15m.get("BTCUSDT"),
                    candles1h.getOrDefault("BTCUSDT", Collections.emptyList()),
                    CoinCategory.TOP,
                    System.currentTimeMillis(),
                    globalContext);
            if (btcIdea != null) btcSide = btcIdea.side;
        }

        for (String symbol : candles15m.keySet()) {
            CoinCategory cat = coinCategories.getOrDefault(symbol, CoinCategory.ALT);

            TradeIdea idea = generate(symbol,
                    candles1m.getOrDefault(symbol, Collections.emptyList()),
                    candles5m.getOrDefault(symbol, Collections.emptyList()),
                    candles15m.get(symbol),
                    candles1h.getOrDefault(symbol, Collections.emptyList()),
                    cat,
                    System.currentTimeMillis(),
                    globalContext);

            if (idea != null) {
                Double lastPrice = lastSignalPrice.get(symbol);
                if (lastPrice != null && Math.abs(idea.price - lastPrice)/Math.max(idea.price,lastPrice) < UNIQUE_PRICE_DIST) continue;

                results.add(idea);
                lastSignalPrice.put(symbol, idea.price);
                currentCycleSignals.add(symbol);
            }
        }

        return results;
    }

    /* ================= CORE GENERATE ================= */
    private TradeIdea generate(String symbol,
                               List<TradingCore.Candle> c1,
                               List<TradingCore.Candle> c5,
                               List<TradingCore.Candle> c15,
                               List<TradingCore.Candle> c1h,
                               CoinCategory cat,
                               long now,
                               GlobalImpulseController.GlobalContext globalContext) {

        if (!valid(c15) || !valid(c1h)) return null;

        double price = last(c15).close;
        double rawAtr = atr(c15, 14);
        double atr = Math.max(rawAtr, price * 0.0015);

        MarketState state = detectState(c15);
        HTFBias bias = detectBias(c1h);

        double scoreLong = 0.0;
        double scoreShort = 0.0;

        double btcInfluence = (!symbol.equals("BTCUSDT") && globalContext != null) ? 0.6 : 1.0;

        // HTF bias
        if (bias == HTFBias.BULL) scoreLong += 0.9 * btcInfluence;
        else if (bias == HTFBias.BEAR) scoreShort += 0.9 * btcInfluence;

        // pullbacks
        if (pullback(c15, true)) scoreLong += 1.0 * btcInfluence;
        if (pullback(c15, false)) scoreShort += 1.0 * btcInfluence;

        // импульсы 1m
        if (impulse(c1)) {
            int size = c1.size();
            double delta = last(c1).close - c1.get(Math.max(0,size-5)).close;
            double factor = (state == MarketState.STRONG_TREND) ? 0.65*btcInfluence : 0.5*btcInfluence;
            if (delta > atr*0.15) scoreLong += factor;
            if (delta < -atr*0.15) scoreShort += factor;
        }

        // дивергенции
        if (bullDiv(c15)) scoreLong += 0.6*btcInfluence;
        if (bearDiv(c15)) scoreShort += 0.6*btcInfluence;
        MicroTrendResult mt = computeMicroTrend(c1);
        if(mt.direction == TradingCore.Side.LONG) scoreLong += 0.3 * btcInfluence;
        if(mt.direction == TradingCore.Side.SHORT) scoreShort += 0.3 * btcInfluence;

        if((scoreLong > scoreShort && mt.direction == TradingCore.Side.SHORT) ||
                (scoreShort > scoreLong && mt.direction == TradingCore.Side.LONG)) {
            scoreLong *= 0.85;
            scoreShort *= 0.85;
        }
        // RSI фильтры
        double rsi14 = rsi(c15, 14);
        if (state != MarketState.STRONG_TREND) {
            if (rsi14 > 82) scoreLong -= 0.15;
            if (rsi14 < 18) scoreShort -= 0.15;
        }

        // проверка разницы
        double scoreDiff = Math.abs(scoreLong - scoreShort);
        if (scoreDiff < 0.03) return null;

        TradingCore.Side side = scoreLong > scoreShort ? TradingCore.Side.LONG : TradingCore.Side.SHORT;

        if (!cooldownAllowed(symbol, side, cat, now)) return null;
        if (!flipAllowed(symbol, side)) return null;

        double probability = computeConfidence(scoreLong, scoreShort, state, cat, atr, price);
        probability *= Math.min(1.0, atr/price*120);
        probability = clamp(probability, 0, 100);
        if (probability < MIN_CONFIDENCE) return null;

        List<String> flags = new ArrayList<>();
        if (atr > price*0.001) flags.add("ATR↑");
        if (volumeSpike(c15, cat)) flags.add("vol:true");
        if (impulse(c1)) flags.add("impulse:true");
        if(mt.direction == TradingCore.Side.LONG) flags.add("microTrend:UP");
        if(mt.direction == TradingCore.Side.SHORT) flags.add("microTrend:DOWN");

        double riskMult = cat == CoinCategory.MEME ? 1.3 : cat == CoinCategory.ALT ? 1.0 : 0.85;
        double rr = probability > 80 ? 3.0 : probability > 70 ? 2.6 : 2.2;
        double stop = side == TradingCore.Side.LONG ? price - atr*riskMult : price + atr*riskMult;
        double take = side == TradingCore.Side.LONG ? price + atr*riskMult*rr : price - atr*riskMult*rr;

        registerSignal(symbol, side, now);

        return new TradeIdea(symbol, side, price, stop, take, probability, flags);
    }

    /* ================= SIGNAL HISTORY ================= */
    private boolean cooldownAllowed(String symbol, TradingCore.Side side, CoinCategory cat, long now) {
        String key = symbol + "_" + side;
        long base = cat == CoinCategory.TOP ? COOLDOWN_TOP : cat == CoinCategory.ALT ? COOLDOWN_ALT : COOLDOWN_MEME;
        Long last = cooldownMap.get(key);
        return last == null || now - last >= base;
    }

    private boolean flipAllowed(String symbol, TradingCore.Side newSide) {
        Deque<String> history = recentDirections.get(symbol);
        if (history == null || history.isEmpty()) return true;
        String lastSide = history.peekLast();
        return lastSide == null || !lastSide.equals(newSide.name());
    }

    private void registerSignal(String symbol, TradingCore.Side side, long now) {
        String key = symbol + "_" + side;
        cooldownMap.put(key, now);
        Deque<String> history = recentDirections.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        history.addLast(side.name());
        if (history.size() > 3) history.removeFirst();
    }
    private double computeConfidence(double scoreLong, double scoreShort, MarketState state, CoinCategory cat, double atr, double price) {
        double edge = Math.abs(scoreLong - scoreShort);
        // базовая вероятность через логистическую функцию
        double prob = 1.0 / (1.0 + Math.exp(-5*(edge-0.1)));

        double baseProb = 50 + prob*45; // нормализуем 50..95%

        // корректируем по состоянию рынка
        if(state == MarketState.STRONG_TREND) baseProb += 5;
        else if(state == MarketState.WEAK_TREND) baseProb += 2;

        // корректируем по категории монеты
        if(cat == CoinCategory.MEME) baseProb -= 5;
        if(cat == CoinCategory.TOP) baseProb += 3;

        return clamp(baseProb, 40, 95); // min/max, чтобы probability была правдоподобной
    }
    /* ================= STATE DETECT ================= */
    private MarketState detectState(List<TradingCore.Candle> c) {
        double adx = adx(c,14);
        if (adx > 26) return MarketState.STRONG_TREND;
        if (adx > 18) return MarketState.WEAK_TREND;
        return MarketState.RANGE;
    }

    private HTFBias detectBias(List<TradingCore.Candle> c) {
        if (!valid(c)) return HTFBias.NONE;
        double ema50 = ema(c,50);
        double ema200 = ema(c,200);
        if (ema50 > ema200) return HTFBias.BULL;
        if (ema50 < ema200) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    /* ================= INDICATORS ================= */
    public double atr(List<TradingCore.Candle> c, int n) {
        if (c.size() < n + 1) return 0;
        double sum=0;
        for(int i=c.size()-n;i<c.size();i++){
            TradingCore.Candle cur=c.get(i);
            TradingCore.Candle prev=c.get(i-1);
            double tr=Math.max(cur.high-cur.low, Math.max(Math.abs(cur.high-prev.close), Math.abs(cur.low-prev.close)));
            sum+=tr;
        }
        return sum/n;
    }

    private double adx(List<TradingCore.Candle> c, int n){
        if(c.size()<n+1) return 15;
        double trSum=0, plusDM=0, minusDM=0;
        for(int i=c.size()-n;i<c.size();i++){
            TradingCore.Candle cur=c.get(i);
            TradingCore.Candle prev=c.get(i-1);
            double highDiff=cur.high-prev.high;
            double lowDiff=prev.low-cur.low;
            double tr=Math.max(cur.high-cur.low, Math.max(Math.abs(cur.high-prev.close), Math.abs(cur.low-prev.close)));
            trSum+=tr;
            if(highDiff>lowDiff && highDiff>0) plusDM+=highDiff;
            if(lowDiff>highDiff && lowDiff>0) minusDM+=lowDiff;
        }
        double atr=trSum/n;
        if(atr==0) return 15;
        double plusDI=100*(plusDM/n)/atr;
        double minusDI=100*(minusDM/n)/atr;
        double denom=plusDI+minusDI;
        if(denom==0) return 15;
        return 100*Math.abs(plusDI-minusDI)/denom;
    }

    private double ema(List<TradingCore.Candle> c, int p){
        if(c.size()<p) return last(c).close;
        double k=2.0/(p+1);
        double e=c.get(c.size()-p).close;
        for(int i=c.size()-p+1;i<c.size();i++) e=c.get(i).close*k + e*(1-k);
        return e;
    }

    public double rsi(List<TradingCore.Candle> c, int period){
        if(c.size()<period+1) return 50.0;
        double gain=0, loss=0;
        for(int i=c.size()-period;i<c.size();i++){
            double change=c.get(i).close-c.get(i-1).close;
            if(change>0) gain+=change; else loss-=change;
        }
        double rs=loss==0?100:gain/loss;
        return 100-(100/(1+rs));
    }

    private boolean bullDiv(List<TradingCore.Candle> c){
        if(c.size()<25) return false;
        int i1=c.size()-5, i2=c.size()-1;
        double low1=c.get(i1).low, low2=c.get(i2).low;
        double rsi1=rsi(c.subList(0,i1+1),14), rsi2=rsi(c,14);
        return low2<low1 && rsi2>rsi1;
    }

    private boolean bearDiv(List<TradingCore.Candle> c){
        if(c.size()<25) return false;
        int i1=c.size()-5, i2=c.size()-1;
        double high1=c.get(i1).high, high2=c.get(i2).high;
        double rsi1=rsi(c.subList(0,i1+1),14), rsi2=rsi(c,14);
        return high2>high1 && rsi2<rsi1;
    }

    public boolean impulse(List<TradingCore.Candle> c){
        if(c==null || c.size()<5) return false;
        double atrVal=atr(c,14);
        return Math.abs(last(c).close-c.get(Math.max(0,c.size()-5)).close)>atrVal*0.08;
    }

    public boolean volumeSpike(List<TradingCore.Candle> c, CoinCategory cat){
        if(c.size()<10) return false;
        double avg=c.subList(c.size()-10,c.size()-1).stream().mapToDouble(cd->cd.volume).average().orElse(1);
        double lastVol=last(c).volume;
        double threshold=cat==CoinCategory.MEME?1.25:cat==CoinCategory.ALT?1.18:1.10;
        return lastVol/avg>threshold;
    }

    private boolean pullback(List<TradingCore.Candle> c, boolean bull){
        double ema21=ema(c,21);
        double price=last(c).close;
        return bull?price<=ema21*0.998:price>=ema21*1.002;
    }
    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             CoinCategory cat,
                             GlobalImpulseController.GlobalContext globalContext) {

        long now = System.currentTimeMillis();

        return generate(
                symbol,
                c1,
                c5,
                c15,
                c1h,
                cat,
                now,
                globalContext
        );
    }
    private TradingCore.Candle last(List<TradingCore.Candle> c){ return c.get(c.size()-1); }
    private boolean valid(List<?> c){ return c!=null && c.size()>=MIN_BARS; }
    private double clamp(double v,double min,double max){ return Math.max(min,Math.min(max,v)); }

}