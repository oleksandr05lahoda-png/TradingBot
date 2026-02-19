package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class Elite5MinAnalyzer {

    /* ================= CONFIG ================= */

    private static final int MIN_M15 = 200;
    private static final int MIN_H1  = 200;

    private static final int MAX_SYMBOLS = 60;

    private static final double MIN_CONFIDENCE = 0.62;
    private static final double MIN_ATR_PCT    = 0.0022;
    private static final double MIN_ADX        = 18.0;

    private static final long BASE_COOLDOWN = 12 * 60_000;

    private final Map<String, Long> cooldown = new ConcurrentHashMap<>();
    private final AdaptiveBrain brain = new AdaptiveBrain();

    /* ================= OUTPUT ================= */

    public static final class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final String reason;

        public TradeSignal(String symbol,
                           TradingCore.Side side,
                           double entry,
                           double stop,
                           double take,
                           double confidence,
                           String reason) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.reason = reason;
        }
    }

    /* ================= MAIN ================= */

    public List<TradeSignal> analyze(List<String> symbols,
                                     Map<String, List<TradingCore.Candle>> m15,
                                     Map<String, List<TradingCore.Candle>> h1) {

        List<TradeSignal> result = new ArrayList<>();
        long now = System.currentTimeMillis();
        int scanned = 0;

        for (String symbol : symbols) {

            if (scanned++ >= MAX_SYMBOLS) break;

            List<TradingCore.Candle> tf15 = m15.get(symbol);
            List<TradingCore.Candle> tf1h = h1.get(symbol);

            if (!valid(tf15, MIN_M15) || !valid(tf1h, MIN_H1))
                continue;

            if (!volatilityOk(tf15))
                continue;

            TradingCore.Side bias = detectHTFBias(tf1h);
            if (bias == null)
                continue;

            MarketState state = detectMarketState(tf15);

            TradeSignal signal =
                    buildSignal(symbol, tf15, tf1h, bias, state, now);

            if (signal != null)
                result.add(signal);
        }

        result.sort(Comparator
                .comparingDouble((TradeSignal s) -> s.confidence)
                .reversed());

        return result;
    }

    /* ================= CORE LOGIC ================= */

    private TradeSignal buildSignal(String symbol,
                                    List<TradingCore.Candle> m15,
                                    List<TradingCore.Candle> h1,
                                    TradingCore.Side bias,
                                    MarketState state,
                                    long now) {

        double price = last(m15).close;
        double atr = atr(m15, 14);
        double adx = adx(m15, 14);
        double rsi = rsi(m15, 14);

        if (adx < MIN_ADX)
            return null;

        TradingCore.Side side = null;
        String reason = null;

        /* ===== 1. TREND CONTINUATION ===== */

        if (state == MarketState.STRONG_TREND) {

            if (bias == TradingCore.Side.LONG
                    && structureBullish(m15)
                    && pullbackZone(m15, TradingCore.Side.LONG)) {

                side = TradingCore.Side.LONG;
                reason = "Strong Trend Pullback";
            }

            if (bias == TradingCore.Side.SHORT
                    && structureBearish(m15)
                    && pullbackZone(m15, TradingCore.Side.SHORT)) {

                side = TradingCore.Side.SHORT;
                reason = "Strong Trend Pullback";
            }
        }

        /* ===== 2. CONTROLLED REVERSAL ===== */

        if (state == MarketState.CLIMAX) {

            if (bias == TradingCore.Side.LONG
                    && rsi < 28
                    && exhaustionCandle(m15)) {

                side = TradingCore.Side.LONG;
                reason = "Climax Reversal Long";
            }

            if (bias == TradingCore.Side.SHORT
                    && rsi > 72
                    && exhaustionCandle(m15)) {

                side = TradingCore.Side.SHORT;
                reason = "Climax Reversal Short";
            }
        }

        if (side == null)
            return null;

        String key = symbol + "_" + side;

        if (cooldown.containsKey(key)
                && now - cooldown.get(key) < dynamicCooldown(adx))
            return null;

        double confidence =
                calculateConfidence(m15, h1, adx, rsi, state);

        confidence = brain.adjust(symbol, confidence);

        if (confidence < MIN_CONFIDENCE)
            return null;

        double risk = atr * 1.25;
        double rr = confidence > 0.78 ? 3.2 : 2.4;

        double stop = side == TradingCore.Side.LONG
                ? price - risk
                : price + risk;

        double take = side == TradingCore.Side.LONG
                ? price + risk * rr
                : price - risk * rr;

        cooldown.put(key, now);

        return new TradeSignal(
                symbol,
                side,
                price,
                stop,
                take,
                clamp(confidence, 0.0, 0.95),
                reason
        );
    }

    /* ================= MARKET STATE ================= */

    private enum MarketState {
        STRONG_TREND,
        WEAK_TREND,
        RANGE,
        CLIMAX
    }

    private MarketState detectMarketState(List<TradingCore.Candle> c) {

        double adx = adx(c, 14);
        double vol = relativeVolume(c);

        if (vol > 2.2) return MarketState.CLIMAX;
        if (adx > 26)  return MarketState.STRONG_TREND;
        if (adx > 17)  return MarketState.WEAK_TREND;
        return MarketState.RANGE;
    }

    /* ================= STRUCTURE ================= */

    private boolean structureBullish(List<TradingCore.Candle> c) {
        return ema(c,21) > ema(c,50)
                && higherHighs(c)
                && last(c).close > ema(c,21);
    }

    private boolean structureBearish(List<TradingCore.Candle> c) {
        return ema(c,21) < ema(c,50)
                && lowerLows(c)
                && last(c).close < ema(c,21);
    }

    private boolean higherHighs(List<TradingCore.Candle> c) {
        int n = c.size();
        return c.get(n-1).high > c.get(n-2).high
                && c.get(n-2).high > c.get(n-3).high;
    }

    private boolean lowerLows(List<TradingCore.Candle> c) {
        int n = c.size();
        return c.get(n-1).low < c.get(n-2).low
                && c.get(n-2).low < c.get(n-3).low;
    }

    private boolean pullbackZone(List<TradingCore.Candle> c,
                                 TradingCore.Side side) {

        double ema21 = ema(c,21);
        double price = last(c).close;

        if (side == TradingCore.Side.LONG)
            return price <= ema21 * 1.01;

        return price >= ema21 * 0.99;
    }

    private boolean exhaustionCandle(List<TradingCore.Candle> c) {
        TradingCore.Candle last = last(c);
        double body = Math.abs(last.close - last.open);
        double range = last.high - last.low;
        return range > 0 && body / range < 0.25;
    }

    /* ================= CONFIDENCE ================= */

    private double calculateConfidence(List<TradingCore.Candle> m15,
                                       List<TradingCore.Candle> h1,
                                       double adx,
                                       double rsi,
                                       MarketState state) {

        double trendH1 = trendStrength(h1);
        double momentum = momentumScore(m15);
        double structure = structureScore(m15);
        double vol = relativeVolume(m15);

        double base = switch (state) {
            case STRONG_TREND -> 0.74;
            case WEAK_TREND   -> 0.66;
            case CLIMAX       -> 0.60;
            default           -> 0.55;
        };

        double conf = base
                + trendH1 * 0.15
                + momentum * 0.12
                + structure * 0.10
                + (adx / 50.0) * 0.10
                + (vol - 1) * 0.05;

        return clamp(conf, 0.52, 0.93);
    }

    /* ================= INDICATORS ================= */

    private double atr(List<TradingCore.Candle> c,int p){
        int n=c.size();
        double sum=0;
        for(int i=n-p;i<n;i++)
            sum+=c.get(i).high-c.get(i).low;
        return sum/p;
    }

    private double adx(List<TradingCore.Candle> c,int p){
        int n=c.size();
        double move=0;
        for(int i=n-p;i<n-1;i++)
            move+=Math.abs(c.get(i+1).close-c.get(i).close);
        return (move/p)/atr(c,p)*25.0;
    }

    private double rsi(List<TradingCore.Candle> c,int p){
        double gain=0,loss=0;
        for(int i=c.size()-p;i<c.size()-1;i++){
            double diff=c.get(i+1).close-c.get(i).close;
            if(diff>0) gain+=diff;
            else loss-=diff;
        }
        if(loss==0) return 100;
        double rs=gain/loss;
        return 100-(100/(1+rs));
    }

    private double ema(List<TradingCore.Candle> c,int p){
        double k=2.0/(p+1);
        double e=c.get(c.size()-p).close;
        for(int i=c.size()-p+1;i<c.size();i++)
            e=c.get(i).close*k+e*(1-k);
        return e;
    }

    private double trendStrength(List<TradingCore.Candle> c){
        double e21=ema(c,21);
        double e50=ema(c,50);
        return clamp(Math.abs(e21-e50)/e50*8,0,1);
    }

    private double momentumScore(List<TradingCore.Candle> c){
        int n=c.size();
        double move=0;
        for(int i=n-6;i<n-1;i++)
            move+=Math.abs(c.get(i+1).close-c.get(i).close);
        return clamp(move/6.0/atr(c,14),0,1);
    }

    private double structureScore(List<TradingCore.Candle> c){
        int n=c.size();
        int up=0;
        for(int i=n-5;i<n-1;i++)
            if(c.get(i+1).close>c.get(i).close) up++;
        return up/4.0;
    }

    private double relativeVolume(List<TradingCore.Candle> c){
        int n=c.size();
        double avg=c.subList(n-20,n-1)
                .stream().mapToDouble(cd->cd.volume)
                .average().orElse(0);
        return avg==0?1:last(c).volume/avg;
    }

    private boolean volatilityOk(List<TradingCore.Candle> c){
        double a=atr(c,14);
        double price=last(c).close;
        return (a/price)>MIN_ATR_PCT;
    }

    private TradingCore.Side detectHTFBias(List<TradingCore.Candle> c){
        double e50=ema(c,50);
        double e200=ema(c,200);
        double price=last(c).close;

        if(price>e50 && e50>e200)
            return TradingCore.Side.LONG;

        if(price<e50 && e50<e200)
            return TradingCore.Side.SHORT;

        return null;
    }

    private static boolean valid(List<?> l,int min){
        return l!=null && l.size()>=min;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c){
        return c.get(c.size()-1);
    }

    private double clamp(double v,double min,double max){
        return Math.max(min,Math.min(max,v));
    }

    private long dynamicCooldown(double adx){
        if(adx>32) return BASE_COOLDOWN/2;
        if(adx<15) return BASE_COOLDOWN*2;
        return BASE_COOLDOWN;
    }

    /* ================= ADAPTIVE ================= */

    static final class AdaptiveBrain {
        private final Map<String,Integer> memory =
                new ConcurrentHashMap<>();

        double adjust(String symbol,double base){
            int s=memory.getOrDefault(symbol,0);
            if(s>=2) base+=0.02;
            if(s<=-2) base-=0.02;
            return base;
        }
    }
}
