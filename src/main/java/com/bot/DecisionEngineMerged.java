package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    /* ================= ENUMS ================= */

    public enum SignalGrade { A, B, C }

    private enum MarketState {
        STRONG_TREND,
        TREND,
        RANGE,
        VOLATILE,
        CLIMAX
    }

    private enum HTFBias { BULL, BEAR, NONE }

    /* ================= CONFIG ================= */

    private static final int MIN_BARS = 200;
    private static final long COOLDOWN_MS = 12 * 60_000;

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

    /* ================= MAIN ================= */

    public List<TradeIdea> evaluate(
            List<String> symbols,
            Map<String, List<TradingCore.Candle>> m1,
            Map<String, List<TradingCore.Candle>> m5,
            Map<String, List<TradingCore.Candle>> m15,
            Map<String, List<TradingCore.Candle>> h1) {

        List<TradeIdea> list = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (String symbol : symbols) {

            List<TradingCore.Candle> c1 = m1.get(symbol);
            List<TradingCore.Candle> c15 = m15.get(symbol);
            List<TradingCore.Candle> htf = h1.get(symbol);

            if (!isValid(c15) || !isValid(htf)) continue;

            HTFBias bias = detectHTFBias(htf);
            MarketState state = detectMarketState(c15);

            TradeIdea idea = buildSignal(symbol, c1, c15, htf, bias, state, now);
            if (idea != null) list.add(idea);
        }

        list.sort(Comparator.comparingDouble((TradeIdea t)->t.confidence).reversed());
        return list;
    }

    /* ================= CORE LOGIC ================= */

    private TradeIdea buildSignal(
            String symbol,
            List<TradingCore.Candle> m1,
            List<TradingCore.Candle> m15,
            List<TradingCore.Candle> h1,
            HTFBias bias,
            MarketState state,
            long now) {

        double price = last(m15).close;
        double atr = atr(m15,14);
        double adx = adx(m15,14);
        double rsi = rsi(m15,14);
        double vol = relativeVolume(m15);

        TradingCore.Side side = null;
        String reason = null;

        /* ========= TREND IMPULSE ========= */

        if (bias != HTFBias.NONE) {

            boolean trendUp = ema(m15,21) > ema(m15,50);
            boolean trendDown = ema(m15,21) < ema(m15,50);

            if (bias == HTFBias.BULL && trendUp && adx > 14 && rsi > 52) {
                side = TradingCore.Side.LONG;
                reason = "Trend continuation";
            }

            if (bias == HTFBias.BEAR && trendDown && adx > 14 && rsi < 48) {
                side = TradingCore.Side.SHORT;
                reason = "Trend continuation";
            }
        }

        /* ========= PULLBACK ENTRY ========= */

        if (side == null && bias != HTFBias.NONE) {

            double ema21 = ema(m15,21);

            if (bias == HTFBias.BULL && price <= ema21*1.01 && rsi>45) {
                side = TradingCore.Side.LONG;
                reason = "Pullback";
            }

            if (bias == HTFBias.BEAR && price >= ema21*0.99 && rsi<55) {
                side = TradingCore.Side.SHORT;
                reason = "Pullback";
            }
        }

        /* ========= BREAKOUT ========= */

        if (side == null && state != MarketState.RANGE) {

            double high = highest(m15,12);
            double low = lowest(m15,12);

            if (price > high*0.999 && adx>13) {
                side = TradingCore.Side.LONG;
                reason = "Breakout";
            }

            if (price < low*1.001 && adx>13) {
                side = TradingCore.Side.SHORT;
                reason = "Breakdown";
            }
        }

        /* ========= REVERSAL ========= */

        if (side == null) {

            if (bullishDivergence(m15) && rsi < 48) {
                side = TradingCore.Side.LONG;
                reason = "Bullish Divergence";
            }

            if (bearishDivergence(m15) && rsi > 52) {
                side = TradingCore.Side.SHORT;
                reason = "Bearish Divergence";
            }
        }

        if (side == null) return null;

        /* ========= COOLDOWN ========= */

        String key = symbol + "_" + side;
        if (cooldown.containsKey(key) && now - cooldown.get(key) < COOLDOWN_MS)
            return null;

        /* ========= CONFIDENCE ========= */

        double confidence = baseConfidence(state);

        confidence += Math.min(adx/50.0,0.18);
        confidence += Math.min((vol-1)*0.06,0.10);

        if (bias != HTFBias.NONE)
            confidence += 0.04;

        confidence = clamp(confidence,0.55,0.95);

        SignalGrade grade =
                confidence > 0.78 ? SignalGrade.A :
                        confidence > 0.66 ? SignalGrade.B :
                                SignalGrade.C;

        /* ========= RISK ========= */

        double risk = atr;
        double rr =
                confidence>0.80?2.7:
                        confidence>0.70?2.2:
                                1.8;

        double stop = side==TradingCore.Side.LONG ? price-risk : price+risk;
        double take = side==TradingCore.Side.LONG ? price+risk*rr : price-risk*rr;

        cooldown.put(key, now);

        return new TradeIdea(symbol,side,price,stop,take,confidence,grade,reason);
    }

    /* ================= MARKET STATE ================= */

    private MarketState detectMarketState(List<TradingCore.Candle> c){

        double adx = adx(c,14);
        double vol = relativeVolume(c);

        if(vol>1.9) return MarketState.CLIMAX;
        if(adx>23) return MarketState.STRONG_TREND;
        if(adx>15) return MarketState.TREND;
        if(vol>1.25) return MarketState.VOLATILE;
        return MarketState.RANGE;
    }

    private HTFBias detectHTFBias(List<TradingCore.Candle> c){
        double e50=ema(c,50);
        double e200=ema(c,200);
        if(e50>e200) return HTFBias.BULL;
        if(e50<e200) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    /* ================= CONFIDENCE ================= */

    private double baseConfidence(MarketState s){
        return switch(s){
            case STRONG_TREND -> 0.72;
            case TREND -> 0.67;
            case RANGE -> 0.61;
            case VOLATILE -> 0.60;
            case CLIMAX -> 0.64;
        };
    }

    /* ================= INDICATORS ================= */

    private double atr(List<TradingCore.Candle> c,int n){
        double sum=0;
        for(int i=c.size()-n;i<c.size();i++){
            TradingCore.Candle cur=c.get(i),prev=c.get(i-1);
            double tr=Math.max(cur.high-cur.low,
                    Math.max(Math.abs(cur.high-prev.close),
                            Math.abs(cur.low-prev.close)));
            sum+=tr;
        }
        return sum/n;
    }

    private double adx(List<TradingCore.Candle> c,int n){
        double move=0;
        for(int i=c.size()-n;i<c.size()-1;i++)
            move+=Math.abs(c.get(i+1).close-c.get(i).close);
        return move/n/atr(c,n)*25;
    }

    private double rsi(List<TradingCore.Candle> c,int n){
        double gain=0,loss=0;
        for(int i=c.size()-n;i<c.size()-1;i++){
            double d=c.get(i+1).close-c.get(i).close;
            if(d>0) gain+=d; else loss-=d;
        }
        if(loss==0) return 100;
        return 100-(100/(1+gain/loss));
    }

    private double ema(List<TradingCore.Candle> c,int p){
        double k=2.0/(p+1);
        double e=c.get(c.size()-p).close;
        for(int i=c.size()-p+1;i<c.size();i++)
            e=c.get(i).close*k + e*(1-k);
        return e;
    }

    private double highest(List<TradingCore.Candle> c,int n){
        return c.subList(c.size()-n,c.size()).stream().mapToDouble(cd->cd.high).max().orElse(0);
    }

    private double lowest(List<TradingCore.Candle> c,int n){
        return c.subList(c.size()-n,c.size()).stream().mapToDouble(cd->cd.low).min().orElse(0);
    }

    private double relativeVolume(List<TradingCore.Candle> c){
        int n=c.size();
        double avg=c.subList(Math.max(0,n-20),n-1).stream().mapToDouble(cd->cd.volume).average().orElse(1);
        return last(c).volume/avg;
    }

    private TradingCore.Candle last(List<TradingCore.Candle> c){
        return c.get(c.size()-1);
    }

    private boolean bullishDivergence(List<TradingCore.Candle> c){
        int n=c.size(); if(n<20) return false;
        double l1=c.get(n-3).low,l2=c.get(n-1).low;
        double r1=rsi(c.subList(0,n-2),14);
        double r2=rsi(c,14);
        return l2<l1 && r2>r1;
    }

    private boolean bearishDivergence(List<TradingCore.Candle> c){
        int n=c.size(); if(n<20) return false;
        double h1=c.get(n-3).high,h2=c.get(n-1).high;
        double r1=rsi(c.subList(0,n-2),14);
        double r2=rsi(c,14);
        return h2>h1 && r2<r1;
    }

    private boolean isValid(List<?> c){
        return c!=null && c.size()>=MIN_BARS;
    }

    private double clamp(double v,double min,double max){
        return Math.max(min,Math.min(max,v));
    }
}