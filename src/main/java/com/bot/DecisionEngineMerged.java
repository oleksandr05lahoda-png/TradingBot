package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    /* ================= ENUMS ================= */
    public enum SignalGrade { A, B, C }
    private enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, CLIMAX, VOLATILE }
    private enum HTFBias { BULL, BEAR, NONE }

    /* ================= CONFIG ================= */
    private static final int MIN_BARS = 200;
    private static final long COOLDOWN_MS = 15 * 60_000; // 15 минут минимальный интервал сигнала
    private static final Map<String, Long> cooldown = new ConcurrentHashMap<>();

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
    }

    /* ================= MAIN EVALUATION ================= */
    public List<TradeIdea> evaluate(List<String> symbols,
                                    Map<String, List<TradingCore.Candle>> m1,
                                    Map<String, List<TradingCore.Candle>> m5,
                                    Map<String, List<TradingCore.Candle>> m15,
                                    Map<String, List<TradingCore.Candle>> h1) {

        List<TradeIdea> ideas = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (String symbol : symbols) {
            List<TradingCore.Candle> c1 = m1.get(symbol);
            List<TradingCore.Candle> c5List = m5.get(symbol);
            List<TradingCore.Candle> c15List = m15.get(symbol);
            List<TradingCore.Candle> c1hList = h1.get(symbol);

            if (!isValid(c15List) || !isValid(c1hList)) continue; // основной TF 15

            HTFBias bias = detectHTFBias(c1hList);
            MarketState state = detectMarketState(c15List);
            boolean microImpulse = detectMicroImpulse(c1);

            TradeIdea idea = generateTradeIdea(symbol, c1, c5List, c15List, c1hList, state, bias, microImpulse, now);
            if (idea != null) ideas.add(idea);
        }

        ideas.sort(Comparator.<TradeIdea>comparingDouble(t -> t.confidence).reversed());
        return ideas;
    }

    /* ================= SIGNAL LOGIC ================= */
    private TradeIdea generateTradeIdea(String symbol,
                                        List<TradingCore.Candle> c1,
                                        List<TradingCore.Candle> c5,
                                        List<TradingCore.Candle> c15,
                                        List<TradingCore.Candle> c1h,
                                        MarketState state,
                                        HTFBias bias,
                                        boolean microImpulse,
                                        long now) {

        double price = last(c15).close;
        double atr = atr(c15, 14);
        double adx = adx(c15, 14);
        double rsi = rsi(c15, 14);
        double vol = relativeVolume(c15);
        double microVol = relativeVolume(c1);

        TradingCore.Side side = null;
        String reason = null;

        // ================= STRATEGIES =================
        // 1️⃣ Trend Following
        if ((state == MarketState.STRONG_TREND || state == MarketState.WEAK_TREND) && bias != HTFBias.NONE) {
            if (bias == HTFBias.BULL && ema(c15,21) > ema(c15,50) && microImpulse) {
                side = TradingCore.Side.LONG; reason = "Trend Bull + MicroImpulse";
            }
            if (bias == HTFBias.BEAR && ema(c15,21) < ema(c15,50) && microImpulse) {
                side = TradingCore.Side.SHORT; reason = "Trend Bear + MicroImpulse";
            }
        }

        // 2️⃣ Pullback near EMA21
        if (side == null && bias != HTFBias.NONE) {
            if (pullbackZone(c15, bias)) {
                side = bias == HTFBias.BULL ? TradingCore.Side.LONG : TradingCore.Side.SHORT;
                reason = "Pullback";
            }
        }

        // 3️⃣ Divergence / Reversal
        if (side == null) {
            if (bullishDivergence(c15)) { side = TradingCore.Side.LONG; reason = "Bullish Divergence"; }
            if (bearishDivergence(c15)) { side = TradingCore.Side.SHORT; reason = "Bearish Divergence"; }
        }

        // 4️⃣ Range / Exhaustion
        if (side == null) {
            double high = highest(c15, 15);
            double low = lowest(c15, 15);
            if (price <= low*1.002) { side = TradingCore.Side.LONG; reason = "Range Support"; }
            if (price >= high*0.998) { side = TradingCore.Side.SHORT; reason = "Range Resistance"; }
        }
        if (side == null && state == MarketState.CLIMAX) {
            if (rsi < 40) { side = TradingCore.Side.LONG; reason = "Exhaustion Reversal"; }
            if (rsi > 60) { side = TradingCore.Side.SHORT; reason = "Exhaustion Reversal"; }
        }

        if (side == null) return null;

        // ================= COOLDOWN =================
        String key = symbol+"_"+side;
        if (cooldown.containsKey(key) && now-cooldown.get(key)<COOLDOWN_MS) return null;

        // ================= CONFIDENCE =================
        double confidence = computeConfidence(c15, state, adx, vol, microVol, microImpulse);
        if (!passesLocalCheck(c15, side)) confidence *= 0.85; // чуть смягчаем

        SignalGrade grade = confidence > 0.75 ? SignalGrade.A : confidence > 0.62 ? SignalGrade.B : SignalGrade.C;
        if (grade == SignalGrade.C && confidence < 0.55) return null;

        // ================= RISK/REWARD =================
        double risk = atr * 1.0;
        double rr = confidence > 0.72 ? 2.8 : 2.2;
        double stop = side == TradingCore.Side.LONG ? price - risk : price + risk;
        double take = side == TradingCore.Side.LONG ? price + risk * rr : price - risk * rr;

        cooldown.put(key, now);
        return new TradeIdea(symbol, side, price, stop, take, confidence, grade, reason);
    }

    /* ================= MARKET STATE ================= */
    private MarketState detectMarketState(List<TradingCore.Candle> c) {
        double adx = adx(c, 14);
        double vol = relativeVolume(c);
        if(adx > 22) return MarketState.STRONG_TREND;
        if(adx > 15) return MarketState.WEAK_TREND;
        if(vol > 1.8) return MarketState.CLIMAX;
        if(vol > 1.2) return MarketState.VOLATILE;
        return MarketState.RANGE;
    }

    private HTFBias detectHTFBias(List<TradingCore.Candle> c){
        if(ema(c,50) > ema(c,200)) return HTFBias.BULL;
        if(ema(c,50) < ema(c,200)) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    /* ================= MICRO TREND DETECTION ================= */
    private boolean detectMicroImpulse(List<TradingCore.Candle> c){
        if(c.size()<5) return false;
        double delta = last(c).close - c.get(c.size()-5).close;
        double vol = relativeVolume(c);
        return (delta>0 && vol>1.05) || (delta<0 && vol>1.05);
    }

    /* ================= CONFIDENCE ================= */
    private double computeConfidence(List<TradingCore.Candle> c, MarketState state, double adx, double vol, double microVol, boolean microImpulse){
        double structure = Math.abs(ema(c,21)-ema(c,50))/ema(c,50)*10;
        double base = switch(state){
            case STRONG_TREND->0.72;
            case WEAK_TREND->0.66;
            case RANGE->0.60;
            case CLIMAX->0.63;
            case VOLATILE->0.58;
        };
        double momentumBoost = (adx/50.0)*0.1;
        double volBoost = Math.min((vol-1)*0.05, 0.08);
        double microBoost = microImpulse ? Math.min((microVol-1)*0.08,0.1) : 0;
        return clamp(base + structure*0.05 + momentumBoost + volBoost + microBoost, 0.55, 0.95);
    }

    /* ================= LOCAL CHECK ================= */
    private boolean passesLocalCheck(List<TradingCore.Candle> c, TradingCore.Side side){
        int n=c.size();
        TradingCore.Candle last=last(c);
        TradingCore.Candle prev=c.get(n-2);
        return side == TradingCore.Side.LONG ? last.close>=prev.low : last.close<=prev.high;
    }

    /* ================= INDICATORS ================= */
    private double atr(List<TradingCore.Candle> c,int n){
        double sum=0;
        for(int i=c.size()-n;i<c.size();i++){
            TradingCore.Candle cur=c.get(i),prev=c.get(i-1);
            double tr=Math.max(cur.high-cur.low,Math.max(Math.abs(cur.high-prev.close),Math.abs(cur.low-prev.close)));
            sum+=tr;
        }
        return sum/n;
    }

    private double adx(List<TradingCore.Candle> c,int n){
        double move=0;
        for(int i=c.size()-n;i<c.size()-1;i++) move+=Math.abs(c.get(i+1).close-c.get(i).close);
        return move/n/atr(c,n)*25;
    }

    private double rsi(List<TradingCore.Candle> c,int n){
        double gain=0,loss=0;
        for(int i=c.size()-n;i<c.size()-1;i++){
            double diff=c.get(i+1).close-c.get(i).close;
            if(diff>0) gain+=diff; else loss-=diff;
        }
        if(loss==0) return 100;
        return 100-(100/(1+gain/loss));
    }

    private double ema(List<TradingCore.Candle> c,int p){
        double k=2.0/(p+1);
        double e=c.get(c.size()-p).close;
        for(int i=c.size()-p+1;i<c.size();i++) e=c.get(i).close*k + e*(1-k);
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

    private TradingCore.Candle last(List<TradingCore.Candle> c){ return c.get(c.size()-1); }
    private boolean isValid(List<?> c){ return c!=null && c.size()>=MIN_BARS; }
    private double clamp(double v,double min,double max){ return Math.max(min,Math.min(max,v)); }

    /* ================= ADDITIONAL STRATEGIES ================= */
    private boolean pullbackZone(List<TradingCore.Candle> c, HTFBias bias){
        double ema21=ema(c,21);
        double price=last(c).close;
        return bias==HTFBias.BULL ? price<=ema21*1.008 : price>=ema21*0.992;
    }

    private boolean bullishDivergence(List<TradingCore.Candle> c){
        int n=c.size(); if(n<20) return false;
        double low1=c.get(n-3).low, low2=c.get(n-1).low;
        double rsi1=rsi(c.subList(0,n-2),14), rsi2=rsi(c,14);
        return low2<low1 && rsi2>rsi1;
    }

    private boolean bearishDivergence(List<TradingCore.Candle> c){
        int n=c.size(); if(n<20) return false;
        double high1=c.get(n-3).high, high2=c.get(n-1).high;
        double rsi1=rsi(c.subList(0,n-2),14), rsi2=rsi(c,14);
        return high2>high1 && rsi2<rsi1;
    }
}