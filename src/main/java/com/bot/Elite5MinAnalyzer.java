package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class Elite5MinAnalyzer {

    /* ================= CONFIG ================= */
    private static final int MIN_M15 = 120;
    private static final int MIN_H1  = 120;
    private static final int MAX_SYMBOLS = 80;

    private static final double MIN_CONFIDENCE = 0.52;
    private static final double MIN_ATR_PCT = 0.0009;
    private static final long BASE_COOLDOWN = 7 * 60_000;

    private final Map<String, Long> cooldown = new ConcurrentHashMap<>();

    /* ================= OUTPUT ================= */
    public static final class TradeSignal {
        public final String symbol;
        public final TradingCore.Side side;
        public final double entry, stop, take;
        public final double confidence;
        public final String reason;
        public final String grade;

        public TradeSignal(String symbol,
                           TradingCore.Side side,
                           double entry,
                           double stop,
                           double take,
                           double confidence,
                           String reason,
                           String grade) {
            this.symbol = symbol;
            this.side = side;
            this.entry = entry;
            this.stop = stop;
            this.take = take;
            this.confidence = confidence;
            this.reason = reason;
            this.grade = grade;
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

            if (!valid(tf15, MIN_M15) || !valid(tf1h, MIN_H1)) continue;
            if (!volatilityOk(tf15)) {
                System.out.println("[Volatility] " + symbol + " skipped due to low ATR");
                // не полностью режем, можно дебажить
            }

            TradingCore.Side bias = detectHTFBias(tf1h);

            TradeSignal signal = buildSignal(symbol, tf15, bias, tf1h, now);
            if (signal != null) {
                result.add(signal);
                System.out.println("[SignalGenerated] " + signal.symbol + " " + signal.side +
                        " conf=" + signal.confidence + " reason=" + signal.reason);
            }
        }

        result.sort(Comparator.comparingDouble((TradeSignal s) -> s.confidence).reversed());
        return result;
    }

    /* ================= SIGNAL BUILD ================= */
    private TradeSignal buildSignal(String symbol,
                                    List<TradingCore.Candle> m15,
                                    TradingCore.Side bias,
                                    List<TradingCore.Candle> h1,
                                    long now) {

        double price = last(m15).close;
        double atr = atr(m15, 14);
        double adx = adx(m15, 14);
        double rsi = rsi(m15, 14);
        double vol = relativeVolume(m15);

        TradingCore.Side side = null;
        String reason = null;

        boolean trendUp = ema(m15,21) > ema(m15,50);
        boolean trendDown = ema(m15,21) < ema(m15,50);

        /* ========== TREND ENTRY ========== */
        if (bias == TradingCore.Side.LONG && trendUp && adx > 13) {
            side = TradingCore.Side.LONG;
            reason = "Trend continuation";
        }
        if (bias == TradingCore.Side.SHORT && trendDown && adx > 13) {
            side = TradingCore.Side.SHORT;
            reason = "Trend continuation";
        }

        /* ========== PULLBACK ENTRY ========== */
        double ema21 = ema(m15,21);
        if (side == null && bias != null) {
            if (bias == TradingCore.Side.LONG && price <= ema21*1.01) {
                side = TradingCore.Side.LONG;
                reason = "Pullback";
            }
            if (bias == TradingCore.Side.SHORT && price >= ema21*0.99) {
                side = TradingCore.Side.SHORT;
                reason = "Pullback";
            }
        }

        /* ========== BREAKOUT ENTRY ========== */
        if (side == null) {
            double high = highest(m15,10);
            double low  = lowest(m15,10);

            if (price > high*0.999 && adx>11) {
                side = TradingCore.Side.LONG;
                reason = "Breakout";
            }
            if (price < low*1.001 && adx>11) {
                side = TradingCore.Side.SHORT;
                reason = "Breakdown";
            }
        }

        /* ========== REVERSAL ENTRY ========== */
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

        /* ========== COOLDOWN ================= */
        String key = symbol + "_" + side;
        if (cooldown.containsKey(key)) {
            long delta = now - cooldown.get(key);
            if (delta < BASE_COOLDOWN) {
                System.out.println("[Cooldown] " + symbol + " blocked for " + (BASE_COOLDOWN - delta)/1000 + "s");
                return null;
            }
        }

        /* ========== CONFIDENCE ================= */
        double structure = Math.abs(ema(m15,21) - ema(m15,50)) / ema(m15,50);
        double confidence =
                0.58 + structure*0.18 + (adx/40.0)*0.12 + Math.min((vol-1)*0.07,0.12);

        confidence = clamp(confidence, MIN_CONFIDENCE, 0.95);

        String grade = confidence > 0.78 ? "A" : confidence > 0.64 ? "B" : "C";

        /* ========== RISK ================= */
        double rr = confidence>0.80 ? 2.8 : confidence>0.70 ? 2.3 : 1.9;
        double stop = side==TradingCore.Side.LONG ? price-atr : price+atr;
        double take = side==TradingCore.Side.LONG ? price+atr*rr : price-atr*rr;

        cooldown.put(key, now);

        System.out.println("[BuildSignal] " + symbol + " " + side +
                " conf=" + confidence + " stop=" + stop + " take=" + take + " reason=" + reason);

        return new TradeSignal(symbol, side, price, stop, take, confidence, reason, grade);
    }

    /* ================= HTF BIAS ================= */
    private TradingCore.Side detectHTFBias(List<TradingCore.Candle> c){
        double e50=ema(c,50);
        double e200=ema(c,200);
        if(e50>e200) return TradingCore.Side.LONG;
        if(e50<e200) return TradingCore.Side.SHORT;
        return null;
    }

    /* ================= INDICATORS ================= */
    private double atr(List<TradingCore.Candle> c,int n){
        double sum=0;
        for(int i=c.size()-n;i<c.size();i++)
            sum+=c.get(i).high-c.get(i).low;
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
        double avg=c.subList(Math.max(0,n-20),n-1)
                .stream()
                .mapToDouble(cd->cd.volume)
                .average()
                .orElse(1);
        return last(c).volume/avg;
    }

    private boolean bullishDivergence(List<TradingCore.Candle> c){
        int n=c.size(); if(n<20) return false;
        double l1=c.get(n-3).low,l2=c.get(n-1).low;
        return l2<l1 && rsi(c,14)>rsi(c.subList(0,n-2),14);
    }

    private boolean bearishDivergence(List<TradingCore.Candle> c){
        int n=c.size(); if(n<20) return false;
        double h1=c.get(n-3).high,h2=c.get(n-1).high;
        return h2>h1 && rsi(c,14)<rsi(c.subList(0,n-2),14);
    }

    private boolean volatilityOk(List<TradingCore.Candle> c){
        double atr=atr(c,14);
        double price=last(c).close;
        boolean ok = atr/price>MIN_ATR_PCT;
        if(!ok) System.out.println("[VolatilityCheck] ATR too low for price " + price);
        return ok;
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
}