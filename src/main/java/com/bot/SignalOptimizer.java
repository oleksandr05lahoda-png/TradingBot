package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SignalOptimizer {

    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, SignalSender.MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    // ---------------- Compute microtrend ----------------
    public SignalSender.MicroTrendResult computeMicroTrend(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < 5) return new SignalSender.MicroTrendResult(0,0,0);
        List<Double> arr = new ArrayList<>(dq);
        int n = Math.min(arr.size(), 20); // берем больше тиков

        double alpha = 0.3; // сглаживание
        double speed = 0;
        for (int i = arr.size() - n + 1; i < arr.size(); i++) {
            double diff = arr.get(i) - arr.get(i-1);
            speed = alpha * diff + (1-alpha) * speed;
        }
        double accel = 0;
        if (arr.size() >= 3) {
            double lastDiff = arr.get(arr.size()-1) - arr.get(arr.size()-2);
            double prevDiff = arr.get(arr.size()-2) - arr.get(arr.size()-3);
            accel = alpha * (lastDiff - prevDiff) + (1-alpha) * accel;
        }
        double avg = arr.stream().mapToDouble(Double::doubleValue).average().orElse(arr.get(arr.size()-1));
        SignalSender.MicroTrendResult res = new SignalSender.MicroTrendResult(speed, accel, avg);
        microTrendCache.put(symbol, res);
        return res;
    }

    // ---------------- Adjust signal confidence ----------------
    public double adjustConfidence(SignalSender.Signal s, double baseConf) {
        SignalSender.MicroTrendResult micro = microTrendCache.get(s.symbol);
        if (micro != null) {
            // Если тренд замедляется → снижаем confidence
            if (Math.abs(micro.speed) < 0.0005 && Math.abs(micro.accel) < 0.0002) {
                baseConf *= 0.75 + Math.abs(micro.speed) * 100;
            }
        }
        // Увеличиваем confidence если сильный импульс
        if (s.impulse) baseConf += 0.05;
        // Ограничиваем диапазон
        baseConf = Math.max(0.5, Math.min(0.95, baseConf));
        return baseConf;
    }

    // ---------------- Dynamic stop/take ----------------
    public void adjustStopTake(SignalSender.Signal s, double atr) {
        double pct = Math.max(0.01, atr / s.price);
        if (s.direction.equals("LONG")) {
            s.stop = s.price * (1 - pct);
            s.take = s.price * (1 + pct*2);
        } else {
            s.stop = s.price * (1 + pct);
            s.take = s.price * (1 - pct*2);
        }
        s.leverage = Math.max(2.0, Math.min(7.0, 2.0 + (s.confidence - 0.5) * 10));
    }

}
