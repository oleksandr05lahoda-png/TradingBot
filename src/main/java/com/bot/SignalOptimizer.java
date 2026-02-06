package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class SignalOptimizer {

    private static final int MAX_TICKS = 40;
    private static final double ALPHA = 0.35;
    private static final double IMPULSE_DEAD = 0.0004;
    private static final double IMPULSE_STRONG = 0.0012;

    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, SignalSender.MicroTrendResult> microTrendCache = new ConcurrentHashMap<>();

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    /* ======================= MICRO TREND ======================= */
    public SignalSender.MicroTrendResult computeMicroTrend(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < 6)
            return new SignalSender.MicroTrendResult(0, 0, 0);

        List<Double> prices = new ArrayList<>(dq);
        int size = prices.size();
        int n = Math.min(size, MAX_TICKS);

        double speed = 0;
        double accel = 0;

        for (int i = size - n + 1; i < size; i++) {
            double diff = prices.get(i) - prices.get(i - 1);
            double prevSpeed = speed;
            speed = ALPHA * diff + (1 - ALPHA) * speed;
            accel = ALPHA * (speed - prevSpeed) + (1 - ALPHA) * accel;
        }

        double avg = prices.subList(size - n, size).stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(prices.get(size - 1));

        SignalSender.MicroTrendResult result = new SignalSender.MicroTrendResult(speed, accel, avg);
        microTrendCache.put(symbol, result);
        return result;
    }

    /* ======================= CONFIDENCE ======================= */
    public double adjustConfidence(SignalSender.Signal s, double base) {
        SignalSender.MicroTrendResult m = microTrendCache.get(s.symbol);
        double conf = base;

        if (m != null) {
            double impulse = Math.abs(m.speed) + Math.abs(m.accel);

            if (impulse < IMPULSE_DEAD) conf *= 0.78;       // рынок мёртв
            else if (impulse > IMPULSE_STRONG) conf += 0.06; // сильный импульс

            // направление против микротренда → штраф
            if ((m.speed > 0 && "SHORT".equals(s.direction)) ||
                    (m.speed < 0 && "LONG".equals(s.direction))) {
                conf *= 0.85;
            }
        }

        if (s.impulse) conf += 0.04;

        return clamp(conf, 0.50, 0.92);
    }

    /* ======================= STOP / TAKE ======================= */
    public void adjustStopTake(SignalSender.Signal s, double atr) {
        double volPct = clamp(atr / s.price, 0.008, 0.035);

        double rr = s.confidence > 0.7 ? 2.2 : s.confidence > 0.6 ? 1.9 : 1.6;

        if ("LONG".equals(s.direction)) {
            s.stop = s.price * (1 - volPct);
            s.take = s.price * (1 + volPct * rr);
        } else {
            s.stop = s.price * (1 + volPct);
            s.take = s.price * (1 - volPct * rr);
        }
    }

    /* ======================= UTILS ======================= */
    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}
