package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class SignalOptimizer {

    private static final int MAX_TICKS = 40;
    private static final double ALPHA = 0.35;

    private final Map<String, Deque<Double>> tickPriceDeque;
    private final Map<String, SignalSender.MicroTrendResult> microTrendCache =
            new ConcurrentHashMap<>();

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = tickPriceDeque;
    }

    /* ======================= MICRO TREND ======================= */
    public SignalSender.MicroTrendResult computeMicroTrend(String symbol) {

        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < 6)
            return new SignalSender.MicroTrendResult(0, 0, 0);

        List<Double> arr = new ArrayList<>(dq);
        int size = arr.size();
        int n = Math.min(size, MAX_TICKS);

        double speed = 0;
        double accel = 0;

        for (int i = size - n + 1; i < size; i++) {
            double diff = arr.get(i) - arr.get(i - 1);
            double prevSpeed = speed;
            speed = ALPHA * diff + (1 - ALPHA) * speed;
            accel = ALPHA * (speed - prevSpeed) + (1 - ALPHA) * accel;
        }

        double avg = arr.subList(size - n, size)
                .stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(arr.get(size - 1));

        SignalSender.MicroTrendResult res =
                new SignalSender.MicroTrendResult(speed, accel, avg);

        microTrendCache.put(symbol, res);
        return res;
    }

    /* ======================= CONFIDENCE ======================= */
    public double adjustConfidence(SignalSender.Signal s, double base) {

        SignalSender.MicroTrendResult m = microTrendCache.get(s.symbol);
        double c = base;

        if (m != null) {

            double impulse = Math.abs(m.speed) + Math.abs(m.accel);

            // рынок мёртв → режем
            if (impulse < 0.0004) c *= 0.78;

                // сильный импульс → усиливаем
            else if (impulse > 0.0012) c += 0.06;

            // направление против микротренда → штраф
            if ((m.speed > 0 && s.direction.equals("SHORT")) ||
                    (m.speed < 0 && s.direction.equals("LONG"))) {
                c *= 0.85;
            }
        }

        if (s.impulse) c += 0.04;

        return clamp(c, 0.50, 0.92);
    }

    /* ======================= STOP / TAKE ======================= */
    public void adjustStopTake(SignalSender.Signal s, double atr) {

        double volPct = atr / s.price;
        volPct = clamp(volPct, 0.008, 0.035);

        double rr =
                s.confidence > 0.7 ? 2.2 :
                        s.confidence > 0.6 ? 1.9 : 1.6;

        if ("LONG".equals(s.direction)) {
            s.stop = s.price * (1 - volPct);
            s.take = s.price * (1 + volPct * rr);
        } else {
            s.stop = s.price * (1 + volPct);
            s.take = s.price * (1 - volPct * rr);
        }

        s.leverage = clamp(
                2.0 + (s.confidence - 0.5) * 8,
                2.0,
                6.5
        );
    }

    /* ======================= UTILS ======================= */
    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }
}
