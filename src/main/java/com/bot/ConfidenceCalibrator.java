package com.bot;

public class ConfidenceCalibrator {

    // подобрано под твой диапазон 0.55–0.9
    private static final double A = 9.0;
    private static final double B = 6.2;

    public static double calibrate(double raw) {
        raw = Math.max(0.0, Math.min(1.0, raw));
        return 1.0 / (1.0 + Math.exp(-(A * raw - B)));
    }
}
