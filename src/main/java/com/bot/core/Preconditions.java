package com.bot.core;

/**
 * Argument checks used across the risk core. Fail-closed: a value that cannot be validated throws
 * rather than being coerced to a safe-looking default.
 */
public final class Preconditions {

    private Preconditions() {}

    public static double positiveFinite(double v, String name) {
        if (!Double.isFinite(v) || v <= 0.0) {
            throw new IllegalArgumentException(name + " must be a finite positive number, got " + v);
        }
        return v;
    }

    public static double nonNegativeFinite(double v, String name) {
        if (!Double.isFinite(v) || v < 0.0) {
            throw new IllegalArgumentException(name + " must be a finite non-negative number, got " + v);
        }
        return v;
    }

    public static double finite(double v, String name) {
        if (!Double.isFinite(v)) {
            throw new IllegalArgumentException(name + " must be finite, got " + v);
        }
        return v;
    }

    public static double inClosedRange(double v, double lo, double hi, String name) {
        finite(v, name);
        if (v < lo || v > hi) {
            throw new IllegalArgumentException(name + " must be within [" + lo + ", " + hi + "], got " + v);
        }
        return v;
    }

    public static int positive(int v, String name) {
        if (v <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + v);
        }
        return v;
    }

    public static <T> T notNull(T v, String name) {
        if (v == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return v;
    }

    public static String notBlank(String v, String name) {
        notNull(v, name);
        if (v.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return v;
    }

    public static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
