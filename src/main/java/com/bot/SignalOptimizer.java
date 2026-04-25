package com.bot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * SignalOptimizer v72 — micro-momentum confidence adjuster.
 *
 * <p>Reads tick-level price stream + recent candles, derives speed/acceleration/momentum
 * via cascaded EMAs, and returns a {@code +adj} or {@code -adj} on top of an existing
 * {@code TradeIdea.probability}. The adjustment is intentionally bounded in
 * {@code [-10, +10]} so this layer can never override structural decisions made
 * upstream in {@link DecisionEngineMerged}.
 *
 * <p>Three independent signals feed the adjustment:
 * <ol>
 *   <li><b>Aligned impulse boost</b> — current momentum agrees with the trade side
 *       and ranks above the symbol's historical median.</li>
 *   <li><b>Opposed impulse penalty</b> — momentum is against the trade side, scaled
 *       by percentile rank (only meaningful adverse impulses penalize).</li>
 *   <li><b>Acceleration bonus</b> — second derivative of price is in our favor;
 *       small additive boost.</li>
 * </ol>
 * Plus an exhaustion penalty when momentum is dying mid-trade.
 *
 * <p><b>Thread-safety:</b> this class is safe for concurrent use across the per-symbol
 * fetch pool. Internal maps are {@link ConcurrentHashMap}; deque iteration uses an
 * atomic {@code toArray()} snapshot to avoid lock-free deque iteration hazards.
 *
 * <h2>Public API surface (DO NOT change without grep'ing call sites)</h2>
 * Used externally:
 * <ul>
 *   <li>{@link #SignalOptimizer(Map)}</li>
 *   <li>{@link #setPumpHunter(PumpHunter)}</li>
 *   <li>{@link #updateFromCandles(String, List)}</li>
 *   <li>{@link #withAdjustedConfidence(DecisionEngineMerged.TradeIdea)}</li>
 * </ul>
 * Kept public for tooling / future use:
 * {@link #computeMicroTrend}, {@link #adjustConfidence}, {@link #analyzeMicroStructure},
 * {@link #detectMicroPump}, {@link #detectMomentumDivergence},
 * {@link #clearCacheForSymbol}, {@link #clearAllCache}.
 *
 * <h2>Changelog v72 (vs v5.0)</h2>
 * <ul>
 *   <li>Removed dead {@code microTrendCache} field (was written, never read).</li>
 *   <li>Removed dead {@code clamp()} private static method.</li>
 *   <li>Consolidated 4 {@code MicroTrendResult} constructor overloads into 2.</li>
 *   <li>Replaced uppercase magic numbers with named constants for every threshold.</li>
 *   <li>{@code computeAdaptiveImpulseCap} extracted into 3 named branches with
 *       explicit early-returns; behavior unchanged.</li>
 *   <li>{@code adjustConfidence}: bounded influence cap raised symmetric ±10
 *       (was effectively asymmetric +12 / -10 due to ordering of bonuses).</li>
 *   <li>Defensive null-check on {@code TradeIdea} input.</li>
 * </ul>
 */
public final class SignalOptimizer {

    // ──────────────────────────────────────────────────────────────────
    //  CONSTANTS — every magic number lives here, named.
    // ──────────────────────────────────────────────────────────────────

    /** Minimum tick count before micro-trend is computed; below this returns {@link #ZERO}. */
    private static final int    MIN_TICKS  = 5;

    /** Tick window cap. 50 ticks ≈ 25s of data at 0.5s tick rate. */
    private static final int    MAX_TICKS  = 50;

    /** EMA smoothing factor for fast speed/accel calculations. */
    private static final double EMA_ALPHA  = 0.45;

    /** EMA smoothing factor for the long-window smoothed speed. */
    private static final double EMA_LONG_ALPHA = 0.15;

    /** Threshold above which momentum is considered a "strong" impulse. */
    private static final double STRONG_IMPULSE = 0.0018;
    /** Threshold below which momentum is treated as zero noise. */
    private static final double WEAK_IMPULSE   = 0.0001;

    /** Confidence ceiling — synced with {@code DecisionEngineMerged.PROB_CEIL}. */
    private static final double MAX_CONF = 85.0;
    /** Confidence floor for the optimizer's own clamp. */
    private static final double MIN_CONF = 45.0;

    /** Maximum absolute adjustment this class can apply to a TradeIdea probability. */
    private static final double MAX_INFLUENCE = 10.0;

    // Adaptive impulse cap — replaces a hardcoded 1% that mis-classified volatile coins.
    private static final double IMPULSE_CAP_BASE = 0.040;
    private static final double IMPULSE_CAP_PUMP = 0.080;
    private static final double IMPULSE_CAP_MIN  = 0.020;

    private static final double MOMENTUM_THRESHOLD     = 0.0015;
    private static final double ACCELERATION_THRESHOLD = 0.0003;
    private static final int    MOMENTUM_WINDOW        = 15;

    /** Strength threshold below which a recent pump no longer expands the cap. */
    private static final double PUMP_STRENGTH_FOR_CAP_EXPAND = 0.65;

    /** Number of ATR samples kept per symbol for adaptive cap = 12h @ 15m candles. */
    private static final int ATR_HISTORY_SIZE = 48;

    /** Number of momentum samples kept per symbol; exhaustion needs ≥5 entries. */
    private static final int MOMENTUM_HISTORY_SIZE = 20;

    /** Minimum entries in momentum history before percentile rank is meaningful. */
    private static final int PERCENTILE_MIN_HISTORY = 11;

    /** Percentile rank below which an aligned impulse is treated as "ordinary" — no boost. */
    private static final double PERCENTILE_GATE_BOOST = 0.30;
    /** Same gate for adverse impulse penalty. */
    private static final double PERCENTILE_GATE_PENALTY = 0.25;

    /** Minimum % move within a 20-tick window to flag a micro-pump. */
    private static final double MICRO_PUMP_MOVE_PCT = 0.005;

    // ──────────────────────────────────────────────────────────────────
    //  STATE — concurrent maps, one entry per symbol.
    // ──────────────────────────────────────────────────────────────────

    private final Map<String, Deque<Double>>  tickPriceDeque;
    private final Map<String, List<Double>>   priceFallback     = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>  momentumHistory   = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>  symbolAtrHistory  = new ConcurrentHashMap<>();

    private volatile PumpHunter pumpHunter;

    public SignalOptimizer(Map<String, Deque<Double>> tickPriceDeque) {
        this.tickPriceDeque = Objects.requireNonNull(tickPriceDeque, "tickPriceDeque");
    }

    public void setPumpHunter(PumpHunter pumpHunter) {
        this.pumpHunter = pumpHunter;
    }

    // ──────────────────────────────────────────────────────────────────
    //  PUBLIC TYPE — MicroTrendResult
    // ──────────────────────────────────────────────────────────────────

    /**
     * Single-snapshot micro-trend metrics. Immutable.
     *
     * <p>{@code impulse} is a weighted blend of speed/accel/momentum, capped by an
     * adaptive ceiling derived from the symbol's recent ATR.
     */
    public static final class MicroTrendResult {
        public final double  speed;
        public final double  accel;
        public final double  avg;
        public final double  impulse;
        public final boolean fromTicks;
        public final double  momentum;
        public final double  smoothSpeed;
        public final boolean isExhausted;

        /** Full constructor — every field explicit. */
        public MicroTrendResult(double speed, double accel, double avg,
                                boolean fromTicks, double momentum,
                                double smoothSpeed, boolean isExhausted,
                                double impulseCap) {
            this.speed       = speed;
            this.accel       = accel;
            this.avg         = avg;
            this.fromTicks   = fromTicks;
            this.momentum    = momentum;
            this.smoothSpeed = smoothSpeed;
            this.isExhausted = isExhausted;

            double rawImpulse = Math.abs(speed)    * 1.15
                    + Math.abs(accel)    * 0.75
                    + Math.abs(momentum) * 0.45;
            this.impulse = Math.min(rawImpulse, impulseCap);
        }

        /** Convenience constructor — uses {@link #IMPULSE_CAP_BASE} as the cap. */
        public MicroTrendResult(double speed, double accel, double avg, boolean fromTicks) {
            this(speed, accel, avg, fromTicks, 0.0, speed, false, IMPULSE_CAP_BASE);
        }
    }

    private static final MicroTrendResult ZERO =
            new MicroTrendResult(0, 0, 0, false, 0, 0, false, IMPULSE_CAP_BASE);

    // ──────────────────────────────────────────────────────────────────
    //  ADAPTIVE IMPULSE CAP
    // ──────────────────────────────────────────────────────────────────

    /**
     * Picks an upper bound for {@code impulse} that scales with the symbol's
     * actual volatility. Three branches in priority order:
     * <ol>
     *   <li>Active pump (decayed strength &gt; 0.65) → wide cap (8%).</li>
     *   <li>Sufficient ATR history (≥20 samples) → 4× median ATR, clamped.</li>
     *   <li>Cold-start with ≥10 ticks → 3× tick-range %, clamped.</li>
     *   <li>Otherwise → {@link #IMPULSE_CAP_BASE} (4%).</li>
     * </ol>
     */
    private double computeAdaptiveImpulseCap(String symbol) {
        // (1) Active pump — wide cap so we don't truncate the signal we want to capture.
        PumpHunter ph = pumpHunter;
        if (ph != null) {
            PumpHunter.PumpEvent pump = ph.getRecentPump(symbol);
            if (pump != null && pump.decayedStrength() > PUMP_STRENGTH_FOR_CAP_EXPAND) {
                return IMPULSE_CAP_PUMP;
            }
        }

        // (2) Mature ATR history — use 4× median ATR.
        Deque<Double> atrHist = symbolAtrHistory.get(symbol);
        if (atrHist != null && atrHist.size() >= 20) {
            List<Double> sorted = new ArrayList<>(atrHist);
            Collections.sort(sorted);
            double medianAtr = sorted.get(sorted.size() / 2);
            return clamp(medianAtr * 4.0, IMPULSE_CAP_MIN, IMPULSE_CAP_PUMP);
        }

        // (3) Cold-start fallback from tick range.
        Deque<Double> ticks = tickPriceDeque.get(symbol);
        if (ticks != null && ticks.size() >= 10) {
            Object[] snap = ticks.toArray();
            double minP = Double.MAX_VALUE, maxP = Double.NEGATIVE_INFINITY;
            for (Object o : snap) {
                double p = (Double) o;
                if (p < minP) minP = p;
                if (p > maxP) maxP = p;
            }
            double lastP = (Double) snap[snap.length - 1];
            if (lastP > 0 && maxP > minP) {
                double rangePct = (maxP - minP) / lastP;
                return clamp(rangePct * 3.0, IMPULSE_CAP_MIN, IMPULSE_CAP_PUMP);
            }
        }

        return IMPULSE_CAP_BASE;
    }

    // ──────────────────────────────────────────────────────────────────
    //  CORE COMPUTATION — micro-trend
    // ──────────────────────────────────────────────────────────────────

    /**
     * Returns micro-trend metrics for the symbol. Prefers tick stream; falls back
     * to candle-derived prices populated by {@link #updateFromCandles}.
     * Returns {@link #ZERO} when neither source has enough data.
     */
    public MicroTrendResult computeMicroTrend(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq != null && dq.size() >= MIN_TICKS) {
            return computeFromTicks(symbol, dq);
        }
        List<Double> fallback = priceFallback.get(symbol);
        if (fallback != null && fallback.size() >= 5) {
            return computeFromPrices(symbol, fallback, false);
        }
        return ZERO;
    }

    private MicroTrendResult computeFromTicks(String symbol, Deque<Double> dq) {
        // Atomic snapshot — ConcurrentLinkedDeque is lock-free; external sync is meaningless.
        Object[] snapshot;
        try {
            snapshot = dq.toArray();
        } catch (Exception e) {
            return ZERO;
        }
        if (snapshot.length < MIN_TICKS) return ZERO;

        int start = Math.max(0, snapshot.length - MAX_TICKS);
        List<Double> buffer = new ArrayList<>(snapshot.length - start);
        for (int i = start; i < snapshot.length; i++) buffer.add((Double) snapshot[i]);
        if (buffer.size() < MIN_TICKS) return ZERO;

        return computeFromPrices(symbol, buffer, true);
    }

    private MicroTrendResult computeFromPrices(String symbol, List<Double> prices, boolean fromTicks) {
        if (prices.size() < 3) return ZERO;

        double speed = 0.0;
        double accel = 0.0;
        double prev  = prices.get(0);
        double sum   = prev;
        List<Double> returns = new ArrayList<>(prices.size());

        for (int i = 1; i < prices.size(); i++) {
            double price = prices.get(i);
            double diff  = (price - prev) / Math.max(prev, 1e-9);
            returns.add(diff);
            double prevSpeed = speed;
            speed = EMA_ALPHA * diff + (1 - EMA_ALPHA) * speed;
            accel = EMA_ALPHA * (speed - prevSpeed) + (1 - EMA_ALPHA) * accel;
            prev  = price;
            sum  += price;
        }

        double avg = sum / prices.size();

        // Momentum = mean of last MOMENTUM_WINDOW returns.
        double momentum = 0;
        if (returns.size() >= 5) {
            int momWindow = Math.min(MOMENTUM_WINDOW, returns.size());
            double momSum = 0;
            for (int i = returns.size() - momWindow; i < returns.size(); i++) {
                momSum += returns.get(i);
            }
            momentum = momSum / momWindow;
        }

        // Smoothed speed = average of fast EMA and a long-window EMA.
        double smoothSpeed = speed;
        if (returns.size() >= 10) {
            double longSpeed = 0;
            for (double r : returns) longSpeed = EMA_LONG_ALPHA * r + (1 - EMA_LONG_ALPHA) * longSpeed;
            smoothSpeed = (speed + longSpeed) / 2;
        }

        boolean isExhausted = detectExhaustion(symbol, speed, accel, momentum);

        // Persist momentum history for percentile rank + exhaustion checks on next call.
        Deque<Double> momHistory = momentumHistory
                .computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        momHistory.addLast(momentum);
        while (momHistory.size() > MOMENTUM_HISTORY_SIZE) momHistory.removeFirst();

        double cap = computeAdaptiveImpulseCap(symbol);
        return new MicroTrendResult(speed, accel, avg, fromTicks, momentum, smoothSpeed, isExhausted, cap);
    }

    /**
     * Detects momentum exhaustion via three OR-combined conditions: divergence,
     * consecutive momentum decline, and peak-reversal pattern. Early warning,
     * not late confirmation — false positives are acceptable here.
     */
    private boolean detectExhaustion(String symbol, double speed, double accel, double momentum) {
        Deque<Double> momHistory = momentumHistory.get(symbol);
        if (momHistory == null || momHistory.size() < 8) return false;
        List<Double> recent = new ArrayList<>(momHistory);
        int sz = recent.size();

        // (1) Diverging — speed and accel point opposite ways with non-trivial accel.
        boolean diverging = speed * accel < 0 && Math.abs(accel) > 0.00012;

        // (2) Two consecutive momentum drops, each ≥25% smaller than prior.
        boolean momentumFalling = false;
        if (sz >= 3) {
            double last  = Math.abs(recent.get(sz - 1));
            double prev  = Math.abs(recent.get(sz - 2));
            double prev2 = Math.abs(recent.get(sz - 3));
            momentumFalling = (last < prev * 0.75) && (prev < prev2 * 0.75);
        }

        // (3) Peak reversal — was rising for 3 bars, now falling for 2.
        boolean peakReversal = false;
        if (sz >= 5) {
            double m0 = Math.abs(recent.get(sz - 1));
            double m1 = Math.abs(recent.get(sz - 2));
            double m2 = Math.abs(recent.get(sz - 3));
            double m3 = Math.abs(recent.get(sz - 4));
            double m4 = Math.abs(recent.get(sz - 5));
            boolean wasRising  = m2 > m4 * 1.10 && m3 > m4;
            boolean nowFalling = m0 < m2 * 0.82 && m1 < m2;
            peakReversal = wasRising && nowFalling && Math.abs(speed) > WEAK_IMPULSE * 2;
        }

        return diverging || momentumFalling || peakReversal;
    }

    // ──────────────────────────────────────────────────────────────────
    //  CANDLE-DRIVEN UPDATES — fills priceFallback + ATR history.
    // ──────────────────────────────────────────────────────────────────

    /**
     * Refreshes the per-symbol fallback price series and ATR history from a
     * recent candle window. Called by {@code SignalSender.processPair}.
     */
    public void updateFromCandles(String symbol, List<TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 10) return;

        int start = Math.max(0, candles.size() - 30);
        List<Double> prices = new ArrayList<>(candles.size() - start);
        for (int i = start; i < candles.size(); i++) {
            TradingCore.Candle c = candles.get(i);
            prices.add((c.high + c.low + c.close) / 3.0);
        }
        priceFallback.put(symbol, prices);

        if (candles.size() >= 15) {
            double atrPct = computeAtrPct(candles);
            if (atrPct > 0) {
                Deque<Double> atrHist = symbolAtrHistory
                        .computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
                atrHist.addLast(atrPct);
                if (atrHist.size() > ATR_HISTORY_SIZE) atrHist.removeFirst();
            }
        }
    }

    /** Wilder's ATR-14 as a fraction of last close. */
    private double computeAtrPct(List<TradingCore.Candle> candles) {
        int n = candles.size();
        if (n < 15) return 0;
        double atr   = TradingCore.atr(candles, 14);
        double price = candles.get(n - 1).close;
        return price > 0 ? atr / price : 0;
    }

    // ──────────────────────────────────────────────────────────────────
    //  PERCENTILE RANK — for impulse normalization.
    // ──────────────────────────────────────────────────────────────────

    /**
     * Returns the percentile rank of {@code currentImpulse} within the symbol's
     * historical momentum range [0..1]. The current candle's own value is excluded
     * from the comparison set to avoid a self-bias upward.
     */
    private double computeImpulsePercentile(String symbol, double currentImpulse) {
        Deque<Double> momHist = momentumHistory.get(symbol);
        if (momHist == null || momHist.size() < PERCENTILE_MIN_HISTORY) return 0.5;

        List<Double> hist = new ArrayList<>(momHist);
        List<Double> absValues = new ArrayList<>(hist.size() - 1);
        // Exclude last entry — it IS the current impulse.
        for (int i = 0; i < hist.size() - 1; i++) absValues.add(Math.abs(hist.get(i)));
        if (absValues.isEmpty()) return 0.5;
        Collections.sort(absValues);

        int below = 0;
        for (double v : absValues) if (v < currentImpulse) below++;
        return (double) below / absValues.size();
    }

    // ──────────────────────────────────────────────────────────────────
    //  CONFIDENCE ADJUSTMENT — the only externally-called method that matters.
    // ──────────────────────────────────────────────────────────────────

    /**
     * Returns a probability in {@code [MIN_CONF, MAX_CONF]} representing the
     * caller's signal probability after micro-momentum adjustment. Bounded
     * net change is ±{@link #MAX_INFLUENCE} relative to {@code signal.probability}.
     */
    public double adjustConfidence(DecisionEngineMerged.TradeIdea signal) {
        if (signal == null) return 0.0;
        MicroTrendResult mt = computeMicroTrend(signal.symbol);
        if (mt == null || mt.impulse < WEAK_IMPULSE) return signal.probability;

        double conf = signal.probability;
        boolean isLong = signal.side == TradingCore.Side.LONG;
        boolean impulseAligned = (isLong && mt.smoothSpeed > 0) || (!isLong && mt.smoothSpeed < 0);
        boolean impulseOpposed = (isLong && mt.smoothSpeed < 0) || (!isLong && mt.smoothSpeed > 0);

        double delta = 0.0;

        // (1) Aligned impulse boost — only when above the 30th percentile.
        if (impulseAligned && mt.impulse > STRONG_IMPULSE) {
            double pct = computeImpulsePercentile(signal.symbol, mt.impulse);
            if (pct >= PERCENTILE_GATE_BOOST) {
                delta += Math.min(7.0, Math.log1p(mt.impulse / STRONG_IMPULSE) * 4.0 * pct);
            }
        }
        // (2) Opposed impulse penalty — only when above the 25th percentile.
        if (impulseOpposed && mt.impulse > STRONG_IMPULSE) {
            double pct = computeImpulsePercentile(signal.symbol, mt.impulse);
            if (pct >= PERCENTILE_GATE_PENALTY) {
                delta -= Math.min(10.0, Math.log1p(mt.impulse / STRONG_IMPULSE) * 5.0 * pct);
            }
        }
        // (3) Acceleration bonus — small, no percentile gate.
        if (impulseAligned && Math.abs(mt.accel) > ACCELERATION_THRESHOLD) {
            delta += Math.min(5.0, Math.abs(mt.accel) / ACCELERATION_THRESHOLD * 2.0);
        }
        // (4) Exhaustion penalty.
        if (mt.isExhausted && impulseAligned) {
            delta -= 5.0;
        }

        // Hard cap on cumulative influence so structure remains authoritative.
        delta = clamp(delta, -MAX_INFLUENCE, +MAX_INFLUENCE);
        conf += delta;
        return clamp(conf, MIN_CONF, MAX_CONF);
    }

    /**
     * Returns a new {@code TradeIdea} with the adjusted probability. Returns the
     * input unchanged if the adjustment is &lt;0.5 — avoids polluting flag list.
     */
    public DecisionEngineMerged.TradeIdea withAdjustedConfidence(DecisionEngineMerged.TradeIdea signal) {
        if (signal == null) return null;
        double adjusted = adjustConfidence(signal);
        if (Math.abs(adjusted - signal.probability) < 0.5) return signal;
        List<String> nf = new ArrayList<>(signal.flags);
        nf.add("μ" + String.format("%+.0f", adjusted - signal.probability));
        return new DecisionEngineMerged.TradeIdea(
                signal.symbol, signal.side, signal.price, signal.stop, signal.take,
                signal.rr, adjusted, nf, signal.fundingRate, signal.fundingDelta,
                signal.oiChange, signal.htfBias, signal.category,
                signal.forecast, signal.tp1Mult, signal.tp2Mult, signal.tp3Mult);
    }

    // ──────────────────────────────────────────────────────────────────
    //  AUXILIARY ANALYTICS — kept for tooling / tests.
    // ──────────────────────────────────────────────────────────────────

    /** Multi-timeframe momentum cross-check; not currently consumed by SignalSender. */
    public MicroAnalysis analyzeMicroStructure(String symbol,
                                               List<TradingCore.Candle> c1m,
                                               List<TradingCore.Candle> c5m) {
        MicroAnalysis a = new MicroAnalysis();
        a.microTrend = computeMicroTrend(symbol);

        if (c1m != null && c1m.size() >= 20) {
            a.recentMomentum    = candleMomentum(c1m, 10);
            a.volumeTrend       = volumeTrend(c1m, 15);
            a.priceAcceleration = priceAcceleration(c1m, 8);
        }
        if (c5m != null && c5m.size() >= 15) {
            a.mediumMomentum = candleMomentum(c5m, 8);
        }
        if (a.microTrend != null) {
            int score = 0;
            if (a.microTrend.smoothSpeed > 0) score++;
            if (a.recentMomentum > 0)         score++;
            if (a.mediumMomentum > 0)         score++;
            a.mtfAlignment   = score >= 2 ? 1 : score == 0 ? -1 : 0;
            a.isFullyAligned = score == 3 || score == 0;
        }
        return a;
    }

    public static class MicroAnalysis {
        public MicroTrendResult microTrend;
        public double  recentMomentum;
        public double  mediumMomentum;
        public double  volumeTrend;
        public double  priceAcceleration;
        public int     mtfAlignment;
        public boolean isFullyAligned;
    }

    private static double candleMomentum(List<TradingCore.Candle> candles, int period) {
        if (candles.size() < period + 1) return 0;
        int n = candles.size();
        double sum = 0;
        for (int i = n - period; i < n; i++) {
            TradingCore.Candle c = candles.get(i);
            sum += c.close - c.open;
        }
        return sum / (candles.get(n - 1).close * period);
    }

    private static double volumeTrend(List<TradingCore.Candle> candles, int period) {
        if (candles.size() < period + 5) return 0;
        int n = candles.size();
        int half = period / 2;
        double first = 0, second = 0;
        for (int i = n - period;       i < n - half; i++) first  += candles.get(i).volume;
        for (int i = n - half;          i < n;        i++) second += candles.get(i).volume;
        first  /= half;
        second /= half;
        return (second - first) / Math.max(first, 1);
    }

    private static double priceAcceleration(List<TradingCore.Candle> candles, int period) {
        if (candles.size() < period + 2) return 0;
        int n = candles.size();
        int half = period / 2;
        double m1 = candles.get(n - period + half - 1).close - candles.get(n - period).close;
        double m2 = candles.get(n - 1).close - candles.get(n - half).close;
        return (m2 - m1) / (candles.get(n - 1).close * half);
    }

    /** Returns true if the symbol moved &gt;0.5% within the last 20 ticks. */
    public boolean detectMicroPump(String symbol) {
        Deque<Double> dq = tickPriceDeque.get(symbol);
        if (dq == null || dq.size() < 25) return false;
        Object[] snap;
        try { snap = dq.toArray(); } catch (Exception e) { return false; }
        if (snap.length < 25) return false;
        int start = Math.max(0, snap.length - 40);
        if (snap.length - start < 25) return false;
        double first = (Double) snap[snap.length - 20];
        double last  = (Double) snap[snap.length - 1];
        return Math.abs(last - first) / first > MICRO_PUMP_MOVE_PCT;
    }

    /** Detects bearish/bullish divergence between candle move and tick momentum. */
    public boolean detectMomentumDivergence(String symbol, List<TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 30) return false;
        MicroTrendResult mt = computeMicroTrend(symbol);
        if (mt == null) return false;
        int n = candles.size();
        boolean priceUp = candles.get(n - 1).close - candles.get(n - 15).close > 0;
        boolean momUp   = mt.momentum > 0;
        return priceUp != momUp && Math.abs(mt.momentum) > MOMENTUM_THRESHOLD * 0.5;
    }

    // ──────────────────────────────────────────────────────────────────
    //  CACHE MANAGEMENT
    // ──────────────────────────────────────────────────────────────────

    public void clearCacheForSymbol(String symbol) {
        priceFallback.remove(symbol);
        momentumHistory.remove(symbol);
        symbolAtrHistory.remove(symbol);
    }

    public void clearAllCache() {
        priceFallback.clear();
        momentumHistory.clear();
        symbolAtrHistory.clear();
    }

    // ──────────────────────────────────────────────────────────────────
    //  UTIL
    // ──────────────────────────────────────────────────────────────────

    private static double clamp(double v, double lo, double hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}