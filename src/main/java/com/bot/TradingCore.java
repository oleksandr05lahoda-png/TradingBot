package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║       TradingCore v22.0 — ADAPTIVE QUANT FOUNDATION                      ║
 * ╠══════════════════════════════════════════════════════════════════════════════╣
 * ║                                                                              ║
 * ║  v15.0 CRITICAL FIXES:                                                       ║
 * ║    · [FIX Дыра 3] LR window: 30→10 bars (catches V-reversals)              ║
 * ║    · [FIX Дыра 3] LR Acceleration (2nd derivative) — detects slope change  ║
 * ║    · [FIX KITEUSDT] VolatilitySqueezeGuard — blocks signals in squeeze     ║
 * ║    · Squeeze zone: directionScore×0.25, confidence×0.5                      ║
 * ║                                                                              ║
 * ║  v13.0 NEW — FORECASTING PRIMITIVES (for 8-candle ahead prediction):        ║
 * ║    · LinearRegressionChannel — slope, std-channel, momentum projection      ║
 * ║    · VolumeProfileEngine — VPOC, VAH, VAL, key magnetic levels              ║
 * ║    · TrendPhaseAnalyzer — EARLY/MID/LATE/EXHAUSTION phase detection         ║
 * ║    · FisherTransform — better oscillator, less lag than RSI                 ║
 * ║    · ATRPercentile — current volatility vs historical regime                 ║
 * ║    · SwingStructure — HH/HL vs LH/LL market structure                       ║
 * ║    · OrderBlockDetector — institutional footprint zones                      ║
 * ║    · WyckoffPhaseDetector — accumulation/distribution detection             ║
 * ║                                                                              ║
 * ║  v10.0 PRESERVED — all original math primitives:                            ║
 * ║    · RSI (Wilder), ATR, EMA, SMA, VWAP, Bollinger, Keltner                 ║
 * ║    · MACD, StochRSI, CCI, MFI, OBV, CMF, ADX                              ║
 * ║    · Hurst Exponent, Shannon Entropy                                         ║
 * ║    · Divergence detection, Market Regime, Risk Engine, Adaptive Brain       ║
 * ║                                                                              ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 */
public final class TradingCore {

    private TradingCore() {}

    /* ════════════════════════════════════════════════════════════════
       [v22.0 NEW] ADAPTIVE CONFIG — Self-Tuning Threshold System

       PROBLEM (Gemini critique): 200+ hardcoded magic numbers (0.55, 1.80, 0.35...)
       perfectly describe past data but break when market microstructure changes.

       SOLUTION: Centralized config with Bayesian self-tuning.
       Every threshold starts at a reasonable default and adjusts based on
       actual signal accuracy over time. This replaces brittle overfitting
       with continuous online learning.

       Usage: Instead of hardcoded `mctx.s(0.55)`, use
       `config.getScore("STRUCTURE_HH_HL")` which returns a value that
       ADAPTS based on how often that signal led to winning trades.
       ════════════════════════════════════════════════════════════════ */

    public static final class AdaptiveConfig {

        // Threshold registry: name → [value, wins, total, lastUpdate]
        private final java.util.concurrent.ConcurrentHashMap<String, double[]> thresholds
                = new java.util.concurrent.ConcurrentHashMap<>();

        // Global regime multiplier (set by market volatility)
        private volatile double regimeMultiplier = 1.0;

        // Learning rate — how fast thresholds adapt (lower = more stable)
        private static final double LEARNING_RATE = 0.05;
        private static final int MIN_SAMPLES_FOR_ADAPTATION = 15;
        private static final double MIN_THRESHOLD = 0.10;
        private static final double MAX_THRESHOLD = 0.90;

        public AdaptiveConfig() {
            initDefaults();
        }

        private void initDefaults() {
            // Structure cluster scores
            reg("STRUCTURE_HH_HL",     0.55);
            reg("STRUCTURE_FVG",       0.50);
            reg("STRUCTURE_OB",        0.52);
            reg("STRUCTURE_LIQ_SWEEP", 0.58);
            reg("STRUCTURE_BOOST",     0.18);

            // Momentum cluster scores
            reg("MOMENTUM_ANTI_LAG",   0.65);
            reg("MOMENTUM_IMPULSE",    0.50);
            reg("MOMENTUM_PULLBACK",   0.55);
            reg("MOMENTUM_PUMP",       0.55);
            reg("MOMENTUM_COMPRESSION",0.58);
            reg("MOMENTUM_CLIMAX_REV", 0.65);

            // Volume cluster scores
            reg("VOLUME_DELTA",        0.55);
            reg("VOLUME_SPIKE",        0.22);

            // HTF cluster scores
            reg("HTF_1H",             0.60);
            reg("HTF_2H",             0.55);
            reg("HTF_AGREE_BONUS",    0.30);
            reg("HTF_VWAP",           0.18);

            // Derivatives cluster scores
            reg("DERIV_FR_NEG",       0.45);
            reg("DERIV_FR_POS",       0.40);
            reg("DERIV_OI",           0.25);
            reg("DERIV_DIV",          0.60);

            // Early reversal
            reg("EARLY_REVERSAL",     0.70);

            // Decision thresholds
            reg("MIN_SCORE_DIFF_TREND",  0.16);
            reg("MIN_SCORE_DIFF_RANGE",  0.28);
            reg("MIN_SCORE_DIFF_WEAK",   0.20);
            reg("DYN_THRESH_TREND",      0.68);
            reg("DYN_THRESH_NORMAL",     0.58);

            // Exhaustion multipliers
            reg("EXHAUST_PENALTY",     0.35);
            reg("EXHAUST_HARD_PENALTY",0.12);

            // Confidence parameters
            reg("CONF_CLUSTER_BONUS_6", 0.14);
            reg("CONF_CLUSTER_BONUS_5", 0.12);
            reg("CONF_CLUSTER_BONUS_4", 0.08);
            reg("CONF_CLUSTER_BONUS_3", 0.04);
            reg("CONF_RANGE_BASE",     22.0);
            reg("CONF_CLUSTER_MULT",    3.5);

            // Risk management
            reg("RUBBER_BAND_THR",     0.015);
            reg("STOP_CAP_PCT",        0.030);
        }

        private void reg(String name, double defaultValue) {
            // [value, wins, total, lastUpdateMs]
            thresholds.put(name, new double[]{ defaultValue, 0, 0, System.currentTimeMillis() });
        }

        /** Get current (possibly adapted) value for a threshold */
        public double get(String name) {
            double[] data = thresholds.get(name);
            if (data == null) return 0.5; // safe fallback
            return data[0] * regimeMultiplier;
        }

        /** Get raw value without regime multiplier (for non-score thresholds) */
        public double getRaw(String name) {
            double[] data = thresholds.get(name);
            return data != null ? data[0] : 0.5;
        }

        /** Get score for cluster analysis, scaled by market context */
        public double getScore(String name, double marketScaleFactor) {
            return get(name) * marketScaleFactor;
        }

        /**
         * BAYESIAN UPDATE: After a trade closes, report whether this signal
         * pattern was correct. The threshold adapts towards more accurate values.
         *
         * If a signal with STRUCTURE_HH_HL = 0.55 keeps winning,
         * the score INCREASES (we trust it more).
         * If it keeps losing, the score DECREASES.
         *
         * This is how we escape the "magic number" trap — numbers evolve.
         */
        public void reportOutcome(String name, boolean win) {
            double[] data = thresholds.get(name);
            if (data == null) return;

            synchronized (data) {
                data[1] += win ? 1 : 0; // wins
                data[2] += 1;            // total
                data[3] = System.currentTimeMillis();

                // Only adapt after enough samples
                if (data[2] >= MIN_SAMPLES_FOR_ADAPTATION) {
                    double winRate = data[1] / data[2];
                    double currentValue = data[0];

                    // Bayesian-style update:
                    // If winRate > 0.55 (profitable), INCREASE the score (trust this signal more)
                    // If winRate < 0.45 (unprofitable), DECREASE the score
                    // In between: no change (insufficient evidence)
                    double adjustment = 0;
                    if (winRate > 0.55) {
                        adjustment = LEARNING_RATE * (winRate - 0.50);
                    } else if (winRate < 0.45) {
                        adjustment = LEARNING_RATE * (winRate - 0.50); // negative
                    }

                    data[0] = clamp(currentValue + adjustment, MIN_THRESHOLD, MAX_THRESHOLD);
                }

                // Decay old data (forget samples > 500 trades ago)
                if (data[2] > 500) {
                    data[1] *= 0.95;
                    data[2] *= 0.95;
                }
            }
        }

        /** Update regime multiplier based on current market volatility.
         *  Called by DecisionEngineMerged when building MarketContext. */
        public void updateRegime(double atrMultiplier) {
            // In low-vol: scores should be lower (less confident signals)
            // In high-vol: scores should be higher (clearer signals)
            regimeMultiplier = clamp(1.0 / Math.sqrt(Math.max(0.3, atrMultiplier)), 0.65, 1.45);
        }

        /** Get stats string for logging */
        public String getAdaptationStats() {
            int adapted = 0;
            for (double[] d : thresholds.values()) {
                if (d[2] >= MIN_SAMPLES_FOR_ADAPTATION) adapted++;
            }
            return String.format("Adapted: %d/%d thresholds", adapted, thresholds.size());
        }

        /** Reset all adaptations (e.g., after major code change) */
        public void resetAdaptations() {
            for (double[] d : thresholds.values()) {
                d[1] = 0; d[2] = 0;
            }
        }

        // ── Session/regime-aware fields (used by DecisionEngineMerged) ──
        private volatile double sessionMult = 1.0;
        private volatile double recentAccuracy = 0.55;
        private volatile double volPercentile = 0.5;

        /** Constructor used by DecisionEngineMerged for per-cycle config */
        public AdaptiveConfig(double volPercentile, MarketRegime regime,
                              double recentAccuracy, TradingSession session) {
            initDefaults();
            this.volPercentile = clamp(volPercentile, 0, 1);
            this.recentAccuracy = clamp(recentAccuracy, 0.3, 0.8);
            this.sessionMult = session != null ? session.confidenceMultiplier : 1.0;
            if (regime != null) updateRegimeFromEnum(regime);
        }

        private void updateRegimeFromEnum(MarketRegime regime) {
            double mult = switch (regime) {
                case STRONG_TREND_UP, STRONG_TREND_DOWN -> 0.92;
                case WEAK_TREND_UP, WEAK_TREND_DOWN -> 1.0;
                case RANGE_BOUND -> 1.08;
                case VOLATILE_CHOP -> 1.18;
                default -> 1.0;
            };
            regimeMultiplier = mult;
        }

        public double getSessionMult() { return sessionMult; }
        public double getRecentAccuracy() { return recentAccuracy; }
        public double getVolPercentile() { return volPercentile; }
        public MarketRegime getRegime() { return null; } // not stored, use updateRegime()

        /** Called each cycle to update session and accuracy context on the singleton */
        public void updatePerCycle(double volPct, double accuracy, TradingSession session) {
            this.volPercentile = clamp(volPct, 0, 1);
            this.recentAccuracy = clamp(accuracy, 0.3, 0.8);
            this.sessionMult = session != null ? session.confidenceMultiplier : 1.0;
        }
    }

    // Global singleton config — shared across all components
    public static final AdaptiveConfig CONFIG = new AdaptiveConfig();

    /* ════════════════════════════════════════════════════════════════
       CANDLE — immutable price bar
       ════════════════════════════════════════════════════════════════ */

    public static final class Candle {
        public final long   openTime;
        public final double open, high, low, close, volume, quoteVolume;
        public final long   closeTime;
        public final int    numberOfTrades;
        public final double takerBuyBaseVolume;
        public final double takerBuyQuoteVolume;

        public final double body;
        public final double upperWick;
        public final double lowerWick;
        public final double range;
        public final boolean isBullish;

        public Candle(long openTime, double open, double high, double low,
                      double close, double volume, double quoteVolume, long closeTime) {
            this(openTime, open, high, low, close, volume, quoteVolume, closeTime, 0, 0, 0);
        }

        public Candle(long openTime, double open, double high, double low,
                      double close, double volume, double quoteVolume, long closeTime,
                      int numberOfTrades, double takerBuyBaseVol, double takerBuyQuoteVol) {
            this.openTime             = openTime;
            this.open                 = open;
            this.high                 = high;
            this.low                  = low;
            this.close                = close;
            this.volume               = volume;
            this.quoteVolume          = quoteVolume;
            this.closeTime            = closeTime;
            this.numberOfTrades       = numberOfTrades;
            this.takerBuyBaseVolume   = takerBuyBaseVol;
            this.takerBuyQuoteVolume  = takerBuyQuoteVol;

            this.body       = Math.abs(close - open);
            this.upperWick  = high - Math.max(open, close);
            this.lowerWick  = Math.min(open, close) - low;
            this.range      = high - low;
            this.isBullish  = close >= open;
        }

        public double typicalPrice()     { return (high + low + close) / 3.0; }
        public double hlc3()             { return typicalPrice(); }
        public double ohlc4()            { return (open + high + low + close) / 4.0; }
        public double bodyRatio()        { return range > 0 ? body / range : 0; }
        public double upperWickRatio()   { return range > 0 ? upperWick / range : 0; }
        public double lowerWickRatio()   { return range > 0 ? lowerWick / range : 0; }

        public double takerBuySellRatio() {
            return volume > 0 ? takerBuyBaseVolume / volume : 0.5;
        }
    }

    /* ════════════════════════════════════════════════════════════════
       ENUMS
       ════════════════════════════════════════════════════════════════ */

    public enum Side { LONG, SHORT }

    public enum CoinType {
        TOP(1.0, 0.0008, 20),
        ALT(1.2, 0.0025, 10),
        MEME(1.5, 0.0060, 5);

        public final double riskMultiplier;
        public final double expectedSlippage;
        public final int    maxLeverage;

        CoinType(double rm, double slip, int maxLev) {
            this.riskMultiplier = rm;
            this.expectedSlippage = slip;
            this.maxLeverage = maxLev;
        }
    }

    /* ════════════════════════════════════════════════════════════════
       MATH: RSI — Wilder's Smoothed Moving Average
       ════════════════════════════════════════════════════════════════ */

    public static double rsi(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period + 1) return 50.0;

        int seedStart = 1;
        int seedEnd   = Math.min(seedStart + period, candles.size());

        double avgGain = 0, avgLoss = 0;
        for (int i = seedStart; i < seedEnd; i++) {
            double change = candles.get(i).close - candles.get(i - 1).close;
            if (change > 0) avgGain += change;
            else            avgLoss -= change;
        }
        int seedBars = seedEnd - seedStart;
        avgGain /= seedBars;
        avgLoss /= seedBars;

        for (int i = seedEnd; i < candles.size(); i++) {
            double change = candles.get(i).close - candles.get(i - 1).close;
            avgGain = (avgGain * (period - 1) + (change > 0 ? change : 0)) / period;
            avgLoss = (avgLoss * (period - 1) + (change < 0 ? -change : 0)) / period;
        }

        if (avgLoss < 1e-12) return 100.0;
        double rs = avgGain / avgLoss;
        return 100.0 - (100.0 / (1.0 + rs));
    }

    public static double rsiAt(List<Candle> candles, int period, int barIndex) {
        if (barIndex < period + 1 || barIndex >= candles.size()) return 50.0;
        return rsi(candles.subList(0, barIndex + 1), period);
    }

    public static double[] rsiSeries(List<Candle> candles, int period) {
        double[] result = new double[candles.size()];
        Arrays.fill(result, 50.0);
        if (candles.size() < period + 1) return result;

        double avgGain = 0, avgLoss = 0;
        for (int i = 1; i <= period; i++) {
            double ch = candles.get(i).close - candles.get(i - 1).close;
            if (ch > 0) avgGain += ch; else avgLoss -= ch;
        }
        avgGain /= period;
        avgLoss /= period;
        result[period] = avgLoss < 1e-12 ? 100.0 : 100.0 - 100.0 / (1.0 + avgGain / avgLoss);

        for (int i = period + 1; i < candles.size(); i++) {
            double ch = candles.get(i).close - candles.get(i - 1).close;
            avgGain = (avgGain * (period - 1) + (ch > 0 ? ch : 0)) / period;
            avgLoss = (avgLoss * (period - 1) + (ch < 0 ? -ch : 0)) / period;
            result[i] = avgLoss < 1e-12 ? 100.0 : 100.0 - 100.0 / (1.0 + avgGain / avgLoss);
        }
        return result;
    }

    /* ════════════════════════════════════════════════════════════════
       MATH: ATR — Wilder's Average True Range
       ════════════════════════════════════════════════════════════════ */

    public static double trueRange(Candle current, Candle previous) {
        return Math.max(current.high - current.low,
                Math.max(Math.abs(current.high - previous.close),
                        Math.abs(current.low - previous.close)));
    }

    public static double atr(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period + 1) return 0;

        double atr = 0;
        for (int i = 1; i <= period; i++) {
            atr += trueRange(candles.get(i), candles.get(i - 1));
        }
        atr /= period;

        for (int i = period + 1; i < candles.size(); i++) {
            atr = (atr * (period - 1) + trueRange(candles.get(i), candles.get(i - 1))) / period;
        }
        return atr;
    }

    public static double[] atrSeries(List<Candle> candles, int period) {
        double[] result = new double[candles.size()];
        if (candles.size() < period + 1) return result;

        double atr = 0;
        for (int i = 1; i <= period; i++) atr += trueRange(candles.get(i), candles.get(i - 1));
        atr /= period;
        result[period] = atr;

        for (int i = period + 1; i < candles.size(); i++) {
            atr = (atr * (period - 1) + trueRange(candles.get(i), candles.get(i - 1))) / period;
            result[i] = atr;
        }
        return result;
    }

    /* ════════════════════════════════════════════════════════════════
       MATH: EMA / SMA / VWAP / Keltner
       ════════════════════════════════════════════════════════════════ */

    public static double ema(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period) return 0;
        double k = 2.0 / (period + 1);
        double ema = candles.get(0).close;
        for (int i = 1; i < candles.size(); i++) {
            ema = candles.get(i).close * k + ema * (1 - k);
        }
        return ema;
    }

    public static double[] emaSeries(List<Candle> candles, int period) {
        double[] result = new double[candles.size()];
        if (candles.isEmpty()) return result;
        double k = 2.0 / (period + 1);
        result[0] = candles.get(0).close;
        for (int i = 1; i < candles.size(); i++) {
            result[i] = candles.get(i).close * k + result[i - 1] * (1 - k);
        }
        return result;
    }

    public static double sma(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period) return 0;
        double sum = 0;
        int n = candles.size();
        for (int i = n - period; i < n; i++) sum += candles.get(i).close;
        return sum / period;
    }

    public static double vwap(List<Candle> candles) {
        if (candles == null || candles.isEmpty()) return 0;
        double sumPV = 0, sumV = 0;
        for (Candle c : candles) { sumPV += c.typicalPrice() * c.volume; sumV += c.volume; }
        return sumV > 0 ? sumPV / sumV : 0;
    }

    public static final class KeltnerResult {
        public final double upper, middle, lower;
        public KeltnerResult(double u, double m, double l) { upper = u; middle = m; lower = l; }
    }

    public static KeltnerResult keltner(List<Candle> candles, int emaPeriod, int atrPeriod, double atrMult) {
        double m = ema(candles, emaPeriod);
        double a = atr(candles, atrPeriod);
        return new KeltnerResult(m + atrMult * a, m, m - atrMult * a);
    }

    /* ════════════════════════════════════════════════════════════════
       MATH: MACD
       ════════════════════════════════════════════════════════════════ */

    public static final class MACDResult {
        public final double macd, signal, histogram;
        public MACDResult(double m, double s, double h) { macd = m; signal = s; histogram = h; }
        public boolean bullishCross()   { return histogram > 0 && macd > signal; }
        public boolean bearishCross()   { return histogram < 0 && macd < signal; }
        public boolean histExpanding()  { return Math.abs(histogram) > 0; }
        public boolean isPositive()     { return macd > 0; }
    }

    public static MACDResult macd(List<Candle> candles, int fastPeriod, int slowPeriod, int signalPeriod) {
        if (candles == null || candles.size() < slowPeriod + signalPeriod) return new MACDResult(0, 0, 0);

        double kFast = 2.0 / (fastPeriod + 1);
        double kSlow = 2.0 / (slowPeriod + 1);
        double kSig  = 2.0 / (signalPeriod + 1);

        double emaFast = candles.get(0).close;
        double emaSlow = candles.get(0).close;
        double macdLine = 0, sigLine = 0;
        boolean sigInit = false;
        int sigCount = 0;

        for (int i = 1; i < candles.size(); i++) {
            double c = candles.get(i).close;
            emaFast = c * kFast + emaFast * (1 - kFast);
            emaSlow = c * kSlow + emaSlow * (1 - kSlow);
            macdLine = emaFast - emaSlow;

            if (i >= slowPeriod) {
                if (!sigInit) {
                    sigLine = macdLine;
                    sigInit = true;
                } else {
                    sigLine = macdLine * kSig + sigLine * (1 - kSig);
                }
            }
        }
        return new MACDResult(macdLine, sigLine, macdLine - sigLine);
    }

    public static MACDResult macd(List<Candle> candles) {
        return macd(candles, 12, 26, 9);
    }

    /* ════════════════════════════════════════════════════════════════
       MATH: ADX — Average Directional Index (Wilder)
       ════════════════════════════════════════════════════════════════ */

    public static final class ADXResult {
        public final double adx, plusDI, minusDI;
        public ADXResult(double adx, double pdi, double mdi) { this.adx = adx; plusDI = pdi; minusDI = mdi; }
        public boolean isTrending() { return adx > 25; }
        public boolean isStrongTrend() { return adx > 35; }
        public boolean bullish() { return plusDI > minusDI; }
        public boolean bearish() { return minusDI > plusDI; }
    }

    public static ADXResult adx(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period * 2) return new ADXResult(20, 20, 20);
        int n = candles.size();

        double sPlusDM = 0, sMinusDM = 0, sTR = 0;
        for (int i = 1; i <= period; i++) {
            Candle c = candles.get(i), p = candles.get(i - 1);
            double pDM = c.high - p.high;
            double mDM = p.low - c.low;
            sPlusDM  += pDM > mDM && pDM > 0 ? pDM : 0;
            sMinusDM += mDM > pDM && mDM > 0 ? mDM : 0;
            sTR += trueRange(c, p);
        }

        double sDX = 0;
        for (int i = period + 1; i < n; i++) {
            Candle c = candles.get(i), p = candles.get(i - 1);
            double pDM = c.high - p.high;
            double mDM = p.low - c.low;
            double pDMv = pDM > mDM && pDM > 0 ? pDM : 0;
            double mDMv = mDM > pDM && mDM > 0 ? mDM : 0;
            double tr = trueRange(c, p);
            sPlusDM  = sPlusDM  - sPlusDM  / period + pDMv;
            sMinusDM = sMinusDM - sMinusDM / period + mDMv;
            sTR      = sTR      - sTR      / period + tr;
            if (sTR < 1e-12) continue;
            double pDI = 100.0 * sPlusDM / sTR;
            double mDI = 100.0 * sMinusDM / sTR;
            double dxSum = pDI + mDI;
            if (dxSum > 0) sDX += 100.0 * Math.abs(pDI - mDI) / dxSum;
        }

        int dxCount = n - period - 1;
        double finalPDI = sTR > 0 ? 100.0 * sPlusDM / sTR : 20;
        double finalMDI = sTR > 0 ? 100.0 * sMinusDM / sTR : 20;
        double adxVal = dxCount > 0 ? sDX / dxCount : 20;

        return new ADXResult(clamp(adxVal, 0, 100), finalPDI, finalMDI);
    }

    /* ════════════════════════════════════════════════════════════════
       MATH: STOCHASTIC RSI
       ════════════════════════════════════════════════════════════════ */

    public static final class StochRSIResult {
        public final double k, d;
        public StochRSIResult(double k, double d) { this.k = k; this.d = d; }
        public boolean isOversold()   { return k < 20 && d < 20; }
        public boolean isOverbought() { return k > 80 && d > 80; }
        public boolean bullishCross() { return k > d && k < 30; }
        public boolean bearishCross() { return k < d && k > 70; }
    }

    public static StochRSIResult stochRsi(List<Candle> candles, int rsiPeriod, int stochPeriod, int kSmooth, int dSmooth) {
        double[] rsiArr = rsiSeries(candles, rsiPeriod);
        int n = rsiArr.length;
        if (n < stochPeriod + kSmooth + dSmooth) return new StochRSIResult(50, 50);

        double[] rawK = new double[n];
        for (int i = stochPeriod - 1; i < n; i++) {
            double minRsi = Double.MAX_VALUE, maxRsi = -Double.MAX_VALUE;
            for (int j = i - stochPeriod + 1; j <= i; j++) {
                minRsi = Math.min(minRsi, rsiArr[j]);
                maxRsi = Math.max(maxRsi, rsiArr[j]);
            }
            rawK[i] = (maxRsi - minRsi) > 0 ? (rsiArr[i] - minRsi) / (maxRsi - minRsi) * 100 : 50;
        }

        double kVal = 0;
        for (int i = n - kSmooth; i < n; i++) kVal += rawK[i];
        kVal /= kSmooth;
        return new StochRSIResult(kVal, kVal);
    }

    public static StochRSIResult stochRsi(List<Candle> candles) {
        return stochRsi(candles, 14, 14, 3, 3);
    }

    /* ════════════════════════════════════════════════════════════════
       MATH: BOLLINGER BANDS
       ════════════════════════════════════════════════════════════════ */

    public static final class BollingerResult {
        public final double upper, middle, lower, bandwidth, percentB;
        public BollingerResult(double upper, double middle, double lower, double bw, double pctB) {
            this.upper = upper; this.middle = middle; this.lower = lower;
            this.bandwidth = bw; this.percentB = pctB;
        }
        public boolean isSqueeze(double threshold) { return bandwidth < threshold; }
    }

    public static BollingerResult bollinger(List<Candle> candles, int period, double numStdDev) {
        if (candles == null || candles.size() < period) return new BollingerResult(0, 0, 0, 0, 0.5);
        double mid = sma(candles, period);
        double sumSq = 0;
        int n = candles.size();
        for (int i = n - period; i < n; i++) sumSq += Math.pow(candles.get(i).close - mid, 2);
        double std = Math.sqrt(sumSq / period);
        double upper = mid + numStdDev * std, lower = mid - numStdDev * std;
        double bw = mid > 0 ? (upper - lower) / mid : 0;
        double price = candles.get(n - 1).close;
        double pctB = (upper - lower) > 0 ? (price - lower) / (upper - lower) : 0.5;
        return new BollingerResult(upper, mid, lower, bw, pctB);
    }





    public static double[] obvSeries(List<Candle> candles) {
        double[] obv = new double[candles.size()];
        obv[0] = candles.get(0).volume;
        for (int i = 1; i < candles.size(); i++) {
            if (candles.get(i).close > candles.get(i - 1).close)       obv[i] = obv[i - 1] + candles.get(i).volume;
            else if (candles.get(i).close < candles.get(i - 1).close)  obv[i] = obv[i - 1] - candles.get(i).volume;
            else                                                         obv[i] = obv[i - 1];
        }
        return obv;
    }



    /* ════════════════════════════════════════════════════════════════
       MATH: HURST EXPONENT — Trend persistence
       ════════════════════════════════════════════════════════════════ */

    public static double hurstExponent(List<Candle> candles, int maxLag) {
        if (candles == null || candles.size() < maxLag * 2) return 0.5;
        int n = candles.size();
        double[] logReturns = new double[n - 1];
        for (int i = 0; i < n - 1; i++) {
            logReturns[i] = Math.log(candles.get(i + 1).close / candles.get(i).close);
        }
        List<double[]> points = new ArrayList<>();
        for (int lag = 10; lag <= maxLag; lag += 5) {
            int numBlocks = logReturns.length / lag;
            if (numBlocks < 2) continue;
            double sumRS = 0; int validBlocks = 0;
            for (int b = 0; b < numBlocks; b++) {
                int start = b * lag, end = start + lag;
                double mean = 0;
                for (int i = start; i < end; i++) mean += logReturns[i];
                mean /= lag;
                double[] cumDev = new double[lag];
                cumDev[0] = logReturns[start] - mean;
                for (int i = 1; i < lag; i++) cumDev[i] = cumDev[i - 1] + (logReturns[start + i] - mean);
                double R = Arrays.stream(cumDev).max().orElse(0) - Arrays.stream(cumDev).min().orElse(0);
                double S = 0;
                for (int i = start; i < end; i++) S += Math.pow(logReturns[i] - mean, 2);
                S = Math.sqrt(S / lag);
                if (S > 1e-12) { sumRS += R / S; validBlocks++; }
            }
            if (validBlocks > 0) points.add(new double[]{ Math.log(lag), Math.log(sumRS / validBlocks) });
        }
        if (points.size() < 3) return 0.5;
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        for (double[] p : points) { sumX += p[0]; sumY += p[1]; sumXY += p[0] * p[1]; sumX2 += p[0] * p[0]; }
        int pn = points.size();
        double denom = pn * sumX2 - sumX * sumX;
        if (Math.abs(denom) < 1e-12) return 0.5;
        return clamp((pn * sumXY - sumX * sumY) / denom, 0.0, 1.0);
    }







    /* ════════════════════════════════════════════════════════════════
       [v13.0 NEW] VOLUME PROFILE ENGINE
       Identifies key price levels by where most volume traded.
       VPOC = highest volume node = strongest magnet for price.
       VAH/VAL = value area — price tends to return here.
       ════════════════════════════════════════════════════════════════ */

    public static final class VolumeProfileResult {
        public final double vpoc;    // Volume Point of Control — highest volume price
        public final double vah;     // Value Area High (70% of volume above this)
        public final double val;     // Value Area Low (70% of volume below this)
        public final double vwap;    // Session VWAP
        public final double[] levelsByVolume; // top-5 volume nodes (sorted by price desc)
        public final double   valueAreaWidth; // VAH - VAL as % of price

        public VolumeProfileResult(double vpoc, double vah, double val, double vwap,
                                   double[] levelsByVolume, double valueAreaWidth) {
            this.vpoc = vpoc; this.vah = vah; this.val = val; this.vwap = vwap;
            this.levelsByVolume = levelsByVolume; this.valueAreaWidth = valueAreaWidth;
        }

        /** Price is below VAL — bearish, likely to fall further OR strong buy zone */
        public boolean isPriceAboveVPOC(double price)   { return price > vpoc; }
        public boolean isPriceBelowVPOC(double price)   { return price < vpoc; }
        public boolean isPriceInValueArea(double price) { return price >= val && price <= vah; }
        public boolean isPriceAboveVAH(double price)    { return price > vah; }
        public boolean isPriceBelowVAL(double price)    { return price < val; }

        /** Distance to nearest key level as fraction of VPOC */
        public double distanceToVPOC(double price) {
            return vpoc > 0 ? Math.abs(price - vpoc) / vpoc : 0;
        }

        public double distanceToVAH(double price) {
            return vah > 0 ? (price - vah) / vah : 0; // positive = above VAH
        }

        public double distanceToVAL(double price) {
            return val > 0 ? (price - val) / val : 0; // negative = below VAL
        }
    }

    public static VolumeProfileResult volumeProfile(List<Candle> candles, int numBins) {
        if (candles == null || candles.size() < 20 || numBins < 10)
            return new VolumeProfileResult(0, 0, 0, 0, new double[0], 0);

        // Find price range
        double highP = -Double.MAX_VALUE, lowP = Double.MAX_VALUE;
        for (Candle c : candles) { highP = Math.max(highP, c.high); lowP = Math.min(lowP, c.low); }
        if (highP <= lowP || highP <= 0) return new VolumeProfileResult(0, 0, 0, 0, new double[0], 0);

        double binSize = (highP - lowP) / numBins;
        double[] volProfile = new double[numBins];
        double[] binPrices  = new double[numBins];

        for (int i = 0; i < numBins; i++) binPrices[i] = lowP + (i + 0.5) * binSize;

        for (Candle c : candles) {
            // Distribute volume across candle range proportionally
            int binLow  = (int) Math.max(0, Math.min(numBins - 1, (c.low  - lowP) / binSize));
            int binHigh = (int) Math.max(0, Math.min(numBins - 1, (c.high - lowP) / binSize));
            int binsSpanned = Math.max(1, binHigh - binLow + 1);
            double volPerBin = c.volume / binsSpanned;
            for (int b = binLow; b <= binHigh; b++) volProfile[b] += volPerBin;
        }

        // VPOC
        int vpocBin = 0;
        double maxVol = 0;
        for (int i = 0; i < numBins; i++) {
            if (volProfile[i] > maxVol) { maxVol = volProfile[i]; vpocBin = i; }
        }
        double vpoc = binPrices[vpocBin];

        // Value Area (70% of total volume around VPOC)
        double totalVol = Arrays.stream(volProfile).sum();
        double targetVol = totalVol * 0.70;
        double accVol = volProfile[vpocBin];
        int vahBin = vpocBin, valBin = vpocBin;

        while (accVol < targetVol) {
            double addUp   = vahBin < numBins - 1 ? volProfile[vahBin + 1] : 0;
            double addDown = valBin > 0           ? volProfile[valBin - 1] : 0;
            if (addUp >= addDown && vahBin < numBins - 1) { vahBin++; accVol += volProfile[vahBin]; }
            else if (addDown > 0 && valBin > 0) { valBin--; accVol += volProfile[valBin]; }
            else break;
        }

        double vah = binPrices[vahBin];
        double val = binPrices[valBin];

        // VWAP
        double sumPV = 0, sumV = 0;
        for (Candle c : candles) { sumPV += c.typicalPrice() * c.volume; sumV += c.volume; }
        double vwapVal = sumV > 0 ? sumPV / sumV : vpoc;

        // Top-5 volume nodes
        double[] topLevels = Arrays.copyOf(binPrices, numBins);
        // Sort by volume descending
        Integer[] indices = new Integer[numBins];
        for (int i = 0; i < numBins; i++) indices[i] = i;
        Arrays.sort(indices, (a, b) -> Double.compare(volProfile[b], volProfile[a]));
        double[] top5 = new double[Math.min(5, numBins)];
        for (int i = 0; i < top5.length; i++) top5[i] = binPrices[indices[i]];
        Arrays.sort(top5);
        // top5 already sorted ascending — reverse for descending (highest volume first)
        for (int i = 0, j = top5.length - 1; i < j; i++, j--) {
            double tmp = top5[i]; top5[i] = top5[j]; top5[j] = tmp;
        }

        double valueAreaWidth = vpoc > 0 ? (vah - val) / vpoc : 0;

        return new VolumeProfileResult(vpoc, vah, val, vwapVal, top5, valueAreaWidth);
    }

    /* ════════════════════════════════════════════════════════════════
       [v13.0 NEW] TREND PHASE ANALYZER
       The most important thing for 8-candle prediction:
       Are we at the BEGINNING of a move or the END?
       EARLY   = best time to enter, high upside
       MID     = good entry, trend confirmed
       LATE    = reduced probability, approaching target
       EXHAUSTION = DON'T enter, move is ending
       ════════════════════════════════════════════════════════════════ */

    public enum TrendPhase {
        EARLY_BULL,    // New uptrend beginning — highest probability long
        EARLY_BEAR,    // New downtrend beginning — highest probability short
        MID_BULL,      // Uptrend continuation, still room to run
        MID_BEAR,      // Downtrend continuation
        LATE_BULL,     // Uptrend aging, weakening momentum
        LATE_BEAR,     // Downtrend aging
        EXHAUSTION_UP,   // Overbought, imminent reversal risk
        EXHAUSTION_DOWN, // Oversold, imminent reversal risk
        ACCUMULATION,    // Sideways before next move (Wyckoff)
        DISTRIBUTION,    // Sideways before breakdown
        COMPRESSION,     // Squeeze, directional break imminent
        CHOP             // No edge, avoid
    }

    public static final class TrendPhaseResult {
        public final TrendPhase phase;
        public final double confidence;  // 0..1 how sure we are of the phase
        public final int barsInPhase;    // how many bars we've been in this phase
        public final double exhaustionScore;  // 0..1, how exhausted is this move
        public final double continuationScore; // 0..1, probability of continuation
        public final List<String> evidence;   // why we think this

        public TrendPhaseResult(TrendPhase phase, double confidence, int barsInPhase,
                                double exhaustionScore, double continuationScore, List<String> evidence) {
            this.phase = phase; this.confidence = confidence; this.barsInPhase = barsInPhase;
            this.exhaustionScore = exhaustionScore; this.continuationScore = continuationScore;
            this.evidence = Collections.unmodifiableList(new ArrayList<>(evidence));
        }

        public boolean isEarlyPhase() {
            return phase == TrendPhase.EARLY_BULL || phase == TrendPhase.EARLY_BEAR;
        }
        public boolean isExhausted() {
            return phase == TrendPhase.EXHAUSTION_UP || phase == TrendPhase.EXHAUSTION_DOWN || exhaustionScore > 0.70;
        }
        public boolean isBullish() {
            return phase == TrendPhase.EARLY_BULL || phase == TrendPhase.MID_BULL || phase == TrendPhase.LATE_BULL;
        }
        public boolean isBearish() {
            return phase == TrendPhase.EARLY_BEAR || phase == TrendPhase.MID_BEAR || phase == TrendPhase.LATE_BEAR;
        }
        public boolean isCompression() {
            return phase == TrendPhase.COMPRESSION || phase == TrendPhase.ACCUMULATION;
        }
    }

    public static TrendPhaseResult analyzeTrendPhase(List<Candle> candles, List<Candle> candles1h) {
        if (candles == null || candles.size() < 50) {
            return new TrendPhaseResult(TrendPhase.CHOP, 0.3, 0, 0.5, 0.3, List.of("insufficient_data"));
        }

        List<String> evidence = new ArrayList<>();
        int n = candles.size();
        double price = candles.get(n - 1).close;

        // === Core indicators ===
        double rsi14   = rsi(candles, 14);
        double rsi_3   = rsi(candles, 3);    // ultra-fast for exhaustion
        double ema9    = ema(candles, 9);
        double ema21   = ema(candles, 21);
        double ema50   = ema(candles, 50);
        double atr14   = atr(candles, 14);
        double[] rsiArr = rsiSeries(candles, 14);
        MACDResult macdR = macd(candles, 12, 26, 9);
        ADXResult adxR   = adx(candles, 14);
        BollingerResult bbR = bollinger(candles, 20, 2.0);

        double hurst = hurstExponent(candles, Math.min(60, n / 3));
        double[] atrArr = atrSeries(candles, 14);

        // === ATR expansion analysis ===
        // Is ATR currently expanding (early trend) or contracting (late trend)?
        double atrCurrent = atr14;
        double atrBefore = 0;
        int atrLookback = 10;
        if (n > 50) {
            atrBefore = 0;
            for (int i = n - 1 - atrLookback; i < n - 1; i++) if (atrArr[i] > 0) atrBefore = atrArr[i];
        }
        boolean atrExpanding = atrCurrent > atrBefore * 1.1 && atrBefore > 0;
        boolean atrContracting = atrCurrent < atrBefore * 0.85 && atrBefore > 0;

        // === EMA structure ===
        boolean emaBullStack = ema9 > ema21 && ema21 > ema50; // classic bull
        boolean emaBearStack = ema9 < ema21 && ema21 < ema50; // classic bear
        boolean emaJustCrossed = Math.abs(ema9 - ema21) < atr14 * 0.3; // fresh cross

        // === RSI trajectory ===
        // Compare RSI trend over last 5 bars
        double rsiNow = rsiArr[n - 1];
        double rsi5ago = n > 5 ? rsiArr[n - 6] : 50;
        double rsiMomentum = rsiNow - rsi5ago; // positive = rising
        boolean rsiExtremeBull = rsiNow > 75;
        boolean rsiExtremeBear = rsiNow < 25;
        boolean rsiFreshBull = rsiNow > 55 && rsi5ago < 50;
        boolean rsiFreshBear = rsiNow < 45 && rsi5ago > 50;

        // === Count bars since last EMA cross ===
        int barsSinceEma9Cross = 0;
        for (int i = n - 2; i >= Math.max(0, n - 60); i--) {
            // We need EMA series for this — approximate using price vs ema
            barsSinceEma9Cross++;
            double prevClose = candles.get(i).close;
            double prevEma21 = ema(candles.subList(0, i + 1), 21);
            // Rough check: did ema9/ema21 cross?
            if ((price > ema21) != (prevClose > prevEma21)) break;
        }

        // === Volume analysis ===
        double avgVol20 = 0;
        for (int i = n - 20; i < n; i++) avgVol20 += candles.get(i).volume;
        avgVol20 /= 20;
        double recentVol5 = 0;
        for (int i = n - 5; i < n; i++) recentVol5 += candles.get(i).volume;
        recentVol5 /= 5;
        double volumeRatio = avgVol20 > 0 ? recentVol5 / avgVol20 : 1.0;
        boolean volumeDecreasing = volumeRatio < 0.75;
        boolean volumeIncreasing = volumeRatio > 1.35;

        // === OBV slope ===
        double[] obvArr = obvSeries(candles);
        double obvNow = obvArr[n - 1];
        double obv10ago = n > 10 ? obvArr[n - 11] : obvNow;
        double obvMomentum = obvNow - obv10ago; // positive = buying pressure

        // === Bollinger bandwidth trend ===
        BollingerResult bb5ago = n > 5 ? bollinger(candles.subList(0, n - 5), 20, 2.0) : bbR;
        boolean bbSqueeze = bbR.bandwidth < 0.03;
        boolean bbExpanding = bbR.bandwidth > bb5ago.bandwidth * 1.2;
        boolean bbExtremeUpper = bbR.percentB > 0.92;
        boolean bbExtremeLower = bbR.percentB < 0.08;

        // === Determine phase ===
        double exhaustionScore = 0;
        double continuationScore = 0;
        TrendPhase phase;
        double phaseConfidence;
        int barsInPhase = barsSinceEma9Cross;

        // --- COMPRESSION / ACCUMULATION ---
        if (bbSqueeze && adxR.adx < 18 && hurst < 0.48) {
            phase = volumeIncreasing ? TrendPhase.ACCUMULATION : TrendPhase.COMPRESSION;
            phaseConfidence = 0.65;
            continuationScore = 0.55;
            exhaustionScore = 0.2;
            evidence.add("BB_SQUEEZE"); evidence.add("ADX_LOW=" + String.format("%.0f", adxR.adx));
            if (hurst < 0.45) evidence.add("MEAN_REVERTING_HURST");
        }
        // --- EXHAUSTION UP ---
        else if (emaBullStack && rsiExtremeBull && bbExtremeUpper
                && (volumeDecreasing || atrContracting)) {
            phase = TrendPhase.EXHAUSTION_UP;
            exhaustionScore = 0.75 + (rsiNow - 75) / 100 * 0.5;
            continuationScore = 0.25;
            phaseConfidence = 0.70;
            evidence.add("RSI_EXTREME=" + String.format("%.0f", rsiNow));
            if (volumeDecreasing) evidence.add("VOL_DECLINING");

        }
        // --- EXHAUSTION DOWN ---
        else if (emaBearStack && rsiExtremeBear && bbExtremeLower
                && (volumeDecreasing || atrContracting)) {
            phase = TrendPhase.EXHAUSTION_DOWN;
            exhaustionScore = 0.75 + (25 - rsiNow) / 100 * 0.5;
            continuationScore = 0.25;
            phaseConfidence = 0.70;
            evidence.add("RSI_EXTREME=" + String.format("%.0f", rsiNow));
            if (volumeDecreasing) evidence.add("VOL_DECLINING");

        }
        // --- EARLY BULL ---
        else if ((rsiFreshBull || emaJustCrossed) && emaBullStack && atrExpanding
                && macdR.histogram > 0 && barsSinceEma9Cross < 8) {
            phase = TrendPhase.EARLY_BULL;
            exhaustionScore = 0.10;
            continuationScore = 0.72 + hurst * 0.15;
            phaseConfidence = 0.65 + (adxR.adx > 25 ? 0.10 : 0);
            evidence.add("EMA_FRESH_BULL_CROSS"); evidence.add("ATR_EXPANDING");
            if (volumeIncreasing) evidence.add("VOL_SURGE");
            if (adxR.adx > 25) evidence.add("ADX_CONFIRM=" + String.format("%.0f", adxR.adx));
        }
        // --- EARLY BEAR ---
        else if ((rsiFreshBear || emaJustCrossed) && emaBearStack && atrExpanding
                && macdR.histogram < 0 && barsSinceEma9Cross < 8) {
            phase = TrendPhase.EARLY_BEAR;
            exhaustionScore = 0.10;
            continuationScore = 0.72 + hurst * 0.15;
            phaseConfidence = 0.65 + (adxR.adx > 25 ? 0.10 : 0);
            evidence.add("EMA_FRESH_BEAR_CROSS"); evidence.add("ATR_EXPANDING");
            if (volumeIncreasing) evidence.add("VOL_SURGE");
        }
        // --- MID BULL ---
        else if (emaBullStack && macdR.isPositive() && rsiNow > 50 && rsiNow < 75
                && hurst > 0.52 && adxR.adx > 22) {
            phase = TrendPhase.MID_BULL;
            exhaustionScore = 0.25;
            continuationScore = 0.60 + (hurst - 0.5) * 0.5;
            phaseConfidence = 0.60;
            evidence.add("EMA_BULL_STACK"); evidence.add("HURST=" + String.format("%.2f", hurst));
        }
        // --- MID BEAR ---
        else if (emaBearStack && !macdR.isPositive() && rsiNow < 50 && rsiNow > 25
                && hurst > 0.52 && adxR.adx > 22) {
            phase = TrendPhase.MID_BEAR;
            exhaustionScore = 0.25;
            continuationScore = 0.60 + (hurst - 0.5) * 0.5;
            phaseConfidence = 0.60;
            evidence.add("EMA_BEAR_STACK"); evidence.add("HURST=" + String.format("%.2f", hurst));
        }
        // --- LATE BULL ---
        else if (emaBullStack && rsiNow > 60 && (volumeDecreasing || macdR.histogram < 0)) {
            phase = TrendPhase.LATE_BULL;
            exhaustionScore = 0.55;
            continuationScore = 0.40;
            phaseConfidence = 0.55;
            evidence.add("MACD_WEAKENING"); if (volumeDecreasing) evidence.add("VOL_DECLINING");
        }
        // --- LATE BEAR ---
        else if (emaBearStack && rsiNow < 40 && (volumeDecreasing || macdR.histogram > 0)) {
            phase = TrendPhase.LATE_BEAR;
            exhaustionScore = 0.55;
            continuationScore = 0.40;
            phaseConfidence = 0.55;
            evidence.add("MACD_WEAKENING"); if (volumeDecreasing) evidence.add("VOL_DECLINING");
        }
        // --- DISTRIBUTION ---
        else if (!emaBullStack && !emaBearStack && rsiNow > 55 && volumeDecreasing) {
            phase = TrendPhase.DISTRIBUTION;
            exhaustionScore = 0.60;
            continuationScore = 0.30;
            phaseConfidence = 0.50;
            evidence.add("DISTRIBUTION_PATTERN");
        }
        // --- DEFAULT CHOP ---
        else {
            phase = TrendPhase.CHOP;
            exhaustionScore = 0.40;
            continuationScore = 0.40;
            phaseConfidence = 0.35;
            evidence.add("NO_CLEAR_PHASE");
        }

        // 1H HTF confirmation
        if (candles1h != null && candles1h.size() > 30) {
            double rsi1h = rsi(candles1h, 14);
            double ema21_1h = ema(candles1h, 21);
            double ema50_1h = ema(candles1h, 50);
            boolean htfBull = candles1h.get(candles1h.size() - 1).close > ema21_1h && ema21_1h > ema50_1h;
            boolean htfBear = candles1h.get(candles1h.size() - 1).close < ema21_1h && ema21_1h < ema50_1h;

            if ((phase == TrendPhase.EARLY_BULL || phase == TrendPhase.MID_BULL) && htfBull) {
                phaseConfidence += 0.10; continuationScore += 0.08; evidence.add("HTF_CONFIRMS_BULL");
            } else if ((phase == TrendPhase.EARLY_BEAR || phase == TrendPhase.MID_BEAR) && htfBear) {
                phaseConfidence += 0.10; continuationScore += 0.08; evidence.add("HTF_CONFIRMS_BEAR");
            } else if ((phase == TrendPhase.EARLY_BULL || phase == TrendPhase.MID_BULL) && htfBear) {
                phaseConfidence -= 0.12; continuationScore -= 0.10; evidence.add("HTF_COUNTER_BULL");
            } else if ((phase == TrendPhase.EARLY_BEAR || phase == TrendPhase.MID_BEAR) && htfBull) {
                phaseConfidence -= 0.12; continuationScore -= 0.10; evidence.add("HTF_COUNTER_BEAR");
            }
        }

        return new TrendPhaseResult(
                phase,
                clamp(phaseConfidence, 0.1, 0.92),
                barsInPhase,
                clamp(exhaustionScore, 0, 1),
                clamp(continuationScore, 0, 1),
                evidence
        );
    }

    /* ════════════════════════════════════════════════════════════════
       [v13.0 NEW] SWING STRUCTURE ANALYZER
       Market structure: HH/HL = bullish, LH/LL = bearish.
       Break of structure (BoS) = strongest signal.
       Change of Character (ChoCh) = potential reversal.
       ════════════════════════════════════════════════════════════════ */

    public enum StructureBias { BULLISH, BEARISH, NEUTRAL }

    public static final class SwingStructureResult {
        public final StructureBias bias;
        public final boolean breakOfStructure;  // price broke last major swing high/low
        public final boolean changeOfChar;       // ChoCh — potential reversal
        public final double lastSwingHigh;
        public final double lastSwingLow;
        public final int swingHighBar;
        public final int swingLowBar;
        public final double structureStrength; // 0..1

        public SwingStructureResult(StructureBias bias, boolean bos, boolean choch,
                                    double swHigh, double swLow, int shBar, int slBar, double strength) {
            this.bias = bias; this.breakOfStructure = bos; this.changeOfChar = choch;
            this.lastSwingHigh = swHigh; this.lastSwingLow = swLow;
            this.swingHighBar = shBar; this.swingLowBar = slBar; this.structureStrength = strength;
        }
    }

    public static SwingStructureResult analyzeSwingStructure(List<Candle> candles, int pivotStrength) {
        if (candles == null || candles.size() < pivotStrength * 3 + 1)
            return new SwingStructureResult(StructureBias.NEUTRAL, false, false, 0, 0, -1, -1, 0.3);

        int n = candles.size();
        // Find last 2 swing highs and lows
        List<double[]> highs = new ArrayList<>(); // [price, barIndex]
        List<double[]> lows  = new ArrayList<>();

        for (int i = pivotStrength; i < n - pivotStrength; i++) {
            boolean isHigh = true, isLow = true;
            for (int j = 1; j <= pivotStrength; j++) {
                if (candles.get(i).high <= candles.get(i - j).high || candles.get(i).high <= candles.get(i + j).high) isHigh = false;
                if (candles.get(i).low  >= candles.get(i - j).low  || candles.get(i).low  >= candles.get(i + j).low)  isLow  = false;
            }
            if (isHigh) highs.add(new double[]{ candles.get(i).high, i });
            if (isLow)  lows.add(new double[]{ candles.get(i).low, i });
        }

        if (highs.size() < 2 || lows.size() < 2)
            return new SwingStructureResult(StructureBias.NEUTRAL, false, false, 0, 0, -1, -1, 0.3);

        double[] lastHigh = highs.get(highs.size() - 1);
        double[] prevHigh = highs.get(highs.size() - 2);
        double[] lastLow  = lows.get(lows.size() - 1);
        double[] prevLow  = lows.get(lows.size() - 2);

        double currentPrice = candles.get(n - 1).close;
        boolean hhhl = lastHigh[0] > prevHigh[0] && lastLow[0] > prevLow[0]; // Higher High, Higher Low
        boolean lhll = lastHigh[0] < prevHigh[0] && lastLow[0] < prevLow[0]; // Lower High, Lower Low
        boolean bos = currentPrice > lastHigh[0] || currentPrice < lastLow[0];
        boolean choch = !hhhl && !lhll; // mixed = potential reversal

        StructureBias bias;
        double strength;

        if (hhhl) {
            bias = StructureBias.BULLISH;
            strength = Math.min(1.0, (lastHigh[0] - prevHigh[0]) / Math.max(1e-10, atr(candles, 14) * 3));
        } else if (lhll) {
            bias = StructureBias.BEARISH;
            strength = Math.min(1.0, (prevLow[0] - lastLow[0]) / Math.max(1e-10, atr(candles, 14) * 3));
        } else {
            bias = StructureBias.NEUTRAL;
            strength = 0.3;
        }

        return new SwingStructureResult(bias, bos, choch,
                lastHigh[0], lastLow[0], (int) lastHigh[1], (int) lastLow[1], strength);
    }

    /* ════════════════════════════════════════════════════════════════
       [v13.0 NEW] ORDER BLOCK DETECTOR
       Order blocks = zones where institutional orders rested.
       Price tends to react strongly when returning to these zones.
       ════════════════════════════════════════════════════════════════ */

    public static final class OrderBlock {
        public final double top;
        public final double bottom;
        public final boolean isBullish; // true = demand zone, false = supply zone
        public final double strength;   // 0..1
        public final int formationBar;

        public OrderBlock(double top, double bottom, boolean bullish, double strength, int bar) {
            this.top = top; this.bottom = bottom; this.isBullish = bullish;
            this.strength = strength; this.formationBar = bar;
        }

        public boolean contains(double price) { return price >= bottom && price <= top; }
        public double midpoint() { return (top + bottom) / 2; }
        public boolean isNearby(double price, double atr) { return Math.abs(price - midpoint()) < atr * 1.5; }
    }

    public static List<OrderBlock> detectOrderBlocks(List<Candle> candles, int lookback) {
        List<OrderBlock> blocks = new ArrayList<>();
        if (candles == null || candles.size() < lookback + 3) return blocks;

        int n = candles.size();
        int start = Math.max(1, n - lookback);
        double atr14 = atr(candles, 14);

        for (int i = start; i < n - 2; i++) {
            Candle c0 = candles.get(i);
            Candle c1 = candles.get(i + 1);
            Candle c2 = candles.get(i + 2);

            // Bullish Order Block: bearish candle immediately followed by a strong bullish impulse
            boolean strongBullishMove = c1.isBullish && c1.body > atr14 * 0.8;
            if (!c0.isBullish && strongBullishMove) {
                // The OB is the last bearish candle before the impulse
                double avgVolLookback = 0;
                int volCount = 0;
                for (int j = Math.max(0, i - 20); j < i; j++) {
                    avgVolLookback += candles.get(j).volume; volCount++;
                }
                avgVolLookback = volCount > 0 ? avgVolLookback / volCount : 1;
                double volRatio = avgVolLookback > 0 ? c1.volume / avgVolLookback : 1;
                double strength = clamp(c1.body / (atr14 + 1e-10) * 0.4 + volRatio * 0.2, 0.1, 0.95);
                blocks.add(new OrderBlock(c0.high, c0.low, true, strength, i));
            }

            // Bearish Order Block: bullish candle immediately followed by a strong bearish impulse
            boolean strongBearishMove = !c1.isBullish && c1.body > atr14 * 0.8;
            if (c0.isBullish && strongBearishMove) {
                double avgVolLookback = 0;
                int volCount = 0;
                for (int j = Math.max(0, i - 20); j < i; j++) {
                    avgVolLookback += candles.get(j).volume; volCount++;
                }
                avgVolLookback = volCount > 0 ? avgVolLookback / volCount : 1;
                double volRatio = avgVolLookback > 0 ? c1.volume / avgVolLookback : 1;
                double strength = clamp(c1.body / (atr14 + 1e-10) * 0.4 + volRatio * 0.2, 0.1, 0.95);
                blocks.add(new OrderBlock(c0.high, c0.low, false, strength, i));
            }
        }

        // Keep only fresh, unmitigated blocks (price hasn't returned to them)
        double currentPrice = candles.get(n - 1).close;
        blocks.removeIf(ob -> {
            // Mitigated = price has touched the zone again after formation
            for (int i = ob.formationBar + 3; i < n; i++) {
                if (candles.get(i).low <= ob.top && candles.get(i).high >= ob.bottom) return true;
            }
            return false;
        });

        // Sort by strength descending
        blocks.sort(Comparator.comparingDouble((OrderBlock ob) -> ob.strength).reversed());
        return blocks.subList(0, Math.min(blocks.size(), 5)); // Top 5 only
    }

    /* ════════════════════════════════════════════════════════════════
       [v13.0 NEW] ATR PERCENTILE — Volatility regime detection
       Critical for knowing if ATR is "normal" or "extreme"
       ════════════════════════════════════════════════════════════════ */

    /**
     * Returns current ATR as a percentile of the last 'lookback' ATR values.
     * 0.0 = lowest volatility, 1.0 = highest volatility in history.
     * Used to calibrate position sizing and signal confidence.
     */
    public static double atrPercentile(List<Candle> candles, int atrPeriod, int lookback) {
        if (candles == null || candles.size() < atrPeriod + lookback) return 0.5;
        double[] atrArr = atrSeries(candles, atrPeriod);
        double current = atrArr[atrArr.length - 1];
        if (current <= 0) return 0.5;

        int start = Math.max(atrPeriod, atrArr.length - lookback);
        int count = 0, rank = 0;
        for (int i = start; i < atrArr.length - 1; i++) {
            if (atrArr[i] > 0) { count++; if (current > atrArr[i]) rank++; }
        }
        return count > 0 ? (double) rank / count : 0.5;
    }

    /* ════════════════════════════════════════════════════════════════
       DIVERGENCE DETECTION
       ════════════════════════════════════════════════════════════════ */

    public static final class Divergence {
        public enum Type { REGULAR_BULLISH, REGULAR_BEARISH, HIDDEN_BULLISH, HIDDEN_BEARISH }

        public final Type type;
        public final int  pivotBar1, pivotBar2;
        public final double priceAtPivot1, priceAtPivot2;
        public final double rsiAtPivot1, rsiAtPivot2;
        public final double strength;

        public Divergence(Type type, int p1, int p2, double price1, double price2,
                          double rsi1, double rsi2, double strength) {
            this.type = type; this.pivotBar1 = p1; this.pivotBar2 = p2;
            this.priceAtPivot1 = price1; this.priceAtPivot2 = price2;
            this.rsiAtPivot1 = rsi1; this.rsiAtPivot2 = rsi2; this.strength = strength;
        }

        public boolean isBullish() { return type == Type.REGULAR_BULLISH || type == Type.HIDDEN_BULLISH; }
        public boolean isBearish() { return type == Type.REGULAR_BEARISH || type == Type.HIDDEN_BEARISH; }
    }

    public static List<Divergence> detectDivergences(List<Candle> candles, int rsiPeriod, int lookback, int pivotStrength) {
        List<Divergence> result = new ArrayList<>();
        if (candles == null || candles.size() < lookback + rsiPeriod) return result;
        double[] rsiArr = rsiSeries(candles, rsiPeriod);
        int n = candles.size();
        int scanStart = Math.max(pivotStrength, n - lookback);
        int scanEnd   = n - pivotStrength;

        List<int[]> swingHighs = new ArrayList<>(), swingLows = new ArrayList<>();
        for (int i = scanStart; i < scanEnd; i++) {
            boolean isHigh = true, isLow = true;
            for (int j = 1; j <= pivotStrength; j++) {
                if (candles.get(i).high <= candles.get(i - j).high || candles.get(i).high <= candles.get(i + j).high) isHigh = false;
                if (candles.get(i).low  >= candles.get(i - j).low  || candles.get(i).low  >= candles.get(i + j).low)  isLow  = false;
            }
            if (isHigh) swingHighs.add(new int[]{i});
            if (isLow)  swingLows.add(new int[]{i});
        }

        for (int i = 0; i < swingLows.size() - 1; i++) {
            for (int j = i + 1; j < swingLows.size(); j++) {
                int bar1 = swingLows.get(i)[0], bar2 = swingLows.get(j)[0];
                if (bar2 - bar1 < 5 || bar2 - bar1 > lookback) continue;
                double p1 = candles.get(bar1).low, p2 = candles.get(bar2).low;
                double r1 = rsiArr[bar1],          r2 = rsiArr[bar2];
                if (p2 < p1 && r2 > r1 + 2)
                    result.add(new Divergence(Divergence.Type.REGULAR_BULLISH, bar1, bar2, p1, p2, r1, r2,
                            clamp((p1-p2)/p1*20 + (r2-r1)/30, 0.1, 1.0)));
                if (p2 > p1 && r2 < r1 - 2)
                    result.add(new Divergence(Divergence.Type.HIDDEN_BULLISH, bar1, bar2, p1, p2, r1, r2,
                            clamp(Math.abs(r1-r2)/20, 0.1, 0.8)));
            }
        }
        for (int i = 0; i < swingHighs.size() - 1; i++) {
            for (int j = i + 1; j < swingHighs.size(); j++) {
                int bar1 = swingHighs.get(i)[0], bar2 = swingHighs.get(j)[0];
                if (bar2 - bar1 < 5 || bar2 - bar1 > lookback) continue;
                double p1 = candles.get(bar1).high, p2 = candles.get(bar2).high;
                double r1 = rsiArr[bar1],            r2 = rsiArr[bar2];
                if (p2 > p1 && r2 < r1 - 2)
                    result.add(new Divergence(Divergence.Type.REGULAR_BEARISH, bar1, bar2, p1, p2, r1, r2,
                            clamp((p2-p1)/p1*20 + (r1-r2)/30, 0.1, 1.0)));
                if (p2 < p1 && r2 > r1 + 2)
                    result.add(new Divergence(Divergence.Type.HIDDEN_BEARISH, bar1, bar2, p1, p2, r1, r2,
                            clamp(Math.abs(r2-r1)/20, 0.1, 0.8)));
            }
        }
        result.sort(Comparator.comparingDouble((Divergence d) -> d.strength).reversed());
        return result;
    }

    /* ════════════════════════════════════════════════════════════════
       MARKET REGIME DETECTION
       ════════════════════════════════════════════════════════════════ */

    public enum MarketRegime {
        STRONG_TREND_UP, STRONG_TREND_DOWN, WEAK_TREND_UP, WEAK_TREND_DOWN,
        RANGE_BOUND, VOLATILE_CHOP, BREAKOUT, COMPRESSION
    }

    public static final class RegimeResult {
        public final MarketRegime regime;
        public final double confidence;
        public final double trendStrength;
        public final double volatilityRank;
        public final double hurst;

        public RegimeResult(MarketRegime regime, double confidence, double trendStr, double volRank, double hurst) {
            this.regime = regime; this.confidence = confidence;
            this.trendStrength = trendStr; this.volatilityRank = volRank; this.hurst = hurst;
        }
    }

    public static RegimeResult detectRegime(List<Candle> candles, int adxPeriod) {
        if (candles == null || candles.size() < 100)
            return new RegimeResult(MarketRegime.RANGE_BOUND, 0.3, 0, 0.5, 0.5);

        ADXResult adxR = adx(candles, adxPeriod);
        double currentATR = atr(candles, 14);
        double hurstVal = hurstExponent(candles, Math.min(80, candles.size() / 3));
        double volPercentile = atrPercentile(candles, 14, 100);
        BollingerResult bb = bollinger(candles, 20, 2);
        double ema20 = ema(candles, 20), ema50 = ema(candles, 50);
        double trendDir = clamp((ema20 - ema50) / (currentATR * 3 + 1e-12), -1, 1);

        MarketRegime regime; double confidence;
        if (bb.bandwidth < 0.03 && adxR.adx < 20) {
            regime = MarketRegime.COMPRESSION; confidence = 1.0 - bb.bandwidth / 0.03;
        } else if (adxR.adx > 30 && hurstVal > 0.55) {
            if (trendDir > 0.3)       { regime = MarketRegime.STRONG_TREND_UP;   confidence = adxR.adx / 50; }
            else if (trendDir < -0.3) { regime = MarketRegime.STRONG_TREND_DOWN; confidence = adxR.adx / 50; }
            else                      { regime = MarketRegime.WEAK_TREND_UP;     confidence = 0.4; }
        } else if (adxR.adx > 20) {
            if (trendDir > 0.15)      { regime = MarketRegime.WEAK_TREND_UP;   confidence = 0.5; }
            else if (trendDir < -0.15){ regime = MarketRegime.WEAK_TREND_DOWN; confidence = 0.5; }
            else                      { regime = MarketRegime.RANGE_BOUND;     confidence = 0.4; }
        } else if (volPercentile > 0.8 && hurstVal < 0.45) {
            regime = MarketRegime.VOLATILE_CHOP; confidence = volPercentile;
        } else {
            regime = MarketRegime.RANGE_BOUND; confidence = 1.0 - adxR.adx / 25;
        }

        return new RegimeResult(regime, clamp(confidence, 0.1, 0.95), trendDir, volPercentile, hurstVal);
    }


    /* ════════════════════════════════════════════════════════════════
       [v22.0] BAYESIAN EVIDENCE SCORER

       PROBLEM (Gemini critique): Linear score addition (0.50 + 0.18 + 0.22)
       doesn't model non-linear markets. 3 weak signals ≠ 1 strong signal.

       SOLUTION: Each cluster provides a LIKELIHOOD RATIO, not a score.
       LR > 1 = evidence FOR direction. LR < 1 = evidence AGAINST.

       P(signal correct | evidence) = P(prior) * LR1 * LR2 * ... / Z

       This gives a PROPER posterior probability, not an arbitrary sum.
       A single strong confirmation (LR=3.0) is worth more than
       five weak ones (LR=1.1^5 = 1.6).
       ════════════════════════════════════════════════════════════════ */

    public static final class BayesianScorer {
        private final double priorWinRate;

        public BayesianScorer(double priorWinRate) {
            this.priorWinRate = clamp(priorWinRate, 0.35, 0.70);
        }

        /** Default: assume 50% base rate (no edge) */
        public BayesianScorer() { this(0.50); }

        /**
         * Convert a cluster score [0..1] into a likelihood ratio.
         * score=0 → LR=0.5 (evidence against), score=0.5 → LR=1.0 (neutral),
         * score=0.75 → LR=2.0 (moderate evidence for), score=1.0 → LR=4.0 (strong)
         */
        public double scoreToLR(double score) {
            if (score <= 0) return 0.5;
            // Exponential mapping: avoids the linear trap
            // score 0.3 → LR 0.8, score 0.5 → LR 1.3, score 0.7 → LR 2.2, score 0.9 → LR 3.5
            return Math.exp((score - 0.35) * 2.5);
        }

        /**
         * Combine multiple likelihood ratios into a posterior probability.
         * Uses log-odds for numerical stability.
         *
         * @param longLRs likelihood ratios for LONG from each cluster
         * @param shortLRs likelihood ratios for SHORT from each cluster
         * @return [posteriorLong, posteriorShort, confidence] in [0,1]
         */
        public double[] computePosterior(double[] longLRs, double[] shortLRs) {
            // Convert prior to log-odds
            double logOddsPrior = Math.log(priorWinRate / (1 - priorWinRate));

            // Accumulate evidence for LONG
            double logOddsLong = logOddsPrior;
            for (double lr : longLRs) {
                if (lr > 0.01) logOddsLong += Math.log(lr);
            }
            // Cap to prevent overflow (max ~95% probability)
            logOddsLong = clamp(logOddsLong, -3.0, 3.0);

            // Same for SHORT
            double logOddsShort = logOddsPrior;
            for (double lr : shortLRs) {
                if (lr > 0.01) logOddsShort += Math.log(lr);
            }
            logOddsShort = clamp(logOddsShort, -3.0, 3.0);

            // Convert back to probabilities
            double pLong  = 1.0 / (1.0 + Math.exp(-logOddsLong));
            double pShort = 1.0 / (1.0 + Math.exp(-logOddsShort));

            // Confidence = how far the stronger side is from 50%
            double maxP = Math.max(pLong, pShort);
            double confidence = (maxP - 0.5) * 2.0; // 0..1

            return new double[]{ pLong, pShort, confidence };
        }

        /**
         * Calibrate a raw probability using historical accuracy.
         * Uses Platt scaling: calibrated = 1 / (1 + exp(A*raw + B))
         * where A and B are fit from historical predictions vs outcomes.
         *
         * Simplified version: if historical accuracy < predicted, scale down.
         */
        public double calibrate(double rawProbability, double historicalAccuracy, int sampleSize) {
            if (sampleSize < 15) return rawProbability; // Not enough data

            // Platt scaling simplified: blend raw with historical accuracy
            // Weight of historical data increases with sample size
            double weight = Math.min(0.5, sampleSize / 100.0);
            double calibrated = rawProbability * (1 - weight) + historicalAccuracy * 100 * weight;

            // Never claim more than 82% (markets are too uncertain)
            // Never claim less than 50% (otherwise why signal?)
            return clamp(calibrated, 50, 82);
        }
    }

    /* ════════════════════════════════════════════════════════════════
       RISK ENGINE
       ════════════════════════════════════════════════════════════════ */

    public static final class RiskEngine {
        private final double maxPortfolioRisk;
        private final double maxTotalExposure;
        private final double kellyFractionFactor;
        private final double maintenanceMarginRate;

        public RiskEngine() { this(0.02, 0.50, 0.25, 0.004); }
        public RiskEngine(double maxPortfolioRisk, double maxTotalExposure,
                          double kellyFraction, double maintenanceMarginRate) {
            this.maxPortfolioRisk = maxPortfolioRisk;
            this.maxTotalExposure = maxTotalExposure;
            this.kellyFractionFactor = kellyFraction;
            this.maintenanceMarginRate = maintenanceMarginRate;
        }

        public static final class TradeSignal {
            public final String symbol; public final Side side; public final CoinType type;
            public final double entry, stop, tp1, tp2, tp3;
            public final double riskRewardRatio, confidence; public final String reason;
            public final int leverage; public final double positionSizeUSDT, riskAmountUSDT;
            public final double liquidationPrice, expectedSlippage, kellyFraction;
            public final List<String> flags;

            public TradeSignal(String symbol, Side side, CoinType type,
                               double entry, double stop, double tp1, double tp2, double tp3,
                               double rr, double confidence, String reason, int leverage,
                               double positionSize, double riskAmount, double liqPrice,
                               double slippage, double kelly, List<String> flags) {
                this.symbol = symbol; this.side = side; this.type = type;
                this.entry = entry; this.stop = stop; this.tp1 = tp1; this.tp2 = tp2; this.tp3 = tp3;
                this.riskRewardRatio = rr; this.confidence = confidence; this.reason = reason;
                this.leverage = leverage; this.positionSizeUSDT = positionSize;
                this.riskAmountUSDT = riskAmount; this.liquidationPrice = liqPrice;
                this.expectedSlippage = slippage; this.kellyFraction = kelly;
                this.flags = flags != null ? List.copyOf(flags) : List.of();
            }
        }

        public double kellyFraction(double winRate, double avgWin, double avgLoss) {
            if (winRate <= 0 || winRate >= 1 || avgWin <= 0 || avgLoss <= 0) return 0;
            double b = avgWin / avgLoss;
            double fullKelly = (winRate * b - (1 - winRate)) / b;
            if (fullKelly <= 0) return 0;
            return Math.min(fullKelly * this.kellyFractionFactor, maxPortfolioRisk);
        }

        public double positionSize(double balance, double entry, double stopLoss,
                                   CoinType type, double confidence, double avgRR) {
            if (balance <= 0 || entry <= 0) return 0;
            double stopPct = Math.max(0.001, Math.abs(entry - stopLoss) / entry);
            double riskFraction = confidence > 0 && avgRR > 0
                    ? kellyFraction(confidence, avgRR, 1.0)
                    : maxPortfolioRisk * 0.5;
            riskFraction = Math.min(riskFraction, maxPortfolioRisk / type.riskMultiplier);
            double posSize = Math.min((balance * riskFraction) / stopPct, balance * maxTotalExposure);
            posSize = Math.min(posSize, balance * type.maxLeverage);
            return Math.max(posSize, 6.5);
        }

        public int optimalLeverage(double entry, double stopLoss, CoinType type) {
            double stopPct = Math.max(0.001, Math.abs(entry - stopLoss) / entry);
            int lev = clampInt((int)(maxPortfolioRisk / stopPct), 1, type.maxLeverage);
            double liqDist = (1.0 / lev) * (1.0 - maintenanceMarginRate);
            if (liqDist < stopPct * 1.5)
                lev = clampInt((int)(1.0 / (stopPct * 1.5 + maintenanceMarginRate)), 1, type.maxLeverage);
            return lev;
        }

        public double liquidationPrice(double entry, int leverage, Side side) {
            if (leverage <= 0) return 0;
            double invLev = 1.0 / leverage;
            return side == Side.LONG ? entry * (1.0 - invLev + maintenanceMarginRate)
                    : entry * (1.0 + invLev - maintenanceMarginRate);
        }

        public TradeSignal buildSignal(String symbol, Side side, CoinType type,
                                       double entry, double atr, double confidence,
                                       double balance, double historicalAvgRR,
                                       String reason, List<String> flags) {
            if (entry <= 0 || atr <= 0 || confidence <= 0.50) return null;
            double stopMult = type == CoinType.MEME ? 2.2 : type == CoinType.ALT ? 1.8 : 1.5;
            double stopDist = Math.max(atr * stopMult, entry * 0.002);
            double rrRatio  = confidence > 0.75 ? 3.5 : confidence > 0.65 ? 2.8 : confidence > 0.58 ? 2.2 : 1.8;
            rrRatio = Math.max(rrRatio, 1.5);
            double stop = side == Side.LONG ? entry - stopDist : entry + stopDist;
            double tp1   = side == Side.LONG ? entry + stopDist       : entry - stopDist;
            double tp2   = side == Side.LONG ? entry + stopDist * rrRatio : entry - stopDist * rrRatio;
            double tp3   = side == Side.LONG ? entry + stopDist * (rrRatio + 1.0) : entry - stopDist * (rrRatio + 1.0);
            double posSize = positionSize(balance, entry, stop, type, confidence, historicalAvgRR);
            int leverage   = optimalLeverage(entry, stop, type);
            double liqPrice = liquidationPrice(entry, leverage, side);
            if (side == Side.LONG ? liqPrice >= stop : liqPrice <= stop)
                liqPrice = liquidationPrice(entry, Math.max(1, leverage - 2), side);
            return new TradeSignal(symbol, side, type, entry, stop, tp1, tp2, tp3, rrRatio, confidence,
                    reason, leverage, posSize, posSize * Math.abs(entry - stop) / entry,
                    liqPrice, type.expectedSlippage, kellyFraction(confidence, historicalAvgRR > 0 ? historicalAvgRR : rrRatio, 1.0),
                    flags);
        }
    }

    /* ════════════════════════════════════════════════════════════════
       ADAPTIVE BRAIN
       ════════════════════════════════════════════════════════════════ */

    public static final class AdaptiveBrain {
        private static final double MAX_BIAS = 0.10, DECAY = 0.99;
        private static final int MAX_HISTORY = 200;
        private final Map<String, Deque<TradeResult>> symbolHistory = new ConcurrentHashMap<>();
        private final Map<String, Double> symbolBias = new ConcurrentHashMap<>();

        public static final class TradeResult {
            public final boolean win; public final double pnlPct, confidence;
            public final long timestamp; public final String strategy;
            public TradeResult(boolean win, double pnlPct, double confidence, String strategy) {
                this.win = win; this.pnlPct = pnlPct; this.confidence = confidence;
                this.timestamp = System.currentTimeMillis(); this.strategy = strategy;
            }
        }

        public void registerResult(String symbol, TradeResult result) {
            Deque<TradeResult> hist = symbolHistory.computeIfAbsent(symbol, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
            synchronized (hist) {
                hist.addLast(result);
                while (hist.size() > MAX_HISTORY) hist.removeFirst();
            }
            double delta = result.win ? 0.008 : -0.010;
            symbolBias.merge(symbol, delta, Double::sum);
            symbolBias.compute(symbol, (k, v) -> clamp((v == null ? 0 : v) * DECAY, -MAX_BIAS, MAX_BIAS));
        }

        public double getCalibratedWinRate(String symbol, double defaultRate) {
            Deque<TradeResult> hist = symbolHistory.get(symbol);
            if (hist == null || hist.size() < 20) return defaultRate;
            synchronized (hist) {
                List<TradeResult> list = new ArrayList<>(hist);
                double wW = 0, tW = 0;
                for (int i = 0; i < list.size(); i++) {
                    double w = 0.5 + 0.5 * ((double) i / list.size());
                    tW += w;
                    if (list.get(i).win) wW += w;
                }
                return tW > 0 ? wW / tW : defaultRate;
            }
        }

        public double getHistoricalAvgRR(String symbol) {
            Deque<TradeResult> hist = symbolHistory.get(symbol);
            if (hist == null || hist.size() < 10) return 2.0;
            synchronized (hist) {
                double sW = 0, sL = 0; int wins = 0, losses = 0;
                for (TradeResult r : hist) {
                    if (r.win) { sW += Math.abs(r.pnlPct); wins++; }
                    else       { sL += Math.abs(r.pnlPct); losses++; }
                }
                double avgL = losses > 0 ? sL / losses : 1;
                return avgL > 0 && wins > 0 ? (sW / wins) / avgL : 2.0;
            }
        }

        public double getSymbolBias(String symbol) { return symbolBias.getOrDefault(symbol, 0.0); }
    }

    /* ════════════════════════════════════════════════════════════════
       UTILITY
       ════════════════════════════════════════════════════════════════ */

    public static double clamp(double v, double min, double max) { return Math.max(min, Math.min(max, v)); }
    public static int    clampInt(int v, int min, int max)       { return Math.max(min, Math.min(max, v)); }
    public static Candle last(List<Candle> c)                    { return c.get(c.size() - 1); }
    public static boolean valid(List<?> c, int minBars)          { return c != null && c.size() >= minBars; }

    /* ════════════════════════════════════════════════════════════════
       [v21.0 NEW] SESSION DETECTION
       Markets have distinct sessions with different characteristics.
       Asia:  00:00-08:00 UTC — low liquidity, false breakouts, tight ranges
       London: 08:00-12:00 UTC — real moves begin, liquidity sweeps, volatility
       NY:    13:00-21:00 UTC — max volatility, trends, institutional flow
       LondonClose: 15:00-17:00 UTC — reversals, profit taking
       DeadZone: 21:00-00:00 UTC — low volume, avoid
       ════════════════════════════════════════════════════════════════ */

    public enum TradingSession {
        ASIA(0.65, false, "Asia — low liquidity, false breakouts common"),
        LONDON_OPEN(1.15, true, "London Open — sweeps & real moves begin"),
        LONDON(1.05, true, "London — trending, good liquidity"),
        NY_OPEN(1.20, true, "NY Open — max volatility, best entries"),
        NY(1.10, true, "NY — strong trends, institutional flow"),
        LONDON_CLOSE(0.85, true, "London Close — reversals, profit taking"),
        NY_CLOSE(0.80, false, "NY Close — winding down"),
        DEAD_ZONE(0.50, false, "Dead Zone — avoid trading");

        public final double confidenceMultiplier;
        public final boolean isActiveTradingSession;
        public final String description;

        TradingSession(double confMult, boolean active, String desc) {
            this.confidenceMultiplier = confMult;
            this.isActiveTradingSession = active;
            this.description = desc;
        }
    }

    public static TradingSession detectSession(long utcMillis) {
        java.time.ZonedDateTime utc = java.time.Instant.ofEpochMilli(utcMillis)
                .atZone(java.time.ZoneId.of("UTC"));
        int h = utc.getHour();
        int m = utc.getMinute();
        double hm = h + m / 60.0;

        if (hm >= 0 && hm < 7.5)    return TradingSession.ASIA;
        if (hm >= 7.5 && hm < 9.5)  return TradingSession.LONDON_OPEN;
        if (hm >= 9.5 && hm < 13)   return TradingSession.LONDON;
        if (hm >= 13 && hm < 14.5)  return TradingSession.NY_OPEN;
        if (hm >= 14.5 && hm < 15)  return TradingSession.NY;
        if (hm >= 15 && hm < 17)    return TradingSession.LONDON_CLOSE;
        if (hm >= 17 && hm < 21)    return TradingSession.NY_CLOSE;
        return TradingSession.DEAD_ZONE; // 21:00-00:00
    }

    public static TradingSession detectCurrentSession() {
        return detectSession(System.currentTimeMillis());
    }

    /** Should this session be treated as low-quality for signal generation? */
    public static boolean isLowQualitySession() {
        TradingSession s = detectCurrentSession();
        return s == TradingSession.ASIA || s == TradingSession.DEAD_ZONE;
    }

    /* ════════════════════════════════════════════════════════════════
       [v21.0 NEW] FAIR VALUE GAP (FVG) DETECTOR — institutional footprints
       FVG = gap between candle[i-2].high and candle[i].low (bullish)
       or candle[i-2].low and candle[i].high (bearish).
       Price tends to fill FVGs, making them magnets.
       ════════════════════════════════════════════════════════════════ */

    public static final class FairValueGap {
        public final double top, bottom;
        public final boolean isBullish;
        public final int formationBar;
        public final double gapSize;

        public FairValueGap(double top, double bottom, boolean bullish, int bar) {
            this.top = top; this.bottom = bottom;
            this.isBullish = bullish; this.formationBar = bar;
            this.gapSize = top - bottom;
        }

        public boolean isFilled(double price) {
            return isBullish ? price <= bottom : price >= top;
        }

        public boolean isPriceInGap(double price) {
            return price >= bottom && price <= top;
        }

        public double midpoint() { return (top + bottom) / 2.0; }
    }

    public static List<FairValueGap> detectFairValueGaps(List<Candle> candles, int lookback) {
        List<FairValueGap> gaps = new ArrayList<>();
        if (candles == null || candles.size() < lookback + 3) return gaps;
        int n = candles.size();
        int start = Math.max(2, n - lookback);
        double currentPrice = candles.get(n - 1).close;

        for (int i = start; i < n; i++) {
            Candle c0 = candles.get(i - 2);
            Candle c2 = candles.get(i);

            // Bullish FVG: c0.high < c2.low → gap up
            if (c0.high < c2.low) {
                FairValueGap fvg = new FairValueGap(c2.low, c0.high, true, i);
                // Only include unfilled gaps
                boolean filled = false;
                for (int j = i + 1; j < n; j++) {
                    if (candles.get(j).low <= c0.high) { filled = true; break; }
                }
                if (!filled) gaps.add(fvg);
            }

            // Bearish FVG: c0.low > c2.high → gap down
            if (c0.low > c2.high) {
                FairValueGap fvg = new FairValueGap(c0.low, c2.high, false, i);
                boolean filled = false;
                for (int j = i + 1; j < n; j++) {
                    if (candles.get(j).high >= c0.low) { filled = true; break; }
                }
                if (!filled) gaps.add(fvg);
            }
        }
        return gaps;
    }

    /* ════════════════════════════════════════════════════════════════
       [v21.0 NEW] CHANGE OF CHARACTER (CHoCH) DETECTOR
       CHoCH = first sign of trend reversal.
       In uptrend: price makes a lower low (breaks the last higher low)
       In downtrend: price makes a higher high (breaks the last lower high)
       This is THE earliest structural reversal signal.
       ════════════════════════════════════════════════════════════════ */

    public static final class CHoCHResult {
        public final boolean detected;
        public final boolean bullishReversal; // true = bearish→bullish, false = bullish→bearish
        public final double level; // price level where CHoCH occurred
        public final int barIndex;
        public final double strength; // 0..1

        public CHoCHResult(boolean detected, boolean bullish, double level, int bar, double strength) {
            this.detected = detected; this.bullishReversal = bullish;
            this.level = level; this.barIndex = bar; this.strength = strength;
        }
    }

    public static CHoCHResult detectCHoCH(List<Candle> candles, int lookback) {
        if (candles == null || candles.size() < lookback + 5)
            return new CHoCHResult(false, false, 0, -1, 0);

        int n = candles.size();
        double atr14 = atr(candles, 14);

        // Find last 3 swing highs and lows using pivot detection
        int pivotStr = 3;
        List<double[]> highs = new ArrayList<>();
        List<double[]> lows = new ArrayList<>();

        for (int i = pivotStr; i < n - pivotStr; i++) {
            boolean isHigh = true, isLow = true;
            for (int j = 1; j <= pivotStr; j++) {
                if (candles.get(i).high <= candles.get(i - j).high || candles.get(i).high <= candles.get(i + j).high) isHigh = false;
                if (candles.get(i).low >= candles.get(i - j).low || candles.get(i).low >= candles.get(i + j).low) isLow = false;
            }
            if (isHigh) highs.add(new double[]{candles.get(i).high, i});
            if (isLow) lows.add(new double[]{candles.get(i).low, i});
        }

        if (highs.size() < 3 || lows.size() < 3)
            return new CHoCHResult(false, false, 0, -1, 0);

        double price = candles.get(n - 1).close;

        // Check for bullish CHoCH: was in downtrend (LH/LL), now making HH
        double lastHigh = highs.get(highs.size() - 1)[0];
        double prevHigh = highs.get(highs.size() - 2)[0];
        double prevPrevHigh = highs.get(highs.size() - 3)[0];

        double lastLow = lows.get(lows.size() - 1)[0];
        double prevLow = lows.get(lows.size() - 2)[0];

        // Was bearish (LH pattern): prevPrevHigh > prevHigh
        // Now bullish break: lastHigh > prevHigh OR current price > prevHigh
        boolean wasBearish = prevPrevHigh > prevHigh;
        boolean bullishBreak = wasBearish && (lastHigh > prevHigh || price > prevHigh);

        if (bullishBreak) {
            double strength = clamp(Math.abs(price - prevHigh) / (atr14 + 1e-10) * 0.5, 0.2, 0.95);
            return new CHoCHResult(true, true, prevHigh, (int) highs.get(highs.size() - 1)[1], strength);
        }

        // Check for bearish CHoCH: was in uptrend (HH/HL), now making LL
        boolean wasBullish = prevLow > lows.get(lows.size() - 3)[0];
        boolean bearishBreak = wasBullish && (lastLow < prevLow || price < prevLow);

        if (bearishBreak) {
            double strength = clamp(Math.abs(price - prevLow) / (atr14 + 1e-10) * 0.5, 0.2, 0.95);
            return new CHoCHResult(true, false, prevLow, (int) lows.get(lows.size() - 1)[1], strength);
        }

        return new CHoCHResult(false, false, 0, -1, 0);
    }

    /* ════════════════════════════════════════════════════════════════
       [v21.0 NEW] LIQUIDATION LEVEL ESTIMATOR
       On futures, clusters of liquidations act as price magnets.
       This estimates where liquidation clusters likely are based on
       recent swing highs/lows + common leverage levels (5x, 10x, 20x, 25x).
       ════════════════════════════════════════════════════════════════ */

    public static final class LiquidationCluster {
        public final double priceLevel;
        public final String description;
        public final double magnetStrength; // 0..1 — how strong the pull

        public LiquidationCluster(double price, String desc, double strength) {
            this.priceLevel = price; this.description = desc; this.magnetStrength = strength;
        }
    }

    public static List<LiquidationCluster> estimateLiquidationLevels(
            List<Candle> candles, double currentPrice) {
        List<LiquidationCluster> clusters = new ArrayList<>();
        if (candles == null || candles.size() < 50) return clusters;

        // Find recent swing highs and lows
        SwingStructureResult ss = analyzeSwingStructure(candles, 3);
        double swHigh = ss.lastSwingHigh;
        double swLow = ss.lastSwingLow;

        // Common leverage levels and their liquidation distances
        int[] leverages = {5, 10, 20, 25};
        double mmr = 0.004; // maintenance margin rate

        // Longs opened near swing low — their liquidation is below
        for (int lev : leverages) {
            double liqBelow = swLow * (1.0 - (1.0 / lev) + mmr);
            if (liqBelow > 0 && liqBelow < currentPrice) {
                double dist = Math.abs(currentPrice - liqBelow) / currentPrice;
                double strength = dist < 0.03 ? 0.8 : dist < 0.05 ? 0.5 : 0.3;
                clusters.add(new LiquidationCluster(liqBelow,
                        "Long liq " + lev + "x from swing low", strength));
            }
        }

        // Shorts opened near swing high — their liquidation is above
        for (int lev : leverages) {
            double liqAbove = swHigh * (1.0 + (1.0 / lev) - mmr);
            if (liqAbove > currentPrice) {
                double dist = Math.abs(liqAbove - currentPrice) / currentPrice;
                double strength = dist < 0.03 ? 0.8 : dist < 0.05 ? 0.5 : 0.3;
                clusters.add(new LiquidationCluster(liqAbove,
                        "Short liq " + lev + "x from swing high", strength));
            }
        }

        // Sort by proximity to current price
        clusters.sort(Comparator.comparingDouble(c ->
                Math.abs(c.priceLevel - currentPrice)));

        return clusters;
    }

    /* ════════════════════════════════════════════════════════════════
       [v21.0 NEW] CORRELATION CHECKER
       Tracks if multiple coins are moving in same direction = correlated risk.
       ════════════════════════════════════════════════════════════════ */

    public static double estimateCorrelation(List<Candle> c1, List<Candle> c2, int period) {
        if (c1 == null || c2 == null || c1.size() < period + 1 || c2.size() < period + 1)
            return 0.85; // Default high correlation for safety

        int n1 = c1.size(), n2 = c2.size();
        int len = Math.min(period, Math.min(n1 - 1, n2 - 1));

        double[] r1 = new double[len];
        double[] r2 = new double[len];
        for (int i = 0; i < len; i++) {
            r1[i] = Math.log(c1.get(n1 - len + i).close / c1.get(n1 - len + i - 1).close);
            r2[i] = Math.log(c2.get(n2 - len + i).close / c2.get(n2 - len + i - 1).close);
        }

        double m1 = 0, m2 = 0;
        for (int i = 0; i < len; i++) { m1 += r1[i]; m2 += r2[i]; }
        m1 /= len; m2 /= len;

        double cov = 0, v1 = 0, v2 = 0;
        for (int i = 0; i < len; i++) {
            cov += (r1[i] - m1) * (r2[i] - m2);
            v1 += (r1[i] - m1) * (r1[i] - m1);
            v2 += (r2[i] - m2) * (r2[i] - m2);
        }

        double denom = Math.sqrt(v1 * v2);
        return denom > 1e-12 ? clamp(cov / denom, -1.0, 1.0) : 0.85;
    }

    /* ════════════════════════════════════════════════════════════════
       [v16.0] FORECAST ENGINE — REVERSAL-AWARE Direction Forecaster
       ════════════════════════════════════════════════════════════════

       PHILOSOPHY CHANGE (v16.0):
       Old engine asked: "what is the trend?" → extrapolated it.
       This is WHY it was always wrong — 5 bullish candles → "BULL" → instant reversal.

       New engine asks: "HOW MUCH ENERGY remains in this move?"

       Core concept: every move has FUEL. Fuel = volume × momentum × fresh structure.
       When fuel runs out → reversal. When fuel is full → trend continues.

       The engine now has TWO brains:
       1. TREND BRAIN: Is there a trend? Which direction? (classic — kept but deprioritized)
       2. EXHAUSTION BRAIN: Is the current move RUNNING OUT OF FUEL? (new — dominant)

       The exhaustion brain OVERRIDES the trend brain.
       5 green candles + exhaustion signals = BEAR forecast (not BULL!).

       New factors:
       · MoveAge — how many bars since last significant reversal
       · MoveDepth — how far has price moved from move origin (% of ATR)
       · VolumeFade — is volume declining during the move? (= running dry)
       · MomentumDecay — is each bar smaller than the previous? (= deceleration)
       · WickRejection — are wicks growing? (= opposition forming)
       · RSI Extreme Zone — is RSI >70 or <30 with divergence?
       · VPOC Magnet — is price far from high-volume node? (rubber band)
       · Squeeze Detection — is ATR at historic lows? (= about to explode)
       · HTF Alignment — does the higher timeframe agree?
       · Orderflow — taker buy/sell ratio
       ════════════════════════════════════════════════════════════════ */

    public static final class ForecastEngine {

        public enum ForecastBias { STRONG_BULL, BULL, NEUTRAL, BEAR, STRONG_BEAR }
        public enum TrendPhase { EARLY, MID, LATE, EXHAUSTION }

        public static final class ForecastResult {
            public final ForecastBias bias;
            public final double directionScore; // [-1..+1]
            public final double confidence;      // [0..1]
            public final TrendPhase trendPhase;
            public final double projectedMovePct;
            public final double magnetLevel;
            public final Map<String, Double> factorScores;

            public ForecastResult(ForecastBias bias, double dirScore, double conf,
                                  TrendPhase phase, double projMove, double magnet,
                                  Map<String, Double> factors) {
                this.bias = bias; this.directionScore = dirScore;
                this.confidence = conf; this.trendPhase = phase;
                this.projectedMovePct = projMove; this.magnetLevel = magnet;
                this.factorScores = factors != null ? Collections.unmodifiableMap(factors) : Map.of();
            }
            @Override public String toString() {
                return String.format("Forecast[%s dir=%.3f conf=%.0f%% phase=%s move=%.3f%%]",
                        bias, directionScore, confidence * 100, trendPhase, projectedMovePct * 100);
            }
        }

        public ForecastResult forecast(List<Candle> c5, List<Candle> c15,
                                       List<Candle> c1h, double volumeDelta) {
            if (c15 == null || c15.size() < 100 || c1h == null || c1h.size() < 50) return null;
            int n = c15.size();
            double price = c15.get(n - 1).close;
            double atr14 = fcAtr(c15, 14);
            if (atr14 <= 0 || price <= 0) return null;

            Map<String, Double> f = new LinkedHashMap<>();

            // ═══════════════════════════════════════════════════════
            // STEP 1: Identify the CURRENT MOVE (direction + age + depth)
            // ═══════════════════════════════════════════════════════
            MoveInfo move = identifyCurrentMove(c15, atr14);
            f.put("MOVE_DIR", (double) move.direction);
            f.put("MOVE_AGE", (double) move.ageBars);
            f.put("MOVE_DEPTH", move.depthAtr);

            // ═══════════════════════════════════════════════════════
            // STEP 2: EXHAUSTION BRAIN — is this move dying?
            // This is the KEY innovation. Each factor answers:
            // "Is the fuel running out?" → positive = exhausted → reversal likely
            // ═══════════════════════════════════════════════════════
            double exhaustionScore = 0;
            int exhaustionSignals = 0;

            // Factor E1: Volume Fade — volume declining during the move
            double volFade = calcVolumeFade(c15, move);
            f.put("VOL_FADE", volFade);
            if (volFade > 0.3) { exhaustionScore += volFade * 0.25; exhaustionSignals++; }

            // Factor E2: Momentum Decay — each bar getting smaller
            double momDecay = calcMomentumDecay(c15, move, atr14);
            f.put("MOM_DECAY", momDecay);
            if (momDecay > 0.3) { exhaustionScore += momDecay * 0.25; exhaustionSignals++; }

            // Factor E3: Wick Rejection — opposition forming
            double wickReject = calcWickRejection(c15, move);
            f.put("WICK_REJ", wickReject);
            if (wickReject > 0.3) { exhaustionScore += wickReject * 0.20; exhaustionSignals++; }

            // Factor E4: RSI Extreme + Divergence
            double rsiExhaust = calcRsiExhaustion(c15, move);
            f.put("RSI_EXH", rsiExhaust);
            if (rsiExhaust > 0.3) { exhaustionScore += rsiExhaust * 0.20; exhaustionSignals++; }

            // Factor E5: Move overextended (moved too far too fast)
            double overext = calcOverextension(move, atr14);
            f.put("OVEREXT", overext);
            if (overext > 0.3) { exhaustionScore += overext * 0.15; exhaustionSignals++; }

            // Factor E6: VPOC Rubber Band — price far from volume magnet
            double vpocPull = calcVpocPull(c15, price, atr14);
            f.put("VPOC_PULL", vpocPull);

            exhaustionScore = clamp(exhaustionScore, 0, 1);
            f.put("EXHAUSTION", exhaustionScore);

            // [v21.0] HTF EXHAUSTION CONTEXT — critical fix
            // Exhaustion on 15m does NOT mean reversal if 1H trend just started.
            // If 1H is in EARLY phase and aligned with the move → reduce exhaustion weight.
            if (c1h != null && c1h.size() >= 50 && exhaustionScore > 0.3) {
                double rsi1h = rsi(c1h, 14);
                double ema9_1h = fcEma(c1h, 9);
                double ema21_1h = fcEma(c1h, 21);
                double ema50_1h = fcEma(c1h, 50);
                double price1h = c1h.get(c1h.size() - 1).close;
                MACDResult macd1h = macd(c1h, 12, 26, 9);

                boolean htf1hBullEarly = price1h > ema9_1h && ema9_1h > ema21_1h
                        && Math.abs(ema9_1h - ema21_1h) < atr(c1h, 14) * 0.5
                        && rsi1h > 50 && rsi1h < 70
                        && macd1h.histogram > 0;
                boolean htf1hBearEarly = price1h < ema9_1h && ema9_1h < ema21_1h
                        && Math.abs(ema9_1h - ema21_1h) < atr(c1h, 14) * 0.5
                        && rsi1h < 50 && rsi1h > 30
                        && macd1h.histogram < 0;

                // If 1H is in early trend AND aligned with the current move → reduce exhaustion
                if ((move.direction > 0 && htf1hBullEarly) || (move.direction < 0 && htf1hBearEarly)) {
                    exhaustionScore *= 0.50; // Halve exhaustion — HTF says "still fresh"
                    f.put("HTF_EXHAUST_OVERRIDE", 1.0);
                }
                // If 1H is AGAINST the move → amplify exhaustion
                if ((move.direction > 0 && htf1hBearEarly) || (move.direction < 0 && htf1hBullEarly)) {
                    exhaustionScore = Math.min(1.0, exhaustionScore * 1.30);
                    f.put("HTF_EXHAUST_AMPLIFY", 1.0);
                }
            }

            // ═══════════════════════════════════════════════════════
            // STEP 3: TREND BRAIN — but weaker voice now
            // ═══════════════════════════════════════════════════════
            double trendDir = 0;

            // Short-term LR slope (fast, 8 bars)
            double lr8 = linRegSlope(c15, 8);
            double lrScore = clamp(lr8 * 6 / atr14 * 0.30, -0.6, 0.6);
            f.put("LR_8", lrScore);
            trendDir += lrScore * 0.20;

            // Swing structure
            double swingScore = com.bot.DecisionEngineMerged.marketStructure(c15) * 0.50;
            f.put("SWING", swingScore);
            trendDir += swingScore * 0.15;

            // Orderflow
            double ofScore = calcOrderflow(c15, volumeDelta);
            f.put("OF", ofScore);
            trendDir += ofScore * 0.15;

            // HTF alignment (1h)
            double htfScore = calcHTF(c15, c1h);
            f.put("HTF", htfScore);
            trendDir += htfScore * 0.15;

            // Fisher cross
            double fisherScore = calcFisher(c15, 10);
            f.put("FISHER", fisherScore);
            trendDir += fisherScore * 0.10;

            trendDir = clamp(trendDir, -1.0, 1.0);

            // ═══════════════════════════════════════════════════════
            // STEP 4: SQUEEZE detection — [v21.0] IMPROVED: don't block,
            // instead WAIT for direction and give breakout signal.
            // Squeeze = lowest volatility → BIGGEST moves come after.
            // Old code blocked ALL signals during squeeze — wrong!
            // New: reduce confidence but watch for breakout candle.
            // ═══════════════════════════════════════════════════════
            boolean squeezed = isVolatilitySqueeze(c15, atr14);
            f.put("SQUEEZE", squeezed ? 1.0 : 0.0);

            // [v21.0] Detect breakout FROM squeeze
            boolean squeezeBreakout = false;
            if (squeezed && n >= 3) {
                Candle last = c15.get(n - 1);
                Candle prev = c15.get(n - 2);
                // Breakout = current bar body > 1.5x previous bar range
                double breakoutThreshold = prev.range * 1.5;
                if (last.body > breakoutThreshold && last.volume > prev.volume * 1.3) {
                    squeezeBreakout = true;
                    f.put("SQUEEZE_BREAK", last.isBullish ? 1.0 : -1.0);
                }
            }

            // ═══════════════════════════════════════════════════════
            // STEP 5: COMBINE — Exhaustion OVERRIDES Trend
            // ═══════════════════════════════════════════════════════
            TrendPhase phase = detectPhase(c15, c1h, move, exhaustionScore);

            double dir;
            if (squeezed && !squeezeBreakout) {
                // In squeeze WITHOUT breakout: minimal directional conviction
                dir = trendDir * 0.15;
            } else if (squeezed && squeezeBreakout) {
                // [v21.0] Squeeze BREAKOUT: strong directional signal!
                // The breakout candle direction determines the forecast
                Candle last = c15.get(n - 1);
                double breakDir = last.isBullish ? 0.65 : -0.65;
                dir = breakDir * 0.70 + trendDir * 0.30;
                // Override exhaustion during breakout — fresh energy
                exhaustionScore = Math.min(exhaustionScore, 0.2);
            } else if (exhaustionScore > 0.50 && exhaustionSignals >= 2) {
                // [v19.0] STRONG exhaustion: forecast REVERSAL (opposite to current move)
                // Lowered from 0.55 / 3 signals to make it catch bottoms/tops faster
                dir = -move.direction * exhaustionScore * 0.85;
                // Trend brain can barely whisper
                dir += trendDir * 0.05;
            } else if (exhaustionScore > 0.30 && exhaustionSignals >= 1) {
                // [v19.0] Moderate exhaustion: heavily reduce trend conviction
                dir = trendDir * 0.25 + vpocPull * 0.25;
                // Stronger counter-move bias
                dir -= move.direction * exhaustionScore * 0.35;
            } else if (phase == TrendPhase.EARLY) {
                // Early in move: trend brain speaks louder
                dir = trendDir * 0.70 + vpocPull * 0.10;
            } else {
                // Normal: balanced
                dir = trendDir * 0.55 + vpocPull * 0.15;
                // Mild exhaustion brake
                if (exhaustionScore > 0.15) dir *= (1.0 - exhaustionScore * 0.4);
            }

            dir = clamp(dir, -1.0, 1.0);

            // ═══════════════════════════════════════════════════════
            // STEP 6: Confidence = how many signals agree?
            // ═══════════════════════════════════════════════════════
            double conf;
            if (squeezed && !squeezeBreakout) {
                conf = 0.18; // Low confidence in unbroken squeeze
            } else if (squeezed && squeezeBreakout) {
                // [v21.0] Squeeze breakout = high conviction — energy was coiled
                conf = clamp(0.55 + Math.abs(dir) * 0.25, 0.50, 0.82);
            } else if (exhaustionScore > 0.55 && exhaustionSignals >= 3) {
                conf = clamp(0.50 + exhaustionScore * 0.30, 0.50, 0.85);
            } else {
                int agree = 0;
                double dirSign = Math.signum(dir);
                for (double v : f.values()) {
                    if (Math.signum(v) == dirSign && Math.abs(v) > 0.10) agree++;
                }
                conf = clamp(0.25 + (double) agree / f.size() * 0.50 + Math.abs(dir) * 0.20, 0.10, 0.85);
            }

            // ═══════════════════════════════════════════════════════
            // STEP 7: Bias classification
            // ═══════════════════════════════════════════════════════
            ForecastBias bias;
            if (dir > 0.40) bias = ForecastBias.STRONG_BULL;
            else if (dir > 0.12) bias = ForecastBias.BULL;
            else if (dir < -0.40) bias = ForecastBias.STRONG_BEAR;
            else if (dir < -0.12) bias = ForecastBias.BEAR;
            else bias = ForecastBias.NEUTRAL;

            double projMove = lr8 * 6 / price;
            double vpoc = calcVPOC(c15, 50);

            return new ForecastResult(bias, dir, conf, phase, projMove, vpoc, f);
        }

        // ═══════════════════════════════════════════════════════
        //  MOVE IDENTIFICATION — find origin, direction, age, depth
        // ═══════════════════════════════════════════════════════

        private static final class MoveInfo {
            final int direction;     // +1 = bullish move, -1 = bearish, 0 = flat
            final int ageBars;       // how many bars since move started
            final double depthAtr;   // how far moved in ATR units
            final int originIdx;     // bar index where the move started
            final double originPrice;
            MoveInfo(int dir, int age, double depth, int origin, double originP) {
                this.direction = dir; this.ageBars = age; this.depthAtr = depth;
                this.originIdx = origin; this.originPrice = originP;
            }
        }

        private MoveInfo identifyCurrentMove(List<Candle> c, double atr) {
            int n = c.size();
            double price = c.get(n - 1).close;

            // [v21.0] IMPROVED: Multi-factor move identification
            // Old: simple EMA5 crossover — missed Order Blocks, FVG, CHoCH
            // New: combines EMA cross + swing structure + momentum shift

            // 1) EMA-based direction (kept as baseline)
            double ema5 = fcEma(c, 5);
            double ema13 = fcEma(c, 13);
            boolean currentBull = price > ema5 && ema5 > ema13;
            boolean currentBear = price < ema5 && ema5 < ema13;
            if (!currentBull && !currentBear) {
                currentBull = price > ema5;
            }

            // 2) Find swing points for structural move origin
            int swingOrigin = -1;
            double swingPrice = price;
            // Walk back looking for the most recent significant swing (reversal point)
            int lookback = Math.min(40, n - 5);
            if (currentBull) {
                // In bullish move: find the lowest low (origin)
                double lowestLow = price;
                for (int i = n - 2; i >= Math.max(0, n - 1 - lookback); i--) {
                    if (c.get(i).low < lowestLow) {
                        lowestLow = c.get(i).low;
                        swingOrigin = i;
                        swingPrice = lowestLow;
                    }
                    // Stop if we hit a bar that was significantly above current range
                    // (meaning the move started later)
                    if (c.get(i).close > price + atr * 0.5) break;
                    // Stop if we find a gap down followed by reversal (CHoCH)
                    if (i < n - 3 && c.get(i).close < c.get(i + 1).close
                            && c.get(i + 1).close < c.get(i + 2).close
                            && c.get(i + 2).close > c.get(i + 1).close) {
                        if (swingOrigin < 0) swingOrigin = i;
                        break;
                    }
                }
            } else {
                // In bearish move: find the highest high (origin)
                double highestHigh = price;
                for (int i = n - 2; i >= Math.max(0, n - 1 - lookback); i--) {
                    if (c.get(i).high > highestHigh) {
                        highestHigh = c.get(i).high;
                        swingOrigin = i;
                        swingPrice = highestHigh;
                    }
                    if (c.get(i).close < price - atr * 0.5) break;
                    if (i < n - 3 && c.get(i).close > c.get(i + 1).close
                            && c.get(i + 1).close > c.get(i + 2).close
                            && c.get(i + 2).close < c.get(i + 1).close) {
                        if (swingOrigin < 0) swingOrigin = i;
                        break;
                    }
                }
            }

            // 3) Fallback to EMA-based origin if swing search failed
            if (swingOrigin < 0) {
                swingOrigin = n - 1;
                for (int i = n - 2; i >= Math.max(0, n - 30); i--) {
                    boolean barBull = c.get(i).close > fcEma(c.subList(0, i + 1), 5);
                    if (barBull != (price > ema5)) {
                        swingOrigin = i + 1;
                        break;
                    }
                    swingOrigin = i;
                }
                swingPrice = c.get(swingOrigin).close;
            }

            int age = n - 1 - swingOrigin;
            double depth = Math.abs(price - swingPrice) / (atr + 1e-12);
            int dir = currentBull ? 1 : -1;
            if (age <= 1 && depth < 0.3) dir = 0;

            return new MoveInfo(dir, age, depth, swingOrigin, swingPrice);
        }

        // ═══════════════════════════════════════════════════════
        //  EXHAUSTION FACTORS
        // ═══════════════════════════════════════════════════════

        /** Volume Fade: average volume of last 3 bars vs first 3 bars of move.
         *  If later bars have less volume → move losing fuel. Returns [0..1]. */
        private double calcVolumeFade(List<Candle> c, MoveInfo move) {
            if (move.ageBars < 4) return 0;
            int n = c.size();
            int start = move.originIdx;
            // Average volume of first 3 bars of move
            double volStart = 0;
            int cnt1 = 0;
            for (int i = start; i < Math.min(start + 3, n); i++) {
                volStart += c.get(i).volume; cnt1++;
            }
            volStart = cnt1 > 0 ? volStart / cnt1 : 1;
            // Average volume of last 3 bars
            double volEnd = 0;
            int cnt2 = 0;
            for (int i = n - 3; i < n; i++) {
                volEnd += c.get(i).volume; cnt2++;
            }
            volEnd = cnt2 > 0 ? volEnd / cnt2 : 1;
            if (volStart < 1e-12) return 0;
            double ratio = volEnd / volStart;
            // ratio < 1 = volume fading. Map: ratio 0.3→1.0, ratio 0.7→0.3, ratio 1.0→0
            return clamp((1.0 - ratio) * 1.5, 0, 1);
        }

        /** Momentum Decay: body size of last 3 bars vs first 3 bars.
         *  If each bar is smaller → momentum dying. Returns [0..1]. */
        private double calcMomentumDecay(List<Candle> c, MoveInfo move, double atr) {
            if (move.ageBars < 4) return 0;
            int n = c.size();
            int start = move.originIdx;
            // Directional body size (in trend direction) of first 3
            double bodyStart = 0;
            int cnt = 0;
            for (int i = start; i < Math.min(start + 3, n); i++) {
                double body = (c.get(i).close - c.get(i).open) * move.direction;
                bodyStart += Math.max(0, body);
                cnt++;
            }
            bodyStart = cnt > 0 ? bodyStart / cnt : 0;
            // Last 3
            double bodyEnd = 0;
            cnt = 0;
            for (int i = n - 3; i < n; i++) {
                double body = (c.get(i).close - c.get(i).open) * move.direction;
                bodyEnd += Math.max(0, body);
                cnt++;
            }
            bodyEnd = cnt > 0 ? bodyEnd / cnt : 0;
            if (bodyStart < atr * 0.05) return 0;
            double ratio = bodyEnd / (bodyStart + 1e-12);
            // Also check: are recent bars going AGAINST the trend?
            double lastBody = (c.get(n - 1).close - c.get(n - 1).open) * move.direction;
            double counterPenalty = lastBody < 0 ? 0.3 : 0; // Last bar is counter-trend
            return clamp((1.0 - ratio) * 1.3 + counterPenalty, 0, 1);
        }

        /** Wick Rejection: wicks against the move growing = opposition forming.
         *  In bullish move: upper wicks growing = sellers pushing back. Returns [0..1]. */
        private double calcWickRejection(List<Candle> c, MoveInfo move) {
            int n = c.size();
            if (move.ageBars < 3 || n < 5) return 0;
            double wickScore = 0;
            for (int i = n - 3; i < n; i++) {
                Candle bar = c.get(i);
                double body = Math.abs(bar.close - bar.open) + 1e-12;
                if (move.direction > 0) {
                    // Bullish move → upper wicks = rejection from above
                    double uw = bar.high - Math.max(bar.close, bar.open);
                    if (uw > body * 1.5) wickScore += 0.35;
                    else if (uw > body) wickScore += 0.15;
                } else {
                    // Bearish move → lower wicks = rejection from below (buyers)
                    double lw = Math.min(bar.close, bar.open) - bar.low;
                    if (lw > body * 1.5) wickScore += 0.35;
                    else if (lw > body) wickScore += 0.15;
                }
            }
            return clamp(wickScore, 0, 1);
        }

        /** RSI Exhaustion: RSI in extreme zone + RSI deceleration.
         *  Returns [0..1], higher = more exhausted. */
        private double calcRsiExhaustion(List<Candle> c, MoveInfo move) {
            int n = c.size();
            if (n < 20) return 0;
            double rsi14 = rsi(c, 14);
            double rsi14_prev = rsi(c.subList(0, n - 3), 14);
            double score = 0;
            if (move.direction > 0) {
                // Bullish move: RSI > 70 = overbought
                if (rsi14 > 75) score += 0.40;
                else if (rsi14 > 68) score += 0.20;
                // RSI declining while price still up = bearish divergence
                if (rsi14 < rsi14_prev - 2 && rsi14 > 55) score += 0.35;
            } else if (move.direction < 0) {
                // Bearish move: RSI < 30 = oversold
                if (rsi14 < 25) score += 0.40;
                else if (rsi14 < 32) score += 0.20;
                // RSI rising while price still down = bullish divergence
                if (rsi14 > rsi14_prev + 2 && rsi14 < 45) score += 0.35;
            }
            return clamp(score, 0, 1);
        }

        /** Overextension: has the move gone too far too fast?
         *  Price > move_origin + 3*ATR in <6 bars = extended. Returns [0..1]. */
        private double calcOverextension(MoveInfo move, double atr) {
            if (move.ageBars < 2 || atr <= 0) return 0;
            // depth per bar — how fast is price moving
            double speedAtr = move.depthAtr / (move.ageBars + 1e-6);
            // >0.6 ATR per bar over 3+ bars = extended
            if (speedAtr > 0.6 && move.ageBars >= 3) return clamp(speedAtr * 0.8, 0, 1);
            // Absolute depth: > 4 ATR moved = stretched rubber band
            if (move.depthAtr > 4.0) return clamp((move.depthAtr - 3.0) * 0.3, 0, 1);
            if (move.depthAtr > 2.5) return clamp((move.depthAtr - 2.0) * 0.2, 0, 0.5);
            return 0;
        }

        /** VPOC Pull: distance from VPOC — price snaps back to high-volume node.
         *  Returns [-1..+1]: positive = VPOC is above (pull up), negative = below (pull down). */
        private double calcVpocPull(List<Candle> c, double price, double atr) {
            double vpoc = calcVPOC(c, 50);
            if (vpoc <= 0 || atr <= 0) return 0;
            double dist = (vpoc - price) / atr;
            // Only significant if > 0.5 ATR away
            if (Math.abs(dist) < 0.5) return 0;
            return clamp(dist * 0.20, -0.5, 0.5);
        }

        // ═══════════════════════════════════════════════════════
        //  TREND PHASE — now uses exhaustion brain
        // ═══════════════════════════════════════════════════════

        public TrendPhase detectPhase(List<Candle> c15, List<Candle> c1h) {
            return detectPhase(c15, c1h,
                    identifyCurrentMove(c15, fcAtr(c15, 14)), 0);
        }

        private TrendPhase detectPhase(List<Candle> c15, List<Candle> c1h,
                                       MoveInfo move, double exhaustion) {
            if (exhaustion > 0.55) return TrendPhase.EXHAUSTION;
            if (exhaustion > 0.35) return TrendPhase.LATE;
            if (move.ageBars <= 3 && move.depthAtr < 1.5) return TrendPhase.EARLY;
            if (move.ageBars <= 8 && move.depthAtr < 3.0) return TrendPhase.MID;
            if (move.ageBars > 12 || move.depthAtr > 3.5) return TrendPhase.LATE;
            return TrendPhase.MID;
        }

        // ═══════════════════════════════════════════════════════
        //  SQUEEZE detection
        // ═══════════════════════════════════════════════════════

        private boolean isVolatilitySqueeze(List<Candle> c, double currentAtr) {
            int n = c.size();
            if (n < 50) return false;
            List<Double> atrHist = new ArrayList<>();
            for (int i = Math.max(15, n - 50); i < n - 1; i += 2) {
                double a = fcAtr(c.subList(Math.max(0, i - 14), i + 1), Math.min(14, i));
                if (a > 0) atrHist.add(a);
            }
            if (atrHist.size() < 10) return false;
            Collections.sort(atrHist);
            double p20 = atrHist.get(atrHist.size() / 5);
            return currentAtr < p20 * 0.85;
        }

        // ═══════════════════════════════════════════════════════
        //  TREND BRAIN helpers (kept but secondary)
        // ═══════════════════════════════════════════════════════

        private double calcOrderflow(List<Candle> c, double vd) {
            int n = c.size(); double s = 0;
            if (n >= 5) {
                double tb = 0, tv = 0;
                for (int i = n - 5; i < n; i++) {
                    tb += c.get(i).takerBuyBaseVolume;
                    tv += c.get(i).volume;
                }
                if (tv > 0) s += (tb / tv - 0.5) * 2.0;
            }
            if (Math.abs(vd) > 0.001)
                s = s * 0.6 + clamp(Math.signum(vd) * Math.min(1, Math.abs(vd) * 3), -1, 1) * 0.4;
            return clamp(s * 0.50, -0.6, 0.6);
        }

        private double calcHTF(List<Candle> c15, List<Candle> c1h) {
            int v = 0;
            if (c1h.size() >= 25) {
                if (fcEma(c1h, 9) > fcEma(c1h, 21)) v++; else v--;
            }
            if (c15.size() >= 50) {
                // Use EMA50 on 15m as "HTF" context
                double e21 = fcEma(c15, 21), e50 = fcEma(c15, 50);
                if (e21 > e50) v++; else v--;
            }
            return switch (v) { case 2 -> 0.50; case -2 -> -0.50; default -> v * 0.15; };
        }

        private double calcFisher(List<Candle> c, int p) {
            int n = c.size();
            if (n < p + 2) return 0;
            double hi = Double.MIN_VALUE, lo = Double.MAX_VALUE;
            for (int i = n - p; i < n; i++) {
                hi = Math.max(hi, c.get(i).high);
                lo = Math.min(lo, c.get(i).low);
            }
            if (hi - lo < 1e-12) return 0;
            double norm = clamp(2.0 * ((c.get(n - 1).close - lo) / (hi - lo) - 0.5) * 0.999, -0.999, 0.999);
            double fish = 0.5 * Math.log((1 + norm) / (1 - norm));
            double pNorm = clamp(2.0 * ((c.get(n - 2).close - lo) / (hi - lo) - 0.5) * 0.999, -0.999, 0.999);
            double pFish = 0.5 * Math.log((1 + pNorm) / (1 - pNorm));
            double s = 0;
            if (fish > 0 && pFish < 0) s = 0.4;
            if (fish < 0 && pFish > 0) s = -0.4;
            s += clamp(fish * 0.12, -0.25, 0.25);
            return clamp(s, -0.8, 0.8);
        }

        // ═══════════════════════════════════════════════════════
        //  MATH primitives
        // ═══════════════════════════════════════════════════════

        private double linRegSlope(List<Candle> c, int p) {
            int n = c.size();
            if (n < p) return 0;
            double sX = 0, sY = 0, sXY = 0, sX2 = 0;
            for (int i = 0; i < p; i++) {
                double y = c.get(n - p + i).close;
                sX += i; sY += y; sXY += i * y; sX2 += i * i;
            }
            double d = p * sX2 - sX * sX;
            return Math.abs(d) < 1e-12 ? 0 : (p * sXY - sX * sY) / d;
        }

        private double calcVPOC(List<Candle> c, int p) {
            int n = c.size(), start = Math.max(0, n - p);
            double lo = Double.MAX_VALUE, hi = Double.MIN_VALUE;
            for (int i = start; i < n; i++) {
                lo = Math.min(lo, c.get(i).low);
                hi = Math.max(hi, c.get(i).high);
            }
            if (hi - lo < 1e-12) return c.get(n - 1).close;
            int bins = 50;
            double bs = (hi - lo) / bins;
            double[] vb = new double[bins];
            for (int i = start; i < n; i++) {
                double tp = (c.get(i).high + c.get(i).low + c.get(i).close) / 3.0;
                int b = Math.min(bins - 1, Math.max(0, (int) ((tp - lo) / bs)));
                vb[b] += c.get(i).volume;
            }
            int mx = 0;
            for (int i = 1; i < bins; i++) if (vb[i] > vb[mx]) mx = i;
            return lo + (mx + 0.5) * bs;
        }

        /** [v21.0 FIX] Wilder's smoothed ATR — matches TradingCore.atr() exactly.
         *  Old code used SMA which diverges 15-20% from Wilder's method.
         *  This caused ForecastEngine to miscalculate exhaustion thresholds,
         *  stop distances, and squeeze detection. */
        private double fcAtr(List<Candle> c, int period) {
            return TradingCore.atr(c, period);
        }

        private double fcEma(List<Candle> c, int p) {
            if (c.size() < p) return c.get(c.size() - 1).close;
            double k = 2.0 / (p + 1), e = c.get(c.size() - p).close;
            for (int i = c.size() - p + 1; i < c.size(); i++)
                e = c.get(i).close * k + e * (1 - k);
            return e;
        }
    } // end ForecastEngine
}