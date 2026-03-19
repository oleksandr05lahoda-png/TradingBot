package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║       TradingCore v13.0 — INSTITUTIONAL QUANT + FORECASTING FOUNDATION      ║
 * ╠══════════════════════════════════════════════════════════════════════════════╣
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

    /* ════════════════════════════════════════════════════════════════
       MATH: CCI / MFI / OBV / CMF
       ════════════════════════════════════════════════════════════════ */

    public static double cci(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period) return 0;
        int n = candles.size();
        double[] tps = new double[period];
        double sumTP = 0;
        for (int i = 0; i < period; i++) { tps[i] = candles.get(n - period + i).typicalPrice(); sumTP += tps[i]; }
        double avgTP = sumTP / period;
        double meanDev = 0;
        for (double tp : tps) meanDev += Math.abs(tp - avgTP);
        meanDev /= period;
        return meanDev > 0 ? (tps[period - 1] - avgTP) / (0.015 * meanDev) : 0;
    }

    public static double mfi(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period + 1) return 50;
        int n = candles.size();
        double posFlow = 0, negFlow = 0;
        for (int i = n - period; i < n; i++) {
            double tp = candles.get(i).typicalPrice();
            double prevTp = candles.get(i - 1).typicalPrice();
            double rawFlow = tp * candles.get(i).volume;
            if (tp > prevTp)      posFlow += rawFlow;
            else if (tp < prevTp) negFlow += rawFlow;
        }
        return negFlow > 0 ? 100.0 - 100.0 / (1.0 + posFlow / negFlow) : 100.0;
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

    public static double cmf(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period) return 0;
        int n = candles.size();
        double sumMFV = 0, sumVol = 0;
        for (int i = n - period; i < n; i++) {
            Candle c = candles.get(i);
            double clv = c.range > 0 ? ((c.close - c.low) - (c.high - c.close)) / c.range : 0;
            sumMFV += clv * c.volume;
            sumVol += c.volume;
        }
        return sumVol > 0 ? sumMFV / sumVol : 0;
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
       MATH: SHANNON ENTROPY
       ════════════════════════════════════════════════════════════════ */

    public static double entropy(List<Candle> candles, int lookback, int bins) {
        if (candles == null || candles.size() < lookback + 1) return 1.0;
        int n = candles.size();
        double[] returns = new double[lookback];
        double minR = Double.MAX_VALUE, maxR = -Double.MAX_VALUE;
        for (int i = 0; i < lookback; i++) {
            returns[i] = (candles.get(n - lookback + i).close - candles.get(n - lookback + i - 1).close)
                    / candles.get(n - lookback + i - 1).close;
            minR = Math.min(minR, returns[i]); maxR = Math.max(maxR, returns[i]);
        }
        if (Math.abs(maxR - minR) < 1e-12) return 0;
        double binWidth = (maxR - minR) / bins;
        int[] counts = new int[bins];
        for (double r : returns) {
            int bin = (int) ((r - minR) / binWidth);
            if (bin >= bins) bin = bins - 1;
            counts[bin]++;
        }
        double h = 0;
        for (int c : counts) {
            if (c > 0) { double p = (double) c / lookback; h -= p * Math.log(p) / Math.log(2); }
        }
        return h / (Math.log(bins) / Math.log(2));
    }

    /* ════════════════════════════════════════════════════════════════
       [v13.0 NEW] FISHER TRANSFORM — Lower-lag oscillator
       Converts price distribution to approximately normal distribution.
       Extreme readings (±2.5+) are much more reliable than RSI extremes.
       ════════════════════════════════════════════════════════════════ */

    public static final class FisherResult {
        public final double fisher;
        public final double signal;  // 1-bar lag of fisher
        public final double value;   // normalized midpoint used

        public FisherResult(double fisher, double signal, double value) {
            this.fisher = fisher; this.signal = signal; this.value = value;
        }

        /** True reversal signal: crossed zero from extreme */
        public boolean bullishReversal() { return fisher > 0 && signal < 0 && fisher > 1.5; }
        public boolean bearishReversal() { return fisher < 0 && signal > 0 && fisher < -1.5; }
        public boolean isOverbought()    { return fisher > 2.5; }
        public boolean isOversold()      { return fisher < -2.5; }
    }

    public static FisherResult fisherTransform(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period + 2) return new FisherResult(0, 0, 0.5);
        int n = candles.size();

        // Find highest high and lowest low over period
        double prevFisher = 0;
        double prevValue = 0;
        double fisher = 0;

        for (int i = period - 1; i < n; i++) {
            double highest = -Double.MAX_VALUE, lowest = Double.MAX_VALUE;
            for (int j = i - period + 1; j <= i; j++) {
                highest = Math.max(highest, candles.get(j).high);
                lowest  = Math.min(lowest, candles.get(j).low);
            }
            double range = highest - lowest;
            double value = range > 1e-10
                    ? clamp(2.0 * ((candles.get(i).close - lowest) / range) - 1.0, -0.999, 0.999)
                    : 0;
            // Smooth the value
            value = 0.33 * value + 0.67 * prevValue;
            value = clamp(value, -0.999, 0.999);

            // Fisher transform: arctanh approximation
            prevFisher = fisher;
            fisher = 0.5 * Math.log((1 + value) / (1 - value));
            prevValue = value;
        }

        return new FisherResult(fisher, prevFisher, prevValue);
    }

    /* ════════════════════════════════════════════════════════════════
       [v13.0 NEW] LINEAR REGRESSION CHANNEL
       Core tool for 8-candle-ahead forecasting.
       Slope tells you where price is headed; channel tells you extremes.
       ════════════════════════════════════════════════════════════════ */

    public static final class LinearRegressionResult {
        public final double slope;        // price change per bar (positive = up)
        public final double intercept;    // y-intercept
        public final double rSquared;     // 0..1, fit quality
        public final double stdDev;       // standard deviation of residuals
        public final double midline;      // LR value at last bar
        public final double upperChannel; // midline + 2*stdDev
        public final double lowerChannel; // midline - 2*stdDev
        public final double projectedPrice8; // projected price 8 bars ahead
        public final double slopeAngleDeg;   // slope as angle in degrees (±90)
        public final double normalizedSlope; // slope / ATR, regime-adjusted

        public LinearRegressionResult(double slope, double intercept, double rSq, double stdDev,
                                      double midline, double upperCh, double lowerCh,
                                      double proj8, double angleDeg, double normSlope) {
            this.slope = slope; this.intercept = intercept; this.rSquared = rSq; this.stdDev = stdDev;
            this.midline = midline; this.upperChannel = upperCh; this.lowerChannel = lowerCh;
            this.projectedPrice8 = proj8; this.slopeAngleDeg = angleDeg; this.normalizedSlope = normSlope;
        }

        /** Is price in upper half of channel (bearish pressure likely) */
        public boolean inUpperHalf()  { return midline > 0 && upperChannel > lowerChannel; }

        /** Trend is strong enough to follow */
        public boolean isTrendingUp()   { return slope > 0 && rSquared > 0.55 && normalizedSlope > 0.15; }
        public boolean isTrendingDown() { return slope < 0 && rSquared > 0.55 && normalizedSlope < -0.15; }

        /** Price is near channel extremes — potential mean reversion */
        public boolean nearUpperExtreme(double price) {
            return upperChannel > lowerChannel && price > lowerChannel + (upperChannel - lowerChannel) * 0.80;
        }
        public boolean nearLowerExtreme(double price) {
            return upperChannel > lowerChannel && price < lowerChannel + (upperChannel - lowerChannel) * 0.20;
        }
    }

    public static LinearRegressionResult linearRegression(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period)
            return new LinearRegressionResult(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        int n = candles.size();
        int start = n - period;
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;

        for (int i = 0; i < period; i++) {
            double x = i;
            double y = candles.get(start + i).close;
            sumX  += x; sumY  += y; sumXY += x * y; sumX2 += x * x;
        }

        double denom = period * sumX2 - sumX * sumX;
        if (Math.abs(denom) < 1e-12) {
            double price = candles.get(n - 1).close;
            return new LinearRegressionResult(0, price, 0, 0, price, price, price, price, 0, 0);
        }

        double slope     = (period * sumXY - sumX * sumY) / denom;
        double intercept = (sumY - slope * sumX) / period;

        // Residuals and R²
        double meanY = sumY / period;
        double ssTot = 0, ssRes = 0;
        double sumResidSq = 0;
        for (int i = 0; i < period; i++) {
            double actual = candles.get(start + i).close;
            double predicted = intercept + slope * i;
            double resid = actual - predicted;
            sumResidSq += resid * resid;
            ssTot += Math.pow(actual - meanY, 2);
            ssRes += Math.pow(resid, 2);
        }

        double rSquared = ssTot > 0 ? 1.0 - ssRes / ssTot : 0;
        double stdDev = Math.sqrt(sumResidSq / period);

        // Value at last bar
        double midline = intercept + slope * (period - 1);
        double upperCh = midline + 2 * stdDev;
        double lowerCh = midline - 2 * stdDev;

        // Project 8 bars ahead
        double proj8 = intercept + slope * (period - 1 + 8);

        // Slope angle in degrees
        double angleDeg = Math.toDegrees(Math.atan(slope / Math.max(1e-10, candles.get(n - 1).close / period)));

        // Normalized slope (slope per bar / current ATR)
        double currentAtr = atr(candles, 14);
        double normSlope = currentAtr > 0 ? slope / currentAtr : 0;

        return new LinearRegressionResult(slope, intercept, clamp(rSquared, 0, 1), stdDev,
                midline, upperCh, lowerCh, proj8, angleDeg, normSlope);
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
        Collections.reverse(Arrays.asList());  // keep sorted

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
        FisherResult fishR = fisherTransform(candles, 9);
        LinearRegressionResult lrr = linearRegression(candles, 34);
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
        else if (emaBullStack && rsiExtremeBull && (bbExtremeUpper || fishR.isOverbought())
                && (volumeDecreasing || atrContracting)) {
            phase = TrendPhase.EXHAUSTION_UP;
            exhaustionScore = 0.75 + (rsiNow - 75) / 100 * 0.5;
            continuationScore = 0.25;
            phaseConfidence = 0.70;
            evidence.add("RSI_EXTREME=" + String.format("%.0f", rsiNow));
            if (volumeDecreasing) evidence.add("VOL_DECLINING");
            if (fishR.isOverbought()) evidence.add("FISHER_OB=" + String.format("%.2f", fishR.fisher));
        }
        // --- EXHAUSTION DOWN ---
        else if (emaBearStack && rsiExtremeBear && (bbExtremeLower || fishR.isOversold())
                && (volumeDecreasing || atrContracting)) {
            phase = TrendPhase.EXHAUSTION_DOWN;
            exhaustionScore = 0.75 + (25 - rsiNow) / 100 * 0.5;
            continuationScore = 0.25;
            phaseConfidence = 0.70;
            evidence.add("RSI_EXTREME=" + String.format("%.0f", rsiNow));
            if (volumeDecreasing) evidence.add("VOL_DECLINING");
            if (fishR.isOversold()) evidence.add("FISHER_OS=" + String.format("%.2f", fishR.fisher));
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
            Deque<TradeResult> hist = symbolHistory.computeIfAbsent(symbol, k -> new ArrayDeque<>());
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
       [v14.0] FORECAST ENGINE — 9-Factor Direction Forecaster
       Прогнозирует направление на 4-8 свечей 15m вперёд.
       Был отсутствующим классом — BotMain ссылался на forecast поля
       которых не существовало. Встроен в TradingCore как inner class.
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
                return String.format("Forecast[%s dir=%.3f conf=%.0f%% phase=%s]",
                        bias, directionScore, confidence * 100, trendPhase);
            }
        }

        // Weights (sum = 1.0)
        private static final double W_LR=0.18, W_VP=0.12, W_TP=0.15, W_FI=0.10,
                W_AR=0.05, W_SS=0.15, W_MA=0.10, W_OF=0.08, W_MT=0.07;

        public ForecastResult forecast(List<Candle> c5, List<Candle> c15,
                                       List<Candle> c1h, double volumeDelta) {
            if (c15 == null || c15.size() < 100 || c1h == null || c1h.size() < 50) return null;
            double price = c15.get(c15.size()-1).close;
            double atr14 = fcAtr(c15, 14);
            if (atr14 <= 0 || price <= 0) return null;

            Map<String, Double> f = new LinkedHashMap<>();
            double lrS = calcLR(c15, 30, atr14); f.put("LR", lrS);
            double vpS = calcVP(c15, 50, price, atr14); f.put("VP", vpS);
            TrendPhase phase = detectPhase(c15, c1h);
            double tpS = calcPhaseScore(c15, phase); f.put("TP", tpS);
            double fiS = calcFisher(c15, 10); f.put("FI", fiS);
            double arS = calcAtrRegime(c15, atr14); f.put("AR", arS);
            double ssS = calcSwing(c15); f.put("SS", ssS);
            double maS = calcMomAccel(c15, atr14); f.put("MA", maS);
            double ofS = calcOrderflow(c15, volumeDelta); f.put("OF", ofS);
            double mtS = calcMTF(c5, c15, c1h); f.put("MT", mtS);

            double dir = lrS*W_LR + vpS*W_VP + tpS*W_TP + fiS*W_FI + arS*W_AR
                    + ssS*W_SS + maS*W_MA + ofS*W_OF + mtS*W_MT;
            dir = clamp(dir, -1.0, 1.0);

            int bull=0, bear=0;
            for (double v : f.values()) { if (v>0.15) bull++; if (v<-0.15) bear++; }
            double conf = clamp((double)Math.max(bull,bear)/f.size()*1.2 + Math.abs(dir)*0.3, 0.1, 0.95);

            ForecastBias bias = dir > 0.45 ? ForecastBias.STRONG_BULL
                    : dir > 0.15 ? ForecastBias.BULL : dir < -0.45 ? ForecastBias.STRONG_BEAR
                    : dir < -0.15 ? ForecastBias.BEAR : ForecastBias.NEUTRAL;

            double lrSlope = linRegSlope(c15, 30);
            double projMove = lrSlope * 8 / price;
            double vpoc = calcVPOC(c15, 50);

            return new ForecastResult(bias, dir, conf, phase, projMove, vpoc, f);
        }

        // ── Factor 1: Linear Regression ──────────────────────
        private double calcLR(List<Candle> c, int p, double atr) {
            double slope = linRegSlope(c, p);
            return clamp(slope * 8 / atr * 0.35, -1.0, 1.0);
        }
        private double linRegSlope(List<Candle> c, int p) {
            int n = c.size(); if (n < p) return 0;
            double sX=0, sY=0, sXY=0, sX2=0;
            for (int i=0; i<p; i++) {
                double y = c.get(n-p+i).close;
                sX+=i; sY+=y; sXY+=i*y; sX2+=i*i;
            }
            double d = p*sX2-sX*sX;
            return Math.abs(d)<1e-12 ? 0 : (p*sXY-sX*sY)/d;
        }

        // ── Factor 2: Volume Profile (VPOC) ─────────────────
        private double calcVP(List<Candle> c, int p, double price, double atr) {
            double vpoc = calcVPOC(c, p);
            return vpoc <= 0 ? 0 : clamp((vpoc-price)/atr*0.25, -0.6, 0.6);
        }
        private double calcVPOC(List<Candle> c, int p) {
            int n=c.size(), start=Math.max(0,n-p);
            double lo=Double.MAX_VALUE, hi=Double.MIN_VALUE;
            for (int i=start;i<n;i++) { lo=Math.min(lo,c.get(i).low); hi=Math.max(hi,c.get(i).high); }
            if (hi-lo<1e-12) return c.get(n-1).close;
            int bins=50; double bs=(hi-lo)/bins; double[] vb=new double[bins];
            for (int i=start;i<n;i++) {
                double tp=(c.get(i).high+c.get(i).low+c.get(i).close)/3.0;
                int b=Math.min(bins-1,Math.max(0,(int)((tp-lo)/bs)));
                vb[b]+=c.get(i).volume;
            }
            int mx=0; for(int i=1;i<bins;i++) if(vb[i]>vb[mx]) mx=i;
            return lo+(mx+0.5)*bs;
        }

        // ── Factor 3: Trend Phase ────────────────────────────
        public TrendPhase detectPhase(List<Candle> c15, List<Candle> c1h) {
            if (c15.size()<60) return TrendPhase.MID;
            double adxV=fcAdx(c15,14), rsiV=rsi(c15,14);
            double e9=fcEma(c15,9), e21=fcEma(c15,21), e50=fcEma(c15,50);
            boolean ordered=(e9>e21&&e21>e50)||(e9<e21&&e21<e50);
            double adxP=c15.size()>5?fcAdx(c15.subList(0,c15.size()-3),14):adxV;
            boolean rising=adxV>adxP*1.02, falling=adxV<adxP*0.97;
            if (adxV<20) return TrendPhase.EARLY;
            if (adxV>=20&&adxV<35&&rising&&ordered) return TrendPhase.MID;
            if ((adxV>=40&&falling)||rsiV>78||rsiV<22) return TrendPhase.EXHAUSTION;
            if (adxV>=35&&falling) return TrendPhase.LATE;
            if (rising&&ordered) return TrendPhase.MID;
            if (falling) return TrendPhase.LATE;
            return TrendPhase.MID;
        }
        private double calcPhaseScore(List<Candle> c, TrendPhase ph) {
            boolean bull=c.get(c.size()-1).close>fcEma(c,21);
            return switch(ph) {
                case EARLY -> bull?0.40:-0.40;
                case MID -> bull?0.25:-0.25;
                case LATE -> bull?0.05:-0.05;
                case EXHAUSTION -> bull?-0.30:0.30;
            };
        }

        // ── Factor 4: Fisher Transform ───────────────────────
        private double calcFisher(List<Candle> c, int p) {
            int n=c.size(); if(n<p+2) return 0;
            double hi=Double.MIN_VALUE,lo=Double.MAX_VALUE;
            for(int i=n-p;i<n;i++){hi=Math.max(hi,c.get(i).high);lo=Math.min(lo,c.get(i).low);}
            if(hi-lo<1e-12) return 0;
            double norm=clamp(2.0*((c.get(n-1).close-lo)/(hi-lo)-0.5)*0.999,-0.999,0.999);
            double fish=0.5*Math.log((1+norm)/(1-norm));
            double pNorm=clamp(2.0*((c.get(n-2).close-lo)/(hi-lo)-0.5)*0.999,-0.999,0.999);
            double pFish=0.5*Math.log((1+pNorm)/(1-pNorm));
            double s=0;
            if(fish>0&&pFish<0) s=0.5; if(fish<0&&pFish>0) s=-0.5;
            s+=clamp(fish*0.15,-0.3,0.3);
            return clamp(s,-1.0,1.0);
        }

        // ── Factor 5: ATR Regime ─────────────────────────────
        private double calcAtrRegime(List<Candle> c, double curAtr) {
            int n=c.size(), lb=Math.min(96,n-15); if(lb<20) return 0;
            List<Double> h=new ArrayList<>();
            for(int i=n-lb;i<n-1;i+=3){double a=fcAtr(c.subList(Math.max(0,i-14),i+1),Math.min(14,i));if(a>0)h.add(a);}
            if(h.size()<5) return 0;
            Collections.sort(h); double med=h.get(h.size()/2);
            if(med<=0) return 0; double r=curAtr/med;
            if(r>2.5) return -0.15; if(r>1.8) return -0.08;
            if(r<0.5) return 0.10; return 0;
        }

        // ── Factor 6: Swing Structure ────────────────────────
        private double calcSwing(List<Candle> c) {
            return com.bot.DecisionEngineMerged.marketStructure(c) * 0.55;
        }

        // ── Factor 7: Momentum Acceleration ──────────────────
        private double calcMomAccel(List<Candle> c, double atr) {
            int n=c.size(); if(n<20||atr<=0) return 0;
            double m1=c.get(n-6).close-c.get(n-11).close;
            double m2=c.get(n-1).close-c.get(n-6).close;
            return clamp((m2-m1)/atr*0.30,-0.7,0.7);
        }

        // ── Factor 8: Orderflow ──────────────────────────────
        private double calcOrderflow(List<Candle> c, double vd) {
            int n=c.size(); double s=0;
            if(n>=5){double tb=0,tv=0;for(int i=n-5;i<n;i++){tb+=c.get(i).takerBuyBaseVolume;tv+=c.get(i).volume;}
                if(tv>0)s+=(tb/tv-0.5)*2.0;}
            if(Math.abs(vd)>0.001) s=s*0.6+clamp(Math.signum(vd)*Math.min(1,Math.abs(vd)*3),-1,1)*0.4;
            return clamp(s*0.55,-0.7,0.7);
        }

        // ── Factor 9: Multi-TF Alignment ─────────────────────
        private double calcMTF(List<Candle> c5, List<Candle> c15, List<Candle> c1h) {
            int v=0;
            if(c15.size()>=25){if(fcEma(c15,9)>fcEma(c15,21))v++;else v--;}
            if(c1h.size()>=25){if(fcEma(c1h,9)>fcEma(c1h,21))v++;else v--;}
            if(c5!=null&&c5.size()>=8){if(c5.get(c5.size()-1).close>c5.get(c5.size()-7).close)v++;else v--;}
            return switch(v){case 3->0.65;case 2->0.35;case -2->-0.35;case -3->-0.65;default->v*0.10;};
        }

        // ── Internal math ────────────────────────────────────
        private double fcAtr(List<Candle> c, int n) {
            int p=Math.min(n,c.size()-1); if(p<=0)return 0; double s=0;
            for(int i=c.size()-p;i<c.size();i++){Candle cu=c.get(i),pr=c.get(i-1);
                s+=Math.max(cu.high-cu.low,Math.max(Math.abs(cu.high-pr.close),Math.abs(cu.low-pr.close)));}
            return s/p;
        }
        private double fcEma(List<Candle> c,int p){
            if(c.size()<p)return c.get(c.size()-1).close;
            double k=2.0/(p+1),e=c.get(c.size()-p).close;
            for(int i=c.size()-p+1;i<c.size();i++)e=c.get(i).close*k+e*(1-k); return e;
        }
        private double fcAdx(List<Candle> c,int p){
            if(c.size()<p*2+1)return 15; int si=Math.max(1,c.size()-p*2);
            double sPDM=0,sMDM=0,sTR=0; int se=si+p;
            for(int i=si;i<se&&i<c.size();i++){Candle cu=c.get(i),pr=c.get(i-1);
                double hd=cu.high-pr.high,ld=pr.low-cu.low;
                double tr=Math.max(cu.high-cu.low,Math.max(Math.abs(cu.high-pr.close),Math.abs(cu.low-pr.close)));
                sTR+=tr;if(hd>ld&&hd>0)sPDM+=hd;if(ld>hd&&ld>0)sMDM+=ld;}
            double sumDX=0;int dc=0;
            for(int i=se;i<c.size();i++){Candle cu=c.get(i),pr=c.get(i-1);
                double hd=cu.high-pr.high,ld=pr.low-cu.low;
                double tr=Math.max(cu.high-cu.low,Math.max(Math.abs(cu.high-pr.close),Math.abs(cu.low-pr.close)));
                sTR=sTR-sTR/p+tr;sPDM=sPDM-sPDM/p+((hd>ld&&hd>0)?hd:0);sMDM=sMDM-sMDM/p+((ld>hd&&ld>0)?ld:0);
                double pDI=sTR>0?100*sPDM/sTR:0,mDI=sTR>0?100*sMDM/sTR:0,ds=pDI+mDI;
                sumDX+=ds>0?100*Math.abs(pDI-mDI)/ds:0;dc++;}
            return dc>0?sumDX/dc:15;
        }
    } // end ForecastEngine
}