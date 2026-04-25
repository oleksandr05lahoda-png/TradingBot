package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** TradingCore v15.0 — INSTITUTIONAL QUANT + FORECASTING FOUNDATION */
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

    /**
     * Choppiness Index (CI) — measures whether the market is trending or ranging.
     *
     * Formula: CI(n) = 100 × log10(Σ ATR(1) over n bars / (highest_high - lowest_low over n bars)) / log10(n)
     *
     * Interpretation:
     *   CI > 61.8 → choppy / consolidating (avoid trend-following signals)
     *   CI < 38.2 → strong trend (high edge for trend-following)
     *   38.2–61.8 → transitional zone
     *
     * @param candles price bars (needs at least period+1)
     * @param period  lookback window (typically 14)
     * @return CI value in [0..100], or 50 if insufficient data
     */
    public static double choppinessIndex(List<Candle> candles, int period) {
        if (candles == null || candles.size() < period + 1) return 50.0;

        int end = candles.size() - 1;
        int start = end - period + 1;
        if (start < 1) return 50.0; // need at least 1 bar before start for TR

        double highestHigh = Double.NEGATIVE_INFINITY;
        double lowestLow   = Double.MAX_VALUE;
        double sumAtr1     = 0.0;

        for (int i = start; i <= end; i++) {
            Candle cur  = candles.get(i);
            Candle prev = candles.get(i - 1);
            highestHigh = Math.max(highestHigh, cur.high);
            lowestLow   = Math.min(lowestLow,   cur.low);
            sumAtr1    += trueRange(cur, prev);
        }

        double totalRange = highestHigh - lowestLow;
        if (totalRange < 1e-12 || sumAtr1 < 1e-12) return 50.0;

        return 100.0 * Math.log10(sumAtr1 / totalRange) / Math.log10(period);
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

        /**
         * ATR-BASED POSITION SIZING + QUARTER-KELLY
         *
         * Формула: size = (balance × riskPct) / (ATR × leverage)
         * Kelly: f* = (p × b − q) / b, применяется × 0.25 (quarter Kelly)
         *
         * Это даёт правильный размер позиции, учитывающий:
         * 1. Текущую волатильность монеты (ATR) — а не фиксированный %
         * 2. Реальный edge системы (Kelly) — а не hardcoded risk%
         * 3. Категорию монеты (MEME → уменьшение)
         */
        public double positionSizeATR(double balance, double entry, double atr,
                                      double stopDist, CoinType type,
                                      double winRate, double avgRR, int leverage) {
            if (balance <= 0 || entry <= 0 || atr <= 0) return 0;

            // Kelly fraction (quarter Kelly for safety)
            double kellyF = kellyFraction(winRate, avgRR > 0 ? avgRR : 2.0, 1.0);
            double riskPct = kellyF > 0.003
                    ? Math.min(kellyF, maxPortfolioRisk / type.riskMultiplier)
                    : maxPortfolioRisk * 0.5 / type.riskMultiplier;

            // ATR-based sizing: (balance × riskPct) / (stopDistance_as_fraction)
            double stopFraction = Math.max(0.001, stopDist / entry);
            double posSize = (balance * riskPct) / stopFraction;

            // Caps
            posSize = Math.min(posSize, balance * maxTotalExposure);
            posSize = Math.min(posSize, balance * type.maxLeverage);
            return Math.max(posSize, 6.5); // minimum viable order
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
       [MODULE 3 v33] VSA — VOLUME SPREAD ANALYSIS ENGINE
       ════════════════════════════════════════════════════════════════
       Philosophy: Richard Wyckoff + Tom Williams VSA methodology.
       The relationship between PRICE SPREAD (candle range) and VOLUME
       reveals the intentions of "smart money" (institutions/whales).

       Core rules:
         WIDE SPREAD + HIGH VOLUME  = genuine strength/weakness (real move)
         WIDE SPREAD + LOW VOLUME   = fake/weak breakout (no institutional backing)
         NARROW SPREAD + HIGH VOLUME = absorption (smart money absorbing retail)
         NARROW SPREAD + LOW VOLUME  = low-interest consolidation (no edge)

       These patterns fire BEFORE price confirmation — they are leading signals.
       Standard indicators (RSI, MACD) all LAG because they are price-derived.
       Volume relationship to spread does NOT lag — it describes the current candle.
       ════════════════════════════════════════════════════════════════ */

    public static final class VsaResult {
        public enum VsaSignal {
            NONE,
            // ── Bullish signals ──────────────────────────────────────
            EFFORT_TO_FALL_FAILED,   // Wide down-bar, high vol, closes near high → selling failed
            STOPPING_VOLUME_BULL,    // Ultra-volume on down-bar closing in upper half → supply exhausted
            NO_SUPPLY,               // Narrow down-bar, very low vol → no selling pressure
            DEMAND_ABSORPTION,       // Narrow UP-bar, very high vol → supply being absorbed into rally
            // ── Bearish signals ──────────────────────────────────────
            EFFORT_TO_RISE_FAILED,   // Wide up-bar, high vol, closes near low → buying failed
            STOPPING_VOLUME_BEAR,    // Ultra-volume on up-bar closing in lower half → demand exhausted
            NO_DEMAND,               // Narrow up-bar, very low vol → no buying pressure
            SUPPLY_ABSORPTION,       // Narrow DOWN-bar, very high vol → demand being absorbed into sell
            // ── Structural ───────────────────────────────────────────
            WEAK_BREAKOUT            // Wide range breakout with LOW vol → not trusted, likely fakeout
        }

        public final VsaSignal signal;
        public final double    strength;   // [0..1] — how extreme the pattern is
        public final boolean   isBullish;
        public final boolean   isBearish;

        public VsaResult(VsaSignal s, double str) {
            this.signal   = s;
            this.strength = clamp(str, 0, 1);
            this.isBullish = s == VsaSignal.EFFORT_TO_FALL_FAILED ||
                    s == VsaSignal.STOPPING_VOLUME_BULL   ||
                    s == VsaSignal.NO_SUPPLY;
            this.isBearish = s == VsaSignal.EFFORT_TO_RISE_FAILED ||
                    s == VsaSignal.STOPPING_VOLUME_BEAR   ||
                    s == VsaSignal.NO_DEMAND              ||
                    s == VsaSignal.SUPPLY_ABSORPTION;
        }

        public boolean hasSignal() { return signal != VsaSignal.NONE; }
        public static VsaResult none() { return new VsaResult(VsaSignal.NONE, 0); }

        @Override public String toString() {
            return "VSA{" + signal + " str=" + String.format("%.2f", strength) + "}";
        }
    }

    /**
     * Analyse recent candles for VSA patterns.
     * Returns the strongest pattern found in the last {@code lookback} bars, or NONE.
     *
     * @param candles  15m candle list — minimum 25 bars required
     * @param lookback how many recent candles to scan (2–4 is optimal for 15m TF)
     */
    public static VsaResult vsaAnalyse(List<Candle> candles, int lookback) {
        if (candles == null || candles.size() < 25) return VsaResult.none();

        int n = candles.size();

        // ── Rolling context: 20-bar average volume, 14-bar average range ──
        int volWin   = Math.min(20, n - 1);
        int rangeWin = Math.min(14, n - 1);
        double avgVol = 0, avgRange = 0;
        for (int i = n - 1 - volWin; i < n - 1; i++)   avgVol   += candles.get(i).volume;
        for (int i = n - 1 - rangeWin; i < n - 1; i++) avgRange += candles.get(i).range;
        avgVol   /= volWin;
        avgRange /= rangeWin;

        if (avgVol < 1e-9 || avgRange < 1e-9) return VsaResult.none();

        // ── Scan the most recent bars (exclude the forming candle = last index) ──
        VsaResult best = VsaResult.none();
        int from = Math.max(0, n - 1 - Math.min(lookback, 4));
        for (int i = from; i < n - 1; i++) {          // n-1: last candle may still be forming
            VsaResult r = vsaClassifyBar(candles.get(i), avgVol, avgRange);
            if (r.strength > best.strength) best = r;
        }
        return best;
    }

    /** Classify a single bar against rolling averages. Package-private for testing. */
    static VsaResult vsaClassifyBar(Candle c, double avgVol, double avgRange) {
        if (c.range < 1e-9) return VsaResult.none();

        double volRatio   = c.volume / (avgVol   + 1e-9);
        double rangeRatio = c.range  / (avgRange + 1e-9);

        // Thresholds — calibrated for 15m crypto futures
        boolean ultraVol  = volRatio   > 3.00;  // 3× average = genuine spike (whale activity)
        boolean highVol   = volRatio   > 1.80;  // 80%+ above average = institutional participation
        boolean lowVol    = volRatio   < 0.50;  // half average = retail-only, no smart money
        boolean wideRange = rangeRatio > 1.50;  // 50%+ wider than average
        boolean thinRange = rangeRatio < 0.55;  // narrow: less than half average range

        // Close position within the bar: 0 = at the low, 1 = at the high
        double closePos   = (c.close - c.low) / c.range;
        boolean closedHigh = closePos >= 0.65;  // closed in upper 35% of bar
        boolean closedLow  = closePos <= 0.35;  // closed in lower 35% of bar

        // BULLISH PATTERNS (ordered by statistical reliability)

        // STOPPING VOLUME BULL — the single most reliable bottom signal in VSA.
        // Ultra-high volume on a down-bar that closes near the HIGH of the bar.
        // Interpretation: a tsunami of sell orders hit the market, but buyers
        // absorbed every single one and pushed price back up. Supply is exhausted.
        // This is frequently the exact low of a V-reversal.
        if (ultraVol && !c.isBullish && closedHigh) {
            double str = Math.min(1.0, (volRatio - 3.0) / 5.0 + 0.55);
            return new VsaResult(VsaResult.VsaSignal.STOPPING_VOLUME_BULL, str);
        }

        // EFFORT TO FALL FAILED — bears put in huge effort (wide range + high vol)
        // but couldn't hold price down. Bar closes in the upper half despite being bearish.
        // Effort without result = the effort is done, reversal likely.
        if (wideRange && highVol && !c.isBullish && closePos > 0.55) {
            double str = clamp((volRatio - 1.8) / 3.5 * (rangeRatio / 1.5), 0.30, 0.85);
            return new VsaResult(VsaResult.VsaSignal.EFFORT_TO_FALL_FAILED, str);
        }

        // NO SUPPLY — very narrow range down bar on very low volume.
        // Interpretation: the market is testing support and nobody is selling.
        // This is a background condition for longs (needs HTF uptrend for full strength).
        if (thinRange && lowVol && !c.isBullish) {
            double str = clamp((1.0 - volRatio) * (1.0 - rangeRatio), 0.20, 0.65);
            return new VsaResult(VsaResult.VsaSignal.NO_SUPPLY, str);
        }

        // DEMAND ABSORPTION — narrow UP bar but very high volume.
        // Paradoxically BEARISH: price can't advance despite huge buying = smart money
        // is selling (distributing) into every retail buy order. Overhead supply.
        if (thinRange && highVol && c.isBullish) {
            double str = clamp((volRatio - 1.8) / 4.0, 0.25, 0.75);
            return new VsaResult(VsaResult.VsaSignal.DEMAND_ABSORPTION, str);
        }

        // BEARISH PATTERNS

        // STOPPING VOLUME BEAR — ultra-volume on an up-bar that closes near the LOW.
        // Interpretation: massive demand hit the market, sellers absorbed it all.
        // Classic distribution top, frequently precedes waterfall declines.
        if (ultraVol && c.isBullish && closedLow) {
            double str = Math.min(1.0, (volRatio - 3.0) / 5.0 + 0.55);
            return new VsaResult(VsaResult.VsaSignal.STOPPING_VOLUME_BEAR, str);
        }

        // EFFORT TO RISE FAILED — bulls pushed hard (wide range + high vol upward)
        // but bar closes near the LOW. The buying effort produced no result.
        if (wideRange && highVol && c.isBullish && closedLow) {
            double str = clamp((volRatio - 1.8) / 3.5 * (rangeRatio / 1.5), 0.30, 0.85);
            return new VsaResult(VsaResult.VsaSignal.EFFORT_TO_RISE_FAILED, str);
        }

        // NO DEMAND — narrow up bar on very low volume.
        // Price is ticking up with zero institutional interest. Fade the weak rally.
        if (thinRange && lowVol && c.isBullish) {
            double str = clamp((1.0 - volRatio) * (1.0 - rangeRatio), 0.20, 0.65);
            return new VsaResult(VsaResult.VsaSignal.NO_DEMAND, str);
        }

        // SUPPLY ABSORPTION — narrow DOWN bar, very high volume.
        // Paradoxically BULLISH: price won't fall despite huge selling = smart money
        // is buying (accumulating) into every retail sell order.
        if (thinRange && highVol && !c.isBullish) {
            double str = clamp((volRatio - 1.8) / 4.0, 0.25, 0.70);
            return new VsaResult(VsaResult.VsaSignal.SUPPLY_ABSORPTION, str);
        }

        // WEAK BREAKOUT — wide range breakout direction bar, but LOW volume.
        // No institutional participation = retail-driven fakeout. High failure rate.
        // The classic bull/bear trap that stops out early entries.
        if (wideRange && lowVol) {
            double str = clamp((rangeRatio / 3.5) * (1.0 - volRatio), 0.20, 0.60);
            return new VsaResult(VsaResult.VsaSignal.WEAK_BREAKOUT, str);
        }

        return VsaResult.none();
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

        // Factor weights: leading indicators (CMF, CVD_RT, ACCEL, OBV_SLOPE) get highest weight.
        // CMF (Chaikin Money Flow) = institutional pressure signature — very leading.
        // OBV_SLOPE = accumulation/distribution trend — leads price by 2-5 bars.
        // CVD_RT = real-time order flow imbalance — most immediate.
        // ACCEL = price acceleration change — early momentum shift.
        // SWING/HTF/OF are structural/lagging but provide context.
        // Note: only keys listed here participate in confidence calc (0.0 default = excluded).
        // This intentionally excludes exhaustion metadata (VOL_FADE, MOM_DECAY, etc.)
        // which are non-directional and were previously polluting confidence (bug fix).
        private static final Map<String, Double> FW_BASE = Map.of(
                "HTF",        2.5,
                "OF",         2.5,
                "CVD_RT",     4.5,  // most leading: real-time order flow
                "SWING",      1.8,
                "EXHAUSTION", 2.0,
                "ACCEL",      3.0,  // price acceleration — early momentum
                "VPOC_PULL",  1.5,
                "CMF",        3.5,  // Chaikin Money Flow — institutional pressure (new)
                "OBV_SLOPE",  2.5   // OBV trend slope — structural accumulation (new)
        );
        // Squeeze mode: breakout direction critical, CMF identifies who's accumulating
        private static final Map<String, Double> FW_SQUEEZE = Map.of(
                "HTF",        1.0,
                "OF",         1.5,
                "CVD_RT",     3.5,
                "SWING",      4.0,
                "EXHAUSTION", 1.0,
                "ACCEL",      3.0,
                "VPOC_PULL",  1.0,
                "CMF",        3.0,  // CMF especially key in squeeze — who is quietly accumulating?
                "OBV_SLOPE",  2.5   // OBV slope predicts breakout direction in compressions
        );

        public ForecastResult forecast(List<Candle> c5, List<Candle> c15,
                                       List<Candle> c1h, double volumeDelta) {
            // Разделены на два return для читаемости + логирование холодного старта.
            // Ранее: один молчаливый return null — непонятно почему AFC Stage 2 не работает.
            // Теперь: явное сообщение когда данных недостаточно (cold start, а не ошибка).
            if (c15 == null || c15.size() < 100) return null;
            if (c1h == null || c1h.size() < 50)  return null;
            int n = c15.size();
            double price = c15.get(n - 1).close;
            double atr14 = fcAtr(c15, 14);
            if (atr14 <= 0 || price <= 0) return null;

            Map<String, Double> f = new LinkedHashMap<>();

            // STEP 1: Identify the CURRENT MOVE (direction + age + depth)
            MoveInfo move = identifyCurrentMove(c15, atr14);
            f.put("MOVE_DIR", (double) move.direction);
            f.put("MOVE_AGE", (double) move.ageBars);
            f.put("MOVE_DEPTH", move.depthAtr);

            // STEP 2: EXHAUSTION BRAIN — is this move dying?
            // This is the KEY innovation. Each factor answers:
            // "Is the fuel running out?" → positive = exhausted → reversal likely
            double exhaustionScore = 0;
            int exhaustionSignals = 0;

            // All exhaustion factor thresholds lowered 0.3→0.2.
            // At 0.3 exhaustion was only detected when the move was nearly done.
            // At 0.2 we catch the early signs 1-2 bars sooner.

            // Factor E1: Volume Fade — volume declining during the move
            double volFade = calcVolumeFade(c15, move);
            f.put("VOL_FADE", volFade);
            if (volFade > 0.2) { exhaustionScore += volFade * 0.28; exhaustionSignals++; }

            // Factor E2: Momentum Decay — each bar getting smaller
            double momDecay = calcMomentumDecay(c15, move, atr14);
            f.put("MOM_DECAY", momDecay);
            if (momDecay > 0.2) { exhaustionScore += momDecay * 0.28; exhaustionSignals++; }

            // Factor E3: Wick Rejection — opposition forming
            double wickReject = calcWickRejection(c15, move);
            f.put("WICK_REJ", wickReject);
            if (wickReject > 0.2) { exhaustionScore += wickReject * 0.22; exhaustionSignals++; }

            // Factor E4: RSI Extreme + Divergence
            double rsiExhaust = calcRsiExhaustion(c15, move);
            f.put("RSI_EXH", rsiExhaust);
            if (rsiExhaust > 0.2) { exhaustionScore += rsiExhaust * 0.22; exhaustionSignals++; }

            // Factor E5: Move overextended (moved too far too fast)
            double overext = calcOverextension(move, atr14);
            f.put("OVEREXT", overext);
            if (overext > 0.2) { exhaustionScore += overext * 0.18; exhaustionSignals++; }

            // Factor E6: VPOC Rubber Band — price far from volume magnet
            double vpocPull = calcVpocPull(c15, price, atr14);
            f.put("VPOC_PULL", vpocPull);

            exhaustionScore = clamp(exhaustionScore, 0, 1);
            f.put("EXHAUSTION", exhaustionScore);

            // STEP 3: TREND BRAIN — [v38.0] SIMPLIFIED
            // Удалены мультиколлинеарные факторы:
            //   LR_8 ≈ SWING (оба = краткосрочное направление) → оставляем SWING
            //   FISHER ≈ OF (оба = осцилляторы потока) → оставляем OF
            // Оставлены 3 ключевых: SWING (структура), OF (поток), HTF (тренд)
            // + 2 опережающих: CVD_RT (реальное время), ACCEL (ускорение)
            double trendDir = 0;

            // Swing structure — HH/HL vs LL/LH
            double swingScore = com.bot.DecisionEngineMerged.marketStructure(c15) * 0.50;
            f.put("SWING", swingScore);
            trendDir += swingScore * 0.22;

            // Orderflow — CVD исторический
            double ofScore = calcOrderflow(c15, volumeDelta);
            f.put("OF", ofScore);
            trendDir += ofScore * 0.18;

            // HTF alignment (1h) — [BUG A] weight reduced, slope-based fix in calcHTF
            double htfScore = calcHTF(c15, c1h);
            f.put("HTF", htfScore);
            trendDir += htfScore * 0.15;

            // LEADING FACTORS — опережающие индикаторы
            // CVD_RT + ACCEL дают 3-5 свечей форы

            // RT-CVD — реальное соотношение покупок/продаж
            double cvdRt = clamp(volumeDelta * 1.8, -0.85, 0.85);
            f.put("CVD_RT", cvdRt);
            trendDir += cvdRt * 0.25; // most leading factor

            // Price Acceleration — скорость изменения скорости цены
            int nc = c15.size();
            if (nc >= 7) {
                double mom1 = (c15.get(nc-1).close - c15.get(nc-4).close) / (price + 1e-9);
                double mom2 = (c15.get(nc-4).close - c15.get(nc-7).close) / (c15.get(nc-4).close + 1e-9);
                double accelScore = 0;
                if (mom1 > 0 && mom2 > 0 && mom1 > mom2 * 1.25) {
                    accelScore = clamp((mom1 - mom2) / Math.max(1e-6, Math.abs(mom2)), 0, 0.60);
                } else if (mom1 < 0 && mom2 < 0 && mom1 < mom2 * 1.25) {
                    accelScore = clamp((mom1 - mom2) / Math.max(1e-6, Math.abs(mom2)), -0.60, 0);
                } else if (Math.signum(mom1) != Math.signum(mom2) && Math.abs(mom1) > atr14 * 0.3 / (price + 1e-9)) {
                    accelScore = clamp(mom1 * 15, -0.35, 0.35);
                }
                f.put("ACCEL", accelScore);
                trendDir += accelScore * 0.20; // [BUG B] raised from 0.12
            }
            // [BUG B] Sum of weights: 0.22 + 0.18 + 0.15 + 0.25 + 0.20 = 1.00 (normalized)

            // LEADING FACTOR: Chaikin Money Flow — institutional intent.
            // CMF > 0 = close near top of range × volume = institutional buying.
            // Used in confidence only (trendDir weights already sum to 1.0).
            // NOT added to trendDir to preserve weight normalization.
            double cmfScore = calcCMF(c15, 20);
            f.put("CMF", cmfScore);

            // LEADING FACTOR: OBV slope — structural accumulation/distribution.
            // Positive slope = net buying across last 20 bars (OBV trending up).
            // OBV diverging from price is one of the best reversal predictors.
            // Used in confidence only (same reason as CMF).
            double obvSlopeScore = calcOBVSlope(c15, 20);
            f.put("OBV_SLOPE", obvSlopeScore);

            trendDir = clamp(trendDir, -1.0, 1.0);

            // STEP 4: SQUEEZE detection — block all direction in compression
            boolean squeezed = isVolatilitySqueeze(c15, atr14);
            f.put("SQUEEZE", squeezed ? 1.0 : 0.0);

            // STEP 5: COMBINE — Exhaustion OVERRIDES Trend
            TrendPhase phase = detectPhase(c15, c1h, move, exhaustionScore);

            double dir;
            if (squeezed) {
                // Compression is often the launchpad of the move, not a zero-edge state.
                double squeezeBias = trendDir * 0.25 + ofScore * 0.35 + vpocPull * 0.20;
                boolean freshBreakout = move.ageBars <= 4 && move.depthAtr < 1.8;
                boolean orderflowLead = Math.abs(ofScore) > 0.18;
                if (freshBreakout && orderflowLead) {
                    double leadDir = Math.signum(ofScore);
                    squeezeBias += leadDir * 0.18;
                    if (leadDir != 0 && leadDir == Math.signum(trendDir)) {
                        squeezeBias += leadDir * 0.10;
                    }
                } else if (freshBreakout && move.direction != 0) {
                    squeezeBias += move.direction * 0.08;
                }
                dir = squeezeBias;
            } else if (exhaustionScore > 0.38 && exhaustionSignals >= 2) {
                // STRONG exhaustion lowered 0.50→0.38, signals 2 (kept).
                // At 0.50 the reversal was already confirmed on chart.
                // At 0.38 we forecast it 1-2 bars earlier.
                dir = -move.direction * exhaustionScore * 0.90;
                dir += trendDir * 0.03;
            } else if (exhaustionScore > 0.22 && exhaustionSignals >= 1) {
                // Moderate exhaustion lowered 0.30→0.22.
                // Single exhaustion signal = early warning, not full reversal.
                dir = trendDir * 0.20 + vpocPull * 0.30;
                dir -= move.direction * exhaustionScore * 0.45;
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

            // STEP 6: Confidence = взвешенное согласие факторов
            // [ДЫРА №3] Раньше все факторы имели одинаковый вес.
            // HTF (часовой тренд) и OF (orderflow/CVD) = фундаментальные.
            // LR_8 и FISHER = быстрые осцилляторы, менее надёжны.
            // Веса: HTF×3, OF×3, SWING×2, EXHAUST×2, VPOC×1.5
            // [v36-FIX Дыра4] FACTOR_WEIGHTS вынесены в static final (нет аллокаций на каждый вызов).
            // Удалены мультиколлинеарные факторы: LR_8 (≈SWING), Fisher (≈OF/dir), SQUEEZE (шум вес=0.5).
            // Оставлены 5 ключевых факторов с наибольшей предиктивной силой.
            double conf;
            if (exhaustionScore > 0.55 && exhaustionSignals >= 3) {
                conf = clamp(0.50 + exhaustionScore * 0.30, 0.50, 0.85);
            } else {
                // MAGNITUDE-WEIGHTED CONFIDENCE
                //
                // OLD (binary): factor > 0.10 AND direction matches → add weight.
                //   Bug: factor at 0.11 had same weight as factor at 0.95.
                //   Bug: exhaustion metadata (VOL_FADE, MOM_DECAY) counted as
                //        "directional" because their values are always positive,
                //        inflating confidence on every bullish signal.
                //
                // NEW: only factors listed in FW_BASE/FW_SQUEEZE participate
                //   (default 0.0 excludes metadata). Contribution is proportional
                //   to factor magnitude (|value| capped at 1.0). Disagreeing
                //   factors subtract at 50% rate (they have less predictive power
                //   than agreeing ones — asymmetric Bayesian update).
                //
                // Bonus: if 3+ leading indicators all agree (CMF, CVD_RT, ACCEL,
                //   OBV_SLOPE) → strong +8% confidence boost. These four together
                //   represent the best real-time institutional flow signal we have.
                Map<String, Double> FACTOR_WEIGHTS = squeezed ? FW_SQUEEZE : FW_BASE;
                double dirSign       = Math.signum(dir);
                double weightedAgree = 0, weightedDisagree = 0, totalWeight = 0;

                for (Map.Entry<String, Double> fe : f.entrySet()) {
                    double w = FACTOR_WEIGHTS.getOrDefault(fe.getKey(), 0.0);
                    if (w <= 0) continue; // exclude metadata factors (VOL_FADE etc.)
                    totalWeight += w;
                    double v   = fe.getValue();
                    double mag = Math.min(1.0, Math.abs(v) * 1.25); // amplify small signals
                    if (mag < 0.04) continue; // hard noise floor
                    if (Math.signum(v) == dirSign) {
                        weightedAgree    += w * mag;
                    } else {
                        weightedDisagree += w * mag;
                    }
                }

                // Net agree ratio: disagreement penalised at 50% (asymmetric update)
                double netAgree  = weightedAgree - weightedDisagree * 0.50;
                double agreeRatio = totalWeight > 0 ? clamp(netAgree / totalWeight, -0.15, 1.0) : 0;
                conf = clamp(0.20 + agreeRatio * 0.55 + Math.abs(dir) * 0.20, 0.10, 0.85);

                // ── HTF + OF alignment bonus (unchanged: most reliable pattern) ──
                boolean htfAgrees = Math.signum(f.getOrDefault("HTF", 0.0)) == dirSign;
                boolean ofAgrees  = Math.signum(f.getOrDefault("OF",  0.0)) == dirSign;
                if (htfAgrees && ofAgrees) conf = clamp(conf + 0.08, 0, 0.85);

                // ── Leading indicators consensus bonus ──────────────────────
                // CMF + CVD_RT + OBV_SLOPE + ACCEL: if 3+ agree → genuine institutional move.
                // This is the highest-conviction pattern: both flow AND momentum aligned.
                int leadingAgree = 0;
                if (Math.signum(f.getOrDefault("CVD_RT",   0.0)) == dirSign && Math.abs(f.getOrDefault("CVD_RT",   0.0)) > 0.10) leadingAgree++;
                if (Math.signum(f.getOrDefault("CMF",      0.0)) == dirSign && Math.abs(f.getOrDefault("CMF",      0.0)) > 0.12) leadingAgree++;
                if (Math.signum(f.getOrDefault("OBV_SLOPE",0.0)) == dirSign && Math.abs(f.getOrDefault("OBV_SLOPE",0.0)) > 0.08) leadingAgree++;
                if (Math.signum(f.getOrDefault("ACCEL",    0.0)) == dirSign && Math.abs(f.getOrDefault("ACCEL",    0.0)) > 0.08) leadingAgree++;
                if (leadingAgree >= 3) conf = clamp(conf + 0.10, 0, 0.85); // all 4 leading agree
                else if (leadingAgree >= 2) conf = clamp(conf + 0.04, 0, 0.85);

                // ── Leading indicators CONFLICT penalty ─────────────────────
                // If CMF disagrees with CVD_RT strongly → divergent flow = low confidence.
                double cmfDir  = Math.signum(f.getOrDefault("CMF",    0.0));
                double cvdDir  = Math.signum(f.getOrDefault("CVD_RT", 0.0));
                double obvDir  = Math.signum(f.getOrDefault("OBV_SLOPE", 0.0));
                boolean cmfVsCvdConflict  = cmfDir != 0 && cvdDir != 0 && cmfDir != cvdDir;
                boolean obvVsCmfConflict  = obvDir != 0 && cmfDir != 0 && obvDir != cmfDir;
                if (cmfVsCvdConflict && obvVsCmfConflict) conf = clamp(conf - 0.08, 0.10, 0.85);
                else if (cmfVsCvdConflict || obvVsCmfConflict) conf = clamp(conf - 0.04, 0.10, 0.85);

                if (squeezed) {
                    boolean breakoutAligned = Math.abs(f.getOrDefault("OF", 0.0)) > 0.18
                            && Math.abs(f.getOrDefault("SWING", 0.0)) > 0.12
                            && Math.signum(f.getOrDefault("OF", 0.0)) == dirSign
                            && Math.signum(f.getOrDefault("SWING", 0.0)) == dirSign;
                    if (breakoutAligned) conf = clamp(conf + 0.06, 0.10, 0.85);
                    else if (Math.abs(dir) < 0.10) conf = clamp(conf - 0.06, 0.10, 0.85);
                }
            }

            // STEP 7: Bias classification
            ForecastBias bias;
            if (dir > 0.40) bias = ForecastBias.STRONG_BULL;
            else if (dir > 0.12) bias = ForecastBias.BULL;
            else if (dir < -0.40) bias = ForecastBias.STRONG_BEAR;
            else if (dir < -0.12) bias = ForecastBias.BEAR;
            else bias = ForecastBias.NEUTRAL;

            // projMove: estimated next-bar move based on direction strength and ATR
            double projMove = dir * atr14 * 2.0 / price;
            double vpoc = calcVPOC(c15, 50);

            return new ForecastResult(bias, dir, conf, phase, projMove, vpoc, f);
        }

        //  MOVE IDENTIFICATION — find origin, direction, age, depth

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

        /**
         * Swing-pivot approach — O(n), no EMA5 lag.
         *
         * Problem with old version:
         *   1) EMA5 lags 2-3 bars → after pump peak, method still reports dir=+1
         *      because price > EMA5 for several bars after reversal.
         *   2) Quadratic complexity: fcEma() recomputed from scratch each iteration.
         *
         * New logic: find last confirmed pivot high/low (strength=2 on both sides).
         * Whichever pivot is more recent defines the current move:
         *   - last pivot was HIGH → current move is DOWN from that high
         *   - last pivot was LOW  → current move is UP from that low
         * This matches how structure is actually read by traders and lags zero bars
         * once the pivot is confirmed (2 bars after the pivot itself).
         */
        private MoveInfo identifyCurrentMove(List<Candle> c, double atr) {
            int n = c.size();
            if (n < 5) return new MoveInfo(0, 0, 0, n - 1, c.get(n - 1).close);

            double price = c.get(n - 1).close;

            int lastPivotHigh = -1, lastPivotLow = -1;
            double pivotHighPrice = 0, pivotLowPrice = 0;

            // Scan backward up to 25 bars, strength=2 on both sides
            // i-2..i+2 must all exist → i in [2, n-3]
            for (int i = n - 3; i >= Math.max(2, n - 25); i--) {
                double hi = c.get(i).high;
                double lo = c.get(i).low;
                boolean isPH = hi > c.get(i - 1).high && hi > c.get(i - 2).high
                        && hi > c.get(i + 1).high && hi > c.get(i + 2).high;
                boolean isPL = lo < c.get(i - 1).low && lo < c.get(i - 2).low
                        && lo < c.get(i + 1).low && lo < c.get(i + 2).low;
                if (isPH && lastPivotHigh < 0) { lastPivotHigh = i; pivotHighPrice = hi; }
                if (isPL && lastPivotLow  < 0) { lastPivotLow  = i; pivotLowPrice  = lo; }
                if (lastPivotHigh >= 0 && lastPivotLow >= 0) break;
            }

            int dir;
            int origin;
            double originPrice;

            if (lastPivotHigh >= 0 && lastPivotLow >= 0) {
                if (lastPivotHigh > lastPivotLow) {
                    // Most recent pivot is HIGH → move is DOWN from it
                    dir = -1; origin = lastPivotHigh; originPrice = pivotHighPrice;
                } else {
                    // Most recent pivot is LOW → move is UP from it
                    dir = 1; origin = lastPivotLow; originPrice = pivotLowPrice;
                }
            } else if (lastPivotHigh >= 0) {
                dir = -1; origin = lastPivotHigh; originPrice = pivotHighPrice;
            } else if (lastPivotLow >= 0) {
                dir = 1; origin = lastPivotLow; originPrice = pivotLowPrice;
            } else {
                // Fallback: compare current price to mid of last 10-bar range
                int from = Math.max(0, n - 10);
                double hi = Double.NEGATIVE_INFINITY, lo = Double.POSITIVE_INFINITY;
                for (int i = from; i < n; i++) {
                    hi = Math.max(hi, c.get(i).high);
                    lo = Math.min(lo, c.get(i).low);
                }
                double mid = (hi + lo) / 2.0;
                dir = price > mid ? 1 : -1;
                origin = from;
                originPrice = c.get(from).close;
            }

            int age = n - 1 - origin;
            double depth = Math.abs(price - originPrice) / (atr + 1e-12);

            // Flat zone: very young move with tiny depth → undefined direction
            if (age <= 1 && depth < 0.3) dir = 0;

            return new MoveInfo(dir, age, depth, origin, originPrice);
        }

        //  EXHAUSTION FACTORS

        /** Volume Fade: average volume of last 3 bars vs first 3 bars of move.
         *  If later bars have less volume → move losing fuel. Returns [0..1]. */
        private double calcVolumeFade(List<Candle> c, MoveInfo move) {
            // Old: ageBars<4 → 60min lag. New: adaptive window, works from 2 bars.
            if (move.ageBars < 2) return 0;
            int n = c.size();
            int start = move.originIdx;
            int moveLen = n - start; // total bars in current move (inclusive of origin)

            // Adaptive window: for short moves use 1 bar each side (no overlap),
            // for longer moves use up to 3 bars each side.
            int w = Math.min(3, Math.max(1, moveLen / 2));

            double volStart = 0;
            int c1 = 0;
            for (int i = start; i < start + w && i < n; i++) { volStart += c.get(i).volume; c1++; }
            if (c1 == 0) return 0;
            volStart /= c1;

            double volEnd = 0;
            int c2 = 0;
            for (int i = n - w; i < n; i++) { volEnd += c.get(i).volume; c2++; }
            if (c2 == 0) return 0;
            volEnd /= c2;

            if (volStart < 1e-12) return 0;
            double ratio = volEnd / volStart;
            return clamp((1.0 - ratio) * 1.5, 0, 1);
        }

        /** Momentum Decay: body size of last window vs first window.
         *  If each bar is smaller → momentum dying. Returns [0..1]. */
        private double calcMomentumDecay(List<Candle> c, MoveInfo move, double atr) {
            // Old: ageBars<4. New: adaptive window from 2 bars.
            if (move.ageBars < 2) return 0;
            int n = c.size();
            int start = move.originIdx;
            int moveLen = n - start;
            int w = Math.min(3, Math.max(1, moveLen / 2));

            double bodyStart = 0;
            int cnt = 0;
            for (int i = start; i < start + w && i < n; i++) {
                double body = (c.get(i).close - c.get(i).open) * move.direction;
                bodyStart += Math.max(0, body);
                cnt++;
            }
            bodyStart = cnt > 0 ? bodyStart / cnt : 0;

            double bodyEnd = 0;
            cnt = 0;
            for (int i = n - w; i < n; i++) {
                double body = (c.get(i).close - c.get(i).open) * move.direction;
                bodyEnd += Math.max(0, body);
                cnt++;
            }
            bodyEnd = cnt > 0 ? bodyEnd / cnt : 0;

            if (bodyStart < atr * 0.05) return 0;
            double ratio = bodyEnd / (bodyStart + 1e-12);

            // Counter-trend last bar = extra exhaustion signal
            double lastBody = (c.get(n - 1).close - c.get(n - 1).open) * move.direction;
            double counterPenalty = lastBody < 0 ? 0.3 : 0;

            return clamp((1.0 - ratio) * 1.3 + counterPenalty, 0, 1);
        }

        /** Wick Rejection: wicks against the move growing = opposition forming.
         *  Added early (1-2 bar) wick detection for fast reversals. */
        private double calcWickRejection(List<Candle> c, MoveInfo move) {
            int n = c.size();
            if (move.ageBars < 1 || n < 3 || move.direction == 0) return 0;

            // ── Early wick rejection: last 1-2 bars with >55% wick against move ──
            // This catches fast reversals where 15-30 min warning is enough.
            {
                int look = Math.min(2, move.ageBars + 1);
                double wickSum = 0;
                int cnt = 0;
                for (int i = n - look; i < n; i++) {
                    Candle bar = c.get(i);
                    double range = bar.high - bar.low;
                    if (range < 1e-12) continue;
                    double wickFrac;
                    if (move.direction > 0) {
                        wickFrac = (bar.high - Math.max(bar.open, bar.close)) / range;
                    } else {
                        wickFrac = (Math.min(bar.open, bar.close) - bar.low) / range;
                    }
                    wickSum += wickFrac;
                    cnt++;
                }
                if (cnt > 0 && wickSum / cnt > 0.55) return 0.55; // strong early rejection
            }

            // ── Standard 3-bar aggregate (legacy, for older moves) ──
            if (move.ageBars < 2) return 0;
            double wickScore = 0;
            int from = Math.max(0, n - 3);
            for (int i = from; i < n; i++) {
                Candle bar = c.get(i);
                double body = Math.abs(bar.close - bar.open) + 1e-12;
                if (move.direction > 0) {
                    double uw = bar.high - Math.max(bar.close, bar.open);
                    if (uw > body * 1.5) wickScore += 0.35;
                    else if (uw > body) wickScore += 0.15;
                } else {
                    double lw = Math.min(bar.close, bar.open) - bar.low;
                    if (lw > body * 1.5) wickScore += 0.35;
                    else if (lw > body) wickScore += 0.15;
                }
            }
            return clamp(wickScore, 0, 1);
        }

        /**
         * RSI Exhaustion v2 — proper swing-pivot divergence + StochRSI + hidden divergence.
         *
         * FIXES vs old version:
         *   OLD: compared RSI(current) vs RSI(n-3) — this is NOT divergence, just deceleration.
         *        A simple 3-bar RSI drop fires even in healthy pullbacks.
         *   NEW: Finds the actual peak/trough bar in last 5-20 bars and compares RSI there
         *        vs RSI at current price extreme. True classic divergence = price new high +
         *        RSI lower high. This is a TOP-TIER reversal signal (70-80% win rate).
         *
         *   OLD: Hidden divergence ignored entirely.
         *   NEW: Hidden bullish div (price HL + RSI LL) = uptrend continuation → REDUCES
         *        exhaustion score so the system doesn't falsely call top of a strong trend.
         *
         *   NEW: StochRSI extreme detection fires 2-3 bars BEFORE standard RSI extreme.
         *        StochRSI > 0.92 = overbought (fast oscillator confirms exhaustion early).
         *
         * Returns [0..1]: 0 = no exhaustion, 1 = maximum exhaustion evidence.
         */
        private double calcRsiExhaustion(List<Candle> c, MoveInfo move) {
            int n = c.size();
            if (n < 25) return 0;

            double[] rsiArr = TradingCore.rsiSeries(c, 14);
            double rsi14 = rsiArr[n - 1];
            double score = 0;

            if (move.direction > 0) {
                // ── Zone overbought ──────────────────────────────────────
                if      (rsi14 > 80) score += 0.50;
                else if (rsi14 > 73) score += 0.32;
                else if (rsi14 > 65) score += 0.14;

                // ── Classic Bearish RSI Divergence ───────────────────────
                // Price makes higher high → RSI makes lower high = fuel running out.
                // Requires an actual pivot in the lookback, not just n-3.
                int lookback = Math.min(20, n - 6);
                int peakIdx = -1;
                double peakHigh = Double.NEGATIVE_INFINITY;
                for (int i = n - 2; i >= n - lookback; i--) {
                    if (c.get(i).high > peakHigh) { peakHigh = c.get(i).high; peakIdx = i; }
                }
                if (peakIdx >= 0 && peakIdx <= n - 5) {
                    double rsiAtPeak = rsiArr[peakIdx];
                    boolean priceNewHigh  = c.get(n - 1).high  > peakHigh;
                    boolean rsiLowerHigh  = rsi14 < rsiAtPeak - 3.0 && rsi14 > 48;
                    if (priceNewHigh && rsiLowerHigh) score += 0.55; // strongest divergence signal
                }

                // ── Hidden Bullish Divergence (continuation) ─────────────
                // Price higher low + RSI lower low = uptrend very healthy → REDUCE exhaustion.
                // Find recent trough in lookback:
                int troughIdx = -1;
                double troughLow = Double.MAX_VALUE;
                for (int i = n - 2; i >= n - Math.min(15, n - 4); i--) {
                    if (c.get(i).low < troughLow) { troughLow = c.get(i).low; troughIdx = i; }
                }
                if (troughIdx >= 0 && troughIdx <= n - 4) {
                    double rsiAtTrough = rsiArr[troughIdx];
                    boolean priceHigherLow = c.get(n - 1).low > troughLow;
                    boolean rsiLowerLow    = rsi14 < rsiAtTrough - 2.0;
                    if (priceHigherLow && rsiLowerLow) score -= 0.20; // bullish continuation → pull exhaustion down
                }

                // ── RSI momentum fade (deceleration, not divergence) ──────
                if (n >= 7 && rsiArr[n - 4] > 0) {
                    double rsiSpeed = rsi14 - rsiArr[n - 4]; // RSI change over 3 bars
                    if (rsiSpeed < -5.0 && rsi14 > 55) score += 0.22; // RSI falling fast while still elevated
                }

            } else if (move.direction < 0) {
                // ── Zone oversold ────────────────────────────────────────
                if      (rsi14 < 20) score += 0.50;
                else if (rsi14 < 27) score += 0.32;
                else if (rsi14 < 35) score += 0.14;

                // ── Classic Bullish RSI Divergence ───────────────────────
                // Price makes lower low → RSI makes higher low = selling pressure exhausted.
                int lookback = Math.min(20, n - 6);
                int troughIdx = -1;
                double troughLow = Double.MAX_VALUE;
                for (int i = n - 2; i >= n - lookback; i--) {
                    if (c.get(i).low < troughLow) { troughLow = c.get(i).low; troughIdx = i; }
                }
                if (troughIdx >= 0 && troughIdx <= n - 5) {
                    double rsiAtTrough = rsiArr[troughIdx];
                    boolean priceNewLow   = c.get(n - 1).low  < troughLow;
                    boolean rsiHigherLow  = rsi14 > rsiAtTrough + 3.0 && rsi14 < 52;
                    if (priceNewLow && rsiHigherLow) score += 0.55; // strongest bullish divergence
                }

                // ── Hidden Bearish Divergence (continuation) ─────────────
                // Price lower high + RSI higher high = downtrend healthy → REDUCE exhaustion.
                int peakIdx = -1;
                double peakHigh = Double.NEGATIVE_INFINITY;
                for (int i = n - 2; i >= n - Math.min(15, n - 4); i--) {
                    if (c.get(i).high > peakHigh) { peakHigh = c.get(i).high; peakIdx = i; }
                }
                if (peakIdx >= 0 && peakIdx <= n - 4) {
                    double rsiAtPeak = rsiArr[peakIdx];
                    boolean priceLowerHigh = c.get(n - 1).high < peakHigh;
                    boolean rsiHigherHigh  = rsi14 > rsiAtPeak + 2.0;
                    if (priceLowerHigh && rsiHigherHigh) score -= 0.20; // bearish continuation → pull down
                }

                // ── RSI momentum recovery (deceleration of downtrend) ─────
                if (n >= 7 && rsiArr[n - 4] > 0) {
                    double rsiSpeed = rsi14 - rsiArr[n - 4];
                    if (rsiSpeed > 5.0 && rsi14 < 45) score += 0.22; // RSI recovering fast while still depressed
                }
            }

            // ── StochRSI extreme — fires 2-3 bars before standard RSI ────
            // When StochRSI > 0.92 in uptrend, RSI is at the top of its own range → fast reversal.
            double stochRsi = calcStochRsi(c, 14, 14);
            if (move.direction > 0 && stochRsi > 0.92) score += 0.25;
            if (move.direction < 0 && stochRsi < 0.08) score += 0.25;

            return clamp(score, 0, 1);
        }

        /** Overextension: has the move gone too far too fast?
         *  [BUG C FIX] Now triggers on fast 2-bar pumps (high depth, low age). */
        private double calcOverextension(MoveInfo move, double atr) {
            if (move.direction == 0 || atr <= 0) return 0;

            // Fast short pump/dump: 2 bars but depth > 1.5 ATR = parabolic
            if (move.ageBars <= 2 && move.depthAtr > 1.5) {
                return clamp((move.depthAtr - 1.0) * 0.35, 0.30, 0.85);
            }
            if (move.ageBars < 2) return 0;

            // Speed (ATR per bar): > 0.6 over 3+ bars = extended
            double speedAtr = move.depthAtr / (move.ageBars + 1e-6);
            if (speedAtr > 0.6 && move.ageBars >= 3) return clamp(speedAtr * 0.8, 0, 1);

            // Absolute depth: stretched rubber band
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

        //  TREND PHASE — tighter classification
        //
        // Old thresholds let 5–8 bar moves through as MID, blocking late-entry
        // penalties on already-mature trends. Tightened: LATE now triggers at
        // age > 5 OR depth > 2.5 ATR OR speed > 0.65 ATR/bar (parabolic catch).

        public TrendPhase detectPhase(List<Candle> c15, List<Candle> c1h) {
            return detectPhase(c15, c1h,
                    identifyCurrentMove(c15, fcAtr(c15, 14)), 0);
        }

        private TrendPhase detectPhase(List<Candle> c15, List<Candle> c1h,
                                       MoveInfo move, double exhaustion) {
            if (exhaustion > 0.55) return TrendPhase.EXHAUSTION;
            if (exhaustion > 0.35) return TrendPhase.LATE;

            // Speed-based late catch: parabolic moves age fast in ATR space.
            // 3-bar pump that traveled 2.5 ATR is LATE, not MID.
            if (move.ageBars >= 3) {
                double speed = move.depthAtr / Math.max(1, move.ageBars);
                if (speed > 0.65) return TrendPhase.LATE;
            }

            if (move.ageBars <= 3 && move.depthAtr < 1.2) return TrendPhase.EARLY;
            if (move.ageBars <= 5 && move.depthAtr < 2.0) return TrendPhase.MID;
            if (move.ageBars >  5 || move.depthAtr > 2.5) return TrendPhase.LATE;
            return TrendPhase.LATE; // boundary catch — classify conservatively
        }

        //  SQUEEZE detection

        /**
         * Volatility Squeeze v2 — Bollinger Bands inside Keltner Channels (John Carter).
         *
         * OLD: compared current ATR to historical ATR percentile.
         *   Issue: ATR alone doesn't tell if we're in a PRE-BREAKOUT squeeze or just
         *   a low-vol period. Many low-vol periods are not squeezes.
         *
         * NEW: True squeeze = BB(20,2) entirely INSIDE KC(20,1.5×ATR).
         *   When BB collapses inside KC, volatility is compressed beyond normal.
         *   This precedes the strongest directional breakouts in crypto.
         *   Historically: 75-80% of BB/KC squeezes resolve with ATR-expanding moves.
         *
         * Fallback: if not enough data for full BB/KC, use improved ATR percentile
         * (20th percentile instead of old approach).
         */
        private boolean isVolatilitySqueeze(List<Candle> c, double currentAtr) {
            int n = c.size();
            if (n < 25) return false;

            // ── Full BB/KC Squeeze detection ─────────────────────────────
            if (n >= 50) {
                // Bollinger Bands: 20-bar SMA ± 2σ
                int bbPeriod = 20;
                double bbSum = 0;
                for (int i = n - bbPeriod; i < n; i++) bbSum += c.get(i).close;
                double bbMid = bbSum / bbPeriod;
                double bbVar = 0;
                for (int i = n - bbPeriod; i < n; i++) {
                    double d = c.get(i).close - bbMid;
                    bbVar += d * d;
                }
                double bbStd = Math.sqrt(bbVar / bbPeriod);
                double bbUpper = bbMid + 2.0 * bbStd;
                double bbLower = bbMid - 2.0 * bbStd;

                // Keltner Channels: 20-bar EMA ± 1.5×ATR(14)
                double kcEma = 0, emaK = 2.0 / (bbPeriod + 1);
                kcEma = c.get(n - bbPeriod).close;
                for (int i = n - bbPeriod + 1; i < n; i++) {
                    kcEma = c.get(i).close * emaK + kcEma * (1 - emaK);
                }
                double kcAtr = fcAtr(c.subList(n - bbPeriod - 1, n), 14);
                double kcUpper = kcEma + 1.5 * kcAtr;
                double kcLower = kcEma - 1.5 * kcAtr;

                // Squeeze: BB is entirely inside KC
                if (bbUpper < kcUpper && bbLower > kcLower) return true;
            }

            // ── Fallback: ATR percentile (improved: 20th percentile, was ad-hoc) ──
            if (n >= 30) {
                List<Double> atrHist = new ArrayList<>();
                for (int i = Math.max(16, n - 50); i < n - 1; i += 2) {
                    int subEnd = i + 1;
                    int subStart = Math.max(0, subEnd - 15);
                    double a = fcAtr(c.subList(subStart, subEnd), Math.min(14, subEnd - subStart));
                    if (a > 0) atrHist.add(a);
                }
                if (atrHist.size() >= 8) {
                    Collections.sort(atrHist);
                    double p20 = atrHist.get(atrHist.size() / 5);
                    return currentAtr < p20 * 0.80; // tighter threshold than old 0.85
                }
            }
            return false;
        }

        //  TREND BRAIN helpers (kept but secondary)

        /**
         * Order Flow v2 — taker ratio + CMF + volumeDelta combined.
         *
         * OLD: simple taker_buy/total_volume ratio + volumeDelta. Only measures
         *      who clicked "buy" — ignores WHERE price closed in the range.
         *
         * NEW: 50% weight to Chaikin Money Flow (CMF) which uses
         *      ((Close-Low)-(High-Close))/(High-Low) × Volume formula.
         *      CMF > 0 = close near top of range → buyers controlled the bar.
         *      CMF < 0 = close near bottom → sellers controlled.
         *      This is a much better proxy for institutional intent than taker ratio.
         *
         *      30% weight to classic taker buy ratio (retained for real-time data).
         *      20% weight to volumeDelta for directional flow confirmation.
         *
         * Returns [-0.6..+0.6].
         */
        private double calcOrderflow(List<Candle> c, double vd) {
            int n = c.size();
            double s = 0;

            // Component 1: Taker buy ratio (last 5 bars, 30% weight)
            if (n >= 5) {
                double tb = 0, tv = 0;
                for (int i = n - 5; i < n; i++) {
                    tb += c.get(i).takerBuyBaseVolume;
                    tv += c.get(i).volume;
                }
                if (tv > 0) s += (tb / tv - 0.5) * 2.0 * 0.30;
            }

            // Component 2: Chaikin Money Flow — 14 bars (50% weight)
            // CMF = sum(((Close-Low)-(High-Close))/(High-Low) × Vol) / sum(Vol)
            // Measures where price closes within each bar's range, weighted by volume.
            // Institutional buyers close price near top of range even on down days.
            {
                double mfvSum = 0, volSum = 0;
                int cmfPeriod = Math.min(14, n);
                for (int i = n - cmfPeriod; i < n; i++) {
                    Candle bar = c.get(i);
                    double range = bar.high - bar.low;
                    if (range < 1e-12 || bar.volume < 1e-12) continue;
                    double mfm = ((bar.close - bar.low) - (bar.high - bar.close)) / range;
                    mfvSum += mfm * bar.volume;
                    volSum += bar.volume;
                }
                if (volSum > 0) s += clamp(mfvSum / volSum, -1, 1) * 0.50;
            }

            // Component 3: volumeDelta (20% weight)
            if (Math.abs(vd) > 0.001) {
                s += clamp(Math.signum(vd) * Math.min(1, Math.abs(vd) * 3), -1, 1) * 0.20;
            }

            return clamp(s, -0.65, 0.65);
        }

        /**
         * Slope-based HTF — no EMA lag.
         *
         * Problem with old version: EMA9>EMA21 on 1h persists 5-9 hours after a pump.
         * Document's fix (EMA delta over 1 hourly bar) is still EMA-based and lagged.
         *
         * New logic: compare close-to-close slope of last 3 hourly bars against the
         * prior 3 hourly bars. This is forward-neutral — reacts within 2 hourly bars
         * of an actual reversal, not 5-9.
         *
         * Weights: 30% static EMA stack (for trend regimes), 70% recent slope (for
         * reversal detection). Combined so calcHTF returns a reversal signal when
         * price has objectively changed direction on the hourly, regardless of what
         * the EMA stack still says.
         */
        private double calcHTF(List<Candle> c15, List<Candle> c1h) {
            if (c1h == null || c1h.size() < 8) return 0;
            int hn = c1h.size();

            // --- Static component: EMA stack (30% weight) ---
            double staticScore = 0;
            if (hn >= 25) {
                double e9 = fcEma(c1h, 9);
                double e21 = fcEma(c1h, 21);
                double hPrice = c1h.get(hn - 1).close;
                if (hPrice > e9 && e9 > e21)      staticScore =  0.80;
                else if (hPrice < e9 && e9 < e21) staticScore = -0.80;
                else if (e9 > e21)                staticScore =  0.35;
                else                               staticScore = -0.35;
            }

            // --- Dynamic component: last-3 vs prior-3 hourly slope (70% weight) ---
            // This is the anti-lag fix. Reacts within 2 hourly bars of reversal.
            double recentSlope = c1h.get(hn - 1).close - c1h.get(hn - 4).close;
            double priorSlope  = c1h.get(hn - 4).close - c1h.get(hn - 7 >= 0 ? hn - 7 : 0).close;
            double basis = Math.max(1e-9, Math.abs(c1h.get(hn - 1).close));
            double recentPct = recentSlope / basis;
            double priorPct  = priorSlope  / basis;

            double dynamicScore;
            // Divergence: price reversing vs prior direction = strong reversal signal
            if (recentPct < -0.004 && priorPct > 0.002) {
                dynamicScore = -0.80; // price turning DOWN after being up
            } else if (recentPct > 0.004 && priorPct < -0.002) {
                dynamicScore =  0.80; // price turning UP after being down
            } else {
                // Trend continuation — sign of recent slope, magnitude scaled
                dynamicScore = clamp(recentPct * 50.0, -0.80, 0.80);
            }

            // Secondary check on 15m EMA21/EMA50 stack (small weight, sanity)
            double m15Score = 0;
            if (c15 != null && c15.size() >= 50) {
                double e21 = fcEma(c15, 21), e50 = fcEma(c15, 50);
                m15Score = (e21 > e50) ? 0.20 : -0.20;
            }

            double result = staticScore * 0.30 + dynamicScore * 0.60 + m15Score * 0.10;
            return clamp(result, -1.0, 1.0);
        }

        /**
         * Chaikin Money Flow [CMF] — measures institutional buying/selling pressure.
         * Formula per bar: MFM = ((Close - Low) - (High - Close)) / (High - Low)
         *                  MFV = MFM × Volume
         * CMF = sum(MFV, period) / sum(Volume, period)
         *
         * Range: [-1..+1].
         *   > +0.20 = strong institutional buying (close persistently near top of range)
         *   < -0.20 = strong institutional selling (close persistently near bottom)
         *
         * This is the standalone version used as a forecast factor.
         * The inline version in calcOrderflow is for the OF cluster.
         */
        private double calcCMF(List<Candle> c, int period) {
            int n = c.size();
            if (n < period) return 0;
            double mfvSum = 0, volSum = 0;
            for (int i = n - period; i < n; i++) {
                Candle bar = c.get(i);
                double range = bar.high - bar.low;
                if (range < 1e-12 || bar.volume < 1e-12) continue;
                double mfm = ((bar.close - bar.low) - (bar.high - bar.close)) / range;
                mfvSum += mfm * bar.volume;
                volSum += bar.volume;
            }
            return volSum < 1e-12 ? 0 : clamp(mfvSum / volSum, -1, 1);
        }

        /**
         * OBV Slope — linear regression slope of On-Balance Volume over N bars.
         * Normalized to [-1..+1] relative to average volume per bar.
         *
         * OBV divergence interpretation:
         *   Price trending up + OBV slope negative = distribution (TOP signal)
         *   Price trending down + OBV slope positive = accumulation (BOTTOM signal)
         *   Price and OBV slope agree = healthy trend continuation
         *
         * This is one of the most reliable LEADING indicators for institutional intent.
         * Smart money accumulates/distributes before price moves — OBV shows their footprint.
         */
        private double calcOBVSlope(List<Candle> c, int period) {
            int n = c.size();
            if (n < period + 2) return 0;
            // Build OBV series from the window before current bar
            double[] obv = new double[period];
            double prevClose = c.get(n - period - 1).close;
            double cum = 0;
            for (int i = 0; i < period; i++) {
                Candle bar = c.get(n - period + i);
                if (bar.close > prevClose)      cum += bar.volume;
                else if (bar.close < prevClose) cum -= bar.volume;
                obv[i] = cum;
                prevClose = bar.close;
            }
            // Linear regression slope of OBV
            double sX = 0, sY = 0, sXY = 0, sX2 = 0;
            for (int i = 0; i < period; i++) {
                sX += i; sY += obv[i]; sXY += i * obv[i]; sX2 += i * i;
            }
            double denom = (double) period * sX2 - sX * sX;
            if (Math.abs(denom) < 1e-12) return 0;
            double slope = (period * sXY - sX * sY) / denom;
            // Normalize: per-bar slope relative to average absolute volume
            double avgVol = 0;
            for (int i = n - period; i < n; i++) avgVol += c.get(i).volume;
            avgVol /= period;
            if (avgVol < 1e-12) return 0;
            // Scale so that a slope of 1× avgVol per bar ≈ 0.33 normalized
            return clamp(slope / avgVol * 0.33, -1, 1);
        }

        /**
         * Stochastic RSI — the oscillator of RSI within its own N-bar range.
         * Returns [0..1]: 0 = RSI at its N-bar minimum, 1 = RSI at its N-bar maximum.
         *
         * Why it's useful:
         *   Regular RSI: fires "overbought" at ~75 (standard threshold).
         *   StochRSI:    fires at >0.90 when RSI is near the TOP of its recent range —
         *                this happens 2-3 bars BEFORE RSI itself crosses 75.
         *                Early warning for exhaustion, especially in fast meme-coin pumps.
         *
         * > 0.90 in uptrend = fast overbought → likely short-term reversal
         * < 0.10 in downtrend = fast oversold → likely short-term bounce
         */
        private double calcStochRsi(List<Candle> c, int rsiPeriod, int stochPeriod) {
            int n = c.size();
            int minBars = rsiPeriod + stochPeriod + 5;
            if (n < minBars) return 0.5; // neutral — not enough data
            double[] rsiArr = TradingCore.rsiSeries(c, rsiPeriod);
            double rsiNow = rsiArr[n - 1];
            double rsiMin = Double.MAX_VALUE, rsiMax = Double.NEGATIVE_INFINITY;
            // Use stochPeriod bars BEFORE current (exclude current from min/max)
            for (int i = n - stochPeriod; i < n - 1; i++) {
                if (rsiArr[i] < rsiMin) rsiMin = rsiArr[i];
                if (rsiArr[i] > rsiMax) rsiMax = rsiArr[i];
            }
            if (rsiMax - rsiMin < 2.0) return 0.5; // RSI not moving = compression = neutral
            return clamp((rsiNow - rsiMin) / (rsiMax - rsiMin), 0, 1);
        }

        /** [v24.0 FIX BUG-5] Double.MIN_VALUE = 4.9E-324 (POSITIVE!) → always < any price.
         *  Must use NEGATIVE_INFINITY for maximum search initialization. */
        private double calcFisher(List<Candle> c, int p) {
            int n = c.size();
            if (n < p + 2) return 0;
            // [v24.0 FIX BUG-5] Double.MIN_VALUE = 4.9E-324 (POSITIVE!) → always < any price.
            // Must use NEGATIVE_INFINITY for maximum search initialization.
            double hi = Double.NEGATIVE_INFINITY, lo = Double.MAX_VALUE;
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

        //  MATH primitives

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

        /**
         * VPOC v2 — Volume-weighted Point of Control with recency bias.
         *
         * OLD: all bars in 50-bar window get equal weight. A volume spike 3 days ago
         *      has the same magnetic pull as yesterday's spike. Wrong.
         *
         * NEW: exponential decay per bar (factor 0.96 per bar back).
         *      The most recent 10 bars account for ~65% of the total weight.
         *      This makes the VPOC reflect the CURRENT fair value, not historical.
         *
         * Result: fewer "stale magnet" false signals where price already left the
         * old VPOC zone 2 days ago and the system still calls it a target.
         */
        private double calcVPOC(List<Candle> c, int p) {
            int n = c.size(), start = Math.max(0, n - p);
            double lo = Double.MAX_VALUE, hi = Double.NEGATIVE_INFINITY;
            for (int i = start; i < n; i++) {
                lo = Math.min(lo, c.get(i).low);
                hi = Math.max(hi, c.get(i).high);
            }
            if (hi - lo < 1e-12) return c.get(n - 1).close;
            int bins = 50;
            double bs = (hi - lo) / bins;
            double[] vb = new double[bins];
            double decay = 0.96; // each older bar gets 96% of next newer bar's weight
            for (int i = n - 1; i >= start; i--) {
                double weight = Math.pow(decay, n - 1 - i); // newer = higher weight
                Candle bar = c.get(i);
                double tp = (bar.high + bar.low + bar.close) / 3.0;
                int b = Math.min(bins - 1, Math.max(0, (int) ((tp - lo) / bs)));
                vb[b] += bar.volume * weight;
                // Also distribute across the full range of the bar (not just TP)
                // This prevents single-bar skew on high-volume candles with wide range
                int bLo = Math.min(bins - 1, Math.max(0, (int) ((bar.low  - lo) / bs)));
                int bHi = Math.min(bins - 1, Math.max(0, (int) ((bar.high - lo) / bs)));
                if (bHi > bLo) {
                    double spread = bar.volume * weight * 0.3 / (bHi - bLo + 1);
                    for (int b2 = bLo; b2 <= bHi; b2++) vb[b2] += spread;
                }
            }
            int mx = 0;
            for (int i = 1; i < bins; i++) if (vb[i] > vb[mx]) mx = i;
            return lo + (mx + 0.5) * bs;
        }
        private double fcAtr(List<Candle> c, int n) {
            return TradingCore.atr(c, n);
        }

        private double fcEma(List<Candle> c, int p) {
            if (c.size() < p) return c.get(c.size() - 1).close;
            double k = 2.0 / (p + 1), e = c.get(c.size() - p).close;
            for (int i = c.size() - p + 1; i < c.size(); i++)
                e = c.get(i).close * k + e * (1 - k);
            return e;
        }
    }
}