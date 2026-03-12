package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    public enum CoinCategory { TOP, ALT, MEME }
    public enum MarketState { STRONG_TREND, WEAK_TREND, RANGE, VOLATILE }
    public enum HTFBias { BULL, BEAR, NONE }

    private static final int MIN_BARS = 150;

    private static final long COOLDOWN_TOP = 15 * 60_000;
    private static final long COOLDOWN_ALT = 15 * 60_000;
    private static final long COOLDOWN_MEME = 15 * 60_000;
    private final Map<String, Double> lastSignalPrice = new ConcurrentHashMap<>();

    // === ADAPTIVE MIN_CONFIDENCE (динамический порог) ===
    private double MIN_CONFIDENCE = 58.0;

    private final Map<String, Long> cooldownMap = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>> recentDirections = new ConcurrentHashMap<>();

    // === НОВОЕ: Хранилище Funding Rate и Open Interest ===
    private final Map<String, FundingOIData> fundingOICache = new ConcurrentHashMap<>();

    // === НОВОЕ: История калибровки probability ===
    private final Map<String, Deque<CalibrationRecord>> calibrationHistory = new ConcurrentHashMap<>();
    private static final int CALIBRATION_WINDOW = 100;

    public DecisionEngineMerged() {
    }

    // === НОВОЕ: Структура для Funding Rate + Open Interest ===
    public static final class FundingOIData {
        public final double fundingRate;      // текущий funding rate (%)
        public final double openInterest;     // текущий OI в USDT
        public final double oiChange1h;       // изменение OI за 1 час (%)
        public final double oiChange4h;       // изменение OI за 4 часа (%)
        public final long timestamp;

        public FundingOIData(double fundingRate, double openInterest, double oiChange1h, double oiChange4h) {
            this.fundingRate = fundingRate;
            this.openInterest = openInterest;
            this.oiChange1h = oiChange1h;
            this.oiChange4h = oiChange4h;
            this.timestamp = System.currentTimeMillis();
        }

        public boolean isValid() {
            return System.currentTimeMillis() - timestamp < 5 * 60_000; // 5 минут актуальность
        }
    }

    // === НОВОЕ: Запись для калибровки ===
    private static final class CalibrationRecord {
        final double predictedProb;
        final boolean wasCorrect;
        final long timestamp;

        CalibrationRecord(double predictedProb, boolean wasCorrect) {
            this.predictedProb = predictedProb;
            this.wasCorrect = wasCorrect;
            this.timestamp = System.currentTimeMillis();
        }
    }

    public static final class TradeIdea {
        public final String symbol;
        public final com.bot.TradingCore.Side side;
        public final double price;
        public final double stop;
        public final double take;
        public final double probability;
        public final List<String> flags;
        // === НОВОЕ: Дополнительная информация ===
        public final double fundingRate;
        public final double oiChange;
        public final String htfBias;

        public TradeIdea(String symbol,
                         com.bot.TradingCore.Side side,
                         double price,
                         double stop,
                         double take,
                         double probability,
                         List<String> flags) {
            this(symbol, side, price, stop, take, probability, flags, 0, 0, "NONE");
        }

        public TradeIdea(String symbol,
                         com.bot.TradingCore.Side side,
                         double price,
                         double stop,
                         double take,
                         double probability,
                         List<String> flags,
                         double fundingRate,
                         double oiChange,
                         String htfBias) {
            this.symbol = symbol;
            this.side = side;
            this.price = price;
            this.stop = stop;
            this.take = take;
            this.probability = probability;
            this.flags = flags != null ? flags : List.of();
            this.fundingRate = fundingRate;
            this.oiChange = oiChange;
            this.htfBias = htfBias;
        }

        @Override
        public String toString() {
            String flagStr = flags == null || flags.isEmpty() ? "-" : String.join(", ", flags);
            String time = java.time.ZonedDateTime
                    .now(java.time.ZoneId.of("Europe/Warsaw"))
                    .toLocalTime()
                    .withNano(0)
                    .toString();

            // === НОВОЕ: Добавляем Funding и OI в вывод ===
            String fundingStr = Math.abs(fundingRate) > 0.001 ?
                    String.format("FR: %.3f%%", fundingRate * 100) : "";
            String oiStr = Math.abs(oiChange) > 0.5 ?
                    String.format("OI: %+.1f%%", oiChange) : "";
            String extraInfo = (!fundingStr.isEmpty() || !oiStr.isEmpty()) ?
                    String.format("\n%s %s", fundingStr, oiStr).trim() : "";

            return String.format(
                    "*%s* → *%s*\n" +
                            "Price: %.6f\n" +
                            "Probability: %.0f%%\n" +
                            "Stop-Take: %.6f - %.6f\n" +
                            "Flags: %s%s\n" +
                            "_time: %s_",
                    symbol, side, price, probability, stop, take, flagStr, extraInfo, time
            );
        }
    }

    // === НОВОЕ: Метод для обновления Funding Rate и OI ===
    public void updateFundingOI(String symbol, double fundingRate, double openInterest, double oiChange1h, double oiChange4h) {
        fundingOICache.put(symbol, new FundingOIData(fundingRate, openInterest, oiChange1h, oiChange4h));
    }

    // === НОВОЕ: Метод для получения актуальных данных ===
    public FundingOIData getFundingOI(String symbol) {
        FundingOIData data = fundingOICache.get(symbol);
        if (data != null && data.isValid()) {
            return data;
        }
        return null;
    }

    /* ===== MAIN GENERATE ===== */
    private TradeIdea generate(String symbol,
                               List<com.bot.TradingCore.Candle> c1,
                               List<com.bot.TradingCore.Candle> c5,
                               List<com.bot.TradingCore.Candle> c15,
                               List<com.bot.TradingCore.Candle> c1h,
                               List<com.bot.TradingCore.Candle> c4h,
                               CoinCategory cat,
                               long now) {

        if (!valid(c15) || !valid(c1h)) {
            return null;
        }

        double price = last(c15).close;
        double atr = atr(c15, 14);
        double lastRange = last(c15).high - last(c15).low;

        // Фильтр экстремальной волатильности
        if (lastRange > atr * 4.0) {
            return null;
        }
        if (atr <= 0) return null;

        atr = Math.max(atr, price * 0.0012);

        MarketState state = detectState(c15);
        HTFBias bias1h = detectBias(c1h);

        // === НОВОЕ: 4H BIAS (главный фильтр направления) ===
        HTFBias bias4h = c4h != null && c4h.size() >= 100 ? detectBias4H(c4h) : HTFBias.NONE;

        // === НОВОЕ: Адаптивный MIN_CONFIDENCE ===
        double btcAtr = atr; // TODO: получить ATR BTC отдельно
        adaptMinConfidence(state, btcAtr, price);

        // === SIDEWAYS FILTER === Не торгуем боковик
        if (state == MarketState.RANGE) {
            double adxVal = adx(c15, 14);
            if (adxVal < 20) {
                return null; // Явный боковик - пропускаем
            }
        }

        double scoreLong = 0;
        double scoreShort = 0;
        List<String> flags = new ArrayList<>();

        // === HTF BIAS 1H ===
        if (bias1h == HTFBias.BULL) {
            scoreLong += 0.40;
            scoreShort -= 0.15;
        } else if (bias1h == HTFBias.BEAR) {
            scoreShort += 0.40;
            scoreLong -= 0.15;
        }

        // === НОВОЕ: 4H BIAS (более сильный вес) ===
        if (bias4h == HTFBias.BULL) {
            scoreLong += 0.55;
            scoreShort -= 0.30;
            flags.add("4H_BULL");
        } else if (bias4h == HTFBias.BEAR) {
            scoreShort += 0.55;
            scoreLong -= 0.30;
            flags.add("4H_BEAR");
        }

        // === НОВОЕ: FUNDING RATE FILTER ===
        FundingOIData fundingData = getFundingOI(symbol);
        double fundingRate = 0;
        double oiChange = 0;

        if (fundingData != null) {
            fundingRate = fundingData.fundingRate;
            oiChange = fundingData.oiChange1h;

            // Funding > 0.05% = перегретый лонг → ищем шорт
            if (fundingRate > 0.0005) {
                scoreShort += 0.35 + Math.min(fundingRate * 100, 0.40);
                scoreLong -= 0.25;
                flags.add("FR_HIGH");
            }
            // Funding < -0.05% = перегретый шорт → ищем лонг
            else if (fundingRate < -0.0005) {
                scoreLong += 0.35 + Math.min(Math.abs(fundingRate) * 100, 0.40);
                scoreShort -= 0.25;
                flags.add("FR_LOW");
            }

            // === НОВОЕ: OPEN INTEREST ANALYSIS ===
            // OI растёт + funding высокий = скоро ликвидации
            if (fundingData.oiChange1h > 3.0 && fundingRate > 0.0003) {
                scoreShort += 0.30;
                flags.add("OI_SQUEEZE_LONG");
            }
            if (fundingData.oiChange1h > 3.0 && fundingRate < -0.0003) {
                scoreLong += 0.30;
                flags.add("OI_SQUEEZE_SHORT");
            }

            // OI падает резко = тренд ослабевает
            if (fundingData.oiChange1h < -5.0) {
                scoreLong *= 0.80;
                scoreShort *= 0.80;
                flags.add("OI_DROP");
            }
        }

        // === STRUCTURE ===
        boolean pullbackUpFlag = pullback(c15, true);
        boolean bullStruct = bullishStructure(c15);
        boolean bearStruct = bearishStructure(c15);
        boolean pullbackDownFlag = pullback(c15, false);

        if (pullbackUpFlag && bullStruct) {
            scoreLong += 1.10;
            flags.add("pullback_long");
        }
        if (pullbackDownFlag && bearStruct) {
            scoreShort += 1.10;
            flags.add("pullback_short");
        }

        // === НОВОЕ: FAIR VALUE GAP (FVG) ===
        FVGResult fvg = detectFVG(c15);
        if (fvg.detected) {
            if (fvg.isBullish && price < fvg.gapHigh && price > fvg.gapLow) {
                scoreLong += 0.45;
                flags.add("FVG_BULL");
            } else if (!fvg.isBullish && price > fvg.gapLow && price < fvg.gapHigh) {
                scoreShort += 0.45;
                flags.add("FVG_BEAR");
            }
        }

        // === НОВОЕ: ORDER BLOCK ===
        OrderBlockResult ob = detectOrderBlock(c15);
        if (ob.detected) {
            if (ob.isBullish && price <= ob.zone * 1.005) {
                scoreLong += 0.50;
                flags.add("OB_BULL");
            } else if (!ob.isBullish && price >= ob.zone * 0.995) {
                scoreShort += 0.50;
                flags.add("OB_BEAR");
            }
        }

        // === PUMP DETECTOR ===
        PumpResult pump = detectPump(c1, c5, cat);
        if (pump.detected) {
            if (pump.direction > 0) {
                scoreLong += pump.strength;
                flags.add("PUMP_UP");
            } else {
                scoreShort += pump.strength;
                flags.add("PUMP_DOWN");
            }
        }

        // === IMPULSE ===
        boolean impulseFlag = impulse(c1);
        if (impulseFlag) {
            double atr1 = atr(c1, 14);
            double delta = last(c1).close - c1.get(c1.size() - 5).close;
            double impulseStrength = Math.abs(delta) / atr1;

            if (delta > 0 && impulseStrength > 0.30) {
                scoreLong += state == MarketState.STRONG_TREND ? 0.50 : 0.38;
                flags.add("impulse:true");
            }
            if (delta < 0 && impulseStrength > 0.30) {
                scoreShort += state == MarketState.STRONG_TREND ? 0.50 : 0.38;
                flags.add("impulse:true");
            }
        }

        // === COMPRESSION BREAKOUT ===
        CompressionResult compression = detectCompressionBreakout(c15, c1);
        if (compression.breakout) {
            if (compression.direction > 0) {
                scoreLong += 0.65;
                flags.add("compression_breakout_up");
            } else {
                scoreShort += 0.65;
                flags.add("compression_breakout_down");
            }
        }

        // === DIVERGENCE ===
        boolean bullDivFlag = bullDiv(c15);
        boolean bearDivFlag = bearDiv(c15);

        if (bullDivFlag) {
            scoreLong += 0.55;
            flags.add("bullish_div");
        }
        if (bearDivFlag) {
            scoreShort += 0.55;
            flags.add("bearish_div");
        }

        // === RSI FILTER ===
        double rsi14 = rsi(c15, 14);
        double rsi7 = rsi(c15, 7);

        // === ФИЛЬТР ПЛОХИХ ЛОНГОВ (в конце тренда) ===
        if (scoreLong > scoreShort) {
            if (isLongExhausted(c15, c1h, rsi14, rsi7, price)) {
                scoreLong *= 0.45;
                flags.add("exhausted_long_filtered");
            }
        }

        // === ФИЛЬТР ПЛОХИХ ШОРТОВ (в конце даунтренда) ===
        if (scoreShort > scoreLong) {
            if (isShortExhausted(c15, c1h, rsi14, rsi7, price)) {
                scoreShort *= 0.45;
                flags.add("exhausted_short_filtered");
            }
        }

        // RSI экстремумы (мягкий фильтр)
        if (state != MarketState.STRONG_TREND) {
            if (rsi14 > 78) scoreLong -= 0.22;
            if (rsi14 < 22) scoreShort -= 0.22;
        }

        // === ADX TREND CONFIRMATION ===
        double adxValue = adx(c15, 14);
        if (adxValue > 32) {
            if (bias1h == HTFBias.BULL && scoreShort > scoreLong) scoreShort *= 0.70;
            if (bias1h == HTFBias.BEAR && scoreLong > scoreShort) scoreLong *= 0.70;
        }

        // === НОВОЕ: 4H BIAS VETO (сильный фильтр) ===
        if (bias4h == HTFBias.BULL && scoreShort > scoreLong && adxValue > 25) {
            scoreShort *= 0.50; // Сильно режем шорты против 4H тренда
            flags.add("4H_VETO");
        }
        if (bias4h == HTFBias.BEAR && scoreLong > scoreShort && adxValue > 25) {
            scoreLong *= 0.50; // Сильно режем лонги против 4H тренда
            flags.add("4H_VETO");
        }

        // === VOLUME CONFIRMATION ===
        if (volumeSpike(c15, cat)) {
            if (scoreLong > scoreShort) scoreLong += 0.15;
            else scoreShort += 0.15;
            flags.add("vol:true");
        }

        // === TREND EXHAUSTION (движение слишком большое) ===
        double move8 = (last(c15).close - c15.get(c15.size() - 8).close) / price;
        if (move8 > 0.035 && scoreLong > scoreShort) {
            scoreLong *= 0.68;
        }
        if (move8 < -0.035 && scoreShort > scoreLong) {
            scoreShort *= 0.68;
        }

        // === MINIMUM SCORE DIFF ===
        double scoreDiff = Math.abs(scoreLong - scoreShort);
        if (scoreDiff < 0.22) return null;

        // === DYNAMIC THRESHOLD ===
        double dynamicThreshold = state == MarketState.STRONG_TREND ? 0.75 : 0.65;
        if (scoreLong < dynamicThreshold && scoreShort < dynamicThreshold) {
            if (!bullDivFlag && !bearDivFlag && !pump.detected && fundingData == null) {
                return null;
            }
        }

        com.bot.TradingCore.Side side = scoreLong > scoreShort
                ? com.bot.TradingCore.Side.LONG
                : com.bot.TradingCore.Side.SHORT;

        // === COOLDOWN & FLIP ===
        if (!cooldownAllowed(symbol, side, cat, now)) return null;
        if (!flipAllowed(symbol, side)) return null;

        // === НОВОЕ: CALIBRATED PROBABILITY ===
        double probability = computeCalibratedConfidence(
                symbol,
                scoreLong, scoreShort,
                state, cat,
                atr, price,
                bullDivFlag, bearDivFlag,
                pullbackUpFlag, pullbackDownFlag,
                impulseFlag, pump.detected,
                fundingData,
                bias4h,
                fvg.detected, ob.detected
        );

        if (probability < MIN_CONFIDENCE) {
            return null;
        }

        // === ATR FLAG ===
        if (atr / price > 0.0015) flags.add("ATR↑");

        // === RISK & STOP/TAKE ===
        double riskMult = cat == CoinCategory.MEME ? 1.35 : cat == CoinCategory.ALT ? 1.05 : 0.88;
        double rr = scoreDiff > 1.0 ? 3.2 : scoreDiff > 0.7 ? 2.7 : 2.3;

        // === НОВОЕ: Фиксированный тейк +2% для фьючерсов ===
        double targetPct = 0.02; // 2%
        double stopPct = targetPct / rr; // Динамический стоп под RR

        double stop = side == com.bot.TradingCore.Side.LONG
                ? price * (1 - stopPct)
                : price * (1 + stopPct);
        double take = side == com.bot.TradingCore.Side.LONG
                ? price * (1 + targetPct)
                : price * (1 - targetPct);

        if (!priceMovedEnough(symbol, price)) return null;
        registerSignal(symbol, side, now);

        return new TradeIdea(symbol, side, price, stop, take, probability, flags,
                fundingRate, oiChange, bias4h.name());
    }

    // === НОВОЕ: Адаптивный MIN_CONFIDENCE ===
    private void adaptMinConfidence(MarketState state, double atr, double price) {
        double volatility = atr / price;

        if (volatility < 0.008) {
            // Низкая волатильность → строже фильтры
            MIN_CONFIDENCE = 62.0;
        } else if (volatility > 0.025) {
            // Высокая волатильность → ловим импульсы
            MIN_CONFIDENCE = 54.0;
        } else {
            MIN_CONFIDENCE = 58.0;
        }

        // Сильный тренд → можем быть менее строгими
        if (state == MarketState.STRONG_TREND) {
            MIN_CONFIDENCE -= 3.0;
        }
    }

    // === НОВОЕ: 4H BIAS Detection ===
    private HTFBias detectBias4H(List<com.bot.TradingCore.Candle> c4h) {
        if (c4h == null || c4h.size() < 50) return HTFBias.NONE;

        double ema21 = ema(c4h, 21);
        double ema50 = ema(c4h, 50);
        double ema100 = c4h.size() >= 100 ? ema(c4h, 100) : ema50;
        double price = last(c4h).close;

        // Проверяем структуру: EMA21 > EMA50 > EMA100 = бычий тренд
        boolean bullishEMA = ema21 > ema50 && ema50 > ema100 * 0.998;
        boolean bearishEMA = ema21 < ema50 && ema50 < ema100 * 1.002;

        // Проверяем позицию цены относительно EMA
        boolean priceAboveEMA = price > ema21 && price > ema50;
        boolean priceBelowEMA = price < ema21 && price < ema50;

        // Проверяем Higher Highs / Lower Lows
        boolean hh_hl = checkHigherHighsHigherLows(c4h);
        boolean ll_lh = checkLowerLowsLowerHighs(c4h);

        // Комбинированное решение
        if (bullishEMA && priceAboveEMA && hh_hl) return HTFBias.BULL;
        if (bearishEMA && priceBelowEMA && ll_lh) return HTFBias.BEAR;
        if (bullishEMA && priceAboveEMA) return HTFBias.BULL;
        if (bearishEMA && priceBelowEMA) return HTFBias.BEAR;

        return HTFBias.NONE;
    }

    private boolean checkHigherHighsHigherLows(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 20) return false;
        int n = c.size();
        double high1 = c.subList(n - 20, n - 10).stream().mapToDouble(x -> x.high).max().orElse(0);
        double high2 = c.subList(n - 10, n).stream().mapToDouble(x -> x.high).max().orElse(0);
        double low1 = c.subList(n - 20, n - 10).stream().mapToDouble(x -> x.low).min().orElse(0);
        double low2 = c.subList(n - 10, n).stream().mapToDouble(x -> x.low).min().orElse(0);
        return high2 > high1 && low2 > low1;
    }

    private boolean checkLowerLowsLowerHighs(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 20) return false;
        int n = c.size();
        double high1 = c.subList(n - 20, n - 10).stream().mapToDouble(x -> x.high).max().orElse(0);
        double high2 = c.subList(n - 10, n).stream().mapToDouble(x -> x.high).max().orElse(0);
        double low1 = c.subList(n - 20, n - 10).stream().mapToDouble(x -> x.low).min().orElse(0);
        double low2 = c.subList(n - 10, n).stream().mapToDouble(x -> x.low).min().orElse(0);
        return high2 < high1 && low2 < low1;
    }

    // === НОВОЕ: Fair Value Gap (FVG) ===
    private static class FVGResult {
        final boolean detected;
        final boolean isBullish;
        final double gapLow;
        final double gapHigh;

        FVGResult(boolean detected, boolean isBullish, double gapLow, double gapHigh) {
            this.detected = detected;
            this.isBullish = isBullish;
            this.gapLow = gapLow;
            this.gapHigh = gapHigh;
        }
    }

    private FVGResult detectFVG(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 10) return new FVGResult(false, false, 0, 0);

        // Ищем FVG в последних 8 свечах
        for (int i = c.size() - 3; i >= c.size() - 8 && i >= 2; i--) {
            com.bot.TradingCore.Candle c1 = c.get(i - 1);
            com.bot.TradingCore.Candle c2 = c.get(i);     // импульсная свеча
            com.bot.TradingCore.Candle c3 = c.get(i + 1);

            double bodySize = Math.abs(c2.close - c2.open);
            double atr = atr(c.subList(Math.max(0, i - 14), i + 1), 14);

            // Bullish FVG: gap между high свечи 1 и low свечи 3
            if (c2.close > c2.open && bodySize > atr * 1.5) {
                double gapLow = c1.high;
                double gapHigh = c3.low;
                if (gapHigh > gapLow) {
                    return new FVGResult(true, true, gapLow, gapHigh);
                }
            }

            // Bearish FVG: gap между low свечи 1 и high свечи 3
            if (c2.close < c2.open && bodySize > atr * 1.5) {
                double gapHigh = c1.low;
                double gapLow = c3.high;
                if (gapHigh > gapLow) {
                    return new FVGResult(true, false, gapLow, gapHigh);
                }
            }
        }

        return new FVGResult(false, false, 0, 0);
    }

    // === НОВОЕ: Order Block Detection ===
    private static class OrderBlockResult {
        final boolean detected;
        final boolean isBullish;
        final double zone;

        OrderBlockResult(boolean detected, boolean isBullish, double zone) {
            this.detected = detected;
            this.isBullish = isBullish;
            this.zone = zone;
        }
    }

    private OrderBlockResult detectOrderBlock(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 15) return new OrderBlockResult(false, false, 0);

        // Ищем последнюю свечу перед импульсом
        for (int i = c.size() - 5; i >= c.size() - 12 && i >= 3; i--) {
            com.bot.TradingCore.Candle potential = c.get(i);

            // Проверяем, был ли импульс после этой свечи
            double moveAfter = 0;
            for (int j = i + 1; j < Math.min(i + 4, c.size()); j++) {
                moveAfter += c.get(j).close - c.get(j).open;
            }

            double atr = atr(c, 14);

            // Bullish Order Block: последняя красная свеча перед сильным ростом
            if (potential.close < potential.open && moveAfter > atr * 2.0) {
                return new OrderBlockResult(true, true, potential.low);
            }

            // Bearish Order Block: последняя зелёная свеча перед сильным падением
            if (potential.close > potential.open && moveAfter < -atr * 2.0) {
                return new OrderBlockResult(true, false, potential.high);
            }
        }

        return new OrderBlockResult(false, false, 0);
    }

    /* ===== PUMP DETECTOR ===== */
    private static class PumpResult {
        final boolean detected;
        final int direction;
        final double strength;

        PumpResult(boolean detected, int direction, double strength) {
            this.detected = detected;
            this.direction = direction;
            this.strength = strength;
        }
    }

    private PumpResult detectPump(List<com.bot.TradingCore.Candle> c1,
                                  List<com.bot.TradingCore.Candle> c5,
                                  CoinCategory cat) {
        if (c1.size() < 10 || c5.size() < 6) {
            return new PumpResult(false, 0, 0);
        }

        double atr1 = atr(c1, 14);
        com.bot.TradingCore.Candle last1 = last(c1);
        com.bot.TradingCore.Candle prev1 = c1.get(c1.size() - 2);

        double candleSize = Math.abs(last1.close - last1.open);
        double fullRange = last1.high - last1.low;

        double bodyRatio = candleSize / (fullRange + 1e-12);
        boolean bigCandle = fullRange > atr1 * 2.8;
        boolean strongBody = bodyRatio > 0.65;

        double avgVol = c1.subList(c1.size() - 8, c1.size() - 1)
                .stream()
                .mapToDouble(c -> c.volume)
                .average()
                .orElse(last1.volume);
        boolean volSpike = last1.volume > avgVol * 1.8;

        double move3m = last1.close - c1.get(c1.size() - 4).close;
        double movePct = Math.abs(move3m) / c1.get(c1.size() - 4).close;

        double pctThreshold = cat == CoinCategory.MEME ? 0.018 :
                cat == CoinCategory.ALT ? 0.022 : 0.025;

        if (bigCandle && strongBody && volSpike && movePct > pctThreshold) {
            int dir = move3m > 0 ? 1 : -1;
            double strength = 0.70 + Math.min(movePct * 10, 0.50);
            return new PumpResult(true, dir, strength);
        }

        int greenCount = 0, redCount = 0;
        double totalMove = 0;
        for (int i = c1.size() - 4; i < c1.size(); i++) {
            com.bot.TradingCore.Candle c = c1.get(i);
            if (c.close > c.open) greenCount++;
            else redCount++;
            totalMove += c.close - c.open;
        }

        double seriesMovePct = Math.abs(totalMove) / c1.get(c1.size() - 4).close;
        if ((greenCount >= 3 || redCount >= 3) && seriesMovePct > pctThreshold * 1.2 && volSpike) {
            int dir = totalMove > 0 ? 1 : -1;
            return new PumpResult(true, dir, 0.55);
        }

        return new PumpResult(false, 0, 0);
    }

    /* ===== COMPRESSION BREAKOUT ===== */
    private static class CompressionResult {
        final boolean breakout;
        final int direction;

        CompressionResult(boolean breakout, int direction) {
            this.breakout = breakout;
            this.direction = direction;
        }
    }

    private CompressionResult detectCompressionBreakout(List<com.bot.TradingCore.Candle> c15,
                                                        List<com.bot.TradingCore.Candle> c1) {
        if (c15.size() < 30 || c1.size() < 10) {
            return new CompressionResult(false, 0);
        }

        double atrRecent = atr(c15.subList(c15.size() - 8, c15.size()), 7);
        double atrPast = atr(c15.subList(c15.size() - 25, c15.size() - 10), 14);

        boolean compressed = atrRecent < atrPast * 0.55;

        if (!compressed) {
            return new CompressionResult(false, 0);
        }

        com.bot.TradingCore.Candle last1 = last(c1);
        double atr1 = atr(c1, 14);
        double breakoutMove = last1.close - c1.get(c1.size() - 4).close;

        if (Math.abs(breakoutMove) > atr1 * 1.8) {
            int dir = breakoutMove > 0 ? 1 : -1;
            return new CompressionResult(true, dir);
        }

        return new CompressionResult(false, 0);
    }

    /* ===== ФИЛЬТР ПЛОХИХ ЛОНГОВ ===== */
    private boolean isLongExhausted(List<com.bot.TradingCore.Candle> c15,
                                    List<com.bot.TradingCore.Candle> c1h,
                                    double rsi14, double rsi7,
                                    double price) {
        if (rsi14 > 75 && rsi7 > 78) return true;

        double ema21 = ema(c15, 21);
        double ema50 = ema(c15, 50);
        double distFromEma = (price - ema21) / ema21;

        if (distFromEma > 0.025 && rsi14 > 68) return true;

        if (c15.size() >= 6) {
            double recentVol = (c15.get(c15.size()-1).volume + c15.get(c15.size()-2).volume) / 2;
            double pastVol = (c15.get(c15.size()-4).volume + c15.get(c15.size()-5).volume) / 2;
            if (recentVol < pastVol * 0.65 && price > ema21) {
                return true;
            }
        }

        if (c1h.size() >= 8) {
            double rsiH1 = rsi(c1h, 14);
            if (rsiH1 > 78) return true;
        }

        return false;
    }

    /* ===== ФИЛЬТР ПЛОХИХ ШОРТОВ ===== */
    private boolean isShortExhausted(List<com.bot.TradingCore.Candle> c15,
                                     List<com.bot.TradingCore.Candle> c1h,
                                     double rsi14, double rsi7,
                                     double price) {
        if (rsi14 < 25 && rsi7 < 22) return true;

        double ema21 = ema(c15, 21);
        double distFromEma = (ema21 - price) / ema21;

        if (distFromEma > 0.025 && rsi14 < 32) return true;

        if (c1h.size() >= 8) {
            double rsiH1 = rsi(c1h, 14);
            if (rsiH1 < 22) return true;
        }

        return false;
    }

    /* ===== COOLDOWN ===== */
    private boolean cooldownAllowed(String symbol, com.bot.TradingCore.Side side, CoinCategory cat, long now) {
        String key = symbol + "_" + side;
        long base = cat == CoinCategory.TOP ? COOLDOWN_TOP :
                cat == CoinCategory.ALT ? COOLDOWN_ALT : COOLDOWN_MEME;

        Long last = cooldownMap.get(key);
        if (last != null && now - last < base) return false;

        cooldownMap.put(key, now);
        return true;
    }

    private boolean flipAllowed(String symbol, com.bot.TradingCore.Side newSide) {
        Deque<String> history = recentDirections.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        if (history.size() < 2) return true;

        Iterator<String> it = history.descendingIterator();
        String last = it.next();
        String prev = it.next();

        if (!last.equals(newSide.name()) && prev.equals(newSide.name())) return false;
        return true;
    }

    private void registerSignal(String symbol, com.bot.TradingCore.Side side, long now) {
        String key = symbol + "_" + side;
        cooldownMap.put(key, now);

        Deque<String> history = recentDirections.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        history.addLast(side.name());
        if (history.size() > 3) history.removeFirst();
    }

    /* ===== НОВОЕ: CALIBRATED CONFIDENCE ===== */
    private double computeCalibratedConfidence(String symbol,
                                               double scoreLong, double scoreShort,
                                               MarketState state, CoinCategory cat,
                                               double atr, double price,
                                               boolean bullDiv, boolean bearDiv,
                                               boolean pullbackUp, boolean pullbackDown,
                                               boolean impulse, boolean pump,
                                               FundingOIData fundingData,
                                               HTFBias bias4h,
                                               boolean hasFVG, boolean hasOB) {

        // === Базовый расчёт на основе разницы скоров ===
        double scoreDiff = Math.abs(scoreLong - scoreShort);
        double maxScore = 4.0; // Увеличен из-за новых индикаторов
        double normScore = scoreDiff / maxScore;

        // === Бонусы за подтверждения ===
        int confirmations = 0;

        if (bullDiv || bearDiv) { normScore += 0.05; confirmations++; }
        if (pullbackUp || pullbackDown) { normScore += 0.04; confirmations++; }
        if (impulse) { normScore += 0.03; confirmations++; }
        if (pump) { normScore += 0.06; confirmations++; }
        if (hasFVG) { normScore += 0.05; confirmations++; }
        if (hasOB) { normScore += 0.05; confirmations++; }

        // === Бонус за Funding Rate ===
        if (fundingData != null) {
            // Если funding подтверждает направление
            boolean fundingConfirmsLong = fundingData.fundingRate < -0.0003 && scoreLong > scoreShort;
            boolean fundingConfirmsShort = fundingData.fundingRate > 0.0003 && scoreShort > scoreLong;
            if (fundingConfirmsLong || fundingConfirmsShort) {
                normScore += 0.08;
                confirmations++;
            }
        }

        // === Бонус за 4H Bias ===
        boolean bias4hConfirms = (bias4h == HTFBias.BULL && scoreLong > scoreShort) ||
                (bias4h == HTFBias.BEAR && scoreShort > scoreLong);
        if (bias4hConfirms) {
            normScore += 0.07;
            confirmations++;
        }

        normScore = Math.min(1.0, normScore);

        // === Базовая вероятность ===
        // Калиброванная формула: probability = 50 + (normScore * range)
        // Range зависит от количества подтверждений
        double range = 30 + Math.min(confirmations * 3, 15); // 30-45%
        double probability = 50 + normScore * range;

        // === Корректировки по состоянию рынка ===
        if (state == MarketState.STRONG_TREND) probability += 3;
        else if (state == MarketState.WEAK_TREND) probability += 1;
        else if (state == MarketState.RANGE) probability -= 3;

        // === Корректировки по категории монеты ===
        if (cat == CoinCategory.MEME) probability -= 5;
        else if (cat == CoinCategory.ALT) probability -= 2;

        // === Калибровка на основе истории (если есть) ===
        Deque<CalibrationRecord> history = calibrationHistory.get(symbol);
        if (history != null && history.size() >= 20) {
            double avgAccuracy = calculateHistoricalAccuracy(history, probability);
            // Сдвигаем probability к исторической точности
            probability = probability * 0.7 + avgAccuracy * 0.3;
        }

        // === Финальное ограничение ===
        // Не даём probability > 85% (нет 100% сигналов в трейдинге)
        // Не даём probability < 50% (это не прогноз)
        probability = clamp(probability, 50, 85);

        return Math.round(probability);
    }

    private double calculateHistoricalAccuracy(Deque<CalibrationRecord> history, double currentProb) {
        // Считаем точность для сигналов с похожей probability
        double sumCorrect = 0;
        double count = 0;

        for (CalibrationRecord r : history) {
            if (Math.abs(r.predictedProb - currentProb) < 10) {
                count++;
                if (r.wasCorrect) sumCorrect++;
            }
        }

        if (count < 5) return currentProb;
        return (sumCorrect / count) * 100;
    }

    // === Метод для записи результата сигнала (вызывать после закрытия сделки) ===
    public void recordSignalResult(String symbol, double predictedProb, boolean wasCorrect) {
        Deque<CalibrationRecord> history = calibrationHistory.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        history.addLast(new CalibrationRecord(predictedProb, wasCorrect));
        while (history.size() > CALIBRATION_WINDOW) {
            history.removeFirst();
        }
    }

    /* ===== INDICATORS ===== */
    private MarketState detectState(List<com.bot.TradingCore.Candle> c) {
        double adx = adx(c, 14);
        if (adx > 28) return MarketState.STRONG_TREND;
        if (adx > 20) return MarketState.WEAK_TREND;
        return MarketState.RANGE;
    }

    private HTFBias detectBias(List<com.bot.TradingCore.Candle> c) {
        if (!valid(c)) return HTFBias.NONE;
        double ema50 = ema(c, 50);
        double ema200 = ema(c, 200);
        if (ema50 > ema200 * 1.002) return HTFBias.BULL;
        if (ema50 < ema200 * 0.998) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    public double atr(List<com.bot.TradingCore.Candle> c, int n) {
        if (c.size() < n + 1) return 0;
        double sum = 0;
        for (int i = c.size() - n; i < c.size(); i++) {
            var cur = c.get(i);
            var prev = c.get(i - 1);
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close), Math.abs(cur.low - prev.close)));
            sum += tr;
        }
        return sum / n;
    }

    private double adx(List<com.bot.TradingCore.Candle> c, int n) {
        if (c.size() < n + 1) return 15;
        double trSum = 0, plusDM = 0, minusDM = 0;

        for (int i = c.size() - n; i < c.size(); i++) {
            var cur = c.get(i);
            var prev = c.get(i - 1);
            double highDiff = cur.high - prev.high;
            double lowDiff = prev.low - cur.low;
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close), Math.abs(cur.low - prev.close)));
            trSum += tr;
            if (highDiff > lowDiff && highDiff > 0) plusDM += highDiff;
            if (lowDiff > highDiff && lowDiff > 0) minusDM += lowDiff;
        }

        double atrVal = trSum / n;
        double plusDI = 100 * (plusDM / n) / atrVal;
        double minusDI = 100 * (minusDM / n) / atrVal;
        return 100 * Math.abs(plusDI - minusDI) / Math.max(plusDI + minusDI, 1);
    }

    private double ema(List<com.bot.TradingCore.Candle> c, int p) {
        if (c.size() < p) return last(c).close;
        double k = 2.0 / (p + 1);
        double e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++) {
            e = c.get(i).close * k + e * (1 - k);
        }
        return e;
    }

    public double rsi(List<com.bot.TradingCore.Candle> c, int period) {
        if (c.size() < period + 1) return 50.0;
        double gain = 0, loss = 0;
        for (int i = c.size() - period; i < c.size(); i++) {
            double change = c.get(i).close - c.get(i - 1).close;
            if (change > 0) gain += change;
            else loss -= change;
        }
        double rs = loss == 0 ? 100 : gain / loss;
        return 100 - (100 / (1 + rs));
    }

    private boolean bullDiv(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 25) return false;
        int i1 = c.size() - 8;
        int i2 = c.size() - 1;
        double low1 = c.get(i1).low;
        double low2 = c.get(i2).low;
        double rsi1 = rsi(c.subList(0, i1 + 1), 14);
        double rsi2 = rsi(c, 14);
        return low2 < low1 * 0.998 && rsi2 > rsi1 + 3;
    }

    private boolean bearDiv(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 25) return false;
        int i1 = c.size() - 8;
        int i2 = c.size() - 1;
        double high1 = c.get(i1).high;
        double high2 = c.get(i2).high;
        double rsi1 = rsi(c.subList(0, i1 + 1), 14);
        double rsi2 = rsi(c, 14);
        return high2 > high1 * 1.002 && rsi2 < rsi1 - 3;
    }

    public boolean impulse(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 15) return false;
        double atrVal = atr(c, 14);
        return Math.abs(last(c).close - c.get(c.size() - 5).close) > atrVal * 0.58;
    }

    public boolean volumeSpike(List<com.bot.TradingCore.Candle> c, CoinCategory cat) {
        if (c.size() < 10) return false;
        double avg = c.subList(c.size() - 10, c.size() - 1)
                .stream().mapToDouble(cd -> cd.volume).average().orElse(1);
        double lastVol = last(c).volume;
        double threshold = cat == CoinCategory.MEME ? 1.30 : cat == CoinCategory.ALT ? 1.22 : 1.15;
        return lastVol / avg > threshold;
    }

    private boolean pullback(List<com.bot.TradingCore.Candle> c, boolean bull) {
        double ema21 = ema(c, 21);
        double price = last(c).close;
        return bull ? price <= ema21 * 0.997 : price >= ema21 * 1.003;
    }

    private boolean bullishStructure(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 12) return false;
        double low1 = c.get(c.size() - 8).low;
        double low2 = c.get(c.size() - 4).low;
        double high1 = c.get(c.size() - 8).high;
        double high2 = c.get(c.size() - 4).high;
        return high2 > high1 && low2 > low1;
    }

    private boolean bearishStructure(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 12) return false;
        double low1 = c.get(c.size() - 8).low;
        double low2 = c.get(c.size() - 4).low;
        double high1 = c.get(c.size() - 8).high;
        double high2 = c.get(c.size() - 4).high;
        return high2 < high1 && low2 < low1;
    }

    private com.bot.TradingCore.Candle last(List<com.bot.TradingCore.Candle> c) {
        return c.get(c.size() - 1);
    }

    private boolean valid(List<?> c) {
        return c != null && c.size() >= MIN_BARS;
    }

    private boolean priceMovedEnough(String symbol, double price) {
        Double last = lastSignalPrice.get(symbol);
        if (last == null) {
            lastSignalPrice.put(symbol, price);
            return true;
        }
        double diff = Math.abs(price - last) / last;
        if (diff < 0.0025) return false;
        lastSignalPrice.put(symbol, price);
        return true;
    }

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    // === СТАРЫЙ МЕТОД analyze (для обратной совместимости) ===
    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             CoinCategory cat) {
        long now = System.currentTimeMillis();
        return generate(symbol, c1, c5, c15, c1h, null, cat, now);
    }

    // === НОВЫЙ МЕТОД analyze с 4H ===
    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             List<com.bot.TradingCore.Candle> c4h,
                             CoinCategory cat) {
        long now = System.currentTimeMillis();
        return generate(symbol, c1, c5, c15, c1h, c4h, cat, now);
    }
}
