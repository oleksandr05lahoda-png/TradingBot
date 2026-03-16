package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DecisionEngineMerged {

    public enum CoinCategory { TOP, ALT, MEME }
    public enum MarketState { STRONG_TREND, WEAK_TREND, RANGE }
    public enum HTFBias { BULL, BEAR, NONE }

    private static final int MIN_BARS = 150;

    private static final long COOLDOWN_TOP = 15 * 60_000;
    private static final long COOLDOWN_ALT = 15 * 60_000;
    private static final long COOLDOWN_MEME = 15 * 60_000;
    private final Map<String, Double> lastSignalPrice = new ConcurrentHashMap<>();

    private double MIN_CONFIDENCE = 58.0;

    private final Map<String, Long> cooldownMap = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>> recentDirections = new ConcurrentHashMap<>();

    private final Map<String, FundingOIData> fundingOICache = new ConcurrentHashMap<>();

    private final Map<String, Deque<CalibrationRecord>> calibrationHistory = new ConcurrentHashMap<>();
    private static final int CALIBRATION_WINDOW = 100;

    // === НОВОЕ: Anti-lag детекция ===
    private final Map<String, ExhaustionContext> exhaustionContexts = new ConcurrentHashMap<>();
    private final Map<String, ReverseContext> reverseContexts = new ConcurrentHashMap<>();

    private PumpHunter pumpHunter;

    public DecisionEngineMerged() {
    }

    public void setPumpHunter(PumpHunter pumpHunter) {
        this.pumpHunter = pumpHunter;
    }

    // === НОВЫЕ КОНТЕКСТЫ ===
    private static final class ExhaustionContext {
        long firstDetectionTime;
        int exhaustionCount;
        double lastMomentum;
        double lastRSI;

        ExhaustionContext() {
            this.firstDetectionTime = System.currentTimeMillis();
            this.exhaustionCount = 1;
        }
    }


    private static final class ReverseContext {
        long detectionTime;
        double divergenceStrength;
        List<String> reverseReasons;

        ReverseContext(double divergenceStrength, List<String> reasons) {
            this.detectionTime = System.currentTimeMillis();
            this.divergenceStrength = divergenceStrength;
            this.reverseReasons = reasons;
        }
    }

    public static final class FundingOIData {
        public final double fundingRate;
        public final double openInterest;
        public final double oiChange1h;
        public final double oiChange4h;
        public final long timestamp;

        public FundingOIData(double fundingRate, double openInterest, double oiChange1h, double oiChange4h) {
            this.fundingRate = fundingRate;
            this.openInterest = openInterest;
            this.oiChange1h = oiChange1h;
            this.oiChange4h = oiChange4h;
            this.timestamp = System.currentTimeMillis();
        }

        public boolean isValid() {
            return System.currentTimeMillis() - timestamp < 5 * 60_000;
        }
    }

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

    public void updateFundingOI(String symbol, double fundingRate, double openInterest, double oiChange1h, double oiChange4h) {
        fundingOICache.put(symbol, new FundingOIData(fundingRate, openInterest, oiChange1h, oiChange4h));
    }

    public FundingOIData getFundingOI(String symbol) {
        FundingOIData data = fundingOICache.get(symbol);
        if (data != null && data.isValid()) {
            return data;
        }
        return null;
    }

    /* ===== MAIN GENERATE - ПЕРЕДЕЛАНО: Anti-Lag + Reverse Detection ===== */
    private TradeIdea generate(String symbol,
                               List<com.bot.TradingCore.Candle> c1,
                               List<com.bot.TradingCore.Candle> c5,
                               List<com.bot.TradingCore.Candle> c15,
                               List<com.bot.TradingCore.Candle> c1h,
                               List<com.bot.TradingCore.Candle> c2h,
                               CoinCategory cat,
                               long now) {

        if (!valid(c15) || !valid(c1h)) {
            return null;
        }

        double price = last(c15).close;
        double atr = atr(c15, 14);
        double lastRange = last(c15).high - last(c15).low;

        if (lastRange > atr * 4.0) {
            return null;
        }
        if (atr <= 0) return null;

        atr = Math.max(atr, price * 0.0012);

        MarketState state = detectState(c15);
        HTFBias bias1h = detectBias(c1h);
        HTFBias bias2h = c2h != null && c2h.size() >= 50 ? detectBias2H(c2h) : HTFBias.NONE;

        adaptMinConfidence(state, atr, price);

        if (state == MarketState.RANGE) {
            double adxVal = adx(c15, 14);
            if (adxVal < 20) {
                return null;
            }
        }

        double scoreLong = 0;
        double scoreShort = 0;
        List<String> flags = new ArrayList<>();

        // === НОВОЕ: Anti-Lag Detection (ловим движение ДО конца свечи) ===
        AntiLagSignal antiLag = detectAntiLag(c1, c5, c15);
        if (antiLag != null && antiLag.strength > 0.45) {
            if (antiLag.direction > 0) {
                scoreLong += antiLag.strength * 1.2;
                flags.add("EARLY_PUMP");
            } else {
                scoreShort += antiLag.strength * 1.2;
                flags.add("EARLY_DUMP");
            }
        }

        ReverseWarning reverseWarning = detectReversePattern(symbol, c15, c1h, state);
        if (reverseWarning != null && reverseWarning.confidence > 0.55) {
            flags.add("⚠REVERSE_" + reverseWarning.type);

            if (reverseWarning.type.equals("LONG_EXHAUSTION")) {
                scoreLong *= 0.20;  // УСИЛЕНО: 0.40 → 0.20 (сильнее режем)
                if (scoreLong < 0.3) return null;  // НОВОЕ: если совсем низко - отклоняем
            } else if (reverseWarning.type.equals("SHORT_EXHAUSTION")) {
                scoreShort *= 0.20;
                if (scoreShort < 0.3) return null;
            }
        }

        if (bias1h == HTFBias.BULL) {
            scoreLong += 0.50;
            scoreShort -= 0.20;
            flags.add("1H_BULL");
        } else if (bias1h == HTFBias.BEAR) {
            scoreShort += 0.50;
            scoreLong -= 0.20;
            flags.add("1H_BEAR");
        }

        if (bias2h == HTFBias.BULL) {
            scoreLong += 0.40;
            scoreShort -= 0.25;
            flags.add("2H_BULL");
        } else if (bias2h == HTFBias.BEAR) {
            scoreShort += 0.40;
            scoreLong -= 0.25;
            flags.add("2H_BEAR");
        }
// === НОВОЕ: ANTI-CONFLICT - если 1H и 2H противоречат, блокируем слабые сигналы ===
        if ((bias1h == HTFBias.BULL && bias2h == HTFBias.BEAR) ||
                (bias1h == HTFBias.BEAR && bias2h == HTFBias.BULL)) {

            // Если противоречие, то требуем ОЧЕНЬ сильный сигнал
            if (scoreLong < 1.5) {
                scoreLong *= 0.25;  // Режем лонг при противоречии
                flags.add("HTF_CONFLICT");
            }
            if (scoreShort < 1.5) {
                scoreShort *= 0.25;  // Режем шорт при противоречии
                flags.add("HTF_CONFLICT");
            }
        }
        // === PumpHunter Integration ===
        if (pumpHunter != null) {
            PumpHunter.PumpEvent pump = pumpHunter.detectPump(symbol, c1, c5, c15);
            if (pump != null && pump.strength > 0.4) {
                double bonus = pump.strength * 0.5;
                if (!pump.isConfirmed) {
                    bonus *= 0.5;
                }
                if (pump.isBullish()) {
                    scoreLong += 0.60 + bonus;
                    flags.add("PUMP_UP_" + String.format("%.0f", pump.strength * 100));
                } else if (pump.isBearish()) {
                    scoreShort += 0.60 + bonus;
                    flags.add("PUMP_DOWN_" + String.format("%.0f", pump.strength * 100));
                }

                if (pump.isMega()) {
                    if (pump.isBullish()) scoreLong += 0.30;
                    else scoreShort += 0.30;
                    flags.add("MEGA");
                }
            }
        }

        // === FUNDING RATE FILTER ===
        FundingOIData fundingData = getFundingOI(symbol);
        double fundingRate = 0;
        double oiChange = 0;

        if (fundingData != null) {
            fundingRate = fundingData.fundingRate;
            oiChange = fundingData.oiChange1h;

            if (fundingRate > 0.0005) {
                scoreShort += 0.35 + Math.min(fundingRate * 100, 0.40);
                scoreLong -= 0.25;
                flags.add("FR_HIGH");
            } else if (fundingRate < -0.0005) {
                scoreLong += 0.35 + Math.min(Math.abs(fundingRate) * 100, 0.40);
                scoreShort -= 0.25;
                flags.add("FR_LOW");
            }

            if (fundingData.oiChange1h > 3.0 && fundingRate > 0.0003) {
                scoreShort += 0.30;
                flags.add("OI_SQUEEZE_LONG");
            }
            if (fundingData.oiChange1h > 3.0 && fundingRate < -0.0003) {
                scoreLong += 0.30;
                flags.add("OI_SQUEEZE_SHORT");
            }

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

        // === FAIR VALUE GAP (FVG) ===
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

        // === ORDER BLOCK ===
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

        // === PUMP DETECTOR (старый метод) ===
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

        boolean impulseFlag = impulse(c1);
        if (impulseFlag) {
            double atr1 = atr(c1, 14);
            double delta = last(c1).close - c1.get(c1.size() - 5).close;
            double impulseStrength = Math.abs(delta) / atr1;

            // === ДОБАВЛЕНО: Проверка согласованности импульса с ценой ===
            if (delta > 0 && impulseStrength > 0.30) {
                // Импульс вверх - бонус ТОЛЬКО если нет медвежьего 2H
                if (bias2h != HTFBias.BEAR) {
                    scoreLong += state == MarketState.STRONG_TREND ? 0.50 : 0.38;
                    flags.add("impulse:up");
                } else {
                    scoreLong += 0.15;  // Очень маленький бонус при конфликте
                    flags.add("impulse:up_conflict");
                }
            }
            if (delta < 0 && impulseStrength > 0.30) {
                // Импульс вниз - бонус ТОЛЬКО если нет бычьего 2H
                if (bias2h != HTFBias.BULL) {
                    scoreShort += state == MarketState.STRONG_TREND ? 0.50 : 0.38;
                    flags.add("impulse:down");
                } else {
                    scoreShort += 0.15;
                    flags.add("impulse:down_conflict");
                }
            }
        }

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
        if (bearDivFlag && scoreLong > scoreShort) {
            flags.remove("bullish_div");  // убираем бычий флаг если есть медвежий
        }
        if (bullDivFlag && scoreShort > scoreLong) {
            flags.remove("bearish_div");  // убираем медвежий флаг если есть бычий
        }
        // === RSI FILTER ===
        double rsi14 = rsi(c15, 14);
        double rsi7 = rsi(c15, 7);

        if (bearDivFlag && scoreLong > scoreShort) {
            scoreLong *= 0.35;
            flags.add("bearish_div_vs_long");
        }
        if (bullDivFlag && scoreShort > scoreLong) {
            scoreShort *= 0.35;
            flags.add("bullish_div_vs_short");
        }

        // === ADX и его изменение ===
        double adxValue = adx(c15, 14);
        double adxPrev = adx(c15.subList(0, c15.size()-1), 14);
        boolean adxFalling = adxPrev > adxValue;

        // === ФИЛЬТР ПЛОХИХ ЛОНГОВ ===
        if (scoreLong > scoreShort) {
            boolean exhausted = isLongExhausted(c15, c1h, rsi14, rsi7, price);
            if (adxValue > 30 && adxFalling) exhausted = true;
            if (bearDivFlag) {
                exhausted = true;
                flags.add("bearish_div_rejection");  // ПЕРЕИМЕНОВАНО для ясности
            }
            if (exhausted) {
                scoreLong *= 0.30;  // УСИЛЕНО: 0.45 �� 0.30
                flags.add("exhausted_long_filtered");
            }
        }

        // === ФИЛЬТР ПЛОХИХ ШОРТОВ ===
        if (scoreShort > scoreLong) {
            boolean exhausted = isShortExhausted(c15, c1h, rsi14, rsi7, price);
            if (adxValue > 30 && adxFalling) exhausted = true;
            if (bullDivFlag) exhausted = true;

            // НОВОЕ: если SHORT против бычьего 1H/2H = extra penalty
            if ((bias1h == HTFBias.BULL || bias2h == HTFBias.BULL) && scoreShort < 1.0) {
                exhausted = true;
                flags.add("short_vs_bull_bias");
            }

            if (exhausted) {
                scoreShort *= 0.25;  // БЫЛО 0.45 → УСИЛЕНО 0.25
                if (scoreShort < 0.2) return null;  // НОВОЕ: полная блокировка
                flags.add("exhausted_short_filtered");
            }
        }

        double ema50 = ema(c15, 50);
        double deviation = (price - ema50) / ema50;
        if (scoreLong > scoreShort && deviation > 0.06) {
            scoreLong *= 0.5;
            flags.add("overextended_long");
        }
        if (scoreShort > scoreLong && deviation < -0.06) {
            scoreShort *= 0.5;
            flags.add("overextended_short");
        }

        if (state != MarketState.STRONG_TREND) {
            if (rsi14 > 78) scoreLong -= 0.22;
            if (rsi14 < 22) scoreShort -= 0.22;
        }

        if (adxValue > 32) {
            if (bias1h == HTFBias.BULL && scoreShort > scoreLong) scoreShort *= 0.70;
            if (bias1h == HTFBias.BEAR && scoreLong > scoreShort) scoreLong *= 0.70;
        }

        if (bias2h == HTFBias.BULL && scoreShort > scoreLong && adxValue > 25) {
            scoreShort *= 0.15;  // БЫЛО 0.55 → УСИЛЕНО ДО 0.15
            flags.add("2H_VETO");
            if (scoreShort < 0.2) return null;  // НОВОЕ: полная блокировка слабых сигналов
        }

        if (bias2h == HTFBias.BEAR && scoreLong > scoreShort && adxValue > 25) {
            scoreLong *= 0.15;  // БЫЛО 0.55 → 0.15
            flags.add("2H_VETO");
            if (scoreLong < 0.2) return null;  // НОВОЕ
        }
        if (bias1h == bias2h && bias1h != HTFBias.NONE) {
            if (bias1h == HTFBias.BULL) {
                scoreLong += 0.50;
                scoreShort -= 0.35;  // БЫЛО -0.20 → -0.35 (сильнее штрафуем SHORT при BULL 1H)
                flags.add("1H_BULL");
            } else if (bias1h == HTFBias.BEAR) {
                scoreShort += 0.50;
                scoreLong -= 0.35;  // БЫЛО -0.20 → -0.35
                flags.add("1H_BEAR");
            }
        }

        // === VOLUME CONFIRMATION ===
        if (volumeSpike(c15, cat)) {
            if (scoreLong > scoreShort) scoreLong += 0.15;
            else scoreShort += 0.15;
            flags.add("vol:true");
        }

        // === TREND EXHAUSTION ===
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

        if (!cooldownAllowed(symbol, side, cat, now)) return null;
        if (!flipAllowed(symbol, side)) return null;

        double probability = computeCalibratedConfidence(
                symbol,
                scoreLong, scoreShort,
                state, cat,
                atr, price,
                bullDivFlag, bearDivFlag,
                pullbackUpFlag, pullbackDownFlag,
                impulseFlag, pump.detected,
                fundingData,
                bias2h,
                fvg.detected, ob.detected
        );

        if (probability < MIN_CONFIDENCE) {
            return null;
        }

        if (atr / price > 0.0015) flags.add("ATR↑");

        double riskMult = cat == CoinCategory.MEME ? 1.35 : cat == CoinCategory.ALT ? 1.05 : 0.88;
        double rr = scoreDiff > 1.0 ? 3.2 : scoreDiff > 0.7 ? 2.7 : 2.3;

        double stopDist = atr * 1.8 * riskMult;

        double stop = side == com.bot.TradingCore.Side.LONG
                ? price - stopDist
                : price + stopDist;

        double take = side == com.bot.TradingCore.Side.LONG
                ? price + (stopDist * rr)
                : price - (stopDist * rr);

        if (!priceMovedEnough(symbol, price)) return null;
        registerSignal(symbol, side, now);

        return new TradeIdea(symbol, side, price, stop, take, probability, flags,
                fundingRate, oiChange, bias2h.name());
    }

    // ============= НОВЫЕ МЕТОДЫ: Anti-Lag & Reverse Detection =============

    private static class AntiLagSignal {
        int direction;       // 1 = up, -1 = down
        double strength;     // 0.0 - 1.0
        int barsAgo;

        AntiLagSignal(int direction, double strength, int barsAgo) {
            this.direction = direction;
            this.strength = strength;
            this.barsAgo = barsAgo;
        }
    }

    private AntiLagSignal detectAntiLag(List<com.bot.TradingCore.Candle> c1,
                                        List<com.bot.TradingCore.Candle> c5,
                                        List<com.bot.TradingCore.Candle> c15) {

        if (c1.size() < 5 || c5.size() < 3) return null;

        int n1 = c1.size();
        int n5 = c5.size();

        double atr1 = atr(c1, 14);
        double avgVol1 = c1.subList(Math.max(0, n1-15), n1-1).stream()
                .mapToDouble(c -> c.volume).average().orElse(1);

        com.bot.TradingCore.Candle curr1m = c1.get(n1 - 1);
        com.bot.TradingCore.Candle prev1m = c1.get(n1 - 2);

        double currBody = Math.abs(curr1m.close - curr1m.open);
        double prevBody = Math.abs(prev1m.close - prev1m.open);
        double currVolRatio = curr1m.volume / avgVol1;

        // ✅ ДОБАВЛЕНО: Проверка силы тренда через скорость
        double speedLast2 = Math.abs(curr1m.close - c1.get(Math.max(0, n1-3)).close);
        double speedPrev2 = Math.abs(c1.get(Math.max(0, n1-3)).close - c1.get(Math.max(0, n1-5)).close);
        boolean accelerating = speedLast2 > speedPrev2 * 1.2 && speedLast2 > atr1 * 0.8;

        // === АГРЕССИВНАЯ ДЕТЕКЦИЯ ===

        // Вариант 1: Одна мощная свеча (памп) — УСИЛЕНО
        if (currBody > atr1 * 1.5 && currVolRatio > 1.4 && accelerating) {  // БЫЛО 1.7 и 1.5
            int dir = curr1m.close > curr1m.open ? 1 : -1;
            double strength = Math.min(0.85, (currBody / atr1 - 1.3) * 0.50);  // БЫЛО 0.80, 1.5, 0.45
            if (strength > 0.40) {  // БЫЛО 0.45
                return new AntiLagSignal(dir, strength, 0);
            }
        }

        // Вариант 2: Две сильные подряд — УСИЛЕНО
        if (currBody > atr1 * 1.2 && prevBody > atr1 * 1.1 && accelerating) {  // БЫЛО 1.3, 1.2
            double totalMove = curr1m.close - prev1m.open;
            int dir = totalMove > 0 ? 1 : -1;
            double strength = Math.min(0.75, Math.abs(totalMove) / atr1 * 0.45);  // БЫЛО 0.70, 0.40
            if (strength > 0.40) {
                return new AntiLagSignal(dir, strength, 1);
            }
        }

        // Вариант 3: На 5M большой разрыв — НОВОЕ
        com.bot.TradingCore.Candle curr5m = c5.get(n5 - 1);
        double atr5 = atr(c5, 14);
        double body5m = Math.abs(curr5m.close - curr5m.open);
        double avgVol5 = c5.subList(Math.max(0, n5-10), n5-1).stream()
                .mapToDouble(c -> c.volume).average().orElse(1);

        if (body5m > atr5 * 1.8 && curr5m.volume > avgVol5 * 1.3) {  // БЫЛО 2.0, 1.4
            int dir = curr5m.close > curr5m.open ? 1 : -1;
            double strength = Math.min(0.80, (body5m / atr5 - 1.6) * 0.40);  // БЫЛО 0.75, 1.8, 0.35
            if (strength > 0.40) {
                return new AntiLagSignal(dir, strength, 0);
            }
        }

        // Вариант 4: Быстрая серия из 3-4 монолитных свечей — НОВОЕ
        int greenCount = 0, redCount = 0;
        double seriesMove = 0;
        for (int i = Math.max(0, n1 - 4); i < n1; i++) {
            com.bot.TradingCore.Candle c = c1.get(i);
            if (c.close > c.open) greenCount++;
            else redCount++;
            seriesMove += c.close - c.open;
        }

        if ((greenCount >= 3 || redCount >= 3) && Math.abs(seriesMove) > atr1 * 1.5) {
            int dir = seriesMove > 0 ? 1 : -1;
            double strength = Math.min(0.70, Math.abs(seriesMove) / atr1 * 0.35);
            if (strength > 0.38) {
                return new AntiLagSignal(dir, strength, 0);
            }
        }

        return null;
    }

    private static class ReverseWarning {
        String type;           // "LONG_EXHAUSTION", "SHORT_EXHAUSTION", "REVERSAL"
        double confidence;     // 0.0 - 1.0
        List<String> reasons;

        ReverseWarning(String type, double confidence, List<String> reasons) {
            this.type = type;
            this.confidence = confidence;
            this.reasons = reasons;
        }
    }

    private ReverseWarning detectReversePattern(String symbol,
                                                List<com.bot.TradingCore.Candle> c15,
                                                List<com.bot.TradingCore.Candle> c1h,
                                                MarketState state) {

        if (c15.size() < 8 || c1h.size() < 5) return null;

        List<String> warnings = new ArrayList<>();
        double warningScore = 0.0;

        // === ФАКТОР 1: RSI экстремумы (БЕЗ ИЗМЕНЕНИЙ) ===
        double rsi1h = rsi(c1h, 14);
        if (rsi1h > 75.0) {
            warnings.add("RSI1H_OVERBOUGHT");
            warningScore += 0.25;
        }
        if (rsi1h < 25.0) {
            warnings.add("RSI1H_OVERSOLD");
            warningScore += 0.25;
        }

        // === ФАКТОР 2: Momentum divergence — УСИЛЕНО ===
        double momentum15_curr = calculateMomentum(c15, 5);
        double momentum15_prev = calculateMomentumPrev(c15, 5, 5);
        double momentum15_prev2 = calculateMomentumPrev(c15, 5, 10);

        boolean momLongFading = momentum15_curr < momentum15_prev * 0.65 &&  // БЫЛО 0.70
                momentum15_prev < momentum15_prev2 * 0.70 &&  // БЫЛО 0.75
                momentum15_prev > 0;
        if (momLongFading) {
            warnings.add("MOMENTUM_DIV_LONG");
            warningScore += 0.45;  // БЫЛО 0.40 — УСИЛИЛИ
        }

        boolean momShortFading = momentum15_curr > momentum15_prev * 1.35 &&  // БЫЛО 1.30
                momentum15_prev > momentum15_prev2 * 1.30 &&  // БЫЛО 1.25
                momentum15_prev < 0;
        if (momShortFading) {
            warnings.add("MOMENTUM_DIV_SHORT");
            warningScore += 0.45;  // БЫЛО 0.40
        }

        // === ФАКТОР 3: Volume collapse ===
        double avgVol = c15.subList(Math.max(0, c15.size()-15), c15.size()-3).stream()
                .mapToDouble(c -> c.volume).average().orElse(1);
        double lastVol = last(c15).volume;

        if (lastVol < avgVol * 0.55) {  // БЫЛО 0.60 — УЖЕСТОЧИЛИ
            warnings.add("VOL_COLLAPSE");
            warningScore += 0.30;  // БЫЛО 0.25
        }

        // === ФАКТОР 4: Wick rejection (ВОТ ТУТ ГЛАВНОЕ) ===
        com.bot.TradingCore.Candle lastC = last(c15);
        double upperWick = lastC.high - Math.max(lastC.open, lastC.close);
        double lowerWick = Math.min(lastC.open, lastC.close) - lastC.low;
        double body = Math.abs(lastC.close - lastC.open);

        // ОЧЕНЬ ВАЖНО: большой wick = отскок от уровня!
        if (upperWick > body * 2.5 && lastC.close < lastC.open) {  // БЫЛО 2.0
            warnings.add("UPPER_WICK_REJECTION");
            warningScore += 0.35;  // БЫЛО 0.25
        }
        if (lowerWick > body * 2.5 && lastC.close > lastC.open) {
            warnings.add("LOWER_WICK_REJECTION");
            warningScore += 0.35;
        }

        // === ФАКТОР 5: ADX falling (свеча слабеет) ===
        double adxVal = adx(c15, 14);
        if (c15.size() >= 2) {
            double adxPrev = adx(c15.subList(0, c15.size()-1), 14);
            if (adxPrev > adxVal && adxVal > 22) {  // БЫЛО 25
                warnings.add("ADX_FALLING");
                warningScore += 0.25;  // БЫЛО 0.20
            }
        }

        // === ФАКТОР 6: Three Candle Exhaustion — НОВОЕ ПРАВИЛО! ===
        if (c15.size() >= 3) {
            com.bot.TradingCore.Candle c1 = c15.get(c15.size()-3);
            com.bot.TradingCore.Candle c2 = c15.get(c15.size()-2);
            com.bot.TradingCore.Candle c3 = c15.get(c15.size()-1);

            // Три зелёные с УМЕНЬШАЮЩИМСЯ телом = усталость быков
            boolean threeGreenDecline = c1.close > c1.open && c2.close > c2.open && c3.close > c3.open &&
                    Math.abs(c1.close - c1.open) > Math.abs(c2.close - c2.open) * 1.05 &&
                    Math.abs(c2.close - c2.open) > Math.abs(c3.close - c3.open) * 1.05;

            if (threeGreenDecline) {
                warnings.add("THREE_CANDLE_EXHAUSTION");
                warningScore += 0.30;  // БЫЛО 0.20
            }
        }

        // === ФАКТОР 7: НОВОЕ — Хвост против движения! ===
        if (c15.size() >= 2) {
            com.bot.TradingCore.Candle curr = c15.get(c15.size()-1);
            com.bot.TradingCore.Candle prev = c15.get(c15.size()-2);

            // Если пре��ыдущая была БОЛЬШАЯ зелёная, а нынешняя — маленькая с верхним хвостом
            if (prev.close > prev.open && curr.high > prev.high) {
                double prevBody = prev.close - prev.open;
                double currBody = Math.abs(curr.close - curr.open);
                if (prevBody > currBody * 2.0 && upperWick > body * 1.8) {
                    warnings.add("TAIL_REJECTION_AFTER_PUMP");
                    warningScore += 0.35;
                }
            }
        }

        if (warningScore < 0.55) return null;
        String reverseType = rsi1h > 75.0 ? "LONG_EXHAUSTION" :
                rsi1h < 25.0 ? "SHORT_EXHAUSTION" : "REVERSAL";

        return new ReverseWarning(reverseType, warningScore, warnings);
    }

    private double calculateMomentum(List<com.bot.TradingCore.Candle> c, int bars) {
        if (c.size() < bars + 1) return 0;
        int n = c.size();
        double move = c.get(n - 1).close - c.get(n - bars - 1).close;
        return move / c.get(n - bars - 1).close;
    }

    private double calculateMomentumPrev(List<com.bot.TradingCore.Candle> c, int bars, int offset) {
        if (c.size() < bars + offset + 1) return 0;
        int n = c.size();
        double move = c.get(n - offset - 1).close - c.get(n - offset - bars - 1).close;
        return move / c.get(n - offset - bars - 1).close;
    }

    private void adaptMinConfidence(MarketState state, double atr, double price) {
        double volatility = atr / price;

        if (volatility < 0.008) {
            MIN_CONFIDENCE = 62.0;
        } else if (volatility > 0.025) {
            MIN_CONFIDENCE = 54.0;
        } else {
            MIN_CONFIDENCE = 58.0;
        }

        if (state == MarketState.STRONG_TREND) {
            MIN_CONFIDENCE -= 3.0;
        }
    }

    private HTFBias detectBias2H(List<com.bot.TradingCore.Candle> c2h) {
        if (c2h == null || c2h.size() < 30) return HTFBias.NONE;

        double ema12 = ema(c2h, 12);
        double ema26 = ema(c2h, 26);
        double ema50 = c2h.size() >= 50 ? ema(c2h, 50) : ema26;
        double price = last(c2h).close;

        boolean bullishEMA = ema12 > ema26 && ema26 > ema50 * 0.998;
        boolean bearishEMA = ema12 < ema26 && ema26 < ema50 * 1.002;

        boolean priceAboveEMA = price > ema12 && price > ema26;
        boolean priceBelowEMA = price < ema12 && price < ema26;

        boolean hh_hl = checkHigherHighsHigherLows(c2h);
        boolean ll_lh = checkLowerLowsLowerHighs(c2h);

        if (bullishEMA && priceAboveEMA && hh_hl) return HTFBias.BULL;
        if (bearishEMA && priceBelowEMA && ll_lh) return HTFBias.BEAR;
        if (bullishEMA && priceAboveEMA) return HTFBias.BULL;
        if (bearishEMA && priceBelowEMA) return HTFBias.BEAR;

        return HTFBias.NONE;
    }

    private boolean checkHigherHighsHigherLows(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 15) return false;
        int n = c.size();
        double high1 = c.subList(n - 15, n - 8).stream().mapToDouble(x -> x.high).max().orElse(0);
        double high2 = c.subList(n - 8, n).stream().mapToDouble(x -> x.high).max().orElse(0);
        double low1 = c.subList(n - 15, n - 8).stream().mapToDouble(x -> x.low).min().orElse(0);
        double low2 = c.subList(n - 8, n).stream().mapToDouble(x -> x.low).min().orElse(0);
        return high2 > high1 && low2 > low1;
    }

    private boolean checkLowerLowsLowerHighs(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 15) return false;
        int n = c.size();
        double high1 = c.subList(n - 15, n - 8).stream().mapToDouble(x -> x.high).max().orElse(0);
        double high2 = c.subList(n - 8, n).stream().mapToDouble(x -> x.high).max().orElse(0);
        double low1 = c.subList(n - 15, n - 8).stream().mapToDouble(x -> x.low).min().orElse(0);
        double low2 = c.subList(n - 8, n).stream().mapToDouble(x -> x.low).min().orElse(0);
        return high2 < high1 && low2 < low1;
    }

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

        for (int i = c.size() - 3; i >= c.size() - 8 && i >= 2; i--) {
            com.bot.TradingCore.Candle c1 = c.get(i - 1);
            com.bot.TradingCore.Candle c2 = c.get(i);
            com.bot.TradingCore.Candle c3 = c.get(i + 1);

            double bodySize = Math.abs(c2.close - c2.open);
            double atr = atr(c.subList(Math.max(0, i - 14), i + 1), 14);

            if (c2.close > c2.open && bodySize > atr * 1.5) {
                double gapLow = c1.high;
                double gapHigh = c3.low;
                if (gapHigh > gapLow) {
                    return new FVGResult(true, true, gapLow, gapHigh);
                }
            }

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

        for (int i = c.size() - 5; i >= c.size() - 12 && i >= 3; i--) {
            com.bot.TradingCore.Candle potential = c.get(i);

            double moveAfter = 0;
            for (int j = i + 1; j < Math.min(i + 4, c.size()); j++) {
                moveAfter += c.get(j).close - c.get(j).open;
            }

            double atr = atr(c, 14);

            if (potential.close < potential.open && moveAfter > atr * 2.0) {
                return new OrderBlockResult(true, true, potential.low);
            }

            if (potential.close > potential.open && moveAfter < -atr * 2.0) {
                return new OrderBlockResult(true, false, potential.high);
            }
        }

        return new OrderBlockResult(false, false, 0);
    }

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

    private boolean isLongExhausted(List<com.bot.TradingCore.Candle> c15,
                                    List<com.bot.TradingCore.Candle> c1h,
                                    double rsi14, double rsi7,
                                    double price) {
        if (rsi14 > 75 && rsi7 > 78) return true;

        double ema21 = ema(c15, 21);
        double distFromEma = (price - ema21) / ema21;
        if (distFromEma > 0.025 && rsi14 > 68) return true;

        if (c1h.size() >= 8) {
            double rsiH1 = rsi(c1h, 14);
            if (rsiH1 > 78) return true;
        }

        if (c15.size() >= 6) {
            double body1 = Math.abs(c15.get(c15.size()-1).close - c15.get(c15.size()-1).open);
            double body2 = Math.abs(c15.get(c15.size()-2).close - c15.get(c15.size()-2).open);
            double body3 = Math.abs(c15.get(c15.size()-3).close - c15.get(c15.size()-3).open);
            if (body1 < body2 * 0.6 && body2 < body3 * 0.8) return true;

            double vol1 = c15.get(c15.size()-1).volume;
            double vol2 = c15.get(c15.size()-2).volume;
            double vol3 = c15.get(c15.size()-3).volume;
            if (price > ema21 && vol1 < vol2 * 0.8 && vol2 < vol3 * 0.9) return true;
        }

        com.bot.TradingCore.Candle last = c15.get(c15.size()-1);
        double upperWick = last.high - Math.max(last.open, last.close);
        double body = Math.abs(last.close - last.open);
        if (upperWick > body * 1.5 && last.close < last.open) return true;

        return false;
    }

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

    private double computeCalibratedConfidence(String symbol,
                                               double scoreLong, double scoreShort,
                                               MarketState state, CoinCategory cat,
                                               double atr, double price,
                                               boolean bullDiv, boolean bearDiv,
                                               boolean pullbackUp, boolean pullbackDown,
                                               boolean impulse, boolean pump,
                                               FundingOIData fundingData,
                                               HTFBias bias2h,
                                               boolean hasFVG, boolean hasOB) {

        double scoreDiff = Math.abs(scoreLong - scoreShort);
        double maxScore = 4.5;
        double normScore = scoreDiff / maxScore;

        int confirmations = 0;

        if (bullDiv || bearDiv) { normScore += 0.05; confirmations++; }
        if (pullbackUp || pullbackDown) { normScore += 0.04; confirmations++; }
        if (impulse) { normScore += 0.03; confirmations++; }
        if (pump) { normScore += 0.06; confirmations++; }
        if (hasFVG) { normScore += 0.05; confirmations++; }
        if (hasOB) { normScore += 0.05; confirmations++; }

        if (fundingData != null) {
            boolean fundingConfirmsLong = fundingData.fundingRate < -0.0003 && scoreLong > scoreShort;
            boolean fundingConfirmsShort = fundingData.fundingRate > 0.0003 && scoreShort > scoreLong;
            if (fundingConfirmsLong || fundingConfirmsShort) {
                normScore += 0.08;
                confirmations++;
            }
        }

        boolean bias2hConfirms = (bias2h == HTFBias.BULL && scoreLong > scoreShort) ||
                (bias2h == HTFBias.BEAR && scoreShort > scoreLong);
        if (bias2hConfirms) {
            normScore += 0.06;
            confirmations++;
        }

        normScore = Math.min(1.0, normScore);

        double range = 30 + Math.min(confirmations * 3, 15);
        double probability = 50 + normScore * range;

        if (state == MarketState.STRONG_TREND) probability += 3;
        else if (state == MarketState.WEAK_TREND) probability += 1;
        else if (state == MarketState.RANGE) probability -= 3;

        if (cat == CoinCategory.MEME) probability -= 5;
        else if (cat == CoinCategory.ALT) probability -= 2;

        Deque<CalibrationRecord> history = calibrationHistory.get(symbol);
        if (history != null && history.size() >= 20) {
            double avgAccuracy = calculateHistoricalAccuracy(history, probability);
            probability = probability * 0.7 + avgAccuracy * 0.3;
        }

        probability = clamp(probability, 50, 85);

        return Math.round(probability);
    }

    private double calculateHistoricalAccuracy(Deque<CalibrationRecord> history, double currentProb) {
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

    public void recordSignalResult(String symbol, double predictedProb, boolean wasCorrect) {
        Deque<CalibrationRecord> history = calibrationHistory.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        history.addLast(new CalibrationRecord(predictedProb, wasCorrect));
        while (history.size() > CALIBRATION_WINDOW) {
            history.removeFirst();
        }
    }

    private MarketState detectState(List<TradingCore.Candle> c) {
        int n = c.size();
        double ema20 = ema(c, 20);
        double ema50 = ema(c, 50);

        // Считаем наклон EMA20 за последние 5 свечей
        double emaPrev = ema(c.subList(0, n - 5), 20);
        double slope = (ema20 - emaPrev) / emaPrev;

        // Считаем волатильность (ATR) относительно цены
        double atr = atr(c, 14);
        double volRelative = atr / c.get(n-1).close;

        // УЛУЧШЕНИЕ: Если наклон почти нулевой или волатильность слишком низкая — это БОКОВИК.
        // Бот перестанет открывать сделки там, где цена просто «пилит» на месте.
        if (Math.abs(slope) < 0.0005 || volRelative < 0.0015) {
            return MarketState.RANGE;
        }

        return (ema20 > ema50 && slope > 0) ? MarketState.STRONG_TREND : MarketState.WEAK_TREND;
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
        double rsi14 = rsi(c, 14);
        if (bull) {
            return price <= ema21 * 1.001 && price >= ema21 * 0.995 && rsi14 > 40 && rsi14 < 55;
        } else {
            return price >= ema21 * 0.999 && price <= ema21 * 1.005 && rsi14 < 60 && rsi14 > 45;
        }
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

    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             CoinCategory cat) {
        long now = System.currentTimeMillis();
        return generate(symbol, c1, c5, c15, c1h, null, cat, now);
    }

    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             List<com.bot.TradingCore.Candle> c2h,
                             CoinCategory cat) {
        long now = System.currentTimeMillis();
        return generate(symbol, c1, c5, c15, c1h, c2h, cat, now);
    }
}