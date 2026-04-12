package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class PumpHunter {

    // ======================= CONFIG =======================

    // [v50] Body-to-ATR thresholds lowered for earlier pump detection.
    // At 2.2× the pump candle was already fully formed. At 1.6× we catch it forming.
    private static final double PUMP_BODY_ATR_MULT = 1.6;       // was 2.2
    private static final double MEGA_PUMP_BODY_ATR_MULT = 2.8;  // was 3.5
    // [v50] Volume spike thresholds also lowered to catch institutional accumulation earlier.
    // Institutional buying often comes in waves: first wave is 2× avg, main wave is 3×+.
    // At 3.0 we missed the first wave entirely. At 2.2 we catch the start.
    private static final double VOLUME_SPIKE_MULT = 2.2;      // was 3.0
    private static final double MEGA_VOLUME_SPIKE_MULT = 3.5;  // was 4.0

    // [v50] MIN_MOVE_PCT lowered 0.9%→0.6%: catches pumps 2-3 candles earlier.
    // Combined with lower volume spike requirement, this catches the institutional
    // accumulation phase before retail FOMO kicks in.
    private static final double MIN_MOVE_PCT = 0.006;    // was 0.009
    private static final double STRONG_MOVE_PCT = 0.014;  // was 0.018
    private static final double MEGA_MOVE_PCT = 0.028;    // was 0.035
    private static final double DUMP_MOVE_PCT = -0.006;   // was -0.009
    private static final double STRONG_DUMP_PCT = -0.014; // was -0.018

    private static final int VOLUME_LOOKBACK = 20;
    private static final int ATR_PERIOD = 14;
    private static final int PUMP_CONFIRM_BARS = 3;

    // [v50] Pump cooldown reduced 10→6 min
    private static final long PUMP_COOLDOWN_MS = 6 * 60_000;  // was 10

    // ======================= STATE =======================

    private final Map<String, Long> lastPumpTime = new ConcurrentHashMap<>();
    private final Map<String, PumpEvent> recentPumps = new ConcurrentHashMap<>();
    private final Map<String, Deque<PumpEvent>> pumpHistory = new ConcurrentHashMap<>();

    // ======================= MODELS =======================

    public boolean isDump(List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < VOLUME_LOOKBACK + 2) return false;

        com.bot.TradingCore.Candle last = candles.get(candles.size() - 1);
        com.bot.TradingCore.Candle prev = candles.get(candles.size() - 2);

        double change = (last.close - prev.close) / prev.close;
        double avgVol = averageVolume(candles, VOLUME_LOOKBACK);

        return change <= DUMP_MOVE_PCT && last.volume > avgVol * VOLUME_SPIKE_MULT;
    }

    public enum PumpType {
        NONE,
        PUMP_UP,
        PUMP_DOWN,
        MEGA_PUMP_UP,
        MEGA_PUMP_DOWN,
        SQUEEZE_UP,
        SQUEEZE_DOWN,
        BREAKOUT_UP,
        BREAKOUT_DOWN
    }

    public static final class PumpEvent {
        public final String symbol;
        public final PumpType type;
        public final double strength;
        public final double movePct;
        public final double volumeRatio;
        public final double bodyToAtrRatio;
        public final boolean isConfirmed;
        public final long timestamp;
        public final List<String> flags;

        public PumpEvent(String symbol, PumpType type, double strength,
                         double movePct, double volumeRatio, double bodyToAtrRatio,
                         boolean isConfirmed, List<String> flags) {
            this.symbol = symbol;
            this.type = type;
            this.strength = strength;
            this.movePct = movePct;
            this.volumeRatio = volumeRatio;
            this.bodyToAtrRatio = bodyToAtrRatio;
            this.isConfirmed = isConfirmed;
            this.timestamp = System.currentTimeMillis();
            this.flags = flags != null ? flags : List.of();
        }

        public boolean isBullish() {
            return type == PumpType.PUMP_UP || type == PumpType.MEGA_PUMP_UP ||
                    type == PumpType.SQUEEZE_UP || type == PumpType.BREAKOUT_UP;
        }

        public boolean isBearish() {
            return type == PumpType.PUMP_DOWN || type == PumpType.MEGA_PUMP_DOWN ||
                    type == PumpType.SQUEEZE_DOWN || type == PumpType.BREAKOUT_DOWN;
        }

        public boolean isMega() {
            return type == PumpType.MEGA_PUMP_UP || type == PumpType.MEGA_PUMP_DOWN;
        }

        @Override
        public String toString() {
            return String.format("PumpEvent{%s %s strength=%.2f move=%.2f%% vol=%.1fx flags=%s}",
                    symbol, type, strength, movePct * 100, volumeRatio, flags);
        }
    }

    public static final class PumpSignal {
        public final String symbol;
        public final com.bot.TradingCore.Side side;
        public final double entryPrice;
        public final double confidence;
        public final PumpEvent event;
        public final String strategy;

        public PumpSignal(String symbol, com.bot.TradingCore.Side side,
                          double entryPrice, double confidence,
                          PumpEvent event, String strategy) {
            this.symbol = symbol;
            this.side = side;
            this.entryPrice = entryPrice;
            this.confidence = confidence;
            this.event = event;
            this.strategy = strategy;
        }
    }

    // ======================= MAIN DETECTION =======================

    // [v34.0] Category-aware overload — TOP coins use relaxed thresholds
    public PumpEvent detectPump(String symbol,
                                List<com.bot.TradingCore.Candle> c1m,
                                List<com.bot.TradingCore.Candle> c5m,
                                List<com.bot.TradingCore.Candle> c15m,
                                com.bot.DecisionEngineMerged.CoinCategory cat) {
        // [FIX #13] Category-specific threshold multipliers.
        // TOP coins (BTC/ETH): heavy assets move slower → relax thresholds by 40%
        // ALT: standard thresholds
        // MEME: was 1.10 (only 10% harder) — MEME coins have 3× more noise than ALT.
        //        10% threshold increase provides almost no protection.
        //        Fixed: 1.35 (35% harder) gives meaningful noise reduction on MEME pairs.
        double catMult = switch (cat) {
            case TOP  -> 0.60;  // 40% easier (BTC 0.9% → 0.54% min move)
            case ALT  -> 1.00;
            case MEME -> 1.35;  // was 1.10 — CRITICAL: MEME noise needs real filtration
        };
        return detectPumpInternal(symbol, c1m, c5m, c15m, catMult);
    }

    public PumpEvent detectPump(String symbol,
                                List<com.bot.TradingCore.Candle> c1m,
                                List<com.bot.TradingCore.Candle> c5m,
                                List<com.bot.TradingCore.Candle> c15m) {
        return detectPumpInternal(symbol, c1m, c5m, c15m, 1.0);
    }

    private PumpEvent detectPumpInternal(String symbol,
                                         List<com.bot.TradingCore.Candle> c1m,
                                         List<com.bot.TradingCore.Candle> c5m,
                                         List<com.bot.TradingCore.Candle> c15m,
                                         double catMult) {

        if (c1m == null || c1m.size() < 30 ||
                c5m == null || c5m.size() < 20 ||
                c15m == null || c15m.size() < ATR_PERIOD + 5) {
            return null;
        }

        Long lastPump = lastPumpTime.get(symbol);
        if (lastPump != null && System.currentTimeMillis() - lastPump < PUMP_COOLDOWN_MS) {
            return null;
        }

        List<String> flags = new ArrayList<>();

        PumpMetrics m1 = analyzeCandle(c1m, ATR_PERIOD);
        PumpMetrics m5 = analyzeCandle(c5m, ATR_PERIOD);
        PumpMetrics m15 = analyzeCandle(c15m, ATR_PERIOD);

        // [v34.0] Apply category multiplier to move thresholds
        double effMinMove    = MIN_MOVE_PCT    * catMult;
        double effStrongMove = STRONG_MOVE_PCT * catMult;
        double effMegaMove   = MEGA_MOVE_PCT   * catMult;
        double effBodyAtr    = PUMP_BODY_ATR_MULT * catMult;
        double effMegaBody   = MEGA_PUMP_BODY_ATR_MULT * catMult;
        double effVolSpike   = VOLUME_SPIKE_MULT * catMult;
        double effMegaVol    = MEGA_VOLUME_SPIKE_MULT * catMult;

        PumpType type = PumpType.NONE;
        double strength = 0;
        double movePct = 0;
        double volumeRatio = 0;
        double bodyToAtr = 0;

        // 15M Analysis
        if (m15.bodyToAtr >= effMegaBody &&
                m15.volumeRatio >= effMegaVol &&
                Math.abs(m15.movePct) >= effMegaMove) {

            type = m15.isGreen ? PumpType.MEGA_PUMP_UP : PumpType.MEGA_PUMP_DOWN;
            strength = calculateStrength(m15, true);
            movePct = m15.movePct;
            volumeRatio = m15.volumeRatio;
            bodyToAtr = m15.bodyToAtr;
            flags.add("15M_MEGA");

        } else if (m15.bodyToAtr >= effBodyAtr &&
                m15.volumeRatio >= effVolSpike &&
                Math.abs(m15.movePct) >= effStrongMove) {

            type = m15.isGreen ? PumpType.PUMP_UP : PumpType.PUMP_DOWN;
            strength = calculateStrength(m15, false);
            movePct = m15.movePct;
            volumeRatio = m15.volumeRatio;
            bodyToAtr = m15.bodyToAtr;
            flags.add("15M_PUMP");
        }
        // 5M Analysis
        else if (m5.bodyToAtr >= effBodyAtr * 0.9 &&
                m5.volumeRatio >= effVolSpike &&
                Math.abs(m5.movePct) >= effMinMove) {

            if (m5.bodyToAtr >= effMegaBody && m5.volumeRatio >= effMegaVol) {
                type = m5.isGreen ? PumpType.MEGA_PUMP_UP : PumpType.MEGA_PUMP_DOWN;
                strength = calculateStrength(m5, true) * 0.95;
                flags.add("5M_MEGA");
            } else {
                type = m5.isGreen ? PumpType.PUMP_UP : PumpType.PUMP_DOWN;
                strength = calculateStrength(m5, false) * 0.90;
                flags.add("5M_PUMP");
            }
            movePct = m5.movePct;
            volumeRatio = m5.volumeRatio;
            bodyToAtr = m5.bodyToAtr;
        }
        // 1M Series Analysis
        else {
            SeriesPumpResult series = detectSeriesPump(c1m);
            if (series.detected) {
                type = series.isUp ? PumpType.PUMP_UP : PumpType.PUMP_DOWN;
                strength = series.strength * 0.85;
                movePct = series.totalMovePct;
                volumeRatio = series.avgVolumeRatio;
                bodyToAtr = series.avgBodyToAtr;
                flags.add("1M_SERIES");
                flags.add("bars=" + series.barsCount);
            }
        }

        if (type == PumpType.NONE) {
            return null;
        }

        // [PATCH v1.0] Snapshot original direction BEFORE squeeze/breakout can overwrite.
        // Prevents dead-cat bounce (3 green bars after MEGA_PUMP_DOWN) from being
        // misclassified as SQUEEZE_UP / BREAKOUT_UP → false LONG signal.
        final boolean origIsBullish = type == PumpType.PUMP_UP || type == PumpType.MEGA_PUMP_UP;
        final boolean origIsBearish = type == PumpType.PUMP_DOWN || type == PumpType.MEGA_PUMP_DOWN;

        SqueezeResult squeeze = detectSqueeze(c5m, c15m);
        if (squeeze.detected) {
            boolean conflict = (origIsBearish && squeeze.isUp)
                    || (origIsBullish && !squeeze.isUp);
            if (conflict) {
                // Opposite direction on secondary signal = dead-cat bounce, not a real squeeze.
                // Keep original type, halve strength, flag it.
                flags.add("CONFLICT_SQUEEZE_BOUNCE");
                strength *= 0.50;
            } else {
                type = squeeze.isUp ? PumpType.SQUEEZE_UP : PumpType.SQUEEZE_DOWN;
                // [PATCH] Reset strength to squeeze-base, do not inherit from canceled pump
                strength = Math.min(0.85, 0.55 + squeeze.intensity * 0.3);
                flags.add("SQUEEZE");
            }
        }

        BreakoutResult breakout = detectBreakout(c15m);
        if (breakout.detected) {
            boolean conflict = (origIsBearish && breakout.isUp)
                    || (origIsBullish && !breakout.isUp);
            if (conflict) {
                flags.add("CONFLICT_BREAKOUT_BOUNCE");
                strength *= 0.50;
            } else {
                type = breakout.isUp ? PumpType.BREAKOUT_UP : PumpType.BREAKOUT_DOWN;
                strength = Math.min(0.90, 0.60 + strength * 0.3);
                flags.add("BREAKOUT");
                flags.add("level=" + String.format("%.6f", breakout.level));
            }
        }

        if (isVolumeClimaxing(c1m)) {
            strength = Math.min(1.0, strength + 0.08);
            flags.add("VOL_CLIMAX");
        }

        boolean confirmed = checkConfirmation(c1m, type);
        if (confirmed) {
            strength = Math.min(1.0, strength + 0.05);
            flags.add("CONFIRMED");
        }

        // [PATCH v1.0] If conflict-downgrades pushed strength too low, drop the event.
        // Prevents noise events with strength 0.1 from polluting the flag stream.
        if (strength < 0.20) {
            return null;
        }

        PumpEvent event = new PumpEvent(
                symbol, type, strength, movePct, volumeRatio, bodyToAtr, confirmed, flags
        );

        lastPumpTime.put(symbol, System.currentTimeMillis());
        recentPumps.put(symbol, event);

        Deque<PumpEvent> history = pumpHistory.computeIfAbsent(symbol, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        history.addLast(event);
        while (history.size() > 50) history.removeFirst();

        System.out.println("[PumpHunter] " + event);

        return event;
    }

    public PumpSignal generateSignal(PumpEvent event, double currentPrice) {
        if (event == null || event.type == PumpType.NONE) {
            return null;
        }

        com.bot.TradingCore.Side side;
        String strategy;
        double confidence;

        switch (event.type) {
            case PUMP_UP, MEGA_PUMP_UP, BREAKOUT_UP -> {
                side = com.bot.TradingCore.Side.LONG;
                strategy = "PUMP_CONTINUATION";
                confidence = 55 + event.strength * 25;
            }
            case PUMP_DOWN, MEGA_PUMP_DOWN, BREAKOUT_DOWN -> {
                side = com.bot.TradingCore.Side.SHORT;
                strategy = "DUMP_CONTINUATION";
                confidence = 55 + event.strength * 25;
            }
            case SQUEEZE_UP -> {
                side = com.bot.TradingCore.Side.LONG;
                strategy = "SHORT_SQUEEZE";
                confidence = 60 + event.strength * 20;
            }
            case SQUEEZE_DOWN -> {
                side = com.bot.TradingCore.Side.SHORT;
                strategy = "LONG_SQUEEZE";
                confidence = 60 + event.strength * 20;
            }
            default -> {
                return null;
            }
        }

        if (event.isConfirmed) confidence += 3;
        if (event.isMega()) confidence += 5;
        if (event.flags.contains("VOL_CLIMAX")) confidence += 2;

        confidence = Math.min(85, Math.max(55, confidence));

        return new PumpSignal(event.symbol, side, currentPrice, confidence, event, strategy);
    }

    // ======================= ANALYSIS HELPERS =======================

    private static class PumpMetrics {
        double bodyToAtr;
        double volumeRatio;
        double movePct;
        boolean isGreen;
        double bodySize;
        double atr;
    }

    private PumpMetrics analyzeCandle(List<com.bot.TradingCore.Candle> candles, int atrPeriod) {
        PumpMetrics m = new PumpMetrics();
        if (candles == null || candles.size() < atrPeriod + 1) return m;

        com.bot.TradingCore.Candle last = candles.get(candles.size() - 1);
        com.bot.TradingCore.Candle prev = candles.get(candles.size() - 2);

        m.atr = calculateATR(candles, atrPeriod);
        if (m.atr <= 0) m.atr = (last.high - last.low);

        m.bodySize = Math.abs(last.close - last.open);
        m.bodyToAtr = m.bodySize / Math.max(m.atr, 1e-10);

        double avgVol = averageVolume(candles, VOLUME_LOOKBACK);
        m.volumeRatio = last.volume / Math.max(avgVol, 1e-10);

        m.movePct = (last.close - prev.close) / prev.close;
        m.isGreen = last.close > last.open;

        return m;
    }

    private double calculateStrength(PumpMetrics m, boolean isMega) {
        double bodyScore = Math.min(1.0, m.bodyToAtr / (isMega ? MEGA_PUMP_BODY_ATR_MULT : PUMP_BODY_ATR_MULT) / 1.5);
        double volScore = Math.min(1.0, m.volumeRatio / (isMega ? MEGA_VOLUME_SPIKE_MULT : VOLUME_SPIKE_MULT) / 1.5);
        double moveScore = Math.min(1.0, Math.abs(m.movePct) / (isMega ? MEGA_MOVE_PCT : STRONG_MOVE_PCT));

        double strength = bodyScore * 0.35 + volScore * 0.35 + moveScore * 0.30;
        return Math.min(1.0, strength);
    }

    // ======================= SERIES PUMP DETECTION =======================

    private static class SeriesPumpResult {
        boolean detected;
        boolean isUp;
        double strength;
        double totalMovePct;
        double avgVolumeRatio;
        double avgBodyToAtr;
        int barsCount;
    }

    private SeriesPumpResult detectSeriesPump(List<com.bot.TradingCore.Candle> c1m) {
        SeriesPumpResult result = new SeriesPumpResult();
        if (c1m == null || c1m.size() < 10) return result;

        int n = c1m.size();
        double atr = calculateATR(c1m, ATR_PERIOD);
        double avgVol = averageVolume(c1m, VOLUME_LOOKBACK);

        int greenCount = 0;
        int redCount = 0;
        double totalMove = 0;
        double totalVolRatio = 0;
        double totalBodyAtr = 0;

        int lookback = Math.min(6, n - 1);

        for (int i = n - lookback; i < n; i++) {
            com.bot.TradingCore.Candle c = c1m.get(i);

            if (c.close > c.open) greenCount++;
            else redCount++;

            totalMove += c.close - c.open;
            totalVolRatio += c.volume / Math.max(avgVol, 1e-10);
            totalBodyAtr += Math.abs(c.close - c.open) / Math.max(atr, 1e-10);
        }

        com.bot.TradingCore.Candle startBar = c1m.get(n - lookback);
        com.bot.TradingCore.Candle endBar = c1m.get(n - 1);

        double totalMovePct = (endBar.close - startBar.open) / Math.max(startBar.open, 1e-10);
        double avgVolRatio = totalVolRatio / lookback;
        double avgBodyAtr = totalBodyAtr / lookback;

        boolean isUpSeries = greenCount >= 4 && Math.abs(totalMovePct) >= MIN_MOVE_PCT;
        boolean isDownSeries = redCount >= 4 && Math.abs(totalMovePct) >= MIN_MOVE_PCT;
        boolean volumeOk = avgVolRatio >= 1.5;

        if ((isUpSeries || isDownSeries) && volumeOk) {
            result.detected = true;
            result.isUp = totalMove > 0;
            result.totalMovePct = totalMovePct;
            result.avgVolumeRatio = avgVolRatio;
            result.avgBodyToAtr = avgBodyAtr;
            result.barsCount = lookback;
            result.strength = Math.min(1.0,
                    (Math.abs(totalMovePct) / STRONG_MOVE_PCT * 0.5) +
                            (avgVolRatio / VOLUME_SPIKE_MULT * 0.3) +
                            (avgBodyAtr / PUMP_BODY_ATR_MULT * 0.2)
            );
        }

        return result;
    }

    // ======================= SQUEEZE DETECTION =======================

    // [PATCH v1.0] Added intensity for confidence scaling
    private static class SqueezeResult {
        boolean detected;
        boolean isUp;
        double intensity;
    }

    // [PATCH v1.0] detectSqueeze now requires:
    //   1. prevTrend SIGNIFICANT (>0.6% of price) — filters noise
    //   2. currentMove > 0.6 × prevTrend (same as before)
    //   3. recent volume >= 1.3 × previous volume average (real flow, not noise)
    //   4. last 15m candle body aligns with the reversal direction (HTF confirmation)
    // Also returns intensity ∈ [0..1] for proportional confidence scaling.
    private SqueezeResult detectSqueeze(List<com.bot.TradingCore.Candle> c5m,
                                        List<com.bot.TradingCore.Candle> c15m) {
        SqueezeResult result = new SqueezeResult();
        if (c5m == null || c5m.size() < 20) return result;
        if (c15m == null || c15m.size() < 10) return result;

        int n = c5m.size();

        double prevTrend = 0;
        double prevVol = 0;
        for (int i = n - 15; i < n - 3; i++) {
            prevTrend += c5m.get(i).close - c5m.get(i).open;
            prevVol   += c5m.get(i).volume;
        }
        double prevVolAvg = prevVol / 12.0;

        double currentMove = 0;
        double currentVol = 0;
        for (int i = n - 3; i < n; i++) {
            currentMove += c5m.get(i).close - c5m.get(i).open;
            currentVol  += c5m.get(i).volume;
        }
        double currentVolAvg = currentVol / 3.0;

        double refPrice = c5m.get(n - 1).close;
        double prevTrendPct = Math.abs(prevTrend) / (refPrice + 1e-9);
        if (prevTrendPct < 0.006) return result;  // no significant prior move → no squeeze

        boolean reversalUp = prevTrend < 0 && currentMove > 0
                && Math.abs(currentMove) > Math.abs(prevTrend) * 0.6
                && currentVolAvg >= prevVolAvg * 1.3;

        boolean reversalDn = prevTrend > 0 && currentMove < 0
                && Math.abs(currentMove) > Math.abs(prevTrend) * 0.6
                && currentVolAvg >= prevVolAvg * 1.3;

        if (!reversalUp && !reversalDn) return result;

        // HTF confirmation: last 15m candle body must align with reversal direction
        int n15 = c15m.size();
        com.bot.TradingCore.Candle last15 = c15m.get(n15 - 1);
        double body15 = last15.close - last15.open;
        if (reversalUp && body15 <= 0) return result;
        if (reversalDn && body15 >= 0) return result;

        result.detected = true;
        result.isUp = reversalUp;
        result.intensity = Math.min(1.0, Math.abs(currentMove) / (Math.abs(prevTrend) + 1e-9));
        return result;
    }

    // ======================= BREAKOUT DETECTION =======================

    private static class BreakoutResult {
        boolean detected;
        boolean isUp;
        double level;
    }

    // [PATCH v1.0] Breakout now requires volume >= 1.5× median of lookback window.
    // A close past prior high/low WITHOUT volume is a classic fake breakout on alts.
    private BreakoutResult detectBreakout(List<com.bot.TradingCore.Candle> c15m) {
        BreakoutResult result = new BreakoutResult();
        if (c15m == null || c15m.size() < 30) return result;

        int n = c15m.size();
        com.bot.TradingCore.Candle last = c15m.get(n - 1);

        double recentHigh = Double.NEGATIVE_INFINITY;
        double recentLow = Double.MAX_VALUE;
        double volSum = 0;
        int volCount = 0;

        for (int i = n - 23; i < n - 3; i++) {
            recentHigh = Math.max(recentHigh, c15m.get(i).high);
            recentLow = Math.min(recentLow, c15m.get(i).low);
            volSum    += c15m.get(i).volume;
            volCount++;
        }
        double medianVol = volCount > 0 ? volSum / volCount : 0;
        boolean volumeConfirmed = last.volume >= medianVol * 1.5;

        if (last.close > recentHigh * 1.002 && last.close > last.open && volumeConfirmed) {
            result.detected = true;
            result.isUp = true;
            result.level = recentHigh;
        }

        if (last.close < recentLow * 0.998 && last.close < last.open && volumeConfirmed) {
            result.detected = true;
            result.isUp = false;
            result.level = recentLow;
        }

        return result;
    }

    // ======================= VOLUME CLIMAX =======================

    private boolean isVolumeClimaxing(List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 21) return false;

        int n = candles.size();
        com.bot.TradingCore.Candle last = candles.get(n - 1);

        double maxVol = 0;
        for (int i = n - 21; i < n - 1; i++) {
            maxVol = Math.max(maxVol, candles.get(i).volume);
        }

        return last.volume >= maxVol * 0.90;
    }

    // ======================= CONFIRMATION =======================

    // [PATCH v1.0] Confirmation now requires ≥2 of PUMP_CONFIRM_BARS bars both:
    //   (a) close past pumpBar's level (within tight tolerance), AND
    //   (b) candle direction aligned with pump direction.
    // This rejects weak dead-cat continuations that previously passed on a single bar.
    private boolean checkConfirmation(List<com.bot.TradingCore.Candle> c1m, PumpType type) {
        if (c1m == null || c1m.size() < PUMP_CONFIRM_BARS + 2) return false;
        if (type == PumpType.NONE) return false;

        int n = c1m.size();
        com.bot.TradingCore.Candle pumpBar = c1m.get(n - PUMP_CONFIRM_BARS - 1);
        double pumpClose = pumpBar.close;

        boolean isUp = type == PumpType.PUMP_UP || type == PumpType.MEGA_PUMP_UP ||
                type == PumpType.SQUEEZE_UP || type == PumpType.BREAKOUT_UP;

        int confirmCount = 0;
        for (int i = n - PUMP_CONFIRM_BARS; i < n; i++) {
            com.bot.TradingCore.Candle c = c1m.get(i);
            // [PATCH] both price AND candle direction must align
            if (isUp  && c.close >= pumpClose * 0.9985 && c.close >= c.open) confirmCount++;
            if (!isUp && c.close <= pumpClose * 1.0015 && c.close <= c.open) confirmCount++;
        }

        return confirmCount >= 2;
    }

    // ======================= UTILITY =======================

    /** [v23.0 FIX] Delegates to TradingCore.atr() — Wilder's smoothed ATR */
    private double calculateATR(List<com.bot.TradingCore.Candle> candles, int period) {
        return com.bot.TradingCore.atr(candles, period);
    }

    private double averageVolume(List<com.bot.TradingCore.Candle> candles, int lookback) {
        if (candles == null || candles.size() < lookback + 1) return 1;

        double sum = 0;
        int n = candles.size();
        int start = Math.max(0, n - lookback - 1);

        for (int i = start; i < n - 1; i++) {
            sum += candles.get(i).volume;
        }

        int count = (n - 1) - start;
        return count > 0 ? sum / count : 1;
    }

    // ======================= PUBLIC API =======================

    public PumpEvent getRecentPump(String symbol) {
        return recentPumps.get(symbol);
    }

    public boolean hadRecentPump(String symbol, long withinMs) {
        Long lastTime = lastPumpTime.get(symbol);
        return lastTime != null && System.currentTimeMillis() - lastTime < withinMs;
    }

    public List<PumpEvent> getPumpHistory(String symbol) {
        Deque<PumpEvent> history = pumpHistory.get(symbol);
        return history != null ? new ArrayList<>(history) : List.of();
    }

    public void clearSymbol(String symbol) {
        lastPumpTime.remove(symbol);
        recentPumps.remove(symbol);
        pumpHistory.remove(symbol);
    }

    public void clearAll() {
        lastPumpTime.clear();
        recentPumps.clear();
        pumpHistory.clear();
    }
}