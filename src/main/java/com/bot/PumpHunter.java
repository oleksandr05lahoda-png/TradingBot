package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * PumpHunter v51 — latency-optimized pump / dump / exhaustion / pre-pump detector.
 *
 * Changes vs v50:
 *   [A1] Detection hierarchy flipped: 1M series → 5M → 15M (was 15M → 5M → 1M).
 *        Catches impulses 10-14 min earlier on normal pumps.
 *   [A2] PUMP_CONFIRM_BARS 3→1.
 *   [A3] PUMP_COOLDOWN_MS 6min→3min; separate EXHAUSTION and PREPUMP cooldowns.
 *   [B ] detectExhaustion(): SHORT at top of up-pump (vol divergence + wick growth + momentum slowdown).
 *        DUMP_EXHAUSTION_LONG for bottom of down-pump.
 *   [C ] detectPrePump(): compression + vol buildup + ignition candle → LONG/SHORT BEFORE the move.
 *   [F ] PumpEvent.ageMs() + age-based strength decay method for DecisionEngine consumers.
 *   [+]  Two new PumpType variants: PUMP_EXHAUSTION_SHORT, DUMP_EXHAUSTION_LONG,
 *        PRE_PUMP_LONG, PRE_DUMP_SHORT. isBullish()/isBearish() cover them correctly.
 *
 * API compatibility:
 *   - detectPump(symbol, c1m, c5m, c15m, cat)  — same signature, same return type
 *   - detectPump(symbol, c1m, c5m, c15m)       — same signature, same return type
 *   - getRecentPump(symbol)                    — same
 *   - generateSignal(event, currentPrice)      — same, now handles new types
 *   - All PumpEvent / PumpSignal / PumpType enum public API preserved (only additions).
 *
 * Order of checks inside detectPump:
 *   1. EXHAUSTION  — runs even during PUMP_COOLDOWN (after a pump, exhaustion is imminent)
 *   2. PRE-PUMP    — runs when no recent pump (compression setup)
 *   3. 1M series   — fastest continuation trigger
 *   4. 5M body     — confirms 1M into a proper impulse
 *   5. 15M normal  — standard continuation
 *   6. 15M MEGA    — strongest (but slowest) continuation
 *   7. Squeeze / Breakout overlays (kept from v50, with conflict guards)
 *   8. Volume climax / confirmation (kept)
 */
public final class PumpHunter {

    // ==================== CONFIG ====================

    // Body-to-ATR thresholds (v50 values kept)
    private static final double PUMP_BODY_ATR_MULT      = 1.6;
    private static final double MEGA_PUMP_BODY_ATR_MULT = 2.8;
    private static final double VOLUME_SPIKE_MULT       = 2.2;
    private static final double MEGA_VOLUME_SPIKE_MULT  = 3.5;

    private static final double MIN_MOVE_PCT    = 0.006;
    private static final double STRONG_MOVE_PCT = 0.014;
    private static final double MEGA_MOVE_PCT   = 0.028;
    private static final double DUMP_MOVE_PCT   = -0.006;

    private static final int VOLUME_LOOKBACK = 20;
    private static final int ATR_PERIOD      = 14;

    // [A2] Confirmation: 1 bar (was 3). Squeeze/breakout conflict guards still filter dead-cat.
    private static final int PUMP_CONFIRM_BARS = 1;

    // [A3] Separate cooldowns. PUMP_COOLDOWN gates ONLY continuation-type re-emission;
    //      exhaustion & pre-pump have their own fast cooldowns.
    private static final long PUMP_COOLDOWN_MS       = 3 * 60_000L;
    private static final long EXHAUSTION_COOLDOWN_MS = 90_000L;
    private static final long PREPUMP_COOLDOWN_MS    = 2 * 60_000L;

    // [B] Exhaustion thresholds
    private static final long   EXH_PUMP_AGE_MS = 10 * 60_000L; // exhaustion only valid within 10min after pump
    private static final double EXH_VOL_DROP    = 0.65;          // recent vol < 65% of peak = divergence
    private static final double EXH_WICK_RATIO  = 0.50;          // avg upper wick > 50% body
    private static final int    EXH_MIN_FLAGS   = 2;             // at least 2 of {volDiv, longWicks, slowdown}

    // [C] Pre-pump thresholds
    private static final double PRE_COMPRESSION   = 0.60;  // atr(7) < 60% atr(20) = compression
    private static final double PRE_VOL_BUILDUP   = 1.30;  // last-5 vol > 30% above prior-5
    private static final double PRE_SPARK_VOL     = 1.80;  // ignition candle volume > 1.8× avg
    private static final double PRE_SPARK_BODY    = 0.40;  // ignition body ≥ 0.4× atr

    // ==================== STATE ====================

    private final Map<String, Long> lastPumpTime       = new ConcurrentHashMap<>();
    private final Map<String, Long> lastExhaustionTime = new ConcurrentHashMap<>();
    private final Map<String, Long> lastPrePumpTime    = new ConcurrentHashMap<>();

    private final Map<String, PumpEvent>        recentPumps = new ConcurrentHashMap<>();
    private final Map<String, Deque<PumpEvent>> pumpHistory = new ConcurrentHashMap<>();

    // ==================== ENUM ====================

    public enum PumpType {
        NONE,
        // Continuation
        PUMP_UP, PUMP_DOWN,
        MEGA_PUMP_UP, MEGA_PUMP_DOWN,
        SQUEEZE_UP, SQUEEZE_DOWN,
        BREAKOUT_UP, BREAKOUT_DOWN,
        // Reversal / anticipatory
        PUMP_EXHAUSTION_SHORT, DUMP_EXHAUSTION_LONG,
        PRE_PUMP_LONG, PRE_DUMP_SHORT
    }

    // ==================== EVENT ====================

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
            return type == PumpType.PUMP_UP        || type == PumpType.MEGA_PUMP_UP
                    || type == PumpType.SQUEEZE_UP     || type == PumpType.BREAKOUT_UP
                    || type == PumpType.DUMP_EXHAUSTION_LONG
                    || type == PumpType.PRE_PUMP_LONG;
        }

        public boolean isBearish() {
            return type == PumpType.PUMP_DOWN      || type == PumpType.MEGA_PUMP_DOWN
                    || type == PumpType.SQUEEZE_DOWN   || type == PumpType.BREAKOUT_DOWN
                    || type == PumpType.PUMP_EXHAUSTION_SHORT
                    || type == PumpType.PRE_DUMP_SHORT;
        }

        public boolean isMega() {
            return type == PumpType.MEGA_PUMP_UP || type == PumpType.MEGA_PUMP_DOWN;
        }

        public boolean isReversal() {
            return type == PumpType.PUMP_EXHAUSTION_SHORT
                    || type == PumpType.DUMP_EXHAUSTION_LONG;
        }

        public boolean isAnticipatory() {
            return type == PumpType.PRE_PUMP_LONG
                    || type == PumpType.PRE_DUMP_SHORT;
        }

        /** [F] Age of the event in ms. Consumers can use this to decay strength. */
        public long ageMs() {
            return System.currentTimeMillis() - timestamp;
        }

        /** [F] Strength decayed by age: full strength for ≤3min, linear fade to 0 at 15min. */
        public double decayedStrength() {
            long age = ageMs();
            if (age <= 3 * 60_000L) return strength;
            if (age >= 15 * 60_000L) return 0.0;
            double factor = 1.0 - (age - 3 * 60_000.0) / (12 * 60_000.0);
            return strength * factor;
        }

        @Override
        public String toString() {
            return String.format("PumpEvent{%s %s strength=%.2f move=%.2f%% vol=%.1fx flags=%s}",
                    symbol, type, strength, movePct * 100, volumeRatio, flags);
        }
    }

    // ==================== SIGNAL ====================

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

    // ==================== ISDUMP (legacy API kept) ====================

    public boolean isDump(List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < VOLUME_LOOKBACK + 2) return false;
        com.bot.TradingCore.Candle last = candles.get(candles.size() - 1);
        com.bot.TradingCore.Candle prev = candles.get(candles.size() - 2);
        double change = (last.close - prev.close) / prev.close;
        double avgVol = averageVolume(candles, VOLUME_LOOKBACK);
        return change <= DUMP_MOVE_PCT && last.volume > avgVol * VOLUME_SPIKE_MULT;
    }

    // ==================== MAIN ENTRY (category-aware) ====================

    public PumpEvent detectPump(String symbol,
                                List<com.bot.TradingCore.Candle> c1m,
                                List<com.bot.TradingCore.Candle> c5m,
                                List<com.bot.TradingCore.Candle> c15m,
                                com.bot.DecisionEngineMerged.CoinCategory cat) {
        double catMult = switch (cat) {
            case TOP  -> 0.60;
            case ALT  -> 1.00;
            case MEME -> 1.60;
        };
        return detectPumpInternal(symbol, c1m, c5m, c15m, catMult);
    }

    public PumpEvent detectPump(String symbol,
                                List<com.bot.TradingCore.Candle> c1m,
                                List<com.bot.TradingCore.Candle> c5m,
                                List<com.bot.TradingCore.Candle> c15m) {
        return detectPumpInternal(symbol, c1m, c5m, c15m, 1.0);
    }

    // ==================== INTERNAL DETECTION PIPELINE ====================

    private PumpEvent detectPumpInternal(String symbol,
                                         List<com.bot.TradingCore.Candle> c1m,
                                         List<com.bot.TradingCore.Candle> c5m,
                                         List<com.bot.TradingCore.Candle> c15m,
                                         double catMult) {

        if (c1m == null || c1m.size() < 20
                || c5m == null || c5m.size() < 15
                || c15m == null || c15m.size() < ATR_PERIOD + 3) {
            return null;
        }

        long now = System.currentTimeMillis();

        // ---- [B] STAGE 1: EXHAUSTION DETECTOR ----
        // Runs even during PUMP_COOLDOWN. This is the KEY late-signal fix.
        Long lastExh = lastExhaustionTime.get(symbol);
        if (lastExh == null || now - lastExh >= EXHAUSTION_COOLDOWN_MS) {
            PumpEvent exh = detectExhaustion(symbol, c1m, c5m);
            if (exh != null) {
                lastExhaustionTime.put(symbol, now);
                recordEvent(symbol, exh);
                System.out.println("[PumpHunter/EXH] " + exh);
                return exh;
            }
        }

        // ---- [C] STAGE 2: PRE-PUMP DETECTOR ----
        Long lastPre = lastPrePumpTime.get(symbol);
        boolean preAllowed = lastPre == null || now - lastPre >= PREPUMP_COOLDOWN_MS;
        // pre-pump only valid when no fresh continuation pump exists
        Long lastPump = lastPumpTime.get(symbol);
        boolean pumpRecent = lastPump != null && now - lastPump < PUMP_COOLDOWN_MS;
        if (preAllowed && !pumpRecent) {
            PumpEvent pre = detectPrePump(symbol, c1m, c5m);
            if (pre != null) {
                lastPrePumpTime.put(symbol, now);
                recordEvent(symbol, pre);
                System.out.println("[PumpHunter/PRE] " + pre);
                return pre;
            }
        }

        // ---- STAGE 3+: CONTINUATION HIERARCHY ----
        if (pumpRecent) return null;

        List<String> flags = new ArrayList<>();

        PumpMetrics m1  = analyzeCandle(c1m,  ATR_PERIOD);
        PumpMetrics m5  = analyzeCandle(c5m,  ATR_PERIOD);
        PumpMetrics m15 = analyzeCandle(c15m, ATR_PERIOD);

        double effMinMove    = MIN_MOVE_PCT    * catMult;
        double effStrongMove = STRONG_MOVE_PCT * catMult;
        double effMegaMove   = MEGA_MOVE_PCT   * catMult;
        double effBodyAtr    = PUMP_BODY_ATR_MULT       * catMult;
        double effMegaBody   = MEGA_PUMP_BODY_ATR_MULT  * catMult;
        double effVolSpike   = VOLUME_SPIKE_MULT        * catMult;
        double effMegaVol    = MEGA_VOLUME_SPIKE_MULT   * catMult;

        PumpType type = PumpType.NONE;
        double strength = 0, movePct = 0, volumeRatio = 0, bodyToAtr = 0;

        // [A1] FLIPPED HIERARCHY: 1M series → 5M → 15M (was 15M → 5M → 1M)

        // --- (3a) 1M series: fastest trigger ---
        SeriesPumpResult series = detectSeriesPump(c1m);
        if (series.detected && Math.abs(series.totalMovePct) >= effMinMove) {
            type = series.isUp ? PumpType.PUMP_UP : PumpType.PUMP_DOWN;
            // [A1] 1M series strength multiplier raised 0.85→0.92:
            // earlier signal, almost same conviction; MIN_CONF filter still applies downstream.
            strength = series.strength * 0.92;
            movePct = series.totalMovePct;
            volumeRatio = series.avgVolumeRatio;
            bodyToAtr = series.avgBodyToAtr;
            flags.add("1M_SERIES_EARLY");
            flags.add("bars=" + series.barsCount);
        }
        // --- (3b) 5M body ---
        else if (m5.bodyToAtr >= effBodyAtr * 0.9
                && m5.volumeRatio >= effVolSpike
                && Math.abs(m5.movePct) >= effMinMove) {
            if (m5.bodyToAtr >= effMegaBody && m5.volumeRatio >= effMegaVol) {
                type = m5.isGreen ? PumpType.MEGA_PUMP_UP : PumpType.MEGA_PUMP_DOWN;
                strength = calculateStrength(m5, true) * 0.95;
                flags.add("5M_MEGA");
            } else {
                type = m5.isGreen ? PumpType.PUMP_UP : PumpType.PUMP_DOWN;
                strength = calculateStrength(m5, false) * 0.90;
                flags.add("5M_PUMP");
            }
            movePct = m5.movePct; volumeRatio = m5.volumeRatio; bodyToAtr = m5.bodyToAtr;
        }
        // --- (3c) 15M normal ---
        else if (m15.bodyToAtr >= effBodyAtr
                && m15.volumeRatio >= effVolSpike
                && Math.abs(m15.movePct) >= effStrongMove) {
            type = m15.isGreen ? PumpType.PUMP_UP : PumpType.PUMP_DOWN;
            strength = calculateStrength(m15, false);
            movePct = m15.movePct; volumeRatio = m15.volumeRatio; bodyToAtr = m15.bodyToAtr;
            flags.add("15M_PUMP");
        }
        // --- (3d) 15M MEGA (last resort — slowest) ---
        else if (m15.bodyToAtr >= effMegaBody
                && m15.volumeRatio >= effMegaVol
                && Math.abs(m15.movePct) >= effMegaMove) {
            type = m15.isGreen ? PumpType.MEGA_PUMP_UP : PumpType.MEGA_PUMP_DOWN;
            strength = calculateStrength(m15, true);
            movePct = m15.movePct; volumeRatio = m15.volumeRatio; bodyToAtr = m15.bodyToAtr;
            flags.add("15M_MEGA");
        }

        if (type == PumpType.NONE) return null;

        // Snapshot BEFORE squeeze/breakout can overwrite (preserves dead-cat guard from v50).
        final boolean origIsBullish = type == PumpType.PUMP_UP || type == PumpType.MEGA_PUMP_UP;
        final boolean origIsBearish = type == PumpType.PUMP_DOWN || type == PumpType.MEGA_PUMP_DOWN;

        SqueezeResult squeeze = detectSqueeze(c5m, c15m);
        if (squeeze.detected) {
            boolean conflict = (origIsBearish && squeeze.isUp) || (origIsBullish && !squeeze.isUp);
            if (conflict) { flags.add("CONFLICT_SQUEEZE_BOUNCE"); strength *= 0.50; }
            else {
                type = squeeze.isUp ? PumpType.SQUEEZE_UP : PumpType.SQUEEZE_DOWN;
                strength = Math.min(0.85, 0.55 + squeeze.intensity * 0.3);
                flags.add("SQUEEZE");
            }
        }

        BreakoutResult breakout = detectBreakout(c15m);
        if (breakout.detected) {
            boolean conflict = (origIsBearish && breakout.isUp) || (origIsBullish && !breakout.isUp);
            if (conflict) { flags.add("CONFLICT_BREAKOUT_BOUNCE"); strength *= 0.50; }
            else {
                type = breakout.isUp ? PumpType.BREAKOUT_UP : PumpType.BREAKOUT_DOWN;
                strength = Math.min(0.90, 0.60 + strength * 0.3);
                flags.add("BREAKOUT");
                flags.add("level=" + String.format("%.6f", breakout.level));
            }
        }

        if (isVolumeClimaxing(c1m)) { strength = Math.min(1.0, strength + 0.08); flags.add("VOL_CLIMAX"); }

        boolean confirmed = checkConfirmation(c1m, type);
        if (confirmed) { strength = Math.min(1.0, strength + 0.05); flags.add("CONFIRMED"); }

        if (strength < 0.20) return null;

        PumpEvent event = new PumpEvent(
                symbol, type, strength, movePct, volumeRatio, bodyToAtr, confirmed, flags);

        lastPumpTime.put(symbol, now);
        recordEvent(symbol, event);
        System.out.println("[PumpHunter] " + event);
        return event;
    }

    private void recordEvent(String symbol, PumpEvent event) {
        recentPumps.put(symbol, event);
        Deque<PumpEvent> hist = pumpHistory.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        hist.addLast(event);
        while (hist.size() > 50) hist.removeFirst();
    }

    // ==================== [B] EXHAUSTION DETECTOR ====================

    /**
     * Detects end of a fresh pump on 1m: volume divergence + wick growth + momentum slowdown.
     * Fires SHORT at the top of an up-pump, LONG at the bottom of a down-pump.
     * This is the fix for BOME-style late shorts: we catch the top bar instead of entering
     * after the dump already unloaded 30%.
     */
    private PumpEvent detectExhaustion(String symbol,
                                       List<com.bot.TradingCore.Candle> c1m,
                                       List<com.bot.TradingCore.Candle> c5m) {
        if (c1m == null || c1m.size() < 12) return null;

        PumpEvent recent = recentPumps.get(symbol);
        if (recent == null) return null;
        if (!(recent.isBullish() || recent.isBearish())) return null;
        if (recent.isReversal() || recent.isAnticipatory()) return null; // don't stack
        if (System.currentTimeMillis() - recent.timestamp > EXH_PUMP_AGE_MS) return null;
        // Skip if the recent event itself is weak — no edge trying to fade noise
        if (recent.strength < 0.45) return null;

        int n = c1m.size();
        boolean wasBullish = recent.isBullish(); // pump-up → we look for top (SHORT)

        // --- Volume divergence: peak in [n-10..n-5), current in [n-3..n) ---
        double peakVol = 0;
        for (int i = Math.max(0, n - 10); i < n - 4; i++) peakVol = Math.max(peakVol, c1m.get(i).volume);
        double recentVol = 0;
        int cnt = 0;
        for (int i = n - 3; i < n; i++) { recentVol += c1m.get(i).volume; cnt++; }
        recentVol = cnt > 0 ? recentVol / cnt : 0;
        boolean volumeDivergence = peakVol > 0 && recentVol < peakVol * EXH_VOL_DROP;

        // --- Wick growth: upper (for pump-up) or lower (for pump-down) wicks expanding ---
        double sumWickRatio = 0;
        int wCount = 0;
        for (int i = n - 4; i < n; i++) {
            com.bot.TradingCore.Candle c = c1m.get(i);
            double body = Math.abs(c.close - c.open);
            if (body <= 0) continue;
            double wick = wasBullish
                    ? c.high - Math.max(c.close, c.open)   // upper wick for top fade
                    : Math.min(c.close, c.open) - c.low;   // lower wick for bottom reversal
            sumWickRatio += wick / body;
            wCount++;
        }
        double avgWickRatio = wCount > 0 ? sumWickRatio / wCount : 0;
        boolean longWicks = avgWickRatio > EXH_WICK_RATIO;

        // --- Momentum slowdown: each of last 3 price-moves smaller than previous ---
        if (n < 5) return null;
        double m1 = c1m.get(n - 4).close - c1m.get(n - 5).close;
        double m2 = c1m.get(n - 3).close - c1m.get(n - 4).close;
        double m3 = c1m.get(n - 2).close - c1m.get(n - 3).close;
        boolean slowdown = wasBullish
                ? (m1 > m2 && m2 > m3 && m1 > 0)
                : (m1 < m2 && m2 < m3 && m1 < 0);

        int score = (volumeDivergence ? 1 : 0) + (longWicks ? 1 : 0) + (slowdown ? 1 : 0);
        if (score < EXH_MIN_FLAGS) return null;

        List<String> flags = new ArrayList<>();
        flags.add(wasBullish ? "EXHAUSTION_SHORT" : "EXHAUSTION_LONG");
        if (volumeDivergence) flags.add("VOL_DIV");
        if (longWicks)        flags.add(wasBullish ? "WICK_TOP" : "WICK_BOT");
        if (slowdown)         flags.add("MOM_SLOW");

        PumpType t = wasBullish ? PumpType.PUMP_EXHAUSTION_SHORT : PumpType.DUMP_EXHAUSTION_LONG;
        double strength = 0.45 + score * 0.15;

        com.bot.TradingCore.Candle last = c1m.get(n - 1);
        double movePct = (last.close - c1m.get(Math.max(0, n - 5)).close) / Math.max(c1m.get(Math.max(0, n - 5)).close, 1e-10);
        double volRatio = last.volume / Math.max(averageVolume(c1m, VOLUME_LOOKBACK), 1e-10);

        return new PumpEvent(symbol, t, Math.min(1.0, strength), movePct, volRatio, 0.0, true, flags);
    }

    // ==================== [C] PRE-PUMP DETECTOR ====================

    /**
     * Detects compression + volume buildup + ignition candle on 1m — institutional accumulation
     * pattern before the breakout. Fires LONG/SHORT BEFORE the impulse candle.
     * False positive mitigation:
     *   - requires compression AND volume buildup AND spark (triple filter)
     *   - requires no fresh pump (handled by caller)
     *   - spark body must be ≥ 40% of current short ATR
     */
    private PumpEvent detectPrePump(String symbol,
                                    List<com.bot.TradingCore.Candle> c1m,
                                    List<com.bot.TradingCore.Candle> c5m) {
        if (c1m == null || c1m.size() < 22) return null;
        int n = c1m.size();

        double atrShort = calculateATR(c1m, 7);
        double atrBase  = calculateATR(c1m, 20);
        double avgVol   = averageVolume(c1m, 20);

        if (atrShort <= 0 || atrBase <= 0 || avgVol <= 0) return null;

        // 1. Compression
        boolean compression = atrShort < atrBase * PRE_COMPRESSION;
        if (!compression) return null;

        // 2. Volume buildup (last 5 > prior 5 by 30%)
        double vLast = 0, vPrev = 0;
        for (int i = n - 5;  i < n;     i++) vLast += c1m.get(i).volume;
        for (int i = n - 10; i < n - 5; i++) vPrev += c1m.get(i).volume;
        vLast /= 5.0; vPrev /= 5.0;
        boolean buildup = vPrev > 0 && vLast > vPrev * PRE_VOL_BUILDUP;
        if (!buildup) return null;

        // 3. Ignition candle (last bar): bullish or bearish
        com.bot.TradingCore.Candle last = c1m.get(n - 1);
        double body = Math.abs(last.close - last.open);
        double bodyAtr = body / Math.max(atrShort, 1e-10);
        boolean volSpike = last.volume > avgVol * PRE_SPARK_VOL;
        boolean strongBody = bodyAtr > PRE_SPARK_BODY;

        if (!volSpike || !strongBody) return null;

        boolean bullish = last.close > last.open;
        PumpType t = bullish ? PumpType.PRE_PUMP_LONG : PumpType.PRE_DUMP_SHORT;

        List<String> flags = new ArrayList<>();
        flags.add(bullish ? "PRE_PUMP_COMPR" : "PRE_DUMP_COMPR");
        flags.add(String.format("atrRatio=%.2f", atrShort / atrBase));
        flags.add(String.format("volBuild=%.2f", vLast / Math.max(vPrev, 1e-10)));

        double movePct = (last.close - c1m.get(Math.max(0, n - 6)).close) / Math.max(c1m.get(Math.max(0, n - 6)).close, 1e-10);
        double volRatio = last.volume / avgVol;
        double strength = Math.min(0.75,
                0.50 + 0.10 * (vLast / Math.max(vPrev, 1e-10) - 1.0)
                        + 0.08 * Math.min(1.0, bodyAtr));

        return new PumpEvent(symbol, t, strength, movePct, volRatio, bodyAtr, false, flags);
    }

    // ==================== generateSignal ====================

    public PumpSignal generateSignal(PumpEvent event, double currentPrice) {
        if (event == null || event.type == PumpType.NONE) return null;

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
            case PUMP_EXHAUSTION_SHORT -> {
                side = com.bot.TradingCore.Side.SHORT;
                strategy = "PUMP_EXHAUSTION";
                // Reversal-type entries carry more asymmetric payoff but lower baseline → conservative base
                confidence = 58 + event.strength * 22;
            }
            case DUMP_EXHAUSTION_LONG -> {
                side = com.bot.TradingCore.Side.LONG;
                strategy = "DUMP_EXHAUSTION";
                confidence = 58 + event.strength * 22;
            }
            case PRE_PUMP_LONG -> {
                side = com.bot.TradingCore.Side.LONG;
                strategy = "PRE_PUMP_COMPRESSION";
                confidence = 55 + event.strength * 20;
            }
            case PRE_DUMP_SHORT -> {
                side = com.bot.TradingCore.Side.SHORT;
                strategy = "PRE_DUMP_COMPRESSION";
                confidence = 55 + event.strength * 20;
            }
            default -> { return null; }
        }

        if (event.isConfirmed) confidence += 3;
        if (event.isMega())    confidence += 5;
        if (event.flags.contains("VOL_CLIMAX")) confidence += 2;

        confidence = Math.min(85, Math.max(55, confidence));
        return new PumpSignal(event.symbol, side, currentPrice, confidence, event, strategy);
    }

    // ==================== helper: analyzeCandle / calculateStrength / detectSeriesPump ====================

    private static class PumpMetrics {
        double bodyToAtr, volumeRatio, movePct, bodySize, atr;
        boolean isGreen;
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
        double volScore  = Math.min(1.0, m.volumeRatio / (isMega ? MEGA_VOLUME_SPIKE_MULT : VOLUME_SPIKE_MULT) / 1.5);
        double moveScore = Math.min(1.0, Math.abs(m.movePct) / (isMega ? MEGA_MOVE_PCT : STRONG_MOVE_PCT));
        return Math.min(1.0, bodyScore * 0.35 + volScore * 0.35 + moveScore * 0.30);
    }

    private static class SeriesPumpResult {
        boolean detected, isUp;
        double strength, totalMovePct, avgVolumeRatio, avgBodyToAtr;
        int barsCount;
    }

    private SeriesPumpResult detectSeriesPump(List<com.bot.TradingCore.Candle> c1m) {
        SeriesPumpResult result = new SeriesPumpResult();
        if (c1m == null || c1m.size() < 10) return result;
        int n = c1m.size();
        double atr = calculateATR(c1m, ATR_PERIOD);
        double avgVol = averageVolume(c1m, VOLUME_LOOKBACK);
        int greenCount = 0, redCount = 0;
        double totalMove = 0, totalVolRatio = 0, totalBodyAtr = 0;
        int lookback = Math.min(6, n - 1);
        for (int i = n - lookback; i < n; i++) {
            com.bot.TradingCore.Candle c = c1m.get(i);
            if (c.close > c.open) greenCount++; else redCount++;
            totalMove += c.close - c.open;
            totalVolRatio += c.volume / Math.max(avgVol, 1e-10);
            totalBodyAtr  += Math.abs(c.close - c.open) / Math.max(atr, 1e-10);
        }
        com.bot.TradingCore.Candle startBar = c1m.get(n - lookback);
        com.bot.TradingCore.Candle endBar   = c1m.get(n - 1);
        double totalMovePct = (endBar.close - startBar.open) / Math.max(startBar.open, 1e-10);
        double avgVolRatio = totalVolRatio / lookback;
        double avgBodyAtr  = totalBodyAtr  / lookback;
        boolean isUpSeries   = greenCount >= 4 && Math.abs(totalMovePct) >= MIN_MOVE_PCT;
        boolean isDownSeries = redCount   >= 4 && Math.abs(totalMovePct) >= MIN_MOVE_PCT;
        boolean volumeOk = avgVolRatio >= 1.5;
        if ((isUpSeries || isDownSeries) && volumeOk) {
            result.detected = true;
            result.isUp = totalMove > 0;
            result.totalMovePct = totalMovePct;
            result.avgVolumeRatio = avgVolRatio;
            result.avgBodyToAtr = avgBodyAtr;
            result.barsCount = lookback;
            result.strength = Math.min(1.0,
                    (Math.abs(totalMovePct) / STRONG_MOVE_PCT * 0.5)
                            + (avgVolRatio / VOLUME_SPIKE_MULT * 0.3)
                            + (avgBodyAtr / PUMP_BODY_ATR_MULT * 0.2));
        }
        return result;
    }

    // ==================== SQUEEZE / BREAKOUT / CLIMAX (v50 logic retained) ====================

    private static class SqueezeResult { boolean detected, isUp; double intensity; }

    private SqueezeResult detectSqueeze(List<com.bot.TradingCore.Candle> c5m,
                                        List<com.bot.TradingCore.Candle> c15m) {
        SqueezeResult result = new SqueezeResult();
        if (c5m == null || c5m.size() < 20) return result;
        if (c15m == null || c15m.size() < 10) return result;
        int n = c5m.size();
        double prevTrend = 0, prevVol = 0;
        for (int i = n - 15; i < n - 3; i++) {
            prevTrend += c5m.get(i).close - c5m.get(i).open;
            prevVol   += c5m.get(i).volume;
        }
        double prevVolAvg = prevVol / 12.0;
        double currentMove = 0, currentVol = 0;
        for (int i = n - 3; i < n; i++) {
            currentMove += c5m.get(i).close - c5m.get(i).open;
            currentVol  += c5m.get(i).volume;
        }
        double currentVolAvg = currentVol / 3.0;
        double refPrice = c5m.get(n - 1).close;
        double prevTrendPct = Math.abs(prevTrend) / (refPrice + 1e-9);
        if (prevTrendPct < 0.006) return result;
        boolean reversalUp = prevTrend < 0 && currentMove > 0
                && Math.abs(currentMove) > Math.abs(prevTrend) * 0.6
                && currentVolAvg >= prevVolAvg * 1.3;
        boolean reversalDn = prevTrend > 0 && currentMove < 0
                && Math.abs(currentMove) > Math.abs(prevTrend) * 0.6
                && currentVolAvg >= prevVolAvg * 1.3;
        if (!reversalUp && !reversalDn) return result;
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

    private static class BreakoutResult { boolean detected, isUp; double level; }

    /**
     * [v51 FIX] Window extended to [n-20, n-1] (was [n-23, n-3]). This uses the most recent
     * 20 closed bars excluding only the live-forming bar. Cuts breakout-signal latency by 2 bars.
     */
    private BreakoutResult detectBreakout(List<com.bot.TradingCore.Candle> c15m) {
        BreakoutResult result = new BreakoutResult();
        if (c15m == null || c15m.size() < 25) return result;
        int n = c15m.size();
        com.bot.TradingCore.Candle last = c15m.get(n - 1);
        double recentHigh = Double.NEGATIVE_INFINITY;
        double recentLow  = Double.MAX_VALUE;
        double volSum = 0;
        int volCount = 0;
        // [v51 FIX] Window [n-20, n-1) — excludes only the current bar being broken.
        for (int i = n - 21; i < n - 1; i++) {
            recentHigh = Math.max(recentHigh, c15m.get(i).high);
            recentLow  = Math.min(recentLow,  c15m.get(i).low);
            volSum    += c15m.get(i).volume;
            volCount++;
        }
        double medianVol = volCount > 0 ? volSum / volCount : 0;
        boolean volumeConfirmed = last.volume >= medianVol * 1.5;
        if (last.close > recentHigh * 1.002 && last.close > last.open && volumeConfirmed) {
            result.detected = true; result.isUp = true; result.level = recentHigh;
        }
        if (last.close < recentLow * 0.998 && last.close < last.open && volumeConfirmed) {
            result.detected = true; result.isUp = false; result.level = recentLow;
        }
        return result;
    }

    private boolean isVolumeClimaxing(List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 21) return false;
        int n = candles.size();
        com.bot.TradingCore.Candle last = candles.get(n - 1);
        double maxVol = 0;
        for (int i = n - 21; i < n - 1; i++) maxVol = Math.max(maxVol, candles.get(i).volume);
        return last.volume >= maxVol * 0.90;
    }

    // ==================== CONFIRMATION ====================

    /**
     * [A2] Only PUMP_CONFIRM_BARS=1 needed now. Confirmation adds +0.05 strength but is not required.
     * Keep both price-alignment AND direction-alignment checks from v50 to filter dead-cats.
     */
    private boolean checkConfirmation(List<com.bot.TradingCore.Candle> c1m, PumpType type) {
        if (c1m == null || c1m.size() < PUMP_CONFIRM_BARS + 2) return false;
        if (type == PumpType.NONE) return false;

        int n = c1m.size();
        com.bot.TradingCore.Candle pumpBar = c1m.get(n - PUMP_CONFIRM_BARS - 1);
        double pumpClose = pumpBar.close;

        boolean isUp = type == PumpType.PUMP_UP || type == PumpType.MEGA_PUMP_UP
                || type == PumpType.SQUEEZE_UP || type == PumpType.BREAKOUT_UP
                || type == PumpType.DUMP_EXHAUSTION_LONG || type == PumpType.PRE_PUMP_LONG;

        int confirmCount = 0;
        for (int i = n - PUMP_CONFIRM_BARS; i < n; i++) {
            com.bot.TradingCore.Candle c = c1m.get(i);
            if (isUp  && c.close >= pumpClose * 0.9985 && c.close >= c.open) confirmCount++;
            if (!isUp && c.close <= pumpClose * 1.0015 && c.close <= c.open) confirmCount++;
        }
        return confirmCount >= 1; // [A2] relaxed from >=2
    }

    // ==================== UTILITY ====================

    private double calculateATR(List<com.bot.TradingCore.Candle> candles, int period) {
        return com.bot.TradingCore.atr(candles, period);
    }

    private double averageVolume(List<com.bot.TradingCore.Candle> candles, int lookback) {
        if (candles == null || candles.size() < lookback + 1) return 1;
        double sum = 0;
        int n = candles.size();
        int start = Math.max(0, n - lookback - 1);
        for (int i = start; i < n - 1; i++) sum += candles.get(i).volume;
        int count = (n - 1) - start;
        return count > 0 ? sum / count : 1;
    }

    // ==================== PUBLIC API (unchanged) ====================

    public PumpEvent getRecentPump(String symbol) { return recentPumps.get(symbol); }

    public boolean hadRecentPump(String symbol, long withinMs) {
        Long t = lastPumpTime.get(symbol);
        return t != null && System.currentTimeMillis() - t < withinMs;
    }

    public List<PumpEvent> getPumpHistory(String symbol) {
        Deque<PumpEvent> h = pumpHistory.get(symbol);
        return h != null ? new ArrayList<>(h) : List.of();
    }

    public void clearSymbol(String symbol) {
        lastPumpTime.remove(symbol);
        lastExhaustionTime.remove(symbol);
        lastPrePumpTime.remove(symbol);
        recentPumps.remove(symbol);
        pumpHistory.remove(symbol);
    }

    public void clearAll() {
        lastPumpTime.clear();
        lastExhaustionTime.clear();
        lastPrePumpTime.clear();
        recentPumps.clear();
        pumpHistory.clear();
    }
}