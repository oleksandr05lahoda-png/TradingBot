package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║        DecisionEngineMerged — GODBOT v7.0 CLUSTER EDITION              ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  ПОЛНАЯ ПЕРЕРАБОТКА SCORING СИСТЕМЫ v7.0:                               ║
 * ║                                                                          ║
 * ║  [CLUSTER] Кластерная система вместо суммирования:                      ║
 * ║    27 факторов → 5 независимых кластеров:                               ║
 * ║    1. STRUCTURE — BOS, HH/HL, Market Structure, FVG, OB, LiqSweep      ║
 * ║    2. MOMENTUM  — Impulse, AntiLag, PumpHunter, Compression            ║
 * ║    3. VOLUME    — VolumeDelta, VolumeSpike, OBI                        ║
 * ║    4. HTF       — 1H bias, 2H bias, VWAP alignment                    ║
 * ║    5. DERIVATIVES — Funding Rate, OI, Divergences                      ║
 * ║    Внутри кластера: max(LONG scores), max(SHORT scores)                ║
 * ║    Между кластерами: сумма — это убирает корреляцию                     ║
 * ║    Бонус за confluence: если 3+ кластера согласны → +0.15              ║
 * ║                                                                          ║
 * ║  [CONF-FIX] Калиброванная уверенность:                                  ║
 * ║    - Базовая 50% + нормализованный score → 50-88%                      ║
 * ║    - Потолок 88% (никогда выше — защита от overconfidence)              ║
 * ║    - Бонус за количество согласных кластеров, а не факторов             ║
 * ║    - Историческая калибровка сохранена                                   ║
 * ║                                                                          ║
 * ║  [CRASH] Crash Mode v2.0:                                               ║
 * ║    - При aggressiveShort: SHORT_EXHAUSTION, TREND_EXH_S = ignored      ║
 * ║    - btcCrashScore → direct SHORT boost (до фильтров)                  ║
 * ║    - btcMomentumAccel → early SHORT detection                           ║
 * ║    - 2H_VETO при краше не блокирует SHORT                              ║
 * ║                                                                          ║
 * ║  [QUALITY] Фильтры качества сигнала:                                    ║
 * ║    - Минимум 2 согласных кластера для сигнала                           ║
 * ║    - Pullback confirmation на 1m/5m                                     ║
 * ║    - Volume delta gate для дивергенций                                   ║
 * ║    - Relative Strength filter (RS Trap при краше)                       ║
 * ║    - Cooldown: TOP=4m / ALT=3m / MEME=2m                               ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public final class DecisionEngineMerged {

    // ── Enums ──────────────────────────────────────────────────────
    public enum CoinCategory { TOP, ALT, MEME }
    public enum MarketState  { STRONG_TREND, WEAK_TREND, RANGE }
    public enum HTFBias      { BULL, BEAR, NONE }

    // ── Константы ─────────────────────────────────────────────────
    private static final int    MIN_BARS        = 150;
    private static final long   COOLDOWN_TOP    = 4  * 60_000L;
    private static final long   COOLDOWN_ALT    = 3  * 60_000L;
    private static final long   COOLDOWN_MEME   = 2  * 60_000L;
    private static final double BASE_CONF       = 52.0;
    private static final int    CALIBRATION_WIN = 120;
    private static final double MIN_CONF_FLOOR  = 47.0;
    private static final double MIN_CONF_CEIL   = 65.0;

    // Дивергенции — штраф вместо хард-лока
    private static final double DIV_PENALTY_SCORE  = 0.55;
    private static final double DIV_VOL_DELTA_GATE = 1.80;
    private static final double DIV_TREND_RSI_GATE = 72.0;

    // [v7.0] Crash score порог
    private static final double CRASH_SCORE_BOOST_THRESHOLD = 0.35;
    private static final double CRASH_SHORT_BOOST_BASE = 0.75;

    // [v7.0] Cluster confluence bonus
    private static final double CLUSTER_CONFLUENCE_BONUS = 0.15;
    private static final int    MIN_AGREEING_CLUSTERS    = 2;

    // ── State ─────────────────────────────────────────────────────
    private final Map<String, Double>           symbolMinConf    = new ConcurrentHashMap<>();
    private volatile double                     globalMinConf    = BASE_CONF;
    private final Map<String, Long>             cooldownMap      = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>>    recentDirs       = new ConcurrentHashMap<>();
    private final Map<String, Double>           lastSigPrice     = new ConcurrentHashMap<>();
    private final Map<String, FundingOIData>    fundingCache     = new ConcurrentHashMap<>();
    private final Map<String, Deque<CalibRecord>> calibHist      = new ConcurrentHashMap<>();
    private final Map<String, Double>           volumeDeltaMap   = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>    vdHistory        = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>    relStrengthHistory = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger>    signalCountBySymbol = new ConcurrentHashMap<>();

    // [v7.0] GIC reference
    private volatile com.bot.GlobalImpulseController gicRef = null;
    private com.bot.PumpHunter pumpHunter;

    public DecisionEngineMerged() {}

    // ── Setters ───────────────────────────────────────────────────
    public void setPumpHunter(com.bot.PumpHunter ph) { this.pumpHunter = ph; }
    public void setGIC(com.bot.GlobalImpulseController gic) { this.gicRef = gic; }

    public void setVolumeDelta(String sym, double delta) {
        volumeDeltaMap.put(sym, delta);
        Deque<Double> hist = vdHistory.computeIfAbsent(sym, k -> new ArrayDeque<>());
        hist.addLast(Math.abs(delta));
        if (hist.size() > 50) hist.removeFirst();
    }

    private double getVolumeDeltaRatio(String sym) {
        Double current = volumeDeltaMap.get(sym);
        if (current == null || current == 0) return 0.0;
        Deque<Double> hist = vdHistory.get(sym);
        if (hist == null || hist.size() < 5) return 1.0;
        double avg = hist.stream().mapToDouble(Double::doubleValue).average().orElse(1.0);
        return avg < 1e-9 ? 1.0 : Math.abs(current) / avg;
    }

    public void updateRelativeStrength(String symbol, double symbolReturn15m, double btcReturn15m) {
        double rs;
        if (Math.abs(btcReturn15m) < 0.0001) {
            rs = symbolReturn15m > 0 ? 0.7 : 0.3;
        } else if (btcReturn15m < 0 && symbolReturn15m > 0) {
            rs = 0.85 + Math.min(Math.abs(symbolReturn15m) * 10, 0.14);
        } else {
            rs = clamp(0.5 + (symbolReturn15m - btcReturn15m) / (Math.abs(btcReturn15m) * 2), 0.0, 1.0);
        }
        Deque<Double> h = relStrengthHistory.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        h.addLast(rs);
        if (h.size() > 20) h.removeFirst();
    }

    private double getRelativeStrength(String symbol) {
        Deque<Double> h = relStrengthHistory.get(symbol);
        if (h == null || h.isEmpty()) return 0.5;
        return h.stream().mapToDouble(Double::doubleValue).average().orElse(0.5);
    }

    // ══════════════════════════════════════════════════════════════
    //  CLUSTER SCORE HOLDER
    //  Каждый кластер хранит свой лучший LONG и SHORT score
    // ══════════════════════════════════════════════════════════════

    private static final class ClusterScores {
        double longScore  = 0;
        double shortScore = 0;
        final List<String> flags = new ArrayList<>();

        void addLong(double score, String flag) {
            longScore = Math.max(longScore, score);
            if (flag != null) flags.add(flag);
        }

        void addShort(double score, String flag) {
            shortScore = Math.max(shortScore, score);
            if (flag != null) flags.add(flag);
        }

        // Мягкое добавление — accumulate вместо max (для бонусов)
        void boostLong(double score, String flag) {
            longScore += score;
            if (flag != null) flags.add(flag);
        }

        void boostShort(double score, String flag) {
            shortScore += score;
            if (flag != null) flags.add(flag);
        }

        void penalizeLong(double mult) { longScore *= mult; }
        void penalizeShort(double mult) { shortScore *= mult; }

        boolean favorsLong()  { return longScore > shortScore && longScore > 0.10; }
        boolean favorsShort() { return shortScore > longScore && shortScore > 0.10; }
        boolean hasSignal()   { return longScore > 0.10 || shortScore > 0.10; }

        double netLong()  { return longScore - shortScore * 0.3; }
        double netShort() { return shortScore - longScore * 0.3; }
    }

    // ══════════════════════════════════════════════════════════════
    //  MARKET CONTEXT
    // ══════════════════════════════════════════════════════════════

    private static final class MarketContext {
        final double volMultiplier;
        final double scoreScale;
        final double thresholdScale;
        final double atrPct;

        MarketContext(double volMult, double atrPct) {
            this.volMultiplier  = volMult;
            this.atrPct         = atrPct;
            this.scoreScale     = clamp(1.0 / Math.sqrt(volMult), 0.65, 1.45);
            this.thresholdScale = clamp(volMult, 0.5, 2.0);
        }

        double s(double baseScore) { return baseScore * scoreScale; }

        static double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
    }

    private MarketContext buildMarketContext(List<com.bot.TradingCore.Candle> c15, double price) {
        double atrCurrent = atr(c15, 14);
        int baseWindow = Math.min(c15.size() - 14, 672);
        double atrBase = baseWindow >= 50
                ? atr(c15.subList(0, baseWindow), Math.min(14, baseWindow - 1))
                : atrCurrent;
        double volMult = atrBase > 0 ? atrCurrent / atrBase : 1.0;
        return new MarketContext(clamp(volMult, 0.3, 3.0), atrCurrent / (price + 1e-9));
    }

    // ══════════════════════════════════════════════════════════════
    //  INNER DATA TYPES
    // ══════════════════════════════════════════════════════════════

    public static final class FundingOIData {
        public final double fundingRate, openInterest, oiChange1h, oiChange4h;
        public final double prevFundingRate, fundingDelta;
        public final long   timestamp;

        public FundingOIData(double fr, double oi, double oi1h, double oi4h) {
            this(fr, oi, oi1h, oi4h, fr, 0.0);
        }
        public FundingOIData(double fr, double oi, double oi1h, double oi4h, double prevFr, double delta) {
            this.fundingRate = fr; this.openInterest = oi;
            this.oiChange1h = oi1h; this.oiChange4h = oi4h;
            this.prevFundingRate = prevFr; this.fundingDelta = delta;
            this.timestamp = System.currentTimeMillis();
        }
        public boolean isValid() { return System.currentTimeMillis() - timestamp < 5 * 60_000L; }
    }

    private static final class CalibRecord {
        final double predicted;
        final boolean correct;
        CalibRecord(double p, boolean c) { predicted = p; correct = c; }
    }

    // ══════════════════════════════════════════════════════════════
    //  TRADE IDEA
    // ══════════════════════════════════════════════════════════════

    public static final class TradeIdea {
        public final String           symbol;
        public final com.bot.TradingCore.Side side;
        public final double           price, stop, take, tp1, tp2, tp3;
        public final double           probability;
        public final List<String>     flags;
        public final double           fundingRate, fundingDelta, oiChange;
        public final String           htfBias;
        public final double           rr;
        public final CoinCategory     category;

        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take, double rr,
                         double probability, List<String> flags,
                         double fundingRate, double fundingDelta,
                         double oiChange, String htfBias, CoinCategory cat) {
            this.symbol = symbol; this.side = side;
            this.price = price; this.stop = stop; this.take = take;
            this.rr = rr; this.probability = probability;
            this.flags = flags != null ? Collections.unmodifiableList(new ArrayList<>(flags)) : List.of();
            this.fundingRate = fundingRate; this.fundingDelta = fundingDelta;
            this.oiChange = oiChange; this.htfBias = htfBias; this.category = cat;

            double risk = Math.abs(price - stop);
            boolean long_ = side == com.bot.TradingCore.Side.LONG;
            this.tp1 = long_ ? price + risk * 1.0 : price - risk * 1.0;
            this.tp2 = long_ ? price + risk * 2.0 : price - risk * 2.0;
            this.tp3 = long_ ? price + risk * 3.2 : price - risk * 3.2;
        }

        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take,
                         double probability, List<String> flags) {
            this(symbol, side, price, stop, take, 2.0, probability, flags,
                    0, 0, 0, "NONE", CoinCategory.ALT);
        }

        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take,
                         double probability, List<String> flags,
                         double fundingRate, double oiChange, String htfBias) {
            this(symbol, side, price, stop, take, 2.0, probability, flags,
                    fundingRate, 0, oiChange, htfBias, CoinCategory.ALT);
        }

        public String toTelegramString() {
            String emoji   = probability >= 83 ? "🔥" : probability >= 74 ? "✅"
                    : probability >= 65 ? "🟡" : "⚪";
            String sideStr = side == com.bot.TradingCore.Side.LONG ? "📈 LONG" : "📉 SHORT";
            String catStr  = category == CoinCategory.MEME ? "🐸 MEME"
                    : category == CoinCategory.TOP  ? "👑 TOP" : "🔷 ALT";

            double riskPct = Math.abs(price - stop) / price * 100;
            double rp1Pct  = Math.abs(tp1 - price) / price * 100;
            double rp2Pct  = Math.abs(tp2 - price) / price * 100;
            double rp3Pct  = Math.abs(tp3 - price) / price * 100;

            String flagStr = flags.isEmpty() ? "-" : String.join(", ", flags);
            String time    = java.time.ZonedDateTime.now(java.time.ZoneId.of("Europe/Warsaw"))
                    .toLocalTime().withNano(0).toString();

            StringBuilder extra = new StringBuilder();
            if (Math.abs(fundingRate) > 0.0008)
                extra.append(String.format("%n💸 FR: %+.3f%%", fundingRate * 100));
            if (Math.abs(fundingDelta) > 0.0003)
                extra.append(String.format(" Δ%+.3f%%", fundingDelta * 100));
            if (Math.abs(oiChange) > 0.5)
                extra.append(String.format(" | OI: %+.1f%%", oiChange));
            if (!"NONE".equals(htfBias) && !htfBias.isEmpty())
                extra.append(String.format("%n📊 2H Bias: %s", htfBias));
            if (rr > 0)
                extra.append(String.format(" | R/R: 1:%.1f", rr));

            return String.format(
                    "%s *%s* → *%s* %s%n"
                            + "💰 Price:  `%.6f`%n"
                            + "🎯 Prob:   *%.0f%%*%n"
                            + "🛡 SL:     `%.6f`  (%.2f%% риска)%n"
                            + "🟢 TP1:    `%.6f`  (+%.2f%%)  50%% → BE%n"
                            + "🔵 TP2:    `%.6f`  (+%.2f%%)  30%%%n"
                            + "💎 TP3:    `%.6f`  (+%.2f%%)  20%% трейл%n"
                            + "🏷 %s%s%n"
                            + "_⏰ %s_",
                    emoji, symbol, sideStr, catStr,
                    price,
                    probability,
                    stop, riskPct,
                    tp1, rp1Pct,
                    tp2, rp2Pct,
                    tp3, rp3Pct,
                    flagStr, extra,
                    time
            );
        }

        @Override public String toString() { return toTelegramString(); }
    }

    // ══════════════════════════════════════════════════════════════
    //  PUBLIC API
    // ══════════════════════════════════════════════════════════════

    public void updateFundingOI(String sym, double fr, double oi, double oi1h, double oi4h) {
        FundingOIData prev = fundingCache.get(sym);
        double prevFr = prev != null ? prev.fundingRate : fr;
        fundingCache.put(sym, new FundingOIData(fr, oi, oi1h, oi4h, prevFr, fr - prevFr));
    }

    public FundingOIData getFundingOI(String sym) {
        FundingOIData d = fundingCache.get(sym);
        return (d != null && d.isValid()) ? d : null;
    }

    public void recordSignalResult(String sym, double prob, boolean correct) {
        Deque<CalibRecord> h = calibHist.computeIfAbsent(sym, k -> new ArrayDeque<>());
        h.addLast(new CalibRecord(prob, correct));
        while (h.size() > CALIBRATION_WIN) h.removeFirst();
        updateSymbolThreshold(sym);
    }

    private void updateSymbolThreshold(String sym) {
        Deque<CalibRecord> hist = calibHist.get(sym);
        if (hist == null || hist.size() < 20) return;
        long correct = hist.stream().filter(r -> r.correct).count();
        double accuracy = (double) correct / hist.size();
        double base = globalMinConf;
        if (accuracy < 0.45)      base += 5.0;
        else if (accuracy < 0.50) base += 2.5;
        else if (accuracy > 0.65) base -= 3.0;
        else if (accuracy > 0.60) base -= 1.5;
        symbolMinConf.put(sym, clamp(base, MIN_CONF_FLOOR, MIN_CONF_CEIL));
    }

    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             List<com.bot.TradingCore.Candle> c2h,
                             CoinCategory cat) {
        return generate(symbol, c1, c5, c15, c1h, c2h, cat, System.currentTimeMillis());
    }

    public TradeIdea analyze(String symbol,
                             List<com.bot.TradingCore.Candle> c1,
                             List<com.bot.TradingCore.Candle> c5,
                             List<com.bot.TradingCore.Candle> c15,
                             List<com.bot.TradingCore.Candle> c1h,
                             CoinCategory cat) {
        return generate(symbol, c1, c5, c15, c1h, null, cat, System.currentTimeMillis());
    }

    // ══════════════════════════════════════════════════════════════
    //  CORE GENERATE — v7.0 CLUSTER ARCHITECTURE
    // ══════════════════════════════════════════════════════════════

    private TradeIdea generate(String symbol,
                               List<com.bot.TradingCore.Candle> c1,
                               List<com.bot.TradingCore.Candle> c5,
                               List<com.bot.TradingCore.Candle> c15,
                               List<com.bot.TradingCore.Candle> c1h,
                               List<com.bot.TradingCore.Candle> c2h,
                               CoinCategory cat,
                               long now) {

        if (!valid(c15) || !valid(c1h)) return null;

        double price     = last(c15).close;
        double atr14     = atr(c15, 14);
        double lastRange = last(c15).high - last(c15).low;

        if (lastRange > atr14 * 4.5 || atr14 <= 0) return null;
        atr14 = Math.max(atr14, price * 0.0012);

        // ── GIC crash state ──────────────────────────────────────
        com.bot.GlobalImpulseController.GlobalContext gicCtx =
                gicRef != null ? gicRef.getContext() : null;
        boolean aggressiveShort = gicRef != null && gicRef.isAggressiveShortMode();
        double btcCrashScore    = gicCtx != null ? gicCtx.btcCrashScore : 0.0;
        double btcAccel         = gicCtx != null ? gicCtx.btcMomentumAccel : 0.0;
        double gicShortBoost    = gicCtx != null ? gicCtx.shortBoost : 1.0;

        MarketContext mctx = buildMarketContext(c15, price);
        MarketState state  = detectState(c15);
        HTFBias     bias1h = detectBias1H(c1h);
        HTFBias     bias2h = (c2h != null && c2h.size() >= 50) ? detectBias2H(c2h) : HTFBias.NONE;

        adaptGlobalMinConf(state, atr14, price);

        // При не-агрессивном режиме: RANGE + ADX < 18 = нет сигнала
        if (!aggressiveShort && state == MarketState.RANGE && adx(c15, 14) < 18) return null;

        // ════════════════════════════════════════════════════════
        //  ИНИЦИАЛИЗАЦИЯ 5 КЛАСТЕРОВ
        // ════════════════════════════════════════════════════════
        ClusterScores cStructure   = new ClusterScores(); // BOS, HH/HL, FVG, OB, LiqSweep
        ClusterScores cMomentum    = new ClusterScores(); // Impulse, AntiLag, Pump, Compression
        ClusterScores cVolume      = new ClusterScores(); // VolumeDelta, VolumeSpike
        ClusterScores cHTF         = new ClusterScores(); // 1H, 2H bias, VWAP
        ClusterScores cDerivatives = new ClusterScores(); // Funding, OI, Divergences
        List<String> allFlags = new ArrayList<>();

        // ════════════════════════════════════════════════════════
        // [CLUSTER 0] BTC CRASH — прямой override, ДО кластеров
        // ════════════════════════════════════════════════════════
        double crashBoost = 0;
        if (btcCrashScore >= CRASH_SCORE_BOOST_THRESHOLD) {
            crashBoost = (btcCrashScore - CRASH_SCORE_BOOST_THRESHOLD)
                    / (1.0 - CRASH_SCORE_BOOST_THRESHOLD) * CRASH_SHORT_BOOST_BASE;
            allFlags.add("BTC_CRASH" + String.format("%.0f", btcCrashScore * 100));
        }

        if (btcAccel > 0.004 && gicCtx != null
                && gicCtx.regime != com.bot.GlobalImpulseController.GlobalRegime.NEUTRAL
                && gicCtx.regime != com.bot.GlobalImpulseController.GlobalRegime.BTC_IMPULSE_UP
                && gicCtx.regime != com.bot.GlobalImpulseController.GlobalRegime.BTC_STRONG_UP) {
            double accelBoost = Math.min(0.50, btcAccel * 40);
            crashBoost += accelBoost;
            allFlags.add("BTC_ACCEL" + String.format("%.0f", btcAccel * 10000));
        }

        // ════════════════════════════════════════════════════════
        // КЛАСТЕР 1: STRUCTURE
        // ════════════════════════════════════════════════════════

        // Market Structure (HH/HL vs LL/LH)
        int structure = marketStructure(c15);
        if (structure ==  1) cStructure.addLong(mctx.s(0.55), "HH_HL");
        if (structure == -1) cStructure.addShort(mctx.s(0.55), "LL_LH");

        // BOS
        boolean bosUp   = detectBOSUp(c15);
        boolean bosDown = detectBOSDown(c15);
        if (bosUp)   cStructure.addLong(mctx.s(0.60), "BOS_UP");
        if (bosDown) cStructure.addShort(mctx.s(0.60), "BOS_DN");

        // FVG
        FVGResult fvg = detectFVG(c15);
        if (fvg.detected) {
            if (fvg.isBullish) cStructure.addLong(mctx.s(0.50), "FVG_BULL");
            else               cStructure.addShort(mctx.s(0.50), "FVG_BEAR");
        }

        // Order Block
        OrderBlockResult ob = detectOrderBlock(c15);
        if (ob.detected) {
            if (ob.isBullish && price <= ob.zone * 1.008)
                cStructure.addLong(mctx.s(0.52), "OB_BULL");
            if (!ob.isBullish && price >= ob.zone * 0.992)
                cStructure.addShort(mctx.s(0.52), "OB_BEAR");
        }

        // Liquidity Sweep
        boolean liqSweep = detectLiquiditySweep(c15);
        if (liqSweep) {
            com.bot.TradingCore.Candle lc = last(c15);
            double uw = lc.high - Math.max(lc.open, lc.close);
            double lw = Math.min(lc.open, lc.close) - lc.low;
            if (lw > uw) cStructure.addLong(mctx.s(0.58), "LIQ_SWEEP_L");
            else         cStructure.addShort(mctx.s(0.58), "LIQ_SWEEP_S");
        }

        // Bullish/Bearish structure
        if (bullishStructure(c15)) cStructure.boostLong(mctx.s(0.18), null);
        if (bearishStructure(c15)) cStructure.boostShort(mctx.s(0.18), null);

        // ════════════════════════════════════════════════════════
        // КЛАСТЕР 2: MOMENTUM
        // ════════════════════════════════════════════════════════

        // Anti-Lag (1m+5m+15m)
        AntiLagResult antiLag = detectAntiLag(c1, c5, c15);
        if (antiLag != null && antiLag.strength > 0.38) {
            double bonus = mctx.s(antiLag.strength * 1.30);
            if (antiLag.direction > 0) cMomentum.addLong(bonus, "ANTI_LAG_UP");
            else                       cMomentum.addShort(bonus, "ANTI_LAG_DN");
        }

        // Impulse (>0.55 ATR за 5 баров)
        boolean impulseFlag = impulse(c15);
        double move5 = (last(c15).close - c15.get(c15.size() - 5).close) / price;
        if (impulseFlag) {
            if (move5 > 0) cMomentum.addLong(mctx.s(0.50), "IMP_UP");
            else           cMomentum.addShort(mctx.s(0.50), "IMP_DN");
        }

        // Pullback к EMA21
        boolean pullUp   = pullback(c15, true);
        boolean pullDown = pullback(c15, false);
        if (pullUp)   cMomentum.addLong(mctx.s(0.55), "PULL_UP");
        if (pullDown) cMomentum.addShort(mctx.s(0.55), "PULL_DN");

        // Old Pump Detector
        OldPumpResult oldPump = detectOldPump(c1, c5, cat);
        if (oldPump.detected) {
            if (oldPump.direction > 0)
                cMomentum.addLong(mctx.s(oldPump.strength * 0.60), "PUMP_BULL");
            else
                cMomentum.addShort(mctx.s(oldPump.strength * 0.60), "PUMP_BEAR");
        }

        // PumpHunter
        if (pumpHunter != null) {
            com.bot.PumpHunter.PumpEvent pump = pumpHunter.getRecentPump(symbol);
            if (pump != null && pump.strength > 0.45) {
                if (pump.isBullish())
                    cMomentum.addLong(mctx.s(pump.strength * 0.55), "PUMP_HUNT_B");
                if (pump.isBearish())
                    cMomentum.addShort(mctx.s(pump.strength * 0.55), "PUMP_HUNT_S");
            }
        }

        // Compression Breakout
        CompressionResult comp = detectCompression(c15, c1);
        if (comp.breakout) {
            if (comp.direction > 0) cMomentum.addLong(mctx.s(0.58), "COMP_BREAK_UP");
            else                    cMomentum.addShort(mctx.s(0.58), "COMP_BREAK_DN");
        }

        // ════════════════════════════════════════════════════════
        // КЛАСТЕР 3: VOLUME
        // ════════════════════════════════════════════════════════

        // Volume Delta
        Double vd = volumeDeltaMap.get(symbol);
        double vdRatio = getVolumeDeltaRatio(symbol);
        if (vd != null && vdRatio > 1.5) {
            double vdScore = mctx.s(Math.min(0.55, vdRatio * 0.14));
            if (vd > 0) cVolume.addLong(vdScore, "VD_BUY");
            else        cVolume.addShort(vdScore, "VD_SELL");
        }

        // Volume Spike
        if (volumeSpike(c15, cat)) {
            // Spike усиливает доминирующее направление движения
            if (move5 > 0) cVolume.boostLong(mctx.s(0.22), "VOL_SPIKE");
            else           cVolume.boostShort(mctx.s(0.22), "VOL_SPIKE");
        }

        // ════════════════════════════════════════════════════════
        // КЛАСТЕР 4: HTF (Higher Timeframe)
        // ════════════════════════════════════════════════════════

        // 1H Bias
        if (bias1h == HTFBias.BULL) {
            cHTF.addLong(mctx.s(0.60), "1H_BULL");
        } else if (bias1h == HTFBias.BEAR) {
            cHTF.addShort(mctx.s(0.60), "1H_BEAR");
        }

        // 2H Bias
        if (bias2h == HTFBias.BULL) {
            cHTF.addLong(mctx.s(0.55), "2H_BULL");
        } else if (bias2h == HTFBias.BEAR) {
            cHTF.addShort(mctx.s(0.55), "2H_BEAR");
        }

        // 1H + 2H согласие = bonus
        if (bias1h == bias2h && bias1h != HTFBias.NONE) {
            if (bias1h == HTFBias.BULL) cHTF.boostLong(mctx.s(0.30), "1H2H_BULL");
            else                         cHTF.boostShort(mctx.s(0.35), "1H2H_BEAR");
        }

        // HTF конфликт = ослабление
        if ((bias1h == HTFBias.BULL && bias2h == HTFBias.BEAR) ||
                (bias1h == HTFBias.BEAR && bias2h == HTFBias.BULL)) {
            if (!aggressiveShort) {
                cHTF.penalizeLong(0.50);
                cHTF.penalizeShort(0.50);
            } else {
                cHTF.penalizeLong(0.30);
            }
            allFlags.add("HTF_CONFLICT");
        }

        // VWAP alignment
        int vwapLen = Math.min(50, c15.size());
        double vwapVal = vwap(c15.subList(c15.size() - vwapLen, c15.size()));
        if (price > vwapVal * 1.0008) cHTF.boostLong(mctx.s(0.18), "VWAP_BULL");
        if (price < vwapVal * 0.9992) cHTF.boostShort(mctx.s(0.18), "VWAP_BEAR");

        // ════════════════════════════════════════════════════════
        // КЛАСТЕР 5: DERIVATIVES (Funding, OI, Divergences)
        // ════════════════════════════════════════════════════════

        FundingOIData frData = fundingCache.get(symbol);
        boolean hasFR = false;
        double fundingRate = 0, fundingDelta = 0, oiChange = 0;
        if (frData != null && frData.isValid()) {
            fundingRate  = frData.fundingRate;
            fundingDelta = frData.fundingDelta;
            oiChange     = frData.oiChange1h;
            if (fundingRate < -0.0005) { cDerivatives.addLong(mctx.s(0.45), "FR_NEG"); hasFR = true; }
            if (fundingRate >  0.0010) { cDerivatives.addShort(mctx.s(0.40), "FR_POS"); hasFR = true; }
            if (fundingDelta < -0.0003) cDerivatives.boostLong(mctx.s(0.18), "FR_FALL");
            if (fundingDelta >  0.0003) cDerivatives.boostShort(mctx.s(0.18), "FR_RISE");
            if (oiChange > 3.0 && move5 > 0) cDerivatives.boostLong(mctx.s(0.25), "OI_UP");
            if (oiChange < -3.0 && move5 < 0) cDerivatives.boostShort(mctx.s(0.25), "OI_DN");
        }

        // RSI Divergences
        double rsi14 = rsi(c15, 14);
        double rsi7  = rsi(c15, 7);
        boolean bullDiv = bullDiv(c15);
        boolean bearDiv = bearDiv(c15);

        if (bullDiv) cDerivatives.addLong(mctx.s(0.60), "BULL_DIV");
        if (bearDiv) cDerivatives.addShort(mctx.s(0.60), "BEAR_DIV");

        // Дивергенции против позиции — штраф
        if (bearDiv) {
            double vdR = getVolumeDeltaRatio(symbol);
            if (vdR < DIV_VOL_DELTA_GATE) {
                cDerivatives.penalizeLong(DIV_PENALTY_SCORE);
                allFlags.add("BEAR_DIV_PENALTY");
            } else {
                allFlags.add("BEAR_DIV_VOL_OVERRIDE");
            }
        }
        if (bullDiv && !aggressiveShort) {
            double vdR = getVolumeDeltaRatio(symbol);
            if (vdR < DIV_VOL_DELTA_GATE) {
                cDerivatives.penalizeShort(DIV_PENALTY_SCORE);
                allFlags.add("BULL_DIV_PENALTY");
            } else {
                allFlags.add("BULL_DIV_VOL_OVERRIDE");
            }
        }

        // ════════════════════════════════════════════════════════
        //  АГРЕГАЦИЯ КЛАСТЕРОВ
        // ════════════════════════════════════════════════════════

        ClusterScores[] clusters = { cStructure, cMomentum, cVolume, cHTF, cDerivatives };
        String[] clusterNames = { "STR", "MOM", "VOL", "HTF", "DRV" };

        double totalLong  = 0;
        double totalShort = 0;
        int longClusters  = 0;
        int shortClusters = 0;

        for (int i = 0; i < clusters.length; i++) {
            ClusterScores cl = clusters[i];
            totalLong  += cl.longScore;
            totalShort += cl.shortScore;
            if (cl.favorsLong())  longClusters++;
            if (cl.favorsShort()) shortClusters++;
            allFlags.addAll(cl.flags);
        }

        // Crash boost добавляется поверх кластеров
        if (crashBoost > 0) {
            totalShort += crashBoost;
            if (btcCrashScore >= 0.65) {
                totalLong -= crashBoost * 0.60;
                allFlags.add("LONG_CRASH_PENALTY");
            }
        }

        // GIC SHORT boost при краше
        if (aggressiveShort && gicShortBoost > 1.0 && totalShort > 0) {
            totalShort *= gicShortBoost;
            allFlags.add("GIC_BOOST" + String.format("%.0f", gicShortBoost * 100));
        }

        // ════════════════════════════════════════════════════════
        // CONFLUENCE BONUS
        // ════════════════════════════════════════════════════════
        double scoreLong  = totalLong;
        double scoreShort = totalShort;

        if (longClusters >= 3)  scoreLong  += CLUSTER_CONFLUENCE_BONUS * longClusters;
        if (shortClusters >= 3) scoreShort += CLUSTER_CONFLUENCE_BONUS * shortClusters;

        // Confluence flag
        if (longClusters >= 3) allFlags.add("CONFL_L" + longClusters);
        if (shortClusters >= 3) allFlags.add("CONFL_S" + shortClusters);

        // ════════════════════════════════════════════════════════
        // REVERSE EXHAUSTION CHECK
        // ════════════════════════════════════════════════════════
        ReverseWarning rw = detectReversePattern(c15, c1h, state);
        if (rw != null && rw.confidence > 0.48) {
            allFlags.add("⚠REV_" + rw.type);
            if ("LONG_EXHAUSTION".equals(rw.type)) {
                scoreLong *= 0.16;
                if (scoreLong < 0.22) return null;
            } else if ("SHORT_EXHAUSTION".equals(rw.type)) {
                if (aggressiveShort) {
                    allFlags.add("REV_IGNORED_CRASH");
                } else {
                    boolean confirmed = confirmReversalStructure(c1, c5, com.bot.TradingCore.Side.LONG);
                    if (!confirmed) {
                        scoreShort *= 0.16;
                        if (scoreShort < 0.22) return null;
                    } else {
                        allFlags.add("REV_CONFIRMED_1M");
                    }
                }
            }
        }

        // ════════════════════════════════════════════════════════
        // EXHAUSTION FILTERS
        // ════════════════════════════════════════════════════════
        double adxV = adx(c15, 14);
        boolean adxFalling = c15.size() > 5 && adxV > adx(c15.subList(0, c15.size() - 1), 14) * 1.05;

        if (scoreLong > scoreShort) {
            boolean ex = isLongExhausted(c15, c1h, rsi14, rsi7, price);
            if (adxV > 30 && adxFalling) ex = true;
            if (bearDiv && !allFlags.contains("BEAR_DIV_VOL_OVERRIDE")) {
                ex = true; allFlags.add("BEAR_DIV_EXH");
            }
            if (ex) {
                if (rsi14 > 74) return null;
                scoreLong *= 0.13;
                allFlags.add("EXH_LONG");
                if (scoreLong < 0.30) return null;
            }
        }

        if (scoreShort > scoreLong) {
            if (!aggressiveShort) {
                boolean ex = isShortExhausted(c15, c1h, rsi14, rsi7, price);
                if (adxV > 30 && adxFalling) ex = true;
                if (bullDiv) ex = true;
                if ((bias1h == HTFBias.BULL || bias2h == HTFBias.BULL) && !bosDown && scoreShort < 0.60) {
                    ex = true; allFlags.add("SHORT_VS_BULL");
                }
                if (ex) {
                    scoreShort *= 0.32;
                    if (scoreShort < 0.18) return null;
                    allFlags.add("EXH_SHORT");
                }
            } else {
                if (rsi14 < 12) {
                    scoreShort *= 0.65;
                    allFlags.add("EXH_SHORT_MILD");
                }
            }
        }

        // ════════════════════════════════════════════════════════
        // EMA50 OVEREXTENDED
        // ════════════════════════════════════════════════════════
        double ema50  = ema(c15, 50);
        double devEma = (price - ema50) / (ema50 + 1e-9);
        if (scoreLong  > scoreShort && devEma >  0.065) { scoreLong  *= 0.48; allFlags.add("OVEREXT_L"); }
        if (scoreShort > scoreLong  && devEma < -0.065) {
            if (!aggressiveShort) { scoreShort *= 0.48; allFlags.add("OVEREXT_S"); }
            else { allFlags.add("OVEREXT_S_SKIP"); }
        }

        // ════════════════════════════════════════════════════════
        // 2H VETO
        // ════════════════════════════════════════════════════════
        if (bias2h == HTFBias.BULL && scoreShort > scoreLong) {
            if (!aggressiveShort) {
                boolean strongLocalBear =
                        (antiLag != null && antiLag.direction < 0 && antiLag.strength > 0.52) ||
                                (oldPump.detected && oldPump.direction < 0 && oldPump.strength > 0.48) ||
                                bosDown || liqSweep;
                scoreShort *= strongLocalBear ? 0.88 : 0.52;
                allFlags.add(strongLocalBear ? "DYN_SHORT_2H" : "2H_BULL_PRESS");
            } else {
                allFlags.add("2H_BULL_IGNORED_CRASH");
            }
        }

        if (bias2h == HTFBias.BEAR && scoreLong > scoreShort && adxV > 20) {
            double rs = getRelativeStrength(symbol);
            if (rs > 0.75) {
                scoreLong *= 0.60;
                allFlags.add("2H_VETO_WEAK_RS" + String.format("%.0f", rs * 100));
            } else {
                scoreLong *= 0.32;
                allFlags.add("2H_VETO");
            }
            if (scoreLong < 0.18) return null;
        }

        // ════════════════════════════════════════════════════════
        // TREND EXHAUSTION (8-bar)
        // ════════════════════════════════════════════════════════
        double move8 = (last(c15).close - c15.get(c15.size() - 8).close) / price;
        if (move8 >  0.038 && scoreLong  > scoreShort) { scoreLong  *= 0.62; allFlags.add("TREND_EXH_L"); }
        if (move8 < -0.038 && scoreShort > scoreLong) {
            if (!aggressiveShort) {
                scoreShort *= 0.62;
                allFlags.add("TREND_EXH_S");
            } else {
                allFlags.add("TREND_EXH_S_SKIP_CRASH");
            }
        }

        // ════════════════════════════════════════════════════════
        // [v7.0] МИНИМУМ КЛАСТЕРОВ СОГЛАСИЯ
        // Если < 2 кластеров поддерживают направление — нет сигнала
        // (кроме aggressiveShort с crashBoost)
        // ════════════════════════════════════════════════════════
        com.bot.TradingCore.Side candidateSide = scoreLong > scoreShort
                ? com.bot.TradingCore.Side.LONG
                : com.bot.TradingCore.Side.SHORT;

        int supportingClusters = candidateSide == com.bot.TradingCore.Side.LONG
                ? longClusters : shortClusters;

        if (supportingClusters < MIN_AGREEING_CLUSTERS) {
            if (!(aggressiveShort && candidateSide == com.bot.TradingCore.Side.SHORT && crashBoost > 0.30)) {
                return null; // Недостаточно независимых подтверждений
            }
        }

        // ════════════════════════════════════════════════════════
        // MINIMUM SCORE DIFFERENCE
        // ════════════════════════════════════════════════════════
        double scoreDiff = Math.abs(scoreLong - scoreShort);
        double minDiff;
        if (aggressiveShort) {
            minDiff = 0.08;
        } else {
            minDiff = state == MarketState.STRONG_TREND ? 0.16 : 0.20;
        }
        if (scoreDiff < minDiff) return null;

        double dynThresh;
        if (aggressiveShort) {
            dynThresh = 0.40;
        } else {
            dynThresh = state == MarketState.STRONG_TREND ? 0.68 : 0.58;
        }
        if (scoreLong < dynThresh && scoreShort < dynThresh) {
            if (!bullDiv && !bearDiv && !oldPump.detected && !hasFR && !aggressiveShort) return null;
        }

        com.bot.TradingCore.Side side = candidateSide;

        // Cooldown
        long shortCooldownOverride = aggressiveShort ? 60_000L : -1;
        if (!cooldownAllowedEx(symbol, side, cat, now, shortCooldownOverride)) return null;
        if (!flipAllowed(symbol, side)) return null;

        // ════════════════════════════════════════════════════════
        // [v7.0] КАЛИБРОВАННАЯ УВЕРЕННОСТЬ — на кластерах
        // ════════════════════════════════════════════════════════
        double probability = computeClusterConfidence(
                symbol, scoreLong, scoreShort, scoreDiff,
                longClusters, shortClusters,
                state, cat, atr14, price,
                bullDiv, bearDiv, pullUp, pullDown,
                impulseFlag, oldPump.detected, hasFR,
                fvg.detected, ob.detected, bosUp || bosDown, liqSweep,
                bias2h, vwapVal
        );

        // Crash mode confidence boost
        if (aggressiveShort && side == com.bot.TradingCore.Side.SHORT) {
            double crashConfBoost = btcCrashScore * 10.0;
            probability = Math.min(88, probability + crashConfBoost);
            allFlags.add("CRASH_CONF_BOOST");
        }

        double minConf = symbolMinConf.getOrDefault(symbol, globalMinConf);
        if (aggressiveShort && side == com.bot.TradingCore.Side.SHORT) {
            minConf = Math.max(45.0, minConf - 8.0);
        }
        if (probability < minConf) return null;

        if (atr14 / price > 0.0020) allFlags.add("HIGH_ATR");

        // ════════════════════════════════════════════════════════
        // СТОП И ТЕЙК
        // ════════════════════════════════════════════════════════
        double riskMult = cat == CoinCategory.MEME ? 1.40 : cat == CoinCategory.ALT ? 1.10 : 0.88;
        double rrRatio  = scoreDiff > 1.2 ? 3.4 : scoreDiff > 0.9 ? 3.0 : scoreDiff > 0.6 ? 2.7 : 2.3;
        double stopDist = Math.max(atr14 * 1.85 * riskMult, price * 0.0018);

        double stopPrice = side == com.bot.TradingCore.Side.LONG  ? price - stopDist : price + stopDist;
        double takePrice = side == com.bot.TradingCore.Side.LONG  ? price + stopDist * rrRatio
                : price - stopDist * rrRatio;

        if (!priceMovedEnough(symbol, price)) return null;
        registerSignal(symbol, side, now);

        return new TradeIdea(symbol, side, price, stopPrice, takePrice, rrRatio,
                probability, allFlags,
                fundingRate, fundingDelta, oiChange, bias2h.name(), cat);
    }

    // ══════════════════════════════════════════════════════════════
    //  [v7.0] CLUSTER-BASED CONFIDENCE
    // ══════════════════════════════════════════════════════════════

    private double computeClusterConfidence(
            String symbol,
            double scoreLong, double scoreShort, double scoreDiff,
            int longClusters, int shortClusters,
            MarketState state, CoinCategory cat,
            double atr, double price,
            boolean bullDiv, boolean bearDiv,
            boolean pullUp, boolean pullDown,
            boolean impulse, boolean pump,
            boolean hasFR, boolean hasFVG, boolean hasOB, boolean hasBOS, boolean liqSweep,
            HTFBias bias2h, double vwap) {

        boolean isLong = scoreLong > scoreShort;
        int clusters = isLong ? longClusters : shortClusters;

        // Базовая нормализация score difference
        double norm = Math.min(1.0, scoreDiff / 5.0); // было 6.5, теперь 5.0 (кластеры дают меньше)

        // [v7.0] Бонус за количество согласных КЛАСТЕРОВ (а не факторов)
        // Это ключевое отличие — 5 факторов из 1 кластера ≠ 5 независимых сигналов
        double clusterBonus = switch (clusters) {
            case 5 -> 0.12;
            case 4 -> 0.08;
            case 3 -> 0.04;
            default -> 0.0;
        };
        norm += clusterBonus;

        // Бонус за HTF alignment
        if ((bias2h == HTFBias.BULL && isLong) || (bias2h == HTFBias.BEAR && !isLong)) {
            norm += 0.05;
        }

        // Бонус за VWAP alignment
        if ((isLong && price > vwap * 1.0005) || (!isLong && price < vwap * 0.9995)) {
            norm += 0.025;
        }

        norm = Math.min(1.0, norm);

        // Range confidence: 50 + norm * range
        double range = 26 + Math.min(clusters * 4.0, 16); // max 26+16=42, итого max 92, clamp 88
        double prob  = 50 + norm * range;

        // Market state adjustment
        if (state == MarketState.STRONG_TREND)      prob += 3.0;
        else if (state == MarketState.WEAK_TREND)   prob += 0.5;
        else if (state == MarketState.RANGE)        prob -= 3.0;

        // Category adjustment
        if (cat == CoinCategory.MEME)               prob -= 5.0;
        else if (cat == CoinCategory.ALT)           prob -= 2.0;

        // Historical calibration
        Deque<CalibRecord> hist = calibHist.get(symbol);
        if (hist != null && hist.size() >= 30) {
            double histAcc = historicalAccuracy(hist, prob);
            prob = prob * 0.70 + histAcc * 0.30;
        }

        return Math.round(clamp(prob, 50, 88)); // Потолок 88, не 90
    }

    private double historicalAccuracy(Deque<CalibRecord> hist, double prob) {
        double sum = 0, cnt = 0;
        List<CalibRecord> list = new ArrayList<>(hist);
        int size = list.size();
        for (int i = 0; i < size; i++) {
            CalibRecord r = list.get(i);
            if (Math.abs(r.predicted - prob) < 12) {
                double weight = 0.5 + 0.5 * ((double) i / size);
                cnt += weight;
                if (r.correct) sum += weight;
            }
        }
        return cnt < 4 ? prob : (sum / cnt) * 100;
    }

    // ══════════════════════════════════════════════════════════════
    //  COOLDOWN
    // ══════════════════════════════════════════════════════════════

    private boolean cooldownAllowedEx(String sym, com.bot.TradingCore.Side side,
                                      CoinCategory cat, long now, long shortOverrideMs) {
        String key  = sym + "_" + side;
        long   base;
        if (side == com.bot.TradingCore.Side.SHORT && shortOverrideMs > 0) {
            base = shortOverrideMs;
        } else {
            base = cat == CoinCategory.TOP  ? COOLDOWN_TOP :
                    cat == CoinCategory.ALT  ? COOLDOWN_ALT : COOLDOWN_MEME;
        }
        Long last = cooldownMap.get(key);
        if (last != null && now - last < base) return false;
        cooldownMap.put(key, now);
        return true;
    }

    private boolean cooldownAllowed(String sym, com.bot.TradingCore.Side side, CoinCategory cat, long now) {
        return cooldownAllowedEx(sym, side, cat, now, -1);
    }

    private boolean flipAllowed(String sym, com.bot.TradingCore.Side newSide) {
        Deque<String> h = recentDirs.computeIfAbsent(sym, k -> new ArrayDeque<>());
        if (h.size() < 2) return true;
        Iterator<String> it = h.descendingIterator();
        String last = it.next(), prev = it.next();
        return !(!last.equals(newSide.name()) && prev.equals(newSide.name()));
    }

    private void registerSignal(String sym, com.bot.TradingCore.Side side, long now) {
        cooldownMap.put(sym + "_" + side, now);
        Deque<String> h = recentDirs.computeIfAbsent(sym, k -> new ArrayDeque<>());
        h.addLast(side.name());
        if (h.size() > 3) h.removeFirst();
        signalCountBySymbol.computeIfAbsent(sym, k -> new AtomicInteger(0)).incrementAndGet();
    }

    private boolean priceMovedEnough(String sym, double price) {
        Double last = lastSigPrice.get(sym);
        if (last == null) { lastSigPrice.put(sym, price); return true; }
        if (Math.abs(price - last) / last < 0.0020) return false;
        lastSigPrice.put(sym, price);
        return true;
    }

    // ══════════════════════════════════════════════════════════════
    //  REVERSAL STRUCTURE CONFIRMATION
    // ══════════════════════════════════════════════════════════════

    private boolean confirmReversalStructure(List<com.bot.TradingCore.Candle> c1,
                                             List<com.bot.TradingCore.Candle> c5,
                                             com.bot.TradingCore.Side side) {
        boolean longSide = side == com.bot.TradingCore.Side.LONG;
        if (c1 != null && c1.size() >= 10) {
            int n = c1.size();
            double localHigh = Double.NEGATIVE_INFINITY;
            double localLow  = Double.POSITIVE_INFINITY;
            for (int i = n - 8; i < n - 1; i++) {
                localHigh = Math.max(localHigh, c1.get(i).high);
                localLow  = Math.min(localLow,  c1.get(i).low);
            }
            com.bot.TradingCore.Candle last1m = last(c1);
            if (longSide  && last1m.close > localHigh * 1.0003) return true;
            if (!longSide && last1m.close < localLow  * 0.9997) return true;
        }
        if (c1 != null && c1.size() >= 5) {
            int n = c1.size();
            int bullCount = 0, bearCount = 0;
            for (int i = n - 4; i < n; i++) {
                com.bot.TradingCore.Candle c = c1.get(i);
                if (c.close > c.open) bullCount++; else bearCount++;
            }
            if (longSide  && bullCount >= 3) return true;
            if (!longSide && bearCount >= 3) return true;
        }
        if (c5 != null && c5.size() >= 3) {
            com.bot.TradingCore.Candle last5m = last(c5);
            double body5  = Math.abs(last5m.close - last5m.open);
            double range5 = last5m.high - last5m.low + 1e-10;
            if (longSide  && last5m.close > last5m.open && body5 / range5 > 0.60) return true;
            if (!longSide && last5m.close < last5m.open && body5 / range5 > 0.60) return true;
        }
        return false;
    }

    // ══════════════════════════════════════════════════════════════
    //  ANTI-LAG DETECTION
    // ══════════════════════════════════════════════════════════════

    private static class AntiLagResult {
        final int direction; final double strength;
        AntiLagResult(int d, double s) { direction = d; strength = s; }
    }

    private AntiLagResult detectAntiLag(List<com.bot.TradingCore.Candle> c1,
                                        List<com.bot.TradingCore.Candle> c5,
                                        List<com.bot.TradingCore.Candle> c15) {
        if (c1 == null || c1.size() < 5 || c5 == null || c5.size() < 3) return null;
        int n1 = c1.size();
        double atr1 = atr(c1, Math.min(14, n1 - 1));

        double range1 = last(c1).high - last(c1).low;
        double body1  = Math.abs(last(c1).close - last(c1).open);
        if (range1 > atr1 * 1.85 && body1 / range1 > 0.70) {
            int d = last(c1).close > last(c1).open ? 1 : -1;
            double s = Math.min(0.78, range1 / atr1 * 0.28);
            if (s > 0.38) return new AntiLagResult(d, s);
        }

        int n5 = c5.size();
        double atr5 = atr(c5, Math.min(14, n5 - 1));
        com.bot.TradingCore.Candle lc5 = last(c5);
        double avgV5 = c5.subList(Math.max(0, n5 - 8), n5 - 1)
                .stream().mapToDouble(c -> c.volume).average().orElse(lc5.volume);
        if (lc5.volume > avgV5 * 1.7) {
            double mv5 = Math.abs(lc5.close - lc5.open);
            if (mv5 > atr5 * 0.60) {
                int d = lc5.close > lc5.open ? 1 : -1;
                double s = Math.min(0.75, mv5 / atr5 * 0.38);
                if (s > 0.36) return new AntiLagResult(d, s);
            }
        }

        int grn = 0, red = 0; double serMove = 0;
        for (int i = Math.max(0, n1 - 4); i < n1; i++) {
            com.bot.TradingCore.Candle c = c1.get(i);
            if (c.close > c.open) grn++; else red++;
            serMove += c.close - c.open;
        }
        if ((grn >= 3 || red >= 3) && Math.abs(serMove) > atr1 * 1.45) {
            int d = serMove > 0 ? 1 : -1;
            double s = Math.min(0.72, Math.abs(serMove) / atr1 * 0.37);
            if (s > 0.34) return new AntiLagResult(d, s);
        }
        return null;
    }

    // ══════════════════════════════════════════════════════════════
    //  REVERSE EXHAUSTION DETECTION
    // ══════════════════════════════════════════════════════════════

    private static class ReverseWarning {
        final String type; final double confidence;
        ReverseWarning(String t, double c) { type = t; confidence = c; }
    }

    private ReverseWarning detectReversePattern(List<com.bot.TradingCore.Candle> c15,
                                                List<com.bot.TradingCore.Candle> c1h,
                                                MarketState state) {
        if (c15.size() < 8 || c1h.size() < 5) return null;
        double score = 0;
        boolean longExh = false, shortExh = false;

        double rsi1h = rsi(c1h, 14);
        if (rsi1h > 75.0) { score += 0.27; longExh  = true; }
        if (rsi1h < 25.0) { score += 0.27; shortExh = true; }

        double mom  = momentumPct(c15, 5, 0);
        double mom1 = momentumPct(c15, 5, 5);
        double mom2 = momentumPct(c15, 5, 10);
        if (mom < mom1 * 0.63 && mom1 < mom2 * 0.68 && mom1 > 0) { score += 0.48; longExh  = true; }
        if (mom > mom1 * 1.37 && mom1 > mom2 * 1.32 && mom1 < 0) { score += 0.48; shortExh = true; }

        double avgVol = c15.subList(Math.max(0, c15.size() - 15), c15.size() - 3)
                .stream().mapToDouble(c -> c.volume).average().orElse(1);
        if (last(c15).volume < avgVol * 0.52) score += 0.32;

        com.bot.TradingCore.Candle lc = last(c15);
        double uw = lc.high - Math.max(lc.open, lc.close);
        double lw = Math.min(lc.open, lc.close) - lc.low;
        double bd = Math.abs(lc.close - lc.open) + 1e-10;
        if (uw > bd * 2.4 && lc.close < lc.open) { score += 0.38; longExh  = true; }
        if (lw > bd * 2.4 && lc.close > lc.open) { score += 0.38; shortExh = true; }

        if (c15.size() >= 2) {
            double adxC = adx(c15, 14);
            double adxP = adx(c15.subList(0, c15.size() - 1), 14);
            if (adxP > adxC && adxC > 20) score += 0.28;
        }

        if (c15.size() >= 3) {
            com.bot.TradingCore.Candle ca = c15.get(c15.size() - 3);
            com.bot.TradingCore.Candle cb = c15.get(c15.size() - 2);
            com.bot.TradingCore.Candle cc = c15.get(c15.size() - 1);
            double ba = Math.abs(ca.close - ca.open);
            double bb = Math.abs(cb.close - cb.open);
            double bc = Math.abs(cc.close - cc.open);
            if (ca.close > ca.open && cb.close > cb.open && cc.close > cc.open
                    && ba > bb * 1.08 && bb > bc * 1.08) { score += 0.32; longExh = true; }
            if (ca.close < ca.open && cb.close < cb.open && cc.close < cc.open
                    && ba > bb * 1.08 && bb > bc * 1.08) { score += 0.32; shortExh = true; }
        }

        if (c15.size() >= 2) {
            com.bot.TradingCore.Candle prevC = c15.get(c15.size() - 2);
            if (prevC.close > prevC.open) {
                double prevBd = prevC.close - prevC.open;
                if (prevBd > bd * 2.0 && uw > bd * 1.7) { score += 0.38; longExh = true; }
            }
        }

        if (score < 0.48) return null;

        String type;
        if (longExh && !shortExh)      type = "LONG_EXHAUSTION";
        else if (shortExh && !longExh) type = "SHORT_EXHAUSTION";
        else                           type = "REVERSAL";

        return new ReverseWarning(type, score);
    }

    private double momentumPct(List<com.bot.TradingCore.Candle> c, int bars, int offset) {
        if (c.size() < bars + offset + 1) return 0;
        int n = c.size();
        double base = c.get(n - offset - bars - 1).close;
        return (c.get(n - offset - 1).close - base) / (base + 1e-9);
    }

    // ══════════════════════════════════════════════════════════════
    //  MARKET STRUCTURE
    // ══════════════════════════════════════════════════════════════

    public static int marketStructure(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 20) return 0;
        List<Integer> highs = swingHighs(c, 5);
        List<Integer> lows  = swingLows(c, 5);
        if (highs.size() < 2 || lows.size() < 2) return 0;

        double lastHigh = c.get(highs.get(highs.size() - 1)).high;
        double prevHigh = c.get(highs.get(highs.size() - 2)).high;
        double lastLow  = c.get(lows.get(lows.size() - 1)).low;
        double prevLow  = c.get(lows.get(lows.size() - 2)).low;

        if (lastHigh > prevHigh && lastLow > prevLow)  return  1;
        if (lastHigh < prevHigh && lastLow < prevLow)  return -1;
        return 0;
    }

    public static List<Integer> swingHighs(List<com.bot.TradingCore.Candle> c, int lr) {
        List<Integer> res = new ArrayList<>();
        for (int i = lr; i < c.size() - lr; i++) {
            double v = c.get(i).high; boolean ok = true;
            for (int l = i - lr; l <= i + lr && ok; l++)
                if (c.get(l).high > v) ok = false;
            if (ok) res.add(i);
        }
        return res;
    }

    public static List<Integer> swingLows(List<com.bot.TradingCore.Candle> c, int lr) {
        List<Integer> res = new ArrayList<>();
        for (int i = lr; i < c.size() - lr; i++) {
            double v = c.get(i).low; boolean ok = true;
            for (int l = i - lr; l <= i + lr && ok; l++)
                if (c.get(l).low < v) ok = false;
            if (ok) res.add(i);
        }
        return res;
    }

    // ══════════════════════════════════════════════════════════════
    //  SMC: FVG + ORDER BLOCK
    // ══════════════════════════════════════════════════════════════

    private static final class FVGResult {
        final boolean detected, isBullish;
        final double gapLow, gapHigh;
        FVGResult(boolean d, boolean b, double lo, double hi) {
            detected = d; isBullish = b; gapLow = lo; gapHigh = hi;
        }
    }

    private FVGResult detectFVG(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 10) return new FVGResult(false, false, 0, 0);
        for (int i = c.size() - 3; i >= c.size() - 9 && i >= 2; i--) {
            com.bot.TradingCore.Candle c1 = c.get(i - 1), c2 = c.get(i), c3 = c.get(i + 1);
            double bs   = Math.abs(c2.close - c2.open);
            double atrL = atr(c.subList(Math.max(0, i - 14), i + 1), Math.min(14, i));
            if (atrL <= 0) continue;
            if (c2.close > c2.open && bs > atrL * 1.45) {
                double lo = c1.high, hi = c3.low;
                if (hi > lo) return new FVGResult(true, true, lo, hi);
            }
            if (c2.close < c2.open && bs > atrL * 1.45) {
                double hi = c1.low, lo = c3.high;
                if (hi > lo) return new FVGResult(true, false, lo, hi);
            }
        }
        return new FVGResult(false, false, 0, 0);
    }

    private static final class OrderBlockResult {
        final boolean detected, isBullish;
        final double zone;
        OrderBlockResult(boolean d, boolean b, double z) { detected = d; isBullish = b; zone = z; }
    }

    private OrderBlockResult detectOrderBlock(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 15) return new OrderBlockResult(false, false, 0);
        double atrL = atr(c, 14);
        for (int i = c.size() - 5; i >= c.size() - 13 && i >= 3; i--) {
            com.bot.TradingCore.Candle pot = c.get(i);
            double move = 0;
            for (int j = i + 1; j < Math.min(i + 5, c.size()); j++)
                move += c.get(j).close - c.get(j).open;
            if (pot.close < pot.open && move > atrL * 2.0)
                return new OrderBlockResult(true, true, pot.low);
            if (pot.close > pot.open && move < -atrL * 2.0)
                return new OrderBlockResult(true, false, pot.high);
        }
        return new OrderBlockResult(false, false, 0);
    }

    // ══════════════════════════════════════════════════════════════
    //  BOS + LIQUIDITY SWEEP
    // ══════════════════════════════════════════════════════════════

    private boolean detectBOSUp(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 8) return false;
        int sz = c.size();
        double localHigh = Double.NEGATIVE_INFINITY;
        for (int i = sz - 7; i < sz - 1; i++) localHigh = Math.max(localHigh, c.get(i).high);
        return last(c).close > localHigh * 1.0004;
    }

    private boolean detectBOSDown(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 8) return false;
        int sz = c.size();
        double localLow = Double.POSITIVE_INFINITY;
        for (int i = sz - 7; i < sz - 1; i++) localLow = Math.min(localLow, c.get(i).low);
        return last(c).close < localLow * 0.9996;
    }

    public static boolean detectLiquiditySweep(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 6) return false;
        com.bot.TradingCore.Candle la = c.get(c.size() - 1);
        com.bot.TradingCore.Candle pr = c.get(c.size() - 2);
        double uw = la.high - Math.max(la.open, la.close);
        double lw = Math.min(la.open, la.close) - la.low;
        double bd = Math.abs(la.close - la.open) + 1e-10;
        return (uw > bd * 1.75 && la.close < pr.close) ||
                (lw > bd * 1.75 && la.close > pr.close);
    }

    // ══════════════════════════════════════════════════════════════
    //  OLD PUMP DETECTOR
    // ══════════════════════════════════════════════════════════════

    private static final class OldPumpResult {
        final boolean detected; final int direction; final double strength;
        OldPumpResult(boolean d, int dir, double s) { detected = d; direction = dir; strength = s; }
    }

    private OldPumpResult detectOldPump(List<com.bot.TradingCore.Candle> c1,
                                        List<com.bot.TradingCore.Candle> c5,
                                        CoinCategory cat) {
        if (c1 == null || c1.size() < 10 || c5 == null || c5.size() < 6)
            return new OldPumpResult(false, 0, 0);
        double atr1 = atr(c1, Math.min(14, c1.size() - 1));
        com.bot.TradingCore.Candle l1 = last(c1);
        double cSize  = Math.abs(l1.close - l1.open);
        double fRng   = l1.high - l1.low;
        double bRatio = cSize / (fRng + 1e-12);
        boolean bigC  = fRng > atr1 * 2.8;
        boolean strB  = bRatio > 0.65;
        double avgVol = c1.subList(Math.max(0, c1.size() - 8), c1.size() - 1)
                .stream().mapToDouble(c -> c.volume).average().orElse(l1.volume);
        boolean volSp = l1.volume > avgVol * 1.75;
        int lookback  = Math.min(4, c1.size() - 1);
        double move   = l1.close - c1.get(c1.size() - 1 - lookback).close;
        double movePct = Math.abs(move) / (c1.get(c1.size() - 1 - lookback).close + 1e-9);
        double thr = cat == CoinCategory.MEME ? 0.017 : cat == CoinCategory.ALT ? 0.021 : 0.024;
        if (bigC && strB && volSp && movePct > thr) {
            int dir = move > 0 ? 1 : -1;
            double str = 0.72 + Math.min(movePct * 10, 0.52);
            return new OldPumpResult(true, dir, str);
        }
        int gC = 0, rC = 0; double totMove = 0;
        for (int i = c1.size() - 1 - lookback; i < c1.size(); i++) {
            com.bot.TradingCore.Candle c = c1.get(i);
            if (c.close > c.open) gC++; else rC++;
            totMove += c.close - c.open;
        }
        double sPct = Math.abs(totMove) / (c1.get(c1.size() - 1 - lookback).close + 1e-9);
        if ((gC >= 3 || rC >= 3) && sPct > thr * 1.15 && volSp)
            return new OldPumpResult(true, totMove > 0 ? 1 : -1, 0.58);
        return new OldPumpResult(false, 0, 0);
    }

    // ══════════════════════════════════════════════════════════════
    //  COMPRESSION BREAKOUT
    // ══════════════════════════════════════════════════════════════

    private static final class CompressionResult {
        final boolean breakout; final int direction;
        CompressionResult(boolean b, int d) { breakout = b; direction = d; }
    }

    private CompressionResult detectCompression(List<com.bot.TradingCore.Candle> c15,
                                                List<com.bot.TradingCore.Candle> c1) {
        if (c15.size() < 30 || c1 == null || c1.size() < 10)
            return new CompressionResult(false, 0);
        double atrRecent = atr(c15.subList(c15.size() - 8, c15.size()), 7);
        double atrPast   = atr(c15.subList(c15.size() - 26, c15.size() - 10), 14);
        if (atrRecent >= atrPast * 0.52) return new CompressionResult(false, 0);
        double atr1 = atr(c1, Math.min(14, c1.size() - 1));
        int lk = Math.min(4, c1.size() - 1);
        double bk = last(c1).close - c1.get(c1.size() - 1 - lk).close;
        if (Math.abs(bk) > atr1 * 1.75)
            return new CompressionResult(true, bk > 0 ? 1 : -1);
        return new CompressionResult(false, 0);
    }

    // ══════════════════════════════════════════════════════════════
    //  EXHAUSTION CHECKS
    // ══════════════════════════════════════════════════════════════

    private boolean isLongExhausted(List<com.bot.TradingCore.Candle> c15,
                                    List<com.bot.TradingCore.Candle> c1h,
                                    double rsi14, double rsi7, double price) {
        if (rsi14 > 76 && rsi7 > 79) return true;
        double ema21 = ema(c15, 21);
        if ((price - ema21) / ema21 > 0.026 && rsi14 > 68) return true;
        if (c1h.size() >= 8 && rsi(c1h, 14) > 78) return true;
        if (c15.size() >= 6) {
            double b1 = Math.abs(c15.get(c15.size()-1).close - c15.get(c15.size()-1).open);
            double b2 = Math.abs(c15.get(c15.size()-2).close - c15.get(c15.size()-2).open);
            double b3 = Math.abs(c15.get(c15.size()-3).close - c15.get(c15.size()-3).open);
            if (b1 < b2 * 0.58 && b2 < b3 * 0.80) return true;
            double v1 = c15.get(c15.size()-1).volume;
            double v2 = c15.get(c15.size()-2).volume;
            double v3 = c15.get(c15.size()-3).volume;
            if (price > ema21 && v1 < v2 * 0.78 && v2 < v3 * 0.88) return true;
        }
        com.bot.TradingCore.Candle lc = last(c15);
        double uw = lc.high - Math.max(lc.open, lc.close);
        double bd = Math.abs(lc.close - lc.open) + 1e-10;
        if (uw > bd * 1.6 && lc.close < lc.open) return true;
        return false;
    }

    private boolean isShortExhausted(List<com.bot.TradingCore.Candle> c15,
                                     List<com.bot.TradingCore.Candle> c1h,
                                     double rsi14, double rsi7, double price) {
        if (rsi14 < 24 && rsi7 < 21) return true;
        double ema21 = ema(c15, 21);
        if ((ema21 - price) / ema21 > 0.026 && rsi14 < 32) return true;
        if (c1h.size() >= 8 && rsi(c1h, 14) < 22) return true;
        if (c15.size() >= 6) {
            double b1 = Math.abs(c15.get(c15.size()-1).close - c15.get(c15.size()-1).open);
            double b2 = Math.abs(c15.get(c15.size()-2).close - c15.get(c15.size()-2).open);
            double b3 = Math.abs(c15.get(c15.size()-3).close - c15.get(c15.size()-3).open);
            if (b1 < b2 * 0.58 && b2 < b3 * 0.80) return true;
            double v1 = c15.get(c15.size()-1).volume;
            double v2 = c15.get(c15.size()-2).volume;
            double v3 = c15.get(c15.size()-3).volume;
            if (price < ema21 && v1 < v2 * 0.78 && v2 < v3 * 0.88) return true;
        }
        com.bot.TradingCore.Candle lc = last(c15);
        double lw = Math.min(lc.open, lc.close) - lc.low;
        double bd = Math.abs(lc.close - lc.open) + 1e-10;
        if (lw > bd * 1.6 && lc.close > lc.open) return true;
        return false;
    }

    // ══════════════════════════════════════════════════════════════
    //  MARKET STATE + HTF BIAS
    // ══════════════════════════════════════════════════════════════

    private MarketState detectState(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 55) return MarketState.WEAK_TREND;
        double ema20 = ema(c, 20);
        double ema50 = ema(c, 50);
        int    n     = c.size();
        double slope = (ema20 - ema(c.subList(0, Math.max(1, n - 10)), 20)) / (c.get(0).close + 1e-9);
        double vol   = atr(c, 14) / (c.get(n - 1).close + 1e-9);
        if (Math.abs(slope) < 0.0005 || vol < 0.0015) return MarketState.RANGE;
        if ((ema20 > ema50 && slope > 0) || (ema20 < ema50 && slope < 0))
            return MarketState.STRONG_TREND;
        return MarketState.WEAK_TREND;
    }

    private HTFBias detectBias1H(List<com.bot.TradingCore.Candle> c) {
        if (!valid(c)) return HTFBias.NONE;
        double e50  = ema(c, 50);
        double e200 = ema(c, 200);
        if (e50 > e200 * 1.002) return HTFBias.BULL;
        if (e50 < e200 * 0.998) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    private HTFBias detectBias2H(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 30) return HTFBias.NONE;
        double ema12 = ema(c, 12);
        double ema26 = ema(c, 26);
        double ema50 = c.size() >= 50 ? ema(c, 50) : ema26;
        double price = last(c).close;
        boolean bullEMA = ema12 > ema26 && ema26 > ema50 * 0.998;
        boolean bearEMA = ema12 < ema26 && ema26 < ema50 * 1.002;
        boolean pAbove  = price > ema12 && price > ema26;
        boolean pBelow  = price < ema12 && price < ema26;
        boolean hh = checkHH_HL(c);
        boolean ll = checkLL_LH(c);
        if (bullEMA && pAbove && hh)  return HTFBias.BULL;
        if (bearEMA && pBelow && ll)  return HTFBias.BEAR;
        if (bullEMA && pAbove)        return HTFBias.BULL;
        if (bearEMA && pBelow)        return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    private boolean checkHH_HL(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 15) return false;
        int n = c.size();
        double h1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.high).max().orElse(0);
        double h2 = c.subList(n-8, n).stream().mapToDouble(x->x.high).max().orElse(0);
        double l1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.low).min().orElse(0);
        double l2 = c.subList(n-8, n).stream().mapToDouble(x->x.low).min().orElse(0);
        return h2 > h1 && l2 > l1;
    }

    private boolean checkLL_LH(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 15) return false;
        int n = c.size();
        double h1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.high).max().orElse(0);
        double h2 = c.subList(n-8, n).stream().mapToDouble(x->x.high).max().orElse(0);
        double l1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.low).min().orElse(0);
        double l2 = c.subList(n-8, n).stream().mapToDouble(x->x.low).min().orElse(0);
        return h2 < h1 && l2 < l1;
    }

    private void adaptGlobalMinConf(MarketState state, double atr, double price) {
        double vol  = atr / (price + 1e-9);
        double base = BASE_CONF;
        if (state == MarketState.STRONG_TREND) base -= 2.0;
        else if (state == MarketState.RANGE)   base += 2.5;
        if (vol > 0.025)      base += 3.5;
        else if (vol > 0.018) base += 2.0;
        else if (vol < 0.005) base -= 1.0;
        int utcHour = java.time.ZonedDateTime.now(java.time.ZoneId.of("UTC")).getHour();
        if (utcHour >= 8 && utcHour <= 12)       base -= 1.0;
        else if (utcHour >= 13 && utcHour <= 21)  base -= 1.5;
        globalMinConf = clamp(base, MIN_CONF_FLOOR, MIN_CONF_CEIL);
    }

    // ══════════════════════════════════════════════════════════════
    //  MATH PRIMITIVES
    // ══════════════════════════════════════════════════════════════

    public double atr(List<com.bot.TradingCore.Candle> c, int n) {
        int p = Math.min(n, c.size() - 1);
        if (p <= 0) return 0;
        double sum = 0;
        for (int i = c.size() - p; i < c.size(); i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            sum += Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low  - prev.close)));
        }
        return sum / p;
    }

    private double adx(List<com.bot.TradingCore.Candle> c, int n) {
        if (c.size() < n + 1) return 15;
        double trS = 0, plusDM = 0, minusDM = 0;
        for (int i = c.size() - n; i < c.size(); i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            double hd = cur.high - prev.high, ld = prev.low - cur.low;
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low  - prev.close)));
            trS += tr;
            if (hd > ld && hd > 0) plusDM  += hd;
            if (ld > hd && ld > 0) minusDM += ld;
        }
        double av = trS / n + 1e-9;
        double pDI = 100 * (plusDM  / n) / av;
        double mDI = 100 * (minusDM / n) / av;
        return 100 * Math.abs(pDI - mDI) / Math.max(pDI + mDI, 1);
    }

    private double ema(List<com.bot.TradingCore.Candle> c, int p) {
        if (c.size() < p) return last(c).close;
        double k = 2.0 / (p + 1), e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    public double rsi(List<com.bot.TradingCore.Candle> c, int period) {
        if (c.size() < period + 1) return 50.0;
        double gain = 0, loss = 0;
        for (int i = c.size() - period; i < c.size(); i++) {
            double ch = c.get(i).close - c.get(i - 1).close;
            if (ch > 0) gain += ch; else loss -= ch;
        }
        double rs = loss == 0 ? 100 : gain / loss;
        return 100 - (100 / (1 + rs));
    }

    private boolean bullDiv(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 25) return false;
        int i1 = c.size() - 8, i2 = c.size() - 1;
        return c.get(i2).low < c.get(i1).low * 0.998 &&
                rsi(c, 14) > rsi(c.subList(0, i1 + 1), 14) + 3;
    }

    private boolean bearDiv(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 25) return false;
        int i1 = c.size() - 8, i2 = c.size() - 1;
        return c.get(i2).high > c.get(i1).high * 1.002 &&
                rsi(c, 14) < rsi(c.subList(0, i1 + 1), 14) - 3;
    }

    public boolean impulse(List<com.bot.TradingCore.Candle> c) {
        if (c == null || c.size() < 15) return false;
        return Math.abs(last(c).close - c.get(c.size() - 5).close) > atr(c, 14) * 0.55;
    }

    public boolean volumeSpike(List<com.bot.TradingCore.Candle> c, CoinCategory cat) {
        if (c.size() < 10) return false;
        double avg = c.subList(c.size() - 10, c.size() - 1)
                .stream().mapToDouble(cd -> cd.volume).average().orElse(1);
        double thr = cat == CoinCategory.MEME ? 1.25 : cat == CoinCategory.ALT ? 1.20 : 1.15;
        return last(c).volume / avg > thr;
    }

    private boolean pullback(List<com.bot.TradingCore.Candle> c, boolean bull) {
        double e21 = ema(c, 21), p = last(c).close, r = rsi(c, 14);
        return bull
                ? p <= e21 * 1.0012 && p >= e21 * 0.993 && r > 37 && r < 58
                : p >= e21 * 0.9988 && p <= e21 * 1.007 && r < 63 && r > 42;
    }

    private boolean bullishStructure(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 12) return false;
        return c.get(c.size()-4).high > c.get(c.size()-8).high &&
                c.get(c.size()-4).low  > c.get(c.size()-8).low;
    }

    private boolean bearishStructure(List<com.bot.TradingCore.Candle> c) {
        if (c.size() < 12) return false;
        return c.get(c.size()-4).high < c.get(c.size()-8).high &&
                c.get(c.size()-4).low  < c.get(c.size()-8).low;
    }

    private double vwap(List<com.bot.TradingCore.Candle> c) {
        double pv = 0, vol = 0;
        for (com.bot.TradingCore.Candle x : c) {
            double tp = (x.high + x.low + x.close) / 3.0;
            pv += tp * x.volume; vol += x.volume;
        }
        return vol == 0 ? last(c).close : pv / vol;
    }

    // ── Utility ─────────────────────────────────────────────────
    private com.bot.TradingCore.Candle last(List<com.bot.TradingCore.Candle> c) { return c.get(c.size() - 1); }
    private boolean valid(List<?> c)  { return c != null && c.size() >= MIN_BARS; }
    private double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
}