package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class DecisionEngineMerged {

    // ── Enums ──────────────────────────────────────────────────────
    public enum CoinCategory { TOP, ALT, MEME }
    public enum MarketState  { STRONG_TREND, WEAK_TREND, RANGE }
    public enum HTFBias      { BULL, BEAR, NONE }

    // ── Константы ─────────────────────────────────────────────────
    private static final int    MIN_BARS        = 150;
    // [v11.0] Increased cooldowns to prevent whipsaw
    private static final long   COOLDOWN_TOP    = 8  * 60_000L;  // was 4min — too short, caused flip-flops
    private static final long   COOLDOWN_ALT    = 6  * 60_000L;  // was 3min
    private static final long   COOLDOWN_MEME   = 4  * 60_000L;  // was 2min
    private static final double BASE_CONF       = 51.0;  // [v11.0] raised from 52 — fewer but better signals
    private static final int    CALIBRATION_WIN = 120;
    private static final double MIN_CONF_FLOOR  = 48.0;  // [v11.0] raised from 47
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

    // [ДЫРА №1] CVD — накопленная дельта объёма, устанавливается из SignalSender
    private final Map<String, Double>           cvdMap           = new ConcurrentHashMap<>();

    // [v7.0] GIC reference
    private volatile com.bot.GlobalImpulseController gicRef = null;
    private com.bot.PumpHunter pumpHunter;
    // [v14.0] ForecastEngine integration
    private volatile com.bot.TradingCore.ForecastEngine forecastEngine = null;

    public DecisionEngineMerged() {}

    // [v23.0] Bayesian prior — updated from ISC real win rate
    private volatile double bayesPrior = 0.50;

    /** Update Bayesian prior from ISC historical win rate */
    public void updateBayesPrior(double winRate) {
        this.bayesPrior = Math.max(0.35, Math.min(0.70, winRate));
    }

    public double getBayesPrior() { return bayesPrior; }

    // ── Setters ───────────────────────────────────────────────────
    public void setPumpHunter(com.bot.PumpHunter ph) { this.pumpHunter = ph; }
    public void setGIC(com.bot.GlobalImpulseController gic) { this.gicRef = gic; }
    public void setForecastEngine(com.bot.TradingCore.ForecastEngine fe) { this.forecastEngine = fe; }

    // [v23.0] Called by SignalSender AFTER ISC.allowSignal() confirms.
    // Now accepts price to properly track lastSigPrice.
    public void confirmSignal(String symbol, com.bot.TradingCore.Side side, double price, long now) {
        registerSignal(symbol, side, now);
        lastSigPrice.put(symbol, price);
    }

    // Backward-compatible overload
    public void confirmSignal(String symbol, com.bot.TradingCore.Side side, long now) {
        registerSignal(symbol, side, now);
    }

    public void setVolumeDelta(String sym, double delta) {
        volumeDeltaMap.put(sym, delta);
        Deque<Double> hist = vdHistory.computeIfAbsent(sym, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        hist.addLast(Math.abs(delta));
        if (hist.size() > 50) hist.removeFirst();
    }

    /** [ДЫРА №1] CVD — устанавливается из SignalSender после вычисления накопленной дельты */
    public void setCVD(String sym, double cvdNormalized) {
        cvdMap.put(sym, cvdNormalized);
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
        Deque<Double> h = relStrengthHistory.computeIfAbsent(symbol, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
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

        // [v11.0] Per-cluster cap reduced: prevents single market event
        // from inflating one cluster's score via correlated sub-signals.
        // 0.75 (was 0.85) — more conservative, fewer false-strong signals
        private static final double CLUSTER_CAP = 0.75;

        void addLong(double score, String flag) {
            longScore = Math.min(CLUSTER_CAP, Math.max(longScore, score));
            if (flag != null) flags.add(flag);
        }

        void addShort(double score, String flag) {
            shortScore = Math.min(CLUSTER_CAP, Math.max(shortScore, score));
            if (flag != null) flags.add(flag);
        }

        void boostLong(double score, String flag) {
            longScore = Math.min(CLUSTER_CAP, longScore + score);
            if (flag != null) flags.add(flag);
        }

        void boostShort(double score, String flag) {
            shortScore = Math.min(CLUSTER_CAP, shortScore + score);
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
        // [v14.0] ForecastEngine integration
        public final com.bot.TradingCore.ForecastEngine.ForecastResult forecast;
        public final String trendPhase;
        // [ДЫРА №6] Адаптивные множители TP по режиму рынка
        public final double tp1Mult, tp2Mult, tp3Mult;

        /** Главный конструктор — с адаптивными TP множителями */
        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take, double rr,
                         double probability, List<String> flags,
                         double fundingRate, double fundingDelta,
                         double oiChange, String htfBias, CoinCategory cat,
                         com.bot.TradingCore.ForecastEngine.ForecastResult forecast,
                         double tp1Mult, double tp2Mult, double tp3Mult) {
            this.symbol = symbol; this.side = side;
            this.price = price; this.stop = stop; this.take = take;
            this.rr = rr; this.probability = probability;
            this.flags = flags != null ? Collections.unmodifiableList(new ArrayList<>(flags)) : List.of();
            this.fundingRate = fundingRate; this.fundingDelta = fundingDelta;
            this.oiChange = oiChange; this.htfBias = htfBias; this.category = cat;
            this.forecast = forecast;
            this.trendPhase = forecast != null ? forecast.trendPhase.name() : "UNKNOWN";
            this.tp1Mult = tp1Mult; this.tp2Mult = tp2Mult; this.tp3Mult = tp3Mult;

            double risk = Math.abs(price - stop);
            boolean long_ = side == com.bot.TradingCore.Side.LONG;
            this.tp1 = long_ ? price + risk * tp1Mult : price - risk * tp1Mult;
            this.tp2 = long_ ? price + risk * tp2Mult : price - risk * tp2Mult;
            this.tp3 = long_ ? price + risk * tp3Mult : price - risk * tp3Mult;
        }

        /** Обратная совместимость — без адаптивных TP (стандартные 1.0/2.0/3.2) */
        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take, double rr,
                         double probability, List<String> flags,
                         double fundingRate, double fundingDelta,
                         double oiChange, String htfBias, CoinCategory cat,
                         com.bot.TradingCore.ForecastEngine.ForecastResult forecast) {
            this(symbol, side, price, stop, take, rr, probability, flags,
                    fundingRate, fundingDelta, oiChange, htfBias, cat, forecast,
                    1.0, 2.0, 3.2);
        }

        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take,
                         double probability, List<String> flags) {
            // Цепочка: 7-arg → 14-arg (forecast=null) → 16-arg (tp mults=1.0/2.0/3.2)
            this(symbol, side, price, stop, take, 2.0, probability, flags,
                    0, 0, 0, "NONE", CoinCategory.ALT, null);
        }

        public TradeIdea(String symbol, com.bot.TradingCore.Side side,
                         double price, double stop, double take,
                         double probability, List<String> flags,
                         double fundingRate, double oiChange, String htfBias) {
            // Цепочка: 10-arg → 14-arg (forecast=null) → 16-arg (tp mults=1.0/2.0/3.2)
            this(symbol, side, price, stop, take, 2.0, probability, flags,
                    fundingRate, 0, oiChange, htfBias, CoinCategory.ALT, null);
        }
        // ── Префиксы внутренних флагов движка — не показываем трейдеру ──────────
        // Всё что начинается с этих префиксов — внутренняя механика анализатора.
        // Трейдеру важны только: размер позиции, OBI, дельта объёма, конфлюэнция.
        private static final java.util.Set<String> INTERNAL_FLAG_PREFIXES = java.util.Set.of(
                "GIC_", "FC_", "PH_", "STRUCT_", "ATR_", "ADX_", "LATE_", "CRASH_CONF_",
                "CONFL_", "HIGH_ATR", "BULL_DIV", "BEAR_DIV", "BULL_DIV_PENALTY",
                "BEAR_DIV_PENALTY", "BULL_DIV_VOL_OVERRIDE", "BEAR_DIV_VOL_OVERRIDE",
                "DIV_", "LONG_CRASH_PENALTY", "CLUST_", "LEXH_", "SEXH_", "REV_",
                "ANTI_LAG_", "RSI_SHIFT_", "EARLY_VETO_", "EARLY_BULL", "EARLY_BEAR",
                "BTC_CRASH", "BTC_ACCEL", "IMP_UP", "IMP_DN", "PULL_UP", "PULL_DN",
                "COMP_BREAK_", "HH_HL", "LL_LH", "FVG_", "OB_", "LIQ_SWEEP_",
                "VD_BUY", "VD_SELL", "VOL_SPIKE", "1H_BULL", "1H_BEAR", "2H_BULL",
                "2H_BEAR", "1H2H_BULL", "1H2H_BEAR", "HTF_CONFLICT", "VWAP_BULL",
                "VWAP_BEAR", "FR_NEG", "FR_POS", "FR_FALL", "FR_RISE", "OI_UP", "OI_DN",
                "PUMP_HUNT_"
        );

        /** Флаги видимые трейдеру — размер, OBI, дельта, конфлюэнция, фаза */
        private List<String> traderFlags() {
            List<String> result = new java.util.ArrayList<>();
            for (String f : flags) {
                boolean internal = false;
                for (String prefix : INTERNAL_FLAG_PREFIXES) {
                    if (f.startsWith(prefix) || f.equals(prefix.replace("_",""))) {
                        internal = true; break;
                    }
                }
                // Показываем: SIZE=, OBI±, ΔBUY/ΔSELL, CONFL_L/S, HIGH_ATR, WEAK_SECTOR,
                //             GIC_BOOST (как "BOOST"), EARLY_REV_PENALTY → "REV_WARN"
                if (!internal) {
                    result.add(f);
                } else if (f.startsWith("SIZE=")) {
                    result.add(f); // всегда показываем размер
                } else if (f.startsWith("LIQ_MAGNET")) {
                    result.add("🧲 " + f); // магнит ликвидаций — важно трейдеру
                } else if (f.equals("CVD_DIV⚠")) {
                    result.add("⚠️ CVD_DIV"); // CVD дивергенция — предупреждение
                } else if (f.startsWith("TP_TREND")) {
                    result.add("📈 TP×TREND"); // расширенные TP в тренде
                } else if (f.equals("TP_RANGE")) {
                    result.add("↔️ TP×RANGE"); // укороченные TP в боковике
                } else if (f.equals("TP_EXHAUST")) {
                    result.add("⛽ TP×СКАЛЬП"); // скальп при истощении
                } else if (f.equals("SESS_NY")) {
                    result.add("🗽 NY_SESSION"); // сигнал в NY — лучшее качество
                } else if (f.equals("SESS_LOW")) {
                    result.add("🌙 LOW_SESSION"); // сигнал ночью — осторожно
                } else if (f.startsWith("GIC_BOOST")) {
                    result.add("📡 BTC_SYNC"); // BTC подтверждает направление
                } else if (f.startsWith("GIC_WEAK")) {
                    result.add("⚠️ BTC_WEAK"); // BTC под давлением
                } else if (f.equals("GIC_BLOCK")) {
                    result.add("🔴 BTC_BLOCK"); // BTC в crash режиме
                } else if (f.equals("HIGH_ATR")) {
                    result.add("⚡ VOLATILE"); // высокая волатильность — стоп шире
                } else if (f.equals("WEAK_SECTOR")) {
                    result.add("📉 СЕКТ_СЛАБ"); // сектор под давлением
                } else if (f.startsWith("CONFL_L") || f.startsWith("CONFL_S")) {
                    result.add("🔥 " + f); // конфлюэнция — много факторов согласны
                }
            }
            return result;
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

            // Только флаги значимые для трейдера
            List<String> tf = traderFlags();
            String flagStr  = tf.isEmpty() ? "-" : String.join(", ", tf);

            String time = java.time.ZonedDateTime.now(java.time.ZoneId.of("Europe/Warsaw"))
                    .toLocalTime().withNano(0).toString();

            // ── Блок дополнительной информации ───────────────────────────────────
            StringBuilder extra = new StringBuilder();

            // Фандинг — только если значимый (>0.08%/8h)
            if (Math.abs(fundingRate) > 0.0008)
                extra.append(String.format("%n💸 FR: %+.3f%%", fundingRate * 100));
            if (Math.abs(fundingDelta) > 0.0003)
                extra.append(String.format(" Δ%+.3f%%", fundingDelta * 100));

            // Открытый интерес
            if (Math.abs(oiChange) > 0.5)
                extra.append(String.format(" | OI: %+.1f%%", oiChange));

            // 2H bias + R/R
            if (!"NONE".equals(htfBias) && !htfBias.isEmpty())
                extra.append(String.format("%n📊 2H: %s", htfBias));
            if (rr > 0)
                extra.append(String.format(" | R/R: 1:%.1f", rr));

            // ── ForecastEngine — понятное описание для трейдера ──────────────────
            if (forecast != null) {
                // Перевод bias в читаемый текст
                String biasRu = switch (forecast.bias) {
                    case STRONG_BULL -> "🟢 Сильный РОСТ";
                    case BULL        -> "🟩 Рост";
                    case NEUTRAL     -> "⬜ Нейтраль";
                    case BEAR        -> "🟥 Падение";
                    case STRONG_BEAR -> "🔴 Сильное ПАДЕНИЕ";
                };
                // Перевод фазы
                String phaseRu = switch (forecast.trendPhase) {
                    case EARLY      -> "🌱 Начало тренда";
                    case MID        -> "📈 Середина тренда";
                    case LATE       -> "⏳ Конец тренда";
                    case EXHAUSTION -> "⛽ Истощение";
                };
                extra.append(String.format(
                        "%n🔮 Прогноз: %s | %.0f%% уверенность | %+.2f%% ход%n"
                                + "📍 Фаза: %s",
                        biasRu,
                        forecast.confidence * 100.0,
                        forecast.projectedMovePct * 100.0,
                        phaseRu));
            }

            return String.format(
                    "%s *%s* → *%s* %s%n"
                            + "💰 Цена:   `%.6f`%n"
                            + "🎯 Вер-ть: *%.0f%%*%n"
                            + "🛑 SL:     `%.6f`  (риск %.2f%%)%n"
                            + "🟢 TP1:    `%.6f`  (+%.2f%%)  ← 50%% позиции%n"
                            + "🔵 TP2:    `%.6f`  (+%.2f%%)  ← 30%% позиции%n"
                            + "💎 TP3:    `%.6f`  (+%.2f%%)  ← 20%% трейл%n"
                            + "🏷 %s%s%n"
                            + "_⏰ %s_",
                    emoji, symbol, sideStr, catStr,
                    price, probability, stop, riskPct,
                    tp1, rp1Pct, tp2, rp2Pct, tp3, rp3Pct,
                    flagStr, extra.toString(), time
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
        Deque<CalibRecord> h = calibHist.computeIfAbsent(sym, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
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

        // [v23.0] ADX low in RANGE: penalty, not veto
        boolean adxRangePenalty = false;
        if (!aggressiveShort && state == MarketState.RANGE && adx(c15, 14) < 15) {
            adxRangePenalty = true;
        }

        int n15 = c15.size();
        double move4bars = last(c15).close - c15.get(n15 - 5).close;
        boolean lateEntryLong = false, lateEntryShort = false;
        if (Math.abs(move4bars) > atr14 * 2.0) {
            int consec = 0;
            boolean up = move4bars > 0;
            for (int i = n15 - 1; i >= Math.max(0, n15 - 6); i--) {
                if ((c15.get(i).close > c15.get(i).open) == up) consec++; else break;
            }
            if (consec >= 3 && up) lateEntryLong = true;
            if (consec >= 3 && !up) lateEntryShort = true;
        }

        // ════════════════════════════════════════════════════════
        //  ИНИЦИАЛИЗАЦИЯ 5 КЛАСТЕРОВ
        // ════════════════════════════════════════════════════════
        ClusterScores cStructure   = new ClusterScores(); // BOS, HH/HL, FVG, OB, LiqSweep
        ClusterScores cMomentum    = new ClusterScores(); // Impulse, AntiLag, Pump, Compression
        ClusterScores cVolume      = new ClusterScores(); // VolumeDelta, VolumeSpike
        ClusterScores cHTF         = new ClusterScores(); // 1H, 2H bias, VWAP
        ClusterScores cDerivatives = new ClusterScores(); // Funding, OI, Divergences
        ClusterScores cEarly       = new ClusterScores(); // [v7.1] Early Reversal Detection
        List<String> allFlags = new ArrayList<>();
        if (adxRangePenalty) allFlags.add("ADX_LOW_RANGE");

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

        // BOS functionality deferred to ForecastEngine analysis

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
        // [v15.0 FIX KITEUSDT] Block ANTI_LAG in volatility squeeze (ATR < 60% of 50-bar avg)
        double atrAvg50 = 0;
        if (c15.size() >= 65) {
            for (int i = c15.size() - 50; i < c15.size(); i++) {
                int pi = Math.max(1, i - 1);
                atrAvg50 += Math.max(c15.get(i).high - c15.get(i).low,
                        Math.max(Math.abs(c15.get(i).high - c15.get(pi).close),
                                Math.abs(c15.get(i).low - c15.get(pi).close)));
            }
            atrAvg50 /= 50;
        }
        boolean atrSqueeze = atrAvg50 > 0 && atr14 < atrAvg50 * 0.60;
        AntiLagResult antiLag = detectAntiLag(c1, c5, c15);
        if (antiLag != null && antiLag.strength > 0.38 && !atrSqueeze) {
            double bonus = mctx.s(antiLag.strength * 1.30);
            if (antiLag.direction > 0) cMomentum.addLong(bonus, "ANTI_LAG_UP");
            else                       cMomentum.addShort(bonus, "ANTI_LAG_DN");
        } else if (atrSqueeze && antiLag != null && antiLag.strength > 0.38) {
            allFlags.add("ANTI_LAG_SQUEEZE_BLOCKED");
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

        // Old Pump functionality replaced by ForecastEngine/PumpHunter
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

        // Volume Delta (мгновенная)
        Double vd = volumeDeltaMap.get(symbol);
        double vdRatio = getVolumeDeltaRatio(symbol);
        if (vd != null && vdRatio > 1.5) {
            double vdScore = mctx.s(Math.min(0.55, vdRatio * 0.14));
            if (vd > 0) cVolume.addLong(vdScore, "VD_BUY");
            else        cVolume.addShort(vdScore, "VD_SELL");
        }

        // [ДЫРА №1] CVD — Cumulative Volume Delta (90×1m накопленная)
        // Вес 0.65 vs 0.14 у мгновенной дельты — CVD показывает реальные намерения.
        // Если цена растёт, а CVD падает — институционалы ПРОДАЮТ в рост → ЛОВУШКА.
        double cvdVal = cvdMap.getOrDefault(symbol, 0.0);
        if (Math.abs(cvdVal) > 0.10) {
            double cvdScore = mctx.s(Math.min(0.65, Math.abs(cvdVal) * 0.70));
            if (cvdVal > 0) cVolume.addLong(cvdScore, "CVD_BUY");
            else            cVolume.addShort(cvdScore, "CVD_SELL");
            // Дивергенции проверяем СНАРУЖИ if-блоков — иначе мёртвый код
            // Цена растёт (+) но CVD отрицательный → институционалы продают в рост → ЛОВУШКА
            if (move5 > 0.002 && cvdVal < -0.10) cVolume.penalizeLong(0.60);
            // Цена падает (−) но CVD положительный → ложный дамп, накопление → ВОЗМОЖНЫЙ РАЗВОРОТ
            if (move5 < -0.002 && cvdVal > 0.10) cVolume.penalizeShort(0.60);
        }
        // CVD-дивергенция высокой уверенности (порог -0.15/+0.15) → добавляем в кластер
        boolean cvdDivBear = move5 > 0.003 && cvdVal < -0.15;
        boolean cvdDivBull = move5 < -0.003 && cvdVal > 0.15;
        if (cvdDivBear) { cVolume.addShort(mctx.s(0.55), "CVD_DIV_BEAR"); allFlags.add("CVD_DIV⚠"); }
        if (cvdDivBull) { cVolume.addLong(mctx.s(0.55),  "CVD_DIV_BULL"); allFlags.add("CVD_DIV⚠"); }

        // Volume Spike
        if (volumeSpike(c15, cat)) {
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

        // HTF конфликт = мягкое ослабление (аддитивное, не множительное)
        if ((bias1h == HTFBias.BULL && bias2h == HTFBias.BEAR) ||
                (bias1h == HTFBias.BEAR && bias2h == HTFBias.BULL)) {
            if (!aggressiveShort) {
                // [v8.0] Аддитивный штраф вместо *= 0.50
                cHTF.boostLong(-0.15, null);
                cHTF.boostShort(-0.15, null);
            } else {
                cHTF.boostLong(-0.20, null);
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
        // КЛАСТЕР 6: EARLY — РАННИЙ РАЗВОРОТ
        // [v7.1] Ловит момент когда текущий тренд ОСЛАБЕВАЕТ
        // но ещё не сломалась структура на 15m.
        // Использует 1m/5m micro-structure + momentum deceleration
        // + volume divergence + wick rejection + RSI shift.
        // Даёт сигнал на 1-2 свечи РАНЬШЕ чем Structure/Momentum.
        // ════════════════════════════════════════════════════════

        EarlyReversalResult earlyRev = detectEarlyReversal(c1, c5, c15, rsi14, rsi7, price, atr14);
        if (earlyRev.detected && earlyRev.strength > 0.35) {
            double earlyScore = mctx.s(earlyRev.strength * 0.70);
            if (earlyRev.direction > 0) {
                cEarly.addLong(earlyScore, "EARLY_BULL");
            } else {
                cEarly.addShort(earlyScore, "EARLY_BEAR");
            }
            allFlags.addAll(earlyRev.flags);
        }

        // ════════════════════════════════════════════════════════
        //  АГРЕГАЦИЯ КЛАСТЕРОВ
        // ════════════════════════════════════════════════════════

        ClusterScores[] clusters = { cStructure, cMomentum, cVolume, cHTF, cDerivatives, cEarly };
        String[] clusterNames = { "STR", "MOM", "VOL", "HTF", "DRV", "EARLY" };

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
                boolean confirmed = confirmReversalStructure(c1, c5, com.bot.TradingCore.Side.SHORT);
                if (!confirmed) {
                    scoreLong *= 0.35;
                    // [v23.0] Flag for penalty, not veto
                    if (scoreLong < 0.20) allFlags.add("LEXH_SCORE_CRUSHED");
                } else {
                    allFlags.add("LEXH_CONFIRMED_1M");
                }
            } else if ("SHORT_EXHAUSTION".equals(rw.type)) {
                if (aggressiveShort) {
                    allFlags.add("REV_IGNORED_CRASH");
                } else {
                    boolean confirmed = confirmReversalStructure(c1, c5, com.bot.TradingCore.Side.LONG);
                    if (!confirmed) {
                        scoreShort *= 0.35;
                        if (scoreShort < 0.20) allFlags.add("SEXH_SCORE_CRUSHED");
                    } else {
                        allFlags.add("REV_CONFIRMED_1M");
                    }
                }
            }
        }

        // ════════════════════════════════════════════════════════
        // EXHAUSTION FILTERS
        // ════════════════════════════════════════════════════════
        // [v18.0] Removed overlapping and arbitrary EXHAUSTION, OVEREXTENDED, and 2H VETO
        // multipliers (e.g. scoreLong *= 0.45, etc.) that effectively forced the bot to
        // "fade" out of perfectly valid setups. ForecastEngine now governs macro directional confidence.

        // ════════════════════════════════════════════════════════
        // [v8.0] МИНИМУМ КЛАСТЕРОВ — АДАПТИВНЫЙ
        // Обычно: 2 кластера
        // Если EARLY сильный (> 0.65): достаточно 1 кластер
        // Это позволяет ловить развороты ДО подтверждения структуры
        // ════════════════════════════════════════════════════════
        com.bot.TradingCore.Side candidateSide = scoreLong > scoreShort
                ? com.bot.TradingCore.Side.LONG
                : com.bot.TradingCore.Side.SHORT;

        // [v19.0] HARD VETO: Если очень мощный разворотный сигнал идет ПРОТИВ тренда,
        // который набрал баллы на отстающих индикаторах (как 1H EMA), убиваем трендовый сигнал.
        if (earlyRev.detected && earlyRev.strength > 0.35) {
            com.bot.TradingCore.Side earlySide = earlyRev.direction > 0 ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT;

            boolean rsiSupportsEarly = (earlySide == com.bot.TradingCore.Side.LONG && rsi14 < 45) ||
                    (earlySide == com.bot.TradingCore.Side.SHORT && rsi14 > 55);
            boolean antiLagSupportsEarly = (earlySide == com.bot.TradingCore.Side.LONG && allFlags.contains("ANTI_LAG_UP")) ||
                    (earlySide == com.bot.TradingCore.Side.SHORT && allFlags.contains("ANTI_LAG_DN"));
            boolean rsiShiftSupportsEarly = (earlySide == com.bot.TradingCore.Side.LONG && allFlags.contains("RSI_SHIFT_UP")) ||
                    (earlySide == com.bot.TradingCore.Side.SHORT && allFlags.contains("RSI_SHIFT_DN"));
            boolean strongOverride = rsiSupportsEarly || antiLagSupportsEarly || rsiShiftSupportsEarly || earlyRev.strength > 0.55;

            if (earlySide != candidateSide) {
                // Прямой конфликт: сильный ранний разворот ПРОТИВ кандидата
                if (strongOverride) {
                    // [v24.0 FIX WEAK-4] Cap at 0.30 (was 0.15 = 85% crush, destroyed valid trends).
                    // One wick rejection + deceleration was killing 5-cluster trend setups.
                    if (candidateSide == com.bot.TradingCore.Side.LONG) scoreLong *= 0.30;
                    else scoreShort *= 0.30;
                    allFlags.add("EARLY_VETO_OPPOSITE");
                    // Пересчитаем кандидата после штрафа
                    candidateSide = scoreLong > scoreShort ? com.bot.TradingCore.Side.LONG : com.bot.TradingCore.Side.SHORT;
                }
            } else {
                // Прямое совпадение: ранний разворот ВЕДЕТ кандидата.
                // Отстающие индикаторы на противоположной стороне просто "съедают" scoreDiff, мешая поймать разворот мгновенно.
                // Штрафуем противоположную сторону, чтобы пробить мин. scoreDiff барьер!
                if (strongOverride && earlyRev.strength > 0.45) {
                    if (candidateSide == com.bot.TradingCore.Side.LONG) {
                        scoreShort *= 0.30;
                        allFlags.add("EARLY_CLEAR_S");
                    } else {
                        scoreLong *= 0.30;
                        allFlags.add("EARLY_CLEAR_L");
                    }
                }
            }
        }

        int supportingClusters = candidateSide == com.bot.TradingCore.Side.LONG
                ? longClusters : shortClusters;

        // [v8.0] EARLY-SOLO: сильный ранний сигнал может пройти с 1 кластером
        boolean earlyStrong = earlyRev.detected && earlyRev.strength > 0.65;
        boolean earlySoloAllowed = earlyStrong && (
                // EARLY + хотя бы Volume или Derivatives — контр-тренд подтверждён
                (candidateSide == com.bot.TradingCore.Side.SHORT && cEarly.favorsShort()
                        && (cVolume.favorsShort() || cDerivatives.favorsShort())) ||
                        (candidateSide == com.bot.TradingCore.Side.LONG && cEarly.favorsLong()
                                && (cVolume.favorsLong() || cDerivatives.favorsLong()))
        );

        int requiredClusters = earlySoloAllowed ? 1 : MIN_AGREEING_CLUSTERS;

        // [v11.0] RANGE market is treacherous — require 3 clusters minimum
        if (state == MarketState.RANGE && !earlySoloAllowed && !aggressiveShort) {
            requiredClusters = 3;
        }

        // [v23.0] Insufficient clusters → penalty flag (was return null)
        boolean clusterPenalty = false;
        if (supportingClusters < requiredClusters) {
            if (!(aggressiveShort && candidateSide == com.bot.TradingCore.Side.SHORT && crashBoost > 0.30)) {
                clusterPenalty = true;
                allFlags.add("LOW_CLUSTERS_" + supportingClusters + "/" + requiredClusters);
            }
        }

        if (earlySoloAllowed) allFlags.add("EARLY_SOLO");

        // ════════════════════════════════════════════════════════
        // MINIMUM SCORE DIFFERENCE
        // ════════════════════════════════════════════════════════
        double scoreDiff = Math.abs(scoreLong - scoreShort);
        double minDiff;
        if (aggressiveShort) {
            minDiff = 0.08;
        } else {
            minDiff = state == MarketState.STRONG_TREND ? 0.16
                    : state == MarketState.RANGE ? 0.28
                    : 0.20;
        }
        // [v23.0] scoreDiff/dynThresh → penalty flags (was return null)
        boolean scoreDiffPenalty = scoreDiff < minDiff;
        boolean dynThreshPenalty = false;
        double dynThresh;
        if (aggressiveShort) {
            dynThresh = 0.40;
        } else {
            dynThresh = state == MarketState.STRONG_TREND ? 0.68 : 0.58;
        }
        if (scoreLong < dynThresh && scoreShort < dynThresh) {
            if (!bullDiv && !bearDiv && !hasFR && !aggressiveShort) {
                dynThreshPenalty = true;
                allFlags.add("DYN_THRESH_LOW");
            }
        }

        com.bot.TradingCore.Side side = candidateSide;

        // [v23.0] LATE ENTRY: penalty, not veto — signals still checked by probability gate
        boolean lateEntryPenalty = false;
        if (side == com.bot.TradingCore.Side.LONG && lateEntryLong && !aggressiveShort) {
            lateEntryPenalty = true;
            allFlags.add("LATE_ENTRY_L");
        }
        if (side == com.bot.TradingCore.Side.SHORT && lateEntryShort && !aggressiveShort) {
            lateEntryPenalty = true;
            allFlags.add("LATE_ENTRY_S");
        }

        // [v11.0] VOLUME CONFIRMATION GATE
        boolean volumeSupports = (side == com.bot.TradingCore.Side.LONG && cVolume.favorsLong())
                || (side == com.bot.TradingCore.Side.SHORT && cVolume.favorsShort());
        boolean volumeOpposes = (side == com.bot.TradingCore.Side.LONG && cVolume.favorsShort())
                || (side == com.bot.TradingCore.Side.SHORT && cVolume.favorsLong());

        // [v14.0 FIX] Volume = SOFT GATE
        if (volumeOpposes && !aggressiveShort) {
            if (candidateSide == com.bot.TradingCore.Side.LONG) scoreLong *= 0.55;
            else scoreShort *= 0.55;
            allFlags.add("VOL_OPPOSE");
            scoreDiff = Math.abs(scoreLong - scoreShort);
        }
        // Нет volume confirmation — мягкий штраф 15%
        if (!volumeSupports && !volumeOpposes && !aggressiveShort) {
            if (candidateSide == com.bot.TradingCore.Side.LONG) scoreLong *= 0.85;
            else scoreShort *= 0.85;
            allFlags.add("VOL_NEUTRAL");
        }

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
                impulseFlag, false, hasFR,
                fvg.detected, ob.detected, false, liqSweep,
                bias2h, vwapVal
        );

        // Crash mode confidence boost
        if (aggressiveShort && side == com.bot.TradingCore.Side.SHORT) {
            double crashConfBoost = btcCrashScore * 10.0;
            probability = Math.min(85, probability + crashConfBoost);
            allFlags.add("CRASH_CONF_BOOST");
        }

        // ════════════════════════════════════════════════════════
        // [v23.0] PENALTY APPLICATION — all collected flags → probability adjustments
        // This replaces 7+ scattered `return null` with ONE probability gate.
        // ════════════════════════════════════════════════════════
        if (adxRangePenalty) probability = Math.max(50, probability - 8);
        if (lateEntryPenalty) probability = Math.max(50, probability - 10);
        if (volumeOpposes && !aggressiveShort) probability = Math.max(50, probability - 6);
        if (clusterPenalty) probability = Math.max(50, probability - 12);
        if (scoreDiffPenalty) probability = Math.max(50, probability - 15);
        if (dynThreshPenalty) probability = Math.max(50, probability - 10);
        if (allFlags.contains("LEXH_SCORE_CRUSHED") || allFlags.contains("SEXH_SCORE_CRUSHED"))
            probability = Math.max(50, probability - 12);

        double minConf = symbolMinConf.getOrDefault(symbol, globalMinConf);
        if (aggressiveShort && side == com.bot.TradingCore.Side.SHORT) {
            minConf = Math.max(45.0, minConf - 8.0);
        }
        if (probability < minConf) return null;

        if (atr14 / price > 0.0020) allFlags.add("HIGH_ATR");

        // ════════════════════════════════════════════════════════
        // СТОП И ТЕЙК — [v11.0] STRUCTURAL STOP PLACEMENT
        // Instead of fixed ATR multiplier, SL goes behind nearest
        // swing high/low. This respects market structure and prevents
        // the #1 killer: arbitrary SL that gets hunted by liquidity grabs.
        // ════════════════════════════════════════════════════════
        double riskMult = cat == CoinCategory.MEME ? 1.40 : cat == CoinCategory.ALT ? 1.10 : 0.88;
        double rrRatio  = scoreDiff > 1.2 ? 3.4 : scoreDiff > 0.9 ? 3.0 : scoreDiff > 0.6 ? 2.7 : 2.3;

        // [v12.0] Find structural stop level (-1 = not found, use ATR)
        double structuralStop = findStructuralStop(c15, side, price, atr14);
        double atrStop = Math.max(atr14 * 1.85 * riskMult, price * 0.0018);

        // Use the WIDER of structural and ATR stop — never tighter than structure
        double stopDist;
        if (structuralStop <= 0) {
            // [v12.0] No swing found — use ATR stop (no silent rejection!)
            stopDist = atrStop;
            allFlags.add("ATR_STOP");
        } else if (side == com.bot.TradingCore.Side.LONG) {
            double structDist = price - structuralStop;
            stopDist = Math.max(structDist, atrStop);
            allFlags.add("STRUCT_STOP");
        } else {
            double structDist = structuralStop - price;
            stopDist = Math.max(structDist, atrStop);
            allFlags.add("STRUCT_STOP");
        }

        // [v11.0] Cap stop at 3% to prevent absurd risk
        stopDist = Math.min(stopDist, price * 0.030);

        // [v23.0] Wide structure → penalty, not veto
        if (stopDist > atrStop * 2.5) {
            allFlags.add("STRUCT_WIDE");
            probability = Math.max(50, probability - 7);
            if (probability < minConf) return null; // re-check after penalty
        }

        double stopPrice = side == com.bot.TradingCore.Side.LONG  ? price - stopDist : price + stopDist;
        double takePrice = side == com.bot.TradingCore.Side.LONG  ? price + stopDist * rrRatio
                : price - stopDist * rrRatio;

        if (!priceMovedEnough(symbol, price)) return null;
        // [v14.0 FIX] НЕ вызываем registerSignal() здесь — cooldown ставится
        // только ПОСЛЕ ISC.allowSignal() в SignalSender через confirmSignal()

        // ════════════════════════════════════════════════════════
        // [v17.0] ForecastEngine Integration — RELAXED GATING
        // Philosophy: ForecastEngine is an ADVISOR, not a DICTATOR.
        // Only block when forecast STRONGLY disagrees. Otherwise,
        // let the cluster-based signal through with a penalty.
        // ════════════════════════════════════════════════════════
        com.bot.TradingCore.ForecastEngine.ForecastResult forecastResult = null;
        // [v24.0 FIX WEAK-3] Save probability before FC for penalty cap
        final double probBeforeFC = probability;
        if (forecastEngine != null) {
            try {
                vd = volumeDeltaMap.getOrDefault(symbol, 0.0);
                forecastResult = forecastEngine.forecast(c5, c15, c1h, vd);
                if (forecastResult != null) {
                    allFlags.add("FC_" + forecastResult.bias.name());
                    allFlags.add("PH_" + forecastResult.trendPhase.name());

                    boolean sigLong = side == com.bot.TradingCore.Side.LONG;
                    double fcDirAbs = Math.abs(forecastResult.directionScore);
                    boolean fcBull = forecastResult.directionScore > 0.2;
                    boolean fcBear = forecastResult.directionScore < -0.2;

                    // ══════════════════════════════════════════════════
                    // [v23.0] ALL VETOES → PENALTIES
                    // ForecastEngine is an ADVISOR, not a DICTATOR.
                    // Squeeze = OPPORTUNITY (penalty reduced, breakout boosted).
                    // ══════════════════════════════════════════════════

                    // SQUEEZE: reduced penalty, NOT veto (was return null)
                    Double squeezeFlag = forecastResult.factorScores.get("SQUEEZE");
                    if (squeezeFlag != null && squeezeFlag > 0.5) {
                        probability = Math.max(50, probability - 5);
                        allFlags.add("FC_SQUEEZE_PENALTY");
                    }

                    // EXHAUSTION: penalty -8 (was return null)
                    Double exhaustionFlag = forecastResult.factorScores.get("EXHAUSTION");
                    if (exhaustionFlag != null && exhaustionFlag > 0.70
                            && forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EXHAUSTION) {
                        Double moveDirFlag = forecastResult.factorScores.get("MOVE_DIR");
                        int moveDir = moveDirFlag != null ? (int) Math.signum(moveDirFlag) : 0;
                        if ((sigLong && moveDir > 0) || (!sigLong && moveDir < 0)) {
                            probability = Math.max(50, probability - 8);
                            allFlags.add("FC_EXHAUST_PENALTY");
                        }
                    }

                    // EXHAUSTION phase: penalty -6 unless overridden (was return null)
                    if (forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EXHAUSTION) {
                        boolean exhaustionOverride = forecastResult.confidence >= 0.76
                                && fcDirAbs >= 0.34
                                && ((sigLong && forecastResult.directionScore > 0)
                                || (!sigLong && forecastResult.directionScore < 0));
                        if (!exhaustionOverride) {
                            probability = Math.max(50, probability - 6);
                            allFlags.add("FC_EXHAUST_PHASE");
                        } else {
                            allFlags.add("FC_EXHAUST_OVERRIDE");
                        }
                    }

                    // STRONG disagreement: heavy penalty -15 (was return null)
                    if (sigLong && forecastResult.bias == com.bot.TradingCore.ForecastEngine.ForecastBias.STRONG_BEAR && fcDirAbs >= 0.35) {
                        probability = Math.max(50, probability - 15);
                        allFlags.add("FC_STRONG_BEAR_PENALTY");
                    }
                    if (!sigLong && forecastResult.bias == com.bot.TradingCore.ForecastEngine.ForecastBias.STRONG_BULL && fcDirAbs >= 0.35) {
                        probability = Math.max(50, probability - 15);
                        allFlags.add("FC_STRONG_BULL_PENALTY");
                    }

                    // Mild disagreement → penalty -4
                    if ((sigLong && fcBear) || (!sigLong && fcBull)) {
                        probability = Math.max(50, probability - 4);
                        allFlags.add("FC_DISAGREE");
                    }

                    // Projected move against direction → penalty -3
                    if (sigLong && forecastResult.projectedMovePct <= 0) {
                        probability = Math.max(50, probability - 3);
                        allFlags.add("FC_PROJ_DN");
                    }
                    if (!sigLong && forecastResult.projectedMovePct >= 0) {
                        probability = Math.max(50, probability - 3);
                        allFlags.add("FC_PROJ_UP");
                    }

                    // Early Counter-Trend: penalty -10 (was return null)
                    if (forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EARLY) {
                        boolean earlyOpposed = (sigLong && forecastResult.directionScore < -0.15)
                                || (!sigLong && forecastResult.directionScore > 0.15);
                        if (earlyOpposed && forecastResult.confidence > 0.40) {
                            probability = Math.max(50, probability - 10);
                            allFlags.add("FC_EARLY_REV_PENALTY");
                        }
                    }

                    // BOOST: Early trend aligned
                    if (forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EARLY
                            && Math.abs(forecastResult.directionScore) > 0.25) {
                        boolean earlyAligned = (sigLong && forecastResult.directionScore > 0)
                                || (!sigLong && forecastResult.directionScore < 0);
                        if (earlyAligned) {
                            probability = Math.min(85, probability + 3);
                            allFlags.add("FC_EARLY_BOOST");
                        }
                    }

                    // ── RANGE quality gate — penalty, NOT veto ──
                    // [v17.0] Was hard veto → now soft penalty (-5)
                    double stopRetAbs = stopDist / (price + 1e-9);
                    double fcMoveAbs = Math.abs(forecastResult.projectedMovePct);
                    double fcConf = forecastResult.confidence;
                    boolean isRange = state == MarketState.RANGE;

                    if (isRange) {
                        boolean moveOk = fcMoveAbs >= stopRetAbs * 0.90;
                        boolean confOk = fcConf >= 0.50;
                        if (!moveOk || !confOk) {
                            probability = Math.max(50, probability - 5);
                            allFlags.add("FC_RANGE_PENALTY");
                        }
                        if (forecastResult.bias == com.bot.TradingCore.ForecastEngine.ForecastBias.NEUTRAL) {
                            probability = Math.max(50, probability - 3);
                            allFlags.add("FC_NEUTRAL_RANGE");
                        }
                    } else {
                        // Non-range: mild adjustments
                        if (forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EXHAUSTION) {
                            probability = Math.max(50, probability - 2);
                            allFlags.add("FC_EXHAUST_SOFT");
                        }
                        if (fcMoveAbs < stopRetAbs * 0.80) {
                            probability = Math.max(50, probability - 2);
                            allFlags.add("FC_MOVE_WEAK");
                        } else {
                            allFlags.add("FC_MOVE_OK");
                        }
                    }
                    // [v17.0] REMOVED: FC_LOWCONF_NEUTRAL veto — NEUTRAL means "I don't know",
                    // not "block everything". Let the cluster signal decide.

                    // [v24.0 FIX WEAK-3] FC PENALTY CAP — max -25 total from ForecastEngine.
                    // Old code stacked 7+ penalties totalling -51 points, always killing signals
                    // even when 5/6 clusters agreed. Cap prevents FC from being a dictator.
                    double fcTotalPenalty = probability - probBeforeFC;
                    if (fcTotalPenalty < -25.0) {
                        probability = Math.max(50, probBeforeFC - 25.0);
                        allFlags.add("FC_PENALTY_CAPPED_" + String.format("%.0f", fcTotalPenalty));
                    }
                }
            } catch (Exception e) {
                System.out.printf("[FC] %s forecast error: %s%n", symbol, e.getMessage());
            }
        }

        // [v23.0] FINAL GATE — after ALL penalties (FC, structural, volume)
        // This is the ONE place that decides if a signal lives or dies.
        probability = Math.max(50, Math.min(85, probability));
        if (probability < minConf) return null;

        // ════════════════════════════════════════════════════════
        // [ДЫРА №6] АДАПТИВНЫЕ TP ПО РЕЖИМУ РЫНКА
        // Одни и те же множители TP для всех режимов — главная причина
        // "недобора" в тренде и "перелёта" в боковике.
        //
        // RANGE:  цена ходит в канале → короткие TP, быстро фиксируем
        // TREND:  цена идёт далеко → длинные TP, не закрываем рано
        // EXHAUST: движение умирает → очень короткие TP, скальп
        // ════════════════════════════════════════════════════════
        double tp1Mult, tp2Mult, tp3Mult;
        boolean isTrendState  = state == MarketState.STRONG_TREND;
        boolean isRangeState  = state == MarketState.RANGE;
        boolean isExhaustPhase = forecastResult != null
                && forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EXHAUSTION;
        boolean isEarlyPhase  = forecastResult != null
                && forecastResult.trendPhase == com.bot.TradingCore.ForecastEngine.TrendPhase.EARLY;

        if (isExhaustPhase) {
            // Истощение — скальп, быстро берём что дают
            tp1Mult = 0.70; tp2Mult = 1.20; tp3Mult = 1.80;
            allFlags.add("TP_EXHAUST");
        } else if (isRangeState) {
            // Боковик — умеренные TP, TP3 часто не достигается
            tp1Mult = 0.80; tp2Mult = 1.40; tp3Mult = 2.20;
            allFlags.add("TP_RANGE");
        } else if (isTrendState && isEarlyPhase) {
            // Сильный тренд + начало движения — максимальные TP
            tp1Mult = 1.30; tp2Mult = 2.60; tp3Mult = 4.20;
            allFlags.add("TP_TREND_EARLY");
        } else if (isTrendState) {
            // Сильный тренд — расширяем TP
            tp1Mult = 1.15; tp2Mult = 2.30; tp3Mult = 3.60;
            allFlags.add("TP_TREND");
        } else {
            // Слабый тренд — стандарт
            tp1Mult = 1.00; tp2Mult = 2.00; tp3Mult = 3.20;
        }

        // Пересчитываем rrRatio на основе выбранного tp3
        double adaptiveRR = tp3Mult;

        return new TradeIdea(symbol, side, price, stopPrice, takePrice, adaptiveRR,
                probability, allFlags,
                fundingRate, fundingDelta, oiChange, bias2h.name(), cat,
                forecastResult,
                tp1Mult, tp2Mult, tp3Mult);
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

        // [v7.1] Бонус за количество согласных КЛАСТЕРОВ (теперь из 6)
        double clusterBonus = switch (clusters) {
            case 6 -> 0.14;  // все 6 согласны = очень сильный сигнал
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

        // [v11.0] Range confidence: 50 + norm * range
        // Reduced range so we don't reach unrealistic probabilities
        double range = 22 + Math.min(clusters * 3.5, 14); // max 22+14=36, итого max 86, clamp 85
        double prob  = 50 + norm * range;

        // Market state adjustment
        if (state == MarketState.STRONG_TREND)      prob += 2.0;  // was 3.0 — overconfident in trends
        else if (state == MarketState.WEAK_TREND)   prob += 0.0;  // was 0.5 — weak trend ≠ bonus
        else if (state == MarketState.RANGE)        prob -= 4.0;  // was -3.0 — range is HARD, penalize more

        // Category adjustment
        if (cat == CoinCategory.MEME)               prob -= 5.0;
        else if (cat == CoinCategory.ALT)           prob -= 2.0;

        // Historical calibration
        Deque<CalibRecord> hist = calibHist.get(symbol);
        if (hist != null && hist.size() >= 30) {
            double histAcc = historicalAccuracy(hist, prob);
            prob = prob * 0.70 + histAcc * 0.30;
        }

        return Math.round(clamp(prob, 50, 85)); // [v11.0] Ceiling 85 (was 88 — unrealistic for crypto scalping)
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

    /**
     * [v24.0 FIX BUG-2] CHECK ONLY — does NOT set cooldown anymore.
     * Old code set cooldown here (line 1295), so rejected signals burned the cooldown window.
     * Valid signals coming 30s later were blocked because the rejected signal consumed the cooldown.
     * Now cooldown is set ONLY through confirmSignal() after ISC approves.
     */
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
        // [v24.0] CHECK ONLY — removed: cooldownMap.put(key, now)
        return last == null || now - last >= base;
    }

    private boolean cooldownAllowed(String sym, com.bot.TradingCore.Side side, CoinCategory cat, long now) {
        return cooldownAllowedEx(sym, side, cat, now, -1);
    }

    private boolean flipAllowed(String sym, com.bot.TradingCore.Side newSide) {
        Deque<String> h = recentDirs.computeIfAbsent(sym, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        if (h.size() < 2) return true;
        Iterator<String> it = h.descendingIterator();
        String last = it.next(), prev = it.next();
        return !(!last.equals(newSide.name()) && prev.equals(newSide.name()));
    }

    private void registerSignal(String sym, com.bot.TradingCore.Side side, long now) {
        cooldownMap.put(sym + "_" + side, now);
        Deque<String> h = recentDirs.computeIfAbsent(sym, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        h.addLast(side.name());
        if (h.size() > 3) h.removeFirst();
        signalCountBySymbol.computeIfAbsent(sym, k -> new AtomicInteger(0)).incrementAndGet();
    }

    /**
     * [v24.0 FIX BUG-3] CHECK ONLY — does NOT update lastSigPrice.
     * Old code updated price here, so rejected signals blocked future valid ones.
     * A signal rejected by FC/ISC would still update lastSigPrice → next valid signal
     * 2 minutes later was blocked as "price not moved enough". Now lastSigPrice
     * is updated ONLY in confirmSignal() after ISC approves.
     */
    private boolean priceMovedEnough(String sym, double price) {
        Double last = lastSigPrice.get(sym);
        if (last == null) return true;
        return Math.abs(price - last) / last >= 0.0035;
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
    //  [v7.1] EARLY REVERSAL DETECTION
    //  5 независимых сигналов раннего разворота:
    //  1. Momentum Deceleration — свечи уменьшаются, тренд слабеет
    //  2. Volume Divergence — цена = новый экстремум, объём падает
    //  3. Wick Rejection — длинная тень на 1m/5m = отвергли уровень
    //  4. RSI Momentum Shift — RSI разворачивается раньше цены
    //  5. Micro Structure Break — на 1m сломалась структура
    // ══════════════════════════════════════════════════════════════

    private static final class EarlyReversalResult {
        final boolean detected;
        final int     direction; // +1 = reversal to LONG, -1 = reversal to SHORT
        final double  strength;  // 0..1
        final List<String> flags;

        EarlyReversalResult(boolean d, int dir, double s, List<String> f) {
            detected = d; direction = dir; strength = s; flags = f;
        }
    }

    private EarlyReversalResult detectEarlyReversal(
            List<com.bot.TradingCore.Candle> c1,
            List<com.bot.TradingCore.Candle> c5,
            List<com.bot.TradingCore.Candle> c15,
            double rsi14, double rsi7,
            double price, double atr14) {

        if (c15.size() < 12) return new EarlyReversalResult(false, 0, 0, List.of());

        double score = 0;
        int bullSignals = 0, bearSignals = 0;
        List<String> flags = new ArrayList<>();

        // ── Определяем текущий тренд на 15m ──────────────────
        double move4 = (last(c15).close - c15.get(c15.size() - 5).close) / price;
        boolean inUptrend   = move4 > 0.003;
        boolean inDowntrend = move4 < -0.003;

        // Если нет выраженного тренда — ранний разворот не применяется
        if (!inUptrend && !inDowntrend) return new EarlyReversalResult(false, 0, 0, List.of());

        // ═══════════════════════════════════════════════════════
        // 1. MOMENTUM DECELERATION
        // Тренд идёт вверх/вниз, но свечи УМЕНЬШАЮТСЯ.
        // Тело 3-й < тело 2-й < тело 1-й = тренд теряет силу.
        // ═══════════════════════════════════════════════════════
        if (c15.size() >= 5) {
            double b1 = Math.abs(c15.get(c15.size()-1).close - c15.get(c15.size()-1).open);
            double b2 = Math.abs(c15.get(c15.size()-2).close - c15.get(c15.size()-2).open);
            double b3 = Math.abs(c15.get(c15.size()-3).close - c15.get(c15.size()-3).open);

            // Все 3 свечи в одном направлении, но уменьшаются
            boolean allUp = c15.get(c15.size()-1).close > c15.get(c15.size()-1).open
                    && c15.get(c15.size()-2).close > c15.get(c15.size()-2).open
                    && c15.get(c15.size()-3).close > c15.get(c15.size()-3).open;
            boolean allDown = c15.get(c15.size()-1).close < c15.get(c15.size()-1).open
                    && c15.get(c15.size()-2).close < c15.get(c15.size()-2).open
                    && c15.get(c15.size()-3).close < c15.get(c15.size()-3).open;

            if (allUp && b1 < b2 * 0.72 && b2 < b3 * 0.82) {
                // Лонг-тренд слабеет → разворот вниз
                score += 0.30;
                bearSignals++;
                flags.add("DECEL_UP");
            }
            if (allDown && b1 < b2 * 0.72 && b2 < b3 * 0.82) {
                // Шорт-тренд слабеет → разворот вверх
                score += 0.30;
                bullSignals++;
                flags.add("DECEL_DN");
            }
        }

        // ═══════════════════════════════════════════════════════
        // 2. VOLUME DIVERGENCE
        // Цена делает новый хай/лой, но объём падает.
        // Smart money уже вышли — розница догоняет.
        // ═══════════════════════════════════════════════════════
        if (c15.size() >= 6) {
            com.bot.TradingCore.Candle cur  = c15.get(c15.size() - 1);
            com.bot.TradingCore.Candle prev = c15.get(c15.size() - 2);
            double avgVol3 = (c15.get(c15.size()-4).volume + c15.get(c15.size()-3).volume
                    + c15.get(c15.size()-2).volume) / 3.0;

            if (inUptrend && cur.high > prev.high && cur.volume < avgVol3 * 0.70) {
                score += 0.28;
                bearSignals++;
                flags.add("VDIV_UP");
            }
            if (inDowntrend && cur.low < prev.low && cur.volume < avgVol3 * 0.70) {
                score += 0.28;
                bullSignals++;
                flags.add("VDIV_DN");
            }
        }

        // ═══════════════════════════════════════════════════════
        // 3. WICK REJECTION на 1m/5m
        // Длинная тень в направлении тренда = цену отвергли.
        // Если на 5m последняя свеча имеет тень > 2× тело
        // в направлении тренда — это rejection.
        // ═══════════════════════════════════════════════════════
        if (c5 != null && c5.size() >= 3) {
            com.bot.TradingCore.Candle lc5 = c5.get(c5.size() - 1);
            double body5 = Math.abs(lc5.close - lc5.open) + 1e-10;
            double upperWick5 = lc5.high - Math.max(lc5.open, lc5.close);
            double lowerWick5 = Math.min(lc5.open, lc5.close) - lc5.low;

            if (inUptrend && upperWick5 > body5 * 2.0 && lc5.close < lc5.open) {
                // Длинная верхняя тень при аптренде + медвежья свеча = rejection сверху
                score += 0.32;
                bearSignals++;
                flags.add("WICK_REJ_UP");
            }
            if (inDowntrend && lowerWick5 > body5 * 2.0 && lc5.close > lc5.open) {
                // Длинная нижняя тень при даунтренде + бычья свеча = rejection снизу
                score += 0.32;
                bullSignals++;
                flags.add("WICK_REJ_DN");
            }
        }

        // Дополнительно проверяем на 1m
        if (c1 != null && c1.size() >= 5) {
            com.bot.TradingCore.Candle lc1 = c1.get(c1.size() - 1);
            double body1 = Math.abs(lc1.close - lc1.open) + 1e-10;
            double uWick1 = lc1.high - Math.max(lc1.open, lc1.close);
            double lWick1 = Math.min(lc1.open, lc1.close) - lc1.low;

            if (inUptrend && uWick1 > body1 * 2.5) {
                score += 0.18;
                bearSignals++;
                flags.add("WICK1M_UP");
            }
            if (inDowntrend && lWick1 > body1 * 2.5) {
                score += 0.18;
                bullSignals++;
                flags.add("WICK1M_DN");
            }
        }

        // ═══════════════════════════════════════════════════════
        // 4. RSI MOMENTUM SHIFT
        // RSI7 начинает падать с зоны перекупленности/перепроданности
        // РАНЬШЕ чем цена развернулась.
        // ═══════════════════════════════════════════════════════
        if (inUptrend && rsi7 < 62 && rsi14 > 65) {
            // RSI7 уже упал ниже 62, но RSI14 ещё выше 65 = дивергенция скоростей
            score += 0.25;
            bearSignals++;
            flags.add("RSI_SHIFT_DN");
        }
        if (inDowntrend && rsi7 > 38 && rsi14 < 35) {
            // RSI7 уже поднялся выше 38, но RSI14 ещё ниже 35
            score += 0.25;
            bullSignals++;
            flags.add("RSI_SHIFT_UP");
        }

        // ═══════════════════════════════════════════════════════
        // 5. MICRO STRUCTURE BREAK на 1m
        // На 15m ещё HH/HL (бычий), но на 1m уже появился LH
        // (Lower High) — микро-структура сломалась РАНЬШЕ.
        // ═══════════════════════════════════════════════════════
        if (c1 != null && c1.size() >= 15) {
            int n1 = c1.size();
            // Ищем структуру на последних 12 минутных свечах
            double micro1H = Double.NEGATIVE_INFINITY;
            double micro2H = Double.NEGATIVE_INFINITY;
            double micro1L = Double.POSITIVE_INFINITY;
            double micro2L = Double.POSITIVE_INFINITY;

            // Первая половина (6 свечей назад)
            for (int i = n1 - 12; i < n1 - 6; i++) {
                micro1H = Math.max(micro1H, c1.get(i).high);
                micro1L = Math.min(micro1L, c1.get(i).low);
            }
            // Вторая половина (последние 6 свечей)
            for (int i = n1 - 6; i < n1; i++) {
                micro2H = Math.max(micro2H, c1.get(i).high);
                micro2L = Math.min(micro2L, c1.get(i).low);
            }

            if (inUptrend && micro2H < micro1H * 0.9995) {
                // LH на 1m при аптренде на 15m = ранний медвежий сигнал
                score += 0.26;
                bearSignals++;
                flags.add("MICRO_LH");
            }
            if (inDowntrend && micro2L > micro1L * 1.0005) {
                // HL на 1m при даунтренде на 15m = ранний бычий сигнал
                score += 0.26;
                bullSignals++;
                flags.add("MICRO_HL");
            }
        }

        // ═══════════════════════════════════════════════════════
        // АГРЕГАЦИЯ
        // Нужно минимум 2 сигнала в одном направлении
        // ═══════════════════════════════════════════════════════
        if (bearSignals >= 2 && inUptrend && score >= 0.45) {
            // Ранний разворот вниз (SHORT): тренд был вверх, но слабеет
            return new EarlyReversalResult(true, -1, Math.min(score, 0.85), flags);
        }
        if (bullSignals >= 2 && inDowntrend && score >= 0.45) {
            // Ранний разворот вверх (LONG): тренд был вниз, но слабеет
            return new EarlyReversalResult(true, 1, Math.min(score, 0.85), flags);
        }

        return new EarlyReversalResult(false, 0, 0, flags);
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
    //  LIQUIDITY SWEEP
    // ══════════════════════════════════════════════════════════════

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
    //  [v11.0] STRUCTURAL STOP PLACEMENT
    //  Finds nearest swing low (for LONG) or swing high (for SHORT)
    //  behind current price. SL goes below/above that level + buffer.
    // ══════════════════════════════════════════════════════════════

    private double findStructuralStop(List<com.bot.TradingCore.Candle> c15,
                                      com.bot.TradingCore.Side side,
                                      double price, double atr14) {
        if (c15.size() < 20) return -1; // [v12.0] -1 = use ATR fallback

        double buffer = atr14 * 0.25;

        if (side == com.bot.TradingCore.Side.LONG) {
            // Find nearest swing low below current price
            List<Integer> lows = swingLows(c15, 3);
            double bestStop = -1;
            for (int idx = lows.size() - 1; idx >= 0 && idx >= lows.size() - 4; idx--) {
                double swLow = c15.get(lows.get(idx)).low;
                if (swLow < price && swLow > price * 0.95) {
                    bestStop = swLow - buffer;
                    break;
                }
            }
            // Fallback: recent 8-bar low
            if (bestStop <= 0) {
                double recentLow = Double.MAX_VALUE;
                for (int i = Math.max(0, c15.size() - 8); i < c15.size() - 1; i++) {
                    recentLow = Math.min(recentLow, c15.get(i).low);
                }
                if (recentLow < price && recentLow > price * 0.95) {
                    bestStop = recentLow - buffer;
                }
            }
            // [v12.0] If still nothing found, return -1 to use ATR stop
            return bestStop > 0 ? bestStop : -1;
        } else {
            List<Integer> highs = swingHighs(c15, 3);
            double bestStop = -1;
            for (int idx = highs.size() - 1; idx >= 0 && idx >= highs.size() - 4; idx--) {
                double swHigh = c15.get(highs.get(idx)).high;
                if (swHigh > price && swHigh < price * 1.05) {
                    bestStop = swHigh + buffer;
                    break;
                }
            }
            if (bestStop <= 0) {
                double recentHigh = Double.NEGATIVE_INFINITY;
                for (int i = Math.max(0, c15.size() - 8); i < c15.size() - 1; i++) {
                    recentHigh = Math.max(recentHigh, c15.get(i).high);
                }
                if (recentHigh > price && recentHigh < price * 1.05) {
                    bestStop = recentHigh + buffer;
                }
            }
            return bestStop > 0 ? bestStop : -1;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  MATH PRIMITIVES
    // ══════════════════════════════════════════════════════════════

    /**
     * [v12.0] Wilder's Smoothed ATR — matches TradingView/Binance exactly.
     * Old code used simple SMA of TR — gives 15-20% different values.
     * All ATR-dependent thresholds (stops, impulse, overextension) were miscalibrated.
     */
    public double atr(List<com.bot.TradingCore.Candle> c, int period) {
        if (c.size() < period + 1) return 0;

        // Step 1: SMA seed for first 'period' TRs
        double atrVal = 0;
        int seedStart = c.size() - period * 2;
        if (seedStart < 1) seedStart = 1;
        int seedEnd = Math.min(seedStart + period, c.size());

        for (int i = seedStart; i < seedEnd; i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            atrVal += Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));
        }
        atrVal /= (seedEnd - seedStart);

        // Step 2: Wilder's smoothing for remaining bars
        for (int i = seedEnd; i < c.size(); i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));
            atrVal = (atrVal * (period - 1) + tr) / period;
        }
        return atrVal;
    }

    /**
     * [v12.0] Wilder's ADX — proper smoothed calculation.
     * Old code used simple sum, not Wilder's smoothing.
     * This caused ADX to read 15 where real ADX was 28 → wrong RANGE detection.
     * The bot was entering RANGE trades that were actually trending, and vice versa.
     */
    private double adx(List<com.bot.TradingCore.Candle> c, int period) {
        if (c.size() < period * 2 + 1) return 15; // not enough data

        int startIdx = c.size() - period * 2;
        if (startIdx < 1) startIdx = 1;

        // Step 1: seed +DI, -DI, TR with SMA
        double sumPlusDM = 0, sumMinusDM = 0, sumTR = 0;
        int seedEnd = startIdx + period;
        for (int i = startIdx; i < seedEnd && i < c.size(); i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            double hd = cur.high - prev.high;
            double ld = prev.low - cur.low;
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));
            sumTR += tr;
            if (hd > ld && hd > 0) sumPlusDM += hd;
            if (ld > hd && ld > 0) sumMinusDM += ld;
        }

        double smoothPlusDM = sumPlusDM;
        double smoothMinusDM = sumMinusDM;
        double smoothTR = sumTR;

        // Step 2: Wilder's smoothing for DI lines
        double sumDX = 0;
        int dxCount = 0;
        for (int i = seedEnd; i < c.size(); i++) {
            com.bot.TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            double hd = cur.high - prev.high;
            double ld = prev.low - cur.low;
            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));

            smoothTR = smoothTR - (smoothTR / period) + tr;
            double curPlusDM = (hd > ld && hd > 0) ? hd : 0;
            double curMinusDM = (ld > hd && ld > 0) ? ld : 0;
            smoothPlusDM = smoothPlusDM - (smoothPlusDM / period) + curPlusDM;
            smoothMinusDM = smoothMinusDM - (smoothMinusDM / period) + curMinusDM;

            double plusDI = smoothTR > 0 ? 100 * smoothPlusDM / smoothTR : 0;
            double minusDI = smoothTR > 0 ? 100 * smoothMinusDM / smoothTR : 0;
            double diSum = plusDI + minusDI;
            double dx = diSum > 0 ? 100 * Math.abs(plusDI - minusDI) / diSum : 0;
            sumDX += dx;
            dxCount++;
        }

        return dxCount > 0 ? sumDX / dxCount : 15;
    }

    private double ema(List<com.bot.TradingCore.Candle> c, int p) {
        if (c.size() < p) return last(c).close;
        double k = 2.0 / (p + 1), e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    /**
     * [v24.0 FIX BUG-1] Delegates to TradingCore.rsi() — SINGLE source of truth.
     * Old code had its own seed window (c.size() - period*2) which diverged 3-8 points
     * from TradingCore.rsi() seed (starting at index 1). Clusters and ForecastEngine
     * saw DIFFERENT RSI for identical data → flipped signals, false divergences.
     */
    public double rsi(List<com.bot.TradingCore.Candle> c, int period) {
        return com.bot.TradingCore.rsi(c, period);
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
                ? p <= e21 * 1.003 && p >= e21 * 0.990 && r > 30 && r < 65
                : p >= e21 * 0.997 && p <= e21 * 1.010 && r < 70 && r > 35;
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