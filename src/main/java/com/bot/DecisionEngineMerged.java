package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║          DecisionEngineMerged — GODBOT EDITION                  ║
 * ║  Абсолютно всё лучшее из обоих проектов + новые фичи            ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  ФАКТОРЫ SCORING (26 штук):                                     ║
 * ║  1.  Anti-Lag (4 варианта: 1m body, 2xbody, 5m body, серия)    ║
 * ║  2.  Reverse Exhaustion (7 факторов: RSI1H, MomDiv, VolColl...) ║
 * ║  3.  HTF Bias 1H (EMA50/200)                                    ║
 * ║  4.  HTF Bias 2H (EMA12/26/50 + HH/HL)                         ║
 * ║  5.  HTF конфликт и согласие (1H=2H бонус)                      ║
 * ║  6.  Market Structure (swing HH/HL vs LL/LH — из старого)       ║
 * ║  7.  PumpHunter 15M/5M/1M серия + Squeeze + Breakout            ║
 * ║  8.  Funding Rate абсолютное значение                           ║
 * ║  9.  Funding Rate DELTA (изменение — главная новинка)           ║
 * ║  10. OI Squeeze (OI рост + FR экстрем)                          ║
 * ║  11. Volume Delta (aggTrade WebSocket, нормализованный)          ║
 * ║  12. Pullback к EMA21 + Bullish/Bearish Structure               ║
 * ║  13. Fair Value Gap (Smart Money)                               ║
 * ║  14. Order Block (последний разворот перед импульсом)           ║
 * ║  15. BOS Up / BOS Down (Break of Structure)                     ║
 * ║  16. Liquidity Sweep (охота за стопами → контр-сигнал)          ║
 * ║  17. Old Pump Detector (1m/5m большие свечи + объём)            ║
 * ║  18. Impulse 1m (движение больше 0.55 ATR за 5 баров)           ║
 * ║  19. Compression Breakout (ATR сжатие + пробой)                 ║
 * ║  20. RSI Bullish/Bearish Divergence                             ║
 * ║  21. RSI extreme (не в тренде → штраф)                          ║
 * ║  22. ADX + ADX Falling                                          ║
 * ║  23. EMA50 Overextended (>6% от EMA → штраф)                   ║
 * ║  24. 2H Priority и 2H Veto                                      ║
 * ║  25. Volume Spike (категорные пороги MEME/ALT/TOP)              ║
 * ║  26. VWAP Alignment (сессионный VWAP)                           ║
 * ║  + Trend Exhaustion (8-bar move >3.5%)                          ║
 * ║  + Exhaustion filters LONG и SHORT (тело, объём, RSI, wick)     ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  COOLDOWN: TOP=12m / ALT=10m / MEME=5m                         ║
 * ║  MIN_BARS: 150 свечей минимум на 15m и 1h                       ║
 * ║  Multi-TP: tp1 (1:1, 50%), tp2 (1:2, 30%), tp3 (1:3.2, 20%)   ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */
public final class DecisionEngineMerged {

    // ── Enums ──────────────────────────────────────────────────────
    public enum CoinCategory { TOP, ALT, MEME }
    public enum MarketState  { STRONG_TREND, WEAK_TREND, RANGE }
    public enum HTFBias      { BULL, BEAR, NONE }

    // ── Глобальные константы ───────────────────────────────────────
    private static final int    MIN_BARS        = 150;
    private static final long   COOLDOWN_TOP    = 12 * 60_000L;
    private static final long   COOLDOWN_ALT    = 10 * 60_000L;
    private static final long   COOLDOWN_MEME   = 5  * 60_000L;
    private static final double BASE_CONF       = 52.0;
    private static final int    CALIBRATION_WIN = 100;

    // ── Адаптивный минимальный порог ──────────────────────────────
    private volatile double MIN_CONFIDENCE = BASE_CONF;

    // ── State ─────────────────────────────────────────────────────
    private final Map<String, Long>           cooldownMap     = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>>  recentDirs      = new ConcurrentHashMap<>();
    private final Map<String, Double>         lastSigPrice    = new ConcurrentHashMap<>();
    private final Map<String, FundingOIData>  fundingCache    = new ConcurrentHashMap<>();
    private final Map<String, Deque<CalibRecord>> calibHist   = new ConcurrentHashMap<>();

    // Volume Delta из aggTrade WebSocket (заполняется снаружи)
    private final Map<String, Double> volumeDeltaMap = new ConcurrentHashMap<>();

    private PumpHunter pumpHunter;

    public DecisionEngineMerged() {}

    // ── Setters ───────────────────────────────────────────────────
    public void setPumpHunter(PumpHunter ph)              { this.pumpHunter = ph; }
    public void setVolumeDelta(String sym, double delta)  { volumeDeltaMap.put(sym, delta); }

    // ══════════════════════════════════════════════════════════════
    //  INNER DATA TYPES
    // ══════════════════════════════════════════════════════════════

    /** Данные по Funding Rate и Open Interest. Включают fundingDelta. */
    public static final class FundingOIData {
        public final double fundingRate;
        public final double openInterest;
        public final double oiChange1h;
        public final double oiChange4h;
        public final double prevFundingRate;
        public final double fundingDelta;      // frCurrent - frPrev — ключевая метрика разворота
        public final long   timestamp;

        public FundingOIData(double fr, double oi, double oi1h, double oi4h) {
            this(fr, oi, oi1h, oi4h, fr, 0.0);
        }
        public FundingOIData(double fr, double oi, double oi1h, double oi4h, double prevFr, double delta) {
            this.fundingRate     = fr;
            this.openInterest    = oi;
            this.oiChange1h      = oi1h;
            this.oiChange4h      = oi4h;
            this.prevFundingRate = prevFr;
            this.fundingDelta    = delta;
            this.timestamp       = System.currentTimeMillis();
        }
        public boolean isValid() { return System.currentTimeMillis() - timestamp < 5 * 60_000L; }
    }

    private static final class CalibRecord {
        final double predicted; final boolean correct;
        CalibRecord(double p, boolean c) { predicted = p; correct = c; }
    }

    // ══════════════════════════════════════════════════════════════
    //  TRADE IDEA — Multi-TP + полный Telegram формат
    // ══════════════════════════════════════════════════════════════

    public static final class TradeIdea {
        public final String           symbol;
        public final TradingCore.Side side;
        public final double           price;
        public final double           stop;
        public final double           take;         // = tp3
        public final double           tp1;          // 1:1.0 — 50% позиции, переносим в BE
        public final double           tp2;          // 1:2.0 — 30% позиции
        public final double           tp3;          // 1:3.2 — 20% трейлинг
        public final double           probability;
        public final List<String>     flags;
        public final double           fundingRate;
        public final double           fundingDelta;
        public final double           oiChange;
        public final String           htfBias;
        public final double           rr;           // risk/reward ratio
        public final CoinCategory     category;

        /** Полный конструктор — автоматически вычисляет все TP */
        public TradeIdea(String symbol, TradingCore.Side side,
                         double price, double stop, double take, double rr,
                         double probability, List<String> flags,
                         double fundingRate, double fundingDelta,
                         double oiChange, String htfBias, CoinCategory cat) {
            this.symbol       = symbol;
            this.side         = side;
            this.price        = price;
            this.stop         = stop;
            this.take         = take;
            this.rr           = rr;
            this.probability  = probability;
            this.flags        = flags != null ? Collections.unmodifiableList(flags) : List.of();
            this.fundingRate  = fundingRate;
            this.fundingDelta = fundingDelta;
            this.oiChange     = oiChange;
            this.htfBias      = htfBias;
            this.category     = cat;

            double risk = Math.abs(price - stop);
            boolean long_ = side == TradingCore.Side.LONG;
            this.tp1 = long_ ? price + risk * 1.0 : price - risk * 1.0;
            this.tp2 = long_ ? price + risk * 2.0 : price - risk * 2.0;
            this.tp3 = long_ ? price + risk * 3.2 : price - risk * 3.2;
        }

        /** Упрощённый конструктор (для early-tick сигналов) */
        public TradeIdea(String symbol, TradingCore.Side side,
                         double price, double stop, double take,
                         double probability, List<String> flags) {
            this(symbol, side, price, stop, take, 2.0, probability, flags,
                    0, 0, 0, "NONE", CoinCategory.ALT);
        }

        /** Конструктор совместимости без fundingDelta (для SignalOptimizer) */
        public TradeIdea(String symbol, TradingCore.Side side,
                         double price, double stop, double take,
                         double probability, List<String> flags,
                         double fundingRate, double oiChange, String htfBias) {
            this(symbol, side, price, stop, take, 2.0, probability, flags,
                    fundingRate, 0, oiChange, htfBias, CoinCategory.ALT);
        }

        /**
         * Красивое Telegram сообщение с emoji, Multi-TP, FR, OI, HTF Bias.
         * Формат проверен на реальных сигналах.
         */
        public String toTelegramString() {
            String emoji   = probability >= 83 ? "🔥" : probability >= 74 ? "✅"
                    : probability >= 65 ? "🟡" : "⚪";
            String sideStr = side == TradingCore.Side.LONG ? "📈 LONG" : "📉 SHORT";
            String catStr  = category == CoinCategory.MEME ? "🐸 MEME"
                    : category == CoinCategory.TOP  ? "👑 TOP" : "🔷 ALT";

            double riskPct = Math.abs(price - stop) / price * 100;
            double rp1Pct  = Math.abs(tp1 - price) / price * 100;
            double rp2Pct  = Math.abs(tp2 - price) / price * 100;
            double rp3Pct  = Math.abs(tp3 - price) / price * 100;

            String flagStr = flags.isEmpty() ? "-" : String.join(", ", flags);
            String time    = java.time.ZonedDateTime.now(java.time.ZoneId.of("Europe/Warsaw"))
                    .toLocalTime().withNano(0).toString();

            // Доп-строки
            StringBuilder extra = new StringBuilder();
            if (Math.abs(fundingRate) > 0.0008)
                extra.append(String.format("\n💸 FR: %+.3f%%", fundingRate * 100));
            if (Math.abs(fundingDelta) > 0.0003)
                extra.append(String.format(" Δ%+.3f%%", fundingDelta * 100));
            if (Math.abs(oiChange) > 0.5)
                extra.append(String.format(" | OI: %+.1f%%", oiChange));
            if (!"NONE".equals(htfBias) && !htfBias.isEmpty())
                extra.append(String.format("\n📊 2H Bias: %s", htfBias));
            if (rr > 0)
                extra.append(String.format(" | R/R: 1:%.1f", rr));

            return String.format(
                    "%s *%s* → *%s* %s\n"
                            + "💰 Price:  `%.6f`\n"
                            + "🎯 Prob:   *%.0f%%*\n"
                            + "🛡 SL:     `%.6f`  (%.2f%% риска)\n"
                            + "🟢 TP1:    `%.6f`  (+%.2f%%)  50%% → BE\n"
                            + "🔵 TP2:    `%.6f`  (+%.2f%%)  30%%\n"
                            + "💎 TP3:    `%.6f`  (+%.2f%%)  20%% трейл\n"
                            + "🏷 %s%s\n"
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
        double prevFr  = prev != null ? prev.fundingRate : fr;
        double delta   = fr - prevFr;
        fundingCache.put(sym, new FundingOIData(fr, oi, oi1h, oi4h, prevFr, delta));
    }

    public FundingOIData getFundingOI(String sym) {
        FundingOIData d = fundingCache.get(sym);
        return (d != null && d.isValid()) ? d : null;
    }

    public void recordSignalResult(String sym, double prob, boolean correct) {
        Deque<CalibRecord> h = calibHist.computeIfAbsent(sym, k -> new ArrayDeque<>());
        h.addLast(new CalibRecord(prob, correct));
        while (h.size() > CALIBRATION_WIN) h.removeFirst();
    }

    /** Анализ с 5 таймфреймами (полный) */
    public TradeIdea analyze(String symbol,
                             List<TradingCore.Candle> c1,
                             List<TradingCore.Candle> c5,
                             List<TradingCore.Candle> c15,
                             List<TradingCore.Candle> c1h,
                             List<TradingCore.Candle> c2h,
                             CoinCategory cat) {
        return generate(symbol, c1, c5, c15, c1h, c2h, cat, System.currentTimeMillis());
    }

    /** Анализ без 2H (обратная совместимость) */
    public TradeIdea analyze(String symbol,
                             List<TradingCore.Candle> c1,
                             List<TradingCore.Candle> c5,
                             List<TradingCore.Candle> c15,
                             List<TradingCore.Candle> c1h,
                             CoinCategory cat) {
        return generate(symbol, c1, c5, c15, c1h, null, cat, System.currentTimeMillis());
    }

    // ══════════════════════════════════════════════════════════════
    //  CORE GENERATE — сердце бота
    // ══════════════════════════════════════════════════════════════

    private TradeIdea generate(String symbol,
                               List<TradingCore.Candle> c1,
                               List<TradingCore.Candle> c5,
                               List<TradingCore.Candle> c15,
                               List<TradingCore.Candle> c1h,
                               List<TradingCore.Candle> c2h,
                               CoinCategory cat,
                               long now) {

        // Проверяем минимум данных
        if (!valid(c15) || !valid(c1h)) return null;

        double price     = last(c15).close;
        double atr14     = atr(c15, 14);
        double lastRange = last(c15).high - last(c15).low;

        // Аномальная свеча (spike 4+ ATR) — пропускаем
        if (lastRange > atr14 * 4.5 || atr14 <= 0) return null;
        atr14 = Math.max(atr14, price * 0.0012);

        // ── Контексты ─────────────────────────────────────────────
        MarketState state  = detectState(c15);
        HTFBias     bias1h = detectBias1H(c1h);
        HTFBias     bias2h = (c2h != null && c2h.size() >= 50) ? detectBias2H(c2h) : HTFBias.NONE;

        // Адаптируем порог уверенности по волатильности
        adaptMinConfidence(state, atr14, price);

        // В флэте без ADX → не торгуем (слишком много ложных сигналов)
        if (state == MarketState.RANGE && adx(c15, 14) < 18) return null;

        // ── Скоры и флаги ─────────────────────────────────────────
        double scoreLong  = 0;
        double scoreShort = 0;
        List<String> flags = new ArrayList<>();

        // ════════════════════════════════════════════════════════
        // ФАКТОР 1: ANTI-LAG — ловим движение ДО закрытия свечи
        // ════════════════════════════════════════════════════════
        AntiLagResult antiLag = detectAntiLag(c1, c5, c15);
        if (antiLag != null && antiLag.strength > 0.38) {
            double bonus = antiLag.strength * 1.30;
            if (antiLag.direction > 0) { scoreLong  += bonus; flags.add("ANTI_LAG_UP"); }
            else                       { scoreShort += bonus; flags.add("ANTI_LAG_DN"); }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 2: REVERSE EXHAUSTION — 7 факторов усталости
        // ════════════════════════════════════════════════════════
        ReverseWarning rw = detectReversePattern(c15, c1h, state);
        if (rw != null && rw.confidence > 0.48) {
            flags.add("⚠REV_" + rw.type);
            if ("LONG_EXHAUSTION".equals(rw.type)) {
                scoreLong *= 0.16;
                if (scoreLong < 0.22) return null;
            } else if ("SHORT_EXHAUSTION".equals(rw.type)) {
                scoreShort *= 0.16;
                if (scoreShort < 0.22) return null;
            }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОРЫ 3-5: HTF BIAS 1H + 2H + согласие/конфликт
        // ════════════════════════════════════════════════════════
        if (bias1h == HTFBias.BULL) {
            scoreLong  += 0.55; scoreShort -= 0.20; flags.add("1H_BULL");
        } else if (bias1h == HTFBias.BEAR) {
            scoreShort += 0.55; scoreLong  -= 0.20; flags.add("1H_BEAR");
        }

        if (bias2h == HTFBias.BULL) {
            scoreLong  += 0.50; scoreShort -= 0.25; flags.add("2H_BULL");
        } else if (bias2h == HTFBias.BEAR) {
            scoreShort += 0.50; scoreLong  -= 0.25; flags.add("2H_BEAR");
        }

        // HTF конфликт — оба наказаны
        if ((bias1h == HTFBias.BULL && bias2h == HTFBias.BEAR) ||
                (bias1h == HTFBias.BEAR && bias2h == HTFBias.BULL)) {
            if (scoreLong  < 1.2) { scoreLong  *= 0.50; }
            if (scoreShort < 1.2) { scoreShort *= 0.50; }
            flags.add("HTF_CONFLICT");
        }

        // 1H и 2H согласны — двойной бонус
        if (bias1h == bias2h && bias1h != HTFBias.NONE) {
            if (bias1h == HTFBias.BULL) {
                scoreLong  += 0.45; scoreShort -= 0.22; flags.add("1H2H_BULL");
            } else {
                scoreShort += 0.50; scoreLong  -= 0.30; flags.add("1H2H_BEAR");
            }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 6: MARKET STRUCTURE (HH/HL vs LL/LH) — из старого проекта
        // ════════════════════════════════════════════════════════
        int structure = marketStructure(c15);
        if (structure == 1)  { scoreLong  += 0.60; flags.add("HH_HL"); }
        if (structure == -1) { scoreShort += 0.60; flags.add("LL_LH"); }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 7: PUMPHUNTER — 15M/5M/1M + Squeeze + Breakout
        // ════════════════════════════════════════════════════════
        boolean pumpDetected = false;
        if (pumpHunter != null) {
            PumpHunter.PumpEvent pump = pumpHunter.detectPump(symbol, c1, c5, c15);
            if (pump != null && pump.strength > 0.30) {
                pumpDetected = true;
                double bonus = pump.strength * 0.60 * (pump.isConfirmed ? 1.0 : 0.65);
                if (pump.isBullish()) {
                    scoreLong  += 0.70 + bonus;
                    flags.add("PUMP_UP_" + pct(pump.strength));
                }
                if (pump.isBearish()) {
                    scoreShort += 0.70 + bonus;
                    flags.add("PUMP_DN_" + pct(pump.strength));
                }
                if (pump.isMega()) {
                    if (pump.isBullish()) scoreLong  += 0.40;
                    else                  scoreShort += 0.40;
                    flags.add("MEGA_PUMP");
                }
                // Volume Climax → ещё бонус
                if (pump.flags.contains("VOL_CLIMAX")) {
                    if (pump.isBullish()) scoreLong  += 0.15;
                    else                  scoreShort += 0.15;
                    flags.add("VOL_CLIMAX");
                }
            }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОРЫ 8-10: FUNDING RATE + FR DELTA + OI
        // ════════════════════════════════════════════════════════
        FundingOIData fOI = getFundingOI(symbol);
        double fundingRate = 0, fundingDelta = 0, oiChange = 0;
        boolean hasFR = false;

        if (fOI != null) {
            hasFR       = true;
            fundingRate = fOI.fundingRate;
            fundingDelta = fOI.fundingDelta;
            oiChange    = fOI.oiChange1h;

            // ── FR абсолютное значение ─────────────────────────
            if (fundingRate > 0.0005) {
                // Лонгисты перегреты → SHORT сигнал
                scoreShort += 0.45 + Math.min(fundingRate * 100, 0.45);
                scoreLong  -= 0.25;
                flags.add("FR_HIGH");
            } else if (fundingRate < -0.0005) {
                // Шортисты перегреты → LONG сигнал
                scoreLong  += 0.45 + Math.min(Math.abs(fundingRate) * 100, 0.45);
                scoreShort -= 0.25;
                flags.add("FR_LOW");
            }

            // ── FR DELTA (изменение ставки — главная новинка) ─
            if (fundingDelta > 0.0003) {
                // Резкий рост FR → лонги перегрелись → SHORT reversal
                scoreShort += 0.45; scoreLong -= 0.22;
                flags.add("FR_SPIKE→SHORT");
            } else if (fundingDelta < -0.0003) {
                // Резкое падение FR → шорты закрываются → LONG reversal
                scoreLong  += 0.45; scoreShort -= 0.22;
                flags.add("FR_DROP→LONG");
            }

            // ── OI Squeeze ─────────────────────────────────────
            if (fOI.oiChange1h > 3.5 && fundingRate > 0.0003) {
                scoreShort += 0.35; flags.add("OI_SQUEEZE_LONG");
            }
            if (fOI.oiChange1h > 3.5 && fundingRate < -0.0003) {
                scoreLong  += 0.35; flags.add("OI_SQUEEZE_SHORT");
            }
            if (fOI.oiChange1h < -5.0) {
                // Резкое падение OI — ликвидация позиций с обеих сторон
                scoreLong  *= 0.78;
                scoreShort *= 0.78;
                flags.add("OI_LIQUIDATION");
            }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 11: VOLUME DELTA (aggTrade WebSocket, 1-минутное окно)
        // ════════════════════════════════════════════════════════
        double volDelta = volumeDeltaMap.getOrDefault(symbol, 0.0);
        if (Math.abs(volDelta) > 0.16) {
            double dBonus = Math.abs(volDelta) * 0.50;
            if (volDelta > 0) {
                scoreLong  += dBonus;
                flags.add("ΔVOL_BUY" + pct(Math.abs(volDelta)));
            } else {
                scoreShort += dBonus;
                flags.add("ΔVOL_SELL" + pct(Math.abs(volDelta)));
            }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 12: PULLBACK к EMA21 + Structure
        // ════════════════════════════════════════════════════════
        boolean pullUp   = pullback(c15, true);
        boolean pullDown = pullback(c15, false);
        boolean bullStr  = bullishStructure(c15);
        boolean bearStr  = bearishStructure(c15);

        if (pullUp   && bullStr) { scoreLong  += 1.20; flags.add("PB_LONG"); }
        if (pullDown && bearStr) { scoreShort += 1.20; flags.add("PB_SHORT"); }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 13: FAIR VALUE GAP (Smart Money)
        // ════════════════════════════════════════════════════════
        FVGResult fvg = detectFVG(c15);
        boolean hasFVG = fvg.detected;
        if (fvg.detected) {
            if ( fvg.isBullish && price < fvg.gapHigh && price > fvg.gapLow) {
                scoreLong  += 0.55; flags.add("FVG_BULL");
            }
            if (!fvg.isBullish && price > fvg.gapLow && price < fvg.gapHigh) {
                scoreShort += 0.55; flags.add("FVG_BEAR");
            }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 14: ORDER BLOCK (SMC)
        // ════════════════════════════════════════════════════════
        OrderBlockResult ob = detectOrderBlock(c15);
        boolean hasOB = ob.detected;
        if (ob.detected) {
            if (ob.isBullish  && price <= ob.zone * 1.006) { scoreLong  += 0.60; flags.add("OB_BULL"); }
            if (!ob.isBullish && price >= ob.zone * 0.994) { scoreShort += 0.60; flags.add("OB_BEAR"); }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОРЫ 15-16: BOS + LIQUIDITY SWEEP
        // ════════════════════════════════════════════════════════
        boolean bosUp   = detectBOSUp(c15);
        boolean bosDown = detectBOSDown(c15);
        boolean hasBOS  = bosUp || bosDown;

        if (bosUp)   { scoreLong  += 0.55; flags.add("BOS_UP"); }
        if (bosDown) { scoreShort += 0.55; flags.add("BOS_DOWN"); }

        boolean liqSweep = detectLiquiditySweep(c15);
        if (liqSweep) {
            TradingCore.Candle lc = last(c15);
            double uw = lc.high - Math.max(lc.open, lc.close);
            double lw = Math.min(lc.open, lc.close) - lc.low;
            double bd = Math.abs(lc.close - lc.open) + 1e-10;
            // Длинный верхний фитиль после подъёма → медведи поглощают
            if (uw > bd * 1.7) { scoreShort += 0.65; flags.add("LIQ_SWEEP_S"); }
            // Длинный нижний фитиль после падения → быки поглощают
            if (lw > bd * 1.7) { scoreLong  += 0.65; flags.add("LIQ_SWEEP_L"); }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 17: СТАРЫЙ PUMP DETECTOR (1m/5m большие свечи)
        // ════════════════════════════════════════════════════════
        OldPumpResult oldPump = detectOldPump(c1, c5, cat);
        if (oldPump.detected) {
            if (oldPump.direction > 0) { scoreLong  += oldPump.strength; flags.add("1M_PUMP"); }
            else                       { scoreShort += oldPump.strength; flags.add("1M_DUMP"); }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 18: IMPULSE на 1M
        // ════════════════════════════════════════════════════════
        boolean impulseFlag = false;
        if (c1 != null && c1.size() >= 15) {
            impulseFlag = impulse(c1);
            if (impulseFlag) {
                double atr1    = atr(c1, 14);
                double delta1m = last(c1).close - c1.get(c1.size() - 5).close;
                double impStr  = Math.abs(delta1m) / (atr1 + 1e-9);

                if (delta1m > 0 && impStr > 0.28) {
                    if (bias2h != HTFBias.BEAR) {
                        double add = state == MarketState.STRONG_TREND ? 0.60 : 0.42;
                        scoreLong  += add; flags.add("IMP_UP");
                    } else {
                        scoreLong += 0.15; flags.add("IMP_UP_CONF");
                    }
                }
                if (delta1m < 0 && impStr > 0.28) {
                    if (bias2h != HTFBias.BULL) {
                        double add = state == MarketState.STRONG_TREND ? 0.60 : 0.42;
                        scoreShort += add; flags.add("IMP_DN");
                    } else {
                        scoreShort += 0.15; flags.add("IMP_DN_CONF");
                    }
                }
                // Очень сильный импульс против 2H — перебивает вето
                if (delta1m < 0 && impStr > 0.65 && bias2h == HTFBias.BULL) {
                    scoreShort += 0.50; flags.add("IMP_OVERRIDE");
                }
            }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 19: COMPRESSION BREAKOUT (ATR сжатие + пробой)
        // ════════════════════════════════════════════════════════
        CompressionResult compr = detectCompression(c15, c1);
        if (compr.breakout) {
            if (compr.direction > 0) { scoreLong  += 0.75; flags.add("COMPRESS_UP"); }
            else                     { scoreShort += 0.75; flags.add("COMPRESS_DN"); }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 20: RSI DIVERGENCE
        // ════════════════════════════════════════════════════════
        double rsi14 = rsi(c15, 14);
        double rsi7  = rsi(c15, 7);
        boolean bullDiv = bullDiv(c15);
        boolean bearDiv = bearDiv(c15);

        if (bullDiv) { scoreLong  += 0.65; flags.add("BULL_DIV"); }
        if (bearDiv) { scoreShort += 0.65; flags.add("BEAR_DIV"); }

        // Дивергенция против позиции — жёсткий штраф
        if (bearDiv && scoreLong  > scoreShort) { scoreLong  *= 0.28; flags.add("BEAR_DIV_BLOCK"); }
        if (bullDiv && scoreShort > scoreLong)  { scoreShort *= 0.28; flags.add("BULL_DIV_BLOCK"); }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 21: RSI EXTREME (не в тренде)
        // ════════════════════════════════════════════════════════
        if (state != MarketState.STRONG_TREND) {
            if (rsi14 > 78) { scoreLong  -= 0.28; }
            if (rsi14 < 22) { scoreShort -= 0.28; }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 22: ADX + ADX FALLING
        // ════════════════════════════════════════════════════════
        double adxV    = adx(c15, 14);
        double adxPrev = c15.size() >= 2 ? adx(c15.subList(0, c15.size() - 1), 14) : adxV;
        boolean adxFalling = adxPrev > adxV;

        // Сильный тренд + HTF против → штраф контр-трендовым
        if (adxV > 32) {
            if (bias1h == HTFBias.BULL && scoreShort > scoreLong) scoreShort *= 0.68;
            if (bias1h == HTFBias.BEAR && scoreLong  > scoreShort) scoreLong *= 0.68;
        }

        // ════════════════════════════════════════════════════════
        // EXHAUSTION FILTERS — точечные блоки плохих сетапов
        // ════════════════════════════════════════════════════════
        if (scoreLong > scoreShort) {
            boolean ex = isLongExhausted(c15, c1h, rsi14, rsi7, price);
            if (adxV > 30 && adxFalling) ex = true;
            if (bearDiv)                 { ex = true; flags.add("BEAR_DIV_REJ"); }

            if (ex) {
                if (bearDiv || rsi14 > 74) return null;   // Hard block
                scoreLong *= 0.13;
                flags.add("EXH_LONG");
                if (scoreLong < 0.30) return null;
            }
        }

        if (scoreShort > scoreLong) {
            boolean ex = isShortExhausted(c15, c1h, rsi14, rsi7, price);
            if (adxV > 30 && adxFalling) ex = true;
            if (bullDiv)                 ex = true;

            // Шорт против бычьего HTF без BOS → слабый сетап
            if ((bias1h == HTFBias.BULL || bias2h == HTFBias.BULL) && !bosDown && scoreShort < 0.60) {
                ex = true; flags.add("SHORT_VS_BULL");
            }

            if (ex) {
                scoreShort *= 0.32;
                if (scoreShort < 0.18) return null;
                flags.add("EXH_SHORT");
            }
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 23: EMA50 OVEREXTENDED
        // ════════════════════════════════════════════════════════
        double ema50  = ema(c15, 50);
        double devEma = (price - ema50) / (ema50 + 1e-9);
        if (scoreLong  > scoreShort && devEma >  0.065) { scoreLong  *= 0.48; flags.add("OVEREXT_L"); }
        if (scoreShort > scoreLong  && devEma < -0.065) { scoreShort *= 0.48; flags.add("OVEREXT_S"); }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 24: 2H PRIORITY + 2H VETO
        // ════════════════════════════════════════════════════════
        if (bias2h == HTFBias.BULL && scoreShort > scoreLong) {
            // Короткие позиции против бычьего 2H
            boolean strongLocalBear =
                    (antiLag != null && antiLag.direction < 0 && antiLag.strength > 0.52) ||
                            (oldPump.detected && oldPump.direction < 0 && oldPump.strength > 0.48) ||
                            bosDown || liqSweep;
            scoreShort *= strongLocalBear ? 0.88 : 0.52;
            flags.add(strongLocalBear ? "DYN_SHORT_2H" : "2H_BULL_PRESS");
        }

        if (bias2h == HTFBias.BEAR && scoreLong > scoreShort && adxV > 20) {
            scoreLong *= 0.32;
            flags.add("2H_VETO");
            if (scoreLong < 0.18) return null;
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 25: VOLUME SPIKE (категорные пороги)
        // ════════════════════════════════════════════════════════
        if (volumeSpike(c15, cat)) {
            if (scoreLong  > scoreShort) { scoreLong  += 0.20; }
            else                         { scoreShort += 0.20; }
            flags.add("VOL_SPIKE");
        }

        // ════════════════════════════════════════════════════════
        // ФАКТОР 26: VWAP ALIGNMENT
        // ════════════════════════════════════════════════════════
        int vwapLen = Math.min(50, c15.size());
        double vwapVal = vwap(c15.subList(c15.size() - vwapLen, c15.size()));
        if (price > vwapVal * 1.0008 && scoreLong  > scoreShort) { scoreLong  += 0.22; flags.add("VWAP_BULL"); }
        if (price < vwapVal * 0.9992 && scoreShort > scoreLong)  { scoreShort += 0.22; flags.add("VWAP_BEAR"); }

        // ════════════════════════════════════════════════════════
        // TREND EXHAUSTION (8-bar big move)
        // ════════════════════════════════════════════════════════
        double move8 = (last(c15).close - c15.get(c15.size() - 8).close) / price;
        if (move8 >  0.038 && scoreLong  > scoreShort) { scoreLong  *= 0.62; flags.add("TREND_EXH_L"); }
        if (move8 < -0.038 && scoreShort > scoreLong)  { scoreShort *= 0.62; flags.add("TREND_EXH_S"); }

        // ════════════════════════════════════════════════════════
        // MINIMUM SCORE DIFFERENCE
        // ════════════════════════════════════════════════════════
        double scoreDiff = Math.abs(scoreLong - scoreShort);
        double minDiff = state == MarketState.STRONG_TREND ? 0.16 : 0.20;
        if (scoreDiff < minDiff) return null;

        // ── Динамический порог суммарного скора ─────────────────
        double dynThresh = state == MarketState.STRONG_TREND ? 0.68 : 0.58;
        if (scoreLong < dynThresh && scoreShort < dynThresh) {
            if (!bullDiv && !bearDiv && !oldPump.detected && !hasFR) return null;
        }

        // ── Определяем сторону ────────────────────────────────────
        TradingCore.Side side = scoreLong > scoreShort
                ? TradingCore.Side.LONG
                : TradingCore.Side.SHORT;

        // ── Кулдаун и flip защита ─────────────────────────────────
        if (!cooldownAllowed(symbol, side, cat, now)) return null;
        if (!flipAllowed(symbol, side)) return null;

        // ── Калиброванная уверенность ─────────────────────────────
        double probability = computeCalibratedConfidence(
                symbol, scoreLong, scoreShort, state, cat,
                atr14, price,
                bullDiv, bearDiv,
                pullUp, pullDown,
                impulseFlag, pumpDetected, hasFR, hasFVG, hasOB, hasBOS, liqSweep,
                bias2h, vwapVal
        );

        if (probability < MIN_CONFIDENCE) return null;

        // ── Метка высокой волатильности ───────────────────────────
        if (atr14 / price > 0.0020) flags.add("HIGH_ATR");

        // ── Stop / Take расчёт ────────────────────────────────────
        double riskMult = cat == CoinCategory.MEME ? 1.40 : cat == CoinCategory.ALT ? 1.10 : 0.88;
        double rrRatio  = scoreDiff > 1.2 ? 3.4 : scoreDiff > 0.9 ? 3.0 : scoreDiff > 0.6 ? 2.7 : 2.3;
        double stopDist = Math.max(atr14 * 1.85 * riskMult, price * 0.0018);

        double stopPrice = side == TradingCore.Side.LONG  ? price - stopDist : price + stopDist;
        double takePrice = side == TradingCore.Side.LONG  ? price + stopDist * rrRatio
                : price - stopDist * rrRatio;

        // ── Финальная проверка: цена слишком близка к прошлому сигналу ─
        if (!priceMovedEnough(symbol, price)) return null;
        registerSignal(symbol, side, now);

        return new TradeIdea(symbol, side, price, stopPrice, takePrice, rrRatio,
                probability, flags,
                fundingRate, fundingDelta, oiChange, bias2h.name(), cat);
    }

    // ══════════════════════════════════════════════════════════════
    //  CALIBRATED CONFIDENCE
    // ══════════════════════════════════════════════════════════════

    private double computeCalibratedConfidence(
            String symbol,
            double scoreLong, double scoreShort,
            MarketState state, CoinCategory cat,
            double atr, double price,
            boolean bullDiv, boolean bearDiv,
            boolean pullUp, boolean pullDown,
            boolean impulse, boolean pump,
            boolean hasFR, boolean hasFVG, boolean hasOB, boolean hasBOS, boolean liqSweep,
            HTFBias bias2h, double vwap) {

        double scoreDiff = Math.abs(scoreLong - scoreShort);
        // Нормализуем по максимально возможному скору (~6.5)
        double norm = Math.min(1.0, scoreDiff / 6.5);
        int conf = 0;

        if (bullDiv || bearDiv)         { norm += 0.055; conf++; }
        if (pullUp  || pullDown)        { norm += 0.045; conf++; }
        if (impulse)                    { norm += 0.035; conf++; }
        if (pump)                       { norm += 0.065; conf++; }
        if (hasFVG)                     { norm += 0.055; conf++; }
        if (hasOB)                      { norm += 0.055; conf++; }
        if (hasBOS)                     { norm += 0.045; conf++; }
        if (liqSweep)                   { norm += 0.055; conf++; }
        if (hasFR)                      { norm += 0.040; conf++; }

        if ((bias2h == HTFBias.BULL && scoreLong  > scoreShort) ||
                (bias2h == HTFBias.BEAR && scoreShort > scoreLong)) { norm += 0.065; conf++; }

        // VWAP alignment
        if ((scoreLong > scoreShort && price > vwap * 1.0005) ||
                (scoreShort > scoreLong && price < vwap * 0.9995)) { norm += 0.030; conf++; }

        norm = Math.min(1.0, norm);

        // Больше подтверждений → шире диапазон
        double range = 28 + Math.min(conf * 3.5, 18);
        double prob  = 50 + norm * range;

        // Поправки на MarketState
        if (state == MarketState.STRONG_TREND)      prob += 3.5;
        else if (state == MarketState.WEAK_TREND)   prob += 0.5;
        else if (state == MarketState.RANGE)        prob -= 3.0;

        // Поправки на CoinCategory
        if (cat == CoinCategory.MEME)               prob -= 6.0;
        else if (cat == CoinCategory.ALT)           prob -= 2.5;

        // Историческая калибровка
        Deque<CalibRecord> hist = calibHist.get(symbol);
        if (hist != null && hist.size() >= 25) {
            double acc = historicalAccuracy(hist, prob);
            prob = prob * 0.68 + acc * 0.32;
        }

        return Math.round(clamp(prob, 50, 90));
    }

    private double historicalAccuracy(Deque<CalibRecord> hist, double prob) {
        double sum = 0, cnt = 0;
        for (CalibRecord r : hist) {
            if (Math.abs(r.predicted - prob) < 12) {
                cnt++;
                if (r.correct) sum++;
            }
        }
        return cnt < 6 ? prob : (sum / cnt) * 100;
    }

    // ══════════════════════════════════════════════════════════════
    //  ANTI-LAG — 4 варианта детекции
    // ══════════════════════════════════════════════════════════════

    private static class AntiLagResult {
        final int    direction;  // 1=UP, -1=DOWN
        final double strength;   // 0..1
        AntiLagResult(int d, double s) { direction = d; strength = s; }
    }

    private AntiLagResult detectAntiLag(List<TradingCore.Candle> c1,
                                        List<TradingCore.Candle> c5,
                                        List<TradingCore.Candle> c15) {
        if (c1 == null || c1.size() < 5 || c5 == null || c5.size() < 3) return null;

        int    n1     = c1.size();
        double atr1   = atr(c1, Math.min(14, n1 - 1));
        double avgV1  = c1.subList(Math.max(0, n1 - 15), n1 - 1)
                .stream().mapToDouble(c -> c.volume).average().orElse(1);

        TradingCore.Candle cur  = c1.get(n1 - 1);
        TradingCore.Candle prev = c1.get(n1 - 2);

        double curB  = Math.abs(cur.close - cur.open);
        double prevB = Math.abs(prev.close - prev.open);
        double vRatio = cur.volume / (avgV1 + 1e-9);

        double spd2  = Math.abs(cur.close - c1.get(Math.max(0, n1 - 3)).close);
        double spdP  = Math.abs(c1.get(Math.max(0, n1 - 3)).close - c1.get(Math.max(0, n1 - 5)).close);
        boolean accel = spd2 > spdP * 1.15 && spd2 > atr1 * 0.70;

        // Вариант 1: одна мощная свеча 1M
        if (curB > atr1 * 1.45 && vRatio > 1.35 && accel) {
            int d = cur.close > cur.open ? 1 : -1;
            double s = Math.min(0.88, (curB / atr1 - 1.25) * 0.52);
            if (s > 0.36) return new AntiLagResult(d, s);
        }

        // Вариант 2: две сильные свечи подряд 1M
        if (curB > atr1 * 1.15 && prevB > atr1 * 1.05 && accel) {
            double total = cur.close - prev.open;
            int d = total > 0 ? 1 : -1;
            double s = Math.min(0.78, Math.abs(total) / atr1 * 0.47);
            if (s > 0.36) return new AntiLagResult(d, s);
        }

        // Вариант 3: большая свеча 5M
        int n5 = c5.size();
        TradingCore.Candle c5l = c5.get(n5 - 1);
        double atr5  = atr(c5, Math.min(14, n5 - 1));
        double body5 = Math.abs(c5l.close - c5l.open);
        double avg5v = c5.subList(Math.max(0, n5 - 10), n5 - 1)
                .stream().mapToDouble(c -> c.volume).average().orElse(1);
        if (body5 > atr5 * 1.75 && c5l.volume > avg5v * 1.25) {
            int d = c5l.close > c5l.open ? 1 : -1;
            double s = Math.min(0.82, (body5 / atr5 - 1.55) * 0.42);
            if (s > 0.36) return new AntiLagResult(d, s);
        }

        // Вариант 4: серия 3+ монолитных свечей 1M
        int grn = 0, red = 0;
        double serMove = 0;
        for (int i = Math.max(0, n1 - 4); i < n1; i++) {
            TradingCore.Candle c = c1.get(i);
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
    //  REVERSE EXHAUSTION — 7 факторов
    // ══════════════════════════════════════════════════════════════

    private static class ReverseWarning {
        final String type;
        final double confidence;
        ReverseWarning(String t, double c) { type = t; confidence = c; }
    }

    private ReverseWarning detectReversePattern(List<TradingCore.Candle> c15,
                                                List<TradingCore.Candle> c1h,
                                                MarketState state) {
        if (c15.size() < 8 || c1h.size() < 5) return null;

        double score = 0;
        boolean longExh = false, shortExh = false;

        // 1. RSI экстрем на 1H
        double rsi1h = rsi(c1h, 14);
        if (rsi1h > 75.0) { score += 0.27; longExh  = true; }
        if (rsi1h < 25.0) { score += 0.27; shortExh = true; }

        // 2. Momentum divergence (3 периода)
        double mom  = momentumPct(c15, 5, 0);
        double mom1 = momentumPct(c15, 5, 5);
        double mom2 = momentumPct(c15, 5, 10);
        if (mom < mom1 * 0.63 && mom1 < mom2 * 0.68 && mom1 > 0) { score += 0.48; longExh  = true; }
        if (mom > mom1 * 1.37 && mom1 > mom2 * 1.32 && mom1 < 0) { score += 0.48; shortExh = true; }

        // 3. Volume collapse
        double avgVol = c15.subList(Math.max(0, c15.size() - 15), c15.size() - 3)
                .stream().mapToDouble(c -> c.volume).average().orElse(1);
        if (last(c15).volume < avgVol * 0.52) score += 0.32;

        // 4. Wick rejection
        TradingCore.Candle lc = last(c15);
        double uw = lc.high - Math.max(lc.open, lc.close);
        double lw = Math.min(lc.open, lc.close) - lc.low;
        double bd = Math.abs(lc.close - lc.open) + 1e-10;
        if (uw > bd * 2.4 && lc.close < lc.open) { score += 0.38; longExh  = true; }
        if (lw > bd * 2.4 && lc.close > lc.open) { score += 0.38; shortExh = true; }

        // 5. ADX falling (тренд слабеет)
        if (c15.size() >= 2) {
            double adxC = adx(c15, 14);
            double adxP = adx(c15.subList(0, c15.size() - 1), 14);
            if (adxP > adxC && adxC > 20) score += 0.28;
        }

        // 6. Три свечи с уменьшающимся телом (истощение)
        if (c15.size() >= 3) {
            TradingCore.Candle ca = c15.get(c15.size() - 3);
            TradingCore.Candle cb = c15.get(c15.size() - 2);
            TradingCore.Candle cc = c15.get(c15.size() - 1);
            double ba = Math.abs(ca.close - ca.open);
            double bb = Math.abs(cb.close - cb.open);
            double bc = Math.abs(cc.close - cc.open);
            if (ca.close > ca.open && cb.close > cb.open && cc.close > cc.open
                    && ba > bb * 1.08 && bb > bc * 1.08) {
                score += 0.32; longExh = true;
            }
            if (ca.close < ca.open && cb.close < cb.open && cc.close < cc.open
                    && ba > bb * 1.08 && bb > bc * 1.08) {
                score += 0.32; shortExh = true;
            }
        }

        // 7. Хвост после большой противоположной свечи
        if (c15.size() >= 2) {
            TradingCore.Candle prevC = c15.get(c15.size() - 2);
            if (prevC.close > prevC.open) {
                double prevBd = prevC.close - prevC.open;
                if (prevBd > bd * 2.0 && uw > bd * 1.7) { score += 0.38; longExh = true; }
            }
        }

        if (score < 0.48) return null;

        String type;
        if (longExh && !shortExh)       type = "LONG_EXHAUSTION";
        else if (shortExh && !longExh)  type = "SHORT_EXHAUSTION";
        else                            type = "REVERSAL";

        return new ReverseWarning(type, score);
    }

    private double momentumPct(List<TradingCore.Candle> c, int bars, int offset) {
        if (c.size() < bars + offset + 1) return 0;
        int n = c.size();
        double base = c.get(n - offset - bars - 1).close;
        return (c.get(n - offset - 1).close - base) / (base + 1e-9);
    }

    // ══════════════════════════════════════════════════════════════
    //  MARKET STRUCTURE (из старого проекта — HH/HL vs LL/LH)
    // ══════════════════════════════════════════════════════════════

    public static int marketStructure(List<TradingCore.Candle> c) {
        if (c == null || c.size() < 20) return 0;
        List<Integer> highs = swingHighs(c, 5);
        List<Integer> lows  = swingLows(c, 5);
        if (highs.size() < 2 || lows.size() < 2) return 0;

        double lastHigh = c.get(highs.get(highs.size() - 1)).high;
        double prevHigh = c.get(highs.get(highs.size() - 2)).high;
        double lastLow  = c.get(lows.get(lows.size() - 1)).low;
        double prevLow  = c.get(lows.get(lows.size() - 2)).low;

        if (lastHigh > prevHigh && lastLow > prevLow)  return  1;  // Bullish HH+HL
        if (lastHigh < prevHigh && lastLow < prevLow)  return -1;  // Bearish LL+LH
        return 0;
    }

    public static List<Integer> swingHighs(List<TradingCore.Candle> c, int lr) {
        List<Integer> res = new ArrayList<>();
        for (int i = lr; i < c.size() - lr; i++) {
            double v = c.get(i).high; boolean ok = true;
            for (int l = i - lr; l <= i + lr && ok; l++)
                if (c.get(l).high > v) ok = false;
            if (ok) res.add(i);
        }
        return res;
    }

    public static List<Integer> swingLows(List<TradingCore.Candle> c, int lr) {
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

    private FVGResult detectFVG(List<TradingCore.Candle> c) {
        if (c.size() < 10) return new FVGResult(false, false, 0, 0);
        for (int i = c.size() - 3; i >= c.size() - 9 && i >= 2; i--) {
            TradingCore.Candle c1 = c.get(i - 1), c2 = c.get(i), c3 = c.get(i + 1);
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

    private OrderBlockResult detectOrderBlock(List<TradingCore.Candle> c) {
        if (c.size() < 15) return new OrderBlockResult(false, false, 0);
        double atrL = atr(c, 14);
        for (int i = c.size() - 5; i >= c.size() - 13 && i >= 3; i--) {
            TradingCore.Candle pot = c.get(i);
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

    private boolean detectBOSUp(List<TradingCore.Candle> c) {
        if (c.size() < 8) return false;
        int sz = c.size();
        double localHigh = Double.NEGATIVE_INFINITY;
        for (int i = sz - 7; i < sz - 1; i++) localHigh = Math.max(localHigh, c.get(i).high);
        return last(c).close > localHigh * 1.0004;
    }

    private boolean detectBOSDown(List<TradingCore.Candle> c) {
        if (c.size() < 8) return false;
        int sz = c.size();
        double localLow = Double.POSITIVE_INFINITY;
        for (int i = sz - 7; i < sz - 1; i++) localLow = Math.min(localLow, c.get(i).low);
        return last(c).close < localLow * 0.9996;
    }

    public static boolean detectLiquiditySweep(List<TradingCore.Candle> c) {
        if (c == null || c.size() < 6) return false;
        TradingCore.Candle la = c.get(c.size() - 1);
        TradingCore.Candle pr = c.get(c.size() - 2);
        double uw = la.high - Math.max(la.open, la.close);
        double lw = Math.min(la.open, la.close) - la.low;
        double bd = Math.abs(la.close - la.open) + 1e-10;
        return (uw > bd * 1.75 && la.close < pr.close) ||
                (lw > bd * 1.75 && la.close > pr.close);
    }

    // ══════════════════════════════════════════════════════════════
    //  OLD PUMP DETECTOR (1m/5m)
    // ══════════════════════════════════════════════════════════════

    private static final class OldPumpResult {
        final boolean detected; final int direction; final double strength;
        OldPumpResult(boolean d, int dir, double s) { detected = d; direction = dir; strength = s; }
    }

    private OldPumpResult detectOldPump(List<TradingCore.Candle> c1,
                                        List<TradingCore.Candle> c5,
                                        CoinCategory cat) {
        if (c1 == null || c1.size() < 10 || c5 == null || c5.size() < 6)
            return new OldPumpResult(false, 0, 0);

        double atr1 = atr(c1, Math.min(14, c1.size() - 1));
        TradingCore.Candle l1 = last(c1);
        double cSize = Math.abs(l1.close - l1.open);
        double fRng  = l1.high - l1.low;
        double bRatio = cSize / (fRng + 1e-12);
        boolean bigC = fRng > atr1 * 2.8;
        boolean strB = bRatio > 0.65;

        double avgVol = c1.subList(Math.max(0, c1.size() - 8), c1.size() - 1)
                .stream().mapToDouble(c -> c.volume).average().orElse(l1.volume);
        boolean volSp = l1.volume > avgVol * 1.75;

        int lookback  = Math.min(4, c1.size() - 1);
        double move   = l1.close - c1.get(c1.size() - 1 - lookback).close;
        double movePct = Math.abs(move) / (c1.get(c1.size() - 1 - lookback).close + 1e-9);

        double thr = cat == CoinCategory.MEME ? 0.017 : cat == CoinCategory.ALT ? 0.021 : 0.024;
        if (bigC && strB && volSp && movePct > thr) {
            int dir  = move > 0 ? 1 : -1;
            double str = 0.72 + Math.min(movePct * 10, 0.52);
            return new OldPumpResult(true, dir, str);
        }

        // Серия свечей
        int gC = 0, rC = 0; double totMove = 0;
        for (int i = c1.size() - 1 - lookback; i < c1.size(); i++) {
            TradingCore.Candle c = c1.get(i);
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

    private CompressionResult detectCompression(List<TradingCore.Candle> c15,
                                                List<TradingCore.Candle> c1) {
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
    //  EXHAUSTION CHECKS — LONG
    // ══════════════════════════════════════════════════════════════

    private boolean isLongExhausted(List<TradingCore.Candle> c15,
                                    List<TradingCore.Candle> c1h,
                                    double rsi14, double rsi7, double price) {
        // RSI двойной экстрем
        if (rsi14 > 76 && rsi7 > 79) return true;

        double ema21 = ema(c15, 21);
        // Сильно выше EMA21 + перекуплен
        if ((price - ema21) / ema21 > 0.026 && rsi14 > 68) return true;

        // RSI на 1H перекуплен
        if (c1h.size() >= 8 && rsi(c1h, 14) > 78) return true;

        // Три уменьшающихся тела подряд + падение объёма
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

        // Верхний фитиль больше тела при медвежьей свече
        TradingCore.Candle lc = last(c15);
        double uw = lc.high - Math.max(lc.open, lc.close);
        double bd = Math.abs(lc.close - lc.open) + 1e-10;
        if (uw > bd * 1.6 && lc.close < lc.open) return true;

        return false;
    }

    // ══════════════════════════════════════════════════════════════
    //  EXHAUSTION CHECKS — SHORT
    // ══════════════════════════════════════════════════════════════

    private boolean isShortExhausted(List<TradingCore.Candle> c15,
                                     List<TradingCore.Candle> c1h,
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

        TradingCore.Candle lc = last(c15);
        double lw = Math.min(lc.open, lc.close) - lc.low;
        double bd = Math.abs(lc.close - lc.open) + 1e-10;
        if (lw > bd * 1.6 && lc.close > lc.open) return true;

        return false;
    }

    // ══════════════════════════════════════════════════════════════
    //  MARKET STATE + HTF BIAS
    // ══════════════════════════════════════════════════════════════

    private MarketState detectState(List<TradingCore.Candle> c) {
        if (c.size() < 55) return MarketState.WEAK_TREND;
        double ema20 = ema(c, 20);
        double ema50 = ema(c, 50);
        int    n     = c.size();
        // Slope через 10 баров
        double slope = (ema20 - ema(c.subList(0, Math.max(1, n - 10)), 20)) / (c.get(0).close + 1e-9);
        double vol   = atr(c, 14) / (c.get(n - 1).close + 1e-9);

        if (Math.abs(slope) < 0.0005 || vol < 0.0015) return MarketState.RANGE;
        if ((ema20 > ema50 && slope > 0) || (ema20 < ema50 && slope < 0))
            return MarketState.STRONG_TREND;
        return MarketState.WEAK_TREND;
    }

    private HTFBias detectBias1H(List<TradingCore.Candle> c) {
        if (!valid(c)) return HTFBias.NONE;
        double e50  = ema(c, 50);
        double e200 = ema(c, 200);
        if (e50 > e200 * 1.002) return HTFBias.BULL;
        if (e50 < e200 * 0.998) return HTFBias.BEAR;
        return HTFBias.NONE;
    }

    private HTFBias detectBias2H(List<TradingCore.Candle> c) {
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

    private boolean checkHH_HL(List<TradingCore.Candle> c) {
        if (c.size() < 15) return false;
        int n = c.size();
        double h1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.high).max().orElse(0);
        double h2 = c.subList(n-8, n).stream().mapToDouble(x->x.high).max().orElse(0);
        double l1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.low).min().orElse(0);
        double l2 = c.subList(n-8, n).stream().mapToDouble(x->x.low).min().orElse(0);
        return h2 > h1 && l2 > l1;
    }

    private boolean checkLL_LH(List<TradingCore.Candle> c) {
        if (c.size() < 15) return false;
        int n = c.size();
        double h1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.high).max().orElse(0);
        double h2 = c.subList(n-8, n).stream().mapToDouble(x->x.high).max().orElse(0);
        double l1 = c.subList(n-15,n-8).stream().mapToDouble(x->x.low).min().orElse(0);
        double l2 = c.subList(n-8, n).stream().mapToDouble(x->x.low).min().orElse(0);
        return h2 < h1 && l2 < l1;
    }

    private void adaptMinConfidence(MarketState state, double atr, double price) {
        double vol  = atr / (price + 1e-9);
        double base = BASE_CONF;
        if (state == MarketState.STRONG_TREND) base -= 2.0;
        else if (state == MarketState.RANGE)   base += 2.0;
        if (vol > 0.022)       base += 2.5;
        else if (vol < 0.005)  base -= 1.0;
        MIN_CONFIDENCE = clamp(base, 47, 62);
    }

    // ══════════════════════════════════════════════════════════════
    //  MATH PRIMITIVES
    // ══════════════════════════════════════════════════════════════

    public double atr(List<TradingCore.Candle> c, int n) {
        int p = Math.min(n, c.size() - 1);
        if (p <= 0) return 0;
        double sum = 0;
        for (int i = c.size() - p; i < c.size(); i++) {
            TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
            sum += Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low  - prev.close)));
        }
        return sum / p;
    }

    private double adx(List<TradingCore.Candle> c, int n) {
        if (c.size() < n + 1) return 15;
        double trS = 0, plusDM = 0, minusDM = 0;
        for (int i = c.size() - n; i < c.size(); i++) {
            TradingCore.Candle cur = c.get(i), prev = c.get(i - 1);
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

    private double ema(List<TradingCore.Candle> c, int p) {
        if (c.size() < p) return last(c).close;
        double k = 2.0 / (p + 1), e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    public double rsi(List<TradingCore.Candle> c, int period) {
        if (c.size() < period + 1) return 50.0;
        double gain = 0, loss = 0;
        for (int i = c.size() - period; i < c.size(); i++) {
            double ch = c.get(i).close - c.get(i - 1).close;
            if (ch > 0) gain += ch; else loss -= ch;
        }
        double rs = loss == 0 ? 100 : gain / loss;
        return 100 - (100 / (1 + rs));
    }

    private boolean bullDiv(List<TradingCore.Candle> c) {
        if (c.size() < 25) return false;
        int i1 = c.size() - 8, i2 = c.size() - 1;
        return c.get(i2).low < c.get(i1).low * 0.998 &&
                rsi(c, 14) > rsi(c.subList(0, i1 + 1), 14) + 3;
    }

    private boolean bearDiv(List<TradingCore.Candle> c) {
        if (c.size() < 25) return false;
        int i1 = c.size() - 8, i2 = c.size() - 1;
        return c.get(i2).high > c.get(i1).high * 1.002 &&
                rsi(c, 14) < rsi(c.subList(0, i1 + 1), 14) - 3;
    }

    public boolean impulse(List<TradingCore.Candle> c) {
        if (c == null || c.size() < 15) return false;
        return Math.abs(last(c).close - c.get(c.size() - 5).close) > atr(c, 14) * 0.55;
    }

    public boolean volumeSpike(List<TradingCore.Candle> c, CoinCategory cat) {
        if (c.size() < 10) return false;
        double avg = c.subList(c.size() - 10, c.size() - 1)
                .stream().mapToDouble(cd -> cd.volume).average().orElse(1);
        double thr = cat == CoinCategory.MEME ? 1.25 : cat == CoinCategory.ALT ? 1.20 : 1.15;
        return last(c).volume / avg > thr;
    }

    private boolean pullback(List<TradingCore.Candle> c, boolean bull) {
        double e21 = ema(c, 21), p = last(c).close, r = rsi(c, 14);
        return bull
                ? p <= e21 * 1.0012 && p >= e21 * 0.993 && r > 37 && r < 58
                : p >= e21 * 0.9988 && p <= e21 * 1.007 && r < 63 && r > 42;
    }

    private boolean bullishStructure(List<TradingCore.Candle> c) {
        if (c.size() < 12) return false;
        return c.get(c.size()-4).high > c.get(c.size()-8).high &&
                c.get(c.size()-4).low  > c.get(c.size()-8).low;
    }

    private boolean bearishStructure(List<TradingCore.Candle> c) {
        if (c.size() < 12) return false;
        return c.get(c.size()-4).high < c.get(c.size()-8).high &&
                c.get(c.size()-4).low  < c.get(c.size()-8).low;
    }

    private double vwap(List<TradingCore.Candle> c) {
        double pv = 0, vol = 0;
        for (TradingCore.Candle x : c) {
            double tp = (x.high + x.low + x.close) / 3.0;
            pv += tp * x.volume; vol += x.volume;
        }
        return vol == 0 ? last(c).close : pv / vol;
    }

    // ── Cooldown / Flip ──────────────────────────────────────────

    private boolean cooldownAllowed(String sym, TradingCore.Side side, CoinCategory cat, long now) {
        String key = sym + "_" + side;
        long base  = cat == CoinCategory.TOP  ? COOLDOWN_TOP :
                cat == CoinCategory.ALT  ? COOLDOWN_ALT : COOLDOWN_MEME;
        Long last  = cooldownMap.get(key);
        if (last != null && now - last < base) return false;
        cooldownMap.put(key, now);
        return true;
    }

    private boolean flipAllowed(String sym, TradingCore.Side newSide) {
        Deque<String> h = recentDirs.computeIfAbsent(sym, k -> new ArrayDeque<>());
        if (h.size() < 2) return true;
        Iterator<String> it = h.descendingIterator();
        String last = it.next(), prev = it.next();
        return !(!last.equals(newSide.name()) && prev.equals(newSide.name()));
    }

    private void registerSignal(String sym, TradingCore.Side side, long now) {
        cooldownMap.put(sym + "_" + side, now);
        Deque<String> h = recentDirs.computeIfAbsent(sym, k -> new ArrayDeque<>());
        h.addLast(side.name());
        if (h.size() > 3) h.removeFirst();
    }

    private boolean priceMovedEnough(String sym, double price) {
        Double last = lastSigPrice.get(sym);
        if (last == null) { lastSigPrice.put(sym, price); return true; }
        if (Math.abs(price - last) / last < 0.0020) return false;
        lastSigPrice.put(sym, price);
        return true;
    }

    // ── Utility ─────────────────────────────────────────────────
    private TradingCore.Candle last(List<TradingCore.Candle> c) { return c.get(c.size() - 1); }
    private boolean valid(List<?> c)  { return c != null && c.size() >= MIN_BARS; }
    private double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
    private static String pct(double v) { return String.format("%.0f", v * 100); }
}
