package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║       GlobalImpulseController — GODBOT PRO EDITION v7.0                ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ v7.0:                                           ║
 * ║                                                                          ║
 * ║  [FIX Дыра 1] ArrayDeque → ConcurrentLinkedDeque ВЕЗДЕ                  ║
 * ║    Проблема: ArrayDeque не потокобезопасна. WS поток пишет тики,         ║
 * ║    runCycle читает — внутренние указатели разрушаются → бесконечный     ║
 * ║    цикл в JVM → поток «умирает» → бот-зомби без ошибок в логах.       ║
 * ║    Теперь: ConcurrentLinkedDeque с bounded trimming.                     ║
 * ║                                                                          ║
 * ║  КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ v6.0:                                           ║
 * ║                                                                          ║
 * ║  [FIX-LATE-SHORT] Ранняя детекция краша BTC до подтверждения на 15m     ║
 * ║    Проблема: сигнал SHORT давался когда монета уже упала -5%+.           ║
 * ║    Решение: BTC velocity gate + momentum acceleration = SHORT бустер    ║
 * ║    на РАННЕЙ стадии падения, ДО того как осциллятор показал истощение.  ║
 * ║                                                                          ║
 * ║  [FIX-CASCADE] Cascade Dump Protection v2.0                             ║
 * ║    Три уровня:                                                           ║
 * ║    1. WATCH (velocity > 0.003): снижаем вес LONG на 40%                 ║
 * ║    2. DANGER (velocity > 0.007 + 2+ bear bars): вес LONG = 0.15         ║
 * ║    3. CRASH (velocity > 0.012 + 3+ bear bars): ВЕТО на все LONG         ║
 * ║    SHORT при CRASH: бустер 1.5× (рынок сам ведёт цену вниз)            ║
 * ║                                                                          ║
 * ║  [FIX-PANIC] PanicManager — встроен в GIC как inner class               ║
 * ║    Триггеры паники:                                                      ║
 * ║    · BTC -3% за 3 свечи (15m) = немедленный краш-режим                  ║
 * ║    · BTC volatility expansion > 3.5× median = EXTREME режим             ║
 * ║    · 4+ consecutive bear bars + acceleration > 0.015 = PANIC            ║
 * ║    При панике: LONG вес = 0, SHORT вес = 1.6×, уведомление              ║
 * ║                                                                          ║
 * ║  [FIX-SECTOR] Sector Contagion Guard                                    ║
 * ║    Если сектор упал > 4% за последние 8 свечей — лонги                  ║
 * ║    в этом секторе блокируются (weight = 0)                               ║
 * ║                                                                          ║
 * ║  [FIX-RS] Relative Strength Trap Protection                             ║
 * ║    RS > 0.80 при CRASH = ЛОВУШКА catch-up dump                          ║
 * ║    Логика перевёрнута: высокий RS при сильном падении BTC                ║
 * ║    УВЕЛИЧИВАЕТ риск каскада (монета ещё не капитулировала)              ║
 * ║                                                                          ║
 * ║  [FIX-TREND] BTC Momentum Acceleration                                  ║
 * ║    Новое поле: btcMomentumAccel — вторая производная движения BTC.      ║
 * ║    Acceleration > 0 при падении = падение ускоряется = SHORT усилен.   ║
 * ║    Используется в DecisionEngineMerged для boost SHORT scorе.           ║
 * ║                                                                          ║
 * ║  [FIX-MULTI-TF] Мульти-таймфреймовый режим                             ║
 * ║    GIC теперь принимает 5m свечи BTC для более быстрого обнаружения     ║
 * ║    начала краша (15m запаздывает на 1-2 свечи = 15-30 минут)           ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public final class GlobalImpulseController {

    // ══════════════════════════════════════════════════════════════
    //  CONFIGURATION
    // ══════════════════════════════════════════════════════════════

    private final int VOL_LOOKBACK;
    private final int BODY_LOOKBACK;

    // Velocity thresholds — скорость падения BTC
    // Velocity = (|move_current| - |move_prev|) / price — ускорение движения
    private static final double VELOCITY_WATCH   = 0.0025;  // Наблюдаем
    private static final double VELOCITY_DANGER  = 0.0060;  // Опасно
    private static final double VELOCITY_CRASH   = 0.0110;  // Краш

    // BTC % drop thresholds для панического режима
    private static final double PANIC_DROP_3BAR  = 0.030;   // -3% за 3 свечи = паника
    private static final double PANIC_DROP_5BAR  = 0.022;   // -2.2% за 5 свечей

    // Sector contagion: если сектор упал больше этого % за 8 свечей → вето лонгов
    private static final double SECTOR_CONTAGION_DROP = 0.040;  // -4%

    // ATR history для режима волатильности
    private final Deque<Double> btcAtrHistory = new ConcurrentLinkedDeque<>();
    private static final int ATR_HISTORY_SIZE = 96; // 96 × 15m = 24h

    // BTC move history для каскадной детекции
    private final Deque<Double> btcMoveHistory = new ConcurrentLinkedDeque<>();
    private static final int BTC_MOVE_WINDOW = 10;

    // BTC 5m данные для ранней детекции (обновляются отдельно)
    private volatile double btcFastMomentum   = 0.0; // из 5m свечей
    private volatile long   lastFastUpdateMs  = 0;
    private static final long FAST_DATA_STALE = 3 * 60_000L; // 3 минуты

    // Consecutive bear/bull bars
    private volatile int btcConsecutiveBearBars = 0;
    private volatile int btcConsecutiveBullBars = 0;

    // Velocity и acceleration
    private volatile double btcDropVelocity      = 0.0;
    private volatile double btcMomentumAccel     = 0.0; // [NEW] вторая производная
    private volatile double btcCrashScore        = 0.0; // [NEW] агрегированный crash score [0..1]

    // Panic state
    private final AtomicBoolean panicMode = new AtomicBoolean(false);
    private volatile long panicStartMs = 0;
    private static final long PANIC_COOLDOWN_MS = 30 * 60_000L; // 30 минут до выхода из паники

    // Panic callback
    private volatile java.util.function.Consumer<String> panicCallback = null;

    // ══════════════════════════════════════════════════════════════
    //  ENUMS
    // ══════════════════════════════════════════════════════════════

    public enum GlobalRegime {
        NEUTRAL,
        BTC_IMPULSE_UP,
        BTC_IMPULSE_DOWN,
        BTC_STRONG_UP,
        BTC_STRONG_DOWN,
        BTC_CRASH,        // [NEW] Отдельный режим для быстрого краша
        BTC_PANIC         // [NEW] Паника — все лонги заблокированы
    }

    public enum VolatilityRegime {
        LOW,
        NORMAL,
        HIGH,
        EXTREME
    }

    // Уровень каскадного риска
    public enum CascadeLevel {
        NONE,    // Всё в порядке
        WATCH,   // Наблюдаем — небольшой штраф лонгам
        DANGER,  // Опасно — сильный штраф лонгам
        CRASH,   // Краш — вето на лонги
        PANIC    // Паника — вето на всё кроме шортов
    }

    // ══════════════════════════════════════════════════════════════
    //  GlobalContext
    // ══════════════════════════════════════════════════════════════

    public static final class GlobalContext {
        public final GlobalRegime     regime;
        public final double           impulseStrength;
        public final double           volatilityExpansion;
        public final boolean          strongPressure;
        public final boolean          onlyLong;
        public final boolean          onlyShort;
        public final double           btcTrend;
        public final VolatilityRegime volRegime;
        public final double           confidenceAdjustment;

        // v5.0 fields
        public final double           btcDropVelocity;
        public final int              btcConsecutiveBearBars;

        // v6.0 NEW fields
        public final double           btcMomentumAccel;   // > 0 = падение ускоряется
        public final double           btcCrashScore;      // [0..1] агрегированный краш риск
        public final CascadeLevel     cascadeLevel;       // текущий уровень каскада
        public final boolean          panicMode;          // true = паника активна
        public final double           shortBoost;         // бустер для SHORT сигналов при краше

        public GlobalContext(GlobalRegime regime, double impulseStrength,
                             double volatilityExpansion, boolean strongPressure,
                             boolean onlyLong, boolean onlyShort, double btcTrend,
                             VolatilityRegime volRegime, double confidenceAdjustment,
                             double btcDropVelocity, int btcConsecutiveBearBars,
                             double btcMomentumAccel, double btcCrashScore,
                             CascadeLevel cascadeLevel, boolean panicMode,
                             double shortBoost) {
            this.regime               = regime;
            this.impulseStrength      = impulseStrength;
            this.volatilityExpansion  = volatilityExpansion;
            this.strongPressure       = strongPressure;
            this.onlyLong             = onlyLong;
            this.onlyShort            = onlyShort;
            this.btcTrend             = btcTrend;
            this.volRegime            = volRegime;
            this.confidenceAdjustment = confidenceAdjustment;
            this.btcDropVelocity      = btcDropVelocity;
            this.btcConsecutiveBearBars = btcConsecutiveBearBars;
            this.btcMomentumAccel     = btcMomentumAccel;
            this.btcCrashScore        = btcCrashScore;
            this.cascadeLevel         = cascadeLevel;
            this.panicMode            = panicMode;
            this.shortBoost           = shortBoost;
        }

        /** Обратная совместимость */
        public GlobalContext(GlobalRegime regime, double impulseStrength,
                             double volatilityExpansion, boolean strongPressure,
                             boolean onlyLong, boolean onlyShort, double btcTrend,
                             VolatilityRegime volRegime, double confidenceAdjustment,
                             double btcDropVelocity, int btcConsecutiveBearBars) {
            this(regime, impulseStrength, volatilityExpansion, strongPressure,
                    onlyLong, onlyShort, btcTrend, volRegime, confidenceAdjustment,
                    btcDropVelocity, btcConsecutiveBearBars,
                    0.0, 0.0, CascadeLevel.NONE, false, 1.0);
        }

        /** Минимальный конструктор */
        public GlobalContext(GlobalRegime regime, double impulseStrength,
                             double volatilityExpansion, boolean strongPressure,
                             boolean onlyLong, boolean onlyShort, double btcTrend) {
            this(regime, impulseStrength, volatilityExpansion, strongPressure,
                    onlyLong, onlyShort, btcTrend, VolatilityRegime.NORMAL, 0.0, 0.0, 0);
        }

        /** Есть ли активная защита от краша */
        public boolean isCrashProtectionActive() {
            return cascadeLevel == CascadeLevel.CRASH
                    || cascadeLevel == CascadeLevel.PANIC
                    || panicMode
                    || regime == GlobalRegime.BTC_CRASH
                    || regime == GlobalRegime.BTC_PANIC;
        }

        /** Разрешены ли LONG позиции в текущем режиме */
        public boolean isLongAllowed() {
            if (panicMode || regime == GlobalRegime.BTC_PANIC) return false;
            if (cascadeLevel == CascadeLevel.CRASH || cascadeLevel == CascadeLevel.PANIC) return false;
            return true;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  SectorContext
    // ══════════════════════════════════════════════════════════════

    public static final class SectorContext {
        public final String  sector;
        public final double  bias;
        public final double  momentum;
        public final double  strength;
        public final boolean leading;
        public final double  drop8bars;   // [NEW] % изменение за 8 свечей

        public SectorContext(String sector, double bias, double momentum,
                             double strength, boolean leading, double drop8bars) {
            this.sector    = sector;
            this.bias      = bias;
            this.momentum  = momentum;
            this.strength  = strength;
            this.leading   = leading;
            this.drop8bars = drop8bars;
        }

        /** Устаревший конструктор — обратная совместимость */
        public SectorContext(String sector, double bias, double momentum,
                             double strength, boolean leading) {
            this(sector, bias, momentum, strength, leading, 0.0);
        }

        /** True если сектор заражён (упал слишком сильно) */
        public boolean isContaminated() {
            return drop8bars < -SECTOR_CONTAGION_DROP;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  STATE
    // ══════════════════════════════════════════════════════════════

    private volatile GlobalContext currentContext = new GlobalContext(
            GlobalRegime.NEUTRAL, 0.0, 1.0, false, false, false, 0.0,
            VolatilityRegime.NORMAL, 0.0, 0.0, 0,
            0.0, 0.0, CascadeLevel.NONE, false, 1.0
    );

    private final Map<String, SectorContext>  sectorMap      = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>  sectorBiasHist = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>  sectorPriceHist= new ConcurrentHashMap<>(); // для drop8bars

    // Multi-window BTC return history для RS calculation
    private final Deque<Double> btcReturnHistory5  = new ConcurrentLinkedDeque<>();
    private final Deque<Double> btcReturnHistory10 = new ConcurrentLinkedDeque<>();
    private final Deque<Double> btcReturnHistory20 = new ConcurrentLinkedDeque<>();

    // История crash scores для обнаружения нарастания паники
    private final Deque<Double> crashScoreHistory = new ConcurrentLinkedDeque<>();
    private static final int CRASH_SCORE_WINDOW = 5;

    // ══════════════════════════════════════════════════════════════
    //  CONSTRUCTORS
    // ══════════════════════════════════════════════════════════════

    public GlobalImpulseController() {
        this.VOL_LOOKBACK  = 20;
        this.BODY_LOOKBACK = 10;
    }

    public GlobalImpulseController(int volLookback, int bodyLookback) {
        this.VOL_LOOKBACK  = volLookback;
        this.BODY_LOOKBACK = bodyLookback;
    }

    public void setPanicCallback(java.util.function.Consumer<String> cb) {
        this.panicCallback = cb;
    }

    // ══════════════════════════════════════════════════════════════
    //  UPDATE — основной метод, вызывается с 15m BTC свечами
    // ══════════════════════════════════════════════════════════════

    public void update(List<com.bot.TradingCore.Candle> btcCandles) {
        if (btcCandles == null || btcCandles.size() < 30) return;

        int n = btcCandles.size();
        com.bot.TradingCore.Candle last  = btcCandles.get(n - 1);
        com.bot.TradingCore.Candle prev1 = btcCandles.get(n - 2);
        com.bot.TradingCore.Candle prev2 = btcCandles.get(n - 3);
        com.bot.TradingCore.Candle prev3 = n > 3 ? btcCandles.get(n - 4) : prev2;
        com.bot.TradingCore.Candle prev4 = n > 4 ? btcCandles.get(n - 5) : prev3;

        double price     = last.close;
        double atr14     = atr(btcCandles, 14);
        double avgVol    = avgVolume(btcCandles, VOL_LOOKBACK);
        double volRatio  = last.volume / (avgVol + 1e-9);

        // ── Движения разных горизонтов ──────────────────────────
        double move1  = (last.close  - prev1.close) / (prev1.close + 1e-9);
        double move3  = (last.close  - prev3.close) / (prev3.close + 1e-9);
        double move5  = n > 5
                ? (last.close - btcCandles.get(n - 6).close) / (btcCandles.get(n - 6).close + 1e-9)
                : move3;

        // [NEW] Для ранней детекции краша — сравниваем скорость движения
        double prevMove3 = n > 6
                ? (prev3.close - btcCandles.get(n - 7).close) / (btcCandles.get(n - 7).close + 1e-9)
                : move3;

        // ── Momentum acceleration (вторая производная) ──────────
        // > 0 при падении = падение ускоряется (ОПАСНО для лонгов)
        btcMomentumAccel = move3 < 0
                ? Math.max(0, Math.abs(move3) - Math.abs(prevMove3))
                : move3 > 0
                ? Math.max(0, Math.abs(move3) - Math.abs(prevMove3))
                : 0;

        // ── BTC move history ─────────────────────────────────────
        btcMoveHistory.addLast(move1);
        if (btcMoveHistory.size() > BTC_MOVE_WINDOW) btcMoveHistory.removeFirst();

        // ── Consecutive bear/bull bars ───────────────────────────
        if (last.close < last.open) {
            btcConsecutiveBearBars = Math.min(btcConsecutiveBearBars + 1, 12);
            btcConsecutiveBullBars = 0;
        } else if (last.close > last.open) {
            btcConsecutiveBullBars = Math.min(btcConsecutiveBullBars + 1, 12);
            btcConsecutiveBearBars = 0;
        }

        // ── Drop Velocity ─────────────────────────────────────────
        // Насколько БЫСТРО ускоряется падение BTC
        if (move3 < 0) {
            btcDropVelocity = Math.max(0.0, Math.abs(move3) - Math.abs(prevMove3));
        } else {
            // При росте BTC — velocity постепенно угасает
            btcDropVelocity = Math.max(0.0, btcDropVelocity * 0.5);
        }

        // ── Crash Score [0..1] — агрегированная оценка краша ─────
        double crashScore = computeCrashScore(move3, move5, atr14, price);
        crashScoreHistory.addLast(crashScore);
        if (crashScoreHistory.size() > CRASH_SCORE_WINDOW) crashScoreHistory.removeFirst();
        // Берём максимум из последних N значений (sticky — краш не исчезает мгновенно)
        double stickycrashScore = crashScoreHistory.stream().mapToDouble(Double::doubleValue).max().orElse(0);
        btcCrashScore = stickycrashScore;

        // ── Cascade Level ─────────────────────────────────────────
        CascadeLevel cascadeLevel = determineCascadeLevel(btcCrashScore, move3, move5);

        // ── Panic Mode ───────────────────────────────────────────
        boolean currentlyInPanic = panicMode.get();
        boolean shouldPanic = shouldEnterPanic(move3, move5, btcCrashScore, cascadeLevel);

        if (shouldPanic && !currentlyInPanic) {
            enterPanicMode(move3, move5, atr14, price);
        } else if (currentlyInPanic) {
            // Выходим из паники только через PANIC_COOLDOWN_MS и только при улучшении
            long panicAge = System.currentTimeMillis() - panicStartMs;
            if (panicAge > PANIC_COOLDOWN_MS && move3 >= 0 && move5 >= -0.005) {
                exitPanicMode();
            }
        }

        // ── Тело свечей ──────────────────────────────────────────
        double bodyPct   = avgBodyPct(btcCandles, BODY_LOOKBACK);
        double curBody   = Math.abs(last.close - last.open) / (last.close + 1e-9);
        double bodyRatio = curBody / (bodyPct + 1e-9);

        // ── EMA тренд ─────────────────────────────────────────────
        double ema20    = ema(btcCandles, 20);
        double ema50    = ema(btcCandles, 50);
        double btcTrend = (ema20 - ema50) / (ema50 + 1e-9) * 100;

        // ── Сила импульса ─────────────────────────────────────────
        double rawStrength = Math.min(1.0,
                (Math.abs(move3) / 0.012) * 0.45 +
                        (Math.min(volRatio, 4.0) / 4.0) * 0.35 +
                        (Math.min(bodyRatio, 3.0) / 3.0) * 0.20
        );

        // ── ATR история ───────────────────────────────────────────
        btcAtrHistory.addLast(atr14 / price);
        if (btcAtrHistory.size() > ATR_HISTORY_SIZE) btcAtrHistory.removeFirst();

        VolatilityRegime volRegime = calcVolatilityRegime(atr14 / price);
        double confAdj = switch (volRegime) {
            case LOW     -> -1.5;
            case NORMAL  ->  0.0;
            case HIGH    -> +2.5;
            case EXTREME -> +6.0;
        };

        double volExpansion = atr14 / (atr(btcCandles.subList(Math.max(0, n - 50), n - 10), 14) + 1e-9);

        // ── Режим ─────────────────────────────────────────────────
        GlobalRegime regime;
        if (panicMode.get()) {
            regime = GlobalRegime.BTC_PANIC;
        } else if (cascadeLevel == CascadeLevel.CRASH) {
            regime = GlobalRegime.BTC_CRASH;
        } else if (move3 > 0.010 && rawStrength > 0.65) {
            regime = GlobalRegime.BTC_STRONG_UP;
        } else if (move3 < -0.010 && rawStrength > 0.65) {
            regime = GlobalRegime.BTC_STRONG_DOWN;
        } else if (move3 < -0.005 && btcCrashScore > 0.50) {
            regime = GlobalRegime.BTC_STRONG_DOWN; // Высокий crash score даже при небольшом движении
        } else if (move3 > 0.004) {
            regime = GlobalRegime.BTC_IMPULSE_UP;
        } else if (move3 < -0.004) {
            regime = GlobalRegime.BTC_IMPULSE_DOWN;
        } else {
            regime = GlobalRegime.NEUTRAL;
        }

        boolean strongPressure = rawStrength > 0.70 && volExpansion > 1.4;
        boolean onlyLong  = regime == GlobalRegime.BTC_STRONG_UP  && rawStrength > 0.82;
        boolean onlyShort = (regime == GlobalRegime.BTC_STRONG_DOWN || regime == GlobalRegime.BTC_CRASH
                || regime == GlobalRegime.BTC_PANIC) && rawStrength > 0.78;

        // ── SHORT Boost — усиление шортов при краше ──────────────
        double shortBoost = computeShortBoost(cascadeLevel, btcCrashScore, btcDropVelocity, btcMomentumAccel);

        // ── Обновляем multi-window BTC return history ─────────────
        btcReturnHistory5.addLast(move1);
        btcReturnHistory10.addLast(move1);
        btcReturnHistory20.addLast(move1);
        while (btcReturnHistory5.size()  > 5)  btcReturnHistory5.removeFirst();
        while (btcReturnHistory10.size() > 10) btcReturnHistory10.removeFirst();
        while (btcReturnHistory20.size() > 20) btcReturnHistory20.removeFirst();

        currentContext = new GlobalContext(
                regime, rawStrength, volExpansion, strongPressure,
                onlyLong, onlyShort, clamp(btcTrend, -1, 1),
                volRegime, confAdj,
                btcDropVelocity, btcConsecutiveBearBars,
                btcMomentumAccel, btcCrashScore,
                cascadeLevel, panicMode.get(),
                shortBoost
        );
    }

    /**
     * [NEW] Обновление с 5m BTC данными для РАННЕЙ детекции краша.
     * Вызывается отдельно — до основного update() или параллельно.
     * 5m свечи дают сигнал на 15-30 минут раньше, чем 15m.
     */
    public void updateFast(List<com.bot.TradingCore.Candle> btc5mCandles) {
        if (btc5mCandles == null || btc5mCandles.size() < 10) return;

        int n = btc5mCandles.size();
        // Движение за последние 3 пятиминутки (15 минут = 1 свеча на 15m)
        double fastMove = (btc5mCandles.get(n - 1).close - btc5mCandles.get(n - 4).close)
                / (btc5mCandles.get(n - 4).close + 1e-9);

        // Ускорение на 5m уровне
        double prevFastMove = n > 7
                ? (btc5mCandles.get(n - 4).close - btc5mCandles.get(n - 7).close)
                / (btc5mCandles.get(n - 7).close + 1e-9)
                : fastMove;

        // fastMomentum: отрицательное при падении
        btcFastMomentum = fastMove;
        lastFastUpdateMs = System.currentTimeMillis();

        // Если 5m уже показывает сильный краш — немедленно обновляем crash score
        if (fastMove < -PANIC_DROP_5BAR && Math.abs(fastMove) > Math.abs(prevFastMove) * 1.3) {
            // Краш ускоряется на 5m — ранний сигнал
            double earlyScore = Math.min(1.0, Math.abs(fastMove) / PANIC_DROP_3BAR);
            btcCrashScore = Math.max(btcCrashScore, earlyScore * 0.85);

            // Принудительно пересчитываем cascade level
            CascadeLevel newLevel = determineCascadeLevel(btcCrashScore, fastMove, fastMove);
            if (newLevel.ordinal() > currentContext.cascadeLevel.ordinal()) {
                // Уровень повысился — обновляем контекст без полного update
                promoteCascadeLevel(newLevel);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  UPDATE SECTOR
    // ══════════════════════════════════════════════════════════════

    public void updateSector(String sector, List<com.bot.TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 25) return;

        int n = candles.size();
        com.bot.TradingCore.Candle last = candles.get(n - 1);

        double ema10   = ema(candles, 10);
        double ema25   = ema(candles, 25);
        double move5   = (last.close - candles.get(n - 6).close) / (candles.get(n - 6).close + 1e-9);
        double atr14   = atr(candles, 14);
        double atrNorm = atr14 / (last.close + 1e-9);

        // [NEW] drop8bars — изменение за 8 свечей (для sector contagion)
        double drop8bars = n >= 9
                ? (last.close - candles.get(n - 9).close) / (candles.get(n - 9).close + 1e-9)
                : 0.0;

        double emaBias  = (ema10 - ema25) / (ema25 + 1e-9) * 20;
        double moveBias = move5 / 0.01;
        double rawBias  = clamp((emaBias + moveBias) / 2, -1.0, 1.0);

        Deque<Double> hist = sectorBiasHist.computeIfAbsent(sector, k -> new ConcurrentLinkedDeque<>());
        hist.addLast(rawBias);
        if (hist.size() > 20) hist.removeFirst();

        double momentum = 0;
        if (hist.size() >= 5) {
            List<Double> list = new ArrayList<>(hist);
            double recent = list.subList(list.size() - 3, list.size())
                    .stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double old = list.subList(0, Math.min(5, list.size()))
                    .stream().mapToDouble(Double::doubleValue).average().orElse(0);
            momentum = recent - old;
        }

        double btcReturn5 = btcReturnHistory5.stream().mapToDouble(Double::doubleValue).sum();
        boolean leading = move5 > btcReturn5 * 1.15 && rawBias > 0;
        double strength = atrNorm > 0.012 ? 1.2 : atrNorm > 0.007 ? 0.9 : 0.6;

        SectorContext sc = new SectorContext(sector, rawBias, momentum, strength, leading, drop8bars);
        sectorMap.put(sector, sc);

        // [FIX-SECTOR] Если сектор заражён — логируем
        if (sc.isContaminated()) {
            System.out.printf("[GIC] SECTOR CONTAMINATED: %s drop8=%.2f%% — LONG VETO%n",
                    sector, drop8bars * 100);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  CRASH SCORE COMPUTATION
    // ══════════════════════════════════════════════════════════════

    /**
     * Агрегированный crash score [0..1].
     * Учитывает: размер движения, скорость, количество bear bars, acceleration.
     */
    private double computeCrashScore(double move3, double move5, double atr14, double price) {
        if (move3 >= 0 && move5 >= 0) return 0.0; // BTC растёт — нет краша

        double score = 0.0;

        // Фактор 1: Абсолютный размер падения
        if (move3 < -0.030) score += 0.40;
        else if (move3 < -0.020) score += 0.30;
        else if (move3 < -0.012) score += 0.20;
        else if (move3 < -0.005) score += 0.10;

        // Фактор 2: Скорость ускорения (velocity)
        if (btcDropVelocity > VELOCITY_CRASH)  score += 0.30;
        else if (btcDropVelocity > VELOCITY_DANGER)  score += 0.20;
        else if (btcDropVelocity > VELOCITY_WATCH)   score += 0.10;

        // Фактор 3: Consecutive bear bars
        int bearBars = btcConsecutiveBearBars;
        if (bearBars >= 5)      score += 0.25;
        else if (bearBars >= 4) score += 0.18;
        else if (bearBars >= 3) score += 0.12;
        else if (bearBars >= 2) score += 0.06;

        // Фактор 4: Acceleration (вторая производная)
        if (btcMomentumAccel > 0.008) score += 0.20;
        else if (btcMomentumAccel > 0.004) score += 0.12;
        else if (btcMomentumAccel > 0.001) score += 0.06;

        // Фактор 5: Быстрые данные (5m BTC) — ранний сигнал
        if (!isFastDataStale() && btcFastMomentum < -PANIC_DROP_5BAR) {
            score += 0.15;
        }

        // Фактор 6: Волатильность расширяется
        if (currentContext.volRegime == VolatilityRegime.EXTREME) score += 0.10;
        else if (currentContext.volRegime == VolatilityRegime.HIGH) score += 0.05;

        // Фактор 7: ATR аномально высокий — признак паники
        if (atr14 / price > 0.020) score += 0.10;
        else if (atr14 / price > 0.012) score += 0.05;

        return clamp(score, 0.0, 1.0);
    }

    /**
     * Определяет уровень каскада на основе crash score и других факторов.
     */
    private CascadeLevel determineCascadeLevel(double crashScore, double move3, double move5) {
        if (panicMode.get()) return CascadeLevel.PANIC;

        // PANIC: сверхбыстрое падение
        if (move3 < -PANIC_DROP_3BAR || (move5 < -0.040 && btcConsecutiveBearBars >= 4)) {
            return CascadeLevel.PANIC;
        }

        // CRASH: высокий crash score + несколько подтверждений
        if (crashScore >= 0.70 || (crashScore >= 0.55 && btcConsecutiveBearBars >= 3)) {
            return CascadeLevel.CRASH;
        }

        // DANGER: умеренный crash score
        if (crashScore >= 0.40 || btcDropVelocity > VELOCITY_DANGER) {
            return CascadeLevel.DANGER;
        }

        // WATCH: первые признаки
        if (crashScore >= 0.20 || btcDropVelocity > VELOCITY_WATCH) {
            return CascadeLevel.WATCH;
        }

        return CascadeLevel.NONE;
    }

    /**
     * Вычисляет SHORT boost — насколько сильно усиливать шорты при краше.
     */
    private double computeShortBoost(CascadeLevel level, double crashScore,
                                     double velocity, double accel) {
        double boost = 1.0;

        switch (level) {
            case PANIC  -> boost = 1.80; // Максимум при панике
            case CRASH  -> boost = 1.55;
            case DANGER -> boost = 1.30;
            case WATCH  -> boost = 1.15;
            case NONE   -> boost = 1.00;
        }

        // Дополнительный boost за acceleration
        if (accel > 0.008) boost = Math.min(boost * 1.20, 1.90);
        else if (accel > 0.003) boost = Math.min(boost * 1.10, 1.90);

        // Быстрые данные подтверждают
        if (!isFastDataStale() && btcFastMomentum < -0.015) {
            boost = Math.min(boost * 1.10, 1.90);
        }

        return boost;
    }

    /**
     * Нужно ли войти в режим паники.
     */
    private boolean shouldEnterPanic(double move3, double move5,
                                     double crashScore, CascadeLevel level) {
        // Уже в панике
        if (panicMode.get()) return false;

        // BTC упал > 3% за 3 свечи (45 минут)
        if (move3 < -PANIC_DROP_3BAR) return true;

        // BTC упал > 4% за 5 свечей
        if (move5 < -0.040) return true;

        // Краш-уровень + 4+ red bars + высокий score
        if (level == CascadeLevel.CRASH && btcConsecutiveBearBars >= 4
                && crashScore >= 0.75) return true;

        // Extreme волатильность + сильное падение
        if (currentContext.volRegime == VolatilityRegime.EXTREME
                && move3 < -0.020 && btcDropVelocity > VELOCITY_CRASH) return true;

        return false;
    }

    private void enterPanicMode(double move3, double move5, double atr14, double price) {
        panicMode.set(true);
        panicStartMs = System.currentTimeMillis();

        String msg = String.format(
                "🚨 *PANIC MODE ACTIVATED*\n" +
                        "BTC: move3=%.2f%% move5=%.2f%%\n" +
                        "Velocity=%.4f CrashScore=%.2f BearBars=%d\n" +
                        "🔴 Все LONG заблокированы\n" +
                        "🔴 SHORT усилены до %.0f%%",
                move3 * 100, move5 * 100,
                btcDropVelocity, btcCrashScore, btcConsecutiveBearBars,
                computeShortBoost(CascadeLevel.PANIC, btcCrashScore, btcDropVelocity, btcMomentumAccel) * 100
        );

        System.out.println("[GIC] " + msg.replace("*", ""));
        if (panicCallback != null) {
            try { panicCallback.accept(msg); } catch (Exception ignored) {}
        }
    }

    private void exitPanicMode() {
        panicMode.set(false);
        System.out.println("[GIC] PANIC MODE DEACTIVATED — рынок стабилизировался");
        if (panicCallback != null) {
            try { panicCallback.accept("✅ PANIC MODE OFF — торговля восстановлена"); } catch (Exception ignored) {}
        }
    }

    /**
     * Повышает уровень каскада в текущем контексте без полного пересчёта.
     * Используется при ранних сигналах с 5m данных.
     */
    private void promoteCascadeLevel(CascadeLevel newLevel) {
        GlobalContext old = currentContext;
        double newShortBoost = computeShortBoost(newLevel, btcCrashScore, btcDropVelocity, btcMomentumAccel);

        GlobalRegime newRegime = newLevel == CascadeLevel.PANIC
                ? GlobalRegime.BTC_PANIC
                : newLevel == CascadeLevel.CRASH
                ? GlobalRegime.BTC_CRASH
                : old.regime;

        currentContext = new GlobalContext(
                newRegime, old.impulseStrength, old.volatilityExpansion, old.strongPressure,
                old.onlyLong, old.onlyShort, old.btcTrend,
                old.volRegime, old.confidenceAdjustment,
                btcDropVelocity, btcConsecutiveBearBars,
                btcMomentumAccel, btcCrashScore,
                newLevel, panicMode.get(),
                newShortBoost
        );

        System.out.printf("[GIC] CASCADE PROMOTED: %s → %s (fastData)%n",
                old.cascadeLevel, newLevel);
    }

    // ══════════════════════════════════════════════════════════════
    //  CASCADE DUMP RISK (для внешних модулей)
    // ══════════════════════════════════════════════════════════════

    /**
     * Возвращает risk score cascade dump [0..1].
     *
     * v6.0 изменения:
     * - Базируется на btcCrashScore (агрегированном) а не на отдельных факторах
     * - RS > 0.80 при CRASH = ЛОВУШКА (RS Trap) — увеличивает риск
     * - Учитывает быстрые 5m данные
     */
    public double getCascadeDumpRisk(double relStrength) {
        GlobalContext ctx = currentContext;

        if (ctx.regime == GlobalRegime.NEUTRAL || ctx.regime == GlobalRegime.BTC_IMPULSE_UP
                || ctx.regime == GlobalRegime.BTC_STRONG_UP) {
            return 0.0;
        }

        double risk = ctx.btcCrashScore * 0.70;

        // [FIX-RS] RS Trap: монета держится при падении BTC = ЕЩЁ НЕ КАПИТУЛИРОВАЛА
        // Высокий RS при краше — это ОПАСНО, не хорошо
        if (relStrength > 0.82 && (ctx.cascadeLevel == CascadeLevel.CRASH
                || ctx.cascadeLevel == CascadeLevel.PANIC)) {
            risk += 0.20; // монета не упала ещё, но упадёт
        } else if (relStrength > 0.70 && ctx.cascadeLevel == CascadeLevel.DANGER) {
            risk += 0.10;
        }

        // Быстрые данные 5m подтверждают
        if (!isFastDataStale() && btcFastMomentum < -PANIC_DROP_5BAR) {
            risk += 0.12;
        }

        // Паника = максимальный риск
        if (ctx.panicMode) risk += 0.30;

        return clamp(risk, 0.0, 1.0);
    }

    public boolean isCascadeDumpRisk(double relStrength, double threshold) {
        return getCascadeDumpRisk(relStrength) >= threshold;
    }

    // ══════════════════════════════════════════════════════════════
    //  FILTER WEIGHT — главный метод для SignalSender
    // ══════════════════════════════════════════════════════════════

    /**
     * Возвращает вес сигнала [0..1.9].
     *
     * v6.0 ПОЛНАЯ ПЕРЕРАБОТКА логики для шортов при краше:
     *
     * КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: При BTC_CRASH и BTC_PANIC SHORT получает BOOST (> 1.0),
     * а не просто "разрешён". Это позволяет боту:
     * 1. Давать SHORT РАНЬШЕ (более высокий вес = ниже порог уверенности)
     * 2. Не ждать подтверждения осциллятора (RSI уже "перепродан")
     * 3. Агрессивно шортить в начале каша, а не в конце
     *
     * LONG при краше: полное вето + умный штраф по уровням.
     *
     * [FIX-SECTOR] Sector Contagion: если сектор заражён → LONG вето.
     */
    public double getFilterWeight(String symbol, boolean isLong, double relStrength, String sectorName) {
        GlobalContext ctx = currentContext;
        double weight = 1.0;

        // ── Sector Contagion Check (для LONG) ────────────────────
        if (isLong && sectorName != null) {
            SectorContext sc = sectorMap.get(sectorName);
            if (sc != null && sc.isContaminated()) {
                System.out.printf("[GIC] SECTOR CONTAGION BLOCK: %s sector=%s drop8=%.1f%%%n",
                        symbol, sectorName, sc.drop8bars * 100);
                return 0.0; // Сектор заражён — полный вето на лонги
            }
        }

        // ── Паника — безоговорочный вето на лонги ───────────────
        if (ctx.panicMode || ctx.regime == GlobalRegime.BTC_PANIC) {
            if (isLong) {
                return 0.0; // В панике LONG невозможен
            } else {
                // SHORT при панике — максимальный буст
                return Math.min(1.90, ctx.shortBoost);
            }
        }

        // ── Уровень каскада определяет вес ──────────────────────
        switch (ctx.cascadeLevel) {

            case CRASH -> {
                if (isLong) {
                    // [FIX-RS] RS Trap: высокий RS = ловушка catch-up dump
                    if (relStrength > 0.85) {
                        // Монета выглядит "сильной" — но это обманчиво при краше
                        SectorContext sc = sectorName != null ? sectorMap.get(sectorName) : null;
                        boolean sectorStrong = sc != null && sc.bias > 0.3 && sc.leading && !sc.isContaminated();
                        if (sectorStrong) {
                            weight = 0.25; // Очень редкий случай — сектор тоже держится
                        } else {
                            weight = 0.0;  // RS Trap — нет. Вето.
                        }
                    } else {
                        weight = 0.0; // Слабая монета при краше BTC — вето
                    }
                } else {
                    // SHORT при CRASH — агрессивный буст
                    weight = Math.min(1.75, ctx.shortBoost);
                }
            }

            case DANGER -> {
                if (isLong) {
                    // DANGER: сильный штраф, но не полный вето
                    double cascadeRisk = getCascadeDumpRisk(relStrength);
                    if (cascadeRisk >= 0.70) {
                        weight = 0.05;
                    } else if (cascadeRisk >= 0.50) {
                        // Высокий RS = RS Trap = ловушка
                        weight = relStrength > 0.75 ? 0.10 : 0.08;
                    } else {
                        // Низкий cascade risk при DANGER
                        if (relStrength > 0.80) {
                            SectorContext sc = sectorName != null ? sectorMap.get(sectorName) : null;
                            boolean sectorLeading = sc != null && sc.bias > 0.4 && sc.leading;
                            weight = sectorLeading ? 0.45 : 0.25;
                        } else if (relStrength > 0.65) {
                            weight = 0.18;
                        } else {
                            weight = 0.08;
                        }
                    }
                } else {
                    // SHORT при DANGER — буст
                    weight = Math.min(1.50, ctx.shortBoost);
                }
            }

            case WATCH -> {
                if (isLong) {
                    // WATCH: умеренный штраф
                    if (relStrength > 0.75) {
                        weight = 0.65;
                    } else if (relStrength > 0.55) {
                        weight = 0.50;
                    } else {
                        weight = 0.35;
                    }
                } else {
                    // SHORT при WATCH — небольшой буст
                    weight = Math.min(1.25, ctx.shortBoost);
                }
            }

            case NONE -> {
                // Нет каскада — используем стандартную логику режимов
                switch (ctx.regime) {

                    case BTC_STRONG_UP -> {
                        if (!isLong) {
                            weight = relStrength < 0.25 ? 0.65
                                    : relStrength < 0.45 ? 0.45 : 0.25;
                        } else {
                            weight = 1.0 + (ctx.impulseStrength - 0.65) * 0.30;
                            weight = Math.min(weight, 1.20);
                        }
                    }

                    case BTC_STRONG_DOWN -> {
                        if (isLong) {
                            // [SCANNER MODE v1.0] LONG weights raised: local reversals / RS leaders still viable.
                            // was: cr>=0.55→0.12, cr>=0.35→0.15/0.35, else→0.10/0.28/0.40
                            double cr = getCascadeDumpRisk(relStrength);
                            if (cr >= 0.55) {
                                weight = 0.40; // was 0.12 — still penalised but not zero
                            } else if (cr >= 0.35) {
                                weight = relStrength > 0.80 ? 0.65 : 0.50; // was 0.35/0.15
                            } else {
                                SectorContext sc = sectorName != null ? sectorMap.get(sectorName) : null;
                                boolean leading = sc != null && sc.bias > 0.4 && sc.leading;
                                weight = relStrength > 0.80 ? (leading ? 0.90 : 0.75)
                                        : relStrength > 0.65 ? 0.65  // was 0.28
                                        : 0.45;                        // was 0.10
                            }
                        } else {
                            // SHORT при BTC_STRONG_DOWN — буст
                            weight = 1.0 + (ctx.impulseStrength - 0.65) * 0.35;
                            if (ctx.btcDropVelocity > VELOCITY_DANGER) {
                                weight = Math.min(weight * 1.20, 1.50);
                            }
                            weight = Math.min(weight, 1.50);
                        }
                    }

                    case BTC_IMPULSE_DOWN -> {
                        if (isLong) {
                            // [SCANNER MODE v1.0] was: cr>=0.45→0.28, rs>0.65→0.75, else→0.50
                            // Now: still penalised but visible — local bounces allowed on RS leaders
                            double cr = getCascadeDumpRisk(relStrength);
                            weight = cr >= 0.45 ? 0.55 : relStrength > 0.65 ? 0.90 : 0.70;
                        } else {
                            // [v8.0] Чуть сильнее SHORT boost при импульсном падении
                            weight = Math.min(1.25, ctx.shortBoost * 0.90);
                        }
                    }

                    case BTC_IMPULSE_UP -> {
                        if (!isLong) {
                            // [v8.0] Мягче: было 0.70 для всех шортов
                            // Если монета слабеет на бычьем BTC — шорт разрешён мягче
                            weight = relStrength < 0.35 ? 0.85 : 0.78;
                        }
                    }

                    case NEUTRAL -> {
                        // Нейтраль — без изменений
                    }

                    default -> {
                        // BTC_CRASH / BTC_PANIC уже обработаны выше
                    }
                }
            }

            default -> {} // PANIC обработан выше
        }

        // ── Корректировка по волатильностному режиму ─────────────
        if (ctx.volRegime == VolatilityRegime.EXTREME) {
            if (isLong) weight *= 0.80; // [SCANNER MODE] was 0.55 — EXTREME vol kills LONGs unfairly
            // SHORT при extreme vol — не штрафуем (волатильность в нашу пользу)
        } else if (ctx.volRegime == VolatilityRegime.HIGH) {
            if (isLong) weight *= 0.90; // [SCANNER MODE] was 0.80
        }

        return clamp(weight, 0.0, 1.90);
    }

    // ══════════════════════════════════════════════════════════════
    //  MULTI-WINDOW RELATIVE STRENGTH
    // ══════════════════════════════════════════════════════════════

    public double calculateWeightedRS(List<Double> symbolReturns20) {
        if (symbolReturns20 == null || symbolReturns20.isEmpty()) return 0.5;

        int size = symbolReturns20.size();
        double rs5  = calculateRSForWindow(symbolReturns20, btcReturnHistory5,  Math.min(5, size));
        double rs10 = calculateRSForWindow(symbolReturns20, btcReturnHistory10, Math.min(10, size));
        double rs20 = calculateRSForWindow(symbolReturns20, btcReturnHistory20, Math.min(20, size));

        // v6.0: краткосрочный RS получает ещё больший вес при краше
        GlobalContext ctx = currentContext;
        if (ctx.cascadeLevel == CascadeLevel.CRASH || ctx.cascadeLevel == CascadeLevel.PANIC) {
            // При краше важнее ТЕКУЩАЯ сила (последние 5 свечей)
            return rs5 * 0.70 + rs10 * 0.20 + rs20 * 0.10;
        }
        return rs5 * 0.50 + rs10 * 0.30 + rs20 * 0.20;
    }

    private double calculateRSForWindow(List<Double> symbolReturns, Deque<Double> btcReturns, int window) {
        if (window <= 0 || btcReturns.isEmpty()) return 0.5;

        int size = symbolReturns.size();
        List<Double> btcList = new ArrayList<>(btcReturns);

        double symSum = 0, btcSum = 0;
        int count = Math.min(window, Math.min(size, btcList.size()));
        for (int i = 0; i < count; i++) {
            symSum += symbolReturns.get(size - 1 - i);
            btcSum += btcList.get(btcList.size() - 1 - i);
        }

        double symReturn = symSum / count;
        double btcReturn = btcSum / count;

        if (Math.abs(btcReturn) < 0.0001) {
            return symReturn > 0 ? 0.70 : 0.30;
        }
        if (btcReturn < 0 && symReturn > 0) {
            return 0.85 + Math.min(Math.abs(symReturn) * 8, 0.14);
        }
        return clamp(0.5 + (symReturn - btcReturn) / (Math.abs(btcReturn) * 2), 0.0, 1.0);
    }

    // ══════════════════════════════════════════════════════════════
    //  SECTOR ANALYSIS
    // ══════════════════════════════════════════════════════════════

    public double getMarketBias() {
        if (sectorMap.isEmpty()) return 0.0;
        return sectorMap.values().stream()
                .mapToDouble(sc -> sc.bias * sc.strength)
                .average().orElse(0.0);
    }

    public double getSectorWeakness(String sectorName) {
        if (sectorMap.size() < 3 || sectorName == null) return 0.0;
        SectorContext target = sectorMap.get(sectorName);
        if (target == null) return 0.0;

        // [FIX-SECTOR] Contaminated sector = максимальная слабость
        if (target.isContaminated()) return 1.0;

        double marketBias = getMarketBias();
        if (marketBias > 0.3 && target.bias < -0.1) {
            return Math.min(1.0, (marketBias - target.bias) / 1.5);
        }
        return 0.0;
    }

    public SectorContext getSectorContext(String sectorName) {
        return sectorMap.get(sectorName);
    }

    public List<String> getLeadingSectors() {
        List<String> leaders = new ArrayList<>();
        for (Map.Entry<String, SectorContext> e : sectorMap.entrySet()) {
            if (e.getValue().leading) leaders.add(e.getKey());
        }
        return leaders;
    }

    /**
     * [NEW] Возвращает список заражённых секторов.
     */
    public List<String> getContaminatedSectors() {
        List<String> contaminated = new ArrayList<>();
        for (Map.Entry<String, SectorContext> e : sectorMap.entrySet()) {
            if (e.getValue().isContaminated()) contaminated.add(e.getKey());
        }
        return contaminated;
    }

    // ══════════════════════════════════════════════════════════════
    //  GETTERS
    // ══════════════════════════════════════════════════════════════

    public GlobalContext getContext() { return currentContext; }

    public boolean isPanicMode() { return panicMode.get(); }

    public double getBtcCrashScore() { return btcCrashScore; }

    public CascadeLevel getCurrentCascadeLevel() { return currentContext.cascadeLevel; }

    /**
     * Возвращает SHORT boost для текущего рыночного состояния.
     * Используется в DecisionEngineMerged для буста scoreShort при краше.
     */
    public double getShortBoostFactor() {
        return currentContext.shortBoost;
    }

    /**
     * [NEW] Возвращает true если SHORT следует давать агрессивно (без ожидания подтверждений).
     * При CRASH/PANIC: даём SHORT РАНЬШЕ, не ждём RSI/дивергенций.
     */
    public boolean isAggressiveShortMode() {
        GlobalContext ctx = currentContext;
        return ctx.cascadeLevel == CascadeLevel.CRASH
                || ctx.cascadeLevel == CascadeLevel.PANIC
                || ctx.panicMode
                || ctx.regime == GlobalRegime.BTC_CRASH
                || ctx.regime == GlobalRegime.BTC_PANIC
                || (ctx.cascadeLevel == CascadeLevel.DANGER && ctx.btcDropVelocity > VELOCITY_DANGER);
    }

    /**
     * [NEW] Momentum acceleration BTC — используется в DecisionEngineMerged
     * для boost SHORT score РАНЬШЕ, чем истощение покажут осцилляторы.
     */
    public double getBtcMomentumAccel() { return btcMomentumAccel; }

    public String getStats() {
        GlobalContext ctx = currentContext;
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "GIC[%s str=%.2f vol=%.2f volReg=%s confAdj=%+.1f " +
                        "bearBars=%d vel=%.4f accel=%.4f crashScore=%.2f cascade=%s panic=%s shortBoost=%.2f]",
                ctx.regime, ctx.impulseStrength, ctx.volatilityExpansion,
                ctx.volRegime, ctx.confidenceAdjustment,
                ctx.btcConsecutiveBearBars, ctx.btcDropVelocity,
                ctx.btcMomentumAccel, ctx.btcCrashScore,
                ctx.cascadeLevel, ctx.panicMode ? "YES🚨" : "no",
                ctx.shortBoost));

        if (!sectorMap.isEmpty()) {
            sb.append(" Sectors:");
            sectorMap.forEach((s, sc) ->
                    sb.append(String.format(" %s=%.2f%s%s",
                            s, sc.bias,
                            sc.leading ? "↑" : "",
                            sc.isContaminated() ? "☠" : "")));
        }

        List<String> contaminated = getContaminatedSectors();
        if (!contaminated.isEmpty()) {
            sb.append(" ☠CONTAM:").append(String.join(",", contaminated));
        }

        if (ctx.btcCrashScore > 0.30) {
            sb.append(String.format(" ⚠️CRASH=%.2f", ctx.btcCrashScore));
        }

        sb.append(String.format(" MktBias=%.2f", getMarketBias()));
        return sb.toString();
    }

    // ══════════════════════════════════════════════════════════════
    //  VOLATILITY REGIME
    // ══════════════════════════════════════════════════════════════

    private VolatilityRegime calcVolatilityRegime(double currentAtrPct) {
        if (btcAtrHistory.size() < 20) return VolatilityRegime.NORMAL;

        List<Double> sorted = new ArrayList<>(btcAtrHistory);
        Collections.sort(sorted);
        double median = sorted.get(sorted.size() / 2);

        if (median <= 0) return VolatilityRegime.NORMAL;
        double ratio = currentAtrPct / median;

        if (ratio > 3.0) return VolatilityRegime.EXTREME;
        if (ratio > 1.6) return VolatilityRegime.HIGH;
        if (ratio < 0.5) return VolatilityRegime.LOW;
        return VolatilityRegime.NORMAL;
    }

    // ══════════════════════════════════════════════════════════════
    //  UTILITY
    // ══════════════════════════════════════════════════════════════

    private boolean isFastDataStale() {
        return System.currentTimeMillis() - lastFastUpdateMs > FAST_DATA_STALE;
    }

    /** [v23.0 FIX] Delegates to TradingCore.atr() — Wilder's smoothed ATR.
     *  Old code used simple SMA which diverges 15-20% from Wilder's method.
     *  This caused crash score to compute on wrong ATR values. */
    private double atr(List<com.bot.TradingCore.Candle> c, int n) {
        return com.bot.TradingCore.atr(c, n);
    }

    private double ema(List<com.bot.TradingCore.Candle> c, int p) {
        if (c.size() < p) return c.get(c.size() - 1).close;
        double k = 2.0 / (p + 1), e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private double avgVolume(List<com.bot.TradingCore.Candle> c, int n) {
        int start = Math.max(0, c.size() - n);
        return c.subList(start, c.size()).stream()
                .mapToDouble(x -> x.volume).average().orElse(1);
    }

    private double avgBodyPct(List<com.bot.TradingCore.Candle> c, int n) {
        int start = Math.max(0, c.size() - n);
        return c.subList(start, c.size()).stream()
                .mapToDouble(x -> Math.abs(x.close - x.open) / (x.close + 1e-9))
                .average().orElse(0.005);
    }

    private double clamp(double v, double lo, double hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    @Deprecated
    public double filterSignal(String symbol, boolean isLong, double confidence,
                               com.bot.DecisionEngineMerged.CoinCategory cat) {
        return confidence * getFilterWeight(symbol, isLong, 0.5, null);
    }
}