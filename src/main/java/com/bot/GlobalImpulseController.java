package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║       GlobalImpulseController — GODBOT EDITION v4.0                 ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  ИСПРАВЛЕНИЯ v4.0:                                                   ║
 * ║                                                                      ║
 * ║  [FIX-4] УБРАН жёсткий onlyShort при BTC_STRONG_DOWN               ║
 * ║    Старый вариант: если BTC падает — запрещаем все LONG              ║
 * ║    Новый вариант: считаем Relative Strength для каждого символа      ║
 * ║    Монеты с RS > 0.70 могут получать LONG даже при BTC дампе        ║
 * ║    (классические примеры: ETH/BTC расхождение, доминация альтов)    ║
 * ║                                                                      ║
 * ║  [FIX-RS] filterSignal() теперь принимает RS символа               ║
 * ║    Если RS > 0.72 при BTC_STRONG_DOWN — разрешаем LONG с весом 0.7  ║
 * ║    Если RS < 0.25 при BTC_STRONG_UP — разрешаем SHORT с весом 0.7   ║
 * ║                                                                      ║
 * ║  [NEW] Метод getFilterWeight() — плавный коэффициент 0..1           ║
 * ║    Вместо бинарного блока — плавное масштабирование скора           ║
 * ║                                                                      ║
 * ║  [NEW] Volatility regime — 4 уровня волатильности                  ║
 * ║    LOW / NORMAL / HIGH / EXTREME                                     ║
 * ║    При EXTREME — порог уверенности автоматически повышается          ║
 * ║                                                                      ║
 * ║  [NEW] Sector Divergence Score — если все секторы BULL кроме одного ║
 * ║    Дивергирующий сектор получает слабость (weakness score)           ║
 * ║                                                                      ║
 * ║  СОХРАНЕНО: BTC режимы / секторные контексты / секторный лидер      ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 */
public final class GlobalImpulseController {

    private final int VOL_LOOKBACK;
    private final int BODY_LOOKBACK;

    // [NEW] История ATR BTC для расчёта режима волатильности
    private final Deque<Double> btcAtrHistory = new ArrayDeque<>();
    private static final int ATR_HISTORY_SIZE = 96; // 96 × 15m = 24h

    public GlobalImpulseController() {
        this.VOL_LOOKBACK  = 20;
        this.BODY_LOOKBACK = 10;
    }

    public GlobalImpulseController(int volLookback, int bodyLookback) {
        this.VOL_LOOKBACK  = volLookback;
        this.BODY_LOOKBACK = bodyLookback;
    }

    // ══════════════════════════════════════════════════════════════
    //  ENUMS
    // ══════════════════════════════════════════════════════════════

    public enum GlobalRegime {
        NEUTRAL,           // BTC флэт / нет импульса
        BTC_IMPULSE_UP,    // BTC растёт умеренно
        BTC_IMPULSE_DOWN,  // BTC падает умеренно
        BTC_STRONG_UP,     // BTC сильный рост
        BTC_STRONG_DOWN    // BTC сильное падение
    }

    /** [NEW] Режим волатильности рынка */
    public enum VolatilityRegime {
        LOW,      // ATR < 50% исторического среднего — мёртвый рынок
        NORMAL,   // ATR 50-150% — нормальная торговля
        HIGH,     // ATR 150-250% — повышенная волатильность
        EXTREME   // ATR > 250% — экстрим (flash crash / pump)
    }

    // ══════════════════════════════════════════════════════════════
    //  GlobalContext
    // ══════════════════════════════════════════════════════════════

    public static final class GlobalContext {
        public final GlobalRegime regime;
        public final double impulseStrength;
        public final double volatilityExpansion;
        public final boolean strongPressure;

        /**
         * [FIX-4] onlyLong и onlyShort больше не являются жёсткими блокировками.
         * Они используются как СИГНАЛЫ предпочтения, но не запрещают торговлю.
         * Для реального запрета используйте getFilterWeight(symbol, side, rs)
         */
        public final boolean onlyLong;   // BTC_STRONG_UP — предпочтение LONG
        public final boolean onlyShort;  // BTC_STRONG_DOWN — предпочтение SHORT (НЕ ЗАПРЕТ LONG!)
        public final double btcTrend;    // -1..+1 (EMA20/EMA50)

        /** [NEW] Режим волатильности */
        public final VolatilityRegime volRegime;

        /** [NEW] Рекомендуемая поправка к MIN_CONFIDENCE при текущей волатильности */
        public final double confidenceAdjustment;

        public GlobalContext(GlobalRegime regime, double impulseStrength,
                             double volatilityExpansion, boolean strongPressure,
                             boolean onlyLong, boolean onlyShort, double btcTrend,
                             VolatilityRegime volRegime, double confidenceAdjustment) {
            this.regime             = regime;
            this.impulseStrength    = impulseStrength;
            this.volatilityExpansion = volatilityExpansion;
            this.strongPressure     = strongPressure;
            this.onlyLong           = onlyLong;
            this.onlyShort          = onlyShort;
            this.btcTrend           = btcTrend;
            this.volRegime          = volRegime;
            this.confidenceAdjustment = confidenceAdjustment;
        }

        /** Обратная совместимость */
        public GlobalContext(GlobalRegime regime, double impulseStrength,
                             double volatilityExpansion, boolean strongPressure,
                             boolean onlyLong, boolean onlyShort, double btcTrend) {
            this(regime, impulseStrength, volatilityExpansion, strongPressure,
                    onlyLong, onlyShort, btcTrend, VolatilityRegime.NORMAL, 0.0);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  SectorContext
    // ══════════════════════════════════════════════════════════════

    public static final class SectorContext {
        public final String  sector;
        public final double  bias;        // -1 (медведь) .. +1 (бык)
        public final double  momentum;    // скорость изменения bias
        public final double  strength;    // сила тренда сектора
        public final boolean leading;     // сектор опережает BTC

        public SectorContext(String sector, double bias, double momentum,
                             double strength, boolean leading) {
            this.sector   = sector;
            this.bias     = bias;
            this.momentum = momentum;
            this.strength = strength;
            this.leading  = leading;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  STATE
    // ══════════════════════════════════════════════════════════════

    private volatile GlobalContext currentContext = new GlobalContext(
            GlobalRegime.NEUTRAL, 0.0, 1.0, false, false, false, 0.0,
            VolatilityRegime.NORMAL, 0.0
    );

    private final Map<String, SectorContext>  sectorMap        = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>  sectorBiasHist   = new ConcurrentHashMap<>();

    // [NEW] История BTC returns для Relative Strength калибровки
    private final Deque<Double> btcReturnHistory = new ArrayDeque<>();
    private static final int RS_HISTORY = 20; // 20 × 15m = 5h

    // ══════════════════════════════════════════════════════════════
    //  UPDATE — вызывается из BotMain с BTC свечами
    // ══════════════════════════════════════════════════════════════

    public void update(List<TradingCore.Candle> btcCandles) {
        if (btcCandles == null || btcCandles.size() < 30) return;

        int n = btcCandles.size();
        TradingCore.Candle last = btcCandles.get(n - 1);

        double atr14    = atr(btcCandles, 14);
        double avgVol   = avgVolume(btcCandles, VOL_LOOKBACK);
        double curVol   = last.volume;
        double volRatio = curVol / (avgVol + 1e-9);

        // Движение за последние 3 свечи
        double move3    = (last.close - btcCandles.get(n - 4).close) / (btcCandles.get(n - 4).close + 1e-9);
        // Движение за последние 8 свечей (2 часа)
        double move8    = (last.close - btcCandles.get(n - 9).close) / (btcCandles.get(n - 9).close + 1e-9);

        // Сила тела последней свечи
        double bodyPct  = avgBodyPct(btcCandles, BODY_LOOKBACK);
        double curBody  = Math.abs(last.close - last.open) / (last.close + 1e-9);
        double bodyRatio = curBody / (bodyPct + 1e-9);

        // EMA тренд BTC
        double ema20    = ema(btcCandles, 20);
        double ema50    = ema(btcCandles, 50);
        double btcTrend = (ema20 - ema50) / (ema50 + 1e-9) * 100;

        // Сила импульса: нормализовано 0..1
        double rawStrength = Math.min(1.0,
                (Math.abs(move3) / 0.012) * 0.45 +
                        (Math.min(volRatio, 4.0) / 4.0) * 0.35 +
                        (Math.min(bodyRatio, 3.0) / 3.0) * 0.20
        );

        // [NEW] ATR история для волатильностного режима
        btcAtrHistory.addLast(atr14 / last.close);
        if (btcAtrHistory.size() > ATR_HISTORY_SIZE) btcAtrHistory.removeFirst();

        VolatilityRegime volRegime = calcVolatilityRegime(atr14 / last.close);
        double confAdj = switch (volRegime) {
            case LOW     -> -1.5;   // мало движения → можно торговать агрессивнее
            case NORMAL  ->  0.0;
            case HIGH    -> +2.0;   // высокая воля → строже фильтруем
            case EXTREME -> +5.0;   // экстрим → только самые сильные сигналы
        };

        // Расширение волатильности vs нормы
        double volExpansion = atr14 / (atr(btcCandles.subList(Math.max(0, n - 50), n - 10), 14) + 1e-9);

        // [FIX-4] Определяем режим без жёсткого запрета LONG при падении
        GlobalRegime regime;
        if (move3 > 0.008 && rawStrength > 0.65) {
            regime = GlobalRegime.BTC_STRONG_UP;
        } else if (move3 < -0.008 && rawStrength > 0.65) {
            regime = GlobalRegime.BTC_STRONG_DOWN;
        } else if (move3 > 0.004) {
            regime = GlobalRegime.BTC_IMPULSE_UP;
        } else if (move3 < -0.004) {
            regime = GlobalRegime.BTC_IMPULSE_DOWN;
        } else {
            regime = GlobalRegime.NEUTRAL;
        }

        boolean strongPressure = rawStrength > 0.70 && volExpansion > 1.4;

        // [FIX-4] onlyLong/onlyShort — теперь это РЕКОМЕНДАЦИИ, а не блокировки
        // Фактическая фильтрация происходит в getFilterWeight()
        boolean onlyLong  = regime == GlobalRegime.BTC_STRONG_UP  && rawStrength > 0.80;
        boolean onlyShort = regime == GlobalRegime.BTC_STRONG_DOWN && rawStrength > 0.80;

        // Обновляем историю BTC returns для RS
        btcReturnHistory.addLast(move3);
        if (btcReturnHistory.size() > RS_HISTORY) btcReturnHistory.removeFirst();

        currentContext = new GlobalContext(
                regime, rawStrength, volExpansion, strongPressure,
                onlyLong, onlyShort, clamp(btcTrend, -1, 1),
                volRegime, confAdj
        );
    }

    // ══════════════════════════════════════════════════════════════
    //  UPDATE SECTOR
    // ══════════════════════════════════════════════════════════════

    public void updateSector(String sector, List<TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 25) return;

        int n = candles.size();
        TradingCore.Candle last = candles.get(n - 1);

        double ema10   = ema(candles, 10);
        double ema25   = ema(candles, 25);
        double move5   = (last.close - candles.get(n - 6).close) / (candles.get(n - 6).close + 1e-9);
        double atr14   = atr(candles, 14);
        double atrNorm = atr14 / (last.close + 1e-9);

        // Bias: от -1 (очень медвежий) до +1 (очень бычий)
        double emaBias = (ema10 - ema25) / (ema25 + 1e-9) * 20; // масштабируем
        double moveBias = move5 / 0.01;                           // 1% движение = bias 1.0
        double rawBias = clamp((emaBias + moveBias) / 2, -1.0, 1.0);

        // История bias для momentum
        Deque<Double> hist = sectorBiasHist.computeIfAbsent(sector, k -> new ArrayDeque<>());
        hist.addLast(rawBias);
        if (hist.size() > 20) hist.removeFirst();

        double momentum = 0;
        if (hist.size() >= 5) {
            List<Double> list = new ArrayList<>(hist);
            double recent = list.subList(list.size() - 3, list.size()).stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double old    = list.subList(0, Math.min(5, list.size())).stream().mapToDouble(Double::doubleValue).average().orElse(0);
            momentum = recent - old;
        }

        // Сектор "ведущий" если его roc > BTC roc за последние 5 баров
        double btcReturn5 = btcReturnHistory.size() >= 3
                ? btcReturnHistory.stream().mapToDouble(Double::doubleValue).sum()
                : 0;
        boolean leading = move5 > btcReturn5 * 1.15 && rawBias > 0;

        double strength = atrNorm > 0.012 ? 1.2 : atrNorm > 0.007 ? 0.9 : 0.6;

        sectorMap.put(sector, new SectorContext(sector, rawBias, momentum, strength, leading));
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-4] FILTER WEIGHT — плавный коэффициент вместо блокировки
    // ══════════════════════════════════════════════════════════════

    /**
     * Возвращает вес фильтрации сигнала от 0.0 до 1.0.
     * 1.0 = сигнал разрешён полностью
     * 0.5 = сигнал ослаблен вдвое (снижаем итоговый скор)
     * 0.0 = сигнал заблокирован
     *
     * @param symbol       торговая пара (например "SOLUSDT")
     * @param isLong       true = LONG сигнал
     * @param relStrength  Relative Strength символа vs BTC (0..1, 0.5 = нейтраль)
     * @param sectorName   сектор монеты (может быть null)
     */
    public double getFilterWeight(String symbol, boolean isLong, double relStrength, String sectorName) {
        GlobalContext ctx = currentContext;
        double weight = 1.0;

        switch (ctx.regime) {
            case BTC_STRONG_UP -> {
                if (!isLong) {
                    // SHORT при сильном BTC вверх
                    if (relStrength < 0.25) {
                        // Слабая монета — можно шортить (divergence SHORT)
                        weight = 0.75;
                    } else if (relStrength < 0.45) {
                        weight = 0.55;
                    } else {
                        // [FIX-4] Раньше было полное BLOCK, теперь просто слабый вес
                        weight = 0.35;
                    }
                }
                // LONG при сильном BTC вверх — полный вес + бонус
                else {
                    weight = 1.0 + (ctx.impulseStrength - 0.65) * 0.3; // до 1.1
                    weight = Math.min(weight, 1.15);
                }
            }

            case BTC_STRONG_DOWN -> {
                if (isLong) {
                    // [FIX-4] КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: не блокируем LONG если RS высокий
                    if (relStrength > 0.75) {
                        // Монета сильно опережает BTC — разрешаем LONG с небольшим штрафом
                        weight = 0.70;
                        // Дополнительная проверка сектора
                        if (sectorName != null) {
                            SectorContext sc = sectorMap.get(sectorName);
                            if (sc != null && sc.bias > 0.4 && sc.leading) {
                                weight = 0.85; // сектор тоже сильный — ещё лучше
                            }
                        }
                    } else if (relStrength > 0.60) {
                        weight = 0.45;  // Умеренный RS — слабый лонг
                    } else if (relStrength > 0.45) {
                        weight = 0.25;  // Нейтральный RS — почти блок
                    } else {
                        weight = 0.0;   // Слабая монета + BTC падает = NO LONG
                    }
                } else {
                    // SHORT при BTC_STRONG_DOWN — усиливаем
                    weight = 1.0 + (ctx.impulseStrength - 0.65) * 0.35;
                    weight = Math.min(weight, 1.2);
                }
            }

            case BTC_IMPULSE_UP -> {
                if (!isLong) weight = 0.72;  // Небольшой штраф для шорта
            }

            case BTC_IMPULSE_DOWN -> {
                if (isLong) {
                    weight = relStrength > 0.65 ? 0.80 : 0.55;
                }
            }

            case NEUTRAL -> {
                // Нейтральный режим — без изменений
            }
        }

        // Корректировка по волатильностному режиму
        if (ctx.volRegime == VolatilityRegime.EXTREME) {
            weight *= 0.60;  // При экстремальной воле — только 60% веса
        } else if (ctx.volRegime == VolatilityRegime.HIGH) {
            weight *= 0.85;
        }

        return clamp(weight, 0.0, 1.2);
    }

    /**
     * Устаревший метод — обратная совместимость.
     * Используйте getFilterWeight() для плавного контроля.
     * @deprecated используйте getFilterWeight(symbol, isLong, relStrength, sectorName)
     */
    @Deprecated
    public double filterSignal(String symbol, boolean isLong, double confidence,
                               DecisionEngineMerged.CoinCategory cat) {
        double rs = 0.5; // нейтральный RS по умолчанию
        double weight = getFilterWeight(symbol, isLong, rs, null);
        return confidence * weight;
    }

    // ══════════════════════════════════════════════════════════════
    //  SECTOR ANALYSIS — агрегированный анализ всех секторов
    // ══════════════════════════════════════════════════════════════

    /**
     * Возвращает общий bias рынка по всем секторам.
     * Используется как дополнительный фильтр в SignalSender.
     * +1.0 = все секторы бычьи, -1.0 = все медвежьи
     */
    public double getMarketBias() {
        if (sectorMap.isEmpty()) return 0.0;
        return sectorMap.values().stream()
                .mapToDouble(sc -> sc.bias * sc.strength)
                .average()
                .orElse(0.0);
    }

    /**
     * [NEW] Sector Divergence — находит секторы, которые расходятся с большинством.
     * Если рынок BULL, но один сектор медвежий — у его монет высокий риск.
     * Возвращает weakness score для сектора (0 = нормально, 1 = сильная слабость).
     */
    public double getSectorWeakness(String sectorName) {
        if (sectorMap.size() < 3 || sectorName == null) return 0.0;

        SectorContext target = sectorMap.get(sectorName);
        if (target == null) return 0.0;

        double marketBias = getMarketBias();
        // Если рынок BULL (bias > 0.3), а сектор BEAR (bias < -0.1) — слабость
        if (marketBias > 0.3 && target.bias < -0.1) {
            return Math.min(1.0, (marketBias - target.bias) / 1.5);
        }
        // Если рынок BEAR (bias < -0.3), а сектор BULL (bias > 0.1) — считаем силой, не слабостью
        return 0.0;
    }

    /**
     * Получает контекст конкретного сектора.
     */
    public SectorContext getSectorContext(String sectorName) {
        return sectorMap.get(sectorName);
    }

    /**
     * Возвращает список ведущих секторов (опережают BTC).
     */
    public List<String> getLeadingSectors() {
        List<String> leaders = new ArrayList<>();
        for (Map.Entry<String, SectorContext> e : sectorMap.entrySet()) {
            if (e.getValue().leading) leaders.add(e.getKey());
        }
        return leaders;
    }

    // ══════════════════════════════════════════════════════════════
    //  GETTERS
    // ══════════════════════════════════════════════════════════════

    public GlobalContext getContext() { return currentContext; }

    public String getStats() {
        GlobalContext ctx = currentContext;
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("GIC[%s str=%.2f vol=%.2f volReg=%s confAdj=%+.1f]",
                ctx.regime, ctx.impulseStrength, ctx.volatilityExpansion,
                ctx.volRegime, ctx.confidenceAdjustment));

        if (!sectorMap.isEmpty()) {
            sb.append(" Sectors:");
            sectorMap.forEach((s, sc) ->
                    sb.append(String.format(" %s=%.2f%s", s, sc.bias, sc.leading ? "↑" : "")));
        }

        double mktBias = getMarketBias();
        sb.append(String.format(" MktBias=%.2f", mktBias));

        return sb.toString();
    }

    // ══════════════════════════════════════════════════════════════
    //  [NEW] VOLATILITY REGIME CALCULATOR
    // ══════════════════════════════════════════════════════════════

    private VolatilityRegime calcVolatilityRegime(double currentAtrPct) {
        if (btcAtrHistory.size() < 20) return VolatilityRegime.NORMAL;

        // Медианный ATR за последние 96 свечей (24h)
        List<Double> sorted = new ArrayList<>(btcAtrHistory);
        Collections.sort(sorted);
        double median = sorted.get(sorted.size() / 2);

        if (median <= 0) return VolatilityRegime.NORMAL;
        double ratio = currentAtrPct / median;

        if (ratio > 2.5) return VolatilityRegime.EXTREME;
        if (ratio > 1.5) return VolatilityRegime.HIGH;
        if (ratio < 0.5) return VolatilityRegime.LOW;
        return VolatilityRegime.NORMAL;
    }

    // ══════════════════════════════════════════════════════════════
    //  MATH PRIMITIVES
    // ══════════════════════════════════════════════════════════════

    private double atr(List<TradingCore.Candle> c, int n) {
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

    private double ema(List<TradingCore.Candle> c, int p) {
        if (c.size() < p) return c.get(c.size() - 1).close;
        double k = 2.0 / (p + 1), e = c.get(c.size() - p).close;
        for (int i = c.size() - p + 1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private double avgVolume(List<TradingCore.Candle> c, int n) {
        int start = Math.max(0, c.size() - n);
        return c.subList(start, c.size()).stream()
                .mapToDouble(x -> x.volume).average().orElse(1);
    }

    private double avgBodyPct(List<TradingCore.Candle> c, int n) {
        int start = Math.max(0, c.size() - n);
        return c.subList(start, c.size()).stream()
                .mapToDouble(x -> Math.abs(x.close - x.open) / (x.close + 1e-9))
                .average().orElse(0.005);
    }

    private double clamp(double v, double lo, double hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}