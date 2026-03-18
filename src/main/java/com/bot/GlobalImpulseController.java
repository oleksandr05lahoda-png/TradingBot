package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║       GlobalImpulseController — GODBOT EDITION v5.0                 ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║  ИСПРАВЛЕНИЯ v5.0:                                                   ║
 * ║                                                                      ║
 * ║  [FIX-BUG-1] ЗАЩИТА ОТ CASCADE DUMP (BUG #1 из аудита)            ║
 * ║    Проблема: на фьюч крипторынке альткоин может держаться           ║
 * ║    при падении BTC первые 15-30 минут, но затем капитулирует        ║
 * ║    (catch-up dump). RS 0.75 в первые свечи падения = ЛОВУШКА.      ║
 * ║                                                                      ║
 * ║    Новая логика getFilterWeight():                                   ║
 * ║    1. Учитываем СКОРОСТЬ падения BTC (velocity), не только размер   ║
 * ║    2. Смотрим на количество consecutive bearish свечей BTC          ║
 * ║    3. При BTC_STRONG_DOWN + velocity > порога:                      ║
 * ║       - RS 0.75+ → LONG weight снижен до 0.45 (было 0.70)          ║
 * ║       - RS 0.85+ + сектор leading → LONG weight 0.60               ║
 * ║       - при 3+ consecutive BTC bear свечах → LONG weight 0.25       ║
 * ║    4. Новое поле btcDropVelocity в GlobalContext                    ║
 * ║    5. Новый метод isCascadeDumpRisk() — публичный для SignalSender  ║
 * ║                                                                      ║
 * ║  [FIX-4] Relative Strength — multi-window (5 / 10 / 20 свечей)    ║
 * ║    Раньше: только скользящее среднее RS за 20 свечей                ║
 * ║    Теперь: взвешенное среднее: RS5 × 0.5 + RS10 × 0.3 + RS20 × 0.2║
 * ║    Это даёт приоритет свежей силе монеты при расчёте веса           ║
 * ║                                                                      ║
 * ║  [NEW] btcDropVelocity в GlobalContext                              ║
 * ║    Скорость падения = изменение momentum за последние 3 свечи       ║
 * ║    Используется внешними модулями (SignalSender)                    ║
 * ║                                                                      ║
 * ║  СОХРАНЕНО: все режимы / волатильностный режим / sector divergence  ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 */
public final class GlobalImpulseController {

    private final int VOL_LOOKBACK;
    private final int BODY_LOOKBACK;

    // История ATR BTC для расчёта режима волатильности
    private final Deque<Double> btcAtrHistory = new ArrayDeque<>();
    private static final int ATR_HISTORY_SIZE = 96; // 96 × 15m = 24h

    // [FIX-BUG-1] История движений BTC для cascade detection
    private final Deque<Double> btcMoveHistory  = new ArrayDeque<>();
    private static final int    BTC_MOVE_WINDOW = 8; // 8 свечей = 2 часа
    // Счётчик consecutive BTC bear свечей
    private volatile int btcConsecutiveBearBars = 0;
    // Скорость падения — насколько быстро ускоряется падение
    private volatile double btcDropVelocity = 0.0;

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
        NEUTRAL,
        BTC_IMPULSE_UP,
        BTC_IMPULSE_DOWN,
        BTC_STRONG_UP,
        BTC_STRONG_DOWN
    }

    public enum VolatilityRegime {
        LOW,
        NORMAL,
        HIGH,
        EXTREME
    }

    // ══════════════════════════════════════════════════════════════
    //  GlobalContext
    // ══════════════════════════════════════════════════════════════

    public static final class GlobalContext {
        public final GlobalRegime      regime;
        public final double            impulseStrength;
        public final double            volatilityExpansion;
        public final boolean           strongPressure;
        public final boolean           onlyLong;
        public final boolean           onlyShort;
        public final double            btcTrend;
        public final VolatilityRegime  volRegime;
        public final double            confidenceAdjustment;

        /** [NEW v5.0] Скорость падения BTC. 0 = нет падения. > 0.5 = опасный cascade */
        public final double            btcDropVelocity;
        /** [NEW v5.0] Количество consecutive медвежьих свечей BTC */
        public final int               btcConsecutiveBearBars;

        public GlobalContext(GlobalRegime regime, double impulseStrength,
                             double volatilityExpansion, boolean strongPressure,
                             boolean onlyLong, boolean onlyShort, double btcTrend,
                             VolatilityRegime volRegime, double confidenceAdjustment,
                             double btcDropVelocity, int btcConsecutiveBearBars) {
            this.regime                  = regime;
            this.impulseStrength         = impulseStrength;
            this.volatilityExpansion     = volatilityExpansion;
            this.strongPressure          = strongPressure;
            this.onlyLong                = onlyLong;
            this.onlyShort               = onlyShort;
            this.btcTrend                = btcTrend;
            this.volRegime               = volRegime;
            this.confidenceAdjustment    = confidenceAdjustment;
            this.btcDropVelocity         = btcDropVelocity;
            this.btcConsecutiveBearBars  = btcConsecutiveBearBars;
        }

        /** Обратная совместимость — без новых полей */
        public GlobalContext(GlobalRegime regime, double impulseStrength,
                             double volatilityExpansion, boolean strongPressure,
                             boolean onlyLong, boolean onlyShort, double btcTrend,
                             VolatilityRegime volRegime, double confidenceAdjustment) {
            this(regime, impulseStrength, volatilityExpansion, strongPressure,
                    onlyLong, onlyShort, btcTrend, volRegime, confidenceAdjustment, 0.0, 0);
        }

        /** Обратная совместимость (минимальный) */
        public GlobalContext(GlobalRegime regime, double impulseStrength,
                             double volatilityExpansion, boolean strongPressure,
                             boolean onlyLong, boolean onlyShort, double btcTrend) {
            this(regime, impulseStrength, volatilityExpansion, strongPressure,
                    onlyLong, onlyShort, btcTrend, VolatilityRegime.NORMAL, 0.0, 0.0, 0);
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
            VolatilityRegime.NORMAL, 0.0, 0.0, 0
    );

    private final Map<String, SectorContext>  sectorMap      = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>>  sectorBiasHist = new ConcurrentHashMap<>();

    // [FIX-4] Multi-window RS — три горизонта для взвешенного расчёта
    private final Deque<Double> btcReturnHistory5  = new ArrayDeque<>();  // 5 свечей
    private final Deque<Double> btcReturnHistory10 = new ArrayDeque<>();  // 10 свечей
    private final Deque<Double> btcReturnHistory20 = new ArrayDeque<>();  // 20 свечей

    // ══════════════════════════════════════════════════════════════
    //  UPDATE — вызывается из BotMain с BTC свечами
    // ══════════════════════════════════════════════════════════════

    public void update(List<TradingCore.Candle> btcCandles) {
        if (btcCandles == null || btcCandles.size() < 30) return;

        int n = btcCandles.size();
        TradingCore.Candle last = btcCandles.get(n - 1);
        TradingCore.Candle prev1 = btcCandles.get(n - 2);
        TradingCore.Candle prev2 = btcCandles.get(n - 3);

        double atr14    = atr(btcCandles, 14);
        double avgVol   = avgVolume(btcCandles, VOL_LOOKBACK);
        double curVol   = last.volume;
        double volRatio = curVol / (avgVol + 1e-9);

        // Движения
        double move3    = (last.close - btcCandles.get(n - 4).close) / (btcCandles.get(n - 4).close + 1e-9);
        double move1    = (last.close - prev1.close) / (prev1.close + 1e-9);   // последняя свеча
        double move2    = (prev1.close - prev2.close) / (prev2.close + 1e-9);  // предпоследняя

        // [FIX-BUG-1] Cascade detection: скорость ускорения падения
        // Если move1 < move2 < move3prev — падение ускоряется
        btcMoveHistory.addLast(move1);
        if (btcMoveHistory.size() > BTC_MOVE_WINDOW) btcMoveHistory.removeFirst();

        // Обновляем счётчик consecutive bear bars
        if (last.close < last.open) {
            btcConsecutiveBearBars = Math.min(btcConsecutiveBearBars + 1, 10);
        } else {
            btcConsecutiveBearBars = 0;
        }

        // Velocity: насколько ускоряется медвежье движение
        // Берём разность: текущий move3 минус move3 с задержкой в 3 свечи
        double olderMove3 = btcCandles.size() >= 7
                ? (btcCandles.get(n - 4).close - btcCandles.get(n - 7).close) / (btcCandles.get(n - 7).close + 1e-9)
                : 0.0;
        // Velocity > 0 = падение ускоряется
        btcDropVelocity = move3 < 0 ? Math.max(0.0, Math.abs(move3) - Math.abs(olderMove3)) : 0.0;

        // Тело свечей
        double bodyPct   = avgBodyPct(btcCandles, BODY_LOOKBACK);
        double curBody   = Math.abs(last.close - last.open) / (last.close + 1e-9);
        double bodyRatio = curBody / (bodyPct + 1e-9);

        // EMA тренд BTC
        double ema20     = ema(btcCandles, 20);
        double ema50     = ema(btcCandles, 50);
        double btcTrend  = (ema20 - ema50) / (ema50 + 1e-9) * 100;

        // Сила импульса
        double rawStrength = Math.min(1.0,
                (Math.abs(move3) / 0.012) * 0.45 +
                        (Math.min(volRatio, 4.0) / 4.0) * 0.35 +
                        (Math.min(bodyRatio, 3.0) / 3.0) * 0.20
        );

        // ATR история для волатильности
        btcAtrHistory.addLast(atr14 / last.close);
        if (btcAtrHistory.size() > ATR_HISTORY_SIZE) btcAtrHistory.removeFirst();

        VolatilityRegime volRegime = calcVolatilityRegime(atr14 / last.close);
        double confAdj = switch (volRegime) {
            case LOW     -> -1.5;
            case NORMAL  ->  0.0;
            case HIGH    -> +2.0;
            case EXTREME -> +5.0;
        };

        double volExpansion = atr14 / (atr(btcCandles.subList(Math.max(0, n - 50), n - 10), 14) + 1e-9);

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
        boolean onlyLong  = regime == GlobalRegime.BTC_STRONG_UP  && rawStrength > 0.80;
        boolean onlyShort = regime == GlobalRegime.BTC_STRONG_DOWN && rawStrength > 0.80;

        // [FIX-4] Обновляем multi-window BTC return history
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
                btcDropVelocity, btcConsecutiveBearBars
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

        double emaBias  = (ema10 - ema25) / (ema25 + 1e-9) * 20;
        double moveBias = move5 / 0.01;
        double rawBias  = clamp((emaBias + moveBias) / 2, -1.0, 1.0);

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

        // Используем multi-window BTC return для определения "leading"
        double btcReturn5 = btcReturnHistory5.stream().mapToDouble(Double::doubleValue).sum();
        boolean leading = move5 > btcReturn5 * 1.15 && rawBias > 0;

        double strength = atrNorm > 0.012 ? 1.2 : atrNorm > 0.007 ? 0.9 : 0.6;

        sectorMap.put(sector, new SectorContext(sector, rawBias, momentum, strength, leading));
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-BUG-1] CASCADE DUMP RISK DETECTION
    //  Публичный метод для проверки наличия cascade risk.
    //  Используется в SignalSender перед отправкой LONG сигнала.
    // ══════════════════════════════════════════════════════════════

    /**
     * Определяет, есть ли риск cascade dump для LONG позиции.
     *
     * Cascade dump — это явление, при котором:
     * 1. BTC начинает активно падать
     * 2. Альткоины держатся 1-3 свечи (кажутся "сильными")
     * 3. Затем они резко капитулируют, догоняя падение BTC
     *
     * Возвращает risk score от 0.0 (нет риска) до 1.0 (максимальный риск).
     *
     * @param relStrength Relative Strength символа (0..1)
     */
    public double getCascadeDumpRisk(double relStrength) {
        GlobalContext ctx = currentContext;
        if (ctx.regime != GlobalRegime.BTC_STRONG_DOWN &&
                ctx.regime != GlobalRegime.BTC_IMPULSE_DOWN) {
            return 0.0;
        }

        double risk = 0.0;

        // Фактор 1: сила падения BTC
        if (ctx.regime == GlobalRegime.BTC_STRONG_DOWN) {
            risk += 0.35 * ctx.impulseStrength;
        } else {
            risk += 0.15 * ctx.impulseStrength;
        }

        // Фактор 2: скорость ускорения падения
        // btcDropVelocity > 0.004 = падение ускоряется — опасно
        if (ctx.btcDropVelocity > 0.006) {
            risk += 0.35;
        } else if (ctx.btcDropVelocity > 0.003) {
            risk += 0.20;
        } else if (ctx.btcDropVelocity > 0.001) {
            risk += 0.10;
        }

        // Фактор 3: consecutive bear bars
        // 3+ красных свечей BTC подряд = начало cascade
        int bearBars = ctx.btcConsecutiveBearBars;
        if (bearBars >= 4) {
            risk += 0.30;
        } else if (bearBars >= 3) {
            risk += 0.20;
        } else if (bearBars >= 2) {
            risk += 0.10;
        }

        // Фактор 4: Relative Strength "сопротивление" — парадоксально опасно
        // Монета с RS > 0.75 при сильном падении BTC — скорее всего ещё не капитулировала
        // Это увеличивает риск cascade, а не уменьшает
        if (relStrength > 0.80 && ctx.regime == GlobalRegime.BTC_STRONG_DOWN) {
            // Монета "держится" при сильном дампе BTC → высокий отложенный риск
            risk += 0.15;
        }

        // Фактор 5: волатильность
        if (ctx.volRegime == VolatilityRegime.EXTREME) {
            risk += 0.15;
        } else if (ctx.volRegime == VolatilityRegime.HIGH) {
            risk += 0.08;
        }

        return clamp(risk, 0.0, 1.0);
    }

    /**
     * Convenience метод — возвращает true если cascade risk высокий.
     * @param relStrength RS символа
     * @param threshold   порог риска (рекомендуется 0.55 для обычного режима)
     */
    public boolean isCascadeDumpRisk(double relStrength, double threshold) {
        return getCascadeDumpRisk(relStrength) >= threshold;
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-BUG-1 + FIX-4] FILTER WEIGHT — плавный коэффициент
    // ══════════════════════════════════════════════════════════════

    /**
     * Возвращает вес фильтрации сигнала от 0.0 до 1.2.
     *
     * [v5.0 изменения]:
     * - BTC_STRONG_DOWN + LONG: теперь учитываем btcDropVelocity и bearBars
     * - RS 0.75+ НЕ является достаточным основанием для полноценного LONG
     *   при сильном дампе с ускорением — cascade dump risk слишком высок
     * - Только RS > 0.85 + sector leading + нет acceleration → 0.60
     *
     * [FIX-4] RS теперь из multi-window (calculateWeightedRS используется
     * в DecisionEngineMerged, здесь принимаем уже взвешенный RS)
     */
    public double getFilterWeight(String symbol, boolean isLong, double relStrength, String sectorName) {
        GlobalContext ctx = currentContext;
        double weight = 1.0;

        switch (ctx.regime) {
            case BTC_STRONG_UP -> {
                if (!isLong) {
                    if (relStrength < 0.25) {
                        weight = 0.75;
                    } else if (relStrength < 0.45) {
                        weight = 0.55;
                    } else {
                        weight = 0.35;
                    }
                } else {
                    weight = 1.0 + (ctx.impulseStrength - 0.65) * 0.3;
                    weight = Math.min(weight, 1.15);
                }
            }

            case BTC_STRONG_DOWN -> {
                if (isLong) {
                    // [FIX-BUG-1] Ключевое изменение v5.0
                    // Старый код: RS > 0.75 → weight = 0.70 (слишком щедро при cascade)
                    // Новый код: учитываем скорость и consecutive bars

                    double cascadeRisk = getCascadeDumpRisk(relStrength);

                    if (cascadeRisk >= 0.65) {
                        // Высокий риск cascade — практически блокируем LONG
                        weight = 0.10;
                    } else if (cascadeRisk >= 0.45) {
                        // Умеренный cascade риск
                        if (relStrength > 0.85) {
                            // Очень сильная монета — даём небольшой шанс
                            SectorContext sc = sectorName != null ? sectorMap.get(sectorName) : null;
                            if (sc != null && sc.bias > 0.5 && sc.leading) {
                                weight = 0.45; // Сектор тоже держится
                            } else {
                                weight = 0.30;
                            }
                        } else {
                            weight = 0.15;
                        }
                    } else {
                        // Низкий cascade риск (медленное падение, 0-1 bear bar)
                        if (relStrength > 0.80) {
                            SectorContext sc = sectorName != null ? sectorMap.get(sectorName) : null;
                            if (sc != null && sc.bias > 0.4 && sc.leading) {
                                weight = 0.65; // Монета + сектор сильнее BTC
                            } else {
                                weight = 0.50;
                            }
                        } else if (relStrength > 0.65) {
                            weight = 0.35;
                        } else if (relStrength > 0.50) {
                            weight = 0.18;
                        } else {
                            weight = 0.0; // Слабая монета + BTC падает = нет LONG
                        }
                    }
                } else {
                    // SHORT при BTC_STRONG_DOWN — усиливаем
                    weight = 1.0 + (ctx.impulseStrength - 0.65) * 0.35;
                    // [FIX-BUG-1] При cascade acceleration — SHORT ещё сильнее
                    if (ctx.btcDropVelocity > 0.003) {
                        weight = Math.min(weight * 1.15, 1.35);
                    }
                    weight = Math.min(weight, 1.35);
                }
            }

            case BTC_IMPULSE_UP -> {
                if (!isLong) weight = 0.72;
            }

            case BTC_IMPULSE_DOWN -> {
                if (isLong) {
                    // При импульсном (не сильном) падении BTC — проверяем cascade
                    double cascadeRisk = getCascadeDumpRisk(relStrength);
                    if (cascadeRisk >= 0.50) {
                        weight = 0.30;
                    } else {
                        weight = relStrength > 0.65 ? 0.80 : 0.55;
                    }
                }
            }

            case NEUTRAL -> {
                // Нейтральный режим — без изменений
            }
        }

        // Корректировка по волатильностному режиму
        if (ctx.volRegime == VolatilityRegime.EXTREME) {
            weight *= 0.60;
        } else if (ctx.volRegime == VolatilityRegime.HIGH) {
            weight *= 0.85;
        }

        return clamp(weight, 0.0, 1.35);
    }

    /**
     * Устаревший метод — обратная совместимость.
     * @deprecated используйте getFilterWeight(symbol, isLong, relStrength, sectorName)
     */
    @Deprecated
    public double filterSignal(String symbol, boolean isLong, double confidence,
                               DecisionEngineMerged.CoinCategory cat) {
        double rs = 0.5;
        double weight = getFilterWeight(symbol, isLong, rs, null);
        return confidence * weight;
    }

    // ══════════════════════════════════════════════════════════════
    //  [FIX-4] MULTI-WINDOW RELATIVE STRENGTH
    //  Рассчитывает взвешенный RS по трём горизонтам.
    //  Вызывается из DecisionEngineMerged при updateRelativeStrength.
    // ══════════════════════════════════════════════════════════════

    /**
     * Возвращает взвешенный Relative Strength с приоритетом недавней динамики.
     * RS5 × 0.5 + RS10 × 0.3 + RS20 × 0.2
     *
     * Это исправляет проблему: монета может держаться последние 2 свечи (RS5 высокий),
     * но за 20 свечей она слабее BTC (RS20 низкий). Взвешенный RS даёт правдивую картину.
     *
     * @param symbolReturns20 список из 20 последних 15m return монеты
     */
    public double calculateWeightedRS(List<Double> symbolReturns20) {
        if (symbolReturns20 == null || symbolReturns20.isEmpty()) return 0.5;

        int size = symbolReturns20.size();

        // RS за 5 свечей
        double rs5 = calculateRSForWindow(symbolReturns20,
                btcReturnHistory5, Math.min(5, size));

        // RS за 10 свечей
        double rs10 = calculateRSForWindow(symbolReturns20,
                btcReturnHistory10, Math.min(10, size));

        // RS за 20 свечей
        double rs20 = calculateRSForWindow(symbolReturns20,
                btcReturnHistory20, Math.min(20, size));

        // Взвешенное среднее с приоритетом краткосрочного RS
        return rs5 * 0.50 + rs10 * 0.30 + rs20 * 0.20;
    }

    private double calculateRSForWindow(List<Double> symbolReturns,
                                        Deque<Double> btcReturns,
                                        int window) {
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
            // Монета растёт при падении BTC — очень сильный RS
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
                .average()
                .orElse(0.0);
    }

    public double getSectorWeakness(String sectorName) {
        if (sectorMap.size() < 3 || sectorName == null) return 0.0;

        SectorContext target = sectorMap.get(sectorName);
        if (target == null) return 0.0;

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

    // ══════════════════════════════════════════════════════════════
    //  GETTERS
    // ══════════════════════════════════════════════════════════════

    public GlobalContext getContext() { return currentContext; }

    public String getStats() {
        GlobalContext ctx = currentContext;
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("GIC[%s str=%.2f vol=%.2f volReg=%s confAdj=%+.1f bearBars=%d vel=%.4f]",
                ctx.regime, ctx.impulseStrength, ctx.volatilityExpansion,
                ctx.volRegime, ctx.confidenceAdjustment,
                ctx.btcConsecutiveBearBars, ctx.btcDropVelocity));

        if (!sectorMap.isEmpty()) {
            sb.append(" Sectors:");
            sectorMap.forEach((s, sc) ->
                    sb.append(String.format(" %s=%.2f%s", s, sc.bias, sc.leading ? "↑" : "")));
        }

        double mktBias = getMarketBias();
        sb.append(String.format(" MktBias=%.2f", mktBias));

        // [FIX-BUG-1] Каскадное предупреждение в логах
        double neutralRS = 0.5;
        double cascadeRisk = getCascadeDumpRisk(neutralRS);
        if (cascadeRisk > 0.45) {
            sb.append(String.format(" ⚠️CASCADE_RISK=%.2f", cascadeRisk));
        }

        return sb.toString();
    }

    // ══════════════════════════════════════════════════════════════
    //  VOLATILITY REGIME CALCULATOR
    // ══════════════════════════════════════════════════════════════

    private VolatilityRegime calcVolatilityRegime(double currentAtrPct) {
        if (btcAtrHistory.size() < 20) return VolatilityRegime.NORMAL;

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