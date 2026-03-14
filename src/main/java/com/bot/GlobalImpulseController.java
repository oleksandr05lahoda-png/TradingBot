package com.bot;

import com.bot.TradingCore;
import java.util.List;

public final class GlobalImpulseController {

    // ================= CONFIG =================
    private final int VOL_LOOKBACK;
    private final int BODY_LOOKBACK;

    public GlobalImpulseController() {
        this.VOL_LOOKBACK = 20;
        this.BODY_LOOKBACK = 10;
    }

    public GlobalImpulseController(int volLookback, int bodyLookback) {
        this.VOL_LOOKBACK = volLookback;
        this.BODY_LOOKBACK = bodyLookback;
    }

    // ================= ENUMS =================
    public enum GlobalRegime {
        NEUTRAL,
        BTC_IMPULSE_UP,
        BTC_IMPULSE_DOWN,
        BTC_STRONG_UP,      // Новый - очень сильный импульс
        BTC_STRONG_DOWN     // Новый - очень сильный импульс
    }

    // ================= CONTEXT =================
    public static final class GlobalContext {
        public final GlobalRegime regime;
        public final double impulseStrength;
        public final double volatilityExpansion;
        public final boolean strongPressure;
        public final boolean onlyLong;
        public final boolean onlyShort;
        public final double btcTrend;  // Новое: направление тренда BTC

        public GlobalContext(GlobalRegime regime,
                             double impulseStrength,
                             double volatilityExpansion,
                             boolean strongPressure,
                             boolean onlyLong,
                             boolean onlyShort,
                             double btcTrend) {

            this.regime = regime;
            this.impulseStrength = impulseStrength;
            this.volatilityExpansion = volatilityExpansion;
            this.strongPressure = strongPressure;
            this.onlyLong = onlyLong;
            this.onlyShort = onlyShort;
            this.btcTrend = btcTrend;
        }

        // Совместимость со старым кодом
        public GlobalContext(GlobalRegime regime,
                             double impulseStrength,
                             double volatilityExpansion,
                             boolean strongPressure,
                             boolean onlyLong,
                             boolean onlyShort) {
            this(regime, impulseStrength, volatilityExpansion, strongPressure, onlyLong, onlyShort, 0);
        }
    }

    // ================= STATE =================
    private GlobalContext current =
            new GlobalContext(GlobalRegime.NEUTRAL, 0.0, 1.0, false, false, false, 0);
    // [MOD] Для отслеживания изменения импульса
    private double prevImpulseStrength = 0.0;

    // ================= UPDATE =================
    public void update(List<TradingCore.Candle> btc) {

        if (btc == null || btc.size() < VOL_LOOKBACK + 5) {
            return;
        }

        double avgRange = Math.max(averageRange(btc, VOL_LOOKBACK), 0.0000001);
        TradingCore.Candle last = btc.get(btc.size() - 1);

        double currentRange = last.high - last.low;
        double volatilityExpansion = currentRange / avgRange;

        double bodyExpansion = bodyExpansionScore(btc);
        double volumeSpike = volumeSpikeScore(btc);

        // Тренд BTC (EMA20 vs EMA50)
        double btcTrend = calculateBtcTrend(btc);

        // Объединение факторов (более мягкое)
        double rawScore = 0.40 * volatilityExpansion + 0.35 * bodyExpansion + 0.25 * volumeSpike;

        double impulseStrength = normalize(rawScore);
        boolean bodyOk = bodyExpansion >= 0.70; // Смягчили с 0.8

        GlobalRegime regime = determineRegime(btc, impulseStrength, bodyOk, btcTrend);

        // Смягченная логика давления
        boolean strong = impulseStrength > 0.70 && volatilityExpansion > 1.4;

        // onlyLong/onlyShort теперь только при СИЛЬНОМ импульсе
        boolean onlyLong = regime == GlobalRegime.BTC_STRONG_UP;
        boolean onlyShort = regime == GlobalRegime.BTC_STRONG_DOWN;

        current = new GlobalContext(
                regime,
                impulseStrength,
                volatilityExpansion,
                strong,
                onlyLong,
                onlyShort,
                btcTrend
        );

        // [MOD] Сохраняем предыдущую силу импульса для анализа затухания
        this.prevImpulseStrength = this.current.impulseStrength;
    }

    public GlobalContext getContext() {
        return current;
    }

    // ================= HELPERS =================
    private double averageRange(List<TradingCore.Candle> candles, int lookback) {
        int size = candles.size();
        double sum = 0.0;
        for (int i = size - lookback; i < size; i++) {
            TradingCore.Candle c = candles.get(i);
            sum += (c.high - c.low);
        }
        return sum / lookback;
    }

    private double bodyExpansionScore(List<TradingCore.Candle> candles) {
        int size = candles.size();
        double sum = 0.0;
        for (int i = size - BODY_LOOKBACK; i < size; i++) {
            TradingCore.Candle c = candles.get(i);
            sum += Math.abs(c.close - c.open);
        }
        double avgBody = sum / BODY_LOOKBACK;
        TradingCore.Candle last = candles.get(size - 1);
        double lastBody = Math.abs(last.close - last.open);
        return avgBody == 0 ? 1.0 : lastBody / avgBody;
    }

    private double volumeSpikeScore(List<TradingCore.Candle> candles) {
        int size = candles.size();
        double sum = 0.0;
        for (int i = size - VOL_LOOKBACK; i < size; i++) {
            sum += candles.get(i).volume;
        }
        double avgVol = sum / VOL_LOOKBACK;
        double lastVol = candles.get(size - 1).volume;
        return avgVol == 0 ? 1.0 : lastVol / avgVol;
    }

    private double calculateBtcTrend(List<TradingCore.Candle> btc) {
        if (btc.size() < 50) return 0;

        double ema20 = ema(btc, 20);
        double ema50 = ema(btc, 50);

        // Возвращаем нормализованное значение тренда
        double diff = (ema20 - ema50) / ema50;
        return Math.max(-1.0, Math.min(1.0, diff * 100)); // -1 to 1
    }

    private double ema(List<TradingCore.Candle> candles, int period) {
        if (candles.size() < period) return candles.get(candles.size() - 1).close;
        double k = 2.0 / (period + 1);
        double e = candles.get(candles.size() - period).close;
        for (int i = candles.size() - period + 1; i < candles.size(); i++) {
            e = candles.get(i).close * k + e * (1 - k);
        }
        return e;
    }

    private GlobalRegime determineRegime(List<TradingCore.Candle> btc,
                                         double strength,
                                         boolean bodyOk,
                                         double btcTrend) {

        // Нет сигнала если нет тела свечи или слабый импульс
        if (!bodyOk || strength < 0.45) {
            return GlobalRegime.NEUTRAL;
        }

        int size = btc.size();

        // Движение за последние 3 свечи
        double move = btc.get(size - 1).close - btc.get(size - 4).close;
        double movePct = Math.abs(move) / btc.get(size - 4).close;

        // Сильный импульс (>0.8% за 3 свечи + strength > 0.70)
        if (movePct > 0.008 && strength > 0.70) {
            if (move > 0) return GlobalRegime.BTC_STRONG_UP;
            else return GlobalRegime.BTC_STRONG_DOWN;
        }

        // Обычный импульс
        if (move > 0 && strength > 0.50) {
            return GlobalRegime.BTC_IMPULSE_UP;
        }

        if (move < 0 && strength > 0.50) {
            return GlobalRegime.BTC_IMPULSE_DOWN;
        }

        return GlobalRegime.NEUTRAL;
    }

    private double normalize(double value) {
        // Более мягкая нормализация
        double n = value / 1.8;
        return Math.min(1.0, Math.max(0.0, n));
    }

    // ================= FILTER HELPER =================
    /**
     * Проверяет, разрешён ли сигнал с учётом глобального контекста.
     * Возвращает коэффициент confidence (1.0 = без изменений, <1.0 = снижение)
     */
    public double filterSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        if (signal == null) return 0;

        GlobalContext ctx = current;

        // [MOD] Определяем, ослабевает ли импульс
        boolean impulseFading = prevImpulseStrength > ctx.impulseStrength && ctx.impulseStrength > 0.5;

        // Нейтральный режим - пропускаем всё
        if (ctx.regime == GlobalRegime.NEUTRAL) {
            return 1.0;
        }

        boolean isLong = signal.side == TradingCore.Side.LONG;
        boolean isShort = signal.side == TradingCore.Side.SHORT;

        // Сильный импульс вверх - шорты только с высокой вероятностью
        if (ctx.regime == GlobalRegime.BTC_STRONG_UP && isShort) {
            return signal.probability >= 72 ? 0.85 : 0.0;
        }

        // Сильный импульс вниз - лонги только с высокой вероятностью
        if (ctx.regime == GlobalRegime.BTC_STRONG_DOWN && isLong) {
            return signal.probability >= 72 ? 0.85 : 0.0;
        }

        // Обычный импульс вверх - немного снижаем шорты
        if (ctx.regime == GlobalRegime.BTC_IMPULSE_UP && isShort) {
            // [MOD] Если импульс ослабевает, контр-тренд становится чуть допустимее
            if (impulseFading) {
                return 0.9;
            }
            return signal.probability >= 65 ? 0.92 : 0.75;
        }

        // Обычный импульс вниз - немного снижаем лонги
        if (ctx.regime == GlobalRegime.BTC_IMPULSE_DOWN && isLong) {
            if (impulseFading) {
                return 0.9;
            }
            return signal.probability >= 65 ? 0.92 : 0.75;
        }

        return 1.0;
    }
}