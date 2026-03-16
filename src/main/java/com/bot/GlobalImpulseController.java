package com.bot;

import com.bot.TradingCore;
import java.util.List;

public final class GlobalImpulseController {

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

    public enum GlobalRegime {
        NEUTRAL,
        BTC_IMPULSE_UP,
        BTC_IMPULSE_DOWN,
        BTC_STRONG_UP,
        BTC_STRONG_DOWN
    }

    public static final class GlobalContext {
        public final GlobalRegime regime;
        public final double impulseStrength;
        public final double volatilityExpansion;
        public final boolean strongPressure;
        public final boolean onlyLong;
        public final boolean onlyShort;
        public final double btcTrend;

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

        public GlobalContext(GlobalRegime regime,
                             double impulseStrength,
                             double volatilityExpansion,
                             boolean strongPressure,
                             boolean onlyLong,
                             boolean onlyShort) {
            this(regime, impulseStrength, volatilityExpansion, strongPressure, onlyLong, onlyShort, 0);
        }
    }

    private GlobalContext current =
            new GlobalContext(GlobalRegime.NEUTRAL, 0.0, 1.0, false, false, false, 0);

    private double prevImpulseStrength = 0.0;

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

        double btcTrend = calculateBtcTrend(btc);

        double rawScore = 0.40 * volatilityExpansion + 0.35 * bodyExpansion + 0.25 * volumeSpike;

        double impulseStrength = normalize(rawScore);
        boolean bodyOk = bodyExpansion >= 0.70;

        GlobalRegime regime = determineRegime(btc, impulseStrength, bodyOk, btcTrend);

        boolean strong = impulseStrength > 0.70 && volatilityExpansion > 1.4;

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

        this.prevImpulseStrength = this.current.impulseStrength;
    }

    public GlobalContext getContext() {
        return current;
    }

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

        double diff = (ema20 - ema50) / ema50;
        return Math.max(-1.0, Math.min(1.0, diff * 100));
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

        if (!bodyOk || strength < 0.45) {
            return GlobalRegime.NEUTRAL;
        }

        int size = btc.size();

        double move = btc.get(size - 1).close - btc.get(size - 4).close;
        double movePct = Math.abs(move) / btc.get(size - 4).close;

        // === УЛУЧШЕНО: Более строгие критерии для STRONG режима ===
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
        double n = value / 1.8;
        return Math.min(1.0, Math.max(0.0, n));
    }

    public double filterSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        if (signal == null) return 0;

        GlobalContext ctx = current;

        boolean impulseFading = prevImpulseStrength > ctx.impulseStrength && ctx.impulseStrength > 0.5;

        if (ctx.regime == GlobalRegime.NEUTRAL) {
            return 1.0;
        }

        boolean isLong = signal.side == TradingCore.Side.LONG;
        boolean isShort = signal.side == TradingCore.Side.SHORT;

        if (ctx.regime == GlobalRegime.BTC_STRONG_UP && isShort) {
            // Улучшение: если волатильность растет, разрешаем шорт смелее (0.45 вместо 0.15)
            double volatilityBonus = ctx.volatilityExpansion > 1.2 ? 0.30 : 0.0;
            double threshold = signal.symbol.contains("BTC") ? 80 : 70;
            if (signal.probability >= threshold) return 0.75;
            if (ctx.volatilityExpansion > 1.5) return 0.9;
            return 0.40;
        }

        if (ctx.regime == GlobalRegime.BTC_STRONG_DOWN && isLong) {
            double threshold = signal.symbol.contains("BTC") ? 82 : 75;
            if (signal.probability >= threshold) return 0.60;
            return 0.15;
        }

        if (ctx.regime == GlobalRegime.BTC_IMPULSE_UP && isShort) {
            if (impulseFading) return signal.probability >= 70 ? 0.80 : 0.40;
            // Оставляем хотя бы 0.25, чтобы супер-сильные альтовые сетапы проходили
            return signal.probability >= 72 ? 0.70 : 0.25;
        }

        if (ctx.regime == GlobalRegime.BTC_IMPULSE_DOWN && isLong) {
            if (impulseFading) return signal.probability >= 70 ? 0.80 : 0.40;
            return signal.probability >= 72 ? 0.70 : 0.25;
        }

        return 1.0;
    }
}