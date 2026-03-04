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
        BTC_IMPULSE_DOWN
    }

    // ================= CONTEXT =================
    public static final class GlobalContext {
        public final GlobalRegime regime;
        public final double impulseStrength;     // 0.0 – 1.0
        public final double volatilityExpansion;
        public final boolean strongPressure;
        public final boolean onlyLong;           // для Decision Engine
        public final boolean onlyShort;

        public GlobalContext(GlobalRegime regime,
                             double impulseStrength,
                             double volatilityExpansion,
                             boolean strongPressure,
                             boolean onlyLong,
                             boolean onlyShort) {

            this.regime = regime;
            this.impulseStrength = impulseStrength;
            this.volatilityExpansion = volatilityExpansion;
            this.strongPressure = strongPressure;
            this.onlyLong = onlyLong;
            this.onlyShort = onlyShort;
        }
    }

    // ================= STATE =================
    private GlobalContext current =
            new GlobalContext(GlobalRegime.NEUTRAL, 0.0, 1.0, false, false, false);

    // ================= UPDATE =================
    public void update(List<TradingCore.Candle> btc) {

        if (btc == null || btc.size() < VOL_LOOKBACK + 2)
            return;

        double avgRange = averageRange(btc, VOL_LOOKBACK);
        TradingCore.Candle last = btc.get(btc.size() - 1);

        double currentRange = last.high - last.low;
        double volatilityExpansion = currentRange / avgRange;

        double bodyExpansion = bodyExpansionScore(btc);
        double volumeSpike = volumeSpikeScore(btc);

        // сырое объединение факторов
        double rawScore = 0.45 * volatilityExpansion + 0.35 * bodyExpansion + 0.20 * volumeSpike;

        double impulseStrength = normalize(rawScore);

        // проверка размера тела свечи для надежного сигнала
        double lastBody = Math.abs(last.close - last.open);
        boolean bodyOk = lastBody >= Math.max(bodyExpansion * 0.8, 0.0001);

        GlobalRegime regime = determineRegime(last, impulseStrength, bodyOk);

        boolean strong = impulseStrength > 0.65 && volatilityExpansion > 1.25;

        boolean onlyLong = regime == GlobalRegime.BTC_IMPULSE_UP;
        boolean onlyShort = regime == GlobalRegime.BTC_IMPULSE_DOWN;

        current = new GlobalContext(
                regime,
                impulseStrength,
                volatilityExpansion,
                strong,
                onlyLong,
                onlyShort
        );
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

    private GlobalRegime determineRegime(TradingCore.Candle last, double strength, boolean bodyOk) {
        if (!bodyOk || strength < 0.45) return GlobalRegime.NEUTRAL;
        boolean bullish = last.close > last.open;
        boolean bearish = last.close < last.open;
        if (bullish && strength > 0.6) return GlobalRegime.BTC_IMPULSE_UP;
        if (bearish && strength > 0.6) return GlobalRegime.BTC_IMPULSE_DOWN;
        return GlobalRegime.NEUTRAL;
    }

    private double normalize(double value) {
        double n = value / 1.5; // корректировка диапазона для более частых сигналов
        return Math.min(1.0, Math.max(0.0, n));
    }
}