package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║       GlobalImpulseController — GODBOT EDITION                  ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  Функции:                                                        ║
 * ║  1. Определяет глобальный режим BTC (5 режимов)                  ║
 * ║  2. Отслеживает силу и волатильность BTC импульса                ║
 * ║  3. Ведёт секторные контексты (6 секторов: MEME/L1/DEFI/etc)    ║
 * ║  4. filterSignal() — коэффициент фильтрации для каждого сигнала  ║
 * ║                                                                  ║
 * ║  ВАЖНО: Создаётся ОДИН раз в BotMain и передаётся               ║
 * ║  как shared объект в SignalSender. Нет дублирования.            ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */
public final class GlobalImpulseController {

    // ── Параметры ──────────────────────────────────────────────────
    private final int VOL_LOOKBACK;
    private final int BODY_LOOKBACK;

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
        BTC_STRONG_UP,     // BTC сильный рост (>0.8% за 3 свечи + strength>0.7)
        BTC_STRONG_DOWN    // BTC сильное падение
    }

    // ══════════════════════════════════════════════════════════════
    //  GlobalContext
    // ══════════════════════════════════════════════════════════════

    public static final class GlobalContext {
        public final GlobalRegime regime;
        public final double impulseStrength;      // 0..1
        public final double volatilityExpansion;  // 1.0 = норма, >1.5 = расширение
        public final boolean strongPressure;      // impulse>0.70 && vol>1.4
        public final boolean onlyLong;            // BTC_STRONG_UP
        public final boolean onlyShort;           // BTC_STRONG_DOWN
        public final double btcTrend;             // -1..+1 (EMA20/EMA50)

        public GlobalContext(GlobalRegime regime, double impulseStrength,
                             double volatilityExpansion, boolean strongPressure,
                             boolean onlyLong, boolean onlyShort, double btcTrend) {
            this.regime             = regime;
            this.impulseStrength    = impulseStrength;
            this.volatilityExpansion = volatilityExpansion;
            this.strongPressure     = strongPressure;
            this.onlyLong           = onlyLong;
            this.onlyShort          = onlyShort;
            this.btcTrend           = btcTrend;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  SectorContext
    // ══════════════════════════════════════════════════════════════

    public static final class SectorContext {
        public final String  sector;
        public final double  bias;       // -1 (медведь) .. +1 (бык)
        public final double  strength;   // 0..1 (абсолют bias)
        public final double  momentum;   // изменение bias за последний цикл
        public final long    ts;

        public SectorContext(String sector, double bias, double strength, double momentum) {
            this.sector   = sector;
            this.bias     = bias;
            this.strength = strength;
            this.momentum = momentum;
            this.ts       = System.currentTimeMillis();
        }

        /** Актуальность 5 минут */
        public boolean isValid() {
            return System.currentTimeMillis() - ts < 5 * 60_000L;
        }

        /** Сектор бычий */
        public boolean isBull() { return bias > 0.35; }

        /** Сектор медвежий */
        public boolean isBear() { return bias < -0.35; }

        /** Разворот сектора (был медведь → стал бык или наоборот) */
        public boolean isReversal() { return Math.abs(momentum) > 0.3 && bias * momentum < 0; }
    }

    // ══════════════════════════════════════════════════════════════
    //  Маппинг символ → сектор
    // ══════════════════════════════════════════════════════════════

    private static final Map<String, String> SYMBOL_SECTOR;
    static {
        SYMBOL_SECTOR = new HashMap<>(64);
        // MEME
        for (String s : new String[]{"PEPEUSDT","DOGEUSDT","SHIBUSDT","FLOKIUSDT","WIFUSDT",
                "BONKUSDT","MEMEUSDT","POPCATUSDT","NEIROUSDT","BRETTUSDT","TURBOUSDT","MOGUSDT"})
            SYMBOL_SECTOR.put(s, "MEME");
        // L1
        for (String s : new String[]{"SOLUSDT","AVAXUSDT","ADAUSDT","DOTUSDT","NEARUSDT",
                "APTUSDT","SUIUSDT","TONUSDT","ALGOUSDT","TRXUSDT"})
            SYMBOL_SECTOR.put(s, "L1");
        // DEFI
        for (String s : new String[]{"UNIUSDT","AAVEUSDT","CRVUSDT","GMXUSDT","JUPUSDT",
                "DYDXUSDT","SNXUSDT","COMPUSDT","MKRUSDT","SUSHIUSDT"})
            SYMBOL_SECTOR.put(s, "DEFI");
        // INFRA
        for (String s : new String[]{"LINKUSDT","RENDERUSDT","FETUSDT","WLDUSDT",
                "GRTUSDT","FILUSDT","ARKMUSDT","IOTAUSDT"})
            SYMBOL_SECTOR.put(s, "INFRA");
        // PAYMENT
        for (String s : new String[]{"XRPUSDT","XLMUSDT","LTCUSDT","XMRUSDT","ZILUSDT"})
            SYMBOL_SECTOR.put(s, "PAYMENT");
        // TOP
        for (String s : new String[]{"BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT"})
            SYMBOL_SECTOR.put(s, "TOP");
    }

    // ══════════════════════════════════════════════════════════════
    //  State
    // ══════════════════════════════════════════════════════════════

    private final Map<String, SectorContext> sectorCache     = new ConcurrentHashMap<>();
    private final Map<String, Double>        prevSectorBias  = new ConcurrentHashMap<>();

    private volatile GlobalContext current =
            new GlobalContext(GlobalRegime.NEUTRAL, 0.0, 1.0, false, false, false, 0);

    private volatile double prevImpulseStrength = 0.0;
    private volatile long   lastUpdateTs        = 0L;

    // ══════════════════════════════════════════════════════════════
    //  UPDATE BTC
    // ══════════════════════════════════════════════════════════════

    public void update(List<TradingCore.Candle> btc) {
        if (btc == null || btc.size() < VOL_LOOKBACK + 5) return;

        double avgRange = Math.max(averageRange(btc, VOL_LOOKBACK), 1e-7);
        TradingCore.Candle last = btc.get(btc.size() - 1);

        double currRange          = last.high - last.low;
        double volatilityExpansion = currRange / avgRange;
        double bodyExpansion      = bodyExpansionScore(btc);
        double volumeSpike        = volumeSpikeScore(btc);
        double btcTrend           = calculateBtcTrend(btc);

        // Взвешенный сырой скор импульса
        double rawScore = 0.40 * volatilityExpansion
                + 0.35 * bodyExpansion
                + 0.25 * volumeSpike;

        double impulseStrength = normalize(rawScore);
        boolean bodyOk = bodyExpansion >= 0.65;

        GlobalRegime regime = determineRegime(btc, impulseStrength, bodyOk, btcTrend);
        boolean strong      = impulseStrength > 0.70 && volatilityExpansion > 1.4;

        prevImpulseStrength = current.impulseStrength;
        lastUpdateTs = System.currentTimeMillis();
        current = new GlobalContext(
                regime, impulseStrength, volatilityExpansion,
                strong,
                regime == GlobalRegime.BTC_STRONG_UP,
                regime == GlobalRegime.BTC_STRONG_DOWN,
                btcTrend
        );
    }

    public GlobalContext getContext() { return current; }

    public long getLastUpdateTs() { return lastUpdateTs; }

    /** Возвращает true если данные BTC свежие (обновлялись менее 2 мин назад) */
    public boolean isFresh() {
        return System.currentTimeMillis() - lastUpdateTs < 2 * 60_000L;
    }

    // ══════════════════════════════════════════════════════════════
    //  UPDATE SECTOR
    // ══════════════════════════════════════════════════════════════

    /**
     * Вызывается из BotMain для каждого сектор-лидера.
     * Использует EMA8/EMA21 bias и сравнивает с предыдущим значением (momentum).
     */
    public void updateSector(String sector, List<TradingCore.Candle> candles) {
        if (candles == null || candles.size() < 30) return;

        double ema8  = ema(candles, 8);
        double ema21 = ema(candles, 21);
        double ema50 = candles.size() >= 50 ? ema(candles, 50) : ema21;

        // Bias = нормализованное расстояние EMA8/EMA21
        double diff = (ema8 - ema21) / (ema21 + 1e-9);
        double bias = Math.max(-1.0, Math.min(1.0, diff * 220));

        // Дополнительный сигнал: EMA21 выше/ниже EMA50
        double longBias = (ema21 - ema50) / (ema50 + 1e-9);
        bias = bias * 0.7 + Math.max(-1.0, Math.min(1.0, longBias * 180)) * 0.3;

        // Momentum = изменение bias с прошлого обновления
        double prev = prevSectorBias.getOrDefault(sector, bias);
        double momentum = bias - prev;
        prevSectorBias.put(sector, bias);

        sectorCache.put(sector, new SectorContext(sector, bias, Math.abs(bias), momentum));
    }

    /** Возвращает секторный контекст для символа (null если нет данных или устарел) */
    public SectorContext getSectorContext(String symbol) {
        String sector = SYMBOL_SECTOR.getOrDefault(symbol, "ALT");
        SectorContext sc = sectorCache.get(sector);
        return (sc != null && sc.isValid()) ? sc : null;
    }

    /** Возвращает все активные секторные контексты */
    public Map<String, SectorContext> getAllSectors() {
        Map<String, SectorContext> result = new HashMap<>();
        for (Map.Entry<String, SectorContext> e : sectorCache.entrySet()) {
            if (e.getValue().isValid()) result.put(e.getKey(), e.getValue());
        }
        return result;
    }

    // ══════════════════════════════════════════════════════════════
    //  FILTER SIGNAL — главная функция фильтрации
    // ══════════════════════════════════════════════════════════════

    /**
     * Возвращает коэффициент фильтрации:
     *   0.0    → сигнал заблокирован полностью
     *   0..1.0 → пропущен со штрафом (вероятность умножается на коэффициент)
     *   1.0    → нейтрально (проходит без изменений)
     *   > 1.0  → бонусный коэффициент (сектор или BTC в нашу сторону)
     *
     * Логика (симметричная):
     *  Приоритет 1: секторный контекст (локальный фактор, важнее BTC)
     *  Приоритет 2: BTC режим (глобальный фактор)
     *  Приоритет 3: Fade (импульс слабеет) → ослабляем штраф
     */
    public double filterSignal(DecisionEngineMerged.TradeIdea signal) {
        if (signal == null) return 0.0;

        GlobalContext ctx = current;
        boolean isLong    = signal.side == TradingCore.Side.LONG;
        boolean isShort   = signal.side == TradingCore.Side.SHORT;
        boolean fadingImpulse = prevImpulseStrength > ctx.impulseStrength
                && ctx.impulseStrength > 0.45;

        // ════════════════════════════════════════════════════════
        //  ПРИОРИТЕТ 1: СЕКТОРНЫЙ КОНТЕКСТ
        // ════════════════════════════════════════════════════════
        SectorContext sc = getSectorContext(signal.symbol);
        if (sc != null && sc.strength > 0.32) {

            // Сектор разворачивается — более важный сигнал
            if (sc.isReversal()) {
                boolean reversalAligned =
                        (sc.bias > 0 && isLong)  ||   // разворот вверх + лонг
                                (sc.bias < 0 && isShort);      // разворот вниз + шорт
                if (reversalAligned) return 1.25;      // сильный бонус за разворот
            }

            // Сектор уверенно бычий
            if (sc.isBull()) {
                if (isShort && signal.probability < 76) return 0.48;  // штраф шортам
                if (isLong)                              return 1.18;  // бонус лонгам
            }

            // Сектор уверенно медвежий
            if (sc.isBear()) {
                if (isLong && signal.probability < 76)  return 0.48;  // штраф лонгам
                if (isShort)                             return 1.18;  // бонус шортам
            }
        }

        // ════════════════════════════════════════════════════════
        //  ПРИОРИТЕТ 2: BTC РЕЖИМ
        // ════════════════════════════════════════════════════════

        // NEUTRAL → всё проходит
        if (ctx.regime == GlobalRegime.NEUTRAL) return 1.0;

        // ── BTC STRONG UP (симметрично с STRONG DOWN) ──────────
        if (ctx.regime == GlobalRegime.BTC_STRONG_UP && isShort) {
            if (signal.probability >= 83) return 0.82;  // очень уверенный шорт пропускаем
            if (ctx.volatilityExpansion > 1.55) return 0.78;  // высокая vol → возможен reversal
            if (signal.probability < 68)  return 0.0;   // слабый шорт → блок
            return 0.62;
        }

        // ── BTC STRONG DOWN (симметрично) ──────────────────────
        if (ctx.regime == GlobalRegime.BTC_STRONG_DOWN && isLong) {
            if (signal.probability >= 83) return 0.82;
            if (ctx.volatilityExpansion > 1.55) return 0.78;
            if (signal.probability < 68)  return 0.0;
            return 0.62;
        }

        // ── BTC IMPULSE UP ─────────────────────────────────────
        if (ctx.regime == GlobalRegime.BTC_IMPULSE_UP && isShort) {
            if (fadingImpulse) {
                // Импульс слабеет → возможен reversal → смягчаем штраф
                return signal.probability >= 64 ? 0.92 : 0.72;
            }
            if (signal.probability < 60) return 0.0;
            return signal.probability >= 72 ? 0.84 : 0.62;
        }

        // ── BTC IMPULSE DOWN ───────────────────────────────────
        if (ctx.regime == GlobalRegime.BTC_IMPULSE_DOWN && isLong) {
            if (fadingImpulse) {
                return signal.probability >= 64 ? 0.92 : 0.72;
            }
            if (signal.probability < 60) return 0.0;
            return signal.probability >= 72 ? 0.84 : 0.62;
        }

        // ── BTC против сигнала, но нет точного режима → умеренный штраф
        if ((ctx.btcTrend > 0.4 && isShort) || (ctx.btcTrend < -0.4 && isLong)) {
            return signal.probability >= 75 ? 0.90 : 0.75;
        }

        return 1.0;
    }

    // ══════════════════════════════════════════════════════════════
    //  PRIVATE HELPERS
    // ══════════════════════════════════════════════════════════════

    private double averageRange(List<TradingCore.Candle> c, int n) {
        int sz = c.size(); double sum = 0;
        for (int i = sz - n; i < sz; i++) sum += c.get(i).high - c.get(i).low;
        return sum / n;
    }

    private double bodyExpansionScore(List<TradingCore.Candle> c) {
        int sz = c.size(); double sum = 0;
        for (int i = sz - BODY_LOOKBACK; i < sz; i++)
            sum += Math.abs(c.get(i).close - c.get(i).open);
        double avgBody = sum / BODY_LOOKBACK;
        double lastBody = Math.abs(c.get(sz-1).close - c.get(sz-1).open);
        return avgBody == 0 ? 1.0 : lastBody / avgBody;
    }

    private double volumeSpikeScore(List<TradingCore.Candle> c) {
        int sz = c.size(); double sum = 0;
        for (int i = sz - VOL_LOOKBACK; i < sz; i++) sum += c.get(i).volume;
        double avgVol = sum / VOL_LOOKBACK;
        return avgVol == 0 ? 1.0 : c.get(sz-1).volume / avgVol;
    }

    private double calculateBtcTrend(List<TradingCore.Candle> btc) {
        if (btc.size() < 52) return 0;
        double ema20 = ema(btc, 20);
        double ema50 = ema(btc, 50);
        double diff  = (ema20 - ema50) / (ema50 + 1e-9);
        return Math.max(-1.0, Math.min(1.0, diff * 100));
    }

    private double ema(List<TradingCore.Candle> c, int p) {
        if (c.size() < p) return c.get(c.size()-1).close;
        double k = 2.0 / (p + 1), e = c.get(c.size()-p).close;
        for (int i = c.size()-p+1; i < c.size(); i++)
            e = c.get(i).close * k + e * (1 - k);
        return e;
    }

    private GlobalRegime determineRegime(List<TradingCore.Candle> btc,
                                         double strength, boolean bodyOk,
                                         double btcTrend) {
        if (!bodyOk || strength < 0.42) return GlobalRegime.NEUTRAL;

        int sz = btc.size();
        double move    = btc.get(sz-1).close - btc.get(sz-4).close;
        double movePct = Math.abs(move) / (btc.get(sz-4).close + 1e-9);

        // Сильный режим: >0.8% за 3 свечи + высокая сила
        if (movePct > 0.008 && strength > 0.68)
            return move > 0 ? GlobalRegime.BTC_STRONG_UP : GlobalRegime.BTC_STRONG_DOWN;

        // Умеренный импульс
        if (move > 0 && strength > 0.48) return GlobalRegime.BTC_IMPULSE_UP;
        if (move < 0 && strength > 0.48) return GlobalRegime.BTC_IMPULSE_DOWN;

        return GlobalRegime.NEUTRAL;
    }

    private double normalize(double value) {
        return Math.min(1.0, Math.max(0.0, value / 1.8));
    }
}
