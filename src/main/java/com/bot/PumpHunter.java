package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class PumpHunter {

    // ======================= CONFIG =======================

    // Пороги для детекции памп-свечей
    private static final double PUMP_BODY_ATR_MULT = 2.2;       // Тело свечи > 2.2 * ATR
    private static final double MEGA_PUMP_BODY_ATR_MULT = 3.5;  // Мега-памп > 3.5 * ATR
    private static final double VOLUME_SPIKE_MULT = 2.5;        // Объём > 2.5x среднего
    private static final double MEGA_VOLUME_SPIKE_MULT = 4.0;   // Мега-объём > 4x

    // Параметры импульса
    private static final double MIN_MOVE_PCT = 0.008;           // Минимум 0.8% движение
    private static final double STRONG_MOVE_PCT = 0.015;        // Сильное движение 1.5%
    private static final double MEGA_MOVE_PCT = 0.025;          // Мега-движение 2.5%

    // Временные окна
    private static final int VOLUME_LOOKBACK = 20;
    private static final int ATR_PERIOD = 14;
    private static final int PUMP_CONFIRM_BARS = 3;

    // Cooldown между сигналами (мс)
    private static final long PUMP_COOLDOWN_MS = 10 * 60_000;   // 10 минут

    // ======================= STATE =======================

    private final Map<String, Long> lastPumpTime = new ConcurrentHashMap<>();
    private final Map<String, PumpEvent> recentPumps = new ConcurrentHashMap<>();
    private final Map<String, Deque<PumpEvent>> pumpHistory = new ConcurrentHashMap<>();

    // ======================= MODELS =======================

    public enum PumpType {
        NONE,
        PUMP_UP,          // Обычный памп вверх
        PUMP_DOWN,        // Обычный дамп вниз
        MEGA_PUMP_UP,     // Мега-памп вверх
        MEGA_PUMP_DOWN,   // Мега-дамп вниз
        SQUEEZE_UP,       // Short squeeze
        SQUEEZE_DOWN,     // Long squeeze
        BREAKOUT_UP,      // Пробой уровня вверх
        BREAKOUT_DOWN     // Пробой уровня вниз
    }

    public static final class PumpEvent {
        public final String symbol;
        public final PumpType type;
        public final double strength;           // 0.0 - 1.0
        public final double movePct;            // процент движения
        public final double volumeRatio;        // отношение к среднему объёму
        public final double bodyToAtrRatio;     // отношение тела к ATR
        public final boolean isConfirmed;       // подтверждено ли следующими свечами
        public final long timestamp;
        public final List<String> flags;

        public PumpEvent(String symbol, PumpType type, double strength,
                         double movePct, double volumeRatio, double bodyToAtrRatio,
                         boolean isConfirmed, List<String> flags) {
            this.symbol = symbol;
            this.type = type;
            this.strength = strength;
            this.movePct = movePct;
            this.volumeRatio = volumeRatio;
            this.bodyToAtrRatio = bodyToAtrRatio;
            this.isConfirmed = isConfirmed;
            this.timestamp = System.currentTimeMillis();
            this.flags = flags != null ? flags : List.of();
        }

        public boolean isBullish() {
            return type == PumpType.PUMP_UP || type == PumpType.MEGA_PUMP_UP ||
                    type == PumpType.SQUEEZE_UP || type == PumpType.BREAKOUT_UP;
        }

        public boolean isBearish() {
            return type == PumpType.PUMP_DOWN || type == PumpType.MEGA_PUMP_DOWN ||
                    type == PumpType.SQUEEZE_DOWN || type == PumpType.BREAKOUT_DOWN;
        }

        public boolean isMega() {
            return type == PumpType.MEGA_PUMP_UP || type == PumpType.MEGA_PUMP_DOWN;
        }

        @Override
        public String toString() {
            return String.format("PumpEvent{%s %s strength=%.2f move=%.2f%% vol=%.1fx flags=%s}",
                    symbol, type, strength, movePct * 100, volumeRatio, flags);
        }
    }

    public static final class PumpSignal {
        public final String symbol;
        public final com.bot.TradingCore.Side side;
        public final double entryPrice;
        public final double confidence;
        public final PumpEvent event;
        public final String strategy;

        public PumpSignal(String symbol, com.bot.TradingCore.Side side,
                          double entryPrice, double confidence,
                          PumpEvent event, String strategy) {
            this.symbol = symbol;
            this.side = side;
            this.entryPrice = entryPrice;
            this.confidence = confidence;
            this.event = event;
            this.strategy = strategy;
        }
    }

    // ======================= MAIN DETECTION =======================

    /**
     * Основной метод детекции памп-свечей
     */
    public PumpEvent detectPump(String symbol,
                                List<TradingCore.Candle> c1m,
                                List<TradingCore.Candle> c5m,
                                List<TradingCore.Candle> c15m) {

        if (c1m == null || c1m.size() < 30 ||
                c5m == null || c5m.size() < 20 ||
                c15m == null || c15m.size() < ATR_PERIOD + 5) {
            return null;
        }

        // Cooldown check
        Long lastPump = lastPumpTime.get(symbol);
        if (lastPump != null && System.currentTimeMillis() - lastPump < PUMP_COOLDOWN_MS) {
            return null;
        }

        List<String> flags = new ArrayList<>();

        // === 1. Анализ последней свечи 1M ===
        PumpMetrics m1 = analyzeCandle(c1m, ATR_PERIOD);

        // === 2. Анализ последней свечи 5M ===
        PumpMetrics m5 = analyzeCandle(c5m, ATR_PERIOD);

        // === 3. Анализ последней свечи 15M ===
        PumpMetrics m15 = analyzeCandle(c15m, ATR_PERIOD);

        // === 4. Детекция типа события ===
        PumpType type = PumpType.NONE;
        double strength = 0;
        double movePct = 0;
        double volumeRatio = 0;
        double bodyToAtr = 0;

        // Приоритет: проверяем от более крупных TF к мелким

        // Проверка МЕГА-пампа на 15M
        if (m15.bodyToAtr >= MEGA_PUMP_BODY_ATR_MULT &&
                m15.volumeRatio >= MEGA_VOLUME_SPIKE_MULT &&
                Math.abs(m15.movePct) >= MEGA_MOVE_PCT) {

            type = m15.isGreen ? PumpType.MEGA_PUMP_UP : PumpType.MEGA_PUMP_DOWN;
            strength = calculateStrength(m15, true);
            movePct = m15.movePct;
            volumeRatio = m15.volumeRatio;
            bodyToAtr = m15.bodyToAtr;
            flags.add("15M_MEGA");
        }
        // Проверка обычного пампа на 15M
        else if (m15.bodyToAtr >= PUMP_BODY_ATR_MULT &&
                m15.volumeRatio >= VOLUME_SPIKE_MULT &&
                Math.abs(m15.movePct) >= STRONG_MOVE_PCT) {

            type = m15.isGreen ? PumpType.PUMP_UP : PumpType.PUMP_DOWN;
            strength = calculateStrength(m15, false);
            movePct = m15.movePct;
            volumeRatio = m15.volumeRatio;
            bodyToAtr = m15.bodyToAtr;
            flags.add("15M_PUMP");
        }
        // Проверка на 5M
        else if (m5.bodyToAtr >= PUMP_BODY_ATR_MULT * 0.9 &&
                m5.volumeRatio >= VOLUME_SPIKE_MULT &&
                Math.abs(m5.movePct) >= MIN_MOVE_PCT) {

            if (m5.bodyToAtr >= MEGA_PUMP_BODY_ATR_MULT && m5.volumeRatio >= MEGA_VOLUME_SPIKE_MULT) {
                type = m5.isGreen ? PumpType.MEGA_PUMP_UP : PumpType.MEGA_PUMP_DOWN;
                strength = calculateStrength(m5, true) * 0.95;
                flags.add("5M_MEGA");
            } else {
                type = m5.isGreen ? PumpType.PUMP_UP : PumpType.PUMP_DOWN;
                strength = calculateStrength(m5, false) * 0.90;
                flags.add("5M_PUMP");
            }
            movePct = m5.movePct;
            volumeRatio = m5.volumeRatio;
            bodyToAtr = m5.bodyToAtr;
        }
        // Быстрый памп на 1M (серия свечей)
        else {
            SeriesPumpResult series = detectSeriesPump(c1m);
            if (series.detected) {
                type = series.isUp ? PumpType.PUMP_UP : PumpType.PUMP_DOWN;
                strength = series.strength * 0.85;
                movePct = series.totalMovePct;
                volumeRatio = series.avgVolumeRatio;
                bodyToAtr = series.avgBodyToAtr;
                flags.add("1M_SERIES");
                flags.add("bars=" + series.barsCount);
            }
        }

        if (type == PumpType.NONE) {
            return null;
        }

        // === 5. Дополнительные проверки ===

        // Проверка на сквиз (резкий разворот после сильного движения)
        SqueezeResult squeeze = detectSqueeze(c1m, c5m, type);
        if (squeeze.detected) {
            type = squeeze.isUp ? PumpType.SQUEEZE_UP : PumpType.SQUEEZE_DOWN;
            strength = Math.min(1.0, strength + 0.15);
            flags.add("SQUEEZE");
        }

        // Проверка на пробой уровня
        BreakoutResult breakout = detectBreakout(c15m, type);
        if (breakout.detected) {
            type = breakout.isUp ? PumpType.BREAKOUT_UP : PumpType.BREAKOUT_DOWN;
            strength = Math.min(1.0, strength + 0.10);
            flags.add("BREAKOUT");
            flags.add("level=" + String.format("%.6f", breakout.level));
        }

        // Проверка Volume Profile
        if (isVolumeClimaxing(c1m)) {
            strength = Math.min(1.0, strength + 0.08);
            flags.add("VOL_CLIMAX");
        }

        // Проверка подтверждения следующими барами
        boolean confirmed = checkConfirmation(c1m, type);
        if (confirmed) {
            strength = Math.min(1.0, strength + 0.05);
            flags.add("CONFIRMED");
        }

        // === 6. Создание события ===
        PumpEvent event = new PumpEvent(
                symbol, type, strength, movePct, volumeRatio, bodyToAtr, confirmed, flags
        );

        // Сохраняем
        lastPumpTime.put(symbol, System.currentTimeMillis());
        recentPumps.put(symbol, event);

        Deque<PumpEvent> history = pumpHistory.computeIfAbsent(symbol, k -> new ArrayDeque<>());
        history.addLast(event);
        while (history.size() > 50) history.removeFirst();

        System.out.println("[PumpHunter] " + event);

        return event;
    }

    /**
     * Генерация торгового сигнала на основе PumpEvent
     */
    public PumpSignal generateSignal(PumpEvent event, double currentPrice) {
        if (event == null || event.type == PumpType.NONE) {
            return null;
        }

        // Определяем направление сигнала
        // При пампе торгуем В НАПРАВЛЕНИИ движения (momentum следование)
        // При сквизе - потенциальный разворот

        com.bot.TradingCore.Side side;
        String strategy;
        double confidence;

        switch (event.type) {
            case PUMP_UP, MEGA_PUMP_UP, BREAKOUT_UP -> {
                // Продолжение движения вверх
                side = com.bot.TradingCore.Side.LONG;
                strategy = "PUMP_CONTINUATION";
                confidence = 55 + event.strength * 25;
            }
            case PUMP_DOWN, MEGA_PUMP_DOWN, BREAKOUT_DOWN -> {
                // Продолжение движения вниз
                side = com.bot.TradingCore.Side.SHORT;
                strategy = "DUMP_CONTINUATION";
                confidence = 55 + event.strength * 25;
            }
            case SQUEEZE_UP -> {
                // Short squeeze = LONG
                side = com.bot.TradingCore.Side.LONG;
                strategy = "SHORT_SQUEEZE";
                confidence = 60 + event.strength * 20;
            }
            case SQUEEZE_DOWN -> {
                // Long squeeze = SHORT
                side = com.bot.TradingCore.Side.SHORT;
                strategy = "LONG_SQUEEZE";
                confidence = 60 + event.strength * 20;
            }
            default -> {
                return null;
            }
        }

        // Бонусы за подтверждения
        if (event.isConfirmed) confidence += 3;
        if (event.isMega()) confidence += 5;
        if (event.flags.contains("VOL_CLIMAX")) confidence += 2;

        // Ограничение
        confidence = Math.min(85, Math.max(55, confidence));

        return new PumpSignal(event.symbol, side, currentPrice, confidence, event, strategy);
    }

    // ======================= ANALYSIS HELPERS =======================

    private static class PumpMetrics {
        double bodyToAtr;
        double volumeRatio;
        double movePct;
        boolean isGreen;
        double bodySize;
        double atr;
    }

    private PumpMetrics analyzeCandle(List<TradingCore.Candle> candles, int atrPeriod) {
        PumpMetrics m = new PumpMetrics();

        if (candles.size() < atrPeriod + 1) {
            return m;
        }

        TradingCore.Candle last = candles.get(candles.size() - 1);

        // ATR
        m.atr = calculateATR(candles, atrPeriod);
        if (m.atr <= 0) m.atr = (last.high - last.low);

        // Body
        m.bodySize = Math.abs(last.close - last.open);
        m.bodyToAtr = m.bodySize / Math.max(m.atr, 1e-10);

        // Volume ratio
        double avgVol = averageVolume(candles, VOLUME_LOOKBACK);
        m.volumeRatio = last.volume / Math.max(avgVol, 1e-10);

        // Move %
        TradingCore.Candle prev = candles.get(candles.size() - 2);
        m.movePct = (last.close - prev.close) / prev.close;

        // Direction
        m.isGreen = last.close > last.open;

        return m;
    }

    private double calculateStrength(PumpMetrics m, boolean isMega) {
        // Базовая сила на основе метрик
        double bodyScore = Math.min(1.0, m.bodyToAtr / (isMega ? MEGA_PUMP_BODY_ATR_MULT : PUMP_BODY_ATR_MULT) / 1.5);
        double volScore = Math.min(1.0, m.volumeRatio / (isMega ? MEGA_VOLUME_SPIKE_MULT : VOLUME_SPIKE_MULT) / 1.5);
        double moveScore = Math.min(1.0, Math.abs(m.movePct) / (isMega ? MEGA_MOVE_PCT : STRONG_MOVE_PCT));

        // Взвешенная сумма
        double strength = bodyScore * 0.35 + volScore * 0.35 + moveScore * 0.30;

        return Math.min(1.0, strength);
    }

    // ======================= SERIES PUMP DETECTION =======================

    private static class SeriesPumpResult {
        boolean detected;
        boolean isUp;
        double strength;
        double totalMovePct;
        double avgVolumeRatio;
        double avgBodyToAtr;
        int barsCount;
    }

    private SeriesPumpResult detectSeriesPump(List<TradingCore.Candle> c1m) {
        SeriesPumpResult result = new SeriesPumpResult();

        if (c1m.size() < 10) return result;

        int n = c1m.size();
        double atr = calculateATR(c1m, ATR_PERIOD);
        double avgVol = averageVolume(c1m, VOLUME_LOOKBACK);

        // Проверяем последние 5-6 баров
        int greenCount = 0;
        int redCount = 0;
        double totalMove = 0;
        double totalVolRatio = 0;
        double totalBodyAtr = 0;

        int lookback = Math.min(6, n - 1);

        for (int i = n - lookback; i < n; i++) {
            TradingCore.Candle c = c1m.get(i);

            if (c.close > c.open) greenCount++;
            else redCount++;

            totalMove += c.close - c.open;
            totalVolRatio += c.volume / Math.max(avgVol, 1e-10);
            totalBodyAtr += Math.abs(c.close - c.open) / Math.max(atr, 1e-10);
        }

        TradingCore.Candle startBar = c1m.get(n - lookback);
        TradingCore.Candle endBar = c1m.get(n - 1);

        double totalMovePct = (endBar.close - startBar.open) / startBar.open;
        double avgVolRatio = totalVolRatio / lookback;
        double avgBodyAtr = totalBodyAtr / lookback;

        // Условия детекции серии
        boolean isUpSeries = greenCount >= 4 && Math.abs(totalMovePct) >= MIN_MOVE_PCT;
        boolean isDownSeries = redCount >= 4 && Math.abs(totalMovePct) >= MIN_MOVE_PCT;
        boolean volumeOk = avgVolRatio >= 1.5;

        if ((isUpSeries || isDownSeries) && volumeOk) {
            result.detected = true;
            result.isUp = totalMove > 0;
            result.totalMovePct = totalMovePct;
            result.avgVolumeRatio = avgVolRatio;
            result.avgBodyToAtr = avgBodyAtr;
            result.barsCount = lookback;
            result.strength = Math.min(1.0,
                    (Math.abs(totalMovePct) / STRONG_MOVE_PCT * 0.5) +
                            (avgVolRatio / VOLUME_SPIKE_MULT * 0.3) +
                            (avgBodyAtr / PUMP_BODY_ATR_MULT * 0.2)
            );
        }

        return result;
    }

    // ======================= SQUEEZE DETECTION =======================

    private static class SqueezeResult {
        boolean detected;
        boolean isUp;
    }

    private SqueezeResult detectSqueeze(List<TradingCore.Candle> c1m,
                                        List<TradingCore.Candle> c5m,
                                        PumpType currentType) {
        SqueezeResult result = new SqueezeResult();

        if (c5m.size() < 20) return result;

        int n = c5m.size();

        // Ищем признаки сквиза:
        // 1. Сильное движение в одном направлении
        // 2. Резкий разворот с высоким объёмом

        // Проверяем предыдущее движение (последние 10-15 баров)
        double prevTrend = 0;
        for (int i = n - 15; i < n - 3; i++) {
            if (i >= 0) {
                prevTrend += c5m.get(i).close - c5m.get(i).open;
            }
        }

        // Текущее движение (последние 3 бара)
        double currentMove = 0;
        for (int i = n - 3; i < n; i++) {
            currentMove += c5m.get(i).close - c5m.get(i).open;
        }

        // Сквиз: предыдущий тренд вниз, текущее резкое движение вверх (short squeeze)
        if (prevTrend < 0 && currentMove > 0 &&
                Math.abs(currentMove) > Math.abs(prevTrend) * 0.5) {
            result.detected = true;
            result.isUp = true;
        }

        // Сквиз: предыдущий тренд вверх, текущее резкое движение вниз (long squeeze)
        if (prevTrend > 0 && currentMove < 0 &&
                Math.abs(currentMove) > Math.abs(prevTrend) * 0.5) {
            result.detected = true;
            result.isUp = false;
        }

        return result;
    }

    // ======================= BREAKOUT DETECTION =======================

    private static class BreakoutResult {
        boolean detected;
        boolean isUp;
        double level;
    }

    private BreakoutResult detectBreakout(List<TradingCore.Candle> c15m, PumpType currentType) {
        BreakoutResult result = new BreakoutResult();

        if (c15m.size() < 30) return result;

        int n = c15m.size();
        TradingCore.Candle last = c15m.get(n - 1);

        // Находим уровни поддержки/сопротивления
        double recentHigh = Double.MIN_VALUE;
        double recentLow = Double.MAX_VALUE;

        // Ищем хай/лоу за последние 20 баров (исключая последние 3)
        for (int i = n - 23; i < n - 3; i++) {
            if (i >= 0) {
                recentHigh = Math.max(recentHigh, c15m.get(i).high);
                recentLow = Math.min(recentLow, c15m.get(i).low);
            }
        }

        // Пробой вверх
        if (last.close > recentHigh * 1.002 && last.close > last.open) {
            result.detected = true;
            result.isUp = true;
            result.level = recentHigh;
        }

        // Пробой вниз
        if (last.close < recentLow * 0.998 && last.close < last.open) {
            result.detected = true;
            result.isUp = false;
            result.level = recentLow;
        }

        return result;
    }

    // ======================= VOLUME CLIMAX =======================

    private boolean isVolumeClimaxing(List<TradingCore.Candle> candles) {
        if (candles.size() < 15) return false;

        int n = candles.size();
        TradingCore.Candle last = candles.get(n - 1);

        // Находим максимальный объём за последние 20 баров
        double maxVol = 0;
        for (int i = Math.max(0, n - 20); i < n - 1; i++) {
            maxVol = Math.max(maxVol, candles.get(i).volume);
        }

        // Volume climax = текущий объём > 90% от максимума за период
        return last.volume >= maxVol * 0.90;
    }

    // ======================= CONFIRMATION =======================

    private boolean checkConfirmation(List<TradingCore.Candle> c1m, PumpType type) {
        if (c1m.size() < PUMP_CONFIRM_BARS + 2) return false;

        int n = c1m.size();
        TradingCore.Candle pumpBar = c1m.get(n - PUMP_CONFIRM_BARS - 1);

        boolean isUp = type == PumpType.PUMP_UP || type == PumpType.MEGA_PUMP_UP ||
                type == PumpType.SQUEEZE_UP || type == PumpType.BREAKOUT_UP;

        // Проверяем, что следующие бары держат направление
        int confirmCount = 0;
        for (int i = n - PUMP_CONFIRM_BARS; i < n; i++) {
            TradingCore.Candle c = c1m.get(i);
            if (isUp && c.close >= pumpBar.close * 0.998) confirmCount++;
            if (!isUp && c.close <= pumpBar.close * 1.002) confirmCount++;
        }

        return confirmCount >= 2;
    }

    // ======================= UTILITY =======================

    private double calculateATR(List<TradingCore.Candle> candles, int period) {
        if (candles.size() < period + 1) return 0;

        double sum = 0;
        int n = candles.size();

        for (int i = n - period; i < n; i++) {
            TradingCore.Candle cur = candles.get(i);
            TradingCore.Candle prev = candles.get(i - 1);

            double tr = Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low - prev.close)));
            sum += tr;
        }

        return sum / period;
    }

    private double averageVolume(List<TradingCore.Candle> candles, int lookback) {
        if (candles.size() < lookback + 1) return 1;

        double sum = 0;
        int n = candles.size();
        int start = Math.max(0, n - lookback - 1);

        for (int i = start; i < n - 1; i++) {
            sum += candles.get(i).volume;
        }

        int count = (n - 1) - start;
        return count > 0 ? sum / count : 1;
    }

    // ======================= PUBLIC API =======================

    /**
     * Получить последний памп для символа
     */
    public PumpEvent getRecentPump(String symbol) {
        return recentPumps.get(symbol);
    }

    /**
     * Проверить, был ли недавний памп
     */
    public boolean hadRecentPump(String symbol, long withinMs) {
        Long lastTime = lastPumpTime.get(symbol);
        return lastTime != null && System.currentTimeMillis() - lastTime < withinMs;
    }

    /**
     * Получить историю пампов для символа
     */
    public List<PumpEvent> getPumpHistory(String symbol) {
        Deque<PumpEvent> history = pumpHistory.get(symbol);
        return history != null ? new ArrayList<>(history) : List.of();
    }

    /**
     * Очистить данные для символа
     */
    public void clearSymbol(String symbol) {
        lastPumpTime.remove(symbol);
        recentPumps.remove(symbol);
        pumpHistory.remove(symbol);
    }

    /**
     * Очистить все данные
     */
    public void clearAll() {
        lastPumpTime.clear();
        recentPumps.clear();
        pumpHistory.clear();
    }
}
