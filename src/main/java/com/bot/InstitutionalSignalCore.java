package com.bot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║       InstitutionalSignalCore — GODBOT EDITION                  ║
 * ║  Полноценный портфолио риск-менеджер уровня хедж-фонда          ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  Функции:                                                        ║
 * ║  · Ограничение активных сигналов (глобально + по символу)       ║
 * ║  · Portfolio exposure лимит                                      ║
 * ║  · Фильтр дублей и конфликтующих позиций                        ║
 * ║  · Оценка истории символа (winRate + score)                     ║
 * ║  · Автоматическая очистка устаревших сигналов (TTL 15 мин)      ║
 * ║  · Пересчёт exposure после каждой очистки                       ║
 * ║  · getStats() для логирования в BotMain                         ║
 * ║                                                                  ║
 * ║  ПАРАМЕТРЫ:                                                      ║
 * ║  maxGlobalSignals = 30 (много, качество = фильтры, не лимит)    ║
 * ║  maxSignalsPerSymbol = 2                                         ║
 * ║  maxPortfolioExposure = 50%                                      ║
 * ║  minConfidence = 56%                                             ║
 * ║  signalTtlMs = 15 мин                                            ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */
public final class InstitutionalSignalCore {

    // ── Конфигурация ───────────────────────────────────────────────
    private final int    maxGlobalSignals;
    private final int    maxSignalsPerSymbol;
    private final double maxPortfolioExposure;
    private final double minConfidence;
    private final double minSignalDiff;        // мин. % разница в цене для дублей
    private final long   signalTtlMs;

    /** Дефолтные параметры — оптимальные для TOP-100 */
    public InstitutionalSignalCore() {
        this.maxGlobalSignals     = 30;
        this.maxSignalsPerSymbol  = 2;
        this.maxPortfolioExposure = 0.50;
        this.minConfidence        = 56.0;
        this.minSignalDiff        = 0.0025;
        this.signalTtlMs          = 15 * 60_000L;
    }

    public InstitutionalSignalCore(int maxGlobal, int maxPerSym,
                                   double maxExposure, double minConf,
                                   double minDiff, long ttlMs) {
        this.maxGlobalSignals     = maxGlobal;
        this.maxSignalsPerSymbol  = maxPerSym;
        this.maxPortfolioExposure = maxExposure;
        this.minConfidence        = minConf;
        this.minSignalDiff        = minDiff;
        this.signalTtlMs          = ttlMs;
    }

    // ── State ──────────────────────────────────────────────────────
    private final Map<String, List<ActiveSignal>> activeSignals = new ConcurrentHashMap<>();
    private final Map<String, List<ClosedTrade>>  history       = new ConcurrentHashMap<>();
    private final Map<String, Double>             symbolScore   = new ConcurrentHashMap<>();
    private final Map<String, Long>               symbolLastWin = new ConcurrentHashMap<>();
    private volatile double currentExposure = 0.0;

    // Time Stop: 15m свечей до принудительного закрытия сигнала без результата
    // 6 свечей = 90 минут. Если за это время цена не пошла к TP1 — сценарий не сработал.
    private static final int TIME_STOP_BARS_15M = 6;
    private static final long TIME_STOP_MS = TIME_STOP_BARS_15M * 15 * 60_000L; // 90 мин

    // Callback для уведомления о Time Stop (опционально)
    private volatile java.util.function.BiConsumer<String, String> timeStopCallback = null;

    public void setTimeStopCallback(java.util.function.BiConsumer<String, String> cb) {
        this.timeStopCallback = cb;
    }

    // ── Models ─────────────────────────────────────────────────────

    public static final class ActiveSignal {
        public final String           symbol;
        public final com.bot.TradingCore.Side side;
        public final double           entry;
        public final double           tp1;    // первый тейк — ориентир для Time Stop
        public final double           stop;   // стоп для проверки
        public final double           probability;
        public final long             timestamp;
        public final String           category;

        public ActiveSignal(String sym, com.bot.TradingCore.Side side, double entry,
                            double tp1, double stop, double prob, long ts, String cat) {
            this.symbol      = sym;
            this.side        = side;
            this.entry       = entry;
            this.tp1         = tp1;
            this.stop        = stop;
            this.probability = prob;
            this.timestamp   = ts;
            this.category    = cat;
        }

        /** Обратная совместимость (без tp1/stop) */
        public ActiveSignal(String sym, com.bot.TradingCore.Side side, double entry,
                            double prob, long ts, String cat) {
            this(sym, side, entry, 0, 0, prob, ts, cat);
        }

        public long ageMs() { return System.currentTimeMillis() - timestamp; }

        /** Сигнал живёт дольше Time Stop */
        public boolean isTimeExpired() { return ageMs() > TIME_STOP_MS; }

        /**
         * Проверяет, идёт ли цена по сценарию.
         * Если через 3 свечи (45 мин) цена не сдвинулась хотя бы на 25% к TP1 — плохой знак.
         */
        public boolean isStalled(double currentPrice) {
            if (tp1 == 0 || ageMs() < 3 * 15 * 60_000L) return false;
            double totalDist = Math.abs(tp1 - entry);
            double progress  = side == com.bot.TradingCore.Side.LONG
                    ? currentPrice - entry
                    : entry - currentPrice;
            return progress / (totalDist + 1e-9) < 0.25;
        }
    }

    public static final class ClosedTrade {
        public final String           symbol;
        public final com.bot.TradingCore.Side side;
        public final double           pnlPct;
        public final long             duration;
        public final long             closedAt;

        public ClosedTrade(String sym, com.bot.TradingCore.Side side,
                           double pnl, long dur) {
            this.symbol   = sym;
            this.side     = side;
            this.pnlPct   = pnl;
            this.duration = dur;
            this.closedAt = System.currentTimeMillis();
        }

        public boolean isWin() { return pnlPct > 0; }
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN FILTER
    // ══════════════════════════════════════════════════════════════

    public synchronized boolean allowSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        cleanupExpiredSignals();

        if (signal == null) {
            log("null → rejected");
            return false;
        }

        String sym = signal.symbol;

        // ── 1. Минимальная уверенность ────────────────────────────
        if (signal.probability < minConfidence) {
            log(sym + " prob=" + signal.probability + " < " + minConfidence + " → rejected");
            return false;
        }

        // ── 2. Глобальный лимит ───────────────────────────────────
        if (getActiveSignalsCount() >= maxGlobalSignals) {
            log("Global limit " + maxGlobalSignals + "/30 → rejected");
            return false;
        }

        // ── 3. История символа (score + winRate) ──────────────────
        double score = symbolScore.getOrDefault(sym, 0.0);
        if (score < -0.28) {
            double wr = getWinRate(sym);
            if (wr < 0.36 && getHistory(sym).size() >= 5) {
                log(sym + " bad history score=" + f2(score) + " wr=" + f2(wr) + " → rejected");
                return false;
            }
        }

        List<ActiveSignal> list = activeSignals.computeIfAbsent(
                sym, k -> new CopyOnWriteArrayList<>());

        // ── 4. Дубли и конфликты ──────────────────────────────────
        for (ActiveSignal a : list) {
            double priceDiff = Math.abs(a.entry - signal.price) / (a.entry + 1e-9);
            double probDiff  = Math.abs(a.probability - signal.probability);

            // Тот же сигнал — похожая цена и вероятность
            if (a.side == signal.side && priceDiff < minSignalDiff && probDiff < 4) {
                log(sym + " duplicate (d=" + f2(priceDiff * 100) + "%) → rejected");
                return false;
            }

            // Противоположный сигнал слишком близко по цене — хаос
            if (a.side != signal.side && priceDiff < minSignalDiff * 1.8) {
                log(sym + " conflict opposite (d=" + f2(priceDiff * 100) + "%) → rejected");
                return false;
            }
        }

        // ── 5. Лимит по символу ───────────────────────────────────
        if (list.size() >= maxSignalsPerSymbol) {
            log(sym + " at limit " + list.size() + "/" + maxSignalsPerSymbol + " → rejected");
            return false;
        }

        // ── 6. Portfolio Exposure ─────────────────────────────────
        double est = estimateExposure(signal.probability,
                signal.category != null ? signal.category.name() : "ALT");
        if (currentExposure + est > maxPortfolioExposure) {
            log("Exposure " + f1(currentExposure*100) + "% + " + f1(est*100)
                    + "% > " + f1(maxPortfolioExposure*100) + "% → rejected");
            return false;
        }

        log("✓ " + sym + " " + signal.side
                + " prob=" + signal.probability
                + " exp+" + f1(est*100) + "% → ALLOWED");
        return true;
    }

    public synchronized void registerSignal(com.bot.DecisionEngineMerged.TradeIdea signal) {
        long now = System.currentTimeMillis();
        String catName = signal.category != null ? signal.category.name() : "ALT";

        ActiveSignal active = new ActiveSignal(
                signal.symbol, signal.side, signal.price,
                signal.tp1, signal.stop,
                signal.probability, now, catName);

        activeSignals.compute(signal.symbol, (sym, lst) -> {
            if (lst == null) lst = new CopyOnWriteArrayList<>();
            lst.removeIf(s -> s.side == signal.side &&
                    Math.abs(s.entry - signal.price) / (s.entry + 1e-9) < minSignalDiff);
            lst.add(active);
            return lst;
        });

        double est = estimateExposure(signal.probability, catName);
        currentExposure = clamp(currentExposure + est, 0, maxPortfolioExposure);
    }

    /** Вызывается когда позиция закрыта (из внешней системы мониторинга) */
    public synchronized void closeTrade(String symbol, com.bot.TradingCore.Side side, double pnlPct) {
        List<ActiveSignal> list = activeSignals.get(symbol);
        if (list == null) return;

        long now = System.currentTimeMillis();
        List<ActiveSignal> toRemove = new ArrayList<>();

        for (ActiveSignal s : list) {
            if (s.side == side) {
                history.computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>())
                        .add(new ClosedTrade(symbol, side, pnlPct, now - s.timestamp));
                currentExposure = clamp(
                        currentExposure - estimateExposure(s.probability, s.category),
                        0, maxPortfolioExposure);
                updateSymbolScore(symbol, pnlPct);
                if (pnlPct > 0) symbolLastWin.put(symbol, now);
                toRemove.add(s);
            }
        }

        list.removeAll(toRemove);
        if (list.isEmpty()) activeSignals.remove(symbol);
        recalcExposure();
    }

    // ══════════════════════════════════════════════════════════════
    //  CLEANUP
    // ══════════════════════════════════════════════════════════════

    private void cleanupExpiredSignals() {
        long now = System.currentTimeMillis();
        for (Iterator<Map.Entry<String, List<ActiveSignal>>> it =
             activeSignals.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, List<ActiveSignal>> e = it.next();
            String sym = e.getKey();
            e.getValue().removeIf(s -> {
                boolean ttlExpired  = (now - s.timestamp > signalTtlMs);
                boolean timeStop    = s.isTimeExpired();

                if (ttlExpired || timeStop) {
                    currentExposure = clamp(
                            currentExposure - estimateExposure(s.probability, s.category),
                            0, maxPortfolioExposure);

                    // Time Stop — сигнал жил 90 мин без результата
                    if (timeStop && !ttlExpired) {
                        history.computeIfAbsent(sym, k -> new CopyOnWriteArrayList<>())
                                .add(new ClosedTrade(sym, s.side, 0.0, now - s.timestamp));
                        updateSymbolScore(sym, 0.0); // BE — штраф -0.005
                        if (timeStopCallback != null) {
                            try {
                                timeStopCallback.accept(sym,
                                        "⏱ *TIME STOP* " + sym + " " + s.side
                                                + " — 6 свечей без результата. Сценарий не сработал.");
                            } catch (Exception ignored) {}
                        }
                        log("TIME STOP: " + sym + " " + s.side
                                + " age=" + (now - s.timestamp) / 60_000 + "min");
                    }
                    return true;
                }
                return false;
            });
            if (e.getValue().isEmpty()) it.remove();
        }
        recalcExposure();
    }

    private void recalcExposure() {
        double exp = 0;
        for (List<ActiveSignal> lst : activeSignals.values())
            for (ActiveSignal s : lst)
                exp += estimateExposure(s.probability, s.category);
        currentExposure = clamp(exp, 0, maxPortfolioExposure);
    }

    // ══════════════════════════════════════════════════════════════
    //  SYMBOL PERFORMANCE SCORING
    // ══════════════════════════════════════════════════════════════

    private void updateSymbolScore(String symbol, double pnl) {
        // Win → +0.015, Loss → -0.020, Breakeven → -0.005
        double delta = pnl > 0.5 ? 0.016 : pnl < -0.5 ? -0.020 : -0.005;
        symbolScore.merge(symbol, delta, Double::sum);
        symbolScore.compute(symbol, (k, v) -> clamp(v == null ? 0 : v, -0.50, 0.50));
    }

    /**
     * Размер позиции зависит от уверенности и категории монеты.
     * MEME монеты получают меньше экспозиции из-за волатильности.
     */
    private double estimateExposure(double probability, String category) {
        double base;
        if (probability >= 85) base = 0.060;
        else if (probability >= 78) base = 0.050;
        else if (probability >= 70) base = 0.038;
        else if (probability >= 62) base = 0.028;
        else base = 0.020;

        // MEME монеты — меньше риска
        double catMult = "MEME".equals(category) ? 0.75
                : "TOP".equals(category)  ? 1.0
                : 0.90;  // ALT
        return base * catMult;
    }

    // Перегрузка для ActiveSignal
    private double estimateExposure(double probability, Object category) {
        return estimateExposure(probability,
                category != null ? category.toString() : "ALT");
    }

    // ══════════════════════════════════════════════════════════════
    //  PUBLIC STATS API
    // ══════════════════════════════════════════════════════════════

    public synchronized int getActiveSignalsCount() {
        return activeSignals.values().stream().mapToInt(List::size).sum();
    }

    public synchronized double getCurrentExposure() { return currentExposure; }

    public double getSymbolScore(String symbol) {
        return symbolScore.getOrDefault(symbol, 0.0);
    }

    public List<ClosedTrade> getHistory(String symbol) {
        return history.getOrDefault(symbol, List.of());
    }

    public int getTotalClosedTrades() {
        return history.values().stream().mapToInt(List::size).sum();
    }

    public double getWinRate(String symbol) {
        List<ClosedTrade> h = history.get(symbol);
        if (h == null || h.isEmpty()) return 0.50;
        long wins = h.stream().filter(ClosedTrade::isWin).count();
        return (double) wins / h.size();
    }

    public double getOverallWinRate() {
        long total = 0, wins = 0;
        for (List<ClosedTrade> lst : history.values()) {
            total += lst.size();
            wins  += lst.stream().filter(ClosedTrade::isWin).count();
        }
        return total == 0 ? 0.50 : (double) wins / total;
    }

    /** Топ-5 символов по score */
    public List<Map.Entry<String, Double>> getTopPerformers() {
        return symbolScore.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(5)
                .collect(Collectors.toList());
    }

    /** Краткая статистика для логов */
    public synchronized String getStats() {
        return String.format("ISC[active=%d/%d exp=%.1f%%/%.0f%% closed=%d wr=%.0f%%]",
                getActiveSignalsCount(), maxGlobalSignals,
                currentExposure * 100, maxPortfolioExposure * 100,
                getTotalClosedTrades(),
                getOverallWinRate() * 100);
    }

    /** Полная статистика (для Telegram сообщения) */
    public synchronized String getFullStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("📊 *ISC Statistics*\n");
        sb.append(String.format("Active: %d/%d | Exposure: %.1f%%/%.0f%%\n",
                getActiveSignalsCount(), maxGlobalSignals,
                currentExposure * 100, maxPortfolioExposure * 100));
        sb.append(String.format("Closed trades: %d | Overall WR: %.0f%%\n",
                getTotalClosedTrades(), getOverallWinRate() * 100));

        List<Map.Entry<String, Double>> top = getTopPerformers();
        if (!top.isEmpty()) {
            sb.append("Top performers: ");
            sb.append(top.stream()
                    .map(e -> e.getKey() + "(" + String.format("%+.2f", e.getValue()) + ")")
                    .collect(Collectors.joining(", ")));
        }
        return sb.toString();
    }

    // ── Utility ─────────────────────────────────────────────────
    private static double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
    private static String f2(double v) { return String.format("%.2f", v); }
    private static String f1(double v) { return String.format("%.1f", v); }
    private static void log(String msg) {
        System.out.println("[ISC " +
                java.time.LocalTime.now().format(
                        java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")) + "] " + msg);
    }
}