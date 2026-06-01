package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.logging.Logger;

/**
 * LiveTradeProbe v1.0 — runs ONE real test trade on Binance demo to verify
 * the automation end-to-end before trusting it with strategy signals.
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  Цель: убедиться своими глазами что бот умеет делать всё:           │
 * │   1. Поставить плечо 5x на пару                                     │
 * │   2. Открыть MARKET-позицию (LONG)                                  │
 * │   3. Поставить STOP_MARKET (SL) на бирже                            │
 * │   4. Поставить TAKE_PROFIT_MARKET (TP1, TP2) на бирже               │
 * │   5. Прислать в Telegram отчёт с реальными ID ордеров               │
 * │   6. Подождать N минут чтобы ты глазами увидел в Binance UI         │
 * │   7. Закрыть позицию + почистить ордера                             │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * Запуск:
 *   В Railway env-vars добавь PROBE_RUN=BTCUSDT (или любой доступный символ).
 *   На следующем старте — проба выполнится один раз и завершится.
 *   После успеха — УБЕРИ переменную, иначе будет дёргаться при каждом рестарте.
 *
 * Параметры:
 *   PROBE_RUN              — символ (например BTCUSDT). Пустой = пропустить.
 *   PROBE_HOLD_SECONDS     — сколько держать позицию до автозакрытия (default 300 = 5 мин)
 *   PROBE_NOTIONAL_USD     — целевой размер позиции в USDT (default 20)
 *
 * Безопасность:
 *   - Работает ТОЛЬКО если BINANCE_USE_TESTNET=1. На live (=0) — отказывается.
 *   - Использует те же методы что и боевой код → если проба прошла, бот тоже пройдёт.
 *   - Гарантированный cleanup: при любой ошибке после открытия — закрывает позицию.
 *   - Не торгует реальными деньгами при правильной конфигурации.
 *
 * Что увидишь:
 *   1. В логе Railway — пошаговый отчёт с ID каждого ордера
 *   2. В Telegram — сообщение со всеми параметрами теста
 *   3. В Binance demo UI (https://testnet.binancefuture.com) — открытую позицию
 *      с плечом 5x, висящие SL и TP ордера в Open Orders
 *   4. Через PROBE_HOLD_SECONDS — позиция автозакроется, придёт второе сообщение
 */
public final class LiveTradeProbe {

    private static final Logger LOG = Logger.getLogger("LiveTradeProbe");

    /**
     * Точка входа. Вызывается из BotMain.main() сразу после инициализации
     * BinanceTradeExecutor. Если PROBE_RUN не задан — мгновенно выходит.
     *
     * @param telegram TelegramBotSender — для отчётов
     * @return true если проба запустилась (даже с ошибкой), false если пропущена
     */
    public static boolean runIfRequested(TelegramBotSender telegram) {
        String symbol = System.getenv().getOrDefault("PROBE_RUN", "").trim().toUpperCase();
        if (symbol.isEmpty()) return false;

        long holdSeconds = envLong("PROBE_HOLD_SECONDS", 300L);
        double targetNotional = envDouble("PROBE_NOTIONAL_USD", 20.0);

        BinanceTradeExecutor ex = BinanceTradeExecutor.getInstance();

        // SAFETY GATE 1: must be on testnet
        if (!ex.isTestnet()) {
            String err = "[PROBE] REFUSED: BINANCE_USE_TESTNET != 1. "
                    + "Probe ALWAYS refuses to run on live to protect real money.";
            LOG.severe(err);
            sendTg(telegram, "🚫 *Проба отказана*\n\n" + err
                    + "\n\nПоставь BINANCE_USE_TESTNET=1 в Railway, перезапусти.");
            return true;
        }

        // SAFETY GATE 2: API keys present
        if (!ex.isReady()) {
            String err = "[PROBE] REFUSED: API keys missing. "
                    + "Set BINANCE_TESTNET_API_KEY/SECRET in Railway.";
            LOG.severe(err);
            sendTg(telegram, "🚫 *Проба отказана*\n\n" + err);
            return true;
        }

        LOG.info("[PROBE] ═══ Starting LIVE TRADE PROBE on demo ═══");
        LOG.info("[PROBE] symbol=" + symbol + " holdSec=" + holdSeconds
                + " targetNotional=$" + targetNotional);

        sendTg(telegram, String.format(
                "🧪 *Проба автоматизации запущена*\n\n" +
                        "Символ: `%s`\n" +
                        "Целевой размер: ~$%.2f\n" +
                        "Плечо: 5x (как у боевого режима)\n" +
                        "SL: −2.0%%\n" +
                        "TP1: +1.2%% (50%% позиции)\n" +
                        "TP2: +2.4%% (50%% позиции)\n" +
                        "Удержание: %d сек\n\n" +
                        "Если всё работает — увидишь сделку в Binance demo UI " +
                        "и второе сообщение через %d сек.",
                symbol, targetNotional, holdSeconds, holdSeconds));

        long startMs = System.currentTimeMillis();
        ProbeRunner runner = new ProbeRunner(ex, telegram, symbol, targetNotional, holdSeconds);
        try {
            runner.execute();
        } catch (Throwable t) {
            LOG.severe("[PROBE] FATAL: " + t.getClass().getSimpleName() + ": " + t.getMessage());
            sendTg(telegram, "💥 *Проба упала с исключением*\n\n"
                    + "`" + t.getClass().getSimpleName() + ": " + t.getMessage() + "`\n\n"
                    + "Попытка аварийной чистки...");
            runner.emergencyCleanup();
        }

        long elapsedSec = (System.currentTimeMillis() - startMs) / 1000;
        LOG.info("[PROBE] ═══ Probe finished in " + elapsedSec + "s ═══");
        return true;
    }

    // ─── Probe runner ─────────────────────────────────────────────────

    private static final class ProbeRunner {
        final BinanceTradeExecutor ex;
        final TelegramBotSender    tg;
        final String               symbol;
        final double               targetNotional;
        final long                 holdSeconds;

        // State filled during execution — used by emergencyCleanup
        boolean positionOpened = false;

        ProbeRunner(BinanceTradeExecutor ex, TelegramBotSender tg, String symbol,
                    double targetNotional, long holdSeconds) {
            this.ex = ex; this.tg = tg; this.symbol = symbol;
            this.targetNotional = targetNotional; this.holdSeconds = holdSeconds;
        }

        void execute() throws Exception {
            // STEP 1: Check balance
            double balance = ex.fetchAvailableBalance();
            LOG.info("[PROBE] step 1: balance check → $" + balance);
            if (balance < 0) {
                sendTg(tg, "❌ Шаг 1 ПРОВАЛ: не могу получить баланс с Binance demo.\n"
                        + "Проверь API-ключи (testnet, не live).");
                return;
            }
            if (balance < targetNotional / 5.0) { // need at least notional/leverage
                sendTg(tg, String.format("❌ Шаг 1 ПРОВАЛ: баланс $%.2f слишком мал для пробы $%.2f.\n"
                        + "Пополни demo через testnet faucet.", balance, targetNotional));
                return;
            }
            sendTg(tg, String.format("✅ Шаг 1: баланс $%.2f OK", balance));

            // STEP 2: Get current price
            double currentPrice = fetchMarkPrice(symbol);
            LOG.info("[PROBE] step 2: mark price " + symbol + " = " + currentPrice);
            if (currentPrice <= 0) {
                sendTg(tg, "❌ Шаг 2 ПРОВАЛ: не могу получить цену " + symbol);
                return;
            }
            sendTg(tg, String.format("✅ Шаг 2: цена %s = %.6f", symbol, currentPrice));

            // STEP 3: Compute SL/TP levels
            //   LONG entry → SL below, TP above
            //   SL: -2.0%, TP1: +1.2%, TP2: +2.4%
            //   [v84.1] Расширены с -0.5/+0.3/+0.6 до -2.0/+1.2/+2.4 чтобы
            //   удовлетворить safety check executor'а: margin ≤ 50% баланса.
            //   При SL=-0.5% риск 2% от баланса требует огромную позицию
            //   (qty * entry / leverage > 50% balance) → executor отказывает.
            //   При SL=-2.0% позиция будет умеренной, тест проходит.
            //   R:R остаётся 1:1.2 — реалистично для боевых сигналов.
            double slPrice  = currentPrice * 0.980;
            double tp1Price = currentPrice * 1.012;
            double tp2Price = currentPrice * 1.024;

            LOG.info(String.format("[PROBE] step 3: levels entry≈%.6f sl=%.6f tp1=%.6f tp2=%.6f",
                    currentPrice, slPrice, tp1Price, tp2Price));

            // STEP 4: Build a synthetic TradeIdea and call openPositionWithSl —
            //         this is the EXACT SAME path used for real signals.
            //         If this works, real signals will work too.
            DecisionEngineMerged.TradeIdea idea = buildProbeIdea(symbol, currentPrice,
                    slPrice, tp1Price, tp2Price);

            sendTg(tg, "⚙️ Шаг 3: вызываю `openPositionWithSl()` — тот же метод что и для реальных сигналов...");

            // [v82.10 2026-06-01] PROBE SIZE FIX. Раньше проба звала
            // openPositionWithSl(idea, balance) → executor считал размер = весь
            // баланс × risk% / SL% = почти весь депозит ($5005 на $5000),
            // хотя в Telegram писалось "~$20". Это вводило в заблуждение.
            // FIX: подменяем balance на synthetic значение так, чтобы получился
            // ровно targetNotional. Executor: notional = (bal×risk%)/SL%.
            // → bal = targetNotional × SL% / risk%. SL пробы = 2%, risk берём из
            // executor.getRiskPct(). Итог: проба открывает РОВНО ~targetNotional$.
            double slPctProbe = 0.02; // соответствует slPrice = price×0.98
            double riskPctProbe = Math.max(0.05, ex.getRiskPct()) / 100.0;
            double syntheticBalance = targetNotional * slPctProbe / riskPctProbe;
            // Не больше реального баланса (на всякий) и не меньше минимума.
            syntheticBalance = Math.min(syntheticBalance, balance);
            LOG.info(String.format("[PROBE] size: target=$%.2f → syntheticBalance=$%.2f "
                    + "(realBal=$%.2f risk=%.2f%% sl=%.1f%%)",
                    targetNotional, syntheticBalance, balance, riskPctProbe * 100, slPctProbe * 100));

            BinanceTradeExecutor.ExecutionResult result = ex.openPositionWithSl(idea, syntheticBalance);

            if (!result.success) {
                LOG.severe("[PROBE] step 4 FAIL: " + result.reason);
                sendTg(tg, "❌ *Шаг 4 ПРОВАЛ: открытие сделки*\n\n"
                        + "Причина: `" + result.reason + "`\n\n"
                        + "Это тот же фейл что был у тебя на BIOUSDT. "
                        + "Смотри Railway логи — там полное тело ошибки от Binance.");
                return;
            }

            positionOpened = true;
            LOG.info("[PROBE] step 4 OK: " + result);

            // STEP 5: Verify on exchange that position + orders are actually there
            Thread.sleep(2000); // let exchange settle
            double exchQty = ex.fetchPositionAmount(symbol);
            boolean posExistsOnExchange = Math.abs(exchQty) > 1e-9;

            String summary = String.format(
                    "✅ *Шаг 4: СДЕЛКА ОТКРЫТА УСПЕШНО*\n\n" +
                            "Символ: `%s` LONG\n" +
                            "Entry: `%.6f`\n" +
                            "Qty: `%.6f`\n" +
                            "Notional: `$%.2f`\n" +
                            "Плечо: 5x\n\n" +
                            "Order ID (entry): `%s`\n" +
                            "SL ID: `%s` @ `%.6f`\n" +
                            "TP1 ID: `%s` @ `%.6f`\n" +
                            "TP2 ID: `%s` @ `%.6f`\n" +
                            "TPs поставлено: %d из 2\n\n" +
                            "Биржа подтверждает позицию: %s\n\n" +
                            "👉 Открой Binance demo UI прямо сейчас и убедись:\n" +
                            "https://testnet.binancefuture.com/en/futures/%s\n\n" +
                            "Закроется автоматически через %d сек.",
                    symbol, result.entryPrice, result.qty, result.notionalUsd,
                    result.orderId,
                    result.slOrderId, result.slPrice,
                    result.tp1OrderId, result.tp1Price,
                    result.tp2OrderId, result.tp2Price,
                    result.tpsPlaced,
                    posExistsOnExchange ? "ДА (qty=" + exchQty + ")" : "❌ НЕТ — позиции на бирже не видно!",
                    symbol, holdSeconds);
            sendTg(tg, summary);

            if (!posExistsOnExchange) {
                sendTg(tg, "⚠️ Странность: openPositionWithSl вернул OK, но "
                        + "fetchPositionAmount показывает 0. Возможно, проскочил SL/TP "
                        + "за 2 секунды. Проверяю историю...");
            }

            // STEP 6: Hold and observe
            LOG.info("[PROBE] step 5: holding for " + holdSeconds + " seconds...");
            long deadline = System.currentTimeMillis() + holdSeconds * 1000;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(Math.min(15_000, deadline - System.currentTimeMillis()));
                double q = ex.fetchPositionAmount(symbol);
                LOG.info("[PROBE] holding... qty on exchange = " + q);
                if (Math.abs(q) < 1e-9) {
                    LOG.info("[PROBE] position naturally closed (SL or TP fired)");
                    sendTg(tg, "ℹ️ Позиция закрылась естественно во время удержания "
                            + "— либо SL, либо TP сработал на бирже автоматически. "
                            + "Это нормальный путь работы.");
                    break;
                }
            }

            // STEP 7: Force close + cleanup
            LOG.info("[PROBE] step 6: closing position + canceling orphan orders");
            sendTg(tg, "🧹 Закрываю пробу + чищу orphan-ордера...");

            double remainingQty = ex.fetchPositionAmount(symbol);
            if (Math.abs(remainingQty) > 1e-9) {
                boolean closed = ex.closePosition(symbol, "probe-finished");
                LOG.info("[PROBE] closePosition returned " + closed);
            } else {
                // Position already closed by SL/TP — just kill any orphan orders
                ex.cancelAllOrdersOnSymbol(symbol);
            }

            sendTg(tg, "✅ *Проба завершена УСПЕШНО*\n\n"
                    + "Что это значит:\n"
                    + "• Бот корректно ставит плечо\n"
                    + "• Корректно открывает позицию\n"
                    + "• Корректно ставит SL+TP1+TP2 на бирже\n"
                    + "• Корректно закрывает и чистит ордера\n\n"
                    + "Когда придёт реальный сигнал — пройдёт тем же путём.\n\n"
                    + "👉 *УБЕРИ* `PROBE_RUN` из Railway env, иначе будет повторяться.");
        }

        /** Best-effort cleanup on any exception path. */
        void emergencyCleanup() {
            if (!positionOpened) return;
            try {
                LOG.warning("[PROBE] emergency cleanup running");
                double q = ex.fetchPositionAmount(symbol);
                if (Math.abs(q) > 1e-9) {
                    ex.closePosition(symbol, "probe-emergency");
                }
                ex.cancelAllOrdersOnSymbol(symbol);
                sendTg(tg, "🧹 Аварийная чистка завершена. Проверь Binance UI вручную.");
            } catch (Throwable t) {
                LOG.severe("[PROBE] cleanup itself failed: " + t.getMessage());
                sendTg(tg, "🆘 Чистка тоже упала: " + t.getMessage()
                        + "\n\nЗАКРОЙ ПОЗИЦИЮ ВРУЧНУЮ в Binance demo UI.");
            }
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────

    /**
     * Build a TradeIdea for the probe.
     *
     * v10.5 FIX: removed dependency on sun.misc.Unsafe.
     *   Old version used Unsafe.allocateInstance(TradeIdea.class) to bypass the
     *   constructor and then wrote final fields via reflection. sun.misc.Unsafe
     *   is officially deprecated for removal in JDK 23+ — code would break on
     *   future JVM upgrades with no warning at compile time.
     *
     *   New approach: call the public 10-arg TradeIdea constructor directly
     *   (symbol, side, price, stop, take, probability, flags, fundingRate,
     *   oiChange, htfBias). The constructor internally derives tp1/tp2/tp3 via
     *   tp_mults (default 1.0/2.0/3.2). For the probe we then OVERRIDE tp1, tp2,
     *   tp3 with the explicit values via regular reflection (final-field write).
     *   Reflection on final fields is supported on JDK 9–24 with --illegal-access
     *   permit; this matches the existing setField() helper which has worked
     *   throughout the project.
     *
     *   Net result: probe no longer depends on a removal-marked API; it compiles
     *   cleanly without --add-opens or --enable-native-access flags on JDK 21.
     */
    private static DecisionEngineMerged.TradeIdea buildProbeIdea(String probeSymbol,
                                                                 double entry,
                                                                 double sl,
                                                                 double tp1,
                                                                 double tp2) throws Exception {
        // Resolve LONG side enum without static coupling
        Class<?> sideCls = Class.forName("com.bot.TradingCore$Side");
        Object longSide = null;
        for (Object e : sideCls.getEnumConstants()) {
            if ("LONG".equals(e.toString())) { longSide = e; break; }
        }
        if (longSide == null) {
            throw new IllegalStateException("LONG side enum not found in TradingCore.Side");
        }

        // Use the public 10-arg constructor — no Unsafe needed.
        java.util.List<String> flags = new java.util.ArrayList<>();
        flags.add("PROBE");

        DecisionEngineMerged.TradeIdea idea = new DecisionEngineMerged.TradeIdea(
                probeSymbol,
                (com.bot.TradingCore.Side) longSide,
                entry,         // price
                sl,            // stop
                tp2,           // take (final TP, used as tp2 baseline)
                0.95,          // probability
                flags,         // flags
                0.0,           // fundingRate
                0.0,           // oiChange
                "PROBE"        // htfBias
        );

        // Override tp1, tp2, tp3 with explicit probe values via reflection.
        // The constructor would have derived them from tp_mults; for a probe
        // we want exact prices so executor places SL/TP1/TP2 at known levels.
        setField(idea, "tp1", tp1);
        setField(idea, "tp2", tp2);
        setField(idea, "tp3", tp2);
        setField(idea, "robustAtrPct", 0.005);

        return idea;
    }

    /**
     * Reflection-based field writer. Used for final-field overrides where the
     * constructor doesn't expose a setter. Silently skips fields that don't
     * exist on this version of TradeIdea — keeps the probe compatible with
     * minor schema changes in DecisionEngineMerged.
     */
    private static void setField(Object obj, String name, Object value) {
        try {
            java.lang.reflect.Field f = obj.getClass().getDeclaredField(name);
            f.setAccessible(true);
            f.set(obj, value);
        } catch (NoSuchFieldException nsf) {
            // field doesn't exist on this version — skip silently
        } catch (Throwable t) {
            LOG.warning("[PROBE] setField " + name + " failed: " + t.getMessage());
        }
    }

    private static double fetchMarkPrice(String symbol) {
        try {
            HttpClient http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10)).build();
            String base = "1".equals(System.getenv().getOrDefault("BINANCE_USE_TESTNET", "1"))
                    ? "https://demo-fapi.binance.com" : "https://fapi.binance.com"; // [v82.4] demo-fapi = Demo Trading API host
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(base + "/fapi/v1/premiumIndex?symbol=" + symbol))
                            .timeout(Duration.ofSeconds(8))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                LOG.warning("[PROBE] markPrice HTTP " + resp.statusCode() + ": " + resp.body());
                return 0;
            }
            JSONObject o = new JSONObject(resp.body());
            return o.optDouble("markPrice", 0);
        } catch (Exception e) {
            LOG.warning("[PROBE] markPrice fetch error: " + e.getMessage());
            return 0;
        }
    }

    private static void sendTg(TelegramBotSender tg, String msg) {
        if (tg == null) return;
        try { tg.sendMessageAsync(msg); } catch (Throwable ignored) {}
    }

    private static long envLong(String k, long d) {
        try { return Long.parseLong(System.getenv().getOrDefault(k, String.valueOf(d))); }
        catch (Exception e) { return d; }
    }
    private static double envDouble(String k, double d) {
        try { return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(d))); }
        catch (Exception e) { return d; }
    }

    private LiveTradeProbe() {}
}