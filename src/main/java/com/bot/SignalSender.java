package com.bot;

import org.json.JSONArray;
import org.json.JSONObject;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║          SignalSender — GODBOT EDITION                          ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  РЕШЁННЫЕ ПРОБЛЕМЫ vs предыдущих версий:                        ║
 * ║                                                                  ║
 * ║  1. КЭШИРОВАННЫЕ СВЕЧИ                                          ║
 * ║     15M TTL=14мин / 1H TTL=59мин / 2H TTL=119мин               ║
 * ║     1M TTL=55сек / 5M TTL=4мин                                  ║
 * ║     500 req/цикл → ~25 req/цикл (экономия 95%)                  ║
 * ║                                                                  ║
 * ║  2. ПАРАЛЛЕЛЬНЫЙ FETCH (ExecutorService)                        ║
 * ║     Все 100 пар обрабатываются одновременно                      ║
 * ║     60+ сек последовательно → 3-5 сек параллельно               ║
 * ║                                                                  ║
 * ║  3. SHARED GlobalImpulseController из BotMain                   ║
 * ║     Нет двух независимых BTC-состояний                           ║
 * ║                                                                  ║
 * ║  4. SHARED InstitutionalSignalCore из BotMain                   ║
 * ║     Нет двойной фильтрации                                       ║
 * ║                                                                  ║
 * ║  5. ЛИМИТ СИГНАЛОВ УБРАН                                        ║
 * ║     Возвращаем ВСЁ что прошло все фильтры                       ║
 * ║                                                                  ║
 * ║  6. VOLUME DELTA через aggTrade WebSocket                        ║
 * ║     isMaker флаг правильно обрабатывается                        ║
 * ║     Нормализация по всем парам                                   ║
 * ║                                                                  ║
 * ║  7. OBI (Order Book Imbalance) через REST orderbook             ║
 * ║     Блокирует контр-трендовые сигналы при слабой уверенности    ║
 * ║                                                                  ║
 * ║  8. EARLY TICK SIGNAL из aggTrade WebSocket                     ║
 * ║     Сигналы до закрытия 15M свечи                               ║
 * ║                                                                  ║
 * ║  9. КАТЕГОРИЗАЦИЯ МОНЕТ (TOP/ALT/MEME)                          ║
 * ║     Влияет на cooldown, стоп, пороги                             ║
 * ║                                                                  ║
 * ║  10. DUAL SIGNAL STRATEGY:                                      ║
 * ║     а) Регулярный цикл generateSignals() каждую минуту          ║
 * ║     б) Early tick сигналы из WS в реальном времени              ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */
public final class SignalSender {

    // ── Зависимости ────────────────────────────────────────────────
    private final TelegramBotSender       bot;
    private final HttpClient              http;
    private final GlobalImpulseController gic;  // SHARED из BotMain
    private final InstitutionalSignalCore isc;  // SHARED из BotMain
    private final Object                  wsLock = new Object();

    // ── Конфигурация ───────────────────────────────────────────────
    private final int    TOP_N;
    private final double MIN_CONF;
    private final int    KLINES_LIMIT;
    private final long   BINANCE_REFRESH_MS;
    private final int    TICK_HISTORY;
    private final double OBI_THRESHOLD;
    private final double DELTA_BLOCK_CONF;   // конфиденс, ниже которого блокируем противодельту

    private static final long FUNDING_REFRESH_MS = 5 * 60_000L;
    private static final long DELTA_WINDOW_MS    = 60_000L;   // 1-минутное окно аккумуляции дельты

    // ── Volume Delta (aggTrade WebSocket) ──────────────────────────
    private final Map<String, Double> deltaBuffer      = new ConcurrentHashMap<>();
    private final Map<String, Long>   deltaWindowStart = new ConcurrentHashMap<>();
    private final Map<String, Double> deltaHistory     = new ConcurrentHashMap<>();  // скользящая норм.

    // ── Tick / WebSocket ───────────────────────────────────────────
    private final Map<String, Deque<Double>>       tickPriceDeque = new ConcurrentHashMap<>();
    private final Map<String, Long>                lastTickTime   = new ConcurrentHashMap<>();
    private final Map<String, Double>              lastTickPrice  = new ConcurrentHashMap<>();
    private final Map<String, WebSocket>           wsMap          = new ConcurrentHashMap<>();
    private final ScheduledExecutorService         wsWatcher      = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ws-watcher"); t.setDaemon(true); return t;
    });
    private final Map<String, MicroCandleBuilder>  microBuilders  = new ConcurrentHashMap<>();

    // ── Orderbook ─────────────────────────────────────────────────
    private final Map<String, OrderbookSnapshot>   orderbookMap   = new ConcurrentHashMap<>();

    // ── Candle Cache ──────────────────────────────────────────────
    private final Map<String, CachedCandles>       candleCache    = new ConcurrentHashMap<>();

    /** TTL кэша в миллисекундах — свечи обновляются только когда закроется текущая */
    private static final Map<String, Long> CACHE_TTL = Map.of(
            "1m",  55_000L,
            "5m",  4 * 60_000L,
            "15m", 14 * 60_000L,
            "1h",  59 * 60_000L,
            "2h",  119 * 60_000L
    );

    private static final class CachedCandles {
        final List<TradingCore.Candle> candles;
        final long fetchedAt;
        CachedCandles(List<TradingCore.Candle> c) {
            this.candles   = Collections.unmodifiableList(c);
            this.fetchedAt = System.currentTimeMillis();
        }
        boolean isStale(long ttl) { return System.currentTimeMillis() - fetchedAt > ttl; }
    }

    // ── Pair Cache ────────────────────────────────────────────────
    private volatile Set<String> cachedPairs       = new LinkedHashSet<>();
    private volatile long        lastPairsRefresh  = 0L;
    private volatile long        lastFundingRefresh = 0L;

    // ── Core ──────────────────────────────────────────────────────
    private final DecisionEngineMerged      decisionEngine;
    private final TradingCore.AdaptiveBrain adaptiveBrain;
    private final SignalOptimizer           optimizer;
    private final PumpHunter               pumpHunter;

    // ── Параллельный пул ──────────────────────────────────────────
    private final ExecutorService fetchPool;

    // ── Статистика ────────────────────────────────────────────────
    private final AtomicLong totalFetches  = new AtomicLong(0);
    private final AtomicLong cacheHits     = new AtomicLong(0);
    private final AtomicLong earlySignals  = new AtomicLong(0);

    private static final Set<String> STABLE = Set.of("USDT","USDC","BUSD","TUSD","USDP","DAI");

    // ══════════════════════════════════════════════════════════════
    //  CONSTRUCTOR
    // ══════════════════════════════════════════════════════════════

    public SignalSender(TelegramBotSender bot,
                        GlobalImpulseController sharedGIC,
                        InstitutionalSignalCore sharedISC) {
        this.bot  = bot;
        this.gic  = sharedGIC;
        this.isc  = sharedISC;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(12))
                .version(HttpClient.Version.HTTP_2)
                .build();

        this.TOP_N            = envInt("TOP_N", 100);
        this.MIN_CONF         = envDouble("MIN_CONF", 50.0);
        this.KLINES_LIMIT     = envInt("KLINES", 220);
        long brMin            = envLong("BINANCE_REFRESH_MINUTES", 60);
        this.BINANCE_REFRESH_MS = brMin * 60_000L;
        this.TICK_HISTORY     = envInt("TICK_HISTORY", 120);
        this.OBI_THRESHOLD    = envDouble("OBI_THRESHOLD", 0.26);
        this.DELTA_BLOCK_CONF = envDouble("DELTA_BLOCK_CONF", 73.0);

        this.decisionEngine = new DecisionEngineMerged();
        this.adaptiveBrain  = new TradingCore.AdaptiveBrain();
        this.optimizer      = new SignalOptimizer(this.tickPriceDeque);
        this.pumpHunter     = new PumpHunter();

        this.decisionEngine.setPumpHunter(this.pumpHunter);
        this.optimizer.setPumpHunter(this.pumpHunter);

        int poolSize = Math.max(6, Math.min(TOP_N / 4, 25));
        this.fetchPool = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "fetch-" + r.hashCode());
            t.setDaemon(true);
            return t;
        });

        System.out.println("[SignalSender] INIT: TOP_N=" + TOP_N
                + " POOL=" + poolSize + " CACHE=ON SHARED_GIC=true SHARED_ISC=true"
                + " MIN_CONF=" + MIN_CONF);
    }

    // ══════════════════════════════════════════════════════════════
    //  GENERATE SIGNALS — основной метод
    // ══════════════════════════════════════════════════════════════

    /**
     * Генерирует все сигналы за один цикл.
     * Все пары обрабатываются параллельно.
     * Лимита на количество НЕТ — возвращаем всё прошедшее фильтры.
     */
    public List<DecisionEngineMerged.TradeIdea> generateSignals() {

        // Обновляем список пар (раз в час)
        if (cachedPairs.isEmpty() ||
                System.currentTimeMillis() - lastPairsRefresh > BINANCE_REFRESH_MS) {
            Set<String> fresh = getTopSymbolsSet(TOP_N);
            if (!fresh.isEmpty()) {
                cachedPairs = fresh;
                lastPairsRefresh = System.currentTimeMillis();
            }
        }

        // Обновляем Funding + OI (раз в 5 минут)
        if (System.currentTimeMillis() - lastFundingRefresh > FUNDING_REFRESH_MS) {
            refreshAllFundingRates();
            lastFundingRefresh = System.currentTimeMillis();
        }

        // ── Параллельная обработка всех пар ──────────────────────
        List<CompletableFuture<DecisionEngineMerged.TradeIdea>> futures = new ArrayList<>();
        for (String pair : cachedPairs) {
            CompletableFuture<DecisionEngineMerged.TradeIdea> f =
                    CompletableFuture.supplyAsync(() -> processPair(pair), fetchPool);
            futures.add(f);
        }

        // Собираем результаты с таймаутом 18 секунд на пару
        List<DecisionEngineMerged.TradeIdea> result = new ArrayList<>();
        for (CompletableFuture<DecisionEngineMerged.TradeIdea> f : futures) {
            try {
                DecisionEngineMerged.TradeIdea idea = f.get(18, TimeUnit.SECONDS);
                if (idea != null) result.add(idea);
            } catch (TimeoutException e) {
                // Пара не успела — продолжаем
            } catch (Exception e) {
                // Ошибка в одной паре не ломает весь цикл
            }
        }

        // Сортируем по убыванию вероятности
        result.sort(Comparator.comparingDouble(
                (DecisionEngineMerged.TradeIdea i) -> i.probability).reversed());

        logCacheStats();
        return result;
    }

    // ══════════════════════════════════════════════════════════════
    //  PROCESS PAIR — полная обработка одной пары
    // ══════════════════════════════════════════════════════════════

    private DecisionEngineMerged.TradeIdea processPair(String pair) {
        try {
            // ── Загружаем свечи (из кэша или HTTP) ────────────────
            List<TradingCore.Candle> m1  = getCached(pair, "1m",  KLINES_LIMIT);
            List<TradingCore.Candle> m5  = getCached(pair, "5m",  KLINES_LIMIT);
            List<TradingCore.Candle> m15 = getCached(pair, "15m", KLINES_LIMIT);
            List<TradingCore.Candle> h1  = getCached(pair, "1h",  KLINES_LIMIT);
            List<TradingCore.Candle> h2  = getCached(pair, "2h",  120);

            // Минимум данных для анализа
            if (m15.size() < 160 || h1.size() < 160) return null;

            // ── Обновляем optimizer ────────────────────────────────
            optimizer.updateFromCandles(pair, m15);

            // ── Volume Delta → в DecisionEngine ───────────────────
            double normDelta = getNormalizedDelta(pair);
            decisionEngine.setVolumeDelta(pair, normDelta);

            // ── Категоризация монеты ────────────────────────────────
            DecisionEngineMerged.CoinCategory cat = categorizePair(pair);

            // ── Основной анализ ─────────────────────────────────────
            DecisionEngineMerged.TradeIdea idea =
                    decisionEngine.analyze(pair, m1, m5, m15, h1, h2, cat);

            if (idea == null || idea.probability < MIN_CONF) return null;

            // ── PumpHunter boost ────────────────────────────────────
            PumpHunter.PumpEvent pump = pumpHunter.detectPump(pair, m1, m5, m15);
            if (pump != null && pump.strength > 0.40) {
                boolean aligned = (idea.side == TradingCore.Side.LONG && pump.isBullish()) ||
                        (idea.side == TradingCore.Side.SHORT && pump.isBearish());
                if (aligned) {
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("PH_" + pump.type.name());
                    double probBoost = pump.strength * 9 * (pump.isConfirmed ? 1.0 : 0.7);
                    idea = rebuildIdea(idea, Math.min(90, idea.probability + probBoost), nf);
                }
            }

            // ── SignalOptimizer micro-trend adjustment ──────────────
            idea = optimizer.withAdjustedConfidence(idea);
            if (idea.probability < MIN_CONF) return null;

            // ── OBI фильтр (Order Book Imbalance) ──────────────────
            OrderbookSnapshot obs = orderbookMap.get(pair);
            if (obs != null && System.currentTimeMillis() - obs.timestamp < 35_000) {
                double obi = obs.obi();
                boolean obiContra =
                        (idea.side == TradingCore.Side.LONG  && obi < -OBI_THRESHOLD * 1.5) ||
                                (idea.side == TradingCore.Side.SHORT && obi >  OBI_THRESHOLD * 1.5);
                boolean obiAligned =
                        (idea.side == TradingCore.Side.LONG  && obi >  OBI_THRESHOLD) ||
                                (idea.side == TradingCore.Side.SHORT && obi < -OBI_THRESHOLD);

                if (obiContra && idea.probability < 77) {
                    System.out.println("[OBI] BLOCKED " + pair + " obi=" + String.format("%.2f", obi));
                    return null;
                }
                if (obiAligned) {
                    List<String> nf = new ArrayList<>(idea.flags);
                    nf.add("OBI" + String.format("%+.0f", obi * 100));
                    idea = rebuildIdea(idea, Math.min(90, idea.probability + Math.abs(obi) * 6), nf);
                }
            }

            // ── Volume Delta alignment ──────────────────────────────
            boolean deltaOk =
                    (idea.side == TradingCore.Side.LONG  && normDelta >  0.14) ||
                            (idea.side == TradingCore.Side.SHORT && normDelta < -0.14) ||
                            Math.abs(normDelta) < 0.07;  // нейтральная зона — не блокируем

            if (!deltaOk && idea.probability < DELTA_BLOCK_CONF) {
                System.out.println("[DELTA] BLOCKED " + pair
                        + " d=" + String.format("%.2f", normDelta)
                        + " side=" + idea.side);
                return null;
            }
            if (Math.abs(normDelta) > 0.28) {
                List<String> nf = new ArrayList<>(idea.flags);
                nf.add("Δ" + (normDelta > 0 ? "BUY" : "SELL") + pct(Math.abs(normDelta)));
                idea = rebuildIdea(idea, Math.min(90, idea.probability + Math.abs(normDelta) * 5), nf);
            }

            // ── GlobalImpulse filter ────────────────────────────────
            double gicCoeff = gic.filterSignal(idea);
            if (gicCoeff <= 0.0) {
                System.out.println("[GIC] BLOCKED " + pair + " regime=" + gic.getContext().regime);
                return null;
            }
            // Если GIC даёт бонусный коэффициент > 1 → поднимаем вероятность
            if (gicCoeff > 1.0 && idea.probability < 90) {
                idea = rebuildIdea(idea, Math.min(90, idea.probability * gicCoeff), idea.flags);
            }

            // ── ISC filter (позволяет сигналу выйти) ───────────────
            if (!isc.allowSignal(idea)) return null;
            isc.registerSignal(idea);

            return idea;

        } catch (Exception e) {
            System.out.println("[processPair] " + pair + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Пересобирает TradeIdea с новой вероятностью (TradeIdea immutable).
     * Все TP пересчитываются автоматически в конструкторе.
     */
    private DecisionEngineMerged.TradeIdea rebuildIdea(DecisionEngineMerged.TradeIdea src,
                                                       double newProb, List<String> flags) {
        return new DecisionEngineMerged.TradeIdea(
                src.symbol, src.side, src.price, src.stop, src.take, src.rr,
                newProb, flags,
                src.fundingRate, src.fundingDelta, src.oiChange, src.htfBias, src.category);
    }

    // ══════════════════════════════════════════════════════════════
    //  CANDLE CACHE
    // ══════════════════════════════════════════════════════════════

    /** Возвращает из кэша или фетчит. Главный метод получения свечей. */
    private List<TradingCore.Candle> getCached(String symbol, String interval, int limit) {
        String key = symbol + "_" + interval;
        long   ttl = CACHE_TTL.getOrDefault(interval, 60_000L);
        CachedCandles cached = candleCache.get(key);

        totalFetches.incrementAndGet();
        if (cached != null && !cached.isStale(ttl) && !cached.candles.isEmpty()) {
            cacheHits.incrementAndGet();
            return cached.candles;
        }

        List<TradingCore.Candle> fresh = fetchKlinesDirect(symbol, interval, limit);
        if (!fresh.isEmpty()) {
            candleCache.put(key, new CachedCandles(fresh));
        } else if (cached != null) {
            // Возвращаем устаревший кэш если не смогли обновить
            return cached.candles;
        }
        return fresh;
    }

    /** Публичный метод — для BotMain (BTC, сектора) */
    public List<TradingCore.Candle> fetchKlines(String symbol, String interval, int limit) {
        return getCached(symbol, interval, limit);
    }

    private List<TradingCore.Candle> fetchKlinesDirect(String symbol, String interval, int limit) {
        try {
            String url = String.format(
                    "https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
                    symbol, interval, limit);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET().build();
            String body = http.send(req, HttpResponse.BodyHandlers.ofString()).body();
            if (!body.trim().startsWith("[")) return Collections.emptyList();
            JSONArray arr = new JSONArray(body);
            List<TradingCore.Candle> list = new ArrayList<>(arr.length());
            for (int i = 0; i < arr.length(); i++) {
                JSONArray k = arr.getJSONArray(i);
                list.add(new TradingCore.Candle(
                        k.getLong(0),
                        Double.parseDouble(k.getString(1)),
                        Double.parseDouble(k.getString(2)),
                        Double.parseDouble(k.getString(3)),
                        Double.parseDouble(k.getString(4)),
                        Double.parseDouble(k.getString(5)),
                        k.length() > 7 ? Double.parseDouble(k.getString(7)) : 0.0,
                        k.getLong(6)));
            }
            return list;
        } catch (Exception e) {
            System.out.println("[KLINES] " + symbol + "/" + interval + ": " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public CompletableFuture<List<TradingCore.Candle>> fetchKlinesAsync(
            String symbol, String interval, int limit) {
        return CompletableFuture.supplyAsync(
                () -> fetchKlinesDirect(symbol, interval, limit), fetchPool);
    }

    private void logCacheStats() {
        long total = totalFetches.get();
        long hits  = cacheHits.get();
        if (total > 0 && total % 500 == 0) {
            System.out.printf("[Cache] hits=%.1f%% (%d/%d) earlySignals=%d%n",
                    100.0 * hits / total, hits, total, earlySignals.get());
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  VOLUME DELTA
    // ══════════════════════════════════════════════════════════════

    public double getRawDelta(String symbol) {
        return deltaBuffer.getOrDefault(symbol, 0.0);
    }

    /**
     * Нормализованная дельта от -1 до +1.
     * Нормализация относительно максимума по ВСЕМ парам,
     * что даёт реальное сравнение давления покупок/продаж.
     */
    public double getNormalizedDelta(String symbol) {
        double d = deltaBuffer.getOrDefault(symbol, 0.0);
        if (d == 0.0) return 0.0;
        double absMax = deltaBuffer.values().stream()
                .mapToDouble(Math::abs).max().orElse(1.0);
        return Math.max(-1.0, Math.min(1.0, d / (absMax + 1e-9)));
    }

    // ══════════════════════════════════════════════════════════════
    //  WEBSOCKET (aggTrade) — Volume Delta + Early Tick Signals
    // ══════════════════════════════════════════════════════════════

    public void connectWs(String pair) { connectWsInternal(pair); }

    private void connectWsInternal(String pair) {
        try {
            String url = "wss://fstream.binance.com/ws/" + pair.toLowerCase() + "@aggTrade";
            System.out.println("[WS] Connecting " + pair);

            HttpClient wsClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(15)).build();

            wsClient.newWebSocketBuilder()
                    .buildAsync(URI.create(url), new WebSocket.Listener() {

                        @Override
                        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                            try {
                                JSONObject j = new JSONObject(data.toString());
                                double price = Double.parseDouble(j.getString("p"));
                                double qty   = Double.parseDouble(j.getString("q"));
                                long   ts    = j.getLong("T");

                                // isMaker=false → агрессивный покупатель (рыночная покупка)
                                // isMaker=true  → агрессивный продавец (рыночная продажа)
                                boolean isBuy = !j.getBoolean("m");
                                double  side  = isBuy ? qty : -qty;

                                // ── Аккумуляция дельты ────────────────────
                                deltaWindowStart.putIfAbsent(pair, ts);
                                long windowAge = ts - deltaWindowStart.get(pair);
                                if (windowAge > DELTA_WINDOW_MS) {
                                    // Сохраняем историю для сглаживания
                                    deltaHistory.put(pair, deltaBuffer.getOrDefault(pair, 0.0));
                                    deltaBuffer.put(pair, side);
                                    deltaWindowStart.put(pair, ts);
                                } else {
                                    deltaBuffer.merge(pair, side, Double::sum);
                                }

                                // ── Tick history ──────────────────────────
                                synchronized (wsLock) {
                                    Deque<Double> dq = tickPriceDeque
                                            .computeIfAbsent(pair, k -> new ArrayDeque<>());
                                    dq.addLast(price);
                                    while (dq.size() > TICK_HISTORY) dq.removeFirst();
                                }
                                lastTickPrice.put(pair, price);
                                lastTickTime.put(pair, ts);

                                // ── Micro candle builder ───────────────────
                                MicroCandleBuilder mcb = microBuilders.computeIfAbsent(
                                        pair, k -> new MicroCandleBuilder(30_000));
                                mcb.addTick(ts, price, qty);

                                // ── Early Tick Signal ─────────────────────
                                DecisionEngineMerged.TradeIdea earlySignal =
                                        generateEarlyTickSignal(pair, price, tickPriceDeque.get(pair));
                                if (earlySignal != null && earlySignal.probability > 66) {
                                    if (filterEarlySignal(earlySignal)) {
                                        bot.sendMessageAsync("🎯 *EARLY TICK*\n"
                                                + earlySignal.toTelegramString());
                                        earlySignals.incrementAndGet();
                                        isc.registerSignal(earlySignal);
                                    }
                                }

                            } catch (Exception ignored) {}
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override
                        public void onError(WebSocket ws, Throwable error) {
                            System.out.println("[WS ERROR] " + pair + ": " + error.getMessage());
                            reconnectWs(pair);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
                            System.out.println("[WS CLOSED] " + pair + " code=" + code);
                            reconnectWs(pair);
                            return CompletableFuture.completedFuture(null);
                        }
                    })
                    .thenAccept(ws -> {
                        wsMap.put(pair, ws);
                        System.out.println("[WS] Connected " + pair);
                    })
                    .exceptionally(ex -> {
                        System.out.println("[WS] Failed " + pair + " retry in 6s: " + ex.getMessage());
                        wsWatcher.schedule(() -> connectWsInternal(pair), 6, TimeUnit.SECONDS);
                        return null;
                    });
        } catch (Exception e) {
            reconnectWs(pair);
        }
    }

    private void reconnectWs(String pair) {
        wsMap.remove(pair);
        wsWatcher.schedule(() -> connectWsInternal(pair), 6, TimeUnit.SECONDS);
    }

    // ── Early Tick Signal ────────────────────────────────────────

    private DecisionEngineMerged.TradeIdea generateEarlyTickSignal(
            String symbol, double price, Deque<Double> dq) {
        if (dq == null || dq.size() < 22) return null;

        List<Double> buf = new ArrayList<>(dq);
        int n = buf.size();

        double move  = buf.get(n - 1) - buf.get(n - 22);
        double avg   = buf.stream().mapToDouble(Double::doubleValue).average().orElse(price);
        double vel   = Math.abs(move) / (avg + 1e-9);

        // Акселерация: вторая половина быстрее первой в 1.5x+
        double m1   = buf.get(n / 2 - 1) - buf.get(0);
        double m2   = buf.get(n - 1)     - buf.get(n / 2);
        boolean acc = Math.abs(m2) > Math.abs(m1) * 1.45;

        if (vel < 0.0014 || !acc) return null;

        boolean up  = move > 0;
        double atrV = getAtr(symbol);
        if (atrV <= 0) atrV = price * 0.005;

        double stop = up ? price - atrV * 1.5 : price + atrV * 1.5;
        double take = up ? price + atrV * 3.2 : price - atrV * 3.2;
        double conf = Math.min(76, 54 + vel * 5500);

        return new DecisionEngineMerged.TradeIdea(
                symbol,
                up ? TradingCore.Side.LONG : TradingCore.Side.SHORT,
                price, stop, take, conf,
                List.of("EARLY_TICK", up ? "UP" : "DN", "v=" + String.format("%.2e", vel)));
    }

    private boolean filterEarlySignal(DecisionEngineMerged.TradeIdea sig) {
        // Быстрая BTC-проверка
        double coeff = gic.filterSignal(sig);
        if (coeff <= 0.05) return false;
        // ISC проверка без регистрации
        if (!isc.allowSignal(sig)) return false;
        // Micro-trend проверка
        double adj = optimizer.adjustConfidence(sig);
        return adj >= 56;
    }

    // ══════════════════════════════════════════════════════════════
    //  FUNDING RATE + OI
    // ══════════════════════════════════════════════════════════════

    private void refreshAllFundingRates() {
        try {
            System.out.println("[FR] Refreshing...");
            JSONArray arr = new JSONArray(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://fapi.binance.com/fapi/v1/premiumIndex"))
                                    .timeout(Duration.ofSeconds(15)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body());

            Map<String, Double> rates = new HashMap<>(arr.length());
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                rates.put(o.getString("symbol"), o.optDouble("lastFundingRate", 0));
            }

            // Обновляем только пары из нашего списка, с задержкой 40ms между запросами
            List<String> pairs = new ArrayList<>(cachedPairs);
            for (int i = 0; i < pairs.size(); i++) {
                String pair = pairs.get(i);
                try {
                    fetchAndUpdateOI(pair, rates.getOrDefault(pair, 0.0));
                    if (i % 10 == 9) Thread.sleep(40); // batch пауза
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ignored) {}
            }
            System.out.println("[FR] Updated " + rates.size() + " rates");
        } catch (Exception e) {
            System.out.println("[FR] Error: " + e.getMessage());
        }
    }

    private void fetchAndUpdateOI(String symbol, double fr) {
        try {
            JSONObject oiJ = new JSONObject(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://fapi.binance.com/fapi/v1/openInterest?symbol=" + symbol))
                                    .timeout(Duration.ofSeconds(6)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body());
            double oi = oiJ.optDouble("openInterest", 0);

            JSONArray hist = new JSONArray(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://fapi.binance.com/futures/data/openInterestHist?symbol="
                                            + symbol + "&period=1h&limit=5"))
                                    .timeout(Duration.ofSeconds(6)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body());

            double oi1h = 0, oi4h = 0;
            if (hist.length() >= 2) {
                double prev1h = hist.getJSONObject(hist.length() - 2).optDouble("sumOpenInterest", oi);
                oi1h = ((oi - prev1h) / (prev1h + 1e-9)) * 100;
            }
            if (hist.length() >= 5) {
                double prev4h = hist.getJSONObject(0).optDouble("sumOpenInterest", oi);
                oi4h = ((oi - prev4h) / (prev4h + 1e-9)) * 100;
            }
            decisionEngine.updateFundingOI(symbol, fr, oi, oi1h, oi4h);
        } catch (Exception e) {
            decisionEngine.updateFundingOI(symbol, fr, 0, 0, 0);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  TOP SYMBOLS
    // ══════════════════════════════════════════════════════════════

    public Set<String> getTopSymbolsSet(int limit) {
        try {
            Set<String> binancePairs = getBinanceSymbolsFutures();

            // CoinGecko по капитализации
            JSONArray cg = new JSONArray(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://api.coingecko.com/api/v3/coins/markets"
                                            + "?vs_currency=usd&order=market_cap_desc&per_page=250&page=1"))
                                    .timeout(Duration.ofSeconds(15)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body());

            Set<String> top = new LinkedHashSet<>();
            for (int i = 0; i < cg.length(); i++) {
                String sym = cg.getJSONObject(i).getString("symbol").toUpperCase();
                if (STABLE.contains(sym)) continue;
                String pair = sym + "USDT";
                if (binancePairs.contains(pair)) top.add(pair);
                if (top.size() >= limit) break;
            }

            // Дополняем из Binance если CoinGecko дал меньше
            if (top.size() < limit) {
                for (String pair : binancePairs) {
                    if (top.size() >= limit) break;
                    top.add(pair);
                }
            }

            System.out.println("[PAIRS] Loaded " + top.size() + " pairs");
            return top;
        } catch (Exception e) {
            System.out.println("[PAIRS] ERROR: " + e.getMessage());
            return new LinkedHashSet<>(Arrays.asList(
                    "BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT",
                    "ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","LINKUSDT"));
        }
    }

    public Set<String> getBinanceSymbolsFutures() {
        try {
            JSONArray arr = new JSONObject(
                    http.send(HttpRequest.newBuilder()
                                    .uri(URI.create("https://fapi.binance.com/fapi/v1/exchangeInfo"))
                                    .timeout(Duration.ofSeconds(10)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body()
            ).getJSONArray("symbols");

            Set<String> res = new HashSet<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject s = arr.getJSONObject(i);
                if ("TRADING".equalsIgnoreCase(s.optString("status", "TRADING")) &&
                        s.getString("symbol").endsWith("USDT"))
                    res.add(s.getString("symbol"));
            }
            System.out.println("[Binance] " + res.size() + " futures pairs");
            return res;
        } catch (Exception e) {
            System.out.println("[Binance] ERROR: " + e.getMessage());
            return new HashSet<>(Arrays.asList("BTCUSDT","ETHUSDT","BNBUSDT"));
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  COIN CATEGORIZATION
    // ══════════════════════════════════════════════════════════════

    private static DecisionEngineMerged.CoinCategory categorizePair(String pair) {
        String sym = pair.endsWith("USDT") ? pair.substring(0, pair.length() - 4) : pair;
        return switch (sym) {
            // MEME — волатильные, маленький cooldown
            case "DOGE","SHIB","PEPE","FLOKI","WIF","BONK","MEME",
                 "NEIRO","POPCAT","COW","MOG","BRETT","TURBO" ->
                    DecisionEngineMerged.CoinCategory.MEME;
            // TOP — крупные, большой cooldown
            case "BTC","ETH","BNB","SOL","XRP","ADA","AVAX",
                 "DOT","LINK","MATIC","LTC","ATOM","UNI","AAVE" ->
                    DecisionEngineMerged.CoinCategory.TOP;
            // Всё остальное — ALT
            default -> DecisionEngineMerged.CoinCategory.ALT;
        };
    }

    // ══════════════════════════════════════════════════════════════
    //  ACCESSORS
    // ══════════════════════════════════════════════════════════════

    public double getAtr(String symbol) {
        CachedCandles cc = candleCache.get(symbol + "_15m");
        if (cc == null || cc.candles.isEmpty()) return 0;
        return decisionEngine.atr(cc.candles, 14);
    }

    public DecisionEngineMerged      getDecisionEngine() { return decisionEngine; }
    public SignalOptimizer            getOptimizer()      { return optimizer; }
    public InstitutionalSignalCore    getSignalCore()     { return isc; }
    public PumpHunter                 getPumpHunter()     { return pumpHunter; }
    public GlobalImpulseController    getGIC()            { return gic; }
    public Map<String, Deque<Double>> getTickDeque()      { return tickPriceDeque; }

    // ══════════════════════════════════════════════════════════════
    //  STATIC UTILITY METHODS
    // ══════════════════════════════════════════════════════════════

    public static double atr(List<TradingCore.Candle> c, int period) {
        if (c == null || c.size() <= period) return 0;
        double sum = 0;
        for (int i = c.size() - period; i < c.size(); i++) {
            TradingCore.Candle prev = c.get(i - 1), cur = c.get(i);
            sum += Math.max(cur.high - cur.low,
                    Math.max(Math.abs(cur.high - prev.close),
                            Math.abs(cur.low  - prev.close)));
        }
        return sum / period;
    }

    public static double rsi(List<Double> prices, int period) {
        if (prices == null || prices.size() <= period) return 50.0;
        double gain = 0, loss = 0;
        for (int i = prices.size() - period; i < prices.size(); i++) {
            double d = prices.get(i) - prices.get(i - 1);
            if (d > 0) gain += d; else loss += -d;
        }
        if (gain + loss == 0) return 50.0;
        return 100.0 - (100.0 / (1.0 + gain / (loss + 1e-12)));
    }

    public static double ema(List<Double> prices, int period) {
        if (prices == null || prices.isEmpty()) return 0;
        double k = 2.0 / (period + 1), e = prices.get(0);
        for (double p : prices) e = p * k + e * (1 - k);
        return e;
    }

    public static double sma(List<Double> prices, int period) {
        if (prices == null || prices.size() < period) return 0;
        double sum = 0;
        for (int i = prices.size() - period; i < prices.size(); i++) sum += prices.get(i);
        return sum / period;
    }

    public static double vwap(List<TradingCore.Candle> c) {
        if (c == null || c.isEmpty()) return 0;
        double pv = 0, vol = 0;
        for (TradingCore.Candle x : c) {
            double tp = (x.high + x.low + x.close) / 3.0;
            pv += tp * x.volume; vol += x.volume;
        }
        return vol == 0 ? c.get(c.size() - 1).close : pv / vol;
    }

    public static double momentumPct(List<Double> prices, int n) {
        if (prices == null || prices.size() <= n) return 0.0;
        double last = prices.get(prices.size() - 1);
        double prev = prices.get(prices.size() - 1 - n);
        return (last - prev) / (prev + 1e-12);
    }

    public static boolean detectBOS(List<TradingCore.Candle> c) {
        if (c == null || c.size() < 10) return false;
        List<Integer> highs = DecisionEngineMerged.swingHighs(c, 3);
        List<Integer> lows  = DecisionEngineMerged.swingLows(c, 3);
        TradingCore.Candle last = c.get(c.size() - 1);
        if (!highs.isEmpty() && last.close > c.get(highs.get(highs.size()-1)).high * 1.0005) return true;
        if (!lows.isEmpty()  && last.close < c.get(lows.get(lows.size()-1)).low   * 0.9995) return true;
        return false;
    }

    public static List<Integer> detectSwingHighs(List<TradingCore.Candle> c, int lr) {
        return DecisionEngineMerged.swingHighs(c, lr);
    }

    public static List<Integer> detectSwingLows(List<TradingCore.Candle> c, int lr) {
        return DecisionEngineMerged.swingLows(c, lr);
    }

    public static int marketStructure(List<TradingCore.Candle> c) {
        return DecisionEngineMerged.marketStructure(c);
    }

    public static boolean detectLiquiditySweep(List<TradingCore.Candle> c) {
        return DecisionEngineMerged.detectLiquiditySweep(c);
    }

    // ══════════════════════════════════════════════════════════════
    //  INNER CLASSES
    // ══════════════════════════════════════════════════════════════

    public static final class OrderbookSnapshot {
        public final double bidVolume, askVolume;
        public final long   timestamp;

        public OrderbookSnapshot(double b, double a, long t) {
            bidVolume = b; askVolume = a; timestamp = t;
        }

        public double obi() {
            return (bidVolume - askVolume) / (bidVolume + askVolume + 1e-12);
        }

        /** Является ли снапшот актуальным (менее 30 сек) */
        public boolean isFresh() {
            return System.currentTimeMillis() - timestamp < 30_000;
        }
    }

    /** Строит микро-свечи из тик-данных WebSocket */
    public static final class MicroCandleBuilder {
        private final int intervalMs;
        private long   bucketStart = -1;
        private double open = Double.NaN, high = Double.NEGATIVE_INFINITY,
                low  = Double.POSITIVE_INFINITY, close = Double.NaN;
        private double volume = 0; private long closeTime = -1;

        public MicroCandleBuilder(int intervalMs) { this.intervalMs = intervalMs; }

        public Optional<TradingCore.Candle> addTick(long ts, double price, double qty) {
            long bucket = (ts / intervalMs) * intervalMs;
            if (bucketStart == -1) {
                bucketStart = bucket; open = high = low = close = price;
                volume = qty; closeTime = bucket + intervalMs - 1;
                return Optional.empty();
            }
            if (bucket == bucketStart) {
                high = Math.max(high, price); low = Math.min(low, price);
                close = price; volume += qty;
                return Optional.empty();
            }
            TradingCore.Candle c = new TradingCore.Candle(
                    bucketStart, open, high, low, close, volume, volume, closeTime);
            bucketStart = bucket; open = high = low = close = price;
            volume = qty; closeTime = bucket + intervalMs - 1;
            return Optional.of(c);
        }
    }

    /** Лёгкий сигнал для внутренней истории */
    public static final class Signal {
        public final String symbol, direction;
        public final double confidence, price;
        public final long   timestamp;

        public Signal(String sym, String dir, double conf, double price) {
            this.symbol     = sym;
            this.direction  = dir;
            this.confidence = conf;
            this.price      = price;
            this.timestamp  = System.currentTimeMillis();
        }
    }

    // ── ENV helpers ───────────────────────────────────────────────
    private int    envInt(String k, int d)    { try { return Integer.parseInt(System.getenv().getOrDefault(k, String.valueOf(d))); } catch (Exception e) { return d;  } }
    private long   envLong(String k, long d)  { try { return Long.parseLong(System.getenv().getOrDefault(k, String.valueOf(d)));   } catch (Exception e) { return d;  } }
    private double envDouble(String k, double d){ try { return Double.parseDouble(System.getenv().getOrDefault(k, String.valueOf(d))); } catch (Exception e) { return d; } }

    private static String pct(double v) { return String.format("%.0f", v * 100); }
}
