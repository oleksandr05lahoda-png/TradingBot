-- bootstrap_fresh_project.sql — поднимает МИНИМАЛЬНЫЙ работающий стек на ЧИСТОМ проекте Supabase.
--
-- Зачем он существует: старая база (проект ptlxojilgqylzxshsrlm) умерла 10.08.2026 — пять
-- параллельных агентов создали 372 МБ временных таблиц в базе 1415 МБ, диск переполнился,
-- Postgres ушёл в петлю восстановления. DDL этой базы не сохранился НИГДЕ: единственный
-- настоящий DDL в репозитории — sql/paper_signals.sql, и он про другую таблицу.
-- Всё, что ниже, восстановлено ОБРАТНЫМ ХОДОМ по коду, воркфлоу n8n и живым ответам PostgREST,
-- сохранившимся в журналах выполнения n8n.
--
-- ─── ЧТО ЭТОТ СКРИПТ ПОКРЫВАЕТ ────────────────────────────────────────────────────────────
--   bot_orders          — очередь ордеров, несущая таблица; без неё не работает ни бот, ни зонд
--   pipeline_heartbeat  — реестр фидов, по нему живёт сторож lab-watchdog (+ строки-заготовки)
--   project_state       — журнал решений, на который ссылаются paper_signals.sql и комментарии
--   universes / universe_members / klines_4h / klines_1h / index_klines_4h / funding_history
--                       — витрина данных стенда (bar/funding), читается PaperStore
--   nl_micro / funding_snaps / trend_signals / liq_events — телеметрия старого стека SignalSender
--
-- ─── ЧЕГО ЭТОТ СКРИПТ НЕ ПОКРЫВАЕТ (осознанно) ────────────────────────────────────────────
--   * ДАННЫХ НЕТ. Восстанавливается только СТРУКТУРА. Свечи, фандинг, разметка листингов, весь
--     реестр лаборатории и её вердикты — не восстанавливаются ничем и ниоткуда: дампа нет.
--     Пустая klines_4h означает, что форвард лаборатории после подъёма стоит на нуле сделок.
--   * paper_signals — она в своём файле, sql/paper_signals.sql, и его надо применить отдельно.
--     ВНИМАНИЕ: тот файл ОТСТАЛ от живой таблицы (см. секцию 9 внизу) — применять с оговоркой.
--   * strategy_trials, lab_forward_status, таблицы листингов, базиса и ликвидаций лаборатории,
--     probe_tick / crypto_digest / v_execution_cost как рабочие объекты — см. секцию 8:
--     от них восстановились имена и одна-две колонки, этого мало, чтобы писать DDL не выдумывая.
--   * pg_cron-джобы. Их определения не сохранились нигде — ни в репозитории, ни в n8n.
--     Заводить заново вручную, см. чек-лист переезда в конце файла.
--
-- ─── ЧЕГО ЭТОТ СКРИПТ НЕ СОЗДАЁТ И ПОЧЕМУ ─────────────────────────────────────────────────
--   sleeve_gates и enqueue_live_order НЕ СОЗДАЮТСЯ. Их единственная задача — ставить в bot_orders
--   БОЕВЫЕ строки (testnet=false) по цене с fapi.binance.com; ровно этот путь однажды исполнил
--   настоящий ордер (bot_orders id=1, XRPUSDT $5.17, 08.07.2026). Стратегии, которая имела бы
--   право этим пользоваться, нет: 0 из 24 гипотез прошли форвард, направленный эдж закрыт как
--   settled. Воссоздать этот контур на новой базе означало бы вооружить его заново, без единого
--   довода в пользу того, что ему есть что исполнять. Кому понадобится — добавит осознанно,
--   отдельным файлом, и будет знать, что именно включил. Здесь его нет намеренно, а не по забывчивости.
--
-- ─── КАК ЧИТАТЬ КОММЕНТАРИИ ───────────────────────────────────────────────────────────────
--   [ДОКАЗАНО]  — имя/значение взято из литерала в коде, из строки запроса или из живого ответа БД
--   [ВЫВЕДЕНО]  — следует из того, КАК код читает поле (getLong/getDouble/optString), но не из схемы
--   [ДОГАДКА]   — правдоподобно и не подтверждено ничем; такие места помечены прямо в тексте
--   Точный Postgres-тип НЕ восстанавливается в принципе ни из одного источника: код различает
--   только «целое / дробное / строка / булево / ISO-время». Везде, где ниже стоит конкретный тип,
--   это НАШ ВЫБОР, а не находка — и там сказано, почему выбран именно он.
--
-- Скрипт идемпотентный: create ... if not exists, вставки с on conflict do nothing.
-- Прогнать дважды безопасно. Он ничего не удаляет и ничего не перезаписывает.

begin;


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- 1. bot_orders — очередь ордеров
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- ОТКУДА СХЕМА: у таблицы два потребителя на двух ветках, и они читают/пишут РАЗНЫЕ наборы колонок.
--   testnet-путь (эта ветка): src/main/java/com/bot/signal/SupabaseQueueSource.java
--       :97-102  select=id,symbol,side,entry,sl,atr,leverage,testnet,created_at
--       :148-153 пишет client_order_id, filled_qty, filled_price, executed_at, exec_note
--       :195-199 select=id,symbol,exec_note (очередь закрытий)
--       :231-238 пишет status, closed_at, close_qty, close_price, exec_note
--   боевой мост (ветка main): src/main/java/com/bot/SupabaseSignalBridge.java
--       :281,307 фильтр status+testnet+sent_at=lt.<ISO>, order=sent_at.asc
--       :335,412 фильтр status+testnet, order=created_at.asc
--       :441-451 читает entry, sl, tp1, tp2, size_mult, kind
--       :519-521 пишет status='sent' ВМЕСТЕ с sent_at
--   README.md:449-467 — «контракт очереди», но он НЕПОЛОН: там 9 колонок из 23 и 3 статуса из 9.
--       Таблица, поднятая строго по README, парализует боевой мост ТИХО: claimPending пишет
--       sent_at, PATCH возвращает 4xx, claim читается как «строку забрал кто-то другой»,
--       и ни одна pending не откроется без единой ошибки в логе. Поэтому здесь — объединение.
-- УВЕРЕННОСТЬ: имена всех 23 колонок — ДОКАЗАНО (литералы select=/JSONObject.put/get*).
--              Типы — ВЫВЕДЕНО и выбрано нами. NOT NULL — ВЫВЕДЕНО из getX-vs-optX, то есть это
--              обязательность ДЛЯ ПОТРЕБИТЕЛЯ, а не декларация из оригинальной схемы.
-- ВСТАВОК В ЭТУ ТАБЛИЦУ В JAVA НЕТ НИ НА ОДНОЙ ВЕТКЕ. Кто её наполняет — pg_cron и RPC снаружи
-- репозитория. Поэтому дефолты ниже выбраны так, чтобы забывчивый писатель ошибался в БЕЗОПАСНУЮ
-- сторону, а не создавал невидимые строки.

create table if not exists public.bot_orders (
    -- [ВЫВЕДЕНО] getLong("id") на обеих ветках; подставляется в URL без кавычек (SQS:156).
    -- bigint vs integer кодом не различить — берём bigint, он вмещает оба.
    -- generated BY DEFAULT, а не ALWAYS: единственный известный писатель (RPC probe_tick) внешний,
    -- и восстановление строк из выгрузки со своими id не должно упираться в OVERRIDING SYSTEM VALUE.
    id               bigint generated by default as identity primary key,

    -- [ВЫВЕДЕНО] row.getString("symbol") SQS:176 — бросает на NULL, поэтому not null.
    -- Код сам приводит к верхнему регистру (SQS:176, :219) — значит в БД регистр не гарантирован,
    -- и мы его тоже не навязываем: иначе писатель, пишущий нижним регистром, начнёт падать.
    symbol           text        not null,

    -- [ВЫВЕДЕНО] Side.valueOf(upper(side)) SQS:177. Домен — см. bot_orders_side_ck ниже.
    side             text        not null,

    -- [ВЫВЕДЕНО] getDouble("entry") SQS:179 и main:443 — обязателен ОБОИМ исполняющим путям.
    -- double precision, а не numeric: так же, как в sql/paper_signals.sql, и PostgREST отдаёт
    -- оба типа одинаковым JSON-числом, так что для кода выбор безразличен.
    entry            double precision not null,

    -- [ДОКАЗАНО nullable] SQS:180-181 явно проверяет row.has("sl") && !row.isNull("sl"),
    -- и тест SupabaseQueueSourceTest.java:79 гоняет "sl": null.
    -- На testnet-пути стоп опционален при наличии atr; боевому мосту он ФАКТИЧЕСКИ обязателен
    -- (main:444 getDouble бросит, строка уедет в 'failed'). Это расхождение потребителей,
    -- а не схемы, — поэтому колонка nullable.
    sl               double precision,

    -- [ДОКАЗАНО nullable] SQS:182-183. Читает ТОЛЬКО testnet-путь; main про atr не знает.
    atr              double precision,

    -- [ВЫВЕДЕНО] row.getInt("leverage") SQS:184-185, при отсутствии берётся defaultLeverage.
    -- Допустимый диапазон [1..5] (RiskConstants.java:13). CHECK здесь НЕ ставим намеренно:
    -- код строку с leverage=20 не зажимает, а ОТКАЗЫВАЕТ ей и записывает причину в exec_note
    -- (SQS:187-189, :276 «причина важна не меньше отказа»). CHECK перенёс бы отказ туда, где его
    -- никто не увидит — в проглоченную ошибку вставки у писателя. Готовый вариант — в секции 8.
    leverage         integer,

    -- [ВЫВЕДЕНО] main:445-446 optDouble(tp1/tp2, 0); 0 или отсутствие = «не переопределять».
    -- Читает ТОЛЬКО боевой мост. tp2 служит одновременно take и tp3 (main:461).
    tp1              double precision,
    tp2              double precision,

    -- [ВЫВЕДЕНО] main:357,447 optDouble("size_mult", 1.0). Дефолт живёт в коде, не в схеме,
    -- поэтому здесь дефолта нет — иначе мы утверждали бы то, чего не видели.
    size_mult        double precision,

    -- [ВЫВЕДЕНО] main:451 optString("kind","basket"). Известные значения: 'basket' (дефолт в коде),
    -- 'probe' (строки зонда стоимости исполнения).
    kind             text,

    -- [ДОКАЗАНО участием в предикатах] eq./in. на обеих ветках. Домен — bot_orders_status_ck ниже.
    -- Дефолт 'pending': все внешние писатели заводят строку именно в этом состоянии, а NULL-статус
    -- сделал бы строку невидимой ОБОИМ потребителям (ни один фильтр eq. по ней не сработает).
    status           text        not null default 'pending',

    -- [ДОКАЗАНО] testnet=is.true (SQS:99,197) и testnet=eq.<bool> (main:281,307,335,412,436).
    -- NOT NULL обязателен: NULL не удовлетворяет НИ eq.true, НИ eq.false — строка станет невидимой
    -- обоим потребителям и застрянет навсегда, молча.
    -- DEFAULT TRUE — сознательный выбор в безопасную сторону: писатель, забывший колонку, получит
    -- строку для ТЕСТОВОЙ сети, а не для боевой. Ошибаться должно быть безопасно.
    testnet          boolean     not null default true,

    -- [ВЫВЕДЕНО] main:336,413 order=created_at.asc; main:346 OffsetDateTime.parse — то есть
    -- ISO-8601 СО СМЕЩЕНИЕМ, ровно как PostgREST рендерит timestamptz (голый timestamp там не
    -- распарсится). Боевой мост по ней же считает строку протухшей (BRIDGE_MAX_PENDING_AGE_MIN).
    -- default now(): без значения строка сразу читается как бесконечно старая и уедет в 'failed'.
    created_at       timestamptz not null default now(),

    -- [ДОКАЗАНО] пишется main:520 вместе с захватом; фильтруется sent_at=lt.<ISO> (main:281,307).
    -- ⚠ testnet-путь sent_at НЕ ПИШЕТ (SQS:261 кладёт только status). Обе развёртки боевого моста
    -- фильтруют sent_at=lt., а NULL под lt. не попадает — значит строки, захваченные testnet-путём,
    -- для reconcileSent/reconcileOpen НЕВИДИМЫ навсегда, при этом countActive их СЧИТАЕТ и они
    -- бессрочно занимают слоты BRIDGE_MAX_OPEN. Это дефект КОДА, а не схемы; чинить в SQS.claim(),
    -- не здесь. Записано, чтобы при подъёме это не выглядело загадкой.
    sent_at          timestamptz,

    -- [ДОКАЗАНО] пишут ОБА пути: SQS:233 и main:319,554.
    closed_at        timestamptz,

    -- [ДОКАЗАНО] SQS:152. Момент фактического филла; пишет только testnet-путь.
    executed_at      timestamptz,

    -- [ДОКАЗАНО] пишут оба пути. ТИП: text, а НЕ varchar(300), хотя main режет до 300 (main:320,553).
    -- Это подсказка, но не доказательство, а testnet-путь пишет БЕЗ обрезки, включая произвольной
    -- длины e.getMessage() (SQS:154,238,276). varchar(300) уронил бы такие записи на 22001 и съел
    -- бы ровно то, ради чего строка существует, — замер и причину отказа.
    exec_note        text,

    -- [ДОКАЗАНО] SQS:149-151, значения из ExecutionFeedback. Только testnet-путь.
    -- Здесь и живёт замер стоимости исполнения: entry — что предполагалось, filled_price — что вышло.
    client_order_id  text,
    filled_qty       double precision,
    filled_price     double precision,

    -- [ДОКАЗАНО] SQS:236-237. Отдельные колонки, а не перезапись filled_*: комментарий SQS:234-235
    -- объясняет почему — перезапись filled_price уничтожила бы замер проскальзывания ВХОДА.
    close_qty        double precision,
    close_price      double precision,

    -- Домен status. Объединение того, что умеют оба потребителя; ни один набор не полон сам по себе:
    --   testnet-путь читает pending, close_requested; пишет sent, rejected, close_sent, closed
    --   боевой мост   читает pending, sent, open, close_requested; пишет sent, open, closed,
    --                        failed, failed_naked, close_sent
    -- 'rejected' боевой мост не знает вовсе, 'failed'/'failed_naked' не знает testnet-путь.
    -- Если CHECK не примет ВСЕ девять — сломается ровно одна из сторон, и по-разному:
    -- testnet-путь на отказе логирует предупреждение и опросит строку СНОВА (SQS:281-284 буквально
    -- об этом предупреждает), боевой мост просто не сдвинет строку.
    -- ИМЯ КОНСТРЕЙНТА — ДОКАЗАНО: bot_orders_status_ck существовал в живой базе и включал
    -- 'failed_naked' (запрос к pg_constraint от 28.07.2026, зафиксирован в удалённом файле
    -- AUDIT_2026-07-28_claims.md:232, достаётся как git show c63342c^:AUDIT_2026-07-28_claims.md).
    -- СОСТАВ СПИСКА — наш, собран объединением по коду: тела оригинального CHECK никто не видел,
    -- и в базе могли быть значения, которые ставил pg_cron и которых Java не знает.
    constraint bot_orders_status_ck check (status in (
        'pending', 'sent', 'open', 'closed',
        'close_requested', 'close_sent',
        'failed', 'failed_naked', 'rejected'
    )),

    -- Домен side. ЗДЕСЬ МЫ СОЗНАТЕЛЬНО РАСХОДИМСЯ С ОРИГИНАЛОМ, и вот почему.
    -- В живой базе стоял bot_orders_side_long CHECK (side = 'LONG') — это ДОКАЗАНО тем же запросом
    -- к pg_constraint (AUDIT_2026-07-28_claims.md:25 и :232) и на него ссылается javadoc
    -- main:SupabaseSignalBridge.java:29-30. Но он был написан под БОЕВОЙ LONG-only мост, которого
    -- этот скрипт не поднимает. Потребитель, который поднимается, — testnet-путь: Side.valueOf
    -- принимает LONG и SHORT, и тест SupabaseQueueSourceTest.java:50 гоняет 'SHORT' и проходит.
    -- С констрейнтом side='LONG' такую строку было бы невозможно ВСТАВИТЬ.
    -- Имя другое (не bot_orders_side_long), чтобы никто не принял это за тот же объект.
    -- Кому нужен старый замок — он в секции 8, готовым оператором.
    constraint bot_orders_side_ck check (side in ('LONG', 'SHORT')),

    -- [ДОКАЗАНО имя и диапазон] bot_orders_mult, границы [0.2, 1.2] — из того же чтения
    -- pg_constraint (AUDIT_2026-07-28_claims.md:232). NULL разрешён: код подставляет 1.0 сам.
    constraint bot_orders_mult check (
        size_mult is null or (size_mult >= 0.2 and size_mult <= 1.2)
    )

    -- ⚠ bot_orders_geom СУЩЕСТВОВАЛ в живой базе — имя есть в том же чтении pg_constraint, —
    -- но его ТЕЛО не раскрыто нигде. Мы его НЕ ВОССТАНАВЛИВАЕМ: написать «правдоподобную геометрию»
    -- значило бы выдумать ограничение и выдать за найденное. Кандидат-реконструкция — в секции 8,
    -- закомментированный и помеченный как реконструкция.
);

-- Индексы под ФАКТИЧЕСКУЮ форму запросов. Колонки равенства впереди, колонка сортировки последней.
-- drainOpens/drainCloses: status=eq.X & testnet=eq.Y & order=created_at.asc  (main:335-336, 412-413)
create index if not exists bot_orders_queue_created
    on public.bot_orders (status, testnet, created_at);

-- reconcileSent/reconcileOpen: status=eq.X & testnet=eq.Y & sent_at=lt.Z & order=sent_at.asc
-- (main:281-282, 307-308). Частичный по not null: строки без sent_at этому запросу не видны
-- в принципе, и держать их в индексе незачем.
create index if not exists bot_orders_queue_sent
    on public.bot_orders (status, testnet, sent_at) where sent_at is not null;

-- poll/pollCloses testnet-пути: status=eq.X & testnet=is.true & order=id.asc (SQS:97-102, 195-199).
create index if not exists bot_orders_queue_id
    on public.bot_orders (status, testnet, id);

comment on table public.bot_orders is
    'Очередь торговых намерений. Всё, что её наполняет, находится ЗА границей доверия системы, поэтому каждая строка всё равно проходит RiskEngine, как если бы её ввели руками. Строка захватывается условным PATCH со старым статусом в предикате — два опрашивающих процесса не могут выиграть её оба. DDL этой таблицы не сохранился: схема восстановлена обратным ходом по коду двух веток, типы выбраны, а не найдены.';
comment on column public.bot_orders.testnet is
    'Фильтр во ВСЕХ запросах обеих веток. NULL здесь делает строку невидимой обоим потребителям, поэтому not null. Дефолт true: забывчивый писатель должен промахиваться в тестовую сеть, а не в боевую.';
comment on column public.bot_orders.entry is
    'Намеренная цена входа. Разница между entry и filled_price и ЕСТЬ стоимость исполнения — ради этого замера строка и живёт после закрытия.';
comment on column public.bot_orders.exec_note is
    'Пишут оба потребителя, но по-разному: боевой мост кладёт сюда операционные заметки, а testnet-путь ЧИТАЕТ эту же колонку как человеческую причину закрытия (SupabaseQueueSource.java:199,218-220). На общей таблице заметка моста станет reason у CloseRequest — это известное расхождение, а не опечатка.';
comment on column public.bot_orders.status is
    'pending -> sent -> (open) -> closed по основному пути; close_requested -> close_sent -> closed по закрытию; rejected/failed/failed_naked — терминальные отказы. Цепочка НЕ линейна: SupabaseQueueSource.reject() пишет rejected БЕЗ предиката статуса и может перекрыть любое состояние.';


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- 2. pipeline_heartbeat — реестр фидов, по которому живёт сторож
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- ОТКУДА СХЕМА: воркфлоу n8n uoNQxhXYHcv4GJGt (lab-watchdog), нода "Read Heartbeats" —
--   GET /rest/v1/pipeline_heartbeat?select=feed,status,last_ok_at,updated_at,note
-- Имена колонок — ДОКАЗАНО строкой select. Значения — ДОКАЗАНО живым ответом БД: execution 1773
-- (ручной прогон 2026-08-10T09:37Z, ещё до смерти базы) содержит все 18 строк целиком.
-- Это последнее окно в живую схему, поэтому список фидов ниже — факт, а не реконструкция.
-- УВЕРЕННОСТЬ: колонки доказаны; типы выведены из формата значений
--   ('2026-08-10T08:15:00.39314+00:00' — микросекунды со смещением, то есть timestamptz).
-- СПИСОК КОЛОНОК — НИЖНЯЯ ГРАНИЦА: select явный, были ли в таблице id или created_at, не видно.

create table if not exists public.pipeline_heartbeat (
    -- Сторож обращается с feed как с уникальным ключом: seen[j.feed] = j (нода "Evaluate Freshness").
    -- Уникальность НЕ доказана DDL-ом, но 18 живых строк её не опровергают, а on conflict ниже
    -- без неё не работает. Это самое сильное допущение в файле — и оно здесь названо.
    feed        text primary key,

    -- [ДОКАЗАНО значения] 'ok' — во всех 18 живых строках; 'error' — сравнение j.status === 'error'
    -- в коде сторожа. CHECK не ставим: третье значение может писать неизвестный нам писатель,
    -- и запретить его означало бы уронить чужую запись ради стройности.
    status      text        not null default 'ok',

    -- Единственное поле, по которому считается свежесть. Сторож защищается new Date(j.last_ok_at || 0),
    -- то есть допускает NULL.
    last_ok_at  timestamptz,

    -- Сторож ВЫБИРАЕТ эту колонку и нигде не использует. Во всех 18 живых строках updated_at
    -- побайтово совпадал с last_ok_at — значит писатель обновляет их одним апдейтом.
    updated_at  timestamptz not null default now(),

    -- Свободный комментарий прогона. Живые примеры: 'riskon=true', 'opened=6 closed=0', 'rows=805'.
    -- В 8 из 18 строк был NULL.
    note        text
);

-- Строки-заготовки: без них сторож на пустой базе рапортует «❓ нет строки» по пяти фидам сразу.
-- ЧЕСТНАЯ ОГОВОРКА: last_ok_at оставлен NULL СПЕЦИАЛЬНО. Проставить сюда now() было бы соблазнительно
-- (сторож замолчал бы на 45 минут), но это записало бы успешный прогон, которого не было, — то есть
-- соврало бы ровно тому прибору, который поставлен ловить враньё. NULL читается сторожем как эпоха
-- 1970 и даёт честное «фид молчит»: на свежем проекте он действительно молчит, писателей ещё нет.
-- Кто хочет тихий старт — раскомментирует альтернативу в секции 8, понимая, что именно подделывает.
--
-- Список — ДОКАЗАНО: 18 значений feed из живого ответа БД (execution 1773). Пороги в скобках —
-- дословно из ноды "Evaluate Freshness":
--   const rules = { dip_klines: 45, dip_bounce: 130, klines_ingest: 320, lab_forward_run: 430, lab_forward: 1560 };
-- Возраст сторож проверяет ТОЛЬКО у этих пяти; status='error' — у любого фида.
insert into public.pipeline_heartbeat (feed, status, last_ok_at, note) values
    -- пять фидов, за возрастом которых сторож реально следит:
    ('dip_klines',        'ok', null, 'seeded by bootstrap — порог сторожа 45 мин, ещё ни разу не выполнялся'),
    ('dip_bounce',        'ok', null, 'seeded by bootstrap — порог сторожа 130 мин, ещё ни разу не выполнялся'),
    ('klines_ingest',     'ok', null, 'seeded by bootstrap — порог сторожа 320 мин, ещё ни разу не выполнялся'),
    ('lab_forward_run',   'ok', null, 'seeded by bootstrap — порог сторожа 430 мин, ещё ни разу не выполнялся'),
    ('lab_forward',       'ok', null, 'seeded by bootstrap — порог сторожа 1560 мин, ещё ни разу не выполнялся'),
    -- остальные наблюдаемые фиды: возраст не гейтится, но status='error' по ним сторож видит:
    ('regime_short_scan', 'ok', null, 'seeded by bootstrap'),
    ('regime_signal',     'ok', null, 'seeded by bootstrap'),
    ('pump_radar',        'ok', null, 'seeded by bootstrap'),
    ('mr_sleeve_run',     'ok', null, 'seeded by bootstrap'),
    ('basis_snapshot',    'ok', null, 'seeded by bootstrap'),
    ('perp_meta',         'ok', null, 'seeded by bootstrap'),
    ('new_listing_alert', 'ok', null, 'seeded by bootstrap'),
    ('fade_v2_scan',      'ok', null, 'seeded by bootstrap'),
    ('fsq_paper',         'ok', null, 'seeded by bootstrap'),
    ('announce_watch',    'ok', null, 'seeded by bootstrap'),
    ('carry_paper',       'ok', null, 'seeded by bootstrap'),
    ('structural_watch',  'ok', null, 'seeded by bootstrap')
on conflict (feed) do nothing;

-- ⚠ Восемнадцатый фид живой базы — long_v2_scan — НЕ ЗАСЕЯН НАМЕРЕННО. Это пульс боевого крона
-- (long_v2_label_one -> enqueue_live_order -> bot_orders с testnet=false), который этот скрипт
-- не поднимает. Строка сама по себе ничего не вооружает, но заявляла бы существование контура,
-- которого нет. Кто восстановит боевой путь осознанно — добавит и её:
--   insert into public.pipeline_heartbeat (feed, status, note)
--        values ('long_v2_scan', 'ok', 'live path — added deliberately') on conflict do nothing;

comment on table public.pipeline_heartbeat is
    'Реестр «когда фид в последний раз отработал». Читает сторож lab-watchdog (n8n uoNQxhXYHcv4GJGt) без фильтра и без limit. Отсутствие строки для гейтируемого фида — сама по себе тревога, поэтому заготовки засеяны при создании.';
comment on column public.pipeline_heartbeat.last_ok_at is
    'Момент последнего УСПЕШНОГО прогона. Единственное, по чему считается свежесть. NULL сторож читает как 1970 год, то есть как «молчит» — это правда для незапущенного фида, и подменять её текущим временем нельзя.';


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- 3. project_state — журнал решений
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- ⚠⚠ САМОЕ СЛАБОЕ МЕСТО ФАЙЛА. ЧИТАТЬ ПЕРЕД ПРИМЕНЕНИЕМ.
-- ОТКУДА СХЕМА: ниоткуда. На эту таблицу ссылаются десятки комментариев («project_state id=17»,
--   sql/paper_signals.sql:2,126; main:SupabaseSignalBridge.java:24,30,34,...), но НИ ОДИН запрос
--   к ней не сделан ни на одной ветке — проверено git grep по обеим. Восстановилась ровно одна
--   вещь: адресация по id. Встреченные id: 2,3,4,11,14,17,18,22,23,25,26,28,29,30,31,32a,50.
-- УВЕРЕННОСТЬ: существование и колонка id — ВЫВЕДЕНО. ВСЕ ОСТАЛЬНЫЕ КОЛОНКИ НИЖЕ — ДОГАДКА,
--   заглушка правдоподобной формы. Ни одного их имени никто не видел.
--   Тип id: значение 'id=32a' (main:SupabaseSignalBridge.java:331) не влезает в целое, поэтому text.
--   Либо id был текстовым, либо '32a' — ручная пометка «пункт 32, поправка a». Разрешить нельзя.
-- ПОСЛЕДСТВИЕ: ничего в коде эту таблицу не читает, поэтому неверная форма ничего не сломает —
--   но и содержимого старого журнала здесь нет и не будет. Если настоящая форма когда-нибудь
--   всплывёт (в выгрузке, в скриншоте, в другом чате) — эту секцию надо заменить, а не дополнить.

create table if not exists public.project_state (
    id          text primary key,               -- ВЫВЕДЕНО (адресация); тип text из-за '32a'
    statement   text,                           -- ДОГАДКА: сам текст утверждения/решения
    status      text,                           -- ДОГАДКА: подтверждено / опровергнуто / открыто
    recorded_at timestamptz not null default now()  -- ДОГАДКА
);

comment on table public.project_state is
    'ЗАГЛУШКА. Настоящая форма таблицы не восстановлена ничем: в репозитории на неё только ссылаются по id, ни одного запроса к ней нет. Колонки кроме id — догадка. Ничего в коде её не читает, поэтому ошибка формы безвредна, но и старого содержимого здесь нет.';


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- 4. Витрина данных стенда: universes, universe_members, klines_*, funding_history
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- ОТКУДА СХЕМА: git show feature/paper-harness:src/main/java/com/bot/paper/PaperStore.java
--   :49-72   loadBars — klines_4h / klines_1h / index_klines_4h: ts_ms, open, high, low, close, volume
--   :77-101  loadUniverse — universes(select=universe_id,frozen_at), universe_members(select=symbol)
--   :105-125 loadFunding — funding_history(funding_time_ms, funding_rate)
-- УВЕРЕННОСТЬ: имена колонок — ДОКАЗАНО (литералы в select= и в getLong/getDouble).
--   getLong/getDouble надёжно задают «целое/дробное», точный SQL-тип — наш выбор.
-- ⚠ Таблицы создаются ПУСТЫМИ. Свечей и фандинга нет и взять их неоткуда: дампа старой базы нет,
--   загрузка истории — отдельная задача (кроны klines_ingest / dip_klines, см. чек-лист).

-- Один и тот же loadBars ходит в три таблицы, поэтому форма у них обязана совпадать.
create table if not exists public.klines_4h (
    symbol   text   not null,
    ts_ms    bigint not null,          -- OPEN бара, эпоха мс; close_ms = ts_ms + 14400000
    open     double precision,
    high     double precision,
    low      double precision,
    close    double precision,
    volume   double precision,         -- optDouble(...,0): на чтении необязательна
    primary key (symbol, ts_ms)        -- ВЫВЕДЕНО из формы запросов, а не найдено в DDL
);

create table if not exists public.klines_1h (
    symbol   text   not null,
    ts_ms    bigint not null,
    open     double precision,
    high     double precision,
    low      double precision,
    close    double precision,
    volume   double precision,
    primary key (symbol, ts_ms)
);

-- Спот-прокси (индексная нога) для карри. Бар перпа и бар спота считаются парой ТОЛЬКО при
-- равенстве ts_ms — «ближайший по времени не подходит» (SPEC_FundingCarryV2.md §0).
-- Часового индексного аналога не существует: карри-маршрут на 1h невозможен по этой причине.
create table if not exists public.index_klines_4h (
    symbol   text   not null,
    ts_ms    bigint not null,
    open     double precision,
    high     double precision,
    low      double precision,
    close    double precision,
    volume   double precision,
    primary key (symbol, ts_ms)
);

create table if not exists public.funding_history (
    symbol           text   not null,
    funding_time_ms  bigint not null,      -- момент расчёта фандинга
    funding_rate     double precision,
    primary key (symbol, funding_time_ms)
);
-- ⚠ Известная ловушка этой таблицы (SPEC_FundingCarryV2.md §5.1): при RLS без политики PostgREST
-- отдаёт ПУСТОЙ МАССИВ БЕЗ ОШИБКИ. «Данных нет» и «читать не дают» выглядят одинаково.
-- Читатель обязан проверять и грант, и политику, а не делать вывод из пустого ответа.

-- Универсум — список символов, зафиксированный ДО прогона. Без заморозки набор можно подобрать
-- задним числом, и предрегистрация перестаёт быть предрегистрацией. Стенд отказывается стартовать
-- и на незамороженном универсуме, и на замороженном без членов (PaperStore.java:85-101).
create table if not exists public.universes (
    universe_id  text primary key,
    frozen_at    timestamptz            -- NULL = НЕ заморожен = запуск запрещён
);

create table if not exists public.universe_members (
    universe_id  text not null references public.universes(universe_id) on delete cascade,
    symbol       text not null,
    primary key (universe_id, symbol)
);
-- ⚠ Внешний ключ выше — НАШЕ ДОБАВЛЕНИЕ. В старой базе его никто не видел; он выведен из того,
-- что PaperStore читает членов по universe_id сразу после проверки самого универсума.
-- Если при загрузке выгрузки он мешает — снимать осознанно, это не находка, а решение.

comment on table public.universes is
    'Замороженный список символов. frozen_at IS NULL означает «менять ещё можно», и стенд на таком универсуме отказывается запускаться: универсум, который может измениться, — не предрегистрация.';


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- 5. Телеметрия старого стека SignalSender
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- ОТКУДА СХЕМА: main:src/main/java/com/bot/SignalSender.java — ключи JSON, которые уходят в POST,
--   и есть колонки (так сказано в комментарии-заголовке билдера :2802). Уникальные индексы
--   ДОКАЗАНЫ строкой запроса on_conflict=... — без них PostgREST вернёт ошибку на этот INSERT.
-- УВЕРЕННОСТЬ: имена и уникальность — ДОКАЗАНО; типы — ВЫВЕДЕНО из типов Java-полей.
-- ⚠ Это наследие УДАЛЁННОГО стека. На текущей ветке SignalSender нет; таблицы создаются на случай,
--   если этот код вернётся или если понадобится разобрать старые выгрузки. Для testnet-бота и
--   зонда стоимости они не нужны — секцию можно вырезать целиком, ничего не сломается.
--   Исключение: funding_snaps упоминалась в живом ответе crypto_digest как источник, который
--   пишет БОТ (stale_feeds = ['funding_snaps (bot)', 'funding_outcomes (cron)']).

create table if not exists public.nl_micro (
    symbol           text   not null,
    open_time        bigint not null,        -- эпоха мс
    open             double precision,
    high             double precision,
    low              double precision,
    close            double precision,
    volume           double precision,
    quote_volume     double precision,
    trades           integer,                -- numberOfTrades
    taker_buy_base   double precision,
    taker_buy_quote  double precision,
    funding          double precision,
    bid              double precision,
    ask              double precision,
    bid_depth        double precision,
    ask_depth        double precision,
    open_interest    double precision,
    snap_ms          bigint,
    live             boolean,
    -- [ДОКАЗАНО] on_conflict=symbol,open_time + Prefer: resolution=ignore-duplicates
    -- (SignalSender.java:2825, :2830)
    primary key (symbol, open_time)
);

create table if not exists public.funding_snaps (
    symbol             text   not null,
    snap_minute        bigint not null,      -- ⚠ НЕ timestamptz: currentTimeMillis()/60000,
                                             --   то есть целая МИНУТА эпохи (SignalSender.java:3004)
    funding_rate       double precision,
    prev_funding_rate  double precision,
    funding_delta      double precision,
    fr_acceleration    double precision,
    open_interest      double precision,
    peak_warn          boolean,
    trough_warn        boolean,
    snap_ms            bigint,
    -- [ДОКАЗАНО] on_conflict=symbol,snap_minute (SignalSender.java:3028)
    primary key (symbol, snap_minute)
);

create table if not exists public.trend_signals (
    symbol        text   not null,
    signal_day    bigint not null,           -- closeTime/86400000 — номер ДНЯ эпохи
    direction     integer not null,          -- +1 / -1
    entry         double precision,
    init_stop     double precision,
    atr           double precision,
    donchian_n    integer,
    bar_close_ms  bigint,
    snap_ms       bigint,
    -- [ДОКАЗАНО] on_conflict=symbol,signal_day,direction (SignalSender.java:3124)
    primary key (symbol, signal_day, direction)
);

-- ⚠ ЕДИНСТВЕННАЯ таблица в этом файле, про которую нельзя утверждать, что список колонок ПОЛОН.
-- В Supabase уходит json.put(e) — ВЕСЬ объект из буфера ликвидаций целиком (SignalSender.java:3267),
-- а не отобранный набор ключей. Список ниже выведен из соседней CSV-строки (:3264-3266). Если
-- WS-обработчик кладёт в буфер лишние ключи, INSERT потребует колонок, которых здесь нет.
-- Отдельно: по памяти проекта таблица мертва — Binance не отдаёт all-market liq-стрим на Railway.
-- Причина инфраструктурная, а не схемная, и этим скриптом не лечится.
create table if not exists public.liq_events (
    symbol      text   not null,
    order_time  bigint not null,
    side        text   not null,
    price       double precision not null,
    qty         double precision not null,
    notional    double precision,
    snap_ms     bigint,
    -- [ДОКАЗАНО] on_conflict=symbol,order_time,side,price,qty (SignalSender.java:3271)
    primary key (symbol, order_time, side, price, qty)
);


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- 6. RLS и доступ
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- ПОЧЕМУ ЭТО ЗДЕСЬ: аудит старой базы (проект ptlxojilgqylzxshsrlm, июнь 2026) нашёл RLS ВЫКЛЮЧЕННЫМ
-- на всех таблицах и открытый anon-доступ к RPC. То есть публикуемого ключа хватало, чтобы читать
-- очередь ордеров и дёргать функции. На новой базе эту ошибку не воспроизводим.
--
-- КАК ЭТО РАБОТАЕТ: RLS включён, политик НЕТ ни одной. Для ролей anon и authenticated это значит
-- «ноль строк при любом запросе», молча и без исключений. Роль service_role RLS обходит по
-- определению — именно поэтому бот ходит СЕКРЕТНЫМ ключом. Ровно на это рассчитывает и код:
-- SupabaseQueueSource.java:76-77 буквально говорит, что publishable-ключ с bot_orders не сделает
-- ничего и ключ обязан быть секретным.
--
-- ⚠ ЕСЛИ ПОЯВИТСЯ КЛИЕНТ, КОТОРОМУ НУЖЕН ДОСТУП БЕЗ СЕРВИСНОГО КЛЮЧА — писать политику ПОИМЁННО
-- под него, а не выключать RLS. Выключение RLS «чтобы заработало» — это и есть та самая дыра.

alter table public.bot_orders          enable row level security;
alter table public.pipeline_heartbeat  enable row level security;
alter table public.project_state       enable row level security;
alter table public.klines_4h           enable row level security;
alter table public.klines_1h           enable row level security;
alter table public.index_klines_4h     enable row level security;
alter table public.funding_history     enable row level security;
alter table public.universes           enable row level security;
alter table public.universe_members    enable row level security;
alter table public.nl_micro            enable row level security;
alter table public.funding_snaps       enable row level security;
alter table public.trend_signals       enable row level security;
alter table public.liq_events          enable row level security;

-- Вторая половина той же дыры: RPC. RLS на таблицах не защищает от функции, которая ходит в них
-- сама, а execute на функции в Postgres даётся PUBLIC по умолчанию — то есть новая RPC оказывается
-- доступна anon в момент создания, без единого явного гранта. Это ровно то, что нашёл прошлый аудит.
-- Строка ниже меняет умолчание для функций, которые ЭТА роль создаст ПОЗЖЕ в схеме public.
-- На уже существующие функции она не действует — их надо разувать поимённо (пример в секции 8).
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon')
       and exists (select 1 from pg_roles where rolname = 'authenticated') then
        execute 'alter default privileges in schema public revoke execute on functions from anon, authenticated';
    end if;
end $$;


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- 7. paper_signals — в отдельном файле, и он устарел
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- Здесь её НЕТ намеренно: DDL живёт в sql/paper_signals.sql, дублировать его значило бы завести
-- вторую редакцию одной схемы. Применять оттуда, ОТДЕЛЬНО и с оговоркой:
--
-- ⚠ sql/paper_signals.sql ОТСТАЛ от той таблицы, что была в живой базе. Доказательство —
--   комментарии git show feature/paper-harness:src/main/java/com/bot/paper/PaperStore.java,
--   которые ссылаются на объекты по имени: «the schema now permits» (:169-202),
--   «the atomic check now demands every term» (:214-232). Живая таблица имела сверх файла:
--     колонки: kind, bar_interval_ms, perp_entry_px, spot_entry_px, basis_entry_bp,
--              perp_exit_px, spot_exit_px, basis_exit_bp
--     констрейнты: paper_signals_carry_legs_ck, paper_signals_carry_exit_legs_ck
--     ослабление: stop_px стал nullable для kind='carry'
--     переписаны: paper_signals_geom_ck и paper_signals_outcome_atomic_ck
--   Их ТОЧНЫЕ формулировки не восстановимы. Если применить файл как есть, карри-строки
--   перестанут вставляться — это не гипотеза, это прямое следствие отсутствия carry-колонок.


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- 8. Что НЕ создано, и заготовки для тех, кто решит создать
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- Всё в этой секции ЗАКОММЕНТИРОВАНО. Здесь лежит то, что либо восстановилось слишком слабо,
-- чтобы писать DDL не выдумывая, либо восстановилось, но включать его — отдельное решение.
--
-- ── 8.1. Боевой контур. НЕ СОЗДАЁТСЯ, и заготовок здесь нет НАМЕРЕННО ─────────────────────
--   sleeve_gates       — таблица-рубильник; criteria->>'live' = true разрешал крону long-v2-scan
--                        ставить в bot_orders строки с testnet=false (HANDOFF.md:68-71)
--   enqueue_live_order — функция, которая берёт цену с БОЕВОГО fapi.binance.com и вставляет
--                        такую строку. Именно она однажды исполнила настоящий ордер.
--   long_v2_label_one  — шаг между кроном и enqueue_live_order (есть в коммите 9538c33,
--                        выпал из текущего HANDOFF.md при переписывании — легко потерять)
--   Ни формы, ни тела, ни заготовки. Кому это понадобится — пусть напишет сам и увидит, что пишет.
--
-- ── 8.2. Объекты, от которых восстановились только имена ──────────────────────────────────
--   strategy_trials    — известна ОДНА колонка passed_v2 (boolean) плюс то, что из неё берётся
--                        лидерборд лабы (комментарий ноды "Format message": «single source of
--                        truth (strategy_trials)»). Ключа рукава никто не назвал.
--                        На 28.07.2026: 63 строки, из них 0 с passed_v2=true.
--   lab_forward_status — известна ОДНА колонка forward_ok (boolean). 23 строки, 0 с true.
--                        ⚠ Не путать: 'lab_forward' и 'lab_forward_run' — это ЗНАЧЕНИЯ
--                        pipeline_heartbeat.feed, а не таблицы. Так же dip_klines и klines_ingest.
--   funding_outcomes   — имя видно только строкой 'funding_outcomes (cron)' в живом ответе
--                        crypto_digest. Размечалась pg_cron. Ни одной колонки.
--   таблицы листингов / базиса / ликвидаций лаборатории — восстановимы только имена ПОЛЕЙ JSON,
--                        которые отдавала crypto_digest (listings_total, top_basis.basis_bp, ...).
--                        Имён самих таблиц не знает ни один источник.
--   paper_signals_superseded — копия строк, посчитанных старым прибором. Формы нет; если нужна,
--                        честнее клонировать с живой:
--                          create table if not exists public.paper_signals_superseded
--                              (like public.paper_signals including all);
--   ⚠ Писать DDL по одной известной колонке — это выдумать таблицу и выдать за восстановленную.
--     Поэтому их здесь нет. Если всплывёт выгрузка — брать оттуда.
--
-- ── 8.3. Старый замок LONG-only ───────────────────────────────────────────────────────────
--   В живой базе он БЫЛ (pg_constraint, 28.07.2026). Здесь заменён на LONG+SHORT, потому что
--   testnet-путь принимает SHORT и его тест это проверяет. Кто поднимает БОЕВОЙ LONG-only мост —
--   тому нужен оригинал; тогда SHORT-строки станут невставляемыми, включая testnet-овые:
--     alter table public.bot_orders drop constraint if exists bot_orders_side_ck;
--     alter table public.bot_orders add  constraint bot_orders_side_long check (side = 'LONG');
--
-- ── 8.4. bot_orders_geom — РЕКОНСТРУКЦИЯ, НЕ ОРИГИНАЛ ─────────────────────────────────────
--   Констрейнт с таким именем существовал, его тело не раскрыто нигде. Ниже — правдоподобная
--   геометрия сделки в духе paper_signals_geom_ck. Это ДОГАДКА о содержании, а не находка.
--   Включать, только понимая, что запрещаешь:
--     alter table public.bot_orders add constraint bot_orders_geom check (
--         entry > 0
--         and (sl is null or sl > 0)
--         and (sl is null or side <> 'LONG'  or sl < entry)
--         and (sl is null or side <> 'SHORT' or sl > entry)
--     );
--
-- ── 8.5. CHECK на leverage — сознательно НЕ включён ───────────────────────────────────────
--   Диапазон [1..5] жёсткий (RiskConstants.java:13), но код строку вне диапазона ОТКАЗЫВАЕТ
--   и пишет причину в exec_note, а CHECK перенёс бы отказ к писателю, где его, скорее всего,
--   проглотит n8n. Видимость дороже строгости. Если всё же нужен:
--     alter table public.bot_orders add constraint bot_orders_leverage_ck
--         check (leverage is null or (leverage between 1 and 5));
--
-- ── 8.6. Тихий старт сторожа (подделывает успешный прогон — понимать, что делаешь) ────────
--     update public.pipeline_heartbeat set last_ok_at = now(), updated_at = now()
--      where last_ok_at is null;
--
-- ── 8.7. v_execution_cost — РЕКОНСТРУКЦИЯ ПО НАЗНАЧЕНИЮ, состав оригинала неизвестен ──────
--   Имя ДОКАЗАНО (sticky note воркфлоу aHRcu1IFVyTVstIj: «Результат: select * from v_execution_cost»).
--   Ни одной колонки оригинала не знает ни один источник. Ниже — то, что вообще ВОЗМОЖНО посчитать
--   из bot_orders: разрыв entry->filled_price и есть стоимость входа, ради неё строка и живёт.
--   Смысл затеи: гейты лаборатории берут стоимость исполнения константой 20-40 б.п., и никто её
--   не мерил. Имена колонок ниже — НАШИ, не оригинальные; отчёты, написанные под старую витрину,
--   с ней не сойдутся.
--     create or replace view public.v_execution_cost as
--     select id, symbol, side, kind, testnet, entry, filled_price, filled_qty,
--            close_price, close_qty, executed_at, closed_at,
--            case when entry > 0 and filled_price is not null
--                 then (filled_price - entry) / entry * 10000 end as entry_slip_bp,
--            case when filled_price is not null and filled_price > 0 and close_price is not null
--                 then (close_price - filled_price) / filled_price * 10000 end as round_trip_bp
--       from public.bot_orders
--      where kind = 'probe' and filled_price is not null;
--
-- ── 8.8. probe_tick — РЕКОНСТРУКЦИЯ ПО ОПИСАНИЮ, тело оригинала не сохранилось нигде ──────
--   ДОКАЗАНО: путь POST /rest/v1/rpc/probe_tick, оба аргумента (p_symbol, p_price), все семь полей
--   ответа (id, symbol, entry, action, expired, close_requested, stuck_closed) и их живые значения —
--   из журналов n8n (воркфлоу aHRcu1IFVyTVstIj, executions 1723/1726/1727/1732/1749/1810/1812).
--   ВЫВЕДЕНО ИЗ ТЕКСТА sticky note e3f34237, а НЕ из SQL: «протухшие pending отклоняются, отжившим
--   5 минут зондам ставится close_requested, и открывается не более ОДНОГО нового зонда
--   (LONG, стоп 2%, плечо 2, kind=probe)»; «pending-строки сами протухают через 10 мин».
--   Тела функции n8n не хранит. Всё ниже — НАША реализация этого описания, и она обязана быть
--   перечитана до включения. Стоп 2% записан ЦЕНОЙ (entry*0.98), потому что bot_orders.sl — цена;
--   хранил ли оригинал проценты в отдельной колонке, неизвестно. testnet=true прибит намертво:
--   зонд меряет стоимость исполнения, а не торгует.
--     create or replace function public.probe_tick(p_symbol text, p_price double precision)
--     returns jsonb language plpgsql security invoker as $$
--     declare v_expired int; v_close int; v_stuck int; v_id bigint; v_action text := 'skipped';
--     begin
--         update public.bot_orders set status = 'rejected', exec_note = 'probe: pending expired (>10 min)'
--          where kind = 'probe' and testnet and status = 'pending'
--            and created_at < now() - interval '10 minutes';
--         get diagnostics v_expired = row_count;
--
--         update public.bot_orders set status = 'close_requested', exec_note = 'probe: 5 min elapsed'
--          where kind = 'probe' and testnet and status in ('sent','open')
--            and coalesce(executed_at, sent_at, created_at) < now() - interval '5 minutes';
--         get diagnostics v_close = row_count;
--
--         update public.bot_orders set status = 'closed', closed_at = now(),
--                exec_note = 'probe: stuck in close, forced'
--          where kind = 'probe' and testnet and status = 'close_sent'
--            and created_at < now() - interval '30 minutes';
--         get diagnostics v_stuck = row_count;
--
--         if p_symbol is not null and p_price is not null and p_price > 0
--            and not exists (select 1 from public.bot_orders
--                             where kind = 'probe' and testnet
--                               and status in ('pending','sent','open','close_requested','close_sent'))
--         then
--             insert into public.bot_orders (symbol, side, entry, sl, leverage, kind, status, testnet)
--             values (upper(p_symbol), 'LONG', p_price, p_price * 0.98, 2, 'probe', 'pending', true)
--             returning id into v_id;
--             v_action := 'opened';
--         end if;
--
--         return jsonb_build_object('id', v_id, 'symbol', p_symbol, 'entry', p_price,
--                'action', v_action, 'expired', v_expired,
--                'close_requested', v_close, 'stuck_closed', v_stuck);
--     end $$;
--     -- И СРАЗУ, не откладывая: execute на функции даётся PUBLIC по умолчанию.
--     revoke execute on function public.probe_tick(text, double precision) from public, anon, authenticated;
--   ⚠ Правило «не более одного открытого зонда» здесь сделано глобальным по kind='probe'. Было ли
--     оно в оригинале глобальным или по символу — источник не различает, и в журнале нет ни одного
--     ответа с action ≠ 'opened' (бот не был запущен, зонды протухали), так что ветку «уже есть
--     открытый» проверить не на чем.
--
-- ── 8.9. crypto_digest — НЕ реконструируется ──────────────────────────────────────────────
--   Форма ответа известна ПОЛНОСТЬЮ (живой ответ, n8n y9gXurmsmOTWG3oU execution 1777): десятки
--   полей, вложенные lab и carry_regime, net_top и так далее. А вот источники половины этих чисел —
--   таблицы листингов, базиса, ликвидаций — не восстановлены даже по именам, и функция вдобавок
--   сама ходит в Binance (ветка carry_regime.error названа «ошибка Binance»). Писать её тело
--   означало бы выдумать половину стека. Дайджест на новой базе не заработает — это в gaps.


commit;


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- ЧЕК-ЛИСТ ПЕРЕЕЗДА: что поменять ВНЕ SQL, чтобы стек ожил на новом проекте
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- Ниже только ИМЕНА переменных и объектов. Значений здесь нет и быть не должно.
--
-- ── 0. Перед всем ─────────────────────────────────────────────────────────────────────────
--   * Новый URL проекта и новые ключи. Старые (проект ptlxojilgqylzxshsrlm) мертвы вместе с базой.
--   * Если будете накатывать выгрузку со старой базы — СНАЧАЛА выбросить мусор, который её и убил:
--       drop table if exists zz_sc_base, zz_sc_bars, _tmp_pavol_bars, _tmp_pavol_hl,
--                            _tmp_pavol_sig, lab3c_p, lab3c_bars;
--     (HANDOFF.md:16-23; 372 МБ временных таблиц от пяти параллельных агентов в базе 1415 МБ.)
--     И урок оттуда же: не давать агентам создавать таблицы в продакшене.
--
-- ── 1. Локальный бот (эта ветка, feature/testnet-risk-core) ───────────────────────────────
--   Читаются в SupabaseQueueSource.fromEnvironmentOrNull (:74-85) и в исполнителе:
--     SUPABASE_URL                  — база PostgREST нового проекта
--     SUPABASE_QUEUE_KEY            — СЕКРЕТНЫЙ ключ; при отсутствии код берёт по порядку
--     SUPABASE_KEY                  —   эти два как запасные (firstPresent, :78-80)
--     SUPABASE_SERVICE_KEY          —
--     BINANCE_TESTNET_API_KEY       — ключи тестовой сети, БЕЗ права вывода
--     BINANCE_TESTNET_API_SECRET    —
--   ⚠ Публикуемый (publishable) ключ здесь НЕ ГОДИТСЯ: RLS включён, политик нет, он увидит ноль
--     строк и это будет выглядеть как «очередь пуста», а не как «доступа нет».
--
-- ── 2. Сервис на Railway (боевой мост, ветка main) ────────────────────────────────────────
--   Читаются в SupabaseSignalBridge (:55-71):
--     SUPABASE_URL                  — тот же новый URL
--     BRIDGE_SUPABASE_KEY           — свой ключ моста; запасной — SUPABASE_KEY
--     SUPABASE_BRIDGE_ENABLED       — мост DEFAULT-OFF, без неё он no-op
--     BINANCE_USE_TESTNET           — какую сеть слушает мост (он же задаёт фильтр testnet=eq.<...>)
--     BRIDGE_ALLOW_REAL             — читается, но ничего сама не разрешает
--     BRIDGE_MAX_OPEN               — потолок одновременно занятых слотов (СТРОК, не нотионала)
--     BRIDGE_BALANCE_PER_LEG        — потолок базы сайзинга
--     BRIDGE_MAX_PENDING_AGE_MIN    — через сколько pending считается протухшей
--     TIMEZONE
--   ⚠ Мост различает формат ключа: при ключе, начинающемся на 'sb_', он НЕ шлёт заголовок
--     Authorization (main:486-487). Новый формат ключей Supabase меняет поведение — проверить.
--   ⚠ Пока стратегии нет (0 из 24 гипотез прошли форвард), включать этот сервис не за чем.
--     Он не «поднимается заодно» — его включение это отдельное решение.
--
-- ── 3. n8n ────────────────────────────────────────────────────────────────────────────────
--   * Один credential типа supabaseApi, на него завязаны ВСЕ ноды всех воркфлоу
--     (lab-watchdog uoNQxhXYHcv4GJGt, execution-cost-probe aHRcu1IFVyTVstIj,
--      crypto-digest y9gXurmsmOTWG3oU). Перевесить credential на новый проект — и этого хватит:
--     хосты в нодах берутся из него, кроме захардкоженных URL — их искать и править ОТДЕЛЬНО
--     (в ноде "Probe tick (RPC)" адрес проекта записан прямо в URL).
--   * Telegram-credential — отдельный, к переезду базы отношения не имеет.
--   * Ловушки инстанса, каждая из которых один раз молча съела правку:
--       update_workflow оставляет ЧЕРНОВИК — нужен publish, иначе правка не в работе;
--       Telegram-нода применяет Markdown даже без parse_mode (лечится HTML + экранированием);
--       замена настроек ноды сбрасывает соседние ключи;
--       error-item приходит на выход 0 или 1 в зависимости от ноды — ветвиться по item.json.error.
--   * crypto-digest и execution-cost-probe на новой базе БУДУТ ПАДАТЬ, пока не появятся RPC
--     crypto_digest и probe_tick (см. секции 8.7-8.9). Это ожидаемо, а не новая поломка.
--
-- ── 4. pg_cron — заводить ЗАНОВО, определений нет нигде ───────────────────────────────────
--   Ни одного определения джоба не сохранилось: ни в репозитории, ни в n8n, ни в выгрузках.
--   Известны только имена и расписания, и то из документации, а не из cron.job — то есть список
--   заведомо неполон, в HANDOFF.md:158-171 прямо записано «кронов больше, чем записано».
--     dip-klines-refresh    — каждые 15 мин (*/15)
--     dip-bounce-scan       — ежечасно в :47
--     lab-forward-run       — каждые 6 часов (был сломан полтора месяца, чинен 10.08)
--     live_time_closer      — расписание НЕИЗВЕСТНО; ставит bot_orders.status='close_requested'
--                             (тайм-стопы). Нигде не задокументирован, встречается только
--                             в комментариях main:SupabaseSignalBridge.java:135 и :303.
--     execution-cost-probe  — каждые 15 мин; объявлен И как pg_cron, И как воркфлоу n8n;
--                             кто из них настоящий планировщик — не проверено. Завести ОДИН,
--                             иначе зонд будет открываться вдвое чаще, чем задумано.
--     long-v2-scan          — БОЕВОЙ, ежечасно в :41. Этот скрипт его контур не поднимает
--                             и заводить этот крон не советует (см. шапку).
--   * Первое, что стоит выполнить на поднятой базе, — снять фактический список, а не верить доке:
--       select jobname, schedule, active from cron.job;
--     На новом проекте он будет пуст, и это честный старт: заводится только то, что осознанно нужно.
--   * Расширение pg_cron на новом проекте включается отдельно (Database → Extensions).
--
-- ── 5. Данных не будет ────────────────────────────────────────────────────────────────────
--   Структура без содержимого. klines_*, funding_history, universes пусты; лаборатория стартует
--   с нуля сделок, форвард — с нуля наблюдений, история зонда потеряна. Ни один из вердиктов
--   старой базы этим скриптом не воскрешается: они остались только в памяти проекта, не в SQL.
