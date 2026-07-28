-- paper_signals — журнал предрегистрированных предсказаний стенда.
-- project_state id=17 (HMAC-цепочка), id=26 (правило: утверждение неподтверждено, пока не проверено).
-- Применять оператору. Ничего в этом файле не выдумано под удобство кода: правила,
-- которые можно нарушить из Java, вынесены в констрейнты, чтобы их нельзя было нарушить вообще.

create table if not exists public.paper_signals (
    id                   bigint generated always as identity primary key,

    -- ── предрегистрация ───────────────────────────────────────────────
    hypothesis_name      text        not null,
    hypothesis_version   text        not null,   -- меняется при ЛЮБОЙ правке параметров
    mode                 text        not null,
    universe_id          text        not null,   -- список символов, зафиксированный ДО прогона

    -- ── сигнал ────────────────────────────────────────────────────────
    symbol               text        not null,
    side                 text        not null,
    signal_bar_close_ms  bigint      not null,   -- бар, ПО КОТОРОМУ принято решение
    entry_bar_open_ms    bigint      not null,   -- OPEN первого бара СТРОГО после сигнального
    entry_px             double precision not null,
    stop_px              double precision not null,
    target_px            double precision,

    -- ── исход, дописывается ПОЗЖЕ отдельным UPDATE ────────────────────
    exit_bar_ms          bigint,
    exit_px              double precision,
    exit_reason          text,
    ret_gross            double precision,
    fees                 double precision,
    funding              double precision,
    ret_net              double precision,       -- единственное, по чему считается всё дальше

    -- ── целостность ───────────────────────────────────────────────────
    is_forward           boolean     not null,
    prev_hash            text,
    row_hash             text        not null,
    recorded_at          timestamptz not null default now(),
    resolved_at          timestamptz,

    constraint paper_signals_mode_ck
        check (mode in ('backtest_dev','backtest_holdout','forward')),
    constraint paper_signals_side_ck
        check (side in ('LONG','SHORT')),

    -- Вход строго ПОСЛЕ сигнального бара. Look-ahead не ловится ревью — ловится здесь.
    constraint paper_signals_entry_after_signal_ck
        check (entry_bar_open_ms > signal_bar_close_ms),

    -- Геометрия сделки: стоп по нужную сторону от входа.
    constraint paper_signals_geom_ck
        check (
            entry_px > 0 and stop_px > 0
            and (side <> 'LONG'  or stop_px < entry_px)
            and (side <> 'SHORT' or stop_px > entry_px)
        ),

    -- is_forward=true ТОЛЬКО если запись сделана ДО бара входа. Проверяется в БД,
    -- а не в Java, потому что именно это утверждение доказывает предсказание.
    constraint paper_signals_forward_is_earned_ck
        check (
            not is_forward
            or (extract(epoch from recorded_at) * 1000)::bigint < entry_bar_open_ms
        ),

    -- Исход целиком: либо всё пусто, либо всё заполнено. ВНИМАНИЕ: этот CHECK НЕ запрещает
    -- вставить сигнал вместе с исходом — CHECK не отличает INSERT от UPDATE. Первая версия
    -- этого файла утверждала обратное; проверка вставкой показала, что утверждение ложно.
    -- Запрет вынесен в триггер ниже.
    constraint paper_signals_outcome_atomic_ck
        check (
            (resolved_at is null and exit_px is null and ret_net is null)
            or (resolved_at is not null and exit_px is not null and ret_net is not null
                and exit_bar_ms is not null and exit_reason is not null)
        ),
    constraint paper_signals_exit_reason_ck
        check (exit_reason is null or exit_reason in ('stop','target','time_stop'))
);

-- Запрет вставлять сигнал вместе с исходом. CHECK этого не может (не видит разницы между
-- INSERT и UPDATE), поэтому триггер. Именно раздельность записи и есть доказательство, что
-- предсказание существовало до факта.
create or replace function public.paper_signals_reject_outcome_on_insert()
returns trigger
language plpgsql
as $$
begin
    if new.exit_bar_ms is not null or new.exit_px is not null or new.exit_reason is not null
       or new.ret_gross is not null or new.fees is not null or new.funding is not null
       or new.ret_net is not null or new.resolved_at is not null then
        raise exception
            'paper_signals: outcome columns must be NULL on INSERT — record the prediction first, '
            'then write the outcome with a separate UPDATE (this is the evidence that the '
            'prediction preceded the fact)'
            using errcode = 'check_violation';
    end if;
    return new;
end $$;

drop trigger if exists paper_signals_no_outcome_on_insert on public.paper_signals;
create trigger paper_signals_no_outcome_on_insert
    before insert on public.paper_signals
    for each row execute function public.paper_signals_reject_outcome_on_insert();

-- ГЛАВНЫЙ КОНСТРЕЙНТ. "Holdout один раз на версию гипотезы" становится физически
-- неотменяемым: второй прогон той же версии по holdout не вставится.
-- Побочно и бесплатно ведёт знаменатель поправки на множественное тестирование —
-- число строк здесь есть число различных версий, доехавших до holdout.
create unique index if not exists paper_signals_holdout_once_per_version
    on public.paper_signals (hypothesis_name, hypothesis_version)
    where mode = 'backtest_holdout';

create index if not exists paper_signals_lookup
    on public.paper_signals (hypothesis_name, hypothesis_version, mode, symbol);
create index if not exists paper_signals_unresolved
    on public.paper_signals (mode, recorded_at) where resolved_at is null;

comment on table public.paper_signals is
    'Предрегистрированные предсказания стенда. Строка вставляется в момент ГЕНЕРАЦИИ сигнала с пустым исходом; исход дописывается позже отдельным UPDATE. Никогда не вставлять сигнал и исход одной операцией — это уничтожает доказательство, что предсказание сделано до факта.';
comment on column public.paper_signals.mode is
    'backtest_dev — разработка и тюнинг (данные до DEV_END=2025-06-30). backtest_holdout — единственный прогон на версию (2025-07-01..настоящее). forward — реальное время.';
comment on column public.paper_signals.universe_id is
    'Идентификатор списка символов, зафиксированного ДО прогона. Без него универсум можно подобрать задним числом.';
comment on column public.paper_signals.ret_net is
    'ret_gross - fees - funding. Единственная величина, по которой считается статистика.';
comment on column public.paper_signals.row_hash is
    'HMAC текущей строки, prev_hash — от предыдущей. Цепочка делает удаление или правку задним числом обнаружимыми (project_state id=17).';
