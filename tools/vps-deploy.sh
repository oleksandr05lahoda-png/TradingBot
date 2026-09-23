#!/usr/bin/env bash
# Deploy (or redeploy) the machine on a plain Ubuntu VPS. Idempotent: run it again after
# copying a new tree over /opt/tradingbot and it rebuilds and restarts with the same data directory.
#
#   bash tools/vps-deploy.sh            test, build, back up, swap, wait until both halves are up
#   bash tools/vps-deploy.sh rollback   back to the image that traded before the last deploy
#
# Why a VPS at all: Railway's "static" outbound IPs are labelled Shared in their own UI,
# and Binance rate-limits per IP. Sharing an address with other people's bots is what put
# this bot in 9-to-17-hour 418 bans on 27, 28 and 29.08. A VPS address is one tenant's -
# and the bot itself was measured using ~6% of Binance's 2400/min budget, so on its own
# address it cannot earn a ban.
#
# 23.09: the script now refuses to ship a tree whose offline tests fail (they used to run only on
# the laptop, when someone remembered), backs the data directory up before every swap (the ledger
# and the journal are the two files a bad image could damage and nothing else could rebuild),
# waits for both halves to say they are up instead of `sleep 6`, and rolls back in one command
# with exactly the flags the deploy uses - the old rollback was a hand-typed `docker run ...`.
# Same day, after review: :prev is taken from the container that is actually trading (a crash-looping
# :latest used to overwrite the good :prev on the next deploy), the stop->run swap ignores a dropped
# SSH session, and "ready" means the container also stays up for a minute after saying so.
set -euo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
DATA_DIR="${DATA_DIR:-/opt/tradingbot-data}"
ENV_FILE="${ENV_FILE:-/opt/tradingbot.env}"
NAME="${NAME:-tradingbot}"
BACKUP_DIR="${BACKUP_DIR:-/opt/backups}"
KEEP_BACKUPS=7
MIN_FREE_KB=$((2 * 1024 * 1024))     # 2 GB: an image build plus one data backup, with room to spare
READY_WAIT_SEC=360                   # the entrypoint itself gives the bot 5 min to read the account
STABLE_SEC=60                        # after both ready lines: how long the container must stay up
ACTION="${1:-deploy}"

# `|| true`: after an SSH drop the terminal is gone and a write fails with EIO; under set -e a failed
# printf would end the script in the middle of the swap it was told to finish (see swap_begin).
say() { printf '\n== %s\n' "$*" 2>/dev/null || true; }
note() { printf '%s\n' "$*" 2>/dev/null || true; }
die() { printf '\n!! %s\n' "$*" >&2 2>/dev/null || true; exit 1; }

# The stop -> run swap must finish once started. INSTALL step 5 runs this over `ssh -t`: a Wi-Fi blip
# during the 45-second stop used to SIGHUP the script, the daemon finished the stop, and nothing ever
# ran `docker run` - the bot stayed down with positions open until a human read the watchdog alert
# (review 23.09). Children inherit the ignore, so the docker CLI mid-call is covered too. Ctrl-C comes
# back with swap_end, before the wait for readiness: that part is only watching.
swap_begin() { trap '' HUP INT TERM PIPE; }
swap_end() { trap - HUP INT TERM PIPE; }

# The ONE set of run flags. Deploy and rollback both start the container through here, so a
# rollback can never come up with a different memory cap, env file or data mount than the deploy.
# --stop-timeout 45: every stop and restart - `docker restart`, a host reboot, this script -
# grants the JVM its shutdown hook, not Docker's 10 s default (audit 03.09).
# --memory 900m: the JVM is capped at 384m in the entrypoint and the scanner needs ~60m; the
# cap keeps a leak from pushing the host into swap, and an image build beside the live bot
# cannot starve it.
run_container() {
    docker run -d --name "$NAME" \
        --restart unless-stopped \
        --stop-timeout 45 \
        --memory 900m \
        --env-file "$ENV_FILE" \
        -v "$DATA_DIR:/app/data" \
        --log-opt max-size=20m --log-opt max-file=5 \
        "$1" >/dev/null
}

# docker rm -f is SIGKILL: the JVM dies mid-order with no shutdown hook - the exact
# 22.08 incident the entrypoint was built to prevent (audit 30.08). Stop first, with
# a grace window long enough for the hook, then remove the stopped container.
stop_container() {
    docker stop -t 45 "$NAME" >/dev/null 2>&1 || true
    docker rm "$NAME" >/dev/null 2>&1 || true
}

# :prev is the image of the container that is TRADING, read off the container - not whatever :latest
# names. A deploy whose container crash-looped left :latest on an image that never traded; the next
# deploy tagged that as :prev, the good image went dangling, and `rollback` brought the broken one back
# (review 23.09). Only a container that is running and has never been restarted by Docker counts as
# proven; anything else keeps the :prev there is, and says so.
save_prev() {
    local info st rc img
    info=$(docker inspect -f '{{.State.Status}} {{.RestartCount}} {{.Image}}' "$NAME" 2>/dev/null || true)
    read -r st rc img <<<"$info" || true
    if [ "${st:-}" = running ] && [ "${rc:-}" = 0 ] && [ -n "${img:-}" ]; then
        docker tag "$img" "$NAME:prev"
        note "   $NAME:prev = образ, который торгует сейчас (${img:7:12})"
    else
        note "   !! прежний контейнер не торгует ровно (${st:-нет контейнера}, перезапусков ${rc:-?}) -
      $NAME:prev оставлен прежним: откат вернёт последний образ, который торговал"
    fi
}

free_kb() {
    mkdir -p "$BACKUP_DIR"
    df -Pk "$BACKUP_DIR" | awk 'NR==2 {print $4}'
}

# The secrets file. Never in the repository, never in an image layer.
check_env_file() {
    [ -f "$ENV_FILE" ] || die "нет файла $ENV_FILE. Создайте его (VPS_SETUP.md, шаг 4) с ключами и
    настройками стратегии и запустите снова."
    chmod 600 "$ENV_FILE"
    # Whitespace and CR stripped first: on a CRLF file `.\+` matched the bare carriage return, an
    # EMPTY key passed, and the running container was stopped for one that refused to boot.
    local key
    key=$(grep -E '^BINANCE_REAL_API_KEY=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d '[:space:]' || true)
    [ -n "$key" ] || die "BINANCE_REAL_API_KEY пуст в $ENV_FILE. Ничего не запущено."
    unset key
    # The venue gate is fail-closed in the container too, but say plainly here what this run
    # is about to become - the operator should never learn it from the exchange.
    local mode arm
    mode=$(grep -E '^REAL_MODE=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d '[:space:]' || true)
    arm=$(grep -E '^REAL_TRADING=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d '[:space:]' || true)
    say "биржа: ${arm:-не задано} / режим: ${mode:-observe}"
}

# A copy of the data directory before the container that writes it is replaced. The ledger is what
# lets a restart confirm resting stops by name, the journal is the only record of live trades, and
# flow/ holds order-book history no exchange will ever serve again. The bot keeps running while this
# is taken: GNU tar's exit 1 ("file changed as we read it" - the journal got a row) is a warning,
# anything above it is a failed backup and stops the deploy.
backup_data() {
    local stamp out rc
    stamp="$(date -u +%Y%m%d-%H%M%SZ)"
    out="$BACKUP_DIR/data-$stamp.tgz"
    say "копия данных -> $out"
    mkdir -p "$BACKUP_DIR"
    rc=0
    tar -czf "$out" -C "$(dirname "$DATA_DIR")" "$(basename "$DATA_DIR")" 2>/tmp/vps-deploy-tar.err || rc=$?
    if [ "$rc" -gt 1 ]; then
        cat /tmp/vps-deploy-tar.err >&2 || true
        rm -f "$out"
        return 1
    fi
    [ "$rc" -eq 0 ] || echo "   (tar: файл менялся во время копии - это журнал бота, копия годная)"
    echo "   $(du -h "$out" | cut -f1), хранятся последние $KEEP_BACKUPS"
    { ls -1t "$BACKUP_DIR"/data-*.tgz 2>/dev/null || true; } | tail -n +$((KEEP_BACKUPS + 1)) | xargs -r rm -f --
}

# Both halves must say they are up. The bot's ready line is the entrypoint's own marker set; the
# scanner's is its start line. Docker's log of a new container starts empty, so an old line cannot
# be mistaken for a new one. Read into a variable, not piped into grep -q: under pipefail, grep
# quitting early makes `docker logs` die of SIGPIPE and a found line reads as a failure.
BOT_READY='adopted from the exchange\|did not converge\|HALTED\|account read complete'
# ...and then the container must STAY up. `docker logs` of a restarted container still holds the
# earlier runs' lines, so a crash loop that began right after the ready lines read as "бот готов" with
# exit 0 (review 23.09). The restart count is taken when both lines are seen and must not move, nor
# the status leave running, for STABLE_SEC more.
BOT_UP=no
SCAN_UP=no
RESTARTS="?"
restarts() { docker inspect -f '{{.RestartCount}}' "$NAME" 2>/dev/null || echo "?"; }
wait_ready() {
    local hint="$1" waited=0 logs st rc0 rc
    say "жду готовности бота и сканера (до $((READY_WAIT_SEC / 60)) мин)"
    while [ "$waited" -lt "$READY_WAIT_SEC" ]; do
        st=$(docker inspect -f '{{.State.Status}}' "$NAME" 2>/dev/null || echo missing)
        if [ "$st" != "running" ]; then
            docker logs --tail 40 "$NAME" 2>&1 || true
            die "контейнер остановился при запуске (состояние: $st). Лог выше.
    $hint"
        fi
        logs=$(docker logs "$NAME" 2>&1 || true)
        if grep -aq "$BOT_READY" <<<"$logs"; then BOT_UP=yes; fi
        if grep -aq 'autoscan start:' <<<"$logs"; then SCAN_UP=yes; fi
        if [ "$BOT_UP" = yes ] && [ "$SCAN_UP" = yes ]; then break; fi
        sleep 5
        waited=$((waited + 5))
    done
    RESTARTS=$(restarts)
    [ "$BOT_UP" = yes ] && [ "$SCAN_UP" = yes ] || return 0
    say "оба на месте; ещё $STABLE_SEC с смотрю, что контейнер не падает"
    rc0="$RESTARTS"
    waited=0
    while [ "$waited" -lt "$STABLE_SEC" ]; do
        sleep 5
        waited=$((waited + 5))
        st=$(docker inspect -f '{{.State.Status}}' "$NAME" 2>/dev/null || echo missing)
        rc=$(restarts)
        if [ "$st" != "running" ] || [ "$rc" != "$rc0" ]; then
            docker logs --tail 40 "$NAME" 2>&1 || true
            die "контейнер упал уже после готовности (состояние: $st, перезапусков: $rc0 -> $rc). Лог выше.
    $hint"
        fi
    done
}

summary() {
    local logs stamp in_md5 src_md5 n match bot scan
    logs=$(docker logs "$NAME" 2>&1 || true)
    stamp=$(docker exec "$NAME" cat /app/BUILD_STAMP 2>/dev/null || echo "?")
    in_md5=$(docker exec "$NAME" md5sum /app/scanner/autoscan.py 2>/dev/null | cut -d' ' -f1 || true)
    src_md5=$(md5sum "$APP_DIR/tools/scanner/autoscan.py" 2>/dev/null | cut -d' ' -f1 || true)
    # The adoption line names the book the bot took over; the 5-minute alive line is the fallback.
    n=$(grep -ao 'ExposureBook\[n=[0-9]*' <<<"$logs" | tail -1 | cut -d= -f2 || true)
    [ -n "$n" ] || n=$(grep -ao '\[Loop\] alive: [0-9]* position' <<<"$logs" | tail -1 | grep -o '[0-9]*' || true)
    if [ -z "$in_md5" ]; then match="(не прочитан)"
    elif [ "$in_md5" = "$src_md5" ]; then match="= исходник"
    else match="НЕ равен исходнику ($src_md5)"
    fi
    if [ "$BOT_UP" = yes ]; then bot="готов"; else bot="НЕ ответил за $((READY_WAIT_SEC / 60)) мин: docker logs $NAME"; fi
    if [ "$SCAN_UP" = yes ]; then scan="запущен"; else scan="НЕ запустился: docker logs $NAME"; fi
    cat <<EOF

== итог: $1
   сборка       $stamp
   autoscan.py  ${in_md5:-?} $match
   бот          $bot
   сканер       $scan
   позиций      ${n:-?} (по логу бота)
   перезапусков $RESTARTS
   откат        bash $APP_DIR/tools/vps-deploy.sh rollback
EOF
}

next_steps() {
    cat <<'EOF'

== дальше
   docker logs -f tradingbot                   лог вживую
   docker restart -t 45 tradingbot             перезапуск (только когда ничего не выставляется)
   docker stop -t 45 tradingbot                остановить совсем (никогда не rm -f: это SIGKILL)
   bash /opt/tradingbot/tools/vps-deploy.sh rollback
                                               вернуть образ, который торговал до этого

   Перезапуск, пока Binance держит бан на этот IP, делает бан ДЛИННЕЕ: запрос при загрузке
   считается стуком во время бана. Если в логе "Exchange is refusing this IP" - не трогать,
   бот пересидит бан и продолжит сам.
EOF
}

# ---------------------------------------------------------------------------------------------
# ROLLBACK: no tests, no build, no network - it exists for the moment the new image is broken.
# :prev is retagged as :latest before the start; the next deploy takes :prev from the running
# container anyway, so the broken image cannot become :prev again. It stays inspectable as :bad.
# ---------------------------------------------------------------------------------------------
if [ "$ACTION" = "rollback" ]; then
    docker image inspect "$NAME:prev" >/dev/null 2>&1 || die "нет образа $NAME:prev - откатываться не на что."
    check_env_file
    if [ "$(free_kb)" -ge "$MIN_FREE_KB" ]; then
        backup_data || echo "   !! копия данных не удалась - откат продолжается: он важнее копии"
    else
        echo "   !! на диске меньше 2 ГБ - копия данных пропущена, откат продолжается"
    fi
    prev_id=$(docker image inspect -f '{{.Id}}' "$NAME:prev")
    latest_id=$(docker image inspect -f '{{.Id}}' "$NAME:latest" 2>/dev/null || true)
    say "откат на $NAME:prev (${prev_id:7:12})"
    swap_begin
    # A second rollback finds :latest == :prev (the first one retagged it). Tagging that as :bad would
    # stamp the GOOD image bad and strip the tag off the real bad one (review 23.09).
    if [ -n "$latest_id" ] && [ "$latest_id" != "$prev_id" ]; then
        docker tag "$NAME:latest" "$NAME:bad" >/dev/null 2>&1 || true
    else
        note "   $NAME:latest уже этот образ - $NAME:bad не трогаю"
    fi
    docker tag "$NAME:prev" "$NAME:latest"
    say "останавливаю текущий контейнер (до 45 с на штатное завершение)"
    stop_container
    say "запускаю"
    run_container "$NAME:latest"
    swap_end
    wait_ready "Откат тоже не поднялся. Второй rollback не поможет: он запустит этот же образ.
    Смотрите docker logs $NAME; образ, с которого откатывались, лежит как $NAME:bad."
    summary "откат"
    next_steps
    exit 0
fi
[ "$ACTION" = "deploy" ] || die "непонятная команда '$ACTION'. Можно: без аргументов (выкатка) или rollback."

# ---------------------------------------------------------------------------------------------
# 1. THE ADDRESS MUST BE CLEAN BEFORE ANYTHING ELSE.
# Datacenter IPv4 gets recycled: the address this server was handed may already be serving
# out someone else's Binance ban. Finding that out AFTER the exchange whitelist is changed
# costs an evening, so it is the first thing checked and the cheapest thing to fix -
# destroy the server, create another, get another address.
# ---------------------------------------------------------------------------------------------
say "спрашиваю Binance, отвечает ли он этому адресу"
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 \
       https://fapi.binance.com/fapi/v1/ping || echo 000)
echo "   fapi.binance.com/fapi/v1/ping -> HTTP $code"
case "$code" in
    200)
        echo "   чисто, с этого адреса можно торговать." ;;
    418|429)
        die "ЭТОТ АДРЕС УЖЕ ЗАБАНЕН BINANCE (HTTP $code).
    НЕ вносите его в белый список API. Удалите сервер, создайте новый - будет другой
    адрес - и запустите скрипт снова. Это минуты и копейки." ;;
    451|403)
        die "Binance не обслуживает этот регион (HTTP $code). Пересоздайте сервер в Германии или
    Финляндии; не в США и не в Великобритании." ;;
    *)
        die "Binance не ответил как надо (HTTP $code). Сначала почините сеть." ;;
esac

# ---------------------------------------------------------------------------------------------
# 2. Room on the disk: an image build and a data backup both land on it.
# ---------------------------------------------------------------------------------------------
avail=$(free_kb)
[ "$avail" -ge "$MIN_FREE_KB" ] || die "на диске свободно $((avail / 1024)) МБ, нужно не меньше 2 ГБ.
    Ничего не тронуто. Освободите место: docker image prune; старые копии в $BACKUP_DIR."

# ---------------------------------------------------------------------------------------------
# 3. Docker.
# ---------------------------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
    say "ставлю Docker"
    curl -fsSL https://get.docker.com | sh
fi

# ---------------------------------------------------------------------------------------------
# 4. The secrets file.
# ---------------------------------------------------------------------------------------------
check_env_file

# ---------------------------------------------------------------------------------------------
# 5. Data directory. The stop-id ledger and the trade journal live here; lose it and every
#    open position reconciles as unknown.
# ---------------------------------------------------------------------------------------------
mkdir -p "$DATA_DIR"

# ---------------------------------------------------------------------------------------------
# 6. The offline tests, on THIS host's python3, before anything is built. A red test stops the
#    deploy with the running bot untouched. They need no network, no keys and no /opt files.
# ---------------------------------------------------------------------------------------------
say "офлайн-тесты сканера и ops"
command -v python3 >/dev/null 2>&1 || die "на сервере нет python3 - тесты не запустить, выкатка остановлена."
tlog=$(mktemp)
ntests=0
for t in "$APP_DIR"/tools/scanner/test_*.py "$APP_DIR"/tools/ops/test_*.py; do
    [ -e "$t" ] || continue
    if python3 -B "$t" >"$tlog" 2>&1; then
        printf '   ok  %-28s %s\n' "$(basename "$t")" "$(tail -1 "$tlog")"
        ntests=$((ntests + 1))
    else
        tail -30 "$tlog" >&2
        rm -f "$tlog"
        die "тест $(basename "$t") не прошёл (вывод выше). Ничего не собрано, бот работает как работал."
    fi
done
rm -f "$tlog"
[ "$ntests" -gt 0 ] || die "в $APP_DIR/tools не найдено ни одного теста - дерево скопировано не целиком?"

# ---------------------------------------------------------------------------------------------
# 7. Build. First, so a compile error never takes the running bot down.
# ---------------------------------------------------------------------------------------------
say "собираю образ (первая сборка идёт несколько минут)"
# Stamp what is about to trade: time, commit when the tree is a checkout, -dirty when it carries
# uncommitted edits. The entrypoint logs it on every boot; before this nothing could prove which
# tree was live (audit 03.09: the running container was built from an uncommitted working copy).
stamp="$(date -u +%Y%m%d-%H%M%SZ)"
if git -C "$APP_DIR" rev-parse --short HEAD >/dev/null 2>&1; then
    stamp="$stamp $(git -C "$APP_DIR" rev-parse --short HEAD)"
    [ -z "$(git -C "$APP_DIR" status --porcelain 2>/dev/null)" ] || stamp="$stamp-dirty"
else
    stamp="$stamp no-git"
fi
echo "   штамп сборки: $stamp"
docker build --build-arg BUILD_STAMP="$stamp" -t "$NAME:new" "$APP_DIR"

# ---------------------------------------------------------------------------------------------
# 8. Backup, then swap. The image that trades now stays reachable as :prev, so a bad build is
#    one `vps-deploy.sh rollback` away from undone. It is tagged only now: a failed build or a
#    failed test must not overwrite the :prev that a rollback would need. From the :prev tag to
#    `docker run` a hangup is ignored: half a swap is a stopped bot.
# ---------------------------------------------------------------------------------------------
backup_data || die "копия $DATA_DIR не удалась (вывод tar выше). Бот не тронут, новый образ лежит как $NAME:new."
swap_begin
save_prev

say "останавливаю прежний контейнер (до 45 с на штатное завершение)"
stop_container
docker tag "$NAME:new" "$NAME:latest"

say "запускаю"
run_container "$NAME:latest"
swap_end
wait_ready "Вернуть прежнюю версию: bash $APP_DIR/tools/vps-deploy.sh rollback"
summary "выкатка"
next_steps
