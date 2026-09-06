#!/usr/bin/env bash
# Deploy (or redeploy) the machine on a plain Ubuntu VPS. Idempotent: run it again after
# a `git pull` and it rebuilds and restarts with the same data directory.
#
#   bash tools/vps-deploy.sh
#
# Why a VPS at all: Railway's "static" outbound IPs are labelled Shared in their own UI,
# and Binance rate-limits per IP. Sharing an address with other people's bots is what put
# this bot in 9-to-17-hour 418 bans on 27, 28 and 29.08. A VPS address is one tenant's -
# and the bot itself was measured using ~6% of Binance's 2400/min budget, so on its own
# address it cannot earn a ban.
set -euo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
DATA_DIR="${DATA_DIR:-/opt/tradingbot-data}"
ENV_FILE="${ENV_FILE:-/opt/tradingbot.env}"
NAME="${NAME:-tradingbot}"

say() { printf '\n== %s\n' "$*"; }
die() { printf '\n!! %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------------------
# 1. THE ADDRESS MUST BE CLEAN BEFORE ANYTHING ELSE.
# Datacenter IPv4 gets recycled: the address this server was handed may already be serving
# out someone else's Binance ban. Finding that out AFTER the exchange whitelist is changed
# costs an evening, so it is the first thing checked and the cheapest thing to fix -
# destroy the server, create another, get another address.
# ---------------------------------------------------------------------------------------
say "asking Binance whether it answers this address"
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 \
       https://fapi.binance.com/fapi/v1/ping || echo 000)
echo "   fapi.binance.com/fapi/v1/ping -> HTTP $code"
case "$code" in
    200)
        echo "   clean. This address can trade." ;;
    418|429)
        die "THIS ADDRESS IS ALREADY BANNED BY BINANCE (HTTP $code).
    Do NOT put it in the API whitelist. Destroy this server, create a new one - you get a
    different address - and run this script again. It costs minutes and a few cents." ;;
    451|403)
        die "Binance refuses this region (HTTP $code). Rebuild the server in Germany or
    Finland; do not use a US or UK location." ;;
    *)
        die "No usable answer from Binance (HTTP $code). Fix networking before going on." ;;
esac

# ---------------------------------------------------------------------------------------
# 2. Docker.
# ---------------------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
    say "installing Docker"
    curl -fsSL https://get.docker.com | sh
fi

# ---------------------------------------------------------------------------------------
# 3. The secrets file. Never in the repository, never in an image layer.
# ---------------------------------------------------------------------------------------
[ -f "$ENV_FILE" ] || die "$ENV_FILE does not exist. Create it first - see VPS_SETUP.md
    step 4 - with the keys and the strategy settings, then run this again."
chmod 600 "$ENV_FILE"
# Whitespace and CR stripped first: on a CRLF file `.\+` matched the bare carriage return, an
# EMPTY key passed, and the running container was stopped for one that refused to boot.
key=$(grep -E '^BINANCE_REAL_API_KEY=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d '[:space:]')
[ -n "$key" ] || die "BINANCE_REAL_API_KEY is empty in $ENV_FILE. Nothing was started."
unset key

# The venue gate is fail-closed in the container too, but say plainly here what this run
# is about to become - the operator should never learn it from the exchange.
mode=$(grep -E '^REAL_MODE=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d '[:space:]')
arm=$(grep -E '^REAL_TRADING=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d '[:space:]')
say "venue: ${arm:-unset} / mode: ${mode:-observe}"

# ---------------------------------------------------------------------------------------
# 4. Data directory. The stop-id ledger and the trade journal live here; lose it and every
#    open position reconciles as unknown.
# ---------------------------------------------------------------------------------------
mkdir -p "$DATA_DIR"

# ---------------------------------------------------------------------------------------
# 5. Build, then swap. Build first so a compile error never takes the running bot down.
# ---------------------------------------------------------------------------------------
say "building the image (first build takes a few minutes)"
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
echo "   build stamp: $stamp"
# The image that trades now stays reachable as :prev, so a bad build is one `docker run` away
# from undone: docker stop -t 45 tradingbot; docker rm tradingbot; then run with tradingbot:prev.
docker tag "$NAME:latest" "$NAME:prev" >/dev/null 2>&1 || true
docker build --build-arg BUILD_STAMP="$stamp" -t "$NAME:new" "$APP_DIR"

say "stopping the previous container, if any"
# docker rm -f is SIGKILL: the JVM dies mid-order with no shutdown hook - the exact
# 22.08 incident the entrypoint was built to prevent (audit 30.08). Stop first, with
# a grace window long enough for the hook, then remove the stopped container.
docker stop -t 45 "$NAME" >/dev/null 2>&1 || true
docker rm "$NAME" >/dev/null 2>&1 || true

docker tag "$NAME:new" "$NAME:latest"

say "starting"
# --stop-timeout 45: every stop and restart - `docker restart`, a host reboot, this script -
# grants the JVM its shutdown hook, not Docker's 10 s default (audit 03.09).
# --memory 900m: the JVM is capped at 384m in the entrypoint and the scanner needs ~60m; the
# cap keeps a leak from pushing the host into swap, and an image build beside the live bot
# cannot starve it.
docker run -d --name "$NAME" \
    --restart unless-stopped \
    --stop-timeout 45 \
    --memory 900m \
    --env-file "$ENV_FILE" \
    -v "$DATA_DIR:/app/data" \
    --log-opt max-size=20m --log-opt max-file=5 \
    "$NAME:latest" >/dev/null

sleep 6
say "first lines"
docker logs --tail 25 "$NAME" || true

cat <<'EOF'

== next
   docker logs -f tradingbot                       follow it live
   docker restart -t 45 tradingbot                 restart (only when nothing is mid-order)
   docker stop -t 45 tradingbot                    stop it entirely (never rm -f: that is SIGKILL)
   docker stop -t 45 tradingbot && docker rm tradingbot && docker run ... tradingbot:prev
                                                   roll back to the image that traded before this one

   A restart while Binance is holding this IP makes the hold LONGER - the boot request
   counts as knocking during a ban. If the log says "Exchange is refusing this IP",
   leave it alone; it sleeps the hold out and resumes by itself.
EOF
