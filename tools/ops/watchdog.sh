#!/usr/bin/env bash
# Host-side watchdog (audit 30.08, revised 03.09). The container runs with --restart
# unless-stopped, so Docker itself restarts a crash; the only state this script could ever
# undo with `docker start` was a DELIBERATE `docker stop` - against the operator's intent and
# against the "never restart during an exchange hold" rule. Now: report, never start. One
# Telegram line per incident, one on recovery.
# 24.09: the flag means "the owner WAS told", not "we tried". It used to be touched even when curl
# failed, and selfcheck.py now stays quiet about a stopped container while the flag stands - so a
# failed delivery must leave no flag: the next run (5 min) tries again until Telegram takes it.
# The same for recovery: the flag goes only once "снова работает" was delivered.
set -u
STATE=/opt/watchdog.alerted
ENVF=/opt/tradingbot.env
st=$(docker inspect -f "{{.State.Status}}" tradingbot 2>/dev/null || echo missing)
tg() {
  # The env file is CRLF on most lines (03.09): a trailing CR inside the URL makes curl refuse
  # it ("URL rejected") and, silenced below, the container-down alert never left the machine.
  # Every other reader strips it; so does this one now.
  tok=$(grep "^TELEGRAM_BOT_TOKEN=" "$ENVF" | tail -1 | cut -d= -f2- | tr -d '\r[:space:]')
  chat=$(grep "^TELEGRAM_CHAT_ID=" "$ENVF" | tail -1 | cut -d= -f2- | tr -d '\r[:space:]')
  # No token = nothing was said: a failure like any other (24.09; it used to count as sent).
  [ -n "$tok" ] || { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) telegram delivery failed (no TELEGRAM_BOT_TOKEN)" >>/opt/watchdog.err; return 1; }
  # The token is a URL path segment: it must not sit in argv (ps, /proc/*/cmdline) nor in the
  # curl error line cron mails to root. The URL goes in through a config read from stdin.
  # A failed delivery is recorded by exit code only - never curl's own text, which could carry
  # the URL - so a mute watchdog is at least visible in /opt/watchdog.err.
  printf 'url = "https://api.telegram.org/bot%s/sendMessage"\n' "$tok" \
    | curl -sf --max-time 15 -K - --data-urlencode "chat_id=${chat}" \
        --data-urlencode "text=$1" >/dev/null 2>&1 \
    || { rc=$?; echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) telegram delivery failed (curl exit $rc)" >>/opt/watchdog.err; return $rc; }
}
if [ "$st" = "running" ]; then
  [ -f "$STATE" ] && tg "[watchdog] контейнер снова работает" && rm -f "$STATE"
  exit 0
fi
if [ ! -f "$STATE" ]; then
  tg "[watchdog] ВНИМАНИЕ: контейнер бота в состоянии '${st}'. Сам ничего не запускаю: падения docker перезапускает сам, а ручной stop переигрывать нельзя. Если это не ваша остановка - docker logs tradingbot" \
    && touch "$STATE"
fi
exit 0
