#!/usr/bin/env python3
"""Self-audit (31.08, revised 03.09). Built because an audit of the CODE missed a broken STATE: the
scanner lost its ownership map on migration and the book froze for six days. Code review
cannot see that; an invariant check can. Runs every 30 min, alerts only on a NEW failure.

03.09: two of the four invariants were wrong about the machine they watch.
  - "older than 48h" applied to every position, but the scanner's max-hold is only for TREND
    entries (autoscan.py: entered_trig != "trend" -> exempt; the 48h cap was measured with the
    near-high trend entry and never with DIP). ALGOUSDT, a dip entry, was flagged at 51h while
    being held correctly by rule. Now: trend entries only, and the cap comes from MAX_HOLD_HOURS.
  - "has a protective order" accepted ANY conditional order, so a take-profit alone would have
    passed as a stop. Now: a STOP_MARKET on the closing side, closePosition or reduceOnly."""
import hmac, hashlib, time, urllib.request, urllib.parse, urllib.error, json, io, os, subprocess, calendar, re

STATE = "/opt/selfcheck.state"
env = {}
for ln in io.open("/opt/tradingbot.env", encoding="utf-8", errors="ignore"):
    s = ln.strip()
    if "=" in s and not s.startswith("#"):
        k, v = s.split("=", 1)
        env[k.strip()] = v.strip()
# .get, not []: a key line commented out or misspelled during a rotation used to kill this whole
# script with a KeyError before the container check ran - the check that exists to catch a
# broken state was itself silent on a broken env file (06.09). The exchange invariants are
# skipped and SAID below; the container, scanner and clock checks still run.
K, S = env.get("BINANCE_REAL_API_KEY", ""), env.get("BINANCE_REAL_API_SECRET", "")

# The exchange's clock, not this host's: a signed request is refused with -1021 when the local
# clock drifts, and the bot survives that by tracking the skew (BinanceFuturesAdapter) while this
# script used raw local time. One /fapi/v1/time call per run, reused for every signature below.
# --- clock:begin
_SKEW = [0]


def _resync():
    # The skew is taken against the moment the answer ARRIVED, not the midpoint of the call.
    # urlopen's time includes DNS, TCP and TLS set-up, all BEFORE the server stamps serverTime,
    # so the midpoint sat early and a slow handshake pushed the skew - and every timestamp -
    # ahead of the exchange: 23.09 11:00 "1000ms ahead of the server's time" on a host whose
    # NTP was in sync. Against the arrival the stamp can only lag by the one-way trip, and a
    # lagging timestamp is what recvWindow is for; a leading one over 1s is refused outright.
    try:
        with urllib.request.urlopen("https://fapi.binance.com/fapi/v1/time", timeout=15) as f:
            srv = json.loads(f.read().decode())["serverTime"]
        _SKEW[0] = int(srv - time.time() * 1000)
    except Exception:
        _SKEW[0] = 0


def call(path, extra="", _retried=False):
    q = "timestamp=%d&recvWindow=10000%s" % (int(time.time() * 1000) + _SKEW[0], extra)
    sig = hmac.new(S.encode(), q.encode(), hashlib.sha256).hexdigest()
    r = urllib.request.Request("https://fapi.binance.com%s?%s&signature=%s" % (path, q, sig),
                               headers={"X-MBX-APIKEY": K})
    try:
        with urllib.request.urlopen(r, timeout=25) as f:
            return json.loads(f.read().decode())
    except urllib.error.HTTPError as e:
        # str(HTTPError) is only "HTTP Error 400: Bad Request". The reason - the code and message
        # Binance puts in the BODY - was thrown away, so the 08.09 alert could not be diagnosed
        # at all, not even a day later. The body is what makes the alert worth sending.
        try:
            body = e.read().decode()[:200]
        except Exception:
            body = "<no body>"
        # -1021 says nothing about the book, only that this request's clock was off: measure the
        # skew again and ask once more. A second -1021 is real and is reported as before.
        if '"code":-1021' in body.replace(" ", "") and not _retried:
            _resync()
            return call(path, extra, _retried=True)
        raise RuntimeError("%s %s -> HTTP %s %s" % (path, extra, e.code, body)) from None
# --- clock:end

fails = []
now = time.time()

# 1. container alive
try:
    st = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}", "tradingbot"],
                        capture_output=True, text=True, timeout=30).stdout.strip()
    if st != "running":
        fails.append("контейнер не работает (%s)" % st)
except Exception as e:
    fails.append("не удалось проверить контейнер: %s" % e)

# 2. the scanner is actually scanning (hourly + grace). Read from the scanner's own log file,
#    not docker logs: a freshly recreated container has an empty docker log and this used to
#    cry "no passes for 2.5 hours" one minute after every deploy (03.09).
try:
    last_pass = None
    for l in io.open("/opt/tradingbot-data/autoscan.log", encoding="utf-8", errors="ignore"):
        if " scan: " in l:
            try:
                last_pass = calendar.timegm(time.strptime(l[:19], "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                pass
    started = subprocess.run(["docker", "inspect", "-f", "{{.State.StartedAt}}", "tradingbot"],
                             capture_output=True, text=True, timeout=30).stdout.strip()
    try:
        import datetime
        up_for = now - datetime.datetime.fromisoformat(started[:26] + "+00:00").timestamp()
    except Exception:
        up_for = 1e9
    if (last_pass is None or now - last_pass > 9000) and up_for > 9000:
        fails.append("сканер не делал проходов больше 2.5 часов")
except Exception as e:
    fails.append("не удалось прочитать лог сканера: %s" % e)

# 3. every open position must carry a STOP on the closing side (a take-profit is not a stop)
positions, unprotected = {}, []
try:
    if not K or not S:
        raise RuntimeError("в /opt/tradingbot.env нет ключей биржи - проверки стопов пропущены")
    _resync()
    positions = {p["symbol"]: float(p["positionAmt"]) for p in call("/fapi/v2/positionRisk")
                 if abs(float(p["positionAmt"])) != 0}
    # ONE call for the account, not one per symbol: the per-symbol loop made 8-15 signed requests
    # plus 2-4s of sleeping every run, and every extra request is another chance to catch the
    # transient refusal that fired on 08.09. openAlgoOrders without a symbol returns them all.
    algo = call("/fapi/v1/openAlgoOrders")
    for s, amt in positions.items():
        want = "SELL" if amt > 0 else "BUY"
        has_stop = any(
            o.get("symbol") == s
            and (o.get("orderType") or o.get("type")) == "STOP_MARKET"
            and o.get("side") == want
            and (o.get("closePosition") or o.get("reduceOnly"))
            for o in algo)
        if not has_stop:
            unprotected.append(s)
    if unprotected:
        fails.append("ПОЗИЦИЯ БЕЗ СТОПА: %s" % ",".join(unprotected))
except Exception as e:
    fails.append("не удалось проверить стопы: %s" % e)

# 4. THE BUG THAT WAS MISSED: every bot-owned position must have a clock in the scanner, and a
#    TREND entry past MAX_HOLD_HOURS (+3h grace for one pass and the close) must be gone.
def bot_owned_rows(rows):
    """The scanner's own definition (autoscan.bot_owned): risk > 0 AND a stop id this machine
    minted (bt-*) or none yet. riskUsd alone adopted the owner's hand trade with an app stop and
    raised a standing false alarm about a position the scanner is RIGHT to leave alone (06.09)."""
    out = set()
    for r in rows or []:
        try:
            if not r.get("symbol") or float(r.get("riskUsd") or 0) <= 0:
                continue
        except (TypeError, ValueError):
            continue
        stop_id = str(r.get("stopId") or "")
        if stop_id == "" or stop_id.startswith("bt-"):
            out.add(r["symbol"])
    return out

try:
    led = json.load(io.open("/opt/tradingbot-data/book-ledger-real.json"))
    owned = bot_owned_rows(led.get("positions", []))
    stt = json.load(io.open("/opt/tradingbot-data/autoscan_state.json"))
    entered = stt.get("entered", {})
    trig = stt.get("entered_trig", {})
    orphan = sorted((owned & set(positions)) - set(entered))
    if orphan:
        fails.append("сканер не считает своими (правило 48ч не сработает): %s" % ",".join(orphan))
    try:
        max_hold = float(env.get("MAX_HOLD_HOURS", "") or 0)
    except ValueError:
        max_hold = 0.0
    if max_hold > 0:
        # 23.09: the bear arm's shorts carry the same cap (autoscan max-hold admits "trend" and "short";
        # a short has no signal exit, so this cap is the ONLY exit the scanner gives it). Dips stay exempt.
        stale = [s for s, t in entered.items()
                 if s in positions and trig.get(s, "trend") in ("trend", "short")
                 and (now - float(t)) > (max_hold + 3) * 3600]
        if stale:
            fails.append("trend/short-позиции старше %.0fч и не закрыты: %s"
                         % (max_hold, ",".join(sorted(stale))))
except Exception as e:
    fails.append("не удалось сверить состояние сканера: %s" % e)

# 5. THE SILENCE THAT WAS MISSED (03.09): the book sat at 2-3 of 15 for three days while every
#    hourly pass listed 9-18 entry-ok candidates and opened nothing. The code audit of 31.08
#    checked that positions have clocks, not that the machine was actually taking entries.
#    Rule: over the last 24 h, if at least 12 passes had room (held <= max-5) AND at least 5
#    entry-ok candidates, and NOT ONE entry was opened in that window, say so.
try:
    import re
    try:
        max_pos = int(env.get("MAX_POSITIONS", "") or 15)
    except ValueError:
        max_pos = 15
    since = now - 24 * 3600
    starved = opened = 0
    pat_idle = re.compile(r"^(\S+ \S+) scan: (\d+) held, \d+ hold-ok, (\d+) entry-ok, nothing to do")
    pat_act = re.compile(r"^(\S+ \S+) scan: (\d+) held -> closing \d+ \([^)]*\), opening (\d+)")
    for ln in io.open("/opt/tradingbot-data/autoscan.log", encoding="utf-8", errors="ignore"):
        m = pat_idle.match(ln) or pat_act.match(ln)
        if not m:
            continue
        try:
            ts = calendar.timegm(time.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            continue
        if ts < since:
            continue
        held = int(m.group(2))
        if not pat_act.match(ln) and held <= max_pos - 5 and int(m.group(3)) >= 5:
            starved += 1
    # Fills, not nominations: a line the scanner wrote and the bot refused is not an entry.
    try:
        import datetime
        for ln in io.open("/opt/tradingbot-data/trades.jsonl", encoding="utf-8", errors="ignore"):
            try:
                row = json.loads(ln)
            except ValueError:
                continue
            if row.get("kind") != "entry":
                continue
            t = row.get("ts", "").rstrip("Z")
            if "." in t:
                t = t.split(".")[0]
            ts = datetime.datetime.fromisoformat(t).replace(tzinfo=datetime.timezone.utc).timestamp()
            if ts >= since:
                opened += 1
    except OSError:
        pass
    if starved >= 18 and opened <= 1:
        fails.append("книга не заполняется: за 24ч %d проходов с местом и кандидатами, налилось входов %d "
                     "(пул/кулдауны/размер - смотри autoscan.log)" % (starved, opened))
except Exception as e:
    fails.append("не удалось проверить заполнение книги: %s" % e)

# 6. THE LOOP ITSELF (audit 11.09, M1). Checks 1-5 watch the container, the scanner and the
#    exchange - none of them the bot's own trading loop. A hung JVM is still "running" to docker,
#    the scanner keeps writing its log, and the exchange stops keep the book safe, so every check
#    above stays green while nothing closes on signal and nothing new is placed. The loop writes
#    "[Loop] alive" every 5 minutes (6 days on 14.09: 1669 beats, longest gap 5.1 min), so four
#    missed beats is a real silence, not jitter. Report only: restarting a loop parked inside an
#    exchange hold is exactly what the watchdog must never do (see /opt/watchdog.sh).
try:
    import re, datetime
    BOT_LOG = "/opt/tradingbot-data/bot_real.err.log"
    STALE_SEC = 20 * 60
    st6 = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}|{{.State.StartedAt}}", "tradingbot"],
                         capture_output=True, text=True, timeout=30).stdout.strip()
    status6, _, started6 = st6.partition("|")
    if status6 == "running":  # a stopped container is check 1's alert; do not say it twice
        try:
            up6 = now - datetime.datetime.fromisoformat(started6[:26] + "+00:00").timestamp()
        except Exception:
            up6 = 1e9
        last_alive = None
        # The tail only: the log grows ~50 KB/day and is read every 30 minutes.
        with io.open(BOT_LOG, "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - 262144))
            tail = f.read().decode("utf-8", "ignore").splitlines()
        # java.util.logging puts the time on the line BEFORE the message; the container runs UTC.
        stamp_re = re.compile(r"^([A-Z][a-z]{2} \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2} [AP]M) ")
        stamp = None
        for ln in tail:
            m = stamp_re.match(ln)
            if m:
                stamp = m.group(1)
            elif "[Loop] alive" in ln and stamp:
                try:
                    last_alive = calendar.timegm(time.strptime(stamp, "%b %d, %Y %I:%M:%S %p"))
                except ValueError:
                    pass
        # A fresh container writes a fresh log: no beat yet is not a silence for the first 20 min.
        if up6 > STALE_SEC and (last_alive is None or now - last_alive > STALE_SEC):
            if last_alive is None:
                fails.append("торговая петля бота молчит: в логе нет ни одного пульса, а контейнер работает")
            else:
                fails.append("торговая петля бота молчит %d мин (последний пульс %s UTC), контейнер при этом "
                             "работает: зависание или пауза лимита биржи - docker logs tradingbot"
                             % ((now - last_alive) // 60, time.strftime("%d.%m %H:%M", time.gmtime(last_alive))))
except FileNotFoundError:
    fails.append("нет лога бота %s - пульс торговой петли не проверить" % BOT_LOG)
except Exception as e:
    fails.append("не удалось проверить пульс торговой петли: %s" % e)

prev = ""
if os.path.exists(STATE):
    prev = io.open(STATE, encoding="utf-8").read().strip()
cur = " | ".join(fails)
# Compared WITHOUT the numbers: "20 passes" and "21 passes" are the same open problem, and the
# count changing every half hour used to make each run a "new" alert (03.09: four in three hours).
key = re.sub(r"\d+", "N", cur)

def tg(text):
    """True when Telegram took the message. A delivery failure used to raise out of the script:
    no state written, nothing printed anywhere cron could keep, and the one alert that mattered
    vanished without a trace (audit 11.09, M3). Now it is recorded, and the caller keeps the old
    state so the same alert is retried on the next run instead of being marked as sent."""
    tok, chat = env.get("TELEGRAM_BOT_TOKEN"), env.get("TELEGRAM_CHAT_ID")
    try:
        if not tok or not chat:
            raise RuntimeError("no TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID in env")
        body = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
        urllib.request.urlopen(urllib.request.Request(
            "https://api.telegram.org/bot%s/sendMessage" % tok, data=body), timeout=20)
        return True
    except Exception as e:
        # The type and HTTP code only: the request URL carries the bot token.
        why = "%s %s" % (type(e).__name__, getattr(e, "code", "") or "")
        with io.open("/opt/selfcheck.err", "a", encoding="utf-8") as f:
            f.write("%s telegram delivery failed: %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), why.strip()))
        return False

delivered = True
if cur and key != prev:
    delivered = tg("Самопроверка нашла проблему:\n\n- " + "\n- ".join(fails) +
                   "\n\nПозиции защищены биржевыми стопами, если выше не написано иначе.")
elif prev and not cur:
    delivered = tg("Самопроверка: всё вернулось в норму.")
if delivered:
    io.open(STATE, "w", encoding="utf-8").write(key)

# External pulse (23.09). Every watcher above - this script, watchdog.sh, the digest - lives ON this
# machine: when the machine, its network or cron dies, they die with it and the owner hears nothing
# (audit 11.09, M2). HEARTBEAT_URL is a push monitor on ANOTHER machine (healthchecks.io, an Uptime
# Kuma push monitor, a Better Stack heartbeat): it is pinged ONLY when every check above passed, and
# the monitor - not this host - raises the alarm when the pings stop. That one silence covers a dead
# host, dead network, dead cron, a crashing check, a dead bot and a revoked Telegram token alike.
# One transient failure (the -1021 of 22.09 17:00) skips one ping: set the monitor's grace above one
# period so it does not page. The URL carries the check's secret id - it is never written to a log.
# --- pulse:begin (tools/ops/test_selfcheck_pulse.py runs this block on its own)
pulse = "no HEARTBEAT_URL"
hb = env.get("HEARTBEAT_URL", "").strip()
if hb:
    if fails:
        pulse = "pulse withheld (%d fail(s))" % len(fails)
    else:
        try:
            urllib.request.urlopen(urllib.request.Request(hb, headers={"User-Agent": "tradingbot-selfcheck"}),
                                   timeout=15)
            pulse = "pulse sent"
        except Exception as e:
            pulse = "pulse FAILED: %s %s" % (type(e).__name__, getattr(e, "code", "") or "")
            with io.open("/opt/selfcheck.err", "a", encoding="utf-8") as f:
                f.write("%s heartbeat failed: %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                                       pulse.split(": ", 1)[1].strip()))
# --- pulse:end
print("%s checks: %d fail(s): %s%s | %s" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), len(fails),
                                             cur or "ок", "" if delivered else " [TELEGRAM NOT DELIVERED, will retry]",
                                             pulse))
