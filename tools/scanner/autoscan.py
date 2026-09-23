# -*- coding: utf-8 -*-
"""
Continuous scanner. Runs as a daemon beside the bot and keeps the book alive:
new opportunities become orders as they appear, positions that stop qualifying
get closed. No weekly list, no human in the loop.

It never talks to the exchange to trade. It appends lines to the script file the
running bot reads, so the bot still sizes every entry from its stop, caps
leverage, checks the liquidation buffer and may refuse any line. This process
proposes; the risk core disposes.

Two entry triggers, both stated up front:

  TREND  trailing return over --lookback days is positive, measured to the LIVE
         price rather than yesterday's close, so a coin crossing into momentum
         is picked up within the hour instead of the next day. Measured, long
         only, 2024-2026 holdout: hedged alpha +1.0%/y at t=0.04 for a 30d
         lookback -- the least bad of four, and not a passing grade.

  DIP    price is >= --dip-depth below its 20d high and has turned up >= 3% off
         its 5d low. This is dip_bounce_v1, the owner's own idea and the only
         hypothesis in the lab that survived day-clustered attack: +0.487%/day
         at t=1.66 raw, t=0.84 after hedging BTC beta. Wounded, not dead.

Exit rule: a position whose trigger no longer holds is closed at the next scan.
Stops and takes stay on the exchange regardless and fire without this process.
"""
import argparse
import hashlib
import hmac
import io
import json
import math
import os
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
FAPI = "https://fapi.binance.com"
DEMO = "https://demo-fapi.binance.com"

# Which exchange the RUNNING BOT executes on. Market data always comes from the live
# exchange (deeper universe, same prices); signed account calls and the tradability
# filter follow the venue. Set once in main() from --venue.
SIGNED_BASE = [DEMO]
KEY_ENV = ["BINANCE_TESTNET_API_KEY"]
SECRET_ENV = ["BINANCE_TESTNET_API_SECRET"]

CG_MARKETS = ("https://api.coingecko.com/api/v3/coins/markets"
              "?vs_currency=usd&order=market_cap_desc&per_page=250&page=%d")
# Measured live 03.09 18:40: three pages (750 cap entries) mapped to 152 tradable Binance USDT perps
# with volume over the floor, i.e. one in five, so a list of N needs ~5N cap entries: one page of
# 250 for the top-100 (as always), six pages for the top-300 a bear day draws from. The single page
# used to yield ~97 coins whatever --bear-top asked for, so the red-day pool was a quintile of the
# top-100, not of the top-300 h5.py measured (audit 03.09). Pages are two seconds apart; a page
# that fails keeps what the earlier ones gave.
CG_PAGE_SIZE = 250
CG_MAX_PAGES = 7


def cg_pages_for(top):
    if top <= 100:
        return 1
    return max(1, min(CG_MAX_PAGES, -(-int(top * 5) // CG_PAGE_SIZE)))
STABLECOINS = {"USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "PYUSD", "USDS",
               "USD1", "BUSD", "USDP", "USDD", "USDF", "RLUSD"}

ATR_PERIOD = 14
STOP_ATR_MULT = 2.0    # mirrors the bot's ATR-fallback stop; keep in sync with RiskEngine
_last = [0.0]
# How many times the venue has throttled us since the current pass began. A sweep watches this
# and gives up rather than paying the back-off once per symbol.
_throttled = [0]
# A pass may spend at most this fraction of its interval sweeping klines. Whatever is left
# unevaluated is simply not judged this hour; held coins are swept first so exits survive it.
SWEEP_BUDGET_FRACTION = 0.5
MAX_THROTTLES_PER_SWEEP = 3


_log_broken = [False]


def log(msg, path):
    line = "%s %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    # The console may not speak UTF-8 (a Windows laptop defaults to cp1251). Since Binance listed
    # perps with CJK tickers, the "skipping non-ASCII symbols" line killed the laptop scanner with a
    # UnicodeEncodeError on print (live demo test 23.09) - the backup machine would not have come up.
    # The file below is always UTF-8; only the console copy degrades.
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        enc = getattr(sys.stdout, "encoding", None) or "ascii"
        print(line.encode(enc, "replace").decode(enc, "replace"), flush=True)
    # Best effort, like the bot's own journal: a full or read-only volume must not take the
    # scanner down - and with it the whole container, so the JVM re-booted (reconcile, alerts,
    # book replay) every few minutes for as long as the disk stayed full (audit 06.09).
    try:
        with io.open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        _log_broken[0] = False
    except OSError as e:
        if not _log_broken[0]:
            print("autoscan.log unwritable (%s) - continuing on stdout only" % e, flush=True)
            _log_broken[0] = True


def get(path, params, base=FAPI, gap=0.25, tries=5):
    url = base + path + ("?" + urllib.parse.urlencode(params) if params else "")
    for attempt in range(tries):
        d = time.time() - _last[0]
        if d < gap:
            time.sleep(gap - d)
        _last[0] = time.time()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "autoscan/1.0"})
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 418):
                # Capped AND once: a 1028-minute ban header would otherwise park this
                # single-threaded loop inside one data call, exits included — and retrying five
                # times multiplies the cap back into ~25 minutes. Wait the capped moment, then let
                # the pass fail so the outer loop reaches its own sleep and the next pass (28.08).
                # The counter lets the caller abandon a sweep instead of paying this per symbol:
                # a 250-coin bear sweep against a throttling venue is 250 x 5 minutes otherwise.
                _throttled[0] += 1
                time.sleep(min(float(e.headers.get("Retry-After") or 30), 300.0) + 5)
                return None
            if e.code >= 500:
                time.sleep(3 * (attempt + 1))
                continue
            return None
        except Exception:
            time.sleep(3 * (attempt + 1))
    return None


_skew = [None]    # local clock minus exchange clock, ms


def signed_get(path, env):
    # The laptop's clock drifts seconds away from the exchange (3.8s and growing on
    # 17.08), which starves the default 5s recvWindow and fails every scan. Sign with
    # the exchange's own clock, as the bot does; resync after any failure so a
    # sleep/resume jump heals on the next call instead of poisoning every scan.
    if _skew[0] is None:
        srv = get("/fapi/v1/time", {}, base=SIGNED_BASE[0], gap=0.5)
        _skew[0] = (int(time.time() * 1000) - int(srv["serverTime"])) if srv else 0
    # Three tries, like the public get(): one transient timeout on positionRisk used to drop the
    # whole pass, exits included, and the bot's own log shows those timeouts arriving in clusters
    # (three on /fapi/v2/account inside four minutes on 01.09). A 429/418 is not retried here -
    # knocking during a hold lengthens it; the caller sleeps and the next pass tries again.
    last = None
    for attempt in range(3):
        q = "timestamp=%d&recvWindow=10000" % (int(time.time() * 1000) - _skew[0])
        sig = hmac.new(env[SECRET_ENV[0]].encode(), q.encode(), hashlib.sha256).hexdigest()
        req = urllib.request.Request("%s%s?%s&signature=%s" % (SIGNED_BASE[0], path, q, sig),
                                     headers={"X-MBX-APIKEY": env[KEY_ENV[0]]})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            _skew[0] = None
            if e.code in (429, 418) or 400 <= e.code < 500:
                raise
            last = e
        except Exception as e:
            _skew[0] = None
            last = e
        if attempt < 2:
            time.sleep(3 * (attempt + 1))
            srv = get("/fapi/v1/time", {}, base=SIGNED_BASE[0], gap=0.5)
            _skew[0] = (int(time.time() * 1000) - int(srv["serverTime"])) if srv else 0
    raise last


def read_env(repo):
    """Real environment first, local.env second. A container has no local.env - the
    secrets arrive as environment variables - and hard-reading the file there is a
    hard stop before the first scan."""
    env = {}
    path = os.path.join(repo, "local.env")
    if os.path.exists(path):
        for ln in io.open(path, encoding="utf-8", errors="ignore"):
            ln = ln.strip()
            if "=" in ln and not ln.startswith("#"):
                k, v = ln.split("=", 1)
                env[k] = v.strip()
    for k in ("BINANCE_TESTNET_API_KEY", "BINANCE_TESTNET_API_SECRET",
              "BINANCE_REAL_API_KEY", "BINANCE_REAL_API_SECRET",
              "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "REGIME_GATE", "RISK_PER_TRADE",
              "ENTRY_NEAR_HIGH", "ENTRY_VOL_MULT", "MAX_HOLD_HOURS", "MAX_CORR", "LOT_ROUND_UP",
              "BEAR_UNIVERSE", "BEAR_DAY_PCT", "ENTRY_NEAR_HIGH90", "ENTRY_VOL_MAX",
              "SHORT_ARMED", "SHORT_LOW_BARS", "SHORT_BTC_SMA_DAYS"):
        v = os.environ.get(k)
        if v:
            env[k] = v.strip()
    return env


def notify(env, text, logpath):
    """One Telegram line, plain text, never raised. The scanner had no voice at all: every
    failure burned an hour in a file nobody reads (audit 22.08). Used only on state changes -
    a streak starting, a streak ending, a regime flip - never per scan."""
    token, chat = env.get("TELEGRAM_BOT_TOKEN"), env.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    try:
        body = urllib.parse.urlencode({"chat_id": chat, "text": "[scanner] " + text}).encode()
        req = urllib.request.Request("https://api.telegram.org/bot%s/sendMessage" % token, data=body)
        urllib.request.urlopen(req, timeout=15).read()
    except Exception as e:
        # The token is a path segment of the URL, and a urllib error can carry the URL in its
        # repr. These lines go to the hosting console - a different trust boundary from this
        # process - so the token is stripped before anything is written.
        log("telegram notify failed: %s" % repr(e).replace(token, "<token>"), logpath)


def btc_regime(lookback, live):
    """BULL when BTC's trailing return over the lookback is >= 0, BEAR below. Measured 2024-26:
    the long rule earns +0.69%/day with t=1.10 in BULL and -0.46%/day in BEAR; staying out of
    BEAR was the cheapest of three machines (lab_regime_switch_measured). The scanner only
    REPORTS the regime unless REGIME_GATE=cash; the bot never sees this flag.

    Also returns TODAY's move, which is a different clock and drives a different switch:
    BEAR_UNIVERSE reacts to the day, the cash gate to the month. Measured 29.08 (regimeuni):
    swapping the entry pool on a red BTC day beat the unswitched base on the clean window
    (+67.0% vs -0.5%, t 1.42) - short of the t>=2.0 bar, so it ships as an opt-in switch."""
    m = evaluate("BTCUSDT", lookback, 0.10, live.get("BTCUSDT", 0.0))
    if not m:
        return None, None, None, None
    closes = m.get("closes") or []
    # The LAST COMPLETED day, not today's part-formed bar. h5.py measured completed daily closes,
    # and a live flag built from a partial bar is a different signal: it flips as the day fills in,
    # is pure noise minutes after midnight, and would swap the pool back and forth inside one day.
    # closes[-1] is today (partial), closes[-2] yesterday's close, closes[-3] the day before.
    ret1 = (closes[-2] / closes[-3] - 1.0) if len(closes) >= 3 and closes[-3] > 0 else None
    return ("BULL" if m["ret"] >= 0 else "BEAR"), m["ret"], ret1, closes


def bot_owned(workdir):
    """Symbols the RISK CORE opened itself, from the ledger it keeps on the same data volume.
    Ownership is the stop id's prefix: every order this machine mints starts with "bt-". riskUsd
    alone is NOT ownership - on the real venue the ledger adopts ANY position that carries a stop,
    the owner's hand trade with a stop from the app included, and assigns it a risk figure (audit
    03.09). A row with risk but no id yet is a position adopted from an entry intent - ours.
    Returns None when no ledger could be read: "unknown" is not "nobody's", and a transient read
    failure must not disown a real position (settle_bookkeeping skips its ownership prune)."""
    for name in ("book-ledger-real.json", "book-ledger.json"):
        path = os.path.join(workdir, name)
        if not os.path.exists(path):
            continue
        try:
            with io.open(path, encoding="utf-8") as f:
                doc = json.load(f)
        except Exception:
            return None
        rows = doc.get("positions", doc) if isinstance(doc, dict) else doc
        out = set()
        for r in rows or []:
            try:
                if not r.get("symbol") or float(r.get("riskUsd") or 0) <= 0:
                    continue
                stop_id = r.get("stopId") or ""
                if stop_id == "" or stop_id.startswith("bt-"):
                    out.add(r["symbol"])
            except (TypeError, ValueError):
                pass
        return out
    return None


def read_journal_feedback(workdir, since_ts):
    """What the risk core did with this scanner's lines, from the trade journal on the same volume:
    (rejected, filled, newest_ts). rejected maps symbol -> reason for every auto-* line the bot
    REFUSED since `since_ts`; filled is the set of symbols whose auto-* entry FILLED. The scanner
    never read this before, so a coin the bot could not size was re-proposed every pass and, on a
    red day, spent the narrow pool's only slot on a line that could never fill (audit 03.09)."""
    path = os.path.join(workdir, "trades.jsonl")
    rejected, filled, newest = {}, set(), since_ts
    if not os.path.exists(path):
        return rejected, filled, newest
    try:
        with io.open(path, encoding="utf-8", errors="ignore") as f:
            for ln in f:
                try:
                    row = json.loads(ln)
                except ValueError:
                    continue
                ts = _journal_ts(row.get("ts"))
                if ts is None or ts <= since_ts:
                    continue
                newest = max(newest, ts)
                sid = row.get("signalId") or ""
                if not sid.startswith("auto-"):
                    continue
                sym = row.get("symbol") or ""
                if row.get("kind") == "rejected" and sym:
                    rejected[sym] = row.get("reason") or "rejected"
                elif row.get("kind") in ("entry", "aborted") and sym:
                    # An aborted entry (filled, then unwound because the stop could not be placed
                    # or the fill slipped) WAS held and paid the round trip: without the cooldown
                    # it was re-proposed hourly while the cause persisted (audit 06.09).
                    filled.add(sym)
                    rejected.pop(sym, None)
    except OSError:
        pass
    return rejected, filled, newest


def _journal_ts(text):
    """ISO-8601 with nanoseconds -> epoch seconds; None when unparseable."""
    if not text:
        return None
    try:
        import datetime
        t = text.rstrip("Z")
        if "." in t:
            head, frac = t.split(".", 1)
            t = head + "." + (frac + "000000")[:6]
        return datetime.datetime.fromisoformat(t).replace(tzinfo=datetime.timezone.utc).timestamp()
    except (ValueError, TypeError):
        return None


def settle_bookkeeping(state, held, feedback, owned, now, interval, cooldown_hours, say=None):
    """The entry clocks, cooldowns and ownership map, settled against what the exchange and the
    journal say. Pure - no I/O - so it is pinned offline (tools/scanner/test_bookkeeping.py).

    `held` is every non-zero position on the account; `feedback` the (rejected, filled, newest)
    triple from read_journal_feedback; `owned` the ledger's bot-owned symbols, or None when no
    ledger could be read. The caller saves the state afterwards.

    A stop or take that fired on the exchange leaves no CLOSE line, so without a cooldown the
    coin was re-proposed at the very next pass while still top of the momentum list. But only a
    coin that was actually HELD earns that cooldown: a proposal the bot refused (stale, risk,
    notional) never traded, and punishing it with 24h of silence was the audit-30.08 finding.
    Never-held proposals are forgotten without a cooldown once they are too old to still fill
    (SIGNAL_MAX_AGE plus one pass).

    Audit 06.09, three ways the map lied:
      - a fill that opened and was stopped out inside one interval is never in the hourly sample;
        the journal's fill evidence used to be merged AFTER the prune, so the coin skipped the
        cooldown once and re-entered an hour after its stop. It is merged first now.
      - a symbol proposed here but opened by the OWNER (the bot refused ours, the owner traded by
        hand) stayed in `entered` and was later CLOSEd by the hold rules. Once the fill window has
        passed, a held symbol with no fill proof of ours - no auto-* fill in the journal, no bt-
        stop in the ledger - is not ours and loses its clock.
      - `was_held` only ever shrank for symbols in `entered`: hand trades and replayed history
        stayed in it forever and turned a later REFUSAL into the 24h cooldown. It is now cut down
        to what is still held or still clocked."""
    say = say or (lambda msg: None)
    rejected, filled, newest = feedback
    entered = state.setdefault("entered", {})
    cooldown = state.setdefault("cooldown", {})
    entered_trig = state.setdefault("entered_trig", {})
    if newest > float(state.get("journal_seen_ts", 0) or 0):
        state["journal_seen_ts"] = newest
    was_held = set(state.setdefault("was_held", [])) | held | filled
    # Fill proof outlives the journal window: the journal is read incrementally, so a fill seen
    # once is remembered until the clock it vouches for is gone.
    confirmed = set(state.setdefault("filled_ok", [])) | filled
    stale_after = interval + 2 * 3600
    for s in [x for x in entered if x not in held]:
        if s in was_held:
            cooldown[s] = now
        elif now - entered.get(s, 0) < stale_after:
            continue          # may still legally fill; judge it next pass
        entered.pop(s, None)
        entered_trig.pop(s, None)
        confirmed.discard(s)
    for s in [x for x in entered if x in held]:
        if now - entered.get(s, 0) <= stale_after or s in confirmed:
            continue
        if owned is None or s in owned:
            continue          # ours by the ledger, or the ledger is unreadable: keep the clock
        say("%s: held, but not ours - no auto-* fill in the journal and no bt- stop in the "
            "ledger; forgetting its clock so the hold rules leave it alone" % s)
        entered.pop(s, None)
        entered_trig.pop(s, None)
    was_held &= (set(entered) | held)
    confirmed &= set(entered)
    # The state file must not grow forever: a week-old cooldown is long expired.
    for s in [x for x, t in cooldown.items() if now - t > 7 * 86400]:
        cooldown.pop(s, None)
    # A refusal earns the coin a rest: six hours for a sizing refusal (equity does not change by
    # the hour), two for anything else.
    for sym, reason in rejected.items():
        if reason.startswith("TRADING_HALTED"):
            # The daily loss limit refuses EVERY line until 00:00 UTC - the coin did nothing to earn
            # a rest. Resting it made the first passes after midnight skip the very coins the list
            # was made of (23.09: PENGU, ASTER, POL, CC refused at 14:43, then again every hour).
            say("%s: refused while the bot is halted (%s) - no rest, the coin is not the reason"
                % (sym, reason[:80]))
            continue
        sizing =(reason.startswith("BELOW_MIN_NOTIONAL") or reason.startswith("BELOW_MIN_QUANTITY")
                  or (reason.startswith("EXCHANGE_REFUSED") and "-4164" in reason))
        rest = (6 if sizing else 2) * 3600
        cooldown[sym] = max(cooldown.get(sym, 0), now - cooldown_hours * 3600 + rest)
        say("%s: the bot refused the last line (%s) - resting %dh" % (sym, reason[:80], rest // 3600))
    state["was_held"] = sorted(was_held)
    state["filled_ok"] = sorted(confirmed)


MIN_NOTIONAL = {}    # symbol -> exchange minimum notional in USDT, refreshed with the universe
STEP_SIZE = {}       # symbol -> LOT_SIZE stepSize; the bot floors quantity to it BEFORE the notional check
MIN_QTY = {}         # symbol -> LOT_SIZE minQty


_EXOTIC_LOGGED = set()


def universe(top, min_volume, by_cap, cached_cap=None, logpath=None):
    """Returns (pool, source). Source 'cap' is the live CoinGecko list; 'cap-cached' is the last
    good one when CoinGecko refuses; 'none' when neither exists. There is deliberately no
    volume fallback for a cap universe: top-100 by volume is where the pumps live, and on
    22.08 06:52 that list plus momentum ranking nominated BTW/AKE/ACE/BOME/MUBARAK/PUMP/HEMI -
    all refused by the risk core, none of them anything the owner would hold."""
    info = get("/fapi/v1/exchangeInfo", {}, gap=0.5)
    if not info:
        # One failed exchangeInfo call used to skip the whole pass, EXITS included, under a log
        # line blaming CoinGecko (audit 03.09). The last good cap list is the right stand-in; the
        # filters cached from the last good call stay in force.
        if cached_cap:
            return [(s, 0.0) for s in cached_cap][:top], "cap-cached"
        return [], "none"
    # isascii(): Binance lists perps whose base asset is written in non-Latin script
    # (07.09: the CJK-named perp aborted a scan mid-write, silently dropping every entry
    # queued after it). Such a symbol is not tradable end to end - the operator channel
    # validates /close against [A-Z0-9]{5,20}, so a position opened in one could never be
    # flattened by hand - so it is dropped from the universe rather than handled downstream.
    tradable = {s["symbol"] for s in info["symbols"]
                if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING" and s.get("underlyingType") == "COIN"
                and s["symbol"].isascii()}
    exotic = sorted(s["symbol"] for s in info["symbols"]
                    if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"
                    and s.get("status") == "TRADING" and s.get("underlyingType") == "COIN"
                    and not s["symbol"].isascii())
    if exotic and logpath and set(exotic) - _EXOTIC_LOGGED:
        # Once per new symbol, not once per scan: the universe is rebuilt every hour.
        log("universe: skipping %d non-ASCII symbol(s) the operator channel could not close: %s"
            % (len(exotic), ",".join(exotic)), logpath)
        _EXOTIC_LOGGED.update(exotic)
    for s in info["symbols"]:
        if s["symbol"] in tradable:
            for f in s.get("filters", []):
                try:
                    if f.get("filterType") == "MIN_NOTIONAL":
                        MIN_NOTIONAL[s["symbol"]] = float(f.get("notional", 5))
                    elif f.get("filterType") == "LOT_SIZE":
                        STEP_SIZE[s["symbol"]] = float(f.get("stepSize", 0) or 0)
                        MIN_QTY[s["symbol"]] = float(f.get("minQty", 0) or 0)
                except (TypeError, ValueError):
                    pass
    # When the bot executes on the demo exchange, which lists a smaller universe than
    # the live one, a candidate absent there is refused every scan and wastes an open
    # slot. On the real venue the live list IS the tradable list, so no intersection.
    # If demo is unreachable this scan, fall back to the live list rather than stall.
    if SIGNED_BASE[0] == DEMO:
        demo_info = get("/fapi/v1/exchangeInfo", {}, base=DEMO, gap=0.5)
        if demo_info:
            tradable &= {s["symbol"] for s in demo_info["symbols"]
                         if s.get("status") == "TRADING"}
    tick = get("/fapi/v1/ticker/24hr", {}, gap=0.5) or []
    vol = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in tick}
    # With no ticker answer every candidate would fail the volume floor and the pass would be
    # skipped, exits included, under a log blaming CoinGecko. Without volumes, do not filter on it.
    floor = min_volume if vol else 0.0
    if not by_cap:
        out = [(s, vol.get(s, 0.0)) for s in tradable if vol.get(s, 0.0) >= floor]
        out.sort(key=lambda x: -x[1])
        return out[:top], "volume"
    cg = []
    for page in range(1, cg_pages_for(top) + 1):
        got = None
        for attempt in range(3):
            try:
                req = urllib.request.Request(CG_MARKETS % page, headers={"User-Agent": "autoscan/1.0"})
                with urllib.request.urlopen(req, timeout=45) as r:
                    got = json.loads(r.read().decode("utf-8"))
                break
            except Exception:
                time.sleep(5 * (attempt + 1))
        if not got:
            break                      # keep what the earlier pages gave; a shorter list beats none
        cg.extend(got)
        if page < cg_pages_for(top):
            time.sleep(2.0)            # the free tier meters requests per minute
    if not cg:
        # The free CoinGecko tier throttles datacenter IPs. The last good cap list is the
        # right stand-in: same coins, an hour or a day stale. With no cache at all there is
        # no pool - exits are still managed, nothing new is proposed.
        if cached_cap:
            out = [(s, vol.get(s, 0.0)) for s in cached_cap if s in tradable]
            return out[:top], "cap-cached"
        return [], "none"
    out, seen = [], set()
    for c in cg:
        sym = (c.get("symbol") or "").upper()
        if not sym or sym in STABLECOINS or sym in seen:
            continue
        seen.add(sym)
        for cand in (sym + "USDT", "1000" + sym + "USDT", "1000000" + sym + "USDT"):
            if cand in tradable and vol.get(cand, 0.0) >= floor:
                out.append((cand, vol.get(cand, 0.0)))
                break
        if len(out) >= top:
            break
    return out, "cap"


def atr(bars, period):
    trs = []
    for i in range(1, len(bars)):
        h, l = float(bars[i][2]), float(bars[i][3])
        pc = float(bars[i - 1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    a = sum(trs[:period]) / period
    for tr in trs[period:]:
        a = (a * (period - 1) + tr) / period
    return a


def evaluate(sym, lookback, dip_depth, live_price):
    """Return raw metrics; entry/hold decisions belong to the caller's bands."""
    need = max(lookback, 20) + ATR_PERIOD + 5
    # 61 closes feed the 60d correlation filter - same request weight (limit < 100). The
    # eligibility guard stays on the OLD minimum so young coins keep exactly the universe
    # membership they had before the filter existed.
    # 91 bars (still one request of weight 1): the no-overhang filter needs the 90d high, i.e.
    # 89 completed days plus today's partial bar - the same convention as hi20 below.
    bars = get("/fapi/v1/klines", {"symbol": sym, "interval": "1d", "limit": max(need, 91)})
    if not bars or len(bars) < need - 3:
        return None
    a = atr(bars[-(ATR_PERIOD + 2):], ATR_PERIOD)
    if not a or a <= 0:
        return None
    price = live_price if live_price > 0 else float(bars[-1][4])
    try:
        base = float(bars[-1 - lookback][4])
    except (IndexError, ValueError):
        return None
    if base <= 0 or price <= 0:
        return None
    ret = price / base - 1.0
    hi20 = max(float(b[2]) for b in bars[-20:])
    lo5 = min(float(b[3]) for b in bars[-5:])
    dip = (hi20 > 0 and lo5 > 0
           and price <= hi20 * (1 - dip_depth) and price >= lo5 * 1.03)
    # Shadow-forward inputs for the two entry filters measured 22.08 (brain_study2): distance
    # below the 20d high, and today's quote volume against the prior 20 days' mean. Reported,
    # never acted on, until the forward log earns them a place.
    # Today's bar is partial when the scanner runs mid-day; compare its volume PROJECTED to a
    # full day (volume so far / fraction of the UTC day elapsed, floored at 10%) against the
    # prior-20d mean. The history measurement used full days; without this the live filter
    # would be far stricter at 08:00 UTC than at 23:00 and pass almost nothing in the morning.
    prior = [float(b[7]) for b in bars[-21:-1]]
    elapsed = (time.time() * 1000 - float(bars[-1][0])) / 86_400_000.0
    if elapsed < 0.10:
        # Minutes into the UTC day a projection from today's trickle is noise (29x too low at
        # 00:05); yesterday's full day is the honest stand-in until ~02:24.
        projected = float(bars[-2][7]) if len(bars) >= 2 else 0.0
    else:
        projected = float(bars[-1][7]) / min(1.0, elapsed)
    vol_ratio = (projected / (sum(prior) / len(prior))) if prior and sum(prior) > 0 else None
    # No-overhang input (21.09, StrategyLab #100 + volume cap: the only change that passed the
    # closed year 2025-09..2026-09 - +6.5% at 4.0% maxDD against the live rule's +8.7% at 14.3%).
    # Distance below the 90d high; None for a coin younger than 91 bars, which the gate treats
    # as "not passing" exactly as the lab's brick does (NaN -> False).
    hi90 = max(float(b[2]) for b in bars[-90:]) if len(bars) >= 91 else 0.0
    # 20d return for the shadow "stronger than the pool median" candidate (logged, never acted on).
    try:
        base20 = float(bars[-21][4])
    except (IndexError, ValueError):
        base20 = 0.0
    return {"ret": ret, "dip": dip, "price": price, "atr": a,
            "from_high": (1 - price / hi20) if hi20 > 0 else None, "vol_ratio": vol_ratio,
            "from_high90": (1 - price / hi90) if hi90 > 0 else None,
            "ret20": (price / base20 - 1.0) if base20 > 0 else None,
            "closes": [float(b[4]) for b in bars[-61:]]}


def entry_gates(m, trig, near_high_max, vol_mult_min, near_high90_max, vol_max):
    """The entry filters, in the order they are applied. Returns (passes, cut_by) where cut_by
    names the 21.09 gate that refused the coin ('overhang' / 'volmax') or None.

    Trend entries only: a DIP buys the drawdown by definition and is never gated here. An unset
    key (None) skips its check, so with every key unset the result is always (True, None).
    A missing metric (None) never passes a gate that is armed - the lab's NaN -> False."""
    passes = True
    if trig == "trend" and near_high_max is not None:
        fh = m.get("from_high")
        passes = fh is not None and fh <= near_high_max
    if passes and trig == "trend" and vol_mult_min is not None:
        vr = m.get("vol_ratio")
        passes = vr is not None and vr >= vol_mult_min
    if passes and trig == "trend" and near_high90_max is not None:
        fh90 = m.get("from_high90")
        if not (fh90 is not None and fh90 <= near_high90_max):
            return False, "overhang"
    if passes and trig == "trend" and vol_max is not None:
        vr = m.get("vol_ratio")
        if not (vr is not None and vr <= vol_max):
            return False, "volmax"
    return passes, None


def btc_below_sma(closes, days):
    """True when BTC's latest close sits below its own `days`-day average, None when the
    series is too short to judge. `closes` is what btc_regime() already fetched, so the bear
    arm costs no extra request. Unreadable is NOT a bear signal - the caller stays out."""
    if not closes or len(closes) < days + 1:
        return None
    window = closes[-days:]
    return closes[-1] < sum(window) / float(len(window))


# The bear arm reads 4h bars - the timeframe it was MEASURED on. The daily-bar version of the
# same idea shipped first (eb0a13a) and, measured faithfully on 22.09, returned -6.7% over 4.8
# years and made the long book WORSE (+55.8% t 1.27 DD -19.0% against the long rule's own
# +66.6% t 2.03 DD -13.9%): a daily close under the 5-day low arrives a day late and sells the
# bounce. On 4h bars, with the bot's shared 1.75R take, the arm returns +28.0% and the book
# +113.9% t 2.01. The stop width is the same either way (~20% median).
SHORT_BARS_4H = 99           # 30 for the breakdown window, the rest to settle Wilder's ATR (99 = weight 1)
SHORT_STOP_ATR4H_MULT = 6.0  # the measured stop: 6 x ATR(14) on 4h bars
SHORT_LIQ_BUFFER = 0.30      # RiskConstants.MIN_LIQUIDATION_BUFFER_FRACTION: |stop-liq| >= 0.30|entry-liq|
SHORT_MMR_ASSUMED = 0.05     # worst first-bracket maintenance rate among the pool's alts; a real MMR
                             # below it only skips a short the bot would have taken - the safe side
SHORT_FRESH_MIN = 90         # act only on a 4h bar that closed at most this long ago


def evaluate_4h(sym, low_bars, now_ms=None):
    """The bear arm's own reading of one coin, decided on COMPLETED 4h bars exactly like the
    lab: a breakdown is the last completed close under the lowest low of the `low_bars` bars
    before it (lab prior_low). bars[-1] is the 4h bar still forming and is never read.

    `fresh` is False when that completed bar closed more than SHORT_FRESH_MIN ago: the lab
    fills at the next 4h open, so a breakdown first seen three hours late is not the trade
    that was measured - it is a later, different one. None on missing or short data."""
    bars = get("/fapi/v1/klines", {"symbol": sym, "interval": "4h", "limit": SHORT_BARS_4H})
    if not bars or len(bars) < low_bars + ATR_PERIOD + 3:
        return None
    done = bars[:-1]
    close = float(done[-1][4])
    ref_low = min(float(b[3]) for b in done[-(low_bars + 1):-1])
    a = atr(done, ATR_PERIOD)
    if a is None or a <= 0 or close <= 0 or ref_low <= 0:
        return None
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    age_min = (now_ms - int(done[-1][6])) / 60000.0
    return {"close4h": close, "ref_low4h": ref_low, "atr4h": a,
            "broke": close < ref_low, "fresh": age_min <= SHORT_FRESH_MIN, "age_min": age_min}


def short_gate(s4, btc_below):
    """Both halves, as measured: BTC below its 50-day average AND a fresh 4h breakdown. Five
    tighter gates and three "strength of the drop" filters were measured on 22.09 and every
    one of them cost more in the bear phase than it saved in the bull - it stays this simple."""
    return bool(btc_below and s4 and s4["broke"] and s4["fresh"])


def short_max_stop_frac(leverage):
    """The widest stop (fraction of entry) the bot will accept on a SHORT at this leverage.

    RiskEngine step 11 refuses a stop that leaves under 30% of the entry-to-liquidation distance
    (LIQUIDATION_BUFFER). For an isolated short, liq = entry x (1 + 1/lev - fee) / (1 + MMR), so the
    stop may sit at most 0.7 x that distance above entry: ~30% at 2x with MMR 5%. The 23.09 review
    caught the scanner capping at 50% only (the shared 1.75R take's limit): from a ~$285 deposit
    it would write shorts with 33-50% stops that the bot refuses, each spending a slot and a 2h
    rest. The 0.5 cap stays as the outer bound that keeps the 1.75R take above zero at 1x."""
    lev = max(1.0, float(leverage))
    liq = (1.0 + 1.0 / lev - 0.0005) / (1.0 + SHORT_MMR_ASSUMED) - 1.0
    return min(0.5, (1.0 - SHORT_LIQ_BUFFER) * liq)


def next_wait(interval, align_4h, pass_start_s, now_s, lead_s=120):
    """Seconds to sleep after a pass.

    Normally the flat interval - with the bear arm off, or BTC above its average, EXACTLY as before.
    While the arm is armed and its gate is open, the scanner does not drift against the 4h UTC
    closes: the arm was measured filling at the open of the bar after the breakdown, and a pass
    landing anywhere up to an hour later trades a later, weaker version of that rule (23.09 review:
    a 1h delay cut the measured edge by ~40% in an independent per-trade re-sim, paired t 5.28).
    So: if this pass began before the latest 4h close could be read, run again in 30 s; otherwise
    sleep no longer than the next 4h close + lead_s."""
    if not align_4h:
        return interval
    period = 4 * 3600
    last_close = now_s - (now_s % period)
    if pass_start_s < last_close + 20:          # 20 s for the exchange to roll the bar
        return 30
    return max(30, min(interval, last_close + period + lead_s - now_s))


def short_line_atr(s4):
    """The atr= written on a SHORT line. The bot places its stop at STOP_ATR_MULT x atr, so the
    scanner scales the value to land that stop at SHORT_STOP_ATR4H_MULT x ATR4h - the stop the
    arm was measured with - without touching the risk core."""
    return SHORT_STOP_ATR4H_MULT * s4["atr4h"] / STOP_ATR_MULT


def holds(side, m, exit_band):
    """Whether a HELD position stays held this pass, judged by the side it actually is.

    A long holds while the coin has not fallen through -exit_band, or while it is still a
    dip. Its mirror holds while the coin has not rallied through +exit_band. This is the
    whole point of reading the side off the exchange: `to_close = held - hold_ok`, so a
    short judged by the long branch would be closed exactly when it starts to win - the
    coin falls, the long rule says "not holding", and the scanner writes a CLOSE on a
    winning position. `side` is None for anything not currently held, which lands on the
    long branch and leaves the old behaviour untouched.

    A SHORT has NO signal exit. The bear arm was measured with three exits only - the stop,
    the take and the 48h cap - and its entry (a breakdown of the 5-day low) never reads the
    30-day return this band is built from. Judging it by that band was a bug the 22.09 review
    caught: a coin whose 30d return is still above +band enters and is immediately outside
    the hold test, so the scanner closed it at --min-hold-hours (24) regardless of profit,
    at half the measured life. Max-hold below still ends it at 48h."""
    if side == "SHORT":
        return True
    return m["ret"] > -exit_band or bool(m["dip"])


def corr60(a, b, n=60):
    """Pearson correlation of the last n aligned daily returns. 0.0 when either side is too
    short (<30 points) - fail-open, exactly how the 26.08 measurement treated missing data:
    an unmeasurable pair must not block an entry."""
    if not a or not b:
        return 0.0
    ra = [a[i] / a[i - 1] - 1 for i in range(1, len(a)) if a[i - 1] > 0]
    rb = [b[i] / b[i - 1] - 1 for i in range(1, len(b)) if b[i - 1] > 0]
    m = min(len(ra), len(rb), n)
    if m < 30:
        return 0.0
    ra, rb = ra[-m:], rb[-m:]
    ma, mb = sum(ra) / m, sum(rb) / m
    va = sum((x - ma) ** 2 for x in ra)
    vb = sum((x - mb) ** 2 for x in rb)
    if va <= 0 or vb <= 0:
        return 0.0
    cov = sum((ra[i] - ma) * (rb[i] - mb) for i in range(m))
    return cov / math.sqrt(va * vb)


def load_state(path):
    """The state file holds the entry clocks, cooldowns and the last good cap list. A corrupt one
    used to be replaced with an empty state in silence: every trend position was re-clocked from
    now (+48 h of hold), every cooldown forgotten (audit 03.09). Now the .bak written on every
    save is tried next, and the loss is said out loud on stdout for the log."""
    for candidate in (path, path + ".bak"):
        try:
            with io.open(candidate, "r", encoding="utf-8") as f:
                doc = json.load(f)
            if not isinstance(doc, dict):
                raise ValueError("state is not an object")
            if candidate != path:
                print("state file %s unreadable - recovered from %s" % (path, candidate), flush=True)
            return _seed_journal_mark(doc)
        except FileNotFoundError:
            continue
        except Exception as e:
            print("state file %s unreadable (%s)" % (candidate, e), flush=True)
    return _seed_journal_mark({"entered": {}, "cooldown": {}})


def _seed_journal_mark(doc):
    """A state without a journal mark starts reading the journal from NOW. From 0 it replayed
    every fill in the file's history as fresh "was held" evidence, and a coin from weeks ago then
    earned the 24h cooldown for a mere refusal (audit 06.09)."""
    if not doc.get("journal_seen_ts"):
        doc["journal_seen_ts"] = time.time()
    return doc


def save_state(path, state):
    tmp = path + ".tmp"
    with io.open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
        f.flush()
        try:
            os.fsync(f.fileno())      # a power loss must not leave a zero-length state file
        except OSError:
            pass
    if os.path.exists(path):
        try:
            # A copy, not a rename: between a rename and the replace below there was no state
            # file at all, and the host-side self-check reading it in that gap alerted.
            shutil.copyfile(path, path + ".bak")
        except OSError:
            pass
    os.replace(tmp, path)


# ---- the scanner view (23.09) -------------------------------------------------------------------
# What the scanner SAW, for the bot's Telegram screens. The owner could always see the book but
# never the queue behind it: "why is nothing opening?" took an offline replay on 03.09 and a
# 22-agent audit on 16.09, and both answers were already in this process's memory. One JSON file
# per pass, beside the state, read by the bot as optional (SCANNER VIEW CONTRACT, v1).
#
# Nothing below may steer a decision. It runs after the pass has decided and written its lines,
# every exception is swallowed with one log line per pass, and the only state it owns is the
# "why" key - the entry reasons of what this scanner opened, kept until the position is gone.
VIEW_FILE = "scanner_view.json"
VIEW_VERSION = 1
VIEW_QUEUE_MAX = 30
VIEW_REASONS = ("book_full", "too_wide", "cooldown", "corr", "outside_pool", "halt", "regime",
                "other")


def _iso(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _num(x):
    """A finite number as JSON, else null. Eight significant digits: enough for a 1000PEPE price
    and a 30d return alike. NaN/inf must never reach the file - the bot's JSON parser is strict."""
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    return float("%.8g" % v)


def _stop_frac(m, line_atr):
    try:
        return _num(STOP_ATR_MULT * float(line_atr) / float(m["price"]))
    except (TypeError, ValueError, KeyError, ZeroDivisionError):
        return None


def _view_note(reason, stop_frac=None):
    if reason == "book_full":
        return "нет свободного места"
    if reason == "too_wide":
        return ("стоп %.1f%%: не по депозиту" % (stop_frac * 100)) if stop_frac else "стоп не по депозиту"
    if reason == "cooldown":
        return "пауза после выхода или отказа"
    if reason == "corr":
        return "ходит вместе с открытыми"
    if reason == "outside_pool":
        return "вне пула этого прохода"
    if reason == "halt":
        return "бот на паузе"
    if reason == "regime":
        return "BTC в минусе за 30д, гейт cash"
    return "причина не определена"


def _corr_skipped(pre, kept, room):
    """Which symbols the MAX_CORR loop in main() skipped, from its input and output alone. The loop
    walks `pre` in order and stops the moment `kept` reaches the room, so everything after the last
    kept symbol was never looked at (book_full), and everything before it that is not kept was a
    correlation skip. When the room was never reached, the loop saw the whole list."""
    if kept and len(kept) >= room:
        last = pre.index(kept[-1]) if kept[-1] in pre else len(pre) - 1
        considered = pre[:last + 1]
    else:
        considered = pre
    kept_set = set(kept)
    return {s for s in considered if s not in kept_set}


def view_queue(pv):
    """Every symbol that passed an entry gate this pass and was NOT opened, with the gate that
    stopped it, in the scanner's own order: the long funnel (strongest 30d first) that gets the
    room first, then the bear arm (weakest first), then long signals outside the entry pool (never
    in the funnel at all). Held symbols are not waiting for anything and are left out.

    Each reason is read off the snapshots main() took between the funnel's steps, in the order the
    steps run; whatever cannot be placed from them is "other" rather than a guess."""
    details = pv.get("details") or {}
    held = pv.get("held") or set()
    entry_ok = pv.get("entry_ok") or set()
    entry_pool = pv.get("entry_pool")
    halted = bool(pv.get("halted"))

    def ret_of(s):
        r = (details.get(s) or {}).get("ret")
        return r if isinstance(r, (int, float)) and math.isfinite(r) else 0.0

    def row(sym, side, trig, reason, line_atr, note=None):
        m = details.get(sym) or {}
        sf = _stop_frac(m, line_atr)
        return {"symbol": sym, "side": side, "trig": trig, "reason": reason,
                "ret30": _num(m.get("ret")), "from_high20": _num(m.get("from_high")),
                "vol_ratio": _num(m.get("vol_ratio")), "stop_frac": sf,
                "note": note or _view_note(reason, sf)}

    out = []
    # --- the long funnel: pool -> cooldown -> sizing -> MAX_CORR -> room -> regime -> halt ------
    final = set(pv.get("l_final") or [])
    in_pool = entry_ok if entry_pool is None else (entry_ok & entry_pool)
    corr_skip = set()
    if pv.get("corr_ran") and "l_feas" in pv and "l_corr" in pv:
        corr_skip = _corr_skipped(pv["l_feas"], pv["l_corr"], pv.get("room") or 0)
    for s in sorted(in_pool - held - final, key=lambda s: -ret_of(s)):
        m = details.get(s) or {}
        steps = [("l_cool", "cooldown"), ("l_feas", "too_wide"), ("l_corr", None),
                 ("l_room", "book_full"), ("l_regime", "regime"), ("l_final", "halt")]
        reason = "other"
        for key, why in steps:
            if key not in pv:
                break                          # the pass ended before this step: unknown
            if s not in pv[key]:
                reason = why or ("corr" if s in corr_skip else "book_full")
                break
        out.append(row(s, "LONG", m.get("trig") or "trend", reason, m.get("atr")))
    # --- the bear arm: same pool/cooldown/sizing, then halt, the long's claim, the room -------
    short_ok = pv.get("short_ok") or set()
    base_pool = pv.get("base_pool")
    s_final = set(pv.get("s_final") or [])
    buying = set(pv.get("l_final") or [])
    s_in = short_ok if base_pool is None else (short_ok & base_pool)
    for s in sorted(s_in - held - s_final, key=ret_of):
        m = details.get(s) or {}
        note = None
        if "s_cool" not in pv:
            reason = "other"
        elif s not in pv["s_cool"]:
            reason = "cooldown"
        elif "s_feas" not in pv:
            reason = "other"
        elif s not in pv["s_feas"]:
            reason = "too_wide"
        elif "s_final" not in pv:
            reason = "other"
        elif halted:
            reason = "halt"
        elif s in buying:
            reason, note = "other", "эту монету берёт лонг"
        else:
            reason = "book_full"
        out.append(row(s, "SHORT", "short", reason, m.get("short_atr"), note))
    for s in sorted(short_ok - s_in - held, key=ret_of):
        out.append(row(s, "SHORT", "short", "outside_pool", (details.get(s) or {}).get("short_atr")))
    # --- long signals the entry pool never admitted (a red day's narrow pool) ------------------
    if entry_pool is not None:
        for s in sorted(entry_ok - entry_pool - held, key=lambda s: -ret_of(s)):
            m = details.get(s) or {}
            out.append(row(s, "LONG", m.get("trig") or "trend", "outside_pool", m.get("atr")))
    return out[:VIEW_QUEUE_MAX]


def _opened_rows(pv):
    """(symbol, side, trig, line_atr) of every line this pass WROTE, in writing order."""
    wrote = pv.get("wrote")
    if not wrote:
        return []
    _closes, longs, shorts, _reasons = wrote
    details = pv.get("details") or {}
    rows = []
    for s in longs:
        m = details.get(s) or {}
        rows.append((s, "LONG", m.get("trig") or "trend", m.get("atr")))
    for s in shorts:
        rows.append((s, "SHORT", "short", (details.get(s) or {}).get("short_atr")))
    return rows


def _close_reason(reason):
    if reason == "max-hold":
        return "max-hold"
    if isinstance(reason, str) and reason.endswith("-exited"):
        return "exit-band"
    return "other"


def note_why(state, pv):
    """Record why this pass opened what it opened, and forget the reasons of what is gone.
    Returns True when state["why"] changed (the caller saves).

    Kept exactly as long as the symbol's entry clock (`entered`): settle_bookkeeping drops the
    clock on the first pass the exchange says the position is gone, or once a line that never
    filled is too old to, or when the position turns out to be the owner's - the three ways a
    position stops being one this scanner opened. A state file from before 23.09 has no key and
    loads as it always did; the key appears with the first entry."""
    old = state.get("why")
    why = dict(old) if isinstance(old, dict) else {}
    details = pv.get("details") or {}
    now = pv.get("now") or time.time()
    for sym, side, trig, line_atr in _opened_rows(pv):
        m = details.get(sym) or {}
        why[sym] = {"opened_ts": _iso(now), "side": side, "trig": trig,
                    "price": _num(m.get("price")), "ret30": _num(m.get("ret")),
                    "from_high20": _num(m.get("from_high")),
                    "from_high90": _num(m.get("from_high90")),
                    "vol_ratio": _num(m.get("vol_ratio")), "atr": _num(line_atr),
                    "stop_frac": _stop_frac(m, line_atr)}
    keep = state.get("entered") or {}
    why = {s: v for s, v in why.items() if s in keep}
    if not why and "why" not in state:
        return False
    if why == old:
        return False
    state["why"] = why
    return True


def build_view(pv, state, now, eta):
    """The whole document, from the pass snapshot. Pure; unknown numbers are null."""
    held = pv.get("held")
    closes = pv.get("btc_closes")
    days = int(pv.get("sma_days") or 50)
    sma = (sum(closes[-days:]) / float(days)) if closes and len(closes) >= days + 1 else None
    below = btc_below_sma(closes, days) if closes else None
    price = pv.get("btc_price") or (closes[-1] if closes else None)
    wrote = pv.get("wrote")
    closed = []
    if wrote:
        closes_w, _l, _s, reasons = wrote
        closed = [{"symbol": s, "reason": _close_reason(reasons.get(s, "trend-exited"))}
                  for s in closes_w]
    why = state.get("why") if isinstance(state.get("why"), dict) else {}
    return {
        "v": VIEW_VERSION,
        "ts": _iso(now),
        "next_pass_eta": _iso(eta),
        "held": len(held) if held is not None else None,
        "room": pv.get("room"),
        "max_positions": pv.get("max_positions"),
        "entry_ok": len(pv["entry_ok"]) if "entry_ok" in pv else None,
        "hold_ok": len(pv["hold_ok"]) if "hold_ok" in pv else None,
        "halted": bool(pv.get("halted")),
        "btc": {"price": _num(price), "sma50": _num(sma),
                "short_gate_open": None if below is None else bool(below)},
        "short_armed": bool(pv.get("short_armed")),
        "queue": view_queue(pv),
        "opened": [{"symbol": s, "side": side, "trig": trig}
                   for s, side, trig, _a in _opened_rows(pv)],
        "closed": closed,
        "why": {s: dict(v) for s, v in sorted(why.items())},
    }


def write_json_atomic(path, doc):
    """The whole file or nothing: a tmp beside it, flushed, then one os.replace. The bot reads
    this file while the scanner writes it; a half-written view must never be what it parses."""
    tmp = path + ".tmp"
    try:
        with io.open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, allow_nan=False, indent=1)
            f.write("\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def publish_view(viewpath, pv, state, statepath, logpath, wait_s):
    """End of every pass, idle ones and cut-short ones included. NEVER raises: a broken view costs
    the owner a screen, never the scanner a pass. `wait_s` is a callable giving the seconds until
    the next pass (evaluated in here so that it, too, cannot throw into the loop)."""
    failed = None
    try:
        if note_why(state, pv):
            save_state(statepath, state)
    except Exception as e:
        failed = e
    try:
        now = time.time()
        write_json_atomic(viewpath, build_view(pv, state, now, now + float(wait_s())))
    except Exception as e:
        failed = failed or e
    if failed is not None:
        try:
            log("view: %s not written (%s: %.160s) - trading unaffected"
                % (VIEW_FILE, type(failed).__name__, failed), logpath)
        except Exception:
            pass


def bot_is_ready(bot_log):
    """True once the bot has adopted the account (its first agreed reconcile). Before that it
    may still be booting or sitting out an exchange-side hold, and a line written now would be
    executed at stale prices whenever it finally reads the book (22.08: a 6h IP ban at boot)."""
    try:
        with io.open(bot_log, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError:
        return False
    # "adopted" is only logged by a healthy bootstrap; a boot that halted on drift and was then
    # /resume'd, or a reconcile that came back after an outage, is just as ready.
    # "account read complete" is the unconditional marker: the bot logs it on BOTH boot paths, so a
    # boot that halted on drift no longer leaves this scanner not-ready forever - which used to
    # silence its EXITS on precisely the book that had just gone wrong (29.08 audit).
    return ("account read complete" in text or "adopted from the exchange" in text
            or "HALT CLEARED at" in text or "reconciliation is back" in text)


def bot_is_halted(bot_log):
    """Halted while the last halt line in the bot's log is newer than the last clear. The
    operator can lift a halt over Telegram (/resume) without a restart, and the scanner must
    follow that within one pass rather than stand down for the rest of the process's life."""
    try:
        with io.open(bot_log, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
    except OSError:
        return False
    return text.rfind("HALTED at") > text.rfind("HALT CLEARED at")


def halt_reason_is_observe(bot_log):
    """True when the latest halt is the REAL_MODE=observe latch: the operator asked for a
    read-only bot, so the scanner must not drive closes either. Every other halt is an
    incident latch, where exits must keep working - see the halted branch in main()."""
    try:
        with io.open(bot_log, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
    except OSError:
        return False
    i = text.rfind("HALTED at")
    return i >= 0 and "REAL_MODE=observe" in text[i:i + 400]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.path.abspath(os.path.join(ROOT, "..", "..")))
    ap.add_argument("--script", required=True, help="the script file the running bot reads")
    ap.add_argument("--interval", type=int, default=3600, help="seconds between scans")
    ap.add_argument("--top", type=int, default=100)
    ap.add_argument("--min-volume", type=float, default=5e6)
    ap.add_argument("--lookback", type=int, default=30)
    ap.add_argument("--dip-depth", type=float, default=0.10)
    ap.add_argument("--max-positions", type=int, default=20)
    ap.add_argument("--leverage", type=int, default=2)
    ap.add_argument("--by-cap", action="store_true")
    ap.add_argument("--cooldown-hours", type=float, default=24.0,
                    help="do not re-enter a symbol this soon after it was closed")
    ap.add_argument("--bot-log", required=True,
                    help="the running bot's stderr log; a HALTED latch there stands this scanner down")
    ap.add_argument("--entry-band", type=float, default=0.02,
                    help="trend entry needs trailing return above +this")
    ap.add_argument("--exit-band", type=float, default=0.02,
                    help="a held position is closed only below -this (hysteresis)")
    ap.add_argument("--min-hold-hours", type=float, default=24.0,
                    help="a freshly opened position is not closed by signal churn before this")
    ap.add_argument("--venue", choices=("demo", "real"), default="demo",
                    help="which exchange the RUNNING BOT executes on; signed calls, the key "
                         "names and the tradability filter follow it")
    ap.add_argument("--regime-gate", choices=("off", "cash"), default=None,
                    help="off: report the BTC regime and what a gate WOULD do (shadow forward); "
                         "cash: open nothing while BTC's 30d return is negative. Default from "
                         "REGIME_GATE in the environment, else off")
    ap.add_argument("--bear-top", type=int, default=300,
                    help="how deep the cap list goes on a bear day when BEAR_UNIVERSE=on; 300 is "
                         "what analysis/regimeuni/h5.py measured (up to 4 CoinGecko pages)")
    ap.add_argument("--workdir", default=None,
                    help="where this scanner's log and state live (default: analysis/forward); "
                         "a demo and a real scanner must never share state")
    args = ap.parse_args()

    if args.venue == "real":
        SIGNED_BASE[0] = FAPI
        KEY_ENV[0] = "BINANCE_REAL_API_KEY"
        SECRET_ENV[0] = "BINANCE_REAL_API_SECRET"

    workdir = args.workdir or os.path.join(args.repo, "analysis", "forward")
    logpath = os.path.join(workdir, "autoscan.log")
    statepath = os.path.join(workdir, "autoscan_state.json")
    viewpath = os.path.join(workdir, VIEW_FILE)
    env = read_env(args.repo)
    gate = args.regime_gate or env.get("REGIME_GATE", "off").lower()
    if gate not in ("off", "cash"):
        gate = "off"

    # The selector switches measured 22.08 (lab_selector_hypotheses_measured). All OFF unless the
    # operator sets them; the scanner keeps shadow-logging what they would do either way.
    #   ENTRY_NEAR_HIGH=0.05  enter only within 5% of the 20d high
    #   ENTRY_VOL_MULT=1.5    and only on a day whose quote volume is >= 1.5x the prior-20d mean
    #   MAX_HOLD_HOURS=48     close a position the scanner opened once it is this old
    def _float_env(name):
        v = env.get(name, "").strip()
        try:
            return float(v) if v else None
        except ValueError:
            return None
    near_high_max = _float_env("ENTRY_NEAR_HIGH")
    vol_mult_min = _float_env("ENTRY_VOL_MULT")
    max_hold_hours = _float_env("MAX_HOLD_HOURS")
    #   ENTRY_NEAR_HIGH90=0.10  trend entries only within 10% of the 90d high (no overhang of
    #                           holders who bought higher in the last quarter)
    #   ENTRY_VOL_MAX=4.0       and not on a blow-off day whose volume is > 4x the 20d mean
    # Both unset = the scanner behaves exactly as before. Trend entries only; DIP buys the
    # drawdown by definition and is never gated by either.
    near_high90_max = _float_env("ENTRY_NEAR_HIGH90")
    vol_max = _float_env("ENTRY_VOL_MAX")
    #   SHORT_ARMED=on          arms the bear arm: shorts the breakdown of the 5-day low
    #                           while BTC is below its 50-day average, 48h max hold.
    # DEFAULT OFF. Measured 22.09 on the cap-100 pool: alone +44.3% over 4.8 years (t 1.25),
    # beside the live long rule +141.2% at t 2.28 against the long rule's own +66.6%/2.03 -
    # but 38 bp per dollar of exposure against the long arm's 71, and its whole contribution
    # is 2021-2023. It employs the money the long rule leaves idle in a bear phase; it is not
    # a second income, and it is the owner's switch to throw, not the scanner's.
    short_armed = env.get("SHORT_ARMED", "").strip().lower() in ("on", "1", "true", "yes")
    def _int_env(name, default, lo, hi):
        """A bad value must never take the scanner down on start - nan/inf reach int() alive."""
        try:
            v = int(_float_env(name) or default)
        except (ValueError, OverflowError):
            v = default
        if not lo <= v <= hi:
            v = default
        return v
    short_low_bars = _int_env("SHORT_LOW_BARS", 30, 6, 60)   # 4h bars; 30 = 5 days, as measured
    short_sma_days = _int_env("SHORT_BTC_SMA_DAYS", 50, 10, 60)  # closes carries 61 daily closes
    # LOT_ROUND_UP=on (or a fraction): when the lot-floored size is refused by the exchange's $5
    # minimum, one step UP is allowed inside this risk tolerance - mirrors RiskEngine step 9b, so
    # the scanner and the bot agree on what is sizeable. 'on' = 0.20; unset/off = never.
    _ru = env.get("LOT_ROUND_UP", "").strip().lower()
    if _ru in ("on", "1", "true", "yes"):
        round_up_tol = 0.20
    else:
        try:
            round_up_tol = min(1.0, max(0.0, float(_ru))) if _ru else 0.0
        except ValueError:
            round_up_tol = 0.0
    # MAX_CORR=0.75: skip a candidate whose 60d correlation with an open position exceeds this.
    # Measured 26.08 on 651 days: worst day -5.4% -> -2.5%, drawdown 11% -> 7%, t 2.18 -> 2.68,
    # cost ~8pp of total return. A plateau, not a spike: 0.65 / 0.75 / 0.85 all improve t.
    max_corr = _float_env("MAX_CORR")
    # BEAR_UNIVERSE=on: on a day BTC closed red, draw NEW entries from the coins that do not
    # follow it - the lowest-correlation fifth of a deeper cap list - instead of the top-100.
    # Exits, stops, takes, leverage and the 48h cap are untouched: same rules, different pool.
    # Measured 29.08 on 2019-26 (analysis/regimeuni/h5.py): on the clean window the switch
    # earns +67.0%/y against the unswitched base's -0.5%/y, t 1.42 against a t>=2.0 bar. Below
    # the bar, so it is opt-in and the owner armed it knowing that. Off => shadow log only.
    bear_universe = env.get("BEAR_UNIVERSE", "").strip().lower() in ("on", "1", "true", "yes")
    # Named PCT and read as PERCENT: -1 means "BTC closed more than 1% down". Stored as a fraction
    # internally. A bare fraction like -0.01 would silently mean -0.01%, i.e. any red day, so the
    # range is checked loudly rather than trusted.
    bear_day_pct = _float_env("BEAR_DAY_PCT")
    if bear_day_pct is None:
        bear_day_pct = 0.0                      # any red BTC day; the measured primary
    elif not -50.0 <= bear_day_pct <= 0.0:
        log("BEAR_DAY_PCT=%s is not a percentage in [-50, 0] - using 0 (any red day)"
            % bear_day_pct, logpath)
        bear_day_pct = 0.0
    bear_day_pct = bear_day_pct / 100.0
    bear_top = max(args.top, args.bear_top)
    if not env.get(KEY_ENV[0]) or not env.get(SECRET_ENV[0]):
        raise SystemExit("missing %s/%s in local.env for --venue %s"
                         % (KEY_ENV[0], SECRET_ENV[0], args.venue))
    state = load_state(statepath)
    if short_armed and max_hold_hours is None:
        # The bear arm has no signal exit by design (its entry never reads the 30d band),
        # so MAX_HOLD_HOURS is the only exit the SCANNER can give it. Without the cap a
        # short rides to its stop or its take and nothing else - say so out loud.
        log("SHORT_ARMED is on but MAX_HOLD_HOURS is unset: shorts will be left to the "
            "exchange stop and take only, with no 48h cap. Set MAX_HOLD_HOURS=48.", logpath)
    log("autoscan start: venue=%s top=%d by_cap=%s lookback=%dd dip=%.0f%% bands=+%.0f%%/-%.0f%% "
        "interval=%ds max_pos=%d min_hold=%.0fh regime_gate=%s near_high=%s vol_mult=%s max_hold=%s "
        "max_corr=%s near_high90=%s vol_max=%s bear_universe=%s short=%s"
        % (args.venue, args.top, args.by_cap, args.lookback, args.dip_depth * 100,
           args.entry_band * 100, args.exit_band * 100, args.interval,
           args.max_positions, args.min_hold_hours, gate, near_high_max, vol_mult_min, max_hold_hours,
           max_corr, near_high90_max, vol_max,
           ("on top-%d @ %+.1f%%" % (bear_top, bear_day_pct * 100)) if bear_universe else "off",
           ("ARMED: 4h breakdown of %d bars, stop %.0fxATR4h, BTC below %dd"
            % (short_low_bars, SHORT_STOP_ATR4H_MULT, short_sma_days))
           if short_armed else "off"),
        logpath)

    bear_gate_open = False          # the bear gate as last read; drives only the post-pass sleep
    while True:
        pass_start = time.time()
        # What this pass saw, for scanner_view.json. Filled with COPIES beside the decisions and
        # never read by them; publish_view() turns it into the file at the end of the pass.
        pv = {"max_positions": args.max_positions, "short_armed": short_armed,
              "sma_days": short_sma_days}
        try:
            halted = bot_is_halted(args.bot_log)
            pv["halted"] = halted
            if halted and halt_reason_is_observe(args.bot_log):
                # REAL_MODE=observe is the operator's own choice of a read-only bot; the scanner
                # must not drive closes into it. Stand down completely, as before.
                log("BOT IS HALTED (REAL_MODE=observe) — standing down; the book is frozen "
                    "until the operator switches to trade", logpath)
                # The KIND is stored, not a boolean: observe->incident and back are different
                # facts, and one flag made the second transition silent.
                if state.get("told_halted") != "observe":
                    notify(env, "bot is in OBSERVE mode - scanner stands down; nothing is "
                                "opened or closed until REAL_MODE=trade", logpath)
                    state["told_halted"] = "observe"
                    save_state(statepath, state)
                publish_view(viewpath, pv, state, statepath, logpath, lambda: min(args.interval, 300))
                time.sleep(min(args.interval, 300))
                continue
            if halted:
                # An incident halt stops ENTRIES only. Standing down entirely deleted the one
                # automatic exit exactly when something had already gone wrong - the halt that
                # follows a missing stop used to strip the trend-exit and max-hold closes with it
                # (28.08 audit). Exits keep flowing; to_open is emptied below.
                log("bot is HALTED — entries suppressed, exits continue (closes ignore the halt)",
                    logpath)
                if state.get("told_halted") != "incident":
                    notify(env, "bot is HALTED - the scanner keeps managing exits and proposes "
                                "no entries; /resume lifts the halt", logpath)
                    state["told_halted"] = "incident"
                    save_state(statepath, state)
            elif state.get("told_halted"):
                state["told_halted"] = None
                save_state(statepath, state)
            if not bot_is_ready(args.bot_log):
                # Short sleep: the moment the bot adopts the account the next pass should feed it.
                log("bot not ready (booting, or held by the exchange) - writing nothing this pass",
                    logpath)
                publish_view(viewpath, pv, state, statepath, logpath, lambda: min(args.interval, 300))
                time.sleep(min(args.interval, 300))
                continue

            reasons = {}
            try:
                rows = signed_get("/fapi/v2/positionRisk", env)
                held = {r["symbol"] for r in rows if float(r["positionAmt"]) != 0}
                # The SIDE comes from the exchange, never from local state: a position
                # carried across a restart or a move between machines must still be judged
                # by the rule that fits it. A long judged as a short (or the reverse) would
                # be closed by `to_close = held - hold_ok` exactly when it is winning.
                held_side = {r["symbol"]: ("LONG" if float(r["positionAmt"]) > 0 else "SHORT")
                             for r in rows if float(r["positionAmt"]) != 0}
                held_since = {r["symbol"]: float(r.get("updateTime", 0)) / 1000.0
                              for r in rows if float(r["positionAmt"]) != 0 and r.get("updateTime")}
                # ADOPTION (30.08). max-hold only ever fired for symbols in `entered`, and that
                # map lives in this state file - so a move between machines (Railway -> laptop ->
                # VPS) silently exempted every carried position from the 48h rule FOREVER. The
                # book then froze: nothing aged out, every slot taken, no new entries for six
                # days. A position the risk core owns (ledger riskUsd > 0) but this scanner has
                # no clock for is adopted here, timed from now: a migration costs at most one
                # extra hold period instead of eternity. Hand trades (absent from the ledger or
                # riskUsd 0) are deliberately left alone, as everywhere else.
                _entered = state.setdefault("entered", {})
                _trig = state.setdefault("entered_trig", {})
                _orphans = sorted((held & (bot_owned(args.workdir) or set())) - set(_entered))
                if _orphans:
                    stamp = time.time()
                    for _s in _orphans:
                        _entered[_s] = stamp
                        _trig[_s] = "short" if held_side.get(_s) == "SHORT" else "trend"
                    save_state(statepath, state)
                    log("adopted %d position(s) the risk core owns but this scanner had no clock "
                        "for: %s - max-hold now applies to them from this moment"
                        % (len(_orphans), ",".join(_orphans)), logpath)
            except Exception as e:
                # str(e) matters: an IP-whitelist rejection or a revoked key looks identical to
                # a network blip by type name alone, and only the detail tells the operator
                # which of the two ate the whole scan hour.
                log("exchange unreachable (%s: %.160s); skipping this scan"
                    % (type(e).__name__, e), logpath)
                if state.get("fail_streak", 0) == 0:
                    notify(env, "exchange unreachable (%s: %.120s) - scans are being skipped "
                                "until it answers; resting stops still protect the book"
                           % (type(e).__name__, e), logpath)
                state["fail_streak"] = state.get("fail_streak", 0) + 1
                save_state(statepath, state)
                publish_view(viewpath, pv, state, statepath, logpath, lambda: min(args.interval, 300))
                # Five minutes, not the whole hour: a pass without a position read also managed no
                # EXITS, and the retry inside signed_get already absorbed the transient case.
                time.sleep(min(args.interval, 300))
                continue
            if state.get("fail_streak", 0) > 0:
                notify(env, "exchange is answering again after %d skipped scan(s)"
                       % state["fail_streak"], logpath)
                state["fail_streak"] = 0
                save_state(statepath, state)
            pv["held"] = set(held)

            # One call, sliced twice: the bull pool is the head of the same ranked list the
            # bear pool is drawn from, so a bear day costs no extra CoinGecko or exchangeInfo
            # request - only the deeper klines sweep below, and only when the switch is armed.
            wide_pool, pool_source = universe(bear_top if bear_universe else args.top,
                                              args.min_volume, args.by_cap,
                                              cached_cap=state.get("cap_pool"), logpath=logpath)
            pool = wide_pool[:args.top]
            top_syms = {sym for sym, _ in pool}
            if not pool:
                log("universe empty (no exchangeInfo/CoinGecko answer and no cached list); skipping", logpath)
                if not state.get("told_universe_empty"):
                    # Once per outage, like every other notify: this one fired every five minutes.
                    notify(env, "universe empty - no exchange/CoinGecko answer and no cached top-100 yet; "
                                "scans are skipped until one answers", logpath)
                    state["told_universe_empty"] = True
                    save_state(statepath, state)
                publish_view(viewpath, pv, state, statepath, logpath, lambda: min(args.interval, 300))
                # Five minutes, not an hour: a pass that produced no pool also produced no EXITS,
                # and one transient refusal should not cost the whole hour's exit management.
                # Matches the not-ready branch above.
                time.sleep(min(args.interval, 300))
                continue
            if state.get("told_universe_empty"):
                state["told_universe_empty"] = False
                save_state(statepath, state)
            if pool_source == "cap":
                # Cache the WIDE list: every consumer slices it (universe returns out[:top]), so the
                # bull path is unchanged, but a CoinGecko outage no longer shrinks a bear day's pool
                # to a fifth of 100 instead of a fifth of 250.
                state["cap_pool"] = [sym for sym, _ in wide_pool]
                state["cap_pool_at"] = time.time()
            elif pool_source == "cap-cached" and state.get("pool_source") != "cap-cached":
                age_h = (time.time() - state.get("cap_pool_at", time.time())) / 3600
                log("CoinGecko unavailable - using the cached top-100 (%.0fh old)" % age_h, logpath)
            state["pool_source"] = pool_source
            save_state(statepath, state)

            # What this account can actually size: risk = balance x fraction, notional = risk /
            # stop distance, and the exchange refuses anything under its minimum. Nominating
            # the infeasible burns the room - seven refused lines at 06:54 while feasible coins
            # further down the ranking went unproposed. Best effort: a failed balance read
            # simply turns the filter off and the risk core refuses as before.
            risk_usd = None
            try:
                # The same figure the risk core sizes from: wallet PLUS unrealised (equityUsd). The
                # wallet alone disagreed with the bot by the open PnL, one more way the two sizings
                # drifted apart on the $5 boundary (audit 03.09).
                acct = signed_get("/fapi/v2/account", env)
                equity = float(acct.get("totalWalletBalance") or 0) + float(acct.get("totalUnrealizedProfit") or 0)
                if equity > 0:
                    # The bot clamps this to its 1% hard cap (RiskConstants); sizing here from the
                    # raw figure would nominate coins the bot then refuses BELOW_MIN_NOTIONAL.
                    frac = float(env.get("RISK_PER_TRADE", "0.005"))
                    if frac > 0.01:
                        log("RISK_PER_TRADE=%s exceeds the bot's 1%% cap - sizing at the cap" % frac,
                            logpath)
                    risk_usd = equity * min(0.01, max(0.0, frac))
            except Exception as e:
                log("account unreadable (%s) - feasibility filter off this pass" % type(e).__name__, logpath)
            tick = get("/fapi/v1/ticker/price", {}, gap=0.5) or []
            live = {t["symbol"]: float(t["price"]) for t in tick}
            # Every entry this pass writes is priced from THIS snapshot, so this is the moment the
            # bot's staleness gate must measure against. Stamping at file-append time instead would
            # call a line fresh after a pass that spent 25 minutes inside a throttled kline call.
            snapshot_at = int(time.time())

            # The regime is reported every scan and announced on a flip. Under the default
            # gate ("off") it changes nothing - the log records what a cash gate WOULD have
            # suppressed, which is the paper forward of that gate, for free.
            regime, btc_ret, btc_ret1, btc_closes = btc_regime(args.lookback, live)
            pv["btc_closes"], pv["btc_price"] = btc_closes, live.get("BTCUSDT")
            # A red BTC day is a different fact from a bear month and drives a different
            # switch. Unreadable => not a bear day: the pool stays where it is rather than
            # swinging on a failed request.
            bear_day = bear_universe and btc_ret1 is not None and btc_ret1 < bear_day_pct
            if regime is None and state.get("regime"):
                # One failed klines call must not switch a cash gate off for an hour.
                regime = state["regime"]
                log("BTC regime unreadable this pass - using last known %s" % regime, logpath)
            if regime and regime != state.get("regime"):
                msg = ("BTC regime is now %s (30d %+.1f%%)%s" % (regime, (btc_ret or 0) * 100,
                       "" if state.get("regime") is None else " - was " + state["regime"]))
                if regime == "BEAR":
                    msg += (". Gate %s: %s" % (gate, "no new entries while it lasts"
                            if gate == "cash" else "observing only, entries continue"))
                log(msg, logpath)
                notify(env, msg, logpath)
                state["regime"] = regime
                save_state(statepath, state)

            entry_ok, hold_ok, details = set(), set(), {}
            short_ok = set()                    # bear-arm candidates this pass
            cut_overhang, cut_volmax = [], []   # what the 21.09 gates removed this pass
            # One reading of the bear regime per pass, from the closes btc_regime() already
            # fetched. None (series too short / unreadable) keeps the bear arm OUT.
            btc_below = btc_below_sma(btc_closes, short_sma_days) if short_armed else False
            bear_gate_open = bool(btc_below)
            if short_armed and btc_below is None:
                log("bear arm armed but BTC history is too short to judge the %dd average"
                    " - no shorts this pass" % short_sma_days, logpath)
            # A held coin that drops out of the top list must still be judged by the rule,
            # never by list membership: to_close = held - hold_ok, so leaving it unevaluated
            # would close it for falling off CoinGecko's page.
            # On a bear day the deeper list is swept too, because the entry pool is chosen from
            # it below. Held coins are always evaluated whichever pool is active - the switch
            # must never be able to close a position by changing what the scanner looks at.
            scan_pool = wide_pool if bear_day else pool
            # HELD FIRST. A sweep can be cut short by a throttling venue or by its own time
            # budget, and what must survive that is the exit decision - a position judged is a
            # position that can be closed. Candidates for new entries come after; missing some
            # costs an hour of opportunity, missing an exit costs money.
            to_evaluate = (sorted(held)
                           + [sym for sym, _ in scan_pool if sym not in held])
            _throttled[0] = 0
            sweep_started = time.time()
            sweep_budget = args.interval * SWEEP_BUDGET_FRACTION
            swept = 0
            for sym in to_evaluate:
                # Two ways to abandon the rest of the sweep, both of which leave the held coins
                # already judged (they are swept first) so exits are still correct this pass.
                if _throttled[0] >= MAX_THROTTLES_PER_SWEEP or \
                        time.time() - sweep_started > sweep_budget:
                    why = ("the venue throttled us %d times" % _throttled[0]
                           if _throttled[0] >= MAX_THROTTLES_PER_SWEEP
                           else "the %.0f min sweep budget ran out" % (sweep_budget / 60))
                    log("sweep stopped after %d of %d coins: %s - exits are judged, entries "
                        "wait for the next pass" % (swept, len(to_evaluate), why), logpath)
                    if not state.get("told_sweep_cut"):
                        notify(env, "scan cut short (%s); positions are still managed, no new "
                                    "entries this pass" % why, logpath)
                        state["told_sweep_cut"] = True
                        save_state(statepath, state)
                    break
                swept += 1
                m = evaluate(sym, args.lookback, args.dip_depth, live.get(sym, 0.0))
                if not m:
                    # No data is not a signal. to_close = held - hold_ok, so a held coin whose
                    # klines request failed this pass would otherwise be closed for a network
                    # blip; keep it and say so.
                    if sym in held:
                        hold_ok.add(sym)
                        log("%s: no data this pass - holding, not judging" % sym, logpath)
                    continue
                details[sym] = m
                # Hysteresis: enter above +band, hold anything above -band. A coin wobbling
                # around zero neither enters nor exits — churn is pure cost, measured at 10bp
                # a round trip.
                # The bot puts its stop at entry - ATR x STOP_ATR_MULT; when that lands at
                # or below zero it auto-rejects, so nominating the coin only burns a slot
                # (BEAT: atr 0.95 on price 0.44). Gate ONLY the entry side - a held coin
                # must stay hold-eligible, this filter must never force a close.
                # One trigger per coin, decided here and used everywhere: the entry filters, the
                # id label on the book line and the max-hold rule must agree on what this entry IS.
                m["trig"] = "trend" if m["ret"] > args.entry_band else ("dip" if m["dip"] else None)
                if m["trig"]:
                    if m["price"] > STOP_ATR_MULT * m["atr"]:
                        passes, cut_by = entry_gates(m, m["trig"], near_high_max, vol_mult_min,
                                                     near_high90_max, vol_max)
                        # The two 21.09 gates are counted when they bite. This is the SIGNAL
                        # stage: a cut coin might still have been refused later by the pool,
                        # cooldown, sizing or room - the list says what the gate saw, not what
                        # the old rule would have bought.
                        if cut_by == "overhang":
                            cut_overhang.append(sym)
                        elif cut_by == "volmax":
                            cut_volmax.append(sym)
                        if passes:
                            entry_ok.add(sym)
                # 4h klines are fetched ONLY while the bear gate is open: in a bull market the
                # arm costs not one extra request.
                # Only coins of the top-100 itself: the arm draws from base_pool, so on a red day
                # with BEAR_UNIVERSE=on the ~200 deeper coins would be fetched and thrown away.
                if (short_armed and btc_below and sym != "BTCUSDT" and sym not in held
                        and sym in top_syms):
                    s4 = evaluate_4h(sym, short_low_bars)
                    if short_gate(s4, btc_below):
                        line_atr = short_line_atr(s4)
                        if (STOP_ATR_MULT * line_atr
                                <= short_max_stop_frac(args.leverage) * m["price"]):
                            m["short_atr"] = line_atr
                            short_ok.add(sym)
                # Judged by the side it actually IS. The long rule holds while the coin has
                # not fallen; its mirror holds while the coin has not rallied. Running a
                # short through the long branch would close it the moment it started to win.
                if holds(held_side.get(sym), m, args.exit_band):
                    hold_ok.add(sym)

            # A cut-short sweep must never close a position it simply did not get to. to_close is
            # held minus hold_ok, so anything held and unjudged is held, exactly as a failed
            # klines request is treated one branch above.
            unjudged = held - hold_ok - {s for s in details}
            for sym in unjudged:
                hold_ok.add(sym)
            if unjudged:
                log("%d held coin(s) not reached this pass - holding, not judging: %s"
                    % (len(unjudged), ",".join(sorted(unjudged))), logpath)
            pv["entry_ok"], pv["hold_ok"], pv["short_ok"] = set(entry_ok), set(hold_ok), set(short_ok)
            pv["details"] = details

            if swept >= len(to_evaluate) and state.get("told_sweep_cut"):
                state["told_sweep_cut"] = False
                save_state(statepath, state)

            # Which pool may supply a NEW entry this pass. Normally the top-100. On a red BTC
            # day with BEAR_UNIVERSE=on, the fifth of the deeper list least correlated with BTC
            # - the coins that are not simply BTC with a multiplier. Exits never consult this.
            # No shadow line when the switch is off: the deeper list is not fetched then, so
            # any "would have" would be computed from the top-100 and would be a lie.
            entry_pool = {sym for sym, _ in pool}
            # Kept before BEAR_UNIVERSE may swap entry_pool below: that swap was measured for
            # the long arm only, and the bear arm was measured on the cap-100 pool itself.
            base_pool = set(entry_pool)
            if bear_day and not btc_closes:
                # No BTC reference means no correlation. corr60 fails OPEN at 0.0, which in a
                # ranking sorts an unmeasurable coin FIRST - so a single failed BTC read would
                # have handed the whole book to whatever sorts first alphabetically, the 1000*
                # meme tickers. Refuse the swap instead, exactly as an unreadable bar is refused
                # as a bear day above.
                log("bear day, but BTC history is unreadable - entry pool stays the top-%d"
                    % args.top, logpath)
            elif bear_day:
                # Score on the correlation only, and drop the ones corr60 could not measure:
                # a tie must never fall through to alphabetical order, and "unmeasurable" is not
                # "uncorrelated". Sorting (score, symbol) tuples is what let that happen.
                scored = []
                for sym, _ in wide_pool:
                    m = details.get(sym)
                    if not m or sym == "BTCUSDT":
                        continue
                    series = m.get("closes")
                    if not series or len(series) < 31:
                        continue
                    c = corr60(series, btc_closes)
                    if c == 0.0:
                        continue          # corr60's fail-open value; unmeasurable, not independent
                    scored.append((c, sym))
                scored.sort(key=lambda t: t[0])
                keep = max(1, len(scored) // 5)
                if scored:
                    entry_pool = {sym for _, sym in scored[:keep]}
                    log("bear day (BTC %+.2f%% yesterday): entries from the %d least "
                        "BTC-correlated of %d measurable coins (corr %.2f..%.2f), not the top-%d"
                        % ((btc_ret1 or 0) * 100, keep, len(scored), scored[0][0],
                           scored[keep - 1][0], args.top), logpath)
                else:
                    log("bear day, but no coin had a measurable correlation - entry pool unchanged",
                        logpath)
            pv["entry_pool"], pv["base_pool"] = set(entry_pool), set(base_pool)

            now = time.time()
            pv["now"] = now
            # What the risk core did with the last lines, from the journal on the same volume,
            # read BEFORE the clocks are settled: a fill is proof the coin WAS held, which the
            # hourly sample above misses when a position opens and closes inside one interval.
            # The rules themselves live in settle_bookkeeping, where they are pinned offline.
            feedback = read_journal_feedback(args.workdir, float(state.get("journal_seen_ts", 0) or 0))
            settle_bookkeeping(state, held, feedback, bot_owned(args.workdir), now, args.interval,
                               args.cooldown_hours, lambda msg: log(msg, logpath))
            save_state(statepath, state)
            entered = state["entered"]
            cooldown = state["cooldown"]
            entered_trig = state["entered_trig"]

            to_close = []
            for s in sorted(held - hold_ok):
                # Only what this scanner opened (or adopted from the ledger above). `held` is every
                # position on the account, and a hand-opened long whose 30d return dipped below the
                # band used to be written as CLOSE and flattened by the bot (audit 03.09). The
                # owner's positions are not this process's to exit, ever.
                if s not in entered:
                    continue
                if now - entered.get(s, 0) < args.min_hold_hours * 3600:
                    continue           # the exchange-side stop still guards it meanwhile
                # Named after the trigger that stopped holding: a dip entry leaving is not a
                # trend exit, and the lab's exit-mix arithmetic counts these labels.
                reasons.setdefault(s, entered_trig.get(s, "trend") + "-exited")
                to_close.append(s)
            if max_hold_hours is not None:
                # Measured 22.08: a near-high entry held at most ~2 days kept 49% winners and cut
                # the drawdown in half; the same cap without the near-high entry was worse than
                # the base rule, so the switch is meant to travel with ENTRY_NEAR_HIGH. Only the
                # scanner's own trend entries: an adopted or manual position has no open time the
                # scanner can trust (the exchange's updateTime moves on every partial fill).
                for s in sorted(held):
                    if s in to_close or s not in entered:
                        continue
                    # "short" belongs here: the bear arm was measured with a 48h cap as part
                    # of the rule, not as an afterthought. Dip entries are still exempt.
                    if entered_trig.get(s, "trend") not in ("trend", "short"):
                        continue
                    if now - entered[s] >= max_hold_hours * 3600:
                        to_close.append(s)
                        reasons[s] = "max-hold"
            room = max(0, args.max_positions - (len(held) - len(to_close)))
            # Strongest trend first. The old sorted() here took the first N ALPHABETICALLY,
            # which filled every book with 1000*/A* coins - the highest-beta names by
            # accident. Measured 2024-26 as a 10-slot portfolio: alphabet -3.0% (worse than
            # random +5%), momentum-first +21% with a smaller drawdown, better in 5 of 5
            # half-years. This removes a handicap; it does not create an edge.
            fresh = [s for s in sorted((entry_ok & entry_pool) - held,
                                       key=lambda s: -details[s]["ret"])
                     if now - cooldown.get(s, 0) > args.cooldown_hours * 3600]
            # The bear arm goes through the SAME funnel: same pool, same held and cooldown
            # rules, same sizing test below. Weakest 30d return first - the mirror of
            # "strongest trend first", so the slot budget goes to the deepest breakdown.
            fresh_short = [s for s in sorted((short_ok & base_pool) - held,
                                             key=lambda s: details[s]["ret"])
                           if now - cooldown.get(s, 0) > args.cooldown_hours * 3600]
            pv["room"], pv["l_cool"], pv["s_cool"] = room, list(fresh), list(fresh_short)
            # The funnel, every pass, so "nothing to do" is never a mystery again. On 03.09 it
            # took an offline replay to learn that 32 entry-ok coins met a 34-coin bear pool in
            # 5 names, all of them unsizeable, while every real momentum name sat outside the
            # pool at BTC-correlation 0.4-0.8 (audit 03.09).
            _in_pool = entry_ok & entry_pool
            _outside = sorted(entry_ok - entry_pool, key=lambda s: -details[s]["ret"])
            log("funnel: entry-ok %d, in pool %d (%s), minus held %d, minus cooldown %d%s"
                % (len(entry_ok), len(_in_pool), ",".join(sorted(_in_pool)) or "-",
                   len(_in_pool - held), len(fresh),
                   ("; strongest OUTSIDE the pool: " + ",".join(_outside[:6])) if _outside else ""),
                logpath)
            if risk_usd:
                def feasible(sym, atr_value=None):
                    # The bot's arithmetic, not an approximation of it: quantity = risk / stop
                    # distance, FLOORED to the lot step, then the notional check. The unrounded
                    # notional cleared $5 for TWTUSDT five passes running while the floored one
                    # was $4.83, and each of those passes spent its slot on a line that could never
                    # fill (audit 03.09).
                    m = details[sym]
                    distance = STOP_ATR_MULT * (m["atr"] if atr_value is None else atr_value)
                    if distance <= 0 or m["price"] <= 0:
                        return False
                    qty = risk_usd / distance
                    step = STEP_SIZE.get(sym) or 0.0
                    if step > 0:
                        qty = int(qty / step + 1e-9) * step
                    # The bot judges the minimum one percent under the signal price, because the
                    # exchange judges it at MARK, which sits below LAST about half the time.
                    floor_price = m["price"] * 0.99
                    ok = (qty > 0 and qty >= MIN_QTY.get(sym, 0.0)
                          and qty * floor_price >= MIN_NOTIONAL.get(sym, 5.0))
                    if ok or step <= 0 or round_up_tol <= 0:
                        return ok
                    # The bot's step 9b (05.09): round UP to the exchange minimum when the floor
                    # is refused, inside the tolerance; the 1% hard cap binds regardless.
                    min_notional = MIN_NOTIONAL.get(sym, 5.0)
                    by_notional = math.ceil(min_notional / floor_price / step - 1e-9) * step
                    minimum = max(qty + step, by_notional, MIN_QTY.get(sym, 0.0))
                    return (minimum * distance <= risk_usd * (1 + round_up_tol) + 1e-9
                            and minimum * distance <= equity * 0.01 + 1e-9
                            and minimum * floor_price >= min_notional)
                infeasible = [sym for sym in fresh if not feasible(sym)]
                if infeasible:
                    log("%d candidate(s) too wide to size at this equity (risk $%.2f vs min notional): %s"
                        % (len(infeasible), risk_usd, ",".join(infeasible[:12])), logpath)
                fresh = [sym for sym in fresh if sym not in set(infeasible)]
                if fresh_short:
                    short_wide = [sym for sym in fresh_short
                                  if not feasible(sym, details[sym]["short_atr"])]
                    if short_wide:
                        log("bear arm: %d candidate(s) too wide to size at this equity"
                            " (risk $%.2f vs min notional): %s"
                            % (len(short_wide), risk_usd, ",".join(short_wide[:12])), logpath)
                    fresh_short = [sym for sym in fresh_short if sym not in set(short_wide)]
            pv["l_feas"], pv["s_feas"] = list(fresh), list(fresh_short)
            # Fifteen versions of the same bet is how a red day costs -5%: the correlation
            # filter keeps a candidate out while it moves in lockstep with something already
            # held. Off (MAX_CORR unset) it only reports; the shadow line below is forward
            # evidence for the 14.09 decision.
            if fresh and room > 0:
                basket = [s for s in held if s in details]
                def _worst_corr(sym, others):
                    worst = 0.0
                    for b in others:
                        c = corr60(details[sym].get("closes"), details[b].get("closes"))
                        if c > worst:
                            worst = c
                    return worst
                if max_corr is not None:
                    kept, skipped = [], []
                    for sym in fresh:
                        c = _worst_corr(sym, basket + kept)
                        if c > max_corr:
                            skipped.append("%s(%.2f)" % (sym, c))
                        else:
                            kept.append(sym)
                        if len(kept) >= room:
                            break
                    if skipped:
                        log("corr-filter >%.2f: skipped %s" % (max_corr, ",".join(skipped)), logpath)
                    fresh = kept
                else:
                    shadow = ["%s(%.2f)" % (s, _worst_corr(s, basket)) for s in fresh[:room]
                              if _worst_corr(s, basket) > 0.75]
                    if shadow:
                        log("shadow corr-filter 0.75 would skip %d of %d entr(ies): %s"
                            % (len(shadow), min(room, len(fresh)), ",".join(shadow)), logpath)
            pv["l_corr"] = list(fresh)
            pv["corr_ran"] = max_corr is not None and bool(pv["l_feas"]) and room > 0
            to_open = fresh[:room]
            pv["l_room"] = list(to_open)
            if to_open:
                def near_high(sym):
                    fh = details[sym].get("from_high")
                    return fh is not None and fh <= 0.05
                def on_volume(sym):
                    vr = details[sym].get("vol_ratio")
                    return vr is not None and vr >= 1.5
                kept_nh = [x for x in to_open if near_high(x)]
                kept_nv = [x for x in to_open if near_high(x) and on_volume(x)]
                log("shadow filters on %d entry(ies): near-high would keep %d (%s); "
                    "near-high+volume would keep %d (%s)"
                    % (len(to_open), len(kept_nh), ",".join(kept_nh) or "-",
                       len(kept_nv), ",".join(kept_nv) or "-"), logpath)
                # Shadow candidate for "reads coins better" (owner, 21.09): stronger than the
                # pool's median 20d return. Logged only - the closed year is spent, so this can
                # earn its place on live outcomes alone.
                r20 = [details[s].get("ret20") for s in details if details[s].get("ret20") is not None]
                if r20:
                    med = sorted(r20)[len(r20) // 2]
                    kept_rs = [x for x in to_open
                               if details[x].get("ret20") is not None and details[x]["ret20"] >= med]
                    log("shadow rel-strength: %d of %d entr(ies) at/above the pool median 20d "
                        "(%+.1f%%): %s" % (len(kept_rs), len(to_open), med * 100,
                                           ",".join(kept_rs) or "-"), logpath)
            if cut_overhang or cut_volmax:
                log("gates cut at signal stage (pre-funnel): no-overhang %d (%s); vol-cap %d (%s)"
                    % (len(cut_overhang), ",".join(cut_overhang) or "-",
                       len(cut_volmax), ",".join(cut_volmax) or "-"), logpath)
            if regime == "BEAR" and to_open:
                if gate == "cash":
                    log("regime BEAR, gate cash: suppressing %d entry(ies) (%s)"
                        % (len(to_open), ",".join(to_open)), logpath)
                    to_open = []
                else:
                    log("regime BEAR, gate off: a cash gate would have suppressed %d entry(ies) (%s)"
                        % (len(to_open), ",".join(to_open)), logpath)
            # REGIME_GATE=cash above governs the LONG arm, which is what it was measured as
            # ("staying out of BEAR"). The bear arm carries its own BTC gate and is the one
            # thing meant to work in that regime, so the cash gate does not touch it.
            pv["l_regime"] = list(to_open)
            if halted and to_open:
                log("halt: suppressing %d entry(ies) (%s); exits keep working"
                    % (len(to_open), ",".join(to_open)), logpath)
                to_open = []
            pv["l_final"] = list(to_open)
            # ONE book, one slot budget, and the bear arm is cut LAST - after the regime gate
            # and the halt have taken their entries away, so it is sized against what the long
            # arm actually opens, and never on a symbol that arm is buying this same pass.
            to_open_short = []
            if short_armed and not halted:
                buying = set(to_open)
                room_short = max(0, room - len(to_open))
                to_open_short = [x for x in fresh_short if x not in buying][:room_short]
                if to_open_short:
                    log("bear arm: %d short(s) proposed (%s); BTC below its %dd average"
                        % (len(to_open_short), ",".join(to_open_short), short_sma_days), logpath)
                both = sorted(buying & set(fresh_short))
                if both:
                    log("bear arm: %d coin(s) wanted by BOTH arms this pass - the long wins, "
                        "the short is dropped: %s" % (len(both), ",".join(both)), logpath)
            elif short_armed and halted:
                log("halt: the bear arm proposes nothing either", logpath)
            pv["s_final"] = list(to_open_short)

            if not to_close and not to_open and not to_open_short:
                log("scan: %d held, %d hold-ok, %d entry-ok, nothing to do"
                    % (len(held), len(hold_ok), len(entry_ok)), logpath)
            else:
                stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                # utf-8, matching the UTF-8 the bot reads this file with: an ascii writer turned an
                # unwritable symbol into a mid-write abort that dropped the rest of the scan.
                with io.open(args.script, "a", encoding="utf-8") as f:
                    f.write("\n# autoscan %s\n" % stamp)
                    for s in to_close:
                        # an explicit id keeps a crash-replay idempotent without colliding
                        # with a close of the same symbol from an earlier scan
                        f.write("CLOSE %s id=auto-close-%s-%s reason=%s\n"
                                % (s, s, stamp, reasons.get(s, "trend-exited")))
                        cooldown[s] = now
                        # entered is NOT dropped here. Writing a CLOSE is a request, not an exit:
                        # if it is refused or abandoned the position is still open, and forgetting
                        # its open time would disable max-hold for it permanently. The prune loop
                        # above removes it on the first pass where the exchange says it is gone -
                        # which is the only honest evidence that it actually closed.
                    for s in to_open:
                        m = details[s]
                        trig = m.get("trig") or "trend"
                        # ts= dates the line at the moment its PRICE was read, not at append time:
                        # the bot refuses an entry older than SIGNAL_MAX_AGE_MIN instead of
                        # executing a stall's backlog at market on prices from another market.
                        f.write("%s LONG entry=%.10g atr=%.10g lev=%d id=auto-%s-%s-%s ts=%d\n"
                                % (s, m["price"], m["atr"], args.leverage, trig, s, stamp,
                                   snapshot_at))
                        entered[s] = now
                        entered_trig[s] = trig
                    for s in to_open_short:
                        m = details[s]
                        # Same line, one word different: the bot reads it with Side.valueOf,
                        # puts the stop ABOVE entry for a SHORT and sizes it from the same
                        # ATR. The "short" label is what the max-hold rule reads back.
                        f.write("%s SHORT entry=%.10g atr=%.10g lev=%d id=auto-short-%s-%s"
                                " ts=%d\n"
                                % (s, m["price"], m["short_atr"], args.leverage, s, stamp,
                                   snapshot_at))
                        entered[s] = now
                        entered_trig[s] = "short"
                # Only once every line is in the book: a write that died halfway reports nothing
                # as opened rather than something that may not be there.
                pv["wrote"] = (list(to_close), list(to_open), list(to_open_short), dict(reasons))
                save_state(statepath, state)
                log("scan: %d held -> closing %d (%s), opening %d long (%s), %d short (%s)"
                    % (len(held), len(to_close), ",".join(to_close) or "-",
                       len(to_open), ",".join(to_open) or "-",
                       len(to_open_short), ",".join(to_open_short) or "-"), logpath)
        except Exception as e:
            log("scan error %r; continuing" % (e,), logpath)
            if not state.get("error_streak"):
                notify(env, "scan error %.160r - continuing next hour" % (e,), logpath)
            state["error_streak"] = state.get("error_streak", 0) + 1
            try:
                save_state(statepath, state)
            except Exception:
                pass
        else:
            if state.get("error_streak"):
                state["error_streak"] = 0
                save_state(statepath, state)
        # After everything the pass decided and wrote, and whatever way it ended. Never raises.
        publish_view(viewpath, pv, state, statepath, logpath,
                     lambda: next_wait(args.interval, short_armed and bear_gate_open, pass_start,
                                       time.time()))
        time.sleep(next_wait(args.interval, short_armed and bear_gate_open, pass_start, time.time()))


if __name__ == "__main__":
    sys.exit(main())
