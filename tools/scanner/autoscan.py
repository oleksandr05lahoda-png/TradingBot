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
import os
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
              "?vs_currency=usd&order=market_cap_desc&per_page=250&page=1")
STABLECOINS = {"USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "PYUSD", "USDS",
               "USD1", "BUSD", "USDP", "USDD", "USDF", "RLUSD"}

ATR_PERIOD = 14
STOP_ATR_MULT = 2.0    # mirrors the bot's ATR-fallback stop; keep in sync with RiskEngine
_last = [0.0]


def log(msg, path):
    line = "%s %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line, flush=True)
    with io.open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


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
                time.sleep(float(e.headers.get("Retry-After") or 30) + 5)
                continue
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
    q = "timestamp=%d&recvWindow=10000" % (int(time.time() * 1000) - _skew[0])
    sig = hmac.new(env[SECRET_ENV[0]].encode(), q.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request("%s%s?%s&signature=%s" % (SIGNED_BASE[0], path, q, sig),
                                 headers={"X-MBX-APIKEY": env[KEY_ENV[0]]})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        _skew[0] = None
        raise


def read_env(repo):
    env = {}
    for ln in io.open(os.path.join(repo, "local.env"), encoding="utf-8", errors="ignore"):
        ln = ln.strip()
        if "=" in ln and not ln.startswith("#"):
            k, v = ln.split("=", 1)
            env[k] = v.strip()
    return env


def universe(top, min_volume, by_cap):
    info = get("/fapi/v1/exchangeInfo", {}, gap=0.5)
    if not info:
        return []
    tradable = {s["symbol"] for s in info["symbols"]
                if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING" and s.get("underlyingType") == "COIN"}
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
    if not by_cap:
        out = [(s, vol.get(s, 0.0)) for s in tradable if vol.get(s, 0.0) >= min_volume]
        out.sort(key=lambda x: -x[1])
        return out[:top]
    try:
        req = urllib.request.Request(CG_MARKETS, headers={"User-Agent": "autoscan/1.0"})
        with urllib.request.urlopen(req, timeout=45) as r:
            cg = json.loads(r.read().decode("utf-8"))
    except Exception:
        return []
    out, seen = [], set()
    for c in cg:
        sym = (c.get("symbol") or "").upper()
        if not sym or sym in STABLECOINS or sym in seen:
            continue
        seen.add(sym)
        for cand in (sym + "USDT", "1000" + sym + "USDT", "1000000" + sym + "USDT"):
            if cand in tradable and vol.get(cand, 0.0) >= min_volume:
                out.append((cand, vol.get(cand, 0.0)))
                break
        if len(out) >= top:
            break
    return out


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
    bars = get("/fapi/v1/klines", {"symbol": sym, "interval": "1d", "limit": need})
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
    return {"ret": ret, "dip": dip, "price": price, "atr": a}


def load_state(path):
    try:
        with io.open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"entered": {}, "cooldown": {}}


def save_state(path, state):
    tmp = path + ".tmp"
    with io.open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def bot_is_halted(bot_log):
    """The halt latch clears only with an operator restart, which starts a new log file."""
    try:
        with io.open(bot_log, "r", encoding="utf-8", errors="ignore") as f:
            return "HALTED at" in f.read()
    except OSError:
        return False


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
    env = read_env(args.repo)
    if not env.get(KEY_ENV[0]) or not env.get(SECRET_ENV[0]):
        raise SystemExit("missing %s/%s in local.env for --venue %s"
                         % (KEY_ENV[0], SECRET_ENV[0], args.venue))
    state = load_state(statepath)
    log("autoscan start: venue=%s top=%d by_cap=%s lookback=%dd dip=%.0f%% bands=+%.0f%%/-%.0f%% "
        "interval=%ds max_pos=%d min_hold=%.0fh"
        % (args.venue, args.top, args.by_cap, args.lookback, args.dip_depth * 100,
           args.entry_band * 100, args.exit_band * 100, args.interval,
           args.max_positions, args.min_hold_hours), logpath)

    while True:
        try:
            if bot_is_halted(args.bot_log):
                # Closes still work during a halt, so feeding the file would slowly eat the book
                # while opening nothing back. Stand down completely and say so, loudly.
                log("BOT IS HALTED — standing down; no closes, no opens, book frozen under its "
                    "stops until the operator restarts the bot", logpath)
                time.sleep(args.interval)
                continue

            try:
                rows = signed_get("/fapi/v2/positionRisk", env)
                held = {r["symbol"] for r in rows if float(r["positionAmt"]) != 0}
            except Exception as e:
                # str(e) matters: an IP-whitelist rejection or a revoked key looks identical to
                # a network blip by type name alone, and only the detail tells the operator
                # which of the two ate the whole scan hour.
                log("exchange unreachable (%s: %.160s); skipping this scan"
                    % (type(e).__name__, e), logpath)
                time.sleep(args.interval)
                continue

            pool = universe(args.top, args.min_volume, args.by_cap)
            if not pool:
                log("universe empty; skipping", logpath)
                time.sleep(args.interval)
                continue
            tick = get("/fapi/v1/ticker/price", {}, gap=0.5) or []
            live = {t["symbol"]: float(t["price"]) for t in tick}

            entry_ok, hold_ok, details = set(), set(), {}
            for sym, _ in pool:
                m = evaluate(sym, args.lookback, args.dip_depth, live.get(sym, 0.0))
                if not m:
                    continue
                details[sym] = m
                # Hysteresis: enter above +band, hold anything above -band. A coin wobbling
                # around zero neither enters nor exits — churn is pure cost, measured at 10bp
                # a round trip.
                # The bot puts its stop at entry - ATR x STOP_ATR_MULT; when that lands at
                # or below zero it auto-rejects, so nominating the coin only burns a slot
                # (BEAT: atr 0.95 on price 0.44). Gate ONLY the entry side - a held coin
                # must stay hold-eligible, this filter must never force a close.
                if m["ret"] > args.entry_band or m["dip"]:
                    if m["price"] > STOP_ATR_MULT * m["atr"]:
                        entry_ok.add(sym)
                if m["ret"] > -args.exit_band or m["dip"]:
                    hold_ok.add(sym)

            now = time.time()
            entered = state.setdefault("entered", {})
            cooldown = state.setdefault("cooldown", {})

            to_close = []
            for s in sorted(held - hold_ok):
                if now - entered.get(s, 0) < args.min_hold_hours * 3600:
                    continue           # the exchange-side stop still guards it meanwhile
                to_close.append(s)
            room = max(0, args.max_positions - (len(held) - len(to_close)))
            fresh = [s for s in sorted(entry_ok - held)
                     if now - cooldown.get(s, 0) > args.cooldown_hours * 3600]
            to_open = fresh[:room]

            if not to_close and not to_open:
                log("scan: %d held, %d hold-ok, %d entry-ok, nothing to do"
                    % (len(held), len(hold_ok), len(entry_ok)), logpath)
            else:
                stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                with io.open(args.script, "a", encoding="ascii") as f:
                    f.write("\n# autoscan %s\n" % stamp)
                    for s in to_close:
                        # an explicit id keeps a crash-replay idempotent without colliding
                        # with a close of the same symbol from an earlier scan
                        f.write("CLOSE %s id=auto-close-%s-%s reason=trend-exited\n"
                                % (s, s, stamp))
                        cooldown[s] = now
                        entered.pop(s, None)
                    for s in to_open:
                        m = details[s]
                        trig = "trend" if m["ret"] > args.entry_band else "dip"
                        f.write("%s LONG entry=%.10g atr=%.10g lev=%d id=auto-%s-%s-%s\n"
                                % (s, m["price"], m["atr"], args.leverage, trig, s, stamp))
                        entered[s] = now
                save_state(statepath, state)
                log("scan: %d held -> closing %d (%s), opening %d (%s)"
                    % (len(held), len(to_close), ",".join(to_close) or "-",
                       len(to_open), ",".join(to_open) or "-"), logpath)
        except Exception as e:
            log("scan error %r; continuing" % (e,), logpath)
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
