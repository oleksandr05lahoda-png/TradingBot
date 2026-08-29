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
                # Capped AND once: a 1028-minute ban header would otherwise park this
                # single-threaded loop inside one data call, exits included — and retrying five
                # times multiplies the cap back into ~25 minutes. Wait the capped moment, then let
                # the pass fail so the outer loop reaches its own sleep and the next pass (28.08).
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
              "ENTRY_NEAR_HIGH", "ENTRY_VOL_MULT", "MAX_HOLD_HOURS", "MAX_CORR",
              "BEAR_UNIVERSE", "BEAR_DAY_PCT"):
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


MIN_NOTIONAL = {}    # symbol -> exchange minimum notional in USDT, refreshed with the universe


def universe(top, min_volume, by_cap, cached_cap=None):
    """Returns (pool, source). Source 'cap' is the live CoinGecko list; 'cap-cached' is the last
    good one when CoinGecko refuses; 'none' when neither exists. There is deliberately no
    volume fallback for a cap universe: top-100 by volume is where the pumps live, and on
    22.08 06:52 that list plus momentum ranking nominated BTW/AKE/ACE/BOME/MUBARAK/PUMP/HEMI -
    all refused by the risk core, none of them anything the owner would hold."""
    info = get("/fapi/v1/exchangeInfo", {}, gap=0.5)
    if not info:
        return [], "none"
    tradable = {s["symbol"] for s in info["symbols"]
                if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING" and s.get("underlyingType") == "COIN"}
    for s in info["symbols"]:
        if s["symbol"] in tradable:
            for f in s.get("filters", []):
                if f.get("filterType") == "MIN_NOTIONAL":
                    try:
                        MIN_NOTIONAL[s["symbol"]] = float(f.get("notional", 5))
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
    cg = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(CG_MARKETS, headers={"User-Agent": "autoscan/1.0"})
            with urllib.request.urlopen(req, timeout=45) as r:
                cg = json.loads(r.read().decode("utf-8"))
            break
        except Exception:
            time.sleep(5 * (attempt + 1))
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
    bars = get("/fapi/v1/klines", {"symbol": sym, "interval": "1d", "limit": max(need, 61)})
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
    return {"ret": ret, "dip": dip, "price": price, "atr": a,
            "from_high": (1 - price / hi20) if hi20 > 0 else None, "vol_ratio": vol_ratio,
            "closes": [float(b[4]) for b in bars[-61:]]}


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
    ap.add_argument("--bear-top", type=int, default=250,
                    help="how deep the cap list goes on a bear day when BEAR_UNIVERSE=on; "
                         "CoinGecko's page caps this at 250")
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
    log("autoscan start: venue=%s top=%d by_cap=%s lookback=%dd dip=%.0f%% bands=+%.0f%%/-%.0f%% "
        "interval=%ds max_pos=%d min_hold=%.0fh regime_gate=%s near_high=%s vol_mult=%s max_hold=%s "
        "max_corr=%s bear_universe=%s"
        % (args.venue, args.top, args.by_cap, args.lookback, args.dip_depth * 100,
           args.entry_band * 100, args.exit_band * 100, args.interval,
           args.max_positions, args.min_hold_hours, gate, near_high_max, vol_mult_min, max_hold_hours,
           max_corr,
           ("on top-%d @ %+.1f%%" % (bear_top, bear_day_pct * 100)) if bear_universe else "off"),
        logpath)

    while True:
        try:
            halted = bot_is_halted(args.bot_log)
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
                time.sleep(min(args.interval, 300))
                continue

            reasons = {}
            try:
                rows = signed_get("/fapi/v2/positionRisk", env)
                held = {r["symbol"] for r in rows if float(r["positionAmt"]) != 0}
                held_since = {r["symbol"]: float(r.get("updateTime", 0)) / 1000.0
                              for r in rows if float(r["positionAmt"]) != 0 and r.get("updateTime")}
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
                time.sleep(args.interval)
                continue
            if state.get("fail_streak", 0) > 0:
                notify(env, "exchange is answering again after %d skipped scan(s)"
                       % state["fail_streak"], logpath)
                state["fail_streak"] = 0
                save_state(statepath, state)

            # One call, sliced twice: the bull pool is the head of the same ranked list the
            # bear pool is drawn from, so a bear day costs no extra CoinGecko or exchangeInfo
            # request - only the deeper klines sweep below, and only when the switch is armed.
            wide_pool, pool_source = universe(bear_top if bear_universe else args.top,
                                              args.min_volume, args.by_cap,
                                              cached_cap=state.get("cap_pool"))
            pool = wide_pool[:args.top]
            if not pool:
                log("universe empty (no CoinGecko answer and no cached list); skipping", logpath)
                notify(env, "universe empty - no CoinGecko answer and no cached top-100 yet; "
                            "this scan is skipped", logpath)
                # Five minutes, not an hour: a pass that produced no pool also produced no EXITS,
                # and one transient refusal should not cost the whole hour's exit management.
                # Matches the not-ready branch above.
                time.sleep(min(args.interval, 300))
                continue
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
                bal = signed_get("/fapi/v2/balance", env)
                usdt = next((float(b["balance"]) for b in bal if b.get("asset") == "USDT"), None)
                if usdt:
                    risk_usd = usdt * float(env.get("RISK_PER_TRADE", "0.005"))
            except Exception as e:
                log("balance unreadable (%s) - feasibility filter off this pass" % type(e).__name__, logpath)
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
            # A held coin that drops out of the top list must still be judged by the rule,
            # never by list membership: to_close = held - hold_ok, so leaving it unevaluated
            # would close it for falling off CoinGecko's page.
            # On a bear day the deeper list is swept too, because the entry pool is chosen from
            # it below. Held coins are always evaluated whichever pool is active - the switch
            # must never be able to close a position by changing what the scanner looks at.
            scan_pool = wide_pool if bear_day else pool
            to_evaluate = ([sym for sym, _ in scan_pool]
                           + sorted(held - {sym for sym, _ in scan_pool}))
            for sym in to_evaluate:
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
                        passes = True
                        if m["trig"] == "trend" and near_high_max is not None:
                            fh = m.get("from_high")
                            passes = fh is not None and fh <= near_high_max
                        if passes and m["trig"] == "trend" and vol_mult_min is not None:
                            vr = m.get("vol_ratio")
                            passes = vr is not None and vr >= vol_mult_min
                        if passes:
                            entry_ok.add(sym)
                if m["ret"] > -args.exit_band or m["dip"]:
                    hold_ok.add(sym)

            # Which pool may supply a NEW entry this pass. Normally the top-100. On a red BTC
            # day with BEAR_UNIVERSE=on, the fifth of the deeper list least correlated with BTC
            # - the coins that are not simply BTC with a multiplier. Exits never consult this.
            # No shadow line when the switch is off: the deeper list is not fetched then, so
            # any "would have" would be computed from the top-100 and would be a lie.
            entry_pool = {sym for sym, _ in pool}
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

            now = time.time()
            entered = state.setdefault("entered", {})
            cooldown = state.setdefault("cooldown", {})
            entered_trig = state.setdefault("entered_trig", {})
            # A stop or take that fired on the exchange leaves no CLOSE line, so without this the
            # coin was re-proposed at the very next pass while still top of the momentum list.
            for s in [x for x in entered if x not in held]:
                cooldown[s] = now
                entered.pop(s, None)
                entered_trig.pop(s, None)
            # The state file must not grow forever: a week-old cooldown is long expired.
            for s in [x for x, t in cooldown.items() if now - t > 7 * 86400]:
                cooldown.pop(s, None)

            to_close = []
            for s in sorted(held - hold_ok):
                if now - entered.get(s, 0) < args.min_hold_hours * 3600:
                    continue           # the exchange-side stop still guards it meanwhile
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
                    if entered_trig.get(s, "trend") != "trend":
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
            if risk_usd:
                def feasible(sym):
                    m = details[sym]
                    stop_frac = STOP_ATR_MULT * m["atr"] / m["price"]
                    return stop_frac > 0 and risk_usd / stop_frac >= MIN_NOTIONAL.get(sym, 5.0)
                infeasible = [sym for sym in fresh if not feasible(sym)]
                if infeasible:
                    log("%d candidate(s) too wide to size at this equity (risk $%.2f vs min notional): %s"
                        % (len(infeasible), risk_usd, ",".join(infeasible[:12])), logpath)
                fresh = [sym for sym in fresh if sym not in set(infeasible)]
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
            to_open = fresh[:room]
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
            if regime == "BEAR" and to_open:
                if gate == "cash":
                    log("regime BEAR, gate cash: suppressing %d entry(ies) (%s)"
                        % (len(to_open), ",".join(to_open)), logpath)
                    to_open = []
                else:
                    log("regime BEAR, gate off: a cash gate would have suppressed %d entry(ies) (%s)"
                        % (len(to_open), ",".join(to_open)), logpath)
            if halted and to_open:
                log("halt: suppressing %d entry(ies) (%s); exits keep working"
                    % (len(to_open), ",".join(to_open)), logpath)
                to_open = []

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
                save_state(statepath, state)
                log("scan: %d held -> closing %d (%s), opening %d (%s)"
                    % (len(held), len(to_close), ",".join(to_close) or "-",
                       len(to_open), ",".join(to_open) or "-"), logpath)
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
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
