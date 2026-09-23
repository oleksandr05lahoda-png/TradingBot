#!/usr/bin/env python3
"""Crowding collector (12.09): who is already positioned, and on which side.

The one data family the lab has never been able to test. Binance serves these series for
THIRTY DAYS only - deeper history cannot be bought later at any price, the same way order
depth cannot. pautina.py already stores depth, funding and open interest; this stores the
account-level positioning that explains them.

Runs hourly, asks for the last 2 hours on the 15m grid, and writes only bars it has not
written before - so a missed run self-heals on the next one and a double run writes nothing
twice. Public endpoints, no keys, no orders: this process cannot touch the trading account.

~400 requests per run against a 2400/min budget shared with a bot that uses ~6%.

DATA CONTRACT (established 14.09 by the first probe, confirmed two ways): for src="taker" the
row's ts is the START of Binance's 15-minute volume bucket, not the moment of the snapshot -
the buy/sell ratio correlates +0.41 with the mid move over [ts, ts+15m] and only +0.04 with the
move before ts. Any test of "taker flow -> future return" must take the return from ts+15m, or
it is looking ahead. For top_pos / top_acc / all_acc, ts is the snapshot moment. The schema is
deliberately left as-is: the collection is running and the rule lives in the reader
(analysis/roots/PREREG_CROWD_2026-12-20.md).
"""
import gzip
import io
import json
import os
import time
import urllib.parse
import urllib.request

OUT = "/opt/tradingbot-data/flow"
STATE = "/opt/crowd.state"
BASE = "https://fapi.binance.com"

# endpoint -> short tag stored in the row, so one file holds all four series
SOURCES = [
    ("/futures/data/topLongShortPositionRatio", "top_pos"),
    ("/futures/data/topLongShortAccountRatio", "top_acc"),
    ("/futures/data/globalLongShortAccountRatio", "all_acc"),
    ("/futures/data/takerlongshortRatio", "taker"),
]

os.makedirs(OUT, exist_ok=True)


def get(path, params=None):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "crowd/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def traded_symbols(days=7):
    """Coins the bot holds or entered recently. The cap list is rebuilt every Monday, so a coin
    the bot is IN can already be out of the list when this runs: 11 of 69 live entries between
    31.08 and 14.09 had no snapshot at all (probe 14.09). A collector that cannot see the bot's
    own trades cannot judge them. Every source is optional; a missing or corrupt file only
    shrinks the union back to the cap list."""
    import calendar
    out = []
    try:
        led = json.load(io.open("/opt/tradingbot-data/book-ledger-real.json"))
        out.extend(p["symbol"] for p in led.get("positions", []) if p.get("symbol"))
    except Exception:
        pass
    try:
        out.extend(json.load(io.open("/opt/tradingbot-data/autoscan_state.json")).get("entered", {}).keys())
    except Exception:
        pass
    try:
        cutoff = time.time() - days * 86400
        for ln in io.open("/opt/tradingbot-data/trades.jsonl", encoding="utf-8", errors="ignore"):
            try:
                r = json.loads(ln)
            except ValueError:
                continue
            if r.get("kind") != "entry" or not r.get("symbol"):
                continue
            try:
                if calendar.timegm(time.strptime((r.get("ts") or "")[:19], "%Y-%m-%dT%H:%M:%S")) >= cutoff:
                    out.append(r["symbol"])
            except ValueError:
                continue
    except Exception:
        pass
    return [s for s in out if isinstance(s, str) and s.endswith("USDT")]


def universe():
    """The scanner's cached cap list when present (else top-100 by traded volume), plus whatever
    the bot actually holds or entered in the last week. Same union pautina.py uses, so the two
    files describe the same coins on the same day. Order kept, duplicates dropped, capped at 150
    so one run stays around 600 requests."""
    syms = []
    try:
        st = json.load(io.open("/opt/tradingbot-data/autoscan_state.json"))
        syms = [s for s in st.get("cap_pool", [])][:120]
    except Exception:
        pass
    if not syms:
        try:
            tick = get("/fapi/v1/ticker/24hr")
            tick.sort(key=lambda t: -float(t.get("quoteVolume", 0)))
            syms = [t["symbol"] for t in tick if t["symbol"].endswith("USDT")][:100]
        except Exception:
            syms = []
    return list(dict.fromkeys(syms + traded_symbols()))[:150]


def load_state():
    """Last bar timestamp already written, keyed 'SYMBOL|tag'. A corrupt file is not fatal:
    losing it re-writes at most two hours of bars, and the reader de-duplicates anyway."""
    try:
        return json.load(io.open(STATE))
    except Exception:
        return {}


syms = universe()
state = load_state()
rows = []
errors = 0

for s in syms:
    for path, tag in SOURCES:
        key = s + "|" + tag
        last = state.get(key, 0)
        try:
            # limit=8 on the 15m grid covers two hours: one missed run heals on the next.
            data = get(path, {"symbol": s, "period": "15m", "limit": 8})
        except Exception:
            errors += 1
            time.sleep(0.4)
            continue
        newest = last
        for d in data:
            try:
                ts = int(d["timestamp"])
            except Exception:
                continue
            if ts <= last:
                continue          # already stored on an earlier run
            row = {"ts": ts // 1000, "s": s, "src": tag}
            if tag == "taker":
                row["buy_vol"] = float(d.get("buyVol") or 0)
                row["sell_vol"] = float(d.get("sellVol") or 0)
                row["ratio"] = float(d.get("buySellRatio") or 0)
            else:
                row["long_acc"] = float(d.get("longAccount") or 0)
                row["short_acc"] = float(d.get("shortAccount") or 0)
                row["ratio"] = float(d.get("longShortRatio") or 0)
            rows.append(row)
            if ts > newest:
                newest = ts
        state[key] = newest
        time.sleep(0.15)

# Group by UTC day so a row always lands in the file for the day it describes, even when a
# run straddles midnight - otherwise the last bars of a day would be filed under the next.
by_day = {}
for r in rows:
    by_day.setdefault(time.strftime("%Y-%m-%d", time.gmtime(r["ts"])), []).append(r)

for day, drows in by_day.items():
    with gzip.open(os.path.join(OUT, "crowd-" + day + ".jsonl.gz"), "ab") as f:
        for r in drows:
            f.write((json.dumps(r, separators=(",", ":")) + "\n").encode())

# The state is written only after the rows are on disk. The reverse order would mark bars as
# stored that a crash never wrote, and those bars are the ones that cannot be fetched again.
tmp = STATE + ".tmp"
with io.open(tmp, "w") as f:
    json.dump(state, f)
os.replace(tmp, STATE)

print("crowd: %d symbols, %d new bar(s) over %d day file(s), %d endpoint error(s)"
      % (len(syms), len(rows), len(by_day), errors))
