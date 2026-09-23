#!/usr/bin/env python3
"""Pautina collector (31.08): orderbook + funding + open interest for the trading universe,
every 15 minutes, into gzipped JSONL on the data volume. This is the ONE data source that
cannot be backfilled later - exchanges do not serve historical depth. ~2 MB/day gzipped,
~300 request-weight per sweep against a 2400/min budget shared with a bot that uses ~6%.
Public endpoints only; no keys, no orders, cannot touch the trading account."""
import urllib.request, json, time, gzip, io, os, urllib.parse

OUT = "/opt/tradingbot-data/flow"
STATE = "/opt/pautina.funding.state"
os.makedirs(OUT, exist_ok=True)

def get(path, params=None):
    url = "https://fapi.binance.com" + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "pautina/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

# universe: the scanner-cached cap list when present, else top-100 by volume
syms = []
try:
    st = json.load(io.open("/opt/tradingbot-data/autoscan_state.json"))
    syms = [s for s in st.get("cap_pool", [])][:120]
except Exception:
    pass
if not syms:
    tick = get("/fapi/v1/ticker/24hr")
    tick.sort(key=lambda t: -float(t.get("quoteVolume", 0)))
    syms = [t["symbol"] for t in tick if t["symbol"].endswith("USDT")][:100]


def traded_symbols(days=7):
    """Coins the bot holds or entered recently. The cap list is rebuilt every Monday, so a coin
    the bot is IN can already be out of the list when this runs: 11 of 69 live entries between
    31.08 and 14.09 had no order-book snapshot at all (probe 14.09). A collector that cannot
    see the bot's own trades cannot judge them. Every source is optional; a missing or corrupt
    file only shrinks the union back to the cap list."""
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


# cap list first, then whatever the bot actually trades; order kept, duplicates dropped, capped so
# one sweep stays under the request budget (each name costs depth + openInterest = 3 weight).
syms = list(dict.fromkeys(syms + traded_symbols()))[:150]

ts = int(time.time())
rows = []

# funding + mark for every symbol in ONE call (weight 10). Outside a try, this single request
# aborted the whole sweep on one timeout - twice in 1111 runs (audit 11.09). Funding and mark
# are the cheap part: they can be refetched from history any time. Depth cannot be refetched
# at all, so losing the sweep to save two fields is the wrong trade.
try:
    prem = {p["symbol"]: p for p in get("/fapi/v1/premiumIndex")}
except Exception:
    prem = {}

for s in syms:
    try:
        d = get("/fapi/v1/depth", {"symbol": s, "limit": 20})     # weight 2
        bids, asks = d.get("bids", []), d.get("asks", [])
        if not bids or not asks:
            continue
        bb, ba = float(bids[0][0]), float(asks[0][0])
        mid = (bb + ba) / 2
        p = prem.get(s, {})
        oi = None
        try:
            oi = float(get("/fapi/v1/openInterest", {"symbol": s})["openInterest"])
        except Exception:
            pass
        rows.append({
            "ts": ts, "s": s,
            "spread_bp": round((ba - bb) / mid * 1e4, 3),
            "b1": float(bids[0][1]), "a1": float(asks[0][1]),
            "b5": round(sum(float(x[1]) for x in bids[:5]), 4),
            "a5": round(sum(float(x[1]) for x in asks[:5]), 4),
            "b20": round(sum(float(x[1]) for x in bids), 4),
            "a20": round(sum(float(x[1]) for x in asks), 4),
            "mid": mid,
            "fund": float(p.get("lastFundingRate") or 0),
            "mark": float(p.get("markPrice") or 0),
            "oi": oi,
        })
        time.sleep(0.15)
    except Exception:
        time.sleep(0.4)

day = time.strftime("%Y-%m-%d", time.gmtime(ts))
with gzip.open(os.path.join(OUT, day + ".jsonl.gz"), "ab") as f:
    for r in rows:
        f.write((json.dumps(r, separators=(",", ":")) + "\n").encode())

# carry wake-up call: alert once when median |funding| crosses 3bp/8h (~33%/yr)
funds = sorted(abs(r["fund"]) for r in rows if r.get("fund") is not None)
med = funds[len(funds)//2] if funds else 0.0
prev = ""
if os.path.exists(STATE):
    prev = io.open(STATE).read().strip()
cur = "awake" if med >= 0.0003 else "calm"
# A failed premiumIndex leaves every fund at 0.0, which reads as "calm" and would fire a
# "carry went back to sleep" alert about a request that simply timed out. No reading, no
# verdict: keep the previous state and say nothing.
if not prem:
    cur = prev or cur
if prev and cur != prev:   # молчим на первом запуске: пустое prev - не событие
    envd = {}
    for ln in io.open("/opt/tradingbot.env", encoding="utf-8", errors="ignore"):
        t = ln.strip()
        if "=" in t and not t.startswith("#"):
            k, v = t.split("=", 1)
            envd[k.strip()] = v.strip()
    msg = ("КАРРИ ПРОСНУЛСЯ: медианный фандинг %.3f%%/8ч (~%.0f%%/год). Режим эйфории - "
           "тот самый, где карри-машина реальна." % (med*100, med*3*365*100)) if cur == "awake" else \
          ("Карри уснул: медианный фандинг снова у нуля (%.4f%%/8ч)." % (med*100))
    body = urllib.parse.urlencode({"chat_id": envd["TELEGRAM_CHAT_ID"], "text": msg}).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(
            "https://api.telegram.org/bot%s/sendMessage" % envd["TELEGRAM_BOT_TOKEN"], data=body), timeout=20)
    except Exception:
        pass
io.open(STATE, "w").write(cur)
print("collected %d symbols; median |funding| %.4f%%/8h; day file %s" % (len(rows), med*100, day))
