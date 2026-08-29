# -*- coding: utf-8 -*-
"""
4h klines for the owner's own timeframe.

Everything measured so far used daily bars. The owner trades 4h and keeps - fairly -
saying that a daily test is not a test of his machine. This downloads 4h history for
the liquid universe so his rule can be measured on his own clock.

Public REST only, no keys. 1500 bars a request = 250 days of 4h, so ~10 requests a
symbol for the full history. Re-runnable: anything already in cache4h/ is skipped.
ASCII stdout only.

Known limitation, declared here: this uses the live-symbol REST endpoint, so delisted
contracts are absent and survivorship is NOT removed the way the daily study removes it.
That bias flatters the hypothesis - dead coins are the ones whose pumps ended badly - so
a NEGATIVE result on this data is conclusive, while a positive one would need the
archive path before it could be believed.
"""
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
TSMOM_CACHE = os.path.join(os.path.dirname(ROOT), "tsmom", "cache")
CACHE = os.path.join(ROOT, "cache4h")
os.makedirs(CACHE, exist_ok=True)

FAPI = "https://fapi.binance.com"
START_MS = 1567900800000          # 2019-09-08
LIQ_FLOOR = 5e6
_last = [0.0]


def log(msg):
    print(msg)
    sys.stdout.flush()


def get(path, params, tries=5):
    q = "&".join("%s=%s" % (k, v) for k, v in params.items())
    url = "%s%s?%s" % (FAPI, path, q)
    for attempt in range(tries):
        d = time.time() - _last[0]
        if d < 0.25:
            time.sleep(0.25 - d)
        _last[0] = time.time()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "regimeuni/1.0"})
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 418):
                wait = min(float(e.headers.get("Retry-After") or 30), 120.0) + 5
                log("  rate limited, waiting %.0fs" % wait)
                time.sleep(wait)
                continue
            if e.code >= 500:
                time.sleep(3 * (attempt + 1))
                continue
            return None
        except Exception:
            time.sleep(3 * (attempt + 1))
    return None


def liquid_universe():
    """Live symbols whose daily median quote volume ever cleared the floor."""
    uni = json.load(io.open(os.path.join(TSMOM_CACHE, "universe_final.json"), encoding="utf-8"))
    out = []
    for s in uni["live"]:
        kp = os.path.join(TSMOM_CACHE, "klines_%s.json" % s)
        if not os.path.exists(kp):
            continue
        try:
            rows = json.load(io.open(kp, encoding="utf-8"))
        except ValueError:
            continue
        if len(rows) < 90:
            continue
        qs = sorted(float(r[6]) for r in rows[-180:] if r[6])
        if not qs:
            continue
        med = qs[len(qs) // 2]
        if med >= LIQ_FLOOR:
            out.append(s)
    return sorted(out)


def fetch_symbol(sym):
    """Full 4h history as [openTimeMs, o, h, l, c, vol, quoteVol]."""
    rows, start = [], START_MS
    while True:
        batch = get("/fapi/v1/klines",
                    {"symbol": sym, "interval": "4h", "startTime": start, "limit": 1500})
        if not batch:
            break
        for b in batch:
            rows.append([b[0], b[1], b[2], b[3], b[4], b[5], b[7]])
        if len(batch) < 1500:
            break
        nxt = batch[-1][0] + 1
        if nxt <= start:
            break
        start = nxt
    return rows


def main():
    syms = liquid_universe()
    log("liquid live universe: %d symbols" % len(syms))
    done = skipped = failed = 0
    for i, s in enumerate(syms, 1):
        path = os.path.join(CACHE, "k4h_%s.json" % s)
        if os.path.exists(path):
            skipped += 1
            continue
        rows = fetch_symbol(s)
        if not rows or len(rows) < 200:
            failed += 1
            log("[%d/%d] %-16s no usable data" % (i, len(syms), s))
            continue
        json.dump(rows, io.open(path, "w", encoding="utf-8"), separators=(",", ":"))
        done += 1
        if done % 20 == 0:
            log("[%d/%d] %-16s %d bars  (done %d, skipped %d, failed %d)"
                % (i, len(syms), s, len(rows), done, skipped, failed))
    log("finished: downloaded %d, already had %d, failed %d" % (done, skipped, failed))


if __name__ == "__main__":
    main()
