# -*- coding: utf-8 -*-
"""
Directional scanner. Produces a signal script for TestnetBot's manual source.

This lives OUTSIDE the bot on purpose: SignalSource is a transport, and a source
that computed an indicator would be a strategy wearing that interface as a
costume. The bot still decides size, stop, leverage and whether to trade at all.

Rule (fixed, not searched -- see analysis/tsmom/PREREG.md):
  direction = sign of the trailing 90d close-to-close return
  LONG if positive, SHORT if negative, one side per coin, never both
  stop distance is left to the bot: we hand it ATR(14) on daily bars

What this rule is worth, measured on 2024-2026 holdout: hedged alpha +27.2%/y at
t = 0.94, against a pre-registered threshold of 2.0. It did NOT pass. It is used
here because it scored highest among everything measured, not because it works.

Usage:
  python scan.py --out signals.txt [--top 120] [--min-volume 5000000] [--leverage 2]
  python scan.py --out signals.txt --max-signals 4      # a small live smoke run
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

FAPI = "https://fapi.binance.com"
LOOKBACK_DAYS = 90
ATR_PERIOD = 14
BARS_NEEDED = LOOKBACK_DAYS + ATR_PERIOD + 5

_last = [0.0]


def get(path, params, gap=0.25, tries=6):
    url = FAPI + path + ("?" + urllib.parse.urlencode(params) if params else "")
    for attempt in range(tries):
        d = time.time() - _last[0]
        if d < gap:
            time.sleep(gap - d)
        _last[0] = time.time()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "scanner/1.0"})
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


def universe(min_volume):
    """Liquid, currently trading, genuinely crypto USDT perps."""
    info = get("/fapi/v1/exchangeInfo", {}, gap=0.5)
    syms = [s["symbol"] for s in info["symbols"]
            if s.get("quoteAsset") == "USDT"
            and s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING"
            and s.get("underlyingType") == "COIN"]
    tickers = get("/fapi/v1/ticker/24hr", {}, gap=0.5)
    vol = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in tickers}
    picked = [(s, vol.get(s, 0.0)) for s in syms if vol.get(s, 0.0) >= min_volume]
    picked.sort(key=lambda x: -x[1])
    return picked


def atr(bars, period):
    """Wilder-style ATR on daily bars: [openTime, o, h, l, c, ...]."""
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--top", type=int, default=150,
                    help="scan at most this many coins, by 24h turnover")
    ap.add_argument("--min-volume", type=float, default=5e6)
    ap.add_argument("--leverage", type=int, default=2)
    ap.add_argument("--max-signals", type=int, default=0,
                    help="emit at most N lines (0 = no limit); use a small N for a smoke run")
    args = ap.parse_args()

    if not 1 <= args.leverage <= 5:
        print("leverage must be 1..5 (the bot's hard cap is 5)")
        return 1

    pool = universe(args.min_volume)[:args.top]
    print("universe: %d coins above $%.0fM/24h" % (len(pool), args.min_volume / 1e6))

    lines, longs, shorts, skipped = [], 0, 0, 0
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    for i, (sym, _) in enumerate(pool):
        bars = get("/fapi/v1/klines", {"symbol": sym, "interval": "1d", "limit": BARS_NEEDED})
        if not bars or len(bars) < BARS_NEEDED - 3:
            skipped += 1
            continue
        try:
            last = float(bars[-1][4])
            base = float(bars[-1 - LOOKBACK_DAYS][4])
        except (IndexError, ValueError):
            skipped += 1
            continue
        if base <= 0 or last <= 0:
            skipped += 1
            continue
        a = atr(bars[-(ATR_PERIOD + 2):], ATR_PERIOD)
        if not a or a <= 0:
            skipped += 1
            continue
        ret = last / base - 1.0
        if ret == 0:
            skipped += 1
            continue
        side = "LONG" if ret > 0 else "SHORT"
        longs += side == "LONG"
        shorts += side == "SHORT"
        lines.append("%s %s entry=%.10g atr=%.10g lev=%d id=tsmom-%s-%s"
                     % (sym, side, last, a, args.leverage, sym, stamp))
        if (i + 1) % 25 == 0:
            print("  scanned %d/%d" % (i + 1, len(pool)))

    if args.max_signals:
        keep, seen_long, seen_short = [], 0, 0
        half = max(1, args.max_signals // 2)
        for ln in lines:                       # keep both directions represented
            is_long = " LONG " in ln
            if is_long and seen_long < half:
                keep.append(ln); seen_long += 1
            elif not is_long and seen_short < args.max_signals - half:
                keep.append(ln); seen_short += 1
            if len(keep) >= args.max_signals:
                break
        lines = keep

    longs = sum(1 for ln in lines if " LONG " in ln)
    shorts = len(lines) - longs

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("# generated %s by tools/scanner/scan.py\n" % stamp)
        f.write("# rule: sign of trailing %dd return; measured t=0.94 on holdout, DID NOT PASS\n"
                % LOOKBACK_DAYS)
        f.write("# the bot still sizes from the stop, caps leverage and may refuse any line\n")
        for ln in lines:
            f.write(ln + "\n")

    print("written %s: %d signals (%d long, %d short), %d coins skipped"
          % (args.out, len(lines), longs, shorts, skipped))
    print("run:  ./gradlew run --args=\"--source manual --script %s\"" % args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
