# -*- coding: utf-8 -*-
"""
H5 - the owner's strategy stated exactly, and the one combination never run.

  BULL day  -> top-100 by liquidity, the bot's normal selector, as it runs today
  BEAR day  -> the SAME selector, but drawn from the coins that do not follow BTC
               (lowest-correlation quintile of the top-300)

The difference from H3 (run.py): H3 flagged BEAR from BTC's 30-day return. In a month-long
downtrend nothing passes "up 30d, near its 20d high, volume x1.5", so the book came out
empty and the result said more about the flag than about the idea. On a single red BTC day
the market can still be broadly rising, so the selector has something to choose from - which
is what the owner has been saying all along.

Same audited engine as run.py; only the bear flag changes. ASCII stdout only.
"""
import io
import json
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(os.path.dirname(ROOT), "tsmom"))

import analyze as A            # noqa: E402
import run as R                # noqa: E402


def main():
    closes, qvols, funding = A.load()
    highs, lows = {}, {}
    tc = os.path.join(os.path.dirname(ROOT), "tsmom", "cache")
    uni = json.load(io.open(os.path.join(tc, "universe_final.json"), encoding="utf-8"))
    for s in uni["live"] + uni["dead"]:
        kp = os.path.join(tc, "klines_%s.json" % s)
        if not os.path.exists(kp) or s not in closes:
            continue
        h, l = {}, {}
        for r in json.load(io.open(kp, encoding="utf-8")):
            d = A.dstr(r[0])
            try:
                h[d], l[d] = float(r[2]), float(r[3])
            except (TypeError, ValueError):
                pass
        highs[s], lows[s] = h, l
    closes = {s: c for s, c in closes.items() if s in highs}
    all_days = A.series_days(closes)
    rets = A.build_returns(closes)
    btc_ret = rets["BTCUSDT"]
    feats = R.build_features(closes, highs, lows, qvols, all_days)
    ret1 = R.btc_trailing(closes, all_days, 1)
    ret30 = R.btc_trailing(closes, all_days, 30)
    corr = R.rolling_corr_to_btc(rets, all_days)

    # simulate() calls it BEAR when btc_ret_w[30][d] < 0, so feed it the flag we want.
    flags = [
        ("control: no switch (base)", None),
        ("switch on BTC red today", {d: v for d, v in ret1.items()}),
        ("switch on BTC < -1% today", {d: v + 0.01 for d, v in ret1.items()}),
        ("switch on BTC < -2% today", {d: v + 0.02 for d, v in ret1.items()}),
        ("switch on BTC 30d < 0 (H3)", ret30),
    ]

    print("BULL: top-100, normal selector. BEAR: same selector, lowest-correlation")
    print("quintile of top-300. Only the bear flag differs between rows.\n")
    print("%-30s %19s %19s" % ("", "design", "holdout"))
    out = {}
    for label, flag in flags:
        kw = dict(universe_top=100)
        if flag is None:
            w = {30: ret30, 1: ret1}
        else:
            w = {30: flag, 1: ret1}
            kw["regime_universe"] = True
        daily = R.simulate(feats, rets, funding, all_days, corr, w, **kw)
        row = {}
        line = "%-30s" % label
        for wl, lo, hi in (("design", "2000-01-01", A.DESIGN_END),
                           ("holdout", A.DESIGN_END, A.HOLDOUT_END)):
            st = A.stats(daily, btc_ret, lo, hi, label)
            row[wl] = st
            line += "  %11.1f%% (%5.2f)" % (st["alpha_ann_pct"] if st else 0,
                                            st["t"] if st else 0)
        print(line)
        out[label] = row

    json.dump(out, io.open(os.path.join(ROOT, "h5_results.json"), "w", encoding="utf-8"),
              indent=1, sort_keys=True, default=str)
    print("\nwrote h5_results.json")


if __name__ == "__main__":
    main()
