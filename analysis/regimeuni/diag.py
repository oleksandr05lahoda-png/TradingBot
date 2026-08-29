# -*- coding: utf-8 -*-
"""
Where does H3's edge actually come from?

H3 keeps the same entry selector (30d return > +2%, near the 20d high, volume x1.5)
and in BEAR only swaps the pool for the lowest-correlation quintile of the top-300.
But in a bear market almost nothing passes a "trend up, near its high" test. If the
BEAR book is mostly EMPTY, then H3 is not "finding independent coins" at all - it is
sitting in cash while BTC's 30d return is negative, which is the already-measured
REGIME_GATE=cash, and the low-correlation selection is decoration.

This splits both runs by regime and counts what is actually held. ASCII stdout only.
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


def simulate_traced(feats, rets, funding, all_days, corr, btc_ret_w,
                    universe_top=100, regime_universe=False,
                    start="2019-11-01", end="2026-08-14"):
    """R.simulate, but also returns per-day (bear_flag, book_size)."""
    idx = {d: i for i, d in enumerate(all_days)}
    held, cooldown, daily, trace = {}, {}, [], []
    btc30 = btc_ret_w[30]

    for i, d in enumerate(all_days):
        if d < start or d >= end or i + 1 >= len(all_days):
            continue
        nxt = all_days[i + 1]
        bear = btc30.get(d) is not None and btc30[d] < 0

        pool = []
        for s, f in feats.items():
            if s == "BTCUSDT":
                continue
            row = f.get(d)
            if row is None or row["liq"] < R.LIQ_FLOOR or row["atr"] is None:
                continue
            pool.append((row["liq"], s))
        pool.sort(reverse=True)
        top = 300 if (bear and regime_universe) else universe_top
        ranked = [s for _, s in pool[:top]]
        if bear and regime_universe:
            scored = [(corr.get(s, {}).get(d), s) for s in ranked]
            scored = [(c, s) for c, s in scored if c is not None]
            scored.sort()
            ranked = [s for _, s in scored[:max(1, len(scored) // 5)]]

        for s in list(held):
            row = feats[s].get(d)
            age = i - held[s]["since"]
            if age < R.MIN_HOLD_DAYS:
                continue
            if row is None or age >= R.MAX_HOLD_DAYS or row["ret30"] < -R.EXIT_BAND:
                del held[s]
                cooldown[s] = i + R.COOLDOWN_DAYS

        room = R.MAX_SLOTS - len(held)
        if room > 0:
            cands = []
            for s in ranked:
                if s in held or cooldown.get(s, -1) > i:
                    continue
                row = feats[s].get(d)
                if row is None or row["atr"] is None:
                    continue
                if row["price"] <= R.STOP_ATR_MULT * row["atr"]:
                    continue
                if row["ret30"] <= R.ENTRY_BAND:
                    continue
                if row["from_high"] is None or row["from_high"] > R.NEAR_HIGH:
                    continue
                if row["vol_ratio"] is None or row["vol_ratio"] < R.VOL_MULT:
                    continue
                cands.append((row["ret30"], s))
            cands.sort(reverse=True)
            for _, s in cands[:room]:
                held[s] = {"since": i, "weight": 0.0}

        trace.append((nxt, bear, len(held)))

        if not held:
            daily.append((nxt, 0.0, 0.0))
            continue
        raw = {}
        for s in held:
            row = feats[s].get(d)
            if row is None or row["atr"] is None or row["price"] <= 0:
                raw[s] = 0.0
                continue
            sf = R.STOP_ATR_MULT * row["atr"] / row["price"]
            raw[s] = (1.0 / sf) if sf > 0 else 0.0
        tot = sum(raw.values())
        if tot <= 0:
            daily.append((nxt, 0.0, 0.0))
            continue
        new_w = {s: raw[s] / tot for s in held}
        prev_w = {s: held[s]["weight"] for s in held}
        turnover = sum(abs(new_w.get(s, 0.0) - prev_w.get(s, 0.0))
                       for s in set(new_w) | set(prev_w))
        gross, fund = 0.0, 0.0
        for s, w in new_w.items():
            r = rets.get(s, {}).get(nxt)
            if r is not None:
                gross += w * r
            fund -= w * funding.get(s, {}).get(nxt, 0.0)
        daily.append((nxt, gross + fund - turnover * R.COST_ROUND_TRIP, gross))
        for s in held:
            held[s]["weight"] = new_w[s]
    return daily, trace


def split(daily, trace, btc_ret, want_bear, lo, hi, label):
    bear_of = {d: b for d, b, _ in trace}
    sub = [(d, n, g) for d, n, g in daily if bear_of.get(d) == want_bear]
    return A.stats(sub, btc_ret, lo, hi, label)


def main():
    closes, qvols, funding = A.load()
    highs, lows = {}, {}
    uni = json.load(io.open(os.path.join(os.path.dirname(ROOT), "tsmom", "cache",
                                         "universe_final.json"), encoding="utf-8"))
    for s in uni["live"] + uni["dead"]:
        kp = os.path.join(os.path.dirname(ROOT), "tsmom", "cache", "klines_%s.json" % s)
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
    btc_ret_w = {w: R.btc_trailing(closes, all_days, w) for w in (1, 30)}
    corr = R.rolling_corr_to_btc(rets, all_days)

    out = {}
    for name, kw in (("base", dict(universe_top=100, regime_universe=False)),
                     ("H3", dict(universe_top=100, regime_universe=True))):
        daily, trace = simulate_traced(feats, rets, funding, all_days, corr, btc_ret_w, **kw)
        bear_days = [t for t in trace if t[1]]
        bull_days = [t for t in trace if not t[1]]
        empty_bear = sum(1 for _, _, n in bear_days if n == 0)
        print("\n=== %s ===" % name)
        print("  BEAR days %d (%.0f%% of sample), BULL days %d"
              % (len(bear_days), 100.0 * len(bear_days) / max(1, len(trace)), len(bull_days)))
        print("  avg book size:  BEAR %.2f   BULL %.2f"
              % (sum(n for _, _, n in bear_days) / max(1, len(bear_days)),
                 sum(n for _, _, n in bull_days) / max(1, len(bull_days))))
        print("  BEAR days holding NOTHING: %d of %d (%.0f%%)"
              % (empty_bear, len(bear_days), 100.0 * empty_bear / max(1, len(bear_days))))
        row = {}
        for wl, lo, hi in (("design", "2000-01-01", A.DESIGN_END),
                           ("holdout", A.DESIGN_END, A.HOLDOUT_END)):
            for regime, flag in (("BEAR", True), ("BULL", False)):
                st = split(daily, trace, btc_ret, flag, lo, hi, "%s/%s/%s" % (name, wl, regime))
                if st:
                    print("  %-7s %-4s days=%4d net=%8.1f%%/y alpha=%8.1f%%/y t=%6.2f"
                          % (wl, regime, st["days"], st["net_ann_pct"],
                             st["alpha_ann_pct"], st["t"]))
                row["%s_%s" % (wl, regime)] = st
        out[name] = row
    json.dump(out, io.open(os.path.join(ROOT, "diag_results.json"), "w", encoding="utf-8"),
              indent=1, sort_keys=True)
    print("\nwrote diag_results.json")


if __name__ == "__main__":
    main()
