# -*- coding: utf-8 -*-
"""
H4 - the owner's ACTUAL idea, which run.py did not test.

His observation: while the market falls, some coin puts in +30% over four 4h candles.
So in a bear market, stop tracking BTC's crowd and hunt the coins that are shooting
RIGHT NOW, wherever they are in the market - not the slow "up 30d, near its 20d high"
rule, which by construction finds nothing while everything falls.

Test: on each day, across the WHOLE liquid universe (no top-100 cut), buy every coin
that just closed up >= JUMP% on volume >= VOL_X its 20d mean; hold HOLD days; compare
BEAR days against BULL days. This is the daily-bar version of "buy what is exploding".

What this can and cannot settle is written in the report at the bottom.
Point-in-time throughout: the signal uses day D's close, the position earns D+1 onward.
ASCII stdout only.
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

MAX_SLOTS = 15
COST_ONE_WAY = 0.0005          # 5bp per side == 10bp round trip (audit fix)
LIQ_FLOOR = 5e6


def simulate_shots(closes, rets, qvols, funding, all_days, btc30,
                   jump, vol_x, hold, start="2019-11-01", end="2026-08-14"):
    """Buy every fresh jump; equal weight; hold `hold` days. Returns (daily, trace)."""
    # per-symbol day index for O(1) prior-window access
    sdays = {s: sorted(c) for s, c in closes.items()}
    sidx = {s: {d: i for i, d in enumerate(ds)} for s, ds in sdays.items()}

    book = []                  # list of dicts: {"sym", "until_i", "w"}
    daily, trace = [], []
    prev_w = {}

    for i, d in enumerate(all_days):
        if d < start or d >= end or i + 1 >= len(all_days):
            continue
        nxt = all_days[i + 1]
        bear = btc30.get(d) is not None and btc30[d] < 0

        # --- expire ---
        book = [p for p in book if p["until_i"] > i]

        # --- find today's jumps, anywhere in the liquid universe ---
        room = MAX_SLOTS - len(book)
        fresh = []
        if room > 0:
            holding = {p["sym"] for p in book}
            for s, ds in sdays.items():
                if s == "BTCUSDT" or s in holding:
                    continue
                j = sidx[s].get(d)
                if j is None or j < 40:
                    continue
                c, q = closes[s], qvols[s]
                c0, c1 = c[ds[j - 1]], c[d]
                if c0 <= 0 or c1 <= 0:
                    continue
                day_ret = c1 / c0 - 1.0
                if day_ret < jump:
                    continue
                prior_q = [q[ds[k]] for k in range(j - 20, j)]
                mean_q = sum(prior_q) / len(prior_q) if prior_q else 0.0
                if mean_q <= 0 or q[d] < vol_x * mean_q:
                    continue
                liq = A.median([q[ds[k]] for k in range(j - 30, j)])
                if liq < LIQ_FLOOR:
                    continue
                fresh.append((day_ret, s))
            fresh.sort(reverse=True)          # biggest mover first
            for _, s in fresh[:room]:
                book.append({"sym": s, "until_i": i + hold, "w": 0.0})

        trace.append((nxt, bear, len(book), len(fresh)))
        if not book:
            daily.append((nxt, 0.0, 0.0))
            prev_w = {}
            continue

        w = 1.0 / len(book)
        new_w = {p["sym"]: w for p in book}
        turnover = sum(abs(new_w.get(s, 0.0) - prev_w.get(s, 0.0))
                       for s in set(new_w) | set(prev_w))
        gross, fund = 0.0, 0.0
        for s in new_w:
            r = rets.get(s, {}).get(nxt)
            if r is not None:
                gross += w * r
            fund -= w * funding.get(s, {}).get(nxt, 0.0)
        daily.append((nxt, gross + fund - turnover * COST_ONE_WAY, gross))
        prev_w = new_w
    return daily, trace


def split_stats(daily, trace, btc_ret, want_bear, lo, hi, label):
    bear_of = {d: b for d, b, _, _ in trace}
    sub = [(d, n, g) for d, n, g in daily if bear_of.get(d) == want_bear]
    return A.stats(sub, btc_ret, lo, hi, label)


def main():
    closes, qvols, funding = A.load()
    all_days = A.series_days(closes)
    rets = A.build_returns(closes)
    btc_ret = rets["BTCUSDT"]
    btc30 = R.btc_trailing(closes, all_days, 30)
    print("universe=%d symbols, %d days, %s -> %s"
          % (len(closes), len(all_days), all_days[0], all_days[-1]))

    grid = []
    for jump in (0.10, 0.15, 0.20, 0.30):
        for hold in (1, 2):
            grid.append((jump, 3.0, hold))

    out = {}
    print("\n%-22s %-8s %8s %8s %8s %8s" % ("rule", "window", "BEARa", "BEARt", "BULLa", "BULLt"))
    for jump, vol_x, hold in grid:
        name = "jump>=%.0f%% vol>=%.0fx hold=%dd" % (jump * 100, vol_x, hold)
        daily, trace = simulate_shots(closes, rets, qvols, funding, all_days, btc30,
                                      jump, vol_x, hold)
        sig_bear = sum(f for _, b, _, f in trace if b)
        sig_bull = sum(f for _, b, _, f in trace if not b)
        row = {"signals_bear": sig_bear, "signals_bull": sig_bull}
        for wl, lo, hi in (("design", "2000-01-01", A.DESIGN_END),
                           ("holdout", A.DESIGN_END, A.HOLDOUT_END)):
            sb = split_stats(daily, trace, btc_ret, True, lo, hi, name)
            su = split_stats(daily, trace, btc_ret, False, lo, hi, name)
            row[wl] = {"bear": sb, "bull": su}
            print("%-22s %-8s %7.1f%% %8.2f %7.1f%% %8.2f"
                  % (name if wl == "design" else "", wl,
                     sb["alpha_ann_pct"] if sb else 0, sb["t"] if sb else 0,
                     su["alpha_ann_pct"] if su else 0, su["t"] if su else 0))
        print("%-22s signals: %d on bear days, %d on bull days"
              % ("", sig_bear, sig_bull))
        out[name] = row

    json.dump(out, io.open(os.path.join(ROOT, "shots_results.json"), "w", encoding="utf-8"),
              indent=1, sort_keys=True)
    print("\nwrote shots_results.json")


if __name__ == "__main__":
    main()
