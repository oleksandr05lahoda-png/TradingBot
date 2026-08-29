# -*- coding: utf-8 -*-
"""
The third option neither of us tested: be in the market LESS during a bear, rather than
be in something else.

The owner's complaint is exact and fair: positions opened before a downturn ride it for
up to 48h while the entry rule only stops NEW trades. Everything measured so far tried
to replace a losing long with a different long. This tries the other direction.

  A  cash gate, slow    no new entries while BTC's 30d return is negative
  B  cash gate, daily   no new entries on a day BTC closed red      (the owner's clock)
  C  fast exit          max hold 24h instead of 48h while in a bear regime
  D  A + C together

Reused engine: run.py's build_features / simulate machinery, already put through a
4-lens adversarial audit for look-ahead, cost and statistics errors. Only the entry gate
and the hold cap change here.

Reports what the owner actually cares about - the worst day and the bear-day average -
alongside the lab's alpha/t. ASCII stdout only.
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


def simulate_less(feats, rets, funding, all_days, btc30, btc1,
                  gate=None, bear_fast_exit=False,
                  start="2019-11-01", end="2026-08-14"):
    """R.simulate's baseline with an entry gate and/or a shorter hold in bear."""
    idx = {d: i for i, d in enumerate(all_days)}
    held, cooldown, daily, trace = {}, {}, [], []

    for i, d in enumerate(all_days):
        if d < start or d >= end or i + 1 >= len(all_days):
            continue
        nxt = all_days[i + 1]
        slow_bear = btc30.get(d) is not None and btc30[d] < 0
        day_bear = btc1.get(d) is not None and btc1[d] < 0

        blocked = (gate == "slow" and slow_bear) or (gate == "daily" and day_bear)
        hold_cap = 1 if (bear_fast_exit and slow_bear) else R.MAX_HOLD_DAYS

        pool = []
        for s, f in feats.items():
            if s == "BTCUSDT":
                continue
            row = f.get(d)
            if row is None or row["liq"] < R.LIQ_FLOOR or row["atr"] is None:
                continue
            pool.append((row["liq"], s))
        pool.sort(reverse=True)
        ranked = [s for _, s in pool[:100]]

        for s in list(held):
            row = feats[s].get(d)
            age = i - held[s]["since"]
            if age < R.MIN_HOLD_DAYS:
                continue
            if row is None or age >= hold_cap or row["ret30"] < -R.EXIT_BAND:
                del held[s]
                cooldown[s] = i + R.COOLDOWN_DAYS

        room = R.MAX_SLOTS - len(held)
        if room > 0 and not blocked:
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

        trace.append((nxt, slow_bear, len(held)))
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
        daily.append((nxt, gross + fund - turnover * 0.0005, gross))
        for s in held:
            held[s]["weight"] = new_w[s]
    return daily, trace


def pain(daily, trace, lo, hi):
    """What the owner feels: worst day, worst 5-day stretch, and the bear-day average."""
    bear_of = {d: b for d, b, _ in trace}
    rows = [(d, n) for d, n, _ in daily if lo <= d < hi]
    if not rows:
        return None
    vals = [n for _, n in rows]
    worst = min(vals)
    worst5 = min(sum(vals[i:i + 5]) for i in range(max(1, len(vals) - 4)))
    bear = [n for d, n in rows if bear_of.get(d)]
    invested = sum(1 for _, _, k in trace if k > 0) / max(1, len(trace))
    return {"worst_day_pct": worst * 100, "worst_5d_pct": worst5 * 100,
            "bear_day_mean_bp": (sum(bear) / len(bear) * 10000) if bear else 0.0,
            "invested_fraction": invested}


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
    btc30 = R.btc_trailing(closes, all_days, 30)
    btc1 = R.btc_trailing(closes, all_days, 1)

    variants = [
        ("base (as the bot runs now)", dict()),
        ("A cash gate, 30d flag", dict(gate="slow")),
        ("B cash gate, daily flag", dict(gate="daily")),
        ("C fast exit 24h in bear", dict(bear_fast_exit=True)),
        ("D A + C", dict(gate="slow", bear_fast_exit=True)),
    ]

    out = {}
    for label, kw in variants:
        daily, trace = simulate_less(feats, rets, funding, all_days, btc30, btc1, **kw)
        print("\n=== %s ===" % label)
        row = {}
        for wl, lo, hi in (("design", "2000-01-01", A.DESIGN_END),
                           ("holdout", A.DESIGN_END, A.HOLDOUT_END)):
            st = A.stats(daily, btc_ret, lo, hi, label)
            pn = pain(daily, trace, lo, hi)
            row[wl] = {"stats": st, "pain": pn}
            if st and pn:
                print("  %-8s net=%7.1f%%/y alpha=%7.1f%%/y t=%5.2f | worst day %6.2f%% "
                      "worst 5d %7.2f%% | bear day avg %+6.1fbp | in market %.0f%%"
                      % (wl, st["net_ann_pct"], st["alpha_ann_pct"], st["t"],
                         pn["worst_day_pct"], pn["worst_5d_pct"],
                         pn["bear_day_mean_bp"], pn["invested_fraction"] * 100))
        out[label] = row
    json.dump(out, io.open(os.path.join(ROOT, "less_results.json"), "w", encoding="utf-8"),
              indent=1, sort_keys=True, default=str)
    print("\nwrote less_results.json")


if __name__ == "__main__":
    main()
