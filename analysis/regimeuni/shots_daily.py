# -*- coding: utf-8 -*-
"""
The owner's regime, not mine.

shots.py flagged BEAR from BTC's 30-day return. The owner trades 4h and says that is
the wrong clock entirely: what matters is what BTC did TODAY. If the top-cap names are
being dragged down right now, switch to the unknown names for that day - and switch
back the moment it stops.

So the bear flag here is BTC's own 1-day close-to-close return against a threshold,
swept from "any red day" to "-3% day". The 30-day version is kept as the control.

Also reports the numbers at the owner's real sizing: a fixed $6 notional per name on a
$140 account rather than "split the whole account across whatever fired". Scaling does
not change the t-stat - it changes what he actually sees on the balance.
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
import shots as S              # noqa: E402

ACCOUNT = 140.0
FIXED_NOTIONAL = 6.0           # what the owner said: 5-6 dollars a name


def bear_flag(btc_ret_1d, threshold):
    """simulate_shots calls it BEAR when the value is < 0, so shift by the threshold."""
    return {d: (v - threshold) for d, v in btc_ret_1d.items()}


def main():
    closes, qvols, funding = A.load()
    all_days = A.series_days(closes)
    rets = A.build_returns(closes)
    btc_ret = rets["BTCUSDT"]
    ret1 = R.btc_trailing(closes, all_days, 1)
    ret30 = R.btc_trailing(closes, all_days, 30)

    flags = [
        ("BTC 30d < 0   (my old flag)", ret30),
        ("BTC today < 0     (any red)", bear_flag(ret1, 0.0)),
        ("BTC today < -1%", bear_flag(ret1, -0.01)),
        ("BTC today < -2%", bear_flag(ret1, -0.02)),
        ("BTC today < -3%", bear_flag(ret1, -0.03)),
    ]

    print("rule: buy any coin that closed +20%% on 3x volume, hold 2 days, whole market")
    print("cost: 50bp round trip (5x my base estimate, to stay honest)\n")
    print("%-30s %6s %19s %19s" % ("bear flag", "days", "BEAR design", "BEAR holdout"))

    S.COST_ONE_WAY = 0.0025      # 50bp round trip
    out = {}
    for label, flag in flags:
        daily, trace = S.simulate_shots(closes, rets, qvols, funding, all_days, flag,
                                        0.20, 3.0, 2)
        nbear = sum(1 for _, b, _, _ in trace if b)
        sd = S.split_stats(daily, trace, btc_ret, True, "2000-01-01", A.DESIGN_END, "d")
        sh = S.split_stats(daily, trace, btc_ret, True, A.DESIGN_END, A.HOLDOUT_END, "h")
        print("%-30s %6d %11.1f%% (%5.2f) %11.1f%% (%5.2f)"
              % (label, nbear,
                 sd["alpha_ann_pct"] if sd else 0, sd["t"] if sd else 0,
                 sh["alpha_ann_pct"] if sh else 0, sh["t"] if sh else 0))
        out[label] = {"bear_days": nbear, "design": sd, "holdout": sh,
                      "signals_bear": sum(f for _, b, _, f in trace if b)}

    # What the owner would actually see, at his sizing, on the best daily flag.
    print("\n--- at the owner's real sizing (%.0f USD a name on a %.0f USD account) ---"
          % (FIXED_NOTIONAL, ACCOUNT))
    scale = FIXED_NOTIONAL / ACCOUNT          # one name = 4.3%% of the account, rest in cash
    for label, _ in flags:
        r = out[label]
        for w in ("design", "holdout"):
            st = r[w]
            if not st:
                continue
            usd = st["net_ann_pct"] / 100.0 * scale * ACCOUNT
            r["%s_usd_per_year_at_fixed_size" % w] = usd
        print("%-30s design %+7.2f USD/yr   holdout %+7.2f USD/yr   (t unchanged)"
              % (label, r.get("design_usd_per_year_at_fixed_size", 0.0),
                 r.get("holdout_usd_per_year_at_fixed_size", 0.0)))

    json.dump(out, io.open(os.path.join(ROOT, "shots_daily_results.json"), "w",
                           encoding="utf-8"), indent=1, sort_keys=True, default=str)
    print("\nwrote shots_daily_results.json")


if __name__ == "__main__":
    main()
