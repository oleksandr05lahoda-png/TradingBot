# -*- coding: utf-8 -*-
"""
How much slippage kills H4?

shots.py found the owner's sign: buying fresh jumps pays more on bear days than bull
days, best at jump>=20% held 2 days. But it charged 10bp round trip - a number measured
on BTC/ETH. The trade being modelled is buying a coin that just closed +20% on triple
volume, which is the widest-spread moment that coin will have all week.

This sweeps the cost and reports where the edge dies, plus the trade count that makes
the cost bite. Everything else is identical to shots.py.
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


def main():
    closes, qvols, funding = A.load()
    all_days = A.series_days(closes)
    rets = A.build_returns(closes)
    btc_ret = rets["BTCUSDT"]
    btc30 = R.btc_trailing(closes, all_days, 30)

    cells = [(0.20, 2), (0.15, 2), (0.10, 2)]
    costs = [0.0005, 0.0015, 0.0025, 0.0050]      # per side: 10 / 30 / 50 / 100 bp round trip

    out = {}
    print("cost sweep - alpha%/y (t) on BEAR days only, both windows\n")
    print("%-18s %-14s %18s %18s" % ("rule", "round-trip", "design", "holdout"))
    for jump, hold in cells:
        for c in costs:
            S.COST_ONE_WAY = c
            daily, trace = S.simulate_shots(closes, rets, qvols, funding, all_days, btc30,
                                            jump, 3.0, hold)
            sd = S.split_stats(daily, trace, btc_ret, True, "2000-01-01", A.DESIGN_END, "d")
            sh = S.split_stats(daily, trace, btc_ret, True, A.DESIGN_END, A.HOLDOUT_END, "h")
            name = "jump>=%.0f%% hold%dd" % (jump * 100, hold)
            print("%-18s %-14s %11.1f%% (%4.2f) %11.1f%% (%4.2f)"
                  % (name, "%.0f bp" % (c * 2 * 10000),
                     sd["alpha_ann_pct"] if sd else 0, sd["t"] if sd else 0,
                     sh["alpha_ann_pct"] if sh else 0, sh["t"] if sh else 0))
            out["%s@%.0fbp" % (name, c * 2 * 10000)] = {
                "design": sd, "holdout": sh,
                "entries": sum(f for _, b, _, f in trace if b),
            }
        print("")

    # How often does the book actually turn over? That is what multiplies the cost.
    S.COST_ONE_WAY = 0.0005
    daily, trace = S.simulate_shots(closes, rets, qvols, funding, all_days, btc30, 0.20, 3.0, 2)
    bear_days = [t for t in trace if t[1]]
    print("at jump>=20%% hold=2d: %d bear days, mean book %.2f, mean fresh signals/day %.2f"
          % (len(bear_days),
             sum(n for _, _, n, _ in bear_days) / max(1, len(bear_days)),
             sum(f for _, _, _, f in bear_days) / max(1, len(bear_days))))

    json.dump(out, io.open(os.path.join(ROOT, "shots_cost_results.json"), "w", encoding="utf-8"),
              indent=1, sort_keys=True, default=str)
    print("\nwrote shots_cost_results.json")


if __name__ == "__main__":
    main()
