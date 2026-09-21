# -*- coding: utf-8 -*-
"""
Offline checks for the two 21.09 entry gates (no network, no keys).

  ENTRY_NEAR_HIGH90  - a trend entry must sit within 10% of the 90d high (no overhang)
  ENTRY_VOL_MAX      - and not on a blow-off day (volume > 4x the 20d mean)

What must hold, and what this pins:
  1. evaluate() returns from_high90 from 89 completed days + today's partial bar (same convention
     as hi20), and None when the coin has fewer than 91 bars - a young coin cannot pass the gate
  2. evaluate() asks for 91 bars (one request, weight 1) and still keeps the OLD eligibility floor
  3. ret20 is exposed for the shadow rel-strength line
  4. read_env forwards the two new keys from the real environment (the MAX_CORR trap: a key
     missing from that tuple is silently ignored inside the container)
  5. the gate logic itself, replayed exactly as the entry block does it: trend gated, dip never

Run: py -3 tools/scanner/test_entry_gates.py
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import autoscan as A  # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name, (" - " + detail) if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def bars(n, high_peak_at=None, peak=200.0):
    """n daily klines, close 100, high 105; one bar's high lifted to ``peak`` when asked."""
    now = int(time.time() * 1000)
    out = []
    for i in range(n):
        t = now - (n - 1 - i) * 86_400_000
        hi = peak if (high_peak_at is not None and i == high_peak_at) else 105.0
        out.append([t, "100", str(hi), "95", "100", "1000", t + 86_400_000 - 1, "1000000", 100, "500", "500000", "0"])
    return out


def with_bars(b):
    A.get = lambda path, params, **kw: b  # the only network call evaluate() makes


def main():
    print("21.09 entry gates - offline checks\n")
    real_get = A.get
    try:
        # --- 1/2. from_high90 and the request size ------------------------------------------
        asked = {}
        def spy(path, params, **kw):
            asked.update(params)
            return bars(91, high_peak_at=5)           # the 90d high is far back, well above price
        A.get = spy
        m = A.evaluate("XUSDT", 30, 0.10, 100.0)
        check("evaluate asks for 91 bars", asked.get("limit") == 91, "limit=%r" % asked.get("limit"))
        check("from_high90 sees the 90d peak (price 100 vs high 200 -> 0.50)",
              m is not None and abs(m["from_high90"] - 0.5) < 1e-9, "got %r" % (m and m.get("from_high90")))
        check("from_high (20d) is unaffected by the old peak (105 -> ~0.048)",
              m is not None and abs(m["from_high"] - (1 - 100 / 105.0)) < 1e-9)

        with_bars(bars(91, high_peak_at=0))            # the peak is the OLDEST bar of 91 -> outside the 90-bar window
        m = A.evaluate("XUSDT", 30, 0.10, 100.0)
        check("the 90d window excludes bar 91 (peak dropped -> from_high90 ~0.048)",
              m is not None and abs(m["from_high90"] - (1 - 100 / 105.0)) < 1e-9, "got %r" % (m and m.get("from_high90")))

        with_bars(bars(61))                             # young coin: eligible as before, but no 90d high
        m = A.evaluate("YUSDT", 30, 0.10, 100.0)
        check("61 bars still evaluate (old eligibility floor kept)", m is not None)
        check("...but from_high90 is None -> the gate cannot pass it", m is not None and m["from_high90"] is None)

        # --- 3. ret20 for the shadow line -----------------------------------------------------
        b = bars(91); b[-21][4] = "80"                  # close 20 bars back = 80 -> ret20 = +25%
        with_bars(b)
        m = A.evaluate("ZUSDT", 30, 0.10, 100.0)
        check("ret20 = price / close[-21] - 1", m is not None and abs(m["ret20"] - 0.25) < 1e-9, "got %r" % (m and m.get("ret20")))

        # --- 4. read_env forwards the new keys ------------------------------------------------
        os.environ["ENTRY_NEAR_HIGH90"] = "0.10"
        os.environ["ENTRY_VOL_MAX"] = "4.0"
        env = A.read_env(os.path.join(HERE, "__no_such_repo__"))
        check("ENTRY_NEAR_HIGH90 reaches env", env.get("ENTRY_NEAR_HIGH90") == "0.10")
        check("ENTRY_VOL_MAX reaches env", env.get("ENTRY_VOL_MAX") == "4.0")

        # --- 5. the gate logic, replayed as the entry block does it -------------------------------
        def gate(m, trig, nh90=0.10, vmax=4.0):
            # the REAL function main() calls, with the old two gates disarmed
            return A.entry_gates(m, trig, None, None, nh90, vmax)[0]
        def cut_by(m, trig, nh90=0.10, vmax=4.0):
            return A.entry_gates(m, trig, None, None, nh90, vmax)[1]
        check("trend 5% below 90d high, vol 2x -> passes", gate({"from_high90": 0.05, "vol_ratio": 2.0}, "trend"))
        check("trend 12% below 90d high -> cut (overhang)", not gate({"from_high90": 0.12, "vol_ratio": 2.0}, "trend"))
        check("trend vol 4.5x -> cut (blow-off)", not gate({"from_high90": 0.05, "vol_ratio": 4.5}, "trend"))
        check("trend young coin (None) -> cut", not gate({"from_high90": None, "vol_ratio": 2.0}, "trend"))
        check("DIP 30% below 90d high -> never gated", gate({"from_high90": 0.30, "vol_ratio": 6.0}, "dip"))
        check("both keys unset -> behaves as before", gate({"from_high90": 0.30, "vol_ratio": 6.0}, "trend", None, None))
        check("both keys unset -> nothing is reported as cut", cut_by({"from_high90": 0.30, "vol_ratio": 6.0}, "trend", None, None) is None)
        check("overhang cut is named", cut_by({"from_high90": 0.12, "vol_ratio": 2.0}, "trend") == "overhang")
        check("vol cap cut is named", cut_by({"from_high90": 0.05, "vol_ratio": 4.5}, "trend") == "volmax")
        check("old near-high gate still refuses before the new ones (no cut label)",
              A.entry_gates({"from_high": 0.08, "from_high90": 0.05, "vol_ratio": 2.0}, "trend", 0.05, 1.5, 0.10, 4.0) == (False, None))
        check("all four gates pass together",
              A.entry_gates({"from_high": 0.03, "from_high90": 0.05, "vol_ratio": 2.0}, "trend", 0.05, 1.5, 0.10, 4.0) == (True, None))
    finally:
        A.get = real_get
        os.environ.pop("ENTRY_NEAR_HIGH90", None)
        os.environ.pop("ENTRY_VOL_MAX", None)

    print("\n%s" % ("ALL CHECKS PASSED" if not FAILED else "FAILED: %s" % ", ".join(FAILED)))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
