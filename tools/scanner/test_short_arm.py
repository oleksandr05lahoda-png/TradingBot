# -*- coding: utf-8 -*-
"""
Offline checks for the 22.09 bear arm (no network, no keys).

  SHORT_ARMED          arms it; DEFAULT OFF, so an unarmed scanner must behave exactly as before
  SHORT_LOW_DAYS=5     short the breakdown of the 5-day low
  SHORT_BTC_SMA_DAYS=50  only while BTC trades below its 50-day average

What this pins, and why each one is here:
  1. btc_below_sma reads the closes btc_regime() already fetched, and says None (not False) when
     the series is too short - unreadable must keep the arm OUT, not let it in
  2. short_gate needs BOTH halves. Measured 22.09: without the BTC half the arm loses 37% of its
     equity in bull phases; without the breakdown it has no trigger at all
  3. short_gate reads COMPLETED bars only. Today's partial bar already contains the live price,
     so including its low would make "below the 5-day low" nearly unreachable - the arm would
     silently never fire
  4. evaluate() exposes exactly those 20 completed lows
  5. holds() - THE LANDMINE. `to_close = held - hold_ok`, and the long rule holds a coin while it
     has NOT fallen. Run a short through that branch and the scanner writes a CLOSE on it exactly
     when the coin falls, i.e. when the short is winning. The side comes from the exchange, so
     this must hold for a position carried across a restart too
  6. read_env forwards the three new keys (the MAX_CORR trap: a key missing from that tuple is
     silently ignored inside the container)

Run: py -3 tools/scanner/test_short_arm.py
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import autoscan as A  # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name,
                         (" - " + detail) if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def bars(n, low_at=None, low=50.0, partial_low=None):
    """n daily klines: close 100, high 105, low 95. One completed bar's low can be pushed down,
    and the LAST bar (today's partial) can be given a low of its own."""
    now = int(time.time() * 1000)
    out = []
    for i in range(n):
        t = now - (n - 1 - i) * 86_400_000
        lo = 95.0
        if low_at is not None and i == low_at:
            lo = low
        if partial_low is not None and i == n - 1:
            lo = partial_low
        out.append([t, "100", "105", str(lo), "100", "1000",
                    t + 86_400_000 - 1, "1000000", 100, "500", "500000", "0"])
    return out


def metrics(price, lows):
    return {"price": price, "lows_done": list(lows), "ret": 0.0, "dip": False}


def main():
    print("22.09 bear arm - offline checks\n")
    real_get = A.get
    try:
        # --- 1. the BTC half ------------------------------------------------------------------
        below = [100.0] * 50 + [90.0]          # last close under the 50d mean
        above = [100.0] * 50 + [110.0]
        check("BTC below its 50d average -> True", A.btc_below_sma(below, 50) is True)
        check("BTC above its 50d average -> False", A.btc_below_sma(above, 50) is False)
        check("too short a series -> None (unreadable is not a bear signal)",
              A.btc_below_sma([100.0] * 10, 50) is None)
        check("empty series -> None", A.btc_below_sma([], 50) is None)
        check("a None verdict never arms the gate",
              A.short_gate(metrics(80.0, [95.0] * 20), 5, A.btc_below_sma([], 50)) is False)

        # --- 2. both halves are required ------------------------------------------------------
        broken = metrics(90.0, [95.0] * 15 + [95.0] * 5)      # price 90 < the 5-day low of 95
        intact = metrics(99.0, [95.0] * 20)                   # price 99 > the 5-day low of 95
        check("breakdown while BTC is below its average -> short",
              A.short_gate(broken, 5, True) is True)
        check("breakdown while BTC is ABOVE its average -> no short (the bull-phase bleed)",
              A.short_gate(broken, 5, False) is False)
        check("no breakdown while BTC is below -> no short",
              A.short_gate(intact, 5, True) is False)
        # An old low of 70 sits outside the 5-day window. Price 90 breaks the window's low (95)
        # but is still above that old 70: code that took the minimum of ALL lows would say "no
        # breakdown" here, so this is what separates the two readings.
        check("the window's low is used, not the minimum of the whole history",
              A.short_gate(metrics(90.0, [70.0] + [95.0] * 19), 5, True) is True,
              "an out-of-window low leaked into the reference")
        check("...and a price above the window's low is still no breakdown",
              A.short_gate(metrics(96.0, [70.0] + [95.0] * 19), 5, True) is False)
        check("too few completed lows -> no short", A.short_gate(metrics(10.0, [95.0] * 3), 5, True) is False)
        check("a non-positive price never trades", A.short_gate(metrics(0.0, [95.0] * 20), 5, True) is False)

        # --- 3/4. completed bars only ---------------------------------------------------------
        # Today's partial bar dives to 10; the completed 5-day low stays 95. A price of 94 is a
        # real breakdown; a price of 96 is not, and must stay untriggered even though the partial
        # bar's low is far below it.
        A.get = lambda path, params, **kw: bars(91, partial_low=10.0)
        m = A.evaluate("XUSDT", 30, 0.10, 96.0)
        check("evaluate exposes 20 completed lows", m is not None and len(m["lows_done"]) == 20,
              "got %r" % (m and len(m.get("lows_done") or [])))
        check("today's partial low is NOT among them",
              m is not None and min(m["lows_done"]) == 95.0,
              "got min %r - the partial bar leaked in" % (m and min(m.get("lows_done") or [0])))
        check("price above the completed low -> no short", A.short_gate(m, 5, True) is False)
        m2 = A.evaluate("XUSDT", 30, 0.10, 94.0)
        check("price below the completed low -> short", A.short_gate(m2, 5, True) is True)

        # --- 5. the landmine: the hold rule must follow the SIDE -------------------------------
        band = 0.02
        falling = {"ret": -0.35, "dip": False}       # the coin is dropping
        rallying = {"ret": +0.35, "dip": False}
        flat = {"ret": 0.0, "dip": False}
        check("a SHORT holds while its coin falls (this is it winning)",
              A.holds("SHORT", falling, band) is True)
        # A SHORT has NO signal exit: the arm was measured with the stop, the take and the 48h cap
        # only. Its entry reads a 5-day breakdown and never the 30-day return this band is built
        # from, so a short can be born already outside the band - judging it there closed winners
        # at the 24h minimum hold, half the measured life (22.09 review).
        check("a SHORT is NOT released by the 30d band - that is not its exit",
              A.holds("SHORT", rallying, band) is True)
        check("...nor by any 30d level at all", all(
            A.holds("SHORT", {"ret": r, "dip": d}, band) is True
            for r in (-5.0, -0.35, 0.0, 0.019, 0.021, 0.35, 5.0) for d in (True, False)))
        check("a LONG is released when its coin falls through the band",
              A.holds("LONG", falling, band) is False)
        check("a LONG holds while its coin rises", A.holds("LONG", rallying, band) is True)
        check("SAME falling coin: short holds, long does not (the whole point)",
              A.holds("SHORT", falling, band) is True and A.holds("LONG", falling, band) is False)
        check("a dip still holds a long inside the band",
              A.holds("LONG", {"ret": -0.35, "dip": True}, band) is True)
        check("an unheld symbol (side None) keeps the OLD long behaviour",
              A.holds(None, falling, band) is False and A.holds(None, flat, band) is True)
        check("the long branch is bit-for-bit the old rule on every input",
              all(A.holds(side, {"ret": r, "dip": d}, band) == (r > -band or d)
                  for side in (None, "LONG", "", "long")
                  for r in (-5.0, -0.021, -0.019, 0.0, 0.35, 5.0) for d in (True, False)))

        # --- 6. the keys reach the container --------------------------------------------------
        os.environ["SHORT_ARMED"] = "on"
        os.environ["SHORT_LOW_DAYS"] = "5"
        os.environ["SHORT_BTC_SMA_DAYS"] = "50"
        env = A.read_env(os.path.join(HERE, "__no_such_repo__"))
        for k in ("SHORT_ARMED", "SHORT_LOW_DAYS", "SHORT_BTC_SMA_DAYS"):
            check("read_env forwards %s" % k, env.get(k) is not None, "missing")
        check("SHORT_ARMED reads as armed only for on/1/true/yes",
              env.get("SHORT_ARMED", "").strip().lower() in ("on", "1", "true", "yes"))
        for k in ("SHORT_ARMED", "SHORT_LOW_DAYS", "SHORT_BTC_SMA_DAYS"):
            os.environ.pop(k, None)
        env = A.read_env(os.path.join(HERE, "__no_such_repo__"))
        check("unset SHORT_ARMED is OFF (the default the owner gets)",
              env.get("SHORT_ARMED", "").strip().lower() not in ("on", "1", "true", "yes"))
    finally:
        A.get = real_get

    print("\n%s" % ("ALL CHECKS PASSED" if not FAILED else "FAILED: " + ", ".join(FAILED)))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
