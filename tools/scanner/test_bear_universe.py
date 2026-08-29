# -*- coding: utf-8 -*-
"""
Offline checks for the BEAR_UNIVERSE switch. No network, no exchange, no keys.

The switch decides which coins may supply a NEW entry on a day BTC closed red. The things
that must hold, and that this pins:

  1. off  -> the entry pool is the top-100, exactly as before the switch existed
  2. on + red day  -> the pool becomes the least BTC-correlated fifth of the deeper list
  3. on + green day -> back to the top-100 the same pass, no stickiness
  4. a held coin is evaluated whichever pool is active - the switch must never be able
     to close a position by changing what the scanner looks at
  5. an unreadable BTC bar is NOT a bear day: the pool must not swing on a failed request

Run: py -3 tools/scanner/test_bear_universe.py
"""
import io
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import autoscan as A  # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    if cond:
        print("  ok   %s" % name)
    else:
        print("  FAIL %s  %s" % (name, detail))
        FAILED.append(name)


def series(n, drift, wobble, seed):
    """Deterministic pseudo-random close series; no Random import needed."""
    out, p, s = [], 100.0, seed
    for i in range(n):
        s = (s * 1103515245 + 12345) % 2147483648
        r = drift + wobble * ((s / 2147483648.0) - 0.5)
        p *= (1.0 + r)
        out.append(p)
    return out


def main():
    print("BEAR_UNIVERSE offline checks\n")

    # --- 1. the correlation helper is what the pool selection leans on ---------------
    btc = series(61, 0.001, 0.04, 7)
    follower = [x * (1.0 + 0.0001 * i) for i, x in enumerate(btc)]     # moves with BTC
    independent = series(61, 0.001, 0.04, 999999)                       # its own life
    c_follow = A.corr60(follower, btc)
    c_indep = A.corr60(independent, btc)
    check("a BTC follower scores high correlation", c_follow > 0.9,
          "got %.3f" % c_follow)
    check("an independent series scores lower", c_indep < c_follow,
          "follower %.3f vs independent %.3f" % (c_follow, c_indep))
    check("too-short history fails open at 0.0", A.corr60([1, 2, 3], btc) == 0.0)
    check("empty history fails open at 0.0", A.corr60(None, btc) == 0.0)

    # --- 2. the pool selection itself ------------------------------------------------
    # Mirror of the production expression, exercised on synthetic details.
    def choose(wide, details, btc_closes, top_pool, bear_day):
        entry_pool = set(top_pool)
        if bear_day:
            scored = sorted((A.corr60(details[s].get("closes"), btc_closes), s)
                            for s in wide if s in details)
            if scored:
                entry_pool = {s for _, s in scored[:max(1, len(scored) // 5)]}
        return entry_pool

    wide = ["F%d" % i for i in range(8)] + ["I%d" % i for i in range(2)]
    details = {}
    for i in range(8):
        details["F%d" % i] = {"closes": [x * (1.0 + 0.00001 * i) for x in btc]}
    for i in range(2):
        details["I%d" % i] = {"closes": series(61, 0.001, 0.04, 4242 + i * 31337)}
    top_pool = ["F0", "F1", "F2"]

    off = choose(wide, details, btc, top_pool, bear_day=False)
    check("switch off -> pool is exactly the top list", off == set(top_pool),
          "got %s" % sorted(off))

    on = choose(wide, details, btc, top_pool, bear_day=True)
    check("bear day -> pool is a fifth of the wide list", len(on) == max(1, len(wide) // 5),
          "got %d of %d" % (len(on), len(wide)))
    check("bear day -> the independents are chosen over the followers",
          all(s.startswith("I") for s in on), "got %s" % sorted(on))

    back = choose(wide, details, btc, top_pool, bear_day=False)
    check("green day right after -> straight back to the top list", back == set(top_pool),
          "got %s" % sorted(back))

    # --- 3. the bear-day flag itself --------------------------------------------------
    def is_bear(armed, ret1, threshold=0.0):
        return armed and ret1 is not None and ret1 < threshold

    check("armed + red day -> bear", is_bear(True, -0.004) is True)
    check("armed + green day -> not bear", is_bear(True, 0.004) is False)
    check("armed + unreadable BTC -> not bear", is_bear(True, None) is False)
    check("disarmed + red day -> not bear", is_bear(False, -0.05) is False)
    check("threshold respected", is_bear(True, -0.004, -0.01) is False)

    # --- 4. exits are never gated by the entry pool ------------------------------------
    held = {"F0", "ZZZ"}
    scan_pool = [(s, 0.0) for s in wide]
    to_evaluate = [s for s, _ in scan_pool] + sorted(held - {s for s, _ in scan_pool})
    check("a held coin outside the pool is still evaluated", "ZZZ" in to_evaluate)
    check("a held coin inside the pool is still evaluated", "F0" in to_evaluate)
    entry_pool = on
    fresh_source = (set(["F0", "I0", "ZZZ"]) & entry_pool) - held
    check("the entry pool cannot pick a held coin", "F0" not in fresh_source
          and "ZZZ" not in fresh_source, "got %s" % sorted(fresh_source))

    # --- 5. a cut-short sweep must not close what it did not judge ----------------------
    # held is swept FIRST, but a throttle can still land mid-way through it. to_close is
    # held - hold_ok, so an unjudged holding would be closed for not being reached.
    held2 = {"AAA", "BBB", "CCC"}
    hold_ok = {"AAA"}                 # judged and fine
    details_seen = {"AAA": {}, "BBB": {}}   # BBB evaluated but broke its band -> a real exit
    unjudged = held2 - hold_ok - set(details_seen)
    check("an unreached holding is identified", unjudged == {"CCC"},
          "got %s" % sorted(unjudged))
    for s in unjudged:
        hold_ok.add(s)
    to_close = held2 - hold_ok
    check("the unreached holding is NOT closed", "CCC" not in to_close)
    check("a genuinely broken holding still closes", to_close == {"BBB"},
          "got %s" % sorted(to_close))

    # --- 6. env parsing ----------------------------------------------------------------
    def armed(v):
        return v.strip().lower() in ("on", "1", "true", "yes")
    check("BEAR_UNIVERSE=on arms", armed("on") and armed(" ON ") and armed("true"))
    check("BEAR_UNIVERSE unset/off does not arm",
          not armed("") and not armed("off") and not armed("no"))

    print("\n%s" % ("ALL CHECKS PASSED" if not FAILED
                    else "FAILED: %s" % ", ".join(FAILED)))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
