# -*- coding: utf-8 -*-
"""
Offline checks for the bear arm (no network, no keys).

  SHORT_ARMED            arms it; DEFAULT OFF, so an unarmed scanner behaves exactly as before
  SHORT_LOW_BARS=30      short a breakdown of the lowest low of the 30 4h bars before the last one
  SHORT_BTC_SMA_DAYS=50  only while BTC trades below its 50-day average

History: the first version (eb0a13a) read DAILY bars. Measured faithfully on 22.09 it returned
-6.7% over 4.8 years and made the long book worse; the arm that was actually measured reads 4h
bars and returns +28% with the bot's shared 1.75R take. These checks pin the 4h arm.

What this pins, and why each one is here:
  1. btc_below_sma says None (not False) on a short series - unreadable keeps the arm OUT
  2. evaluate_4h decides on COMPLETED 4h bars only, like the lab: the bar still forming is never
     read, the reference is the lowest low of the N bars BEFORE the last completed one (lab
     prior_low), and a low outside that window does not count
  3. a breakdown seen too late (bar closed > SHORT_FRESH_MIN ago) is not the measured trade
  4. short_gate needs all three: BTC below its average, a breakdown, and a fresh one
  5. the atr= on a SHORT line is scaled so the bot's 2 x atr stop lands at 6 x ATR4h
  6. holds() - THE LANDMINE: a short has no signal exit; the long branch is bit-for-bit the old rule
  7. read_env forwards the keys (the MAX_CORR trap: a key missing there is silently ignored)
  8. the 4h klines are fetched only while the bear gate is open (no extra requests in a bull market)

Run: py -3 tools/scanner/test_short_arm.py
"""
import io
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import autoscan as A  # noqa: E402

FAILED = []
H4 = 4 * 3600 * 1000


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name,
                         (" - " + detail) if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def bars4h(n, last_close=100.0, lows=None, forming_low=None, closed_ago_min=30):
    """n 4h klines ending with the bar still forming. Every bar: open 100, high 102, low 98,
    close 100, except: `lows` {index: low} overrides lows of COMPLETED bars (index from the start),
    the last COMPLETED bar closes at `last_close`, and the forming bar can get its own low.
    The last completed bar closed `closed_ago_min` minutes before 'now'."""
    now = int(time.time() * 1000)
    last_done_close_time = now - closed_ago_min * 60000
    out = []
    for i in range(n):
        # bar i closes at: last_done_close_time - (n - 2 - i) * H4   (bar n-2 is the last completed)
        close_time = last_done_close_time - (n - 2 - i) * H4
        open_time = close_time - H4 + 1
        lo = 98.0
        c = 100.0
        if lows and i in lows:
            lo = lows[i]
        if i == n - 2:
            c = last_close
            lo = min(lo, last_close)
        if i == n - 1 and forming_low is not None:
            lo = forming_low
        out.append([open_time, "100", "102", str(lo), str(c), "1000", close_time,
                    "100000", 100, "500", "50000", "0"])
    return out, now


def feed(b):
    A.get = lambda path, params, **kw: b


def main():
    print("bear arm (4h) - offline checks\n")
    real_get = A.get
    try:
        # --- 1. the BTC half ------------------------------------------------------------------
        check("BTC below its 50d average -> True", A.btc_below_sma([100.0] * 50 + [90.0], 50) is True)
        check("BTC above its 50d average -> False", A.btc_below_sma([100.0] * 50 + [110.0], 50) is False)
        check("too short a series -> None (unreadable is not a bear signal)",
              A.btc_below_sma([100.0] * 10, 50) is None)

        # --- 2. completed 4h bars only, lab prior_low semantics ---------------------------------
        asked = {}
        b, now = bars4h(100, last_close=97.0)          # 97 < every prior low (98) -> breakdown
        def spy(path, params, **kw):
            asked.update(params)
            return b
        A.get = spy
        s4 = A.evaluate_4h("XUSDT", 30, now_ms=now)
        check("asks for 4h bars", asked.get("interval") == "4h", "interval=%r" % asked.get("interval"))
        check("asks for SHORT_BARS_4H of them", asked.get("limit") == A.SHORT_BARS_4H)
        check("a close under the 30-bar prior low is a breakdown", s4 is not None and s4["broke"] is True,
              "got %r" % s4)
        check("the reference is the prior low (98), not the breakdown bar's own low",
              s4 is not None and abs(s4["ref_low4h"] - 98.0) < 1e-12, "got %r" % (s4 and s4["ref_low4h"]))

        b, now = bars4h(100, last_close=99.0, forming_low=50.0)   # the FORMING bar dives to 50
        feed(b)
        s4 = A.evaluate_4h("XUSDT", 30, now_ms=now)
        check("the bar still forming is never read (its low of 50 changes nothing)",
              s4 is not None and s4["broke"] is False and abs(s4["close4h"] - 99.0) < 1e-12)

        # a very low low 40 bars back sits OUTSIDE the 30-bar window: price 97 still breaks 98
        b, now = bars4h(100, last_close=97.0, lows={100 - 2 - 40: 80.0})
        feed(b)
        s4 = A.evaluate_4h("XUSDT", 30, now_ms=now)
        check("a low outside the window does not raise the bar (code that took ALL lows says no)",
              s4 is not None and s4["broke"] is True)
        # ...and the same low INSIDE the window blocks the breakdown
        b, now = bars4h(100, last_close=97.0, lows={100 - 2 - 10: 80.0})
        feed(b)
        s4 = A.evaluate_4h("XUSDT", 30, now_ms=now)
        check("a lower low inside the window blocks it", s4 is not None and s4["broke"] is False)

        b, now = bars4h(100, last_close=97.0)
        feed(b)
        s4 = A.evaluate_4h("XUSDT", 30, now_ms=now)
        check("ATR is the scanner's own Wilder atr() on the completed bars",
              s4 is not None and abs(s4["atr4h"] - A.atr(b[:-1], A.ATR_PERIOD)) < 1e-12)

        feed(bars4h(40)[0])
        check("too few bars -> None", A.evaluate_4h("XUSDT", 30) is None)
        feed([])
        check("no data -> None", A.evaluate_4h("XUSDT", 30) is None)

        # --- 3. freshness ------------------------------------------------------------------------
        b, now = bars4h(100, last_close=97.0, closed_ago_min=30)
        feed(b)
        check("a bar that closed 30 min ago is fresh", A.evaluate_4h("XUSDT", 30, now_ms=now)["fresh"] is True)
        b, now = bars4h(100, last_close=97.0, closed_ago_min=A.SHORT_FRESH_MIN + 60)
        feed(b)
        stale = A.evaluate_4h("XUSDT", 30, now_ms=now)
        check("a bar that closed %d min ago is NOT fresh" % (A.SHORT_FRESH_MIN + 60), stale["fresh"] is False)

        # --- 4. the gate: all three halves ------------------------------------------------------
        b, now = bars4h(100, last_close=97.0)
        feed(b)
        good = A.evaluate_4h("XUSDT", 30, now_ms=now)
        check("breakdown + fresh + BTC below -> short", A.short_gate(good, True) is True)
        check("BTC ABOVE its average -> no short (the bull-phase bleed)", A.short_gate(good, False) is False)
        check("an unreadable BTC verdict (None) -> no short", A.short_gate(good, None) is False)
        check("a stale breakdown -> no short", A.short_gate(stale, True) is False)
        b, now = bars4h(100, last_close=99.0)
        feed(b)
        check("no breakdown -> no short", A.short_gate(A.evaluate_4h("XUSDT", 30, now_ms=now), True) is False)
        check("no data -> no short", A.short_gate(None, True) is False)

        # --- 5. the stop the bot will place ------------------------------------------------------
        line = A.short_line_atr(good)
        check("bot stop (STOP_ATR_MULT x atr=) lands at 6 x ATR4h",
              abs(A.STOP_ATR_MULT * line - A.SHORT_STOP_ATR4H_MULT * good["atr4h"]) < 1e-12)
        # the stop cap mirrors RiskEngine step 11 (LIQUIDATION_BUFFER), not just the take's limit
        cap2 = A.short_max_stop_frac(2)
        check("at 2x the widest short stop is ~30%% (got %.3f)" % cap2, 0.29 <= cap2 <= 0.31)
        entry, mmr = 100.0, A.SHORT_MMR_ASSUMED
        liq = entry * (1 + 1 / 2.0 - 0.0005) / (1 + mmr)          # isolated short, first bracket
        stop = entry * (1 + cap2)
        buffer = (liq - stop) / (liq - entry)
        check("a stop AT the cap leaves the bot's 30%% buffer to liquidation (%.3f)" % buffer,
              buffer >= A.SHORT_LIQ_BUFFER - 1e-9)
        check("the cap never exceeds 0.5, so the shared 1.75R take stays above zero at any leverage",
              all(A.short_max_stop_frac(L) <= 0.5 and 1.0 - 1.75 * A.short_max_stop_frac(L) > 0
                  for L in (1, 2, 3, 5, 10)))
        check("higher leverage -> tighter cap", A.short_max_stop_frac(3) < cap2 < A.short_max_stop_frac(1))

        # --- 5b. the wake is aligned to the 4h close only while the arm is armed AND its gate is open --
        P = 4 * 3600
        close = 1_790_000_000 - (1_790_000_000 % P)                # a real 4h UTC boundary
        check("OFF (or BTC above): exactly the flat interval, whatever the clock says",
              all(A.next_wait(3600, False, t - 50, t) == 3600 for t in (close + 1, close + 7000, close + P - 5)))
        check("a pass that began before the latest 4h close runs again in 30 s",
              A.next_wait(3600, True, close - 40, close + 25) == 30)
        check("a pass that began after the close sleeps no longer than the next close + 2 min",
              A.next_wait(3600, True, close + 3 * 3600 + 1800, close + 3 * 3600 + 1860) == P + 120 - (3 * 3600 + 1860))
        check("...and never longer than the interval", A.next_wait(3600, True, close + 200, close + 300) == 3600)
        check("never shorter than 30 s", A.next_wait(3600, True, close + P - 200, close + P - 150) >= 30)

        # --- 6. the landmine: the hold rule follows the SIDE --------------------------------------
        band = 0.02
        falling = {"ret": -0.35, "dip": False}
        rallying = {"ret": +0.35, "dip": False}
        flat = {"ret": 0.0, "dip": False}
        check("a SHORT holds while its coin falls", A.holds("SHORT", falling, band) is True)
        check("a SHORT has no signal exit at all", all(
            A.holds("SHORT", {"ret": r, "dip": d}, band) is True
            for r in (-5.0, -0.35, 0.0, 0.019, 0.021, 0.35, 5.0) for d in (True, False)))
        check("SAME falling coin: short holds, long does not",
              A.holds("SHORT", falling, band) is True and A.holds("LONG", falling, band) is False)
        check("an unheld symbol (side None) keeps the OLD long behaviour",
              A.holds(None, falling, band) is False and A.holds(None, flat, band) is True)
        check("the long branch is bit-for-bit the old rule on every input",
              all(A.holds(side, {"ret": r, "dip": d}, band) == (r > -band or d)
                  for side in (None, "LONG", "", "long")
                  for r in (-5.0, -0.021, -0.019, 0.0, 0.35, 5.0) for d in (True, False)))
        check("...and a dip still holds a long", A.holds("LONG", {"ret": -0.35, "dip": True}, band) is True)
        check("a rallying long holds", A.holds("LONG", rallying, band) is True)

        # --- 7. the keys reach the container ------------------------------------------------------
        os.environ["SHORT_ARMED"] = "on"
        os.environ["SHORT_LOW_BARS"] = "30"
        os.environ["SHORT_BTC_SMA_DAYS"] = "50"
        env = A.read_env(os.path.join(HERE, "__no_such_repo__"))
        for k in ("SHORT_ARMED", "SHORT_LOW_BARS", "SHORT_BTC_SMA_DAYS"):
            check("read_env forwards %s" % k, env.get(k) is not None, "missing")
        for k in ("SHORT_ARMED", "SHORT_LOW_BARS", "SHORT_BTC_SMA_DAYS"):
            os.environ.pop(k, None)
        env = A.read_env(os.path.join(HERE, "__no_such_repo__"))
        check("unset SHORT_ARMED is OFF",
              env.get("SHORT_ARMED", "").strip().lower() not in ("on", "1", "true", "yes"))

        # --- 8. no extra requests in a bull market ------------------------------------------------
        src = open(os.path.join(HERE, "autoscan.py"), encoding="utf-8").read()
        call = src.find("evaluate_4h(sym, short_low_bars)")
        guard = src.rfind("short_armed and btc_below and", 0, call)
        check("the 4h fetch is limited to the top-100 the arm draws from",
              "sym in top_syms" in src[guard:call])
        check("the post-pass sleep goes through next_wait, gated on the arm",
              "time.sleep(next_wait(args.interval, short_armed and bear_gate_open" in src)
        calls = src.count("evaluate_4h(") - src.count("def evaluate_4h(")
        check("the only 4h fetch sits under 'short_armed and btc_below'",
              call > 0 and calls == 1 and 0 < call - guard < 200, "call sites %d" % calls)
        # --- 9. a console that cannot print a CJK ticker must not kill the scanner --------------
        import tempfile
        real_stdout = sys.stdout
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".log"); tmp.close()
        try:
            sys.stdout = io.TextIOWrapper(io.BytesIO(), encoding="cp1251")
            raised = False
            try:
                A.log("universe: skipping 3 non-ASCII symbol(s): 哈基米USDT", tmp.name)
            except UnicodeEncodeError:
                raised = True
        finally:
            sys.stdout = real_stdout
        with io.open(tmp.name, encoding="utf-8") as fh:
            written = fh.read()
        os.unlink(tmp.name)
        check("a cp1251 console does not kill the scanner on a CJK ticker", not raised)
        check("...and the log file keeps the ticker intact (UTF-8)", "哈基米USDT" in written)
    finally:
        A.get = real_get

    print("\n%s" % ("ALL CHECKS PASSED" if not FAILED else "FAILED: " + ", ".join(FAILED)))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
