# -*- coding: utf-8 -*-
"""
Offline checks for the scanner's entry-clock bookkeeping (audit 06.09). No network, no keys.

settle_bookkeeping owns the map of what this scanner opened, the cooldowns and the "was held"
evidence. The things that must hold, and that this pins:

  1. a coin that filled and was stopped out INSIDE one interval gets the 24h cooldown at the
     very next pass (the journal's fill is merged before the prune, not after)
  2. a refused proposal that was never held is forgotten without a cooldown once it is too old
     to fill - and a refusal earns the 2h/6h rest, never the 24h cooldown
  3. a symbol proposed here but opened by the OWNER (no fill of ours, no bt- stop) loses its
     clock once the fill window has passed, so the hold rules cannot close it; with a bt- stop
     in the ledger it keeps the clock; with an unreadable ledger it keeps the clock
  4. was_held is cut down to what is still held or clocked, so an old hand trade does not turn a
     later refusal into 24h of silence
  5. an aborted entry (filled and unwound) counts as a fill
  6. a fresh state reads the journal from now, and a dead log path does not kill the pass

Run: py -3 tools/scanner/test_bookkeeping.py
"""
import io
import json
import os
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import autoscan as A  # noqa: E402

FAILED = []
H = 3600.0
INTERVAL = 3600
COOLDOWN_H = 24
T0 = 1_800_000_000.0


def check(name, cond, detail=""):
    if cond:
        print("  ok   %s" % name)
    else:
        print("  FAIL %s  %s" % (name, detail))
        FAILED.append(name)


def settle(state, held, now, rejected=None, filled=None, owned=None, said=None):
    A.settle_bookkeeping(state, set(held), (rejected or {}, set(filled or ()), 0), owned, now,
                         INTERVAL, COOLDOWN_H, (said.append if said is not None else None))
    return state


def main():
    print("bookkeeping offline checks\n")

    # --- 1. stop-out inside one interval still earns the cooldown next pass -------------
    st = {"entered": {"X": T0}, "cooldown": {}, "entered_trig": {"X": "trend"}, "was_held": []}
    settle(st, held=set(), now=T0 + 1 * H, filled={"X"})
    check("filled-and-flat symbol leaves entered", "X" not in st["entered"])
    check("... and is in cooldown from now", st["cooldown"].get("X") == T0 + 1 * H,
          "cooldown=%r" % st["cooldown"])
    check("... so it cannot be fresh", T0 + 1 * H - st["cooldown"].get("X", 0) <= COOLDOWN_H * H)

    # --- 2. never-held refusal: forgotten late, rested short -----------------------------
    st = {"entered": {"Y": T0}, "cooldown": {}, "entered_trig": {"Y": "trend"}, "was_held": []}
    settle(st, held=set(), now=T0 + 1 * H, rejected={"Y": "BELOW_MIN_NOTIONAL 4.83"})
    check("refused proposal keeps its clock inside the fill window", "Y" in st["entered"])
    check("refusal rest is 6h, not 24h",
          abs(st["cooldown"]["Y"] - (T0 + 1 * H - COOLDOWN_H * H + 6 * H)) < 1,
          "cooldown=%r" % st["cooldown"])
    settle(st, held=set(), now=T0 + 4 * H)
    check("refused proposal is forgotten once too old to fill", "Y" not in st["entered"])
    check("... without the 24h cooldown",
          st["cooldown"]["Y"] < T0 + 4 * H - 12 * H, "cooldown=%r" % st["cooldown"])

    # --- 3. held but not ours ------------------------------------------------------------
    def owner_case(owned, filled=(), age_h=4):
        st = {"entered": {"CAKE": T0}, "cooldown": {}, "entered_trig": {"CAKE": "trend"},
              "was_held": [], "filled_ok": []}
        said = []
        settle(st, held={"CAKE"}, now=T0 + age_h * H, filled=set(filled), owned=owned, said=said)
        return st, said

    st, said = owner_case(owned=set())
    check("owner's position on a proposed symbol loses its clock after the window",
          "CAKE" not in st["entered"] and "CAKE" not in st["entered_trig"])
    check("... and the log says so", any("not ours" in s for s in said), repr(said))
    st, _ = owner_case(owned=set(), age_h=2)
    check("inside the fill window the clock is kept", "CAKE" in st["entered"])
    st, _ = owner_case(owned={"CAKE"})
    check("a bt- stop in the ledger keeps the clock", "CAKE" in st["entered"])
    st, _ = owner_case(owned=None)
    check("an unreadable ledger keeps the clock", "CAKE" in st["entered"])
    st, _ = owner_case(owned=set(), filled={"CAKE"})
    check("a journal fill of ours keeps the clock", "CAKE" in st["entered"])
    check("... and the fill proof is persisted", st["filled_ok"] == ["CAKE"])
    settle(st, held={"CAKE"}, now=T0 + 9 * H, owned=set())
    check("... across later passes without a fresh journal row", "CAKE" in st["entered"])

    # --- 4. was_held does not remember hand trades forever --------------------------------
    st = {"entered": {}, "cooldown": {}, "entered_trig": {}, "was_held": ["TWT"]}
    settle(st, held=set(), now=T0)
    check("a was_held symbol that is neither held nor clocked is dropped", st["was_held"] == [])
    st["entered"]["TWT"] = T0
    settle(st, held=set(), now=T0 + 1 * H, rejected={"TWT": "BELOW_MIN_NOTIONAL"})
    check("a later refusal on it rests 6h, not 24h",
          abs(st["cooldown"]["TWT"] - (T0 + 1 * H - COOLDOWN_H * H + 6 * H)) < 1,
          "cooldown=%r" % st["cooldown"])
    st = {"entered": {"Z": T0}, "cooldown": {}, "entered_trig": {}, "was_held": []}
    settle(st, held={"Z"}, now=T0 + 1 * H, owned={"Z"})
    settle(st, held=set(), now=T0 + 2 * H, owned=set())
    check("a seen-held clocked symbol that goes flat gets the cooldown",
          st["cooldown"].get("Z") == T0 + 2 * H and "Z" not in st["entered"])

    # --- 5. an aborted entry is a fill for the journal reader ----------------------------
    d = tempfile.mkdtemp()
    with io.open(os.path.join(d, "trades.jsonl"), "w", encoding="utf-8") as f:
        f.write(json.dumps({"ts": "2026-09-06T10:00:00.123456789Z", "kind": "aborted",
                            "symbol": "ABC", "signalId": "auto-trend-ABC-x"}) + "\n")
        f.write(json.dumps({"ts": "2026-09-06T10:01:00Z", "kind": "rejected",
                            "symbol": "DEF", "signalId": "auto-dip-DEF-x", "reason": "STALE"}) + "\n")
        f.write(json.dumps({"ts": "2026-09-06T10:02:00Z", "kind": "entry",
                            "symbol": "GHI", "signalId": "manual-1"}) + "\n")
    rejected, filled, newest = A.read_journal_feedback(d, 0)
    check("aborted auto-* row counts as filled", filled == {"ABC"}, repr(filled))
    check("rejected auto-* row is a refusal", rejected == {"DEF": "STALE"}, repr(rejected))
    check("non auto-* rows are ignored", "GHI" not in filled)
    check("newest ts advances", newest > 0)

    # --- 6. a fresh state reads the journal from now, not from 0 --------------------------
    st = A.load_state(os.path.join(d, "no-such-state.json"))
    check("fresh state carries a journal mark near now",
          abs(st.get("journal_seen_ts", 0) - time.time()) < 60, repr(st))

    # --- 7. a log path that cannot be opened does not raise -------------------------------
    try:
        A.log("x", os.path.join(d, "trades.jsonl", "child"))   # parent is a regular file
        check("log() survives an unwritable path", True)
    except Exception as e:
        check("log() survives an unwritable path", False, repr(e))

    print()
    if FAILED:
        print("FAILED: %d" % len(FAILED))
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
