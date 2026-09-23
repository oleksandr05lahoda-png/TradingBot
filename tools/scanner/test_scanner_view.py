# -*- coding: utf-8 -*-
"""
Offline checks for scanner_view.json (23.09). No network, no keys.

The view tells the owner what the scanner saw: the queue behind the book and why each coin in it
is waiting, what this pass opened and closed, and why every open position was opened. It is for
reading only, so the one thing that matters more than its content is that it CANNOT change a
decision. What this pins, and why each one is here:

  1. THE PROOF. main() runs one full pass on a fixture (2 held, 7 candidates, bear arm armed,
     MAX_CORR and MAX_HOLD_HOURS set) four ways: view layer removed, working, writer raising,
     every view function raising. The book lines, the state file and the " scan: " line are the
     same in all four (the state differs only by the view's own "why" key, absent when the
     view is removed or its why step fails); a failure costs exactly one "view:" log line
  2. every gate of the funnel is named correctly on that fixture: cooldown, too_wide, corr,
     book_full, the short that loses its coin to the long ("other"), in the scanner's order
  3. schema and field types of the whole document, strict JSON (no NaN), unknown numbers null
  4. atomic write: the target holds the old view until one os.replace swaps in a complete
     file; a write that dies halfway leaves the old view and no tmp behind
  5. queue: best first by the scanner's own order, outside-pool last, never more than 30;
     regime / halt / outside_pool reasons and the MAX_CORR loop's break point
  6. "why" survives a restart (a new main() reading the state file), is dropped once the
     position is gone, and an old state file without the key loads and grows one
  7. a pass that does nothing, and a pass that stops early (bot not ready), still write a view

Run: py -3 tools/scanner/test_scanner_view.py
"""
import io
import json
import os
import random
import re
import shutil
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import autoscan as A  # noqa: E402

FAILED = []
H = 3600.0
T0 = 1_800_000_000.0          # 2027-01-15, a fixed clock so two runs stamp identical lines


def check(name, cond, detail=""):
    if cond:
        print("  ok   %s" % name)
    else:
        print("  FAIL %s  %s" % (name, detail))
        FAILED.append(name)


class StopLoop(BaseException):
    """Raised by the fake sleep: ends main() after one pass. BaseException, so the pass's own
    `except Exception` cannot swallow it and loop forever."""


class FakeTime(object):
    """Stands in for the time module inside autoscan: a frozen clock, and a sleep that ends the
    run. Every stamp the scanner writes (book header, entry clocks, view ts) comes from here."""

    def __init__(self, t):
        self.t = t

    def time(self):
        return self.t

    def sleep(self, s):
        raise StopLoop(s)

    def strftime(self, fmt, tup=None):
        return time.strftime(fmt, time.gmtime(self.t) if tup is None else tup)

    def gmtime(self, t=None):
        return time.gmtime(self.t if t is None else t)


def series(seed, n=61, base=100.0):
    r = random.Random(seed)
    out, p = [], base
    for _ in range(n):
        p *= 1.0 + r.uniform(-0.04, 0.04)
        out.append(p)
    return out


H1_CLOSES = series(1)


def coin(ret, price=10.0, atr_frac=0.03, closes=None, seed=0):
    return {"ret": ret, "dip": False, "price": price, "atr": atr_frac * price,
            "from_high": 0.02, "vol_ratio": 1.8, "from_high90": 0.05, "ret20": ret / 2,
            "closes": list(closes) if closes else series(seed)}


def fixture_pass1():
    """room = 3 max - (2 held - 1 max-hold close) = 2.
    Long funnel by 30d: A1 .50 | C1 .45 (twin of H1 -> corr) | A2 .40 (cooldown) |
    A3 .35 (stop 20% -> too wide at $140) | A4 .30 | A5 .25. Opens A1, A4; A5 finds no room.
    Bear arm (BTC below its 50d): S1 and A4 break down; room is gone -> S1 book_full, A4 is
    the long's."""
    return {
        "H1USDT": coin(0.10, closes=H1_CLOSES),
        "H2USDT": coin(0.10, seed=2),
        "A1USDT": coin(0.50, seed=11),
        "C1USDT": coin(0.45, closes=H1_CLOSES),
        "A2USDT": coin(0.40, seed=12),
        "A3USDT": coin(0.35, atr_frac=0.10, seed=13),
        "A4USDT": coin(0.30, seed=14),
        "A5USDT": coin(0.25, seed=15),
        "S1USDT": coin(-0.20, seed=16),
    }


BTC = {"ret": 0.05, "dip": False, "price": 90.0, "atr": 2.0, "from_high": 0.1,
       "vol_ratio": 1.0, "from_high90": 0.1, "ret20": 0.0,
       "closes": [100.0] * 60 + [90.0]}          # above 30d ago, below its 50d average
SHORTS = {"S1USDT", "A4USDT"}
ENV = {"BINANCE_TESTNET_API_KEY": "k", "BINANCE_TESTNET_API_SECRET": "s",
       "MAX_CORR": "0.9", "MAX_HOLD_HOURS": "48", "SHORT_ARMED": "on", "RISK_PER_TRADE": "0.005"}


def run_pass(workdir, now, held, coins, ready=True, patches=None, shorts=SHORTS):
    """One pass of the real main() on the fixture. Returns (book text, state dict, log text)."""
    book = os.path.join(workdir, "book.txt")
    botlog = os.path.join(workdir, "bot.err.log")
    with io.open(botlog, "w", encoding="utf-8") as f:
        f.write("boot\n" + ("account read complete\n" if ready else ""))
    if not os.path.exists(book):
        io.open(book, "w", encoding="utf-8").close()
    pool = [(s, 1e8) for s in sorted(coins)]
    rows = [{"symbol": s, "positionAmt": "1", "updateTime": int((now - 5 * H) * 1000)}
            for s in sorted(held)]
    ticker = [{"symbol": s, "price": str(m["price"])} for s, m in coins.items()]
    ticker.append({"symbol": "BTCUSDT", "price": "90.0"})

    def fake_signed(path, env):
        if path == "/fapi/v2/positionRisk":
            return rows
        if path == "/fapi/v2/account":
            return {"totalWalletBalance": "140", "totalUnrealizedProfit": "0"}
        raise AssertionError(path)

    def fake_eval(sym, lookback, dip_depth, live_price):
        if sym == "BTCUSDT":
            return json.loads(json.dumps(BTC))
        m = coins.get(sym)
        return json.loads(json.dumps(m)) if m else None

    def fake_4h(sym, low_bars, now_ms=None):
        if sym not in shorts:
            return None
        p = coins[sym]["price"]
        return {"close4h": p, "ref_low4h": p * 1.01, "atr4h": 0.02 * p,
                "broke": True, "fresh": True, "age_min": 10}

    saved = {k: getattr(A, k) for k in ("time", "read_env", "signed_get", "get", "universe",
                                        "evaluate", "evaluate_4h", "notify")}
    extra = dict(patches or {})
    saved.update({k: getattr(A, k) for k in extra})
    argv, stdout = sys.argv, sys.stdout
    try:
        sys.stdout = io.StringIO()          # log() echoes every line; keep the report readable
        A.time = FakeTime(now)
        A.read_env = lambda repo: dict(ENV)
        A.signed_get = fake_signed
        A.get = lambda path, params, **kw: ticker if path == "/fapi/v1/ticker/price" else None
        A.universe = lambda top, mv, by_cap, cached_cap=None, logpath=None: (list(pool), "cap")
        A.evaluate = fake_eval
        A.evaluate_4h = fake_4h
        A.notify = lambda env, text, logpath: None
        for k, v in extra.items():
            setattr(A, k, v)
        sys.argv = ["autoscan.py", "--script", book, "--bot-log", botlog, "--workdir", workdir,
                    "--repo", workdir, "--max-positions", "3", "--interval", "3600"]
        try:
            A.main()
        except StopLoop:
            pass
    finally:
        sys.argv, sys.stdout = argv, stdout
        for k, v in saved.items():
            setattr(A, k, v)
    text = io.open(book, encoding="utf-8").read()
    with io.open(os.path.join(workdir, "autoscan_state.json"), encoding="utf-8") as f:
        state = json.load(f)
    logtext = io.open(os.path.join(workdir, "autoscan.log"), encoding="utf-8").read()
    return text, state, logtext


def seed_state(workdir, now):
    """A state file from BEFORE the view existed: no "why" key. H1 opened 10h ago, H2 50h ago
    (past the 48h cap), A2 closed an hour ago (cooldown)."""
    st = {"entered": {"H1USDT": now - 10 * H, "H2USDT": now - 50 * H},
          "entered_trig": {"H1USDT": "trend", "H2USDT": "trend"},
          "cooldown": {"A2USDT": now - 1 * H}, "was_held": ["H1USDT", "H2USDT"],
          "filled_ok": ["H1USDT", "H2USDT"], "journal_seen_ts": now - 2 * H, "regime": "BULL"}
    with io.open(os.path.join(workdir, "autoscan_state.json"), "w", encoding="utf-8") as f:
        json.dump(st, f)


def scan_lines(logtext):
    return [ln[20:] for ln in logtext.splitlines() if " scan: " in ln]


def view_lines(logtext):
    return [ln for ln in logtext.splitlines() if " view: " in ln]


def boom(*a, **k):
    raise RuntimeError("forced view failure")


def main():
    print("scanner view - offline checks\n")
    root = tempfile.mkdtemp(prefix="scanview-")
    try:
        _main(root)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print()
    if FAILED:
        print("FAILED: %d check(s): %s" % (len(FAILED), ", ".join(FAILED)))
        return 1
    print("ALL CHECKS PASSED")
    return 0


def _main(root):
    held = {"H1USDT", "H2USDT"}
    runs = {}
    variants = [
        ("off", {"publish_view": lambda *a, **k: None}),      # the scanner as it was before 23.09
        ("on", {}),
        ("writer-raises", {"write_json_atomic": boom}),
        ("all-raise", {"write_json_atomic": boom, "build_view": boom, "note_why": boom}),
    ]
    for name, patches in variants:
        wd = os.path.join(root, name)
        os.makedirs(wd)
        seed_state(wd, T0)
        runs[name] = run_pass(wd, T0, held, fixture_pass1(), patches=patches)

    # --- 1. the proof: the view cannot change a decision -------------------------------------
    print("1. no behaviour change")
    book_off, st_off, log_off = runs["off"]
    book_on, st_on, log_on = runs["on"]
    check("the fixture pass actually trades (closes H2, opens A1 and A4)",
          "CLOSE H2USDT" in book_off and "A1USDT LONG" in book_off and "A4USDT LONG" in book_off,
          book_off)
    check("the fixture pass writes no short (the room went to the longs)", " SHORT " not in book_off)
    for name in ("on", "writer-raises", "all-raise"):
        book, st, logt = runs[name]
        check("%s: book lines identical to the scanner without a view" % name, book == book_off)
        strip = {k: v for k, v in st.items() if k != "why"}
        check("%s: state identical apart from the view's own key" % name, strip == st_off,
              "%r vs %r" % (strip, st_off))
        check("%s: the ' scan: ' line is unchanged" % name, scan_lines(logt) == scan_lines(log_off),
              "%r" % scan_lines(logt))
    check("writer raising: state identical to the working view, why included",
          runs["writer-raises"][1] == st_on)
    check("working view: no 'view:' line in the log", view_lines(log_on) == [])
    for name in ("writer-raises", "all-raise"):
        vl = view_lines(runs[name][2])
        check("%s: exactly one 'view:' log line, and it says trading is unaffected" % name,
              len(vl) == 1 and "trading unaffected" in vl[0], "%r" % vl)
        check("%s: no view file, no tmp left behind" % name,
              not os.path.exists(os.path.join(root, name, A.VIEW_FILE))
              and not os.path.exists(os.path.join(root, name, A.VIEW_FILE + ".tmp")))
    check("the view layer removed: no view file (the baseline really was the old scanner)",
          not os.path.exists(os.path.join(root, "off", A.VIEW_FILE)))

    # --- 2 + 3. what the working view says, and its schema ------------------------------------
    print("\n2. the funnel, named")
    with io.open(os.path.join(root, "on", A.VIEW_FILE), encoding="utf-8") as f:
        raw = f.read()
    view = json.loads(raw)
    q = [(e["symbol"], e["side"], e["reason"]) for e in view["queue"]]
    want = [("C1USDT", "LONG", "corr"), ("A2USDT", "LONG", "cooldown"),
            ("A3USDT", "LONG", "too_wide"), ("A5USDT", "LONG", "book_full"),
            ("S1USDT", "SHORT", "book_full"), ("A4USDT", "SHORT", "other")]
    check("queue = every gate, in the scanner's order", q == want, "%r" % q)
    check("held coins are not in the queue", not any(e["symbol"] in held for e in view["queue"]))
    check("the short that lost its coin to the long says so",
          view["queue"][-1]["note"] == "эту монету берёт лонг", view["queue"][-1]["note"])
    check("too_wide carries the stop it could not size", "20.0%" in view["queue"][2]["note"]
          and abs(view["queue"][2]["stop_frac"] - 0.2) < 1e-9, "%r" % view["queue"][2])
    check("opened = what the book got, long first",
          view["opened"] == [{"symbol": "A1USDT", "side": "LONG", "trig": "trend"},
                             {"symbol": "A4USDT", "side": "LONG", "trig": "trend"}],
          "%r" % view["opened"])
    check("closed = H2 by max-hold", view["closed"] == [{"symbol": "H2USDT", "reason": "max-hold"}],
          "%r" % view["closed"])
    check("counts: 2 held, room 2 of 3, 8 entry-ok, 8 hold-ok (S1 is falling)",
          (view["held"], view["room"], view["max_positions"], view["entry_ok"], view["hold_ok"])
          == (2, 2, 3, 8, 8), "%r" % ((view["held"], view["room"], view["max_positions"],
                                       view["entry_ok"], view["hold_ok"]),))
    check("btc: live price, 50d average, gate open; arm armed; not halted",
          view["btc"] == {"price": 90.0, "sma50": 99.8, "short_gate_open": True}
          and view["short_armed"] is True and view["halted"] is False, "%r" % view["btc"])

    print("\n3. schema")
    check("v = 1", view.get("v") == 1)
    iso = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
    check("ts is the pass end in UTC", view["ts"] == A._iso(T0), view["ts"])
    check("next_pass_eta is ISO UTC and not before ts",
          iso.match(view["next_pass_eta"]) is not None and view["next_pass_eta"] >= view["ts"],
          view["next_pass_eta"])
    top = {"v": int, "ts": str, "next_pass_eta": str, "held": int, "room": int,
           "max_positions": int, "entry_ok": int, "hold_ok": int, "halted": bool, "btc": dict,
           "short_armed": bool, "queue": list, "opened": list, "closed": list, "why": dict}
    check("exactly the contract's top-level keys", set(view) == set(top), "%r" % sorted(view))
    check("top-level types", all(isinstance(view[k], t) for k, t in top.items()))
    qkeys = {"symbol", "side", "trig", "reason", "ret30", "from_high20", "vol_ratio",
             "stop_frac", "note"}
    check("queue entries: exactly the contract's keys",
          all(set(e) == qkeys for e in view["queue"]))
    check("queue: reasons from the contract's list only",
          all(e["reason"] in A.VIEW_REASONS for e in view["queue"]))
    check("queue: numbers are numbers (or null), texts are texts",
          all(all(e[k] is None or isinstance(e[k], float) for k in
                  ("ret30", "from_high20", "vol_ratio", "stop_frac"))
              and all(isinstance(e[k], str) for k in ("symbol", "side", "trig", "reason", "note"))
              for e in view["queue"]))
    wkeys = {"opened_ts", "side", "trig", "price", "ret30", "from_high20", "from_high90",
             "vol_ratio", "atr", "stop_frac"}
    check("why: A1 and A4, with exactly the contract's fields",
          sorted(view["why"]) == ["A1USDT", "A4USDT"]
          and all(set(v) == wkeys for v in view["why"].values()), "%r" % view["why"])
    w = view["why"].get("A1USDT", {})
    check("why A1: opened now, the line's price and 30d, stop 6%",
          w.get("opened_ts") == A._iso(T0) and w.get("price") == 10.0 and w.get("ret30") == 0.5
          and abs((w.get("stop_frac") or 0) - 0.06) < 1e-9, "%r" % w)
    check("strict JSON: no NaN/Infinity tokens", "NaN" not in raw and "Infinity" not in raw)
    check("the state file carries the same why", st_on.get("why") == view["why"])
    doc = A.build_view({"held": None, "btc_closes": None,
                        "details": {"X": {"ret": float("nan"), "price": 1.0,
                                          "atr": float("inf")}},
                        "entry_ok": {"X"}, "entry_pool": {"X"}}, {}, T0, T0 + 60)
    json.dumps(doc, allow_nan=False)
    check("unknown numbers are null (unread held, no BTC, NaN metrics)",
          doc["held"] is None and doc["room"] is None and doc["btc"]["price"] is None
          and doc["btc"]["short_gate_open"] is None and doc["queue"][0]["ret30"] is None
          and doc["queue"][0]["stop_frac"] is None, "%r" % doc)
    check("...and a pass that died mid-funnel says 'other', not a guess",
          doc["queue"][0]["reason"] == "other", doc["queue"][0]["reason"])

    # --- 4. atomic write ----------------------------------------------------------------------
    print("\n4. atomic write")
    wd = os.path.join(root, "atomic")
    os.makedirs(wd)
    path = os.path.join(wd, A.VIEW_FILE)
    A.write_json_atomic(path, {"v": 1, "old": True})
    seen = {}
    real_replace = A.os.replace

    def spying_replace(src, dst):
        with io.open(dst, encoding="utf-8") as f:
            seen["target"] = json.load(f)
        with io.open(src, encoding="utf-8") as f:
            seen["tmp"] = json.load(f)
        seen["same_dir"] = os.path.dirname(os.path.abspath(src)) == os.path.dirname(os.path.abspath(dst))
        return real_replace(src, dst)

    big = {"v": 1, "queue": [{"symbol": "X%dUSDT" % i, "note": "н" * 50} for i in range(500)]}
    A.os.replace = spying_replace
    try:
        A.write_json_atomic(path, big)
    finally:
        A.os.replace = real_replace
    check("until the swap the target still holds the OLD complete view", seen.get("target") == {"v": 1, "old": True})
    check("the tmp is complete JSON before the swap", seen.get("tmp") == big)
    check("the tmp sits in the same directory (os.replace stays atomic)", seen.get("same_dir") is True)
    with io.open(path, encoding="utf-8") as f:
        check("after the swap the target is the new view", json.load(f) == big)
    try:
        A.write_json_atomic(path, {"v": 1, "queue": [1, 2, object()]})   # dies mid-serialisation
        died = False
    except TypeError:
        died = True
    with io.open(path, encoding="utf-8") as f:
        check("a write that dies halfway raises, leaves the last good view intact",
              died and json.load(f) == big)
    check("...and removes its tmp", not os.path.exists(path + ".tmp"))
    try:
        A.write_json_atomic(path, {"v": 1, "x": float("nan")})
        nan_ok = False
    except ValueError:
        nan_ok = True
    check("a NaN that slipped past _num is refused, not written as a bare NaN token", nan_ok)

    # --- 5. queue ordering and length ---------------------------------------------------------
    print("\n5. queue order, cap and the other reasons")
    details = {"L%02dUSDT" % i: coin(0.9 - i * 0.01) for i in range(40)}
    details["OUTUSDT"] = coin(5.0)                       # strongest of all, but outside the pool
    details["SHAUSDT"] = coin(-0.3)
    details["SHAUSDT"]["short_atr"] = 0.3
    ins = sorted((s for s in details if s.startswith("L")), key=lambda s: -details[s]["ret"])
    pv = {"details": details, "held": set(), "entry_ok": set(ins) | {"OUTUSDT"},
          "entry_pool": set(ins), "base_pool": set(ins) | {"SHAUSDT"}, "room": 0,
          "short_ok": {"SHAUSDT"}, "l_cool": ins, "l_feas": ins, "l_corr": ins, "l_room": [],
          "l_regime": [], "l_final": [], "s_cool": ["SHAUSDT"], "s_feas": ["SHAUSDT"],
          "s_final": []}
    qq = A.view_queue(pv)
    check("never more than 30", len(qq) == 30, str(len(qq)))
    check("strongest 30d first inside the funnel",
          [e["symbol"] for e in qq] == ins[:30], "%r" % [e["symbol"] for e in qq][:5])
    check("a book with no room says book_full", all(e["reason"] == "book_full" for e in qq))
    pv_small = dict(pv, details={k: details[k] for k in ("L00USDT", "L01USDT", "OUTUSDT", "SHAUSDT")},
                    entry_ok={"L00USDT", "L01USDT", "OUTUSDT"}, entry_pool={"L00USDT", "L01USDT"},
                    l_cool=["L00USDT", "L01USDT"], l_feas=["L00USDT", "L01USDT"],
                    l_corr=["L00USDT", "L01USDT"])
    order = [(e["symbol"], e["reason"]) for e in A.view_queue(pv_small)]
    check("funnel longs, then the bear arm, then outside-pool last (even the strongest)",
          order == [("L00USDT", "book_full"), ("L01USDT", "book_full"), ("SHAUSDT", "book_full"),
                    ("OUTUSDT", "outside_pool")], "%r" % order)
    two = ["L00USDT", "L01USDT"]
    base = dict(pv_small, entry_ok=set(two), room=2, l_cool=two, l_feas=two, l_corr=two,
                l_room=two, short_ok=set(), s_cool=[], s_feas=[], s_final=[])
    r = [e["reason"] for e in A.view_queue(dict(base, l_regime=[], l_final=[]))]
    check("REGIME_GATE=cash suppression -> regime", r == ["regime", "regime"], "%r" % r)
    r = [e["reason"] for e in A.view_queue(dict(base, l_regime=two, l_final=[], halted=True))]
    check("halt suppression -> halt", r == ["halt", "halt"], "%r" % r)
    r = [e["reason"] for e in A.view_queue(dict(base, l_final=[], l_regime=two, halted=True,
                                                short_ok={"SHAUSDT"}, s_cool=["SHAUSDT"],
                                                s_feas=["SHAUSDT"]))]
    check("a halted bear arm -> halt", r[-1] == "halt", "%r" % r)
    three = ["L00USDT", "L01USDT", "L02USDT"]
    pv_c = dict(base, details=details, entry_ok=set(three), entry_pool=set(three), room=2,
                corr_ran=True, l_cool=three, l_feas=three, l_corr=["L00USDT"], l_room=["L00USDT"],
                l_regime=["L00USDT"], l_final=["L00USDT"])
    r = [(e["symbol"], e["reason"]) for e in A.view_queue(pv_c)]
    check("MAX_CORR never reached the room: every non-kept coin was a corr skip",
          r == [("L01USDT", "corr"), ("L02USDT", "corr")], "%r" % r)
    pv_c2 = dict(pv_c, room=1, l_corr=["L00USDT"])
    pv_c2["l_feas"] = ["L01USDT", "L00USDT", "L02USDT"]
    r = [(e["symbol"], e["reason"]) for e in A.view_queue(pv_c2)]
    check("MAX_CORR stopped at the room: before it corr, after it book_full",
          r == [("L01USDT", "corr"), ("L02USDT", "book_full")], "%r" % r)

    # --- 6. why: restart and prune --------------------------------------------------------------
    print("\n6. why across a restart")
    wd = os.path.join(root, "on")          # continues the working run: A1, A4 opened at T0
    c2 = fixture_pass1()
    # pass 2, an hour later, in a NEW main() (the restart): A1 filled, A4 still pending
    _b, st2, _l = run_pass(wd, T0 + 1 * H, {"H1USDT", "A1USDT"}, c2)
    v2 = json.load(io.open(os.path.join(wd, A.VIEW_FILE), encoding="utf-8"))
    check("after a restart A1's reason is still there, stamped at its real open",
          v2["why"].get("A1USDT", {}).get("opened_ts") == A._iso(T0), "%r" % v2["why"].get("A1USDT"))
    check("a line still waiting to fill keeps its reason", "A4USDT" in v2["why"])
    check("the state file and the view agree", st2.get("why") == v2["why"])
    # pass 3, four hours later: A1 was stopped out, A4 never filled (pass 2 re-proposed it, as the
    # scanner always has for an unfilled line); nothing qualifies to enter, no breakdowns
    quiet = {s: dict(m, ret=0.0) for s, m in fixture_pass1().items()}
    quiet["H1USDT"]["ret"] = 0.10
    book_before = io.open(os.path.join(wd, "book.txt"), encoding="utf-8").read()
    _b3, st3, _l3 = run_pass(wd, T0 + 5 * H, {"H1USDT"}, quiet, shorts=set())
    v3 = json.load(io.open(os.path.join(wd, A.VIEW_FILE), encoding="utf-8"))
    check("a gone position loses its reason, in the view and in the state",
          "A1USDT" not in v3["why"] and "A1USDT" not in st3.get("why", {}), "%r" % v3["why"])
    check("a line that never filled loses it once its clock is dropped", "A4USDT" not in v3["why"])
    check("a pass with nothing to do still writes a fresh view",
          v3["ts"] == A._iso(T0 + 5 * H) and v3["opened"] == [] and v3["closed"] == []
          and _b3 == book_before, v3["ts"])
    old = {"entered": {"Z": 1.0}, "cooldown": {}}
    check("an old state without the key: nothing to record -> no key added, nothing saved",
          A.note_why(old, {}) is False and "why" not in old)
    old2 = {"entered": {"Z": 1.0}, "cooldown": {}, "why": "garbage"}
    check("a corrupt why value is replaced, not trusted",
          A.note_why(old2, {}) is True and old2["why"] == {})

    # --- 7. a pass that stops early still says so ---------------------------------------------------
    print("\n7. early passes")
    wd = os.path.join(root, "notready")
    os.makedirs(wd)
    seed_state(wd, T0)
    b, _s, _l = run_pass(wd, T0, held, fixture_pass1(), ready=False)
    vn = json.load(io.open(os.path.join(wd, A.VIEW_FILE), encoding="utf-8"))
    check("bot not ready: nothing written to the book, a view still published",
          b == "" and vn["ts"] == A._iso(T0), vn["ts"])
    check("...with the unknowns null and the next pass in 5 minutes",
          vn["held"] is None and vn["room"] is None and vn["queue"] == []
          and vn["next_pass_eta"] == A._iso(T0 + 300), "%r" % vn)


if __name__ == "__main__":
    sys.exit(main())
