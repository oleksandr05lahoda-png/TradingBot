# -*- coding: utf-8 -*-
"""Offline checks for lessons.py (no network: every bar is a fake built here).

Run: python tools/ops/test_lessons.py
"""
import json
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lessons as L  # noqa: E402

T0 = L.parse_ts("2026-09-10T00:00:00Z")
H = 3600.0


def row(kind, sym, ts, **kw):
    r = {"kind": kind, "symbol": sym, "ts": ts}
    r.update(kw)
    return json.dumps(r)


def entry(sym, ts, fill, qty, trig="trend", stop=None, tp=None, side="LONG"):
    kw = {"side": side, "fill": str(fill), "qty": str(qty), "signalId": "auto-%s-%s-20260910T000000Z" % (trig, sym)}
    if stop is not None:
        kw["stopPrice"] = str(stop)
    if tp is not None:
        kw["tp"] = [str(tp)]
    return row("entry", sym, ts, **kw)


def bar(t_sec, o, h, l, c):
    return [int(t_sec * 1000), str(o), str(h), str(l), str(c), "0", int(t_sec * 1000) + 59999]


def trade(**kw):
    t = {"symbol": "AAAUSDT", "side": "LONG", "qty": 10.0, "entry_ts": T0, "exit_ts": T0 + 10 * H,
         "entry_px": 1.0, "exit_px": 0.9, "exit_px_approx": False, "stop": 0.85, "tp": 1.2, "risk_usd": 1.5,
         "signal_id": "auto-trend-AAAUSDT-x", "trigger": "trend", "exit_kind": "max-hold",
         "exit_reason": "max-hold", "qty_mismatch": False}
    t.update(kw)
    return t


def ctx(**kw):
    c = {"btc_entry": 100.0, "btc_exit": 100.0, "btc_72h": 100.0, "coin_72h": 1.0,
         "beta": 1.0, "beta_n": 30, "beta_fallback": False}
    c.update(kw)
    return c


class Journal(unittest.TestCase):
    def test_parse_ts_nanoseconds(self):
        self.assertAlmostEqual(L.parse_ts("2026-08-30T18:06:05.512627300Z") - L.parse_ts("2026-08-30T18:06:05Z"),
                               0.5126273, places=6)
        self.assertEqual(L.parse_ts("1970-01-01T00:00:10Z"), 10.0)
        with self.assertRaises(ValueError):
            L.parse_ts("yesterday")

    def test_load_rows_skips_blank_and_reports_bad(self):
        rows, bad = L.load_rows(["", entry("A", "2026-09-10T00:00:00Z", 1, 1), "{not json", '{"kind":"x"}'])
        self.assertEqual(len(rows), 1)
        self.assertEqual(bad, [3, 4])

    def test_trigger_and_exit_kind(self):
        self.assertEqual(L.trigger_of("auto-dip-JSTUSDT-20260831T140942Z"), "dip")
        self.assertEqual(L.trigger_of("auto-short-XUSDT-1"), "short")
        self.assertEqual(L.trigger_of("op-open-X"), "other")
        self.assertEqual(L.trigger_of(None), "other")
        ek = L.exit_kind
        self.assertEqual(ek({"kind": "close", "reason": "max-hold"}), "max-hold")
        self.assertEqual(ek({"kind": "close", "reason": "daily-loss-kill-switch"}), "kill-switch")
        self.assertEqual(ek({"kind": "close", "reason": "operator-lock-in"}), "manual")
        self.assertEqual(ek({"kind": "close", "reason": "dip-exited"}), "other")
        self.assertEqual(ek({"kind": "exchange-exit", "cause": "stop-loss"}), "stop")
        self.assertEqual(ek({"kind": "exchange-exit", "cause": "take-profit"}), "take")
        self.assertEqual(ek({"kind": "exchange-exit", "cause": "hand-close"}), "manual")
        self.assertEqual(ek({"kind": "exchange-exit"}), "other")

    def test_pairing_in_time_order_per_symbol(self):
        lines = [
            row("close", "OLD", "2026-09-09T00:00:00Z", reason="max-hold", price="1", qty="1"),     # pre-journal
            entry("B", "2026-09-10T02:00:00Z", 2.0, 5, trig="dip", stop=1.8, tp=2.4),
            entry("A", "2026-09-10T01:00:00Z", 1.0, 10, stop=0.9, tp=1.2),                          # out of file order
            row("rejected", "C", "2026-09-10T01:30:00Z", reason="BELOW_MIN_NOTIONAL", signalId="auto-trend-C-1"),
            row("exchange-exit", "A", "2026-09-10T03:00:00Z", cause="stop-loss", price="0.9", qty="10"),
            row("close", "B", "2026-09-10T04:00:00Z", reason="daily-loss-kill-switch", price="0", qty="5"),
            entry("A", "2026-09-10T05:00:00Z", 1.1, 7),
            entry("A", "2026-09-10T06:00:00Z", 1.2, 7),                                             # entry over entry
            row("close", "A", "2026-09-10T07:00:00Z", reason="max-hold", price="1.3", qty="6"),     # qty mismatch
            entry("D", "2026-09-10T08:00:00Z", 1.0, 1),                                             # still open
        ]
        rows, bad = L.load_rows(lines)
        self.assertEqual(bad, [])
        trades, unpaired = L.pair_trades(rows)
        self.assertEqual([(t["symbol"], t["exit_kind"]) for t in trades],
                         [("A", "stop"), ("B", "kill-switch"), ("A", "max-hold")])
        a1, b, a2 = trades
        self.assertEqual((a1["entry_px"], a1["exit_px"], a1["stop"], a1["tp"]), (1.0, 0.9, 0.9, 1.2))
        self.assertIsNone(b["exit_px"])                     # price "0" -> must be recovered by the caller
        self.assertEqual(b["trigger"], "dip")
        self.assertEqual(a2["entry_px"], 1.2)               # paired with the LATER entry
        self.assertTrue(a2["qty_mismatch"])
        self.assertFalse(a1["qty_mismatch"])
        reasons = sorted(u["reason"] for u in unpaired)
        self.assertEqual(reasons, ["entry followed by another entry", "entry still open (no exit yet)",
                                   "exit without an entry in this journal"])


class Bars(unittest.TestCase):
    def test_price_at_open_close_and_edges(self):
        bars = [bar(T0, 10, 11, 9, 10.5), bar(T0 + 60, 10.5, 12, 10, 11.5)]
        self.assertEqual(L.price_at(bars, T0 + 5, 60), 10.0)       # first half -> open
        self.assertEqual(L.price_at(bars, T0 + 45, 60), 10.5)      # second half -> close
        self.assertEqual(L.price_at(bars, T0 + 61, 60), 10.5)
        self.assertEqual(L.price_at(bars, T0 + 150, 60), 11.5)     # just after the last bar -> its close
        self.assertIsNone(L.price_at(bars, T0 + 1000, 60))
        self.assertIsNone(L.price_at(bars, T0 - 1, 60))
        self.assertIsNone(L.price_at([], T0, 60))

    def _daily(self, closes, start):
        return [[int((start + i * 86400) * 1000), "0", "0", "0", str(c), "0", 0] for i, c in enumerate(closes)]

    def test_beta_recovers_a_known_beta_and_ignores_bars_not_closed(self):
        start = T0 - 40 * 86400
        btc, coin = [100.0], [1.0]
        rets = [0.01, -0.02, 0.015, -0.005, 0.03, -0.01] * 7
        for r in rets:
            btc.append(btc[-1] * (1 + r))
            coin.append(coin[-1] * (1 + 2 * r))          # beta exactly 2
        # a crazy bar on the entry day itself must NOT count (it has not closed at entry)
        entry_ts = start + 40 * 86400 + 3600
        coin_b = self._daily(coin[:40] + [coin[39] * 5], start)
        btc_b = self._daily(btc[:41], start)
        beta, n, fb = L.beta_from_daily(coin_b, btc_b, entry_ts)
        self.assertFalse(fb)
        self.assertEqual(n, 30)
        self.assertAlmostEqual(beta, 2.0, places=9)

    def test_beta_fallback_for_a_new_listing(self):
        start = T0 - 40 * 86400
        btc_b = self._daily([100 + i for i in range(41)], start)
        coin_b = self._daily([1 + 0.01 * i for i in range(10)], start + 30 * 86400)
        beta, n, fb = L.beta_from_daily(coin_b, btc_b, start + 40 * 86400 + 60)
        self.assertEqual((beta, fb), (L.BETA_FALLBACK, True))
        self.assertLess(n, L.BETA_MIN_RETURNS)


class Classify(unittest.TestCase):
    def test_pnl_with_10bp_per_side(self):
        p = L.trade_pnl(trade(entry_px=1.0, exit_px=0.9, qty=10))
        self.assertAlmostEqual(p["gross"], -1.0)
        self.assertAlmostEqual(p["cost"], 0.001 * (10 + 9))
        self.assertAlmostEqual(p["net"], -1.019)
        s = L.trade_pnl(trade(side="SHORT", entry_px=1.0, exit_px=0.9, qty=10))
        self.assertAlmostEqual(s["gross"], 1.0)

    def test_small_gross_win_eaten_by_cost_is_a_loss(self):
        rec = L.classify(trade(entry_px=1.0, exit_px=1.0005, qty=10), ctx())
        self.assertEqual(rec["outcome"], "loss")

    def test_market_cause(self):
        # BTC -5%, beta 1.5 -> market part -7.5%; coin -8% -> residual -0.5%  => market
        rec = L.classify(trade(exit_px=0.92), ctx(btc_exit=95.0, beta=1.5))
        self.assertEqual(rec["cause"], "market")
        self.assertAlmostEqual(rec["market_part"], -0.075)
        self.assertAlmostEqual(rec["residual"], -0.005)
        self.assertTrue(rec["moved_with_btc"])

    def test_coin_cause_when_residual_dominates_or_btc_rose(self):
        rec = L.classify(trade(exit_px=0.85), ctx(btc_exit=98.0, beta=1.0))    # -2% market vs -13% own
        self.assertEqual(rec["cause"], "coin")
        rec = L.classify(trade(exit_px=0.95), ctx(btc_exit=101.0, beta=1.0))   # BTC up: never "market"
        self.assertEqual(rec["cause"], "coin")
        self.assertFalse(rec["moved_with_btc"])

    def test_boundary_equal_parts_is_market(self):
        # beta*M = -5%, residual = -5% -> |bM| >= |res| -> market (the >= is pre-registered)
        rec = L.classify(trade(exit_px=0.9), ctx(btc_exit=95.0, beta=1.0))
        self.assertAlmostEqual(abs(rec["market_part"]), abs(rec["residual"]))
        self.assertEqual(rec["cause"], "market")

    def test_winner_has_no_cause(self):
        rec = L.classify(trade(exit_px=1.1), ctx(btc_exit=90.0))
        self.assertEqual((rec["outcome"], rec["cause"]), ("win", ""))

    def test_late_entry_threshold_and_attributes(self):
        rec = L.classify(trade(entry_px=1.10), ctx(coin_72h=1.0))              # exactly +10%
        self.assertTrue(rec["late_entry"])
        rec = L.classify(trade(entry_px=1.0999), ctx(coin_72h=1.0))
        self.assertFalse(rec["late_entry"])
        rec = L.classify(trade(stop=None), ctx(coin_72h=None, btc_72h=None))
        self.assertIsNone(rec["late_entry"])
        self.assertIsNone(rec["stop_width_pct"])
        self.assertIsNone(rec["btc_ret72"])
        rec = L.classify(trade(entry_px=1.0, stop=0.85, exit_ts=T0 + 36 * H), ctx(btc_72h=110.0))
        self.assertAlmostEqual(rec["stop_width_pct"], 15.0)
        self.assertAlmostEqual(rec["hours_held"], 36.0)
        self.assertAlmostEqual(rec["btc_ret72"], 100 / 110 - 1)
        self.assertEqual(rec["exit_day"], "2026-09-11")


class Crowd(unittest.TestCase):
    def test_same_pass_counts_as_one_bet(self):
        recs = [
            {"entry_ts": T0, "exit_ts": T0 + 10 * H, "net": -1.0},
            {"entry_ts": T0 + 5, "exit_ts": T0 + 10 * H, "net": -0.5},     # same pass, 5 s later
            {"entry_ts": T0 + 2 * H, "exit_ts": T0 + 3 * H, "net": 0.4},
            {"entry_ts": T0 + 20 * H, "exit_ts": T0 + 21 * H, "net": -0.1},  # alone
        ]
        L.crowding(recs)
        self.assertEqual([r["open_at_entry"] for r in recs], [1, 1, 2, 0])
        self.assertEqual([r["others_lost"] for r in recs], [1, 1, 2, 0])


class Hold(unittest.TestCase):
    def test_stop_take_both_and_open(self):
        bars = [bar(T0, 1.0, 1.05, 0.98, 1.02), bar(T0 + 60, 1.02, 1.25, 0.99, 1.2), bar(T0 + 120, 1.2, 1.2, 0.8, 0.9)]
        h = L.simulate_hold("LONG", 10, 1.0, 0.85, 1.2, bars, T0)
        self.assertEqual((h["how"], h["exit_px"]), ("take", 1.2))
        self.assertAlmostEqual(h["net"], 2.0 - 0.001 * (10 + 12))
        h = L.simulate_hold("LONG", 10, 1.0, 0.85, 1.2, bars, T0, honour_tp=False)
        self.assertEqual((h["how"], h["exit_px"]), ("stop", 0.85))
        both = [bar(T0, 1.0, 1.3, 0.8, 1.0)]
        self.assertEqual(L.simulate_hold("LONG", 1, 1.0, 0.85, 1.2, both, T0)["how"], "stop")   # conservative
        calm = [bar(T0, 1.0, 1.01, 0.99, 1.0), bar(T0 + 60, 1.0, 1.02, 0.99, 1.01)]
        h = L.simulate_hold("LONG", 1, 1.0, 0.85, 1.2, calm, T0)
        self.assertEqual((h["how"], h["exit_px"]), ("open", 1.01))
        self.assertIsNone(L.simulate_hold("LONG", 1, 1.0, 0.85, 1.2, calm, T0 + 999))

    def test_until_limits_the_window_and_short_side(self):
        bars = [bar(T0, 1.0, 1.01, 0.99, 1.0), bar(T0 + 60, 1.0, 1.01, 0.5, 0.6)]
        h = L.simulate_hold("LONG", 1, 1.0, 0.85, 1.2, bars, T0, until_ts=T0 + 30)
        self.assertEqual(h["how"], "open")
        s = L.simulate_hold("SHORT", 1, 1.0, 1.15, 0.9, [bar(T0, 1.0, 1.2, 0.95, 1.1)], T0)
        self.assertEqual((s["how"], s["exit_px"]), ("stop", 1.15))


def _rec(outcome, net, **kw):
    r = {"outcome": outcome, "net": net, "gross": net + 0.01, "cost": 0.01, "late_entry": False, "ret72": 0.0,
         "stop_width_pct": 10.0, "hours_held": 24.0, "btc_ret72": 0.0, "open_at_entry": 3, "others_lost": 1,
         "trigger": "trend", "exit_kind": "max-hold", "cause": "coin" if outcome == "loss" else "",
         "exit_day": "2026-09-15", "entry_ts": T0, "exit_ts": T0 + H}
    r.update(kw)
    return r


class Aggregate(unittest.TestCase):
    def test_welch_t_known_values(self):
        d, t, se = L.welch_t([1, 2, 3, 4], [2, 4, 6, 8])
        self.assertAlmostEqual(d, -2.5)
        self.assertAlmostEqual(se, math.sqrt((5 / 3) / 4 + (20 / 3) / 4))
        self.assertAlmostEqual(t, -2.5 / se)
        self.assertEqual(L.welch_t([1], [1, 2])[1], None)
        self.assertEqual(L.welch_t([1, 1], [1, 1])[1], None)

    def test_gate_needs_30_in_both_groups_and_t2(self):
        # a huge difference on 10 losers is still "too little data"
        recs = [_rec("win", 0.5, stop_width_pct=5.0 + (i % 3)) for i in range(40)] + \
               [_rec("loss", -0.5, stop_width_pct=20.0 + (i % 3)) for i in range(10)]
        c = {x["attr"]: x for x in L.compare(recs)}["stop_width_pct"]
        self.assertGreater(abs(c["t"]), 10)
        self.assertTrue(c["verdict"].startswith("noted, too little data (n<30"))
        recs += [_rec("loss", -0.5, stop_width_pct=20.0 + (i % 3)) for i in range(25)]
        c = {x["attr"]: x for x in L.compare(recs)}["stop_width_pct"]
        self.assertEqual(c["verdict"], "worth a lab test")
        self.assertGreater(c["mde"], 0)
        same = {x["attr"]: x for x in L.compare(recs)}["hours_held"]
        self.assertIsNone(same["t"])                                     # no variance at all
        self.assertTrue(same["verdict"].startswith("noted"))
        self.assertTrue(same["outcome_linked"])

    def test_share_attrs_and_missing_values(self):
        recs = [_rec("win", 1.0, late_entry=None), _rec("win", 1.0, late_entry=True),
                _rec("loss", -1.0, late_entry=True), _rec("loss", -1.0, late_entry=True, trigger="dip")]
        c = {x["attr"]: x for x in L.compare(recs)}
        self.assertEqual((c["late_entry"]["n_win"], c["late_entry"]["mean_win"]), (1, 1.0))
        self.assertIn("trigger_dip", c)
        self.assertIn("exit_kill-switch", c)
        self.assertEqual(c["trigger_dip"]["mean_loss"], 0.5)

    def test_clustering_and_totals(self):
        recs = [_rec("loss", -3.0, exit_day="2026-09-23", cause="market"),
                _rec("loss", -1.0, exit_day="2026-09-15", exit_kind="stop"),
                _rec("loss", -0.5, exit_day="2026-09-10"),
                _rec("loss", -0.5, exit_day="2026-09-11"),
                _rec("win", 2.0, exit_day="2026-09-12")]
        a = L.aggregate(recs)
        self.assertEqual((a["n"], a["n_win"], a["n_loss"]), (5, 1, 4))
        self.assertAlmostEqual(a["loss_usd"], -5.0)
        self.assertAlmostEqual(a["net"], -3.0)
        self.assertEqual([d for d, _, _ in a["worst3_days"]][:2], ["2026-09-23", "2026-09-15"])
        self.assertAlmostEqual(a["worst3_share"], 4.5 / 5.0)
        self.assertEqual(a["loss_by_cause"]["market"]["n"], 1)
        self.assertAlmostEqual(a["loss_by_exit"]["stop"]["usd"], -1.0)
        self.assertEqual(a["loss_days"], 4)


class Weekly(unittest.TestCase):
    def test_plural_ru(self):
        f = ("сделка", "сделки", "сделок")
        self.assertEqual([L.plural_ru(n, f) for n in (1, 2, 5, 11, 12, 21, 32, 93, 111, 125)],
                         ["1 сделка", "2 сделки", "5 сделок", "11 сделок", "12 сделок", "21 сделка",
                          "32 сделки", "93 сделки", "111 сделок", "125 сделок"])

    def test_message_is_3_to_8_lines_and_says_nothing_changes(self):
        recs = [_rec("loss", -1.0, exit_day="2026-09-23", late_entry=True, cause="market", btc_ret=-0.03),
                _rec("loss", -0.4, exit_day="2026-09-22", exit_kind="stop", btc_ret=-0.01),
                _rec("loss", -0.1, exit_day="2026-09-22", btc_ret=0.02),
                _rec("win", 0.7, exit_day="2026-09-22")]
        lines = L.weekly_digest(recs, recs, "21–27.09")
        self.assertTrue(3 <= len(lines) <= 8, lines)
        text = "\n".join(lines)
        self.assertIn("4 сделки", lines[0])
        self.assertIn("−$1.50", text)            # losses of the week
        self.assertIn("23.09", text)                  # worst day
        self.assertIn("своя монета: 2 (−$0.50), из них при падающем BTC 1", text)
        self.assertIn("Поздний вход", text)
        self.assertIn("ничего не меняет", lines[-1])
        self.assertIn("Узоров для лабы нет", text)

    def test_lab_line_names_real_patterns_never_outcome_linked_ones(self):
        # winners all closed by hand, losers all stopped (outcome-linked, must NOT be offered to the lab);
        # stop width differs for real (an entry-time attribute, must be offered)
        recs = [_rec("win", 0.5, exit_kind="manual", stop_width_pct=5.0 + (i % 3)) for i in range(40)] + \
               [_rec("loss", -0.5, exit_kind="stop", stop_width_pct=20.0 + (i % 3)) for i in range(35)]
        text = "\n".join(L.weekly_digest(recs[:3], recs, "x"))
        self.assertIn("В лабу", text)
        self.assertIn("stop_width_pct", text)
        self.assertNotIn("exit_", text)

    def test_empty_week(self):
        lines = L.weekly_digest([], [], "01–07.10")
        self.assertTrue(2 <= len(lines) <= 8)
        self.assertIn("закрытых сделок нет", lines[0])


if __name__ == "__main__":
    unittest.main(verbosity=1)
