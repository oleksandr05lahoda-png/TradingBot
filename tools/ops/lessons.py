# -*- coding: utf-8 -*-
"""lessons.py - an honest loss autopsy of the live journal.  DIAGNOSTIC ONLY: nothing here
changes trading, sizes, filters or limits.  A self-tuning loop was measured twice and made the
book worse (it chased noise), so this module only EXPLAINS each closed trade with causes that
were fixed before the results were looked at, aggregates them, and flags a pattern as worth a
lab test only when the sample is big enough to say anything.

Pure functions, stdlib only, no network.  The caller supplies journal rows and market bars
(Binance kline arrays: [openTime_ms, open, high, low, close, ...]).

PRE-REGISTERED DEFINITIONS (fixed 23.09.2026 before any result was seen; do NOT tune them)
------------------------------------------------------------------------------------------
  cost            10 bp per side on notional (entry notional + exit notional); funding ignored
  net P&L         side * (exit - fill) * qty - cost;  loser = net < 0 (break-even counts as a win)
  M               BTC return over the holding window, entry ts -> exit ts, from BTC 1m bars
  beta            the coin's beta to BTC from the last 30 daily returns whose bars CLOSED before
                  the entry (both series on the same days); fewer than 20 returns -> beta = 1.0,
                  flagged beta_fallback
  r               the coin's own return over the holding window, fill -> exit price
  residual        r - beta * M
  cause (losers)  "market" if side*M < 0 and |beta*M| >= |residual|, else "coin"
  late_entry      side * (coin return over the 72h before entry) >= +10%
                  (price 72h before = the 1m bar at entry-72h; end = the fill)
  trigger         from signalId auto-<trigger>-SYM-stamp (trend / dip / short / ...)
  stop width %    |fill - stopPrice| / fill * 100 (missing on the oldest entries -> blank)
  exit kind       stop / take / max-hold / kill-switch / manual / other
  hours held      (exit ts - entry ts) / 3600
  BTC 3d return   BTC at entry vs BTC 72h earlier (1m bars)
  open_at_entry   other paired trades open at this entry: their entry <= this entry + 60 s
                  (the same scan pass counts as one bet) and their exit > this entry
  others_lost     how many of those open positions closed with net < 0
  loss day        UTC date of the exit
  lab-worthy      n >= 30 in BOTH groups and |Welch t| >= 2; otherwise "noted, too little data"
"""
import calendar
import json
import math
import re
import time

COST_BP_PER_SIDE = 10.0
LATE_ENTRY_RET72 = 0.10
BETA_WINDOW = 30
BETA_MIN_RETURNS = 20
BETA_FALLBACK = 1.0
SAME_PASS_SEC = 60
LAB_MIN_N = 30
LAB_MIN_T = 2.0
MDE_Z = 2.8          # 80% power, two-sided 5%: minimum detectable difference ~= 2.8 * SE

EXIT_KINDS = ("stop", "take", "max-hold", "kill-switch", "manual", "other")
# attributes that are DEFINED by the outcome (a stop exit is a loss by construction, a trade
# stopped out early is short by construction): a t on them describes the outcome, not a cause
OUTCOME_LINKED = ("hours_held", "others_lost") + tuple("exit_" + k for k in EXIT_KINDS)


# ---------------------------------------------------------------- journal ----
_TS = re.compile(r"^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d+)?Z?$")


def parse_ts(s):
    """ISO-8601 UTC with up to nanoseconds ('2026-08-30T18:06:05.512627300Z') -> epoch seconds."""
    m = _TS.match(str(s).strip())
    if not m:
        raise ValueError("bad timestamp: %r" % (s,))
    y, mo, d = (int(x) for x in m.group(1).split("-"))
    base = calendar.timegm((y, mo, d, int(m.group(2)), int(m.group(3)), int(m.group(4)), 0, 0, 0))
    frac = float("0" + m.group(5)) if m.group(5) else 0.0
    return base + frac


def utc_day(ts):
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def load_rows(lines):
    """jsonl lines -> (rows, bad_line_numbers).  Blank lines are skipped silently."""
    rows, bad = [], []
    for i, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
            r["_ts"] = parse_ts(r["ts"])
            r["_line"] = i
            rows.append(r)
        except (ValueError, KeyError, TypeError):
            bad.append(i)
    return rows, bad


def _num(x):
    """Positive float or None (missing, unparseable, zero or negative -> None)."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v > 0 and math.isfinite(v) else None


def trigger_of(signal_id):
    parts = str(signal_id or "").split("-")
    return parts[1] if len(parts) >= 3 and parts[0] == "auto" else "other"


def exit_kind(row):
    """Map a close / exchange-exit row onto the six pre-registered exit kinds."""
    if row.get("kind") == "close":
        reason = str(row.get("reason") or "")
        if reason == "max-hold":
            return "max-hold"
        if reason == "daily-loss-kill-switch":
            return "kill-switch"
        if reason.startswith("operator"):
            return "manual"
        return "other"
    if row.get("kind") == "exchange-exit":
        cause = str(row.get("cause") or "")
        return {"stop-loss": "stop", "take-profit": "take", "hand-close": "manual"}.get(cause, "other")
    return "other"


def side_sign(side):
    return -1 if str(side).upper() == "SHORT" else 1


def pair_trades(rows):
    """Pair each entry with the next exit of the same symbol, in time order.

    Returns (trades, unpaired).  An exit with no open entry, an entry followed by another entry
    before any exit, and an entry still open at the end all land in `unpaired` with a reason.
    A zero / missing exit price leaves exit_px None (the caller recovers it from bars and marks
    it approximate).  Rejected rows are not trades and are ignored.
    """
    ordered = sorted(rows, key=lambda r: (r["_ts"], r.get("_line", 0)))
    open_by_sym, trades, unpaired = {}, [], []
    for r in ordered:
        kind, sym = r.get("kind"), r.get("symbol")
        if kind == "entry":
            if sym in open_by_sym:
                unpaired.append({"row": open_by_sym[sym], "reason": "entry followed by another entry"})
            open_by_sym[sym] = r
        elif kind in ("close", "exchange-exit"):
            e = open_by_sym.pop(sym, None)
            if e is None:
                unpaired.append({"row": r, "reason": "exit without an entry in this journal"})
                continue
            qty = _num(e.get("qty"))
            xqty = _num(r.get("qty"))
            tps = e.get("tp") or []
            trades.append({
                "symbol": sym,
                "side": str(e.get("side") or "LONG").upper(),
                "qty": qty,
                "entry_ts": e["_ts"],
                "exit_ts": r["_ts"],
                "entry_px": _num(e.get("fill")),
                "exit_px": _num(r.get("price")),
                "exit_px_approx": False,
                "stop": _num(e.get("stopPrice")),
                "tp": _num(tps[0]) if tps else None,
                "risk_usd": _num(e.get("riskUsd")),
                "signal_id": e.get("signalId", ""),
                "trigger": trigger_of(e.get("signalId")),
                "exit_kind": exit_kind(r),
                "exit_reason": r.get("reason") or r.get("cause") or "",
                "qty_mismatch": xqty is not None and qty is not None and abs(xqty - qty) > 1e-12,
            })
    for sym, e in open_by_sym.items():
        unpaired.append({"row": e, "reason": "entry still open (no exit yet)"})
    return trades, unpaired


# ------------------------------------------------------------------ bars -----
def price_at(bars, ts, interval_sec):
    """Price at an instant from klines: the bar that contains ts -> its open if ts sits in the
    first half of the bar, else its close.  A ts after the last bar but within two intervals
    -> that bar's close.  Otherwise None."""
    if not bars:
        return None
    t_ms = ts * 1000.0
    step = interval_sec * 1000.0
    lo, hi = 0, len(bars) - 1
    if t_ms < float(bars[0][0]):
        return None
    while lo < hi:                                  # last bar with openTime <= t
        mid = (lo + hi + 1) // 2
        if float(bars[mid][0]) <= t_ms:
            lo = mid
        else:
            hi = mid - 1
    b = bars[lo]
    start = float(b[0])
    if t_ms < start + step:
        return float(b[1]) if (t_ms - start) < step / 2 else float(b[4])
    if t_ms < start + 2 * step:
        return float(b[4])
    return None


def beta_from_daily(coin_daily, btc_daily, entry_ts, window=BETA_WINDOW, min_n=BETA_MIN_RETURNS):
    """(beta, n_returns, fallback) from daily bars that closed before entry_ts."""
    cut = entry_ts * 1000.0
    day = 86400000.0

    def closed(bars):
        return {int(float(b[0])): float(b[4]) for b in bars if float(b[0]) + day - 1 < cut}

    c, b = closed(coin_daily), closed(btc_daily)
    days = sorted(set(c) & set(b))[-(window + 1):]
    rc, rb = [], []
    for d0, d1 in zip(days, days[1:]):
        if d1 - d0 != int(day):                     # a gap breaks the pair, never bridge it
            continue
        rc.append(c[d1] / c[d0] - 1.0)
        rb.append(b[d1] / b[d0] - 1.0)
    n = len(rb)
    if n < min_n:
        return BETA_FALLBACK, n, True
    mb, mc = sum(rb) / n, sum(rc) / n
    var = sum((x - mb) ** 2 for x in rb)
    if var <= 0:
        return BETA_FALLBACK, n, True
    cov = sum((x - mb) * (y - mc) for x, y in zip(rb, rc))
    return cov / var, n, False


# --------------------------------------------------------------- one trade ----
def trade_pnl(trade, cost_bp=COST_BP_PER_SIDE):
    s, q = side_sign(trade["side"]), trade["qty"]
    e, x = trade["entry_px"], trade["exit_px"]
    gross = s * (x - e) * q
    cost = cost_bp / 1e4 * (e * q + x * q)
    return {"gross": gross, "cost": cost, "net": gross - cost}


def classify(trade, ctx):
    """One closed trade + its market context -> the record of pre-registered attributes.

    ctx keys: btc_entry, btc_exit, btc_72h (BTC prices), coin_72h (coin price 72h before entry,
    may be None), beta, beta_n, beta_fallback."""
    s = side_sign(trade["side"])
    p = trade_pnl(trade)
    r = trade["exit_px"] / trade["entry_px"] - 1.0
    M = ctx["btc_exit"] / ctx["btc_entry"] - 1.0
    beta = ctx["beta"]
    resid = r - beta * M
    loss = p["net"] < 0
    cause = ""
    if loss:
        cause = "market" if (s * M < 0 and abs(beta * M) >= abs(resid)) else "coin"
    ret72 = trade["entry_px"] / ctx["coin_72h"] - 1.0 if ctx.get("coin_72h") else None
    stop_w = (abs(trade["entry_px"] - trade["stop"]) / trade["entry_px"] * 100.0) if trade.get("stop") else None
    rec = dict(trade)
    rec.update({
        "gross": p["gross"], "cost": p["cost"], "net": p["net"],
        "outcome": "loss" if loss else "win",
        "coin_ret": r, "btc_ret": M, "beta": beta, "beta_n": ctx.get("beta_n"),
        "beta_fallback": bool(ctx.get("beta_fallback")),
        "market_part": beta * M, "residual": resid, "cause": cause,
        "moved_with_btc": (r > 0) == (M > 0) and r != 0 and M != 0,
        "ret72": ret72,
        "late_entry": (s * ret72 >= LATE_ENTRY_RET72) if ret72 is not None else None,
        "stop_width_pct": stop_w,
        "hours_held": (trade["exit_ts"] - trade["entry_ts"]) / 3600.0,
        "btc_ret72": ctx["btc_entry"] / ctx["btc_72h"] - 1.0 if ctx.get("btc_72h") else None,
        "exit_day": utc_day(trade["exit_ts"]),
    })
    return rec


def crowding(records, same_pass_sec=SAME_PASS_SEC):
    """Adds open_at_entry / others_lost to every record (in place) and returns the list."""
    for i, a in enumerate(records):
        others = [b for j, b in enumerate(records)
                  if j != i and b["entry_ts"] <= a["entry_ts"] + same_pass_sec and b["exit_ts"] > a["entry_ts"]]
        a["open_at_entry"] = len(others)
        a["others_lost"] = sum(1 for b in others if b["net"] < 0)
    return records


def simulate_hold(side, qty, entry_px, stop, tp, bars_1m, from_ts, until_ts=None, honour_tp=True,
                  cost_bp=COST_BP_PER_SIDE):
    """What holding a position from from_ts would have given, with the resting stop (and take)
    honoured on 1m bars.  A bar that touches both stop and take counts as the STOP (conservative).
    Returns dict(exit_px, exit_ts, how in {'stop','take','open'}, net)."""
    s = side_sign(side)
    start_ms = math.floor(from_ts / 60.0) * 60000.0
    last = None
    for b in bars_1m:
        t0 = float(b[0])
        if t0 < start_ms:
            continue
        if until_ts is not None and t0 > until_ts * 1000.0:
            break
        hi, lo = float(b[2]), float(b[3])
        hit_stop = stop is not None and ((lo <= stop) if s > 0 else (hi >= stop))
        hit_tp = honour_tp and tp is not None and ((hi >= tp) if s > 0 else (lo <= tp))
        if hit_stop or hit_tp:
            px, how = (stop, "stop") if hit_stop else (tp, "take")
            return _held(s, qty, entry_px, px, t0 / 1000.0, how, cost_bp)
        last = b
    if last is None:
        return None
    return _held(s, qty, entry_px, float(last[4]), float(last[0]) / 1000.0 + 60, "open", cost_bp)


def _held(s, qty, entry_px, px, ts, how, cost_bp):
    net = s * (px - entry_px) * qty - cost_bp / 1e4 * (entry_px + px) * qty
    return {"exit_px": px, "exit_ts": ts, "how": how, "net": net}


# -------------------------------------------------------------- aggregate -----
def welch_t(a, b):
    """(mean_a - mean_b, t, se).  t/se None when either group has < 2 values or no variance."""
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return (sum(a) / na - sum(b) / nb) if na and nb else None, None, None
    ma, mb = sum(a) / na, sum(b) / nb
    va = sum((x - ma) ** 2 for x in a) / (na - 1)
    vb = sum((x - mb) ** 2 for x in b) / (nb - 1)
    se = math.sqrt(va / na + vb / nb)
    if se == 0:
        return ma - mb, None, 0.0
    return ma - mb, (ma - mb) / se, se


def _verdict(n_w, n_l, t):
    if n_w < LAB_MIN_N or n_l < LAB_MIN_N:
        return "noted, too little data (n<%d)" % LAB_MIN_N
    if t is None or abs(t) < LAB_MIN_T:
        return "noted, too little data (|t|<%g)" % LAB_MIN_T
    return "worth a lab test"


def comparison_attrs(records):
    """The pre-registered winner-vs-loser attributes: (name, kind, getter)."""
    attrs = [
        ("late_entry", "share", lambda r: r["late_entry"]),
        ("ret72_pct", "mean", lambda r: None if r["ret72"] is None else r["ret72"] * 100),
        ("stop_width_pct", "mean", lambda r: r["stop_width_pct"]),
        ("hours_held", "mean", lambda r: r["hours_held"]),
        ("btc_ret72_pct", "mean", lambda r: None if r["btc_ret72"] is None else r["btc_ret72"] * 100),
        ("open_at_entry", "mean", lambda r: r["open_at_entry"]),
        ("others_lost", "mean", lambda r: r["others_lost"]),
    ]
    for trig in sorted({r["trigger"] for r in records}):
        attrs.append(("trigger_" + trig, "share", lambda r, t=trig: r["trigger"] == t))
    for k in EXIT_KINDS:
        attrs.append(("exit_" + k, "share", lambda r, k=k: r["exit_kind"] == k))
    return attrs


def compare(records):
    """Winners vs losers on every attribute: means (shares for yes/no), difference, Welch t,
    n per group, the smallest difference this sample could detect, and the verdict."""
    out = []
    for name, kind, get in comparison_attrs(records):
        w = [get(r) for r in records if r["outcome"] == "win"]
        l = [get(r) for r in records if r["outcome"] == "loss"]
        w = [float(v) for v in w if v is not None]
        l = [float(v) for v in l if v is not None]
        diff, t, se = welch_t(l, w)
        out.append({
            "attr": name, "kind": kind,
            "n_win": len(w), "n_loss": len(l),
            "mean_win": sum(w) / len(w) if w else None,
            "mean_loss": sum(l) / len(l) if l else None,
            "diff_loss_minus_win": diff, "t": t,
            "mde": MDE_Z * se if se else None,
            "outcome_linked": name in OUTCOME_LINKED,
            "verdict": _verdict(len(w), len(l), t),
        })
    return out


def _sum_by(records, key):
    agg = {}
    for r in records:
        k = key(r)
        a = agg.setdefault(k, {"n": 0, "usd": 0.0})
        a["n"] += 1
        a["usd"] += r["net"]
    return agg


def aggregate(records):
    losers = [r for r in records if r["outcome"] == "loss"]
    winners = [r for r in records if r["outcome"] == "win"]
    loss_usd = sum(r["net"] for r in losers)
    by_day = _sum_by(losers, lambda r: r["exit_day"])
    worst = sorted(by_day.items(), key=lambda kv: kv[1]["usd"])[:3]
    worst_usd = sum(v["usd"] for _, v in worst)
    nets = [r["net"] for r in records]
    n = len(nets)
    mean = sum(nets) / n if n else 0.0
    sd = math.sqrt(sum((x - mean) ** 2 for x in nets) / (n - 1)) if n > 1 else 0.0
    return {
        "n": n, "n_win": len(winners), "n_loss": len(losers),
        "gross": sum(r["gross"] for r in records), "cost": sum(r["cost"] for r in records),
        "net": sum(nets), "win_usd": sum(r["net"] for r in winners), "loss_usd": loss_usd,
        "mean_net": mean, "sd_net": sd,
        "mde_mean_net": MDE_Z * sd / math.sqrt(n) if n > 1 else None,
        "loss_by_cause": _sum_by(losers, lambda r: r["cause"]),
        "loss_by_exit": _sum_by(losers, lambda r: r["exit_kind"]),
        "all_by_exit": _sum_by(records, lambda r: r["exit_kind"]),
        "loss_by_trigger": _sum_by(losers, lambda r: r["trigger"]),
        "all_by_trigger": _sum_by(records, lambda r: r["trigger"]),
        "loss_by_day": by_day,
        "worst3_days": [(d, v["usd"], v["n"]) for d, v in worst],
        "worst3_share": (worst_usd / loss_usd) if loss_usd < 0 else None,
        "loss_days": len(by_day),
        "comparisons": compare(records),
    }


# ------------------------------------------------------------ weekly text -----
def _usd(x):
    return ("+$%.2f" % x) if x >= 0 else ("−$%.2f" % -x)


def plural_ru(n, forms):
    """n + the Russian form: (1 сделка, 2 сделки, 5 сделок); 11-14 take the third form."""
    n = int(n)
    if n % 10 == 1 and n % 100 != 11:
        w = forms[0]
    elif 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        w = forms[1]
    else:
        w = forms[2]
    return "%d %s" % (n, w)


_DEALS = ("сделка", "сделки", "сделок")
_MINUS = ("минус", "минуса", "минусов")


def weekly_digest(week_records, all_records, label):
    """The weekly 'разбор минусов' message (plain text, 3-8 lines).  week_records = trades
    CLOSED in the week; all_records = the whole history, for the lab check."""
    lines = []
    wk = aggregate(week_records) if week_records else None
    if not wk or wk["n"] == 0:
        lines.append("\U0001F4CB Разбор минусов %s: закрытых сделок нет" % label)
    else:
        lines.append("\U0001F4CB Разбор минусов %s: %s, в минусе %d (%s), в плюсе %d (%s), итог %s"
                     % (label, plural_ru(wk["n"], _DEALS), wk["n_loss"], _usd(wk["loss_usd"]), wk["n_win"],
                        _usd(wk["win_usd"]), _usd(wk["net"])))
        if wk["n_loss"]:
            c = wk["loss_by_cause"]
            m, k = c.get("market", {"n": 0, "usd": 0.0}), c.get("coin", {"n": 0, "usd": 0.0})
            # "coin" is decided by SIZE (own move > BTC's part), not by direction: say how many of
            # them still fell together with BTC, so "своя монета" is not read as "unrelated to market"
            coin_btc_down = sum(1 for r in week_records
                                if r["outcome"] == "loss" and r["cause"] == "coin" and r.get("btc_ret", 0) < 0)
            lines.append("\U0001F30A Рынок (BTC утащил): %d (%s) · \U0001FA99 своя монета: %d (%s), "
                         "из них при падающем BTC %d"
                         % (m["n"], _usd(m["usd"]), k["n"], _usd(k["usd"]), coin_btc_down))
            ex = sorted(wk["loss_by_exit"].items(), key=lambda kv: kv[1]["usd"])
            lines.append("\U0001F6AA Как вышли минусы: " + ", ".join(
                "%s %d (%s)" % (_EXIT_RU.get(k2, k2), v["n"], _usd(v["usd"])) for k2, v in ex))
            d, usd, n = wk["worst3_days"][0]
            share = usd / wk["loss_usd"] * 100 if wk["loss_usd"] < 0 else 0
            lines.append("\U0001F4C5 Худший день %s: %s, %s (%.0f%% минусов недели)"
                         % (d[8:10] + "." + d[5:7], _usd(usd), plural_ru(n, _MINUS), share))
            late = [r for r in week_records if r["outcome"] == "loss" and r["late_entry"]]
            if late:
                lines.append("⏰ Поздний вход (+10%% за 72ч до покупки): %d из %d минусов"
                             % (len(late), wk["n_loss"]))
    allagg = aggregate(all_records) if all_records else None
    if allagg:
        lab = [c for c in allagg["comparisons"]
               if c["verdict"] == "worth a lab test" and not c["outcome_linked"]]
        if lab:
            lines.append("\U0001F52C В лабу (n≥30 и |t|≥2 на всей истории): "
                         + ", ".join("%s t=%.1f" % (c["attr"], c["t"]) for c in lab))
        else:
            lines.append("\U0001F52C Узоров для лабы нет (порог n≥30 и |t|≥2; за всю историю %s: "
                         "%d плюс / %d минус)" % (plural_ru(allagg["n"], _DEALS), allagg["n_win"], allagg["n_loss"]))
    lines.append("ℹ️ Разбор ничего не меняет в торговле")
    return lines[:8]


_EXIT_RU = {"stop": "стоп", "take": "тейк", "max-hold": "по времени", "kill-switch": "лимит дня",
            "manual": "руками", "other": "сигнал/прочее"}
