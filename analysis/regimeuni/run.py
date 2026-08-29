# -*- coding: utf-8 -*-
"""
Three owner hypotheses about regime and universe, measured under PREREG.md.

  H1  daily short switch      BTC down >3% in a day -> next day's book goes short
  H2  wider universe          top-100 vs top-300 vs top-500 by trailing liquidity
  H3  regime universe switch  BULL: top-100 as now; BEAR: low-BTC-correlation names

Baseline is the live bot's rule as closely as the archive allows (see PREREG).
Data and statistics are reused from analysis/tsmom: same 679-symbol survivorship-free
universe, same Newey-West alpha, same design/holdout split.

Everything is point-in-time: a filter for day D may only read bars strictly before D,
and a position opened on D earns the return of D+1. ASCII stdout only.
"""
import io
import json
import math
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
TSMOM = os.path.join(os.path.dirname(ROOT), "tsmom")
sys.path.insert(0, TSMOM)

import analyze as A  # noqa: E402  (path set above)

OUT = os.path.join(ROOT, "results.json")

# ---- live bot's parameters, from tools/scanner/autoscan.py and its Railway env ----
ENTRY_BAND = 0.02          # 30d return must exceed +2% to enter
EXIT_BAND = 0.02           # held while 30d return is above -2%
NEAR_HIGH = 0.05           # within 5% of the 20d high            (ENTRY_NEAR_HIGH)
VOL_MULT = 1.5             # today's volume >= 1.5x prior-20d mean (ENTRY_VOL_MULT)
MAX_SLOTS = 15             # MAX_POSITIONS
MIN_HOLD_DAYS = 1          # min_hold 24h
MAX_HOLD_DAYS = 2          # MAX_HOLD_HOURS=48
COOLDOWN_DAYS = 1          # cooldown 24h
ATR_PERIOD = 14
STOP_ATR_MULT = 2.0
TREND_LOOKBACK = 30
HIGH_LOOKBACK = 20
VOL_LOOKBACK = 20
MIN_HISTORY = 60
LIQ_FLOOR = 5e6            # $5m median 30d quote volume
COST_ROUND_TRIP = 0.0010   # 10bp per round trip, charged on turnover


def log(msg):
    print(msg)
    sys.stdout.flush()


# ---------- point-in-time features -------------------------------------------------

def build_features(closes, highs, lows, qvols, all_days):
    """Per symbol, per day: everything a signal on that day may use.

    Every value is computed from bars UP TO AND INCLUDING that day's close, which is
    when the live scanner reads them; the position it opens earns the NEXT day's return.
    """
    feats = {}
    for s in closes:
        c, h, l, q = closes[s], highs[s], lows[s], qvols[s]
        days = sorted(c)
        f = {}
        tr = []            # true ranges, aligned to days[1:]
        for i in range(len(days)):
            d = days[i]
            if i > 0:
                pc = c[days[i - 1]]
                tr.append(max(h[d] - l[d], abs(h[d] - pc), abs(l[d] - pc)))
            if i < MIN_HISTORY:
                continue
            price = c[d]
            if price <= 0:
                continue
            # trailing 30d return
            p0 = c[days[i - TREND_LOOKBACK]]
            if p0 <= 0:
                continue
            ret30 = price / p0 - 1.0
            # distance below the 20d high (the high of the last 20 closed bars incl. today)
            hi20 = max(h[days[j]] for j in range(i - HIGH_LOOKBACK + 1, i + 1))
            from_high = (hi20 - price) / hi20 if hi20 > 0 else None
            # today's quote volume against the prior-20d mean (prior: excludes today)
            prior_q = [q[days[j]] for j in range(i - VOL_LOOKBACK, i)]
            mean_q = sum(prior_q) / len(prior_q) if prior_q else 0.0
            vol_ratio = (q[d] / mean_q) if mean_q > 0 else None
            # ATR(14) over the last 14 true ranges (ending today)
            atr = sum(tr[-ATR_PERIOD:]) / ATR_PERIOD if len(tr) >= ATR_PERIOD else None
            # liquidity: median quote volume over the prior 30 days (excludes today)
            liq = A.median([q[days[j]] for j in range(i - 30, i)])
            f[d] = {"price": price, "ret30": ret30, "from_high": from_high,
                    "vol_ratio": vol_ratio, "atr": atr, "liq": liq}
        feats[s] = f
    return feats


def btc_trailing(closes, all_days, window):
    """BTC close-to-close return over the trailing `window` days, ending at each day."""
    c = closes["BTCUSDT"]
    days = sorted(c)
    idx = {d: i for i, d in enumerate(days)}
    out = {}
    for d in all_days:
        i = idx.get(d)
        if i is None or i < window:
            continue
        p0, p1 = c[days[i - window]], c[d]
        if p0 > 0:
            out[d] = p1 / p0 - 1.0
    return out


def rolling_corr_to_btc(rets, all_days, window=60):
    """corr(symbol daily returns, BTC daily returns) over the trailing `window` days.

    Computed on each day from returns strictly up to that day. Sampled every 5 days and
    carried forward - a 60d correlation does not move materially day to day, and the
    full daily computation over 679 symbols x 2500 days is not worth the time.
    """
    btc = rets["BTCUSDT"]
    out = {}
    sample_days = all_days[::5]
    for s, r in rets.items():
        if s == "BTCUSDT":
            continue
        sd = sorted(r)
        idx = {d: i for i, d in enumerate(sd)}
        vals = {}
        for d in sample_days:
            i = idx.get(d)
            if i is None or i < window:
                continue
            xs, ys = [], []
            for j in range(i - window + 1, i + 1):
                day = sd[j]
                if day in btc:
                    xs.append(r[day])
                    ys.append(btc[day])
            if len(xs) < window // 2:
                continue
            n = len(xs)
            mx, my = sum(xs) / n, sum(ys) / n
            sxy = sum((xs[k] - mx) * (ys[k] - my) for k in range(n))
            sxx = sum((xs[k] - mx) ** 2 for k in range(n))
            syy = sum((ys[k] - my) ** 2 for k in range(n))
            if sxx > 0 and syy > 0:
                vals[d] = sxy / math.sqrt(sxx * syy)
        out[s] = vals
    # carry forward onto every day
    carried = {}
    for s, vals in out.items():
        sd = sorted(vals)
        cur = {}
        vi = 0
        last = None
        for d in all_days:
            while vi < len(sd) and sd[vi] <= d:
                last = vals[sd[vi]]
                vi += 1
            if last is not None:
                cur[d] = last
        carried[s] = cur
    return carried


# ---------- the machine ------------------------------------------------------------

def simulate(feats, rets, funding, all_days, corr, btc_ret_w,
             universe_top=100, short_switch=None, regime_universe=False,
             start="2019-11-01", end="2026-08-14"):
    """One daily-rebalanced run. Returns [(day, net, gross)] for days in [start, end).

    universe_top      liquidity rank cut for the candidate pool
    short_switch      None, or (window, threshold): a BTC trailing return below the
                      threshold makes the NEXT day's book short
    regime_universe   H3: in BEAR (BTC 30d < 0) draw from the lowest-correlation quintile
                      of the top-300 instead of the top-100
    """
    idx = {d: i for i, d in enumerate(all_days)}
    held = {}          # symbol -> {"since": day_index, "weight": w}
    cooldown = {}      # symbol -> day_index when it became free again
    daily = []
    btc30 = btc_ret_w[30]
    switch_ret = btc_ret_w[short_switch[0]] if short_switch else None

    for i, d in enumerate(all_days):
        if d < start or d >= end or i + 1 >= len(all_days):
            continue
        nxt = all_days[i + 1]

        # --- which side and which pool, decided from data up to today's close ---
        side = 1
        if short_switch and switch_ret.get(d) is not None:
            if switch_ret[d] < short_switch[1]:
                side = -1
        bear = regime_universe and btc30.get(d) is not None and btc30[d] < 0

        # --- liquidity-ranked pool ---
        pool = []
        for s, f in feats.items():
            if s == "BTCUSDT":
                continue
            row = f.get(d)
            if row is None or row["liq"] < LIQ_FLOOR or row["atr"] is None:
                continue
            pool.append((row["liq"], s))
        pool.sort(reverse=True)
        top = universe_top
        if bear:
            top = 300
        ranked = [s for _, s in pool[:top]]

        if bear:
            # lowest-correlation quintile of the wider pool
            scored = [(corr.get(s, {}).get(d), s) for s in ranked]
            scored = [(c, s) for c, s in scored if c is not None]
            scored.sort()
            keep = max(1, len(scored) // 5)
            ranked = [s for _, s in scored[:keep]]

        allowed = set(ranked)

        # --- exits: hysteresis, min hold, max hold, and falling out of the pool ---
        for s in list(held):
            row = feats[s].get(d)
            age = i - held[s]["since"]
            if age < MIN_HOLD_DAYS:
                continue
            drop = False
            if row is None:
                drop = True                       # no data today: cannot judge, let it go
            elif age >= MAX_HOLD_DAYS:
                drop = True                       # MAX_HOLD_HOURS=48
            elif row["ret30"] < -EXIT_BAND:
                drop = True                       # hysteresis exit
            if drop:
                del held[s]
                cooldown[s] = i + COOLDOWN_DAYS

        # --- entries ---
        room = MAX_SLOTS - len(held)
        if room > 0:
            cands = []
            for s in ranked:
                if s in held or cooldown.get(s, -1) > i:
                    continue
                row = feats[s].get(d)
                if row is None or row["atr"] is None:
                    continue
                if row["price"] <= STOP_ATR_MULT * row["atr"]:
                    continue                      # stop would land at or below zero
                if row["ret30"] <= ENTRY_BAND:
                    continue
                if row["from_high"] is None or row["from_high"] > NEAR_HIGH:
                    continue
                if row["vol_ratio"] is None or row["vol_ratio"] < VOL_MULT:
                    continue
                cands.append((row["ret30"], s))
            cands.sort(reverse=True)               # strongest trend first, as the bot does
            for _, s in cands[:room]:
                held[s] = {"since": i, "weight": 0.0}

        if not held:
            daily.append((nxt, 0.0, 0.0))
            continue

        # --- weights: inverse stop distance == the bot's risk/stop-distance sizing ---
        raw = {}
        for s in held:
            row = feats[s].get(d)
            if row is None or row["atr"] is None or row["price"] <= 0:
                raw[s] = 0.0
                continue
            stop_frac = STOP_ATR_MULT * row["atr"] / row["price"]
            raw[s] = (1.0 / stop_frac) if stop_frac > 0 else 0.0
        tot = sum(raw.values())
        if tot <= 0:
            daily.append((nxt, 0.0, 0.0))
            continue
        new_w = {s: side * raw[s] / tot for s in held}

        # --- turnover cost against yesterday's book, then next day's return ---
        prev_w = {s: held[s]["weight"] for s in held}
        turnover = 0.0
        seen = set(new_w) | set(prev_w)
        for s in seen:
            turnover += abs(new_w.get(s, 0.0) - prev_w.get(s, 0.0))
        cost = turnover * COST_ROUND_TRIP

        gross = 0.0
        fund = 0.0
        for s, w in new_w.items():
            r = rets.get(s, {}).get(nxt)
            if r is not None:
                gross += w * r
            fr = funding.get(s, {}).get(nxt, 0.0)
            fund -= w * fr                        # long pays a positive rate, short receives
        daily.append((nxt, gross + fund - cost, gross))

        for s in held:
            held[s]["weight"] = new_w[s]

    return daily


# ---------- reporting ---------------------------------------------------------------

def window_stats(daily, btc_ret, lo, hi, label):
    return A.stats(daily, btc_ret, lo, hi, label)


def main():
    log("loading the tsmom cache (679 symbols, survivorship-free)...")
    closes, qvols, funding = A.load()
    # highs and lows are not in A.load(); read them alongside
    highs, lows = {}, {}
    uni = json.load(io.open(os.path.join(TSMOM, "cache", "universe_final.json"), encoding="utf-8"))
    for s in uni["live"] + uni["dead"]:
        kp = os.path.join(TSMOM, "cache", "klines_%s.json" % s)
        if not os.path.exists(kp) or s not in closes:
            continue
        h, l = {}, {}
        for r in json.load(io.open(kp, encoding="utf-8")):
            d = A.dstr(r[0])
            try:
                h[d] = float(r[2])
                l[d] = float(r[3])
            except (TypeError, ValueError):
                pass
        highs[s], lows[s] = h, l
    closes = {s: c for s, c in closes.items() if s in highs}
    all_days = A.series_days(closes)
    rets = A.build_returns(closes)
    btc_ret = rets["BTCUSDT"]
    log("symbols=%d days=%d  %s -> %s" % (len(closes), len(all_days), all_days[0], all_days[-1]))

    log("building point-in-time features...")
    feats = build_features(closes, highs, lows, qvols, all_days)
    btc_ret_w = {w: btc_trailing(closes, all_days, w) for w in (1, 2, 3, 30)}
    log("building 60d correlations to BTC...")
    corr = rolling_corr_to_btc(rets, all_days)

    runs = {
        "base_top100": dict(universe_top=100),
        "H1_short_1d_-3pct": dict(universe_top=100, short_switch=(1, -0.03)),
        "H1_var_1d_-2pct": dict(universe_top=100, short_switch=(1, -0.02)),
        "H1_var_1d_-4pct": dict(universe_top=100, short_switch=(1, -0.04)),
        "H1_var_2d_-3pct": dict(universe_top=100, short_switch=(2, -0.03)),
        "H1_var_3d_-3pct": dict(universe_top=100, short_switch=(3, -0.03)),
        "H2_top300": dict(universe_top=300),
        "H2_var_top500": dict(universe_top=500),
        "H3_regime_universe": dict(universe_top=100, regime_universe=True),
    }

    out = {}
    for name, kw in runs.items():
        log("running %s ..." % name)
        daily = simulate(feats, rets, funding, all_days, corr, btc_ret_w, **kw)
        row = {}
        for wlabel, lo, hi in (("design", "2000-01-01", A.DESIGN_END),
                               ("holdout", A.DESIGN_END, A.HOLDOUT_END)):
            st = window_stats(daily, btc_ret, lo, hi, name)
            row[wlabel] = st
            if st:
                log("   %-8s days=%4d net=%7.1f%%/y beta=%5.2f alpha=%7.1f%%/y t=%6.2f"
                    % (wlabel, st["days"], st["net_ann_pct"], st["beta"],
                       st["alpha_ann_pct"], st["t"]))
        out[name] = row

    json.dump(out, io.open(OUT, "w", encoding="utf-8"), indent=1, sort_keys=True)
    log("\nwrote %s" % OUT)

    log("\n=== VERDICT (pre-registered: holdout net alpha > 0 AND t >= 2.0, AND beats base) ===")
    base = out["base_top100"]["holdout"]
    log("  base_top100          alpha %7.1f%%/y  t %5.2f" % (base["alpha_ann_pct"], base["t"]))
    for name in ("H1_short_1d_-3pct", "H2_top300", "H3_regime_universe"):
        h = out[name]["holdout"]
        passes = (h["alpha_ann_pct"] > 0 and h["t"] >= 2.0
                  and h["alpha_ann_pct"] > base["alpha_ann_pct"])
        log("  %-20s alpha %7.1f%%/y  t %5.2f  -> %s"
            % (name, h["alpha_ann_pct"], h["t"], "PASS" if passes else "FAIL"))


if __name__ == "__main__":
    main()
