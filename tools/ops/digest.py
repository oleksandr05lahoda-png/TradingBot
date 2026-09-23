#!/usr/bin/env python3
# Daily digest (31.08, revised 03.09): one scheduled look instead of anxious refreshing. Read-only.
# 03.09: the journal counter counted EVERY row (rejections and closes included), so "29 of 100"
# was really 12 entries. The n>=100 verdict is judged on round trips, i.e. on entry rows.
# Also: each open position now shows its age and trigger, because a DIP entry has no time cap
# by design and its age is otherwise invisible until a self-check misreads it as a bug.
# 14.09 (audit 11.09, M3 + L2): a run that failed was indistinguishable from a run that never
# happened - env read with [], no try anywhere, cron output thrown away, and signatures on raw
# host time while selfcheck already corrected for clock skew. Now a failure is SAID: in Telegram
# when possible, and always in the cron log with the reason.
# 23.09: rebuilt to be read on a phone in five seconds. The first line is the machine's STATE, the
# money sits in one narrow table, and a 30-day chart (account against BTC) follows as a picture.
# The host has no matplotlib and must not grow a pip dependency for a report, so the PNG is drawn
# here in plain Python (zlib + a line rasterizer); numbers live in the caption, not on the axes.
# Every section is independent: a dead exchange, a missing log or a failed chart shrinks the
# digest and is said in it - it never silences the rest. Telegram gets HTML with every dynamic
# string escaped, and one plain-text resend if it still answers 400 (an alert is never lost to
# formatting). Nothing here can touch the trading account: two signed GETs and public klines.
#
# 24.09: NOT RUN ANY MORE. The owner removed the 18:00 line from crontab.txt: the same numbers came
# from the digest, the bot's menu and the lab's /panel, and he keeps only the panel in the lab bot
# (he asks for the current state there when he wants it). Kept in the repo, tested, unused; putting
# it back is one cron line - tools/ops/INSTALL.md, section "24.09: одна панель".
# Was run by cron at 18:00 host time (Europe/Berlin = Warsaw): /opt/digest.py >> .../ops/digest.log
# Offline checks: tools/ops/test_digest.py
import calendar, hashlib, hmac, html, io, json, os, re, struct, subprocess, sys, time, zlib
import urllib.error, urllib.parse, urllib.request

ENV_PATH = "/opt/tradingbot.env"
DATA = "/opt/tradingbot-data"
TG_LIMIT = 4096          # Telegram's hard cap for one message
CAPTION_LIMIT = 1024     # ...and for a photo caption
MINUS = "\u2212"

env = {}
# .get, not []: a key line commented out during a rotation killed the digest with a KeyError
# before it could say anything. Same fix selfcheck got on 06.09. Filled by main().
K, S = "", ""


def load_env(path):
    out = {}
    for ln in io.open(path, encoding="utf-8", errors="ignore"):
        s = ln.strip()
        if "=" in s and not s.startswith("#"):
            k, v = s.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log_err(msg):
    print("%s %s" % (utc_now(), msg), file=sys.stderr)


# The exchange's clock, not this host's. This block is selfcheck.py's, byte for byte: the midpoint
# formula the digest carried until 23.09 put timestamps ahead of the exchange on a slow handshake.
# test_digest.py fails the moment the two copies drift apart.
# --- clock:begin
_SKEW = [0]


def _resync():
    # The skew is taken against the moment the answer ARRIVED, not the midpoint of the call.
    # urlopen's time includes DNS, TCP and TLS set-up, all BEFORE the server stamps serverTime,
    # so the midpoint sat early and a slow handshake pushed the skew - and every timestamp -
    # ahead of the exchange: 23.09 11:00 "1000ms ahead of the server's time" on a host whose
    # NTP was in sync. Against the arrival the stamp can only lag by the one-way trip, and a
    # lagging timestamp is what recvWindow is for; a leading one over 1s is refused outright.
    try:
        with urllib.request.urlopen("https://fapi.binance.com/fapi/v1/time", timeout=15) as f:
            srv = json.loads(f.read().decode())["serverTime"]
        _SKEW[0] = int(srv - time.time() * 1000)
    except Exception:
        _SKEW[0] = 0


def call(path, extra="", _retried=False):
    q = "timestamp=%d&recvWindow=10000%s" % (int(time.time() * 1000) + _SKEW[0], extra)
    sig = hmac.new(S.encode(), q.encode(), hashlib.sha256).hexdigest()
    r = urllib.request.Request("https://fapi.binance.com%s?%s&signature=%s" % (path, q, sig),
                               headers={"X-MBX-APIKEY": K})
    try:
        with urllib.request.urlopen(r, timeout=25) as f:
            return json.loads(f.read().decode())
    except urllib.error.HTTPError as e:
        # str(HTTPError) is only "HTTP Error 400: Bad Request". The reason - the code and message
        # Binance puts in the BODY - was thrown away, so the 08.09 alert could not be diagnosed
        # at all, not even a day later. The body is what makes the alert worth sending.
        try:
            body = e.read().decode()[:200]
        except Exception:
            body = "<no body>"
        # -1021 says nothing about the book, only that this request's clock was off: measure the
        # skew again and ask once more. A second -1021 is real and is reported as before.
        if '"code":-1021' in body.replace(" ", "") and not _retried:
            _resync()
            return call(path, extra, _retried=True)
        raise RuntimeError("%s %s -> HTTP %s %s" % (path, extra, e.code, body)) from None
# --- clock:end


def public(path, params):
    url = "https://fapi.binance.com%s?%s" % (path, urllib.parse.urlencode(params))
    req = urllib.request.Request(url, headers={"User-Agent": "tradingbot-digest"})
    with urllib.request.urlopen(req, timeout=25) as f:
        return json.loads(f.read().decode())


# ---------------------------------------------------------------------------------------------
# Formatting: one vocabulary for every number the owner reads.
# ---------------------------------------------------------------------------------------------
def esc(s):
    """Every dynamic string goes through here before it meets parse_mode=HTML: a symbol, a log
    line or an exchange error with '<' in it would otherwise turn the whole digest into a 400."""
    return html.escape(str(s), quote=False)


def money(x, signed=True):
    """+$1.23 / \u2212$0.40 / $0.00; unsigned for a balance."""
    x = float(x)
    body = "$%.2f" % abs(x)
    if not signed:
        return (MINUS if x <= -0.005 else "") + body
    if abs(x) < 0.005:
        return "$0.00"
    return ("+" if x > 0 else MINUS) + body


def pct(x):
    x = float(x)
    if abs(x) < 0.05:
        return "0.0%"
    return ("+" if x > 0 else MINUS) + "%.1f%%" % abs(x)


def _last_sunday(year, month):
    last = calendar.monthrange(year, month)[1]
    wd = calendar.weekday(year, month, last)            # Monday=0 .. Sunday=6
    return last - (wd + 1) % 7


def warsaw_offset(ts):
    """Seconds east of UTC in Warsaw at epoch ts. zoneinfo when the host has tzdata; the EU rule
    otherwise (last Sunday of March 01:00 UTC to last Sunday of October 01:00 UTC is CEST) - the
    laptop running the tests has no tz database, and a report must not die for want of one."""
    try:
        from zoneinfo import ZoneInfo
        import datetime
        dt = datetime.datetime.fromtimestamp(ts, ZoneInfo("Europe/Warsaw"))
        return int(dt.utcoffset().total_seconds())
    except Exception:
        return eu_offset(ts)


def eu_offset(ts):
    y = time.gmtime(ts).tm_year
    start = calendar.timegm((y, 3, _last_sunday(y, 3), 1, 0, 0))
    end = calendar.timegm((y, 10, _last_sunday(y, 10), 1, 0, 0))
    return 7200 if start <= ts < end else 3600


def wtime(ts, fmt="%H:%M"):
    return time.strftime(fmt, time.gmtime(ts + warsaw_offset(ts)))


def short_sym(s):
    s = str(s)
    return s[:-4] if s.endswith("USDT") and len(s) > 4 else s


# ---------------------------------------------------------------------------------------------
# Money: /fapi/v1/income, split into trading results and money moved.
# ---------------------------------------------------------------------------------------------
# Trading results only. /fapi/v1/income also lists TRANSFER / INTERNAL_TRANSFER (deposits and
# withdrawals) and bonuses; summed blindly, a $200 deposit was "the day's profit" (06.09).
# INSURANCE_CLEAR is a loss the account took, so it stays on the trading side.
PNL_TYPES = ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE", "INSURANCE_CLEAR")
MOVE_TYPES = ("TRANSFER", "INTERNAL_TRANSFER", "WELCOME_BONUS")


def pnl_split(rows):
    """(trading P&L, money moved) over income rows."""
    day = sum(float(i["income"]) for i in rows if i.get("incomeType") in PNL_TYPES)
    moved = sum(float(i["income"]) for i in rows if i.get("incomeType") in MOVE_TYPES)
    return day, moved


def fetch_income(since_ms, fetch=None, max_pages=20):
    """Every income row since since_ms. One page is 1000 rows and 30 days of a 15-slot book with
    8-hourly funding is more than that, so it pages forward by time; a row seen on two pages (they
    share the boundary millisecond) is counted once."""
    fetch = fetch or (lambda start: call("/fapi/v1/income", "&startTime=%d&limit=1000" % start))
    rows, seen, start = [], set(), int(since_ms)
    for _ in range(max_pages):
        batch = fetch(start) or []
        fresh = []
        for r in batch:
            key = (r.get("incomeType"), r.get("tranId"), r.get("time"), r.get("income"), r.get("symbol"))
            if key not in seen:
                seen.add(key)
                fresh.append(r)
        rows.extend(fresh)
        if len(batch) < 1000 or not fresh:
            break
        start = max(int(r["time"]) for r in batch)
    return rows


def money_windows(rows, now):
    """Trading P&L over 24h / 7d / 30d, the 30-day fees and funding, and the day's transfers."""
    def since(sec, types):
        lo = (now - sec) * 1000
        return sum(float(r["income"]) for r in rows
                   if r.get("incomeType") in types and float(r.get("time", 0)) >= lo)
    return {
        "d1": since(86400, PNL_TYPES), "d7": since(7 * 86400, PNL_TYPES),
        "d30": since(30 * 86400, PNL_TYPES),
        "fees30": since(30 * 86400, ("COMMISSION",)),
        "funding30": since(30 * 86400, ("FUNDING_FEE",)),
        "moved1": since(86400, MOVE_TYPES),
    }


# ---------------------------------------------------------------------------------------------
# Local state: the journal, the scanner, the self-check, the container, the bot's own log.
# ---------------------------------------------------------------------------------------------
def journal_ts(text):
    """ISO-8601 with nanoseconds -> epoch seconds; None when unparseable."""
    import datetime
    try:
        t = (text or "").rstrip("Z")
        if "." in t:
            head, frac = t.split(".", 1)
            t = head + "." + (frac + "000000")[:6]
        return datetime.datetime.fromisoformat(t).replace(tzinfo=datetime.timezone.utc).timestamp()
    except (ValueError, TypeError):
        return None


# How a round trip ended, in the digest's vocabulary. exchange-exit rows carry a cause, the bot's
# own closes a reason; both are mapped so "5 closes" says HOW (stop, take, clock, hand).
# A liquidation and an exit nobody can explain are NOT folded into "other": Reconciler.ExitCause
# .expected() keeps them out of the calm alerts on purpose, and a digest that led with a green line on
# the day a position was liquidated would be the same lie one level up (review 23.09).
EXIT_MARKS = (("take", "\u2705"), ("stop", "\U0001F6D1"), ("time", "\u23F1"), ("hand", "\u270B"),
              ("limit", "\U0001F9EF"), ("signal", "сигнал"), ("liq", "ликвидация"), ("unclear", "неясно"),
              ("other", "прочее"))
UNCLEAR_CAUSES = ("unexplained", "closed-early", "closed-while-down")


def exit_kind(row):
    if row.get("kind") == "exchange-exit":
        cause = str(row.get("cause") or "")
        if cause == "liquidation":
            return "liq"
        if cause in UNCLEAR_CAUSES:
            return "unclear"
        if "take" in cause:
            return "take"
        if "stop" in cause:
            return "stop"
        if "hand" in cause or "manual" in cause:
            return "hand"
        return "other"
    reason = str(row.get("reason") or "")
    if reason == "max-hold":
        return "time"
    if reason.startswith("operator"):
        return "hand"
    if "kill-switch" in reason or "daily-loss" in reason:
        return "limit"
    if reason.endswith("-exited"):
        return "signal"
    return "other"


def journal_stats(lines, now):
    out = {"entries_total": 0, "entries_24h": 0, "closes_24h": 0, "exits": {}, "limit_at": None,
           "rejected_total": 0, "liquidated": [], "unclear": []}
    for ln in lines:
        try:
            row = json.loads(ln)
        except ValueError:
            continue
        if not isinstance(row, dict):
            continue
        kind = row.get("kind")
        ts = journal_ts(row.get("ts"))
        recent = ts is not None and ts >= now - 86400
        if kind == "entry":
            out["entries_total"] += 1
            out["entries_24h"] += recent
        elif kind == "rejected":
            out["rejected_total"] += 1
        elif kind in ("close", "exchange-exit") and recent:
            out["closes_24h"] += 1
            k = exit_kind(row)
            out["exits"][k] = out["exits"].get(k, 0) + 1
            if k == "liq":
                out["liquidated"].append(str(row.get("symbol") or "?"))
            elif k == "unclear":
                out["unclear"].append(str(row.get("symbol") or "?"))
        # The day-loss limit shows up as refusals, not as a halt the loop reports (23.09 14:43).
        if recent and (str(row.get("reason") or "").startswith("TRADING_HALTED: daily loss")
                       or row.get("reason") == "daily-loss-kill-switch"):
            out["limit_at"] = ts if out["limit_at"] is None else min(out["limit_at"], ts)
    return out


def selfcheck_stats(lines, now):
    """Runs of the last 24h from ops/selfcheck.log: how many, how many found something, and the
    newest line - which is what "now" means for the self-check."""
    runs = bad = 0
    last = None
    pat = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ) checks: (\d+) fail\(s\): (.*)$")
    for ln in lines:
        m = pat.match(ln.rstrip("\n"))
        if not m:
            continue
        try:
            ts = calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%SZ"))
        except ValueError:
            continue
        if ts < now - 86400:
            continue
        runs += 1
        n = int(m.group(2))
        bad += n > 0
        last = (ts, n, m.group(3).split(" | no HEARTBEAT")[0].split(" | pulse")[0])
    return {"runs": runs, "bad": bad, "last": last}


def scanner_queue(path, now):
    """Queue size from scanner_view.json, when the scanner writes one. The file is new (23.09) and
    its shape may still move, so every reasonable spelling is accepted and anything else is
    "unknown", never an exception. Older than 3h = the scanner is not refreshing it: said, not used."""
    try:
        view = json.load(io.open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(view, dict):
        return None
    n = None
    for key in ("queue_size", "queued", "queue", "candidates", "entry_ok"):
        v = view.get(key)
        if isinstance(v, bool):
            continue
        if isinstance(v, int):
            n = v
            break
        if isinstance(v, (list, dict)):
            n = len(v)
            break
    if n is None:
        return None
    ts = view.get("ts") or view.get("updated") or view.get("generated_at")
    age = None
    if isinstance(ts, (int, float)):
        age = now - (ts / 1000.0 if ts > 1e12 else ts)
    elif isinstance(ts, str):
        t = journal_ts(ts)
        age = None if t is None else now - t
    if age is None:
        try:
            age = now - os.path.getmtime(path)
        except OSError:
            age = None
    return {"n": n, "stale": age is not None and age > 3 * 3600}


def bot_state(log_tail):
    """(halted, reason) from the tail of the bot's log. The loop's own "[Loop] alive ... halt=YES"
    is the truth about NOW; the TradingHalt line only supplies the words."""
    halted, reason = False, ""
    for ln in log_tail:
        if "[Loop] alive" in ln:
            halted = "halt=YES" in ln
        m = re.search(r"\[TradingHalt\] HALTED at \S+: (.*?)(?: \u2014 no new positions.*)?$", ln)
        if m:
            reason = m.group(1)
        if "HALT CLEARED" in ln:
            reason = ""
    return halted, (reason if halted else "")


def read_tail(path, nbytes=262144):
    with io.open(path, "rb") as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell() - nbytes))
        return f.read().decode("utf-8", "ignore").splitlines()


def container_status():
    try:
        return subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}", "tradingbot"],
                              capture_output=True, text=True, timeout=30).stdout.strip() or "missing"
    except Exception:
        return "unknown"


def positions_summary(pos):
    """Open positions -> count, winners, losers, best, worst, and every row (worst first). The move
    is side-aware: a short that the price fell under is a winner (the 03.09 digest printed
    mark/entry for longs only)."""
    rows = []
    for p in pos:
        amt = float(p.get("positionAmt", 0) or 0)
        entry, mark = float(p.get("entryPrice", 0) or 0), float(p.get("markPrice", 0) or 0)
        side = 1 if amt > 0 else -1
        move = (mark / entry - 1) * 100 * side if entry > 0 else 0.0
        rows.append((p.get("symbol", "?"), move, float(p.get("unRealizedProfit", 0) or 0)))
    rows.sort(key=lambda r: r[1])
    return {"n": len(rows), "up": sum(1 for r in rows if r[2] > 0), "down": sum(1 for r in rows if r[2] < 0),
            "best": rows[-1] if rows else None, "worst": rows[0] if rows else None, "rows": rows}


# What opened a position, in four letters or five. The scanner's words; anything else is shown as is.
TRIG_LABEL = {"trend": "тренд", "dip": "дип", "short": "шорт"}


def age_label(sec):
    h = max(0, int(sec // 3600))
    return "%dч" % h if h < 1000 else "%dд" % (h // 24)


def book_rows(ps, scan_state, now, limit):
    """One phone-width row per open position, best first: symbol, move, $, trigger, age.
    The age and the trigger come from the scanner's autoscan_state.json ("entered" = when, "entered_trig"
    = why). A DIP entry has no time cap by design, so its age is the one number nothing else shows;
    a position the scanner never opened (the owner's hand trade, or an adoption that failed) is
    marked as someone else's - both were on the 03.09 digest and went missing on 23.09 (review).
    scan_state None = the file could not be read: no trigger, no age, and nobody is called foreign."""
    entered = trig = None
    if isinstance(scan_state, dict):
        entered = scan_state.get("entered") if isinstance(scan_state.get("entered"), dict) else {}
        trig = scan_state.get("entered_trig") if isinstance(scan_state.get("entered_trig"), dict) else {}
    out = []
    for sym, move, upnl in sorted(ps.get("rows") or [], key=lambda r: -r[1])[:limit]:
        label = age = ""
        if entered is not None:
            if sym in entered:
                t = str(trig.get(sym) or "")
                label = TRIG_LABEL.get(t, t[:5])
                try:
                    age = age_label(now - float(entered[sym]))
                except (TypeError, ValueError):
                    age = "?"
            else:
                label = "чужая"
        out.append(("%-8s%7s%8s %-5s%5s" % (short_sym(sym)[:8], pct(move), money(upnl), label, age)).rstrip())
    return out


# ---------------------------------------------------------------------------------------------
# The chart: 30 days, account vs BTC, as a PNG made here.
# ---------------------------------------------------------------------------------------------
BG, GRID, ZERO = (22, 24, 29), (40, 44, 52), (92, 98, 110)
ACCT, BTC = (79, 195, 247), (247, 147, 26)
UP_FILL, DOWN_FILL = (61, 220, 132), (239, 83, 80)


def png_encode(w, h, rgb):
    """Truecolor 8-bit PNG: signature, IHDR, one IDAT (filter 0 on every row), IEND."""
    if len(rgb) != w * h * 3:
        raise ValueError("pixel buffer is %d bytes, want %d" % (len(rgb), w * h * 3))

    def chunk(tag, data):
        return (struct.pack(">I", len(data)) + tag + data
                + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))
    stride = w * 3
    raw = bytearray()
    for y in range(h):
        raw.append(0)
        raw += rgb[y * stride:(y + 1) * stride]
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(bytes(raw), 9))
            + chunk(b"IEND", b""))


class Canvas:
    """RGB canvas drawn at SS x the output size and box-averaged down: anti-aliasing for free, in
    a few lines, at a cost of ~1 s on the VPS once a day."""
    SS = 2

    def __init__(self, w, h, bg):
        self.w, self.h = w * self.SS, h * self.SS
        self.ow, self.oh = w, h
        self.px = bytearray(bytes(bg) * (self.w * self.h))

    def put(self, x, y, c, a=1.0):
        if 0 <= x < self.w and 0 <= y < self.h:
            i = (y * self.w + x) * 3
            if a >= 1.0:
                self.px[i:i + 3] = bytes(c)
            else:
                p = self.px
                p[i] = int(p[i] + (c[0] - p[i]) * a)
                p[i + 1] = int(p[i + 1] + (c[1] - p[i + 1]) * a)
                p[i + 2] = int(p[i + 2] + (c[2] - p[i + 2]) * a)

    def hline(self, y, x0, x1, c, thick=1):
        for t in range(thick * self.SS):
            yy = int(y * self.SS) + t
            if 0 <= yy < self.h:
                a, b = max(0, int(x0 * self.SS)), min(self.w, int(x1 * self.SS))
                i = (yy * self.w + a) * 3
                self.px[i:i + (b - a) * 3] = bytes(c) * (b - a)

    def vline(self, x, y0, y1, c):
        for t in range(self.SS):
            xx = int(x * self.SS) + t
            for yy in range(max(0, int(y0 * self.SS)), min(self.h, int(y1 * self.SS))):
                self.put(xx, yy, c)

    def disc(self, x, y, r, c):
        cx, cy, rr = x * self.SS, y * self.SS, r * self.SS
        for yy in range(int(cy - rr), int(cy + rr) + 1):
            for xx in range(int(cx - rr), int(cx + rr) + 1):
                if (xx - cx) ** 2 + (yy - cy) ** 2 <= rr * rr:
                    self.put(xx, yy, c)

    def polyline(self, pts, c, width=2.5):
        r = width * self.SS / 2.0
        ri = int(r) + 1
        stamp = [(dx, dy) for dy in range(-ri, ri + 1) for dx in range(-ri, ri + 1) if dx * dx + dy * dy <= r * r]
        for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
            x0, y0, x1, y1 = x0 * self.SS, y0 * self.SS, x1 * self.SS, y1 * self.SS
            n = max(1, int(max(abs(x1 - x0), abs(y1 - y0))))
            for k in range(n + 1):
                x = int(round(x0 + (x1 - x0) * k / n))
                y = int(round(y0 + (y1 - y0) * k / n))
                for dx, dy in stamp:
                    self.put(x + dx, y + dy, c)

    def fill_to(self, pts, y_base, c_up, c_down, alpha):
        """Tint between a polyline and a baseline, column by column (above: c_up, below: c_down)."""
        if len(pts) < 2:
            return
        yb = y_base * self.SS
        for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
            a, b = int(x0 * self.SS), int(x1 * self.SS)
            for xx in range(a, max(a + 1, b)):
                t = (xx - a) / float(max(1, b - a))
                yl = (y0 + (y1 - y0) * t) * self.SS
                lo, hi = (int(yl), int(yb)) if yl < yb else (int(yb), int(yl))
                col = c_up if yl < yb else c_down
                for yy in range(lo, hi):
                    self.put(xx, yy, col, alpha)

    def to_rgb(self):
        """Box-average SS x SS blocks down to the output size (SS=2), a row pair at a time."""
        w, s3 = self.w, self.w * 3
        out = bytearray(self.ow * self.oh * 3)
        o3 = self.ow * 3
        for y in range(self.oh):
            a = self.px[(2 * y) * s3:(2 * y + 1) * s3]
            b = self.px[(2 * y + 1) * s3:(2 * y + 2) * s3]
            v = [p + q for p, q in zip(a, b)]
            row = bytearray(o3)
            for ch in range(3):
                row[ch::3] = bytes((l + r) >> 2 for l, r in zip(v[ch::6], v[3 + ch::6]))
            out[y * o3:(y + 1) * o3] = row
        return bytes(out)


def nice_step(span):
    """A grid step of 1/2/5 x 10^k percent giving 2-6 lines across the span."""
    span = max(span, 1e-9)
    raw = span / 4.0
    k = 10 ** int(("%e" % raw).split("e")[1])
    for m in (1, 2, 5, 10):
        if raw <= m * k:
            return m * k
    return 10 * k


def render_chart(acct, btc, w=800, h=450, pad=22):
    """acct, btc: [(epoch_s, percent)] over the same window; btc may be empty.
    Returns (png bytes, grid step in percent)."""
    if len(acct) < 2:
        raise ValueError("not enough account points for a chart (%d)" % len(acct))
    t0 = min(p[0] for p in acct + btc)
    t1 = max(p[0] for p in acct + btc)
    vals = [p[1] for p in acct + btc] + [0.0]
    lo, hi = min(vals), max(vals)
    span = hi - lo
    lo -= span * 0.08 + 0.1
    hi += span * 0.08 + 0.1
    step = nice_step(hi - lo)

    def X(t):
        return pad + (t - t0) / float(max(1, t1 - t0)) * (w - 2 * pad)

    def Y(v):
        return pad + (hi - v) / (hi - lo) * (h - 2 * pad)

    cv = Canvas(w, h, BG)
    g = step * int(lo / step)
    while g <= hi:
        if abs(g) > step / 1000 and lo <= g:
            cv.hline(Y(g), pad, w - pad, GRID)
        g += step
    # a faint tick every 7 days, counted back from now
    t = t1
    while t > t0:
        cv.vline(X(t), pad, h - pad, GRID)
        t -= 7 * 86400
    ap = [(X(t), Y(v)) for t, v in acct]
    cv.fill_to(ap, Y(0), UP_FILL, DOWN_FILL, 0.16)
    cv.hline(Y(0), pad, w - pad, ZERO)
    if len(btc) >= 2:
        bp = [(X(t), Y(v)) for t, v in btc]
        cv.polyline(bp, BTC, 2.0)
        cv.disc(bp[-1][0], bp[-1][1], 4, BTC)
    cv.polyline(ap, ACCT, 3.0)
    cv.disc(ap[-1][0], ap[-1][1], 5, ACCT)
    return png_encode(w, h, cv.to_rgb()), step


def chart_series(rows, klines, wallet_now, now):
    """The account as percent of its wallet at the window's start (trading P&L only - a deposit is
    not a return) and BTC as percent of its price then, on BTC's 4h grid."""
    if not klines:
        t0 = now - 30 * 86400
        grid = [t0 + i * 4 * 3600 for i in range(181) if t0 + i * 4 * 3600 <= now] + [now]
        btc = []
    else:
        t0 = klines[0][0] / 1000.0
        base = float(klines[0][1])
        grid = [t0] + [min(now, k[0] / 1000.0 + 4 * 3600) for k in klines]
        btc = [(t0, 0.0)] + [(min(now, k[0] / 1000.0 + 4 * 3600), (float(k[4]) / base - 1) * 100) for k in klines]
    after = sum(float(r["income"]) for r in rows if float(r.get("time", 0)) / 1000.0 > t0)
    w0 = wallet_now - after
    if w0 <= 0:
        w0 = wallet_now if wallet_now > 0 else 1.0
    pnl = sorted((float(r["time"]) / 1000.0, float(r["income"])) for r in rows
                 if r.get("incomeType") in PNL_TYPES and float(r.get("time", 0)) / 1000.0 > t0)
    acct, cum, i = [], 0.0, 0
    for t in grid:
        while i < len(pnl) and pnl[i][0] <= t:
            cum += pnl[i][1]
            i += 1
        acct.append((t, cum / w0 * 100))
    return acct, btc


# ---------------------------------------------------------------------------------------------
# The message.
# ---------------------------------------------------------------------------------------------
def pre_row(label, value, width=14, vwidth=11):
    return "%-*s%*s" % (width, label, vwidth, value)


def compose(now, *, ex=None, ex_error=None, journal=None, sc=None, queue=None,
            container="unknown", halted=False, halt_reason="", mode="", max_pos=0, chart_error=None,
            scan_state=None):
    """Everything the owner reads, in HTML. ex = exchange facts (None when it did not answer);
    scan_state = the scanner's autoscan_state.json (None when unreadable)."""
    problems, warns = [], []
    if container != "running":
        problems.append("контейнер бота: %s" % esc(container))
    if journal and journal.get("liquidated"):
        problems.append("ликвидация: %s" % esc(", ".join(short_sym(s) for s in journal["liquidated"])))
    if ex_error:
        problems.append("биржа не ответила")
    if sc is not None:
        if sc["runs"] == 0:
            problems.append("самопроверка не запускалась 24ч")
        elif sc["last"] and sc["last"][1] > 0:
            problems.append("самопроверка нашла проблему")
        elif sc["bad"]:
            warns.append("самопроверка: были замечания")
    if journal and journal.get("limit_at"):
        warns.append("сработал лимит дня")
    if journal and journal.get("unclear"):
        warns.append("неясное закрытие: %s" % esc(", ".join(short_sym(s) for s in journal["unclear"])))
    stamp = wtime(now, "%d.%m %H:%M")
    if problems:
        head = "\U0001F534 %s \u00B7 %s" % (problems[0][:1].upper() + problems[0][1:], stamp)
    elif halted:
        head = "\u23F8 Пауза: новые входы стоят \u00B7 %s" % stamp
    elif mode == "observe":
        head = "\U0001F441 Режим наблюдения \u00B7 %s" % stamp
    elif warns:
        head = "\U0001F7E0 %s \u00B7 %s" % (warns[0][:1].upper() + warns[0][1:], stamp)
    else:
        head = "\U0001F7E2 Всё в порядке \u00B7 %s" % stamp
    out = [head]
    for p in problems[1:]:
        out.append("\U0001F534 " + p)
    if halted and problems:
        out.append("⏸ Пауза: новые входы стоят")
    if halted and halt_reason:
        out.append("\u23F8 " + esc(halt_reason[:200]))

    out.append("")
    if ex is not None:
        w = ex["windows"]
        rows = [pre_row("Счёт", money(ex["wallet"] + ex["unreal"], signed=False)),
                pre_row(" плавающий", money(ex["unreal"])),
                pre_row("24 часа", money(w["d1"])),
                pre_row("7 дней", money(w["d7"])),
                pre_row("30 дней", money(w["d30"])),
                pre_row(" комиссии 30д", money(w["fees30"])),
                pre_row(" фандинг 30д", money(w["funding30"]))]
        if abs(w["moved1"]) >= 0.005:
            rows.append(pre_row("Переводы 24ч", money(w["moved1"])))
        out.append("\U0001F4B0 <b>Деньги</b> (итог сделок с комиссиями)")
        out.append("<pre>" + esc("\n".join(rows)) + "</pre>")
        ps = ex["positions"]
        cap = (" из %d" % max_pos) if max_pos else ""
        if ps["n"] == 0:
            out.append("\U0001F4D2 <b>Книга пуста</b> (0%s)" % cap)
        else:
            out.append("\U0001F4D2 <b>Книга %d%s</b> \u00B7 в плюсе %d, в минусе %d" % (ps["n"], cap, ps["up"], ps["down"]))
            limit = max_pos if max_pos > 0 else 15
            tbl = book_rows(ps, scan_state, now, limit)
            out.append("<pre>" + esc("\n".join(tbl)) + "</pre>")
            if ps["n"] > limit:
                out.append("\u2026 и ещё %d" % (ps["n"] - limit))
    else:
        out.append("\U0001F534 Биржа: %s" % esc((ex_error or "нет данных")[:300]))

    if journal is not None:
        out.append("Сделки за 24ч: входов %d, закрытий %d" % (journal["entries_24h"], journal["closes_24h"]))
        marks = ["%s %d" % (mark, journal["exits"][k]) for k, mark in EXIT_MARKS if journal["exits"].get(k)]
        if marks:
            out.append(" \u00B7 ".join(marks))
        # a liquidation is already a red line at the top; an unclear exit is orange, and an orange
        # head loses to any red one - so it is said here as well, where the closes are
        if journal.get("unclear"):
            out.append("\U0001F7E0 Закрытие без объяснения биржи: %s"
                       % esc(", ".join(short_sym(x) for x in journal["unclear"])))
        if journal.get("limit_at"):
            out.append("\U0001F9EF Лимит дня сработал в %s" % wtime(journal["limit_at"]))
    if queue is not None:
        out.append("\u23F3 Очередь сканера: %d%s" % (queue["n"], " (файл устарел)" if queue["stale"] else ""))
    if sc is not None:
        if sc["runs"] == 0:
            pass  # already the first line
        elif sc["last"] and sc["last"][1] > 0:
            out.append("\U0001F534 Самопроверка %s: %s" % (wtime(sc["last"][0]), esc(sc["last"][2][:300])))
        elif sc["bad"]:
            out.append("\U0001F7E0 Самопроверка: %d из %d с замечаниями, сейчас чисто" % (sc["bad"], sc["runs"]))
        else:
            out.append("\U0001F7E2 Самопроверка: %d из %d чисто" % (sc["runs"], sc["runs"]))
    if journal is not None:
        out.append("\U0001F4D2 Журнал: %d входов всего \u00B7 отказов %d"
                   % (journal["entries_total"], journal.get("rejected_total", 0)))
    if chart_error:
        out.append("\U0001F7E0 График не собрался: %s" % esc(chart_error[:200]))
    return "\n".join(out)


def caption(acct, btc, step):
    a = acct[-1][1] if acct else 0.0
    parts = ["\U0001F4CA 30 дней: счёт %s" % pct(a)]
    parts.append(" \u00B7 BTC %s" % pct(btc[-1][1]) if btc else " \u00B7 BTC нет данных")
    legend = "голубая — счёт, оранжевая — BTC"
    return "%s\n%s\nсетка через %g%%" % ("".join(parts), legend, step)


def plain(text):
    """The same message without markup, for the one resend after a 400."""
    return html.unescape(re.sub(r"</?(b|i|pre|code)>", "", text))


def fit(text, limit=TG_LIMIT):
    """(text, is_html). Over the limit it cannot be cut safely inside a tag, so it goes out as
    plain text cut at a line boundary - shorter, never lost."""
    if len(text) <= limit:
        return text, True
    p = plain(text)
    cut = p[:limit - 2]
    if "\n" in cut[limit // 2:]:
        cut = cut[:cut.rfind("\n")]
    return cut + "\n\u2026", False


# ---------------------------------------------------------------------------------------------
# Telegram. Failures are recorded by type and HTTP code only: the request URL carries the token.
# ---------------------------------------------------------------------------------------------
def _post(method, fields, files=None, opener=None):
    opener = opener or urllib.request.urlopen
    tok = env.get("TELEGRAM_BOT_TOKEN")
    url = "https://api.telegram.org/bot%s/%s" % (tok, method)
    if files:
        boundary = "----digest%d" % int(time.time() * 1000)
        body = bytearray()
        for k, v in fields.items():
            body += ("--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\n\r\n%s\r\n"
                     % (boundary, k, v)).encode("utf-8")
        for k, (name, data, ctype) in files.items():
            body += ("--%s\r\nContent-Disposition: form-data; name=\"%s\"; filename=\"%s\"\r\n"
                     "Content-Type: %s\r\n\r\n" % (boundary, k, name, ctype)).encode("utf-8")
            body += data + b"\r\n"
        body += ("--%s--\r\n" % boundary).encode("utf-8")
        req = urllib.request.Request(url, data=bytes(body),
                                     headers={"Content-Type": "multipart/form-data; boundary=%s" % boundary})
    else:
        req = urllib.request.Request(url, data=urllib.parse.urlencode(fields).encode())
    with opener(req, timeout=30) as f:
        f.read()


def tg_send(method, fields, html_key, files=None, opener=None):
    """True when Telegram took it. HTML first; a 400 (a formatting refusal) is resent ONCE as plain
    text, so a stray character can cost the markup but never the message."""
    tok, chat = env.get("TELEGRAM_BOT_TOKEN"), env.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        log_err("telegram delivery failed: no TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID in env")
        return False
    f = dict(fields, chat_id=chat, parse_mode="HTML")
    try:
        _post(method, f, files, opener)
        return True
    except urllib.error.HTTPError as e:
        if e.code != 400:
            log_err("telegram %s failed: HTTPError %s" % (method, e.code))
            return False
        log_err("telegram %s: 400 on HTML, resending as plain text" % method)
    except Exception as e:
        log_err("telegram %s failed: %s %s" % (method, type(e).__name__, getattr(e, "code", "") or ""))
        return False
    f = dict(fields, chat_id=chat)
    f[html_key] = plain(f[html_key])
    try:
        _post(method, f, files, opener)
        return True
    except Exception as e:
        log_err("telegram %s plain resend failed: %s %s" % (method, type(e).__name__, getattr(e, "code", "") or ""))
        return False


# ---------------------------------------------------------------------------------------------
def gather_exchange(now):
    """The two signed reads and the income history. Raises with the reason when the exchange is
    unusable; the caller says so in the digest and in the cron log."""
    if not K or not S:
        raise RuntimeError("в /opt/tradingbot.env нет ключей биржи")
    _resync()
    a = call("/fapi/v2/account")
    pos = [p for p in call("/fapi/v2/positionRisk") if abs(float(p["positionAmt"])) != 0]
    rows = fetch_income(int((now - 30 * 86400 - 4 * 3600) * 1000))
    return {"wallet": float(a["totalWalletBalance"]), "unreal": float(a["totalUnrealizedProfit"]),
            "positions": positions_summary(pos), "windows": money_windows(rows, now), "rows": rows}


def read_scan_state(path):
    """The scanner's state file, or None. Its own try: a missing or half-written file costs the
    book table its ages, never the digest."""
    try:
        st = json.load(io.open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return st if isinstance(st, dict) else None


def _lines(path):
    try:
        return io.open(path, encoding="utf-8", errors="ignore").readlines()
    except OSError:
        return None


def main():
    global env, K, S
    try:
        env = load_env(ENV_PATH)
    except OSError as e:
        log_err("digest FAILED: env file unreadable: %s" % type(e).__name__)
        return 1
    K, S = env.get("BINANCE_REAL_API_KEY", ""), env.get("BINANCE_REAL_API_SECRET", "")
    now = time.time()

    ex = ex_error = None
    try:
        ex = gather_exchange(now)
    except Exception as e:
        ex_error = "%s: %s" % (type(e).__name__, str(e)[:300])
        log_err("digest: exchange part FAILED: %s" % ex_error)

    jl = _lines(os.path.join(DATA, "trades.jsonl"))
    journal = journal_stats(jl, now) if jl is not None else None
    scl = _lines(os.path.join(DATA, "ops", "selfcheck.log"))
    sc = selfcheck_stats(scl, now) if scl is not None else None
    queue = scanner_queue(os.path.join(DATA, "scanner_view.json"), now)
    scan_state = read_scan_state(os.path.join(DATA, "autoscan_state.json"))
    try:
        halted, reason = bot_state(read_tail(os.path.join(DATA, "bot_real.err.log")))
    except OSError:
        halted, reason = False, ""
    try:
        max_pos = int(env.get("MAX_POSITIONS", "") or 0)
    except ValueError:
        max_pos = 0

    png = cap = chart_error = None
    if ex is not None:
        try:
            try:
                kl = public("/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "4h", "limit": 180})
            except Exception as e:
                log_err("digest: BTC klines failed (%s) - chart without BTC" % type(e).__name__)
                kl = []
            acct, btc = chart_series(ex["rows"], kl, ex["wallet"], now)
            png, step = render_chart(acct, btc)
            cap = caption(acct, btc, step)
        except Exception as e:
            chart_error = "%s: %s" % (type(e).__name__, str(e)[:150])
            log_err("digest: chart FAILED: %s" % chart_error)

    text = compose(now, ex=ex, ex_error=ex_error, journal=journal, sc=sc, queue=queue,
                   container=container_status(), halted=halted, halt_reason=reason,
                   mode=env.get("REAL_MODE", "").strip().lower(), max_pos=max_pos, chart_error=chart_error,
                   scan_state=scan_state)
    text, is_html = fit(text)
    if is_html:
        ok = tg_send("sendMessage", {"text": text}, "text")
    else:
        ok = tg_send_plain(text)
    if not ok:
        log_err("digest built but NOT delivered to Telegram")
        return 1
    if png is not None:
        if not tg_send("sendPhoto", {"caption": cap[:CAPTION_LIMIT]}, "caption",
                       files={"photo": ("digest.png", png, "image/png")}):
            log_err("digest: chart NOT delivered (the text was)")
    print("%s digest sent%s%s" % (utc_now(), "" if png is not None else " (no chart)",
                                   " [exchange part failed]" if ex_error else ""))
    return 1 if ex_error else 0


def tg_send_plain(text, opener=None):
    tok, chat = env.get("TELEGRAM_BOT_TOKEN"), env.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        log_err("telegram delivery failed: no TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID in env")
        return False
    try:
        _post("sendMessage", {"chat_id": chat, "text": text}, None, opener)
        return True
    except Exception as e:
        log_err("telegram sendMessage failed: %s %s" % (type(e).__name__, getattr(e, "code", "") or ""))
        return False


if __name__ == "__main__":
    sys.exit(main())
