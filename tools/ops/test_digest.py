# -*- coding: utf-8 -*-
"""Offline checks for digest.py (no network, no docker, no /opt).

digest.py is imported as a module (its work sits behind main()), and every door to the outside -
the exchange, the klines, docker, Telegram - is replaced by a fake, so what runs is the shipped code.

  1. the clock block is selfcheck.py's, byte for byte (the digest carried the old midpoint formula)
  2. the PNG is a real PNG: signature, IHDR, every chunk CRC (zlib.crc32), and it decodes back to
     the pixels drawn - background in the corner, the account's colour at the account's end dot
  3. a chart that fails does NOT stop the text: the text goes out, says so, no photo is sent
  4. an exchange that fails does NOT stop the text either: first line red, the reason in it, rc 1
  5. HTML: every dynamic string is escaped; no '<' survives outside the few tags the digest owns
  6. a 400 on the HTML message is resent ONCE as plain text; any other failure is not resent,
     and the bot token appears in no log line
  7. length: the message never exceeds 4096, the caption never 1024, every <pre> row fits a
     phone (34 chars)
  8. the first line always states the state (red / halt / observe / orange / green)
  9. numbers: +$1.23 / −$0.40 / $0.00, signed percent with one decimal, Warsaw time across DST
 10. money: income pages are followed and de-duplicated, a deposit is neither the day's profit
     nor a jump in the account's curve
 11. local readers: journal exits by kind, the day-loss limit, self-check runs, the scanner queue
     file in every shape (and garbage), the halt read from the loop's own line
 12. the book table: one row per position with its trigger and age from autoscan_state.json, "чужая"
     for a position the scanner never opened, capped at MAX rows; an unreadable state file costs the
     ages only (review 23.09: the rows had gone missing)
 13. a liquidation is a red first line, an exit the exchange cannot explain an orange one - never
     "прочее" under a green head (review 23.09); refusals are counted again

Run: py -3 tools/ops/test_digest.py
"""
import io, json, os, re, shutil, struct, sys, tempfile, time, urllib.error, zlib

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import digest as D  # noqa: E402

FAILED = []
PASSED = [0]
NOW = 1790190000.0          # 23.09.2026 19:00 UTC = 21:00 Warsaw (CEST)
TOKEN = "123456:SECRET-TOKEN-VALUE"


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name, (" - " + detail) if detail and not cond else ""))
    if cond:
        PASSED[0] += 1
    else:
        FAILED.append(name)


def block(src, tag):
    m = re.search(r"# --- %s:begin.*?\n(.*?)# --- %s:end" % (tag, tag), src, re.S)
    return m.group(1) if m else None


def read_png(data):
    """A minimal PNG reader: returns (w, h, rows of RGB bytes, chunk tags); raises on any defect."""
    assert data[:8] == b"\x89PNG\r\n\x1a\n", "bad signature"
    pos, tags, idat, ihdr = 8, [], b"", None
    while pos < len(data):
        (n,) = struct.unpack(">I", data[pos:pos + 4])
        tag, body = data[pos + 4:pos + 8], data[pos + 8:pos + 8 + n]
        (crc,) = struct.unpack(">I", data[pos + 8 + n:pos + 12 + n])
        assert crc == zlib.crc32(tag + body) & 0xFFFFFFFF, "bad CRC on %r" % tag
        tags.append(tag)
        if tag == b"IHDR":
            ihdr = struct.unpack(">IIBBBBB", body)
        elif tag == b"IDAT":
            idat += body
        pos += 12 + n
    w, h, depth, ctype, comp, filt, lace = ihdr
    assert (depth, ctype, comp, filt, lace) == (8, 2, 0, 0, 0), "unexpected IHDR %r" % (ihdr,)
    raw = zlib.decompress(idat)
    assert len(raw) == h * (1 + 3 * w), "IDAT is %d bytes, want %d" % (len(raw), h * (1 + 3 * w))
    rows = []
    for y in range(h):
        line = raw[y * (1 + 3 * w):(y + 1) * (1 + 3 * w)]
        assert line[0] == 0, "row %d filter %d" % (y, line[0])
        rows.append(line[1:])
    return w, h, rows, tags


def series():
    t0 = NOW - 180 * 4 * 3600
    kl = [[int((t0 + i * 4 * 3600) * 1000), "60000", "0", "0", str(60000 * (1 - 0.0005 * i))] for i in range(180)]
    rows = [{"incomeType": "REALIZED_PNL", "income": "0.10", "time": int((t0 + i * 3 * 3600) * 1000),
             "tranId": i, "symbol": "XUSDT"} for i in range(1, 200)]
    return kl, rows


def exchange_facts(n_pos=2):
    pos = [{"symbol": "PENGUUSDT", "positionAmt": "100", "entryPrice": "0.030", "markPrice": "0.031",
            "unRealizedProfit": "0.41"},
           {"symbol": "CCUSDT", "positionAmt": "-10", "entryPrice": "1.0", "markPrice": "1.018",
            "unRealizedProfit": "-0.22"}][:n_pos]
    kl, rows = series()
    return {"wallet": 144.30, "unreal": -1.20, "positions": D.positions_summary(pos),
            "windows": D.money_windows(rows, NOW), "rows": rows}


class Sent:
    """Records what would have gone to Telegram."""
    def __init__(self, fail_photo=False):
        self.calls, self.fail_photo = [], fail_photo

    def post(self, method, fields, files=None, opener=None):
        if method == "sendPhoto" and self.fail_photo:
            raise urllib.error.URLError("down")
        self.calls.append((method, dict(fields), files))


def run_main(tmp, *, gather=None, render=None, klines=None, sent=None, env_extra=""):
    """main() against a temp env file and data dir, with every outside door faked."""
    envp = os.path.join(tmp, "tradingbot.env")
    io.open(envp, "w", encoding="utf-8").write(
        "BINANCE_REAL_API_KEY=k\nBINANCE_REAL_API_SECRET=s\nTELEGRAM_BOT_TOKEN=%s\r\nTELEGRAM_CHAT_ID=42\n"
        "MAX_POSITIONS=15\n%s" % (TOKEN, env_extra))
    saved = {k: getattr(D, k) for k in ("ENV_PATH", "DATA", "gather_exchange", "public", "container_status",
                                         "_post", "render_chart")}
    err = io.StringIO()
    real_err = sys.stderr
    try:
        D.ENV_PATH, D.DATA = envp, tmp
        D.gather_exchange = gather or (lambda now: exchange_facts())
        D.public = lambda path, params: klines if klines is not None else series()[0]
        D.container_status = lambda: "running"
        sent = sent or Sent()
        D._post = sent.post
        if render:
            D.render_chart = render
        sys.stderr = err
        rc = D.main()
    finally:
        sys.stderr = real_err
        for k, v in saved.items():
            setattr(D, k, v)
    return rc, sent.calls, err.getvalue()


def main():
    # A Windows console is cp1251: a U+2212 minus in a check name must not crash the run that reports it.
    try:
        sys.stdout.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass
    print("digest - offline checks\n")
    tmp = tempfile.mkdtemp(prefix="digest-test-")
    os.makedirs(os.path.join(tmp, "ops"))
    try:
        _main(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\n%d passed, %d failed" % (PASSED[0], len(FAILED)))
    print("ALL CHECKS PASSED" if not FAILED else "FAILED: " + ", ".join(FAILED))
    return 1 if FAILED else 0


def _main(tmp):
    # 1. the clock
    sc_src = io.open(os.path.join(HERE, "selfcheck.py"), encoding="utf-8").read()
    dg_src = io.open(os.path.join(HERE, "digest.py"), encoding="utf-8").read()
    a, b = block(sc_src, "clock"), block(dg_src, "clock")
    check("clock block present in both scripts", a is not None and b is not None)
    check("digest's clock block == selfcheck's, byte for byte", a == b)

    # 2. the PNG
    kl, rows = series()
    acct, btc = D.chart_series(rows, kl, 150.0, NOW)
    png, step = D.render_chart(acct, btc)
    try:
        w, h, px, tags = read_png(png)
        check("PNG: signature, IHDR, CRCs and IDAT all valid", True)
    except AssertionError as e:
        check("PNG: signature, IHDR, CRCs and IDAT all valid", False, str(e))
        return
    check("PNG: chunks are IHDR, IDAT, IEND in order", tags == [b"IHDR", b"IDAT", b"IEND"], repr(tags))
    check("PNG: 800x450", (w, h) == (800, 450), "%sx%s" % (w, h))
    check("PNG: the corner is the background", bytes(px[0][0:3]) == bytes(D.BG), repr(px[0][0:3]))
    # the account's end dot sits at the right edge: some pixel near it must be the account colour
    hit = any(bytes(px[y][x * 3:x * 3 + 3]) == bytes(D.ACCT) for y in range(h) for x in range(w - 40, w))
    check("PNG: the account's line is drawn in its colour", hit)
    tiny = D.png_encode(2, 1, bytes([255, 0, 0, 0, 0, 255]))
    tw, th, trow, _ = read_png(tiny)
    check("PNG: a 2x1 image decodes to exactly the pixels given", bytes(trow[0]) == bytes([255, 0, 0, 0, 0, 255]))
    try:
        D.png_encode(2, 2, b"\x00" * 5)
        check("PNG: a short pixel buffer is refused", False)
    except ValueError:
        check("PNG: a short pixel buffer is refused", True)
    try:
        D.render_chart([(NOW, 1.0)], [])
        check("chart: one point is refused, not drawn as garbage", False)
    except ValueError:
        check("chart: one point is refused, not drawn as garbage", True)
    png2, _ = D.render_chart(acct, [])
    check("chart: draws without BTC (klines unavailable)", read_png(png2)[0] == 800)
    cap = D.caption(acct, btc, step)
    check("caption: numbers are in it and it fits 1024", "BTC" in cap and "%" in cap and len(cap) <= 1024, cap)

    # 3. a chart that fails does not stop the text
    def boom(*a, **k):
        raise MemoryError("no room for pixels")
    rc, calls, err = run_main(tmp, render=boom)
    methods = [c[0] for c in calls]
    check("chart failure: the text still goes out", methods == ["sendMessage"], repr(methods))
    check("chart failure: the text says the chart failed", "График не собрался" in calls[0][1]["text"])
    check("chart failure: rc 0 (the digest itself was delivered)", rc == 0, str(rc))
    check("chart failure: the cron log has the reason", "chart FAILED" in err and "MemoryError" in err, err)

    rc, calls, err = run_main(tmp)
    check("normal run: text, then the photo", [c[0] for c in calls] == ["sendMessage", "sendPhoto"],
          repr([c[0] for c in calls]))
    photo = calls[1][2]["photo"]
    check("normal run: the photo is a PNG with its caption", photo[1][:8] == b"\x89PNG\r\n\x1a\n"
          and "счёт" in calls[1][1]["caption"])
    check("normal run: HTML parse mode on the text", calls[0][1].get("parse_mode") == "HTML")
    check("normal run: rc 0", rc == 0, str(rc))

    rc, calls, err = run_main(tmp, sent=Sent(fail_photo=True))
    check("photo delivery failure: text delivered, rc 0, said in the log",
          [c[0] for c in calls] == ["sendMessage"] and rc == 0 and "chart NOT delivered" in err, err)

    # 4. an exchange that fails does not stop the text
    def dead(now):
        raise RuntimeError("/fapi/v2/account  -> HTTP 401 {\"code\":-2015,\"msg\":\"Invalid <API-key>\"}")
    rc, calls, err = run_main(tmp, gather=dead)
    text = calls[0][1]["text"] if calls else ""
    check("exchange failure: the text still goes out", [c[0] for c in calls] == ["sendMessage"], repr(calls))
    check("exchange failure: first line is red and says so", text.startswith("\U0001F534 Биржа не ответила"),
          text.split("\n")[0])
    check("exchange failure: the reason is in the text, escaped", "-2015" in text and "&lt;API-key&gt;" in text)
    check("exchange failure: rc 1 and the reason in the cron log", rc == 1 and "exchange part FAILED" in err, err)

    # 5. HTML escaping
    j = {"entries_total": 3, "entries_24h": 1, "closes_24h": 0, "exits": {}, "limit_at": None}
    sc = {"runs": 48, "bad": 1, "last": (NOW - 60, 1, "ПОЗИЦИЯ БЕЗ СТОПА: A<B>&C")}
    ex = exchange_facts()
    ex["positions"]["rows"].append(("<X>&USDT", 1.0, 0.5))
    t = D.compose(NOW, ex=ex, journal=j, sc=sc, container="running", halted=True,
                  halt_reason="daily loss <3%> & more", max_pos=15)
    bare = re.sub(r"</?(b|pre)>", "", t)
    check("escape: no raw '<' outside the digest's own tags", "<" not in bare and ">" not in bare, bare)
    check("escape: halt reason, self-check text and symbol escaped",
          "&lt;3%&gt; &amp; more" in t and "A&lt;B&gt;&amp;C" in t and "&lt;X&gt;&amp;" in t, t)

    # 6. Telegram: 400 -> one plain resend; other errors -> no resend; token never logged
    D.env = {"TELEGRAM_BOT_TOKEN": TOKEN, "TELEGRAM_CHAT_ID": "42"}
    seen = []

    class Reply:
        def read(self):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def opener_400_then_ok(req, timeout=None):
        seen.append(req)
        if len(seen) == 1:
            raise urllib.error.HTTPError(req.full_url, 400, "Bad Request", {}, io.BytesIO(b"can't parse entities"))
        return Reply()

    err = io.StringIO()
    real_err, sys.stderr = sys.stderr, err
    try:
        ok = D.tg_send("sendMessage", {"text": "<b>Сводка</b> &amp; x"}, "text", opener=opener_400_then_ok)
    finally:
        sys.stderr = real_err
    body2 = seen[1].data.decode() if len(seen) > 1 else ""
    check("400: resent once, delivered", ok and len(seen) == 2, str(len(seen)))
    check("400: the resend is plain (no parse_mode, no tags, entities undone)",
          "parse_mode" not in body2 and "%3Cb%3E" not in body2 and "%26+x" in body2, body2)
    check("400: the token is not in the log", TOKEN not in err.getvalue() and "SECRET" not in err.getvalue(), err.getvalue())

    seen.clear()

    def opener_500(req, timeout=None):
        seen.append(req)
        raise urllib.error.HTTPError(req.full_url, 502, "Bad Gateway", {}, io.BytesIO(b""))
    err = io.StringIO()
    real_err, sys.stderr = sys.stderr, err
    try:
        ok = D.tg_send("sendMessage", {"text": "x"}, "text", opener=opener_500)
    finally:
        sys.stderr = real_err
    check("502: not resent, reported as undelivered", ok is False and len(seen) == 1, str(len(seen)))
    check("502: the log has the code, not the URL", "502" in err.getvalue() and TOKEN not in err.getvalue(), err.getvalue())

    seen.clear()
    D.tg_send("sendPhoto", {"caption": "c"}, "caption", files={"photo": ("digest.png", png, "image/png")},
              opener=lambda req, timeout=None: (seen.append(req), Reply())[1])
    mp = seen[0].data if seen else b""
    check("sendPhoto: multipart carries the PNG bytes, the chat and the caption",
          png in mp and b'name="chat_id"\r\n\r\n42' in mp and b'filename="digest.png"' in mp
          and "multipart/form-data" in seen[0].get_header("Content-type", ""))

    # 7. length
    huge = {"runs": 48, "bad": 48, "last": (NOW, 99, "x" * 20000)}
    t = D.compose(NOW, ex=exchange_facts(), journal=j, sc=huge, container="exited", halted=True,
                  halt_reason="y" * 20000, max_pos=15, chart_error="z" * 20000)
    f, is_html = D.fit(t)
    check("length: a normal-shaped digest is well under 4096", len(t) < 2000, str(len(t)))
    big = t + "\n" + ("строка очень длинная <b>\n" * 400)
    f, is_html = D.fit(big)
    check("length: an oversized message is cut to <= 4096 and sent plain", len(f) <= 4096 and not is_html, str(len(f)))
    state = {"entered": {"PENGUUSDT": NOW - 30 * 3600, "CCUSDT": NOW - 5000 * 3600},
             "entered_trig": {"PENGUUSDT": "dip", "CCUSDT": "short"}}
    t = D.compose(NOW, ex=exchange_facts(), journal=j, sc=sc, container="running", max_pos=15, scan_state=state)
    pre_rows = [ln for chunk in re.findall(r"<pre>(.*?)</pre>", t, re.S) for ln in D.html.unescape(chunk).split("\n")]
    check("phone width: every <pre> row is <= 34 chars", pre_rows and max(len(r) for r in pre_rows) <= 34,
          repr(max(pre_rows, key=len)))

    # 8. the first line states the state
    def head(**kw):
        return D.compose(NOW, ex=exchange_facts(), journal=j, sc={"runs": 48, "bad": 0, "last": (NOW, 0, "ок")},
                         **kw).split("\n")[0]
    check("state: all well -> green", head(container="running").startswith("\U0001F7E2 Всё в порядке"))
    check("state: container down -> red, named", head(container="exited").startswith("\U0001F534 Контейнер бота: exited"))
    check("state: halted -> pause", head(container="running", halted=True).startswith("⏸ Пауза"))
    check("state: observe mode -> eye", head(container="running", mode="observe").startswith("\U0001F441"))
    jl = dict(j, limit_at=NOW - 3600)
    check("state: the day limit tripped -> orange",
          D.compose(NOW, ex=exchange_facts(), journal=jl, sc={"runs": 48, "bad": 0, "last": (NOW, 0, "ок")},
                    container="running").startswith("\U0001F7E0 Сработал лимит дня"))
    check("state: no self-check in 24h -> red",
          D.compose(NOW, ex=exchange_facts(), journal=j, sc={"runs": 0, "bad": 0, "last": None},
                    container="running").startswith("\U0001F534 Самопроверка не запускалась"))
    check("state: the stamp is Warsaw time", "23.09 21:00" in head(container="running"), head(container="running"))
    t = D.compose(NOW, ex=exchange_facts(0), journal=j, sc=sc, container="running", max_pos=15)
    check("empty book is said as such", "Книга пуста</b> (0 из 15)" in t)
    t = D.compose(NOW, ex=exchange_facts(1), journal=j, sc=sc, container="running", max_pos=15)
    check("one position: one row, not called best or worst",
          "PENGU" in t and "лучшая" not in t and "худшая" not in t and "Книга 1 из 15" in t, t)

    # 12. the book table
    state = {"entered": {"PENGUUSDT": NOW - 30 * 3600 - 60}, "entered_trig": {"PENGUUSDT": "dip"}}
    t = D.compose(NOW, ex=exchange_facts(), journal=j, sc=sc, container="running", max_pos=15, scan_state=state)
    tbl = D.html.unescape(re.findall(r"<pre>(.*?)</pre>", t, re.S)[-1]).split("\n")
    check("book: one row per position, best first", len(tbl) == 2 and tbl[0].startswith("PENGU")
          and tbl[1].startswith("CC"), repr(tbl))
    check("book: the scanner's DIP shows its trigger and its age (30ч)",
          "+3.3%" in tbl[0] and "+$0.41" in tbl[0] and "дип" in tbl[0] and tbl[0].endswith("30ч"), repr(tbl[0]))
    check("book: a position the scanner never opened is someone else's",
          "\u22121.8%" in tbl[1] and "чужая" in tbl[1], repr(tbl[1]))
    check("book: the rows fit a phone (<= 34)", max(len(r) for r in tbl) <= 34, repr(tbl))
    t = D.compose(NOW, ex=exchange_facts(), journal=j, sc=sc, container="running", max_pos=15, scan_state=None)
    tbl = D.html.unescape(re.findall(r"<pre>(.*?)</pre>", t, re.S)[-1]).split("\n")
    check("book: no state file -> rows without age, nobody called foreign",
          len(tbl) == 2 and "чужая" not in t and "дип" not in t, repr(tbl))
    t = D.compose(NOW, ex=exchange_facts(), journal=j, sc=sc, container="running", max_pos=1, scan_state=state)
    tbl = D.html.unescape(re.findall(r"<pre>(.*?)</pre>", t, re.S)[-1]).split("\n")
    check("book: capped at MAX rows, the rest counted", len(tbl) == 1 and "и ещё 1" in t, repr(tbl))
    check("age: hours, and days past 999 hours", (D.age_label(3599), D.age_label(30 * 3600), D.age_label(1200 * 3600),
                                                 D.age_label(-50)) == ("0ч", "30ч", "50д", "0ч"))
    io.open(os.path.join(tmp, "autoscan_state.json"), "w", encoding="utf-8").write(json.dumps(state))
    rc, calls, err = run_main(tmp)
    check("main: the scanner's state file is read (trigger and 'чужая' in the digest)",
          rc == 0 and "дип" in calls[0][1]["text"] and "чужая" in calls[0][1]["text"], calls[0][1]["text"] if calls else "")
    io.open(os.path.join(tmp, "autoscan_state.json"), "w", encoding="utf-8").write("{half a fi")
    rc, calls, err = run_main(tmp)
    check("main: a broken state file costs the ages only, the digest goes out",
          rc == 0 and [c[0] for c in calls] == ["sendMessage", "sendPhoto"] and "PENGU" in calls[0][1]["text"]
          and "чужая" not in calls[0][1]["text"], repr([c[0] for c in calls]))
    os.remove(os.path.join(tmp, "autoscan_state.json"))

    # 13. exits the exchange did not explain
    def iso13(ts):
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)) + ".5Z"
    rows13 = [{"kind": "exchange-exit", "cause": "liquidation", "symbol": "SOLUSDT", "ts": iso13(NOW - 900)},
              {"kind": "exchange-exit", "cause": "unexplained", "symbol": "AUSDT", "ts": iso13(NOW - 900)},
              {"kind": "exchange-exit", "cause": "closed-early", "symbol": "BUSDT", "ts": iso13(NOW - 900)},
              {"kind": "exchange-exit", "cause": "closed-while-down", "symbol": "CUSDT", "ts": iso13(NOW - 900)},
              {"kind": "exchange-exit", "cause": "liquidation", "symbol": "OLDUSDT", "ts": iso13(NOW - 3 * 86400)},
              {"kind": "rejected", "reason": "book full", "ts": iso13(NOW - 900)},
              {"kind": "rejected", "reason": "stale", "ts": iso13(NOW - 5 * 86400)}]
    j13 = D.journal_stats([json.dumps(r) for r in rows13], NOW)
    check("journal: a liquidation is its own kind, never 'other'",
          j13["exits"].get("liq") == 1 and j13["liquidated"] == ["SOLUSDT"] and "other" not in j13["exits"], repr(j13))
    check("journal: unexplained / closed-early / closed-while-down are 'unclear'",
          j13["exits"].get("unclear") == 3 and j13["unclear"] == ["AUSDT", "BUSDT", "CUSDT"], repr(j13))
    check("journal: refusals counted (all time)", j13["rejected_total"] == 2, repr(j13))
    ok_sc = {"runs": 48, "bad": 0, "last": (NOW, 0, "ок")}
    t = D.compose(NOW, ex=exchange_facts(), journal=j13, sc=ok_sc, container="running")
    check("liquidation: the first line is red and names the coin",
          t.startswith("\U0001F534 Ликвидация: SOL \u00B7"), t.split("\n")[0])
    check("liquidation: counted among the trades, said once, and the orange line under a red head is not lost",
          t.count("Ликвидация: SOL") == 1 and "\U0001F7E0 Закрытие без объяснения биржи: A, B, C" in t
          and "ликвидация 1" in t and "неясно 3" in t, t)
    j_unclear = dict(j13, liquidated=[], exits={"unclear": 3})
    t = D.compose(NOW, ex=exchange_facts(), journal=j_unclear, sc=ok_sc, container="running")
    check("unclear exit alone: the first line is orange, never green",
          t.startswith("\U0001F7E0 Неясное закрытие: A, B, C"), t.split("\n")[0])
    check("refusals are on the journal line", "отказов 2" in t, t)
    j_esc = dict(j13, liquidated=["<X>&USDT"], unclear=[])
    t = D.compose(NOW, ex=exchange_facts(), journal=j_esc, sc=ok_sc, container="running")
    check("liquidation: the symbol is escaped", "&lt;X&gt;&amp;" in t and "<X>" not in t, t.split("\n")[0])

    # 9. numbers and time
    check("money: +$1.23 / −$0.40 / $0.00 / $143.10",
          (D.money(1.234), D.money(-0.4), D.money(-0.001), D.money(143.1, signed=False))
          == ("+$1.23", "−$0.40", "$0.00", "$143.10"))
    check("pct: +1.2% / −0.4% / 0.0%", (D.pct(1.24), D.pct(-0.44), D.pct(0.01)) == ("+1.2%", "−0.4%", "0.0%"))
    import calendar
    t_before = calendar.timegm((2026, 10, 25, 0, 59, 0))
    t_after = calendar.timegm((2026, 10, 25, 1, 0, 0))
    t_spring = calendar.timegm((2026, 3, 29, 1, 0, 0))
    check("EU rule: CEST until 25.10.2026 01:00 UTC, CET after, CEST from 29.03 01:00 UTC",
          (D.eu_offset(t_before), D.eu_offset(t_after), D.eu_offset(t_spring - 1), D.eu_offset(t_spring))
          == (7200, 3600, 3600, 7200))
    check("wtime: 23.09 19:00 UTC is 21:00 in Warsaw", D.wtime(NOW) == "21:00", D.wtime(NOW))
    check("short symbols: PENGUUSDT -> PENGU, USDT stays", (D.short_sym("PENGUUSDT"), D.short_sym("USDT")) == ("PENGU", "USDT"))
    ps = D.positions_summary([{"symbol": "S", "positionAmt": "-1", "entryPrice": "100", "markPrice": "90",
                               "unRealizedProfit": "10"}])
    check("a short the price fell under is a winner (+10.0%)", ps["up"] == 1 and D.pct(ps["best"][1]) == "+10.0%",
          repr(ps))

    # 10. money
    pages = [[{"incomeType": "REALIZED_PNL", "income": "1", "time": 1000 + i, "tranId": i} for i in range(1000)],
             [{"incomeType": "REALIZED_PNL", "income": "1", "time": 1999, "tranId": 999}]
             + [{"incomeType": "COMMISSION", "income": "-0.1", "time": 2000 + i, "tranId": 5000 + i} for i in range(10)]]
    asked = []
    got = D.fetch_income(1000, fetch=lambda start: (asked.append(start), pages[len(asked) - 1])[1])
    check("income: a full page is followed from its last time", asked == [1000, 1999], repr(asked))
    check("income: the boundary row is counted once", len(got) == 1010, str(len(got)))
    rows = [{"incomeType": "REALIZED_PNL", "income": "2", "time": int((NOW - 3600) * 1000)},
            {"incomeType": "TRANSFER", "income": "200", "time": int((NOW - 1800) * 1000)},
            {"incomeType": "COMMISSION", "income": "-0.5", "time": int((NOW - 3 * 86400) * 1000)},
            {"incomeType": "FUNDING_FEE", "income": "-0.25", "time": int((NOW - 10 * 86400) * 1000)}]
    w = D.money_windows(rows, NOW)
    check("windows: a $200 deposit is not the day's profit",
          abs(w["d1"] - 2) < 1e-9 and abs(w["moved1"] - 200) < 1e-9, repr(w))
    check("windows: 7d and 30d add the older rows, fees and funding split out",
          abs(w["d7"] - 1.5) < 1e-9 and abs(w["d30"] - 1.25) < 1e-9 and abs(w["fees30"] + 0.5) < 1e-9
          and abs(w["funding30"] + 0.25) < 1e-9, repr(w))
    kl = [[int((NOW - (3 - i) * 4 * 3600) * 1000), "100", "0", "0", "100"] for i in range(3)]
    dep = [{"incomeType": "TRANSFER", "income": "100", "time": int((NOW - 5 * 3600) * 1000)}]
    acct, _ = D.chart_series(dep, kl, 200.0, NOW)
    check("curve: a deposit mid-window is not a return (flat at 0%)", all(abs(v) < 1e-9 for _, v in acct), repr(acct))
    win = [{"incomeType": "REALIZED_PNL", "income": "10", "time": int((NOW - 5 * 3600) * 1000)}]
    acct, _ = D.chart_series(win, kl, 110.0, NOW)
    check("curve: +$10 on a $100 start reads +10%", abs(acct[-1][1] - 10.0) < 1e-9 and abs(acct[0][1]) < 1e-9, repr(acct))

    # 11. local readers
    def iso(ts):
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)) + ".123456789Z"
    lines = [json.dumps(r) for r in [
        {"kind": "entry", "ts": iso(NOW - 3600)}, {"kind": "entry", "ts": iso(NOW - 3 * 86400)},
        {"kind": "close", "reason": "max-hold", "ts": iso(NOW - 100)},
        {"kind": "close", "reason": "operator-lock-in", "ts": iso(NOW - 100)},
        {"kind": "close", "reason": "trend-exited", "ts": iso(NOW - 100)},
        {"kind": "exchange-exit", "cause": "stop-loss", "ts": iso(NOW - 100)},
        {"kind": "exchange-exit", "cause": "take-profit", "ts": iso(NOW - 100)},
        {"kind": "exchange-exit", "cause": "hand-close", "ts": iso(NOW - 100)},
        {"kind": "close", "reason": "max-hold", "ts": iso(NOW - 2 * 86400)},
        {"kind": "rejected", "reason": "TRADING_HALTED: daily loss 3.37% ...", "ts": iso(NOW - 7200)},
    ]] + ["not json", "[]"]
    js = D.journal_stats(lines, NOW)
    check("journal: entries total / 24h", (js["entries_total"], js["entries_24h"]) == (2, 1), repr(js))
    check("journal: closes by kind (time, hand x2, signal, stop, take)",
          js["closes_24h"] == 6 and js["exits"] == {"time": 1, "hand": 2, "signal": 1, "stop": 1, "take": 1}, repr(js))
    check("journal: the day-loss limit found in the refusals", js["limit_at"] is not None and abs(js["limit_at"] - (NOW - 7200)) < 1)
    scl = ["%s checks: 0 fail(s): ок | no HEARTBEAT_URL\n" % time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(NOW - k * 1800))
           for k in range(60, 0, -1)]
    scl[-5] = "%s checks: 1 fail(s): сканер не делал проходов | pulse withheld (1 fail(s))\n" % scl[-5][:20]
    s = D.selfcheck_stats(scl, NOW)
    check("self-check: 48 runs in 24h, one with a finding, clean now",
          (s["runs"], s["bad"], s["last"][1]) == (48, 1, 0), repr(s))
    s = D.selfcheck_stats(scl[:-4], NOW)
    check("self-check: the newest line's finding is kept without the pulse tail",
          s["last"][2] == "сканер не делал проходов", repr(s["last"]))
    vp = os.path.join(tmp, "scanner_view.json")
    # the shape autoscan.py writes (SCANNER VIEW CONTRACT, v1): an ISO ts and a list of waiting symbols
    v1 = {"v": 1, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(NOW - 1800)), "held": 3, "entry_ok": 9,
          "queue": [{"symbol": "AUSDT", "reason": "book_full"}, {"symbol": "BUSDT", "reason": "too_wide"}]}
    for shape, want in ((v1, (2, False)),
                        ({"queue": ["A", "B", "C"], "ts": NOW - 60}, (3, False)),
                        ({"queue_size": 7, "ts": (NOW - 60) * 1000}, (7, False)),
                        ({"candidates": {"A": 1}, "updated": iso(NOW - 5 * 3600)}, (1, True))):
        io.open(vp, "w", encoding="utf-8").write(json.dumps(shape))
        q = D.scanner_queue(vp, NOW)
        check("scanner queue from %s" % ("the v1 contract" if shape.get("v") else sorted(shape)[0]), q is not None and (q["n"], q["stale"]) == want, repr(q))
    for bad in ("{not json", "[1,2]", json.dumps({"other": 1}), json.dumps({"queue": True})):
        io.open(vp, "w", encoding="utf-8").write(bad)
        check("scanner queue: %r -> no line, no exception" % bad[:12], D.scanner_queue(vp, NOW) is None)
    os.remove(vp)
    check("scanner queue: no file -> no line", D.scanner_queue(vp, NOW) is None)
    tail = ["SEVERE: [TradingHalt] HALTED at 2026-09-23T14:43:49Z: reconcile failed 3x — no new positions will be opened",
            "INFO: [Loop] alive: 2 position(s), halt=YES, reconcile failures=0"]
    check("halt: read from the loop's own line, with the reason", D.bot_state(tail) == (True, "reconcile failed 3x"),
          repr(D.bot_state(tail)))
    tail += ["WARNING: [TradingHalt] HALT CLEARED at 2026-09-23T15:00:00Z by operator",
             "INFO: [Loop] alive: 2 position(s), halt=none"]
    check("halt: cleared -> not halted", D.bot_state(tail) == (False, ""), repr(D.bot_state(tail)))


if __name__ == "__main__":
    sys.exit(main())
