# -*- coding: utf-8 -*-
"""Offline checks for the exchange clock in selfcheck.py (no network, no /opt).

The block between '# --- clock:begin' and '# --- clock:end' is cut out of the real script and run
against a fake clock and a fake exchange, so what is tested is the code that ships.

  1. slow handshake             -> the skew never puts the timestamp ahead of the exchange
                                   (the midpoint formula it replaced was 1.4s ahead here: the
                                   23.09 11:00 "-1021 ... 1000ms ahead" on an NTP-synced host)
  2. one -1021                  -> skew measured again, the request asked once more, answer returned
  3. -1021 twice                -> reported with Binance's body, not retried forever
  4. any other refusal (-2015)  -> reported at once, no retry

Run: py -3 tools/ops/test_selfcheck_clock.py
"""
import hashlib, hmac, io, json, os, re, sys, types, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = io.open(os.path.join(HERE, "selfcheck.py"), encoding="utf-8").read()
m = re.search(r"# --- clock:begin.*?\n(.*?)# --- clock:end", SRC, re.S)
assert m, "clock markers not found in selfcheck.py"
BLOCK = m.group(1)
FAILED = []


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name, (" - " + detail) if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


class Clock:
    """Local clock == true time (NTP in sync); the exchange stamps with the same true time."""
    def __init__(self):
        self.ms = 1_790_000_000_000.0

    def time(self):
        return self.ms / 1000.0


class Reply:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return self.payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def load(script):
    """script: list of answers for signed calls - a dict (JSON) or ('err', code, body)."""
    clk = Clock()
    seen = {"signed": [], "time_calls": 0}

    def urlopen(req, timeout=None):
        if isinstance(req, str) and req.endswith("/fapi/v1/time"):
            seen["time_calls"] += 1
            clk.ms += 2900          # DNS + TCP + TLS set-up, all before the server stamps
            srv = int(clk.ms)
            clk.ms += 100           # the answer's trip back
            return Reply(json.dumps({"serverTime": srv}).encode())
        url = req.full_url
        ts = int(re.search(r"timestamp=(\d+)", url).group(1))
        clk.ms += 100               # the request's trip to the exchange
        seen["signed"].append(ts - int(clk.ms))   # >0 = stamped ahead of the exchange on arrival
        ans = script.pop(0)
        if isinstance(ans, tuple):
            _, code, body = ans
            raise urllib.error.HTTPError(url, code, "Bad Request", {}, io.BytesIO(body.encode()))
        return Reply(json.dumps(ans).encode())

    fake_urllib = types.SimpleNamespace(
        request=types.SimpleNamespace(urlopen=urlopen, Request=urllib.request.Request),
        error=urllib.error)
    ns = {"urllib": fake_urllib, "json": json, "time": clk, "hmac": hmac, "hashlib": hashlib,
          "S": "secret", "K": "key"}
    exec(BLOCK, ns)
    return ns, clk, seen


def main():
    print("selfcheck exchange clock - offline checks\n")

    ns, clk, seen = load([[{"symbol": "X", "positionAmt": "1"}]])
    t0 = clk.ms
    ns["_resync"]()
    old_skew = (t0 + 2900) - (t0 + clk.ms) / 2      # the replaced midpoint formula, same exchange
    check("slow handshake: skew does not lead the exchange", ns["_SKEW"][0] <= 0, "skew %s" % ns["_SKEW"][0])
    check("the replaced midpoint formula would have led by >1s here", old_skew > 1000, "old %s" % old_skew)
    ns["call"]("/fapi/v2/positionRisk")
    check("signed request arrives at or behind the exchange clock", seen["signed"][-1] <= 0, str(seen["signed"]))

    ns, clk, seen = load([("err", 400, '{"code":-1021,"msg":"Timestamp for this request was 1000ms ahead of the server\'s time."}'),
                          [{"symbol": "X", "positionAmt": "1"}]])
    out = ns["call"]("/fapi/v2/positionRisk")
    check("one -1021: answer returned after one retry", out == [{"symbol": "X", "positionAmt": "1"}], repr(out))
    check("one -1021: skew measured again before the retry", seen["time_calls"] == 1, str(seen["time_calls"]))
    check("one -1021: exactly two signed requests", len(seen["signed"]) == 2, str(seen["signed"]))

    body = '{"code":-1021,"msg":"Timestamp for this request was 1000ms ahead of the server\'s time."}'
    ns, clk, seen = load([("err", 400, body), ("err", 400, body)])
    try:
        ns["call"]("/fapi/v2/positionRisk")
        check("-1021 twice: reported", False, "no exception")
    except RuntimeError as e:
        check("-1021 twice: reported with Binance's body", "-1021" in str(e), str(e))
    check("-1021 twice: not retried a third time", len(seen["signed"]) == 2, str(seen["signed"]))

    ns, clk, seen = load([("err", 401, '{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}')])
    try:
        ns["call"]("/fapi/v1/openAlgoOrders")
        check("-2015: reported", False, "no exception")
    except RuntimeError as e:
        check("-2015: reported at once with the body", "-2015" in str(e), str(e))
    check("-2015: no retry, no resync", len(seen["signed"]) == 1 and seen["time_calls"] == 0,
          "%s / %s" % (seen["signed"], seen["time_calls"]))

    print("\n%s" % ("ALL OK" if not FAILED else "FAILED: " + ", ".join(FAILED)))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
