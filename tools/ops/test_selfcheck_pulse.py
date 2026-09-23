# -*- coding: utf-8 -*-
"""Offline checks for the external pulse in selfcheck.py (no docker, no network, no /opt).

The block between '# --- pulse:begin' and '# --- pulse:end' is cut out of the real script and run
against fakes, so what is tested is the code that ships, not a copy of it.

  1. no HEARTBEAT_URL           -> nothing is requested (the default a fresh env file gets)
  2. any failed check           -> the pulse is WITHHELD: silence is how the outside monitor learns
  3. every check passed         -> exactly one request, to exactly that URL
  4. the request fails          -> recorded, the script goes on, and the URL (it carries the check's
                                   secret id) is written nowhere

Run: py -3 tools/ops/test_selfcheck_pulse.py
"""
import io, os, re, sys, tempfile, types, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = io.open(os.path.join(HERE, "selfcheck.py"), encoding="utf-8").read()
m = re.search(r"# --- pulse:begin.*?\n(.*?)# --- pulse:end", SRC, re.S)
assert m, "pulse markers not found in selfcheck.py"
BLOCK = m.group(1)
FAILED = []


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name, (" - " + detail) if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def run(env, fails, raise_exc=None):
    calls = []

    def fake_urlopen(req, timeout=None):
        calls.append(req.full_url if hasattr(req, "full_url") else req)
        if raise_exc:
            raise raise_exc
        return types.SimpleNamespace(read=lambda: b"OK")

    err = tempfile.NamedTemporaryFile(delete=False, suffix=".err")
    err.close()
    fake_urllib = types.SimpleNamespace(request=types.SimpleNamespace(urlopen=fake_urlopen, Request=urllib.request.Request))
    real_open = io.open

    def fake_open(path, *a, **k):
        return real_open(err.name if path == "/opt/selfcheck.err" else path, *a, **k)

    ns = {"env": env, "fails": fails, "urllib": fake_urllib, "time": __import__("time"),
          "io": types.SimpleNamespace(open=fake_open)}
    exec(BLOCK, ns)
    text = real_open(err.name, encoding="utf-8").read()
    os.unlink(err.name)
    return ns["pulse"], calls, text


def main():
    print("selfcheck external pulse - offline checks\n")
    url = "https://hc-ping.com/0f1e2d3c-secret-uuid"
    p, calls, _ = run({}, [])
    check("no HEARTBEAT_URL -> nothing requested", calls == [] and p == "no HEARTBEAT_URL", p)
    p, calls, _ = run({"HEARTBEAT_URL": "   "}, [])
    check("a blank HEARTBEAT_URL counts as unset", calls == [], p)
    p, calls, _ = run({"HEARTBEAT_URL": url}, ["сканер не делал проходов больше 2.5 часов"])
    check("a failed check WITHHOLDS the pulse", calls == [] and p.startswith("pulse withheld"), p)
    p, calls, _ = run({"HEARTBEAT_URL": url + "\r"}, [])
    check("all checks passed -> exactly one request to that URL (CRLF env line tolerated)",
          calls == [url] and p == "pulse sent", "%s %s" % (p, calls))
    import urllib.error
    p, calls, err = run({"HEARTBEAT_URL": url}, [], raise_exc=urllib.error.URLError("down"))
    check("a failed request is recorded and does not raise", p.startswith("pulse FAILED") and "heartbeat failed" in err, p)
    check("the URL (secret id) is written nowhere", "secret-uuid" not in err and "secret-uuid" not in p, err)
    print("\n%s" % ("ALL CHECKS PASSED" if not FAILED else "FAILED: " + ", ".join(FAILED)))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
