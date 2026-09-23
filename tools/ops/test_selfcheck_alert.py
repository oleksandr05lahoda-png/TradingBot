# -*- coding: utf-8 -*-
"""Offline checks for the one-incident-one-message rule in selfcheck.py (no docker, no network, no /opt).

24.09, owner: a stopped container reached the same chat twice - from watchdog.sh (every 5 min) and
from selfcheck's check 1 (every 30 min). The block between '# --- alert:begin' and '# --- alert:end'
(and checks 1-2 between '# --- container:begin/end') is cut out of the real script and run against
fakes (the watchdog's flag and error log, the state file, docker, Telegram), so what is tested is the
code that ships.

  1. container down, watchdog flag set after the stop  -> no message, the failure stays in fails
  2. container down, NO flag                -> selfcheck still says it (the backup voice)
  3. container down + another failure       -> one message, the other failure only
  4. flag present, container fine           -> every other failure alerts as before
  5. still down after selfcheck spoke first  -> no false "back to normal"; it comes once all is clear
  6. Telegram refuses                        -> state not written, the alert is retried next run
  7. check 1's text starts with the prefix the block filters on (a rewording cannot re-open it)
  8. review 24.09 - the flag must PROVE this stop was told:
     - a watchdog delivery failure in the same second as the flag (the 03.09 watchdog touched the
       flag after a failed curl)                  -> selfcheck speaks
     - a failure >= 5 min before the flag (a retry that then worked) -> still silent
     - a flag older than this stop (stale, watchdog dead since)      -> selfcheck speaks
     - stop moment unknown                                           -> selfcheck speaks
  9. checks 1-2 against a fake docker: FinishedAt parsed; a stopped container's silent scanner is
     not a second alert; a running container's silent scanner still is

Run: py -3 tools/ops/test_selfcheck_alert.py
"""
import calendar, io, os, re, shutil, sys, tempfile, time, types

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = io.open(os.path.join(HERE, "selfcheck.py"), encoding="utf-8").read()
m = re.search(r"# --- alert:begin.*?\n(.*?)# --- alert:end", SRC, re.S)
assert m, "alert markers not found in selfcheck.py"
BLOCK = m.group(1)
mc = re.search(r"# --- container:begin.*?\n(.*?)# --- container:end", SRC, re.S)
assert mc, "container markers not found in selfcheck.py"
CONTAINER_BLOCK = mc.group(1)
CONST = dict(re.findall(r'^(WATCHDOG_FLAG|WATCHDOG_ERR|CONTAINER_DOWN) = "(.*)"$', SRC, re.M))
assert set(CONST) == {"WATCHDOG_FLAG", "WATCHDOG_ERR", "CONTAINER_DOWN"}, CONST
DOWN = CONST["CONTAINER_DOWN"] + " (exited)"
SCANNER = "сканер не делал проходов больше 2.5 часов"
NOSTOP = "ПОЗИЦИЯ БЕЗ СТОПА: XUSDT"
FAILED = []


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name, (" - " + detail) if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


class Host:
    """The files the block touches, mapped into a temp dir; Telegram is a list. The container
    stopped 10 minutes ago unless a case says otherwise."""

    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="selfcheck-alert-")
        self.state = os.path.join(self.dir, "selfcheck.state")
        self.flag = os.path.join(self.dir, "watchdog.alerted")
        self.err = os.path.join(self.dir, "watchdog.err")
        self.sent = []
        self.down_since = time.time() - 600

    def _touch(self, path, at):
        io.open(path, "a").close()
        at = time.time() if at is None else at
        os.utime(path, (at, at))

    def watchdog_alerted(self, on, at=None):
        if on:
            self._touch(self.flag, at)
        elif os.path.exists(self.flag):
            os.unlink(self.flag)

    def watchdog_failed(self, at=None):
        self._touch(self.err, at)

    def read_state(self):
        return io.open(self.state, encoding="utf-8").read() if os.path.exists(self.state) else None

    def run(self, fails, telegram_ok=True):
        def tg(text):
            if telegram_ok:
                self.sent.append(text)
            return telegram_ok

        ns = {"fails": list(fails), "tg": tg, "re": re, "os": os, "io": io, "down_since": self.down_since,
              "STATE": self.state, "WATCHDOG_FLAG": self.flag, "WATCHDOG_ERR": self.err,
              "CONTAINER_DOWN": CONST["CONTAINER_DOWN"]}
        exec(BLOCK, ns)
        return ns

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def run_container_checks(inspect_out, log_lines, now):
    """Checks 1-2 against a fake docker (every inspect answers the same Status|time line; check 1
    asks for FinishedAt, check 2 for StartedAt) and a fake scanner log."""
    def run(cmd, **kw):
        return types.SimpleNamespace(stdout=inspect_out + "\n")

    class FakeIo:
        @staticmethod
        def open(path, *a, **kw):
            return io.StringIO("".join(l + "\n" for l in log_lines))

    ns = {"fails": [], "now": now, "subprocess": types.SimpleNamespace(run=run), "io": FakeIo,
          "calendar": calendar, "time": time, "CONTAINER_DOWN": CONST["CONTAINER_DOWN"]}
    exec(CONTAINER_BLOCK, ns)
    return ns


def main():
    print("selfcheck one incident = one message - offline checks\n")

    h = Host()
    h.watchdog_alerted(True)
    ns = h.run([DOWN])
    check("down + watchdog flag -> no message", h.sent == [], str(h.sent))
    check("down + watchdog flag -> the failure is still counted (pulse withheld, cron log)",
          ns["fails"] == [DOWN] and ns["cur"] == DOWN and ns["told"] == [], "%s %s" % (ns["fails"], ns["told"]))
    check("down + watchdog flag -> state not advanced", h.read_state() is None, repr(h.read_state()))
    h.close()

    h = Host()
    h.run([DOWN])
    check("down, no flag -> selfcheck says it (watchdog has not)",
          len(h.sent) == 1 and DOWN in h.sent[0], str(h.sent))
    h.close()

    h = Host()
    h.watchdog_alerted(True)
    h.run([DOWN, NOSTOP])
    check("down + another failure -> one message, with the other failure only",
          len(h.sent) == 1 and NOSTOP in h.sent[0] and CONST["CONTAINER_DOWN"] not in h.sent[0], str(h.sent))
    h.run([DOWN, NOSTOP])
    check("...and not repeated next run", len(h.sent) == 1, str(h.sent))
    h.close()

    h = Host()
    h.watchdog_alerted(True)
    h.run([SCANNER])
    check("flag present, container fine -> other checks alert as before",
          len(h.sent) == 1 and SCANNER in h.sent[0], str(h.sent))
    h.close()

    # selfcheck ran in the same minute as the watchdog, before the flag existed, and spoke first
    h = Host()
    h.run([DOWN])
    h.watchdog_alerted(True)
    h.run([DOWN])
    h.run([DOWN])
    check("still down after selfcheck spoke first -> silent, no false 'back to normal'",
          len(h.sent) == 1, str(h.sent))
    h.watchdog_alerted(False)
    h.run([])
    check("...all clear -> 'back to normal' exactly once",
          len(h.sent) == 2 and "норму" in h.sent[1], str(h.sent))
    h.run([])
    check("...and not again", len(h.sent) == 2, str(h.sent))
    h.close()

    # an older alert is open, then the container stops: the watchdog owns the incident now
    h = Host()
    h.run([NOSTOP])
    h.watchdog_alerted(True)
    h.run([DOWN])
    check("open alert, then container down with flag -> silent, state kept",
          len(h.sent) == 1 and h.read_state() == re.sub(r"\d+", "N", NOSTOP), "%s %r" % (h.sent, h.read_state()))
    h.close()

    h = Host()
    h.run([SCANNER], telegram_ok=False)
    check("Telegram refuses -> state not written", h.read_state() is None, repr(h.read_state()))
    h.run([SCANNER])
    check("...and the alert goes out on the next run", len(h.sent) == 1, str(h.sent))
    h.close()

    # --- review 24.09: the flag must prove THIS stop reached Telegram
    t = time.time()
    h = Host()
    h.watchdog_failed(at=t)
    h.watchdog_alerted(True, at=t)
    h.run([DOWN])
    check("flag touched right after a failed watchdog delivery (03.09 watchdog) -> selfcheck speaks",
          len(h.sent) == 1 and DOWN in h.sent[0], str(h.sent))
    h.run([DOWN])
    check("...once, not every half hour", len(h.sent) == 1, str(h.sent))
    h.close()

    h = Host()
    h.watchdog_failed(at=t - 5)
    h.watchdog_alerted(True, at=t)
    h.run([DOWN])
    check("failure 5 s before the flag counts as the same failed try -> selfcheck speaks",
          len(h.sent) == 1, str(h.sent))
    h.close()

    h = Host()
    h.watchdog_failed(at=t - 300)
    h.watchdog_alerted(True, at=t)
    h.run([DOWN])
    check("failed try 5 min before a delivered flag (fixed watchdog retried) -> silent", h.sent == [], str(h.sent))
    h.close()

    h = Host()
    h.watchdog_failed(at=t + 120)
    h.watchdog_alerted(True, at=t)
    h.run([DOWN])
    check("watchdog failure AFTER the flag -> selfcheck speaks", len(h.sent) == 1, str(h.sent))
    h.close()

    h = Host()
    h.watchdog_alerted(True, at=t - 3 * 86400)  # left by an incident three days ago
    h.run([DOWN])
    check("flag older than this stop (stale, watchdog dead since) -> selfcheck speaks",
          len(h.sent) == 1 and DOWN in h.sent[0], str(h.sent))
    h.close()

    h = Host()
    h.watchdog_alerted(True, at=h.down_since - 30)
    h.run([DOWN])
    check("flag 30 s before FinishedAt (cron/clock tolerance) -> still counts, silent", h.sent == [], str(h.sent))
    h.close()

    h = Host()
    h.down_since = None
    h.watchdog_alerted(True)
    h.run([DOWN])
    check("stop moment unknown -> the flag cannot be tied to it, selfcheck speaks", len(h.sent) == 1, str(h.sent))
    h.close()

    # --- checks 1-2 against a fake docker
    now = calendar.timegm((2026, 9, 24, 3, 0, 0, 0, 0, 0))
    old_log = ["2026-09-23 20:00:00 scan: 12 pairs"]  # 7 h ago: stale
    ns = run_container_checks("exited|2026-09-24T01:00:00.123456789Z", old_log, now)
    check("check 1: stopped container -> failure, FinishedAt parsed into down_since",
          ns["fails"] == [DOWN] and ns["down_since"] == now - 7200, "%s %s" % (ns["fails"], ns.get("down_since")))
    check("check 2: stopped container's silent scanner is NOT a second alert", SCANNER not in ns["fails"], str(ns["fails"]))
    ns = run_container_checks("running|2026-09-20T01:00:00.1Z", old_log, now)
    check("check 2: running container, scanner silent 7 h -> alert as before",
          ns["fails"] == [SCANNER] and ns["down_since"] is None, "%s %s" % (ns["fails"], ns.get("down_since")))
    ns = run_container_checks("running|2026-09-20T01:00:00.1Z", ["2026-09-24 02:30:00 scan: 12 pairs"], now)
    check("checks 1-2: running + fresh scan -> nothing", ns["fails"] == [], str(ns["fails"]))
    ns = run_container_checks("created|0001-01-01T00:00:00Z", [], now)
    check("check 1: never-stopped 'created' container -> down_since unknown",
          ns["fails"] == [CONST["CONTAINER_DOWN"] + " (created)"] and ns["down_since"] is None,
          "%s %s" % (ns["fails"], ns.get("down_since")))
    ns = run_container_checks("", [], now)
    check("check 1: docker answers nothing -> failure, down_since unknown",
          ns["fails"] == [CONST["CONTAINER_DOWN"] + " ()"] and ns["down_since"] is None,
          "%s %s" % (ns["fails"], ns.get("down_since")))

    check1 = re.search(r"# 1\. container alive\n(.*?)\n# 2\.", SRC, re.S)
    check("check 1 builds its text from CONTAINER_DOWN",
          check1 is not None and "(CONTAINER_DOWN, st)" in check1.group(1), check1.group(1) if check1 else "")
    check("the cron log line marks a failure kept out of Telegram",
          "said by watchdog" in SRC.split("# --- pulse:end", 1)[1])

    print("\n%s" % ("ALL CHECKS PASSED" if not FAILED else "FAILED: " + ", ".join(FAILED)))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
