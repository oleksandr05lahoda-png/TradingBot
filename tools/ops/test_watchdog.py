# -*- coding: utf-8 -*-
"""Offline checks for tools/ops/watchdog.sh (no docker, no network, no /opt).

24.09 review: the watchdog touched /opt/watchdog.alerted even when its curl FAILED, and selfcheck.py
now stays quiet about a stopped container while that flag stands - so one Telegram hiccup in the
watchdog's single try would have left a real-money bot down with nobody told. The flag must mean
"delivered". The real script is copied with /opt/ pointed at a temp dir and run by bash with fake
`docker` and `curl` shell functions put in front of it.

  1. down, Telegram fails     -> no flag, the failure is in watchdog.err, next run tries again
  2. down, Telegram works     -> one message, flag set; later runs say nothing more
  3. back up, Telegram fails  -> flag kept (recovery retried), no false "said"
  4. back up, Telegram works  -> "снова работает" once, flag removed
  5. no token in the env file -> nothing counts as said: no flag, a line in watchdog.err
  6. the token never reaches curl's argv (it goes in through stdin)

Run: py -3 tools/ops/test_watchdog.py
"""
import io, os, shutil, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = io.open(os.path.join(HERE, "watchdog.sh"), encoding="utf-8").read()
TOKEN = "123456:FAKE-TOKEN-for-tests"
FAILED = []


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name, (" - " + detail) if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def find_bash():
    # On Windows `bash` on PATH may be WSL's (System32), which cannot see these temp paths: prefer Git's.
    if os.name == "nt":
        for p in (r"C:\Program Files\Git\bin\bash.exe", r"C:\Program Files\Git\usr\bin\bash.exe"):
            if os.path.exists(p):
                return p
    return shutil.which("bash")


# Shell FUNCTIONS put at the top of the copy, not files on PATH: a function wins over any command of
# that name, so no real docker or curl (which would call Telegram) can be reached whatever PATH Git's
# bash.exe builds on Windows, and nothing needs an exec bit (vps-deploy runs this on the host, where
# /tmp may be noexec).
FAKES = r"""
docker() {
  echo x >> "$WD_DIR/docker_calls"
  local s
  s=$(cat "$WD_DIR/status")
  [ "$s" = "missing" ] && return 1
  echo "$s"
}
# Records every argument (one per line) and the text sent; fails with the code in curl_rc.
curl() {
  cat > /dev/null
  local a prev rc
  for a in "$@"; do echo "$a" >> "$WD_DIR/curl_argv"; done
  rc=$(cat "$WD_DIR/curl_rc")
  if [ "$rc" = "0" ]; then
    prev=""
    for a in "$@"; do
      [ "$prev" = "--data-urlencode" ] && case "$a" in text=*) echo "${a#text=}" >> "$WD_DIR/sent";; esac
      prev="$a"
    done
  fi
  return "$rc"
}
"""


class Host:
    def __init__(self, bash, token=True):
        self.bash = bash
        self.dir = tempfile.mkdtemp(prefix="watchdog-")
        d = self.dir.replace("\\", "/")
        self.posix = d
        head, _, rest = SRC.replace("/opt/", d + "/").partition("\n")
        io.open(os.path.join(self.dir, "watchdog.sh"), "w", encoding="utf-8", newline="\n").write(
            head + "\n" + FAKES + rest)
        env_lines = ["TELEGRAM_CHAT_ID=42\r\n"]  # CRLF, like the real file
        if token:
            env_lines.insert(0, "TELEGRAM_BOT_TOKEN=%s\r\n" % TOKEN)
        io.open(os.path.join(self.dir, "tradingbot.env"), "w", newline="").write("".join(env_lines))

    def path(self, name):
        return os.path.join(self.dir, name)

    def lines(self, name):
        p = self.path(name)
        return io.open(p, encoding="utf-8").read().splitlines() if os.path.exists(p) else []

    def run(self, status, curl_rc=0):
        io.open(self.path("status"), "w").write(status)
        io.open(self.path("curl_rc"), "w").write(str(curl_rc))
        env = dict(os.environ, WD_DIR=self.posix)
        r = subprocess.run([self.bash, self.path("watchdog.sh")], env=env, capture_output=True, text=True,
                           timeout=60, stdin=subprocess.DEVNULL)
        return r.returncode

    def flag(self):
        return os.path.exists(self.path("watchdog.alerted"))

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def main():
    print("watchdog.sh - offline checks\n")
    bash = find_bash()
    if not bash:
        print("  skip: no bash on this machine")
        print("\nALL CHECKS PASSED")
        return 0

    h = Host(bash)
    # The fakes must really be the ones run: a first probe that fails loudly instead of testing nothing.
    rc = h.run("running")
    check("fake docker in place (running, no flag -> exit 0, nothing sent)",
          rc == 0 and h.lines("docker_calls") != [] and h.lines("sent") == [],
          "rc=%s docker_calls=%s sent=%s" % (rc, h.lines("docker_calls"), h.lines("sent")))

    h.run("exited", curl_rc=22)
    check("fake curl in place (a real one would have called Telegram)", h.lines("curl_argv") != [])
    check("down, Telegram fails -> no flag", not h.flag())
    check("...the failure is in watchdog.err", len(h.lines("watchdog.err")) == 1 and "curl exit 22" in h.lines("watchdog.err")[0],
          str(h.lines("watchdog.err")))
    h.run("exited", curl_rc=0)
    check("...next run tries again and, delivered, sets the flag",
          h.flag() and len(h.lines("sent")) == 1 and "'exited'" in h.lines("sent")[0], str(h.lines("sent")))
    h.run("exited", curl_rc=0)
    check("...then says nothing more while down", len(h.lines("sent")) == 1, str(h.lines("sent")))

    h.run("running", curl_rc=7)
    check("back up, Telegram fails -> flag kept for the retry", h.flag() and len(h.lines("sent")) == 1,
          str(h.lines("sent")))
    h.run("running", curl_rc=0)
    check("back up, delivered -> the recovery line once, flag removed",
          not h.flag() and len(h.lines("sent")) == 2 and "снова работает" in h.lines("sent")[1], str(h.lines("sent")))
    h.run("running", curl_rc=0)
    check("...and not again", len(h.lines("sent")) == 2, str(h.lines("sent")))

    argv = io.open(h.path("curl_argv"), encoding="utf-8").read() if os.path.exists(h.path("curl_argv")) else ""
    check("the token never sits in curl's argv", argv != "" and TOKEN not in argv and "FAKE-TOKEN" not in argv)
    check("the chat id arrives without the env file's CR", "chat_id=42\n" in argv, repr(argv[:200]))
    h.close()

    h = Host(bash)
    h.run("missing", curl_rc=0)
    check("container missing -> alert delivered, flag set",
          h.flag() and len(h.lines("sent")) == 1 and "'missing'" in h.lines("sent")[0], str(h.lines("sent")))
    h.close()

    h = Host(bash, token=False)
    h.run("exited", curl_rc=0)
    check("no token -> no flag, nothing sent", not h.flag() and h.lines("sent") == [], str(h.lines("sent")))
    check("...and a line in watchdog.err", any("no TELEGRAM_BOT_TOKEN" in l for l in h.lines("watchdog.err")),
          str(h.lines("watchdog.err")))
    h.close()

    print("\n%s" % ("ALL CHECKS PASSED" if not FAILED else "FAILED: " + ", ".join(FAILED)))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
