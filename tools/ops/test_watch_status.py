# -*- coding: utf-8 -*-
"""Offline checks for watch-status.sh, the one command the restricted watcher key may run.

The laptop's watcher (StrategyLab lab/watch/vps.py) parses what its REMOTE one-liner prints. The
forced command replaces that one-liner, so its output must be the same BYTES - otherwise the watcher
reads a healthy server as "no answer" and pages the owner at night. Both are run here under bash,
side by side, against the same fake docker and fake log files:

  1. the REMOTE copy below is still the one in StrategyLab (when that checkout is on this machine)
  2. healthy server           -> identical bytes (the epoch compared as 10 digits), and the watcher's parser gets all four keys
  3. container missing, logs missing -> identical bytes ("status=missing", empty scan/selfcheck)
  4. the command the client asks for is ignored (a forced command runs regardless); the probe is a
     harmless sentinel file, because this test also runs as root on the production host
  5. the script writes nothing into the data directory

Run: py -3 tools/ops/test_watch_status.py
"""
import ast, io, os, re, shutil, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "watch-status.sh")
LAB_VPS = os.path.join(HERE, "..", "..", "..", "StrategyLab", "lab", "watch", "vps.py")
FAILED = []
PASSED = [0]

# StrategyLab lab/watch/vps.py REMOTE, as of 24.09.2026. Check 1 keeps this copy honest.
# 24.09: the watcher dropped the container and scanner lines (the server's own watchdog and selfcheck say
# those) and now asks for two facts only. The forced command still prints all four, so a key installed on
# 23.09 keeps working: the watcher's parser ignores keys it does not read. The checks below therefore ask
# that every line REMOTE prints comes out of the script byte for byte - not that the outputs are equal.
REMOTE = (
    "echo now=$(date -u +%s); "
    "echo selfcheck=$(tail -n 1 /opt/tradingbot-data/ops/selfcheck.log 2>/dev/null | cut -c1-20)"
)


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name, (" - " + detail) if detail and not cond else ""))
    if cond:
        PASSED[0] += 1
    else:
        FAILED.append(name)


def find_bash():
    # Not whatever "bash" resolves to first on Windows: System32\bash.exe is WSL, a different machine.
    for cand in (r"C:\Program Files\Git\bin\bash.exe", r"C:\Program Files\Git\usr\bin\bash.exe",
                 "/bin/bash", "/usr/bin/bash", shutil.which("bash")):
        if cand and os.path.exists(cand) and "system32" not in cand.lower():
            return cand
    return None


def posix(path):
    """A Windows path as Git Bash sees it; unchanged elsewhere."""
    if os.name != "nt":
        return path
    p = os.path.abspath(path).replace("\\", "/")
    return "/" + p[0].lower() + p[2:] if re.match(r"^[A-Za-z]:", p) else p


def parse(stdout):
    """The watcher's own parser (vps.ssh_snapshot)."""
    out = {}
    for line in stdout.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def run(bash, tmp, data, docker_state, *, script=True, client_cmd=""):
    fakebin = os.path.join(tmp, "bin")
    os.makedirs(fakebin, exist_ok=True)
    with io.open(os.path.join(fakebin, "docker"), "w", newline="\n") as f:
        if docker_state is None:
            f.write("#!/bin/bash\necho 'Error: No such object: tradingbot' >&2\nexit 1\n")
        else:
            f.write("#!/bin/bash\necho %s\n" % docker_state)
    os.chmod(os.path.join(fakebin, "docker"), 0o755)
    # Git Bash takes a POSIX PATH when started from Windows with one; the fakes must come first.
    tail = "/usr/bin:/bin" if os.name == "nt" else os.environ.get("PATH", "/usr/bin:/bin")
    env = dict(os.environ, PATH=posix(fakebin) + ":" + tail, SSH_ORIGINAL_COMMAND=client_cmd)
    if script:
        env["WATCH_DATA"] = posix(data)
        cmd = [bash, posix(SCRIPT)]
    else:
        cmd = [bash, "-c", REMOTE.replace("/opt/tradingbot-data", posix(data))]
    res = subprocess.run(cmd, capture_output=True, env=env, timeout=60)
    # The clock is the host's own `date` (Git Bash will not be talked out of its date.exe): the two
    # runs may straddle a second, so the epoch is checked for shape and compared as N.
    return re.sub(rb"^now=\d{10}$", b"now=N", res.stdout, flags=re.M)


def carries(script_out, remote_out):
    """Every line the watcher's own REMOTE prints is in the forced command's output, byte for byte."""
    lines = script_out.splitlines()
    return remote_out != b"" and all(line in lines for line in remote_out.splitlines())


def lab_remote():
    try:
        src = io.open(LAB_VPS, encoding="utf-8").read()
    except OSError:
        return None
    m = re.search(r"^REMOTE = (\(.*?\n\))", src, re.S | re.M)
    return ast.literal_eval(m.group(1)) if m else "unparsed"


def main():
    # A Windows console is cp1251: a U+2212 minus in a check name must not crash the run that reports it.
    try:
        sys.stdout.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass
    print("watch-status.sh - offline checks\n")
    bash = find_bash()
    if bash is None:
        print("  SKIP: no bash on this machine")
        return 0
    lab = lab_remote()
    if lab is None:
        print("  (StrategyLab checkout not found beside this repo - REMOTE copy not re-checked)")
    else:
        check("the REMOTE copy here is still StrategyLab's", lab == REMOTE, repr(lab)[:200])

    tmp = tempfile.mkdtemp(prefix="watch-status-")
    try:
        data = os.path.join(tmp, "data")
        os.makedirs(os.path.join(data, "ops"))
        scan_lines = ["2026-09-23 17:%02d:01 scan: 3 held, 40 hold-ok, 2 entry-ok, nothing to do\n" % m
                      for m in range(0, 50, 10)]
        with io.open(os.path.join(data, "autoscan.log"), "w", encoding="utf-8", newline="\n") as f:
            f.write("2026-09-23 05:38:41 autoscan start: venue=real top=100\n" + "".join(scan_lines)
                    + "2026-09-23 17:55:00 bear gate closed\n")
        with io.open(os.path.join(data, "ops", "selfcheck.log"), "w", encoding="utf-8", newline="\n") as f:
            f.write("2026-09-23T15:00:03Z checks: 0 fail(s): ок | no HEARTBEAT_URL\n"
                    "2026-09-23T15:30:03Z checks: 0 fail(s): ок | no HEARTBEAT_URL\n")
        before = sorted(os.listdir(data)) + sorted(os.listdir(os.path.join(data, "ops")))

        a = run(bash, tmp, data, "running", script=False)
        b = run(bash, tmp, data, "running")
        check("healthy: the script prints every REMOTE line byte for byte", carries(b, a), "%r vs %r" % (a, b))
        snap = parse(b.decode("utf-8"))
        check("healthy: the watcher reads all four keys",
              snap == {"status": "running", "now": "N", "scan": "2026-09-23 17:40:01",
                       "selfcheck": "2026-09-23T15:30:03Z"}, repr(snap))

        # A harmless sentinel, never a real payload: vps-deploy.sh runs this file as root on the
        # production host before every build, so a future edit that evaluated SSH_ORIGINAL_COMMAND must
        # at worst create one file in this temp dir - not touch /opt or print a secret into the deploy
        # output (review 23.09: the payload here used to be `rm -rf /; cat /opt/tradingbot.env`).
        pwned = os.path.join(tmp, "pwned")
        c = run(bash, tmp, data, "running", client_cmd="touch %s; echo pwned=yes" % posix(pwned))
        check("the client's command is ignored: same bytes, sentinel not created",
              c == b and not os.path.exists(pwned), repr(c))

        empty = os.path.join(tmp, "empty")
        os.makedirs(empty)
        a = run(bash, tmp, empty, None, script=False)
        b = run(bash, tmp, empty, None)
        check("container missing, no logs: every REMOTE line byte for byte", carries(b, a), "%r vs %r" % (a, b))
        check("container missing, no logs: status=missing, empty scan and selfcheck",
              parse(b.decode()) == {"status": "missing", "now": "N", "scan": "", "selfcheck": ""},
              repr(b))
        after = sorted(os.listdir(data)) + sorted(os.listdir(os.path.join(data, "ops")))
        check("nothing was written into the data directory", before == after, "%s -> %s" % (before, after))
        check("script has no CR (a CRLF copy would not run on the VPS)", b"\r" not in io.open(SCRIPT, "rb").read())
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\n%d passed, %d failed" % (PASSED[0], len(FAILED)))
    print("ALL CHECKS PASSED" if not FAILED else "FAILED: " + ", ".join(FAILED))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
