# -*- coding: utf-8 -*-
"""Offline checks for tools/vps-deploy.sh (no docker, no network, no /opt).

The real script runs under bash against a fake tree (APP_DIR), a fake data dir, a fake env file,
and fakes of docker, curl, df and sleep that record every call. What is pinned:

  1. a failing offline test stops the deploy BEFORE the build: the running bot is never touched
  2. under 2 GB free the deploy stops before the build
  3. the happy path: tests -> build -> backup -> tag :prev -> stop -t 45 -> rm -> run; never rm -f,
     never kill; the backup holds the data dir; the summary names the stamp, the autoscan.py md5
     (= the source), the bot ready, the scanner up, the positions count from the bot's log and the
     restart count
  4. only the last 7 backups are kept
  5. a container that dies at start stops the script with the rollback command in the message; one
     that dies AFTER the ready lines (its restart count moves) does too
  6. rollback: :latest kept as :bad, :prev becomes :latest, stop -t 45, and the container is started
     with EXACTLY the deploy's run flags; a second rollback (:latest == :prev) does not stamp the
     good image :bad; a rollback that does not come up does not suggest another rollback
  7. rollback with no :prev refuses and touches nothing; an unknown command refuses
  8. :prev is the image the RUNNING container trades on, and only when it never restarted: a
     crash-looping or stopped container leaves the old :prev alone (review 23.09)
  9. a hangup during the stop -> run swap is ignored and the container still starts; once the
     swap is done, a hangup ends the script as usual (so the first check is not vacuous)

Run: py -3 tools/ops/test_vps_deploy.py
"""
import io, os, re, shutil, subprocess, sys, tarfile, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
DEPLOY = os.path.normpath(os.path.join(HERE, "..", "vps-deploy.sh"))
FAILED = []
PASSED = [0]
# Not "tradingbot": vps-deploy.sh runs this file ON the production host before every build. Should the
# fake docker ever lose the PATH race there, the real one is asked about a container that does not exist.
NAME = "tbtest-fake-deploy"

DOCKER = r'''#!/bin/bash
printf '%s\n' "$*" >> "$FAKE_LOG"
case "$1" in
  inspect)
    case "$*" in
      *"{{.State.Status}} {{.RestartCount}} {{.Image}}"*)
        echo "${FAKE_OLD_STATE:-running} ${FAKE_OLD_RESTARTS:-0} sha256:feedc0ffee0123456789abcdef" ;;
      *"{{.RestartCount}}"*)
        # FAKE_RESTART_GROW: every look finds one more restart - a crash loop after the ready lines
        if [ -n "${FAKE_RESTART_GROW:-}" ]; then
          n=$(( $(cat "$FAKE_COUNTER" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$FAKE_COUNTER"; echo "$n"
        else echo 0; fi ;;
      *) echo "${FAKE_STATE:-running}" ;;
    esac ;;
  image)
    tag="${!#}"
    case "$tag" in *":prev") [ -z "${FAKE_NO_PREV:-}" ] || exit 1 ;; esac
    case "$*" in *"-f"*)
      if [ -n "${FAKE_SAME_IDS:-}" ]; then echo "sha256:5a5e5a5e5a5e5a5e5a5e"; else echo "sha256:id-${tag##*:}-0123456789ab"; fi ;;
    esac ;;
  stop) [ -z "${FAKE_HUP_ON_STOP:-}" ] || kill -HUP "$PPID" ;;
  logs) cat "$FAKE_LOGS" ;;
  exec)
    case "$3" in
      cat) echo "20260923-120000Z abc1234" ;;
      md5sum) md5sum "$FAKE_AUTOSCAN" | sed 's|  .*|  /app/scanner/autoscan.py|' ;;
    esac ;;
esac
exit 0
'''
CURL = "#!/bin/bash\nprintf 200\n"
DF = "#!/bin/bash\necho 'Filesystem 1024-blocks Used Available Capacity Mounted'\necho \"/dev/vda4 60000000 5000000 ${FAKE_FREE_KB:-50000000} 10% /\"\n"
# FAKE_HUP_ON_SLEEP: the only direct `sleep` is in wait_ready, after the swap - a hangup there must still kill.
SLEEP = "#!/bin/bash\n[ -z \"${FAKE_HUP_ON_SLEEP:-}\" ] || kill -HUP \"$PPID\"\nexit 0\n"
BOT_LOG = ("2026-09-23 12:00:01 build: 20260923-120000Z abc1234\n"
           "INFO: [Reconciler] start-up state adopted from the exchange: ExposureBook[n=15 long=$84.53 short=$0.00]\n"
           "INFO: [Boot] account read complete - the loop is accepting closes\n"
           "2026-09-23 12:00:40 autoscan start: venue=real top=100 by_cap=True\n")


def check(name, cond, detail=""):
    print("  %s %s%s" % ("ok " if cond else "FAIL", name, (" - " + detail) if detail and not cond else ""))
    if cond:
        PASSED[0] += 1
    else:
        FAILED.append(name)


def find_bash():
    for cand in (r"C:\Program Files\Git\bin\bash.exe", r"C:\Program Files\Git\usr\bin\bash.exe",
                 "/bin/bash", "/usr/bin/bash", shutil.which("bash")):
        if cand and os.path.exists(cand) and "system32" not in cand.lower():
            return cand
    return None


def posix(path):
    if os.name != "nt":
        return path
    p = os.path.abspath(path).replace("\\", "/")
    return "/" + p[0].lower() + p[2:] if re.match(r"^[A-Za-z]:", p) else p


def write(path, text, mode=0o755):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with io.open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    os.chmod(path, mode)


class World:
    def __init__(self, root):
        self.root = root
        self.app = os.path.join(root, "app")
        self.data = os.path.join(root, "opt", "tradingbot-data")
        self.backups = os.path.join(root, "opt", "backups")
        self.env_file = os.path.join(root, "opt", "tradingbot.env")
        self.bin = os.path.join(root, "bin")
        self.log = os.path.join(root, "docker.log")
        self.logs = os.path.join(root, "container.log")
        write(os.path.join(self.app, "tools", "scanner", "autoscan.py"), "print('scanner')\n", 0o644)
        write(os.path.join(self.app, "tools", "scanner", "test_ok.py"),
              "print('  ok one')\nprint('ALL CHECKS PASSED')\n", 0o644)
        write(os.path.join(self.data, "trades.jsonl"), '{"kind":"entry"}\n', 0o644)
        write(os.path.join(self.data, "book-ledger-real.json"), '{"positions":[]}\n', 0o644)
        write(self.env_file, "BINANCE_REAL_API_KEY=abc\r\nREAL_TRADING=ARMED\nREAL_MODE=trade\n", 0o600)
        write(self.logs, BOT_LOG, 0o644)
        write(os.path.join(self.bin, "docker"), DOCKER)
        write(os.path.join(self.bin, "curl"), CURL)
        write(os.path.join(self.bin, "df"), DF)
        write(os.path.join(self.bin, "sleep"), SLEEP)
        write(os.path.join(self.bin, "python3"), '#!/bin/bash\nexec "%s" "$@"\n' % posix(sys.executable))

    def run(self, bash, *args, **extra):
        open(self.log, "w").close()
        env = dict(os.environ, NAME=NAME, APP_DIR=posix(self.app), DATA_DIR=posix(self.data), ENV_FILE=posix(self.env_file),
                   BACKUP_DIR=posix(self.backups), FAKE_LOG=posix(self.log), FAKE_LOGS=posix(self.logs),
                   FAKE_AUTOSCAN=posix(os.path.join(self.app, "tools", "scanner", "autoscan.py")),
                   FAKE_COUNTER=posix(os.path.join(self.root, "restarts.n")))
        if os.path.exists(os.path.join(self.root, "restarts.n")):
            os.remove(os.path.join(self.root, "restarts.n"))
        for k in ("FAKE_STATE", "FAKE_NO_PREV", "FAKE_FREE_KB", "FAKE_OLD_STATE", "FAKE_OLD_RESTARTS",
                  "FAKE_RESTART_GROW", "FAKE_SAME_IDS", "FAKE_HUP_ON_STOP", "FAKE_HUP_ON_SLEEP"):
            env.pop(k, None)
        env.update(extra)
        # PATH is set INSIDE bash: Git Bash rewrites a PATH handed to it from Windows.
        # ...and the run is refused unless `docker` resolves to the fake: exit 97, nothing executed.
        fake = posix(os.path.join(self.bin, "docker"))
        cmd = [bash, "-c", 'export PATH="%s:$PATH"; [ "$(command -v docker)" = "%s" ] || exit 97; exec bash "%s" "$@"'
               % (posix(self.bin), fake, posix(DEPLOY)), "x"] + list(args)
        res = subprocess.run(cmd, capture_output=True, env=env, timeout=300)
        out = (res.stdout + res.stderr).decode("utf-8", "replace")
        calls = io.open(self.log, encoding="utf-8").read().splitlines()
        return res.returncode, out, calls


def first(calls, prefix):
    for i, c in enumerate(calls):
        if c.startswith(prefix):
            return i
    return -1


def run_line(calls):
    return [c for c in calls if c.startswith("run ")]


def main():
    # A Windows console is cp1251: a U+2212 minus in a check name must not crash the run that reports it.
    try:
        sys.stdout.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass
    print("vps-deploy.sh - offline checks\n")
    bash = find_bash()
    if bash is None:
        print("  SKIP: no bash on this machine")
        return 0
    tmp = tempfile.mkdtemp(prefix="vps-deploy-")
    try:
        w = World(tmp)

        # 1. a red test stops everything before the build
        bad = os.path.join(w.app, "tools", "ops", "test_bad.py")
        write(bad, "import sys\nprint('  FAIL something')\nsys.exit(1)\n", 0o644)
        rc, out, calls = w.run(bash)
        check("red test: deploy refused", rc != 0, out[-400:])
        check("red test: the failing file is named", "test_bad.py" in out, out[-400:])
        check("red test: nothing built, stopped or started",
              not any(c.split()[0] in ("build", "stop", "rm", "run", "tag") for c in calls), repr(calls))
        os.remove(bad)

        # 2. too little disk
        rc, out, calls = w.run(bash, FAKE_FREE_KB="1500000")
        check("low disk: deploy refused before the build", rc != 0 and first(calls, "build") < 0, out[-300:])
        check("low disk: said in megabytes", "1464 МБ" in out, out[-300:])

        # 3. the happy path
        for i in range(9):
            p = os.path.join(w.backups, "data-20260901-00000%dZ.tgz" % i)
            write(p, "old", 0o644)
            os.utime(p, (1000000 + i, 1000000 + i))
        rc, out, calls = w.run(bash)
        check("deploy: exit 0", rc == 0, out[-800:])
        order = [first(calls, p) for p in ("build", "tag sha256:feedc0ffee0123456789abcdef {0}:prev".format(NAME),
                                           "stop -t 45 " + NAME, "rm " + NAME, "tag {0}:new {0}:latest".format(NAME),
                                           "run ")]
        check("deploy: build -> tag :prev (the running container's image) -> stop -t 45 -> rm -> tag :latest -> run",
              all(i >= 0 for i in order) and order == sorted(order), "%s\n%s" % (order, calls))
        check("deploy: :prev is never taken from the :latest tag",
              first(calls, "tag {0}:latest {0}:prev".format(NAME)) < 0, repr(calls))
        check("deploy: never rm -f, never kill", not any(re.search(r"\brm -f\b|\bkill\b", c) for c in calls), repr(calls))
        check("deploy: the ping was answered (fake curl)", "fapi/v1/ping -> HTTP 200" in out)
        check("deploy: the tests ran and passed", "ok  test_ok.py" in out and "ALL CHECKS PASSED" in out, out[:600])
        run = run_line(calls)
        check("deploy: run flags (restart, stop-timeout 45, memory 900m, env file, data mount, log caps)",
              len(run) == 1 and all(f in run[0] for f in (
                  "--restart unless-stopped", "--stop-timeout 45", "--memory 900m", "--env-file " + posix(w.env_file),
                  "-v " + posix(w.data) + ":/app/data", "--log-opt max-size=20m", "--log-opt max-file=5",
                  NAME + ":latest")), repr(run))
        fresh = sorted(f for f in os.listdir(w.backups) if not f.startswith("data-20260901"))
        check("deploy: one new backup", len(fresh) == 1, repr(os.listdir(w.backups)))
        if fresh:
            names = tarfile.open(os.path.join(w.backups, fresh[0])).getnames()
            check("deploy: the backup holds the journal and the ledger",
                  "tradingbot-data/trades.jsonl" in names and "tradingbot-data/book-ledger-real.json" in names, repr(names))
        check("deploy: only the last 7 backups kept", len(os.listdir(w.backups)) == 7, repr(sorted(os.listdir(w.backups))))
        check("deploy: the oldest backups were the ones removed",
              not os.path.exists(os.path.join(w.backups, "data-20260901-000000Z.tgz"))
              and os.path.exists(os.path.join(w.backups, "data-20260901-000008Z.tgz")))
        check("deploy: backup taken before the old container stops",
              out.find("копия данных") < out.find("останавливаю прежний"), out[-900:])
        summary = out[out.find("== итог"):]
        check("summary: stamp", "20260923-120000Z abc1234" in summary, summary)
        check("summary: autoscan.py md5 equals the source", "= исходник" in summary, summary)
        check("summary: bot ready, scanner up", "бот          готов" in summary and "сканер       запущен" in summary, summary)
        check("summary: 15 positions from the bot's own log", "позиций      15" in summary, summary)
        check("summary: the rollback command", "vps-deploy.sh rollback" in summary, summary)
        check("summary: the restart count", "перезапусков 0" in summary, summary)
        check("deploy: watched for a minute after the ready lines",
              "ещё 60 с смотрю" in out and len([c for c in calls if "RestartCount}}" in c and "Image" not in c]) >= 13,
              repr(calls))
        deploy_run = run[0] if run else ""

        # 5. a container that dies at start
        rc, out, calls = w.run(bash, FAKE_STATE="exited")
        check("dead at start: script fails and names the rollback", rc != 0 and "rollback" in out, out[-400:])
        rc, out, calls = w.run(bash, FAKE_RESTART_GROW="1")
        check("dies after the ready lines: script fails, names the rollback, no summary",
              rc != 0 and "упал уже после готовности" in out and "vps-deploy.sh rollback" in out
              and "== итог" not in out, out[-500:])

        # 8. :prev comes from a container that is trading, or not at all
        for label, extra in (("crash-looping (restarted 3x)", {"FAKE_OLD_RESTARTS": "3"}),
                             ("stopped", {"FAKE_OLD_STATE": "exited"}),
                             ("restarting", {"FAKE_OLD_STATE": "restarting", "FAKE_OLD_RESTARTS": "7"})):
            rc, out, calls = w.run(bash, **extra)
            check("old container %s: :prev left alone, said, deploy goes on" % label,
                  rc == 0 and not any(c.startswith("tag") and c.endswith(":prev") for c in calls)
                  and "оставлен прежним" in out and first(calls, "run ") >= 0, "%s\n%s" % (out[-600:], calls))

        # 9. hangups
        rc, out, calls = w.run(bash, FAKE_HUP_ON_STOP="1")
        check("hangup during the stop: ignored, the container is started, exit 0",
              rc == 0 and first(calls, "run ") > first(calls, "stop -t 45"), "%s\n%s" % (rc, calls))
        rc, out, calls = w.run(bash, FAKE_HUP_ON_SLEEP="1")
        check("hangup after the swap (while watching): ends the script, the container already runs",
              rc != 0 and first(calls, "run ") >= 0 and "== итог" not in out, "%s\n%s" % (rc, out[-300:]))
        rc, out, calls = w.run(bash, "rollback", FAKE_HUP_ON_STOP="1")
        check("rollback: hangup during the stop is ignored too", rc == 0 and first(calls, "run ") >= 0, repr(calls))

        # 6. rollback
        rc, out, calls = w.run(bash, "rollback")
        check("rollback: exit 0", rc == 0, out[-600:])
        order = [first(calls, p) for p in ("tag {0}:latest {0}:bad".format(NAME), "tag {0}:prev {0}:latest".format(NAME),
                                           "stop -t 45 " + NAME, "rm " + NAME, "run ")]
        check("rollback: :latest kept as :bad, :prev -> :latest, stop -t 45, rm, run",
              all(i >= 0 for i in order) and order == sorted(order), "%s\n%s" % (order, calls))
        check("rollback: no build, no ping", first(calls, "build") < 0 and "fapi/v1/ping" not in out)
        check("rollback: started with EXACTLY the deploy's run flags", run_line(calls) == [deploy_run],
              "%s vs %s" % (run_line(calls), deploy_run))
        check("rollback: never rm -f", not any(re.search(r"\brm -f\b", c) for c in calls), repr(calls))
        check("rollback: summary printed", "== итог: откат" in out and "позиций      15" in out, out[-500:])
        rc, out, calls = w.run(bash, "rollback", FAKE_SAME_IDS="1")
        check("second rollback (:latest == :prev): the good image is not tagged :bad, still restarted",
              rc == 0 and first(calls, "tag {0}:latest {0}:bad".format(NAME)) < 0 and first(calls, "run ") >= 0
              and "не трогаю" in out, "%s\n%s" % (out[-400:], calls))
        rc, out, calls = w.run(bash, "rollback", FAKE_STATE="exited")
        check("rollback that does not come up: fails, and does not suggest another rollback",
              rc != 0 and "Откат тоже не поднялся" in out and "vps-deploy.sh rollback" not in out, out[-500:])

        # 7. refusals
        rc, out, calls = w.run(bash, "rollback", FAKE_NO_PREV="1")
        check("rollback without :prev: refused, nothing stopped",
              rc != 0 and not any(c.startswith(("stop", "rm", "run", "tag")) for c in calls), repr(calls))
        rc, out, calls = w.run(bash, "deploi")
        check("unknown command: refused, nothing touched", rc != 0 and calls == [], repr(calls))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\n%d passed, %d failed" % (PASSED[0], len(FAILED)))
    print("ALL CHECKS PASSED" if not FAILED else "FAILED: " + ", ".join(FAILED))
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
