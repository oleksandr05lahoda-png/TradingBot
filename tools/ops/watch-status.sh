#!/bin/bash
# The ONE command the laptop's watcher key may run (23.09). The lab bot on the laptop SSHes in
# every 15 min (StrategyLab lab/watch/vps.py) - with a full root key until now, so a stolen laptop
# was a stolen server. Installed as /opt/watch-status.sh and pinned in authorized_keys with
# command="/opt/watch-status.sh",restrict: whatever the client asks for ($SSH_ORIGINAL_COMMAND),
# this runs instead, reads four facts, writes nothing, starts nothing. See tools/ops/watch-key.md.
#
# The output must stay byte-for-byte what vps.py's REMOTE prints today - the watcher parses these
# key=value lines, and a changed format would read as "no answer" and page the owner at night.
# tools/ops/test_watch_status.py runs both side by side against the same fake files and compares.
# bash, not sh: REMOTE runs in root's login shell (bash), and dash's echo treats backslashes
# differently. No `set -e`: REMOTE has none, and every line must print even if the one before failed.
# WATCH_DATA exists for the offline test only; sshd with restrict passes no client environment.
D="${WATCH_DATA:-/opt/tradingbot-data}"
echo status=$(docker inspect -f '{{.State.Status}}' tradingbot 2>/dev/null || echo missing)
echo now=$(date -u +%s)
echo scan=$(tail -n 4000 "$D/autoscan.log" 2>/dev/null | grep ' scan: ' | tail -1 | cut -c1-19)
echo selfcheck=$(tail -n 1 "$D/ops/selfcheck.log" 2>/dev/null | cut -c1-20)
