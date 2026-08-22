#!/usr/bin/env bash
# Starts the machine inside one container: risk core + scanner, sharing a book file
# and a persistent data directory. Mirrors tools/start-real.ps1, which is the tested
# procedure on the laptop; every line here exists because that script needed it.
set -euo pipefail

DATA_DIR="${DATA_DIR:-/app/data}"
BOOK="$DATA_DIR/book_real.txt"
BOT_LOG="$DATA_DIR/bot_real.err.log"
mkdir -p "$DATA_DIR"

say() { printf '%s %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S')" "$*"; }

# The venue gate is fail-closed by design: REAL_TRADING must be exactly ARMED and the
# real keys must be present, or the bot refuses to start. Say plainly which venue this
# container is about to become, because the log is the only place anyone will look.
say "venue: ${REAL_TRADING:-unset} / mode: ${REAL_MODE:-observe}"
if [ "${REAL_TRADING:-}" = "ARMED" ] && [ -z "${BINANCE_REAL_API_KEY:-}" ]; then
  say "REAL_TRADING=ARMED but BINANCE_REAL_API_KEY is empty - refusing to start."
  exit 1
fi

# The bot replays the whole book from byte zero at boot. A stale CLOSE would then fire
# against whatever holds that symbol now (laptop incident 17.08, commit e4206dc), so
# every run starts with a fresh book; open positions re-arm from the ledger instead.
if [ -s "$BOOK" ]; then
  mv "$BOOK" "$DATA_DIR/book_real.$(date -u '+%Y%m%d-%H%M%S').txt"
  ls -1t "$DATA_DIR"/book_real.*.txt 2>/dev/null | tail -n +11 | xargs -r rm -f
fi
printf '# real-venue book - scanner appends, the bot executes\n' > "$BOOK"

if [ -s "$BOT_LOG" ]; then
  mv "$BOT_LOG" "$DATA_DIR/bot_real.$(date -u '+%Y%m%d-%H%M%S').err.log"
  ls -1t "$DATA_DIR"/bot_real.*.err.log 2>/dev/null | tail -n +11 | xargs -r rm -f
fi

# The ledger is what lets a restart confirm resting stops by name. On an ephemeral
# disk it is lost, reconciliation calls every position unknown, and the bot halts.
export BOOK_LEDGER_PATH="${BOOK_LEDGER_PATH:-$DATA_DIR/book-ledger-real.json}"

say "starting the risk core..."
# Process substitution, not a pipe: after `java | tee &`, $! is tee's PID, so TERM on shutdown
# reached tee and the JVM was orphaned and killed hard - its shutdown hook never ran (22.08).
java -jar /app/bot.jar --source manual --script "$BOOK" > >(tee -a "$BOT_LOG") 2>&1 &
BOT_PID=$!

# The scanner stands down when it sees a halt in the bot's log, so it must not start
# before the log exists, and the book must not be fed before the account is adopted.
for _ in $(seq 1 60); do
  grep -aq 'adopted from the exchange\|did not converge\|HALTED' "$BOT_LOG" 2>/dev/null && break
  kill -0 "$BOT_PID" 2>/dev/null || { say "the bot exited during boot - see the log above."; exit 1; }
  sleep 5
done

say "starting the scanner..."
python3 /app/scanner/autoscan.py \
  --repo /app \
  --script "$BOOK" \
  --bot-log "$BOT_LOG" \
  --workdir "$DATA_DIR" \
  --venue "${SCAN_VENUE:-real}" \
  --by-cap --top "${SCAN_TOP:-100}" --lookback 30 --interval "${SCAN_INTERVAL:-3600}" \
  --max-positions "${MAX_POSITIONS:-10}" --leverage "${DEFAULT_LEVERAGE:-2}" &
SCANNER_PID=$!

# If either half dies the machine is broken: a bot with no scanner is a frozen book,
# a scanner with no bot writes into nothing. Take the container down so the platform
# restarts both together rather than leaving a half-machine that looks healthy.
term() { say "shutting down"; kill "$BOT_PID" "$SCANNER_PID" 2>/dev/null || true; }
trap term TERM INT
wait -n "$BOT_PID" "$SCANNER_PID"
say "one half exited - stopping the container so both restart together"
term
wait || true
