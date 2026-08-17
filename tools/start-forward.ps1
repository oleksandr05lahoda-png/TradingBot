# Brings the demo forward back up after the laptop was off.
#
# Safe to run twice: it stops anything already running first, and the bot
# re-arms its book from analysis/forward/book-ledger.json, so positions that
# survived on the exchange are picked back up with their stop ids intact
# instead of being flagged as unknown and halting the machine.
#
#   powershell -ExecutionPolicy Bypass -File tools\start-forward.ps1
#
# Stop everything:  tools\stop-forward.ps1

param(
    # Restart even if the machine already looks alive. Without this the script is a no-op when
    # it is, which is what lets it be wired to "at logon" and "on unlock" at the same time.
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$fwd  = Join-Path $root 'analysis\forward'
$botLog = Join-Path $fwd 'bot_live.err.log'

# Under Task Scheduler nobody is watching the console, so the launcher keeps its own record.
# Without it, a start that half-worked (bot up, scanner not) looks identical to a healthy one.
$launcherLog = Join-Path $fwd 'launcher.log'
function Say([string]$msg, [string]$colour = 'Gray') {
    $line = "{0} {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg
    Write-Host $line -ForegroundColor $colour
    Add-Content -Path $launcherLog -Value $line -Encoding utf8
}

# "Already running" needs BOTH halves alive. The bot's log is fresh for a minute or so after the
# bot is killed, so the log alone would report a stopped machine as healthy — and a bot with no
# scanner is a frozen book that looks identical to a working one from outside.
function Test-ScannerAlive {
    @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -like '*autoscan*' }).Count -ge 1
}
if (-not $Force -and (Test-Path $botLog) -and (Test-ScannerAlive)) {
    $age = (Get-Date) - (Get-Item $botLog).LastWriteTime
    if ($age.TotalSeconds -lt 90) {
        Say "already running (bot log written $([int]$age.TotalSeconds)s ago, scanner alive) - nothing to do." 'Green'
        exit 0
    }
}

Say "--- start-forward ---"
Say "stopping anything still running..."
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -like '*autoscan*' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -Confirm:$false }
Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -Confirm:$false
Start-Sleep -Seconds 4

# Credentials and settings. Keys stay in local.env and are never printed.
Get-Content (Join-Path $root 'local.env') | ForEach-Object {
    $l = $_.Trim()
    if ($l -match '^([A-Z_]+)=(.*)$') { Set-Item -Path "env:$($Matches[1])" -Value $Matches[2] }
}
$env:JAVA_HOME        = 'C:\Users\Asus_F15\.jdks\ms-21.0.9'
$env:MAX_POSITIONS    = '14'    # the exchange caps conditional orders; 14 x 2 stays under it
$env:DEFAULT_LEVERAGE = '2'
$env:TP_R_MULTIPLE    = '1.75'
$env:BOOK_LEDGER_PATH = Join-Path $fwd 'book-ledger.json'

# The bot replays the whole book at boot. Replayed entries are harmless (the ledger re-arms
# open positions, the scanner re-issues wanted entries within the hour), but a stale CLOSE
# from a previous run replays against whatever holds that symbol NOW: on 17.08 a close from
# 15.08 was matched against a since-reopened ADAUSDT and halted the machine on a false
# "partial close" — at every restart. Each run therefore starts with a fresh book.
$script = Join-Path $fwd 'book_live.txt'
if ((Test-Path $script) -and (Get-Item $script).Length -gt 0) {
    $stamp = (Get-Item $script).LastWriteTime.ToString('yyyyMMdd-HHmmss')
    Move-Item $script (Join-Path $fwd "book_live.$stamp.txt") -Force
    Get-ChildItem (Join-Path $fwd 'book_live.*.txt') |
        Sort-Object LastWriteTime -Descending | Select-Object -Skip 10 |
        Remove-Item -Force -ErrorAction SilentlyContinue
}
Set-Content -Path $script -Encoding ascii -Value "# live forward book - autoscan appends, the bot executes"

# Start-Process truncates a redirect target, so without this every restart destroys the evidence
# of why the previous run stopped. It cost one diagnosis already: the bot halted at 16:05 on 15.08
# and the reason was gone by the time anyone looked. Keep the last ten runs.
if ((Test-Path $botLog) -and (Get-Item $botLog).Length -gt 0) {
    $stamp = (Get-Item $botLog).LastWriteTime.ToString('yyyyMMdd-HHmmss')
    Move-Item $botLog (Join-Path $fwd "bot_live.$stamp.err.log") -Force
    Get-ChildItem (Join-Path $fwd 'bot_live.*.err.log') |
        Sort-Object LastWriteTime -Descending | Select-Object -Skip 10 |
        Remove-Item -Force -ErrorAction SilentlyContinue
}

Say "starting the bot..."
Start-Process -FilePath (Join-Path $root 'gradlew.bat') -WorkingDirectory $root -WindowStyle Hidden `
    -ArgumentList 'run','-q','--console=plain',"`"--args=--source manual --script $script`"" `
    -RedirectStandardOutput (Join-Path $fwd 'bot_live.out.log') `
    -RedirectStandardError  $botLog

# The scanner must not start before the bot has adopted the book: it reads the
# bot's log to detect a halt, and an empty log would read as "healthy".
Say "waiting for the book to be adopted..."
$log = $botLog
$deadline = (Get-Date).AddMinutes(4)
do {
    Start-Sleep -Seconds 5
    $text = if (Test-Path $log) { Get-Content $log -Raw -ErrorAction SilentlyContinue } else { '' }
} until ($text -match 'adopted from the exchange|did not converge' -or (Get-Date) -gt $deadline)

if ($text -match 'did not converge') {
    Say "BOOT HALTED - the bot could not confirm every position. Closes still work, opens do not." 'Red'
    exit 1
}
if ($text -notmatch 'adopted from the exchange') {
    Say "the bot did not report within 4 minutes - check $log" 'Yellow'
    exit 1
}
($text -split "`n" | Select-String 're-armed|adopted from the exchange') |
    ForEach-Object { Say ("  " + $_.ToString().Trim()) }

# Task Scheduler hands the script a leaner PATH than an interactive shell, so a bare "python"
# can vanish there while working fine by hand. Resolve it, and say which one was used.
$python = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $python) {
    $python = @(
        "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe"
    ) | Where-Object { Test-Path $_ } | Select-Object -First 1
}
if (-not $python) {
    Say "PYTHON NOT FOUND - the bot is up but the scanner is NOT, so nothing new will open." 'Red'
    exit 1
}

Say "starting the scanner ($python)..."
try {
    Start-Process -FilePath $python -WorkingDirectory $root -WindowStyle Hidden -ArgumentList `
        (Join-Path $root 'tools\scanner\autoscan.py'),
        '--script', $script,
        '--bot-log', $log,
        '--by-cap','--top','100','--lookback','30','--interval','3600',
        '--max-positions','14','--leverage','2' `
        -RedirectStandardOutput (Join-Path $fwd 'autoscan.out.log') `
        -RedirectStandardError  (Join-Path $fwd 'autoscan.err.log')
} catch {
    Say "SCANNER FAILED TO START: $($_.Exception.Message)" 'Red'
    exit 1
}

# A scanner that dies on its first breath leaves the bot holding a frozen book, which looks
# identical to a healthy machine from the outside. Check rather than assume.
Start-Sleep -Seconds 6
if (-not (Test-ScannerAlive)) {
    Say "SCANNER DIED IMMEDIATELY - see $fwd\autoscan.err.log" 'Red'
    exit 1
}

Say "running: bot + hourly scanner. Do not let the laptop sleep." 'Green'
exit 0
