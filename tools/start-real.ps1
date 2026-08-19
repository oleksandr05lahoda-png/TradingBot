# Brings the REAL-venue bot up. REAL MONEY — this launcher arms it, you run it.
#
#   powershell -ExecutionPolicy Bypass -File tools\start-real.ps1              # observe: no entries
#   powershell -ExecutionPolicy Bypass -File tools\start-real.ps1 -Mode trade  # full operation
#
# Deliberately separate from start-forward.ps1: the demo machine (book_live) and the
# real machine (book_real) own disjoint processes, books, ledgers, logs and scanner
# state, and each launcher kills only its own. Stop everything: tools\stop-real.ps1.
#
# The bot itself refuses the real exchange unless REAL_TRADING=ARMED and the real
# keys are present — this script sets the flag; the keys are yours to create on
# binance.com with WITHDRAWALS DISABLED and an IP whitelist, and to put in local.env.

param(
    [ValidateSet('observe', 'trade')]
    [string]$Mode = 'observe',
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$dir  = Join-Path $root 'analysis\real'
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$botLog = Join-Path $dir 'bot_real.err.log'

$launcherLog = Join-Path $dir 'launcher.log'
function Say([string]$msg, [string]$colour = 'Gray') {
    $line = "{0} {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg
    Write-Host $line -ForegroundColor $colour
    Add-Content -Path $launcherLog -Value $line -Encoding utf8
}

# One launcher at a time — the same race that wiped the demo ledger on 19.08 must
# not exist here at all.
try {
    $script:launcherMutex = New-Object System.Threading.Mutex($false, 'Global\TradingBotRealLauncher')
    $acquired = $script:launcherMutex.WaitOne(0)
} catch [System.Threading.AbandonedMutexException] { $acquired = $true }
if (-not $acquired) {
    Say "another real-venue launcher is already running - exiting." 'Yellow'
    exit 0
}

Say "--- start-real (mode: $Mode) ---"

# Credentials and settings. Keys stay in local.env and are never printed.
Get-Content (Join-Path $root 'local.env') | ForEach-Object {
    $l = $_.Trim()
    if ($l -match '^([A-Z_]+)=(.*)$') { Set-Item -Path "env:$($Matches[1])" -Value $Matches[2] }
}
if (-not $env:BINANCE_REAL_API_KEY -or -not $env:BINANCE_REAL_API_SECRET) {
    Say "BINANCE_REAL_API_KEY / BINANCE_REAL_API_SECRET are not in local.env." 'Red'
    Say "Create the key on binance.com with WITHDRAWALS DISABLED and an IP whitelist," 'Red'
    Say "add both lines to local.env, then run this again. Nothing was started." 'Red'
    exit 1
}

$env:JAVA_HOME        = 'C:\Users\Asus_F15\.jdks\ms-21.0.9'
$env:REAL_TRADING     = 'ARMED'
$env:REAL_MODE        = $Mode
$env:MAX_POSITIONS    = '14'
$env:DEFAULT_LEVERAGE = '2'
$env:TP_R_MULTIPLE    = '1.75'
$env:BOOK_LEDGER_PATH = Join-Path $dir 'book-ledger-real.json'

Say "stopping anything of ours still running..."
# Scoped kills: only processes carrying the book_real marker. The demo machine is not ours.
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -like '*autoscan*' -and $_.CommandLine -like '*book_real*' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -Confirm:$false }
Get-CimInstance Win32_Process -Filter "Name='java.exe'" |
    Where-Object { $_.CommandLine -like '*book_real*' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -Confirm:$false }
Start-Sleep -Seconds 4

# The bot replays the whole book at boot; a stale CLOSE from a previous run replays
# against whatever holds that symbol now (the 17.08 demo lesson). Fresh book every run;
# positions re-arm from the ledger, wanted entries are re-issued by the scanner.
$script = Join-Path $dir 'book_real.txt'
if ((Test-Path $script) -and (Get-Item $script).Length -gt 0) {
    $stamp = (Get-Item $script).LastWriteTime.ToString('yyyyMMdd-HHmmss')
    Move-Item $script (Join-Path $dir "book_real.$stamp.txt") -Force
    Get-ChildItem (Join-Path $dir 'book_real.*.txt') |
        Sort-Object LastWriteTime -Descending | Select-Object -Skip 10 |
        Remove-Item -Force -ErrorAction SilentlyContinue
}
Set-Content -Path $script -Encoding ascii -Value "# real-venue book - autoscan appends, the bot executes"

# Keep the last ten runs' logs: evidence of why a run stopped must survive the restart.
if ((Test-Path $botLog) -and (Get-Item $botLog).Length -gt 0) {
    $stamp = (Get-Item $botLog).LastWriteTime.ToString('yyyyMMdd-HHmmss')
    Move-Item $botLog (Join-Path $dir "bot_real.$stamp.err.log") -Force
    Get-ChildItem (Join-Path $dir 'bot_real.*.err.log') |
        Sort-Object LastWriteTime -Descending | Select-Object -Skip 10 |
        Remove-Item -Force -ErrorAction SilentlyContinue
}

# Build once, then run plain java: the real bot must not depend on a gradle daemon,
# and a compile error must surface here, before anything touches the exchange.
Say "building..."
& (Join-Path $root 'gradlew.bat') -q classes --console=plain 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Say "BUILD FAILED - nothing was started. Fix the build, then run this again." 'Red'
    exit 1
}
$jsonJar = Get-ChildItem "$env:USERPROFILE\.gradle\caches\modules-2\files-2.1\org.json\json" `
        -Recurse -Filter 'json-*.jar' -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $jsonJar) {
    Say "org.json jar not found in the gradle cache - run a gradle build once, then retry." 'Red'
    exit 1
}
$cp = @(
    (Join-Path $root 'build\classes\java\main'),
    (Join-Path $root 'build\resources\main'),
    $jsonJar.FullName
) -join ';'

Say "starting the REAL-venue bot (mode: $Mode)..."
Start-Process -FilePath (Join-Path $env:JAVA_HOME 'bin\java.exe') -WorkingDirectory $root -WindowStyle Hidden `
    -ArgumentList '-cp', $cp, 'com.bot.app.TestnetBot', '--source', 'manual', '--script', $script `
    -RedirectStandardOutput (Join-Path $dir 'bot_real.out.log') `
    -RedirectStandardError  $botLog

Say "waiting for the account to be adopted..."
$deadline = (Get-Date).AddMinutes(4)
do {
    Start-Sleep -Seconds 5
    $text = if (Test-Path $botLog) { Get-Content $botLog -Raw -ErrorAction SilentlyContinue } else { '' }
} until ($text -match 'adopted from the exchange|did not converge|REAL_TRADING|credentials are not set' `
         -and $text -match 'adopted from the exchange|did not converge' `
         -or (Get-Date) -gt $deadline)

if ($text -match 'did not converge') {
    Say "BOOT HALTED - the bot could not confirm every position on the REAL account." 'Red'
    Say "Something is on the exchange that this machine does not know. Closes still work," 'Red'
    Say "opens do not. Inspect the account before doing anything else." 'Red'
    exit 1
}
if ($text -notmatch 'adopted from the exchange') {
    Say "the bot did not report within 4 minutes - check $botLog" 'Yellow'
    exit 1
}
($text -split "`n" | Select-String 're-armed|adopted from the exchange|REAL EXCHANGE|OBSERVE') |
    ForEach-Object { Say ("  " + $_.ToString().Trim()) }

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
        '--bot-log', $botLog,
        '--venue', 'real',
        '--workdir', $dir,
        '--by-cap','--top','100','--lookback','30','--interval','3600',
        '--max-positions','14','--leverage','2' `
        -RedirectStandardOutput (Join-Path $dir 'autoscan.out.log') `
        -RedirectStandardError  (Join-Path $dir 'autoscan.err.log')
} catch {
    Say "SCANNER FAILED TO START: $($_.Exception.Message)" 'Red'
    exit 1
}

Start-Sleep -Seconds 6
$scannerAlive = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -like '*autoscan*' -and $_.CommandLine -like '*book_real*' }).Count -ge 1
if (-not $scannerAlive) {
    Say "SCANNER DIED IMMEDIATELY - see $dir\autoscan.err.log" 'Red'
    exit 1
}

if ($Mode -eq 'observe') {
    Say "running in OBSERVE mode: reading the account, opening NOTHING." 'Yellow'
    Say "When the observe run looks right: tools\start-real.ps1 -Mode trade" 'Yellow'
} else {
    Say "running: REAL-venue bot + hourly scanner. Do not let the laptop sleep." 'Green'
}
exit 0
