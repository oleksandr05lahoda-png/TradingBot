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
    [string]$Mode = 'observe'
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
# Book width IS the real daily bound: the kill switch blocks new entries after -3% but
# closes nothing, so a correlated gap night costs width x 0.5% plus gap slippage.
# 10 is the operator's choice (20.08), knowing it puts a worst all-stops night near -5%
# rather than the -3% the kill switch advertises. The -8% experiment stop still binds.
$env:MAX_POSITIONS    = '10'
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
# Start-Process instead of `& gradlew 2>&1`: under ErrorActionPreference=Stop, PS 5.1
# throws on any native stderr line, which killed this script's own failure diagnostics.
Say "building..."
$buildOut = Join-Path $dir 'build.out.log'
$buildErr = Join-Path $dir 'build.err.log'
$build = Start-Process -FilePath (Join-Path $root 'gradlew.bat') -WorkingDirectory $root `
    -ArgumentList '-q', 'classes', '--console=plain' -WindowStyle Hidden -Wait -PassThru `
    -RedirectStandardOutput $buildOut -RedirectStandardError $buildErr
if ($build.ExitCode -ne 0) {
    Say "BUILD FAILED - nothing was started. Last compiler lines:" 'Red'
    Get-Content $buildErr -Tail 12 -ErrorAction SilentlyContinue | ForEach-Object { Say ("  " + $_) 'Red' }
    exit 1
}

# The exact jar build.gradle pins — never "newest in the shared cache", which any other
# project on this machine could silently change under a running real-money bot.
$jsonJar = Get-ChildItem "$env:USERPROFILE\.gradle\caches\modules-2\files-2.1\org.json\json" `
        -Recurse -Filter 'json-20240303.jar' -ErrorAction SilentlyContinue |
    Select-Object -First 1
if (-not $jsonJar) {
    Say "json-20240303.jar (the version build.gradle pins) is not in the gradle cache." 'Red'
    Say "Run a gradle build once, then retry. Refusing to guess another version." 'Red'
    exit 1
}

# Freeze this run's binaries: the scheduled demo launcher recompiles build\classes at every
# logon/unlock, and swapped class files under a live JVM corrupt lazily-loaded code paths —
# the emergency-close path being the worst possible victim. The real bot runs from its own
# immutable copy, taken now.
$runtime = Join-Path $dir 'runtime'
if (Test-Path $runtime) { Remove-Item $runtime -Recurse -Force }
New-Item -ItemType Directory -Force -Path $runtime | Out-Null
Copy-Item (Join-Path $root 'build\classes\java\main') (Join-Path $runtime 'classes') -Recurse
if (Test-Path (Join-Path $root 'build\resources\main')) {
    Copy-Item (Join-Path $root 'build\resources\main') (Join-Path $runtime 'resources') -Recurse
} else {
    New-Item -ItemType Directory -Force -Path (Join-Path $runtime 'resources') | Out-Null
}
Copy-Item $jsonJar.FullName (Join-Path $runtime 'json.jar')
$cp = @(
    (Join-Path $runtime 'classes'),
    (Join-Path $runtime 'resources'),
    (Join-Path $runtime 'json.jar')
) -join ';'

Say "starting the REAL-venue bot (mode: $Mode)..."
Start-Process -FilePath (Join-Path $env:JAVA_HOME 'bin\java.exe') -WorkingDirectory $root -WindowStyle Hidden `
    -ArgumentList '-cp', $cp, 'com.bot.app.TestnetBot', '--source', 'manual', '--script', $script `
    -RedirectStandardOutput (Join-Path $dir 'bot_real.out.log') `
    -RedirectStandardError  $botLog

Say "waiting for the account to be adopted..."
$deadline = (Get-Date).AddMinutes(4)
$text = ''
do {
    Start-Sleep -Seconds 5
    $text = if (Test-Path $botLog) { Get-Content $botLog -Raw -ErrorAction SilentlyContinue } else { '' }
    # A refusal at the venue gate is final — surface it now, not after a blind 4-minute wait.
    if ($text -match 'credentials are not set|REAL_TRADING is set|REAL_MODE is set') {
        Say "THE BOT REFUSED TO START:" 'Red'
        (Get-Content (Join-Path $dir 'bot_real.out.log') -Tail 8 -ErrorAction SilentlyContinue) +
            (Get-Content $botLog -Tail 8 -ErrorAction SilentlyContinue) |
            ForEach-Object { Say ("  " + $_) 'Red' }
        exit 1
    }
} until ((($text -match 'adopted from the exchange') -or ($text -match 'did not converge')) `
         -or ((Get-Date) -gt $deadline))

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

# Start-Process truncates its redirect targets; keep the last ten scanner runs' logs.
foreach ($base in @('autoscan.err.log', 'autoscan.out.log')) {
    $p = Join-Path $dir $base
    if ((Test-Path $p) -and (Get-Item $p).Length -gt 0) {
        $stamp = (Get-Item $p).LastWriteTime.ToString('yyyyMMdd-HHmmss')
        Move-Item $p (Join-Path $dir ($base -replace '\.log$', ".$stamp.log")) -Force
        Get-ChildItem (Join-Path $dir ($base -replace '\.log$', '.*.log')) |
            Sort-Object LastWriteTime -Descending | Select-Object -Skip 10 |
            Remove-Item -Force -ErrorAction SilentlyContinue
    }
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
        '--max-positions','10','--leverage','2' `
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
