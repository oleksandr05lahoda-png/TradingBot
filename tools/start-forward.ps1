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

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$fwd  = Join-Path $root 'analysis\forward'

Write-Host "stopping anything still running..."
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

$script = Join-Path $fwd 'book_live.txt'
if (-not (Test-Path $script)) {
    Set-Content -Path $script -Encoding ascii -Value "# live forward book - autoscan appends, the bot executes"
}

Write-Host "starting the bot..."
Start-Process -FilePath (Join-Path $root 'gradlew.bat') -WorkingDirectory $root -WindowStyle Hidden `
    -ArgumentList 'run','-q','--console=plain',"`"--args=--source manual --script $script`"" `
    -RedirectStandardOutput (Join-Path $fwd 'bot_live.out.log') `
    -RedirectStandardError  (Join-Path $fwd 'bot_live.err.log')

# The scanner must not start before the bot has adopted the book: it reads the
# bot's log to detect a halt, and an empty log would read as "healthy".
Write-Host "waiting for the book to be adopted..."
$log = Join-Path $fwd 'bot_live.err.log'
$deadline = (Get-Date).AddMinutes(4)
do {
    Start-Sleep -Seconds 5
    $text = if (Test-Path $log) { Get-Content $log -Raw -ErrorAction SilentlyContinue } else { '' }
} until ($text -match 'adopted from the exchange|did not converge' -or (Get-Date) -gt $deadline)

if ($text -match 'did not converge') {
    Write-Host "BOOT HALTED - the bot could not confirm every position. Closes still work," -ForegroundColor Red
    Write-Host "opens do not. Ask Claude before trading on." -ForegroundColor Red
    exit 1
}
if ($text -notmatch 'adopted from the exchange') {
    Write-Host "the bot did not report within 4 minutes - check $log" -ForegroundColor Yellow
    exit 1
}
($text -split "`n" | Select-String 're-armed|adopted from the exchange') | ForEach-Object { Write-Host "  $_" }

Write-Host "starting the scanner..."
Start-Process -FilePath 'python' -WorkingDirectory $root -WindowStyle Hidden -ArgumentList `
    (Join-Path $root 'tools\scanner\autoscan.py'),
    '--script', $script,
    '--bot-log', $log,
    '--by-cap','--top','100','--lookback','30','--interval','3600',
    '--max-positions','14','--leverage','2' `
    -RedirectStandardOutput (Join-Path $fwd 'autoscan.out.log') `
    -RedirectStandardError  (Join-Path $fwd 'autoscan.err.log')

Start-Sleep -Seconds 3
Write-Host ""
Write-Host "running: bot + hourly scanner. Do not let the laptop sleep." -ForegroundColor Green
Write-Host "  book:    $fwd\book-ledger.json"
Write-Host "  bot log: $log"
Write-Host "  scanner: $fwd\autoscan.log"
