# Stops the bot and the scanner. Open positions are NOT touched: their stops and
# takes rest on the exchange and keep working with this machine off.
#
#   powershell -ExecutionPolicy Bypass -File tools\stop-forward.ps1

$ErrorActionPreference = 'Continue'

Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -like '*autoscan*' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -Confirm:$false; Write-Host "scanner stopped" }

if (Get-Process java -ErrorAction SilentlyContinue) {
    Get-Process java | Stop-Process -Force -Confirm:$false
    Write-Host "bot stopped"
}

Write-Host ""
Write-Host "Open positions keep their stops and takes on the exchange." -ForegroundColor Green
Write-Host "No new trades will be opened, and none will be closed by signal, until you"
Write-Host "run tools\start-forward.ps1 again."
