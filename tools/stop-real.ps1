# Stops the REAL-venue bot and its scanner. Open positions are NOT touched: their
# stops and takes rest on the exchange and keep working with this machine off.
#
#   powershell -ExecutionPolicy Bypass -File tools\stop-real.ps1

$ErrorActionPreference = 'Continue'

# Scoped to the real machine (book_real): the demo machine must never be stopped from here.
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -like '*autoscan*' -and $_.CommandLine -like '*book_real*' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -Confirm:$false; Write-Host "scanner stopped" }

Get-CimInstance Win32_Process -Filter "Name='java.exe'" |
    Where-Object { $_.CommandLine -like '*book_real*' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -Confirm:$false; Write-Host "bot stopped" }

Write-Host ""
Write-Host "Open positions keep their stops and takes on the exchange." -ForegroundColor Green
Write-Host "No new trades will be opened, and none will be closed by signal, until you"
Write-Host "run tools\start-real.ps1 again."
