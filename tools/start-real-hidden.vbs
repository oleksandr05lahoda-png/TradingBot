' Launches start-real.ps1 -Mode trade with a truly hidden window, for Task Scheduler
' and the Startup folder. Mirrors start-forward-hidden.vbs and exists for the same
' reason: powershell's own -WindowStyle Hidden still flashes a Windows Terminal
' window at creation; WScript.Shell.Run with windowStyle 0 does not.
'
' Safe to fire repeatedly: without -Force the launcher is a no-op while both halves
' are alive and the log is fresh ("already running - nothing to do"), so wiring this
' to "at logon" plus a 15-minute repeating trigger restarts a dead machine without
' ever tearing down a live one.
Set shell = CreateObject("WScript.Shell")
scriptDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
cmd = "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File """ & scriptDir & "\start-real.ps1"" -Mode trade"
shell.Run cmd, 0, False
