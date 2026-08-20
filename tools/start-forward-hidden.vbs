' Launches start-forward.ps1 with a truly hidden window.
' powershell.exe's own -WindowStyle Hidden isn't enough on Windows 11: when
' Windows Terminal is the default console host, it creates a visible window
' for the new console before the hidden style takes effect. WScript.Shell.Run
' with windowStyle 0 hides it at creation time instead, so Task Scheduler's
' "at logon" / "on unlock" triggers no longer flash a terminal on screen.
Set shell = CreateObject("WScript.Shell")
scriptDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
cmd = "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File """ & scriptDir & "\start-forward.ps1"""
shell.Run cmd, 0, False
