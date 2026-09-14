Option Explicit
' Use the windowless script host so PowerShell never flashes a startup console.
' Wait and forward its exit code so Task Scheduler still reports failures.
Dim shell, files, scriptPath, command, result
If WScript.Arguments.Count <> 1 Then WScript.Quit 2
Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")
scriptPath = files.GetAbsolutePathName(WScript.Arguments(0))
If Not files.FileExists(scriptPath) Then WScript.Quit 2
command = Chr(34) & shell.ExpandEnvironmentStrings("%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe") & Chr(34) & " -NoProfile -NonInteractive -WindowStyle Hidden -File " & Chr(34) & scriptPath & Chr(34)
result = shell.Run(command, 0, True)
WScript.Quit result
