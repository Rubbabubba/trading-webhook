param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$taskScript = Join-Path $PSScriptRoot 'start_sports_live_v34.ps1'
$taskAction = New-ScheduledTaskAction -Execute (Join-Path $env:SystemRoot 'System32/wscript.exe') -Argument ('//B //Nologo "{0}" "{1}"' -f (Join-Path $PSScriptRoot 'run_hidden_powershell.vbs'), $taskScript) -WorkingDirectory $repoRoot
$taskUser = (Get-ScheduledTask -TaskName 'Codex Sports Research Recovery 20260910').Principal.UserId
$taskTriggers = @((New-ScheduledTaskTrigger -AtLogOn -User $taskUser), (New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration (New-TimeSpan -Days 8)))
$taskPrincipal = New-ScheduledTaskPrincipal -UserId $taskUser -LogonType Interactive -RunLevel Limited
$taskSettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName 'Codex Sports Live V34 Recovery 20260913' -Action $taskAction -Trigger $taskTriggers -Principal $taskPrincipal -Settings $taskSettings -Description 'Recover corrected prospective paper research only, bounded through September 18 at 12:00 UTC.' -Force | Select-Object TaskName,State
Start-ScheduledTask -TaskName 'Codex Sports Live V34 Recovery 20260913'
