param(
    [string]$TaskName = "Trading Operator Bundle Collector",
    [string]$ScriptPath = "$PSScriptRoot\pull_operator_bundles.ps1"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ScriptPath)) {
    throw "Collector script not found: $ScriptPath"
}

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""

$times = @("09:45", "11:30", "13:30", "15:45")
$triggers = foreach ($time in $times) {
    New-ScheduledTaskTrigger -Daily -At $time
}

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $triggers `
    -Settings $settings `
    -Description "Pulls trading diagnostic bundles during market hours." `
    -Force | Out-Null

Write-Host "Installed scheduled task: $TaskName"
Write-Host "Runs daily at: $($times -join ', ')"
Write-Host "Script: $ScriptPath"