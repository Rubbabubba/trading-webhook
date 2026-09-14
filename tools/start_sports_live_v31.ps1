param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
if ([DateTimeOffset]::UtcNow -ge [DateTimeOffset]::Parse('2026-09-18T12:00:00Z')) { exit 0 }
$outputPath = Join-Path $repoRoot 'sports_paper/live_v31_20260912'
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
if (Test-Path -LiteralPath (Join-Path $outputPath 'PAUSE')) { exit 0 }
$pidPath = Join-Path $outputPath 'paper.pid'
if (Test-Path -LiteralPath $pidPath) {
    $paperId = [int](Get-Content -LiteralPath $pidPath -Raw).Trim()
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $paperId" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -like '*opportunity_lab.sports_live_v31*') {
        Write-Output "Paper research already running with PID $paperId."
        exit 0
    }
}
foreach ($credentialName in @('KALSHI_API_KEY_ID','KALSHI_PRIVATE_KEY_PATH')) {
    $userSetting = [Environment]::GetEnvironmentVariable($credentialName, 'User')
    if ($userSetting) { [Environment]::SetEnvironmentVariable($credentialName, $userSetting, 'Process') }
}
$paperProcess = Start-Process -FilePath (Join-Path $repoRoot '.venv/Scripts/python.exe') -ArgumentList @('-m','opportunity_lab.sports_live_v31') -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $outputPath 'paper.stdout.log') -RedirectStandardError (Join-Path $outputPath 'paper.stderr.log')
Set-Content -LiteralPath $pidPath -Value $paperProcess.Id
Start-Sleep -Seconds 3
$paperProcess.Refresh()
if ($paperProcess.HasExited) { throw 'Paper research exited. Check paper.stderr.log.' }
Write-Output "Paper research started with PID $($paperProcess.Id)."
