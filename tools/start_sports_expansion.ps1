param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
if ([DateTimeOffset]::UtcNow -ge [DateTimeOffset]::Parse('2026-09-18T12:00:00Z')) { exit 0 }
$outputPath = Join-Path $repoRoot 'sports_paper/expansion_20260910'
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
if (Test-Path -LiteralPath (Join-Path $outputPath 'PAUSE')) { exit 0 }
$pidPath = Join-Path $outputPath 'capture.pid'
if (Test-Path -LiteralPath $pidPath) {
    $captureId = [int](Get-Content -LiteralPath $pidPath -Raw).Trim()
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $captureId" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -like '*opportunity_lab.sports_capture_expansion*') {
        Write-Output "Expansion already running with PID $captureId."
        exit 0
    }
}
$captureProcess = Start-Process -FilePath (Join-Path $repoRoot '.venv/Scripts/python.exe') -ArgumentList @('-m','opportunity_lab.sports_capture_expansion') -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $outputPath 'capture.stdout.log') -RedirectStandardError (Join-Path $outputPath 'capture.stderr.log')
Set-Content -LiteralPath $pidPath -Value $captureProcess.Id
Start-Sleep -Seconds 2
$captureProcess.Refresh()
if ($captureProcess.HasExited) { throw 'Expansion exited. Check capture.stderr.log.' }
Write-Output "Sports expansion started with PID $($captureProcess.Id)."
