param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $repoRoot 'configs/sports_capture_20260910/manifest.json'
$outputPath = Join-Path $repoRoot 'sports_paper/capture_20260910'
$pythonPath = Join-Path $repoRoot '.venv/Scripts/python.exe'
# The launcher is bounded to the frozen experiment, including repeated task runs.
if ([DateTimeOffset]::UtcNow -ge [DateTimeOffset]::Parse('2026-09-15T12:15:00Z')) { exit 0 }
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
if (Test-Path -LiteralPath (Join-Path $outputPath 'PAUSE')) { exit 0 }
$pidPath = Join-Path $outputPath 'capture.pid'
if (Test-Path -LiteralPath $pidPath) {
    $captureId = [int](Get-Content -LiteralPath $pidPath -Raw).Trim()
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $captureId" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -like '*opportunity_lab.sports_capture_v3*') {
        Write-Output "Capture already running with PID $captureId."
        exit 0
    }
}
$arguments = @('-m', 'opportunity_lab.sports_capture_v3', '--manifest', ('"{0}"' -f $manifestPath), '--output', ('"{0}"' -f $outputPath))
$captureProcess = Start-Process -FilePath $pythonPath -ArgumentList $arguments -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $outputPath 'capture.stdout.log') -RedirectStandardError (Join-Path $outputPath 'capture.stderr.log')
Set-Content -LiteralPath $pidPath -Value $captureProcess.Id
Start-Sleep -Seconds 2
$captureProcess.Refresh()
if ($captureProcess.HasExited) { throw 'Capture exited. Check capture.stderr.log.' }
Write-Output "Raw capture started with PID $($captureProcess.Id)."
