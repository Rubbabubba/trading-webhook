param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $repoRoot '.venv/Scripts/python.exe'
$configPath = Join-Path $repoRoot 'configs/ncaaf_smu_fsu_20260907.json'
$outputPath = Join-Path $repoRoot 'ncaaf_paper/smu_fsu_20260907'
if (-not (Test-Path -LiteralPath $pythonPath)) { throw 'Create the repository Python virtual environment first.' }
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
$pidPath = Join-Path $outputPath 'collector.pid'
if (Test-Path -LiteralPath $pidPath) {
    $collectorId = [int](Get-Content -LiteralPath $pidPath -Raw).Trim()
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $collectorId" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -like '*opportunity_lab.college_football_paper*') {
        Write-Output "Collector is already running with PID $collectorId."
        exit 0
    }
}
$arguments = @('-m', 'opportunity_lab.college_football_paper', '--config', ('"{0}"' -f $configPath), '--output', ('"{0}"' -f $outputPath))
$collectorProcess = Start-Process -FilePath $pythonPath -ArgumentList $arguments -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $outputPath 'collector.stdout.log') -RedirectStandardError (Join-Path $outputPath 'collector.stderr.log')
Set-Content -LiteralPath $pidPath -Value $collectorProcess.Id
Start-Sleep -Seconds 2
$collectorProcess.Refresh()
if ($collectorProcess.HasExited) {
    throw "Collector exited. Inspect $outputPath/collector.stderr.log."
}
Write-Output "Paper collector started with PID $($collectorProcess.Id). Status: $outputPath/status.json"
