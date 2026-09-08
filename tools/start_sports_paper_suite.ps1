param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $repoRoot '.venv/Scripts/python.exe'
$manifestPath = Join-Path $repoRoot 'configs/sports_week_20260908/manifest.json'
$outputPath = Join-Path $repoRoot 'sports_paper/week_20260908'
if (-not (Test-Path -LiteralPath $pythonPath)) { throw 'Create the repository Python virtual environment first.' }
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
if (Test-Path -LiteralPath (Join-Path $outputPath 'PAUSE')) { throw 'The paper suite is deliberately paused.' }
$pidPath = Join-Path $outputPath 'suite.pid'
if (Test-Path -LiteralPath $pidPath) {
    $suiteId = [int](Get-Content -LiteralPath $pidPath -Raw).Trim()
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $suiteId" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -like '*opportunity_lab.sports_paper_suite*') {
        Write-Output "Paper suite is already running with PID $suiteId."
        exit 0
    }
}
$arguments = @('-m', 'opportunity_lab.sports_paper_suite', 'run', '--manifest', ('"{0}"' -f $manifestPath), '--output', ('"{0}"' -f $outputPath))
$suiteProcess = Start-Process -FilePath $pythonPath -ArgumentList $arguments -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $outputPath 'suite.stdout.log') -RedirectStandardError (Join-Path $outputPath 'suite.stderr.log')
Set-Content -LiteralPath $pidPath -Value $suiteProcess.Id
Start-Sleep -Seconds 2
$suiteProcess.Refresh()
if ($suiteProcess.HasExited) { throw "Paper suite exited. Inspect $outputPath/suite.stderr.log." }
Write-Output "Paper suite started with PID $($suiteProcess.Id). Status: $outputPath/status.json"
