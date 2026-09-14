param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
if ([DateTimeOffset]::UtcNow -ge [DateTimeOffset]::Parse('2026-09-21T14:30:00Z')) { exit 0 }
$outputPath = Join-Path $repoRoot 'sports_paper/broad_v11_20260914'
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
if (Test-Path -LiteralPath (Join-Path $outputPath 'PAUSE')) { exit 0 }
$pidPath = Join-Path $outputPath 'lab.pid'
if (Test-Path -LiteralPath $pidPath) {
    $paperId = [int](Get-Content -LiteralPath $pidPath -Raw).Trim()
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $paperId" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -like '*opportunity_lab.kalshi_broad_paper_v11*') {
        Write-Output "Broad paper experiment already running with PID $paperId."
        exit 0
    }
}
$paperProcess = Start-Process -FilePath (Join-Path $repoRoot '.venv/Scripts/python.exe') -ArgumentList @('-m','opportunity_lab.kalshi_broad_paper_v11') -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $outputPath 'lab.stdout.log') -RedirectStandardError (Join-Path $outputPath 'lab.stderr.log')
Set-Content -LiteralPath $pidPath -Value $paperProcess.Id
Start-Sleep -Seconds 3
$paperProcess.Refresh()
if ($paperProcess.HasExited) { throw 'Broad paper experiment exited. Check lab.stderr.log.' }
Write-Output "Broad paper experiment started with PID $($paperProcess.Id)."
