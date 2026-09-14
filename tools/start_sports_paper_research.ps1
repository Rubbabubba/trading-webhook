param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
if ([DateTimeOffset]::UtcNow -ge [DateTimeOffset]::Parse('2026-09-18T12:00:00Z')) { exit 0 }
$outputPath = Join-Path $repoRoot 'sports_paper/research_20260910'
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
if (Test-Path -LiteralPath (Join-Path $outputPath 'PAUSE')) { exit 0 }
$pidPath = Join-Path $outputPath 'paper.pid'
if (Test-Path -LiteralPath $pidPath) {
    $paperId = [int](Get-Content -LiteralPath $pidPath -Raw).Trim()
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $paperId" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -like '*opportunity_lab.sports_paper_research*') {
        Write-Output "Paper research already running with PID $paperId."
        exit 0
    }
}
$paperProcess = Start-Process -FilePath (Join-Path $repoRoot '.venv/Scripts/python.exe') -ArgumentList @('-m','opportunity_lab.sports_paper_research') -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $outputPath 'paper.stdout.log') -RedirectStandardError (Join-Path $outputPath 'paper.stderr.log')
Set-Content -LiteralPath $pidPath -Value $paperProcess.Id
Start-Sleep -Seconds 3
$paperProcess.Refresh()
if ($paperProcess.HasExited) { throw 'Paper research exited. Check paper.stderr.log.' }
Write-Output "Paper research started with PID $($paperProcess.Id)."
