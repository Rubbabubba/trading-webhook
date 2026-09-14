param()
$ErrorActionPreference = 'Stop'
if ([DateTimeOffset]::UtcNow -ge [DateTimeOffset]::Parse('2026-09-15T12:15:00Z')) { exit 0 }
$repoRoot = Split-Path -Parent $PSScriptRoot
$failed = $false
foreach ($entry in @(
    @{ Script = 'start_sports_capture.ps1'; Pause = 'sports_paper/capture_20260910/PAUSE' },
    @{ Script = 'start_sports_paper_suite.ps1'; Pause = 'sports_paper/week_20260908/PAUSE' }
)) {
    if (Test-Path -LiteralPath (Join-Path $repoRoot $entry.Pause)) { continue }
    & powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -File (Join-Path $PSScriptRoot $entry.Script)
    if ($LASTEXITCODE -ne 0) { $failed = $true }
}
if ($failed) { exit 1 }
