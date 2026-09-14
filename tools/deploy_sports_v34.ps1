$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot
foreach ($target in @('sports_paper/live_v34_20260913/paper.sqlite3','sports_paper/churn_v11_20260913/paper.sqlite3')) {
    if (Test-Path -LiteralPath (Join-Path $repoRoot $target)) { throw "Migration destination already exists: $target" }
}
$success = $false
try {
    Disable-ScheduledTask -TaskName 'Codex Sports Live V33 Recovery 20260912' | Out-Null
    Disable-ScheduledTask -TaskName 'Codex Sports Churn Paper Recovery 20260913' | Out-Null
    Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { $_.CommandLine -match 'opportunity_lab\.(sports_live_v33|sports_churn_paper)(\s|$)' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    & .venv/Scripts/python.exe tools/migrate_sports_v34.py
    if ($LASTEXITCODE -ne 0) { throw 'State-preserving migration failed' }
    & tools/start_sports_live_v34.ps1
    & tools/start_sports_churn_v11.ps1
    & tools/install_sports_live_v34_recovery.ps1
    & tools/install_sports_churn_v11_recovery.ps1
    $success = $true
    Write-Output 'Both replacement paper workers started; recovery tasks installed.'
} finally {
    if (-not $success) {
        Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { $_.CommandLine -match 'opportunity_lab\.sports_live_v34(\s|$)|configs/sports_churn_v11_20260913/manifest.json' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
        foreach ($folder in @('live_v33_20260912','churn_20260913')) {
            $pausePath = Join-Path $repoRoot "sports_paper/$folder/PAUSE"
            if ((Test-Path -LiteralPath $pausePath) -and (Get-Content -LiteralPath $pausePath -Raw).StartsWith('migration_v34:')) { Remove-Item -LiteralPath $pausePath }
        }
        Enable-ScheduledTask -TaskName 'Codex Sports Live V33 Recovery 20260912' | Out-Null
        Enable-ScheduledTask -TaskName 'Codex Sports Churn Paper Recovery 20260913' | Out-Null
        & tools/start_sports_live_v33.ps1
        & tools/start_sports_churn.ps1
    }
}
