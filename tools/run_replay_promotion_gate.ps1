param(
    [string]$InputDir = "",
    [string]$OutputDir = "$env:USERPROFILE\TradingDiagnostics\replay_promotion_gate",
    [int]$MinTrades = 10,
    [double]$MinTotalPnl = 0.0,
    [double]$MinAvgR = 0.05,
    [double]$MinWinRate = 0.5,
    [double]$MaxDrawdown = 0.0,
    [int]$Limit = 100,
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $PSScriptRoot "replay_promotion_gate.py"

if (-not (Test-Path -LiteralPath $scriptPath)) {
    throw "Missing replay promotion gate script: $scriptPath"
}

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    $bundledPython = Join-Path $env:USERPROFILE ".cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
    if (Test-Path -LiteralPath $bundledPython) {
        $PythonExe = $bundledPython
    }
    else {
        $cmd = Get-Command python -ErrorAction SilentlyContinue
        if ($cmd) {
            $PythonExe = $cmd.Source
        }
    }
}

if ([string]::IsNullOrWhiteSpace($PythonExe) -or -not (Test-Path -LiteralPath $PythonExe)) {
    throw "Python executable not found. Pass -PythonExe or install Python."
}

$argsList = @(
    $scriptPath,
    "--output-dir", $OutputDir,
    "--min-trades", $MinTrades,
    "--min-total-pnl", $MinTotalPnl,
    "--min-avg-r", $MinAvgR,
    "--min-win-rate", $MinWinRate,
    "--max-drawdown", $MaxDrawdown,
    "--limit", $Limit
)

if (-not [string]::IsNullOrWhiteSpace($InputDir)) {
    $argsList += @("--input-dir", $InputDir)
}

Push-Location $repoRoot
try {
    & $PythonExe @argsList
}
finally {
    Pop-Location
}
