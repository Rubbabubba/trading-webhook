param(
    [string]$OutputDir = "$env:USERPROFILE\TradingDiagnostics\swing_two_week_replay",
    [int]$Days = 10,
    [int]$WarmupDays = 100,
    [string]$Symbols = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $PSScriptRoot "swing_two_week_replay.py"

if (-not (Test-Path -LiteralPath $scriptPath)) {
    throw "Missing replay script: $scriptPath"
}

if ([string]::IsNullOrWhiteSpace($env:APCA_API_KEY_ID) -and [string]::IsNullOrWhiteSpace($env:ALPACA_KEY_ID) -and [string]::IsNullOrWhiteSpace($env:ALPACA_API_KEY_ID)) {
    throw "Missing Alpaca key env. Set APCA_API_KEY_ID or ALPACA_KEY_ID."
}

if ([string]::IsNullOrWhiteSpace($env:APCA_API_SECRET_KEY) -and [string]::IsNullOrWhiteSpace($env:ALPACA_SECRET_KEY) -and [string]::IsNullOrWhiteSpace($env:ALPACA_API_SECRET_KEY)) {
    throw "Missing Alpaca secret env. Set APCA_API_SECRET_KEY or ALPACA_SECRET_KEY."
}

$argsList = @(
    $scriptPath,
    "--days", $Days,
    "--warmup-days", $WarmupDays,
    "--output-dir", $OutputDir
)

if (-not [string]::IsNullOrWhiteSpace($Symbols)) {
    $argsList += @("--symbols", $Symbols)
}

Push-Location $repoRoot
try {
    python @argsList
}
finally {
    Pop-Location
}