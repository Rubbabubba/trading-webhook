param(
    [string]$BaseUrl = $env:TRADING_WEBHOOK_BASE_URL,
    [string]$AdminSecret = $env:TRADING_WEBHOOK_ADMIN_SECRET,
    [string]$OutputDir = "$env:USERPROFILE\TradingDiagnostics",
    [int]$TimeoutSec = 90
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($BaseUrl)) {
    throw "Missing TRADING_WEBHOOK_BASE_URL environment variable."
}

$BaseUrl = $BaseUrl.TrimEnd("/")
$stamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$archiveDir = Join-Path $OutputDir "archive"

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
New-Item -ItemType Directory -Force -Path $archiveDir | Out-Null

$headers = @{}
if (-not [string]::IsNullOrWhiteSpace($AdminSecret)) {
    $headers["X-Admin-Secret"] = $AdminSecret
}

$targets = @(
    @{
        Name = "swing"
        Url = "$BaseUrl/diagnostics/operator_bundle?scope=swing&refresh_live=true&limit=50"
    },
    @{
        Name = "intraday"
        Url = "$BaseUrl/diagnostics/operator_bundle?scope=intraday&refresh_live=true&limit=50"
    },
    @{
        Name = "performance"
        Url = "$BaseUrl/diagnostics/operator_bundle?scope=performance&refresh_live=true&limit=50"
    },
    @{
        Name = "risk"
        Url = "$BaseUrl/diagnostics/operator_bundle?scope=risk&refresh_live=true&limit=50"
    },
    @{
        Name = "no_trade"
        Url = "$BaseUrl/diagnostics/no_trade_brief?refresh_live=true&limit=50"
    }
)

$summary = [ordered]@{
    ok = $true
    generated_local = (Get-Date).ToString("o")
    base_url = $BaseUrl
    output_dir = $OutputDir
    results = @()
}

foreach ($target in $targets) {
    $name = $target.Name
    $url = $target.Url
    $latestPath = Join-Path $OutputDir "latest_$name.json"
    $archivePath = Join-Path $archiveDir "${stamp}_$name.json"

    try {
        $response = Invoke-RestMethod `
            -Method Get `
            -Uri $url `
            -Headers $headers `
            -TimeoutSec $TimeoutSec

        $json = $response | ConvertTo-Json -Depth 100
        Set-Content -LiteralPath $latestPath -Value $json -Encoding UTF8
        Set-Content -LiteralPath $archivePath -Value $json -Encoding UTF8

        $summary.results += [ordered]@{
            name = $name
            ok = $true
            latest_path = $latestPath
            archive_path = $archivePath
        }
    }
    catch {
        $summary.ok = $false
        $summary.results += [ordered]@{
            name = $name
            ok = $false
            url = $url
            error = $_.Exception.Message
        }
    }
}

$summaryJson = $summary | ConvertTo-Json -Depth 20
Set-Content -LiteralPath (Join-Path $OutputDir "latest_pull_summary.json") -Value $summaryJson -Encoding UTF8
Set-Content -LiteralPath (Join-Path $archiveDir "${stamp}_pull_summary.json") -Value $summaryJson -Encoding UTF8

if (-not $summary.ok) {
    exit 1
}

exit 0