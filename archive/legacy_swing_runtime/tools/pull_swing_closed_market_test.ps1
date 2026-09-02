param(
    [string]$BaseUrl = $env:TRADING_WEBHOOK_BASE_URL,
    [string]$AdminSecret = $env:TRADING_WEBHOOK_ADMIN_SECRET,
    [string]$OutputDir = "$env:USERPROFILE\TradingDiagnostics\swing_closed_market_test",
    [int]$TimeoutSec = 180
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
    @{ Name = "swing_runtime_config"; Url = "$BaseUrl/diagnostics/swing_runtime_config" },
    @{ Name = "runtime_coverage_preview"; Url = "$BaseUrl/diagnostics/runtime_coverage_preview" },
    @{ Name = "candidate_coverage_opportunity_audit"; Url = "$BaseUrl/diagnostics/candidate_coverage_opportunity_audit?limit=25" },
    @{ Name = "current_scan_suppression_truth"; Url = "$BaseUrl/diagnostics/current_scan_suppression_truth?limit=25" },
    @{ Name = "market_open_selection_audit_light"; Url = "$BaseUrl/diagnostics/market_open_selection_audit_light?limit=25" },
    @{ Name = "swing_submit_path_trace"; Url = "$BaseUrl/diagnostics/swing_submit_path_trace?heavy=false&limit=25" },
    @{ Name = "selected_submission_truth_light"; Url = "$BaseUrl/diagnostics/selected_submission_truth_light" },
    @{ Name = "active_exit_protection_truth"; Url = "$BaseUrl/diagnostics/active_exit_protection_truth" },
    @{ Name = "breakout_stall_loss_containment"; Url = "$BaseUrl/diagnostics/breakout_stall_loss_containment" },
    @{ Name = "worker_exit_status"; Url = "$BaseUrl/diagnostics/worker_exit_status" },
    @{ Name = "broker_daily_goal_truth"; Url = "$BaseUrl/diagnostics/broker_daily_goal_truth" },
    @{ Name = "broker_reconciled_strategy_attribution"; Url = "$BaseUrl/diagnostics/broker_reconciled_strategy_attribution?limit=10" },
    @{ Name = "swing_performance_alignment_brief"; Url = "$BaseUrl/diagnostics/swing_performance_alignment_brief?limit=10" },
    @{ Name = "first_2k_rank_relaxation_replay"; Url = "$BaseUrl/diagnostics/first_2k_rank_relaxation_replay?limit=25" },
    @{ Name = "breakout_distance_relaxation_replay"; Url = "$BaseUrl/diagnostics/breakout_distance_relaxation_replay?limit=25" }
)

$summary = [ordered]@{
    ok = $true
    generated_local = (Get-Date).ToString("o")
    base_url = $BaseUrl
    output_dir = $OutputDir
    results = @()
    quick_read = [ordered]@{}
}

foreach ($target in $targets) {
    $name = $target.Name
    $url = $target.Url
    $latestPath = Join-Path $OutputDir "latest_$name.json"
    $archivePath = Join-Path $archiveDir "${stamp}_$name.json"

    try {
        $response = Invoke-RestMethod -Method Get -Uri $url -Headers $headers -TimeoutSec $TimeoutSec
        $json = $response | ConvertTo-Json -Depth 100
        Set-Content -LiteralPath $latestPath -Value $json -Encoding UTF8
        Set-Content -LiteralPath $archivePath -Value $json -Encoding UTF8

        $summary.results += [ordered]@{
            name = $name
            ok = $true
            latest_path = $latestPath
            archive_path = $archivePath
            patch_version = $response.patch_version
            mode = $response.mode
            recommended_action = $response.recommended_action
        }

        if ($name -eq "runtime_coverage_preview") {
            $summary.quick_read.runtime_coverage = [ordered]@{
                scanned_or_previewed = $response.preview_count
                configured_symbols = $response.configured_symbol_count
                recommended_action = $response.recommended_action
            }
        }

        if ($name -eq "current_scan_suppression_truth") {
            $summary.quick_read.current_scan = [ordered]@{
                selected_symbols = $response.selected_symbols
                eligible_symbols = $response.eligible_new_entry_symbols
                rejected_count = $response.rejected_count
                recommended_action = $response.recommended_action
            }
        }

        if ($name -eq "active_exit_protection_truth") {
            $summary.quick_read.exit_protection = [ordered]@{
                position_count = $response.summary.position_count
                missing_protection_count = $response.summary.missing_protection_count
                exit_watch_count = $response.summary.exit_watch_count
                all_active_positions_protected = $response.summary.all_active_positions_protected
            }
        }

        if ($name -eq "breakout_stall_loss_containment") {
            $summary.quick_read.breakout_containment = [ordered]@{
                breakout_position_count = $response.breakout_position_count
                partial_profit_bias_ready_symbols = $response.partial_profit_bias_ready_symbols
                stall_loss_reduce_first_ready_symbols = $response.stall_loss_reduce_first_ready_symbols
                recommended_action = $response.recommended_action
            }
        }

        if ($name -eq "broker_reconciled_strategy_attribution") {
            $summary.quick_read.performance = [ordered]@{
                recommended_action = $response.recommended_action
                recommended_actions = $response.recommended_actions
            }
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

$summaryJson = $summary | ConvertTo-Json -Depth 100
Set-Content -LiteralPath (Join-Path $OutputDir "latest_swing_closed_market_test_summary.json") -Value $summaryJson -Encoding UTF8
Set-Content -LiteralPath (Join-Path $archiveDir "${stamp}_swing_closed_market_test_summary.json") -Value $summaryJson -Encoding UTF8

$summaryJson

if (-not $summary.ok) {
    exit 1
}

exit 0