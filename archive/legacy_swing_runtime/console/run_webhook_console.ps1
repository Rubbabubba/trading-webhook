param(
    [int]$Port = 8090,
    [switch]$NoReload
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $Python)) {
    throw "Local Python environment not found. Create .venv and install requirements.txt first."
}

$Arguments = @("-m", "uvicorn", "webhook_console:app", "--host", "127.0.0.1", "--port", $Port)
if (-not $NoReload) {
    $Arguments += "--reload"
}

Write-Host "Webhook Console: http://127.0.0.1:$Port" -ForegroundColor Green
& $Python @Arguments
