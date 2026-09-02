# Operator Bundle Collector

This local collector pulls diagnostic bundles from the trading webhook and saves them for upload/analysis.

## Required Local Environment Variables

Set these on your Windows machine:

```powershell
[Environment]::SetEnvironmentVariable("TRADING_WEBHOOK_BASE_URL", "https://your-render-url.onrender.com", "User")
[Environment]::SetEnvironmentVariable("TRADING_WEBHOOK_ADMIN_SECRET", "your-admin-secret", "User")