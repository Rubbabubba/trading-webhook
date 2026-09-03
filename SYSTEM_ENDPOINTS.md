# System endpoint manifest

Generated from the intraday-only application by `tools/generate_endpoint_manifest.py`. Do not edit endpoint rows manually.

Total application routes: **14**.

Dashboard and diagnostic detail require operator authentication. Worker routes require the worker secret.

## Operator dashboards

| Method | Endpoint |
|---|---|
| `GET` | [/dashboard/intraday](https://trading-webhook-q4d5.onrender.com/dashboard/intraday) |

## Regime intraday

| Method | Endpoint |
|---|---|
| `GET` | [/diagnostics/regime_intraday](https://trading-webhook-q4d5.onrender.com/diagnostics/regime_intraday) |
| `GET` | [/diagnostics/regime_intraday_ledger](https://trading-webhook-q4d5.onrender.com/diagnostics/regime_intraday_ledger) |
| `GET` | [/diagnostics/regime_intraday_readiness](https://trading-webhook-q4d5.onrender.com/diagnostics/regime_intraday_readiness) |
| `POST` | [/diagnostics/regime_intraday_replay](https://trading-webhook-q4d5.onrender.com/diagnostics/regime_intraday_replay) |
| `POST` | [/worker/regime_intraday_after_hours_replay](https://trading-webhook-q4d5.onrender.com/worker/regime_intraday_after_hours_replay) |
| `POST` | [/worker/regime_intraday_paper_close](https://trading-webhook-q4d5.onrender.com/worker/regime_intraday_paper_close) |
| `POST` | [/worker/regime_intraday_paper_mechanical_drill](https://trading-webhook-q4d5.onrender.com/worker/regime_intraday_paper_mechanical_drill) |
| `POST` | [/worker/regime_intraday_paper_reconcile](https://trading-webhook-q4d5.onrender.com/worker/regime_intraday_paper_reconcile) |
| `POST` | [/worker/regime_intraday_paper_roundtrip](https://trading-webhook-q4d5.onrender.com/worker/regime_intraday_paper_roundtrip) |
| `POST` | [/worker/regime_intraday_scan](https://trading-webhook-q4d5.onrender.com/worker/regime_intraday_scan) |

## Runtime workers and controls

| Method | Endpoint |
|---|---|

## Swing active diagnostics

| Method | Endpoint |
|---|---|

## Research and deprecation candidates

| Method | Endpoint |
|---|---|

## Shared and other

| Method | Endpoint |
|---|---|
| `GET` | [/](https://trading-webhook-q4d5.onrender.com/) |
| `GET` | [/diagnostics/route_catalog](https://trading-webhook-q4d5.onrender.com/diagnostics/route_catalog) |
| `GET` | [/health](https://trading-webhook-q4d5.onrender.com/health) |
