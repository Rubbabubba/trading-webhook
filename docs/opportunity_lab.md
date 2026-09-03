# Opportunity Lab

Opportunity Lab is the isolated research, shadow, and eventual execution plane for non-Regime opportunities. It reuses control patterns from the Regime system without sharing its ledger, credentials, capital allocation, or kill switch.

## Locked roadmap

The canonical seven-candidate registry is `opportunity_lab/catalog.py`. A candidate is never removed because early results are poor; its status and evidence are updated instead. The initial active candidate is cost-aware, long/flat Alpaca crypto regime trading because it has the fewest external dependencies.

## Promotion lifecycle

`queued -> research -> backtest_passed -> shadow -> limited_live -> live`

Promotion is evidence-based rather than calendar-based. Backtests must use completed data, chronological holdouts, transaction costs, and parameter stability checks. Shadow promotion additionally requires executable quotes, measured latency and slippage, reconciliation, restart recovery, and no unresolved risk-control failures. Live submission is hard-closed until a reviewed implementation adds an explicit per-strategy transport gate.

## Commands

```powershell
python -m opportunity_lab.cli catalog
python -m opportunity_lab.cli backtest-crypto --symbol BTC/USD --days 730 --timeframe 1Hour
```

The crypto command uses Opportunity Lab-specific Alpaca market-data environment variables, makes no account mutation, selects parameters only on the older 70% of bars, and reports the untouched newer 30% separately.

The research output includes a fee-adjusted buy-and-hold benchmark, a fixed chronological holdout, five expanding-window walk-forward folds, results for every neighboring parameter set, four fee/slippage stress scenarios, and a deterministic 2,000-run bootstrap of holdout trades. These are diagnostics rather than a live-readiness declaration. Empty trade samples are reported as missing evidence rather than converted into favorable statistics.

The second research generation compares dual moving-average trend, slope-confirmed adaptive trend, channel breakout, volatility-filtered momentum, and long-only mean-reversion configurations. A configuration is ineligible unless its training result is positive, has at least eight closed trades, and keeps compounded drawdown at or below 35%. If every training candidate fails, the lab selects nothing and does not manufacture holdout or Monte Carlo evidence. The output also reports the buy-and-hold result over the exact holdout interval and strategy exposure hours.

## Service boundary

Deploy the eventual API/dashboard and continuous worker separately from `intraday_app.py`. Use a distinct database schema, worker secret, administrator surface, credentials, risk budget, and emergency stop. Shared code should be limited to generic HTTP, authentication, and reporting utilities.

The first web service entry point is `uvicorn opportunity_app:app --host 0.0.0.0 --port $PORT`. Its catalog and backtest routes are operator-protected, and the application contains no execution route or broker-order transport.

The protected `/dashboard/opportunity-lab` page provides a browser runner for BTC/USD and ETH/USD research. HTTP Basic authentication accepts any username and requires `OPPORTUNITY_ADMIN_SECRET` as the password.

## Render environment

The initial research-only web service requires:

- `OPPORTUNITY_ADMIN_SECRET`
- `OPPORTUNITY_ALPACA_API_KEY_ID`
- `OPPORTUNITY_ALPACA_API_SECRET_KEY`

Do not configure `ADMIN_SECRET`, `WORKER_SECRET`, Regime ledger paths, or Regime live-trading controls on this service. The Alpaca values may initially reference the same underlying account credentials, but their environment-variable names are deliberately separate so the service never inherits trading authority accidentally. Before an execution adapter is introduced, use separately bounded credentials or a separate broker account and add independent Opportunity Lab execution and kill-switch variables.
