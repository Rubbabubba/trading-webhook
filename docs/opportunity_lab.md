# Opportunity Lab

Opportunity Lab is the isolated research, shadow, and eventual execution plane for non-Regime opportunities. It reuses control patterns from the Regime system without sharing its ledger, credentials, capital allocation, or kill switch.

## Locked roadmap

The canonical seven-candidate registry is `opportunity_lab/catalog.py`. A candidate is never removed because early results are poor; its status and evidence are updated instead. Full-coverage hourly, four-hour, and daily validation rejected the initial long/flat crypto regime implementation out of sample. ETH funding/basis remains a long-duration research candidate, while sports/prediction arbitrage is the active build.

## Funding/basis research

`opportunity_lab/crypto_basis.py` evaluates a market-neutral long-spot/short-derivative position from executable ask/bid prices. It accounts for basis convergence, funding, fees on both legs, round-trip slippage, derivative collateral, available capital, and executable depth. Positive normalized funding means the short receives payment.

The calculator reports profit per matched notional separately from annualized return on required capital. Annualization is a comparison metric, not a forecast. The protected dashboard and `/diagnostics/opportunity_lab/basis/evaluate` route accept manually sourced observations; `/diagnostics/opportunity_lab/basis/backtest` accepts normalized historical funding rates. Neither route connects to an exchange or submits orders.

Historical venue adapters must normalize each venue's funding sign, interval, contract size, quote currency, fee tier, and product eligibility before their data can enter this model. Venue availability and account eligibility must be confirmed before execution work begins.

## Sports and prediction arbitrage

`opportunity_lab/odds_arbitrage.py` is the venue-neutral scanner core. It normalizes American or decimal prices, adjusts winnings for commission, respects per-leg maximum stake and stake increments, allocates stakes across mutually exclusive outcomes, and verifies the worst rounded payout. An apparent mathematical edge is always blocked until the operator confirms that settlement, overtime, cancellation, participant, and market-definition rules are compatible across venues. The scanner has no bet-submission transport.

`opportunity_lab/kalshi_market_data.py` reads unauthenticated public Kalshi events and nested market quotes. It ranks displayed complete-set price dislocations but deliberately leaves every result ineligible until outcome exhaustiveness, actual fees, account eligibility, and jurisdiction are verified. It never reads an account or submits an order.

For events explicitly marked mutually exclusive, the collector also evaluates every two-outcome strategy that buys NO on both outcomes. Because both outcomes cannot resolve YES, at least one NO contract must pay out; this does not require the event's displayed markets to exhaust every possible result. The estimate uses displayed size, a conservative general taker-fee formula, and the latest leg close time. Results remain blocked pending the applicable series fee schedule and account/jurisdiction verification.

The collector also persists a two-sided market-making screen for every market with a valid top-of-book spread and depth. It models optimistic, base, and conservative fill/adverse-selection scenarios and applies the quadratic maker-fee coefficient even though some series do not charge maker fees. The screen is not a fill backtest: queue position, series-specific fees, and actual fills remain blockers, and no order transport exists.

On each scheduled run, the collector also downloads up to three pages of public trades from the prior two hours. The replay compares those prints with the previous stored quote, credits a hypothetical maker fill only after qualifying trade volume clears the displayed queue ahead, pairs opposing fills, marks residual inventory against the next executable quote, and deducts the conservative maker-fee estimate. Public prints cannot prove an account-specific fill or continuous quote presence, so replay profits remain ineligible research evidence.

The market-making evidence report chronologically separates the oldest two-thirds of stored replays from the newest third. It reports observed fills, paired round trips, marked P&L, and profitable replay counts for both periods. Retention requires at least 30 calibration replays, 15 validation replays, and positive marked P&L in both; an unsuccessful formulation remains available for retuning rather than retiring the candidate category.

Each new replay also reports queue-position sensitivity with 0%, 25%, 50%, and 100% of the displayed depth assumed ahead of the hypothetical order. The 100% case remains the conservative headline. Comparing the scenarios identifies whether an apparent edge is robust or depends on unrealistically favorable queue placement.

For U.S. Coinbase CFM products, `/diagnostics/opportunity_lab/coinbase/reconstruct-funding` reconstructs BTC or ETH hourly funding over 7–365 days from aligned CDE-future and Coinbase-spot hourly candles. It applies Coinbase's published `/24` premium scaling and 75% current/25% previous smoothing, but uses hourly closes instead of the official twenty three-minute representative-price samples. Its output is therefore explicitly a proxy, never official historical funding. Exact U.S. historical funding must be requested from Coinbase and should later be used to validate and calibrate the proxy.

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

The one-shot `opportunity_lab_worker.py` process calls only `/worker/opportunity-lab/collect-kalshi`. That route requires its own `OPPORTUNITY_WORKER_SECRET`, scans public market data, and persists scan summaries, fee-positive pair candidates, and the closest fee-adjusted pair in every eligible event in the dedicated `opportunity_lab` PostgreSQL schema. It cannot invoke Regime worker routes or submit orders. `/diagnostics/opportunity_lab/kalshi/history` reports recent collection runs.

`/diagnostics/opportunity_lab/scoreboard` aggregates the most recent observation window. After at least 60 runs spanning approximately 72 hours, it recommends investigating execution feasibility if any pair was fee-positive, continuing higher-frequency validation if the closest result was within 0.25% ROI of break-even, or rejecting the current strategy otherwise. This verdict is research metadata and cannot enable execution.

## Render environment

The initial research-only web service requires:

- `OPPORTUNITY_ADMIN_SECRET`
- `OPPORTUNITY_ALPACA_API_KEY_ID`
- `OPPORTUNITY_ALPACA_API_SECRET_KEY`

The Coinbase funding/basis research adapter additionally uses a Coinbase Advanced Trade key with view-only permission:

- `OPPORTUNITY_COINBASE_API_KEY_NAME`
- `OPPORTUNITY_COINBASE_API_KEY_SECRET`

Continuous observation additionally requires:

- `OPPORTUNITY_DATABASE_URL` — the internal URL for a separate Render Postgres database
- `OPPORTUNITY_WORKER_SECRET` — a new random secret used only by the Opportunity Lab collector

A scheduled collector service uses:

- `OPPORTUNITY_SERVICE_URL=https://opportunity-lab.onrender.com`
- `OPPORTUNITY_WORKER_SECRET` — the same Opportunity Lab-only worker secret
- command: `python opportunity_lab_worker.py`

Do not reuse `DATABASE_URL`, `WORKER_SECRET`, or any Regime schema. Free Render Postgres is suitable only for the initial validation window because it expires after 30 days and has no backups.

The protected `/diagnostics/opportunity_lab/coinbase/status` route tests authentication and returns sanitized perpetual product economics. It deliberately omits account balances and cannot place or cancel orders. Never grant this research key trade, transfer, send, withdrawal, or management permissions.

Do not configure `ADMIN_SECRET`, `WORKER_SECRET`, Regime ledger paths, or Regime live-trading controls on this service. The Alpaca values may initially reference the same underlying account credentials, but their environment-variable names are deliberately separate so the service never inherits trading authority accidentally. Before an execution adapter is introduced, use separately bounded credentials or a separate broker account and add independent Opportunity Lab execution and kill-switch variables.
# Cross-exchange crypto monitor

The Opportunity Lab includes a read-only Coinbase/Kraken BTC and ETH spot comparison. It requests public order books, sweeps multiple price levels up to the configured per-leg notional, and evaluates both buy/sell venue directions. The default model uses the observed Coinbase Intro 1 taker rate (1.2%) and Kraken entry-tier taker rate (0.8%).

Results are based on separately requested public snapshots, so they are never executable guarantees. Simultaneous-fill risk, venue inventory, withdrawal/rebalancing costs, account eligibility, and jurisdiction remain blockers. The scanner cannot submit orders and exposes no balance or account data.

The hourly Opportunity Lab collector stores BTC and ETH observations in `opportunity_lab.cross_exchange_observations`. The profitability scoreboard reports the best fee-adjusted observation and count of positive directions over the selected window.

The Kraken triangular monitor evaluates both USD→BTC→ETH→USD and USD→ETH→BTC→USD using the public BTC/USD, ETH/USD, and ETH/BTC books. It consumes visible depth and compounds the conservative entry-tier taker fee on all three legs. Observations are stored hourly in `opportunity_lab.triangular_observations` and surfaced on the same scoreboard. Three-leg atomicity, latency, and the account's actual fee tier remain explicit blockers.

The Dallas weather-value monitor compares active `KXHIGHTDAL` and `KXLOWTDAL` contracts with an NWS hourly point forecast at DFW Airport. A normal error distribution converts the forecast daily extreme into bracket probabilities, and the scanner evaluates both YES and NO asks after a conservative Kalshi taker-fee estimate. The error sigma is configurable but initially uncalibrated. Because the contracts currently settle from The Weather Company rather than directly from the NWS forecast, every result remains ineligible until completed-event data measures both forecast error and source divergence. Hourly observations are stored in `opportunity_lab.weather_value_observations`.

Each collector run also requests recently settled Dallas markets and reconciles them with saved snapshots. `opportunity_lab.weather_calibrations` stores bracket Brier score, forecast horizon, a conservative lower bound on temperature error, and the realized fee-adjusted P&L of the snapshot's best modeled contract. The mechanical gate requires at least 30 distinct settled events, positive aggregate paper P&L, and an average Brier score below 0.15 before recommending execution-feasibility research; it still cannot enable trading.

The dashboard also provides a manual historical Dallas backtest. It pairs Open-Meteo's archived GFS `previous_day1` temperatures with settled Kalshi brackets and uses the latest completed hourly bid/ask candle no later than midnight Dallas time on the target date. This is explicitly lookahead-safe, but remains a proxy: GFS differs from both the NWS forecaster grid and The Weather Company settlement source, and candle closes are not guaranteed executable fills. Historical results therefore accelerate rejection and model tuning; they do not satisfy the forward calibration gate by themselves.

The 60-day walk-forward control searches a fixed grid of forecast-error sigmas and minimum post-fee edges using only the oldest two-thirds of dates. It selects one configuration mechanically, then reports profitability on the untouched final third. At least five validation trades and positive validation P&L are required merely to retain the model for forward validation; the control never enables execution.

Weather Model v2 uses Kalshi's reported `expiration_value` as the realized Dallas temperature. For each event it estimates high/low bias and forecast-error dispersion exclusively from earlier settled events, then scores that event using the corrected forecast. A fixed grid of rolling windows, minimum edges, and YES/NO-side restrictions is selected on the oldest two-thirds; only the selected configuration is judged on the final third. The first 60-day run selected a seven-event window, 3% minimum post-fee edge, and both sides, producing +$0.84 over 64 calibration trades and +$0.51 over 39 untouched validation trades at one contract per trade. This small result retains the model for forward testing but does not enable execution.
