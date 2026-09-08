# MLB and NFL paper experiments, September 8–14, 2026

This is a local simulation using public ESPN game summaries and Kalshi game-winner order books. It cannot submit orders. The frozen slate has 15 MLB games on September 8 and 16 NFL games on September 9–14. Dates and times below use America/Chicago. The old SMU–Florida State experiment remains separate.

| Slate | First game | Coverage |
|---|---|---|
| MLB, Tuesday September 8 | Cleveland at Baltimore, 5:35 p.m. | All 15 mapped games tonight |
| NFL, Wednesday September 9 | New England at Seattle, 7:20 p.m. | Opening game |
| NFL, Thursday September 10 | San Francisco at Los Angeles Rams, 7:35 p.m. | Thursday game |
| NFL, Sunday September 13 | Noon games | 13 Sunday games |
| NFL, Monday September 14 | Denver at Kansas City, 7:15 p.m. | Monday game |

The exact ESPN IDs, Kalshi team tickers, dates, rules and mapping evidence are in `configs/sports_week_20260908/`. Matching requires both team tickers and the Eastern date; MLB additionally requires the scheduled start time to distinguish doubleheaders. This slate is fixed, not a daily discovery service. Postponements and schedule changes need review, not automatic substitution of markets.

## Protocol fixed before the games

Each game has an independent $1,000 simulated account. This is not a shared portfolio or a recommendation to fund 31 accounts. Each account allows one position at a time, up to 10 contracts and $10 total entry cost, at most five entries, and a $20 loss halt. Stops request simulated exits and cannot guarantee a loss ceiling if prices gap or liquidity disappears.

- Entry requires a fresh probability linked to the current play and matching scores and team IDs. Timestamps are never repaired from other fields. Nonzero NFL tie probability is currently unsupported and blocks new entries.
- The estimated probability must exceed acquisition costs plus a modeled exit-cost allowance by at least eight percentage points. This ESPN-versus-market discrepancy is a hypothesis, not established mispricing.
- The book spread must be at most three cents, with sufficient displayed size on both sides. The immediate modeled liquidation loss must be at most 12% of entry cost, leaving room before the 25% position stop.
- A second observation at least 10 seconds later must reconfirm the entry. Normal live polling is 20 seconds. Entries use the worse of the two asks; exits use the worse of the two bids. No partial fills or resting maker fills are assumed.
- The fee model uses `0.07 × contracts × price × (1-price)`, rounded upward to whole cents, plus one cent slippage per contract per transaction. This is deliberately conservative: the September 8 preflight reported MLB multiplier 0.5 and NFL multiplier 1, and the current published schedule allows finer rounding. Observed fee metadata is saved in every sample. Unknown fee types or rates above the allowance block new entries. These modeled costs must not be described as exact broker charges.
- Positions can be held while estimated value remains. There is no arbitrary 30-minute holding limit. Net exit value within two cents of estimated payout triggers a staged exit. Risk limits and game completion also trigger exit attempts.
- A brief bad game feed gets five minutes of grace before a feed-driven exit. Healthy scheduled breaks get up to 30 minutes; a transport failure during a break gets only the five-minute grace. Price risk stops remain active during breaks and interruptions.
- Final payout is credited only from a finalized Kalshi yes/no result. Ties, cancellations or unresolved markets are retained for review if not otherwise closed; a deadline never invents a settlement.

The quote conversion follows [Kalshi's order book documentation](https://docs.kalshi.com/getting_started/orderbook_responses). Fee assumptions were checked against the [published fee schedule](https://kalshi.com/docs/kalshi-fee-schedule.pdf) and public series metadata. Quotes may disappear between polls, so simulated fills remain an approximation.

## Running and reviewing

Start or safely resume the suite from the repository:

```powershell
.\tools\start_sports_paper_suite.ps1
```

The supervisor starts each worker two hours before its scheduled start, polls its lifecycle every 30 seconds, and avoids duplicate collectors using OS locks. Workers sample once a minute before kickoff and every 20 seconds afterward. They stop when the game finishes with no position, or 12 hours after scheduled kickoff. State and pending actions persist transactionally in SQLite across restarts. The computer must remain awake with internet access. The Codex follow-up monitor additionally requires the app to remain available.

Main status: `sports_paper/week_20260908/status.json`. Each game folder contains `paper.sqlite3`, `status.json`, `report.json`, `worker.log` and process information. Preflight evidence is in `preflight/`. Logs and ledgers are ignored by Git. The supervisor process ID is in `suite.pid`.

To pause simulated decisions across all games while continuing collection:

```powershell
New-Item -ItemType File -Path sports_paper/week_20260908/PAUSE
```

A `PAUSE` file in a single game folder pauses that game. Pending intents are canceled; finalized settlement may still be recorded. Pausing also prevents launching new workers. Remove only that specific pause file to resume. Do not delete ledgers or change configs mid-experiment.

## What the tests measure

SQLite `samples` records observations, blockers, decisions, accounts, feed timings, bid/ask depth and fee metadata. `signals` records one fresh signal per play, including opportunities skipped by entry gates. `markouts` records cost-adjusted hypothetical exits 1, 5 and 15 minutes later when valid depth is available, with at most 45 seconds timing tolerance. Missing observations remain missing; future prices never inform current decisions.

Review by game and sport: signal coverage, timestamp/play mismatches, spread and entry-cost exclusions, fills, exit reasons, realized P&L after modeled costs, drawdown and unresolved positions. Compare trade results with the later price behavior of skipped signals. Summarize games with no entries as recorder-only tests, not successful trading tests. A current final score alone cannot calibrate dozens of correlated observations as independent outcomes.

Freeze this protocol across the slate. Investigate results after the games, then evaluate any revised version on additional untouched games. Report confidence and sample limitations; one evening or one NFL week cannot establish profitability.
