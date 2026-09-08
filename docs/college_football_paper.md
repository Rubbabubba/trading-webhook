# SMU at Florida State — September 7, 2026 paper test

Kickoff: **6:30 p.m. America/Chicago / 7:30 p.m. Eastern**.
[Florida State's announcement](https://seminoles.com/news/2026/5/12/football-game-times-announced-for-smu-florida-games).

This is a single-game experiment, not a trained or validated profitable strategy.
It uses ESPN's published live win probability as a proxy and Kalshi's executable
bid/ask snapshots. It never submits an order, reads account credentials, or uses
real funds. No data subscription or additional Python package is needed.

## Start and inspect

From the repository root on this Windows computer:

```powershell
./tools/start_college_football_paper.ps1
.venv/Scripts/python.exe -m opportunity_lab.college_football_paper --config configs/ncaaf_smu_fsu_20260907.json --status
```

The launcher starts a hidden background Python process. It checks for an existing
collector, and the collector locks its output directory to prevent duplicate
workers. **Keep this computer powered on, awake, and online.** Closing the laptop,
sleeping, restarting, or losing network connectivity can interrupt data collection.
This is a local process, not a hosted service. Run the launcher again after a
restart; the ledger resumes without resetting cash or positions.

Outputs are in `ncaaf_paper/smu_fsu_20260907/` (ignored by Git):

- `status.json`: latest game state, action, account, and transport errors.
- `report.json`: sample count, paper P&L, position, drawdown, and last actions.
- `paper.sqlite3`: every observed game state, quotes, fees, decisions, and account.
- `collector.stdout.log` / `collector.stderr.log`: runtime diagnostics.
- `collector.pid`: process ID for stopping the specific collector if needed.

To pause all new paper buy/sell intents, create
`ncaaf_paper/smu_fsu_20260907/PAUSE`. Finalized settlement processing continues.
Remove it to resume. A pause does not liquidate an existing paper position.

The worker polls every 60 seconds before scheduled kickoff and every 15 seconds
thereafter. It finishes when ESPN reports completion and no paper position remains,
or at **3:00 a.m. Central on September 8**. An unsettled position at that deadline
stays open in the report; it is never silently paid out or discarded. Delays beyond
that deadline need an explicit follow-up experiment.

## Locked experiment settings

The tracked configuration is `configs/ncaaf_smu_fsu_20260907.json`. It maps ESPN
event `401858212`, home team `52` (FSU), and away team `2567` (SMU) to verified
Kalshi markets `KXNCAAFGAME-26SEP07SMUFSU-FSU` and
`KXNCAAFGAME-26SEP07SMUFSU-SMU`. The database rejects a changed configuration;
changing parameters requires a separate output directory so tonight's results
cannot silently mix strategies.

Defaults: $1,000 paper cash; at most $10 and ten contracts per entry; one position
at a time; at most twenty entries; a $20 account loss threshold; a 25% position
loss trigger; 30-minute maximum holding time; and two-minute reentry cooldown.
Loss thresholds request an exit; they cannot guarantee an exit price or fill.
Missing exit liquidity keeps the position open and is recorded.

## Entry and exit

Buy YES on either team only if its ESPN probability exceeds the available ask by
at least eight percentage points **after** modeled entry fees, entry slippage,
an estimated exit fee, and exit slippage. The fee estimate uses the quadratic taker
formula with coefficient 0.07; current series metadata must indicate a supported
quadratic fee type with multiplier one. Slippage is one cent per contract on each
side. The displayed spread must be at most five cents.

Signals must match the configured game/team identities, latest play ID, score,
and a play wall-clock timestamp no more than 90 seconds old. The game must be
actively in progress. No entry uses a pregame projection or an invented probability.
Missing, delayed, or mismatched live probabilities produce a recorded no-trade.
ESPN's public endpoint is not a guaranteed low-latency or stable trading feed.

Every entry/exit is staged and checked again in a later poll (at least ten seconds
later, expiring after 45 seconds). A buy requires the edge to remain, sufficient
depth, and no more than two cents adverse ask movement. The simulated fill uses
the worse of the two observations, plus fees/slippage; better second quotes do
not improve the assumed fill. A sell also needs enough displayed bid depth, and
uses the worse observed bid. These are hypothetical fills, not exchange fills.

Exit when the net available bid is within two cents of modeled fair value, the
position/account loss trigger fires, the maximum hold time expires, the game
finishes, or the live signal becomes unusable. A pending fair-value exit is checked
against the next probability update. Risk exits can proceed on fresh Kalshi data
when ESPN is unavailable. Stop conditions can still lose more than their thresholds.

Realized paper profit appears only after a simulated sell or Kalshi's finalized
YES/NO settlement. ESPN's final score never directly pays out a position. Scalar
or cancellation settlements need manual review. Marked equity deducts estimated
liquidation costs and is unavailable if sufficient exit liquidity is missing.

## How to interpret tonight

Check data coverage, feed errors, timestamp ages, entry/exit reasons, costs, and
drawdown before interpreting profit. One game can validate plumbing and expose
failure modes; it cannot demonstrate a repeatable edge. Zero entries is a valid
outcome. A missing live probability stream means a recorder-only run, and should
be reported as such rather than changing the strategy mid-game.
