# Kalshi paper bot

Run from the repository root with Python 3.10 or newer. This standalone bot needs
only the Python standard library and public Kalshi data. It never submits orders.

```powershell
.venv/Scripts/python.exe -m opportunity_lab.kalshi_bot
.venv/Scripts/python.exe -m opportunity_lab.kalshi_bot --watch --interval 60
.venv/Scripts/python.exe -m opportunity_lab.kalshi_bot --status
```

The first command runs one scan; the second repeats until Ctrl+C. No background
service is installed. `--status` reads the local ledger without network requests.
The default database is `kalshi_paper.sqlite3` in the current working directory.
Use an absolute `--db` path when launching from different directories.

## Strategy and limits

The bot searches all categories returned by a bounded event scan (600 events by
default, up to 2,000 with `--pages 10`). This does not cover the entire exchange;
repeated scans use the same first pages. It looks for:

- YES and NO on the same binary contract costing less than the combined payout.
- NO on two outcomes within an event explicitly marked mutually exclusive.
  At least one NO wins under ordinary binary settlement; both may win.

It rechecks up to ten candidates against current market status and opposing bids
in the order books. Entry requires at least five minutes before close, whole
contracts available at the best level on both legs, rechecks completed within ten
seconds, and at least five cents estimated total profit after modeled costs.

Defaults: $1,000 paper cash, $10 per trade, $25 per event, $100 total open cost,
and ten contracts per leg. These are test settings, not a proposed live bankroll.
Cash and exposure checks are transactional. Open trades cannot reuse a market;
the same pair cannot be entered again in the same database. Restarting preserves
cash, exposure, and settlement. `--initial-cents` applies only to a new database.

Fees use the general quadratic taker estimate, rounded up per leg, plus one cent
per contract per leg for slippage. Actual series-specific fees are **not verified**;
`--fee-coefficient` and `--slippage` support cost sensitivity experiments. See
[Kalshi's fee information](https://help.kalshi.com/en/articles/13823805-fees).
Displayed bid conversion follows the
[order-book API](https://docs.kalshi.com/api-reference/market/get-market-orderbook).

## Accounting and stopping

Trade costs, including estimated fees and slippage, immediately reduce paper cash.
Only finalized YES/NO results release settlement proceeds and create realized
paper P&L. Open positions remain at cost, with no unrealized profit claim. The bot
holds to settlement; it has no early-exit or mark-to-market model. Voided, scalar,
missing, or inaccessible settlements remain open for manual review. Discovery
errors prevent new entries, and settlement errors appear in the run report.

Create the `KALSHI_STOP` file in the working directory to pause new paper entries:

```powershell
New-Item KALSHI_STOP -ItemType File
```

Settlement checks continue while paused. Remove the file to resume, or press
Ctrl+C to stop the process. `--kill-file` accepts an alternate path.

The SQLite `runs` table stores scan reports; `trades` stores simulated entries and
settlements. Each report exposes remaining cash, open cost, and realized paper
profit separately. A clean scan with no qualifying opportunities is expected.

## What this proves

This is a runnable paper prototype, not a validated profitable live bot. Simulated
simultaneous fills cannot establish real fills, atomic multi-leg execution, or
account-specific profitability. Books can change between requests; a ten-second
request window is not an exchange timestamp freshness guarantee. Cross-market
settlement treatment and applicable fees need verification before live execution.
Complementary crossed quotes will normally disappear through exchange matching.
Weather models, nested-threshold inference, and market-making estimates are not
used to open trades because they require additional model or rule validation.

Live deployment still needs authenticated order handling, partial-fill recovery,
account reconciliation, verified fees and settlement rules, and evidence that an
edge survives actual execution costs. No credentials are needed for this version.
