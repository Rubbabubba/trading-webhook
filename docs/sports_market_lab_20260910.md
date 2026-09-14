# Expanded market research, September 10–17

User authorized testing as much as possible within each game. Version sports_market_lab_1.0 is a separate public-data recorder and fixed-schedule quote-probe laboratory. It does not replace the frozen game-winner bot, place orders, change account limits, or assert a model-derived edge.

It discovers 95 catalog-confirmed series across MLB, NFL, college football, EPL, MLS and ATP/WTA. These include winner, spread, total, team total, selected halves/quarters/innings, player statistics, soccer both-teams-to-score/correct-score and tennis sets/games where available. A series existing does not imply a contract for every game. Only contracts whose league and entire event-code segment match the existing bounded slate are included. Different encodings and unmatched instruments are excluded. Capture preserves contract rules and hashes; changes to rules or event identity block probes. Tennis remains limited to the already matched US Open slate.

The module records public depth-20 books and discovery/fee response bytes in sports_paper/market_lab_20260910/laboratory.sqlite3. Markets are discovered every five minutes after the preceding discovery completes. Up to 200 eligible instruments rotate through each sampling cycle with three concurrent requests; actual cycle durations and sampling counts are reported. This is not tick-by-tick or exhaustive continuous coverage. Additional markets discovered after a game starts have only prospective observations. Game coverage begins two hours before the configured start and ends at the existing deadline.

## What is simulated

At fixed 15-minute time buckets after scheduled start, eligible YES and NO contracts each receive independent one-contract quote probes with five- and fifteen-minute holding windows. There is no probability forecast or entry signal: this deliberately samples execution costs and subsequent price changes before choosing strategies. Scheduled-time probes do not themselves establish that play is active. They are not added to baseline trade counts or presented as a funded portfolio. Opposing sides, horizons, adjacent thresholds and players in one game are correlated.

Both entry and exit need a different captured book at least ten seconds later, within three minutes of staging. Entry uses the worse ask; exit uses the worse bid. Required displayed size is one contract; spread is at most four cents and immediate modeled liquidation loss at most 15%. Fees use observed supported series metadata, conservatively rounded to cents; slippage is one cent per contract per transaction. Failed entry confirmation is recorded without a fill. Missing exit liquidity leaves an unresolved probe and the eventual delay must be reported. Probes never receive invented scoreboard settlements. No queue priority or actual execution is claimed.

## Review and comparison

Read status.json, markets.json, report.json/report.md and per-instrument probes in the SQLite ledger. Report by game, sport, market family and holding horizon. Measure coverage, rejected/expired entries, unresolved exits, actual holding duration, net changes after modeled costs and concentration by game. Keep costs and adverse results. These probes can reject impractical market families and generate hypotheses; favorable retrospectively selected subgroups are not proven strategies. A signal-based candidate needs a predeclared hypothesis and future unstarted evaluation games, with existing risk caps, before being compared with the frozen baseline. Player-specific predictive models remain future work requiring verified player statistics.

## Operation and daily email

Run tools/start_sports_market_lab.ps1. Windows task Codex Sports Market Lab Recovery 20260910 runs that launcher every five minutes and at logon, bounded through September 18 at 12:00 UTC. Respect market_lab_20260910/PAUSE and <league>_<event_id>.PAUSE. The worker has an exclusive process lock and source fingerprints in configs/sports_market_lab_20260910/manifest.json. Preserve this source and ledger; fixes need another version.

Include laboratory coverage and probe results in the morning Gmail digest in a clearly separate section. Never pool probe P&L with signal-driven paper accounts. Filter entry_at/exit_at to the previous Central calendar day and retain unresolved-at-cutoff cases; current aggregate report.json is not a daily accounting substitute. No immediate setup email is authorized.

## Active correction: sports_market_lab_1.1

The initial 1.0 discovery found 19,680 contracts. At this scale, pure round-robin sampling could revisit staged entries after the three-minute confirmation deadline. Version 1.0 is deliberately paused with its evidence preserved. Active version 1.1 prioritizes instruments with staged/open/exit-staged probes before adding new samples, retaining the 200-book cycle bound and every cost/freshness guard. No strategy parameter changed. Four tests passed, including a 1,000-instrument priority regression.

Active module: opportunity_lab/sports_market_lab_v11.py. Manifest: configs/sports_market_lab_v11_20260910/manifest.json. Output/ledger: sports_paper/market_lab_v11_20260910/laboratory.sqlite3. Launcher: tools/start_sports_market_lab_v11.ps1. The same Codex Sports Market Lab Recovery 20260910 task now targets that launcher. Include both versions and their actual deployment/paused status in daily email; 1.0 records do not transfer into 1.1. Sampling is still bounded, so neither complete coverage nor exact exit timing is guaranteed.
