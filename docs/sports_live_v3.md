# Sports paper v3: coverage and execution

This separately deployed operational candidate implements the six authorized improvements. It does not establish profitability. Original research 1.0/1.1 and laboratory 1.1/1.2 remain independent comparisons with their original ledgers and rules. No historical fills or anchors are imported.

## Six changes

1. Authenticated Kalshi WebSocket snapshots/deltas, signed locally using RSA-PSS; sequence gaps and disconnects invalidate stream books, periodic snapshots refresh quiet markets, and reconnects establish fresh sessions. Credentials are absent at initial launch, so streaming is implemented and fixture-tested but NOT live-validated. Explicit REST fallback continues paper diagnostics. Four book GETs/second maximum, no orders or account endpoints.
2. Core full-game winner outcomes plus one most-traded listed spread and total per game, where mapped. Volume ranks a provisional core line, not guaranteed liquidity. Missing families are listed. Held/pending tickers retain dedicated priority; admission requires fresh quotes and recent service intervals. Broad alternate/player catalog remains in separate labs. At most 40 distinct serviced pending/held tickers, with at most 12 carrying independent probes. These are service budgets, not additional account risk allowances.
3. Independently refreshed game summaries/scoreboards, a 15-second transport-age gate, explicit last-state-change/play-age diagnostics, and an additional probability-period consistency check. Existing identity, play/score matching, 90-second probability age and sport-specific gates remain. Fresh transport with an old play is labeled separately; timestamps are not repaired and old probabilities are not admitted. No paid feed purchased.
4. p95 measured public book GET round-trip time plus processing delay, with a 250ms floor, replaces the fixed 10-second wait. This is a conservative proxy, NOT measured exchange order-routing latency. New entries require five RTT measurements and latency no greater than five seconds. An order needs a distinct, fresh, post-arrival book observation. Its observed delay is recorded; no fill is backdated. Conservative top-level displayed quantity can partially fill; unfilled entry quantity is canceled. No deeper-level or passive queue fills are assumed.
5. Service-capacity admission, bid-side liquidation checks, held-market priority, partial exits with preserved residual cost basis, measured exit delays for probes, and verified-market settlement for strategy positions. Existing $10/10-contract entry caps, $20 account-loss halt, five-entry cap, one position and five-minute cooldown remain. NFL keeps its stricter three-cent spread and 12% immediate-loss constraints. Missing liquidity remains unresolved.
6. Prospective signal observations and one-contract ask-to-bid cost-adjusted markouts at 5/15/30/60 seconds. Each horizon requires a later book at or after its target and within five seconds. Missing horizons remain missing; no historical/future interpolation. Game-level reports distinguish strategy P&L, independent quote probes and timing diagnostics. Reports run from online snapshots in a separate thread, outside the decision loop.

## Run and inspect

Manifest: configs/sports_live_v3_20260912/manifest.json. Fresh output and ledger: sports_paper/live_v3_20260912/paper.sqlite3. Launcher: tools/start_sports_live_v3.ps1. Recovery task: Codex Sports Live V3 Recovery 20260912. Bounded through September 18, 2026 at 12:00 UTC. Root/per-game PAUSE files suppress paper decisions. Status exposes rest_fallback versus websocket and the authentication/connection state. Per-game reports: games/<slug>/report.md and report.json; combined report.json updates approximately every minute. Read report_error.json if reporting fails.

57 tests passed across v3 and existing candidate/lab regressions, including RSA signing, sequence gaps, negative/crossed books, game/family allocation, old-play versus transport classification, preserved NFL limits, post-arrival evidence, partial-fill cash conservation, missing horizons, and an offline runtime/report integration test. A current public-data preflight matched California-Syracuse identities and both books, and correctly rejected a 176-second-old probability-linked play despite fresh transport. This was a parser check, not a backtest or a fill.

Registration captured 96 remaining/ongoing mapped games: 62 college football, 4 EPL, 15 MLS, 1 ATP and 14 NFL. Existing mapping-paused games and completed MLB were excluded. Already-started soccer/tennis cannot obtain retrospective anchors; they can still produce independent quote observations when admissible. Report actual coverage, not just the configured slate.

## Streaming setup

Kalshi production credentials are needed for real market-data streaming while execution remains simulated. Configure KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH as user environment variables; the launcher loads those values. Store the private key outside this Git repository. Never paste or log its contents. Restart v3 via its bounded launcher after a controlled stop to load newly configured variables, preserving the ledger. Do not create duplicate workers. Authenticate only to the documented Kalshi WebSocket host; no real order routes exist in v3.

Reference: https://docs.kalshi.com/getting_started/quick_start_websockets ; https://docs.kalshi.com/websockets/orderbook-updates ; https://docs.kalshi.com/getting_started/rate_limits .

## Review and email

Review in-game coverage, staged/canceled/partial/filled orders, markout missingness and unresolved exits every follow-up. Capture mode transitions with timestamps and compare REST and streaming results separately. Compare future shared games with unchanged versions; do not pool holding variants or labs, or tune and score on the same games. Prior-Central-day email must query v3 actions joined to sample timestamps, including partial_sell profit, and account snapshots at the cutoff. Probe entry_at/exit_at are UTC epoch seconds in this version; convert them explicitly for daily windows. Markouts are diagnostics, never realized trading profit. No automatic promotion or profit guarantee.


## Current deployment: 3.1

The initial 3.0 live run exposed a null period field in UL Monroe-UAB. It was paused and verified to have zero entries, positions, pending orders or active probes before stopping. Its ledger and source files remain preserved; recovery task Codex Sports Live V3 Recovery 20260912 is disabled.

Current module opportunity_lab.sports_live_v31 uses nullable-field-safe sports_live_sync_v31 and sports_live_report_v31. Status/ledger/reports now live under sports_paper/live_v31_20260912; manifest configs/sports_live_v31_20260912/manifest.json. Launcher tools/start_sports_live_v31.ps1; recovery task Codex Sports Live V31 Recovery 20260912. All other six-change mechanics remain as documented above. Sixty tests passed across the corrected candidate and existing research/laboratory tests. Null period and possession remain unknown and never remove model blockers. Same bounded deadline, separate fresh ledger and unchanged risk caps. Streaming credentials still required for authenticated activation.


Initial 3.1 operation produced per-game reports, live observations and markouts. Subsequent public REST HTTP 429 responses triggered the implemented 30-second backoff. This confirms that REST fallback cannot promise full-slate streaming coverage. The status rate_limit key retains the last incident flag; use recent response metadata to establish whether rate limiting persists. Streaming activation remains pending credentials.


## Current deployment: 3.2, authenticated streaming

User provided credentials and authentication succeeded. Kalshi omits empty yes/no arrays in some snapshots; sports_live_books_v32 treats these as empty books, so unavailable liquidity blocks fills without disconnecting other markets. Full subscription preflight parsed 20,117 messages across 117 markets, including snapshot refresh. Sixty-three tests passed.

Current worker opportunity_lab.sports_live_v32 uses bounded 256-message batches, preserving normalized stream messages and resulting full book views while coalescing per-market decision updates. Output sports_paper/live_v32_20260912; manifest configs/sports_live_v32_20260912/manifest.json; launcher tools/start_sports_live_v32.ps1; recovery Codex Sports Live V32 Recovery 20260912. Same September 18 deadline and execution rules.

At 2026-09-12T20:42:14.594137+00:00, stopped v3.1 and copied its ledger to v3.2. All account and probe rows verified identical. Original ledger/source preserved and old recovery disabled. For reviews and email, v3.2 is the authoritative continuation, including inherited history; do not add v3.1 totals again. Alternatively split actions before/after the migration cutoff. Market-data credential activation does not authorize real orders.


## Live 3.3: load handling and automatic reviews, September 12

Launched at 21:06:19.969812 UTC as an exact state-preserving continuation of 3.2. See configs/sports_live_v33_20260912/manifest.json and sports_paper/live_v33_20260912/migration.json. Both older 3.1 and 3.2 histories are inherited: use only the authoritative continuation for totals. Original sources and ledgers remain preserved; old recovery is disabled.

Every incoming stream delta is applied to the reconstructed book. Complete observed checkpoints are published at 250 ms cadence through a bounded latest-per-ticker mailbox. Intermediate events are deliberately coalesced; this is not a lossless raw stream archive. Sequence gaps invalidate the session. Quiet books receive refresh requests; healthy sessions no longer reconnect every minute. Target changes are batched.

All Kalshi GETs share a one-per-second ceiling and exponential 30/60/120 second rate-limit backoff. Healthy streamed books avoid repeated REST polling except RTT sampling. REST cannot overwrite or invalidate a fresh streamed book. Independent game feeds continue during Kalshi backoff; settlement gets reserved service. Metadata processing excludes unrelated games. Status load counters distinguish accumulated incidents, present backoff, fresh selected books and mailbox pressure. Fresh reconstruction can still have no executable two-sided quote.

67 tests passed, including 100,000 sequential deltas with a stalled consumer, exact resulting size, bounded handoff memory, reset invalidation, rate backoff, and deduplicated review thresholds. Early live observations showed 165/165 fresh reconstructed books, no 429 incidents or disconnects, mailbox peak165/1024 and maximum handoff age1.04 seconds. This is initial verification, not a full-game load guarantee.

The existing 30-minute heartbeat now runs tools/review_sports_live.py. It reviews zero strategy entries or zero probe entries after15 minutes of a running game, at the first scheduled check thereafter; each running-game checkpoint remains reviewable again30 minutes later. It reviews each sport after3 completed games. At5 traded games and20 unique fully closed entry episodes, losing after-cost performance in either holding variant triggers strategy diagnosis. Partial exits do not count as completed episodes; holding variants and probes are not pooled.

The authorized response is to investigate coverage, stale models, costs, adverse price movement, exits and calibration, then build/test one separate preregistered paper challenger per sport when supported. Evaluate on untouched future games and preserve the control. These thresholds trigger investigation, not proof of edge. No automatic risk-cap increases or lower freshness standards to force trades. If no defensible change exists, record the evidence and next review threshold. Resolved alerts require an evidence/decision journal entry. Morning email reporting retains the user's existing Gmail authorization and includes build changes. Monitoring and recovery remain bounded through September18 at12:00 UTC.


September12 22:51UTC: Review reads exposed rollback-journal commit timeouts. After stopping, live3.3 SQLite ledger switched to WAL with account rows verified identical. Recovered worker committed385 samples during a7second held read transaction. All access is local. Preserve WAL/SHM alongside the database while running; never copy the live main database file alone. Reviewer uses indexed short reads, and live diagnostics are not a final accounting-cutoff snapshot.


## Current3.4, September13 02:57UTC migration

Soccer scoreboard queries now span previous/current/nextUTCdates. Single-dayUTC queries caused MLS games to disappear aftermidnight; actualpublicmulti-dayqueryverifiedrestoredeventIDs.8MLSgamescompletedafterfix. Trade/historygap remains documented; neverbackfillfills. Module sports_live_v34, outputlive_v34_20260913, recoveryV34. Fullstate/historycopied from3.3; no doublecount. Embedded reportnowusescurrent34directWALreads.32tests passed. Churn1.1copiedexactaccounts/cursors from1.0 andnowreadslive3.4; originalregisteredcohort andone-entryexperiment unchanged. BotholdworkersPAUSED/recoverydisabled, originals preserved. Bothnewrecoverytasksactive.


Manual report correction September13 05:20UTC: concurrent manual and embedded writes collided on report.json.tmp. Use sports_live_report_review34.report with current live output; it writes only manual_reports/games and manual_reports/report.json. Embedded sources and trading ledger unchanged. Do not launch duplicate manual reporters.
