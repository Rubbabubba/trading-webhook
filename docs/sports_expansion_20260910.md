# Sports capture expansion, September 10–17, 2026

Authorized alongside the existing frozen MLB/NFL experiments. EPL means England's Premier League. MLS is the US/Canadian club league; this does not include US national-team games.

## Scope

The separately fingerprinted configs/sports_expansion_20260910/manifest.json enables public capture of Kalshi college-football game winners, EPL, MLS, and ATP/WTA US Open match winners. New rounds are discovered every five minutes. Initial discovery found 121 college-football events, 10 EPL matches, 15 MLS matches, and four US Open semifinals. These are market listings, not guaranteed matched game feeds or liquid opportunities.

Raw ESPN scoreboards, football/soccer summaries, Kalshi market/rule/fee snapshots, and 20-level order books are saved losslessly with SHA-256 and transport timestamps. All soccer outcomes, including draws, are retained. No binary complement shortcut is used. Tennis selection requires US Open in the market rules. The manifest bounds event dates to September 10–17 and the worker stops September 18 at 12:00 UTC.

Capture is nominally once per minute, plus any cycle overrun. Books are collected over a broad event-date window from 00:00 Eastern to noon UTC the following day, not an inferred exact kickoff. Football/soccer summaries are collected from two hours before scheduled start through twelve hours after. Four parallel requests and a half-second batch pause limit load. This is sampled REST data, unsuitable for claiming subsecond execution. Actual request times and cycle durations remain available for gap analysis.

## Validation still required

Market events and ESPN event IDs remain separate until identities, start times, outcomes and settlement rules are verified. College-football market coverage may exceed the ESPN scoreboard's coverage. Missing feeds must be reported individually. Tennis raw scoreboards contain nested tournament/match records; point/server completeness and freshness must be established before any probability model is used. Soccer probabilities must include home, draw and away, and respect 90 minutes plus stoppage time. Capturing quotes does not validate a model, establish fills, or prove profitability.

This worker contains no trading decisions, credentials or order submission. The MLB/NFL baseline and v3 source files are unchanged. No automatic promotion into paper trading is permitted.

## Operation and reports

Launch/resume with tools/start_sports_expansion.ps1. Output: sports_paper/expansion_20260910/capture.sqlite3, markets.json, status.json and capture.stderr.log. Root PAUSE stops collection/recovery. An event-ticker.PAUSE pauses that event's books; league_ESPNID.PAUSE pauses its summary. League scoreboards and discovery continue unless root-paused.

Windows recovery task Codex Sports Expansion Recovery 20260910 launches this worker at logon and every five minutes through the bounded experiment. The machine must remain awake and signed in. The existing Codex follow-up includes this expansion through September 18. Produce a separate capture-validation report per market event under sports_paper/expansion_20260910/reviews, with linked ESPN evidence only after verified matching. Include sampled coverage, missing intervals, fees/rules, observed book depth/spreads, transport delays, and unresolved validation. No trading P&L is available for these captures. Save delivery records to reviews/notifications.json.

## Novig

tools/assess_novig_public.py saves content-addressed copies of seven published trade days and the latest market census under sports_paper/novig_public_20260910. assessment.md and assessment.json count straight-contract TAKER rows once, avoiding maker-side double counting. Contract quantity is dollar notional; cost is taker stake excluding fees. Combo trades are excluded from this straight-market assessment. This is historical activity, not historical book depth or fill simulation. Anonymous IDs do not establish named-game mapping against Kalshi.

Live Novig data requires Novig-issued API access. No credentials or account connection have been configured. The published live taker fees must be included in any later paper model.

Sources: https://docs.novig.com/api-reference/trade-data ; https://docs.novig.com/api-reference/authentication ; https://docs.novig.com/fees ; https://docs.kalshi.com/api-reference/market/get-markets .


September13 storage correction: capture.sqlite3 uses WAL after a rollback-journal commit lock crash. Protocol/source fingerprints preserved. Existing recovery resumed and223new responses verified. See journal_mode_change_20260913.json. Preserve WAL/SHM when handling a live database; no strategy change.
