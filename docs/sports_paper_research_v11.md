# Corrected prospective paper test, version 1.1

Launched September 12, 2026, first sample 2026-09-12T19:12:13.301360+00:00. Paper only. No demonstrated profitability.

Confirmed defects: soccer active play arrives as STATUS_FIRST_HALF or STATUS_SECOND_HALF; version 1.0 accepted only STATUS_IN_PROGRESS. Scheduled soccer summaries can omit scores, incorrectly blocking pregame anchors. Per-game reports scanned all actions inside the decision loop, with observed multi-minute processing gaps.

Candidate accepts the two active soccer statuses while preserving state/completion, halftime, identity, red-card, freshness and price/cost gates. Absent scheduled scores are allowed only with matching identities and pregame states; any supplied score must be zero when its counterpart is missing. Live scores remain required. Reports use an action index and process one queued game after each decision cycle; timestamps refresh per game. Reporting errors are saved separately in report_error.json.

## Frozen scope

Manifest: configs/sports_paper_research_v11_20260912/manifest.json. 81 games with kickoff after the preregistered cutoff: 61 college football, 18 soccer, two tennis. Original experiments, source files and ledgers remain intact. No historical anchors, fills, accounts or parameter training are imported. Five- and fifteen-minute variants remain separate, with existing entry margins, execution costs and risk controls. MLB/NFL remain in their existing tests.

Module: opportunity_lab.sports_paper_research_v11. Output: sports_paper/research_v11_20260912. Launcher: tools/start_sports_paper_research_v11.ps1. Separate Windows recovery task runs at logon/every five minutes, bounded by the September 18 12:00 UTC launcher deadline. Root/per-game PAUSE files remain effective.

## Review

46 regression tests passed, including active halves, excluded phases, live disagreement, absent pregame scores, no retrospective anchor, stale feeds and restart/account preservation. Initial production checks confirmed fresh observations, no worker errors and indexed report lookup. This verifies operation; prospective games must establish coverage and execution results.

Review each game independently; after three completed games per sport assess valid-observation coverage, blockers, staged/canceled orders and unique completed episodes. Investigate no-trade/zero-coverage failures immediately. Include candidate results separately in morning Gmail, using the prior Central calendar day and historical ledger state at cutoff. Do not pool correlated holding variants or laboratory probes. Pause for identity/accounting/safety regressions. No automatic profitability promotion; parameter changes require another frozen prospective test.
