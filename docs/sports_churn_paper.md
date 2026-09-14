# NCAAF repeated-entry paper comparison

Version ncaaf_churn_1.0 is a separate prospective experiment. Registered 2026-09-13T02:18:36.657845+00:00. It reads the live3.3 WAL ledger without new network calls or real orders. Baselines, positions and histories are unchanged.

The one main change is a maximum of one filled entry per game instead of five. A matched five-entry control runs on the exact same newly observed inputs and reader delay. Both arms retain five- and fifteen-minute horizons, original fees/slippage, displayed-depth fill rules, freshness, loss exits, $10 entry limit and other execution safeguards. No holding or feed-failure exit rule was relaxed.

Rationale: the diagnostic NCAAF sample recorded36 repeated five-minute entry episodes losing$17.15 including$17.05 modeled costs;32 repeated fifteen-minute episodes lost$18.89 including$15.52 costs. First entries also lost, so fewer entries is a risk/cost hypothesis, not proof of profitability or improved return per dollar. Partial proceeds were accumulated within an entry episode. The sample is correlated and mixes transport regimes; no parameter sweep was used.

Cohort: Sacramento State-Fresno State, Montana State-Nevada, Louisiana-USC, and September17Syracuse-Pittsburgh. Each was unstarted at registration. No historical observations can fill. All arms start with fresh hypothetical accounts. One-entry admission affects future entries only; existing exits remain identical.

Run: tools/start_sports_churn.ps1; recovery Codex Sports Churn Paper Recovery20260913; deadline September18 12UTC. Manifest configs/sports_churn_20260913/manifest.json; output sports_paper/churn_20260913.31 regression tests passed, including arm equivalence before first entry, reentry suppression, unchanged open-position exits, and stale-source rejection. Startup verified16fresh accounts, zeroentries.

Ledger accounts(game,arm,horizon,state); actions(game,arm,horizon,at,source_sample_id,detail,account); observations(game,at,source_sample_id,age). At timestamps use UTC ISO8601. For morning mail filter actions to prior Centralday, sum profit_cents including partial_sell/settle, and use latest account atcutoff for open positions. Never pool arms/horizons. Status is cumulative, not a daily accounting report.

Evaluate matched per-game net, costs, drawdown, missed later profitable entries, exposure and unresolved exits. Lower trade count alone is not success. No live-money promotion or profitable-edge claim.


Operationalcontinuation1.1 at02:57UTC: sourceledger nowlive_v34_20260913 aftersoccerdatefix. Same module with --manifest configs/sports_churn_v11_20260913/manifest.json; outputchurn_v11_20260913; launcher start_sports_churn_v11.ps1; recoveryCodex Sports Churn V11 Paper Recovery20260913. Copiedaccounts/cursorsexact, samecohort. Do notpool1.0and1.1inheritedhistory.
