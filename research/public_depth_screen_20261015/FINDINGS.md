# Public Kalshi strategy screen — October 15, 2026

## Decision

No strategy qualifies for a new Demo order trial or live deployment. The research is complete for these fixed screens; the overall engineering backlog and profitable-system objective remain incomplete.

## Work performed

Downloaded the public Oddpool/PredictionMarketBench repository at immutable revision `611d66941717310858683278940df21c33c406f2`. Its four January 2026 episodes contain 378,596 order-book snapshots and separately supplied trade tapes/settlements. File SHA256 values are retained in the replay output. No signup, broker credentials, application model calls, purchases, schedules or orders were used. Standard PyArrow 26.0.0 was installed in an isolated workspace directory to read Parquet; provider simulator code was not executed.

Registered fixed rules before inspecting prices/outcomes, then implemented an independent delayed taker replay. Every simulated entry crosses displayed depth after a one-second delay, expires if the first subsequent snapshot is more than five seconds later, and pays rounded one-contract benchmark taker fees. No passive fill assumptions. An extra 2/5 cents per fill is reported separately. Six targeted synthetic tests pass for fees, invalid books, delayed execution, losses, absent settlements, source result normalization and structural leg stress. They do not prove authentic execution.

| Screen | Observed result | Decision |
|---|---|---|
| Favorites at 90–98 cents | 19 complete modeled entries across three events; net +7 cents at benchmark fees, −31 cents at extra 2 cents per entry, −88 cents at extra 5 cents | Fails cost stress; sample and lifecycle insufficient |
| Prior-20-observation Bollinger mean reversion | Zero entries under contiguous fresh five-minute observations | No performance evidence; do not loosen freshness/history to generate trades |
| Nested threshold conditional arbitrage | 113,597 pair/decision checks:112,636 missing/invalid decision books;961 without required stressed surplus;zero candidates | No observed executable edge; exact rule identity unavailable |

Favorite event totals before extra stress: BTC −23 cents, NYC weather +19 cents, college football +11 cents; NFL no entries. Correlated tickers do not count as independent event evidence. No outcomes or costs were optimized. These small analytical cents are not broker P&L.

## Preserved adapter error

`results-v1.json` is the original run and incorrectly treats uppercase provider settlement results as missing. It reports19 unresolved entries and no completed P&L. The case-only adapter correction plus a regression test produced `results-v1-case-corrected.json` using identical source files and unchanged registration. Original output is preserved, never silently replaced. This is an implementation repair, not a new independent experiment. No parameter search followed the result.

## Official metadata recovery and corrected trading windows

After the exploratory screen, recovered all33 historical contract records using documented public Kalshi GET endpoints, without authentication. All33 benchmark settlement labels match the official returned labels. Immutable responses are retained under `official-metadata/`, dated and hashed in `results-v1-official-cutoffs.json`. The replay excludes rows outside returned open time and earliest close/expected-expiration/occurrence timestamp. This is a data-validity correction, not parameter optimization; earlier results remain intact.

**The cutoff-valid favorite screen has17 complete modeled entries, net−4 cents at benchmark fees,−38 cents at extra2c per entry and−89 cents at extra5c.** Two earlier entries were outside the documented trading window. Removing them eliminates the misleading+7c raw result. Bollinger still has zero entries. Current official metadata does not prove exact historical metadata vintage or when sports outcomes first became knowable. Official rules are now available; historical lifecycle and outcome-known validation remain incomplete. This correction supersedes the earlier raw exploratory totals above.

## Remaining validity limits

The four publicly selected episodes are not an unused, representative holdout. Full contract rules, lifecycle/true outcome-known cutoffs and market-specific historical/current fees need independent validation. Recorded snapshot update age is not transport latency: missing heartbeat/continuous recorder coverage prevents proving that an old unchanged book was still executable. Unknowns are rejected rather than filled forward. Displayed depth after a delay supports only a conditional modeled fill, not our fill or a verified queue model. The supplied trade tapes were downloaded and hashed, but not used to assert passive fills. A ticker suffix is not exact nested-rule proof. No live/profitability gates are passed.

## Public source review

* [PredictionMarketBench paper](https://arxiv.org/abs/2602.00133) and [source/data](https://github.com/Oddpool/PredictionMarketBench): usable public replay dataset; only four events and a simulator whose fills require independent review.
* [Makers and Takers](https://www.ucd.ie/economics/t4media/WP2025_19.pdf): evidence supporting a favorite/longshot research hypothesis; transaction-based population patterns do not establish our ask-price execution edge. The fresh independent screen above fails extra cost stress.
* [When do prophets profit in prediction markets?](https://arxiv.org/abs/2607.06166): useful forecast-to-sizing research, with author-reported live returns. Reproducing its forecasting process requires historical contemporaneous forecasts and grounded source availability; present-day forecasts on resolved markets would leak outcomes. No new paid/model calls or historical forecasts were manufactured. Its public trading dashboard yields no usable trade ledger through the current web reader, so author-reported returns remain unverified here.
* [Public Reddit experiment discussion](https://www.reddit.com/r/Kalshi/comments/1vobcrl/i_ran_about_40_automated_trading_experiments_on/): source-reported maker favorite ideas, losses and an explicit paper/live fill gap. Entry timing is undisclosed; insufficient for faithful replication. It is a lead, not proof. No self-bot, messages or private scraping.
* Search-discovered `dylanzonix/kalshi-btc-15m-study` repository returned404 on direct verification. Search snippets are not treated as available data or verified results.

## Next feasible evidence work

Use cached source files before any repeat download. Obtain full historical contract rules/true cutoffs through official public endpoints and validate data continuity before extending the replay. Seek a broader accessible public corpus and separately freeze any new hypothesis before evaluating it. Keep failed screens and all existing loss/trial baselines. Synpath signup is declined by owner and is not an owner blocker to keep re-requesting. Do not retest unchanged snapshots merely to generate activity. Live activation remains strategy-specific and requires its original evidence gates plus owner approval.
