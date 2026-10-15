# Historical forecast/market overlap evaluation — complete October15,2026

## Decision

The bounded data-overlap and fixed diagnostic evaluation is complete. This point-forecast candidate does not advance. No executable profitable system, Demo order qualification or live qualification was established. The overall engineering backlog is not certified complete.

## Registered evaluation

New York Central Park high-temperature events for June1–30,2026, with one decision at20:00UTC on the preceding date. Original parameters were frozen before requesting new prices or forecasts. ECMWF IFS and GFS prior-date00UTC single runs provide temperature forecasts; later forecasts and reanalysis are prohibited. This is a separate exploratory protocol, not Weather EnsembleV14 and not a change to its evidence gates.

The original daily endpoint requests returned400 because the run did not begin at local midnight. Those14 errors and the original registration remain immutable. Before fetching usable forecasts, registered hourly-v2: same models/run/date, exactly24 target-local-date hourly temperatures, fixed maximum, fixed4°F normal uncertainty proxy, no fitted correction. No initialization or decision time was moved to obtain results. This hourly maximum differs from a continuous station high; its probability calibration is unproven.

Indicative entries use only the exact completed minute's closing bid/ask; NO ask is1−YES bid. Missing quotes are rejected, not replaced by last trades, future candles or final prices. Require spread≤10c, entry10–90c, both model edges≥12c, model-center disagreement≤3°F, rounded benchmark taker fee and2c additional model/cost stress. At most one best candidate per city-day. Selection receives no settlement outcomes.

## Results

| Measure | Result |
|---|---:|
| Frozen calendar dates |30|
| Official contracts cached |180|
| Dates with both complete forecast maxima |28|
| Dates rejected for model-center disagreement |8|
| Dates with comparable eligible quotes |17|
| Comparable binary inputs |55|
| Missing exact completed-candle exclusions |65|
| Selected city-days / complete conditional outcomes |14 /14|
| Conditional settlement total after benchmark fee+2c stress |+92c =+$0.92|
| With another2c per entry |+64c =+$0.64|
| With another5c per entry |+22c =+$0.22|
| Mean city-day Brier difference, forecast minus market |+0.0303594; positive means worse|
| Approximate city-day95% lower bound, mean conditional cents per selected day |−16.034c|
| Actual verified executable signals / fills |0 /0|

Two dates have nonfinite/incomplete target-day forecast values. Three of the20 model-agreeing dates lack comparable quotes. Three of the17 comparable dates have no selected candidate. These exclusions remain explicit in `result-v2.json`; they are not replaced or optimized away. The small conditional positive total fails the confidence criterion and does not establish superior forecast accuracy. Its bounds are exploratory normal intervals, not a robust multi-city/season qualification.

All180 contracts' official records were retained. Numeric settlement-high consistency could be checked for179 and matched all179 binary labels; one record has an empty expiration value and cannot independently verify the measured high. Official labels are not our broker settlement/fill reconciliation.

## Access and integrity

284 unique cached publicGET responses:30 event metadata,180 narrow candle requests,60 hourly forecast runs and14 retained rejected daily requests. No application model calls, credentials, orders, purchases or new schedules. Per-response hashes and both registration hashes are in the result. Five targeted synthetic tests passed for complete local-day inputs, probability/fees, no future/last-trade substitution, NO ask arithmetic and two-model edge requirements.

The pmxt public archive index timed out once via the web reader and once through a bounded direct request. No archive book files were obtained and no absence of global coverage is inferred. KalshiBackTest's reviewed offering requires an account/key and describes crypto coverage; it was not represented as available weather data. Synpath signup remains declined, with no further owner request.

## Validity limits and remaining requirement

Single-run initialization is not historical receipt time. The fixed8-hour availability lag is an assumption, not verified contemporaneous availability. Model grid coordinates differ from exact station coordinates by normal model-grid resolution. Minute candle quote summaries have no displayed depth, true update-age guarantee or delayed execution path. Their closes are indicative accounting proxies, never modeled guaranteed fills or actual fills. Modern archived market metadata is not an original historical metadata vintage. Only one city and season were evaluated.

Therefore the required executable historical overlap remains incomplete even though quote/forecast calendar overlap exists. A larger untouched test needs timestamped disseminated forecasts, matched book depth/coverage and market-specific fees. Do not expand/invert/tune this failed hypothesis on these30dates. Preserve its fixed negative skill and confidence evidence. The existing prospective V14 study and frozen300city-day/6city/2season gates remain unchanged; this diagnostic supplies none of those passes.

## Sources

* [Open-Meteo Single Runs API](https://open-meteo.com/en/docs/single-runs-api): preserves individual initialization runs and distinguishes initialization from publication.
* [Kalshi historical markets](https://docs.kalshi.com/api-reference/historical/get-historical-markets) and [historical candles](https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks): official metadata and minute quote summaries.
* [Prediction Markets Beat the Weather Forecast on Tomorrow's High Temperature](https://arxiv.org/abs/2609.23969): source-grounded negative prior for naive forecast-versus-market strategies. Published aggregate findings are not independent reproduction of our results.

Full machine-readable evidence: `result-v2.json`, `settlement-consistency-audit.json`, original and hourly-v2 registrations and the immutable `cache/` directory. Reuse all cached inputs before any additional API request.
