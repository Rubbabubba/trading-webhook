# Independent mention NO-taker necessary-condition screen

**Failed qualification. No orders, fills, executable profit proof or deployment.**

Preregistered before new historical inputs:4b11a83e931224addfd4f1e5f03fde96c6ca7058305a53edabb12dcf34a51347. Selected30 series by a fixed hash from the official current Mentions registry. Queried historical metadata for those exact series, then projected only identity, rules and lifecycle fields. Selected one contract per event by fixed hashes within July1–August10,2026. No substitution after price/result inspection. The hypothesis is source-informed; we cannot establish disjointness from the author's unpublished historical sample.

## Results

40 distinct events selected from3863 returned market rows.18 were not open at the fixed decision,13 lacked the exact completed minute,4 failed the fixed20–80c NO-ask range, and5 yielded complete conditional analytical paths across5 dates and2 series.

| One analytical contract per candidate | Total cents |
|---|---:|
| Gross settlement result | +20 |
| Rounded benchmark taker entry fee | −10 |
| Net benchmark | +10 |
| Net with extra2c per entry | 0 |
| Net with extra5c per entry | −15 |

The extra2c date-cluster bootstrap95% lower bound on mean result is−33.4c per candidate. All statistical gates fail:100complete/100events/30dates/5series, complete enumeration, positive stressed net and positive clustered lower bound. These five outcomes are modeled, not broker P&L.

## Query repair and data limits

Initial30 requests returned400 because mutually exclusive series and MVE filters were combined. They were not zero-market observations. The error responses' bodies were not captured in that first collector version; status/URL/time receipts and original empty incomplete artifacts are preserved. A committed repair removed the extra filter, retained the exact sample/rules and added error-body retention/stopping. Corrected collection made71 successful new requests:31metadata pages and40 candle requests. Total101 attempts, within global180cap. One series KXTRUMPMENTION still has a cursor after its fixed2page limit; therefore the sample is explicitly incomplete. No extra pagination or replacement was added after inspecting returns.

Entries use NO ask=1−YES bid from the exact completed minute at24h before the earliest supplied close/expected-expiration/occurrence time. Midpoints and assumed maker fills are excluded. Candles do not establish displayed depth, original receipt age, executable activation latency, historical metadata vintage or series-specific fees. Current-registry survivorship and cross-event speaker/date correlations remain. A candle-positive result alone would not permit a Demo order trial.

## Verification

Five targeted synthetic tests passed for outcome-blind selection, completed-minute timing, correct bid/ask transformation, rounded fees, trade-direction counterexamples and opening-time exclusion. A separate verifier, importing neither collector nor evaluator, checked71 raw response hashes, reproduced all40 selected contracts and all5 conditional settlement calculations. It retained30 rejected-request receipts. Internal consistency passed; the strategy screen failed.

Preserve this experiment and its failures. Do not optimize/invert it or repeatedly fetch the same prices. Future research needs a separately registered hypothesis or independent-date execution evidence. Frozen V10/V14 and deployed worker ecaf928c13e5056e251703e720597b22de460c9b are unchanged; runtime health was not freshly reverified during this offline study.
