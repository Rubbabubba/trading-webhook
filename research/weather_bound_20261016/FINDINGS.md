# Weather measurement qualification — October 16, 2026 UTC

**No profitable executable strategy qualified.** This was an unpriced source-qualification study, not a trading backtest.

The preregistered frame contains 186 city-days across New York, Chicago and Los Angeles, July–August 2026. Two previously inspected Los Angeles dates were excluded. Of the remaining 184, only **41 city-days across 30 dates** qualified: Chicago 26, Los Angeles 15, New York zero. There were 28 logical contract-side candidates and no observed lower-bound contradictions. This falls short of the frozen 150 city-day, 45-date, three-station and 40-per-station requirements. Original publication/receipt timing remains unverified. There are no price inputs, orders, fills or P&L.

## Source findings

- 89 city-days had no qualifying available six-hour report wholly inside the climate day. Correction/maintenance and ambiguity filters remain unchanged; no buffer tuning was performed.
- 54 city-days specify The Weather Company settlement, incompatible with this NWS-based measurement protocol. Current GLOBALTEMPERATURE terms prioritize TWC, then NWS, then the national weather agency. Old NWS evidence cannot automatically qualify those contracts. This is not a verified defect in deployed V14.
- Empty historical responses for 51 events were a storage-routing issue. A preregistered repair used the official cutoff and current-market endpoint. All original responses and the initial blocked result remain retained.
- The strict Los Angeles Airport alias was corroborated with official station identity. Historical contract PDF vintage remains unverified. A diagnostic Windows text-decoding artifact did not require a parser change.

## Verification and preservation

Fourteen synthetic tests passed. A separate verifier checked 242 immutable public HTTP responses and hashes, all 186 planned city-days, 246 numeric contract labels and 28 selected raw measurement bounds. It passed internal consistency; it does not certify source qualification, executable fills or profit.

The deployed worker remains ecaf928c13e5056e251703e720597b22de460c9b. These are offline research changes. Frozen V10/V14, account limits and live approval gates remain unchanged.

Next research must verify the exact current settlement agency, station, precision, climate-day and historical publication semantics before pricing any measurement strategy, and obtain independent-date executable order-book evidence without selecting dates for profitable outcomes.
