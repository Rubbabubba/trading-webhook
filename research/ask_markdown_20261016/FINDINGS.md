# Ask markdown settlement diagnostic — October16 UTC

## Decision

No profitable executable strategy established. Do not advance this candidate. The registered two-file decision window is incomplete because receipt-clock ordering fails. Its valid prefix also loses after the original cost reserve. This is another preserved failed/incomplete research result, not a filled trading return or qualification.

## Frozen test and source

Registration was written at2026-10-16T00:14:54.395878Z before either new file was downloaded or prices inspected. It tests the existing engineering-queue ask-markdown hypothesis, separately from the failed momentum hypothesis. No parameters were fitted to prior losses. Fixed conditions: ask falls≥3c over10seconds while bid holds/strengthens, current spread≤3c, ask10–90c, completed5second decision grid and quote age≤1second. Both sides use the same rule; YES is deterministic priority if both pass. One earliest attempt per ticker and official event; failed attempts are not replaced. Entry uses a later displayed ask after1second, at most1second late, with no price chase. One-contract settlement accounting reserves5c fees+1c slippage, with additional2c/5c stresses.

Two next chronological previously uninspected source files were downloaded from the public Hugging Face dataset,128,083,432compressed bytes, verified against catalogLFS SHA256. Four previously cached files were reused solely for history/snapshot/sequence initialization; their prices generated no entries. All six files total360,284,283bytes. No raw files were committed or redistributed. Source license is`other`, not a production permission grant.

## Source fault and preservation

The initial replay raised`ValueError: time_reorder` and produced no performance artifact. That failure is retained in`replay-abort-receipt.json`. The bounded timestamp-only audit retained the first20 cross-shard clock reversals in the sixth file, including one3.724882ms backward step. It stopped at20; this is not the entire fault count. It did not sort data or inspect post-fault prices for performance.

The implementation was corrected to preserve already observed prefix evidence on the same fail-closed stop, rather than lose all evidence on exception. Signal/entry/cost parameters and the source-order guard remain unchanged. The corrected prefix processes3,364,214records,2,247,496valid noncombo quote updates, and stops at the first backward clock. Its decision interval is2026-07-19T02:32:40.212302Z through02:36:22.889643Z. Eighteen subscription sequence resets to1 were conservatively rejected as broken continuity;245,025rows on those keys were excluded. A reset may represent a connection restart rather than missing messages; connection-epoch recovery is not verified. Do not call the whole source corrupt or silently restore eligibility under the old protocol.

## Reviewed prefix results

26preliminary ticker attempts;20official-event attempts after6duplicate-event exclusions. Fourteen conditional entries have a final official result and numeric settlement value agreeing;5entries were late/unfilled and1failed the unchanged delayed-price guard. Previously cached official metadata was reused, with one additional public GET returning all20uncached contracts. All selected entries passed the original official trading-window checks.

| Conditional prefix accounting | Cents |
|---|---:|
| Gross settlement payout less entry prices |80|
| Net after5c fee+1c slippage reserve per entry |−4|
| Net with additional2c per entry |−32|
| Net with additional5c per entry |−74|
| Actual orders / broker fills |0 /0|

There are no unknown settlements among the14observed conditional entries. This does not repair the truncated source window or establish unobserved fillability. Neither displayed depth nor retrospective settlement labels prove a real fill or contemporaneous rule availability. Historical fee applicability remains unverified.

`review.json` reports a normal lower-bound diagnostic of−27.3995c per completed official event at original reserves. It is **not an independently validated parent-event/session clustered confidence interval**: related sports totals/spreads and crypto thresholds may share outcomes. All evidence comes from one short date; a session-cluster confidence interval is unavailable. The frozen multi-date/independent-event/holdout/operational gates cannot pass.

## Verification and next action

Four synthetic targeted tests passed, including fail-closed prefix preservation without timestamp sorting. Tests are implementation checks, not market evidence. The initial registration console hash used an LF string before Windows CRLF writing;`registration-hash-receipt.json` records both hashes. The registration content is unchanged, and narrow Git attributes preserve its original bytes.

Retain this failure and the earlier momentum/weather/favorite failures. Do not invert, optimize or rerun this window until a winner appears. Before further source-based research, independently register connection-epoch/receipt-order handling on an unused window, or use a source with verified chronology. No new service, schedule, application model call, paid feed, risk change, Demo order or live activation was made by this study. Full engineering and profitable-system objectives remain incomplete.
