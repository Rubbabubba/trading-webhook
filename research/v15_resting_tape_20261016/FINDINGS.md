# V15 resting-order / public-tape test — October16 UTC

## Decision

No profitable executable strategy was established. This separately registered fixed model produced one conditional primary entry that lost at all three liquidation horizons, plus five ambiguous possible fills. Do not advance it to a Demo trial. These are counterfactual calculations, **not our orders, broker fills or realized losses**. Native V10/V12/V15, prior failures and all frozen gates remain unchanged.

## Preregistration and causal correction

Initial source/model registration and code were committed atb8cffbe before ten unused files25–34 were acquired. Cached19–24 were warmup only. Decision files25–30 and exit-only31–34 were fixed in advance; no tail entries were added. All500,180,465new compressed bytes matched published LFS hashes. Raw cache is outsideGit; source receipts and derived evidence are retained here.

Draftv1 combined client quote eligibility and order arrival at one timestamp. Before any public tape or fill outcomes were evaluated, the draft was retained and causalv2 separately registered at2026-10-16T02:34:06.966613UTC, SHA256`25348a8ae0b19d977b1d34af917b3ca86d5821a1519ba983ad39bfac51fae702`, commit0f6dd75. v1source-only signal/view capture continued unchanged, with its original planned timestamps retained explicitly. Do not present draftv1 as a valid execution result. Source-registration final hash is`17d1e76d0b9a076f68143f1fbb37298306573f0df7c6ad49b2ee89b750fc5d92`.

v2 keeps the frozen V15 quote formula (V12 signal with gross edge at least5c, bid+2limit), price, costs and TTL. Client checks the book at decision+1second and submits only if current best bid<=limit<ask. Modeled post-only arrival is decision+2seconds; only a marketable arrival ask rejects it. A worse arrival queue cannot retrospectively prevent a sent order. Missing arrival evidence remains uncertain possible acceptance/fill. TTL60seconds begins at arrival; cancellation acknowledgement takes1second. Exit liquidation includes another1second transport delay; no first-view search for a favorable price.

A qualifying conditional fill requires an opposing non-block public execution of at least1contract strictly at least1c through the economic outcome limit, within the guarded clock interval. Earlier touches, partial trades or activation/cancellation clock bands are ambiguous; they cannot be skipped in favor of a later convenient trade. The assumed1second exchange-versus-recorder clock margin is **unverified**, so this is not operational fill proof. Displayed queue size is at most a lower-bound observation, not our queue position.

Each5/30/300second exit is a separate counterfactual. Entry pays the resting limit plus2c fee and1c slippage reserve; exit uses a post-latency bid minus1c slippage and2c fee. Extra2c/5c per transaction scenarios remain fixed. Actual historical series fees, original publication vintages and network latency remain unverified.

## Source and selection

The new decision window was July19 03:13:47.481191–03:27:47.283600UTC, about14minutes on one source date.8,567,135input rows included5,931,023fully reconstructed book rows and2,636,108initial unverified rows that remained excluded.50inferred snapshot epochs,1,629global clock reversals and a maximum21.068485ms modeled availability fence were retained.2,702,880zero-truncated derived views were rebuilt from raw ladders. The hash-verified one-second cache has66,970views;64,999non-null views passed independent causal-clock checks. Its911,234compressed bytes are at the manifest's private workspace cache path.

181source ticker attempts became76eligible official-event attempts across30families.94duplicate-event attempts,7outside documented windows and4missing official metadata remained excluded. Earlier missing metadata responses were reused rather than re-requested. Official event IDs may share parent outcomes; these are not76independent parent events or sessions. Earliest eligible event attempts were selected before the public tape was fetched, without any PnL ranking.

71bounded official public-trade GETs produced71complete ticker windows,109unique validated executions and no page/transport/global-identity faults.13windows had any trades;58were empty. Five additional event windows needed no tape GET because all modeled quotes failed activation. Every raw page body, request URL, hash and capture receipt is retained in the workspace cache. Public API completeness does not certify our order acknowledgements or market impact.

| Matched track | No signal | Quote rejected | No qualifying trade | Ambiguous | Conditional entry |
|---|---:|---:|---:|---:|---:|
| V15 |0|6|64|5|1|
| V12 |0|6|65|5|0|
| V10 |63|0|11|1|1|

The V15 and V10 conditional paths refer to the **same underlying public trade/event**, not two independent trades. V12 has no conditional cash outcome; its PnL is null rather than a fictional zero-return trading history. Ambiguous primary cases:3activation-clock bands,1partial-contract execution,1price touch without strict trade-through. All remain uncertain; no clock, size or price guard was relaxed.

## Primary conditional cash result

The sole primary path isKXWCAWARD-26BYP-PCUBA, NO at modeled75c. Client book was73/81c; arrival book73/82c. A strict opposing public execution supplies only a counterfactual fill proxy. No owned fill occurred.

| Horizon | Gross bid minus entry | Net after fixed costs | Net with extra2c/transaction |
|---|---:|---:|---:|
|5seconds|-2c|-8c|-12c|
|30seconds|0c|-6c|-10c|
|300seconds|-1c|-7c|-11c|

These horizons must never be summed as separate positions. Five ambiguous paths carry235c total full-entry-principal/cost stress under the original reserve. Primary combined analytical stress is-243/-241/-242c at5/30/300seconds; with extra2c per transaction,-257/-255/-256c. These are stress calculations, not realized account PnL. No valid session-cluster confidence interval exists from this one date and one conditional event.

## Independent verification and remaining work

34relevant synthetic tests passed, including causal two-stage timing, no arrival-book look-ahead, post-only rejection, adverse queue movement, clock races, partial prints, exact duplicate handling, conflicting identities, cache-payload/raw-body consistency and incomplete pagination. A separate audit does not import the execution or accounting model: it verified all76event selections,64,999causal source views,71raw tape pages/109executions,2conditional benchmark paths on one event,11ambiguous benchmark paths and every cash scenario. Its result is internal consistency, not external source truth, broker fill or profit certification.

This test identifies low execution participation and nonpositive executable liquidation in the bounded cohort. It does not prove no strategy can work, statistically reject every passive V15 variant, or justify repricing this inspected window until a winner appears. Preserve all failures and ambiguity. Next work needs a distinct preregistered source-grounded hypothesis or genuinely broader independent evidence, rather than more cost/clock/limit tuning here.

Required100complete entries/40verified independent parent events/5families on unused independent dates, positive stressed results and valid parent/session lower bound remain unmet. Actual fees, source publication, clock alignment, authentic queue/ack/fill, cancellation and cash reconciliation evidence are missing. Source license isother; no production data grant is inferred. Last verified deployed worker remainsecaf928; this research was not deployed and sent0orders. No application model calls, paid services, schedules or risk changes were added. Profitable-system goal remains active and incomplete.
