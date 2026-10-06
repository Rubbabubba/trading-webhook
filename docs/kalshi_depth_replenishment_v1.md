# Prospective displayed depth recorder

The frozen registration is `configs/kalshi_depth_replenishment_v1_20261006/registration.json`. `observe_frame` records the maker's existing validated Demo order books without adding requests or changing any order decision, V10/V12 protocol, trial cap or loss baseline.

The recorder retains at most 20,000 immutable snapshots. It tracks a drop to at most 20% of prior displayed size at the same best bid and a subsequent recovery to at least 80%, with a maximum 120-second interval. Sampling gaps and changed prices are retained as incomplete episodes. Duplicate/nonincreasing observations, invalid/crossed depth and changed event identity are rejected. Pending episodes survive a process restart. Once capacity is reached capture stops rather than erasing prior observations.

This is sampled depth, not a streaming trade feed. A depletion may be cancellation, not execution. No fill, queue position, fee-adjusted profit, promotion readiness or order authority is inferred. Announcement exclusions, trade/cancel attribution, latency-aware fee replay, independent holdout and actual Demo fills remain unimplemented parts of the broader Liquidity Replenishment Timing proposal.

The research experiment registry reads the maker SQLite database in read-only mode and counts only independent event observations after registration. Life OS separately validates the quote-only flags and accounting counts. The next budgeted idea batch can select this exact capture-only capability. Engineering receipts distinguish this implemented recorder from the remaining execution adapter work.
