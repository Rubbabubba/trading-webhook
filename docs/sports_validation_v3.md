# Locked sports research protocol — capture v3.0

Accepted September 10, 2026. This is the next phase of the MLB/NFL research program, following the complete September 8 MLB audit. The audit found 8,982 observations, 7,445 in-progress observations, 259 accepted observations (3.48%), and zero original trades. A diagnostic replay combining a half-sized fee coefficient and zero net-edge requirement produced two trades and lost $0.26. This does not establish a profitable strategy.

## Scope and separation

The new manifest is configs/sports_capture_20260910/manifest.json: five MLB games September 10 and the remaining 15 NFL games September 10–14. Each MLB game has an exact two-team match to MLB's public reference feed, with matching scheduled start time. The old college and MLB ledgers and v2 NFL configuration remain the baseline. No old data are rewritten, no gates are relaxed, and no real orders are authorized.

The capture program has no account, order routing, credential handling or trading decisions. It cannot automatically promote itself into a trader. Its manifest and source SHA-256 fingerprints are frozen; changed source/configuration requires a separately versioned experiment. Current v2 parser output is recorded as a diagnostic, not asserted to be a corrected parser.

## Phase 1 — now running

Capture original response bytes before parsing, losslessly gzip-compressed with SHA-256, URL, request-start/receive timestamps, latency, HTTP status and headers. Preserve malformed JSON responses for diagnosis. Store all ESPN plays and win-probability arrays, Kalshi market metadata and 20 book levels, and MLB's full reference game feed. Record request failures and diagnostic exceptions explicitly. Nothing may silently repair a timestamp or discard a mismatch.

The MLB comparison records reference identity, scores, inning/half, outs, count and current play. A score match alone is not a validation pass; source timing and within-inning changes still need examination. NFL currently has no second state provider configured and remains explicitly unvalidated.

Collect from two hours before each scheduled start, once per minute pregame and every 20 seconds thereafter, stopping on recorded completion or the frozen 12-hour deadline. This remains sampled market data; a streaming connection is a later step requiring authenticated setup. Sequential game cycles can take longer during slow responses; measured request timestamps reveal this rather than pretending intervals are exact.

## Required review before any v3 trading

- Verify archive hashes and replay raw payloads into deterministic parser fixtures covering pitches, at-bat markers, scoring plays, inning changes, breaks, delays, corrections and outages.
- Establish actual game-state alignment against the second source, explain discrepancies and estimate lag. Compare inning/half, outs, baserunners, score and count, not just scores. Capture timing evidence before declaring a probability current.
- Report valid-state coverage and every excluded interval by game. Do not proceed solely because an arbitrary coverage percentage is met while scoring-play discrepancies remain unexplained.
- Resolve runtime interruptions and measure dropped intervals. A local computer going offline cannot be recovered by a local process.
- Build a new candidate parser only from verified cases, retain fail-closed behavior for unknown state transitions, and validate it prospectively on additional untouched games.

## Phase 2 — prospective small paper experiment, gated

After Phase 1 review passes, specify a separate version with a calibrated probability model matched to the intended holding horizon. Compare a small, declared set of value-based/5-minute/15-minute hypotheses, using current series fees and separate slippage/latency stress cases. Save exact per-candidate rejection reasons and executable size. Lower fees must not conceal unrealistic fills. Stream and synchronize books when authenticated data access is configured; faster market data cannot repair slow game data.

Require positive expected value after costs and uncertainty allowances. Do not impose a trade quota. Evaluate outcomes on untouched games, clustered by game, against market-implied baselines. Report calibration, net returns, drawdown, missing data and confidence. In-sample positive markouts do not count as proof. Paper exposure remains small; real-money execution is outside this protocol.

## Operation

Start/resume with tools/start_sports_capture.ps1. Output is sports_paper/capture_20260910/capture.sqlite3 and status.json. The OS lock prevents duplicate writers. The launcher is bounded through September 15 at 12:15 UTC. A PAUSE file in the output directory prevents captures and automatic launch; a file named <league>_<event_id>.PAUSE pauses that game. Existing data remain intact.

Local automatic process recovery can restart stopped processes while the signed-in computer is available. It cannot operate while the computer is shut down. The Codex follow-up continues separate reporting of baseline results and capture validation; capture-only results must never be called trading returns.
