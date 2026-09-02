# Intraday validation protocol

Production settings are never changed automatically by a replay or validation result. A human must review and commit every promotion.

## Nightly evidence

The after-hours report records:

- SPY-only, QQQ-only, and shared-limit SPY/QQQ outcomes;
- bounded mean-reversion parameter stability;
- 0.12R, 0.20R, 0.30R, and 0.50R transaction-cost stress;
- 0, 30-second, one-minute, and two-minute latency sensitivity;
- block-bootstrap loss probability, drawdown, and losing-streak estimates;
- a locked validation decision and explicit blockers.

Historical results use underlying minute bars. They are not represented as historical option-spread fills. The latency model remains a sensitivity penalty until enough timestamped Alpaca paper fills exist.

## Rolling validation

Use `rolling_mean_reversion_walk_forward` for repeated chronological tests. Training windows may overlap; test windows do not. A threshold pair must remain positive after modeled costs across multiple test windows and neighboring parameter values.

## Forward paper evidence

Every completed Alpaca paper spread records entry and exit fills, signal-to-submission latency, submission-to-fill latency, adverse slippage, and realized dollars. Forward validation remains blocked until at least 20 completed roundtrips exist.

## Operational chaos requirements

The automated suite covers duplicate signals, active-order locks, daily trade and loss locks, stale entries, deterministic broker client IDs, recovery after lost broker responses, automatic exits, and filled-roundtrip persistence. Market-hours supervision must still verify real quote availability, Alpaca behavior, and Render recovery.

## Promotion rule

No live-capital promotion may be inferred from a passing paper gate. Live submission remains a separate, hard-closed transport path until an explicit reviewed change is made.
