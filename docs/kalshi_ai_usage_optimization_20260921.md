# Kalshi AI-usage optimization rollout

## Verified baseline

The Render Demo background worker `kalshi-demo-v6-maker` runs
`opportunity_lab.kalshi_demo_v5_maker_worker` on a two-second operational loop.
It owns order submission, cancellation, exits, reconciliation, risk limits and
evidence collection. Those paths do not call an AI model. Render's native
service-failure alert independently detects a stopped worker and has already
delivered failure notifications for this service.

Before this rollout, the Codex task heartbeat ran every 30 minutes, or up to 336
times per week. It combined sports reviews with Kalshi operational and research
review. The September 21 audit supplied by the operator reported 49 completed
heartbeat responses, 44 suppressed notifications, approximately 29% of Kalshi
raw token traffic in heartbeat-associated turns, and approximately 88% of the
observed local weekly raw token traffic attributed to Kalshi. These audit figures
are a reported baseline; the repository cannot independently reconstruct exact
subscription charges. At rollout start, the account usage tool reported 66% of
the current seven-day Codex allowance used.

The live pre-change worker status was fresh and healthy: Demo environment,
production disabled, no errors, no open position, no unresolved order, 194
post-only V9 attempts, one completed maker fill, and V10 shadow execution
disabled. V10 had not yet emitted a qualifying signal.

## Implemented architecture

`opportunity_lab.kalshi_deterministic_monitor` now runs inside the existing
worker at most once per minute. It makes no model or network call. Each check
validates status freshness, phase, errors, Demo and production safeguards,
strategy identities, the single-position/single-order invariants, and the V10
execution lock. It evaluates the frozen V10 gate only from complete data.

The checker writes four bounded, atomic artifacts on the existing persistent
disk:

- `monitor/review_packet.json`: current compact worker, health, evidence delta
  and V10 gate state.
- `monitor/checkpoint.json`: durable fault, gate and evidence checkpoint.
- `monitor/metrics.json`: checks, zero-AI checks, requested investigations,
  trigger categories and duplicate suppressions.
- `monitor/escalation.json`: the most recent unique actionable transition.

Faults are deduplicated. A new fault, the third identical fault observation, a
recovery, a V10 gate transition, or the daily review boundary creates one
investigation request. Healthy unchanged checks only update the packet and
metrics. Restarting the worker preserves the checkpoint and cannot replay the
same escalation indefinitely.

V10 status now includes complete signals, independent events and a deterministic
event-cluster 95% lower confidence bound for each registered horizon. Missing
events, horizons or confidence bounds cannot pass the gate. This adds evidence
measurement only; it does not alter signals, orders, risk or promotion rules.
Each eligible V10 market evaluation is also counted and classified as a signal
or a specific rejection reason. If the running worker records no new evaluation
for 15 minutes, the deterministic monitor raises `v10_evidence_stalled`; the
fault clears when evaluation progress resumes. This distinguishes a functioning
strategy that rejects current books from a stalled evidence pipeline.

Render remains the independent stopped-process detector. The in-process checker
detects stale status and persistent application faults. A critical process exit
continues to invoke Render's existing failure notification rather than an AI
poll.

## AI schedule and routing

After live packet verification, the mixed 30-minute task heartbeat is reduced to
one daily review at approximately 08:15 America/Chicago. The daily run consumes
the compact packet first and reads detailed databases or logs only for a specific
trigger. Existing blocked-message and recipient restrictions remain in force.

Routine classification is ordinary Python and therefore uses no model. The
current thread-heartbeat interface does not expose a verifiable per-automation
model override, so this rollout does not claim that GPT-5.6 Luna is selected for
the daily task. No API key, paid API traffic or substitute AI service was added.
OpenAI's model documentation describes Luna as appropriate for clear, repeatable,
high-volume work; it remains the preferred trial when heartbeat model routing is
exposed and can be verified.

## Validation and controlled faults

Tests cover healthy unchanged checks, stale status, new and persistent errors,
duplicate suppression, recovery, production-safeguard changes, missing gate
data, a valid gate transition, checkpoint survival across restart, and the
unchanged V9/V10 worker integration. The full repository suite is required before
deployment.

The first live check must show a fresh packet, Demo mode, both production and V10
execution disabled, and incrementing zero-AI check metrics. The old heartbeat is
not reduced until those live artifacts are verified.

## Measurement

The durable monitor metrics establish the post-rollout denominator: total
checks, checks completed with zero AI, unique investigation requests and
duplicate requests suppressed. Compare these with Codex usage after 24 hours and
seven days. Allowance usage is account-wide and the audit measures raw tokens, so
neither metric proves a precise subscription reduction by itself.

## Bounded development workflow

Operational faults may trigger immediate repair. Each strategy or repair item
must state one hypothesis or defect, the evidence cutoff, the smallest useful
change, validation, completion condition and stop condition. One registered
strategy challenger is evaluated at a time. Unchanged evidence does not justify
another full suite, redeployment or broad research pass.

Ownership is now:

- Kalshi Trading Bot task: worker operation and the active Demo experiment.
- Trading Edge Lab: broader strategy research using exported evidence, without
  duplicating worker health review.
- Profit Opportunity Scout: opportunity discovery, excluding routine supervision
  or tuning of the active Kalshi Demo worker.

Proposed edit for Trading Edge Lab: “Consume the latest compact Kalshi review
packet only when a registered research gate changes or the Kalshi task requests
a bounded hypothesis. Do not poll worker health, redeploy the worker, or tune the
active V9/V10 experiment.”

Proposed edit for Profit Opportunity Scout: “Exclude routine Kalshi Demo worker
monitoring and active V9/V10 tuning. Report a Kalshi opportunity to the Kalshi
task once with its evidence source and stop condition; do not repeatedly revisit
unchanged candidates.”

Those two prompt edits belong to their respective tasks and are not applied from
this repository.

## Rollback

Roll back the deployment to commit `3e911cb` and restore the prior 30-minute
heartbeat. The monitoring files are additive and may remain on disk; the older
worker ignores them. No order or evidence schema used by V9 is removed. If only
the AI schedule needs rollback, restore the prior heartbeat frequency without
changing the Render worker.
