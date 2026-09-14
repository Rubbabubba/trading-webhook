# Kalshi production preparation — September 14, 2026

Target requested: September 15 afternoon; working checkpoint 3 p.m. Central.
User selected the existing Render account and intends to fund Kalshi with $500.
This is an engineering target, not a statement that a live strategy has passed.

## Implemented

- Isolated GET-only production account observer with signed requests, primary-account
  scope, bounded pagination, rate backoff, persistent snapshots and single-writer lock.
- $500 ceiling displayed separately from actual cash. This observer cannot spend.
- Offline durable order journal with stable IDs, transactional reservations, persistent
  stop, uncertain-submission blocking, partial-fill and identity checks after restart.
- Render worker blueprint at `deploy/kalshi-observer.render.yaml`. This deploys the
  observer only, not the offline journal or a trading bot. Hosting is not yet deployed.
- Existing paper strategies, ledgers and experiments remain separate.

Account authentication succeeded September 14: cash $56.25, positions 0,
resting orders 0. This is a point-in-time observation; reread before decisions.
All 12 account-observer and offline-journal tests passed. Render browser access
reached the sign-in screen; user sign-in is pending. The existing 30-minute review
automation now includes this preparation work and a one-time September 15 3 p.m.
Central readiness checkpoint. The account observer has only been run once locally;
no continuous observer or Render worker has been launched.

## Remaining delivery sequence

1. Deploy the observer on the selected Render account; verify fresh snapshots across
   a service restart and persistent storage. A local process is not a Render deploy.
2. Implement a separate broker adapter against current V2 event orders, including
   authenticated order/fill lookup, position reconciliation, exits and cancellations.
   The offline journal is a rehearsal scaffold, not a complete broker adapter.
3. Use mocked failure cases and Kalshi demo credentials to verify accepted/rejected
   orders, timeout after acceptance, partial fills, cancel/fill race, disconnects,
   fees, restart, external/manual positions and daily loss accounting.
4. Compare strategy evidence from independent prospective tests; identify the exact
   version and markets proposed for user review. Current loss-making controls and
   exploratory reversals have not established a profitable release candidate.
5. Publish a readiness report by the checkpoint with completed evidence and exact
   remaining blockers. User retains real-money activation and trading decisions.

The research process can propose new versions; it does not change an active version
or increase its budget. Execution and reconciliation should be deterministic services.
An AI reviewer may inspect reports and propose code; it should not directly control
broker credentials or bypass execution checks.

## Render setup

Use a separate worker named `kalshi-account-observer` from this repository, with
the build/start commands and disk in the blueprint. Do not change the equity,
Opportunity Lab or existing paper services. Upload the existing private key as a
Render secret file named `kalshi-private.pem`, then set only the key ID in the
worker environment. Never commit a key or paste it in chat. Render service creation
and secret provisioning have not been performed. A paid worker/disk is specified;
review its actual displayed cost before provisioning.

Run locally:

```powershell
.venv/Scripts/python.exe -m opportunity_lab.kalshi_account_monitor
.venv/Scripts/python.exe -m pytest tests/test_kalshi_account_monitor.py tests/test_kalshi_order_journal.py -q
```

Local private account snapshots go under ignored `sports_paper/production_readiness_20260914`.
The observer has no POST/DELETE transport, no live-enable flag and no order submission.
The journal currently supports buy-YES rehearsal only; it conservatively retains all
reserved capital after terminal orders pending a future position/settlement ledger.
Its $5/order and $50 aggregate rehearsal caps are test defaults, not a validated
investment allocation or a live loss limit.

References: [authentication](https://docs.kalshi.com/getting_started/quick_start_authenticated_requests),
[V2 event orders](https://docs.kalshi.com/api-reference/orders/create-order-v2),
[Render secrets](https://render.com/docs/configure-environment-variables),
[Render Blueprint specification](https://render.com/docs/blueprint-spec).
