# Render environment ownership

Ordinary production tuning belongs in `runtime_defaults.py`. Render variables
are reserved for secrets, service-specific addresses, and controls that an
operator may need to change without waiting for a deploy.

## Web service: `trading-webhook`

Keep only:

- `ADMIN_SECRET`
- `APCA_API_KEY_ID`
- `APCA_API_SECRET_KEY`
- `APCA_PAPER`
- `DRY_RUN`
- `KILL_SWITCH`
- `LIVE_TRADING_ENABLED`
- `NEW_ENTRIES_ENABLED`
- `SYSTEM_RELEASE_STAGE`
- `WEBHOOK_SECRET`
- `WORKER_SECRET`

Render may also inject platform-owned variables such as `PORT`; those are not
part of this list and should not be recreated manually.

## Exit worker: `trading-webhook-background-worker`

Keep its seven service-specific values:

- `BASE_URL`
- `EXIT_INTERVAL_SEC`
- `EXIT_PATH`
- `STRATEGY_MODE`
- `WORKER_HTTP_TIMEOUT_SEC`
- `WORKER_MODE`
- `WORKER_SECRET`

## Scanner worker: `equities-scanner`

Keep its existing worker variables until the scanner-only defaults are moved
into the applicable isolated worker. Do not remove `MAIN_SERVICE_URL` or `WORKER_SECRET`.

The new regime-intraday scheduler must run `python regime_intraday_worker.py`. The legacy swing scanner may temporarily run `python scanner.py`, and the legacy exit manager may temporarily run `python worker.py`. Do not use the legacy scanner as the scheduler for regime-intraday scans.

## Safe migration order

1. Deploy code containing `runtime_defaults.py`.
2. Confirm the web service is healthy and its runtime config matches the prior
   deployment.
3. Remove the 288 duplicated non-secret web-service variables.
4. Save, rebuild, and deploy once.
5. Recheck health, readiness, scanner heartbeat, and exit-worker heartbeat.

Never put a credential or secret value into `runtime_defaults.py`.
