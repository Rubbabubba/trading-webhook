# System Separation Cutover

## Intended Production State

- Web service: runs `uvicorn intraday_app:app` and serves only the regime-intraday surface.
- `equities-scanner`: runs `python regime_intraday_worker.py`; it can call only regime-intraday scan and paper-reconciliation endpoints.
- Legacy background worker: suspended after the operator confirmed there were no swing positions or open orders.
- Legacy swing routes and workers: absent from the production web process.
- Intraday live submission: remains hard-disabled.

## Cutover Order

1. Push the reviewed commits to `main`.
2. Confirm the legacy exit worker is suspended and the account is flat.
3. Change the web service start command to `uvicorn intraday_app:app --host 0.0.0.0 --port $PORT`.
4. Verify `/health` identifies only `regime_intraday` and reports live trading disabled.
5. Verify the route catalog contains exactly the intraday surface and no swing workers.
6. Verify the dedicated worker logs contain successful scan and reconcile events.
7. Run `python tools/verify_system_separation.py`.

## Rollback

If the web deployment fails, restore the web start command to `uvicorn app:app --host 0.0.0.0 --port $PORT` and deploy commit `a3029f2`.

If only the dedicated intraday worker fails, keep the web service on the intraday-only app and roll the scanner service back to commit `a3029f2` while diagnosing it.

Do not resume the legacy exit worker unless the web service is also restored to the legacy application and its broker state has been reviewed.
