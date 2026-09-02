# System Separation Cutover

## Intended Production State

- Web service: serves the new `regime_intraday_api` router and the temporary legacy swing management surface.
- `equities-scanner`: runs `python regime_intraday_worker.py`; it can call only regime-intraday scan and paper-reconciliation endpoints.
- Legacy background worker: continues running `python worker.py` for swing exits until the live account has no swing positions or open swing orders.
- New swing entries: stopped because no production scheduler calls `/worker/scan_entries`.
- Intraday live submission: remains hard-disabled.

## Cutover Order

1. Push the reviewed commits to `main`.
2. Wait for the web and both workers to deploy successfully.
3. Change the `equities-scanner` Render start command from `python scanner.py` to `python regime_intraday_worker.py`.
4. Verify the new worker logs contain `regime-intraday-worker` scan and reconcile events.
5. Verify the legacy exit worker remains healthy.
6. Run `python tools/verify_system_separation.py`.
7. Confirm there are no new calls to `/worker/scan_entries`.

## Rollback

If the web deployment fails, roll all three services back to commit `9016ae4`.

If only the dedicated intraday worker fails, restore the scanner start command to `python scanner.py` and roll that service back to `9016ae4`. The prior combined scanner will resume the earlier behavior while the failure is diagnosed.

Do not suspend or delete the legacy exit worker during rollback or cutover while swing positions or orders may remain.
