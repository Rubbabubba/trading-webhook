# Legacy Swing Runtime Archive

This directory preserves the retired swing production application, worker entry
points, package, configuration, tests, and operational tools. None of these
files are imported or started by the production regime-intraday service.

The last mixed-runtime production state is commit `a3029f2`. To restore it,
deploy that commit and set Render's web start command back to:

`uvicorn app:app --host 0.0.0.0 --port $PORT`

Do not resume the suspended legacy background worker without first reviewing
the broker account and restoring the legacy web application it calls.
