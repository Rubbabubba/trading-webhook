# Hosted paper deployment package

Blueprint: `deploy/kalshi-paper.render.yaml`. Service: `kalshi-paper-portfolio`.
Start command: `python -m opportunity_lab.kalshi_hosted_paper --data-root /var/data/kalshi-paper`.
Build dependencies: `deploy/kalshi-paper.requirements.txt` (timezone data only).

The separate worker requires no account keys or secret files. It discovers markets
from public Kalshi APIs and runs an independently frozen paper portfolio. It does
not deploy any production broker or modify the existing account observer. Its
eight-source manifest is `configs/kalshi_hosted_paper_20260914/manifest.json`.

One starter instance plus a 1 GB persistent disk is approximately **$7.25/month**
in additional service charges, before taxes or workspace-specific charges, using
[Render pricing](https://render.com/pricing): $7 compute and $0.25/GB storage.
Creating this service requires approval of the additional recurring expense.
No service has been created by preparing this package.

All runtime discovery, configuration and SQLite ledgers live under the disk mount.
Windows/Linux process locking prevents duplicate instances. Startup checks source
hashes and rejects changed ledger configuration. Manual deployments are selected
to avoid unintended changes to a running experiment. A disk-backed redeploy has
a brief stop/start rather than zero downtime, per [Render's disk documentation](https://render.com/docs/disks).

Local tests verify self-contained discovery bootstrap, ledger reopening and lock
exclusion. A public-data smoke run succeeded locally in
`sports_paper/hosted_acceptance_v2_20260914`. The run discovered and visited a 12-market cohort, then restarted against the same configuration and ledgers without errors. All five SQLite integrity checks passed. This machine has neither Docker nor an
installed WSL Linux distribution, so actual Linux/Render acceptance remains to be
performed after deployment; it must not be claimed from the Windows test.

Acceptance sequence after cost approval and publishing the reviewed source:

1. Deploy this blueprint with one instance and the persistent disk, no secrets.
2. Confirm fresh discovery and paper status, source checks and error logs.
3. Record ledger integrity and observation/action counts.
4. Restart the same service; verify the same ledger, retained cash/inventory,
   increasing counts and no duplicate worker.
5. Confirm shutdown/restart behavior and visibility of failures.

The experiment's sampling deadline is September 21 at 14:30 UTC. Stopping the
experiment does not automatically cancel Render billing; suspend/remove the
service separately when the hosted trial ends. No profitable strategy or real
trading activation is implied by deployment.
