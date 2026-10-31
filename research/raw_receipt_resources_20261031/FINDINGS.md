Partial offline resource engineering, October 31

The new raw_receipt_physical_guard_v1_20261031 component wraps the unchanged
October 30 archive on a NEW path. It adds an 8 MiB SQLite page ceiling, DELETE
rollback journal policy, separate 8 MiB journal reserve, a 128 MiB free floor
and 4 KiB sidecar allowance. Admission reserves the full database plus journal
budget in addition to the free floor; payload bytes alone are not disk usage.
The original 1,000 receipts / 4 MiB payload / six-hour bounds remain unchanged.

An exclusive, fsynced external stop survives SQLite FULL and restart without
writing to the full database. Missing registered databases are never recreated;
old databases cannot be adopted, and stopped runs cannot resume after disk
recovery. Eight synthetic tests include actual SQLite FULL/rollback with no
partial receipt, low-space restoration and unexpected WAL rejection.

This is a single-writer offline component, not a launched run, HTTP transport
observer or deployed worker. No existing studies or observations are imported.
It does not establish authenticated metadata, fees, queue priority, fills,
exits or profit. Worker integration still needs a separately frozen operational
registration, existing-GET observer, lifecycle/fault integration and process
concurrency control before collection. No new market data or orders requested.

Primary bottleneck remains authentic fillability and positive executable exit
economics. Existing source-v5 and comparison-v4 stay stopped with prior misses
retained. Their fixed limits are not reopened, extended or silently rolled over.
