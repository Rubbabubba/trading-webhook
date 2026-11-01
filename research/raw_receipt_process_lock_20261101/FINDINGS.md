Partial offline process-concurrency component, November 1

raw_receipt_process_lock_component_v1_20261101 acquires an OS-owned,
nonblocking single-writer lock before any resource registration, stop receipt
or SQLite open. The stable one-byte lock file is never unlinked/replaced, and
no age/PID timeout can steal another process's ownership. File descriptors are
noninheritable; normal close and process death release kernel ownership.

It composes the unchanged October 30 archive and October 31 physical guard.
Fresh-path requirements, immutable registration, durable stops, six-hour /
1000-receipt / 4MiB-payload bounds, 8MiB database/journal budgets and 128MiB
free-space floor remain unchanged. A closed wrapper cannot reopen itself.

Eight distinct synthetic cases passed on Windows (0.473s) and Linux (0.440s),
including a genuine second Python process contending while a writer is alive,
unchanged database bytes after that rejection, kernel release after os._exit,
constructor-error release, no registration before ownership and retained stops.
These are the SAME eight cases on two OS branches, not sixteen independent
market observations. The Linux test used temporary fixtures on the existing
Demo host, with a stripped environment, no production database imports and no
network requests, application model calls or orders.

This is not worker integration, operational registration or an active collector.
No exhausted source study was adopted, reset, rolled over or extended. A bounded
existing-GET observer and frozen operational protocol remain missing, along with
authentic metadata/fee provenance and executable entry/exit/fill evidence. The
primary economic bottleneck remains authentic fillability and positive net
executable economics. All frozen V10/V12/V14/V15 gates and failed results remain.
