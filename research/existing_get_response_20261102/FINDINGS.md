Partial offline response admission engineering, November 2

existing_get_response_admission_component_v1_20261102 consumes an already
returned metadata response without a transport, retry, broker or order API.
It requires matching prepared-request and response URLs at the exact Demo
endpoint, GET, no redirects, successful status, bounded identical entity bytes,
exact ticker, explicit finite HTTP Age <=2 seconds and causal request/receipt
clocks. Missing Age remains unknown rather than zero. Decoded entity bytes are
not original wire-compression bytes. Structural Response properties and hashes
do not authenticate venue origin, publication time, fees, queue or fills.

The existing archive, physical guard and OS writer lock enforce unchanged
6-hour/1000-receipt/4MiB-payload bounds, 8MiB database/journal budgets and
128MiB free-space floor. Ten synthetic cases passed in 0.508 seconds, including
real temporary SQLite reopen/deduplication/durable stop and rejection cases.
No network, market-data, application-model or order request was made by tests.

This is NOT worker integration, an operational protocol, active collection or
a new evidence allowance. No original source studies are adopted or reset.
Next work requires a separately frozen operational protocol and reviewed hook
around the existing authenticated GET, with actual request/response clocks and
explicit handling of absent Age. No synthetic clocks or cached legacy response
may become prospective evidence. The primary economic blocker still requires
authentic entry fillability and positive executable exit economics with proper
metadata and fee provenance. All frozen strategy and first-live gates remain.
