# Durable raw metadata receipt component, October 30

Partial offline engineering for the frozen V15 executable-evidence bottleneck.
This new component persists the exact v2 receipt envelope and original body;
its copied receipt validator is byte-identical to the October 29 component.
It has no HTTP client, broker interface, worker import or automatic scheduler.

Thirteen synthetic tests passed. Durable SQLite registration fixes the start
and protocol: no restart extension, protocol replacement or capacity increase.
Limits are 1,000 receipts, 4 MiB serialized payload and six hours per declared
run, with 90,000 bytes per envelope. These payload limits exclude SQLite page
overhead. A future worker integration still needs a physical file-size/disk
floor, network-response interception, fault reporting and a separately frozen
operational registration before authentic capture. This module has not begun
such a run and does not reopen stopped source-v5 or comparison-v4.

Append-only data, exact byte/envelope hashes, causal start/receipt/observation
clocks, duplicate suppression and durable capacity stops are covered. Counter
and receipt limits survive reopen. Old receipt and failure records are retained.
Fixture integrity never verifies venue authenticity, current fees, owned queue
priority, fills, executable exits or profit; all corresponding flags stay false.

No new market data, prices or outcomes were requested. No active trading rules,
frozen studies, baseline, budgets, service or schedule changed. Not deployed.
Binding stage remains a genuine contemporaneous raw response and fee provenance
joined to adequate causal depth/trades and independently verified owned fills.
Next step is the bounded existing-GET observer integration, not another replay
of the already inspected negative-price windows or a new order grant.
