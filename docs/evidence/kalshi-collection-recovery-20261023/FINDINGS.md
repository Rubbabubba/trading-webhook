# Collection recovery: separately bounded successor

The approved2GB disk genuinely has1.08GB free, but v3 stopped at its frozen48MiB SQLite cap (85,475 records) and continuing capture v1 reached33,553,905 accounted bytes against32MiB. Storage expansion cannot unfreeze those trials. Main REST/forecast discovery continued; detailed stream capture did not.

New prospectively registered source v4 has its own48MiB cap,100,000 records,32MiB compressed cap,128MiB free floor and72h lifetime. Admission stops at44MiB to leave sealing room. No automatic rotation/reset. New formula comparison v3 uses unchanged native formulas/costs and separate16MiB/10000frame/100signal/72h limits, original5/30/300s+5s deadlines,900GET cap. All old source/comparison data, failures and two missed v2 marks stay retained read-only, never imported.

Fixed concrete issues: permanent continuing-capacity errors repeatedly generated writes; an exhausted archive now becomes a read-only view with no further quote/poll/fault writes. New source reports exact storage-stop reason, low disk can recover without a restart while lifetime caps cannot, and expired comparison admission is reported as closed rather than collecting. No new broker/order interface, source purchase, schedule or application model call.

77 initial regression cases plus5 subtests passed; final63 cases passed including new successor/source/deadline/monitor gates. Counts overlap. First attempt lacked pytest, second was interrupted on sandbox Windows loop initialization; elevated local synthetic run passed. Tests prove behavior only, not market observations, fills or profitability.

Deployment and prospective verification are pending at this engineering checkpoint. Old frozen strategy and research failures must remain visible.

## Genuine first deployment exposed a separate source defect

Commit4bdc034 deployed Live and genuinely recorded fresh books, then failed closed with book_level_capacity. One separately bounded20-message official DemoWSS diagnostic stopped at its first rejected snapshot: only no_dollars_fp was present, with one valid level; YES was omitted. Raw diagnostic SHA b7d9fe6ba67639245a67dc2ef6b2bd7473110d296fbfe8790e5bb0a2b427530b is retained. This was not a storage-cap condition or a profitable signal.

A separately preregistered availability-v5 adapter validates the supplied ladder, UUID, scope, clocks and sequence, then marks only that symbol unavailable. Subsequent deltas for that incomplete snapshot remain unavailable; no missing side is inferred as zero and no quote enters comparison until a complete snapshot exists. Other subscribed symbols can continue. Malformed/null/oversized/negative ladders and identity/sequence faults still poison the connection. Raw archive payloads are unchanged.

V4 source and comparison-v3 prefixes remain separate; new v5 source and comparison-v4 use fresh bounded files with identical per-study budgets and unchanged economic formulas. Final70 synthetic regressions passed, overlapping prior totals. Second deployment and fresh observation remain pending at this checkpoint.
