# Collection recovery: separately bounded successor

The approved2GB disk genuinely has1.08GB free, but v3 stopped at its frozen48MiB SQLite cap (85,475 records) and continuing capture v1 reached33,553,905 accounted bytes against32MiB. Storage expansion cannot unfreeze those trials. Main REST/forecast discovery continued; detailed stream capture did not.

New prospectively registered source v4 has its own48MiB cap,100,000 records,32MiB compressed cap,128MiB free floor and72h lifetime. Admission stops at44MiB to leave sealing room. No automatic rotation/reset. New formula comparison v3 uses unchanged native formulas/costs and separate16MiB/10000frame/100signal/72h limits, original5/30/300s+5s deadlines,900GET cap. All old source/comparison data, failures and two missed v2 marks stay retained read-only, never imported.

Fixed concrete issues: permanent continuing-capacity errors repeatedly generated writes; an exhausted archive now becomes a read-only view with no further quote/poll/fault writes. New source reports exact storage-stop reason, low disk can recover without a restart while lifetime caps cannot, and expired comparison admission is reported as closed rather than collecting. No new broker/order interface, source purchase, schedule or application model call.

77 initial regression cases plus5 subtests passed; final63 cases passed including new successor/source/deadline/monitor gates. Counts overlap. First attempt lacked pytest, second was interrupted on sandbox Windows loop initialization; elevated local synthetic run passed. Tests prove behavior only, not market observations, fills or profitability.

Deployment and prospective verification are pending at this engineering checkpoint. Old frozen strategy and research failures must remain visible.
