Patch 114 — preserve broker-backed execution state over scan/startup dry-run planning

- recovers open broker orders into authoritative pending-entry plans at startup/reconcile
- prevents startup/manual/dry-run planning from overwriting a live broker-backed open order
- adopts open broker orders for a symbol before creating any dry-run plan for that symbol
- patch marker: patch-114-preserve-broker-backed-execution-state
