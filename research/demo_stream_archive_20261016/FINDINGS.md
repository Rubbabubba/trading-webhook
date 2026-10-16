# Demo stream archive engineering

Implemented the durable storage foundation for an eventual existing-worker public Demo stream adapter. This module has no network, broker or order interface and is not imported by the deployed worker.

Eleven synthetic tests pass: interrupted process epochs, independent fresh subscriptions, sequence gaps, immutable messages, public-field restrictions, capacity and disk stops, preconnection receipt rejection, public-trade versus owned-fill semantics, and independent hash-chain reproduction. Initial test harness lacked writable temporary storage; the first executable run caught an integer/REAL timestamp serialization mismatch, corrected with canonical float receipt timestamps before hashing. Failed attempts were retained in the artifact root.

The retained 479-message feasibility prefix was replayed once through the new archive. Its original compressed SHA256 was checked, every message committed, and the closed database reopened with identical chain verification. This reuses old source evidence for engineering compatibility; it supplies no new market observation, fill, independent holdout or profitability evidence.

Limits are 100,000 messages,32MiB compressed source,48MiB SQLite,128MiB minimum free disk,256KiB/message,1,024 epochs and eight targets/epoch. A source/storage fault seals the current epoch, preserves valid prior records and does not store arbitrary invalid messages or exception text. A process restart records interruption and cannot reuse its old decoder state.

Continuous worker/network integration is still pending. Browser transport remains unresponsive, so fresh private Life OS capability-work, Kalshi review packet, Alpaca account reconciliation/fills, and private engineering receipt could not be verified or submitted. No Demo deployment or restart was performed. Frozen execution permissions and all risk/strategy gates are unchanged.

The two sports scripts ran once: zero newly reviewed/completed games; retained rolling evidence is stale (latest sample September18), so this is not current sports readiness. Public Life OS health matches f6666e37a42620639e65bb4d59dd9700db0feda4 and Alpaca health matches f2948678519a8c3908bde1f8e844d8c8e4076abe in paper mode. Public health does not establish private account reconciliation.
