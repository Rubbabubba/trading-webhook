# Demo book/trade stream feasibility — October16,2026UTC

**No profitable executable strategy qualified.** This is an authentic bounded data-access/reconstruction test. A continuous worker recorder is not yet integrated or deployed.

## Access verified

Using existing Demo credentials on the official dedicated Demo WebSocket endpoint, one60second connection subscribed to orderbook_delta and trade for the exact current eight-market native sampling window. No order/user-fill/private-position channel was subscribed. No API key, private key or signature was emitted. Redirects were disabled. The official PyPI websockets15.0.1 wheel was installed only in an isolated/tmp directory; no service, account, subscription, schedule or application model call was added.

The session connected, acknowledged both requested channels, and recorded479 normalized sanitized messages:2acknowledgements,8initial snapshots and469deltas. It produced176025 canonical JSON-line bytes, compressed to5395bytes. Exact captured archive and receipt hashes are retained locally and on the Demo disk. Actual records required no owner-field redaction. The archive preserves parsed source values and receipt times, not byte-identical original WebSocket whitespace/framing.

## Reconstruction verified

The pure decoder in opportunity_lab/kalshi_demo_stream_source.py has no transport, broker, strategy or order interface. Fourteen tests cover bounds/redaction plus fractional depth, sequence gaps, identity changes, deltas before snapshots, negative depth, stale clocks, side conflicts and unusable crossed books.

All477book-state observations across8markets reconstructed without a subscription sequence gap or source-domain fault. A separate Fraction-based reference reconstruction matched the Decimal decoder after every state, with final book SID1 sequence477. Apparent server-sending timestamp to local-receipt differences ranged0.590–56.233ms. Clock synchronization was not independently verified; these numbers are not a certified latency guarantee.

## Remaining evidence gap

**Zero public trades were received in this session.** No same-stream book/trade overlap, queue position, our activation, fill probability, actual fee, settlement return or profitability has been established. Acknowledging a trade subscription does not prove absence of trading or full trade-feed coverage. Demo market data does not qualify production liquidity.

The ordinary worker remains deployed at bbbb166f49ec4433206e7d5416def1de04825ad1. This probe ended and closed its connection; do not call it an autonomous continuing collector. The reusable decoder and source evidence are committed, but no new production runtime integration is claimed. Frozen V10/V14/V15 trials, earlier failures and risk/account baselines remain unchanged.

## Next engineering work

Register a separate durable stream-source protocol and implement bounded, isolated recording within the existing Demo service: exact target/metadata provenance, connection boundaries, sequence gaps, raw evidence, restart/capacity accounting, sanitized faults and honest status. Verify actual continuing capture and public-trade overlap before a separately frozen execution replay. Preserve all missing windows; never manufacture trades or use the transport test as a promotion gate.

Primary format/auth references: https://docs.kalshi.com/getting_started/quick_start_websockets ; https://docs.kalshi.com/websockets/orderbook-updates ; https://docs.kalshi.com/websockets/public-trades .
