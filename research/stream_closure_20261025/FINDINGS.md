# Finite stream experiment closure — October 25, 2026

The registered Demo source v5 reached its original 11,264-page admission stop at 2026-10-24 20:26:56 UTC. The archive is 46,137,344 bytes (44 MiB), below the unchanged 48 MiB hard ceiling. It contains 78,518 messages: 518 acknowledgements, 2,078 snapshots and 75,922 deltas across 259 epochs. A bounded final message-type count found **zero public trade messages in this archive**, not merely zero in its final epoch. This is selected Demo feed coverage, not evidence that other markets have no liquidity.

Comparison v4 ends with 5,792 accepted inputs across 361 input events and 363 tickers. These are not completed economic outcomes: there are six signals, five complete event paths, seventeen marks, one missing 300-second horizon, twenty requests and no pending outcomes. No new signal/outcome prefix was added after the October 24 audit. Retained quote outcomes were not replayed or newly downloaded.

The final classification is **insufficient evidence**. The frozen 100-complete-signal/40-complete-event gate fails. Authentic owned entry/queue/fees/exits, independently current metadata and untouched validation remain absent. The previously audited conditional displayed-bid liquidation totals remain -20/-16/-16 cents at 5/30/300 seconds under original stress; they are not realized trades or guaranteed exits. Positive midpoint results do not establish executable profit. The five-event sample does not justify a strategy rejection at the frozen sample threshold or a promotion pass.

The runtime comparison's `collecting` label persists after its source has stopped. It does not indicate active collection. Source status explicitly reports `capacity_stop`, and the compact packet reports inactive new inputs. The new offline closure adapter resolves this distinction for review; it does not change runtime state, caps, tables, rules or permissions. Old `usable_symbols` and acknowledgements are historical at closure, not fresh quotes.

`close.py` binds the final database count/hash receipt to the SHA256 of the already retained outcome audit. It rejects pending outcome drains, changed prefixes, identity/count mismatch, unsupported cap assertions, active sources, wrong protocols and owned fills needing a separate execution dossier. It never issues a promotion pass, even for a synthetic large positive sample. Twelve synthetic unit tests passed once. Empty experiments retain null trading results. Code tests are not operational or profitability evidence.

## Reproduction

Run `python -m unittest -v test_close` from this directory. Run `python close.py terminal-proof.json retained-bid-exit-audit.json d579062a6f38574c0d89c7e63cfd078f87b144b6dda830bd8c694e4b2c6f6412` to render the retained closure. The proof is a saved authorized read-only runtime receipt; this tool does not fetch market data or independently re-audit raw books. Source and comparison file hashes anchor the closed snapshot. No old failed studies are overwritten.

## Next binding stage

Authentic entry fillability and positive executable exit economics remain unproven. Before another distinct experiment, prepare a separately registered causal queue/fillability adapter with exact side/direction, independently current metadata/lifecycle, actual fee semantics, receipt/activation/TTL/cancel boundaries and explicit uncertain/partial/missing paths. The present archive has no public trades with which to validate that chain. Do not synthesize fills, relax costs, tune inspected prices or automatically roll over the exhausted allowance. No new capture, trial or order grant follows from this closure. Existing unrelated frozen workers and research cadence continue.

This is offline review engineering only. The deployed Demo worker remains `91bf044d1c1ad2b980e66665e84d8338db878b4c`; this module is not imported into it. Life OS owner UI redirected to login, so no owner-page engineering note was submitted. The priority-zero package was read through the existing authorized read-only backend connection; an honest closure note is retained locally pending authenticated page access.
