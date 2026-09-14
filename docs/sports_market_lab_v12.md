# Quote laboratory 1.2: game coverage

Separate prospective test of sampling allocation. Diagnosis at September 12 19:55 UTC found 13 of 16 reviewed afternoon candidate games had no laboratory entries; the old pending-first sampler can allocate all 200 requests to pending probes. Valid candidate winner quotes existed for several omitted games.

Reserve 40 of 200 requests for new active markets, interleave games and families in each queue, and allow either queue to borrow unused capacity. No changes to fill confirmation, costs, freshness, identity or one-contract sizing. Ten tests passed across versions 1.1 and 1.2, including saturated pending queues, diverse game/family coverage, closed-market filtering and unchanged execution guards.

Fresh ledger sports_paper/market_lab_v12_20260912/laboratory.sqlite3. Config configs/sports_market_lab_v12_20260912/manifest.json. Module opportunity_lab.sports_market_lab_v12. Launcher tools/start_sports_market_lab_v12.ps1; separate bounded recovery task Codex Sports Market Lab V12 Recovery 20260912. Stop September 18 12 UTC. Existing version 1.1 continues its independent probes and preserves positions. Do not combine either version or holding variant as a portfolio. Games already underway may enter only prospectively after launch; no historical states/fills imported.

Check new-game coverage and pending confirmation delays on every follow-up. Reservation improves access but cannot guarantee valid liquidity or fills; finite 200-book capacity remains. Include separate per-game and prior-Central-day email results, modeled costs and unresolved positions. This tests execution coverage, not profitability or a predictive edge.
