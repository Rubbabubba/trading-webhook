# Bounded Kalshi strategy factory v1

The demo research-sleeves worker now registers and evaluates one new
favorite/longshot ask-price hypothesis at a time. The fixed grammar has eight
combinations: two market groups (sports and non-sports) by four ask-price bins.
Historical resolved parent events rank untried combinations but never enter a
new version's prospective result. The chosen specification, hash, and UTC
registration time are durable in `research_sleeves.sqlite3`.

Only newly observed parent events after registration count. A completed event
uses the existing hypothetical ask-to-settlement outcome, which subtracts 2c
of fee stress, then this factory subtracts another 3c. This 5c stress is a
screen; it is not an actual fee record or a trade fill. At 15 complete events
with mean net at or below -1c, reject the version and register the next. At
45 days with fewer than 30 complete events, reject for low productivity. A
version becomes a *Demo trial candidate* only after at least 14 days, 30
independent completed events, positive total stressed net, and a positive
event-level 95% lower bound. This is a shadow gate only.

The factory never places orders, assumes a fill, changes live controls, or
creates a Life OS promotion packet. Before any candidate can run a Demo order
trial, a separate versioned executor must verify current market-specific fees,
use the shared one-contract Demo journal and risk limits, and produce actual
reconciled fill and fee evidence. The existing Life OS live-promotion screen
remains separate and requires owner review. If all eight combinations fail,
the factory reports `new_approved_primitive_required` instead of repeating
failed tests or claiming success.
