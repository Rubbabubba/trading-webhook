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

After a shadow pass, a separate untouched holdout begins. Events from the
first split are excluded. Ten held events with a mean at or below -1c reject
the candidate, stopping new Demo entries. The holdout figures are still
counterfactual, not filled returns.

A passed version can also start a frozen **Demo-only** execution trial through
the existing one-contract order journal. It requotes a recent, untouched
event, checks that the actual ask still fits the registered bin and that the
market expires within 24 hours, then attempts an immediate-or-cancel buy. The
trial caps attempts at 3 per UTC day and 100 total, fills at 40, and stops
new entries after a 100c cumulative realized trial loss. Each new order's
price plus its 5c fee reserve must also fit the remaining 100c loss budget;
this can exclude high-price bins from Demo execution. The shared 110c per-order, 160c
capital, and 100c daily-loss controls still apply. A filled trial position
is held for settlement and reconciled against actual Demo fills, fees, and
settlements. After five fills, a flat realized loss of at least 20c retires
the trial version; a later qualified version can rotate in when the account
is flat and all orders are terminal. If settlement remains unresolved 48 hours after entry, the
journal stops new orders and reports the fault.

No candidate currently meets the shadow gate, so no factory Demo order can be
placed yet. The existing Life OS live-promotion screen remains separate and
requires a complete fee-aware evidence packet and owner review; this trial
does not create that packet or enable live trading. If all eight combinations
fail, the factory reports `new_approved_primitive_required` instead of
repeating failed tests or claiming success.
