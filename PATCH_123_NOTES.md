PATCH 123 NOTES

Goal:
Fix stop logic regression introduced by Patch 122 without drifting into unrelated systems.

Changes:
- Added long-stop sanity clamping so a protective stop cannot arm at or above the live price.
- Preserved profit-lock intent by resetting invalid in-profit stops back below market, preferring entry when possible.
- Prevented dry-run exits and dry-run partial exits from transitioning plans into close_submitted lifecycle state.
- Left startup restore, reconcile, journal integrity, and entry selection untouched.

Expected outcome:
- No repeated false stop exits in dry run.
- Long plans keep protective logic below market.
- Patch 122 partial-profit and time-grace logic stay available without immediate self-triggering.
