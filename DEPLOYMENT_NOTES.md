patch-093-lifecycle-hygiene-cleanup


## Patch 095 — admin-scope auth cleanup + universe_shadow fix
- Narrow `ADMIN_SECRET` enforcement to explicit `/admin/*` routes only by converting the legacy `require_admin_if_configured(...)` shim into a no-op for non-admin routes.
- Restores frictionless access to `/dashboard` and routine read-only diagnostics without headers.
- Fixes `/diagnostics/universe_shadow` crash by defining `regime_mode` before candidate evaluation.
- No strategy, release workflow, scanner universe, or risk logic changes.


Patch 096 note: corrects build metadata version string to patch-096-admin-scope-auth-and-universe-shadow while preserving Patch 095 logic.
