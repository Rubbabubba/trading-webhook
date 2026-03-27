# Patch 096 — admin scope auth + universe shadow + corrected patch version

This patch is functionally the same as Patch 095, but corrects the in-code `PATCH_VERSION` constant so deployed build metadata reflects the actual baseline.

Included fixes:
- Restrict ADMIN_SECRET enforcement to explicit `/admin/*` routes only via the legacy no-op shim for non-admin routes.
- Keep true admin routes protected by `require_admin(...)`.
- Fix `/diagnostics/universe_shadow` by defining `regime_mode` before candidate evaluation.
- Update `PATCH_VERSION` to `patch-096-admin-scope-auth-and-universe-shadow`.
