# Route Migration Policy

The application currently exposes both the live swing system and the paper-only regime intraday system. Route cleanup must preserve that distinction.

## Ownership

- `swing`: the existing live production strategy and its operator surfaces.
- `regime_intraday`: the new SPY intraday paper-validation strategy.
- `shared`: health, broker infrastructure, worker controls, and compatibility surfaces used by both.
- `legacy_research`: older experiments and diagnostic routes that are candidates for retirement.

`GET /diagnostics/route_catalog` is the machine-readable inventory. `SYSTEM_ENDPOINTS.md` is generated from the source and is the human-readable inventory.

## Access Rules

Public health and readiness routes may expose only non-account-sensitive state. Dashboards and diagnostics containing positions, orders, journals, approval candidates, or account details require the configured `ADMIN_SECRET`. Browsers may use HTTP Basic authentication with any username and the secret as the password; API clients may send `x-admin-secret`.

Worker and webhook routes retain their existing dedicated authentication and execution gates.

## Deprecation Process

1. Classify the route as `deprecation_candidate` in `route_catalog.py`.
2. Confirm no dashboard, worker, test, or external automation depends on it.
3. Observe usage before removal.
4. Remove routes in small batches with focused tests and a rollback commit.
5. Regenerate `SYSTEM_ENDPOINTS.md` after each batch.

The live swing surface is not eligible for wholesale removal while it remains the production trading system. The intraday system cannot be promoted to live merely by changing a route or environment flag; its readiness gates must pass.
