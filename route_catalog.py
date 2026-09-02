"""Route classification for migration, operations, and cleanup."""

from __future__ import annotations


SENSITIVE_PATHS = {
    "/diagnostics/orders", "/diagnostics/journal", "/diagnostics/regime_intraday_ledger", "/diagnostics/paper_lifecycle",
}

SENSITIVE_PREFIXES = (
    "/dashboard",
    "/diagnostics/broker_",
    "/diagnostics/fast_broker_",
    "/diagnostics/live_",
    "/diagnostics/position",
    "/diagnostics/reconcile",
    "/diagnostics/strategy_state_broker_",
    "/diagnostics/weak_position_",
)


def is_sensitive_path(path: str) -> bool:
    return path in SENSITIVE_PATHS or any(path.startswith(prefix) for prefix in SENSITIVE_PREFIXES)


def classify_path(path: str, methods: list[str] | set[str]) -> dict:
    method_set = sorted(str(method) for method in methods if method not in {"HEAD", "OPTIONS"})
    mutating = any(method in {"POST", "PUT", "PATCH", "DELETE"} for method in method_set)
    if "regime_intraday" in path or path == "/dashboard/intraday":
        owner, lifecycle = "regime_intraday", "active"
    elif path.startswith("/worker/") or path in {"/health", "/", "/scanner/status"}:
        owner, lifecycle = "shared_runtime", "active"
    elif path.startswith("/dashboard"):
        owner, lifecycle = "swing", "legacy_active"
    elif any(token in path for token in ("lab", "simulation", "tuning", "audit", "forensics", "research", "replay")):
        owner, lifecycle = "swing_research", "deprecation_candidate"
    elif path.startswith("/diagnostics/"):
        owner, lifecycle = "swing", "legacy_active"
    else:
        owner, lifecycle = "shared_runtime", "active"
    return {"path": path, "methods": method_set, "owner": owner, "lifecycle": lifecycle, "mutating": mutating, "sensitive": is_sensitive_path(path) or mutating}


def build_route_catalog(routes, archived_routes=None) -> dict:
    rows = []
    for route in routes:
        path = str(getattr(route, "path", "") or "")
        if not path or path.startswith("/openapi") or path.startswith("/docs") or path.startswith("/redoc"):
            continue
        rows.append(classify_path(path, set(getattr(route, "methods", set()) or set())))
    rows.sort(key=lambda row: row["path"])
    counts = {}
    for row in rows:
        key = f"{row['owner']}:{row['lifecycle']}"
        counts[key] = counts.get(key, 0) + 1
    archived = sorted((dict(row) for row in (archived_routes or [])), key=lambda row: row.get("path", ""))
    return {"ok": True, "route_count": len(rows), "archived_route_count": len(archived), "counts": counts, "routes": rows, "archived_routes": archived}
