"""Read-only production verifier for the intraday-only production cutover."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


BASE_URL = (os.getenv("VERIFY_BASE_URL") or "https://trading-webhook-q4d5.onrender.com").rstrip("/")
RETIRED_ROUTE = "/diagnostics/swing_tuning_simulator"


def _request(path: str) -> tuple[int, dict, dict]:
    request = urllib.request.Request(f"{BASE_URL}{path}", headers={"User-Agent": "system-separation-verifier/1"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8", errors="replace")
            return response.status, json.loads(raw or "{}") if raw.lstrip().startswith("{") else {}, dict(response.headers)
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", errors="replace")
        return error.code, json.loads(raw or "{}") if raw.lstrip().startswith("{") else {}, dict(error.headers)


def main() -> int:
    checks: list[tuple[str, bool, object]] = []
    health_status, health, _ = _request("/health")
    systems = dict(health.get("systems") or {})
    intraday = dict(systems.get("regime_intraday") or {})
    checks.extend([
        ("health_200", health_status == 200, health_status),
        ("intraday_only", set(systems) == {"regime_intraday"}, sorted(systems)),
        ("intraday_paper_only", health.get("paper_only") is True and health.get("live_trading_enabled") is False and intraday.get("live_entries_enabled") is False, intraday),
    ])

    catalog_status, catalog, _ = _request("/diagnostics/route_catalog")
    active = {str(row.get("path")) for row in list(catalog.get("routes") or [])}
    checks.extend([
        ("catalog_200", catalog_status == 200, catalog_status),
        ("active_route_count_12", catalog.get("route_count") == 12, catalog.get("route_count")),
        ("no_swing_routes", not any("swing" in path for path in active), sorted(path for path in active if "swing" in path)),
        ("no_legacy_workers", not ({"/worker/exit", "/worker/scan_entries", "/worker/swing_fast_scan"} & active), sorted(active)),
    ])

    retired_status, _, _ = _request(RETIRED_ROUTE)
    dashboard_status, _, dashboard_headers = _request("/dashboard/intraday")
    worker_status, _, _ = _request("/worker/regime_intraday_scan")
    auth_header = next((value for key, value in dashboard_headers.items() if key.lower() == "www-authenticate"), "")
    checks.extend([
        ("retired_route_not_served", retired_status == 404, retired_status),
        ("dashboard_requires_auth", dashboard_status == 401 and "Basic" in str(auth_header), {"status": dashboard_status, "header": auth_header}),
        ("worker_rejects_get", worker_status == 405, worker_status),
    ])

    for name, passed, evidence in checks:
        print(f"{'PASS' if passed else 'FAIL'} {name}: {evidence}")
    return 0 if all(passed for _, passed, _ in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
