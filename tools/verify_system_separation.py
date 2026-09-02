"""Read-only production verifier for the swing/intraday separation cutover."""

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
    swing = dict(systems.get("swing") or {})
    checks.extend([
        ("health_200", health_status == 200, health_status),
        ("intraday_paper_only", intraday.get("status") == "paper_validation" and intraday.get("live_entries_enabled") is False, intraday),
        ("legacy_swing_identified", swing.get("status") == "legacy_active", swing),
    ])

    catalog_status, catalog, _ = _request("/diagnostics/route_catalog")
    archived = {str(row.get("path")) for row in list(catalog.get("archived_routes") or [])}
    checks.extend([
        ("catalog_200", catalog_status == 200, catalog_status),
        ("active_route_count_243", catalog.get("route_count") == 243, catalog.get("route_count")),
        ("archived_route_count_34", catalog.get("archived_route_count") == 34, catalog.get("archived_route_count")),
        ("retired_route_cataloged", RETIRED_ROUTE in archived, len(archived)),
    ])

    retired_status, _, _ = _request(RETIRED_ROUTE)
    dashboard_status, _, dashboard_headers = _request("/dashboard/intraday")
    checks.extend([
        ("retired_route_not_served", retired_status == 404, retired_status),
        ("dashboard_requires_auth", dashboard_status == 401 and "Basic" in str(dashboard_headers.get("WWW-Authenticate", "")), dashboard_status),
    ])

    for name, passed, evidence in checks:
        print(f"{'PASS' if passed else 'FAIL'} {name}: {evidence}")
    return 0 if all(passed for _, passed, _ in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
