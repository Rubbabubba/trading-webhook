"""Pure readiness assessment for paper and guarded-live promotion."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def readiness_snapshot(*, config: dict[str, Any], ledger: dict[str, Any], last_scan: dict[str, Any], paper_credentials_present: bool) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    scan_ts = str(last_scan.get("ts_utc") or "")
    try:
        age = max(0.0, (now - datetime.fromisoformat(scan_ts.replace("Z", "+00:00"))).total_seconds())
    except (TypeError, ValueError):
        age = None
    plans = [dict(row.get("plan") or {}) for row in list(last_scan.get("option_plans") or []) if isinstance(row, dict)]
    selected = [row for row in plans if row.get("status") == "selected"]
    live_selected = [row for row in selected if bool(row.get("live_eligible"))]
    paper_blockers = []
    if not paper_credentials_present:
        paper_blockers.append("paper_credentials_missing")
    if not bool(config.get("paper_submit_enabled")):
        paper_blockers.append("paper_submit_gate_closed")
    if age is None or age > float(config.get("max_scan_age_sec") or 600):
        paper_blockers.append("recent_scan_missing")
    live_blockers = list(paper_blockers)
    if str(config.get("option_feed") or "").lower() != "opra":
        live_blockers.append("opra_feed_required")
    if not live_selected:
        live_blockers.append("greeks_selected_contract_required")
    closed = list(ledger.get("closed") or [])
    paper_orders = dict(ledger.get("orders") or {})
    if not paper_orders:
        live_blockers.append("paper_order_roundtrip_required")
    if not any(str(row.get("status") or "").lower() in {"filled_closed", "closed"} for row in paper_orders.values()):
        live_blockers.append("paper_exit_recovery_required")
    if len(closed) < int(config.get("min_shadow_closed") or 10):
        live_blockers.append("minimum_shadow_sample_not_met")
    return {
        "paper_ready": not paper_blockers,
        "paper_blockers": paper_blockers,
        "live_ready": not live_blockers,
        "live_blockers": list(dict.fromkeys(live_blockers)),
        "last_scan_age_sec": round(age, 1) if age is not None else None,
        "selected_spread_count": len(selected),
        "live_eligible_spread_count": len(live_selected),
        "paper_order_count": len(paper_orders),
        "shadow_closed_count": len(closed),
        "live_submission": False,
    }
