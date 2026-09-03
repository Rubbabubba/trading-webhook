"""Pure monitoring views and live-scan freshness guards."""
from datetime import datetime, timezone


def candidate_views(ledger: dict, *, now: datetime, blocker: str = "") -> dict:
    active, history = {}, {}
    for identity, original in dict(ledger.get("pending_candidates") or {}).items():
        row = dict(original)
        status = str(row.get("status") or "")
        if status == "awaiting_paper_approval":
            try:
                expires = datetime.fromisoformat(str(row.get("expires_at") or "").replace("Z", "+00:00"))
                if expires <= now:
                    row["display_status"] = "expired"
                elif blocker:
                    row["display_status"] = f"blocked: {blocker}"
                else:
                    row["display_status"] = status
                    active[identity] = row
                    continue
            except (ValueError, TypeError):
                row["display_status"] = "invalid_expiration"
        else:
            row["display_status"] = status
        history[identity] = row
    return {"active": active, "history": history}


def apply_live_freshness(scan: dict, *, now: datetime, max_age_sec: int = 180) -> None:
    """A live entry requires completed, recent bars for every sleeve input."""
    blocked = []
    for symbol, feature in dict(scan.get("features") or {}).items():
        try:
            stamp = datetime.fromisoformat(str(feature.get("last_ts") or "").replace("Z", "+00:00"))
            age = (now.astimezone(timezone.utc) - stamp).total_seconds()
            reason = "fresh" if 60 <= age <= max_age_sec else "incomplete_or_future_bar" if age < 60 else "stale_bar"
        except (ValueError, TypeError):
            age, reason = None, "missing_bar_timestamp"
        feature.update(bar_age_sec=round(age, 1) if age is not None else None, freshness=reason,
                       bar_count_ready=bool(feature.get("ready")))
        if reason != "fresh":
            feature["ready"] = False
            blocked.append(symbol)
    if blocked:
        scan.update(signals=[], signal_count=0, regime={"name": "not_ready", "direction": None,
                    "trade_allowed": False, "reason": "market_data_not_fresh", "blocked_symbols": blocked})
