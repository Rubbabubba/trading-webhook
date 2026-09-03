"""Read-only invalidation checks for pending paper entries."""
from datetime import datetime, timezone


def pending_entry_invalidation(signal: dict, feature: dict, submitted_at: str, now: datetime) -> str | None:
    try:
        bar = datetime.fromisoformat(str(feature.get("last_ts") or "").replace("Z", "+00:00"))
        submitted = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
        age = (now.astimezone(timezone.utc) - bar).total_seconds()
        if not feature.get("ready") or not 60 <= age <= 180 or bar < submitted:
            return None
        stop, target = float(signal["stop_price"]), float(signal["target_price"])
        low, high = float(feature["last_low"]), float(feature["last_high"])
        if signal.get("underlying_side") == "buy":
            return "underlying_stop_breached" if low <= stop else "underlying_target_already_reached" if high >= target else None
        if signal.get("underlying_side") == "sell":
            return "underlying_stop_breached" if high >= stop else "underlying_target_already_reached" if low <= target else None
    except (ValueError, KeyError, TypeError):
        return None
    return None
