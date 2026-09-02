"""Persistent, broker-free lifecycle for the regime intraday sleeve."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LEDGER_VERSION = "v1-underlying-r-shadow-ledger"


def empty_ledger() -> dict[str, Any]:
    return {"version": LEDGER_VERSION, "open": {}, "closed": [], "events": []}


def load_ledger(path: str) -> dict[str, Any]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else empty_ledger()
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return empty_ledger()


def save_ledger(path: str, ledger: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=str(target.parent))
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(ledger, stream, separators=(",", ":"), sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_name, target)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def _now(ts_utc: str | None = None) -> str:
    return ts_utc or datetime.now(timezone.utc).isoformat()


def _session(ts_utc: str) -> str:
    return str(ts_utc)[:10]


def update_ledger(
    ledger: dict[str, Any],
    scan: dict[str, Any],
    *,
    max_open_positions: int = 1,
    max_daily_loss_r: float = 2.0,
    ts_utc: str | None = None,
) -> dict[str, Any]:
    """Mark exits and admit new shadow positions; never talks to a broker."""
    now = _now(ts_utc or str(scan.get("ts_utc") or "") or None)
    session = _session(now)
    opened = dict(ledger.get("open") or {})
    closed = list(ledger.get("closed") or [])
    events = list(ledger.get("events") or [])
    features = dict(scan.get("features") or {})

    for symbol, position in list(opened.items()):
        feature = dict(features.get(symbol) or {})
        price = float(feature.get("price") or 0.0)
        if price <= 0:
            continue
        side = str(position.get("underlying_side") or "buy")
        stop = float(position.get("stop_price") or 0.0)
        target = float(position.get("target_price") or 0.0)
        stop_hit = price <= stop if side == "buy" else price >= stop
        target_hit = price >= target if side == "buy" else price <= target
        if not (stop_hit or target_hit):
            continue
        entry = float(position.get("entry_price") or 0.0)
        risk = abs(entry - stop)
        pnl_points = (price - entry) if side == "buy" else (entry - price)
        row = {**position, "exit_price": price, "exit_ts_utc": now, "exit_reason": "target" if target_hit else "stop", "realized_r": round(pnl_points / risk, 4) if risk > 0 else 0.0}
        closed.append(row)
        events.append({"event": "shadow_exit", "ts_utc": now, "symbol": symbol, "reason": row["exit_reason"], "realized_r": row["realized_r"]})
        opened.pop(symbol, None)

    realized_today = sum(float(row.get("realized_r") or 0.0) for row in closed if _session(str(row.get("exit_ts_utc") or "")) == session)
    blocked = realized_today <= -abs(float(max_daily_loss_r))
    for signal in list(scan.get("signals") or []):
        symbol = str(signal.get("symbol") or "").upper()
        if blocked or not symbol or symbol in opened or len(opened) >= max(1, int(max_open_positions)):
            continue
        position = {**dict(signal), "entry_ts_utc": now, "session": session, "status": "shadow_open"}
        opened[symbol] = position
        events.append({"event": "shadow_entry", "ts_utc": now, "symbol": symbol, "strategy": signal.get("strategy")})

    ledger.update({"version": LEDGER_VERSION, "open": opened, "closed": closed[-500:], "events": events[-1000:], "updated_at": now})
    ledger["summary"] = {
        "open_count": len(opened),
        "closed_count": len(closed),
        "realized_r_today": round(realized_today, 4),
        "daily_loss_blocked": blocked,
        "win_rate": round(sum(1 for row in closed if float(row.get("realized_r") or 0) > 0) / len(closed), 4) if closed else None,
        "average_r": round(sum(float(row.get("realized_r") or 0) for row in closed) / len(closed), 4) if closed else None,
    }
    return ledger
