"""Persistent, broker-free lifecycle for the regime intraday sleeve."""

from __future__ import annotations

import json
import hashlib
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LEDGER_VERSION = "v2-durable-signal-order-ledger"
SHADOW_ACCOUNTING = "sampled_ohlc_stop_first_v1"


def performance_views(ledger: dict) -> dict:
    """Read-only views; historical rows are never migrated or rewritten."""
    shadow = [r for r in ledger.get("closed", []) if not r.get("paper_signal_id") and r.get("status") != "filled_closed"]
    current = [r for r in shadow if r.get("accounting_method") == SHADOW_ACCOUNTING]
    legacy = [r for r in shadow if r.get("accounting_method") != SHADOW_ACCOUNTING]
    orders = list(dict(ledger.get("orders") or {}).values())
    closed = [r for r in orders if r.get("status") == "filled_closed"]
    pnl = []
    for row in closed:
        entry = dict(row.get("broker") or {}).get("filled_avg_price")
        close = dict(dict(row.get("close_order") or {}).get("broker") or {}).get("filled_avg_price")
        if entry is not None and close is not None:
            pnl.append((abs(float(close)) - abs(float(entry))) * 100)
    return {"shadow": {"open_count": len(ledger.get("open") or {}), "closed_count": len(current),
                        "total_r": round(sum(float(r.get("realized_r") or 0) for r in current), 4),
                        "legacy_closed_count": len(legacy), "accounting_method": SHADOW_ACCOUNTING},
            "broker_paper": {"recorded_order_count": len(orders), "closed_roundtrips": len(closed),
                             "verified_fill_roundtrips": len(pnl), "missing_fill_roundtrips": len(closed) - len(pnl),
                             "gross_realized_dollars_from_fills": round(sum(pnl), 2)}}


def empty_ledger() -> dict[str, Any]:
    return {"version": LEDGER_VERSION, "open": {}, "closed": [], "orders": {}, "pending_candidates": {}, "events": []}


def assign_setup_identities(ledger: dict, scan: dict) -> None:
    """Persist episodes; rearm only after two distinct ready bars without a signal.

    This creates new opportunities, never retries an existing broker submission.
    Initial episodes preserve old IDs for safe deployment over existing orders.
    """
    states = ledger.setdefault("setup_episodes", {})
    signals = list(scan.get("signals") or [])
    active_keys = {(s.get("symbol"), s.get("strategy"), s.get("underlying_side")) for s in signals}
    for state in states.values():
        feature = dict(dict(scan.get("features") or {}).get(state["symbol"]) or {})
        stamp = str(feature.get("last_ts") or "")
        if not feature.get("ready") or not stamp or stamp <= state.get("last_bar", ""):
            continue
        key = (state["symbol"], state["strategy"], state["side"])
        if key not in active_keys:
            state["absent_bars"] = state.get("absent_bars", 0) + 1
            if state["absent_bars"] >= 2:
                state["armed"] = True
        state["last_bar"] = stamp
    for signal in signals:
        key = "|".join(str(signal.get(k) or "") for k in ("symbol", "strategy", "underlying_side"))
        stamp = str(dict(dict(scan.get("features") or {}).get(signal["symbol"]) or {}).get("last_ts") or "")
        base = str(signal["signal_id"])
        state = states.setdefault(key, {"symbol": signal["symbol"], "strategy": signal["strategy"], "side": signal["underlying_side"], "id": base})
        if state.get("session") and state["session"] != stamp[:10]:
            state.update(id=base, armed=False)
        if state.get("armed") and stamp:
            state["id"] = hashlib.sha256(f"{base}|setup|{stamp}".encode()).hexdigest()[:24]
        state.update(armed=False, absent_bars=0, last_bar=stamp, session=stamp[:10])
        signal["base_signal_id"] = base
        signal["signal_id"] = state["id"]


def record_pending_candidate(ledger: dict[str, Any], signal: dict[str, Any], plan: dict[str, Any], *, ts_utc: str, expires_at: str) -> dict[str, Any]:
    signal_id = str(signal.get("signal_id") or "")
    if not signal_id or plan.get("status") != "selected":
        return ledger
    pending = dict(ledger.get("pending_candidates") or {})
    if signal_id not in pending:
        pending[signal_id] = {"signal": dict(signal), "plan": dict(plan), "created_at": ts_utc, "expires_at": expires_at, "status": "awaiting_paper_approval"}
        events = list(ledger.get("events") or [])
        events.append({"event": "paper_candidate_queued", "signal_id": signal_id, "ts_utc": ts_utc, "expires_at": expires_at})
        ledger["events"] = events[-1000:]
    ledger["pending_candidates"] = pending
    return ledger


def pending_candidate(ledger: dict[str, Any], signal_id: str, *, now_utc: str) -> dict[str, Any] | None:
    row = dict(dict(ledger.get("pending_candidates") or {}).get(signal_id) or {})
    if not row or row.get("status") != "awaiting_paper_approval":
        return None
    try:
        if datetime.fromisoformat(str(now_utc).replace("Z", "+00:00")) >= datetime.fromisoformat(str(row.get("expires_at") or "").replace("Z", "+00:00")):
            return None
    except (TypeError, ValueError):
        return None
    return row


def paper_submission_decision(
    ledger: dict[str, Any],
    signal_id: str,
    *,
    session: str,
    max_trades_per_day: int = 2,
    max_consecutive_losses: int = 2,
    max_daily_loss_dollars: float = 200.0,
) -> dict[str, Any]:
    orders = dict(ledger.get("orders") or {})
    closed = [row for row in ledger.get("closed", []) if row.get("paper_signal_id") or row.get("status") == "filled_closed"]
    if not signal_id:
        return {"allowed": False, "reason": "missing_signal_id"}
    if signal_id in orders:
        return {"allowed": False, "reason": "duplicate_signal_order", "existing_order": orders[signal_id]}
    today_orders = [row for row in orders.values() if str(row.get("session") or "") == session]
    if len(today_orders) >= max(1, int(max_trades_per_day)):
        return {"allowed": False, "reason": "daily_trade_limit"}
    today_closed = [row for row in closed if str(row.get("session") or _session(str(row.get("exit_ts_utc") or ""))) == session]
    today_closed.sort(key=lambda row: str(row.get("exit_ts_utc") or ""))
    if any(row.get("realized_dollars") is None for row in today_closed):
        return {"allowed": False, "reason": "broker_realized_pnl_missing"}
    realized_dollars = sum(float(row.get("realized_dollars") or 0.0) for row in today_closed)
    if realized_dollars <= -abs(float(max_daily_loss_dollars)):
        return {"allowed": False, "reason": "daily_loss_lock", "realized_dollars": round(realized_dollars, 2)}
    loss_streak = 0
    for row in reversed(today_closed):
        if float(row.get("realized_dollars") or 0.0) < 0:
            loss_streak += 1
        else:
            break
    if loss_streak >= max(1, int(max_consecutive_losses)):
        return {"allowed": False, "reason": "consecutive_loss_lock", "loss_streak": loss_streak}
    active = [row for row in orders.values() if str(row.get("status") or "").lower() not in {"canceled", "cancelled", "expired", "filled_closed", "rejected"}]
    if active:
        return {"allowed": False, "reason": "active_paper_order_or_position", "active_count": len(active)}
    return {"allowed": True, "reason": "risk_checks_passed", "today_order_count": len(today_orders), "loss_streak": loss_streak}


def record_broker_order(ledger: dict[str, Any], signal_id: str, record: dict[str, Any], *, ts_utc: str | None = None) -> dict[str, Any]:
    now = _now(ts_utc)
    orders = dict(ledger.get("orders") or {})
    orders[signal_id] = {**dict(record), "signal_id": signal_id, "recorded_at": now, "session": _session(now)}
    events = list(ledger.get("events") or [])
    events.append({"event": "paper_order_recorded", "ts_utc": now, "signal_id": signal_id, "order_id": record.get("order_id"), "status": record.get("status")})
    ledger.update({"orders": orders, "events": events[-1000:], "updated_at": now})
    return ledger


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
        exit_price = price
        if position.get("accounting_method") == SHADOW_ACCOUNTING:
            bar_ts = str(feature.get("last_ts") or "")
            if bar_ts and bar_ts <= str(position.get("last_evaluated_bar_ts") or ""):
                continue
            high = float(feature.get("last_high") or price)
            low = float(feature.get("last_low") or price)
            bar_open = float(feature.get("last_open") or price)
            stop_hit = low <= stop if side == "buy" else high >= stop
            target_hit = high >= target if side == "buy" else low <= target
            # When both thresholds occur in the sampled bar, assume the stop came first.
            exit_price = (min(stop, bar_open) if side == "buy" else max(stop, bar_open)) if stop_hit else target
            position = {**position, "last_evaluated_bar_ts": bar_ts,
                        "coverage_note": "Sampled bars only; gaps and execution costs are not modeled."}
            opened[symbol] = position
        if not (stop_hit or target_hit):
            continue
        entry = float(position.get("entry_price") or 0.0)
        risk = abs(entry - stop)
        pnl_points = (exit_price - entry) if side == "buy" else (entry - exit_price)
        row = {**position, "exit_price": exit_price, "exit_ts_utc": now, "exit_reason": "stop" if stop_hit else "target", "realized_r": round(pnl_points / risk, 4) if risk > 0 else 0.0, "status": "shadow_closed"}
        closed.append(row)
        events.append({"event": "shadow_exit", "ts_utc": now, "symbol": symbol, "reason": row["exit_reason"], "realized_r": row["realized_r"]})
        opened.pop(symbol, None)

    realized_today = sum(float(row.get("realized_r") or 0.0) for row in closed if _session(str(row.get("exit_ts_utc") or "")) == session)
    blocked = realized_today <= -abs(float(max_daily_loss_r))
    for signal in list(scan.get("signals") or []):
        symbol = str(signal.get("symbol") or "").upper()
        if blocked or not symbol or symbol in opened or len(opened) >= max(1, int(max_open_positions)):
            continue
        position = {**dict(signal), "entry_ts_utc": now, "session": session, "status": "shadow_open",
                    "accounting_method": SHADOW_ACCOUNTING,
                    "last_evaluated_bar_ts": str(dict(features.get(symbol) or {}).get("last_ts") or "")}
        opened[symbol] = position
        events.append({"event": "shadow_entry", "ts_utc": now, "symbol": symbol, "strategy": signal.get("strategy")})

    ledger.update({"version": LEDGER_VERSION, "open": opened, "closed": closed[-500:], "orders": dict(ledger.get("orders") or {}), "pending_candidates": dict(ledger.get("pending_candidates") or {}), "events": events[-1000:], "updated_at": now})
    shadow_closed = [r for r in closed if not r.get("paper_signal_id") and r.get("status") != "filled_closed"]
    ledger["summary"] = {
        "scope": "underlying_shadow_simulation_including_legacy",
        "open_count": len(opened),
        "closed_count": len(shadow_closed),
        "realized_r_today": round(realized_today, 4),
        "daily_loss_blocked": blocked,
        "win_rate": round(sum(1 for row in shadow_closed if float(row.get("realized_r") or 0) > 0) / len(shadow_closed), 4) if shadow_closed else None,
        "average_r": round(sum(float(row.get("realized_r") or 0) for row in shadow_closed) / len(shadow_closed), 4) if shadow_closed else None,
        "paper_order_count": len(dict(ledger.get("orders") or {})),
    }
    return ledger
