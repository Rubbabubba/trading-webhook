"""Evidence-first attribution for verified paper option-spread roundtrips."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from regime_intraday_fill_accounting import verified_roundtrip


def _minutes(start: Any, end: Any) -> float | None:
    try:
        a = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
        b = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
        return round(max(0.0, (b - a).total_seconds()) / 60, 2)
    except (TypeError, ValueError):
        return None


def _bucket(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for row in rows:
        name = str(row.get(key) or "unknown")
        bucket = output.setdefault(name, {"count": 0, "wins": 0, "gross_pnl_dollars": 0.0})
        bucket["count"] += 1
        bucket["wins"] += int(float(row["gross_pnl_dollars"]) > 0)
        bucket["gross_pnl_dollars"] += float(row["gross_pnl_dollars"])
    for bucket in output.values():
        bucket["gross_pnl_dollars"] = round(bucket["gross_pnl_dollars"], 2)
        bucket["win_rate"] = round(bucket["wins"] / bucket["count"], 4)
        bucket["expectancy_dollars"] = round(bucket["gross_pnl_dollars"] / bucket["count"], 2)
    return output


def roundtrip_forensic_report(ledger: dict[str, Any], *, estimated_fee_dollars: float = 1.30) -> dict[str, Any]:
    rows = []
    unresolved = []
    for signal_id, record in dict(ledger.get("orders") or {}).items():
        if record.get("mechanical_test") or str(record.get("status") or "").lower() != "filled_closed":
            continue
        result = verified_roundtrip(record)
        if not result["complete"]:
            unresolved.append({"signal_id": signal_id, "reasons": result.get("problems")})
            continue
        signal, plan = dict(record.get("signal") or {}), dict(record.get("plan") or {})
        entry_broker = dict(record.get("broker") or {})
        close_order = dict(record.get("close_order") or {})
        close_broker = dict(close_order.get("broker") or {})
        gross = float(result["realized_dollars"])
        reason = str(close_order.get("reason") or dict(record.get("exit_decision") or {}).get("reason") or "unknown")
        thesis = "target_reached" if "target" in reason else ("stopped" if "stop" in reason else "other_exit")
        if thesis == "target_reached" and gross < 0:
            attribution = "underlying_target_but_option_loss"
        elif thesis == "stopped" and gross < 0:
            attribution = "underlying_setup_stopped"
        elif gross > 0:
            attribution = "profitable_roundtrip"
        else:
            attribution = "nonpositive_other_exit"
        entry_time = entry_broker.get("filled_at")
        hour = None
        try:
            hour = datetime.fromisoformat(str(entry_time).replace("Z", "+00:00")).hour
        except (TypeError, ValueError):
            pass
        rows.append({
            "signal_id": signal_id, "symbol": plan.get("underlying") or signal.get("symbol"),
            "strategy": signal.get("strategy") or "unknown", "direction": signal.get("underlying_side"),
            "entry_debit": result["entry_debit"], "exit_credit": result["exit_credit"],
            "gross_pnl_dollars": gross, "estimated_fees_dollars": abs(float(estimated_fee_dollars)),
            "net_pnl_dollars": round(gross - abs(float(estimated_fee_dollars)), 2),
            "exit_reason": reason, "thesis_outcome": thesis, "attribution": attribution,
            "hold_minutes": _minutes(entry_time, close_broker.get("filled_at") or record.get("closed_at")),
            "entry_hour_utc": hour, "fill_sources": {"entry": result["entry"].get("source"), "close": result["close"].get("source")},
        })
    gross = round(sum(row["gross_pnl_dollars"] for row in rows), 2)
    net = round(sum(row["net_pnl_dollars"] for row in rows), 2)
    return {
        "status": "complete" if rows and not unresolved else ("partial" if rows else "unavailable"),
        "verified_roundtrips": len(rows), "unresolved_roundtrips": len(unresolved), "unresolved": unresolved,
        "gross_pnl_dollars": gross, "net_after_estimated_fees_dollars": net,
        "win_rate": round(sum(row["gross_pnl_dollars"] > 0 for row in rows) / len(rows), 4) if rows else None,
        "by_symbol": _bucket(rows, "symbol"), "by_exit_reason": _bucket(rows, "exit_reason"),
        "by_attribution": _bucket(rows, "attribution"), "trades": rows,
        "conclusion": "No live promotion: require positive after-fee expectancy across at least 30 independent verified roundtrips.",
        "live_submission": False,
    }
