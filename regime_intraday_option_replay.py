"""Auditable option-quote execution replay; never labels reconstructed data as OPRA."""

from __future__ import annotations

from typing import Any


def _number(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0


def _spread_pct(bid: float, ask: float) -> float:
    mid = (bid + ask) / 2
    return (ask - bid) / mid if bid > 0 and ask >= bid and mid > 0 else 999.0


def _entry_debit(row: dict[str, Any], slippage: float) -> float | None:
    long_ask, short_bid = _number(row, "long_ask"), _number(row, "short_bid")
    if min(long_ask, short_bid) <= 0 or long_ask <= short_bid:
        return None
    return round(long_ask - short_bid + slippage, 2)


def _exit_credit(row: dict[str, Any], slippage: float) -> float | None:
    long_bid, short_ask = _number(row, "long_bid"), _number(row, "short_ask")
    if min(long_bid, short_ask) <= 0 or long_bid <= short_ask:
        return None
    return round(max(0.01, long_bid - short_ask - slippage), 2)


def replay_option_execution(case: dict[str, Any]) -> dict[str, Any]:
    """Replay one defined-risk spread from timestamped bid/ask observations."""
    source = str(case.get("data_source") or "unspecified").lower()
    if source == "opra" and not case.get("opra_provenance"):
        return {"ok": False, "status": "rejected", "reason": "opra_provenance_required", "actual_opra": False}
    quotes = [dict(row) for row in list(case.get("quotes") or [])]
    entry_latency = max(0, int(case.get("entry_latency_minutes") or 0))
    exit_latency = max(0, int(case.get("exit_latency_minutes") or 0))
    confirmations_required = max(1, int(case.get("option_stop_confirmations") or 2))
    slippage = max(0.0, float(case.get("slippage_per_side") or 0))
    fees = max(0.0, float(case.get("roundtrip_fees_dollars") or 0))
    max_spread_pct = max(0.0, float(case.get("max_leg_spread_pct") or .08))
    limit = max(0.0, float(case.get("entry_limit_debit") or 0))
    if entry_latency >= len(quotes):
        return {"ok": True, "status": "no_fill", "reason": "latency_exceeds_quote_history", "actual_opra": source == "opra"}
    entry_quote = quotes[entry_latency]
    spreads = [_spread_pct(_number(entry_quote, "long_bid"), _number(entry_quote, "long_ask")),
               _spread_pct(_number(entry_quote, "short_bid"), _number(entry_quote, "short_ask"))]
    if max(spreads) > max_spread_pct:
        return {"ok": True, "status": "rejected", "reason": "wide_leg_spread", "maximum_leg_spread_pct": round(max(spreads), 4), "actual_opra": source == "opra"}
    entry = _entry_debit(entry_quote, slippage)
    if entry is None or (limit and entry > limit):
        return {"ok": True, "status": "no_fill", "reason": "entry_not_marketable", "entry_debit": entry, "actual_opra": source == "opra"}
    if str(case.get("broker_entry_status") or "filled") == "partially_filled":
        return {"ok": True, "status": "requires_attention", "reason": "partial_fill", "entry_debit": entry, "actual_opra": source == "opra"}
    if str(case.get("broker_entry_status") or "filled") == "rejected":
        return {"ok": True, "status": "rejected", "reason": "broker_rejected", "actual_opra": source == "opra"}

    stop_threshold = entry * (1 - max(0.0, min(1.0, float(case.get("stop_loss_fraction") or .5))))
    confirmations = 0
    path: list[dict[str, Any]] = []
    decision_index = None
    reason = None
    for index in range(entry_latency + 1, len(quotes)):
        row = quotes[index]
        credit = _exit_credit(row, slippage)
        observation = {"timestamp": row.get("timestamp"), "liquidation_credit": credit}
        if row.get("underlying_stop_hit"):
            reason, decision_index = "underlying_stop", index
        elif row.get("underlying_target_hit"):
            reason, decision_index = "underlying_target", index
        elif row.get("end_of_day"):
            reason, decision_index = "end_of_day", index
        elif credit is None:
            confirmations = 0
            observation["state"] = "quote_unavailable"
        elif credit <= stop_threshold:
            confirmations += 1
            observation.update(state="option_stop_breach", confirmations=confirmations)
            if confirmations >= confirmations_required:
                reason, decision_index = "confirmed_option_stop", index
        else:
            confirmations = 0
            observation["state"] = "hold"
        path.append(observation)
        if reason:
            break
    if decision_index is None:
        return {"ok": True, "status": "open_at_end_of_data", "entry_debit": entry, "stop_threshold": round(stop_threshold, 4), "path": path, "actual_opra": source == "opra"}
    fill_index = decision_index + exit_latency
    if fill_index >= len(quotes):
        return {"ok": True, "status": "exit_unfilled", "reason": reason, "entry_debit": entry, "path": path, "actual_opra": source == "opra"}
    exit_credit = _exit_credit(quotes[fill_index], slippage)
    if exit_credit is None:
        return {"ok": True, "status": "exit_requires_attention", "reason": "exit_quote_unavailable", "trigger": reason, "entry_debit": entry, "path": path, "actual_opra": source == "opra"}
    gross = round((exit_credit - entry) * 100, 2)
    return {"ok": True, "status": "closed", "reason": reason, "entry_debit": entry, "exit_credit": exit_credit,
            "gross_pnl_dollars": gross, "net_pnl_dollars": round(gross - fees, 2), "fees_dollars": fees,
            "entry_latency_minutes": entry_latency, "exit_latency_minutes": exit_latency,
            "stop_confirmations_required": confirmations_required, "path": path, "actual_opra": source == "opra",
            "data_source": source, "data_quality_note": "Actual OPRA quotes." if source == "opra" else "Not actual OPRA; conclusions are execution stress evidence only."}


def replay_option_batch(body: dict[str, Any]) -> dict[str, Any]:
    rows = [replay_option_execution(dict(case)) for case in list(body.get("cases") or [])]
    closed = [row for row in rows if row.get("status") == "closed"]
    return {"ok": True, "case_count": len(rows), "closed_count": len(closed), "results": rows,
            "net_pnl_dollars": round(sum(float(row.get("net_pnl_dollars") or 0) for row in closed), 2),
            "actual_opra_case_count": sum(bool(row.get("actual_opra")) for row in rows), "live_submission": False}
