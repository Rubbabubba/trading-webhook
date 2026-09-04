"""Deterministic robustness gates for the intraday paper strategy."""

from __future__ import annotations

import random
from datetime import datetime
from statistics import fmean
from typing import Any

from regime_intraday_replay import cost_adjusted_report


def _seconds_between(start: Any, end: Any) -> float | None:
    try:
        first = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
        second = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
        return max(0.0, (second - first).total_seconds())
    except (TypeError, ValueError):
        return None


def update_canceled_entry_outcomes(ledger: dict[str, Any], scan: dict[str, Any]) -> None:
    """Advance explicitly counterfactual outcomes for zero-fill entry cancellations."""
    features = dict(scan.get("features") or {})
    for signal_id, record in dict(ledger.get("orders") or {}).items():
        if record.get("mechanical_test") or str(record.get("status") or "").lower() not in {"canceled", "cancelled", "expired", "rejected"}:
            continue
        broker = dict(record.get("broker") or {})
        if float(broker.get("filled_qty") or 0) != 0:
            continue
        signal = dict(record.get("signal") or {})
        symbol = str(signal.get("symbol") or "").upper()
        feature = dict(features.get(symbol) or {})
        bar_ts = str(feature.get("last_ts") or "")
        if not feature.get("ready") or not bar_ts:
            continue
        outcome = dict(record.get("counterfactual_underlying_outcome") or {})
        if outcome.get("status") in {"stop", "target", "eod"} or bar_ts <= str(outcome.get("last_evaluated_bar_ts") or ""):
            continue
        entry, stop, target = (float(signal.get(key) or 0) for key in ("entry_price", "stop_price", "target_price"))
        if min(entry, stop, target) <= 0:
            continue
        side = str(signal.get("underlying_side") or "buy")
        high = float(feature.get("last_high") or feature.get("price") or 0)
        low = float(feature.get("last_low") or feature.get("price") or 0)
        price = float(feature.get("price") or 0)
        stop_hit = low <= stop if side == "buy" else high >= stop
        target_hit = high >= target if side == "buy" else low <= target
        reason, exit_price = ("stop", stop) if stop_hit else (("target", target) if target_hit else ("tracking", None))
        try:
            stamp = datetime.fromisoformat(bar_ts.replace("Z", "+00:00"))
            if reason == "tracking" and stamp.hour * 60 + stamp.minute >= 15 * 60 + 45:
                reason, exit_price = "eod", price
        except (TypeError, ValueError):
            pass
        risk = abs(entry - stop)
        points = ((exit_price - entry) if side == "buy" else (entry - exit_price)) if exit_price is not None else None
        record["counterfactual_underlying_outcome"] = {
            "status": reason, "last_evaluated_bar_ts": bar_ts, "exit_price": exit_price,
            "realized_r": round(points / risk, 4) if points is not None and risk else None,
            "assumption": "Assumes the canceled spread filled when the underlying signal fired; not broker P/L.",
            "coverage": "Observed completed bars after cancellation; earlier intrabar path may be unavailable.",
            "stop_first_when_same_bar_touches_both": True,
        }


def entry_execution_analysis(ledger: dict[str, Any]) -> dict[str, Any]:
    """Summarize fill evidence and counterfactual outcomes without recommending repricing."""
    rows = []
    for signal_id, record in dict(ledger.get("orders") or {}).items():
        if record.get("mechanical_test"):
            continue
        plan = dict(record.get("plan") or {})
        selected = dict(plan.get("selection_quotes") or {})
        terminal = dict(record.get("terminal_quotes") or {})
        limit = float(plan.get("limit_debit") or 0)
        terminal_debit = terminal.get("entry_debit_from_quotes")
        required = max(0.0, float(terminal_debit) - limit) if terminal_debit is not None and limit else None
        broker = dict(record.get("broker") or {})
        rows.append({
            "signal_id": signal_id, "symbol": plan.get("underlying"), "status": record.get("status"),
            "limit_debit": limit or None, "selection_quote_debit": selected.get("entry_debit_from_quotes"),
            "terminal_quote_debit": terminal_debit, "quote_path_points": len(record.get("entry_quote_path") or []),
            "submit_to_terminal_seconds": _seconds_between(broker.get("submitted_at") or record.get("recorded_at"), broker.get("canceled_at") or record.get("cancel_requested_at") or record.get("reconciled_at")),
            "required_limit_increase_at_terminal": round(required, 4) if required is not None else None,
            "terminal_quote_was_within_one_cent": bool(required is not None and required <= 0.01),
            "counterfactual_underlying_outcome": record.get("counterfactual_underlying_outcome"),
            "note": "Quotes do not prove a fill; counterfactual outcomes are not broker P/L.",
        })
    canceled = [row for row in rows if str(row.get("status") or "").lower() in {"canceled", "cancelled", "expired", "rejected"}]
    return {"order_count": len(rows), "zero_fill_terminal_count": len(canceled), "rows": rows[-100:],
            "policy": "Observational only; no automatic entry repricing or resubmission."}


def paper_fill_reconciliation(ledger: dict[str, Any], *, minimum_roundtrips: int = 20) -> dict[str, Any]:
    rows = []
    pending = dict(ledger.get("pending_candidates") or {})
    for signal_id, record in dict(ledger.get("orders") or {}).items():
        if str(record.get("status") or "").lower() != "filled_closed":
            continue
        plan = dict(record.get("plan") or {})
        entry_broker = dict(record.get("broker") or {})
        close = dict(record.get("close_order") or {})
        close_broker = dict(close.get("broker") or {})
        expected_entry = float(plan.get("limit_debit") or 0)
        actual_entry = abs(float(entry_broker.get("filled_avg_price") or expected_entry))
        expected_exit = float(dict(record.get("valuation") or {}).get("liquidation_credit") or 0)
        actual_exit = abs(float(close_broker.get("filled_avg_price") or expected_exit))
        realized = round((actual_exit - actual_entry) * 100, 2) if actual_entry and actual_exit else None
        adverse_slippage = round(((actual_entry - expected_entry) + (expected_exit - actual_exit)) * 100, 2) if expected_entry and expected_exit else None
        rows.append({
            "signal_id": signal_id,
            "symbol": plan.get("underlying"),
            "expected_entry_debit": expected_entry or None,
            "actual_entry_debit": actual_entry or None,
            "expected_exit_credit": expected_exit or None,
            "actual_exit_credit": actual_exit or None,
            "actual_realized_dollars": realized,
            "adverse_slippage_dollars": adverse_slippage,
            "signal_to_submit_seconds": _seconds_between(dict(pending.get(signal_id) or {}).get("created_at"), record.get("recorded_at")),
            "submit_to_fill_seconds": _seconds_between(entry_broker.get("submitted_at"), entry_broker.get("filled_at")),
        })
    slippage = [float(row["adverse_slippage_dollars"]) for row in rows if row.get("adverse_slippage_dollars") is not None]
    realized = [float(row["actual_realized_dollars"]) for row in rows if row.get("actual_realized_dollars") is not None]
    return {
        "roundtrip_count": len(rows),
        "minimum_roundtrips": int(minimum_roundtrips),
        "forward_validation_ready": len(rows) >= int(minimum_roundtrips),
        "average_adverse_slippage_dollars": round(fmean(slippage), 2) if slippage else None,
        "actual_total_realized_dollars": round(sum(realized), 2),
        "actual_win_rate": round(sum(value > 0 for value in realized) / len(realized), 4) if realized else None,
        "rows": rows[-100:],
    }


def cost_stress(report: dict[str, Any], *, risk_dollars: float = 100.0, costs_r: tuple[float, ...] = (0.12, 0.20, 0.30, 0.50)) -> list[dict[str, Any]]:
    return [cost_adjusted_report(report, risk_dollars=risk_dollars, round_trip_cost_r=cost) for cost in costs_r]


def latency_stress(
    report: dict[str, Any],
    *,
    risk_dollars: float = 100.0,
    round_trip_cost_r: float = 0.12,
    adverse_r_per_minute: float = 0.08,
    delays_minutes: tuple[float, ...] = (0.0, 0.5, 1.0, 2.0),
) -> list[dict[str, Any]]:
    """Conservative sensitivity model until timestamped option fills exist."""
    rows = []
    for delay in delays_minutes:
        friction = round_trip_cost_r + max(0.0, delay) * adverse_r_per_minute
        result = cost_adjusted_report(report, risk_dollars=risk_dollars, round_trip_cost_r=friction)
        rows.append({"delay_minutes": delay, "modeled_adverse_r": round(delay * adverse_r_per_minute, 4), **result})
    return rows


def parameter_stability(walk_forward: dict[str, Any]) -> dict[str, Any]:
    candidates = list(walk_forward.get("candidates") or [])
    eligible = [row for row in candidates if row.get("eligible")]
    positive = [row for row in candidates if float(dict(row.get("cost_adjusted") or {}).get("net_average_r") or 0) > 0]
    scores = [float(row.get("selection_score") or 0) for row in eligible]
    return {
        "candidate_count": len(candidates),
        "eligible_count": len(eligible),
        "positive_after_cost_count": len(positive),
        "positive_after_cost_fraction": round(len(positive) / len(candidates), 4) if candidates else None,
        "eligible_score_range": [round(min(scores), 4), round(max(scores), 4)] if scores else None,
        "stable": bool(candidates and len(positive) / len(candidates) >= 0.5),
    }


def _drawdown(values: list[float]) -> float:
    equity = peak = worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = max(worst, peak - equity)
    return worst


def _percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * probability)))
    return round(ordered[index], 4)


def monte_carlo_daily(
    report: dict[str, Any],
    *,
    trials: int = 2000,
    block_size: int = 3,
    round_trip_cost_r: float = 0.12,
    seed: int = 20260902,
) -> dict[str, Any]:
    sessions = sorted({str(row.get("session")) for row in list(report.get("trades") or []) if row.get("session")})
    observed = int(report.get("accepted_session_count") or report.get("session_count") or len(sessions))
    daily = {session: 0.0 for session in sessions}
    for row in list(report.get("trades") or []):
        session = str(row.get("session") or "")
        if session:
            daily[session] = daily.get(session, 0.0) + float(row.get("realized_r") or 0) - abs(round_trip_cost_r)
    values = [daily[session] for session in sorted(daily)]
    values.extend([0.0] * max(0, observed - len(values)))
    if not values:
        return {"ready": False, "reason": "no_observed_sessions"}
    rng = random.Random(seed)
    totals, drawdowns, longest_losses = [], [], []
    size = max(1, min(int(block_size), len(values)))
    for _ in range(max(100, int(trials))):
        sample = []
        while len(sample) < len(values):
            start = rng.randrange(len(values))
            sample.extend(values[(start + offset) % len(values)] for offset in range(size))
        sample = sample[: len(values)]
        totals.append(sum(sample))
        drawdowns.append(_drawdown(sample))
        streak = longest = 0
        for value in sample:
            streak = streak + 1 if value < 0 else 0
            longest = max(longest, streak)
        longest_losses.append(longest)
    return {
        "ready": True,
        "trials": max(100, int(trials)),
        "sessions_per_trial": len(values),
        "block_size": size,
        "probability_negative_total": round(sum(value < 0 for value in totals) / len(totals), 4),
        "total_r_p05": _percentile(totals, 0.05),
        "total_r_p50": _percentile(totals, 0.50),
        "max_drawdown_r_p50": _percentile(drawdowns, 0.50),
        "max_drawdown_r_p95": _percentile(drawdowns, 0.95),
        "longest_losing_streak_p95": _percentile([float(value) for value in longest_losses], 0.95),
    }


def paper_promotion_gate(
    *,
    walk_forward: dict[str, Any],
    baseline: dict[str, Any],
    stability: dict[str, Any],
    costs: list[dict[str, Any]],
    monte_carlo: dict[str, Any],
) -> dict[str, Any]:
    test = dict(walk_forward.get("test") or {})
    blockers = []
    if not walk_forward.get("out_of_sample_positive"):
        blockers.append("out_of_sample_not_positive")
    if int(test.get("trade_count") or 0) < 20:
        blockers.append("out_of_sample_trade_count_below_20")
    if not stability.get("stable"):
        blockers.append("parameter_neighborhood_unstable")
    cost_030 = next((row for row in costs if abs(float(row.get("round_trip_cost_r") or 0) - 0.30) < 1e-9), {})
    if float(cost_030.get("net_average_r") or 0) <= 0:
        blockers.append("edge_fails_0_30r_cost_stress")
    loss_probability = monte_carlo.get("probability_negative_total")
    if not monte_carlo.get("ready") or loss_probability is None or float(loss_probability) > 0.25:
        blockers.append("monte_carlo_loss_probability_above_25pct")
    return {
        "paper_validation_pass": not blockers,
        "blockers": blockers,
        "promotion_locked": True,
        "promotion_rule": "human review required; validation never changes production settings",
        "baseline_trade_count": int(baseline.get("trade_count") or 0),
    }


def daily_goal_feasibility(report: dict[str, Any], *, risk_dollars: float = 100.0, goals: tuple[float, ...] = (100.0, 200.0)) -> dict[str, Any]:
    net = cost_adjusted_report(report, risk_dollars=risk_dollars, round_trip_cost_r=0.12)
    average = float(net.get("average_daily_dollars") or 0)
    drawdown_r = float(report.get("max_drawdown_r") or 0)
    rows = []
    for goal in goals:
        scale = goal / average if average > 0 else None
        required_risk = risk_dollars * scale if scale is not None else None
        rows.append({
            "daily_goal_dollars": goal,
            "required_risk_per_trade_dollars": round(required_risk, 2) if required_risk is not None else None,
            "projected_historical_drawdown_dollars": round(drawdown_r * required_risk, 2) if required_risk is not None else None,
            "fits_current_100_dollar_trade_cap": bool(required_risk is not None and required_risk <= 100.0),
        })
    return {"current_risk_dollars": risk_dollars, "modeled_average_daily_dollars": net.get("average_daily_dollars"), "goals": rows}


def validation_lab(*, baseline: dict[str, Any], walk_forward: dict[str, Any], instrument_reports: dict[str, dict[str, Any]], candidate_reports: dict[str, dict[str, Any]] | None = None, risk_dollars: float = 100.0) -> dict[str, Any]:
    costs = cost_stress(baseline, risk_dollars=risk_dollars)
    latency = latency_stress(baseline, risk_dollars=risk_dollars)
    stability = parameter_stability(walk_forward)
    monte = monte_carlo_daily(baseline)
    instruments = {
        name: {key: value for key, value in report.items() if key != "trades"} | {"cost_adjusted": cost_adjusted_report(report, risk_dollars=risk_dollars)}
        for name, report in instrument_reports.items()
    }
    candidates = {}
    for name, report in dict(candidate_reports or {}).items():
        ordinary = cost_adjusted_report(report, risk_dollars=risk_dollars, round_trip_cost_r=0.12)
        stressed = cost_adjusted_report(report, risk_dollars=risk_dollars, round_trip_cost_r=0.30)
        blockers = []
        if int(report.get("trade_count") or 0) < 20:
            blockers.append("trade_count_below_20")
        if float(ordinary.get("net_average_r") or 0) <= 0:
            blockers.append("ordinary_cost_edge_not_positive")
        if float(stressed.get("net_average_r") or 0) <= 0:
            blockers.append("fails_0_30r_cost_stress")
        candidates[name] = {key: value for key, value in report.items() if key != "trades"} | {
            "ordinary_cost": ordinary,
            "stressed_cost": stressed,
            "research_pass": not blockers,
            "blockers": blockers,
            "execution_enabled": False,
        }
    return {
        "paper_only": True,
        "historical_option_fill_model": False,
        "limitations": [
            "underlying-bar outcomes are not historical option-spread fills",
            "latency impact is a conservative sensitivity penalty until timestamped paper fills exist",
            "the previously reviewed holdout is no longer pristine future evidence",
        ],
        "instrument_comparison": instruments,
        "candidate_sleeves": candidates,
        "cost_stress": costs,
        "latency_stress": latency,
        "parameter_stability": stability,
        "monte_carlo": monte,
        "daily_goal_feasibility": daily_goal_feasibility(baseline, risk_dollars=risk_dollars),
        "gate": paper_promotion_gate(walk_forward=walk_forward, baseline=baseline, stability=stability, costs=costs, monte_carlo=monte),
    }
